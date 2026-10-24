"""Clearance: the shared token bucket, fail closed."""

import hashlib
from importlib import resources

import pytest

from tests.fakes import FakeRedis, FakeRedisError

# The script is the cross-language contract: byte-identical to the Java repo's
# traction-core/src/main/resources/traction/clearance.lua.
JAVA_CLEARANCE_LUA_SHA256 = "c0b6b3fc744e59438bac6715fdeb4c8c45172250e1d30c4a0931b2f27de3285e"


def slots(limit=20, window_s=1.0, burst=None):
    from traction.spi import SlotsConfig

    return SlotsConfig(limit=limit, window_s=window_s, burst=burst or limit)


def service(redis):
    from traction.clearance import ClearanceService

    return ClearanceService(redis)


def test_lua_is_vendored_verbatim():
    script = (resources.files("traction") / "clearance.lua").read_bytes()
    assert hashlib.sha256(script).hexdigest() == JAVA_CLEARANCE_LUA_SHA256


async def test_grant():
    redis = FakeRedis()
    redis.evalsha_results = [[1, 0]]
    result = await service(redis).acquire("crm:py", slots(), cost=1)
    assert result.granted and result.wait_ms == 0
    sha, keys, args = redis.evalsha_calls[0]
    assert keys == ["tr:slots:crm:py"]
    assert args == ["1000"]


async def test_denial_returns_wait():
    redis = FakeRedis()
    redis.evalsha_results = [[0, 450]]
    result = await service(redis).acquire("crm:py", slots(), cost=1)
    assert not result.granted and result.wait_ms == 450


async def test_cost_scales_weight_and_bounds():
    from traction.clearance import ClearanceService

    redis = FakeRedis()
    redis.evalsha_results = [[1, 0]]
    svc = ClearanceService(redis)
    await svc.acquire("crm:py", slots(), cost=5)
    assert redis.evalsha_calls[0][2] == ["5000"]
    with pytest.raises(ValueError):
        await svc.acquire("crm:py", slots(), cost=0)
    with pytest.raises(ValueError):
        await svc.acquire("crm:py", slots(burst=20), cost=21)


async def test_reconcile_writes_only_config_fields_once():
    redis = FakeRedis()
    redis.evalsha_results = [[1, 0], [1, 0]]
    svc = service(redis)
    await svc.acquire("crm:py", slots(limit=20, window_s=1.0), cost=1)
    await svc.acquire("crm:py", slots(limit=20, window_s=1.0), cost=1)
    assert redis.hset_calls == [
        ("tr:slots:crm:py", {"capacity_milli": "20000", "refill_per_sec_milli": "20000"})
    ]


async def test_reconcile_rewrites_on_changed_slots():
    redis = FakeRedis()
    redis.evalsha_results = [[1, 0], [1, 0]]
    svc = service(redis)
    await svc.acquire("crm:py", slots(limit=20, window_s=1.0), cost=1)
    await svc.acquire("crm:py", slots(limit=10, window_s=1.0), cost=1)
    assert len(redis.hset_calls) == 2
    assert redis.hset_calls[1][1]["refill_per_sec_milli"] == "10000"


async def test_refill_formula_rounds_up_to_one():
    # 1 per hour: round(1 * 1_000_000 / 3_600_000) = 0 -> floored to 1 (Java Math.max(1, ...)).
    redis = FakeRedis()
    redis.evalsha_results = [[1, 0]]
    await service(redis).acquire("slow", slots(limit=1, window_s=3600.0), cost=1)
    assert redis.hset_calls[0][1]["refill_per_sec_milli"] == "1"


async def test_unconfigured_after_reconcile_fails_closed():
    from traction.clearance import ClearanceUnavailableError

    redis = FakeRedis()
    redis.evalsha_results = [[-1, 0], [1, 0]]
    svc = service(redis)
    with pytest.raises(ClearanceUnavailableError):
        await svc.acquire("crm:py", slots(), cost=1)
    # the config was dropped from the applied cache, so the next acquire re-writes it
    await svc.acquire("crm:py", slots(), cost=1)
    assert len(redis.hset_calls) == 2


async def test_redis_failure_fails_closed():
    from traction.clearance import ClearanceUnavailableError

    redis = FakeRedis()
    redis.fail.add("evalsha")
    with pytest.raises(ClearanceUnavailableError):
        await service(redis).acquire("crm:py", slots(), cost=1)


async def test_noscript_reloads_and_retries():
    import redis.exceptions as redis_exceptions

    redis = FakeRedis()
    redis.evalsha_results = [redis_exceptions.NoScriptError(), [1, 0]]
    result = await service(redis).acquire("crm:py", slots(), cost=1)
    assert result.granted
    assert len(redis.scripts) == 2  # loaded once lazily, once after NOSCRIPT


async def test_reconcile_failure_fails_closed_and_stays_unapplied():
    from traction.clearance import ClearanceUnavailableError

    redis = FakeRedis()
    redis.fail.add("hset")
    svc = service(redis)
    with pytest.raises(ClearanceUnavailableError):
        await svc.acquire("crm:py", slots(), cost=1)
    redis.fail.clear()
    redis.evalsha_results = [[1, 0]]
    await svc.acquire("crm:py", slots(), cost=1)  # retried config write
    assert len(redis.hset_calls) == 1


async def test_unexpected_script_result_fails_closed():
    from traction.clearance import ClearanceUnavailableError

    redis = FakeRedis()
    redis.evalsha_results = [None]
    with pytest.raises(ClearanceUnavailableError):
        await service(redis).acquire("crm:py", slots(), cost=1)
