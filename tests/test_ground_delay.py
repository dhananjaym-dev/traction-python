"""Ground delay: the value is the resume time; the TTL is only the backstop."""

import time

from tests.fakes import FakeRedis


def service(redis):
    from traction.ground_delay import GroundDelayService

    return GroundDelayService(redis)


async def test_delay_writes_resume_epoch_with_ttl():
    redis = FakeRedis()
    before = time.time()
    await service(redis).delay("crm:py", 30.0)
    key, value, px = redis.set_calls[0]
    assert key == "tr:grounddelay:crm:py"
    assert abs(int(value) - int((before + 30) * 1000)) < 2000
    assert px == 30_000


async def test_delay_write_failure_is_best_effort():
    redis = FakeRedis()
    redis.fail.add("set")
    await service(redis).delay("crm:py", 30.0)  # must not raise


async def test_remaining_reads_the_value():
    redis = FakeRedis()
    redis.strings["tr:grounddelay:crm:py"] = str(int((time.time() + 10) * 1000))
    remaining = await service(redis).remaining("crm:py")
    assert 8.0 < remaining <= 10.0


async def test_remaining_none_when_absent_past_garbage_or_error():
    svc = service(FakeRedis())
    assert await svc.remaining("crm:py") is None

    redis = FakeRedis()
    redis.strings["tr:grounddelay:crm:py"] = str(int((time.time() - 5) * 1000))
    assert await service(redis).remaining("crm:py") is None

    redis.strings["tr:grounddelay:crm:py"] = "not-a-number"
    assert await service(redis).remaining("crm:py") is None

    redis.fail.add("get")
    assert await service(redis).remaining("crm:py") is None
