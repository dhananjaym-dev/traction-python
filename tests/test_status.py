"""Status reads and tr:events parsing."""

import pytest

from tests.fakes import FakeRedis


def reader(redis):
    from traction.status import StatusReader

    return StatusReader(redis)


async def test_absent_scope_is_open():
    from traction.status import AirportStatus

    assert await reader(FakeRedis()).get("crm:py") is AirportStatus.OPEN


async def test_ground_stop_blocks_consumption():
    from traction.status import AirportStatus

    redis = FakeRedis()
    redis.hashes["tr:status:crm:py"] = {"status": "GROUND_STOP"}
    status = await reader(redis).get("crm:py")
    assert status is AirportStatus.GROUND_STOP
    assert not status.allows_consumption
    assert AirportStatus.OPEN.allows_consumption
    assert not AirportStatus.CLOSED.allows_consumption


async def test_gate_scope_key():
    redis = FakeRedis()
    redis.hashes["tr:status:crm:py/contact-sync"] = {"status": "CLOSED"}
    from traction.status import AirportStatus, scope_gate

    assert await reader(redis).get(scope_gate("crm:py", "contact-sync")) is AirportStatus.CLOSED


def test_scope_gate_rejects_bad_ids():
    from traction.status import scope_airport, scope_gate

    assert scope_airport("crm:py") == "crm:py"
    assert scope_gate("crm:py", "g-1.x") == "crm:py/g-1.x"
    with pytest.raises(ValueError):
        scope_gate("crm:py", "a/b")
    with pytest.raises(ValueError):
        scope_gate("crm:py", None)


async def test_unknown_value_and_errors_fail_open():
    from traction.status import AirportStatus

    redis = FakeRedis()
    redis.hashes["tr:status:crm:py"] = {"status": "SIDEWAYS"}
    assert await reader(redis).get("crm:py") is AirportStatus.OPEN

    redis.fail.add("hget")
    assert await reader(redis).get("crm:py") is AirportStatus.OPEN


def test_parse_status_changed():
    from traction.status import AirportStatus, parse_status_changed

    event = parse_status_changed("STATUS_CHANGED|crm:py|GROUND_STOP|AIRPORT_FAULT")
    assert (event.scope, event.status, event.reason) == (
        "crm:py", AirportStatus.GROUND_STOP, "AIRPORT_FAULT",
    )
    reopened = parse_status_changed("STATUS_CHANGED|crm:py/contact-sync|OPEN|")
    assert (reopened.scope, reopened.status, reopened.reason) == (
        "crm:py/contact-sync", AirportStatus.OPEN, None,
    )
    assert parse_status_changed("SOMETHING_ELSE|crm:py|OPEN|") is None
    assert parse_status_changed("STATUS_CHANGED|crm:py") is None
    assert parse_status_changed("STATUS_CHANGED||OPEN|") is None
    assert parse_status_changed("STATUS_CHANGED|crm:py|SIDEWAYS|") is None
    assert parse_status_changed("") is None


def test_scope_parts():
    from traction.status import scope_airport_id, scope_gate_id

    assert scope_airport_id("crm:py/contact-sync") == "crm:py"
    assert scope_gate_id("crm:py/contact-sync") == "contact-sync"
    assert scope_airport_id("crm:py") == "crm:py"
    assert scope_gate_id("crm:py") is None
