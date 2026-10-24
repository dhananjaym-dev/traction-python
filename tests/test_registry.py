"""AirportRegistry: validate → declare → announce → consume; pause/resume; tr:events."""

import pytest
from pydantic import BaseModel

from tests.fakes import FakeConnection, FakeRedis


class Contact(BaseModel):
    id: str


def make_spec(airport_id="crm:py", **overrides):
    from traction.spi import AirportSpec, Landed, SlotsConfig

    async def land(flight):
        return None

    base = dict(
        airport_id=airport_id,
        display_name="Acme CRM (Python)",
        slots=SlotsConfig.of(20, 1.0),
        gates={"contact-sync": 1, "company-sync": 2},
        pilot=type("P", (), {"land": staticmethod(land)})(),
        reporter=type("R", (), {"report": staticmethod(lambda flight, error: Landed())})(),
        payload_types={"demo.Contact": Contact},
    )
    base.update(overrides)
    return AirportSpec(**base)


def make_registry(connection=None, redis=None):
    from traction.registry import AirportRegistry

    return AirportRegistry(
        connection=connection or FakeConnection(),
        redis=redis or FakeRedis(),
        owning_process="py-tenant",
    )


def tower_messages(connection):
    return [
        m
        for ch in connection.channels
        for m in ch.default_exchange.published
        if m.routing_key == "tr.tower.queue"
    ]


async def test_registration_declares_announces_and_consumes():
    connection = FakeConnection()
    registry = make_registry(connection)
    await registry.register_all([make_spec()])

    declared = [name for ch in connection.channels for name, _ in ch.declared]
    for queue in ("tr.crm:py.parked", "tr.crm:py.holding", "tr.crm:py.runway",
                  "tr.dlq", "tr.tower.queue"):
        assert queue in declared

    announcements = tower_messages(connection)
    assert len(announcements) == 1
    assert announcements[0].headers["__TypeId__"] == "dev.traction.core.registry.AirportAnnouncement"
    body = announcements[0].json
    assert body["airportId"] == "crm:py"
    assert body["owningProcess"] == "py-tenant"
    assert body["displayName"] == "Acme CRM (Python)"
    assert (body["slotsLimit"], body["slotsWindowSeconds"], body["slotsBurst"]) == (20, 1, 20)
    assert body["gates"] == [
        {"gateId": "contact-sync", "slotCost": 1},
        {"gateId": "company-sync", "slotCost": 2},
    ]

    consume_channel = connection.channels[0]
    assert consume_channel.qos == 1  # one flight at a time per airport
    runway = consume_channel.queues["tr.crm:py.runway"]
    assert len(runway.consumers) == 1
    assert registry.is_registered("crm:py")
    assert registry.gates_of("crm:py") == {"contact-sync": 1, "company-sync": 2}
    assert registry.gates_of("unknown") is None


async def test_validate_all_before_any_side_effect():
    connection = FakeConnection()
    registry = make_registry(connection)
    bad = make_spec(airport_id="other", gates={"g": 999})  # cost above burst
    with pytest.raises(ValueError):
        await registry.register_all([make_spec(), bad])
    assert connection.channels == []  # nothing declared, nothing announced


async def test_duplicate_airport_id_rejected():
    registry = make_registry()
    with pytest.raises(ValueError):
        await registry.register_all([make_spec(), make_spec()])


async def test_pause_cancels_and_is_idempotent():
    connection = FakeConnection()
    registry = make_registry(connection)
    await registry.register_all([make_spec()])
    runway = connection.channels[0].queues["tr.crm:py.runway"]

    await registry.request_pause("crm:py")
    assert runway.consumers == {}
    await registry.request_pause("crm:py")  # no-op
    await registry.request_pause("unknown")  # never raises
    assert len(runway.cancelled) == 1


async def test_resume_rechecks_stored_status():
    connection = FakeConnection()
    redis = FakeRedis()
    registry = make_registry(connection, redis)
    await registry.register_all([make_spec()])
    runway = connection.channels[0].queues["tr.crm:py.runway"]
    await registry.request_pause("crm:py")

    # Stored status still GROUND_STOP: a stale OPEN event must not restart the airport.
    redis.hashes["tr:status:crm:py"] = {"status": "GROUND_STOP"}
    await registry.handle_event("STATUS_CHANGED|crm:py|OPEN|")
    assert runway.consumers == {}

    redis.hashes["tr:status:crm:py"] = {"status": "OPEN"}
    await registry.handle_event("STATUS_CHANGED|crm:py|OPEN|")
    assert len(runway.consumers) == 1

    await registry.handle_event("STATUS_CHANGED|crm:py|OPEN|")  # idempotent re-broadcast
    assert len(runway.consumers) == 1


async def test_events_pause_on_non_consuming_status():
    connection = FakeConnection()
    registry = make_registry(connection)
    await registry.register_all([make_spec()])
    runway = connection.channels[0].queues["tr.crm:py.runway"]

    await registry.handle_event("STATUS_CHANGED|crm:py|CLOSED|OPERATOR")
    assert runway.consumers == {}


async def test_events_ignore_gate_scopes_unknown_airports_and_garbage():
    connection = FakeConnection()
    registry = make_registry(connection)
    await registry.register_all([make_spec()])
    runway = connection.channels[0].queues["tr.crm:py.runway"]

    await registry.handle_event("STATUS_CHANGED|crm:py/contact-sync|CLOSED|")
    await registry.handle_event("STATUS_CHANGED|other:airport|CLOSED|")
    await registry.handle_event("garbage")
    assert len(runway.consumers) == 1  # untouched


async def test_stop_cancels_consumers_and_closes_channels():
    connection = FakeConnection()
    registry = make_registry(connection)
    await registry.register_all([make_spec()])
    await registry.stop()
    runway = connection.channels[0].queues["tr.crm:py.runway"]
    assert runway.consumers == {}
    assert all(ch.closed for ch in connection.channels)


async def test_stop_waits_for_the_in_flight_flight():
    # A graceful stop must let the delivery that is mid-land finish (and settle) before the
    # channels close, or every deploy under load costs a duplicate vendor call.
    import asyncio

    from tests.fakes import FakeIncomingMessage

    connection = FakeConnection()
    redis = FakeRedis()
    redis.evalsha_results = [[1, 0]]  # clearance granted
    gate = asyncio.Event()
    landed = []

    class SlowPilot:
        async def land(self, flight):
            await gate.wait()
            landed.append(flight)

    registry = make_registry(connection, redis)
    await registry.register_all([make_spec(pilot=SlowPilot())])
    runway = connection.channels[0].queues["tr.crm:py.runway"]
    callback = next(iter(runway.consumers.values()))

    message = FakeIncomingMessage({
        "flightId": "1f0e8a9c-9b1d-4b7e-8a30-2f6f3f3b9b21",
        "airportId": "crm:py", "gateId": "contact-sync", "domainRef": "c-1",
        "charterRef": None, "attemptCount": 0, "holdCount": 0,
        "correlationId": "a1b2c3", "payloadType": "demo.Contact",
        "payload": {"id": "c-1"},
    })
    delivery = asyncio.create_task(callback(message))
    for _ in range(20):  # let it reach the pilot
        await asyncio.sleep(0)

    stop_task = asyncio.create_task(registry.stop())
    for _ in range(20):
        await asyncio.sleep(0)
    assert not stop_task.done()
    assert not any(ch.closed for ch in connection.channels)  # still draining

    gate.set()
    await asyncio.wait_for(stop_task, timeout=2)
    await delivery
    assert landed and message.acks == 1
    assert all(ch.closed for ch in connection.channels)
