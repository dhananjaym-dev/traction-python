"""Tower.dispatch: validates the destination, mints the envelope, best-effort runway publish."""

from uuid import UUID

import pytest
from pydantic import BaseModel

from tests.fakes import FakeChannel, FakeConnection, FakeRedis
from tests.test_registry import make_spec


class Contact(BaseModel):
    id: str


async def make_tower():
    from traction.registry import AirportRegistry
    from traction.tower import Tower

    registry = AirportRegistry(
        connection=FakeConnection(), redis=FakeRedis(), owning_process="py-tenant"
    )
    await registry.register_all([make_spec(payload_types={"demo.Contact": Contact})])
    channel = FakeChannel()
    return Tower(registry, channel), channel


async def test_dispatch_mints_the_envelope():
    from traction.tower import DispatchOptions

    tower, channel = await make_tower()
    flight = await tower.dispatch(
        "crm:py", "contact-sync", Contact(id="c-1"),
        DispatchOptions(domain_ref="c-1", charter_ref=None),
    )
    assert UUID(str(flight.flight_id))
    assert (flight.attempt_count, flight.hold_count) == (0, 0)
    assert len(flight.correlation_id) == 6
    assert flight.payload_type == "demo.Contact"

    published = channel.default_exchange.published[0]
    assert published.routing_key == "tr.crm:py.runway"
    assert published.headers["__TypeId__"] == "dev.traction.core.api.Flight"
    body = published.json
    assert body["airportId"] == "crm:py"
    assert body["gateId"] == "contact-sync"
    assert body["domainRef"] == "c-1"
    assert body["charterRef"] is None
    assert body["payload"] == {"id": "c-1"}
    assert body["payloadType"] == "demo.Contact"


async def test_dispatch_rejects_unknown_destinations_before_publishing():
    tower, channel = await make_tower()
    with pytest.raises(ValueError):
        await tower.dispatch("unknown", "contact-sync", Contact(id="c"))
    with pytest.raises(ValueError):
        await tower.dispatch("crm:py", "unknown-gate", Contact(id="c"))
    with pytest.raises(ValueError):
        await tower.dispatch("crm:py", "contact-sync", None)
    with pytest.raises(ValueError):  # unregistered payload type

        class Other(BaseModel):
            pass

        await tower.dispatch("crm:py", "contact-sync", Other())
    assert channel.default_exchange.published == []


async def test_dispatch_without_options():
    tower, channel = await make_tower()
    flight = await tower.dispatch("crm:py", "contact-sync", Contact(id="c-2"))
    assert flight.domain_ref is None and flight.charter_ref is None


def test_public_api_reexports():
    import traction

    for name in (
        "AirportSpec", "SlotsConfig", "HoldPolicy", "Landed", "Skipped", "Incident",
        "IncidentCategory", "Evidence", "Flight", "Tower", "DispatchOptions",
        "AirportRegistry", "AirportStatus",
    ):
        assert hasattr(traction, name), name
