"""Tenant SPI: outcomes, evidence, policies, the airport spec."""

import pytest
from pydantic import BaseModel


class Contact(BaseModel):
    id: str


def spec_kwargs(**overrides):
    from traction.spi import SlotsConfig

    async def land(flight):
        return None

    def report(flight, error):
        from traction.spi import Landed

        return Landed()

    base = dict(
        airport_id="crm:py",
        display_name="Acme CRM (Python)",
        slots=SlotsConfig.of(20, 1.0),
        gates={"contact-sync": 1, "company-sync": 1},
        pilot=type("P", (), {"land": staticmethod(land)})(),
        reporter=type("R", (), {"report": staticmethod(report)})(),
        payload_types={"demo.Contact": Contact},
    )
    base.update(overrides)
    return base


def test_hold_policy_defaults():
    from traction.spi import HoldPolicy, IncidentCategory

    assert HoldPolicy.default_for(IncidentCategory.TRANSIENT).schedule == (30, 120, 600)
    assert HoldPolicy.default_for(IncidentCategory.INTERNAL).schedule == (30,)
    assert HoldPolicy.default_for(IncidentCategory.GATE_FAULT).schedule == ()
    assert HoldPolicy.default_for(IncidentCategory.RATE_LIMIT).schedule == ()


def test_evidence_from_exception_and_display():
    from traction.spi import Evidence

    evidence = Evidence.from_exception(ValueError("boom " + "x" * 400))
    assert evidence.code == "ValueError"
    assert evidence.message_key is None
    assert len(evidence.excerpt) == 300
    assert evidence.display().startswith("ValueError | boom")
    assert Evidence.from_exception(None) is None
    assert Evidence(None, None, None).display() is None


def test_evidence_from_exception_empty_message():
    from traction.spi import Evidence

    evidence = Evidence.from_exception(RuntimeError())
    assert evidence.code == "RuntimeError"
    assert evidence.excerpt is None


def test_skipped_requires_reason():
    from traction.spi import Skipped

    with pytest.raises(ValueError):
        Skipped("  ")
    assert Skipped("GATE_GROUNDED").reason == "GATE_GROUNDED"


def test_slots_config_validation():
    from traction.spi import SlotsConfig

    assert SlotsConfig.of(20, 1.0).burst == 20
    with pytest.raises(ValueError):
        SlotsConfig(limit=0, window_s=1.0, burst=1)
    with pytest.raises(ValueError):
        SlotsConfig(limit=1, window_s=0, burst=1)
    with pytest.raises(ValueError):
        SlotsConfig(limit=1, window_s=1.0, burst=0)


def test_spec_validation():
    from traction.spi import AirportSpec

    AirportSpec(**spec_kwargs()).validate()

    with pytest.raises(ValueError):  # bad gate id (a '/' would break scope keys)
        AirportSpec(**spec_kwargs(gates={"contact/sync": 1})).validate()
    with pytest.raises(ValueError):  # bad airport id
        AirportSpec(**spec_kwargs(airport_id="crm acme")).validate()
    with pytest.raises(ValueError):  # cost above burst can never clear
        AirportSpec(**spec_kwargs(gates={"contact-sync": 21})).validate()
    with pytest.raises(ValueError):  # cost below 1
        AirportSpec(**spec_kwargs(gates={"contact-sync": 0})).validate()
    with pytest.raises(ValueError):  # missing pilot
        AirportSpec(**spec_kwargs(pilot=None)).validate()
    with pytest.raises(ValueError):  # missing reporter
        AirportSpec(**spec_kwargs(reporter=None)).validate()


def test_hold_policy_override_only_for_holding_categories():
    from traction.spi import AirportSpec, HoldPolicy, IncidentCategory

    spec = AirportSpec(**spec_kwargs(hold_policy=HoldPolicy(schedule=(5,))))
    assert spec.hold_policy_for(IncidentCategory.TRANSIENT).schedule == (5,)
    assert spec.hold_policy_for(IncidentCategory.INTERNAL).schedule == (5,)
    assert spec.hold_policy_for(IncidentCategory.GATE_FAULT).schedule == ()

    plain = AirportSpec(**spec_kwargs())
    assert plain.hold_policy_for(IncidentCategory.TRANSIENT).schedule == (30, 120, 600)


def test_payload_type_name_reverse_lookup():
    from traction.spi import AirportSpec

    spec = AirportSpec(**spec_kwargs())
    assert spec.payload_type_name(Contact(id="c1")) == "demo.Contact"

    class Unregistered(BaseModel):
        pass

    with pytest.raises(ValueError):
        spec.payload_type_name(Unregistered())


def test_slot_cost_fallback():
    from traction.spi import AirportSpec

    spec = AirportSpec(**spec_kwargs(gates={"contact-sync": 3}))
    assert spec.slot_cost("contact-sync") == 3
    assert spec.slot_cost("gone-gate") == 1
    assert spec.slot_cost(None) == 1


def test_incident_category_values():
    from traction.spi import IncidentCategory

    assert {c.value for c in IncidentCategory} == {
        "AIRPORT_FAULT", "GATE_FAULT", "FLIGHT_FAULT", "TRANSIENT", "RATE_LIMIT", "INTERNAL",
    }


def test_spec_rejects_a_synchronous_pilot():
    # A `def land` instead of `async def land` would run the side effect, then fail on the
    # await, hold, and repeat the side effect every round. Reject it at registration.
    from traction.spi import AirportSpec

    class SyncPilot:
        def land(self, flight):
            return None

    with pytest.raises(ValueError):
        AirportSpec(**spec_kwargs(pilot=SyncPilot())).validate()
