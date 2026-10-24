"""The demo tenant's vendor-error mapping and airport spec."""

import pytest


def test_error_for_maps_statuses():
    from demo.app.acme import (
        AuthError,
        BadPayloadError,
        ContractError,
        RateLimitedError,
        VendorUnavailableError,
        error_for,
    )

    assert isinstance(error_for(401, "token expired"), AuthError)
    assert isinstance(error_for(422, "unknown field"), ContractError)
    assert isinstance(error_for(400, "bad payload"), BadPayloadError)
    assert isinstance(error_for(429, "rate limited"), RateLimitedError)
    assert isinstance(error_for(500, "boom"), VendorUnavailableError)
    assert isinstance(error_for(503, "down"), VendorUnavailableError)
    with pytest.raises(RuntimeError):
        raise error_for(418, "teapot")


def test_report_maps_errors_to_the_level_they_break():
    from demo.app.acme import (
        AuthError,
        BadPayloadError,
        ContractError,
        RateLimitedError,
        VendorUnavailableError,
    )
    from demo.app.airport import report
    from traction import Incident, IncidentCategory, Landed

    assert report(None, None) == Landed()

    cases = [
        (AuthError(401, "token expired"), IncidentCategory.AIRPORT_FAULT),
        (ContractError(422, "unknown field"), IncidentCategory.GATE_FAULT),
        (BadPayloadError(400, "bad payload"), IncidentCategory.FLIGHT_FAULT),
        (VendorUnavailableError(0, "timeout"), IncidentCategory.TRANSIENT),
        (RateLimitedError(429, "slow down"), IncidentCategory.RATE_LIMIT),
        (RuntimeError("our own bug"), IncidentCategory.INTERNAL),
    ]
    for error, category in cases:
        outcome = report(None, error)
        assert isinstance(outcome, Incident)
        assert outcome.category is category, error
        assert outcome.evidence.code == type(error).__name__


async def test_crm_py_spec_routes_gates_to_vendor_paths():
    from demo.app.airport import AIRPORT, COMPANY_SYNC, CONTACT_SYNC, Company, Contact, crm_py_spec

    calls = []

    class FakeAcme:
        async def upsert(self, path, record):
            calls.append((path, record))

    spec = crm_py_spec(FakeAcme())
    spec.validate()
    assert spec.airport_id == AIRPORT == "crm:py"
    assert set(spec.gates) == {CONTACT_SYNC, COMPANY_SYNC}
    assert spec.slots.limit == 20 and spec.slots.window_s == 1.0
    assert spec.payload_types == {"demo.Contact": Contact, "demo.Company": Company}

    from traction import Flight

    contact_flight = Flight(
        airportId=AIRPORT, gateId=CONTACT_SYNC,
        payload=Contact(id="c1", email="c1@example.com", bad=False),
    )
    await spec.pilot.land(contact_flight)
    company_flight = Flight(
        airportId=AIRPORT, gateId=COMPANY_SYNC,
        payload=Company(id="k1", domain="k1.example.com", bad=False),
    )
    await spec.pilot.land(company_flight)
    assert calls == [
        ("/contacts", {"id": "c1", "email": "c1@example.com", "bad": False}),
        ("/companies", {"id": "k1", "domain": "k1.example.com", "bad": False}),
    ]
