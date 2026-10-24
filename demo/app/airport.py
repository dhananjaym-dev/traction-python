"""The Acme CRM as one airport: one shared rate limit, one credential set, one outage fate —
the Python twin of the Java demo's CRM airport, under its own id."""

from __future__ import annotations

from pydantic import BaseModel

from demo.app.acme import (
    COMPANIES,
    CONTACTS,
    AcmeClient,
    AuthError,
    BadPayloadError,
    ContractError,
    RateLimitedError,
    VendorUnavailableError,
)
from traction import (
    AirportSpec,
    Evidence,
    Flight,
    FlightReport,
    Incident,
    IncidentCategory,
    Landed,
    SlotsConfig,
)

AIRPORT = "crm:py"
CONTACT_SYNC = "contact-sync"
COMPANY_SYNC = "company-sync"

PATHS = {CONTACT_SYNC: CONTACTS, COMPANY_SYNC: COMPANIES}


class Contact(BaseModel):
    id: str
    email: str
    bad: bool = False


class Company(BaseModel):
    id: str
    domain: str
    bad: bool = False


def report(flight: Flight | None, error: BaseException | None) -> FlightReport:
    """Classifies every outcome to the level it breaks."""
    if error is None:
        return Landed()
    evidence = Evidence.from_exception(error)
    if isinstance(error, AuthError):  # dead credentials → the whole airport ground-stops
        return Incident(IncidentCategory.AIRPORT_FAULT, evidence)
    if isinstance(error, ContractError):  # this work type is rejected → a streak stops the gate
        return Incident(IncidentCategory.GATE_FAULT, evidence)
    if isinstance(error, BadPayloadError):  # this record's data → rejected, re-drive from source
        return Incident(IncidentCategory.FLIGHT_FAULT, evidence)
    if isinstance(error, RateLimitedError):  # throttled → pace the airport down, never a failure
        return Incident(IncidentCategory.RATE_LIMIT, evidence)
    if isinstance(error, VendorUnavailableError):  # 5xx or timeout → hold with backoff, then park
        return Incident(IncidentCategory.TRANSIENT, evidence)
    return Incident(IncidentCategory.INTERNAL, evidence)  # our own bug → hold briefly, then park


class _Pilot:
    def __init__(self, acme: AcmeClient):
        self._acme = acme

    async def land(self, flight: Flight) -> None:
        """Lands one flight: one Acme call, routed by gate. Never touches the broker."""
        await self._acme.upsert(PATHS[flight.gate_id], flight.payload.model_dump(mode="json"))


class _Reporter:
    def report(self, flight: Flight, error: BaseException | None) -> FlightReport:
        return report(flight, error)


def crm_py_spec(acme: AcmeClient) -> AirportSpec:
    return AirportSpec(
        airport_id=AIRPORT,
        display_name="Acme CRM (Python)",
        slots=SlotsConfig.of(20, 1.0),  # the shared rate limit
        gates={CONTACT_SYNC: 1, COMPANY_SYNC: 1},
        pilot=_Pilot(acme),
        reporter=_Reporter(),
        payload_types={"demo.Contact": Contact, "demo.Company": Company},
    )
