"""Wire models — the JSON here is handwritten from the Java records and is the contract."""

import json
from uuid import UUID

# One Flight exactly as the Java Tower dispatches it (Jackson: record component names,
# explicit nulls, UUID as string). From core/api/Flight.java.
JAVA_FLIGHT_JSON = json.dumps(
    {
        "flightId": "1f0e8a9c-9b1d-4b7e-8a30-2f6f3f3b9b21",
        "airportId": "crm:acme",
        "gateId": "contact-sync",
        "domainRef": "contact-1",
        "charterRef": None,
        "attemptCount": 0,
        "holdCount": 0,
        "correlationId": "a1b2c3",
        "payloadType": "dev.traction.demo.AcmeContact",
        "payload": {"id": "contact-1", "email": "contact-1@example.com", "bad": False},
    }
)


def test_flight_round_trip_wire_json():
    from traction.models import Flight

    flight = Flight.model_validate_json(JAVA_FLIGHT_JSON)
    assert flight.flight_id == UUID("1f0e8a9c-9b1d-4b7e-8a30-2f6f3f3b9b21")
    assert flight.airport_id == "crm:acme"
    assert flight.charter_ref is None
    assert flight.payload == {"id": "contact-1", "email": "contact-1@example.com", "bad": False}

    wire = flight.to_wire()
    assert wire == json.loads(JAVA_FLIGHT_JSON)  # same keys, same values, nulls kept


def test_flight_ignores_unknown_fields():
    from traction.models import Flight

    raw = json.loads(JAVA_FLIGHT_JSON)
    raw["someFutureField"] = "x"
    flight = Flight.model_validate(raw)
    assert "someFutureField" not in flight.to_wire()


def test_increment_helpers():
    from traction.models import Flight

    flight = Flight.model_validate_json(JAVA_FLIGHT_JSON)
    attempted = flight.with_incremented_attempt()
    assert (attempted.attempt_count, attempted.hold_count) == (1, 0)
    held = attempted.with_incremented_hold()
    assert (held.attempt_count, held.hold_count) == (1, 1)
    assert flight.attempt_count == 0  # copies, never mutation


def test_report_wire_shape():
    from traction.models import FlightReportMessage

    row = FlightReportMessage(
        recordType="RECORD",
        flightId=UUID("1f0e8a9c-9b1d-4b7e-8a30-2f6f3f3b9b21"),
        airportId="crm:py",
        gateId="contact-sync",
        domainRef=None,
        charterRef=None,
        outcome="LANDED",
        category=None,
        reason=None,
        evidenceCode=None,
        evidenceMessageKey=None,
        evidenceExcerpt=None,
        attemptCount=1,
        holdCount=0,
        correlationId="a1b2c3",
        reportId=UUID("2f0e8a9c-9b1d-4b7e-8a30-2f6f3f3b9b22"),
        emittedAtEpochMillis=1_700_000_000_000,
        disposition="TERMINAL",
    )
    wire = row.to_wire()
    # The 18 fields of core/report/FlightReportMessage.java, camelCase, nulls explicit.
    assert set(wire) == {
        "recordType", "flightId", "airportId", "gateId", "domainRef", "charterRef",
        "outcome", "category", "reason", "evidenceCode", "evidenceMessageKey",
        "evidenceExcerpt", "attemptCount", "holdCount", "correlationId", "reportId",
        "emittedAtEpochMillis", "disposition",
    }
    assert wire["category"] is None
    assert wire["flightId"] == "1f0e8a9c-9b1d-4b7e-8a30-2f6f3f3b9b21"
    assert wire["disposition"] == "TERMINAL"


def test_evidence_clamp():
    from traction.models import FlightReportMessage

    row = FlightReportMessage(
        recordType="RECORD",
        flightId=UUID(int=1),
        airportId="a",
        gateId=None,
        domainRef=None,
        charterRef=None,
        outcome="INCIDENT",
        category="TRANSIENT",
        reason=None,
        evidenceCode="c" * 65,
        evidenceMessageKey="k" * 129,
        evidenceExcerpt="e" * 1001,
        attemptCount=1,
        holdCount=0,
        correlationId=None,
        reportId=UUID(int=2),
        emittedAtEpochMillis=0,
        disposition="PARKED",
    )
    clamped = row.with_clamped_evidence()
    assert len(clamped.evidence_code) == 64
    assert len(clamped.evidence_message_key) == 128
    assert len(clamped.evidence_excerpt) == 1000
    # identity when already compliant
    assert clamped.with_clamped_evidence() == clamped


def test_announcement_wire_shape():
    from traction.models import AirportAnnouncement, GateAnnouncement

    announcement = AirportAnnouncement(
        announcementId=UUID(int=3),
        airportId="crm:py",
        owningProcess="py-tenant",
        displayName="Acme CRM (Python)",
        slotsLimit=20,
        slotsWindowSeconds=1,
        slotsBurst=20,
        gates=[GateAnnouncement(gateId="contact-sync", slotCost=2)],
        epochMillis=1_700_000_000_000,
    )
    wire = announcement.to_wire()
    assert set(wire) == {
        "announcementId", "airportId", "owningProcess", "displayName",
        "slotsLimit", "slotsWindowSeconds", "slotsBurst", "gates", "epochMillis",
    }
    assert wire["gates"] == [{"gateId": "contact-sync", "slotCost": 2}]


def test_disposition_and_record_type_values():
    from traction.models import Disposition, RecordType

    assert [d.value for d in Disposition] == ["RETRYING", "PARKED", "TERMINAL"]
    assert [r.value for r in RecordType] == ["RECORD", "INTERIM"]


def test_reserved_reasons():
    from traction.models import FlightReportMessage

    assert FlightReportMessage.REASON_GATE_GROUNDED == "GATE_GROUNDED"
    assert FlightReportMessage.REASON_CHARTER_ABORTED == "CHARTER_ABORTED"
