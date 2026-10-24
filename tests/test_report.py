"""The report emitter: wire rows to the tower queue, clamped, confirmed."""

import json
from uuid import UUID

import pytest

from tests.fakes import FakePublisher

FLIGHT_JSON = {
    "flightId": "1f0e8a9c-9b1d-4b7e-8a30-2f6f3f3b9b21",
    "airportId": "crm:py",
    "gateId": "contact-sync",
    "domainRef": "contact-1",
    "charterRef": "crm:py/contact-sync/run-1",
    "attemptCount": 1,
    "holdCount": 0,
    "correlationId": "a1b2c3",
    "payloadType": "demo.Contact",
    "payload": {"id": "contact-1"},
}


def flight():
    from traction.models import Flight

    return Flight.model_validate(FLIGHT_JSON)


def emitter(publisher):
    from traction.report import FlightReportEmitter

    return FlightReportEmitter(publisher)


async def test_landed_record_row():
    from traction.models import Disposition
    from traction.spi import Landed

    publisher = FakePublisher()
    await emitter(publisher).emit_record(flight(), Landed(), Disposition.TERMINAL)
    queue, body, type_id, expiration = publisher.published[0]
    assert queue == "tr.tower.queue"
    assert type_id == "dev.traction.core.report.FlightReportMessage"
    assert expiration is None
    assert body["recordType"] == "RECORD"
    assert body["outcome"] == "LANDED"
    assert body["disposition"] == "TERMINAL"
    assert body["category"] is None and body["reason"] is None
    assert body["evidenceCode"] is None
    assert body["attemptCount"] == 1 and body["holdCount"] == 0
    assert body["flightId"] == FLIGHT_JSON["flightId"]
    assert body["charterRef"] == FLIGHT_JSON["charterRef"]
    assert UUID(body["reportId"])  # fresh per message
    assert body["emittedAtEpochMillis"] > 1_700_000_000_000
    assert json.dumps(body)  # wire-serializable


async def test_skipped_row_carries_its_reason():
    from traction.models import Disposition
    from traction.spi import Skipped

    publisher = FakePublisher()
    await emitter(publisher).emit_record(flight(), Skipped("CHARTER_ABORTED"), Disposition.TERMINAL)
    body = publisher.published[0][1]
    assert body["outcome"] == "SKIPPED"
    assert body["reason"] == "CHARTER_ABORTED"


async def test_incident_record_row_carries_category_and_evidence():
    from traction.models import Disposition
    from traction.spi import Evidence, Incident, IncidentCategory

    publisher = FakePublisher()
    report = Incident(IncidentCategory.GATE_FAULT, Evidence("422", "contract", "unknown field"))
    await emitter(publisher).emit_record(flight(), report, Disposition.PARKED)
    body = publisher.published[0][1]
    assert body["outcome"] == "INCIDENT"
    assert body["category"] == "GATE_FAULT"
    assert body["disposition"] == "PARKED"
    assert (body["evidenceCode"], body["evidenceMessageKey"], body["evidenceExcerpt"]) == (
        "422", "contract", "unknown field",
    )


async def test_interim_row():
    from traction.spi import Evidence, Incident, IncidentCategory

    publisher = FakePublisher()
    incident = Incident(IncidentCategory.TRANSIENT, Evidence("503", None, None))
    await emitter(publisher).emit_interim(flight(), incident, reason="HELD_2")
    body = publisher.published[0][1]
    assert body["recordType"] == "INTERIM"
    assert body["disposition"] == "RETRYING"
    assert body["reason"] == "HELD_2"
    assert body["outcome"] == "INCIDENT"


async def test_evidence_is_clamped_on_the_wire():
    from traction.models import Disposition
    from traction.spi import Evidence, Incident, IncidentCategory

    publisher = FakePublisher()
    report = Incident(IncidentCategory.TRANSIENT, Evidence("c" * 999, "k" * 999, "e" * 2000))
    await emitter(publisher).emit_record(flight(), report, Disposition.PARKED)
    body = publisher.published[0][1]
    assert len(body["evidenceCode"]) == 64
    assert len(body["evidenceMessageKey"]) == 128
    assert len(body["evidenceExcerpt"]) == 1000


async def test_publish_failure_propagates():
    from traction._amqp import PublishException
    from traction.models import Disposition
    from traction.spi import Landed

    publisher = FakePublisher()
    publisher.fail_next = 1
    with pytest.raises(PublishException):
        await emitter(publisher).emit_record(flight(), Landed(), Disposition.TERMINAL)
