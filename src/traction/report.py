"""Publishes each flight outcome as a FlightReportMessage to the tower queue, confirmed. A
failed publish THROWS and the delivery is not acked — the consumer's catch-all parks the flight
instead of losing the row."""

from __future__ import annotations

import time
from uuid import uuid4

from traction._amqp import TYPE_ID_REPORT, ConfirmedPublisher
from traction.models import Disposition, Flight, FlightReportMessage, RecordType
from traction.spi import FlightReport, Incident, Landed, Skipped
from traction.topology import TOWER_QUEUE


class FlightReportEmitter:
    def __init__(self, publisher: ConfirmedPublisher):
        self._publisher = publisher

    async def emit_record(self, flight: Flight, report: FlightReport, disposition: Disposition,
                          reason: str | None = None) -> None:
        """A settled outcome. A SKIPPED report's reason rides along as the row's reason."""
        if reason is None and isinstance(report, Skipped):
            reason = report.reason
        await self._emit(RecordType.RECORD, flight, report, disposition, reason)

    async def emit_interim(self, flight: Flight, incident: Incident, reason: str) -> None:
        """A step along the way: ``HELD_<n>`` (holding round) or ``DEFERRED`` (runway tail)."""
        await self._emit(RecordType.INTERIM, flight, incident, Disposition.RETRYING, reason)

    async def _emit(self, record_type: RecordType, flight: Flight, report: FlightReport,
                    disposition: Disposition, reason: str | None) -> None:
        if isinstance(report, Landed):
            outcome, category, evidence = "LANDED", None, None
        elif isinstance(report, Skipped):
            outcome, category, evidence = "SKIPPED", None, None
        else:
            outcome, category, evidence = "INCIDENT", report.category.value, report.evidence
        message = FlightReportMessage(
            recordType=record_type.value,
            flightId=flight.flight_id,
            airportId=flight.airport_id,
            gateId=flight.gate_id,
            domainRef=flight.domain_ref,
            charterRef=flight.charter_ref,
            outcome=outcome,
            category=category,
            reason=reason,
            evidenceCode=evidence.code if evidence else None,
            evidenceMessageKey=evidence.message_key if evidence else None,
            evidenceExcerpt=evidence.excerpt if evidence else None,
            attemptCount=flight.attempt_count,
            holdCount=flight.hold_count,
            correlationId=flight.correlation_id,
            reportId=uuid4(),
            emittedAtEpochMillis=int(time.time() * 1000),
            disposition=disposition.value,
        ).with_clamped_evidence()
        await self._publisher.publish(TOWER_QUEUE, message.to_wire(), TYPE_ID_REPORT)
