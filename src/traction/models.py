"""The wire models, mirroring the Java records field for field. Aliases are the wire names;
``to_wire()`` emits by alias with explicit nulls, matching Jackson's record output, so the JSON
is identical whichever language wrote it."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, ClassVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _WireModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    def to_wire(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, mode="json")


class Flight(_WireModel):
    """The envelope around one unit of work (core/api/Flight.java). Fields are nullable on the
    wire; what makes an envelope flyable is the consumer's validation, not the model."""

    flight_id: UUID | None = Field(default=None, alias="flightId")
    airport_id: str | None = Field(default=None, alias="airportId")
    gate_id: str | None = Field(default=None, alias="gateId")
    domain_ref: str | None = Field(default=None, alias="domainRef")
    charter_ref: str | None = Field(default=None, alias="charterRef")
    attempt_count: int = Field(default=0, alias="attemptCount")
    hold_count: int = Field(default=0, alias="holdCount")
    correlation_id: str | None = Field(default=None, alias="correlationId")
    payload_type: str | None = Field(default=None, alias="payloadType")
    payload: Any = None

    def with_incremented_attempt(self) -> Flight:
        return self.model_copy(update={"attempt_count": self.attempt_count + 1})

    def with_incremented_hold(self) -> Flight:
        return self.model_copy(update={"hold_count": self.hold_count + 1})


class Disposition(StrEnum):
    RETRYING = "RETRYING"
    PARKED = "PARKED"
    TERMINAL = "TERMINAL"


class RecordType(StrEnum):
    RECORD = "RECORD"
    INTERIM = "INTERIM"


class FlightReportMessage(_WireModel):
    """One flight outcome on the tower queue (core/report/FlightReportMessage.java)."""

    REASON_GATE_GROUNDED: ClassVar[str] = "GATE_GROUNDED"
    REASON_CHARTER_ABORTED: ClassVar[str] = "CHARTER_ABORTED"

    EVIDENCE_MAX_CODE_LENGTH: ClassVar[int] = 64
    EVIDENCE_MAX_MESSAGE_KEY_LENGTH: ClassVar[int] = 128
    EVIDENCE_MAX_EXCERPT_LENGTH: ClassVar[int] = 1000

    record_type: str = Field(alias="recordType")
    flight_id: UUID | None = Field(default=None, alias="flightId")
    airport_id: str | None = Field(default=None, alias="airportId")
    gate_id: str | None = Field(default=None, alias="gateId")
    domain_ref: str | None = Field(default=None, alias="domainRef")
    charter_ref: str | None = Field(default=None, alias="charterRef")
    outcome: str
    category: str | None = None
    reason: str | None = None
    evidence_code: str | None = Field(default=None, alias="evidenceCode")
    evidence_message_key: str | None = Field(default=None, alias="evidenceMessageKey")
    evidence_excerpt: str | None = Field(default=None, alias="evidenceExcerpt")
    attempt_count: int = Field(default=0, alias="attemptCount")
    hold_count: int = Field(default=0, alias="holdCount")
    correlation_id: str | None = Field(default=None, alias="correlationId")
    report_id: UUID = Field(alias="reportId")
    emitted_at_epoch_millis: int = Field(alias="emittedAtEpochMillis")
    disposition: str | None = None

    def with_clamped_evidence(self) -> FlightReportMessage:
        code = _truncate(self.evidence_code, self.EVIDENCE_MAX_CODE_LENGTH)
        key = _truncate(self.evidence_message_key, self.EVIDENCE_MAX_MESSAGE_KEY_LENGTH)
        excerpt = _truncate(self.evidence_excerpt, self.EVIDENCE_MAX_EXCERPT_LENGTH)
        if (code, key, excerpt) == (self.evidence_code, self.evidence_message_key,
                                    self.evidence_excerpt):
            return self
        return self.model_copy(update={
            "evidence_code": code, "evidence_message_key": key, "evidence_excerpt": excerpt,
        })


def _truncate(value: str | None, limit: int) -> str | None:
    return value if value is None or len(value) <= limit else value[:limit]


class GateAnnouncement(_WireModel):
    gate_id: str = Field(alias="gateId")
    slot_cost: int = Field(alias="slotCost")


class AirportAnnouncement(_WireModel):
    """An airport's self-description on the tower queue (core/registry/AirportAnnouncement.java)."""

    announcement_id: UUID = Field(alias="announcementId")
    airport_id: str = Field(alias="airportId")
    owning_process: str = Field(alias="owningProcess")
    display_name: str = Field(alias="displayName")
    slots_limit: int | None = Field(default=None, alias="slotsLimit")
    slots_window_seconds: int | None = Field(default=None, alias="slotsWindowSeconds")
    slots_burst: int | None = Field(default=None, alias="slotsBurst")
    gates: list[GateAnnouncement] = Field(default_factory=list)
    epoch_millis: int = Field(alias="epochMillis")
