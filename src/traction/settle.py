"""The disposition matrix: how a classified flight settles, and the row it writes. Pure (no
I/O); the FlightConsumer carries it out. Port of core/consume/SettleDecision.java:

    LANDED / SKIPPED     ACK             RECORD  TERMINAL
    FLIGHT_FAULT         ACK             RECORD  TERMINAL   (rejected, no retry)
    GATE_FAULT           PARK            RECORD  PARKED
    AIRPORT_FAULT        DEFER_AND_PAUSE INTERIM RETRYING DEFERRED
    RATE_LIMIT           DELAY_AND_DEFER INTERIM RETRYING DEFERRED
    TRANSIENT, INTERNAL  HOLD_OR_PARK    the holding pattern decides (after_holding)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from traction.models import Disposition, RecordType
from traction.spi import FlightReport, Incident, IncidentCategory

REASON_DEFERRED = "DEFERRED"
REASON_HELD_PREFIX = "HELD_"


class Route(Enum):
    ACK = "ACK"
    PARK = "PARK"
    HOLD_OR_PARK = "HOLD_OR_PARK"
    DEFER_AND_PAUSE = "DEFER_AND_PAUSE"
    DELAY_AND_DEFER = "DELAY_AND_DEFER"


class HoldOutcome(Enum):
    HELD = "HELD"
    PARKED = "PARKED"


@dataclass(frozen=True)
class SettleDecision:
    route: Route
    record_type: RecordType | None
    disposition: Disposition | None
    reason: str | None

    @staticmethod
    def of(report: FlightReport) -> SettleDecision:
        if not isinstance(report, Incident):
            return _final_record()
        category = report.category
        if category is IncidentCategory.FLIGHT_FAULT:
            return _final_record()
        if category is IncidentCategory.GATE_FAULT:
            return _parked()
        if category is IncidentCategory.AIRPORT_FAULT:
            return _deferred(Route.DEFER_AND_PAUSE)
        if category is IncidentCategory.RATE_LIMIT:
            return _deferred(Route.DELAY_AND_DEFER)
        return SettleDecision(Route.HOLD_OR_PARK, None, None, None)

    @staticmethod
    def after_holding(outcome: HoldOutcome, held_count: int) -> SettleDecision:
        """The row for a holding-pattern outcome; ``held_count`` is the flight's hold count
        AFTER the hold."""
        if outcome is HoldOutcome.PARKED:
            return _parked()
        return SettleDecision(Route.HOLD_OR_PARK, RecordType.INTERIM, Disposition.RETRYING,
                              f"{REASON_HELD_PREFIX}{held_count}")


def _final_record() -> SettleDecision:
    return SettleDecision(Route.ACK, RecordType.RECORD, Disposition.TERMINAL, None)


def _parked() -> SettleDecision:
    return SettleDecision(Route.PARK, RecordType.RECORD, Disposition.PARKED, None)


def _deferred(route: Route) -> SettleDecision:
    return SettleDecision(route, RecordType.INTERIM, Disposition.RETRYING, REASON_DEFERRED)
