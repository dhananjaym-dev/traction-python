"""The settle matrix: pure port of core/consume/SettleDecision.java."""

from traction.models import Disposition, RecordType
from traction.settle import REASON_DEFERRED, HoldOutcome, Route, SettleDecision
from traction.spi import Evidence, Incident, IncidentCategory, Landed, Skipped


def incident(category):
    return Incident(category=category, evidence=Evidence("c", None, None))


def test_landed_and_skipped_and_flight_fault_are_final_records():
    for report in (Landed(), Skipped("X"), incident(IncidentCategory.FLIGHT_FAULT)):
        decision = SettleDecision.of(report)
        assert (decision.route, decision.record_type, decision.disposition, decision.reason) == (
            Route.ACK, RecordType.RECORD, Disposition.TERMINAL, None,
        )


def test_gate_fault_parks():
    decision = SettleDecision.of(incident(IncidentCategory.GATE_FAULT))
    assert (decision.route, decision.record_type, decision.disposition, decision.reason) == (
        Route.PARK, RecordType.RECORD, Disposition.PARKED, None,
    )


def test_airport_fault_defers_and_pauses():
    decision = SettleDecision.of(incident(IncidentCategory.AIRPORT_FAULT))
    assert (decision.route, decision.record_type, decision.disposition, decision.reason) == (
        Route.DEFER_AND_PAUSE, RecordType.INTERIM, Disposition.RETRYING, REASON_DEFERRED,
    )


def test_rate_limit_delays_and_defers():
    decision = SettleDecision.of(incident(IncidentCategory.RATE_LIMIT))
    assert (decision.route, decision.record_type, decision.disposition, decision.reason) == (
        Route.DELAY_AND_DEFER, RecordType.INTERIM, Disposition.RETRYING, REASON_DEFERRED,
    )


def test_transient_and_internal_go_to_the_holding_pattern():
    for category in (IncidentCategory.TRANSIENT, IncidentCategory.INTERNAL):
        decision = SettleDecision.of(incident(category))
        assert (decision.route, decision.record_type, decision.disposition, decision.reason) == (
            Route.HOLD_OR_PARK, None, None, None,
        )


def test_after_holding_held_is_an_interim_row_with_post_increment_count():
    decision = SettleDecision.after_holding(HoldOutcome.HELD, held_count=1)
    assert (decision.route, decision.record_type, decision.disposition, decision.reason) == (
        Route.HOLD_OR_PARK, RecordType.INTERIM, Disposition.RETRYING, "HELD_1",
    )


def test_after_holding_parked_is_a_parked_record():
    decision = SettleDecision.after_holding(HoldOutcome.PARKED, held_count=3)
    assert (decision.route, decision.record_type, decision.disposition, decision.reason) == (
        Route.PARK, RecordType.RECORD, Disposition.PARKED, None,
    )
