"""FlightConsumer: the per-flight loop, ported step for step from core/consume/FlightConsumer.java.

The harness builds a consumer over fakes for every seam and records what settled where. Sleeps
are injected so backoffs and delay slices assert on requested durations instead of wall time.
"""

import json
from uuid import uuid4

from pydantic import BaseModel

from tests.fakes import FakeChannel, FakeIncomingMessage, FakePublisher
from traction._amqp import PublishException, TYPE_ID_FLIGHT
from traction.clearance import ClearanceResult, ClearanceUnavailableError
from traction.models import Disposition, Flight, RecordType
from traction.spi import (
    AirportSpec,
    Evidence,
    HoldPolicy,
    Incident,
    IncidentCategory,
    Landed,
    SlotsConfig,
)
from traction.status import AirportStatus


class Contact(BaseModel):
    id: str
    email: str = "x@example.com"


class FakeStatusReader:
    def __init__(self):
        self.statuses: dict[str, AirportStatus] = {}

    async def get(self, scope):
        return self.statuses.get(scope, AirportStatus.OPEN)


class FakeClearance:
    def __init__(self):
        self.results: list = [ClearanceResult(granted=True, wait_ms=0)]
        self.calls: list[tuple[str, SlotsConfig, int]] = []

    async def acquire(self, airport_id, slots, cost):
        self.calls.append((airport_id, slots, cost))
        result = self.results.pop(0) if len(self.results) > 1 else self.results[0]
        if isinstance(result, Exception):
            raise result
        return result


class FakeGroundDelay:
    def __init__(self):
        self.remaining_values: list[float | None] = [None]
        self.delays: list[float] = []

    async def remaining(self, airport_id):
        return (
            self.remaining_values.pop(0)
            if len(self.remaining_values) > 1
            else self.remaining_values[0]
        )

    async def delay(self, airport_id, cooldown_s):
        self.delays.append(cooldown_s)


class FakeCharters:
    def __init__(self):
        self.aborted: set[str] = set()

    async def is_aborted(self, ref):
        return ref in self.aborted


class FakeEmitter:
    def __init__(self):
        self.records: list[tuple[Flight, object, Disposition, str | None]] = []
        self.interims: list[tuple[Flight, Incident, str]] = []
        self.fail_next = 0

    def _maybe_fail(self):
        if self.fail_next > 0:
            self.fail_next -= 1
            raise PublishException("tower publish failed")

    async def emit_record(self, flight, report, disposition, reason=None):
        self._maybe_fail()
        self.records.append((flight, report, disposition, reason))

    async def emit_interim(self, flight, incident, reason):
        self._maybe_fail()
        self.interims.append((flight, incident, reason))


class Harness:
    def __init__(self, pilot_error=None, reporter=None, hold_policy=None, gates=None,
                 pauser_error=None):
        self.landed: list[Flight] = []
        self.pilot_error = pilot_error
        self.status = FakeStatusReader()
        self.clearance = FakeClearance()
        self.ground_delay = FakeGroundDelay()
        self.charters = FakeCharters()
        self.emitter = FakeEmitter()
        self.publisher = FakePublisher()
        self.channel = FakeChannel()
        self.pauses = 0
        self.sleeps: list[float] = []

        harness = self

        class Pilot:
            async def land(self, flight):
                harness.landed.append(flight)
                if harness.pilot_error is not None:
                    raise harness.pilot_error

        class Reporter:
            def report(self, flight, error):
                if reporter is not None:
                    return reporter(flight, error)
                if error is None:
                    return Landed()
                return Incident(IncidentCategory.TRANSIENT, Evidence.from_exception(error))

        self.spec = AirportSpec(
            airport_id="crm:py",
            display_name="x",
            slots=SlotsConfig.of(20, 1.0),
            gates=gates or {"contact-sync": 1, "heavy": 5},
            pilot=Pilot(),
            reporter=Reporter(),
            payload_types={"demo.Contact": Contact},
            hold_policy=hold_policy,
        )

        from traction.consumer import FlightConsumer

        async def pause():
            self.pauses += 1
            if pauser_error is not None:
                raise pauser_error

        async def sleep(seconds):
            self.sleeps.append(seconds)

        self.consumer = FlightConsumer(
            spec=self.spec,
            channel=self.channel,
            status_reader=self.status,
            emitter=self.emitter,
            publisher=self.publisher,
            pauser=pause,
            clearance=self.clearance,
            ground_delay=self.ground_delay,
            charters=self.charters,
            sleep=sleep,
        )

    def flight_message(self, **overrides):
        body = {
            "flightId": str(uuid4()),
            "airportId": "crm:py",
            "gateId": "contact-sync",
            "domainRef": "contact-1",
            "charterRef": None,
            "attemptCount": 0,
            "holdCount": 0,
            "correlationId": "a1b2c3",
            "payloadType": "demo.Contact",
            "payload": {"id": "contact-1"},
        }
        body.update(overrides)
        return FakeIncomingMessage(body, headers={"__TypeId__": TYPE_ID_FLIGHT})

    def confirmed_to(self, queue):
        return [p for p in self.publisher.published if p[0] == queue]

    def raw_published_to(self, queue):
        return [m for m in self.channel.default_exchange.published if m.routing_key == queue]


# ---------------------------------------------------------------- park raw

async def test_undeserializable_body_parks_raw():
    h = Harness()
    message = FakeIncomingMessage(b"not json {{{", headers={"k": "v"})
    await h.consumer.on_message(message)
    parked = h.raw_published_to("tr.crm:py.parked")
    assert len(parked) == 1
    assert parked[0].body == b"not json {{{"
    assert parked[0].headers == {"k": "v"}
    assert message.acks == 1 and message.nacks == []
    assert h.emitter.records == [] and h.emitter.interims == []


async def test_bad_envelopes_park_raw():
    h = Harness()
    for overrides in (
        {"flightId": None},
        {"airportId": "someone-else"},
        {"gateId": None},
        {"gateId": "  "},
        {"payloadType": None},
        {"payloadType": ""},
    ):
        message = h.flight_message(**overrides)
        await h.consumer.on_message(message)
        assert message.acks == 1, overrides
    assert len(h.raw_published_to("tr.crm:py.parked")) == 6
    assert h.emitter.records == []
    assert h.landed == []


async def test_raw_park_failure_nacks_requeue():
    h = Harness()
    h.channel.default_exchange.fail_next = 1
    message = FakeIncomingMessage(b"not json")
    await h.consumer.on_message(message)
    assert message.acks == 0 and message.nacks == [True]


# ---------------------------------------------------------------- status / ground delay / gate

async def test_closed_airport_defers_and_pauses():
    h = Harness()
    h.status.statuses["crm:py"] = AirportStatus.GROUND_STOP
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert h.sleeps[0] == 0.5  # bounded republish spin
    deferred = h.confirmed_to("tr.crm:py.runway")
    assert len(deferred) == 1
    assert deferred[0][1]["attemptCount"] == 0  # untouched envelope
    assert message.acks == 1
    assert h.pauses == 1
    assert h.emitter.records == [] and h.emitter.interims == []


async def test_defer_publish_failure_falls_back_to_nack():
    h = Harness()
    h.status.statuses["crm:py"] = AirportStatus.CLOSED
    h.publisher.fail_next = 1
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert message.acks == 0 and message.nacks == [True]


async def test_ground_delay_sleeps_in_slices_then_proceeds():
    h = Harness()
    h.ground_delay.remaining_values = [700.0, 100.0, None]
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert h.sleeps[:2] == [600.0, 100.0]  # slice cap, then the remainder
    assert h.landed  # proceeded once the marker cleared


async def test_ground_delay_budget_exhausted_defers():
    h = Harness()
    h.ground_delay.remaining_values = [10_000.0]
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert sum(h.sleeps) <= 1500.0
    assert len(h.confirmed_to("tr.crm:py.runway")) == 1
    assert message.acks == 1
    assert h.pauses == 0


def test_delay_slice_bounds():
    from traction.consumer import delay_slice_s

    assert delay_slice_s(5.0, 0.0) == 5.0
    assert delay_slice_s(0.0, 0.0) > 0  # never a zero-length sleep loop
    assert delay_slice_s(10_000.0, 0.0) == 600.0
    assert delay_slice_s(10_000.0, 1400.0) == 100.0  # the unspent budget


async def test_grounded_gate_parks_with_skipped_record():
    from traction.spi import Skipped

    h = Harness()
    h.status.statuses["crm:py/contact-sync"] = AirportStatus.GROUND_STOP
    message = h.flight_message()
    await h.consumer.on_message(message)
    flight, report, disposition, reason = h.emitter.records[0]
    assert isinstance(report, Skipped) and report.reason == "GATE_GROUNDED"
    assert disposition is Disposition.PARKED
    assert len(h.confirmed_to("tr.crm:py.parked")) == 1
    assert message.acks == 1
    assert h.landed == []


# ---------------------------------------------------------------- charter abort

async def test_aborted_charter_skips_before_clearance():
    from traction.spi import Skipped

    h = Harness()
    h.charters.aborted.add("crm:py/contact-sync/run-1")
    message = h.flight_message(charterRef="crm:py/contact-sync/run-1")
    await h.consumer.on_message(message)
    flight, report, disposition, reason = h.emitter.records[0]
    assert isinstance(report, Skipped) and report.reason == "CHARTER_ABORTED"
    assert disposition is Disposition.TERMINAL
    assert h.clearance.calls == []  # burned no slots
    assert h.confirmed_to("tr.crm:py.parked") == []
    assert message.acks == 1


# ---------------------------------------------------------------- payload

async def test_unresolved_payload_type_parks():
    from traction.consumer import PAYLOAD_TYPE_UNRESOLVED

    h = Harness()
    message = h.flight_message(payloadType="demo.Renamed")
    await h.consumer.on_message(message)
    flight, report, disposition, reason = h.emitter.records[0]
    assert report.category is IncidentCategory.INTERNAL
    assert report.evidence.message_key == PAYLOAD_TYPE_UNRESOLVED
    assert report.evidence.excerpt == "demo.Renamed"
    assert disposition is Disposition.PARKED
    assert len(h.confirmed_to("tr.crm:py.parked")) == 1
    assert h.clearance.calls == []  # before clearance, like the Java step order
    assert message.acks == 1


async def test_unreadable_payload_rejects():
    h = Harness()
    message = h.flight_message(payload={"id": ["not", "a", "string"]})
    await h.consumer.on_message(message)
    flight, report, disposition, reason = h.emitter.records[0]
    assert report.category is IncidentCategory.FLIGHT_FAULT
    assert report.evidence.message_key == "payload-unreadable"
    assert disposition is Disposition.TERMINAL
    assert h.confirmed_to("tr.crm:py.parked") == []
    assert message.acks == 1
    assert h.landed == []


async def test_null_payload_lands():
    h = Harness()
    message = h.flight_message(payload=None)
    await h.consumer.on_message(message)
    assert len(h.landed) == 1
    assert h.landed[0].payload is None
    assert h.emitter.records[0][1] == Landed()


async def test_resolved_payload_reaches_the_pilot_as_the_model():
    h = Harness()
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert isinstance(h.landed[0].payload, Contact)
    assert h.landed[0].payload.id == "contact-1"
    assert h.landed[0].attempt_count == 1  # this attempt


# ---------------------------------------------------------------- clearance

async def test_clearance_short_wait_sleeps_and_retries():
    h = Harness()
    h.clearance.results = [
        ClearanceResult(granted=False, wait_ms=200),
        ClearanceResult(granted=True, wait_ms=0),
    ]
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert 0.2 in h.sleeps
    assert len(h.landed) == 1


async def test_clearance_long_wait_ground_delays_and_defers():
    h = Harness()
    h.clearance.results = [ClearanceResult(granted=False, wait_ms=60_000)]
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert h.ground_delay.delays == [60.0]  # the denial's own wait
    assert len(h.confirmed_to("tr.crm:py.runway")) == 1
    assert message.acks == 1
    assert h.pauses == 0
    assert h.landed == []


async def test_clearance_budget_exhausted_ground_delays_with_window():
    h = Harness()
    h.clearance.results = [ClearanceResult(granted=False, wait_ms=9_000)]
    message = h.flight_message()
    await h.consumer.on_message(message)
    # 9s sleeps accumulate past the 15s budget, then the airport is paced one slots window
    assert h.ground_delay.delays == [1.0]
    assert len(h.confirmed_to("tr.crm:py.runway")) == 1
    assert message.acks == 1


async def test_clearance_unavailable_fails_closed():
    h = Harness()
    h.clearance.results = [ClearanceUnavailableError("redis down")]
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert 0.5 in h.sleeps  # backoff before the deferral
    assert len(h.confirmed_to("tr.crm:py.runway")) == 1
    assert message.acks == 1
    assert h.landed == []


async def test_clearance_cost_is_the_gate_slot_cost():
    h = Harness()
    message = h.flight_message(gateId="heavy")
    await h.consumer.on_message(message)
    assert h.clearance.calls[0][2] == 5


async def test_unknown_gate_costs_one():
    h = Harness()
    message = h.flight_message(gateId="gone-gate")
    await h.consumer.on_message(message)
    assert h.clearance.calls[0][2] == 1
    assert len(h.landed) == 1


# ---------------------------------------------------------------- land, report, settle

async def test_landed_flight_acks_with_terminal_record():
    h = Harness()
    message = h.flight_message()
    await h.consumer.on_message(message)
    flight, report, disposition, reason = h.emitter.records[0]
    assert report == Landed()
    assert disposition is Disposition.TERMINAL
    assert flight.attempt_count == 1
    assert message.acks == 1


async def test_flight_fault_rejects_terminal():
    def reporter(flight, error):
        return Incident(IncidentCategory.FLIGHT_FAULT, Evidence.from_exception(error))

    h = Harness(pilot_error=ValueError("bad record"), reporter=reporter)
    message = h.flight_message()
    await h.consumer.on_message(message)
    flight, report, disposition, reason = h.emitter.records[0]
    assert disposition is Disposition.TERMINAL
    assert h.confirmed_to("tr.crm:py.parked") == []
    assert message.acks == 1


async def test_gate_fault_parks():
    def reporter(flight, error):
        return Incident(IncidentCategory.GATE_FAULT, Evidence.from_exception(error))

    h = Harness(pilot_error=ValueError("422"), reporter=reporter)
    message = h.flight_message()
    await h.consumer.on_message(message)
    flight, report, disposition, reason = h.emitter.records[0]
    assert disposition is Disposition.PARKED
    parked = h.confirmed_to("tr.crm:py.parked")
    assert len(parked) == 1 and parked[0][1]["attemptCount"] == 1
    assert message.acks == 1


async def test_airport_fault_defers_interim_and_pauses():
    def reporter(flight, error):
        return Incident(IncidentCategory.AIRPORT_FAULT, Evidence.from_exception(error))

    h = Harness(pilot_error=ValueError("401"), reporter=reporter)
    message = h.flight_message()
    await h.consumer.on_message(message)
    flight, incident, reason = h.emitter.interims[0]
    assert reason == "DEFERRED"
    assert len(h.confirmed_to("tr.crm:py.runway")) == 1
    assert message.acks == 1
    assert h.pauses == 1
    assert h.emitter.records == []


async def test_rate_limit_ground_delays_and_defers():
    def reporter(flight, error):
        return Incident(IncidentCategory.RATE_LIMIT, Evidence.from_exception(error))

    h = Harness(pilot_error=ValueError("429"), reporter=reporter)
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert h.emitter.interims[0][2] == "DEFERRED"
    assert h.ground_delay.delays == [1.0]  # the slots window
    assert len(h.confirmed_to("tr.crm:py.runway")) == 1
    assert message.acks == 1
    assert h.pauses == 0


async def test_transient_first_failure_holds_with_backoff():
    h = Harness(pilot_error=TimeoutError("slow vendor"))
    message = h.flight_message()
    await h.consumer.on_message(message)
    held = h.confirmed_to("tr.crm:py.holding")
    assert len(held) == 1
    queue, body, type_id, expiration_ms = held[0]
    assert body["holdCount"] == 1
    assert expiration_ms == 30_000  # test_hold_publish_expiration_ms: first TRANSIENT round, in ms
    assert type_id == TYPE_ID_FLIGHT
    assert h.emitter.interims[0][2] == "HELD_1"
    assert message.acks == 1
    assert h.emitter.records == []


async def test_hold_rounds_index_the_schedule():
    h = Harness(pilot_error=TimeoutError("slow vendor"))
    message = h.flight_message(holdCount=2)
    await h.consumer.on_message(message)
    queue, body, type_id, expiration_ms = h.confirmed_to("tr.crm:py.holding")[0]
    assert expiration_ms == 600_000  # third round
    assert body["holdCount"] == 3
    assert h.emitter.interims[0][2] == "HELD_3"


async def test_hold_budget_spent_parks():
    h = Harness(pilot_error=TimeoutError("slow vendor"))
    message = h.flight_message(holdCount=3)
    await h.consumer.on_message(message)
    assert h.confirmed_to("tr.crm:py.holding") == []
    assert len(h.confirmed_to("tr.crm:py.parked")) == 1
    flight, report, disposition, reason = h.emitter.records[0]
    assert disposition is Disposition.PARKED
    assert flight.hold_count == 3  # park never increments
    assert message.acks == 1


async def test_internal_uses_its_one_round_schedule():
    def reporter(flight, error):
        return Incident(IncidentCategory.INTERNAL, Evidence.from_exception(error))

    h = Harness(pilot_error=RuntimeError("our bug"), reporter=reporter)
    first = h.flight_message()
    await h.consumer.on_message(first)
    assert h.confirmed_to("tr.crm:py.holding")[0][3] == 30_000

    second = h.flight_message(holdCount=1)
    await h.consumer.on_message(second)
    assert len(h.confirmed_to("tr.crm:py.parked")) == 1


async def test_hold_policy_override_applies():
    h = Harness(pilot_error=TimeoutError("x"), hold_policy=HoldPolicy(schedule=(5,)))
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert h.confirmed_to("tr.crm:py.holding")[0][3] == 5_000
    done = h.flight_message(holdCount=1)
    await h.consumer.on_message(done)
    assert len(h.confirmed_to("tr.crm:py.parked")) == 1


# ---------------------------------------------------------------- reporter guard

async def test_reporter_returning_none_is_internal():
    h = Harness(reporter=lambda flight, error: None)
    message = h.flight_message()
    await h.consumer.on_message(message)
    # INTERNAL holds one round, so the flight ends up in the holding queue
    flight, incident, reason = h.emitter.interims[0]
    assert incident.category is IncidentCategory.INTERNAL
    assert incident.evidence.message_key == "reporter-returned-null"


async def test_reporter_raising_is_internal_with_evidence():
    def reporter(flight, error):
        raise KeyError("oops")

    h = Harness(reporter=reporter)
    message = h.flight_message()
    await h.consumer.on_message(message)
    flight, incident, reason = h.emitter.interims[0]
    assert incident.category is IncidentCategory.INTERNAL
    assert incident.evidence.code == "KeyError"


# ---------------------------------------------------------------- framework errors

async def test_report_publish_failure_parks_after_land():
    h = Harness()
    h.emitter.fail_next = 1  # the LANDED record publish fails after the pilot landed
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert len(h.landed) == 1
    # catch-all: best-effort INTERNAL record, park on the plain channel, exactly one settle
    parked = h.raw_published_to("tr.crm:py.parked")
    assert len(parked) == 1
    assert json.loads(parked[0].body)["flightId"] is not None
    assert message.acks == 1 and message.nacks == []


async def test_framework_park_failure_nacks():
    h = Harness()
    h.emitter.fail_next = 2  # record fails, then the catch-all's best-effort record also fails
    h.channel.default_exchange.fail_next = 1  # and the plain park fails too
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert message.acks == 0 and message.nacks == [True]


async def test_error_after_settle_never_settles_twice():
    # The pauser throws AFTER the deferral settled the delivery: logged, never a second settle.
    def reporter(flight, error):
        return Incident(IncidentCategory.AIRPORT_FAULT, Evidence.from_exception(error))

    h = Harness(pilot_error=ValueError("401"), reporter=reporter,
                pauser_error=RuntimeError("pause failed"))
    message = h.flight_message()
    await h.consumer.on_message(message)
    assert message.acks == 1 and message.nacks == []
    assert h.raw_published_to("tr.crm:py.parked") == []  # not parked: it was settled
