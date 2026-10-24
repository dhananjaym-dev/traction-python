"""The per-flight loop for one airport (manual ack, one delivery at a time): airport status →
ground delay → gate status → charter abort → payload → clearance → land → report → settle.
A step-for-step port of the Java FlightConsumer; the comments there carry the full argument.

Invariant: every delivery settles exactly once, by ack (after any confirmed republish / hold /
park) or by nack-requeue. Deliberate deferrals republish a fresh message to the runway tail and
ack, so the runway's delivery limit only counts genuine crashes. A step that throws before the
delivery settled lands in the catch-all: record INTERNAL, park, ack. A message that does not
decode, or whose envelope is not a flight for this airport, parks raw.

Clearance fails closed: if the shared bucket cannot be read the flight is deferred, after a
short backoff, and never flown unpaced. The settle matrix is traction.settle.SettleDecision.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from traction._amqp import TYPE_ID_FLIGHT, json_message
from traction.charters import CharterAbortReader
from traction.clearance import ClearanceService, ClearanceUnavailableError
from traction.ground_delay import GroundDelayService
from traction.models import Disposition, Flight, RecordType
from traction.report import FlightReportEmitter
from traction.settle import HoldOutcome, Route, SettleDecision
from traction.spi import AirportSpec, Evidence, FlightReport, Incident, IncidentCategory
from traction.status import StatusReader, scope_airport, scope_gate
from traction.topology import queue_names

logger = logging.getLogger(__name__)

# Backoff before a deliberate re-queue while the airport is stopped or clearance is down.
DEFER_BACKOFF_S = 0.5
# A clearance wait above this escalates to a ground delay instead of sleeping the consumer.
MAX_CLEARANCE_SLEEP_S = 10.0
# Total clearance sleeping one delivery may do before escalating to a ground delay.
MAX_CLEARANCE_WAIT_S = 15.0
# One ground-delay sleep slice; caps the latency of reacting to a shutdown or reopen.
MAX_DELAY_SLEEP_S = 600.0
# Total unacked ground-delay sleep, safely under the broker's 30 min consumer timeout.
MAX_TOTAL_DELAY_S = 1500.0

# Evidence message key of a flight parked because its payloadType does not resolve here.
PAYLOAD_TYPE_UNRESOLVED = "PAYLOAD_TYPE_UNRESOLVED"


def delay_slice_s(remain_s: float, slept_s: float) -> float:
    """Next ground-delay slice: bounded by the marker's remaining time, the slice cap and the
    unspent budget."""
    return min(max(remain_s, 0.001), MAX_DELAY_SLEEP_S, MAX_TOTAL_DELAY_S - slept_s)


class FlightConsumer:
    def __init__(self, *, spec: AirportSpec, channel, status_reader: StatusReader,
                 emitter: FlightReportEmitter, publisher, pauser: Callable[[], Awaitable[None]],
                 clearance: ClearanceService, ground_delay: GroundDelayService,
                 charters: CharterAbortReader,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        self._spec = spec
        self._airport_id = spec.airport_id
        self._names = queue_names(spec.airport_id)
        self._channel = channel  # the plain channel: raw parks and the last-resort park only
        self._status = status_reader
        self._emitter = emitter
        self._publisher = publisher
        self._pauser = pauser
        self._clearance = clearance
        self._ground_delay = ground_delay
        self._charters = charters
        self._sleep = sleep
        self._in_flight = 0
        self._idle = asyncio.Event()
        self._idle.set()

    async def drain(self, timeout_s: float) -> bool:
        """Waits for the delivery in flight (if any) to settle; False on timeout. A graceful
        stop cancels the consumer, then drains, then closes the channel — so a flight mid-land
        finishes instead of being redelivered as a duplicate vendor call."""
        try:
            await asyncio.wait_for(self._idle.wait(), timeout_s)
            return True
        except TimeoutError:
            return False

    async def on_message(self, message) -> None:
        self._in_flight += 1
        self._idle.clear()
        try:
            await self._on_message(message)
        finally:
            self._in_flight -= 1
            if self._in_flight == 0:
                self._idle.set()

    async def _on_message(self, message) -> None:
        try:
            flight = Flight.model_validate_json(message.body)
        except Exception:
            await self._park_raw(message, "undeserializable message")
            return
        problem = self._envelope_problem(flight)
        if problem is not None:
            # Nothing about this flight can be trusted (not its scope, not its type): park it
            # as it came.
            await self._park_raw(message, f"{problem} (flight {flight.flight_id})")
            return

        # Per-delivery state. A one-element carrier, so a step that settles and THEN throws
        # still tells the catch-all not to settle again (a double settle closes the channel).
        settled = [False]
        try:
            await self._fly(flight, message, settled)
        except asyncio.CancelledError:
            raise
        except Exception as framework_error:
            if settled[0]:
                logger.error("airport %s: framework error after flight %s was settled; left as is",
                             self._airport_id, flight.flight_id, exc_info=framework_error)
            else:
                await self._park_after_framework_error(flight, message, framework_error)

    async def _fly(self, flight: Flight, message, settled: list[bool]) -> None:
        # 1. Airport status: authoritative per flight (covers a lagging pause).
        airport_status = await self._status.get(scope_airport(self._airport_id))
        if not airport_status.allows_consumption:
            await self._sleep(DEFER_BACKOFF_S)  # bounds the republish spin until the pause lands
            await self._defer_to_runway_tail(flight, message, settled)
            await self._pauser()
            return

        # 2. Ground delay: the sleeping consumer is the resume timer.
        if not await self._sleep_through_ground_delay(flight, message, settled):
            return

        # 3. Gate status: a grounded gate parks its flights while the rest of the airport flows.
        if flight.gate_id is not None:
            gate_status = await self._status.get(scope_gate(self._airport_id, flight.gate_id))
            if not gate_status.allows_consumption:
                from traction.spi import Skipped

                skipped = Skipped("GATE_GROUNDED")
                await self._emitter.emit_record(flight, skipped, Disposition.PARKED)
                await self._publisher.publish(self._names.parked, flight.to_wire(), TYPE_ID_FLIGHT)
                await self._ack(message, settled)
                return

        # 3b. Charter abort: an aborted charter's remaining flights settle SKIPPED
        # CHARTER_ABORTED (TERMINAL, acked, never parked), before clearance so they burn no
        # slots. The marker read fails open.
        if flight.charter_ref is not None and await self._charters.is_aborted(flight.charter_ref):
            from traction.spi import Skipped

            await self._emitter.emit_record(flight, Skipped("CHARTER_ABORTED"), Disposition.TERMINAL)
            await self._ack(message, settled)
            return

        # 4. Payload, before clearance so a rejected or parked flight never burns a slot. A
        # stamped type this process cannot resolve is not the flight's fault: it parks, and a
        # replay after a redeploy recovers it. JSON that won't become the resolved type is the
        # flight's fault: rejected.
        model = self._spec.payload_types.get(flight.payload_type)
        if model is None:
            parked = Incident(IncidentCategory.INTERNAL,
                              Evidence("KeyError", PAYLOAD_TYPE_UNRESOLVED, flight.payload_type))
            self._log_failure(Disposition.PARKED, parked, flight, None)
            await self._emitter.emit_record(flight, parked, Disposition.PARKED)
            await self._publisher.publish(self._names.parked, flight.to_wire(), TYPE_ID_FLIGHT)
            await self._ack(message, settled)
            return
        if flight.payload is None:
            payload = None
        else:
            try:
                payload = model.model_validate(flight.payload)
            except Exception as e:
                rejected = Incident(IncidentCategory.FLIGHT_FAULT,
                                    Evidence(type(e).__name__, "payload-unreadable",
                                             str(e)[: Evidence.MAX_EXCERPT] or None))
                self._log_failure(Disposition.TERMINAL, rejected, flight, e)
                await self._emitter.emit_record(flight, rejected, Disposition.TERMINAL)
                await self._ack(message, settled)
                return

        # 5. Clearance: the shared slot bucket, before any work.
        if not await self._clear(flight, message, settled):
            return

        # 6. Land
        attempted = flight.with_incremented_attempt()
        typed = attempted.model_copy(update={"payload": payload})
        error: BaseException | None = None
        try:
            await self._spec.pilot.land(typed)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            error = e

        # 7. Report (a broken reporter must not lose the flight), 8. settle
        await self._settle(self._classify(typed, error), attempted, message, settled, error)

    def _classify(self, flight: Flight, error: BaseException | None) -> FlightReport:
        try:
            report = self._spec.reporter.report(flight, error)
            if report is not None:
                return report
            logger.error("airport %s: reporter returned null; classifying INTERNAL",
                         self._airport_id)
            return Incident(IncidentCategory.INTERNAL,
                            Evidence(None, "reporter-returned-null", None))
        except Exception as reporter_error:
            logger.error("airport %s: reporter threw; classifying INTERNAL", self._airport_id,
                         exc_info=reporter_error)
            return Incident(IncidentCategory.INTERNAL, Evidence.from_exception(reporter_error))

    async def _settle(self, report: FlightReport, flight: Flight, message, settled: list[bool],
                      pilot_error: BaseException | None) -> None:
        decision = SettleDecision.of(report)
        if decision.route is Route.ACK:
            await self._write(decision, report, flight, pilot_error)
            await self._ack(message, settled)
        elif decision.route is Route.PARK:
            await self._write(decision, report, flight, pilot_error)
            await self._publisher.publish(self._names.parked, flight.to_wire(), TYPE_ID_FLIGHT)
            await self._ack(message, settled)
        elif decision.route is Route.HOLD_OR_PARK:
            await self._hold_or_park(report, flight, message, settled, pilot_error)
        elif decision.route is Route.DEFER_AND_PAUSE:
            # The control plane ground-stops the airport on this report; pausing now keeps this
            # consumer from burning the runway during the round trip. The flight re-flies on
            # reopen.
            await self._write(decision, report, flight, pilot_error)
            await self._defer_to_runway_tail(flight, message, settled)
            await self._pauser()
        elif decision.route is Route.DELAY_AND_DEFER:
            # Not the flight's problem: pace the airport down. No pause; later deliveries sleep
            # through the delay at step 2.
            await self._write(decision, report, flight, pilot_error)
            await self._ground_delay.delay(self._airport_id, self._spec.slots.window_s)
            await self._defer_to_runway_tail(flight, message, settled)

    async def _hold_or_park(self, report: FlightReport, flight: Flight, message,
                            settled: list[bool], pilot_error: BaseException | None) -> None:
        """The holding pattern (a TTL retry queue): while the flight has hold budget left,
        publish it to the holding queue with the backoff for its round; the broker dead-letters
        it back to the runway on expiry. Budget spent: park it, never drop it. For retries only,
        never for pacing (that is clearance)."""
        assert isinstance(report, Incident)
        policy = self._spec.hold_policy_for(report.category)
        rounds = len(policy.schedule)
        if flight.hold_count >= rounds:
            await self._publisher.publish(self._names.parked, flight.to_wire(), TYPE_ID_FLIGHT)
            logger.info("flight %s exhausted its hold budget (%d rounds); parked",
                        flight.flight_id, rounds)
            outcome, after = HoldOutcome.PARKED, flight
        else:
            ttl_s = policy.schedule[flight.hold_count]
            after = flight.with_incremented_hold()
            await self._publisher.publish(self._names.holding, after.to_wire(), TYPE_ID_FLIGHT,
                                          expiration_ms=int(ttl_s * 1000))
            logger.info("flight %s holding for %.0fs (round %d/%d)", flight.flight_id, ttl_s,
                        after.hold_count, rounds)
            outcome = HoldOutcome.HELD
        await self._write(SettleDecision.after_holding(outcome, after.hold_count), report, after,
                          pilot_error)
        await self._ack(message, settled)

    async def _write(self, decision: SettleDecision, report: FlightReport, flight: Flight,
                     pilot_error: BaseException | None) -> None:
        """Emits the decision's row (and logs the failure line for incidents)."""
        if isinstance(report, Incident):
            self._log_failure(decision.disposition, report, flight, pilot_error)
            if decision.record_type is RecordType.INTERIM:
                await self._emitter.emit_interim(flight, report, decision.reason)
                return
        await self._emitter.emit_record(flight, report, decision.disposition)

    async def _clear(self, flight: Flight, message, settled: list[bool]) -> bool:
        """One non-blocking acquire per attempt; short waits sleep and retry, long waits (or too
        much total sleeping) ground-delay the airport and defer. A clearance failure defers
        after a backoff. Returns True when cleared; otherwise the delivery has been settled."""
        cost = self._spec.slot_cost(flight.gate_id)
        slept = 0.0
        while True:
            try:
                result = await self._clearance.acquire(self._airport_id, self._spec.slots, cost)
            except ClearanceUnavailableError:
                logger.warning("airport %s: clearance unavailable; deferring flight %s to the "
                               "runway tail (never flown unpaced)", self._airport_id,
                               flight.flight_id, exc_info=True)
                await self._sleep(DEFER_BACKOFF_S)
                await self._defer_to_runway_tail(flight, message, settled)
                return False
            if result.granted:
                return True
            wait_s = max(result.wait_ms, 1) / 1000.0
            if wait_s > MAX_CLEARANCE_SLEEP_S or slept >= MAX_CLEARANCE_WAIT_S:
                cooldown = wait_s if wait_s > MAX_CLEARANCE_SLEEP_S else self._spec.slots.window_s
                await self._ground_delay.delay(self._airport_id, cooldown)
                await self._defer_to_runway_tail(flight, message, settled)  # no pause: step 2 sleeps
                return False
            await self._sleep(wait_s)
            slept += wait_s

    async def _sleep_through_ground_delay(self, flight: Flight, message,
                                          settled: list[bool]) -> bool:
        """Sleeps while the airport's ground-delay marker is live, in bounded slices. Past the
        total cap the delivery is released to the runway tail (the marker persists, so it sleeps
        again there). Returns False when the delivery has been settled."""
        slept = 0.0
        while (remaining := await self._ground_delay.remaining(self._airport_id)) is not None:
            if slept >= MAX_TOTAL_DELAY_S:
                await self._defer_to_runway_tail(flight, message, settled)
                return False
            slice_s = delay_slice_s(remaining, slept)
            await self._sleep(slice_s)
            slept += slice_s
        return True

    async def _defer_to_runway_tail(self, flight: Flight, message, settled: list[bool]) -> None:
        """Deliberate deferral: confirmed republish to the runway tail, then ack. A publish
        failure falls back to nack-requeue (broker-owned redelivery: degraded accounting, never
        loss)."""
        try:
            await self._publisher.publish(self._names.runway, flight.to_wire(), TYPE_ID_FLIGHT)
            await message.ack()
        except Exception:
            logger.warning("airport %s: deferral republish failed for flight %s; falling back "
                           "to nack", self._airport_id, flight.flight_id, exc_info=True)
            await message.nack(requeue=True)
        settled[0] = True

    async def _park_after_framework_error(self, flight: Flight, message,
                                          framework_error: BaseException) -> None:
        """Never drop, never spin: best-effort INTERNAL record, then park and ack. The park uses
        the plain channel: the final safety net, where a confirm failure would leave no safe
        option."""
        incident = Incident(IncidentCategory.INTERNAL, Evidence.from_exception(framework_error))
        self._log_failure(Disposition.PARKED, incident, flight, framework_error,
                          framework_error=True)
        try:
            await self._emitter.emit_record(flight, incident, Disposition.PARKED)
        except Exception:
            logger.warning("airport %s: park record emit failed for flight %s", self._airport_id,
                           flight.flight_id, exc_info=True)
        try:
            await self._channel.default_exchange.publish(
                json_message(flight.to_wire(), TYPE_ID_FLIGHT), routing_key=self._names.parked)
        except Exception:
            logger.exception("airport %s: last-resort park failed for flight %s; nack backstop",
                             self._airport_id, flight.flight_id)
            await message.nack(requeue=True)
            return
        await message.ack()

    async def _park_raw(self, message, why: str) -> None:
        """The uninterpretable-message path: the raw message parks as it came and is acked. No
        record is emitted: nothing in it can be trusted to account it against."""
        logger.error("flight-failure disposition=PARKED airport=%s category=INTERNAL %s; "
                     "parking it raw", self._airport_id, why)
        try:
            await self._channel.default_exchange.publish(
                _raw_copy(message), routing_key=self._names.parked)
        except Exception:
            logger.exception("airport %s: raw park failed; nack backstop", self._airport_id)
            await message.nack(requeue=True)
            return
        await message.ack()

    def _envelope_problem(self, flight: Flight) -> str | None:
        """Why this envelope cannot be flown here, or None when it can: it must carry a flight
        id, this consumer's airport, a gate and a payload type."""
        if flight is None:
            return "empty envelope"
        if flight.flight_id is None:
            return "envelope has no flightId"
        if flight.airport_id != self._airport_id:
            return f"envelope airportId '{flight.airport_id}' is not this airport"
        if flight.gate_id is None or not flight.gate_id.strip():
            return "envelope has no gateId"
        if flight.payload_type is None or not flight.payload_type.strip():
            return "envelope has no payloadType"
        return None

    def _log_failure(self, disposition: Disposition | None, incident: Incident, flight: Flight,
                     error: BaseException | None, framework_error: bool = False) -> None:
        """The structured failure line: WARNING for every disposed flight (the framework handled
        it), ERROR only for framework defects. Never emitted for LANDED/SKIPPED."""
        evidence = incident.evidence.display() if incident.evidence else None
        logger.log(
            logging.ERROR if framework_error else logging.WARNING,
            'flight-failure disposition=%s airport=%s gate=%s category=%s attempt=%d holds=%d '
            'domainRef=%s charterRef=%s flight=%s correlationId=%s evidence="%s"',
            disposition, self._airport_id, flight.gate_id, incident.category.value,
            flight.attempt_count, flight.hold_count, flight.domain_ref, flight.charter_ref,
            flight.flight_id, flight.correlation_id, evidence or "", exc_info=error)

    @staticmethod
    async def _ack(message, settled: list[bool]) -> None:
        await message.ack()
        settled[0] = True


def _raw_copy(message):
    """The original bytes and headers, republished as they came."""
    import aio_pika

    return aio_pika.Message(
        body=message.body,
        content_type=getattr(message, "content_type", None),
        headers=dict(getattr(message, "headers", None) or {}),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
