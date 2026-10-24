"""Holds the registered airports and owns their consumers. Registration validates the spec,
declares its queues (and the tower's), announces it on the tower queue and starts one sequential
consumer on the runway. Pause cancels the consumer (the quorum queue buffers); resume re-reads
the stored status first, so a stale or out-of-order tr:events broadcast can never restart a
grounded airport."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from uuid import uuid4

from traction._amqp import TYPE_ID_ANNOUNCEMENT, ConfirmedPublisher
from traction.charters import CharterAbortReader
from traction.clearance import ClearanceService
from traction.consumer import FlightConsumer
from traction.ground_delay import GroundDelayService
from traction.models import AirportAnnouncement, GateAnnouncement
from traction.report import FlightReportEmitter
from traction.spi import AirportSpec
from traction.status import (
    EVENTS_CHANNEL,
    StatusReader,
    parse_status_changed,
    scope_airport,
    scope_gate_id,
)
from traction.topology import TOWER_QUEUE, declare_airport, declare_tower, queue_names

logger = logging.getLogger(__name__)

# A graceful stop waits this long for the in-flight delivery to settle before closing channels.
DRAIN_TIMEOUT_S = 5.0


@dataclass
class _Registered:
    spec: AirportSpec
    channel: object  # the confirm channel the consumer and its publishes run on
    plain_channel: object  # raw parks and the last-resort park
    consumer: FlightConsumer
    queue: object | None = None
    consumer_tag: str | None = None


class AirportRegistry:
    def __init__(self, *, connection, redis, owning_process: str):
        self._connection = connection
        self._redis = redis
        self._owning_process = owning_process
        self._status_reader = StatusReader(redis)
        self._airports: dict[str, _Registered] = {}
        self._events_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def register_all(self, specs: list[AirportSpec]) -> None:
        """Validates every spec first, then registers them, so a bad spec or a duplicate id
        fails the whole batch before any queue is declared or any airport announced."""
        ids: set[str] = set()
        for spec in specs:
            spec.validate()
            if spec.airport_id in ids or spec.airport_id in self._airports:
                raise ValueError(f"duplicate airport id: {spec.airport_id}")
            ids.add(spec.airport_id)
        for spec in specs:
            await self._register(spec)

    async def _register(self, spec: AirportSpec) -> None:
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=1)  # one flight at a time per airport
        plain_channel = await self._connection.channel(publisher_confirms=False)
        await declare_airport(channel, spec.airport_id)
        await declare_tower(channel)

        publisher = ConfirmedPublisher(channel)
        announcement = AirportAnnouncement(
            announcementId=uuid4(),
            airportId=spec.airport_id,
            owningProcess=self._owning_process,
            displayName=spec.display_name,
            slotsLimit=spec.slots.limit,
            slotsWindowSeconds=int(spec.slots.window_s),
            slotsBurst=spec.slots.burst,
            gates=[GateAnnouncement(gateId=g, slotCost=c) for g, c in spec.gates.items()],
            epochMillis=int(time.time() * 1000),
        )
        # Fail-fast: once the broker acks, the durable tower queue guarantees eventual
        # projection even while the control plane is down.
        await publisher.publish(TOWER_QUEUE, announcement.to_wire(), TYPE_ID_ANNOUNCEMENT)

        airport_id = spec.airport_id
        consumer = FlightConsumer(
            spec=spec,
            channel=plain_channel,
            status_reader=self._status_reader,
            emitter=FlightReportEmitter(publisher),
            publisher=publisher,
            pauser=lambda: self.request_pause(airport_id),
            clearance=ClearanceService(self._redis),
            ground_delay=GroundDelayService(self._redis),
            charters=CharterAbortReader(self._redis),
        )
        registered = _Registered(spec=spec, channel=channel, plain_channel=plain_channel,
                                 consumer=consumer)
        self._airports[airport_id] = registered
        await self._start_consuming(registered)
        logger.info("airport %s registered and consuming", airport_id)

    async def _start_consuming(self, registered: _Registered) -> None:
        if registered.consumer_tag is not None:
            return
        registered.queue = await registered.channel.get_queue(
            queue_names(registered.spec.airport_id).runway)
        registered.consumer_tag = await registered.queue.consume(registered.consumer.on_message)

    async def request_pause(self, airport_id: str) -> None:
        """Cancels the airport's consumer. A no-op when unknown or already paused; never raises
        (callers settle the delivery before asking for a pause)."""
        registered = self._airports.get(airport_id)
        if registered is None or registered.consumer_tag is None:
            return
        try:
            async with self._lock:
                if registered.consumer_tag is None:
                    return
                tag, registered.consumer_tag = registered.consumer_tag, None
                await registered.queue.cancel(tag)
            logger.info("airport %s paused (consumer cancelled)", airport_id)
        except Exception:
            logger.error("airport %s pause failed", airport_id, exc_info=True)

    async def resume(self, airport_id: str) -> None:
        """Restarts a paused airport's consumer — only when the stored status allows consumption
        (events can arrive stale or out of order, so the stored status is re-checked)."""
        registered = self._airports.get(airport_id)
        if registered is None or registered.consumer_tag is not None:
            return
        status = await self._status_reader.get(scope_airport(airport_id))
        if not status.allows_consumption:
            logger.info("airport %s resume skipped (stored status %s)", airport_id, status)
            return
        async with self._lock:
            if registered.consumer_tag is None:
                await self._start_consuming(registered)
        logger.info("airport %s resumed", airport_id)

    async def handle_event(self, payload: str) -> None:
        """One tr:events payload: a ground stop decided by the control plane pauses this
        process's consumer for that airport, a reopen resumes it. Gate scopes are ignored (the
        consumer checks gate status per flight). Never raises."""
        try:
            event = parse_status_changed(payload)
            if event is None or scope_gate_id(event.scope) is not None:
                return
            airport_id = event.scope
            if airport_id not in self._airports:
                return
            if event.status.allows_consumption:
                await self.resume(airport_id)
            else:
                await self.request_pause(airport_id)
        except Exception:
            logger.warning("tr:events payload not applied: %s", payload, exc_info=True)

    def start_events(self) -> None:
        """Subscribes this process to tr:events. Separate from registration (like the Java
        auto-config, where the pub/sub container is its own bean): the application's runtime
        calls it once, after the airports are registered. Required for a working tenant: resume
        only ever arrives as an event, so without this a paused airport stays paused until
        restart."""
        if self._events_task is None:
            self._events_task = asyncio.create_task(self._events_loop(), name="tr-events")

    async def _events_loop(self) -> None:
        """Subscribes this process to tr:events; reconnects forever (pub/sub is at-most-once and
        the stored status is the truth, so a dropped subscription only delays a pause/resume)."""
        while True:
            try:
                pubsub = self._redis.pubsub()
                await pubsub.subscribe(EVENTS_CHANNEL)
                async for message in pubsub.listen():
                    if message.get("type") != "message":
                        continue
                    data = message.get("data")
                    payload = data.decode() if isinstance(data, (bytes, bytearray)) else str(data)
                    await self.handle_event(payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("tr:events subscription dropped; reconnecting in 5s", exc_info=True)
                await asyncio.sleep(5)

    def is_registered(self, airport_id: str | None) -> bool:
        return airport_id is not None and airport_id in self._airports

    def gates_of(self, airport_id: str | None) -> dict[str, int] | None:
        registered = self._airports.get(airport_id) if airport_id else None
        return dict(registered.spec.gates) if registered else None

    def spec_of(self, airport_id: str | None) -> AirportSpec | None:
        registered = self._airports.get(airport_id) if airport_id else None
        return registered.spec if registered else None

    async def stop(self) -> None:
        """Stops every consumer, then closes the channels; one failure never prevents the rest."""
        if self._events_task is not None:
            self._events_task.cancel()
            with contextlib.suppress(BaseException):
                await self._events_task
            self._events_task = None
        for airport_id, registered in self._airports.items():
            await self.request_pause(airport_id)
            # Let the delivery that is mid-flight settle before its channel closes (the Java
            # container's stop waits the same way); past the timeout the broker redelivers.
            if not await registered.consumer.drain(DRAIN_TIMEOUT_S):
                logger.warning("airport %s: in-flight delivery did not settle within %.0fs; "
                               "the broker will redeliver it", airport_id, DRAIN_TIMEOUT_S)
            for channel in (registered.channel, registered.plain_channel):
                try:
                    await channel.close()
                except Exception:
                    logger.warning("airport %s channel close failed", airport_id, exc_info=True)
