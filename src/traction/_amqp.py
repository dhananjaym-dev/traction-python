"""The AMQP seam: wire headers, the JSON message builder, confirmed publishes, and the pamqp
signed-integer shim.

Tower-queue messages are dispatched by the Java control plane on the ``__TypeId__`` header,
restricted to the trusted packages in TowerMessageListener.converter — so a non-JVM producer
must stamp the literal Java class names below.
"""

from __future__ import annotations

import json
from datetime import timedelta

import aio_pika
from pamqp import encode

TYPE_ID_HEADER = "__TypeId__"
TYPE_ID_FLIGHT = "dev.traction.core.api.Flight"
TYPE_ID_REPORT = "dev.traction.core.report.FlightReportMessage"
TYPE_ID_ANNOUNCEMENT = "dev.traction.core.registry.AirportAnnouncement"

# Broker-confirm wait, like the Java ConfirmedPublisher.
CONFIRM_TIMEOUT_S = 5.0


def _apply_pamqp_shim() -> None:
    """pamqp (<= 3.3.0) packs small field-table ints through an unsigned byte, so a negative
    value such as the contract's ``x-delivery-limit: -1`` raises struct.error before the frame
    leaves the client. Encode negative int32 as AMQP 'I' + signed int32 — byte-identical to the
    Java client — and delegate everything else."""
    original = encode.table_integer
    if getattr(original, "__traction_shim__", False):
        return

    def table_integer(value: int) -> bytes:
        if -(2**31) <= value < 0:
            return b"I" + encode.long_int(value)
        return original(value)

    table_integer.__traction_shim__ = True  # type: ignore[attr-defined]
    encode.table_integer = table_integer


_apply_pamqp_shim()


def json_message(
    body: dict, type_id: str, expiration_ms: int | None = None
) -> aio_pika.Message:
    """A persistent JSON message in the Traction wire format (explicit nulls stay)."""
    return aio_pika.Message(
        body=json.dumps(body).encode("utf-8"),
        content_type="application/json",
        headers={TYPE_ID_HEADER: type_id},
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        expiration=timedelta(milliseconds=expiration_ms) if expiration_ms is not None else None,
    )


class PublishException(Exception):
    """The broker did not confirm (or refused) a publish; the caller must not ack the original."""


class ConfirmedPublisher:
    """Publishes that wait for the broker's confirm. The channel must have publisher confirms
    on (aio-pika's default); ``publish`` resolves only once the broker acks, and anything else
    becomes a :class:`PublishException` so callers fall back to nack-requeue."""

    def __init__(self, channel: aio_pika.abc.AbstractChannel):
        self._channel = channel

    async def publish(
        self, queue: str, body: dict, type_id: str, expiration_ms: int | None = None
    ) -> None:
        message = json_message(body, type_id, expiration_ms)
        try:
            await self._channel.default_exchange.publish(
                message, routing_key=queue, timeout=CONFIRM_TIMEOUT_S
            )
        except Exception as e:
            raise PublishException(f"publish to {queue} not confirmed") from e
