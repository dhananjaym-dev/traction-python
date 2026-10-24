"""Queue names and declarations for one airport. Everything uses the default exchange, so
routing key == queue name. Arguments are pinned to the Java Topology (a mismatch on a shared
queue is a broker PRECONDITION_FAILED):

    runway  (quorum)  --DLX--> parked   (delivery-limit 20: crash loops and broker rejects park)
    holding (quorum)  --DLX--> runway   (per-message expiration set at publish time; limit -1)
    parked  (quorum)  no DLX            (replayed or discarded by an operator; limit -1)
    tower   (quorum)  --DLX--> tr.dlq   (delivery-limit 20)
    tr.dlq  (quorum)  no DLX            (limit -1)

The -1 arguments are why traction._amqp ships the pamqp shim."""

from __future__ import annotations

from dataclasses import dataclass

PREFIX = "tr."
TOWER_QUEUE = PREFIX + "tower.queue"
DLQ = PREFIX + "dlq"


@dataclass(frozen=True)
class QueueNames:
    airport_id: str
    runway: str
    holding: str
    parked: str


def queue_names(airport_id: str) -> QueueNames:
    base = PREFIX + airport_id
    return QueueNames(
        airport_id=airport_id,
        runway=base + ".runway",
        holding=base + ".holding",
        parked=base + ".parked",
    )


async def declare_airport(channel, airport_id: str) -> QueueNames:
    """Declares the per-airport queue set (idempotent). Order is load-bearing: the DLX target
    (parked) is declared before the queue that dead-letters to it (runway)."""
    names = queue_names(airport_id)
    await channel.declare_queue(names.parked, durable=True, arguments={
        "x-queue-type": "quorum",
        "x-delivery-limit": -1,
    })
    await channel.declare_queue(names.holding, durable=True, arguments={
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": names.runway,
        "x-delivery-limit": -1,
    })
    await channel.declare_queue(names.runway, durable=True, arguments={
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": names.parked,
        "x-delivery-limit": 20,
    })
    return names


async def declare_tower(channel) -> None:
    """Declares the tower queue and its dead-letter queue — by every process that publishes to
    or consumes from the tower queue, so the arguments live in exactly one shape."""
    await channel.declare_queue(DLQ, durable=True, arguments={
        "x-queue-type": "quorum",
        "x-delivery-limit": -1,
    })
    await channel.declare_queue(TOWER_QUEUE, durable=True, arguments={
        "x-queue-type": "quorum",
        "x-delivery-limit": 20,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": DLQ,
    })
