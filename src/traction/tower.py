"""Producer facade: validates the destination, builds the Flight envelope and publishes it to
the airport's runway. Only airports registered in this process are known. An unknown airport or
gate fails immediately and nothing is queued.

Publishing is a plain best-effort send on the default exchange (routing key = queue name), like
the Java Tower — deliberately NOT confirmed, so it is safe to call from inside the caller's own
transaction boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from pydantic import BaseModel

from traction._amqp import TYPE_ID_FLIGHT, json_message
from traction.models import Flight
from traction.topology import queue_names

CORRELATION_ID_LENGTH = 6


@dataclass(frozen=True)
class DispatchOptions:
    domain_ref: str | None = None
    charter_ref: str | None = None


class Tower:
    def __init__(self, registry, channel):
        self._registry = registry
        self._channel = channel  # plain, no confirms

    async def dispatch(self, airport_id: str, gate_id: str, payload: BaseModel,
                       options: DispatchOptions | None = None) -> Flight:
        if airport_id is None:
            raise ValueError("airportId is required")
        gates = self._registry.gates_of(airport_id)
        if gates is None:
            raise ValueError(f"unknown airport: {airport_id}")
        if gate_id is None or gate_id not in gates:
            raise ValueError(f"unknown gate '{gate_id}' on airport {airport_id}")
        if payload is None:
            raise ValueError("payload is required (its type is stamped on the flight)")
        spec = self._registry.spec_of(airport_id)
        payload_type = spec.payload_type_name(payload)  # raises for an unregistered type
        opts = options or DispatchOptions()
        flight = Flight(
            flightId=uuid4(),
            airportId=airport_id,
            gateId=gate_id,
            domainRef=opts.domain_ref,
            charterRef=opts.charter_ref,
            attemptCount=0,
            holdCount=0,
            correlationId=str(uuid4())[:CORRELATION_ID_LENGTH],
            payloadType=payload_type,
            # By alias: the consumer re-validates with the same model, which reads aliases, so a
            # field-name dump would reject every flight of an alias-carrying payload model.
            payload=payload.model_dump(mode="json", by_alias=True),
        )
        await self._channel.default_exchange.publish(
            json_message(flight.to_wire(), TYPE_ID_FLIGHT),
            routing_key=queue_names(airport_id).runway,
        )
        return flight
