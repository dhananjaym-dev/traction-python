"""Data-plane status reads and the tr:events contract.

One Redis hash per scope, no TTL, written by the control plane's StatusStore:
``tr:status:<airportId>`` and ``tr:status:<airportId>/<gateId>``, field ``status``. An absent
key or field is OPEN; a read failure or unknown value also reads OPEN — with Redis down the
flight path is already stopped by clearance (which fails closed), so failing closed here would
only change which error the operator sees.

``tr:events`` is pub/sub, at-most-once: the durable hash stays the truth for a process that
missed a broadcast, which is why resume re-reads it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum

from traction.spi import ID_PATTERN

logger = logging.getLogger(__name__)

STATUS_KEY_PREFIX = "tr:status:"
STATUS_FIELD = "status"
EVENTS_CHANNEL = "tr:events"
STATUS_CHANGED = "STATUS_CHANGED"


class AirportStatus(StrEnum):
    OPEN = "OPEN"
    GROUND_STOP = "GROUND_STOP"
    CLOSED = "CLOSED"

    @property
    def allows_consumption(self) -> bool:
        return self is AirportStatus.OPEN


def scope_airport(airport_id: str) -> str:
    return airport_id


def scope_gate(airport_id: str, gate_id: str | None) -> str:
    # The same pattern as spec ids (critically: no '/'), so the compound separator can never
    # appear inside a gate id.
    if gate_id is None or not ID_PATTERN.match(gate_id):
        raise ValueError(f"gateId must match {ID_PATTERN.pattern} but was: {gate_id}")
    return f"{airport_id}/{gate_id}"


def scope_airport_id(scope: str) -> str:
    """The airport a scope belongs to (the whole value for an airport scope)."""
    slash = scope.find("/")
    return scope if slash < 0 else scope[:slash]


def scope_gate_id(scope: str) -> str | None:
    """The gate id for a gate scope; None for an airport scope."""
    slash = scope.find("/")
    return None if slash < 0 else scope[slash + 1:]


class StatusReader:
    def __init__(self, redis):
        self._redis = redis

    async def get(self, scope: str) -> AirportStatus:
        try:
            value = await self._redis.hget(STATUS_KEY_PREFIX + scope, STATUS_FIELD)
            if value is None:
                return AirportStatus.OPEN
            if isinstance(value, (bytes, bytearray)):
                value = value.decode()
            return AirportStatus(value)
        except Exception:
            logger.warning("status read failed for %s; treating it as OPEN", scope, exc_info=True)
            return AirportStatus.OPEN


@dataclass(frozen=True)
class StatusChanged:
    scope: str
    status: AirportStatus
    reason: str | None


def parse_status_changed(payload: str) -> StatusChanged | None:
    """``STATUS_CHANGED|<scopeRef>|<status>|<reason or empty>``; None for anything else."""
    parts = (payload or "").split("|")
    if len(parts) < 3 or parts[0] != STATUS_CHANGED or not parts[1]:
        return None
    try:
        status = AirportStatus(parts[2])
    except ValueError:
        return None
    reason = parts[3] if len(parts) > 3 and parts[3] else None
    return StatusChanged(scope=parts[1], status=status, reason=reason)
