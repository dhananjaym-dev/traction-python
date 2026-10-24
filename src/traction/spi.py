"""The tenant-facing SPI: the closed outcome vocabulary, the Pilot and FlightReporter
protocols, hold and slot policies, and the airport spec. Tenant code never touches ack/nack,
buckets or status keys — it lands flights and classifies failures."""

from __future__ import annotations

import inspect
import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel

from traction.models import Flight

# Ids become queue-name segments and Redis-scope segments (critically: no '/').
ID_PATTERN = re.compile(r"^[a-zA-Z0-9:_\-.]+$")


class IncidentCategory(StrEnum):
    """The level a failure breaks: airport, gate or flight. Classify to the level it breaks;
    if unsure between airport and gate, choose the narrower one (streaks converge upward)."""

    AIRPORT_FAULT = "AIRPORT_FAULT"
    GATE_FAULT = "GATE_FAULT"
    FLIGHT_FAULT = "FLIGHT_FAULT"
    TRANSIENT = "TRANSIENT"
    RATE_LIMIT = "RATE_LIMIT"
    INTERNAL = "INTERNAL"


@dataclass(frozen=True)
class Evidence:
    """Failure detail carried by an incident. All fields nullable. Sanitize PII when
    classifying; ``excerpt`` is bounded at creation, the wire clamps again on emit."""

    code: str | None
    message_key: str | None
    excerpt: str | None

    MAX_EXCERPT = 300

    @staticmethod
    def from_exception(error: BaseException | None) -> Evidence | None:
        if error is None:
            return None
        message = str(error)
        return Evidence(type(error).__name__, None,
                        message[: Evidence.MAX_EXCERPT] if message else None)

    def display(self) -> str | None:
        parts = [p for p in (self.code, self.message_key, self.excerpt) if p and p.strip()]
        return " | ".join(parts) if parts else None


@dataclass(frozen=True)
class Landed:
    pass


@dataclass(frozen=True)
class Skipped:
    reason: str

    def __post_init__(self) -> None:
        if not self.reason or not self.reason.strip():
            raise ValueError("Skipped requires a non-blank reason")


@dataclass(frozen=True)
class Incident:
    category: IncidentCategory
    evidence: Evidence | None = None


FlightReport = Landed | Skipped | Incident


@runtime_checkable
class Pilot(Protocol):
    """Lands one flight: does the real work. The payload is already resolved. Errors raise."""

    async def land(self, flight: Flight) -> None: ...


@runtime_checkable
class FlightReporter(Protocol):
    """Maps (flight, error-or-None) to the closed vocabulary. No side effects."""

    def report(self, flight: Flight, error: BaseException | None) -> FlightReport: ...


@dataclass(frozen=True)
class HoldPolicy:
    """Holding rounds (and backoff seconds per round) a category gets before the flight parks.
    An empty schedule means the category is never held."""

    schedule: tuple[float, ...]

    @staticmethod
    def default_for(category: IncidentCategory) -> HoldPolicy:
        if category is IncidentCategory.TRANSIENT:
            return TRANSIENT_DEFAULT
        if category is IncidentCategory.INTERNAL:
            return INTERNAL_DEFAULT
        return NEVER


TRANSIENT_DEFAULT = HoldPolicy(schedule=(30, 120, 600))
INTERNAL_DEFAULT = HoldPolicy(schedule=(30,))
NEVER = HoldPolicy(schedule=())
HoldPolicy.TRANSIENT_DEFAULT = TRANSIENT_DEFAULT  # type: ignore[attr-defined]
HoldPolicy.INTERNAL_DEFAULT = INTERNAL_DEFAULT  # type: ignore[attr-defined]
HoldPolicy.NEVER = NEVER  # type: ignore[attr-defined]


@dataclass(frozen=True)
class SlotsConfig:
    """The shared rate limit: ``limit`` slots per ``window_s`` seconds, up to ``burst``."""

    limit: int
    window_s: float
    burst: int

    def __post_init__(self) -> None:
        if self.limit <= 0 or self.burst <= 0 or self.window_s <= 0:
            raise ValueError("SlotsConfig requires positive limit/burst and a positive window")

    @staticmethod
    def of(limit: int, window_s: float) -> SlotsConfig:
        return SlotsConfig(limit=limit, window_s=window_s, burst=limit)


@dataclass
class AirportSpec:
    """One airport: identity, the shared rate limit, its gates with slot costs, the pilot, the
    reporter, and the payload types its flights carry (name on the wire -> pydantic model)."""

    airport_id: str
    display_name: str
    slots: SlotsConfig
    gates: dict[str, int]
    pilot: Pilot
    reporter: FlightReporter
    payload_types: dict[str, type[BaseModel]] = field(default_factory=dict)
    hold_policy: HoldPolicy | None = None

    def validate(self) -> None:
        _check_id("airportId", self.airport_id)
        if self.slots is None:
            raise ValueError(f"slots is required for airport {self.airport_id}")
        if self.pilot is None:
            raise ValueError(f"pilot is required for airport {self.airport_id}")
        # A synchronous land would run the side effect, fail on the await, and repeat the side
        # effect every holding round — reject the easy mistake here instead.
        if not inspect.iscoroutinefunction(self.pilot.land):
            raise ValueError(
                f"pilot.land must be `async def` for airport {self.airport_id}")
        if self.reporter is None:
            raise ValueError(f"reporter is required for airport {self.airport_id}")
        for gate_id, cost in self.gates.items():
            _check_id("gateId", gate_id)
            if cost <= 0:
                raise ValueError(f"gate '{gate_id}' slotCost must be positive")
            if cost > self.slots.burst:
                raise ValueError(
                    f"airport '{self.airport_id}' gate '{gate_id}' has slotCost {cost} above "
                    f"the slot burst {self.slots.burst}; it could never be cleared")

    def hold_policy_for(self, category: IncidentCategory) -> HoldPolicy:
        holds = category in (IncidentCategory.TRANSIENT, IncidentCategory.INTERNAL)
        if holds and self.hold_policy is not None:
            return self.hold_policy
        return HoldPolicy.default_for(category)

    def payload_type_name(self, payload: Any) -> str:
        for name, model in self.payload_types.items():
            if type(payload) is model:
                return name
        raise ValueError(
            f"payload type {type(payload).__name__} is not registered on airport "
            f"{self.airport_id}")

    def slot_cost(self, gate_id: str | None) -> int:
        """The gate's slot cost; 1 for a flight whose gate is not (or no longer) declared."""
        if gate_id is None:
            return 1
        return self.gates.get(gate_id, 1)


def _check_id(what: str, value: str | None) -> None:
    if value is None or not ID_PATTERN.match(value):
        raise ValueError(f"{what} must match {ID_PATTERN.pattern} but was: {value}")
