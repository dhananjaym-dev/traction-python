"""Clearance: the shared rate-limit token acquire.

One atomic Lua call against the airport's single bucket ``tr:slots:<airportId>``, so every
process — JVM or not — draws from the same tokens. ``clearance.lua`` is vendored byte-identical
from the companion Java repo (traction-core/src/main/resources/traction/clearance.lua); a change
there is a cross-repo contract bump, never a local edit.

Fails CLOSED: a Redis failure raises instead of granting, so an outage can never bypass the
limit; the caller defers the flight to the runway tail.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources

from redis.exceptions import NoScriptError

from traction.spi import SlotsConfig

KEY_PREFIX = "tr:slots:"
MILLI_PER_SLOT = 1000

CLEARANCE_LUA = (resources.files("traction") / "clearance.lua").read_text(encoding="utf-8")


class ClearanceUnavailableError(Exception):
    """The bucket could not be read or was unconfigured: the flight is deferred, never flown
    unpaced."""


@dataclass(frozen=True)
class ClearanceResult:
    granted: bool
    wait_ms: int


class ClearanceService:
    def __init__(self, redis):
        self._redis = redis
        self._sha: str | None = None
        self._applied: dict[str, SlotsConfig] = {}

    async def acquire(self, airport_id: str, slots: SlotsConfig, cost: int) -> ClearanceResult:
        """Draws ``cost`` slots from the airport's shared bucket.

        Raises ``ValueError`` when cost is below 1 or above the burst (it could never be
        granted) and ``ClearanceUnavailableError`` on any Redis failure or an unconfigured
        bucket (fail closed)."""
        if cost < 1 or cost > slots.burst:
            raise ValueError(f"cost must be between 1 and burst ({slots.burst}): {cost}")
        key = KEY_PREFIX + airport_id
        try:
            await self._reconcile(airport_id, key, slots)
            result = await self._eval(key, str(cost * MILLI_PER_SLOT))
        except ClearanceUnavailableError:
            raise
        except Exception as e:
            raise ClearanceUnavailableError(f"clearance unavailable for {airport_id}") from e
        granted = int(result[0])
        if granted == -1:
            # The hash vanished between reconcile and eval (flush/eviction). Fail closed and
            # make the next acquire write the config again.
            self._applied.pop(airport_id, None)
            raise ClearanceUnavailableError(f"clearance bucket for {airport_id} is unconfigured")
        if granted == 1:
            return ClearanceResult(granted=True, wait_ms=0)
        return ClearanceResult(granted=False, wait_ms=int(result[1]))

    async def _eval(self, key: str, weight: str) -> list:
        if self._sha is None:
            self._sha = await self._redis.script_load(CLEARANCE_LUA)
        try:
            result = await self._redis.evalsha(self._sha, 1, key, weight)
        except NoScriptError:
            # Redis restarted or failed over: reload and retry once.
            self._sha = await self._redis.script_load(CLEARANCE_LUA)
            result = await self._redis.evalsha(self._sha, 1, key, weight)
        if result is None or len(result) < 2:
            raise ClearanceUnavailableError(f"unexpected clearance script result: {result}")
        return result

    async def _reconcile(self, airport_id: str, key: str, slots: SlotsConfig) -> None:
        """Makes the shared bucket's config match ``slots``: on the first acquire for an airport
        in this process, and whenever the config changed. Writes ONLY config fields, never
        tokens/last_refill, so it never resets shared bucket state. A failure propagates and
        leaves the config unapplied, so the next acquire retries."""
        if self._applied.get(airport_id) == slots:
            return
        capacity_milli = slots.burst * MILLI_PER_SLOT
        window_ms = slots.window_s * 1000.0
        refill_per_sec_milli = max(1, round(slots.limit * 1_000_000.0 / window_ms))
        await self._redis.hset(key, mapping={
            "capacity_milli": str(capacity_milli),
            "refill_per_sec_milli": str(refill_per_sec_milli),
        })
        self._applied[airport_id] = slots
