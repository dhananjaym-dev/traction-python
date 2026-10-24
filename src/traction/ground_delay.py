"""Ground delay: a timed, self-expiring pause for an airport (e.g. after a rate-limit response).

The marker ``tr:grounddelay:<airportId>`` holds the intended resume epoch-millis — the value is
authoritative for ``remaining``; the key's TTL is the backstop that removes it. Consumers sleep
through the marker in slices and re-check between slices, so the sleeping consumer is the resume
timer and no leader election is needed. Reads fail open (a wrongly cleared delay just
re-triggers on the next rate-limit response)."""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

KEY_PREFIX = "tr:grounddelay:"


class GroundDelayService:
    def __init__(self, redis):
        self._redis = redis

    async def delay(self, airport_id: str, cooldown_s: float) -> None:
        """Writes the cooldown marker only; pausing is the caller's business. Best effort."""
        try:
            resume_ms = int(time.time() * 1000 + cooldown_s * 1000)
            await self._redis.set(KEY_PREFIX + airport_id, str(resume_ms),
                                  px=max(1, int(cooldown_s * 1000)))
            logger.info("airport %s ground-delayed for %.1fs", airport_id, cooldown_s)
        except Exception:
            logger.warning("ground-delay marker write failed for %s; the caller still "
                           "re-checks the marker each slice", airport_id, exc_info=True)

    async def remaining(self, airport_id: str) -> float | None:
        """Seconds until the stored resume time; None when there is no marker, it is
        unparseable, it has passed, or Redis errors."""
        try:
            raw = await self._redis.get(KEY_PREFIX + airport_id)
            if raw is None:
                return None
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode()
            remaining_s = (int(raw) - time.time() * 1000) / 1000.0
            return remaining_s if remaining_s > 0 else None
        except Exception:
            logger.warning("ground-delay remaining read failed for %s", airport_id, exc_info=True)
            return None
