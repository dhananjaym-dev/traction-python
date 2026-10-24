"""The data plane's only charter concern: whether a charter's abort skip-window is live.
Charter opening, counting and completion are control-plane work. The read fails open: an
unreadable marker must never skip a flight."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

CHARTER_KEY_PREFIX = "tr:charter:"


def aborted_key(charter_ref: str) -> str:
    return f"{CHARTER_KEY_PREFIX}{charter_ref}:aborted"


class CharterAbortReader:
    def __init__(self, redis):
        self._redis = redis

    async def is_aborted(self, charter_ref: str) -> bool:
        try:
            return bool(await self._redis.exists(aborted_key(charter_ref)))
        except Exception:
            logger.warning("charter %s abort marker unreadable; treating it as not aborted",
                           charter_ref, exc_info=True)
            return False
