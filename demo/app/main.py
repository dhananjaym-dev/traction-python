"""The demo tenant: a FastAPI app whose lifespan runs one Traction airport. Seeding dispatches
synthetic flights through the Python Tower; everything else — pacing, holds, parks, ground
stops, replay — is the framework's and the Java control plane's business."""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from itertools import count

import aio_pika
import redis.asyncio as redis_asyncio
from fastapi import FastAPI, HTTPException

from demo.app.acme import AcmeClient
from demo.app.airport import AIRPORT, COMPANY_SYNC, CONTACT_SYNC, Company, Contact, crm_py_spec
from traction import AirportRegistry, DispatchOptions, Tower

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

AMQP_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
ACME_BASE_URL = os.environ.get("ACME_BASE_URL", "http://acme-vendor:9090")

_sequence = count(1)

logger = logging.getLogger(__name__)


async def connect_with_retry(url: str, *, connect=aio_pika.connect_robust,
                             attempts: int = 30, delay_s: float = 2.0):
    """The broker's healthcheck can pass before its listener accepts connections, and
    connect_robust raises on a failed FIRST connect — so the tenant waits for the broker
    instead of dying at startup."""
    for attempt in range(1, attempts + 1):
        try:
            return await connect(url)
        except Exception:
            if attempt == attempts:
                raise
            logger.info("broker not ready (attempt %d/%d); retrying in %.0fs",
                        attempt, attempts, delay_s)
            await asyncio.sleep(delay_s)


@asynccontextmanager
async def lifespan(app: FastAPI):
    connection = await connect_with_retry(AMQP_URL)
    store = redis_asyncio.from_url(REDIS_URL, decode_responses=True)
    acme = AcmeClient(ACME_BASE_URL)
    registry = AirportRegistry(connection=connection, redis=store, owning_process="py-tenant")
    await registry.register_all([crm_py_spec(acme)])
    registry.start_events()
    app.state.tower = Tower(registry, await connection.channel(publisher_confirms=False))
    try:
        yield
    finally:
        await registry.stop()
        await acme.aclose()
        await store.aclose()
        await connection.close()


app = FastAPI(title="Traction Python demo tenant", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.post("/seed/contacts")
async def seed_contacts(count: int, bad: int = 0):
    """``count`` contact-sync flights; the first ``bad`` carry bad=true."""
    _require_counts(count, bad)
    for i in range(count):
        record_id = f"contact-{next(_sequence)}"
        await app.state.tower.dispatch(
            AIRPORT, CONTACT_SYNC,
            Contact(id=record_id, email=f"{record_id}@example.com", bad=i < bad),
            DispatchOptions(domain_ref=record_id),
        )
    return {"dispatched": count, "bad": min(bad, count)}


@app.post("/seed/companies")
async def seed_companies(count: int):
    """``count`` company-sync flights."""
    _require_counts(count, 0)
    for _ in range(count):
        record_id = f"company-{next(_sequence)}"
        await app.state.tower.dispatch(
            AIRPORT, COMPANY_SYNC,
            Company(id=record_id, domain=f"{record_id}.example.com"),
            DispatchOptions(domain_ref=record_id),
        )
    return {"dispatched": count}


def _require_counts(count: int, bad: int) -> None:
    if count < 0 or bad < 0:
        raise HTTPException(status_code=400, detail="count and bad must not be negative")
