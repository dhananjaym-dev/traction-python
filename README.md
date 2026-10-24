# traction-python

A **Python data plane** for [Traction](https://github.com/dhananjaym-dev/traction), the
containment framework for third-party integrations. This repo exists to prove Traction's central
claim: the control plane and the data planes share **nothing but a message broker and a
key-value store** — no RPC, no shared database, no common runtime, and, as of this repo, not
even a common language.

One compose file runs the **unmodified Java control plane** from the companion repo next to a
Python tenant built on this library. The Java dashboard ground-stops the Python airport on its
first `AIRPORT_FAULT` report, one curl reopens it, parked flights replay from the same admin
API — the whole lifecycle, across the language boundary, through nothing but RabbitMQ and Redis.

```
┌──────────────────────┐   tr.tower.queue (reports, announcements)   ┌──────────────────────┐
│  Java control plane   │ ◄─────────────────────────────────────────  │  Python tenant        │
│  (companion repo,     │                                             │  (this repo:          │
│   unmodified)         │   tr:status:* · tr:events · tr:slots:*      │   traction library +  │
│  dashboard + rules    │ ──────────────────────────────────────────► │   FastAPI demo)       │
└──────────────────────┘        RabbitMQ + Redis, nothing else        └──────────────────────┘
```

## Quickstart

You need Docker, bash, curl and jq. Nothing else: the Java images build from the companion
repo's GitHub URL inside Docker, and the broker/store are internal to the compose network
(nothing is published on host 5672/6379/15672; HTTP ports bind loopback only).

```bash
docker compose up -d --build --wait
./demo/scenario-python.sh
```

The scenario drives, with assertions, exactly the story above: a healthy batch lands paced at
the airport's shared 20/s slot bucket; an expired vendor token ground-stops the airport from the
Java side after **one** failed flight (199 of 200 never burn a vendor call); one curl reopens it
and everything lands; a broken contract on one gate parks that gate's flights while the rest of
the airport keeps flying; an operator replay lands them after the fix. It ends with
`OK scenario-python`. The dashboard at <http://localhost:8080/traction> shows the Python airport
(`crm:py`) beside the control plane's own Java airports while it runs.

`docker compose down -v` removes everything. (This compose shares host ports 8080/9090 with the
companion repo's own demo, so run one of the two at a time.)

## The library

`src/traction/` is the data-plane side of the wire contract: consume, clearance, settle, report.
A tenant declares an airport and flies:

```python
from traction import AirportRegistry, AirportSpec, SlotsConfig, Tower
from traction import Landed, Incident, IncidentCategory, Evidence

spec = AirportSpec(
    airport_id="crm:py",
    display_name="Acme CRM (Python)",
    slots=SlotsConfig.of(20, 1.0),              # 20 flights per second, shared across processes
    gates={"contact-sync": 1, "company-sync": 1},
    pilot=MyPilot(),                            # async def land(flight): one vendor call
    reporter=MyReporter(),                      # def report(flight, error): classify the outcome
    payload_types={"demo.Contact": Contact},    # wire name -> pydantic model
)

registry = AirportRegistry(connection=amqp, redis=store, owning_process="py-tenant")
await registry.register_all([spec])             # declare queues, announce, consume
registry.start_events()                         # REQUIRED: without it, a paused airport
                                                #   (ground stop, AIRPORT_FAULT) never resumes

tower = Tower(registry, plain_channel)
await tower.dispatch("crm:py", "contact-sync", Contact(id="c-1", email="c-1@example.com"))
```

The pilot does the work; the reporter names what a failure *is* (`AIRPORT_FAULT`, `GATE_FAULT`,
`FLIGHT_FAULT`, `TRANSIENT`, `RATE_LIMIT`, `INTERNAL`); the framework does everything else —
pacing against the shared Redis token bucket, deferrals that never burn the quorum queue's
delivery limit, hold-with-backoff through the broker's TTL dead-lettering, parking, reporting
every outcome to the tower queue, and pausing/resuming on the control plane's say-so.

## The wire contract

The companion repo's `traction-core` is the source of truth; this library emits JSON that is
field-for-field identical to Jackson's (same names, explicit nulls — whitespace aside) and
matches every queue argument, key shape and constant exactly. The highlights:

| Surface | Shape |
|---|---|
| Queues | `tr.<airportId>.runway` / `.holding` / `.parked`, `tr.tower.queue`, `tr.dlq` — quorum, with the exact delivery-limit and dead-letter arguments of the Java `Topology` |
| Flight envelope | camelCase JSON with explicit nulls (`flightId`, `airportId`, `gateId`, `domainRef`, `charterRef`, `attemptCount`, `holdCount`, `correlationId`, `payloadType`, `payload`) |
| Reports | `FlightReportMessage` rows to `tr.tower.queue`: `RECORD` (settled, counted) vs `INTERIM` (`HELD_<n>`, `DEFERRED`), evidence clamped to 64/128/1000 |
| Type dispatch | the `__TypeId__` AMQP header carries the **Java** class name (`dev.traction.core.report.FlightReportMessage`, …) — the one place the wire names a class |
| Status | hash `tr:status:<scope>`, field `status`; pub/sub `tr:events` (`STATUS_CHANGED\|scope\|status\|reason`); reads fail open |
| Clearance | `tr:slots:<airportId>` token bucket; `clearance.lua` vendored **byte-identical** from the Java repo; fails closed (Redis down ⇒ defer, never fly unpaced) |
| Ground delay | `tr:grounddelay:<airportId>`, value = resume epoch-millis, TTL as backstop |
| Charter abort | existence of `tr:charter:<ref>:aborted` skips the flight before clearance; read fails open |

One cross-language wrinkle worth knowing: aio-pika's AMQP encoder (pamqp ≤ 3.3.0) cannot marshal
the negative field-table integers the queue contract requires (`x-delivery-limit: -1`), so
`traction._amqp` ships a small tested shim that encodes them exactly as the Java client does
(type `I`, signed int32). Without it, "cross-language" fails not at JSON but at a field-table
type byte.

## Development

```bash
python3.12 -m venv .venv && .venv/bin/pip install -e '.[demo]' pytest pytest-asyncio ruff
.venv/bin/pytest          # unit tests, no broker or store needed
.venv/bin/ruff check src tests demo
```

The scenario is the acceptance test and runs in CI against the pinned companion-repo commit in
`docker-compose.yml`; bump that pin deliberately.

## License

Apache-2.0, same as the companion repo.
