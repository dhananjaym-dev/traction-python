# Traction Python tenant — design

**Date:** 2026-10-24
**Status:** approved for planning
**Companion repo:** [traction](https://github.com/dhananjaym-dev/traction) (the Java framework; its
`traction-core` sources are the binding wire contract)

## Intent

Traction's central claim is that the control plane and the data planes share nothing but a
message broker and a key-value store: no RPC, no shared database, no common runtime. This repo
proves that claim by implementing a complete Traction **data plane in Python** — consume,
clearance, settle, report — plus a small FastAPI demo tenant, governed end to end by the
**unmodified Java control plane** from the companion repo. One compose file runs both: the Java
dashboard shows a Python airport ground-stop, reopen and replay exactly as it does a Java one.

The repo is a public reference implementation (Apache-2.0), written to the same bar as the Java
repo: readable, tested, demo-able entirely inside Docker.

## Goals

1. A Python library (`traction` package) implementing the data-plane side of the public wire
   contract **exactly** — byte-compatible JSON envelopes, identical queue names and arguments,
   identical Redis key shapes and Lua, the same settle matrix and failure semantics.
2. A FastAPI demo tenant: one airport (`crm:py`) with two gates, landing flights against the
   companion repo's `acme-vendor` simulator.
3. A compose demo where the Java control plane governs the Python tenant through a full
   lifecycle: dispatch → land → AIRPORT_FAULT ground stop → reopen → gate fault park → replay.
   This scenario is the acceptance test, in CI.
4. Unit tests for every library module; a scenario script with assertions like the Java repo's.

## Non-goals

- **No control plane in Python.** Ground-stop rules, streak tracking, status writes, charter
  tallies, replay/discard, the dashboard — all stay in Java. The Python side only reads the
  status hash and the charter abort marker, and publishes reports.
- **No charter opening.** The data plane honors the charter abort skip-window (the per-flight
  marker read); opening charters and counting them is control-plane work the demo doesn't need.
- **No notify consumers.** Notifications are control-plane `NotifyHook` beans; there is no
  notify queue in the contract.
- **No PyPI packaging ceremony.** The library installs from the repo (`pip install -e .`); no
  publishing pipeline in v1.

## Anti-goals

- A wire shape that is "more Pythonic" than the contract. Where Java and Python idioms
  disagree, the wire wins: camelCase JSON fields, explicit nulls, Java-FQCN `__TypeId__`
  headers on tower messages.
- A demo that needs anything on the host beyond Docker. Compose publishes loopback-only ports
  and never 5672/6379/15672.

## The binding contract

The Java repo's `traction-core` is the source of truth. The port must match these, file by file:

| Contract surface | Java source of truth |
|---|---|
| Queue names `tr.<airportId>.runway/holding/parked`, `tr.tower.queue`, `tr.dlq` | `topology/QueueNames.java` |
| Queue arguments (quorum; runway DLX→parked limit 20; holding DLX→runway limit −1; parked limit −1; tower limit 20 DLX→dlq; dlq limit −1; declare order parked→holding→runway) | `topology/Topology.java` |
| Flight envelope JSON (flightId, airportId, gateId, domainRef, charterRef, attemptCount, holdCount, correlationId, payloadType, payload; plain default naming, nulls present, unknown fields ignored) | `api/Flight.java`, `config/TractionAutoConfiguration.wireObjectMapper` |
| Envelope validation and park-raw semantics (undeserializable or missing flightId / wrong airportId / blank gateId / blank payloadType → park the raw message, ack, no report) | `consume/FlightConsumer.envelopeProblem`, `parkRaw` |
| Consume-loop order: airport status → ground delay → gate status → charter abort → payload → clearance → land → report → settle | `consume/FlightConsumer.fly` |
| Settle matrix (LANDED/SKIPPED ack TERMINAL; FLIGHT_FAULT ack TERMINAL; GATE_FAULT park; AIRPORT_FAULT defer+pause; RATE_LIMIT delay+defer; TRANSIENT/INTERNAL hold-or-park) | `consume/SettleDecision.java` |
| Hold schedule and rounds (TRANSIENT 30s/2m/10m, INTERNAL 30s, override per airport; TTL = schedule[holdCount]; interim reason `HELD_<n>` with n = the count **after** the hold; park past budget) | `api/HoldPolicy.java`, `consume/HoldingPatternService.java` |
| Timing constants (defer backoff 500 ms; clearance sleep cap 10 s, wait budget 15 s; ground-delay slice 10 min, budget 25 min) | `consume/FlightConsumer` constants |
| FlightReportMessage JSON (RECORD/INTERIM, outcome, category, reason, evidence triple with clamps 64/128/1000, attemptCount, holdCount, correlationId, reportId, emittedAtEpochMillis, disposition RETRYING/PARKED/TERMINAL; reserved reasons GATE_GROUNDED, CHARTER_ABORTED, DEFERRED, HELD_ prefix) | `report/FlightReportMessage.java`, `report/FlightReportEmitter.java` |
| AirportAnnouncement JSON (announcementId, airportId, owningProcess, displayName, slotsLimit, slotsWindowSeconds, slotsBurst, gates:[{gateId, slotCost}], epochMillis) | `registry/AirportAnnouncement.java` |
| Tower-queue type dispatch: `__TypeId__` message header carrying the Java FQCN (`dev.traction.core.report.FlightReportMessage`, `dev.traction.core.registry.AirportAnnouncement`) | `controlplane/TowerMessageListener.converter` |
| Status reads: hash `tr:status:<airportId>` and `tr:status:<airportId>/<gateId>`, field `status`, values OPEN/GROUND_STOP/CLOSED, absent/unreadable ⇒ OPEN (fail open) | `status/StatusReader.java`, `status/ScopeRef.java` |
| Status events: Redis pub/sub channel `tr:events`, payload `STATUS_CHANGED|<scopeRef>|<status>|<reason or empty>`; airport scope pauses/resumes the consumer, resume re-reads the stored status; gate scope ignored | `status/Events.java`, `status/EventsListener.java`, `registry/AirportRegistry.resume` |
| Clearance: bucket hash `tr:slots:<airportId>` (capacity_milli, refill_per_sec_milli, tokens_milli, last_refill_ms), the Lua script **vendored byte-identical** from `traction/clearance.lua`, cost = gate slotCost × 1000, config reconcile writes only the two config fields, refill = round(limit × 1 000 000 / window_ms) min 1, **fail closed** (Redis error ⇒ defer, never fly unpaced) | `clearance/ClearanceService.java`, `resources/traction/clearance.lua` |
| Ground delay: key `tr:grounddelay:<airportId>`, value = resume epoch-millis (authoritative), TTL as backstop; absent/unparseable/error ⇒ not delayed | `consume/GroundDelayService.java` |
| Charter abort: existence of `tr:charter:<charterRef>:aborted` skips the flight (SKIPPED CHARTER_ABORTED, TERMINAL, before clearance); unreadable marker fails open | `charter/CharterKeys.java`, `charter/CharterBoard.isAborted`, `consume/FlightConsumer` step 3b |
| Payload step: a stamped `payloadType` this process cannot resolve → park, INTERNAL, evidence messageKey `PAYLOAD_TYPE_UNRESOLVED`; a payload that will not become the resolved type → reject, FLIGHT_FAULT, messageKey `payload-unreadable` | `consume/FlightConsumer.toPayload` + step 4 |
| Reporter guard: reporter returns null → INTERNAL `reporter-returned-null`; reporter throws → INTERNAL with the error's evidence | `consume/FlightConsumer.classify` |
| Dispatch: envelope minted with uuid4 flightId, 6-char correlationId, attempt/hold 0; plain (unconfirmed) publish to the runway; unknown airport/gate rejected before anything is queued | `api/Tower.java` |
| Registration: validate spec (ids match `^[a-zA-Z0-9:_\-.]+$`, slots/pilot/reporter required, gate slotCost ≤ burst), declare queues, declare tower, announce (confirmed), then consume | `registry/AirportRegistry.java` |
| One flight at a time per airport: prefetch 1, single sequential consumer, manual ack | `registry/ContainerFactory.java` |
| Evidence: code = exception class name, excerpt capped at 300 on creation, wire clamps on emit | `api/Evidence.java` |
| Structured failure log: one WARN per disposed flight (`flight-failure disposition=… airport=… gate=… category=… attempt=… holds=… domainRef=… charterRef=… evidence="…"`), ERROR only for framework defects; correlation + flight ids on every line | `consume/FlightConsumer.logFailure` |

Known deliberate differences from the Java implementation (behavioral surface only, never wire):

- **Pause is a consumer cancel, not a container stop.** asyncio has no listener container; pause
  cancels the AMQP consumer (the queue buffers), resume re-subscribes after re-reading the
  stored status. Same observable behavior.
- **No FlightEventHook.** It is an in-process extension point, not wire contract; v1 omits it.
- **Payload resolution is a registry, not a class loader.** `payloadType` is an opaque string
  on the wire; the Python side resolves it against the airport's declared payload types (see
  below) rather than importing code by name. Same park/reject semantics.

## Architecture

Python 3.12, asyncio throughout.

- **aio-pika ~9.x** for AMQP. Publisher-confirm channels give the confirmed-publish semantics
  `ConfirmedPublisher` requires; a plain channel backs `Tower.dispatch` and the raw parks.
- **redis-py (asyncio)** for the store: status hash reads, clearance Lua (`SCRIPT LOAD` +
  `EVALSHA` with NOSCRIPT reload), ground-delay marker, abort-marker existence, `tr:events`
  pub/sub.
- **pydantic v2** wire models with camelCase aliases; `to_wire()` emits by alias with explicit
  nulls so the JSON matches Jackson's record output.
- **FastAPI** for the demo tenant (lifespan starts/stops the runtime); **httpx** for the vendor
  client.

**pamqp signed-integer shim.** The AMQP encoder under aio-pika (pamqp ≤ 3.3.0) cannot marshal
negative field-table integers — `table_integer(-1)` routes through an unsigned `'B'` pack and
raises `struct.error` — so `x-delivery-limit: -1` cannot be declared as-is. The library ships a
small, tested shim that patches `pamqp.encode.table_integer` to encode negative int32 values as
AMQP type `'I'` + signed int32, exactly as the Java client does. RabbitMQ's declare-equivalence
check then accepts either side declaring the same queue. pamqp is pinned; a unit test asserts
the shim round-trips `-1` so an upgrade that changes the encoder fails loudly.

### Package layout

```
traction-python/
├── src/traction/
│   ├── __init__.py          # public API re-exports
│   ├── _amqp.py             # pamqp shim, ConfirmedPublisher, __TypeId__ constants
│   ├── models.py            # Flight, FlightReportMessage, AirportAnnouncement wire models
│   ├── spi.py               # Landed/Skipped/Incident, Evidence, categories, Pilot/FlightReporter
│   │                        #   protocols, HoldPolicy, AirportSpec (incl. payload-type registry)
│   ├── topology.py          # QueueNames, queue args, declare_airport / declare_tower
│   ├── clearance.py         # ClearanceService (fail closed) + vendored clearance.lua
│   ├── ground_delay.py      # GroundDelayService (value-authoritative marker)
│   ├── status.py            # StatusReader, ScopeRef, events parse + listener
│   ├── charters.py          # charter key helpers, is_aborted (fail open)
│   ├── report.py            # FlightReportEmitter (clamped wire rows, confirmed tower publish)
│   ├── consumer.py          # FlightConsumer: the per-flight loop and settle matrix
│   ├── tower.py             # Tower.dispatch + DispatchOptions
│   ├── registry.py          # AirportRegistry: validate → declare → announce → consume,
│   │                        #   pause/resume, tr:events subscription
│   └── py.typed
├── demo/
│   ├── app/                 # FastAPI tenant: airport crm:py, acme client+reporter, /seed API
│   ├── Dockerfile
│   └── scenario-python.sh   # the acceptance scenario (+ lib.sh adapted from the Java repo)
├── tests/                   # pytest + pytest-asyncio unit tests, golden wire fixtures
├── docker-compose.yml
├── .github/workflows/ci.yml
├── pyproject.toml
├── LICENSE                  # Apache-2.0
└── README.md
```

### The payload-type registry

`AirportSpec` declares the payload types the airport's pilot accepts:
`payload_types={"demo.Contact": Contact, "demo.Company": Company}` (pydantic models).
`Tower.dispatch` stamps `payloadType` with the registered name of the payload's class and
serializes the model into `payload`; the consumer resolves the stamped name against the same
registry. A name not in the registry parks the flight (INTERNAL, `PAYLOAD_TYPE_UNRESOLVED`) —
the renamed-class redeploy case; JSON that fails the model's validation rejects the flight
(FLIGHT_FAULT, `payload-unreadable`). Within one airport the dispatcher and consumer are the
same process, exactly as in the Java tenant, so names never need to resolve across languages.

### The demo tenant

One airport, mirroring the Java demo's CRM shape so the dashboard comparison is direct:

- `crm:py`, display name "Acme CRM (Python)", slots 20/s, gates `contact-sync` and
  `company-sync` (slot cost 1), landing on the companion repo's `acme-vendor` at
  `/contacts` and `/companies`.
- The reporter maps vendor failures the same way the Java demo does: 401 → AIRPORT_FAULT,
  422 → GATE_FAULT, 400 → FLIGHT_FAULT, 429 → RATE_LIMIT, 5xx/timeouts → TRANSIENT,
  anything else → INTERNAL.
- FastAPI endpoints: `POST /seed/contacts?count=&bad=`, `POST /seed/companies?count=`
  (dispatch through the Python Tower, `domainRef` = the record id), `GET /healthz`.

### The compose stack

Five services, same discipline as the Java repo (internal-only broker/store, loopback-only
publishes):

- `rabbitmq` (3.13-management, mgmt UI on 127.0.0.1:15674 to avoid colliding with the Java
  repo's 15673), `redis` (valkey 8) — ports **not** published.
- `acme-vendor` and `control-plane` — built **from the companion repo's GitHub URL as the
  Docker build context** (`https://github.com/dhananjaym-dev/traction.git#<pinned ref>`,
  dockerfiles `acme-vendor/Dockerfile` and `demo-app/Dockerfile`), so this repo vendors no Java
  and the two repos stay independently cloneable. The control plane is the Java demo-app
  unmodified: its dashboard at 127.0.0.1:8080/traction shows its own Java airports (idle) next
  to the Python airport (flying) — the cross-language picture is the point.
- `py-tenant` — this repo's demo app on 127.0.0.1:8000.

`demo/scenario-python.sh` drives the acceptance flow with assertions, in the Java repo's
scenario style: seed N contacts healthy and watch them land paced; flip the vendor to
token_expired, seed N more, assert the **Java** control plane ground-stops `crm:py`, the runway
holds N, exactly one GROUND_STOP notification; heal + one reopen curl, assert all land, parked
and dlq empty; then flip companies to contract_break, seed companies, assert they park
(GATE_FAULT); heal, replay via the Java admin API, assert they land and the parked queue drains.

### CI

GitHub Actions, mirroring the Java repo's workflow: least-privilege permissions; job 1 ruff +
pytest (Python 3.12); job 2 `docker compose up -d --build --wait`, run
`demo/scenario-python.sh`, grep its final `OK` line, dump compose logs on failure, always tear
down with `-v`.

## Decisions already made (and by whom)

| Decision | Choice | Whose |
|---|---|---|
| Repo scope: data plane + demo tenant only | yes | owner (brief) |
| Wire contract source of truth = Java `traction-core` | yes | owner (brief) |
| Everything demo-able inside Docker, loopback-only publishes | yes | owner (brief) |
| License, repo home | Apache-2.0, github.com/dhananjaym-dev | owner (brief) |
| Repo name | `traction-python` | builder's call |
| Import package name | `traction` | builder's call |
| Async stack: aio-pika + redis-py asyncio + pydantic v2 + FastAPI + httpx | yes | builder's call |
| Python version floor | 3.12 | builder's call |
| Demo airport `crm:py` mirroring the Java CRM demo shape | yes | builder's call |
| Control plane in compose = Java demo-app built from the GitHub repo URL, pinned ref | yes | builder's call |
| pamqp shim for negative field-table ints (vs. deviating from the queue-args contract) | shim | builder's call |
| Payload-type registry (vs. import-by-name) | registry | builder's call |
| Sequential consumer, prefetch 1 (Java parity) vs. concurrent | sequential | builder's call |
| Omit FlightEventHook, notify consumers, charter opening | yes | builder's call |

## Left to the builder

- Exact pydantic model/field spellings, module-internal structure, test organization.
- The golden wire fixtures (hand-checked against the Java record definitions).
- Scenario script timing/assertion details; compose healthchecks.
- README structure (follow the Java repo's register: problem → quickstart → scenarios). Note in
  it that this compose and the Java repo's compose share host ports 8080/9090 and so don't run
  side by side.
- Which companion-repo ref to pin for the compose build (a commit SHA at build time; bump
  deliberately).

## Acceptance

1. `pytest` green; `ruff check` clean.
2. `docker compose up -d --build --wait && demo/scenario-python.sh` ends `OK scenario-python`
   on a clean checkout with nothing on host 5672/6379/15672 touched.
3. `grep -riE 'personatech|phoenix|PTI-|hangar|terminal(Id)?|ADVISORY|DIVERTED' .` returns
   nothing outside `docs/superpowers/` (process documents, exempt as in the Java repo), with
   one in-code exemption: the case-sensitive disposition token TERMINAL.
4. CI green on GitHub.
