# Traction Python Tenant Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Python data-plane library + FastAPI demo tenant, wire-exact against the Java
Traction contract, governed end to end by the unmodified Java control plane in one compose stack.

**Architecture:** asyncio library (`src/traction/`) mirroring `traction-core`'s data-plane
modules one-for-one; a demo tenant (`demo/`) registering one airport `crm:py` against the
companion repo's acme-vendor; compose runs the Java demo-app (control plane) + acme-vendor built
from the companion repo's GitHub URL.

**Tech Stack:** Python 3.12, aio-pika ~9.6 (pamqp 3.3 + signed-int shim), redis-py asyncio,
pydantic v2, FastAPI + httpx + uvicorn, pytest + pytest-asyncio, ruff.

**Spec:** `docs/superpowers/specs/2026-10-24-traction-python-design.md` — its "binding
contract" table names the Java file that pins every value; the executor reads the named Java
file whenever a task references one. Java sources live at
`/Users/personatech/Desktop/Projects/traction/traction-core/src/main/java/dev/traction/core/`
(referenced below as `core/`).

## Global Constraints

- Wire names are the contract: queues `tr.<airportId>.runway|holding|parked`, `tr.tower.queue`,
  `tr.dlq`; Redis keys `tr:slots:`, `tr:status:`, `tr:grounddelay:`, `tr:charter:`, channel
  `tr:events`; JSON fields camelCase with explicit nulls; `__TypeId__` header values
  `dev.traction.core.api.Flight`, `dev.traction.core.report.FlightReportMessage`,
  `dev.traction.core.registry.AirportAnnouncement`.
- Public vocabulary only. `grep -riE 'personatech|phoenix|PTI-|hangar|terminal(Id)?|ADVISORY|DIVERTED' .`
  must return nothing outside `docs/superpowers/`, except the case-sensitive token TERMINAL.
- Timing constants (seconds): defer backoff 0.5; clearance sleep cap 10, wait budget 15;
  ground-delay slice 600, budget 1500; hold defaults TRANSIENT (30, 120, 600), INTERNAL (30,);
  broker-confirm timeout 5.
- Clearance fails CLOSED (Redis error ⇒ defer). Status, ground-delay and abort-marker reads fail
  OPEN.
- Every commit by repo-local identity `dhananjaym-dev <14110654+dhananjaym-dev@users.noreply.github.com>`;
  no attribution lines in commit messages.
- Nothing in compose publishes host ports 5672, 6379 or 15672; published ports bind 127.0.0.1.
- Comment style follows the Java repo: explain the invariant the code can't show; no narration.

## Review Focus

Failure modes the spec implies that need a pinned test (each is added to its owning task below):

1. A tower report publish that the broker does not confirm after the pilot landed must park the
   flight via the catch-all, never ack-without-record or double-settle → Task 9 (`test_report_publish_failure_parks_after_land`).
2. A stale `tr:events` OPEN rebroadcast while the stored status is GROUND_STOP must not resume
   the consumer → Task 10 (`test_resume_rechecks_stored_status`).
3. The clearance bucket hash vanishing between reconcile and eval (Redis flush) must defer the
   flight and re-write config on the next acquire, never grant → Task 5 (`test_unconfigured_after_reconcile_fails_closed`).
4. A holding publish must carry per-message expiration in **milliseconds** on the wire (e.g.
   `"30000"` for the first TRANSIENT round), or the broker never dead-letters it back → Task 9 (`test_hold_publish_expiration_ms`).
5. A flight whose `payload` is JSON null must land with payload `None` (Java passes null through),
   not reject → Task 9 (`test_null_payload_lands`).

---

### Task 1: Project scaffold

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `LICENSE` (Apache-2.0, copied verbatim from the Java
  repo's LICENSE), `src/traction/__init__.py`, `src/traction/py.typed`, `tests/__init__.py`

**Interfaces:**
- Produces: installable package `traction`; `pytest` and `ruff check` runnable.

- [ ] **Step 1: Write `pyproject.toml`**: project name `traction-python`, version 0.1.0,
  `requires-python = ">=3.12"`, packages from `src/`; dependencies `aio-pika~=9.6`,
  `redis~=5.3`, `pydantic~=2.9`; optional group `demo`: `fastapi`, `httpx`, `uvicorn[standard]`;
  dev group: `pytest`, `pytest-asyncio`, `ruff`. Tool config: ruff line-length 100;
  pytest `asyncio_mode = "auto"`, testpaths `tests`.
- [ ] **Step 2: Create the venv and install** (`uv venv .venv && uv pip install -e '.[demo]' --group dev`
  or the pip equivalent). Run: `.venv/bin/pytest --collect-only` → collects 0 tests, exits 0 (add a
  trivial `tests/test_sanity.py` asserting `import traction` works).
- [ ] **Step 3: Run `ruff check src tests`** → clean.
- [ ] **Step 4: Commit** `chore: project scaffold`

### Task 2: AMQP layer — pamqp shim, message builder, confirmed publisher

**Files:**
- Create: `src/traction/_amqp.py`
- Test: `tests/test_amqp.py`

**Interfaces:**
- Produces:
  - `TYPE_ID_HEADER = "__TypeId__"`; `TYPE_ID_FLIGHT`, `TYPE_ID_REPORT`, `TYPE_ID_ANNOUNCEMENT`
    (the three Java FQCNs from Global Constraints).
  - `json_message(body: dict, type_id: str, expiration_ms: int | None = None) -> aio_pika.Message`
    — UTF-8 JSON body, `content_type="application/json"`, persistent delivery, `__TypeId__`
    header, expiration converted to aio-pika's unit so the wire property is `str(expiration_ms)`.
  - `class PublishException(Exception)`.
  - `class ConfirmedPublisher:` ctor takes an `aio_pika.abc.AbstractChannel` (publisher confirms
    on); `async def publish(self, queue: str, body: dict, type_id: str, expiration_ms: int | None = None) -> None`
    — publish to the default exchange, routing key = queue, 5 s confirm timeout; any nack,
    timeout or AMQP error raises `PublishException`.
  - Importing `traction._amqp` applies the pamqp shim (idempotent module-level call
    `_apply_pamqp_shim()`); negative int32 field-table values encode as `b'I' + signed int32`.

- [ ] **Step 1: Write failing tests**: `test_shim_encodes_negative_delivery_limit`
  (`pamqp.encode.field_table({"x-delivery-limit": -1})` contains `b"x-delivery-limitI\xff\xff\xff\xff"`),
  `test_shim_leaves_positive_ints_alone` (round-trips 20 via `pamqp.decode.field_table`),
  `test_json_message_shape` (body JSON, content_type, header, delivery mode persistent),
  `test_json_message_expiration_ms` (wire expiration property == `"30000"` for
  `expiration_ms=30_000`), `test_confirmed_publisher_raises_on_failure` (fake channel whose
  exchange.publish raises → `PublishException`).
- [ ] **Step 2: Run tests** → FAIL (module missing).
- [ ] **Step 3: Implement `_amqp.py`**. The shim wraps `pamqp.encode.table_integer`: values in
  `[-2**31, 0)` → `b'I' + pamqp.encode.long_int(value)`, else delegate to the original.
- [ ] **Step 4: Run tests** → PASS. `ruff check` clean.
- [ ] **Step 5: Commit** `feat: amqp layer with signed-int shim and confirmed publisher`

### Task 3: Wire models

**Files:**
- Create: `src/traction/models.py`
- Test: `tests/test_models.py`

**Interfaces:**
- Produces (all pydantic v2, `populate_by_name=True`, `extra="ignore"`, camelCase aliases,
  `to_wire() -> dict` dumping by alias, JSON mode, **nulls included**):
  - `class Flight`: `flight_id: UUID | None` (alias flightId), `airport_id: str | None`,
    `gate_id: str | None`, `domain_ref: str | None`, `charter_ref: str | None`,
    `attempt_count: int = 0`, `hold_count: int = 0`, `correlation_id: str | None`,
    `payload_type: str | None`, `payload: Any = None`. Methods `with_incremented_attempt() -> Flight`,
    `with_incremented_hold() -> Flight` (copies, exactly like `core/api/Flight.java`).
  - `class Disposition(StrEnum)`: RETRYING, PARKED, TERMINAL.
  - `class RecordType(StrEnum)`: RECORD, INTERIM.
  - `class FlightReportMessage`: the 18 fields of `core/report/FlightReportMessage.java`,
    same aliases; `with_clamped_evidence() -> FlightReportMessage` using
    `EVIDENCE_MAX_CODE_LENGTH=64`, `EVIDENCE_MAX_MESSAGE_KEY_LENGTH=128`,
    `EVIDENCE_MAX_EXCERPT_LENGTH=1000`; constants `REASON_GATE_GROUNDED = "GATE_GROUNDED"`,
    `REASON_CHARTER_ABORTED = "CHARTER_ABORTED"`.
  - `class GateAnnouncement`: `gate_id` (gateId), `slot_cost: int` (slotCost).
  - `class AirportAnnouncement`: announcementId, airportId, owningProcess, displayName,
    slotsLimit (int|None), slotsWindowSeconds (int|None), slotsBurst (int|None),
    `gates: list[GateAnnouncement]`, epochMillis — exactly `core/registry/AirportAnnouncement.java`.

- [ ] **Step 1: Write failing tests** incl. golden fixtures:
  `test_flight_round_trip_wire_json` (parse the exact JSON a Java `Tower.dispatch` writes —
  handwrite it from `Flight.java` — and re-emit byte-equal key set with nulls),
  `test_flight_ignores_unknown_fields`, `test_increment_helpers`,
  `test_report_wire_shape` (all 18 keys present, camelCase, nulls explicit),
  `test_evidence_clamp` (65/129/1001-char inputs truncate to 64/128/1000),
  `test_announcement_wire_shape` (gates `[{"gateId": "g", "slotCost": 2}]`).
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS, ruff clean.
- [ ] **Step 5: Commit** `feat: wire models with golden fixtures`

### Task 4: Tenant SPI — outcomes, evidence, policies, spec

**Files:**
- Create: `src/traction/spi.py`
- Test: `tests/test_spi.py`

**Interfaces:**
- Produces:
  - `class IncidentCategory(StrEnum)`: AIRPORT_FAULT, GATE_FAULT, FLIGHT_FAULT, TRANSIENT,
    RATE_LIMIT, INTERNAL.
  - `@dataclass(frozen=True) Evidence(code: str | None, message_key: str | None, excerpt: str | None)`;
    `MAX_EXCERPT = 300`; `@staticmethod from_exception(e: BaseException | None) -> Evidence | None`
    (code = class name, excerpt = str(e)[:300] or None); `display() -> str | None` (non-blank
    parts joined `" | "`).
  - Outcomes: `@dataclass(frozen=True) Landed`; `Skipped(reason: str)` raising `ValueError` on
    blank; `Incident(category: IncidentCategory, evidence: Evidence | None)`;
    `FlightReport = Landed | Skipped | Incident`.
  - `class Pilot(Protocol)`: `async def land(self, flight: Flight) -> None` (payload already
    resolved; errors are raised).
  - `class FlightReporter(Protocol)`: `def report(self, flight: Flight, error: BaseException | None) -> FlightReport`.
  - `@dataclass(frozen=True) HoldPolicy(schedule: tuple[float, ...])`; `TRANSIENT_DEFAULT`,
    `INTERNAL_DEFAULT`, `NEVER`; `@staticmethod default_for(category) -> HoldPolicy`.
  - `@dataclass(frozen=True) SlotsConfig(limit: int, window_s: float, burst: int)` with
    `of(limit, window_s)` defaulting burst=limit; validation per `core/api/SlotsConfig.java`.
  - `ID_PATTERN = re.compile(r"^[a-zA-Z0-9:_\-.]+$")`.
  - `@dataclass AirportSpec(airport_id, display_name, slots: SlotsConfig, gates: dict[str, int],
    pilot: Pilot, reporter: FlightReporter, payload_types: dict[str, type[pydantic.BaseModel]],
    hold_policy: HoldPolicy | None = None)`; `validate()` raising `ValueError` on bad ids,
    missing parts, slot cost < 1 or > burst; `hold_policy_for(category) -> HoldPolicy`
    (override applies to TRANSIENT/INTERNAL only, per `core/api/AirportSpec.java`);
    `payload_type_name(payload) -> str` (reverse lookup by exact class, `ValueError` if absent);
    `slot_cost(gate_id) -> int` (1 when gate unknown/None).

- [ ] **Step 1: Write failing tests**: category/hold defaults (`default_for(TRANSIENT).schedule == (30, 120, 600)`,
  INTERNAL `(30,)`, others `()`), evidence from_exception + display, Skipped blank rejection,
  spec validation (bad gate id, cost > burst, missing pilot), hold override applies only to
  TRANSIENT/INTERNAL, payload_type_name reverse lookup, slot_cost fallback 1.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS, ruff clean.
- [ ] **Step 5: Commit** `feat: tenant SPI`

### Task 5: Clearance (fail closed) + vendored Lua

**Files:**
- Create: `src/traction/clearance.py`, `src/traction/clearance.lua` (byte-identical copy of the
  Java repo's `traction-core/src/main/resources/traction/clearance.lua`; provenance noted in
  clearance.py's docstring, never inside the .lua)
- Test: `tests/test_clearance.py`

**Interfaces:**
- Produces:
  - `KEY_PREFIX = "tr:slots:"`, `MILLI_PER_SLOT = 1000`.
  - `class ClearanceUnavailableError(Exception)`.
  - `@dataclass(frozen=True) ClearanceResult(granted: bool, wait_ms: int)`.
  - `class ClearanceService:` ctor `(redis)`;
    `async def acquire(self, airport_id: str, slots: SlotsConfig, cost: int) -> ClearanceResult`.
    Semantics per `core/clearance/ClearanceService.java`: `ValueError` when cost < 1 or > burst;
    reconcile first (below); one `EVALSHA` (`SCRIPT LOAD` lazily, reload on NOSCRIPT) with
    `cost * 1000` as ARGV; script `-1` → drop the airport from `_applied`, raise
    `ClearanceUnavailableError`; any Redis error → `ClearanceUnavailableError`. Reconcile: skip
    when `_applied[airport_id] == slots`; else HSET only `capacity_milli = burst * 1000` and
    `refill_per_sec_milli = max(1, round(limit * 1_000_000 / (window_s * 1000)))`, then record.

- [ ] **Step 1: Copy the Lua** and add `test_lua_is_vendored_verbatim` asserting
  `sha256 == <hash computed from the Java repo's file at plan time>` (compute and inline the hex).
- [ ] **Step 2: Write failing tests** (FakeRedis in `tests/fakes.py`: dict-backed `hset/hget`,
  scripted `script_load/evalsha` returning queued results): grant path deducts via queued
  `[1, 0]`; denial returns wait; cost bounds raise ValueError; reconcile writes exactly the two
  config fields once and rewrites on changed slots; refill formula `limit=20, window_s=1` →
  `20000`, `limit=1, window_s=3600` → `278`; `test_unconfigured_after_reconcile_fails_closed`
  (evalsha returns `[-1, 0]` → raises, next acquire re-writes config); Redis error raises.
- [ ] **Step 3: Run** → FAIL. **Step 4: Implement.** **Step 5: Run** → PASS, ruff clean.
- [ ] **Step 6: Commit** `feat: clearance service with vendored lua, fail closed`

### Task 6: Store reads — status, ground delay, charter abort

**Files:**
- Create: `src/traction/status.py`, `src/traction/ground_delay.py`, `src/traction/charters.py`
- Test: `tests/test_status.py`, `tests/test_ground_delay.py`, `tests/test_charters.py`

**Interfaces:**
- Produces:
  - `status.py`: `class AirportStatus(StrEnum)` OPEN/GROUND_STOP/CLOSED with property
    `allows_consumption` (True only for OPEN); `STATUS_KEY_PREFIX = "tr:status:"`,
    `STATUS_FIELD = "status"`; `def scope_airport(airport_id) -> str`;
    `def scope_gate(airport_id, gate_id) -> str` (validates gate against `ID_PATTERN`, joins
    `/`); `class StatusReader:` ctor `(redis)`, `async def get(self, scope: str) -> AirportStatus`
    — HGET, absent/unknown/error ⇒ OPEN; `EVENTS_CHANNEL = "tr:events"`;
    `@dataclass StatusChanged(scope: str, status: AirportStatus, reason: str | None)`;
    `def parse_status_changed(payload: str) -> StatusChanged | None` per `core/status/Events.java`
    (split on `|`, ≥3 parts, verb `STATUS_CHANGED`, non-empty scope, known status).
  - `ground_delay.py`: `KEY_PREFIX = "tr:grounddelay:"`; `class GroundDelayService:` ctor
    `(redis)`; `async def delay(self, airport_id: str, cooldown_s: float) -> None` — SET value
    `str(now_ms + cooldown_s*1000)` with `px=int(cooldown_s*1000)` (min 1 ms), best-effort
    (errors logged); `async def remaining(self, airport_id: str) -> float | None` — seconds from
    the stored epoch; None when absent/unparseable/≤0/error.
  - `charters.py`: `CHARTER_KEY_PREFIX = "tr:charter:"`; `def aborted_key(charter_ref) -> str`
    (`tr:charter:<ref>:aborted`); `class CharterAbortReader:` ctor `(redis)`,
    `async def is_aborted(self, charter_ref: str) -> bool` — EXISTS, fail open (error ⇒ False).

- [ ] **Step 1: Write failing tests**: status absent/OPEN/GROUND_STOP/garbage/error paths; gate
  scope composition + bad gate id raises; event parse good/short/unknown-status/empty-reason→None;
  ground delay write shape (value + px) and remaining (absent, stored-future, stored-past,
  garbage, error); abort reader exists/missing/error-fails-open.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS, ruff clean.
- [ ] **Step 5: Commit** `feat: status, ground-delay and charter-abort store reads`

### Task 7: Settle matrix

**Files:**
- Create: `src/traction/settle.py`
- Test: `tests/test_settle.py`

**Interfaces:**
- Produces (pure, no I/O — the port of `core/consume/SettleDecision.java`):
  - `class Route(Enum)`: ACK, PARK, HOLD_OR_PARK, DEFER_AND_PAUSE, DELAY_AND_DEFER.
  - `REASON_DEFERRED = "DEFERRED"`, `REASON_HELD_PREFIX = "HELD_"`.
  - `class HoldOutcome(Enum)`: HELD, PARKED.
  - `@dataclass(frozen=True) SettleDecision(route: Route, record_type: RecordType | None,
    disposition: Disposition | None, reason: str | None)`;
    `@staticmethod of(report: FlightReport) -> SettleDecision`;
    `@staticmethod after_holding(outcome: HoldOutcome, held_count: int) -> SettleDecision`.

- [ ] **Step 1: Write failing tests** — the whole matrix: Landed/Skipped/FLIGHT_FAULT →
  (ACK, RECORD, TERMINAL, None); GATE_FAULT → (PARK, RECORD, PARKED, None); AIRPORT_FAULT →
  (DEFER_AND_PAUSE, INTERIM, RETRYING, DEFERRED); RATE_LIMIT → (DELAY_AND_DEFER, INTERIM,
  RETRYING, DEFERRED); TRANSIENT/INTERNAL → (HOLD_OR_PARK, None, None, None);
  `after_holding(HELD, 1)` → (HOLD_OR_PARK, INTERIM, RETRYING, "HELD_1");
  `after_holding(PARKED, _)` → (PARK, RECORD, PARKED, None).
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS, ruff clean.
- [ ] **Step 5: Commit** `feat: settle matrix`

### Task 8: Report emitter

**Files:**
- Create: `src/traction/report.py`
- Test: `tests/test_report.py`

**Interfaces:**
- Consumes: `ConfirmedPublisher`, models, spi, `TYPE_ID_REPORT`, `TOWER` queue name
  (Task 9's topology provides `TOWER`; to keep this task independent define
  `TOWER_QUEUE = "tr.tower.queue"` in `src/traction/topology.py` **now** if Task 9 hasn't run —
  see Task 9 Interfaces; the name must come from topology, not be duplicated).
- Produces:
  - `class FlightReportEmitter:` ctor `(publisher: ConfirmedPublisher)`;
    `async def emit_record(self, flight: Flight, report: FlightReport, disposition: Disposition, reason: str | None = None) -> None`
    (a Skipped report's own reason wins when `reason` is None, per
    `core/report/FlightReportEmitter.java`);
    `async def emit_interim(self, flight: Flight, incident: Incident, reason: str) -> None`
    (RETRYING). Both build `FlightReportMessage` (uuid4 reportId, `int(time.time()*1000)`,
    clamped evidence) and publish to the tower queue with `TYPE_ID_REPORT`. A publish failure
    propagates.

- [ ] **Step 1: Write failing tests** (FakePublisher capturing (queue, body, type_id)):
  landed record row (outcome LANDED, category/reason/evidence null, disposition TERMINAL);
  skipped row carries its reason; incident record row carries category + evidence triple;
  interim row (INTERIM, RETRYING, reason HELD_2); clamping applied; publish failure propagates;
  type id + queue correct.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS, ruff clean.
- [ ] **Step 5: Commit** `feat: flight report emitter`

### Task 9: Topology + FlightConsumer

**Files:**
- Create: `src/traction/topology.py`, `src/traction/consumer.py`
- Test: `tests/test_topology.py`, `tests/test_consumer.py`, fakes extended in `tests/fakes.py`

**Interfaces:**
- Consumes: everything from Tasks 2–8.
- Produces:
  - `topology.py`: `@dataclass(frozen=True) QueueNames(airport_id, runway, holding, parked)`;
    `def queue_names(airport_id) -> QueueNames`; `TOWER_QUEUE = "tr.tower.queue"`,
    `DLQ = "tr.dlq"`; `async def declare_airport(channel, airport_id) -> QueueNames`;
    `async def declare_tower(channel) -> None`. Queue arguments exactly `core/topology/Topology.java`:
    all quorum; runway limit 20 DLX→parked; holding limit −1 DLX→runway; parked limit −1 no DLX;
    tower limit 20 DLX→`tr.dlq`; dlq limit −1; declare order parked→holding→runway and
    dlq→tower; all durable.
  - `consumer.py`: constants `DEFER_BACKOFF_S = 0.5`, `MAX_CLEARANCE_SLEEP_S = 10.0`,
    `MAX_CLEARANCE_WAIT_S = 15.0`, `MAX_DELAY_SLEEP_S = 600.0`, `MAX_TOTAL_DELAY_S = 1500.0`,
    `PAYLOAD_TYPE_UNRESOLVED = "PAYLOAD_TYPE_UNRESOLVED"`;
    `def delay_slice_s(remain_s: float, slept_s: float) -> float` (pure, mirrors
    `FlightConsumer.delaySliceMs`);
    `class FlightConsumer:` ctor
    `(spec: AirportSpec, channel, status_reader: StatusReader, emitter: FlightReportEmitter,
    publisher: ConfirmedPublisher, pauser: Callable[[], Awaitable[None]],
    clearance: ClearanceService, ground_delay: GroundDelayService,
    charters: CharterAbortReader)`; `async def on_message(self, message) -> None` (manual
    ack/nack on the aio-pika message; never raises except CancelledError).
    The loop is a straight port of `core/consume/FlightConsumer.java` — same step order, same
    settle carry-out, same `settled` latch, same fallbacks (defer publish failure → nack-requeue;
    park-raw and framework park on the plain channel; framework error after settle → log only).
    Payload step: `spec.payload_types` lookup; name missing → park INTERNAL evidence
    `(KeyError-name, PAYLOAD_TYPE_UNRESOLVED, payload_type)`; `model_validate` failure → reject
    FLIGHT_FAULT evidence `(code, "payload-unreadable", excerpt)`; payload None stays None.
    Structured failure line format from the design's contract table.

- [ ] **Step 1: Write failing topology tests** (FakeChannel recording declare_queue calls):
  names; exact args dicts incl. −1 values; declare order.
- [ ] **Step 2: Write failing consumer tests.** Fakes: FakeIncomingMessage (body, headers, ack/nack
  recorded), FakeChannel (default_exchange.publish recorded, can be told to fail), fake
  clearance/status/ground-delay/charters/emitter/publisher. Tests, grouped:
  - park-raw: undeserializable body; missing flightId; wrong airportId; blank gateId; blank
    payloadType → raw bytes+headers republished to parked, acked, **no report emitted**; park
    publish failure → nack-requeue.
  - airport closed → defer (runway-tail publish + ack) after 0.5 s backoff + pauser awaited; no report.
  - ground delay live → sleeps slices then proceeds when cleared; budget exhausted → defer; `delay_slice_s` boundary values.
  - gate grounded → SKIPPED/GATE_GROUNDED record with disposition PARKED + flight (not raw)
    published to parked + ack.
  - charter aborted → SKIPPED/CHARTER_ABORTED record, disposition TERMINAL, ack, **before** clearance (clearance fake asserts not called).
  - payload: `test_unresolved_payload_type_parks` (INTERNAL record, PARKED, parked publish, ack);
    `test_unreadable_payload_rejects` (FLIGHT_FAULT record, TERMINAL, ack, no park);
    `test_null_payload_lands` (pilot receives flight with payload None).
  - clearance: granted → lands; denial short wait → sleeps and retries; denial wait > 10 s →
    ground_delay.delay(wait) + defer; accumulated sleep ≥ 15 s → ground_delay.delay(window) + defer;
    `ClearanceUnavailableError` → 0.5 s backoff + defer (fail closed); cost = gate slot cost.
  - land/settle: pilot ok → LANDED record TERMINAL, attempt_count incremented on the row;
    FLIGHT_FAULT → TERMINAL ack; GATE_FAULT → park; AIRPORT_FAULT → INTERIM DEFERRED + defer +
    pauser; RATE_LIMIT → INTERIM DEFERRED + ground_delay.delay(window_s) + defer; TRANSIENT first
    failure → holding publish with incremented hold + `test_hold_publish_expiration_ms`
    (expiration property `"30000"`) + INTERIM `HELD_1`; hold budget spent → parked + RECORD
    PARKED; INTERNAL uses its 1-round schedule; spec hold_policy override honored.
  - reporter guard: returns None → INTERNAL (`reporter-returned-null`); raises → INTERNAL with
    evidence from the error.
  - framework: emitter raising after land → `test_report_publish_failure_parks_after_land`
    (catch-all: INTERNAL record attempted, flight parked on plain channel, acked once — the ack
    count over the whole test is exactly 1); step raising after settle → logged, no second settle.
- [ ] **Step 3: Run** → FAIL. **Step 4: Implement `topology.py` then `consumer.py`.**
- [ ] **Step 5: Run full suite** → PASS, ruff clean.
- [ ] **Step 6: Commit** `feat: topology and flight consumer`

### Task 10: Tower dispatch + airport registry

**Files:**
- Create: `src/traction/tower.py`, `src/traction/registry.py`; update `src/traction/__init__.py`
  re-exports (public API: AirportSpec, SlotsConfig, HoldPolicy, Landed, Skipped, Incident,
  IncidentCategory, Evidence, Flight, Tower, DispatchOptions, AirportRegistry, AirportStatus)
- Test: `tests/test_tower.py`, `tests/test_registry.py`

**Interfaces:**
- Consumes: Tasks 2–9.
- Produces:
  - `tower.py`: `@dataclass(frozen=True) DispatchOptions(domain_ref: str | None = None,
    charter_ref: str | None = None)`; `class Tower:` ctor `(registry: AirportRegistry, channel)`
    (plain channel, no confirms — best-effort like `core/api/Tower.java`);
    `async def dispatch(self, airport_id: str, gate_id: str, payload: pydantic.BaseModel,
    options: DispatchOptions | None = None) -> Flight` — unknown airport/gate/None payload →
    `ValueError` before anything is queued; envelope: uuid4 flightId, attempt/hold 0,
    `correlation_id = str(uuid4())[:6]` (Java's substring(0, 6)),
    payloadType from the spec's registry, payload = `payload.model_dump(mode="json")`;
    publish `TYPE_ID_FLIGHT` to the runway.
  - `registry.py`: `class AirportRegistry:` ctor
    `(connection: aio_pika.abc.AbstractConnection, redis, owning_process: str)`;
    `async def register_all(self, specs: list[AirportSpec]) -> None` — validate every spec (and
    duplicate ids) before any side effect, then per airport: open channel (prefetch 1, publisher
    confirms for the consumer's confirmed paths + a plain channel for raw parks/dispatch),
    `declare_airport`, `declare_tower`, announce (`AirportAnnouncement` built from the spec,
    confirmed publish, `TYPE_ID_ANNOUNCEMENT` — fail fast), build `FlightConsumer`, start
    consuming the runway (manual ack), start one `tr:events` pubsub task for the registry;
    `async def request_pause(self, airport_id) -> None` (cancel consumer tag, idempotent, never
    raises); `async def resume(self, airport_id) -> None` (re-reads `StatusReader.get(airport)`
    first; re-subscribes only when OPEN and not already consuming);
    `def gates_of(self, airport_id) -> dict[str, int] | None`;
    `def spec_of(self, airport_id) -> AirportSpec | None`; `async def stop(self) -> None`.
    Events handling per `core/status/EventsListener.java`: gate scopes and unknown airports
    ignored; non-consuming → request_pause; OPEN → resume.

- [ ] **Step 1: Write failing tower tests**: envelope shape (uuid, counts 0, 6-char correlation,
  payloadType name, wire payload dict); unknown airport/gate/null payload raise with nothing
  published; publish goes to `tr.<id>.runway` with `TYPE_ID_FLIGHT`.
- [ ] **Step 2: Write failing registry tests** (fake connection/channels/redis pubsub):
  validate-all-before-declare (second spec invalid → no declares at all); registration declares
  queues + tower and announces with gates `[{gateId, slotCost}]`; pause cancels + idempotent;
  `test_resume_rechecks_stored_status` (stored GROUND_STOP + OPEN event → stays paused; stored
  OPEN → resumes); events: gate-scope event ignored, unknown airport ignored, CLOSED pauses.
- [ ] **Step 3: Run** → FAIL. **Step 4: Implement.** **Step 5: Run** → PASS, ruff clean.
- [ ] **Step 6: Commit** `feat: tower dispatch and airport registry`

### Task 11: Demo tenant (FastAPI)

**Files:**
- Create: `demo/app/__init__.py`, `demo/app/acme.py`, `demo/app/airport.py`, `demo/app/main.py`
- Test: `tests/test_demo_airport.py`

**Interfaces:**
- Consumes: the `traction` public API.
- Produces:
  - `acme.py`: `class AcmeError(Exception)` with `status: int`, `body: str`; subclasses
    `AuthError` (401), `ContractError` (422), `BadPayloadError` (400), `RateLimitedError` (429),
    `VendorUnavailableError` (5xx, timeouts, connect errors — status 0 for transport);
    `def error_for(status: int, body: str) -> AcmeError` (mapping above; other 4xx →
    `RuntimeError`); `class AcmeClient:` ctor `(base_url: str)` (httpx.AsyncClient, 2 s timeout);
    `async def upsert(self, path: str, record: dict) -> None`; `CONTACTS = "/contacts"`,
    `COMPANIES = "/companies"`.
  - `airport.py`: `AIRPORT = "crm:py"`, `CONTACT_SYNC = "contact-sync"`,
    `COMPANY_SYNC = "company-sync"`; pydantic payloads `Contact(id, email, bad: bool = False)`,
    `Company(id, domain, bad: bool = False)`; `def report(flight, error) -> FlightReport`
    mapping exactly like the Java demo's `CrmAirportConfig.report` (None→Landed, AuthError→
    AIRPORT_FAULT, ContractError→GATE_FAULT, BadPayloadError→FLIGHT_FAULT,
    VendorUnavailableError→TRANSIENT, RateLimitedError→RATE_LIMIT, else INTERNAL; evidence
    `Evidence.from_exception`); `def crm_py_spec(acme: AcmeClient) -> AirportSpec` — slots
    20/1 s, both gates cost 1, payload_types `{"demo.Contact": Contact, "demo.Company": Company}`,
    pilot routing gate → path.
  - `main.py`: FastAPI app; lifespan connects `aio_pika.connect_robust` (env `AMQP_URL`,
    default `amqp://guest:guest@rabbitmq:5672/`) + `redis.asyncio.from_url` (env `REDIS_URL`,
    default `redis://redis:6379/0`, `decode_responses=True`), registry
    (owning_process `"py-tenant"`), Tower; routes `POST /seed/contacts?count=&bad=` and
    `POST /seed/companies?count=` dispatching through Tower with `domain_ref` = record id
    (ids `contact-<n>`/`company-<n>`, first `bad` contacts flagged, 400 on negative params,
    response `{"dispatched": n}`), `GET /healthz` → `{"status": "ok"}`.

- [ ] **Step 1: Write failing tests**: `error_for` mapping incl. 418→RuntimeError; `report`
  mapping table (each exception class → category, None → Landed); `crm_py_spec` validates and
  routes contact gate to `/contacts` (fake client records path).
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS, ruff clean.
- [ ] **Step 5: Commit** `feat: crm:py demo tenant`

### Task 12: Compose stack + scenario (the acceptance test)

**Files:**
- Create: `demo/Dockerfile`, `docker-compose.yml`, `demo/lib.sh`, `demo/scenario-python.sh`

**Interfaces:**
- Consumes: the Java repo's compose/scenario conventions (copy `demo/lib.sh` from the companion
  repo and adapt: `APP` → the control plane at :8080, add `PY="http://localhost:8000"`, drop the
  naive-queue helpers).
- Produces: `docker compose up -d --build --wait` + `demo/scenario-python.sh` ending
  `OK scenario-python`.

- [ ] **Step 1: Write `demo/Dockerfile`**: `python:3.12-slim`, copy `pyproject.toml` + `src` +
  `demo`, `pip install '.[demo]'`, run `uvicorn demo.app.main:app --host 0.0.0.0 --port 8000`
  (adjust module path to the chosen layout); healthcheck via `GET /healthz` (install curl or use
  python -c urllib).
- [ ] **Step 2: Write `docker-compose.yml`**: services `rabbitmq` (rabbitmq:3.13-management,
  mgmt on `127.0.0.1:${RABBIT_MGMT_PORT:-15674}:15672`, ping healthcheck), `redis`
  (valkey/valkey:8, ping healthcheck), `acme-vendor` (build context
  `https://github.com/dhananjaym-dev/traction.git#<SHA pinned at implementation time>`,
  dockerfile `acme-vendor/Dockerfile`, `127.0.0.1:9090:9090`, `/mode` healthcheck),
  `control-plane` (same git context, dockerfile `demo-app/Dockerfile`, `127.0.0.1:8080:8080`,
  env SPRING_RABBITMQ_HOST/SPRING_DATA_REDIS_HOST/ACME_BASE_URL, `/traction/api/airports`
  healthcheck, depends_on healthy rabbitmq/redis/acme-vendor), `py-tenant` (build `.`,
  dockerfile `demo/Dockerfile`, `127.0.0.1:8000:8000`, env AMQP_URL/REDIS_URL/ACME_BASE_URL,
  depends_on healthy rabbitmq/redis/acme-vendor). No 5672/6379/15672 host publishes.
- [ ] **Step 3: Write `demo/scenario-python.sh`** (sourcing the adapted lib.sh), asserting in
  order: reset; healthy batch of 40 contacts lands (vendor ok == 40, backlog 0); flip contacts
  token_expired + seed 200 → `crm:py` status GROUND_STOP (wait ≤30 s), runway holds 200,
  exactly 1 GROUND_STOP notification in control-plane logs; flip healthy + one
  `POST $API/airports/crm:py/reopen` → all 240 landed, parked 0, dlq 0; flip companies
  contract_break + seed 30 companies → parked == 30 (GATE_FAULT), gate company-sync
  GROUND_STOP... **check the Java control plane's actual gate rule first** (3 distinct
  domainRefs in 5 min stops the gate — assert gate status GROUND_STOP too); flip healthy, reopen
  the gate, `POST $API/airports/crm:py/replayParked` → parked drains to 0, companies ok == 30;
  final `ok "scenario-python"` printing `OK scenario-python`.
- [ ] **Step 4: Run it**: `docker compose up -d --build --wait && ./demo/scenario-python.sh`.
  Expected: `OK scenario-python`. Verify host isolation: `lsof -nP -iTCP:5672 -iTCP:6379` shows
  no compose binds; fix and re-run until green.
- [ ] **Step 5: Commit** `feat: interop demo — java control plane governing the python tenant`

### Task 13: README + CI + vocabulary gate

**Files:**
- Create: `README.md`, `.github/workflows/ci.yml`

- [ ] **Step 1: Write `README.md`** in the Java repo's register: what this proves (one control
  plane, two languages, nothing shared but broker + store), quickstart (clone, compose up,
  scenario), the wire-contract pointers (table reusing the design doc's), the pamqp −1 note,
  library usage sketch (spec + registry + tower in ~20 lines), link to the companion repo; note
  the shared 8080/9090 host ports mean the two repos' demos don't run side by side.
- [ ] **Step 2: Write `.github/workflows/ci.yml`**: `permissions: contents: read`; job `test`
  (setup-python 3.12, `pip install -e '.[demo]' pytest pytest-asyncio ruff`, `ruff check src tests demo`,
  `pytest`); job `scenario` (needs test, `docker compose up -d --build --wait`, run
  `./demo/scenario-python.sh | tee`, assert final `OK scenario-python` line, compose logs on
  failure, always `docker compose down -v`), timeout-minutes 30.
- [ ] **Step 3: Run the vocabulary gate**:
  `grep -riE 'personatech|phoenix|PTI-|hangar|terminal(Id)?|ADVISORY|DIVERTED' . --exclude-dir=.git --exclude-dir=docs --exclude-dir=.venv`
  plus a case-sensitive check that the only `TERMINAL` hits are the disposition token. Expected: empty.
- [ ] **Step 4: Full local verification**: `pytest` green, `ruff check` clean, scenario still
  green from Task 12 (re-run if any code changed since).
- [ ] **Step 5: Commit** `docs: readme and ci`

### Task 14: Final review

- [ ] **Step 1:** Run the repo-wide self-checks: full test suite, ruff, vocabulary gate, scenario.
- [ ] **Step 2:** Dispatch a fresh code review of the whole repo (code-review skill, high effort)
  against the design doc; fix CONFIRMED findings; re-run the suite + scenario after fixes.
- [ ] **Step 3:** Ask the owner before creating the GitHub repo and pushing (per the brief);
  include the chosen repo name and the pinned companion-repo SHA in that message.
