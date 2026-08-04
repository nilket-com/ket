# Impl Plan 071: Worker + Observability-Access Audit Expansion

**Status:** Promoted, paired with
`plans/adr/0071_worker_observability_audit_expansion.md`.
**Posture:** Extend the ADR 066 Toron audit consumer to two deferred surfaces —
worker task-lifecycle and observability `ready`/`metrics` access — over the
existing `observability::audit` substrate. **Strictly fail-open** (the 066
`try_send` model; `audit.fail_closed` deferred); disabled-by-default worker
handle (no wire change); bounded fields + bounded access classes recording only
the admitted monitor principal (no raw cert material); `/health` excluded; shared
crates policy-free. Five phases.

## Invariants carried in

- **Fail-open**: never fail a task / `ready`-`metrics` response / TLS-accept on
  audit enqueue or sink failure (66 model: `try_send`, drop + security-unrecorded
  metrics, sink failure → degraded readiness). **`audit.fail_closed` is OUT**
  (deferred); **no `audit_failure_closes_task_execution` fixture/guard**. No
  worker/observability **wire** change; no SQL-wire/Wendelon/nilket/storage/
  shared-observability-policy change; `[audit]`-disabled ⇒ exact no-op; fixture
  worker source unchanged; bounded fields only; `/health` unaudited; no raw cert
  material.

## Phase 0 — Audit (verified anchors)

- **Emitter (fail-open):** `crates/toron/src/audit.rs` — `ToronAuditEmitter`/
  `ToronAuditOperation`/`ToronAuditEventClass`/`ToronAuditEvent`/
  `ToronAuditMetrics`; `record` uses `tx.try_send` (`:774`); sink failure sets
  `degraded_reason` (`:733/752/837`), never fails the caller.
- **Worker:** `crates/toron/src/server/mod.rs` task loop
  (`execute_task_local_with_telemetry` + the `StartTask` decode/route/register/
  accept path); `TaskSpec.identity`/`.context` carry bounded task identity.
- **Listeners:** `crates/toron/src/network/observability.rs` (`spawn` + ready/
  metrics handlers); `ListenerRole::Observability` SAN admission policy is the
  monitor-principal source; `/health` handler untouched.
- **Daemon:** `crates/toron/src/daemon.rs` builds `ToronAuditEmitter` (after
  metrics/readiness) and the runtime worker.
- **Guard to retarget:** the ADR 066 guard forbidding audit in worker paths
  (`toron/tests/architecture_test.rs`), distinct from the fixture-worker-source
  guard (which stays).

## Phase 1 — Taxonomy (`audit.rs`)

- Add `ToronAuditEventClass::{TaskLifecycle, ObservabilityAccess}` (+
  `ALL`/`COUNT`/metrics rendering). Add `ToronAuditOperation`
  `WorkerTask{Started,Completed,Failed,Cancelled}` +
  `Observability{Ready,Metrics}Access`. Add bounded actor-role metadata for
  `internal_compute` and `observability_probe` through `ToronAuditEvent` /
  `AuditActor` parameters; no public Toron `Actor` enum is required. Bounded
  `ToronAuditEvent` builders cover worker task identity and access
  endpoint/auth/status/method classes plus `actor_key_id`. Bounded worker
  `error_class` comes from `QueryError`/`TaskFailureKind`.

## Phase 2 — Worker instrumentation (disabled-by-default handle; fail-open)

- Add `ToronAuditEmitter` field to `ToronWorker`, defaulting to
  `ToronAuditEmitter::disabled()` (existing constructors / fixture workers
  unchanged); daemon passes the configured emitter to the runtime worker. **No
  task wire change.**
- Emit `started` after decode/route-validate/register/accept (cancel-before-start
  ⇒ `cancelled` terminal, no `started`); a **one-shot terminal recorder** emits
  exactly one of `completed`/`failed`(+bounded error class)/`cancelled`. All via
  the fail-open `try_send` emitter.

## Phase 3 — Listener instrumentation (`network/observability.rs`)

- Thread the emitter from `spawn` into the **ready** + **metrics** handlers only;
  emit `ObservabilityAccess` with `AuditOutcome` + bounded `endpoint`/`auth_class`/
  `status_class`/`method_class`/`outcome_class`, and the **admitted monitor
  principal** (`actor_key_id=monitor:<principal>` via the
  `ListenerRole::Observability` SAN admission policy) — **no** raw SAN/DN/IP/
  serial/header/cert material. `/health` untouched (no audit hook). Shared
  `observability::{http,audit}` unchanged.

## Phase 4 — Guard reconciliation

- Retarget the ADR 066 worker-no-audit guard: runtime `ToronWorker` audit now
  required; keep `fixture_worker_source_unchanged`, SQL-wire, Wendelon, nilket,
  and shared-observability-policy-free guards. Meta:
  `adr071_prior_adr065_066_audit_guards_continue_to_pass_except_worker_retarget`.

## Phase 5 — Guards, fixtures, evidence

- 19 + convention/meta guards (incl. **fail-open / no-request-failure**,
  **no-fail-closed-path**, disabled-by-default-handle, no-wire-change,
  bounded-fields, admitted-monitor-principal-only, no-raw-cert-material,
  health-unaudited, shared-crates-policy-free, the 066 retarget).
- 14 fixtures (ADR list) — anchored by **`worker_audit_queue_full_records_drop_
  without_failing_task`** (fail-open pin), `observability_health_does_not_emit_
  audit`, `observability_access_records_admitted_monitor_principal_only`,
  `..._omits_raw_san_dn_ip_and_certificate_material`. **No**
  `audit_failure_closes_task_execution` fixture (that's `audit.fail_closed`).
- `plans/impl/0071_worker_observability_audit_expansion_evidence.md`:
  `architecture_test adr071`, full `architecture_test` (incl. the retargeted 066
  guards green), `cargo test -p toron` (audit + worker + listener suites),
  `cargo check -p toron` feature matrix, `clippy -D warnings`, `git diff --check`.

## Watch-items

1. **Fail-open is the hard line** — never fail the caller; the queue-full fixture
   pins it; no `audit.fail_closed` request-failure path/fixture creeps in.
2. **Disabled-by-default handle** — existing worker constructors + fixture
   workers stay audit-free (no wire change, no forced setup).
3. **Admitted monitor principal only** — bounded `actor_key_id`; raw SAN/DN/IP/
   cert material forbidden; reuse the `ListenerRole::Observability` admission.
4. **`/health` excluded** — negative guard + fixture.
5. **066 guard retarget** — runtime worker audit allowed; fixture-worker guard intact.

## Acceptance checklist

- [ ] Phase 1: two event classes + worker/access operations + bounded actor-role
      metadata + bounded builders + worker error-class.
- [ ] Phase 2: disabled-by-default `ToronWorker` emitter; started-after-accept +
      one-shot terminal; fail-open `try_send`; no task wire change.
- [ ] Phase 3: ready/metrics audited in `network/observability.rs`, bounded
      classes + admitted monitor principal, no raw cert material; `/health`
      untouched; shared crates unchanged.
- [ ] Phase 4: 066 worker-no-audit guard retargeted; fixture-worker guard intact.
- [ ] Phase 5: 19 guards + 14 fixtures (incl. the fail-open pin) + evidence; no
      fail-closed fixture/path; 065/066 suites green.
