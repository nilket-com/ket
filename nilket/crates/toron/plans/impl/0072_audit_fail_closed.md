# Impl Plan 072: `audit.fail_closed` Enforcement

**Status:** Promoted, paired with `plans/adr/0072_audit_fail_closed.md`.
**Posture:** Add an opt-in fail-closed audit mode over the existing ADR 066/071
emitter — a mode-aware `record_required` (fail-open passthrough when off),
durable = local-chain written + `fsync`'d through the current local sink emit,
per-event ack, gating the Required Surface Table, with rejects over **existing**
wire shapes (no new enum variant). **Toron-home; no event-shape,
shared-substrate, Wendelon, SQL-wire, or worker-wire change.** Six phases.

## Invariants carried in

- `fail_closed=false` / `[audit]`-disabled ⇒ **behavior-equivalent to 071**
  (regression net). `record(event)` stays fail-open (`try_send`). Required path
  **never hangs** (bounded timeout → `AuditUnavailable`). **Audit-sink-failure
  diagnostics are never required** (no recursion). Durable = local chain
  written+`fsync`; downstream sinks don't widen the gate. **Response-gating** (no
  rollback, no pre-op intent audit). **No new `SqlErrorCode`/`TaskFailureKind`
  variant** — reuse existing + bounded reason. Oracle-safe rejects (no
  sink/fs/queue/principal/SQL leak). Event shapes unchanged; `observability::audit`
  policy-free; no Wendelon/SQL-wire/worker-wire/storage drift.

## Phase 0 — Audit (verified anchors)

- **Emitter (fail-open):** `crates/toron/src/audit.rs` — `record(event)`
  `tx.try_send` (`:1170/1176`); `security_relevant()` (`:786`) = eligibility;
  degraded-readiness (`:1135/1154/1239`); the async `run_audit_worker` drives the
  local chain sink.
- **Config:** `AuditEnabledConfig` + `RawAuditConfig` (`config.rs`) — where
  `fail_closed` + `fail_closed_ack_timeout_ms` slot, and the `fail_closed&&!enabled`
  + zero-timeout startup-invalid checks go (raw-config normalization).
- **Gated hooks (Surface Table):** SQL edge-denials + query-terminal + cancel
  (`daemon.rs`); worker task start + terminal (`server/mod.rs`,
  `execute_task_local_with_telemetry` + the `StartTask` accept path); ready/metrics
  (`network/observability.rs`); `/health`, lifecycle, sink-failure-diag **ungated**.
- **Existing wire shapes (reuse, no new variant):** `SqlErrorCode::Internal`
  (`sql_wire.rs`) + a static bounded "audit unavailable" reason; an existing
  emittable `TaskFailureKind` (`error/distributed.rs`) + bounded reason.

## Phase 1 — Handshake infra (`audit.rs`)

- Channel payload → an envelope: `FireAndForget(ToronAuditEvent)` |
  `Require { event, ack: oneshot::Sender<Result<(), AuditUnavailable>> }`.
- `record(event)` → `try_send(FireAndForget(event))` — **unchanged fail-open**
  (drop metrics intact).
- `record_required(event).await -> Result<(), AuditUnavailable>`:
  **fail_closed=false ⇒ delegate to `record`, return `Ok(())`**; fail_closed=true ⇒
  `try_send(Require{event, ack})` + `tokio::time::timeout(ack_timeout, rx)`.
  Queue-full/closed/serialization-fail/sink-fail/timeout ⇒ `AuditUnavailable`;
  `Disabled` is a valid fail-open no-op, and `Unavailable` returns
  `AuditUnavailable` only when the emitter was configured fail-closed.
- **Worker (ack after emit):** process each required command through the current
  shared sink API and send `Ok(())` only after `sink.emit(shared_event)` returns
  success. Today that means `LocalAuditSink::emit` has written the record and
  completed `sync_data()`. On sink failure send `Err(AuditUnavailable)` for that
  required event + the existing degraded-readiness path. **Never enqueue a
  `Require` for an audit-sink-failure diagnostic** (no recursion) — those stay
  `FireAndForget` + security-unrecorded. Batched fsync or watermark acking is a
  later substrate optimization, not part of 072.

## Phase 2 — Config + readiness (`config.rs`, `audit.rs`)

- `AuditEnabledConfig`: `fail_closed: bool` (default false) +
  `fail_closed_ack_timeout_ms` (bounded, **nonzero**, default ~500). **Reject at
  startup** if `fail_closed && !audit.enabled` or zero timeout. `ReadinessState`:
  under fail_closed, persistent sink error keeps security-relevant readiness false
  (Not-Ready). Queue-full rejects the current required operation; only mark
  readiness degraded for sustained saturation if the implementation adds an
  explicit, non-transient signal.

## Phase 3 — SQL enforcement (`daemon.rs`)

- Edge-denial + query-terminal + cancel paths: `audit.record_required(event).await`
  (delegate ⇒ no-op when off). Query-terminal gate is **after result known, before
  the first result batch / `Complete` / `Error` frame** (so success isn't streamed
  before durability). On `Err` ⇒ existing `SqlErrorCode::Internal` + bounded "audit
  unavailable" reason (no new variant; oracle-safe).

## Phase 4 — Worker enforcement (`server/mod.rs`)

- **Start (pre-op):** gate `worker_task_started` via `record_required` **before
  task execution** (post decode/route/register) ⇒ no execution without a durable
  started-audit. **Terminal:** gate completed/failed/cancelled before the terminal
  response; on `Err` ⇒ existing emittable `TaskFailureKind` + bounded reason (no
  new variant). The 071 one-shot terminal recorder composes.

## Phase 5 — Observability enforcement (`network/observability.rs`)

- ready/metrics handlers `record_required` the access event **before writing
  success headers**; on `Err` ⇒ `503` (no sink detail). `/health` untouched.

## Phase 6 — Guards, fixtures, evidence

- 20 + convention/meta guards (incl. **sink-failure-never-required**,
  **ack-after-local-emit**, **no-new-wire-enum-variant**, record-stays-fail-open,
  record_required-mode-aware, eligibility-not-global-control-flow,
  call-sites-match-surface-table, query-results-not-streamed-before-ack,
  worker-start-before-execution, response-gating-documented, config-honesty,
  bounded-nonzero-timeout, oracle-safe, health-ungated, readiness-shed,
  no-substrate/Wendelon/wire drift).
- 15 fixtures (ADR list) — anchored by `fail_closed_default_false_preserves_071_
  behavior` (regression net), `sink_failure_diagnostic_does_not_recursively_
  require_audit`, `worker_started_audit_failure_prevents_task_execution` (pre-op),
  `query_result_batches_not_sent_before_required_audit_ack`,
  `required_event_written_and_fsynced_before_gated_success`,
  `no_new_sql_or_worker_wire_enum_variant`.
- `plans/impl/0072_audit_fail_closed_evidence.md`: `architecture_test adr072`,
  full `architecture_test` (065/066/071 green), `cargo test -p toron` (audit +
  daemon + worker + listener suites), `cargo check -p toron` feature matrix,
  `clippy -D warnings`, `git diff --check`; **latency note** —
  fdatasync-per-required-event overhead unless a later substrate batching ADR
  changes the sink API.

## Watch-items

1. **No audit-of-audit recursion** — a sink-failure diagnostic must never take
   the `Require` path (it'd deadlock on the failing sink); guarded + fixture.
2. **Durable = fsync** — ack fires only after local sink emit returns; the
   current `LocalAuditSink::emit` performs `sync_data()`.
3. **Never hang** — bounded nonzero timeout; any failure mode → `AuditUnavailable`.
4. **Fail-open when off** — `record_required` delegates to `record` + `Ok(())`;
   default byte-identical to 071.
5. **No new wire variant** — reuse `SqlErrorCode::Internal` / existing
   `TaskFailureKind` + bounded reason; SQL-wire + worker-wire enums unchanged.
6. **Response-gating, not rollback** — worker-start pre-op; SQL/worker-terminal +
   access response-gated.
7. **Config honesty** — `fail_closed && !enabled` (and zero timeout) rejected at
   startup.

## Acceptance checklist

- [ ] Phase 1: envelope + `record` fail-open + `record_required` (delegate when
      off; ack+timeout when on); worker acks after local sink emit; sink-diag
      never required.
- [ ] Phase 2: `fail_closed` + nonzero timeout config; startup-invalid if
      `!enabled`/zero; readiness-shed under degraded chain.
- [ ] Phase 3: SQL denial + query-terminal (before first frame) + cancel gated;
      `SqlErrorCode::Internal` + bounded reason (no new variant).
- [ ] Phase 4: worker start gates execution; terminal gated; existing
      `TaskFailureKind` + bounded reason (no new variant).
- [ ] Phase 5: ready/metrics gated → `503`; `/health` ungated.
- [ ] Phase 6: 20 guards + 15 fixtures (incl. regression net + sink-recursion +
      pre-op + no-new-wire-variant) + evidence; 065/066/071 green.
