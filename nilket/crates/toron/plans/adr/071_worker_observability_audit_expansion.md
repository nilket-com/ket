# ADR 071: Worker + Observability-Access Audit Expansion

**Status:** Promoted (plan) — **grounded 2-of-3 (Codex, Claude) on fail-open**;
Gemini iter-5 held a fail-closed posture (recorded in Council History) that
contradicts the implemented ADR 066 emitter and the separately-deferred
`audit.fail_closed` capability.
**Seed:** ADR 066 built the Toron audit consumer (lifecycle, SQL edge denials,
query outcomes, drop/sink metrics, degraded readiness) but **deferred worker-task
and observability-listener coverage**. 071 fills exactly those two gaps over the
existing `observability::audit` substrate.
**Category:** Toron compute-ring audit consumer. **Coverage ADR only** — not a new
audit substrate, emission mode, worker/HTTP wire, or shared-observability policy.

## Fail-Open Lock (the decisive decision)

071 preserves ADR 066 emission semantics. `ToronAuditEmitter::record` stays
nonblocking + bounded via **`try_send`** (verified `audit.rs:774`); queue-full /
closed-emitter → drop + security-unrecorded metrics; sink-write failure →
**degraded readiness** (`audit_local_chain_write_failed`, `:837`). **A worker
task, a `ready`/`metrics` response, or a TLS-accept path must NOT fail because an
audit event cannot be enqueued or written.**

`audit.fail_closed` (making audit failure fail the request) **changes emission
semantics** and remains a **separate, evidence-gated ADR** (the 066 deferral) —
its standing place on the carried list is unchanged. Fixtures or guards that
require task/request failure on audit failure are **explicitly out of scope** for
071. Pinned by `adr071_audit_emission_preserves_try_send_fail_open_no_request_
failure` + `adr071_no_audit_fail_closed_request_failure_path`, and a positive
fixture (`worker_audit_queue_full_records_drop_without_failing_task`).

## Decisions (14, Promotion-Locked)

**D1 Extend existing Toron audit types** — `ToronAuditOperation`,
`ToronAuditEventClass`, `ToronAuditEvent`, `ToronAuditEmitter`,
`ToronAuditMetrics` (`crates/toron/src/audit.rs`). No parallel `AuditLog` facade.

**D2 Two new event classes** — `TaskLifecycle` (`task_lifecycle`),
`ObservabilityAccess` (`observability_access`); update
`ToronAuditEventClass::ALL`/`COUNT`, metrics rendering + tests, guards together.

**D3 Worker task operations** — `WorkerTaskStarted/Completed/Failed/Cancelled` →
`toron.worker.task.{started,completed,failed,cancelled}`.

**D4 `started` = accepted for execution** — emitted only after the `StartTask`
payload is decoded, route identity validated, the task registered, and not
cancelled-before-start. Cancellation registered before start emits one
`cancelled` terminal and **no** `started`.

**D5 Exactly one worker terminal event** — `completed`/`failed`/`cancelled`, via
a one-shot terminal recorder so execution/streaming/cancellation paths cannot
double-record. Protocol/envelope failures before an accepted task stay outside
task-lifecycle audit.

**D6 Worker fields bounded** — tenant (existing field), `query_id`, `stage_id`,
`task_id`, `attempt_id`, `worker_id`, `stage_role`, `operator_kind`,
`outcome_class`, **bounded `error_class`** (from `QueryError`/`TaskFailureKind`,
reusing existing classes; raw strings forbidden), duration/row-count buckets.
**Forbid** SQL/plan-debug/table/column/descriptor/runtime-filter-key/literal/
Arrow-bytes/peer-address/raw-error.

**D7 Observability access operations** — `ObservabilityReadyAccess`/
`ObservabilityMetricsAccess` → `toron.observability.{ready,metrics}.access`, with
`AuditOutcome::{Allowed,Denied,Error}` + bounded `endpoint`/`method_class`/
`status_class`/`auth_class`/`outcome_class`.

**D8 `/health` stays unaudited** — plaintext-loopback, high-frequency,
operationally noisy; a negative guard + fixture prove health handling does not
call `ToronAuditEmitter`.

**D9 Observability audit is Toron network policy** —
`crates/toron/src/network/observability.rs` threads `ToronAuditEmitter` into
ready/metrics only; `observability::{http,audit}` stay **policy-free** (no Toron
operation names / event types / async enqueue).

**D10 Record the admitted monitor principal, not raw peer identity.** For
accepted ready/metrics TLS connections, derive the monitor principal by reusing
Toron's SAN admission policy for `ListenerRole::Observability`; record a **bounded
`actor_key_id=monitor:<principal>` + `actor_role=observability_probe`** — **no**
raw SANs / subject DNs / peer IPs / cert serials / headers / cert material.
Implement the needed TLS helper expansion inside 071 so the
`observability_access_records_admitted_monitor_principal_only` guard and fixture
are not advisory. *(Codex iter-5's resolution — the access-audit value, the
principal, recorded in bounded form; supersedes the iter-1/4 raw-SAN-vs-bounded
fork.)*

**D11 Process-local chains unchanged** — workers reuse the 066 `[audit]` config,
`toron-n-<u32>` node-id validation, local chain format, sink fanout, metrics,
file-permission expectations. No per-worker chain format / node-id namespace.

**D12 Default worker constructors audit-disabled** — existing `ToronWorker`
constructors + test helpers default to `ToronAuditEmitter::disabled()`; the daemon
passes the configured emitter to the runtime worker. **No worker task-wire
change**; fixture/test workers stay audit-free.

**D13 Retarget the ADR 066 worker-no-audit guard** — runtime `ToronWorker` audit
is now required; the **fixture-worker-source-unchanged**, SQL-wire, Wendelon,
nilket, and shared-observability-policy-free guards remain. (The 069/074
meta-guard pattern.)

**D14 No protocol/storage drift** — task payloads, HTTP response bodies, SQL
wire, Wendelon, nilket, storage layout, and shared-observability crate APIs are
unchanged. Bounded actor metadata records `internal_compute` and
`observability_probe` roles through `ToronAuditEvent` / `AuditActor`; no public
Toron actor enum is required unless the implementation introduces a private one.

## Architecture Guards (19 + convention/meta)

`adr071_toron_audit_event_classes_include_task_lifecycle_and_observability_access`,
`adr071_toron_audit_operations_include_worker_task_and_observability_access`,
`adr071_worker_task_audit_uses_toron_audit_emitter`,
`adr071_worker_task_started_after_decode_route_register_and_accept`,
`adr071_worker_task_audit_emits_exactly_one_terminal_event`,
`adr071_worker_task_audit_fields_are_bounded`,
`adr071_worker_task_audit_has_no_sql_plan_table_descriptor_value_payload_or_raw_error`,
`adr071_worker_task_audit_does_not_change_task_wire_protocol`,
`adr071_worker_audit_is_disabled_by_default_handle`,
`adr071_observability_ready_metrics_audit_is_in_toron_network_layer`,
`adr071_observability_health_listener_remains_unaudited`,
`adr071_observability_access_records_admitted_monitor_principal_only`,
`adr071_observability_access_has_no_raw_san_dn_ip_or_certificate_material`,
`adr071_shared_observability_crates_remain_policy_free`,
`adr071_audit_emission_preserves_try_send_fail_open_no_request_failure`,
`adr071_no_audit_fail_closed_request_failure_path`,
`adr071_runtime_worker_audit_allowed_fixture_worker_stays_audit_free`,
`adr071_worker_node_id_matches_toron_n_u32`,
`adr071_no_wendelon_nilket_sql_wire_storage_or_worker_wire_change`.
**Convention/meta:** `adr071_nilket_root_stays_runtime_free`,
`adr071_prior_adr065_066_audit_guards_continue_to_pass_except_worker_retarget`.

## Integration Fixtures (14)

`worker_task_success_emits_started_and_completed_audit`;
`worker_task_failure_emits_failed_with_bounded_error_class`;
`worker_task_cancelled_before_start_emits_cancelled_without_started`;
`worker_task_cancelled_during_execution_emits_single_cancelled_terminal_event`;
`worker_audit_disabled_is_noop`;
**`worker_audit_queue_full_records_drop_without_failing_task`** (the fail-open
pin); `worker_audit_node_id_validation`;
`observability_ready_allowed_emits_access_audit`;
`observability_ready_tls_failure_emits_denied_access_audit`;
`observability_metrics_allowed_emits_access_audit`;
`observability_metrics_method_not_allowed_emits_denied_access_audit`;
`observability_health_does_not_emit_audit`;
`observability_access_records_admitted_monitor_principal_only`;
`observability_access_audit_omits_raw_san_dn_ip_and_certificate_material`.
**Excluded:** any `audit_failure_closes_task_execution` (that's
`audit.fail_closed`).

## Out of Scope / Carried

`audit.fail_closed` (emission-mode change — separate evidence-gated ADR); typed
table-mutation trace continuation (Wendelon-home producer ≈075 + Toron consumer,
the 074→069 pattern, carrying the iter-2 refinements — DELETE is 1:1 with its
request, bulk writes share one `toron.storage.write` parent); composite-PK
(cross-ring schema-V2 storage/identity — Wendelon `validate_primary_key` is
exact-one-column); fragmentable bulk DML (distributed write-back).

## Council History

iter-0: 3-way split (Claude composite-PK, Codex typed-mutation-trace, Gemini
audit). iter-1: placement reconciliation (trace → Wendelon, the 074→069 shape).
iter-2: Claude conceded composite-PK (Codex verified Wendelon's exact-one-column
`validate_primary_key` — it's cross-ring, not Toron-bounded); Codex moved to
audit. iter-3: topic settled 3-of-3 (worker + observability-access audit); Claude
caught the fail-open/fail-closed contradiction (grounded in `audit.rs:774`).
iters-4/5: Codex + Claude converged the dual on **fail-open**; **Gemini iter-5
held fail-closed** ("audit emission a prerequisite for task/request completion") —
resolved against by the implemented 066 `try_send` emitter and the fact that
fail-closed is the separately-deferred `audit.fail_closed` capability. Codex
iter-5 resolved the peer-identity fork to the bounded admitted-monitor-principal.

## Promotion criteria met

D-set 14; 19 + convention/meta guards; 14 fixtures; consumer-only (no
substrate/wire/Wendelon/storage drift); **fail-open preserved** (no
`audit.fail_closed` smuggled in — 2-of-3 grounded); the 066 worker-no-audit guard
retarget specified. Migration: `plans/impl/071_worker_observability_audit_
expansion.md`.

## Conclusion

071 completes Toron's audit arc — worker task-lifecycle + observability-access
coverage over the 066 substrate — strictly **fail-open** (the 066 `try_send`
model; `audit.fail_closed` stays deferred), disabled-by-default worker handle (no
wire change), bounded fields + bounded access classes recording only the admitted
monitor principal (no raw cert material), `/health` excluded, shared crates
policy-free, with the 066 worker-no-audit guard retargeted.
