# ADR 072: `audit.fail_closed` (High-Assurance Audit Enforcement)

**Status:** Promoted (plan) — **3-of-3 on the core** (opt-in ack-based
durable-or-reject); **2-of-3 (Codex, Claude) on no-new-wire-enum-variant**
(Gemini iter-5 added `SqlErrorCode::AuditUnavailable` / `TaskFailureKind::Audit
Unavailable`; resolved against by the cycle-long no-wire-drift invariant — see
Council History).
**Seed:** ADR 066 introduced Toron's default-disabled, **fail-open** audit
consumer; ADR 071 completed worker-task + observability-access coverage while
preserving fail-open. 066 deferred `audit.fail_closed` as a separate
high-assurance mode. 072 adds it.
**Category:** Toron compute-ring audit emission mode over the 066/071 event
surface. **No shared audit substrate, Wendelon, storage, SQL-wire, worker-wire,
or event-schema change.**

## The guarantee (stated precisely)

> Under `[audit] fail_closed = true`, Toron does not **execute past a pre-op
> gate** or **report success / an authorized protected response** for a *gated*
> operation unless its required audit event has reached the **durable local audit
> chain** through a successful local sink emit.

This is **response-gating**, not side-effect rollback: a SQL write may already be
committed when its query-terminal audit is gated — 072 prevents *reporting
success* without durable evidence, not the commit. True **no-unaudited-committed-
write** needs a **pre-operation intent audit** — a stronger, separate model,
**out of scope** (carried).

## Decisions (15, Promotion-Locked)

**D1 Opt-in; default unchanged.** `[audit] fail_closed = false` default. Missing
`[audit]`, `audit.enabled = false`, or `fail_closed = false` ⇒ **behavior-
equivalent to 071** (fail-open `try_send`). The regression invariant.

**D2 Config honesty.** `fail_closed = true` with `audit.enabled = false` is
**startup-invalid** (a high-assurance flag that can't be enforced must not
silently no-op). `audit.fail_closed_ack_timeout_ms` is bounded + **nonzero**
(default ~500 ms). Validated in Toron raw-config normalization.

**D3 Eligible vs Required.** `security_relevant() == true` (`audit.rs:786`) is the
**eligibility** boundary, **not** a global control-flow gate. *Required* = an
eligible event emitted from an ADR-072 gated producer hook via the required path.
The marker's meaning is unchanged.

**D4 `record` fail-open; `record_required` mode-aware.** `ToronAuditEmitter::
record(event)` stays the 066/071 synchronous `try_send` path. Add
`record_required(event).await -> Result<(), AuditUnavailable>`. **In fail-open
mode it delegates to `record` and returns `Ok(())`** — gated hooks use one helper
without changing default behavior.

**D5 Required = durable-or-reject via worker ack.** In fail-closed mode,
`record_required` sends the event + an ack handle to the audit worker; the worker
acks success **only after** the event reaches the **local tamper-evident chain**
through a successful local sink emit. With the current shared
`observability::audit` substrate, `LocalAuditSink::emit` writes the record and
returns only after `sync_data()`. Required auditing is a delivery contract for
the existing 066/071 events — **not a new event shape**.

**D6 Per-event ack first; batching deferred.** The first implementation uses one
ack per required event, fired after that event's sink emit succeeds. Do not add a
new shared batch-fsync or watermark API in 072. A worker watermark or batched
durability API may replace per-event acks later if it is introduced by a
separate substrate change and proven behavior-equivalent.

**D7 Bounded ack-timeout.** Queue-full / closed-worker / serialization-fail /
sink-fail / **timeout** ⇒ `AuditUnavailable`. A required wait **never hangs** a
query, task, or request.

**D8 Audit-sink-failure diagnostics are NEVER required (the recursion catch).** An
eligible event that *is* the audit-sink-failure diagnostic must not require a
durable ack from the same failing sink (it would deadlock). Those stay
best-effort → security-unrecorded metric + degraded readiness. The worker must
not emit a *required* event while handling a sink failure.

**D9 Required Surface Table.**

| Surface | Required event | Gate point | On `AuditUnavailable` |
|---|---|---|---|
| SQL edge denial | `edge_denial` | before denial response/close | bounded audit-unavailable denial/close |
| SQL query terminal | `query_outcome` | after result known, **before first result batch / `Complete` / `Error`** | don't report success; bounded reject |
| SQL cancel | cancellation `query_outcome` | before cancel-accepted response | bounded reject |
| Worker task **start** | `worker_task_started` | **before task execution** (post decode/route/register) | reject task (pre-execution) |
| Worker task terminal | completed/failed/cancelled | before terminal response | don't report success; bounded reject |
| Observability ready/metrics | `observability_access` | before protected response | `503`, no sink detail |
| `/health`, lifecycle, sink-failure diag | none | ungated / best-effort | unchanged |

**D10 No new SQL/worker wire enum variant (the wire-stable resolution).** Do
**not** add a `SqlErrorCode` or `TaskFailureKind` variant for 072. SQL uses an
existing code (e.g. `SqlErrorCode::Internal`) with a **static bounded "audit
unavailable" reason**; worker rejection uses an existing emittable `TaskFailureKind`
with a static bounded reason. Preserves the cycle-long **no-SQL-wire / no-worker-
wire drift** invariant; the audit-unavailable distinction rides the bounded
reason, not a wire-surface change. *(Gemini iter-5 proposed new variants for
client clarity; resolved against by wire-stability + keeping 072 a surgical audit
change.)*

**D11 Oracle-safe.** The client-visible result exposes only that audit is
unavailable — **no** sink path, filesystem cause (disk-full vs perms), queue
depth, node id, principal detail, SQL text, table name, or raw error string.

**D12 Readiness interaction.** A degraded/unavailable local audit chain under
fail-closed ⇒ gated operations reject + **Not-Ready** for protected surfaces
(sheds security-relevant load while the audit gap exists — the point). `/health`
unaffected.

**D13 Local chain is the durability boundary.** The ack means the Toron-owned
local chain accepted the event. Archive/syslog/remote projections keep emitting
failure metrics + degraded readiness but **don't widen** the success definition
(unless the sink abstraction already treats them as local durable emit).

**D14 Response-gating, not side-effect-gating (documented).** Worker-start gating
is **side-effect-preventing** (pre-execution); SQL/worker-terminal + access gating
is **response-gating** (writes commit before the terminal audit). No rollback;
pre-op intent audit carried.

**D15 Toron-home; no drift.** Emitter + config + producer-hook change over the
066/071 `ToronAuditEvent` shapes (unchanged); `observability::audit` stays
synchronous + policy-free; no Wendelon / storage / SQL-wire / worker-wire change.

## Architecture Guards (20 + convention/meta)

`adr072_audit_fail_closed_is_opt_in_default_false`,
`adr072_fail_closed_false_is_behavior_equivalent_to_071`,
`adr072_fail_closed_true_requires_audit_enabled`,
`adr072_fail_closed_ack_timeout_is_bounded_and_nonzero`,
`adr072_record_remains_try_send_fail_open`,
`adr072_record_required_is_mode_aware`,
`adr072_required_audit_uses_worker_ack`,
`adr072_required_ack_means_local_chain_written_and_fsynced`,
`adr072_required_ack_uses_oneshot_after_sink_emit_returns`,
`adr072_security_relevant_is_eligibility_not_global_control_flow`,
`adr072_required_call_sites_match_surface_table`,
`adr072_audit_sink_failure_is_never_required`,
`adr072_query_results_are_not_streamed_before_required_terminal_audit`,
`adr072_worker_started_required_before_task_execution`,
`adr072_required_queue_full_closed_sink_error_or_timeout_rejects`,
`adr072_audit_unavailable_errors_are_bounded_oracle_safe`,
`adr072_no_new_sql_error_or_task_failure_enum_variant`,
`adr072_response_gating_not_side_effect_gating_documented`,
`adr072_fail_closed_degrades_readiness_for_security_relevant_surfaces`,
`adr072_no_wendelon_storage_sql_wire_worker_wire_or_shared_substrate_change`.
**Convention/meta:** `adr072_nilket_root_stays_runtime_free`,
`adr072_prior_adr065_066_071_audit_guards_continue_to_pass`.

## Integration Fixtures (15)

`fail_closed_default_false_preserves_071_behavior` (regression net);
`fail_closed_true_with_audit_disabled_is_rejected`;
`fail_closed_zero_ack_timeout_is_rejected`;
`record_required_delegates_fail_open_when_fail_closed_false`;
`required_event_written_and_fsynced_before_gated_success`;
`query_result_batches_not_sent_before_required_audit_ack`;
`required_audit_queue_full_rejects_with_bounded_audit_unavailable`;
`required_audit_sink_error_degrades_readiness_and_rejects`;
`required_audit_ack_timeout_rejects_without_hanging`;
`worker_started_audit_failure_prevents_task_execution` (pre-op);
`worker_terminal_audit_failure_does_not_report_task_complete` (response-gating);
`observability_metrics_audit_failure_returns_503_without_sink_detail`;
`health_endpoint_is_not_fail_closed_gated`;
`sink_failure_diagnostic_does_not_recursively_require_audit`;
`no_new_sql_or_worker_wire_enum_variant`.

## Out of Scope / Carried

Pre-operation intent audit (true no-unaudited-side-effect for SQL writes);
rollback/compensation for committed writes; per-surface fail-closed flags
(one global flag now); worker watermark / batch-fsync acks (per-event first over
the current sink API); new wire enum variants (reuse existing); audit-query APIs;
typed table-mutation trace continuation (Wendelon ≈075 parallel); composite-PK;
fragmentable bulk DML.

## Council History

iter-0: 3-way split (Claude/Gemini fail_closed; Codex trace). iter-1: Codex
moved to fail_closed (trace → Wendelon ≈075); ack-based 3-of-3; Claude raised the
response-vs-side-effect guarantee. iter-2: Codex's `record_required` call-site API
+ Eligible/Required + the **audit-sink-failure non-recursion catch** + Required
Surface Table; Gemini's fsync-before-ack + batch-sync; Claude synthesized the
per-surface guarantee. iter-3+: Codex + Claude converged the dual (per-event-ack/
current-sink-durability, response-gating documented, **no new wire enum
variant**); **Gemini iter-5 added `SqlErrorCode`/`TaskFailureKind`
`AuditUnavailable` variants** for client clarity — resolved against by the
cycle-long no-wire-drift invariant (keep 072 a surgical audit-emitter change; the
distinction rides a bounded reason on an existing code).

## Promotion criteria met

D-set 15; 20 + convention/meta guards; 15 fixtures; opt-in (default fail-open —
regression net); durable = local-chain written+`fsync`; the audit-sink-failure
non-recursion catch; response-gating guarantee stated honestly; **no new wire
enum variant** (wire-stable); Toron-home, no substrate/Wendelon/wire drift.
Migration: `plans/impl/072_audit_fail_closed.md`.

## Conclusion

072 is the opt-in fail-closed capstone of the audit arc: a mode-aware
`record_required` (fail-open passthrough when off), durable = local-chain
written+`fsync`'d via a worker ack after successful local sink emit, gating the
Required Surface Table (worker-start pre-op; SQL/worker-terminal + access
response-gating), with audit-sink-failure diagnostics never recursively required,
oracle-safe rejects over **existing** wire shapes (no new enum variant), and
readiness-shed — over the unchanged 066/071 event surface.
