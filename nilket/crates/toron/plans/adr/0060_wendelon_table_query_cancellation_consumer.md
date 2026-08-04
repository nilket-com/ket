# ADR 060: Wendelon Table-Query Cancellation Consumer

**Status:** Promoted (3-of-3 — Claude, Codex, Gemini)
**Seed:** the deferred half of Toron ADR 059's honesty boundary, now
consumable. Wendelon ADR 073 shipped the producer primitive
(`TableQueryRequestId`, `TABLE_QUERY_CANCEL_REQ/RESP`,
`cancel_table_query_attempt`); 060 wires 059's cancellation to call it,
closing the **059 → 073 → 060** end-to-end cancellation chain.
**Council:** the run's second straight clean 3-of-3 (after Wendelon 073).
Forks settled: the cancel is **parent-signal-fired** (not a scan-drop guard
or a per-scan `AbortOnDrop` watcher — both raced/bypassed by 059's `select!`
teardown); Toron **surfaces** the lower client's per-attempt id via a hook
(it does **not** generate one — the lower client regenerates per attempt at
`client/core.rs:599`, so a scan-generated id would be overwritten → no-op
cancel).
**Category:** Toron compute-ring substrate — distributed-execution storage
boundary. Exterior Substrate → Toron home. Thin consumer; node-local
cancellation state; no 059 protocol redesign, no 073-producer change, no
new SQL grammar / Toron worker wire / Wendelon server wire / Control-Ring
RPC / unidirectional stream / listener / transport redesign.

## Summary

When a Toron query is cancelled (statement timeout / client disconnect / SQL
`Cancel` / worker `CancelTask`), its in-flight Wendelon `TABLE_QUERY_REQ`
reads keep running — Wendelon **materializes before responding** (073), so
the dropped stream is observed too late. 060 closes this: a Wendelon-client
**hook** surfaces the concrete `(NodeId, TenantId, TableQueryRequestId)`
attempt the lower retry loop chose (after selection, before write);
`WendelonStorageScan` **registers** it (holding an RAII guard) with a
**downstream-attempt registry** owned by the parent 059 cancellation state;
the parent's `cancel()` **snapshots and fires** bounded detached
`cancel_table_query_attempt(...)` — the same discipline as 059's worker
fan-out, with the client never waiting on the ack (059 D17). This retires
059's "no Wendelon storage-cancellation claim," narrowly: best-effort,
checkpoint-bounded `TABLE_QUERY_REQ` reads only.

## Grounding (verified)

- `client/core.rs:599` `attempt_request.meta.request_id =
  TableQueryRequestId::random()` — the **lower** client generates a fresh
  non-zero id **per attempt** (`:579` initial; `:1747` send rejects zero;
  `:619` `cancel_table_query_attempt` exists). Reads are already cancellable
  server-side.
- The `wendelon-client` facade passes `TableQueryRequestId::ZERO`
  (lib.rs:985) and hides the chosen `(target, request_id)` → 060 **surfaces**
  via a hook; does **not** generate.
- 059 seams: coordinator `ActiveQueryHandle` and the worker active-task
  signal (`distributed/cancellation.rs`) already snapshot + fan out remote
  task cancels; downstream Wendelon cancels join that path.
- `ScanRequest` (storage.rs:45) / `ExecutionContext` (exec.rs) carry the
  optional downstream-cancel context; in-memory scans ignore it.

## Decisions (22, Promotion-Locked)

**D1 Placement.** Toron consumer of Wendelon 073; 073 remains producer
owner; 060 retargets 059's no-Wendelon caveat. No Wendelon/transport change.

**D2 Scope.** Only `WendelonStorageScan` `TABLE_QUERY_REQ` reads. Out: legacy
`QUERY_REQ`/`QUERY_STREAM`, `AGG_REQ`, writes, deletes, CTAS, descriptor/
admin, CDC, sweepers, compaction, SQL grammar, transport surfaces.

**D3 Attempt identity.** `(wendelon NodeId, wendelon TenantId,
TableQueryRequestId)`.

**D4 No Toron identity export.** No `QueryId`/`TaskIdentity`/session/table-id/
descriptor-fingerprint/SQL-text/endpoint/peer/worker identity crosses the
Wendelon cancel wire or appears in metrics.

**D5 Surface, do not generate.** Toron observes the id the lower retry loop
generated; it does not generate one in `WendelonStorageScan` (the lower
client would overwrite it).

**D6 Client hook.** The Wendelon client facade exposes a hook-bearing
table-query API; the hook is implemented inside the lower retry loop and runs
**after** target/request-id selection and **before** `TABLE_QUERY_REQ` write.

**D7 Hook owns lifecycle.** The hook returns/installs an RAII attempt guard;
a pure "observed" notification is insufficient — redirect/retry must remove
the old attempt before registering the new one.

**D8 No retry duplication.** Toron does not duplicate Wendelon routing/
redirect/stats-pruning/retry.

**D9 Register before write.** Registration precedes the request write; if
parent cancellation is already set, registration fails and no request is
written.

**D10 Exact RAII removal.** Success, error, redirect, local future drop, and
stream close remove the exact registered attempt.

**D11 Parent-fired trigger.** Explicit Wendelon cancel fires from the parent
059 cancellation state. Scan-local watcher tasks, scan drop, and
`Drop`-spawned async work are **not** primary triggers.

**D12 Coordinator triggers.** Statement timeout, client disconnect, and SQL
`Cancel` share `ActiveQueryHandle` and therefore one downstream path.

**D13 Worker trigger.** Worker `CancelTask` fires downstream attempts
attached to the active task **before/alongside** drop-at-await abort.

**D14 Nonblocking surface.** SQL timeout / SQL `Cancel` / disconnect cleanup
/ `TaskCancelled` ack do not wait for Wendelon cancel acks (059 D17).

**D15 Bounded fan-out.** Cancel sends are bounded/tracked with fixed
connect/ack timeouts; duplicate parent cancels do not refire the same
snapshot.

**D16 No pre-admission cancel.** Pre-admission rejects emit no Wendelon
cancel (no attempt exists).

**D17 Already-cancelled before scan.** If parent cancellation is observed
before scan issue, no `TABLE_QUERY_REQ` is sent.

**D18 Stream-drop fallback.** Wendelon stream-drop is fallback cleanup;
explicit `TABLE_QUERY_CANCEL_REQ` is the prompt primary (materialize-then-
respond).

**D19 Outcome mapping.** Wendelon `QUERY_CANCELLED` / interrupted table-
query read maps through an explicit Toron cancellation storage surface to
`TaskFailureKind::Cancelled` / query-cancelled — **not** `Internal` /
`Unavailable` / `ResourceExhausted` / descriptor-refresh / schema-
instability. Descriptor-evolved mapping remains unchanged.

**D20 Metrics.** A **separate** identity-free counter
`toron_downstream_storage_cancellations_total{trigger,outcome}` (closed-set
labels) — keeps 059's worker-task metrics stable, no overloading of
`RemoteAck`/`RemoteTimeout`.

**D21 Guard retarget.** Retarget `adr059_no_wendelon_storage_cancellation_
claim` to the narrow residual: Toron cancels in-flight Wendelon table-query
reads via 073, best-effort and checkpoint-bounded, no broader claim.

**D22 Compatibility.** Existing Wendelon client table-query APIs keep current
behavior; the hook-bearing API is opt-in for Toron.

## Architecture Guards (20 + 3 convention + meta)

1. `adr060_consumes_wendelon_adr073_without_new_wire`
2. `adr060_client_hook_exposes_attempt_without_retry_duplication`
3. `adr060_hook_returns_or_holds_raii_attempt_guard`
4. `adr060_attempt_identity_is_target_tenant_request_id_only`
5. `adr060_no_toron_identity_crosses_wendelon_cancel_wire`
6. `adr060_attempt_registers_before_table_query_write`
7. `adr060_retry_redirect_replaces_attempt_registration`
8. `adr060_raii_removes_attempt_on_terminal_paths`
9. `adr060_cancel_is_parent_fired_not_scan_watcher_primary`
10. `adr060_coordinator_cancel_triggers_share_downstream_path`
11. `adr060_worker_cancel_task_fires_downstream_before_drop_at_await`
12. `adr060_pre_admission_rejects_emit_no_wendelon_cancel`
13. `adr060_already_cancelled_before_scan_sends_no_table_query_req`
14. `adr060_wendelon_query_cancelled_maps_to_toron_cancelled`
15. `adr060_downstream_cancel_fanout_is_bounded_and_nonblocking`
16. `adr060_stream_drop_is_fallback_not_primary`
17. `adr060_metrics_are_aggregate_only`
18. `adr060_no_writes_cdc_admin_agg_or_legacy_query_scope`
19. `adr060_no_control_ring_uni_stream_sidecar_or_sql_grammar`
20. `adr060_retargets_adr059_no_wendelon_claim_narrowly`

**Convention:** `adr060_nilket_root_stays_runtime_free`,
`adr060_fixture_worker_source_unchanged`,
`adr060_new_surface_is_brand_clean_and_has_no_v1_suffix`.
**Meta:** `adr060_prior_adr049_to_059_guards_continue_to_pass_except_retarget`.

## Integration Fixtures (14 required)

1. `statement_timeout_cancels_dispatched_wendelon_table_query_attempt`
2. `sql_cancel_cancels_same_tenant_wendelon_table_query_attempt`
3. `client_disconnect_after_execute_cancels_wendelon_table_query_attempt`
4. `worker_cancel_task_cancels_inflight_wendelon_table_query_before_future_drop`
5. `cancel_before_wendelon_scan_start_sends_no_table_query_req`
6. `pre_admission_reject_sends_no_wendelon_cancel`
7. `wendelon_query_cancelled_maps_to_toron_cancelled`
8. `wendelon_redirect_retry_replaces_attempt_registration`
9. `downstream_attempt_raii_removes_on_success_error_redirect_and_drop`
10. `downstream_cancel_racing_completion_is_terminal_or_ack_without_error`
11. `downstream_cancel_fanout_timeout_does_not_delay_sql_cancel_response`
12. `downstream_cancel_metrics_have_no_identity_labels`
13. `in_memory_storage_scan_ignores_cancellation_context`
14. `legacy_query_agg_write_delete_and_descriptor_paths_emit_no_wendelon_cancel`

## Out of Scope

- Toron-generated Wendelon request ids (lower client owns generation).
- Scan-local watcher tasks / `Drop`-spawned async as the primary trigger.
- Cancellation for legacy query / aggregate / writes / deletes / descriptor
  / admin / CDC / sweepers / compaction.
- New SQL grammar, Toron worker wire, Wendelon server wire, Control-Ring RPC,
  unidirectional stream, sidecar listener, or transport abstraction.
- Blocking any client-visible cancellation surface on the Wendelon ack.

## Council History

- iter-0: 3-of-3 on topic; trigger fork (Claude scan-drop guard / Gemini
  per-scan watcher / Codex parent registry).
- iter-1: scan-drop trigger retired — 059's `select!` drops the whole task
  future, bypassing it; converged on parent-fired registry; Claude added the
  059-guard-retarget.
- iter-2: verified `client/core.rs:599` regenerates the id per attempt ⇒
  **surface, not generate** (Gemini's generate-in-scan would no-op); Claude
  flagged Gemini's `AbortOnDrop` watcher reintroduces the teardown race.
- iter-3: **3-of-3** — Gemini conceded the watcher; Codex added RAII-guard
  hook precision (D7).
- iter-4: metric shape (separate counter, 2-of-3 Codex+Claude) and guard/
  fixture set locked.

## Promotion criteria met

1. D-set 22, **3-of-3**.
2. Guards 20 + 3 convention + meta; `adr059_no_wendelon_storage_cancellation_
   claim` **retargeted** (the meta-guard is scoped `…_except_retarget`).
3. Fixtures 14.
4. Toron-ring consumer; no 059 protocol redesign / 073-producer / wire /
   transport change.
5. Cross-ring boundary: one cancel per in-flight attempt; no Toron identity
   exported; no Toron-side Wendelon state beyond the attempt record.
6. Closes the 059 → 073 → 060 end-to-end cancellation arc.

## Conclusion

ADR 060 completes the distributed-cancellation lifecycle while preserving
ring isolation: cancelled Toron queries now actively cancel in-flight
Wendelon table-query reads — surfaced (not generated) per-attempt id,
parent-fired bounded detached fan-out, narrow guard retarget — without
exporting Toron identities, changing the Wendelon producer wire, or blocking
user-visible cancellation on storage acks. Migration:
`plans/impl/0060_wendelon_table_query_cancellation_consumer.md`.
