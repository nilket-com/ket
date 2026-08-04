# ADR 059: Distributed Query Cancellation Completeness

**Status:** Promoted (2-of-3 — Claude + Codex; Gemini control-protocol
redesign recorded as minority)
**Seed:** the verified gap from the ADR 058 selection audit — statement
timeout drops the coordinator future with no `CancelTask` propagation;
`cancel_query` is coordinator-local; the worker awaits the task **inline**
on the `StartTask` stream; `CancelTask` is **ack-only** (no active-task
registry); SQL `Cancel` returns `CancellationUnsupported`. In-flight remote
work continues after timeout/disconnect.
**Council:** strongest-aligned start of the run — all three iter-0s
converged on the topic + spine. iter-1/2 resolved the forks (SQL `Cancel`
adds a cross-connection registry — named, not free; abort = drop-at-await +
pre-publish checkpoint, not operator-wide token threading; fresh
**bidirectional** out-of-band stream; bounded tombstone for the early-cancel
race). iter-3 anchored the decisive transport fact: `ToronTransport` /
`QuicEndpoint::connect_stream` are **bidi-only (`open_bi`); there is no
`open_uni`**. Claude + Codex converged on the existing-wire design (Codex
D1–D24 ≈ this set); **Gemini held a `ControlMessage` / unidirectional /
worker-control-listener / textual-`CANCEL` redesign through iter-6**,
recorded as minority.
**Category:** Toron compute-ring substrate ADR — distributed execution
lifecycle (coordinator + worker). Node-local; no Wendelon change; no new SQL
wire variant / protocol; no transport-abstraction change.

## Summary

Make cancellation **real** for already-dispatched Toron distributed work: a
query the coordinator stops waiting for — statement timeout, client
disconnect, or SQL `Cancel` — actually **aborts its worker tasks** and frees
their resources, instead of letting them run to natural completion and
discarding the output. The spine is a **worker active-task registry** (today
`CancelTask` is ack-only — nothing aborts a running task). Triggers route
through **one idempotent coordinator cancellation handle**; remote cancel
goes **out of band** over a fresh **bidirectional** stream using the
**existing** `CancelTask`/`TaskCancelled` wire. Best-effort, bounded, and
honest at the Wendelon boundary (dropping a Toron future does not cancel
already-issued Wendelon storage work).

## Motivation

The 049–058 governance arc bounds admission, rate, connections, and ingress
— but its timeouts/rejections only *stop waiting*; they don't *stop the
work*. A timed-out or disconnected distributed query's worker tasks keep
consuming CPU/memory until they finish, then results are discarded. 059
closes that leak, making the governance arc's resource accounting real.

## Decisions (22, Promotion-Locked)

**D1 Placement.** Toron-only distributed query cancellation across the SQL
listener, coordinator, scheduler, and worker task runtime. No Nilket /
Wendelon / SQL-grammar / request-byte-limit change.

**D2 Scope.** Cancel **already-dispatched** work via three triggers:
statement timeout, client disconnect (after `Execute` decode), SQL `Cancel`.

**D3 Identity.** Coordinator/SQL boundary keys by **`(TenantId, QueryId)`**;
worker boundary keys by **`TaskIdentity`** (matching the existing wire
payloads). Bare `QueryId` lookup is forbidden (cross-tenant oracle).

**D4 Coordinator active-query registry.** Keyed by `(TenantId, QueryId)`,
**RAII-registered and removed on every terminal path** (completion, error,
timeout, cancel, disconnect, post-registration bind/plan failure, shutdown).
A small wrapper type so tenant-scoped lookup + removal are unit-testable.

**D5 One handle, three triggers.** Timeout, disconnect, and SQL `Cancel` all
invoke the **same idempotent** cancellation handle — the primary guard
against drift.

**D6 Statement-timeout trigger.** Fire cancellation **before** returning
`SqlErrorCode::StatementTimeout`.

**D7 Client-disconnect trigger.** After an `Execute` is decoded and a
`query_id` exists, client disconnect fires cancellation; **disconnect before
decode does nothing**.

**D8 SQL `Cancel`.** Uses the **existing** `SqlMessage::Cancel(CancelRequest
{ query_id })` frame on a separate request stream; authenticates/resolves
tenant exactly like `Execute`; returns `SqlErrorCode::Cancelled` for
accepted, missing, already-terminal, **and tenant-invisible** ids — **never
revealing cross-tenant query existence** (oracle-safe). **No** textual
`CANCEL <query_id>` grammar.

**D9 Out-of-band remote cancel.** Over a **fresh bidirectional** worker
stream using the **existing** `CancelTask` / `TaskCancelled` messages and
`cancel_task_on_worker` (carrying the ack the bounded-wait needs). **No**
`ControlMessage`, **no** unidirectional stream, **no** worker control
endpoint. ("Out of band" = not the busy `StartTask` stream — not a new
protocol.)

**D10 Concurrent cancel handling.** The worker must process the fresh cancel
stream **concurrently** with the busy `StartTask` stream — this is what
makes out-of-band cancel effective; guard it.

**D11 Worker active-task registry.** Keyed by `TaskIdentity`, holding a
cancellation signal; **RAII-deregistered** on completion, error, cancel,
publish failure, send failure, and connection drop.

**D12 `CancelTask` is not ack-only.** It **signals** an active task and
remains **idempotent** — acking `TaskCancelled` for missing, completed,
pending-cancelled, or already-cancelled identities.

**D13 Early-cancel race.** A `CancelTask` may arrive **before** the matching
`StartTask` registers (independent streams). Register before any visible
`TaskStarted` ack **and** keep a pending-cancel tombstone keyed by
`TaskIdentity`, checked at `StartTask` registration to abort immediately.

**D14 Bounded tombstone.** Tombstones are bounded by count and/or TTL,
**consumed** when the matching `StartTask` arrives, and **evicted** when
stale — never an unbounded growth map (the 055/056 discipline).

**D15 Abort mechanism.** Race the **inline** task future against the
cancellation signal and **drop it at an await boundary**. **No**
cancellation-token threading through every executor operator; add a targeted
cooperative check only where a fixture proves a non-awaiting loop needs one.

**D16 Publish boundary.** Cancel observed **before** publish suppresses it
plus further `TaskBatch` frames and `TaskComplete`. Cancel **during** publish:
either publish is proven atomic for the current partition store, or local
cleanup makes partial output **unreachable** for the cancelled query — pinned
by fixture.

**D17 Client never blocks on remote acks.** Timeout returns
`StatementTimeout` and SQL `Cancel` returns `Cancelled` **promptly**; the
remote `CancelTask` fan-out runs as **detached bounded background cleanup**
with a **fixed internal** wait bound (no config knob).

**D18 Bounded background fan-out.** Background cleanup is **tracked/bounded**
(coordinator/runtime-owned), not untracked detached work — it must not
become an unbounded shutdown drain or a new hang vector.

**D19 Local cleanup always runs.** Coordinator-local scheduler/task-state/
dependency/filter-merge/partition cleanup runs **even when remote fan-out
fails** (unreachable worker → existing worker-loss semantics).

**D20 Resource release.** Admission, memory reservations, worker registry
entries, and SQL connection/request resources release through **normal
RAII** ownership paths after cancellation.

**D21 No pre-admission cancel.** ADR 055–058 pre-admission rejects emit
**no** cancellation — nothing remote was dispatched. Guard the boundary.

**D22 No Wendelon claim + identity-free metrics.** Dropping a Toron worker
future stops Toron work and frees Toron resources; it does **not** cancel
already-issued Wendelon storage work (a later ADR). Metrics are
aggregate-only — trigger/outcome labels allowed; **no** query/task/worker/
table/tenant/SQL-text/peer/endpoint labels.

**Single-plan posture; Toron-ring only; node-local.**

## Architecture Guards (20, Promotion-Locked)

1. `adr059_sql_cancel_uses_existing_cancel_frame_not_execute_grammar`
2. `adr059_sql_cancel_is_tenant_scoped_and_not_cross_tenant_oracle`
3. `adr059_active_query_registry_is_tenant_scoped_and_raii_removed`
4. `adr059_timeout_disconnect_and_sql_cancel_share_one_handle`
5. `adr059_statement_timeout_fires_cancel_before_returning`
6. `adr059_client_disconnect_before_execute_decode_sends_no_cancel`
7. `adr059_client_response_does_not_wait_for_remote_cancel_ack`
8. `adr059_cancel_fanout_is_bounded_background_cleanup`
9. `adr059_remote_cancel_uses_existing_cancel_task_wire`
10. `adr059_out_of_band_cancel_stream_is_bidirectional_for_ack`
11. `adr059_no_control_message_uni_stream_or_control_endpoint`
12. `adr059_worker_handles_cancel_stream_concurrently_with_task_stream`
13. `adr059_worker_active_registry_keyed_by_task_identity_raii_deregistered`
14. `adr059_worker_cancel_task_not_ack_only_and_idempotent`
15. `adr059_cancel_before_start_task_tombstones_and_aborts_on_registration`
16. `adr059_pending_cancel_tombstone_is_bounded_and_evicted`
17. `adr059_abort_is_drop_at_await_with_pre_publish_suppression`
18. `adr059_local_cleanup_runs_when_remote_cancel_fails`
19. `adr059_pre_admission_rejects_emit_no_cancel`
20. `adr059_no_wendelon_storage_cancellation_claim_and_metrics_identity_free`

**Convention guards:** `adr059_nilket_root_stays_runtime_free`,
`adr059_fixture_worker_source_unchanged`,
`adr059_new_surface_is_brand_clean_and_has_no_v1_suffix`.

**Prior-guard reconciliation:** retarget the guard pinning SQL
`CancellationUnsupported` → SQL `Cancel` uses the existing frame, no new
variant (and ADR 058's cancel-frame guard still asserts `Cancel` bypasses
statement-byte capping). ADR 049-058 meta-guard persists.

## Integration Fixtures (19 required)

1. `statement_timeout_returns_promptly_and_cancels_dispatched_worker_task`
2. `sql_cancel_frame_cancels_active_query_for_same_tenant`
3. `sql_cancel_cross_tenant_query_is_not_observable`
4. `sql_cancel_missing_or_terminal_query_is_idempotent`
5. `client_disconnect_after_execute_decode_cancels_active_query`
6. `client_disconnect_before_execute_decode_sends_no_cancel`
7. `cancel_task_arrives_out_of_band_while_start_task_stream_is_busy`
8. `worker_cancel_task_signals_active_task_not_ack_only`
9. `worker_cancel_task_before_registration_tombstones_and_prevents_execution`
10. `pending_cancel_tombstone_is_bounded_and_consumed`
11. `active_task_registry_deregisters_on_all_terminal_paths`
12. `cancel_after_compute_before_publish_suppresses_publish`
13. `cancel_during_batch_send_stops_later_batches`
14. `remote_cancel_ack_timeout_does_not_delay_statement_timeout_surface`
15. `background_cancel_cleanup_is_bounded_or_shutdown_tracked`
16. `local_cleanup_runs_when_remote_cancel_fanout_fails`
17. `admission_and_memory_release_after_cancellation`
18. `pre_admission_rejects_emit_no_cancel_task`
19. `cancellation_metrics_have_no_identity_labels`

## Out of Scope

- Wendelon storage-server request cancellation (later ADR — the deferred
  half of this ADR's honesty boundary).
- New `ControlMessage` family, unidirectional stream routing, or a worker
  control endpoint (transport is `open_bi`-only; existing wire suffices).
- Textual `CANCEL <query_id>` SQL grammar (separate SQL-surface ADR).
- Operator-wide cancellation-token threading (drop-at-await + targeted
  checks only where proven needed).
- Durable/cross-process cancellation state; audit sinks; parser throttling;
  ADR 058 byte-limit changes.

## Council History

- **iter-0:** 3-of-3 on topic + spine (worker registry, out-of-band cancel,
  coordinator handle, timeout trigger, best-effort/bounded, reuse wire,
  no-pre-admission-cancel, no-Wendelon-claim).
- **iter-1:** abort = drop-at-await + pre-publish checkpoint (not
  weave-everywhere); SQL `Cancel` needs a cross-connection `(TenantId,
  QueryId)` registry (named, not "free"); fresh bidi stream + ack.
- **iter-2:** Claude converged to the full cut (one substrate, three
  triggers); bounded-tombstone sharpening.
- **iter-3:** transport fact anchored — `open_bi` only, no `open_uni`.
- **iter-4/5 (dual):** Claude and Codex stabilized the existing-wire D-set
  (Codex D1–D24 ≈ this set).
- **Minority (Gemini, through iter-6, recorded):** a new
  `ControlMessage::CancelTask` family on **unidirectional** streams + a
  worker control listener, a bare global `QueryId` registry, textual
  `CANCEL` grammar, and operator-wide token threading. Not adopted: (a) the
  transport exposes **no `open_uni`** — it's a transport+protocol redesign
  for a cancel the existing **bidi** `CancelTask`/`TaskCancelled` wire
  already carries *with* the ack bounded-wait needs; (b) a bare `QueryId`
  registry is a **cross-tenant oracle**; (c) textual `CANCEL` is separate
  SQL-surface scope; (d) threading tokens through every operator is more
  invasive than drop-at-await + a publish checkpoint. The control-plane/
  data-plane split is a legitimate *future transport* idea, not a
  cancellation ADR.

## Promotion criteria met

1. D-set 22, 2-of-3 (Claude+Codex); Gemini minority recorded with reasons.
2. Guards 20 + 3 convention; `CancellationUnsupported` guard retargeted;
   ADR 049-058 meta-guard.
3. Fixtures 19 required.
4. Toron-ring; reuses the existing cancel wire; no Wendelon / SQL
   wire-variant / transport-protocol change.
5. Node-local distributed-execution scale boundary.
6. Brand-clean / no-`*V1`; fixture worker unchanged; nilket runtime-free.
7. Closes the verified timeout/disconnect resource leak; makes the
   already-present `Cancel` frame real.

## Conclusion

The smallest *complete* cancellation substrate: a worker active-task
registry that actually aborts running tasks, one idempotent coordinator
handle for three triggers, out-of-band cancel on the existing bidi wire,
bounded tombstones for the early-cancel race, drop-at-await + pre-publish
suppression, client-never-blocks + bounded background fan-out, honest at the
Wendelon boundary. No new protocol or transport surface. Migration:
`plans/impl/0059_distributed_query_cancellation.md`.
