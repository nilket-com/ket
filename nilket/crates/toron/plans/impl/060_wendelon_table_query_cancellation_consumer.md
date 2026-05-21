# Impl Plan 060: Wendelon Table-Query Cancellation Consumer

**Status:** Promoted, paired with
`plans/adr/060_wendelon_table_query_cancellation_consumer.md`.
**Posture:** Toron-ring consumer. **No 059 protocol redesign** (extends and
reuses the existing cancellation handle/signal + fan-out), **no 073-producer
change** (calls the existing `cancel_table_query_attempt`), **no new wire /
Control-Ring / transport**. Eight phases. The novel pieces are the parent-
owned downstream-attempt registry + the Wendelon-client attempt hook
(surface, not generate).

## Invariants carried in

- ADR 049–059 behavior unchanged except the retargeted 059 no-Wendelon-claim
  guard. `crates/nilket` runtime-free; fixture worker unchanged; no-`*V1`.
- No Toron identity (`QueryId`/`TaskIdentity`/SQL text/table-id/fingerprint)
  on the Wendelon cancel wire or in metric labels.

## Phase 0 — Audit (verified anchors)

- `wendelon::client::WendelonClient` (`client/core.rs`): `:599`
  `attempt_request.meta.request_id = TableQueryRequestId::random()` —
  **fresh non-zero id per attempt** (`:579` initial; `:1747` send rejects
  zero); `:619` `cancel_table_query_attempt(target, tenant_id, request_id)`
  exists. The `wendelon-client` **facade** passes `…::ZERO` (lib.rs:985) and
  hides the chosen `(target, request_id)`. → **surface via hook, do not
  generate.**
- `distributed/cancellation.rs`: coordinator `ActiveQueryHandle` (`cancel()`
  + remote-task snapshot/fan-out) and worker active-task signal/registry —
  the parent states that already fire 059 cancels; the downstream registry
  attaches here.
- `ScanRequest` (storage.rs:45) and `ExecutionContext` (exec.rs) carry the
  optional downstream context; in-memory scans ignore it.
  `WendelonStorageScan::scan` (storage.rs:735) issues the read;
  `map_wendelon_client_error_to_storage` (storage.rs:467) is the status/error
  mapping site.

## Phase 1 — Wendelon client seam (wendelon-client facade + lower loop)

- Add a facade helper `table_query_with_runtime_filter_and_attempt_hook(…,
  hook)` whose **lower retry loop** invokes the hook **after** target +
  `:599` request-id selection and **before** the request write (redirect/
  retry → fresh hook event). The hook **returns/installs an RAII attempt
  guard** (D7 — not a bare notification). A hook failure prevents the
  request write. Ordinary table-query APIs unchanged.
- Expose a facade `cancel_table_query_attempt(target, tenant_id, request_id)`
  delegating to the lower `:619` helper.

## Phase 2 — Downstream-attempt registry (distributed/, near cancellation.rs)

- `WendelonTableQueryAttempt { target: NodeId, tenant_id, request_id }` —
  internal Toron state only (never wire, never metric label). The registry
  must also have the process-local Wendelon client handle or sender needed
  to issue the cancel RPC; that handle is not part of the attempt identity.
- `DownstreamAttemptRegistry`: `register_table_query_attempt(attempt) ->
  AttemptGuard` (RAII removes the exact attempt on drop — all terminal
  paths); `cancel_registered_attempts(trigger)` → **snapshot** + **bounded
  detached** fan-out of `cancel_table_query_attempt`; `is_cancelled()`.
  Store registrations by local registration id, not only by attempt value,
  so guard removal is exact. Bounded by a semaphore (reuse 059's
  `cancel_fanout_permits`); fixed connect/ack timeout; idempotent
  (duplicate parent cancels don't refire the snapshot); identity-free
  outcome recording.

## Phase 3 — Coordinator integration (coordinator.rs, cancellation.rs)

- Attach a `DownstreamAttemptRegistry` to `ActiveQueryHandle`.
  `ActiveQueryHandle::cancel()` snapshots downstream attempts **alongside**
  the existing remote-task cancellations; the coordinator schedules **both**
  fan-outs **without blocking** the caller (timeout/disconnect/SQL `Cancel`
  return promptly — D14/059 D17). RAII-removed with the handle.

## Phase 4 — Worker integration (server/mod.rs, cancellation.rs)

- Store the registry **beside** the worker active-task signal (keep
  `TaskCancellationSignal` a pure signal; the active-task state owns the side
  effect). On `CancelTask`, fire `cancel_registered_attempts`
  **before/alongside** the drop-at-await abort, so the explicit cancel is not
  gated on the dropped scan future. Pass the registry handle into the task
  `ExecutionContext` before executing the plan.

## Phase 5 — Execution + scan plumbing (exec.rs, storage.rs)

- Add an **optional** downstream-cancel context (registry handle +
  `is_cancelled`) to `ExecutionContext` (exec.rs) → `ScanRequest`.
  Coordinator-local
  execution builds it from the `ActiveQueryHandle`; worker execution from the
  active-task state. **In-memory scans ignore it**; other storage traits not
  widened.
- `WendelonStorageScan::scan`: if `is_cancelled()` already → return the
  existing Toron cancelled path, **no `TABLE_QUERY_REQ`** (D17). Otherwise
  call the hook variant; the hook **registers** the surfaced attempt and the
  scan holds the `AttemptGuard` for the read lifetime (RAII removal on
  return/error/redirect/drop). Register-before-write; registration fails if
  parent cancellation already set (D9).

## Phase 6 — Outcome mapping + metrics (storage.rs, observability)

- Add an explicit Toron cancellation storage surface, e.g.
  `StorageError::Cancelled`, and map Wendelon `QUERY_CANCELLED` /
  interrupted table-query reads to it in `map_wendelon_client_error_to_storage`.
  The execution / worker layers then translate that surface to
  `TaskFailureKind::Cancelled` / query-cancelled; **not** `Internal` /
  `Unavailable` / `ResourceExhausted` / descriptor-refresh. Descriptor-
  evolved mapping unchanged.
- **Separate** identity-free counter
  `toron_downstream_storage_cancellations_total{trigger,outcome}` —
  closed-set labels, not overloading 059's `RemoteAck/Timeout/Error`.

## Phase 7 — Guards + retarget + fixtures + evidence + roadmap

- 20 + 3 convention + meta guards (ADR list). **Retarget**
  `adr059_no_wendelon_storage_cancellation_claim` → narrow table-query claim
  (it would go false otherwise); meta-guard scoped `…_except_retarget`.
- 14 fixtures (ADR list).
- `plans/impl/060_wendelon_table_query_cancellation_consumer_evidence.md`:
  `architecture_test adr060`, full `architecture_test`, `cargo test -p
  toron` (+ `-p wendelon` for the facade hook), `clippy … -D warnings`,
  `git diff --check` (capped-linker procedure).
- Roadmap: 060 promoted — closes the 059 → 073 → 060 end-to-end cancellation
  arc; the no-Wendelon-claim caveat retired.

## Phase 8 — (verification) end-to-end smoke

- A fixture-level path: cancel a Toron query mid-`TABLE_QUERY_REQ` and assert
  the Wendelon attempt receives `TABLE_QUERY_CANCEL_REQ` (or, in the unit
  seam, that the registry fan-out fired the surfaced attempt) — the chain's
  observable closure.

## Watch-items

1. **Surface, not generate** — register the **hook-surfaced** id; a
   scan-generated id is overwritten at `core.rs:599` → **no-op cancel**.
2. **Parent-fired only** — RAII = removal/disarm; explicit cancel fires from
   the parent `cancel()`, never a scan-drop guard or per-scan watcher (059's
   `select!` teardown races both).
3. **Worker fires before drop-at-await** — snapshot on `CancelTask` before
   the task future is dropped.
4. **Client never blocks on the ack** (059 D17); fan-out semaphore-bounded
   with fixed timeouts; cancel-racing-completion is harmless (073 →
   `TERMINAL`).
5. **QUERY_CANCELLED → Cancelled** mapping (not Internal/ResourceExhausted);
   descriptor-evolved unchanged.
6. **Guard retarget** — `adr059_no_wendelon_storage_cancellation_claim` would
   go false; retarget explicitly; meta-guard scoped `…_except_retarget`.
7. **No Toron identity** on the Wendelon wire or metric labels; in-memory
   scans ignore the context.

## Acceptance checklist

- [ ] Phase 1: facade attempt-hook (lower-loop impl, before write, returns
      RAII guard) + facade cancel helper; ordinary APIs unchanged.
- [ ] Phase 2: `DownstreamAttemptRegistry` + RAII `AttemptGuard` + bounded
      detached idempotent fan-out + identity-free outcomes.
- [ ] Phase 3: attached to `ActiveQueryHandle`; cancel snapshots downstream +
      remote-task without blocking.
- [ ] Phase 4: beside the worker active-task signal; fires on `CancelTask`
      before drop-at-await.
- [ ] Phase 5: optional context on `ExecutionContext`/`ScanRequest`; in-memory
      ignores; scan registers via hook + checks already-cancelled.
- [ ] Phase 6: `QUERY_CANCELLED`→`Cancelled`; separate identity-free counter.
- [ ] Phase 7-8: 20+3+meta guards, 059-guard retarget, 14 fixtures, evidence
      + roadmap; end-to-end closure; no generate-in-scan / scan-watcher /
      new-wire / Toron-identity drift.
