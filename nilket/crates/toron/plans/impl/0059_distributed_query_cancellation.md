# Impl Plan 059: Distributed Query Cancellation Completeness

**Status:** Promoted, paired with `plans/adr/0059_distributed_query_cancellation.md`.
**Posture:** Toron-ring only. Reuses the existing `CancelTask`/`TaskCancelled`
bidi wire; **no new wire variant, no `ControlMessage`, no unidirectional
stream, no transport redesign** (transport is `open_bi`-only). No Wendelon /
SQL-grammar change. Eight phases, dependency-ordered. The spine (worker
active-task abort) is the novel piece; everything else routes through it.

## Invariants carried in

- ADR 049-058 behavior unchanged; pre-admission rejects emit no cancel.
- `crates/nilket` runtime-free; `toron_fixture_worker.rs` unchanged;
  brand-clean / no-`*V1`. Node-local. No Wendelon storage-cancellation claim.

## Phase 0 — Audit (verified anchors)

- `daemon.rs:771` wraps `coordinator.execute_query_with_context(&plan,
  context)` in `tokio::time::timeout`; on elapse returns
  `SqlErrorCode::StatementTimeout` and **drops** the future — the
  cancel-trigger site. `daemon.rs:817` SQL `Cancel` → `CancellationUnsupported`
  (replace). `handle_sql_connection` (~daemon.rs:704).
- `server/mod.rs:348` `StartTask` runs `execute_task_local_with_telemetry(&
  spec).await` **inline** (streams `TaskBatch`, then `publish_task_output`
  ~:382) — the future to race against cancel. `mod.rs:416` `CancelTask`
  arm **acks only** — make it abort via the registry. `mod.rs:1346`
  `cancel_task_on_worker` (bidi: sends `CancelTask`, awaits `TaskCancelled`).
- `wire.rs:34` `CancelTask`/`TaskCancelled` + payloads (`:246/:251`) — reuse.
- `server/transport.rs` / `quic.rs:126` `connect_stream` → `open_bi()`;
  **no `open_uni`** — out-of-band cancel = a fresh **bidi** stream.
- `distributed/scheduler.rs::cancel_query` (coordinator-local cleanup — keep
  + invoke); `ExecutionContext.query_id`; `TenantId`.

## Phase 1 — Worker active-task registry + abortable execution (server/mod.rs) — the spine

- `ActiveTaskRegistry`: `Mutex<HashMap<TaskIdentity, CancelSignal>>`. On
  `StartTask`, **register before sending `TaskStarted`**; deregister via an
  **RAII guard** so every terminal path (complete/error/cancel/publish-fail/
  send-fail/connection-drop) removes the entry.
- Make execution abortable: `tokio::select! { r =
  execute_task_local_with_telemetry(&spec) => …, _ = signal.cancelled() =>
  unwind }`. Drop at the await boundary; partial results discarded. **No
  token threading through operators.**
- **Pre-publish suppression:** before `publish_task_output` / `TaskComplete`,
  check the signal; if cancelled, suppress publish + terminal success +
  further `TaskBatch` sends (D15/D16). Pin the publish-during-cancel behavior
  with a fixture (atomic publish, or local cleanup makes output unreachable).

## Phase 2 — Worker CancelTask abort + early-cancel tombstone (server/mod.rs)

- `CancelTask` arm (mod.rs:416): look up `TaskIdentity` → fire the signal →
  ack `TaskCancelled` (idempotent for missing/terminal/pending). **Not
  ack-only.**
- **Pending-cancel tombstone:** `CancelTask` for an unregistered identity
  records a tombstone; `StartTask` checks it at registration and aborts
  immediately if present. **Bounded** (count/TTL), consumed on `StartTask`,
  evicted when stale (D13/D14).
- **Concurrency:** confirm the worker accept loop spawns per-connection/stream
  handlers so the fresh cancel stream runs **concurrently** with the busy
  `StartTask` stream (D10).

## Phase 3 — Out-of-band remote cancel delivery (server/mod.rs, transport)

- Coordinator cancellation fan-out opens a **fresh bidi** worker stream
  (existing `connect`/`connect_stream`) and calls `cancel_task_on_worker`
  (existing `CancelTask`→`TaskCancelled`). **No** new endpoint/uni/
  `ControlMessage`.

## Phase 4 — Coordinator active-query registry + cancellation handle (coordinator.rs)

- `ActiveQueryRegistry`: small wrapper over `Mutex<HashMap<(TenantId,
  QueryId), CancelHandle>>`, **RAII insert/remove** on all terminal paths
  (D4). The `CancelHandle` records dispatched `(TaskIdentity, WorkerEntry)`
  — or an equivalent worker record with the stable identity required by
  `connect_worker` — **as they become real** and is **idempotent**. Cancel-before-
  any-dispatch → local cleanup only, no remote `CancelTask`.
- The handle invokes `cancel_query`'s existing coordinator-local cleanup
  (D19), which still runs if remote fan-out fails.

## Phase 5 — Triggers (daemon.rs, coordinator.rs)

- **Timeout:** fire the handle **before** returning `StatementTimeout`
  (daemon.rs:771) (D6).
- **Disconnect:** race execution against `connection.closed()` after
  `Execute` decode → fire the handle; before decode, nothing (D7).
- **SQL `Cancel`:** handle the existing `SqlMessage::Cancel` frame in
  `handle_sql_connection` (replace `CancellationUnsupported`); authenticate/
  resolve tenant like `Execute`; look up `(TenantId, QueryId)`; fire the
  handle; return `Cancelled` for accepted/missing/terminal/cross-tenant
  (oracle-safe) (D8).

## Phase 6 — Fan-out / wait semantics (coordinator.rs)

- Client response **never** awaits remote acks (D17): return the outcome
  promptly; the `CancelTask` fan-out is **detached, bounded** background
  cleanup owned by coordinator/runtime state (not untracked) (D18), with a
  **fixed internal** ack-wait bound; unreachable workers fall back to
  worker-loss semantics. Resource release via normal RAII (D20).

## Phase 7 — Metrics (network/observability.rs)

- Aggregate identity-free counters: queries cancelled, remote cancel
  requests, cancel acks/timeouts — trigger/outcome labels only; **no**
  identity labels (D22).

## Phase 8 — Guards + reconciliation + evidence + roadmap

- 20 architecture + 3 convention guards (ADR list); **retarget** the
  `CancellationUnsupported` guard → SQL cancel uses the existing frame, no
  new variant. Add architecture tests **before/with** behavior fixtures so
  the D-set can't drift toward the rejected control-protocol design.
  Meta-guard extended through 058.
- 19 fixtures (ADR list).
- `plans/impl/0059_distributed_query_cancellation_evidence.md`:
  `architecture_test adr059`, full `architecture_test`, `cargo test -p
  toron`, scheduler/server/sql_listener suite counts, `clippy -p toron
  --all-targets -- -D warnings`, `git diff --check` (capped-linker procedure).
- Roadmap: 059 promoted (completes the governance arc's resource accounting).

## Watch-items

1. **Tombstone must be bounded** (count/TTL, consumed on `StartTask`) — else
   a cancel-before-dispatch leaves a permanent entry: new unbounded map.
2. **Publish-boundary drop-safety** — the one place a mid-abort could leave
   partial partition state; suppress-before-publish, and pin the
   during-publish behavior with a fixture.
3. **Client response never awaits remote acks**; fan-out detached **but
   bounded/tracked** (not unbounded detached work at shutdown).
4. **Concurrent cancel stream** — the worker must handle it while the task
   stream is busy; verify the accept loop spawns concurrent handlers.
5. **Reuse the bidi wire** — no `ControlMessage`/uni (transport is
   `open_bi`-only); guard #11.
6. **SQL `Cancel` oracle-safety** — identical surface for accepted/missing/
   terminal/cross-tenant; never leak cross-tenant existence.
7. **No pre-admission cancel** (055-058 reject pre-dispatch); **no Wendelon
   storage-cancellation claim**.

## Acceptance checklist

- [ ] Phase 1: worker registry register-before-`TaskStarted`, RAII
      deregister; `select!` abort; pre-publish suppression.
- [ ] Phase 2: `CancelTask` aborts (not ack-only); bounded tombstone;
      concurrent cancel-stream handling.
- [ ] Phase 3: out-of-band via fresh bidi stream + existing wire; no new
      protocol/endpoint.
- [ ] Phase 4: coordinator `(TenantId,QueryId)` registry, RAII, idempotent
      handle owning dispatched `(TaskIdentity, WorkerEntry)`.
- [ ] Phase 5: timeout fires before `StatementTimeout`; disconnect after
      decode; SQL `Cancel` via existing frame, oracle-safe.
- [ ] Phase 6: client never blocks on acks; bounded tracked fan-out; RAII
      release.
- [ ] Phase 7-8: identity-free metrics; 20+3 guards, `CancellationUnsupported`
      retarget, 19 fixtures, evidence + roadmap; no control-protocol /
      Wendelon / SQL-grammar drift.
