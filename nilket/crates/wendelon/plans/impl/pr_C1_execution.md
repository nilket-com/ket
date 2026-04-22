# PR C1 Execution Pack

Date: 2026-04-22
Author Role: Lead Architect
Parent Backlog: `plans/impl/workstream_C_backlog.md`

## PR Intent

Eliminate hot-path full `SmState` cloning on the data-path admission surface by publishing and consuming a narrow admission snapshot.

This PR should reduce request-path state cloning without trying to replace every `state_snapshot()` call in the repo.

## Current Code Reality

The hottest `state_snapshot()` reads are in `src/network/data_server.rs`, not `src/network/server.rs`.

The biggest request-path users are:

- `WendelonDataServer::write(...)`
  - reads `e_safe_watermarks` only to compute current epoch
- `WendelonDataServer::read(...)`
  - reads `e_safe_watermarks` for current epoch and linearizable watermark gate
- `prepare_query_execution_plan_with_migration(...)`
  - reads `e_safe_watermarks` for query admission
- `prepare_query_execution_plan(...)`
  - same pattern

Other `state_snapshot()` callers, such as:

- migration handlers
- background reconciler loops
- control-plane handlers

do not need to move in this PR.

## Tactical Goal

Create a published `AdmissionSnapshot` off state-machine apply and consume it in the hot request-path epoch and watermark gates.

## Primary Files

- `src/consensus/raft_storage.rs`
- `src/network/data_server.rs`

Secondary only if needed:

- `src/node.rs`

Preferred outcome: no `node.rs` change is required because `WendelonDataServer::new(...)` can subscribe directly to the state machine’s admission feed.

## Target Types

### New Read-Mostly Snapshot

Add a small type in `src/consensus/raft_storage.rs`:

```rust
pub struct AdmissionSnapshot {
    pub current_epoch: u64,
    pub e_safe_watermarks: Arc<HashMap<u64, HybridTimestamp>>,
}
```

Notes:

- keep it intentionally small
- do not pull `tenants`, `migrations`, or other large maps into v1
- `current_epoch` should be precomputed so callers do not rescan the map every request

### New Publication Surface On The State Machine

Add to `WendelonStateMachine`:

- a `watch::Sender<Arc<AdmissionSnapshot>>`
- `pub fn subscribe_admission_snapshot(&self) -> watch::Receiver<Arc<AdmissionSnapshot>>`

Seed it:

- in `new()`
- in `from_recovery()`

Update it:

- after apply batches commit
- after snapshot install if needed

This should follow the existing observer/publication pattern already used for:

- tier observer
- tenant KEK observer
- checkpoint observer
- finalization broadcast

## Target Functions And Edit Plan

### 1. `WendelonStateMachine::new()` And `from_recovery()`

Required change:

- create the admission watch channel from the initial state

Use a helper:

```rust
fn admission_snapshot_from_state(state: &SmState) -> AdmissionSnapshot
```

### 2. `WendelonStateMachine::apply(...)`

Required change:

- compute the next admission snapshot from the committed state
- send it after the write lock is released, just like the other observer-ish callbacks

Do not:

- make this observer async
- add heavy cloning beyond the small watermark map

### 3. `WendelonDataServer` Struct And Constructor

Add a field such as:

```rust
admission_snapshot: watch::Receiver<Arc<AdmissionSnapshot>>,
```

Initialize it in `WendelonDataServer::new(...)` with:

- `state_machine.subscribe_admission_snapshot()`

Update manual `Clone` impl accordingly.

### 4. Replace Hot Call Sites First

Use the admission snapshot in:

- `WendelonDataServer::write(...)`
- `WendelonDataServer::read(...)`
- `prepare_query_execution_plan_with_migration(...)`
- `prepare_query_execution_plan(...)`

Specifically replace the parts that currently need only:

- `current_epoch`
- `e_safe_watermarks`

Do **not** try to replace snapshot consumers that actually need:

- `migrations`
- `tombstones`
- `tenant_keysets`
- full tenant records

Those can stay on `state_snapshot()` for now.

### 5. Leave These On Full Snapshot In This PR

- `handle_migrate(...)`
- `try_migrate_point_read(...)`
- `sync_tenant_policy_cache_once(...)`
- background sweeper or reclaim loops

Those are not the first-order admission bottleneck.

## Explicit Non-Goals For This PR

- no `io_uring` topology work
- no QUIC admission limits
- no control-plane-wide state snapshot cleanup
- no migration-ledger refactor
- no replacement of every `state_snapshot()` caller

## Suggested Commit Slices

### Commit 1: Add `AdmissionSnapshot` To The State Machine

- define the snapshot type
- add `subscribe_admission_snapshot()`
- seed and publish it from state-machine lifecycle points
- add focused state-machine tests

### Commit 2: Wire The Data Server To The Admission Feed

- add receiver field to `WendelonDataServer`
- initialize in constructor
- update manual clone

### Commit 3: Replace Write And Read Epoch Gates

- swap `state_snapshot()` reads for admission snapshot reads in:
  - `write(...)`
  - `read(...)`

### Commit 4: Replace Query Admission Gates

- move `prepare_query_execution_plan_with_migration(...)`
- move `prepare_query_execution_plan(...)`

### Commit 5: Bench And Clean Up

- add or update microbench hooks
- leave comments explaining why some snapshot users were intentionally deferred

## Required Tests

### State-Machine Tests

- admission snapshot is seeded correctly from recovery
- admission snapshot updates after apply
- admission snapshot current epoch tracks new finalization commits

### Data-Path Regression Tests

Keep stale-epoch and read-gate behavior unchanged in existing tests that cover:

- write epoch staleness
- linearizable read gating
- query epoch gating

### Benchmark Or Measurement Hook

Add a benchmark or microbenchmark hook for the request admission path before and after the snapshot change.

If there is no benchmark harness yet, at minimum add a narrow benchmark scaffold with:

- repeated `write(...)` stale-epoch gate path
- repeated `read(...)` linearizable gate path

## Review Checklist

- does the new snapshot stay intentionally small?
- are only the hot request-path call sites moved in this PR?
- is `state_machine` still available for non-admission paths that genuinely need full state?
- did the PR avoid turning into a repo-wide `state_snapshot()` cleanup?

## Abort Conditions

Stop and split the work if:

- the admission snapshot starts absorbing large state-machine maps unrelated to request admission
- `node.rs` becomes the main site of change
- migrate or control-plane code becomes part of the first landing

If that happens, the PR has lost scope.

## Recommended First Code Move

Start in `src/consensus/raft_storage.rs`:

1. add `AdmissionSnapshot`
2. add `subscribe_admission_snapshot()`
3. publish it from `new()`, `from_recovery()`, and `apply(...)`

Only after that should the data server switch over to consuming it.
