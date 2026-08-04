# Impl Plan — ADR 124: Bounded Live-Partition Backpressure

**Migration for** `plans/adr/0124_bounded_live_partition_backpressure.md`.
**Scope:** Toron interior — `distributed/partition_store.rs` (trait widening +
`LivePartition` extension + append/pull/cancel/seal protocol) +
`server/mod.rs` (live producer async sink). **Non-scope:** `exec.rs`,
`distributed/fragment.rs`, `distributed/scheduler.rs` (consumer-side
scheduling unchanged), `task.rs`, `binder.rs`, `pbc.rs`, `ir*.rs`,
`sql_planner.rs`, `rune_engine.rs`, Wendelon source. Producer-side
`partition_batches` retention (`server/mod.rs:470-471, :494, :501-510`) is
**preserved** per ADR D10 — named follow-on, not in 124.

## Pre-flight anchors (verify before editing)

| Concern | Anchor |
|---|---|
| `PartitionStore` trait def | `partition_store.rs:25-72` |
| Default `append_live` (returns `LiveUnsupported`) | `partition_store.rs:49-51` |
| `LivePartition` struct | `partition_store.rs:279-287` |
| `LivePartition::placeholder` | `partition_store.rs:289-301` |
| `InMemoryPartitionStore::append_live` | `partition_store.rs:440-464` |
| `seal_live` | `partition_store.rs:466-525` |
| `cancel_live` | `partition_store.rs:527-535` |
| `pull_live` | `partition_store.rs:537-598` |
| `pull_live` unfold success branch | `partition_store.rs:572-584` |
| `HybridPartitionStore::append_live` | `partition_store.rs:962-964` |
| `EncryptedSpillPartitionStore` impl block | `partition_store.rs:740` |
| `execute_live_shuffle_producer` | `server/mod.rs:425-560` |
| `append_batch` closure + `SyncStreamableBatchSink` wrap | `server/mod.rs:472-499` |
| `StreamableBatchSink` trait | `server/mod.rs:3070` |
| `SyncStreamableBatchSink` | `server/mod.rs:3090-3110` |
| `async_trait` in tree (use the existing dep) | `distributed/scheduler.rs:7` |

## Phase 0 — Failing gates first (test-driven)

Land all behavior gates (G-A through G-I) before implementation. For backlog
gates, put the tests in `partition_store.rs`'s unit-test module so they can use
the real private `LIVE_BACKLOG_DEPTH`; append exactly `DEPTH` chunks, spawn a
`DEPTH + 1` append via `tokio::spawn`, assert the spawned future is pending via
`tokio::time::timeout(Duration::from_millis(50), ...)` returning `Err(_)`.

Test homes:
- G-A `live_producer_blocks_when_backlog_full_until_consumer_pulls`,
  G-B `pull_releases_one_backlog_slot_once`,
  G-D `cancel_live_wakes_blocked_appender_with_cancellation`,
  G-E `seal_with_live_chunks_materializes_sealed_payload`,
  G-F `late_sealed_pull_after_streaming_succeeds`,
  G-G `retry_pull_live_restarts_from_index_zero`,
  G-I `shuffle_byte_budget_still_enforced_at_live_append`:
  `src/distributed/partition_store.rs` unit tests.
- G-C `scan_pull_pauses_when_live_append_awaits` (integration with a
  test-storage scan counter): `server/mod.rs` unit tests beside the existing
  live producer / ADR 123 fixtures.
- G-H `adr_123_live_input_dispatch_still_works` (regression selector,
  parametric over 8 join kinds): re-use existing 123 fixture.

Honesty check ([[feedback_cut_must_deliver_the_name]]): new backpressure gates
(G-A/G-B/G-C/G-D) must fail against `main` before implementation; regression
pins (G-E/G-F/G-G/G-H/G-I) should pass on `main` and stay green. Especially
G-B, which would silently pass if `farthest_released_index` is omitted but only
becomes detectable through accumulated retry-replay drift — the test must
construct the double-release scenario explicitly.

## Phase 1 — Trait widening (async_trait)

```rust
// partition_store.rs:25 (top of trait definition)
#[async_trait::async_trait]
pub trait PartitionStore: Send + Sync {
    // ...
    async fn append_live(&self, payload: PartitionPayload)
        -> Result<(), PartitionStoreError> {
        Err(PartitionStoreError::LiveUnsupported)
    }
    // ...
}
```

Annotate the three impl blocks:
- `partition_store.rs:326` — `#[async_trait::async_trait] impl PartitionStore
  for InMemoryPartitionStore { ... }`
- `partition_store.rs:740` — `#[async_trait::async_trait] impl PartitionStore
  for EncryptedSpillPartitionStore { ... }` (no override needed — uses
  default).
- `partition_store.rs:910` — `#[async_trait::async_trait] impl PartitionStore
  for HybridPartitionStore { ... }` — its `append_live` becomes
  `async fn append_live(&self, payload) { self.in_memory.append_live(payload).await }`.

Update direct test/helper calls to `.await`: `partition_store.rs:1122`,
`:1160`, `:1202`, `:1228`, plus `server/mod.rs:2611` and the
`seed_live_partition` helper around `server/mod.rs:2865`. Convert synchronous
unit tests/helpers to async where needed; several existing tests are already
`#[tokio::test]`.

## Phase 2 — `LivePartition` state extension

```rust
// partition_store.rs (module level, near other consts)
const LIVE_BACKLOG_DEPTH: usize = 4;

// partition_store.rs:279 (extend struct)
struct LivePartition {
    opened: bool,
    chunks: Vec<PartitionPayload>,
    reservations: Vec<BudgetReservation>,
    sealed: bool,
    cancelled: Option<PartitionMissingReason>,
    downstream_cancellation: Option<DownstreamCancellationContext>,
    notify: Arc<Notify>,
    // new:
    pacing_semaphore: Arc<tokio::sync::Semaphore>,
    farthest_released_index: usize,
}

// partition_store.rs:289 (extend placeholder)
impl LivePartition {
    fn placeholder() -> Self {
        Self {
            // ... existing ...
            pacing_semaphore: Arc::new(Semaphore::new(LIVE_BACKLOG_DEPTH)),
            farthest_released_index: 0,
        }
    }
}
```

Do not replace `chunks: Vec<PartitionPayload>` — it is the replay ledger +
seal source. Do not replace `notify` — late-readers/seal still wake on it.

## Phase 3 — Async append protocol (ADR D4)

Refactor `InMemoryPartitionStore::append_live` (`partition_store.rs:440-464`)
to async:

```rust
async fn append_live(&self, payload: PartitionPayload)
    -> Result<(), PartitionStoreError>
{
    // (1) Pre-acquire: validate + clone semaphore.
    let semaphore = {
        let inner = self.inner.read().expect("partition store poisoned");
        if inner.cancelled_queries.contains(&payload.address.query_id) {
            return Err(PartitionStoreError::CancelledQuery {
                query_id: payload.address.query_id,
            });
        }
        match inner.live_partitions.get(&payload.address) {
            Some(live) if live.cancelled.is_some() || live.sealed => {
                return Err(PartitionStoreError::CancelledQuery {
                    query_id: payload.address.query_id,
                });
            }
            Some(live) => live.pacing_semaphore.clone(),
            None => {
                drop(inner);
                let mut inner = self.inner.write().expect("partition store poisoned");
                let live = inner
                    .live_partitions
                    .entry(payload.address)
                    .or_insert_with(LivePartition::placeholder);
                live.pacing_semaphore.clone()
            }
        }
    };

    // (2) Await capacity. Closed semaphore → cancellation.
    let permit = semaphore
        .acquire_owned()
        .await
        .map_err(|_| PartitionStoreError::CancelledQuery {
            query_id: payload.address.query_id,
        })?;

    // (3) Reserve bytes (preserves global shuffle byte budget).
    let reservation = self
        .budget
        .reserve(BudgetDimension::ShuffleBytes, payload.size_bytes())?;
    // (permit drops here on early return → capacity released.)

    // (4) Write lock; re-check after await window; push.
    let mut inner = self.inner.write().expect("partition store poisoned");
    if inner.cancelled_queries.contains(&payload.address.query_id) {
        return Err(PartitionStoreError::CancelledQuery {
            query_id: payload.address.query_id,
        });
    }
    let live = inner
        .live_partitions
        .entry(payload.address)
        .or_insert_with(LivePartition::placeholder);
    if live.cancelled.is_some() || live.sealed {
        return Err(PartitionStoreError::CancelledQuery {
            query_id: payload.address.query_id,
        });
    }
    live.opened = true;
    live.chunks.push(payload);
    live.reservations.push(reservation);
    live.notify.notify_waiters();

    // (5) Consume the permit slot — released later by pull_live first-read.
    permit.forget();
    Ok(())
}
```

Update `HybridPartitionStore::append_live` (`:962-964`) to await the inner
store.

Honesty check: if any of steps (3) or (4)'s re-checks fail, the `permit`
binding drops normally, returning the slot. This is the
`AcquireError ↔ CancelledQuery` invariant pinned by Gᵃ⁴.

## Phase 4 — Release capacity on first read (ADR D5)

Modify `pull_live`'s `stream::unfold` body (`partition_store.rs:559-597`).
The success branch where `live.chunks.get(index)` returns `Some(chunk)`
currently runs under a read lock; switch that branch to take a write lock so
we can mutate `farthest_released_index`:

```rust
// Inside the unfold async move loop:
let notify = {
    let guard = inner.read().expect("partition store poisoned");
    if guard.cancelled_queries.contains(&address.query_id) {
        return Some((
            Err(PartitionMissingReason::CancelledTaskCleanup),
            (inner.clone(), address, index),
        ));
    }
    let live = guard.live_partitions.get(&address)?;
    if let Some(reason) = &live.cancelled {
        return Some((Err(reason.clone()), (inner.clone(), address, index)));
    }
    if live.chunks.get(index).is_some() {
        drop(guard);
        let mut wguard = inner.write().expect("partition store poisoned");
        let live = wguard.live_partitions.get_mut(&address)?;
        let chunk = live.chunks.get(index).cloned()?;
        if index == live.farthest_released_index {
            live.farthest_released_index += 1;
            live.pacing_semaphore.add_permits(1);
        }
        return Some((Ok(chunk), (inner.clone(), address, index + 1)));
    }
    if live.sealed {
        return None;
    }
    live.notify.clone()
};
notify.notified().await;
```

**Invariant pinned by Gᵃ⁵:** `add_permits(1)` is called **only** when
`index == live.farthest_released_index`. Retry-replay (a fresh `pull_live`
attempt) restarts at `index=0`; for `index < farthest_released_index` the
release path is skipped — no double-release, the backlog bound holds across
retries.

## Phase 5 — Cancel + seal (ADR D6 + D8)

In `cancel_live` (`:527-535`):
```rust
fn cancel_live(&self, address: PartitionAddress, reason: PartitionMissingReason) {
    let mut inner = self.inner.write().expect("partition store poisoned");
    let live = inner
        .live_partitions
        .entry(address)
        .or_insert_with(LivePartition::placeholder);
    live.cancelled = Some(reason);
    live.notify.notify_waiters();
    live.pacing_semaphore.close();  // wakes blocked appenders
}
```

Equivalent semaphore-close in the query-cancellation path that iterates live
partitions on query cancel.

In `seal_live` (`:466-525`): preserve the body verbatim through chunk
materialization. After `live.sealed = true` (`:497, :510`), optionally
`live.pacing_semaphore.close()` to reject late appends cleanly. The
`PartitionStoreError::CancelledQuery` mapping (existing at `:454-458`)
already covers post-seal append rejection, so the semaphore close is
defense-in-depth — the existing `if live.sealed` check at the entry of the
re-acquire write lock in `append_live` (Phase 3 step 4) is the primary
rejection.

## Phase 6 — Live producer async sink (ADR D7)

In `server/mod.rs::execute_live_shuffle_producer` (`:425-560`), replace the
sync `append_batch` closure (`:472-497`) + `SyncStreamableBatchSink::new`
wrap (`:498`) with a typed async sink:

```rust
struct LiveAppendStreamableBatchSink<'a> {
    executor: &'a Executor,
    partition_store: Arc<dyn PartitionStore>,
    addresses: &'a [PartitionAddress],
    partition_count: PartitionCount,
    partition_key: &'a BoundExprProgram,
    partition_batches: &'a mut Vec<Vec<RecordBatch>>,
}

impl StreamableBatchSink for LiveAppendStreamableBatchSink<'_> {
    fn emit<'b>(&'b mut self, batch: RecordBatch) -> StreamableBatchFuture<'b> {
        Box::pin(async move {
            let parts = partition_record_batch(
                self.executor, &batch, self.partition_count.get(), self.partition_key,
            )?;
            for (partition, partition_batch) in parts.into_iter().enumerate() {
                let Some(partition_batch) = partition_batch else { continue; };
                let bytes = encode_record_batch(&partition_batch)?;
                let digest = ArrowIpcDigest::new(wendelon::hash::blake3::digest(&bytes));
                let payload = PartitionPayload::new(
                    self.addresses[partition],
                    bytes,
                    RowCount::new(u64::try_from(partition_batch.num_rows())
                        .expect("row count fits u64")),
                    digest,
                );
                // The async append — pauses upstream scan when backlog full.
                self.partition_store
                    .append_live(payload)
                    .await
                    .map_err(partition_store_publish_error)?;
                self.partition_batches[partition].push(partition_batch);
            }
            Ok(true)
        })
    }
}
```

Replace the call at `:499`:
```rust
let mut append_sink = LiveAppendStreamableBatchSink {
    executor: &executor,
    partition_store: partition_store.clone(),
    addresses: &addresses,
    partition_count: *partition_count,
    partition_key,
    partition_batches: &mut partition_batches,
};
drive_streamable_plan(input, &executor, morsel, &mut append_sink).await?;
```

`partition_batches` accumulation at `:494` is preserved — this is the ADR
D10 non-scope. `publish_task_output` call at `:511-514` is unchanged.

Note: `StreamableBatchSink` already returns `StreamableBatchFuture<'a>` from
`emit`; do not convert the trait to `async_trait`. The new sink should follow
the existing boxed-future pattern used by `WorkerTaskBatchSink` and friends.

## Phase 7 — Architecture guards

Add the 11 guards (Gᵃ¹–Gᵃ¹¹) to the existing distributed-execution architecture
guard module (`tests/architecture/distributed_execution/scheduler_dispatch.rs`)
or a sibling file registered from that module. Follow the current ADR 119/123
source-scan pattern: each guard reads the file under test and runs structural
assertions (substring presence/absence + named-constant detection). Examples:

- Gᵃ² (named constant): scan `partition_store.rs` for `const
  LIVE_BACKLOG_DEPTH: usize`.
- Gᵃ⁴ (acquire-forget ordering): scan for `acquire_owned`, `permit.forget`,
  AND that the acquire literal precedes the byte-reservation literal which
  precedes the chunk push.
- Gᵃ⁵ (release-at-frontier): scan for both `farthest_released_index` AND
  `add_permits(1)` co-located within a block guarded by the equality check.
- Gᵃ⁶ (Codex A pin): scan `seal_live` body for
  `inner.partitions.insert(address, payload)` AND the chunk-concatenation
  loop.

Update 123's `adr123_executor_pbc_wendelon_ir_fragmenter_unchanged` (G6)
reject-set to include `adr124_*` markers (or, equivalently, the 124 guard
Gᵃ¹⁰ asserts the same emptiness from the 124 side — discipline #4
reconciliation).

## Phase 8 — Verification

Run in order:
```sh
cargo fmt
cargo test -p toron distributed::partition_store
cargo test -p toron distributed::scheduler::tests
cargo test -p toron --test architecture_test adr119 adr120 adr123 adr124
cargo test -p toron
cargo check -p toron --all-targets
git diff --check
```

The `--all-targets` check is per
[[feedback_verify_all_targets_on_new_ir_field]]: trait-signature changes
(async `append_live`) ripple to benches/examples, which `cargo test` alone
skips.

## Risk register

- **Lock-protocol deadlock:** the append protocol drops the read lock, awaits
  semaphore, then takes write lock. Two concurrent appenders for *different*
  partitions never contend on the semaphore (per-partition); for the *same*
  partition the semaphore serializes correctly. Cancellation during the await
  window is handled by step (4)'s re-check. Pinned by G-D.
- **Permit drift on retry-replay:** the load-bearing risk —
  `farthest_released_index` is the mechanism. Pinned by G-B. If the guard is
  removed or inverted, the test catches it deterministically.
- **`SyncStreamableBatchSink` removal scope creep:** the existing sync sink
  has other uses (`:537`, `:2719`). Only the live producer's append path
  switches to async; the wrappers (`FilterStreamableBatchSink`, etc.) and
  other sync uses are out of scope. Pinned by Gᵃ⁹ (scoped to
  `execute_live_shuffle_producer`).
- **Test-time flakiness on the `tokio::time::timeout` assertion (G-A):** use
  `pending!()` / explicit yield-yields rather than wall-clock sleep where
  possible. If wall-clock unavoidable, use a generous millisecond bound (50ms+)
  and a tight assertion that `timeout` returns `Err`.
- **`async_trait` virtual-dispatch overhead:** `append_live` runs once per
  emitted batch; the cost is negligible vs the partitioning + encoding +
  digest work already in the path. Not a regression risk.
- **`partition_batches` retention overclaim (ADR D10):** the doctrine + the
  guard Gᵃ⁹'s scope explicitly preserve this; any reviewer who reads
  "bounded live shuffle" as "bounded total live shuffle memory" gets
  redirected to the doctrine's "honesty boundary" section + the named
  follow-on. The cut is **named** "bounded unread backlog" precisely to
  prevent this drift.

## Acceptance

ADR 124 is accepted when:
- The full 9-gate behavior set (G-A through G-I) passes.
- The 11-guard architecture set (Gᵃ¹ through Gᵃ¹¹) passes.
- 123's parametric 8-join-kind gate passes verbatim.
- `cargo check -p toron --all-targets` passes.
- A `git diff --check` clean diff.
- No file outside `partition_store.rs`, `server/mod.rs`, and the new
  test/guard files is touched (modulo the `adr123` guard reconcile in
  Phase 7).
- The promoted ADR doctrine names the `partition_batches` honesty boundary
  (D10) explicitly. **The cut delivers its name: bounded unread backlog, not
  bounded total memory.**
