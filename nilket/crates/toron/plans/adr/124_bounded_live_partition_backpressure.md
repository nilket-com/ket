# ADR 124: Bounded Live-Partition Backpressure

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** —
converged on the additive-pacing-over-replay-ledger design after two
concessions across three iters (Claude conceded broadcast → bounded
backpressure on substrate grounding; Gemini conceded transient-channel
recovery break) and Codex's two iter-2 sharpenings (`farthest_released_index`
release invariant + `partition_batches` honesty caveat) folded in iter-3.
**Category:** Toron interior — distributed execution substrate
(`distributed/partition_store.rs` + the live producer sink in
`server/mod.rs::execute_live_shuffle_producer`).
**Predecessors:** 119 (introduced `append_live`/`pull_live` and the
`LivePartition.chunks` ledger this ADR paces); 120 (end-to-end bounded first
rows — the cost-bound chain this ADR completes producer-side for streamable
joins); 123 (multi-upstream live dispatch — iter-3 honestly dropped the
"natural backpressure" gate; this ADR repairs the producer half of that
boundary).
**Migration:** `plans/impl/124_bounded_live_partition_backpressure.md`.

## Doctrine — pace the producer, preserve replay, name the honest scope

> The 123 honesty boundary the council dropped iter-3: producer-side
> backpressure through the live partition boundary. This ADR delivers a
> **per-partition unread-backlog bound** that paces the producer when the live
> consumer is not pulling — observable end-to-end as the upstream Wendelon scan
> pausing while `append_live` awaits semaphore capacity. Recovery semantics
> (sealed materialization, index-replay, late sealed pull) stay bit-identical
> to today: the pacing semaphore is **additive over the existing replay
> ledger**, not a data-model swap.

**The honesty boundary (Codex iter-2 caveat, doctrine-level pin):**

- ADR 124 bounds **unread live backlog** per partition — at most
  `LIVE_BACKLOG_DEPTH` chunks may sit appended-but-unread before the producer
  awaits.
- ADR 124 does **not** bound total live shuffle memory. The producer-side
  `partition_batches: Vec<Vec<RecordBatch>>` accumulator at
  `server/mod.rs:470-471` still retains every emitted batch through
  `publish_task_output`, independent of the partition store ledger. Removing
  that producer-side retention is a separately-named follow-on (build the
  manifest/output directly from the live ledger, or redesign `seal_live` to
  drain to the sealed map without re-materialization).

What the ADR can claim:
1. A live producer cannot run more than `LIVE_BACKLOG_DEPTH` chunks ahead of a
   non-pulling live consumer for a single partition.
2. When the unread live backlog reaches the bound, `append_live` awaits
   capacity.
3. The upstream Wendelon storage scan pauses while `append_live` awaits
   (because `drive_streamable_plan`'s sink awaits append).
4. Sealed replay semantics intact: `pull_live` retries from index zero;
   `seal_live` materializes a sealed payload from `live.chunks`; late sealed
   pulls (`pull`, not `pull_live`) succeed.

What the ADR does **not** claim:
- Constant-memory live shuffle for arbitrarily large outputs.
- Executor build/probe reorder.
- Output-side joins-as-streamable-producers.
- Multi-producer fan-in / remote live transport.
- End-to-end natural backpressure for joins (composes 124 with executor reorder
  + output-side streaming).
- Rune/SQL broadcast authoring (follow-on, distinct topic).

## Decisions

**D1 — Additive pacing semaphore over the existing replay ledger.** Add a
`pacing_semaphore: Arc<tokio::sync::Semaphore>` and a `farthest_released_index:
usize` field to `LivePartition`. Initial permits = `LIVE_BACKLOG_DEPTH`. The
`chunks: Vec<PartitionPayload>`, `reservations: Vec<BudgetReservation>`,
`notify: Arc<Notify>`, `cancelled`, `sealed`, and `downstream_cancellation`
fields stay unchanged. The Vec remains the sole storage of payloads; the
semaphore is a pacing primitive layered over it.

**D2 — `LIVE_BACKLOG_DEPTH = 4` (named module-level constant).** Tight enough
that backlog bound is observable in unit tests; loose enough to permit pipeline
overlap. Not wired to `QueryBudget` in V1 (matches the 120 V2 cursor-depth
deferral pattern). Pinned by a guard requiring it be a named constant, not
inlined.

**D3 — Async `PartitionStore::append_live` via `async_trait`.** Change the
trait method signature to async; annotate the trait + three impl blocks
(`InMemoryPartitionStore`, `EncryptedSpillPartitionStore`,
`HybridPartitionStore`) with `#[async_trait::async_trait]`. The dep is already
in tree (`distributed/scheduler.rs:7` + `distributed/cancellation.rs:232`).
Default trait method returns `Err(PartitionStoreError::LiveUnsupported)`;
`EncryptedSpillPartitionStore` continues to use the default;
`HybridPartitionStore` awaits the in-memory store; only
`InMemoryPartitionStore` implements real pacing.

**D4 — Append protocol: acquire → re-check → reserve bytes → push → forget.**
Within `InMemoryPartitionStore::append_live`:

1. Take the lock needed to validate query state and obtain/create the
   `LivePartition` (read first, then drop/re-take write if the entry is absent);
   check not cancelled/sealed; clone `Arc<Semaphore>`.
2. Drop lock; `await semaphore.clone().acquire_owned()`.
3. Reserve `BudgetDimension::ShuffleBytes`.
4. Re-acquire write lock; re-check cancelled/sealed (the await window).
5. Push payload + reservation; `notify_waiters`.
6. `permit.forget()` — the slot stays consumed until the consumer releases it.

If step 3 fails (byte budget exhausted): drop the owned permit (capacity
returned). If step 4 re-check fails (race-cancelled during await): drop the
owned permit. `AcquireError` from a closed semaphore (cancellation, D6) maps
to `PartitionStoreError::CancelledQuery { query_id }`.

**D5 — Release capacity exactly once on first read** (the load-bearing
invariant against retry-replay double-release). In `pull_live`'s unfold
success branch where `live.chunks.get(index)` returns Some:

- Switch the inner-block lock from `read()` to `write()` for the success path.
- Clone the chunk for the yield.
- If `index == live.farthest_released_index`: increment
  `live.farthest_released_index` AND `live.pacing_semaphore.add_permits(1)`.
- Else (retry-replay, `index < farthest_released_index`): do not release.

Without `farthest_released_index`, a `pull_live` retry-from-zero would
re-yield already-released chunks and re-call `add_permits`, drifting the
bound upward permanently. The `(index == farthest_released_index)` guard
ensures one release per chunk across the lifetime of the live partition.

**D6 — Cancellation closes the semaphore.** Extend `cancel_live` and the
query-cancellation path to call `live.pacing_semaphore.close()` in addition
to setting `live.cancelled` and notifying waiters. The blocked appender's
`acquire_owned().await` resolves with `AcquireError`; the impl maps that to
`PartitionStoreError::CancelledQuery`.
`LivePartitionStream::with_cancel_on_drop` and `DownstreamCancellationContext`
are unchanged.

**D7 — Live producer awaits append through an async sink.**
`execute_live_shuffle_producer` (`server/mod.rs:425-560`) stops using
`SyncStreamableBatchSink` for the live append path. A new
`LiveAppendStreamableBatchSink` (or an equivalent async-closure adapter)
implements `StreamableBatchSink` directly, awaiting
`partition_store.append_live(payload).await` inside `emit`. This is the seam
that makes upstream `drive_streamable_plan` scan-pull pause when the live
backlog is full.

**D8 — Preserve `seal_live` materialization** (Gemini conceded; Codex A
locked). `seal_live` keeps its existing chunk-concatenation +
`inner.partitions.insert` shape at `partition_store.rs:466-525`. After seal,
optionally close the semaphore to reject accidental late appends. The Gemini
iter-1 "no sealed materialization" path is rejected on the grounding that
`scheduler.rs:856-885`'s `sealed_ref_for_producer` returns None on a missing
partition entry → `PartitionManifestMismatch` hard error, with **no
producer-restart path** anywhere in the scheduler.

**D9 — Shuffle byte budget unchanged.** `BudgetDimension::ShuffleBytes`
reservation at `append_live` is preserved as the global memory guard; the
semaphore is the per-partition pacing layer. Reservation runs *after*
semaphore acquire (D4 step 3) so that backlog pacing doesn't paper over the
byte budget.

**D10 — Honest non-scope: producer-side `partition_batches` retention stays
in V1.** `execute_live_shuffle_producer` continues to push to
`partition_batches[partition]` at `server/mod.rs:494` for the
`publish_task_output` path at `:511`. Removing that retention requires either
(a) building the publish manifest directly from the live ledger or (b) a
scheduler-side recovery redesign — both substantial cuts named in §"What to
do next." Without this caveat, the ADR's name would silently overclaim
constant-memory live shuffle; pinning it in the doctrine + decisions keeps
the cut [[feedback_cut_must_deliver_the_name]]-honest.

**D11 — Scope boundary.** No `exec.rs` change (123 G6 holds). No
`distributed/fragment.rs` change. No `task.rs::streamable_unary_plan` widening
(123 G2 — Join still rejected). No `drive_streamable_plan` Join change (123
G3). No multi-producer fan-in (123 D2 boundary holds). No IR / PBC / Wendelon
/ SQL grammar / Rune authoring surface. The change surface is precisely:
trait widening + 2 fields on `LivePartition` + 1 named constant + the append
protocol refactor + `pull_live` release path + cancel-close + live producer
async sink + behavior/architecture guards.

## Behavior gates

- **`live_producer_blocks_when_backlog_full_until_consumer_pulls`** (G-A,
  headline) — append exactly `LIVE_BACKLOG_DEPTH` chunks without pulling;
  spawn an additional append; assert pending via `tokio::time::timeout` for
  ≥N ms; pull one chunk; the pending future resolves and writes the chunk.
- **`pull_releases_one_backlog_slot_once`** (G-B, retry-replay invariant) —
  append depth chunks; pull all `depth` (releases `depth` permits); spawn
  `depth` more appends (all admitted concurrently); spawn one more (blocked);
  restart a fresh `pull_live` from index zero — the replay must NOT release
  additional permits (the spawned over-depth append stays blocked). Pin
  against retry-replay double-release.
- **`scan_pull_pauses_when_live_append_awaits`** (G-C) — full integration: a
  live producer over a slow consumer; a counter injected into the upstream
  scan shows row-pull stalls at the backlog boundary and resumes after the
  consumer drains. The end-to-end "producer-side natural backpressure" gate
  iter-3 of 123 honestly dropped, now true at the producer half.
- **`cancel_live_wakes_blocked_appender_with_cancellation`** (G-D) — backlog
  full; an append is pending on `acquire_owned`; call `cancel_live` (or
  cancel the query); the blocked appender wakes with
  `PartitionStoreError::CancelledQuery`.
- **`seal_with_live_chunks_materializes_sealed_payload`** (G-E,
  regression-as-pin for Codex A) — append N chunks; consumer reads K (K<N);
  call `seal_live` with a manifest containing the partition; assert
  `inner.partitions` contains the sealed payload with full row count + digest
  matching the pre-ADR Vec-backed path.
- **`late_sealed_pull_after_streaming_succeeds`** (G-F) — append + consume
  via `pull_live`; drop the live stream; `seal_live`; a new `pull` (not
  `pull_live`) returns the full sealed payload.
- **`retry_pull_live_restarts_from_index_zero`** (G-G) — append N chunks;
  drop pull stream at chunk K; new `pull_live` attempt yields chunks
  `[0..N]` in order (and the replayed chunks `[0..K]` do NOT release new
  permits per G-B).
- **`adr_123_live_input_dispatch_still_works`** (G-H, regression) — 123's
  multi-upstream-streamable-both consumer still dispatches before sealed
  manifests; all 8 HashPartitioned join kinds execute correctly through
  `LiveInputStorageScan` (parametric).
- **`shuffle_byte_budget_still_enforced_at_live_append`** (G-I, regression)
  — a payload exceeding `BudgetDimension::ShuffleBytes` still fails at append
  with the existing budget error; the semaphore permit acquired before
  reservation is released.

Regressions: 115/116/117/118/119/120/121/122/123 suites green;
`distributed::partition_store` full suite green (esp. the seal-from-live-chunks
tests around `partition_store.rs:1200+`).

## Architecture guards

- **`adr124_live_partition_has_pacing_semaphore_and_farthest_released_index`**
  (Gᵃ¹) — `LivePartition` struct contains a `tokio::sync::Semaphore`-typed
  field and a `farthest_released_index: usize` field.
- **`adr124_live_backlog_depth_is_named_constant`** (Gᵃ²) — module exposes
  `LIVE_BACKLOG_DEPTH` as a `const` (not an inlined literal).
- **`adr124_partition_store_append_live_is_async`** (Gᵃ³) — the
  `PartitionStore` trait's `append_live` returns a future (async via
  `async_trait`); the three impl blocks are annotated.
- **`adr124_append_live_acquires_owned_permit_forget_on_success`** (Gᵃ⁴) —
  `InMemoryPartitionStore::append_live` body contains `acquire_owned().await`,
  a successful-push `permit.forget()`, AND ordering: semaphore acquire BEFORE
  byte reservation BEFORE push.
- **`adr124_pull_live_releases_only_at_farthest_released_index`** (Gᵃ⁵) —
  `pull_live`'s success branch contains the
  `index == farthest_released_index` guard around the
  `farthest_released_index += 1` + `add_permits(1)` calls.
- **`adr124_seal_live_materializes_from_chunks`** (Gᵃ⁶) — `seal_live` still
  contains the chunk-concatenation + `inner.partitions.insert` shape at
  `partition_store.rs:501-515`. Pins Codex A against accidental Gemini B drift.
- **`adr124_pull_live_unfolds_by_index_for_replay`** (Gᵃ⁷) — `pull_live`
  still uses the `stream::unfold` shape over `live.chunks` by index.
- **`adr124_cancel_live_closes_pacing_semaphore`** (Gᵃ⁸) — `cancel_live`
  body contains `pacing_semaphore.close()` (or equivalent appender-wake).
- **`adr124_live_shuffle_producer_uses_async_append_sink`** (Gᵃ⁹) —
  `execute_live_shuffle_producer` does NOT construct
  `SyncStreamableBatchSink` for the live-append path; the live append sink
  awaits `partition_store.append_live`.
- **`adr124_no_exec_fragmenter_pbc_wendelon_change`** (Gᵃ¹⁰) — `exec.rs`,
  `distributed/fragment.rs`, `pbc.rs`, Wendelon source contain no `adr124_*`
  markers; the 123 G6 boundary widens its reject set to include `adr124_*`.
- **`adr124_streamable_unary_plan_still_rejects_join`** (Gᵃ¹¹) — 123 G2
  preserved verbatim; `streamable_unary_plan` continues to reject
  `ExecutionPlan::Join`.

**Recipe:** `cargo test -p toron distributed::partition_store` +
`cargo test -p toron distributed::scheduler::tests` + `cargo test -p toron
--test architecture_test adr119 adr120 adr123 adr124` + `cargo test -p toron`
+ `cargo check -p toron --all-targets`.

## Council history

- **iter-0:** Gemini led broadcast (misframed as missing execution); Claude
  recommended multi-producer fan-in (under-scoped per Codex's three additional
  scheduler bails + absent remote live transport); Codex recommended broadcast
  authoring as the corrected small cut. Topic at end-of-iter-0: broadcast.
- **iter-1:** Gemini pivoted to bounded-channel partition store with "no
  sealed materialization"; Codex pivoted to bounded backpressure with stricter
  framing (called Gemini's recovery sub-claim ungrounded); Claude stayed on
  broadcast and claimed stale-3-of-3 (Codex iter-1 explicitly caught it).
  Topic forked.
- **iter-2:** Claude conceded broadcast → bounded backpressure on substrate
  grounding AND independently grounded Gemini's "scheduler restarts producer"
  claim at `scheduler.rs:856-885` (no restart path; `PartitionManifestMismatch`
  hard error); Codex A locked. Codex iter-2 dual + caveat: producer-side
  `partition_batches` retention means this is unread-backlog backpressure, not
  constant-memory live shuffle. Gemini iter-2 conceded the transient-channel
  recovery break.
- **iter-3:** Dual paired. Folds Codex's two iter-2 sharpenings — the
  `farthest_released_index` invariant against retry-replay permit
  double-release (D5) and the producer-batch retention honesty caveat (D10)
  — into the locked design. Promotion-locked.

**Twelfth grounding-correction in the 119→124 arc.** The pattern
[[feedback_ground_the_premise_before_sketching_scope]] applied recursively at
every layer, including peer sub-decisions that look like free wins. The two
substantive iter-2 sharpenings Codex caught (`farthest_released_index` and
the `partition_batches` caveat) are exactly the mechanical-check pattern:
when implementation feels "obvious," grep the actual code path before locking
the decision. Without `farthest_released_index` the bound silently drifts;
without the `partition_batches` caveat the cut silently overclaims its name.

## What to do next

- **Producer-side `partition_batches` retention removal** — build the publish
  manifest directly from the live ledger, or redesign `seal_live` to drain
  the sealed map without re-materialization. The substantial cut to take
  constant-memory live shuffle from claim to truth.
- **Executor build/probe reorder** — pairs with 124 for end-to-end natural
  backpressure for joins (build = smaller side first; probe = stream second).
- **Output-side joins-as-streamable-producers** — widen
  `streamable_unary_plan` for `Join` + restructure the join executor to yield
  mid-probe.
- **Multi-producer fan-in via fragmenter parallelism + remote live transport**
  — Codex iter-0's complete framing of the iter-1 Claude conceded cut.
- **Configurable `LIVE_BACKLOG_DEPTH` via `QueryBudget`** — earns a cut when
  a real workload demonstrates the constant is wrong.
- **Consumer-pull-side bounded channel** — symmetric pacing on the consumer
  side; earns a cut if producer-side pacing exposes consumer-buffering cost.
- **Rune-authored broadcast join + SQL hint authoring** — clean near-follow-on;
  pairs with 122; broadcast execution is fully wired at `fragment.rs:473`.

## Promotion criteria

All 9 behavior gates (G-A through G-I) green; G-A + G-B + G-C are the
headline observables (producer blocks; release-only-at-frontier;
scan-pull-pauses end-to-end); G-E + G-F + G-G are the Codex A
sealed-replay-preservation pins; 115–123 regressions green;
`distributed::partition_store` suite green; Gᵃ¹–Gᵃ¹¹ hold;
`cargo test -p toron` + `cargo test -p toron --test architecture_test
adr119 adr120 adr123 adr124` + `cargo check -p toron --all-targets` green.
Migration: `plans/impl/124_bounded_live_partition_backpressure.md`.
