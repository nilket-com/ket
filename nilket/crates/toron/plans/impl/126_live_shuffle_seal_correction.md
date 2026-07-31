# Impl Plan — ADR 126: Live-Shuffle Seal Correction

**Migration for** `plans/adr/126_live_shuffle_seal_correction.md`.
**Scope:** a shared Arrow IPC helper module + `distributed/partition_store.rs`
(schema field, seal rewrite, seal reservation) + `server/mod.rs` (live open with
schema, drop decoded retention, suppress live-producer results, manifest
telemetry) + partition-store/server live-shuffle tests + distributed-execution
architecture guards.
**Non-scope:** scheduler topology, fragmenter, binder, SQL/Rune frontend,
Wendelon, chunk-granular sealed manifests, remote live transport, build/probe
reorder, Arrow IPC wire format.

## Pre-flight anchors (verify before editing)

| Concern | Anchor |
|---|---|
| Live producer decoded retention | `server/mod.rs:470-491` |
| Live producer publish + seal | `server/mod.rs:492-498` |
| Worker `TaskBatch` emission over `report.results` | `server/mod.rs:687-735` |
| Telemetry derived from `results` | `server/mod.rs:300-305` |
| Live append sink pushes retained batches | `server/mod.rs:3221-3260` |
| Arrow IPC helpers (currently private in server) | `server/mod.rs:4233-4250` |
| `publish_task_output` (keep for non-live) | `server/mod.rs:523-593` |
| `PartitionStore::open_live` trait surface | `partition_store.rs:40-49` |
| `LivePartition` fields | `partition_store.rs:283-292` |
| `LivePartition::placeholder` | `partition_store.rs:295-308` |
| `open_live` / `…_with_downstream_cancellation` (InMemory) | `partition_store.rs:419-447` |
| Append reservation + pacing | `partition_store.rs:493-514` |
| `seal_live` (rewrite target) | `partition_store.rs:520-581` |
| Broken concat fallback | `partition_store.rs:556-561` |
| `publish` reservation (deleted from live path) | `partition_store.rs:336-362` |
| Live pull by chunk index (preserved) | `partition_store.rs:636-654` |
| `HybridPartitionStore` open_live forwarding | `partition_store.rs:1028-1049` |
| Synthetic-bytes seal test (must be re-fixtured) | `partition_store.rs:1431` |
| ADR 124 seal guard to retarget | `tests/architecture/distributed_execution/scheduler_dispatch.rs:496` |

## Phase 0 — Failing gates first (test-driven)

Land the behavior gates + guards before implementation; verify the new ones
**fail on `main`** (the honesty check, [[feedback_cut_must_deliver_the_name]]).

- **Re-fixture `seal_with_live_chunks_materializes_sealed_payload`
  (`partition_store.rs:1431`)** to use **real Arrow IPC** chunk bytes (today it
  asserts `chunks.concat()` over synthetic `vec![1,2]`-style bytes — that
  synthetic assertion is exactly what hid the bug). After re-fixturing it must
  **fail on `main`** (decodes only the first chunk). This becomes
  `live_seal_reassembles_multi_chunk_arrow_ipc`.
- Add `live_seal_empty_partition_emits_schema_bearing_ipc` — open live with
  schema, append nothing, seal, pull, decode → 0 rows + correct schema.
- Add seal-budget tests: successful seal-from-chunks reserves `ShuffleBytes`;
  cleanup releases both live and sealed reservations; over-budget seal rejects
  with **no** manifest entry and **no** sealed pull
  (`seal_live_over_shuffle_budget_is_rejected_without_partial_publish`).
- In `server/mod.rs` tests: live shuffle producer `report.results.is_empty()`
  with `Some(ShuffleProducer { manifest })`; manifest row counts match produced
  rows; worker dispatch emits no `TaskBatch` for a live shuffle producer.
- Architecture: add the five `adr126_*` guards and **retarget**
  `adr124_seal_live_materializes_from_chunks` away from
  `bytes.extend_from_slice`.

Expected pass-on-main regressions: ADR 124 pacing + retry-from-zero;
downstream-cancellation; the normal `publish` budget tests.

## Phase 1 — Shared Arrow IPC helper module

Create `crates/toron/src/distributed/arrow_ipc.rs` (declared from
`distributed/mod.rs`) and move `encode_record_batch` / `decode_record_batches`
out of `server/mod.rs` (behavior-neutral; refactoring travels with the ADR,
discipline #6). Add:

```rust
pub(crate) fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>, ArrowIpcError>;
pub(crate) fn decode_record_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, ArrowIpcError>;
pub(crate) fn encode_empty_record_batch(schema: SchemaRef) -> Result<Vec<u8>, ArrowIpcError>;
pub(crate) fn reassemble_partition_chunks(
    schema: Option<SchemaRef>,
    chunks: &[PartitionPayload],
) -> Result<(Vec<u8>, RowCount), ArrowIpcError>;
```

- `reassemble_partition_chunks` opens **one** `StreamWriter`; for each chunk it
  opens a `StreamReader` on `chunk.bytes`, writes each decoded batch into the one
  writer, and **drops the decoded batch before the next chunk** (transient
  bounded to one chunk).
- It sums row counts and **cross-checks decoded rows against payload metadata**,
  erroring on mismatch (catches a corrupt chunk before a bad sealed manifest).
- Empty chunk list **with** a schema → schema-bearing empty stream; empty chunk
  list **without** a schema → internal error.
- Add `PartitionStoreError::ArrowIpc { operation, message }` (or map an
  `ArrowIpcError`); `partition_store_publish_error` keeps mapping non-budget
  store errors to `QueryError::Internal`. Move `server/mod.rs` callers to the
  helper; leave no duplicate private encode/decode behind.

## Phase 2 — Schema through live open

Widen the trait surface:

```rust
fn open_live(&self, address: PartitionAddress, schema: SchemaRef)
    -> Result<(), PartitionStoreError>;
fn open_live_with_downstream_cancellation(
    &self,
    address: PartitionAddress,
    schema: SchemaRef,
    downstream_cancellation: Option<DownstreamCancellationContext>,
) -> Result<(), PartitionStoreError>;
```

- `LivePartition` gains `schema: Option<SchemaRef>`; `placeholder()` sets
  `None` (a `pull_live` may create the entry before the producer opens it).
- `open_live` / `…_with_downstream_cancellation` set `live.schema = Some(schema)`
  and keep the existing duplicate/cancel checks + `notify`.
- `HybridPartitionStore` forwards schema to the in-memory store
  (`:1028-1049`); unsupported stores keep returning `LiveUnsupported`.
- `execute_live_shuffle_producer` opens every partition with `input.schema()`
  (already in hand at `mod.rs:486`). Update all other callers + tests of
  `open_live` to pass schema (verify the full set —
  [[feedback_verify_all_targets_on_new_ir_field]]).

## Phase 3 — Remove decoded retention; build skeleton manifest

In `LiveAppendStreamableBatchSink` (`mod.rs:3221-3260`):
- Remove `partition_batches: &'a mut Vec<Vec<RecordBatch>>` and the
  `partition_batches[partition].push(partition_batch)` at `:3259`.
- Add owned counters: `partition_row_counts: Vec<u64>`,
  `partition_chunk_counts: Vec<u64>`, `emitted_rows: u64`, `emitted_batches:
  u64`; update them after each successful `append_live`, storing no batch.

In `execute_live_shuffle_producer` (`mod.rs:430-499`):
- Drop the `partition_batches` declaration (`:470`) and the `results`
  concat (`:482-491`).
- After `drive_streamable_plan`, build a **skeleton manifest** (one placeholder
  `PartitionEntry` per partition), call `partition_store.seal_live(skeleton)`
  directly, and return
  `(Vec::new(), Some(TaskCompletionOutput::ShuffleProducer { manifest }))`.
- **Do not** call `publish_task_output` here. Keep `publish_task_output`
  (`:523-593`) unchanged for non-live shuffle producers.

## Phase 4 — Reassemble + budget in `seal_live`

Rewrite `InMemoryPartitionStore::seal_live` (`:520-581`):

1. Under the lock: validate not-cancelled; snapshot each target partition's
   address, `schema`, and cloned `chunks`. **Keep the pre-published-payload
   branch** (`:545-554`) for compatibility and direct callers that seal an
   already published live payload. Non-live shuffle producers continue to use
   `publish_task_output`, but they do not rely on `seal_live`.
2. Outside the lock: build chunk-derived sealed payloads via
   `reassemble_partition_chunks` (empty chunks + schema → empty schema-bearing
   stream); compute digest + row count from the sealed bytes.
3. **Reserve `ShuffleBytes` for each new sealed payload before insertion.**
4. Re-lock: re-check cancellation/duplicates; insert payloads + reservations;
   replace manifest entries (skeleton → real size/row-count/digest); mark
   `live.sealed = true`; `pacing_semaphore.close()`; notify waiters.
5. On any pre-insert error: drop reservations, leave no manifest entry / no
   sealed payload (atomicity gate).

**Do not** drop live chunks at seal — in-flight `pull_live` consumers + ADR 124
retry-from-zero require them (D7); refcounted release is a follow-on.

## Phase 5 — Telemetry + worker result channel

- For live shuffle producers, derive `emitted_rows` =
  `manifest.partitions.iter().map(row_count).sum()` and `emitted_batches` =
  `manifest.partitions.len()` (or from the sink counters) in the telemetry
  closure (`mod.rs:300-305`) — not from `results`.
- `published_shuffle_bytes` stays sourced at completion.
- Returning empty `results` already suppresses the worker `TaskBatch` loop
  (`:687-735`); the `live_shuffle_worker_sends_no_task_batch_for_shuffle_producer`
  gate guards against a future refactor reintroducing a separate path.

## Phase 6 — Architecture guards

In `scheduler_dispatch.rs`:
- **Retarget** `adr124_seal_live_materializes_from_chunks`: drop the
  `bytes.extend_from_slice(&chunk.bytes)` assertion; assert the seal body routes
  through the reassembly helper, reserves `ShuffleBytes` for the sealed payload,
  still inserts into `inner.partitions`, **and preserves
  `live.pacing_semaphore.close()`** (the 124 close-on-seal clause must survive —
  Codex + Gemini both flagged). Rename to
  `adr126_seal_live_reassembles_arrow_ipc_not_concat` with a 124-era alias
  comment.
- Add `adr126_live_append_sink_retains_no_record_batches`,
  `adr126_live_producer_does_not_publish_task_output`,
  `adr126_live_open_carries_schema`, `adr126_seal_live_reserves_sealed_payload`.
- Leave the ADR 124 pacing guards intact
  (`adr124_append_live_acquires_owned_permit_forget_on_success`,
  `adr124_pull_live_releases_only_at_farthest_released_index`,
  `adr124_pull_live_unfolds_by_index_for_replay`,
  `adr124_cancel_live_closes_pacing_semaphore`,
  `adr124_live_shuffle_producer_uses_async_append_sink`).

## Verification

```sh
cargo fmt
cargo test -p toron distributed::partition_store
cargo test -p toron --test architecture_test adr124 adr126
cargo test -p toron live_shuffle
cargo test -p toron --test distributed_process_test
cargo test -p toron
cargo check -p toron --all-targets
git diff --check
```

`--all-targets` is load-bearing here: the `LivePartition.schema` field + the
`open_live` signature change ripple to benches/examples that `cargo test` alone
skips ([[feedback_verify_all_targets_on_new_ir_field]]).

## Risk register

- **`open_live` signature churn:** the schema parameter touches every caller +
  the three impls + Hybrid forwarding. Bounded but wide — Phase 2 lands it as one
  mechanical sweep; `--all-targets` catches stragglers.
- **Seal-time transient spike:** the streaming re-encode bounds the decode
  transient to one chunk, but the sealed buffer still grows to full partition
  size (it is the output) and must be reserved (D6). Not constant-memory — named
  honestly.
- **Telemetry regression:** suppressing `results` would zero `emitted_rows` if
  Phase 5 is skipped; the `live_shuffle_producer_telemetry_uses_manifest_rows`
  gate pins it.
- **Atomic seal budget:** reserve-before-insert + drop-on-error is the
  invariant; `seal_live_over_shuffle_budget_is_rejected_without_partial_publish`
  pins it against a partial-publish regression.
- **Fork B rollback** (ADR §Rollback): if schema threading proves too wide,
  pre-publish only empty schema-bearing partitions; never byte-concat, never
  insert unreserved sealed bytes.

## Acceptance

ADR 126 is accepted when: all behavior gates pass (headline
`live_seal_reassembles_multi_chunk_arrow_ipc` failed on `main` beforehand); the
five `adr126_*` guards pass; the retargeted
`adr124_seal_live_materializes_from_chunks` passes and no other 124 guard
regresses; 119/123/124 live suites green; `cargo check -p toron --all-targets`
and `git diff --check` clean. The cut delivers its name: a **correct,
single-source seal** with bounded memory — not constant memory, not single copy.
