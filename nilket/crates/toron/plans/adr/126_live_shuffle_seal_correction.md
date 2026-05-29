# ADR 126: Live-Shuffle Seal Correction

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** on
every load-bearing decision: Fork A schema carriage, Arrow IPC reassembly,
single-source seal, manifest-derived telemetry, atomic seal budget, and the
ADR 124 seal-guard retarget. Converged after Codex's iter-0 *proved* (throwaway
Arrow IPC test) the multi-chunk concat bug Claude's iter-0 only suspected, and
after grounding corrected Claude's "3 copies → 1" headline to a **2-copy steady
state**. Gemini conceded Fork B → Fork A in the final pass on the grounded fact
that the schema is already in hand at `open_live`.
**Category:** Toron interior — distributed live-shuffle substrate (producer seal
path + partition-store budget). Substrate behavior change (correctness +
memory); no new IR / opcode / wire type.
**Predecessors:** 119 (live pipelining — sealed-before-consumer ordering); 123
(multi-upstream live dispatch); 124 (bounded live backpressure — this ADR
rewrites the `seal_live` chunk-materialization body 124's
`adr124_seal_live_materializes_from_chunks` guard pins, and moves where shuffle
bytes are reserved; **retargets that one 124 guard in-change**). 125 deferred
this correction ("Codex's empty-partition Arrow IPC trap").
**Migration:** `plans/impl/126_live_shuffle_seal_correction.md`.

## Doctrine — one source of truth for sealed bytes

> The live-shuffle producer today materializes a partition's bytes **three
> times** and reserves budget **twice**: decoded `RecordBatch`es retained in
> `partition_batches` (`server/mod.rs:470,3259`) and re-encoded into a
> transmitted `TaskBatch` (`mod.rs:300-305,~690`); encoded chunks in
> `live.chunks` reserved at append (`partition_store.rs:495,513`); and a
> *third* re-encode in `publish_task_output` (`mod.rs:564`) reserved again at
> `publish` (`partition_store.rs:362`), which is the payload `seal_live`
> actually consumes (`:545-554`). The chunks branch that *could* seal from the
> retained bytes (`:556-561`) is dead on the normal path **and incorrect**: it
> byte-concatenates N independently-framed Arrow IPC streams, and
> `decode_record_batches`' single `StreamReader` (`mod.rs:4244`) stops at the
> first EOS — so a multi-chunk seal-from-chunks decodes to **only the first
> chunk's rows**. This is invisible to the suite because the one covering test
> uses synthetic non-IPC bytes (`partition_store.rs:1431`). ADR 126 makes
> `live.chunks` the **single source** seal derives sealed bytes from, fixes the
> reassembly into one valid stream, deletes the redundant decoded retention +
> result transmission + live-producer republish, and keeps budget honest.

This is a **correctness repair first** (a latent multi-chunk data-loss bug that
becomes live the moment chunks are the only seal source), memory reduction
second (one fewer full materialization + the wire re-send).

## Name — "seal correction," not "single-pass" / "constant-memory"

The cut is named for what it delivers. We explicitly **reject** "single-pass"
and "constant-memory": **encode count stays 2** (append + seal) and **resident
copy count stays 2** — the chunks become orphaned-for-sealed-reads but remain
resident for in-flight `pull_live` consumers and retry-from-zero replay until
query cleanup. True 1-copy / constant-memory sealing requires a representation
change (the deferred chunk-granular manifest, §What to do next). The guards are
written so that **none can pass on `main`**, yet **none claims a reduction this
ADR does not make** ([[feedback_cut_must_deliver_the_name]]).

## Decisions

**D1 — Seal reassembles Arrow IPC; never byte-concats framed streams.** Each
live chunk is a complete framed stream (`encode_record_batch` opens a
`StreamWriter`, writes one batch, finishes). `seal_live` builds the sealed
payload by **streaming re-encode**: open one output `StreamWriter`; per chunk,
`StreamReader`-decode → write each decoded batch → **drop the decoded batch
before the next chunk**. One valid multi-batch IPC stream; correct digest + row
count; seal-time decode transient bounded to **one chunk**, not all of them.
Reassembly cross-checks decoded rows against payload metadata and errors on
mismatch — a corrupt chunk fails the seal rather than shipping a bad manifest.

**D2 — Schema carriage (Fork A).** `PartitionManifest` carries no schema, and a
never-appended partition has no chunk to recover one from. So `open_live` and
`open_live_with_downstream_cancellation` accept a `SchemaRef`, and
`LivePartition` stores `schema: Option<SchemaRef>` (`Option` because `pull_live`
may create a placeholder before the producer opens the partition). `seal_live`
emits **schema-bearing empty IPC** for never-appended partitions from that
stored schema; non-empty partitions need no external schema (each decoded chunk
carries its own). The producer already holds the schema (`input.schema()`,
`mod.rs:486`), so the plumbing is one field + one seed — Fork A is the complete
cut, chosen over Fork B (which keeps the store schema-agnostic by retaining an
empty-only publish — preserved only as the §Rollback contingency).

**D3 — Single source: drop decoded retention + result transmission.**
`LiveAppendStreamableBatchSink` drops `partition_batches: Vec<Vec<RecordBatch>>`
(`mod.rs:470`, sink push `:3259`), keeping per-partition row/chunk counters
instead. `execute_live_shuffle_producer` returns `results = Vec::new()` with
`output = Some(ShuffleProducer { manifest })`; the worker emits no `TaskBatch`
because it iterates `report.results` (`mod.rs:687-735`) and there are none.

**D4 — Skeleton manifest replaces the live producer's `publish_task_output`.**
Deleting the live producer's republish removes the manifest `seal_live`
iterates. The producer instead builds a **skeleton manifest** (one placeholder
`PartitionEntry` per partition) and calls `seal_live(skeleton)` directly;
`seal_live` fills size / row-count / digest from the reassembled payloads. **The
general `publish_task_output` path is preserved** — non-live shuffle producers
still publish through that path; only the live producer stops calling it (Codex
iter-2 final correction).

**D5 — Telemetry from the manifest, not `results`.** `emitted_rows` /
`emitted_batches` (`mod.rs:300-305`) are derived from the sealed manifest (or
sink counters) for live shuffle producers, so the producer does not go dark when
`results` is suppressed. `published_shuffle_bytes` stays sourced at completion.

**D6 — Budget honest: one reservation per resident byte, atomic.** The
append-time reservation on `live.chunks` (`partition_store.rs:495`, ADR 124's
backpressure reservation) stays. The sealed payload is a *new* resident copy and
**must be reserved** at seal — replacing the deleted publish-path reservation,
never inserted unreserved. Reservation is **atomic**: reserve before inserting
into `inner.partitions`; on any pre-insert error (budget exhaustion), drop
reservations and leave **no manifest entry and no sealed pull** behind.
Reservation *count* stays 2; what changes is *where* the second is taken (seal,
not publish).

**D7 — Live chunks stay resident through seal.** Chunks are not dropped at seal:
in-flight `pull_live` consumers (`:636-651`) may still be reading by index, and
retry-from-zero is an ADR 124 contract. The 2-copy window (seal → query
cleanup) is therefore real, and D6's sealed reservation is mandatory, not
optional. Opportunistic refcounted release is a named follow-on.

**D8 — Scope boundary.** Touch surface is precisely: a shared Arrow IPC helper
module (codec + reassembly), `partition_store.rs` (schema field + seal rewrite +
seal reservation), `server/mod.rs` (live open with schema, drop retention,
suppress results, manifest telemetry), and the architecture guards. **No**
chunk-granular sealed manifest, **no** virtual sealed payload aliasing live
chunks, **no** remote live transport / multi-producer fan-in, **no** build/probe
reorder, **no** Arrow IPC wire-format change, **no** SQL/Rune authoring change.

## Behavior gates

- **`live_seal_reassembles_multi_chunk_arrow_ipc`** (headline / the spine) —
  append ≥2 real Arrow IPC chunks to one live partition, seal without
  pre-publish, pull + decode the sealed payload, observe **all** rows in order.
  *Must fail on `main`* (today decodes the first chunk only).
- **`live_seal_empty_partition_emits_schema_bearing_ipc`** — open a live
  partition with schema, append nothing, seal, pull + decode: zero rows, correct
  schema. *Must fail on `main`* because the live seal path has no schema source
  for a never-appended partition without a pre-published payload.
- **`live_shuffle_producer_returns_no_result_batches`** — local execution of a
  streamable exchange producer returns `report.results.is_empty()` with
  `Some(ShuffleProducer { manifest })`.
- **`live_shuffle_worker_sends_no_task_batch_for_shuffle_producer`** — worker
  dispatch of a live shuffle producer emits `TaskStarted` then `TaskComplete`
  with a shuffle manifest, and no `TaskBatch` frames.
- **`live_shuffle_producer_telemetry_uses_manifest_rows`** — emitted row/batch
  counters remain correct though `results` is empty.
- **`seal_live_reserves_materialized_sealed_payload`** — sealing from chunks
  increments `ShuffleBytes` for the sealed payload and releases it on query
  cleanup.
- **`seal_live_over_shuffle_budget_is_rejected_without_partial_publish`** —
  seal-time budget exhaustion leaves no manifest entry and no sealed pull
  (atomicity / reserve-before-insert).
- Existing ADR 124 pacing, retry-from-zero, and downstream-cancellation tests
  stay green.

## Architecture guards

`adr126_*` in
`tests/architecture/distributed_execution/scheduler_dispatch.rs`:

- **`adr126_live_append_sink_retains_no_record_batches`** — the
  `LiveAppendStreamableBatchSink` struct + impl contain no `partition_batches`
  field and no `partition_batches.push`.
- **`adr126_live_producer_does_not_publish_task_output`** —
  `execute_live_shuffle_producer` does not call `publish_task_output`, builds a
  skeleton manifest, calls `seal_live` directly, and returns empty results with
  a shuffle output.
- **`adr126_live_open_carries_schema`** — the live-open trait surface accepts
  `SchemaRef` and `LivePartition` stores `schema: Option<SchemaRef>`.
- **`adr126_seal_live_reassembles_arrow_ipc_not_concat`** — the `seal_live` body
  no longer contains `bytes.extend_from_slice(&chunk.bytes)` and routes chunk
  bytes through the shared Arrow IPC reassembly helper.
- **`adr126_seal_live_reserves_sealed_payload`** — the seal path reserves
  `BudgetDimension::ShuffleBytes` before inserting chunk-derived sealed payloads
  into `inner.partitions`.

**Retarget (discipline #4, in-change).**
`adr124_seal_live_materializes_from_chunks` (`scheduler_dispatch.rs:496`)
currently asserts the seal body contains `for chunk in &live.chunks`,
`bytes.extend_from_slice(&chunk.bytes)`, and
`inner.partitions.insert(address, payload)`. ADR 126 **deletes the
byte-concat loop** (the proven bug), so the `extend_from_slice` assertion would
fail. Retarget the guard to the reassembly invariant — seal materializes from
`live.chunks` via the Arrow IPC reassembly helper (no `extend_from_slice`),
reserves `ShuffleBytes` for the sealed payload, still inserts into
`inner.partitions`, **and preserves `live.pacing_semaphore.close()`** (the
124 close-on-seal clause the guard currently asserts must survive the rewrite —
flagged by both Codex and Gemini) — under the name
`adr126_seal_live_reassembles_arrow_ipc_not_concat` (with a 124-era alias
comment so the lineage stays legible). The other ADR 124 guards do **not**
invert: the pacing-semaphore guards
(`adr124_live_partition_has_pacing_semaphore_and_farthest_released_index`,
`adr124_append_live_acquires_owned_permit_forget_on_success`,
`adr124_pull_live_releases_only_at_farthest_released_index`,
`adr124_pull_live_unfolds_by_index_for_replay`,
`adr124_cancel_live_closes_pacing_semaphore`) pin append/pull/cancel paths 126
leaves intact, and the append reservation is preserved verbatim.

**Recipe:** `cargo test -p toron` + `cargo test -p toron --test
architecture_test adr124 adr126` + `cargo check -p toron --all-targets`.

## Rollback (Fork B contingency)

If schema-threading into `open_live` proves wider than the one-field estimate,
fall back to **Fork B**: pre-publish *only* schema-bearing empty partitions (the
producer holds the schema), let non-empty partitions seal from chunks reading
schema from the first chunk. **Non-negotiable in either fork:** never
byte-concat framed streams; never insert unreserved sealed bytes. The
multi-chunk IPC reassembly, result suppression, and seal-time budget accounting
are required regardless. This is a documented contingency, not a second promoted
shape.

## Council history

- **iter-0:** Claude framed the cut, suspected the multi-chunk concat bug, and
  over-claimed "3 copies → 1." Codex *proved* the concat bug (a throwaway
  two-stream Arrow IPC round-trip returned only the first stream), grounded that
  `results` is transmitted as `TaskBatch` (not harmless bookkeeping) and that
  telemetry derives from `results`, and corrected the memory claim to a 2-copy
  steady state. Gemini concurred on the grounding and recommended Fork B.
- **iter-1:** Claude conceded the overclaim, added the streaming re-encode
  mechanism (bounded one-chunk transient), and reversed to Fork A on the
  grounded fact that the schema is cheap to reach at `open_live`.
- **iter-2:** Claude promoted the adr/impl dual (Fork A, skeleton manifest,
  atomic seal budget, the 124 guard retarget) and self-corrected an earlier
  "no 124 guard inverts" error against `scheduler_dispatch.rs:496`. Codex
  independently reached the same Fork A + the same guard retarget, and supplied
  the skeleton-manifest / atomic-budget / row-cross-check mechanisms. Codex's
  final correction: preserve the general `publish_task_output` path for non-live
  shuffle producers. Gemini conceded Fork B → Fork A on the grounded
  schema-availability fact.
- **iter-3 (final):** Genuine 3-of-3. No load-bearing divergence remains.

The cut is the **first link** of ADR 125's deferred "live shuffle memory
chain." The honest framing was earned by grounding over assertion at every step
([[feedback_ground_the_premise_before_sketching_scope]],
[[feedback_concession_cycle_ground_not_headcount]]) — the cut is named for what
it delivers (correctness + bounded memory), not for the constant-memory headline
it does not.

## What to do next

- **Chunk-granular sealed manifest** — true constant-memory / single-copy
  sealed output: reference-or-move live chunks into the sealed read path so no
  second physical copy exists (multi-entry-per-partition manifest OR streaming
  sealed assembly). The only path to "1 copy."
- **Opportunistic live-chunk release after seal** — reclaim the 2nd copy when no
  live readers remain active, without the full representation change.
- **Configurable `LIVE_BACKLOG_DEPTH` via `QueryBudget`** — earns a cut when a
  real workload shows the constant is wrong.
- **Executor build/probe reorder** — pairs with 124 for end-to-end natural
  backpressure; now unmasked since seal stops double-buffering.
- **Output-side joins-as-streamable-producers** — widen `streamable_unary_plan`
  for `Join` + restructure the join executor to yield mid-probe.

## Promotion criteria

All behavior gates green (headline:
`live_seal_reassembles_multi_chunk_arrow_ipc`, which must fail on `main`
beforehand); the empty-partition schema-bearing gate fails on `main` before
schema is carried through live open; ADR 124 pacing / retry / cancellation
regressions green; the five `adr126_*` guards hold; the retargeted
`adr124_seal_live_materializes_from_chunks` holds; `cargo test -p toron` +
`cargo test -p toron --test architecture_test adr124 adr126` + `cargo check -p
toron --all-targets` green. Migration:
`plans/impl/126_live_shuffle_seal_correction.md`.
