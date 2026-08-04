# claude iter-3 — ADR 446: shape-fenced local runtime-filter bridge for the reoriented inner chain

Promotion-ready pair with `claude_iter_3_impl_446.md`. Converges iter-2 with the
operator's request-plumbing, filter-composition, and source-ordinal corrections.

## Context

q05 is the worst cell on the closed 22/22 board — 177,026.577 ms against DuckDB's
654.000 ms, **270.68x** (`impl/356_...:67,100`). The reservoir holds the **full,
unfiltered lineitem**: ADR 413's trace pins `build_rows=59,986,052` — the entire
sf=10 cardinality — at 87.9 bytes/row (`impl/413_...:94-97`). DuckDB streams
lineitem as the probe and builds on the small filtered side: **206 MB peak RSS,
zero spill, 654 ms** (`impl/356_...:127`).

Pushdown is not the defect and cannot be — lineitem carries no local predicate in
q05; both filters live on region and orders. ADR 401 closed SQL-side placement
2026-07-07, two days before ADR 413 measured the 5.35 GB. The spill is not
memory-driven — the 356 baseline and the 444 cohort both ran at
`MemoryMax=infinity` on a 61 GB machine and spilled anyway. The only lever is a
**semijoin reduction** from dated orders. 443's oracle bounds the prize:
dated-order lineitems = **9,103,367** (6.6x).

**Why the build side was never questioned:** ADR 132's smaller-build comparator
fires only "when *both* join inputs are eligible **broadcast** builds"
(`adr/132:3-6`); lineitem is never broadcast-eligible, so no size-based build-side
choice ever runs. ADRs 429/443/444 profile the reservoir's internals and take the
5.35 GB as given.

**The ordering permits the filter.** `base_batches` is fully accumulated by an
awaited visit loop (`streaming.rs:12050-12076`), counted (`:12081`), and passed as
a complete slice into `:12255`, which only then scans lineitem at `:12345`. The
444 ledger corroborates: `base_accumulation` (6.07 s) precedes
`reservoir_partition_spill_write` (74.02 s). The base is resident and capped
(`join_output_materialization_memory_cap`, `:12053`).

## Decision — a Toron-local ordinal-keyed bridge, fenced and non-overwriting

On the admitted reoriented-chain grouped-reservoir path only: build a
`RuntimeFilterFragment` from the resident base's join key, convert it with
`descriptor_from_fragment`, and assign it to the **terminal lineitem
`ScanRequest`**. Wendelon validates and applies it.

**The existing builder does not wire in.** The only `pub(crate)` entry,
`build_runtime_filter_fragment(batches, key: &BoundExprProgram)`
(`exec/runtime_filter.rs:25`), is program-keyed via `single_supported_key`; the
ordinal-keyed `build_bloom_runtime_filter_fragment` (`:164`) is private. The sole
production descriptor construction (`distributed/scheduler.rs:975-1005`) is
scheduler-shaped end to end — it needs a `TaskSpec`, a
`TaskOutputBinding::ShuffleProducer`, one upstream stage, a `runtime_filter_merges`
registry, a shuffle `partition_key`, and `first_scan_fingerprint`. None exists
locally.

**Behavior-neutral refactor, travelling with this ADR** (per
`continuous-refactoring.md`): expose
`build_runtime_filter_fragment_for_ordinal(batches, ordinal, key_type)` and make
the program-keyed entry a thin wrapper that resolves `single_supported_key` and
delegates. **Rejected — synthesizing a `BoundExprProgram`** from the ordinal to fit
the scheduler-shaped API: it fabricates a producer program no planner emitted,
purely to satisfy a signature.

**Reused unchanged:** `descriptor_from_fragment` (`:264`, already ordinal-keyed),
`runtime_filter_key_type`, `bloom_sizing`, `cap_distinct_threshold`, and all of
Wendelon's validate + mask.

### Ordinals are source (pre-projection) ordinals

`probe_key_columns` are **source scan ordinals** on both paths. In-memory
(`storage.rs:582-706`): filter at `:636`, projection at `:680`. Wendelon — the path
q05/sf=10 persisted Parquet actually uses (`storage.rs:860+`): `pbc` (projection
appended at `:889`) and `request.runtime_filter` both go to
`table_query_stream_with_runtime_filter_and_attempt_hook`, and server-side
`runtime_filter_mask_for_projected` (`crates/wendelon/src/network/data_server.rs:10335-10355`)
resolves each ordinal via `ProjectedRecordBatch::physical_for(*ordinal)` — a
logical→physical mapping. **The descriptor speaks source ordinals; Wendelon maps.**

Output projection reordering or omitting the key is therefore **safe and not a
decline condition**. If projection drops the column, `physical_for` returns `None`
→ `MaterializationIncompatible` → the server observes
`storage_runtime_filter_advisory_dropped` and **returns the unfiltered batch**
(`data_server.rs:10383-10390`).

### The composition rule — never overwrite, never merge

`bound_scan_request` sets `runtime_filter: self.context.runtime_filter.clone()`
(`exec.rs:289`), and `ExecutionContext.runtime_filter` (`:110`) is cloned into
**every** scan request the context builds (`:289`, `:382`). So:

1. **The local descriptor is threaded as an explicit optional parameter and
   assigned to the terminal request only.** A context-level assignment would leak
   the lineitem filter onto every scan in the query.
2. **`ScanRequest.runtime_filter` is a single `Option`** — two filters cannot
   coexist. If `context.runtime_filter` is already `Some` (the distributed-worker
   case), the bridge **declines** and preserves the existing descriptor.

Declining is provably safe, not merely cautious: a runtime filter is always an
optimization — dropping it admits extra rows the join's own equality rejects — so
refusing costs reduction, never correctness. **Merging is rejected**:
`fragments_compatible` (`:286`) and `merge_fragments` (`:306`) check kind and
parameter compatibility, **not probe-key identity**, so merging filters keyed on
different columns would emit a descriptor whose `probe_key_columns` is false.

### The fence — `!null_safe_keys`

`build_runtime_filter_mask` discards null probe keys
(`crates/wendelon/src/schema/runtime_filter.rs:293-297`): `append_value(false)`.
For an inner equi-join a null key matches nothing, so dropping it is correct. Under
`null_safe_keys` (IS NOT DISTINCT FROM) **null matches null**, and dropping it
loses rows. Fenced exactly as ADR 439 fences (`adr/439:29-33`); `null_safe_keys` is
already a parameter at `streaming.rs:15425`. Unsupported key types decline.

## The risk surface — what is and is not fail-safe

**Fail-safe:** a bloom false positive (the join's equality rejects it); a
fingerprint mismatch (`FingerprintMismatch` → advisory drop → full scan); an
unsupported type at the ordinal (`UnsupportedShape` → drop); a projected-away key
(`MaterializationIncompatible` → drop). All degrade to *less reduction*, never a
wrong answer.

**NOT fail-safe — the whole risk:**

1. **Wrong-space ordinal onto a same-type sibling.** Wendelon checks only the
   **type at the resolved ordinal** (`data_server.rs:10349-10351`,
   `schema/runtime_filter.rs:136-138`). q05's lineitem carries `l_orderkey: Int64`
   **and** `l_suppkey: Int64` (`toron-tpch/src/lib.rs:776,778`) in the same
   projection. An ordinal in the wrong space landing on the sibling **passes
   validation** and filters lineitem by whether `l_suppkey` is in the
   dated-**orderkey** bloom — silently dropping correct rows.
2. **Null-safe key loss** — fenced above.

Neither is caught downstream. The ordinal mapping is therefore a
**correctness-critical construction that must be proven by test, not delegated to
the validator.**

## Acceptance

- Strict q05 sf=10 `pass@compare`: unchanged status, digest, schema, and **row
  count**. Correctness is the gate. The descriptor is attached at request
  execution, not bound-plan construction, so the plan fingerprint is expected to
  remain `757d7c625669b21a` and is recorded as a cross-check.
- **Mandatory tests for the two uncaught failures**: a same-type sibling ordinal
  test asserting the descriptor names the join key; a reordering-projection test
  asserting the emitted ordinal is the **source** one; a null-safe decline test
  that fails if the fence is removed.
- A context-filter preservation test: with `context.runtime_filter = Some(..)`,
  the bridge declines and the original descriptor survives.
- A projected-away-key test: rows correct via Wendelon's advisory drop, **without**
  the bridge declining.
- Measured rows entering the reservoir reported, not inferred: `build_rows` falls
  from 59,986,052 toward ~17.9M at the current cap.
- The persisted-Parquet harness must apply `ScanRequest.runtime_filter` against
  the full source-schema batch before PBC/projection; otherwise a reachability run
  can silently exercise a no-op bridge. A harness behavioral test pins source
  ordinal filtering before projection.
- 4 GiB / no-swap survival; scratch empty after.
- **A wall claim is permitted** here, unlike 443/444/445 — the ceiling clears the
  band. Five children, unprofiled, scope order randomized and recorded.

### The prize, honestly bounded

At the existing 1 MiB cap (`exec/runtime_filter.rs:22`), q05's 2,275,919
dated-order keys are 2.60x over `cap_distinct_threshold()` (875,175), but
`bloom_sizing` clamps rather than refuses — the policy **degrades, it does not
disable**:

| | value |
|---|---|
| bloom at 1 MiB / 2,275,919 keys | 3.69 bits/key, k=3 → ~17.3% FPR |
| lineitem rows reaching the reservoir | ~17,891,020 |
| reduction | **3.35x**; spill 5.27 GB → ~1.57 GB |
| cap raised to 9.585 bits/key (~2.7 MiB) | ~1% FPR → 6.2x; spill → 0.85 GB |

Reservoir = 87.4% of root; a 70% input cut removes up to 61% of root if cost
scales — clearing the **23.86%** band even at half scaling. First candidate in the
441-445 arc whose ceiling exceeds the band. **FPR figures are computed, not
measured**; the receipt reports measured `build_rows`.

## Guardrails and risks

- **Ordinal mapping is the risk, not the filter.** Prove it by construction and
  pin it with a test; do not rely on the validator.
- **The fences are load-bearing.** An architecture guard asserts the fragment build
  occurs after base accumulation, before the terminal scan, under
  `!null_safe_keys`, and that the assign site is guarded by
  `req.runtime_filter.is_none()` — ordering and fence identity, in the shape of
  439's and 445's guards.
- **Placement is Toron-home; no Wendelon change.** Wendelon owns the descriptor,
  validator, and mask, built for the scheduler path; it would not build "semijoin
  reduction for a reoriented inner chain". If a Wendelon edit proves necessary,
  **stop** — the placement premise failed and it becomes a separate producer ADR.
- **Cap raise deferred.** 1 MiB → ~2.7 MiB takes 17.3% → ~1% FPR, 3.35x → 6.2x.
  Board-wide tuning reach; named-deferred, contingent on this landing.
- **Chain reorientation deferred.** Making lineitem the base probe is the
  structurally correct answer — DuckDB's 206 MB says so — but re-shapes grouped
  geometry board-wide (442's fear, `adr/442:12-14`). This cut shrinks the build
  side; it does not fix the choice of build side.

## Forward arc

**Chain reorientation** (lineitem as base probe, build on the filtered side) is the
447 candidate and the real answer; this cut buys 3.35x without it.

**The 64 MiB budget inconsistency** is a separate governance topic:
`memory_per_task: 64 MiB` reaches production via
`impl Default for QueryBudget { fn default() -> Self { Self::test_default() } }`
(`distributed/budget.rs:95,107-110`). A budget permitting a 3.06 GB process while
forcing a 5.35 GB spill is not bounding what it claims. Not folded.

## Deferred (named)

- `MAX_FILTER_BYTES` cap raise (1 MiB → ~2.7 MiB), contingent on this landing.
- Chain reorientation (447 candidate).
- Query-budget realism (`memory_per_task` via `test_default`).
- A size-based build-side choice for spill-eligible joins — ADR 132's comparator
  never runs for non-broadcast pairs (`adr/132:3-6`).
- q05 wall characterization (bimodality) — 443's write spread 0.2882 vs 444's
  0.0869 does not replicate; less urgent if this delivers a 60% cut.
- q17 remeasurement; q18 strict one-pass; q21 residual; q19 re-diagnosis + 438
  widening; q13 fresh diagnosis; Cut A (439 mirror); generic O(1) LRU rewrite.
