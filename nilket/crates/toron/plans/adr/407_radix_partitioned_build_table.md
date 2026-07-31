# ADR 407 - Radix-partitioned (build-only) primitive join table

## Context

405/406 landed the fenced `PrimitiveBuildTable<K>` for non-null-safe `Int64`/`Int32` in-memory joins; 405 banked a
measured q03 sf=1 win (~-13%) and named radix partitioning as the follow-on. Post-405, q03's join build is ~142 ms of a
~371 ms profile loop (~38%), and the single open-addressing table for 829,958 unique `i64` keys is ~40 MB - far larger
than L2 - so its inserts/probes are plausibly **cache-miss-bound**. Radix partitioning into R cache-resident sub-tables
is the standard fix (DuckDB/Polars).

Crucially, `perf_event_paranoid=1` is now set, so ADR 407 can **verify** the cache-bound premise before building the
mechanism - the grounding lever 404 lacked. 407 is a **perf-gated, build-only** radix cut: partition the build; leave
the probe in probe-row order. Partitioning the probe would reshape output order/materialization - the boundary 405 drew
as its stop-line - and is explicitly a separate ADR.

## Decision

### Phase 0 - prove cache-bound join-build (perf-gated, before code)
1. **Isolate the loop under perf, not the wrapper.** `perf stat` must be the process launching `toron-tpch`, inside a
   cgroup scope - not wrapping `systemd-run --wait` (which would measure the wrapper/wait path). Use a `--scope`:
   ```bash
   systemd-run --user --scope -p MemoryMax=4G -p MemoryHigh=4G -p TasksMax=512 \
     perf stat -e cache-misses,llc-load-misses,l1-dcache-load-misses,instructions,cycles \
     -- target/release/toron-tpch perf-run-one --engine rune --query q03 --mode strict --sf 1 \
        --run-index 0 --data-source parquet --data-dir target/tpch/data
   ```
2. **Prove *join-build* cache-bound, not whole-query.** q03 also decodes Parquet and materializes. Use warm loop runs
   (`TORON_TPCH_PROFILE_LOOP`) + `TORON_JOIN_BUILD_PROFILE=1`, and classify cache-bound only when **all** hold: join-build
   time is a high loop share, LLC/L1 miss rate is high, and a **sampled** profile still points at the primitive-table
   build/probe path. If the counters are dominated by Parquet/RLE decode or `memmove`, **stop or re-scope**.

Record the classification (build-time share, miss rates, sampled top frames) in the results doc before code.

### The cut - build-only radix, generic over K (only if Phase 0 says cache-bound)
- A radix table generic over `K` (`RadixPrimitiveBuildTable<K>`, or a radix mode inside `PrimitiveBuildTable<K>`),
  usable for `i32`/`i64`. It routes each key to one of R partitions by a partition hash and holds R per-partition
  `PrimitiveBuildTable<K>` sub-tables (reusing the proven table + `BuildRowList` value, so the output contract is kept).
  The partition bits must be **distinct** from the within-partition bucket bits (avoid radix/bucket hash correlation).
- **R is formula-derived, not q03-tuned.** Choose R from the known build size: e.g.
  `next_power_of_two(ceil(estimated_primitive_table_bytes / target_partition_bytes))`, bounded by a named maximum and
  never below 2 when radix is selected. The results doc records the target bytes, max partition count, and the R chosen
  for q03. A fixed "because q03" partition count is rejected.
- **Preserve the no-resize invariant deliberately.** The single table guarantees termination by sizing buckets from the
  known row count. Radix does the same **per partition** via a **pre-count first cut**: pass 1 histograms rows per
  partition; allocate each per-partition table to its exact count; pass 2 inserts. No grow path, no unbounded probe. (A
  prehashed-insert optimization to avoid double-hashing is a later refinement, not this cut.)
- **The two-pass seam is `build_in_memory_join_rows`.** The existing `JoinBuildRows::insert` API is one-row-at-a-time;
  exact per-partition allocation requires the builder to histogram the key arrays before constructing the radix table.
  The implementation must not hide a growing radix table behind `insert` and call it "pre-counted."
- **Build-only.** The probe stays in probe-row order: `get(key)` hashes to its partition and probes that sub-table;
  `matched_build_rows` stays indexed by original build row ids; duplicate/probe order and outer/anti/semi semantics are
  preserved by construction. **Do not partition the probe in 407.**
- **Radix gate is mandatory.** Radix is selected only for non-null-safe `Int64`/`Int32` builds whose **known** size
  (build rows / estimated primitive-table bytes) exceeds a cache-pressure threshold; smaller builds stay single-table.
  The threshold rationale is *"avoid radix overhead below cache-pressure size,"* a structural size gate on known counts -
  **not** a planner cardinality estimate or cost model.
- **Memory accounting remains conservative.** Radix may allocate more aggregate bucket capacity than the single table due
  to per-partition power-of-two rounding. The join-build guard must account for the radix table's aggregate bucket/entry
  capacity (or prove the old estimate dominates it). A radix path that admits a build whose actual radix allocation can
  exceed the memory cap is rejected.

### Stop-lines
- **Not cache-bound** (Phase 0) -> seal a diagnostic; do not build radix.
- **Probe cache also dominates** -> build-only radix still lands, and **build+probe radix is named as a follow-on ADR**
  (it crosses the output-order/materialization boundary); do not smuggle probe partitioning into 407.

## Acceptance
- **Phase 0 recorded:** the isolated-`perf stat` cache classification (join-build time share, LLC/L1 miss rate, sampled
  top frames) proving cache-bound join-build - or the diagnostic that it isn't.
- **Either** the build-only radix table **or** a Phase 0 diagnostic (not cache-bound).
- **Behavior-neutral:** q03 fingerprint stays `666e0ac028036f70`; correctness `pass@compare`; rows `11620`; spill `0`;
  duplicate/probe order + all-8-join-kinds equality preserved (contract tests, incl. multi-partition routing and
  per-partition spare-bucket).
- **No-resize preserved:** a test proving the pre-count build fills per-partition tables without a grow/resize path.
- **Memory guard preserved:** under/equal/over-cap tests for a radix-eligible build prove the guard is conservative for
  aggregate radix allocation, not just the old single-table estimate.
- **Radix gate:** a test proving a below-threshold build stays single-table (`PrimitiveBuildTable`) and an
  above-threshold build uses radix.
- **Primary (Int64/q03):** q03 sf=1 four-way before/after (fresh same-session baseline) - Toron SQL / Polars beats the
  runs=7 band, SQL/Rune stays ~1.00x, both move together. **Int32 is correctness-only** (no Int32 perf claim; it rides
  the generic).
- **Controls:** q04/q19/q08 sf=10 no-regress.
- **Full gate:** `--no-fail-fast` both crates; de-branded; **no committed `.perf.data`**.
- **Scope honesty:** targets the build cache-miss cost; **not** a fix for the >360 s sf=10 timeouts; probe partitioning
  is a separate ADR.

## Guardrails
- **Perf-gated** - build only if Phase 0 proves cache-bound *join-build* (isolated `perf stat`, join-build-specific, not
  whole-query, not the wrapper path).
- **Build-only** - no probe partitioning; the output-order/materialization boundary is 405's stop-line.
- **No-resize preserved** - pre-count per partition; no unbounded probe; termination test.
- **Memory guard conservative** - account for aggregate per-partition bucket rounding or prove the existing estimate is
  an upper bound.
- **Radix gate mandatory** - structural size threshold on known counts ("avoid radix overhead below cache-pressure
  size"), never a planner/cost estimate; small builds stay single-table.
- **Generic K, Int64/q03 is the driver** - Int32 rides correctness only.
- **Behavior-neutral** - contract tests + q04/q19/q08 controls are the tripwire, not q03 alone.

## Risks
- **Not cache-bound** -> Phase 0 diagnostic (perf-grounded, pre-empted before building).
- **Probe cache dominates too** -> build-only lands; build+probe radix named as a follow-on; not smuggled in here.
- **Radix/bucket hash correlation** -> partition on distinct hash bits from the within-partition bucket.
- **No-resize violated** -> pre-count histogram + exact per-partition allocation; termination test.
- **Memory undercount** -> radix-specific aggregate table estimate or proof the current estimate dominates.
- **Overhead on small builds** -> mandatory gate; single-table below threshold.
- **Doesn't bank** -> named honestly (404-style) - but Phase 0's cache classification gates this up front.
- **Sibling regression** -> q04/q19/q08 + `--no-fail-fast`.

## Forward Arc
Build-only radix banks the sf=1 build cache win (if Phase 0 confirms cache-bound). If probe cache also dominates,
build+probe radix - crossing the output-order/materialization boundary - is the next ADR. Beyond that lie the >360 s
sf=10 timeouts, which are the streaming-completion / execution-model thread, not a cache-locality cut. Int32 rides the
generic table throughout. Article-grade.

## Grounding evidence (file:line)
Primitive table + `BuildRowList` value contract: `exec/join/build_rows.rs` (`PrimitiveBuildTable<K>`, `insert`/`get`/
`find_bucket`, `with_capacity` sizing buckets from the known row count). Fence + build/probe seam: `exec/join.rs:1284`
(`build_in_memory_join_rows` plan/null-safe match), `:1364` (`probe_in_memory_join_rows`, `get_int64`/`get_int32`).
Post-405 q03 build cost: `plans/impl/405_int64_join_primitive_table_results.md` (build_micros ~142 ms; 3,241,776 build
rows / 829,958 unique keys). 405 probe/output-reshaping stop-line: `plans/adr/405_int64_join_primitive_table.md`. perf
cache-miss events (`cache-misses`, `llc-load-misses`, `l1-dcache-load-misses`) available under `perf_event_paranoid=1`.
