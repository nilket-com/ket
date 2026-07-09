# ADR 407 - Implementation plan: radix-partitioned (build-only) primitive join table

Pairs with `plans/adr/407_radix_partitioned_build_table.md`. Perf-gated, build-only radix over the 405/406
`PrimitiveBuildTable<K>`. Phase 0 must prove the post-405 q03 join-build is cache-bound (isolated `perf stat`) before any
code; if not, seal a diagnostic. Build-only (no probe partitioning), pre-count (no resize), mandatory size gate,
generic K with Int64/q03 as the driver and Int32 correctness-only.

## Phase 0 - prove cache-bound join-build (before code)
1. **Symbol-gated build** (perf attribution): `CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false
   CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup build -p toron-tpch --features polars-sql-reference --release`.
2. **Isolated cache counters** - `perf stat` launches `toron-tpch` *inside* a scope (not wrapping `systemd-run --wait`):
   ```bash
   systemd-run --user --scope -p MemoryMax=4G -p MemoryHigh=4G -p TasksMax=512 \
     perf stat -e cache-misses,llc-load-misses,l1-dcache-load-misses,instructions,cycles \
     -- env TORON_TPCH_PROFILE_LOOP=16 TORON_JOIN_BUILD_PROFILE=1 \
        target/release/toron-tpch perf-run-one --engine rune --query q03 --mode strict --sf 1 \
          --run-index 0 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr407_q03_phase0_cache_stat.out 2>&1
   ```
3. **Sampled attribution** - `perf record -F 999 --call-graph dwarf ...` (same isolation) + `perf report --stdio
   --sort symbol`, to confirm the miss-heavy frames are the primitive-table build/probe, not Parquet/RLE/`memmove`.
4. **Classify (record before code):** cache-bound join-build only if join-build time share is high **and** LLC/L1 miss
   rate is high **and** the sampled top frames are the primitive table. If Parquet/RLE/memmove dominate -> **stop-line
   diagnostic, no radix**. If the probe cache also dominates -> note build+probe radix as a follow-on; 407 stays
   build-only.

## Phase 1 - build-only radix table (only if cache-bound)
- Generic radix table (`RadixPrimitiveBuildTable<K>` or a radix mode inside `PrimitiveBuildTable<K>`), R partitions, each
  a per-partition `PrimitiveBuildTable<K>`. Route keys by a partition hash on **distinct bits** from the within-partition
  bucket hash.
- R selection is formula-based, not q03-tuned: derive R from known build size, for example
  `next_power_of_two(ceil(estimated_primitive_table_bytes / target_partition_bytes))`, bounded by a named maximum and
  never below 2 when radix is selected. Record the target bytes, maximum, and chosen R in the results doc.
- **Pre-count build (no resize):** pass 1 histograms rows per partition; allocate each per-partition table to its exact
  count; pass 2 inserts. Preserves the known-count termination invariant.
- **Implementation seam:** the pre-count happens in `build_in_memory_join_rows` over the key arrays before constructing
  the radix table. Do not implement a growing radix table hidden behind the one-row-at-a-time `JoinBuildRows::insert`
  API and then call it pre-counted.
- **Build-only probe:** `get(key)` -> partition -> sub-table `get`; probe stays in probe-row order;
  `matched_build_rows` indexed by original build row ids. No probe partitioning.
- **Mandatory gate** in `build_in_memory_join_rows`: non-null-safe `Int64`/`Int32` **and** known size (rows / estimated
  primitive-table bytes) over the cache-pressure threshold -> radix; else the single `PrimitiveBuildTable`. Document the
  threshold as "avoid radix overhead below cache-pressure size," not a cost model.
- **Memory accounting:** the fit/spill guard must account for aggregate radix bucket/entry allocation after per-partition
  power-of-two rounding, or prove the existing estimate is conservative for radix. No cap bump and no undercount.

## Tests (behavior-neutral; discriminating)
- **Multi-partition routing:** an `Int64` build spanning multiple radix partitions returns the same per-key
  `BuildRowList` (insertion order) as the single table; probe output row-for-row equals the single-table path;
- **Duplicate/probe order + null routing:** the 405/406 order test, now through the radix table (fails under LIFO);
- **All 8 join kinds:** radix == single-table == hash-fallback for inner/left/right/full/semi/anti;
- **No-resize:** a build that exactly fills per-partition tables inserts without a grow path (assert per-partition
  bucket capacities sized from the pre-count);
- **R formula:** a test/fixture demonstrates R is derived from known size and capped, not hard-coded to q03;
- **Radix gate:** below-threshold build -> single `PrimitiveBuildTable`; above-threshold -> radix (assert the mode);
- **Memory guard:** radix build preserves or tightens the `estimated_build_hash_bytes` fit/spill decision and is
  conservative for aggregate radix allocation;
- Int32 correctness mirror (routing + order) - no Int32 perf claim;
- carry the existing join suite green.

## Phase 2 - re-measure (Int64/q03 primary, controls)
```bash
# fresh four-way baseline (worktree at the 407 plan commit) + post-cut, same session, then:
rm -rf target/tpch/perf_adr407_q03_sf1 && \
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr407_q03_sf1
# controls
for q in q04 q19 q08; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 3 --warmup 1 --query "$q" \
      --engines toron,duckdb --data-source parquet --data-dir target/tpch/data \
      --output-dir "target/tpch/perf_adr407_${q}_sf10"
done
```
- Primary: Toron SQL / Polars beats the runs=7 band vs the fresh baseline; SQL/Rune ~1.00x; fingerprint `666e0ac...`.
- Re-run the Phase 0 cache stat post-cut to show LLC/L1 miss rate on the build dropped (the mechanism proof).
- Controls: q04/q19/q08 no-regress.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron join_build_rows --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (core join executor cut)
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast              # REQUIRED
target/release/toron-tpch correctness --query q03 --sf 1 --mode strict --data-source parquet --data-dir target/tpch/data
git diff --check
find . -name '*.perf.data' -print
```
Note: cap rustc jobs (`CARGO_CGROUP_JOBS=4`) - the large `toron` crate has tripped systemd-oomd at higher counts.

## Deliverable (results doc)
`crates/toron/plans/impl/407_radix_partitioned_build_table_results.md`:
- Phase 0: isolated cache-stat classification (build-time share, LLC/L1 miss rate, sampled frames), before code;
- **either** the build-only radix table (design: R formula and chosen R, partition-hash bits, pre-count, aggregate
  memory estimate, gate threshold + rationale) + q03 four-way before/after + post-cut cache-miss drop + q04/q19/q08
  controls, **or** the Phase 0 diagnostic (not cache-bound / probe-cache-also-dominates -> build+probe follow-on named);
- confirmation: fingerprint unchanged, behavior-neutral across kinds, no-resize + gate tests, Int32 correctness-only,
  full gates green, no `.perf.data`, scope honesty (not a >360 s fix; probe partitioning deferred).

## Risk / blast radius
- **Not cache-bound / wrong producer** -> Phase 0 diagnostic.
- **Probe cache dominates** -> build-only lands; build+probe named, not smuggled in.
- **No-resize violated / probe non-termination** -> pre-count + termination test.
- **Memory undercount** -> radix-specific aggregate table estimate or proof the existing estimate dominates.
- **Hash-bit correlation** -> distinct partition vs bucket bits.
- **Small-build overhead** -> mandatory gate + below-threshold single-table test.
- **Core-join regression** -> full `--no-fail-fast` + contract tests + controls.

## Seal
`plan: adr 407 radix partitioned build table` (the dual). Implementation (table or diagnostic + cache proof + measure):
`impl: adr 407 radix partitioned build table`. Do not commit `.perf.data`.
