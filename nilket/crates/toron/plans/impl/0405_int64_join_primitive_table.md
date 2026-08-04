# ADR 405 - Implementation plan: Int64 join build/probe primitive table

Pairs with `plans/adr/0405_int64_join_primitive_table.md`. Replace the enum-keyed `HashMap<JoinKey, BuildRowList>` build
for `JoinKeyPlan::Int64` + `!null_safe_keys` with a specialized open-addressing Int64 table, keeping `BuildRowList`
value semantics so the probe/output/duplicate-order contract is preserved by construction. Fenced (Int32/Generic/null-
safe unchanged), behavior-neutral, q03-driven. Stop-line to radix design if semantics need probe/output reshaping.

## Phase 0 - seam + fence census + fresh baseline (before the table)
1. **Fresh four-way baseline** (same session/machine as post-cut):
   ```bash
   rm -rf target/tpch/perf_adr405_q03_sf1_baseline && \
   systemd-run --user --same-dir --wait --collect --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
       --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
       --output-dir target/tpch/perf_adr405_q03_sf1_baseline
   ```
   DuckDB is resolved from `PATH` (`duckdb` v1.5.4 in the current environment); set
   `TORON_TPCH_DUCKDB_BIN` only if a future environment lacks the expected CLI on `PATH`.
2. **Seam census (record before code):**
   - confirm the build/probe seam: `build_in_memory_join_rows` (`join.rs:1274`) produces `JoinBuildRows`;
     `probe_in_memory_join_rows` (`join.rs:1357`) does `build_rows.get(&key)` -> `append_join_matches_for_kind(..,
     matches.rows(), matched_build_rows, ..)`;
   - confirm which callers reach the `Int64` in-memory build: the plain in-memory join, and **whether** the
     spilled/`PartitionedScan` per-partition build (403) reuses `build_in_memory_join_rows`. If it does, the table
     applies there too (in-scope, generic); if not, the table only affects the pure in-memory path. Either way q08/q19
     are controls.
   - enumerate every `JoinKind` that consumes `matches.rows()` + the `matched_build_rows` bitmap (inner/left/right/full/
     semi/anti) so the new table's `lookup` covers all of them.

## Phase 1 - the Int64 primitive table (fenced)
- Add an `Int64BuildTable` (open-addressing over raw `i64`, control bytes / power-of-two capacity, no enum, no
  hashbrown), value = the existing `BuildRowList` (keeps insertion-order duplicates + inline overflow).
- Select it **only** for `JoinKeyPlan::Int64 && !null_safe_keys`; route `Int32`/`Generic`/null-safe to the unchanged
  `HashMap<JoinKey, BuildRowList>` path.
- Build: insert raw `i64` -> `BuildRowList::push` (unchanged semantics). Probe: `lookup(i64) -> Option<&BuildRowList>`,
  feed `matches.rows()` into the unchanged `append_join_matches_for_kind`. Nulls in the probe/build arrays route as
  no-match exactly as today.
- Keep the memory-cap accounting (`estimated_build_hash_bytes` / `join_output_materialization_memory_cap`) conservative
  so 403's routing/wall behavior is unchanged. If the primitive table uses a new table-specific estimate, lock it with
  under/equal/over-cap tests against the old guard decision; otherwise keep the old estimate as a conservative guard.

## Tests (behavior-neutral; discriminating)
- **Order determinism:** an `Int64` join with >=2 build rows per key asserts output row order == insertion order - this
  test must **fail under a LIFO/reordered table** (locks the contract);
- **Probe-row order:** multi-probe-row case asserts output grouped in probe-row order (not build-iteration order);
- **All join kinds:** inner/left/right/full/semi/anti over `Int64` keys match the `HashMap` path row-for-row;
- **Null routing:** `Int64` arrays with nulls (probe and build) produce no spurious matches; null-safe join still takes
  the `Generic` path (fence test);
- **Fence:** `Int32`/`Generic` plans still construct the `HashMap` table (unchanged);
- **Memory guard equivalence:** under/equal/over-cap `Int64` builds choose the same fit/spill/reject decision as the
  current estimate, or a documented table-specific estimate that is conservative relative to the current guard;
- carry the existing join suite (unique/duplicate/null/generic-order/preallocation, 399) green.

## Phase 2 - re-measure (four-way, SQL headline, Rune control)
```bash
rm -rf target/tpch/perf_adr405_q03_sf1 && \
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr405_q03_sf1
# controls (join-kind / spill / partition), sf=10, ADR 398/403 scope
for q in q04 q19 q08; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 3 --warmup 1 --query "$q" \
      --engines toron,duckdb --data-source parquet --data-dir target/tpch/data \
      --output-dir "target/tpch/perf_adr405_${q}_sf10"
done
```
- Primary: Toron SQL / Polars beats the runs=7 band vs the fresh baseline; SQL/Rune ~1.00x; fingerprint `666e0ac...`.
- Re-profile (optional, perf now enabled) to show `rustc_entry`/`build_in_memory_join_rows`/`HashMap::get` shrink.
- Controls: q04/q19/q08 no-regress (q19 still spills, q08 still passes via 403 path).

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron join --lib                       # build/probe + new order-determinism tests
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (core join executor cut)
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast              # REQUIRED
target/release/toron-tpch correctness --query q03 --sf 1 --mode strict --data-source parquet --data-dir target/tpch/data
git diff --check
find . -name '*.perf.data' -print
```
Note: cap rustc jobs (`CARGO_CGROUP_JOBS=4`) - the large `toron` crate has tripped systemd-oomd at higher counts.

## Deliverable (results doc)
`crates/toron/plans/impl/0405_int64_join_primitive_table_results.md`:
- Phase 0: seam + fence census + fresh four-way baseline, before code;
- **either** the fenced Int64 table (design, why the contract is preserved, the order-determinism test) + q03 four-way
  before/after + profile-frame movement + q04/q19/q08 controls, **or** the stop-line design/diagnostic (why the contract
  needed probe/output reshaping) + the named radix follow-on;
- confirmation: fence honored, fingerprint unchanged, behavior-neutral across join kinds, full gates green, no
  `.perf.data`, scope honesty (Int64-only, not Polars parity).

## Risk / blast radius
- **Duplicate/probe order** -> keep `BuildRowList`; order-determinism test fails under LIFO.
- **Null / null-safe** -> fenced out; null routing test.
- **Outer/anti/semi bitmap** -> per-kind row-for-row tests vs the `HashMap` path.
- **Spill/partition interop** -> Phase 0 census + q08/q19 controls.
- **Memory guard drift** -> guard equivalence/conservatism tests; no cap bump.
- **Doesn't bank** -> named honestly (404-style), not forced.
- **Core-join regression** -> full `--no-fail-fast` + join suite.

## Seal
`plan: adr 405 int64 join primitive table` (the dual). Implementation (table or design/diagnostic + measure):
`impl: adr 405 int64 join primitive table`. Do not commit `.perf.data`.
