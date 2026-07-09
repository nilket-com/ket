# ADR 413 - Implementation plan: one-pass partition reservoir

Pairs with `plans/adr/413_one_pass_partition_reservoir.md`. The capstone mechanism: hoist the source scan out of the
per-group loop at `try_load_resident_partitioned_streamed_inner_scan_build` - one pass writes all partitions'
`SpilledInnerJoinBuild` reservoirs, groups load from the reservoir instead of re-scanning. q05 primary (44 scans ->
1 pass), q12 co-driver (index shards per active group). Reuse writers/pages/loader/active-group probe; no second spill
subsystem; no cap bump. Stop-line: probe/output-order rewrite -> seal as design/spec with proof.

## Phase 0 - measure + confirm (before code)
1. **q05 stage identification** (bounded probe, hooks on):
   ```bash
   systemd-run --user --same-dir --wait --collect --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=360s \
     --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH=target/tpch/adr413_q05_sf10_scan_progress_pre.jsonl \
     --setenv=TORON_JOIN_BUILD_PROFILE_PATH=target/tpch/adr413_q05_sf10_join_profiles_pre.jsonl \
     /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q05 \
       --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr413_q05_sf10_pre.json 2> target/tpch/adr413_q05_sf10_pre.err || true
   ```
   Record: the walling/looping join stage, `partition_count`/`group_count`, per-pass scan volume, confirmation the
   `:9443` loop is the producer, and why the stage was outside 409's terminal flip.
2. **q12 route + index arithmetic:** re-probe q12 (component payload shows the split); compute per-active-group index
   bytes under the reservoir (total index / groups; max-partition skew). **Skew floor named now if max-partition index
   alone > cap.** Confirm whether q12's failing route joins the partitioned-group path or needs the same sharding at
   its own seam.
3. **Write-phase residency math:** `partition_count x` writer-scratch/page bytes vs the retained cap for the one-pass
   write (412's per-site payload measures it live on a synthetic probe if needed).
4. Record all three before code.

## Phase 1 - the hoist (only if Phase 0 clears)
- In the `try_load_resident_partitioned_streamed_inner_scan_build` family (and q12's route per Phase 0):
  - create `SpilledInnerJoinBuild` writers for **all** partitions up front;
  - **one** source pass: partition each row (existing partition hash/keys) -> its writer;
  - per group: `LoadedSpilledInnerBuild` loads from the reservoir pages (existing loader), builds the group index
    (existing), probes via the unchanged active-group machinery;
  - existing residency guards apply at write (all-writer scratch) and load (per-group resident) - 412's component
    payload covers both sites.
- Below-threshold / fits-resident routes: **byte-unchanged** (q08's working path must not move).
- Generic; no qNN branch; no cap/multiplier/scope change; no second spill subsystem.

## Tests (discriminating)
- **One-pass proof:** a synthetic multi-group over-cap scan-build asserts the source is scanned **once** (scan-count
  hook / recording scan) while producing output row-for-row equal to the current re-scan path - fails on pre-413 code
  (which scans per group);
- **Group residency:** only the active group's loaded build/index is resident (existing guards + payload asserted);
- **Skew handling:** a deliberately skewed key set either completes (groups still under cap) or fails with the named
  producer + per-partition payload (the skew floor is diagnosable, not silent);
- **Write-phase guard:** all-partition writer scratch over a tiny cap fails cleanly at the write site (guard armed);
- **q08-route fence:** fits-resident and small-build paths take their existing routes (assert mode unchanged);
- carry spill/join/chain/adaptive suites green.

## Phase 2 - re-measure
```bash
# q05 primary (hooks on; mechanism proof = scan starts ~1 per source)
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH=target/tpch/adr413_q05_sf10_scan_progress_post.jsonl \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q05 \
    --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr413_q05_sf10_post.json 2> target/tpch/adr413_q05_sf10_post.err || true
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  target/release/toron-tpch correctness --query q05 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data
# q12 co-driver (same shape; component payload shows per-group index under cap or the named skew floor)
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q12 \
    --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr413_q12_sf10_post.json 2> target/tpch/adr413_q12_sf10_post.err || true
# correctness for any greened cell + controls
for q in q03 q04 q08 q10 q19; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=360s \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data
done
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr413_q03_sf1_control
```
- q05: scan collapse (44 -> ~1 per source) + pass@compare, or named + reproduced next wall.
- q12: off the index wall (pass@compare / named next wall), or the skew floor named with per-partition evidence.
- Controls: q03/q04/q08/q10/q19 pass@compare (q08 = same-path sentinel; q19 = spill sentinel); q03 sf=1 four-way in
  band (fp `666e0ac028036f70`).

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron spill --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (core executor cut); JOBS=4 if oomd
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast   # REQUIRED (full crate)
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)
`crates/toron/plans/impl/413_one_pass_partition_reservoir_results.md`:
- Phase 0: q05 stage + loop confirmation (+ why-not-409), q12 route + per-group index arithmetic (skew floor if
  present), write-phase residency math - before code;
- **either** the hoist (seam, one-pass write, unchanged loader/probe, guards) + q05 scan-collapse proof + q12
  index-shard proof + reach-board moves + all controls, **or** the sealed reservoir design/spec with the order-contract
  proof (stop-line);
- confirmation: no cap/multiplier/scope change, no second spill subsystem, q08/q19 sentinels green, full gates, no
  `.perf.data`, scope honesty (skew floor / next walls named; execution speed a separate thread).

## Risk / blast radius
- **Skew floor** -> named with per-partition evidence; recursive split is a future mechanism.
- **Write-phase scratch x partitions** -> Phase 0 math; flush policy within existing guards; else named.
- **q08 regression** -> fits-resident/small routes byte-unchanged + control.
- **Next wall behind q05** -> reach-board honesty.
- **Order contract** -> operator stop-line: design/spec with proof, no unsafe reorder.

## Seal
`plan: adr 413 one pass partition reservoir` (the dual). Implementation (hoist or design/spec + evidence):
`impl: adr 413 one pass partition reservoir`. Do not commit `.perf.data`.
