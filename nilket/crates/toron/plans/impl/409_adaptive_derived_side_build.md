# ADR 409 - Implementation plan: adaptive derived-side build (over-cap streamed-inner stages)

Pairs with `plans/adr/409_adaptive_derived_side_build.md`. Execute 408's named fork: when a reoriented streamed-inner
stage's scan-side build is over-cap (today: ~30.8 lineitem passes, 82 GB), materialize the derived side under the cap,
measure it, and if it fits build on it and stream the scan side in a single pass. Fenced to the over-cap path; fork B
(one-pass partition reservoir) stays a named separate cut. No q03 branch, no cap bump.

## Phase 0 - measure + seam + audit (before code)
1. **Measured derived-side size at sf=10.** Run the c x o subplan in isolation under the bounded scope with the 408
   hooks (`TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH`, `TORON_JOIN_BUILD_PROFILE_PATH`) and record actual rows/bytes +
   `estimated_build_hash_bytes`. (SQL isolation: `SELECT o_orderkey, o_orderdate, o_shippriority FROM customer, orders
   WHERE c_custkey = o_custkey AND c_mktsegment = 'BUILDING' AND o_orderdate < DATE '1995-03-15'` at sf=10, or the
   equivalent harness probe.) The 408 `orders` 1,024-row artifact is only partial progress and is not size evidence. If
   the measured derived side does not fit the build cap -> fork B, stop.
2. **Seam confirmation.** Confirm the flip hangs at the existing over-cap decision
   (`reoriented_inner_stage_scan_build_decision` and the `PartitionedScan` routing behind it): what "materialize the
   derived side" means there (the derived side is currently the streaming probe), whether the existing
   `try_visit_streaming_inner_join_scan_probe_chunks` / `visit_streaming_in_memory_inner_scan_probe_chunks` helper
   surface can be reused by swapping build/probe roles, and that the cap-guarded materialization can abort back to
   today's path.
3. **Order/consumer audit.** What consumes the chain output for q03/q05/q10/q12 (aggregate/sort above?); does any
   order-bearing contract bind the stage's emission order? If yes -> stop-line, fork B.
4. Record all three in the results doc before code.

## Phase 1 - the flip (fork A; only if Phase 0 clears)
- At the over-cap branch of the reoriented stage decision: materialize the derived (probe) side **under the memory
  cap** (abort -> today's partitioned path on overflow); measure actual bytes; if under the build cap:
  - build on the derived side (405 `PrimitiveBuildTable` via the existing `build_in_memory_join_rows` seam);
  - stream the scan side through as a single-pass probe, using the existing in-memory-build / streaming-scan-probe path
    where possible;
  - restore column order via `probe_columns_first`/existing restore machinery;
  - memory guard: the derived build passes the same `estimated_build_hash_bytes` fit decision.
- Below-cap stages and non-fitting derived sides: **byte-unchanged** current behavior.
- Generic: keys on measured bytes + plan shape; no query/table branch.

## Tests (discriminating)
- **Flip fires:** over-cap scan-side build + fitting derived side -> derived-side build, single-pass scan probe (assert
  the mode + one scan start via the progress hook in an integration probe);
- **Seam reuse / variant fence:** if the implementation cannot reuse the existing in-memory-build / streaming-scan-probe
  helper, any new runtime variant is narrow to the adaptive flip and does not introduce a broad executor rewrite;
- **Flip declines:** over-cap + non-fitting derived side -> today's `PartitionedScan` path (assert unchanged);
- **Fence:** below-cap stage -> byte-unchanged existing path;
- **Correctness across orientation:** flipped stage output equals the unflipped path's output (same rows; order per the
  Phase 0 audit), column order restored, nulls route as no-match;
- **Cap-guarded materialization:** derived-side materialization exceeding the cap aborts cleanly to today's path;
- carry the join suite + chain tests green.

## Phase 2 - re-measure
```bash
# driver: q03 sf=10 (mechanism proof: ONE lineitem pass via progress hook, then the perf rail)
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH=target/tpch/adr409_q03_sf10_scan_progress.jsonl \
  /usr/bin/time -v timeout -k 10s 360s \
    target/release/toron-tpch perf-run-one --engine toron --query q03 --mode strict --sf 10 \
      --run-index 0 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr409_q03_sf10_probe.json 2> target/tpch/adr409_q03_sf10_probe.err || true
# if it completes: perf rail runs=3 + persisted correctness at sf=10
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 10 --mode strict --runs 3 --warmup 1 --query q03 \
    --engines toron,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr409_q03_sf10
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness --query q03 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data
# won ground: q03 sf=1 four-way unchanged (below-cap fence)
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr409_q03_sf1_control
# controls + generalization
for q in q04 q19 q08; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 3 --warmup 1 --query "$q" \
      --engines toron,duckdb --data-source parquet --data-dir target/tpch/data \
      --output-dir "target/tpch/perf_adr409_${q}_sf10"
done
# q05/q10/q12: single bounded re-probe each (RuntimeMaxSec-style timeout), record movement
```
- Primary: q03 sf=10 pass@compare (or named + reproduced next wall) + the one-pass scan proof.
- sf=1 four-way within band, fp `666e0ac028036f70`; q08 keeps its 403 win; q19 keeps its spill.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (core executor cut)
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast   # REQUIRED (full crate)
target/release/toron-tpch correctness --query q03 --sf 0.01 --mode strict --data-source parquet --data-dir target/tpch/data
git diff --check
find . -name '*.perf.data' -print
```
Note: cap rustc jobs (`CARGO_CGROUP_JOBS=4`-`12` per machine headroom); rebuild release clean before probes.

## Deliverable (results doc)
`crates/toron/plans/impl/409_adaptive_derived_side_build_results.md`:
- Phase 0: measured c x o size (rows/bytes/fit verdict), seam confirmation, order/consumer audit - before code;
- **either** fork A (the flip: decision site, fence, cap-guarded materialization, tests) + q03 sf=10 reach-board move +
  the one-pass mechanism proof (progress hook: 1 lineitem pass vs 408's 30.8) + sf=1 control + q04/q19/q08 controls +
  q05/q10/q12 generalization, **or** the fork-B pivot (measured non-fit / order contract) with the reservoir named;
- confirmation: measured-bytes gate (no planner cardinality), no cap bump, no q03 branch, full gates green, no
  `.perf.data`, scope honesty (B remains for non-fitting derived sides).

## Risk / blast radius
- **Derived side too big** -> runtime fence falls back; fork B named.
- **Chain dataflow inversion** -> cap-guarded materialization, only on the already-pathological path.
- **Order contract binds** -> stop-line to B.
- **q08 family regression** -> control; flip must not fire where scan-side build is right.
- **Next wall after the flip** -> named + reproduced (reach-board honesty).
- **Core-join regression** -> full gates + chain tests.

## Seal
`plan: adr 409 adaptive derived side build` (the dual). Implementation (flip or fork-B pivot + evidence + measure):
`impl: adr 409 adaptive derived side build`. Do not commit `.perf.data`.
