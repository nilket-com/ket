# ADR 411 - Implementation plan: sort materialization cap alignment

Pairs with `plans/adr/0411_sort_materialization_cap_alignment.md`. Align `execute_sort`'s materialization cap with the
x16 materialization-family policy via a named `sort_materialization_memory_cap(...)`. Policy alignment, not an
external-sort mechanism: over-family-cap sorts must still fail. Stop-line: if q10 needs more than the family cap, no
policy change - external sort required.

## Phase 0 - prove + quantify (before code)
1. Reproduce q10's sort wall on the 411 worktree (fresh clean release build first):
   ```bash
   systemd-run --user --same-dir --wait --collect --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=360s \
     /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q10 \
       --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr411_q10_sf10_pre.json 2> target/tpch/adr411_q10_sf10_pre.err || true
   ```
   Record requested bytes, input rows/batches at the failure (the wall payload + progress hooks if useful).
2. Quantify: requested vs raw task budget (67,108,864) vs family cap (1,073,741,824). **If requested > family cap ->
   stop-line: seal diagnostic, external sort required, no policy change.**

## Phase 1 - the cut
- Add `sort_materialization_memory_cap(task_memory: u64) -> u64` near `execute_sort` with a named multiplier constant
  (e.g. `SORT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER: u64 = 16`) and a comment tying it to the materialization-family
  policy (join build x16, spill retained x16). Swap `execute_sort`'s check from raw `task_budget.memory.get()` to the
  named cap. Producer string and failure shape unchanged.
- Nothing else changes: no scope/cgroup/`memory_per_task`/sibling-cap edits; no query branch.

## Tests (discriminating)
- **Alignment fires:** a synthetic sort whose estimate is just over the raw task budget but under the family cap
  completes and returns correctly ordered output - **fails on pre-411 code** (old check errors);
- **Honesty guard holds:** a synthetic sort whose estimate exceeds the family cap still fails with
  `sort materialization` (requested/available reflect the new cap) - locks "aligned, not unbounded";
- offset/limit/empty-input sort behavior unchanged (existing sort tests green);
- carry the join/chain/adaptive suites green (no coupling).

## Phase 2 - re-measure
```bash
# driver: q10 sf=10
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q10 \
    --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr411_q10_sf10_post.json 2> target/tpch/adr411_q10_sf10_post.err || true
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  target/release/toron-tpch correctness --query q10 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data
# controls: sf=10 correctness + q03 sf=1 four-way
for q in q03 q04 q08 q19; do
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
    --output-dir target/tpch/perf_adr411_q03_sf1_control
# optional fence evidence: q12 wall unchanged (different producer; ~8 s probe)
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  target/release/toron-tpch perf-run-one --engine toron --query q12 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr411_q12_sf10_fence.json 2>&1 || true
```
- Primary: q10 pass@compare, or a named + reproduced next wall.
- Controls: q03/q04/q08/q19 pass@compare; q03 sf=1 four-way in band (fp `666e0ac028036f70`, spill 0); q12 wall
  unchanged (67,728 over) as fence evidence.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron sort --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (executor cut); JOBS=4 if oomd
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast   # REQUIRED (full crate)
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)
`crates/toron/plans/impl/0411_sort_materialization_cap_alignment_results.md`:
- Phase 0: reproduced wall + requested/rows vs raw and family caps - before code;
- the named cap (constant, comment, call site) + both discriminating tests (alignment fires / honesty guard holds);
- q10 reach-board move (pass@compare or named + reproduced next wall) + all controls + the q12 fence;
- confirmation: policy alignment framing (not external sort), no budget/scope drift, full gates green, no
  `.perf.data`, scope honesty (external sort named for over-family-cap sorts; 412 = q12 shedding policy; then q05
  reservoir).

## Risk / blast radius
- **Requested > family cap** -> Phase 0 stop-line; external-sort ADR instead.
- **Next wall behind the sort** -> reach-board honesty.
- **Unbounded-sort drift** -> over-family-cap test locks the ceiling.
- **Coupling** -> named-function swap at one call site; full gates + controls.

## Seal
`plan: adr 411 sort materialization cap alignment` (the dual). Implementation:
`impl: adr 411 sort materialization cap alignment`. Do not commit `.perf.data`.
