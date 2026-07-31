# ADR 408 - Implementation plan: q03 sf=10 time-cell diagnostic

Pairs with `plans/adr/408_q03_sf10_time_cell_diagnostic.md`. Diagnostic-first: attribute q03's >100x superlinear sf=10
blowup (373 ms sf=1 -> >360 s sf=10, CPU-bound), classify it, and only implement if Phase 0 names a bounded/fenceable
generic fix - else seal the streaming-completion design spec with the evidence. No q03 hack, no cap bump.

## Phase 0 - attribute the six minutes (before any fix)
0. **Clean rebuild** (the on-disk release binary is 407's rejected radix candidate):
   ```bash
   CARGO_CGROUP_BACKEND=affinity CARGO_CGROUP_JOBS=4 \
     scripts/cargo-cgroup build -p toron-tpch --features polars-sql-reference --release
   # symbol-gated profile target for sampled attribution (separate CARGO_TARGET_DIR, per 407):
   CARGO_TARGET_DIR=target/adr408_profile CARGO_BUILD_JOBS=4 CARGO_PROFILE_RELEASE_DEBUG=true \
     CARGO_PROFILE_RELEASE_STRIP=false CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
     cargo build -p toron-tpch --features polars-sql-reference --release
   ```
1. **Fresh live probe** (fixed 398 scope, bounded window). Toron SQL is the primary board cell; Rune is the
   same-executor control/isolation row. Capture parseable CPU/wall via `/usr/bin/time -v` because `systemd-run
   --quiet --pipe` may suppress the unit accounting summary:
   ```bash
   systemd-run --user --same-dir --wait --collect --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=360s --property=CPUAccounting=yes \
     /usr/bin/time -v target/release/toron-tpch perf-run-one \
       --engine toron --query q03 --mode strict --sf 10 \
       --run-index 0 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr408_q03_toron_sf10_probe.json \
     2> target/tpch/adr408_q03_toron_sf10_probe.err || true

   systemd-run --user --same-dir --wait --collect --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=360s --property=CPUAccounting=yes \
     /usr/bin/time -v target/release/toron-tpch perf-run-one \
       --engine rune --query q03 --mode strict --sf 10 \
       --run-index 0 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr408_q03_rune_sf10_probe.json \
     2> target/tpch/adr408_q03_rune_sf10_probe.err || true
   ```
   Record wall/CPU/peak + row-or-timeout for both. If Toron and Rune differ, the plan proof decides whether this is a
   SQL-shape issue or an executor issue.
2. **Plan-shape-at-scale proof:**
   ```bash
   target/release/toron-tpch q03-plan-proof --sf 10 --mode strict --data-source parquet \
     --data-dir target/tpch/data > target/tpch/adr408_q03_plan_proof_sf10.json
   ```
   Diff SQL vs Rune at sf=10, then diff both against the sf=1 proof (`666e0ac028036f70`). A SQL/Rune divergence,
   strategy flip, exchange return, or 402-collapse non-fire at sf=10 facts redirects the fix to the binder.
3. **Bounded-window attribution** (durable progress first, then one sampled pass):
   Existing `TORON_TPCH_PARQUET_SCAN_LEDGER` / `TORON_JOIN_BUILD_PROFILE` rows are in-memory and normally print only
   when execution returns. For a timeout cell, do not infer from a missing JSON row. If the normal rail cannot flush
   scan counts and join-build profiles before termination, add a temporary/off-by-default diagnostic hook that writes
   progress incrementally (for example JSONL under `target/tpch/adr408_q03_progress_*.jsonl`), then remove it or keep it
   only if it is cleanly reusable.
   ```bash
   # Existing profile-loop telemetry is useful only if it returns before the timeout.
   systemd-run --user --same-dir --wait --collect --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=360s --property=CPUAccounting=yes \
     --setenv=TORON_TPCH_PROFILE_LOOP=1 \
     --setenv=TORON_JOIN_BUILD_PROFILE=1 \
     --setenv=TORON_TPCH_PARQUET_SCAN_LEDGER=1 \
     /usr/bin/time -v \
     target/release/toron-tpch perf-run-one --engine rune --query q03 --mode strict --sf 10 \
       --run-index 0 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr408_q03_sf10_ledger.out 2>&1 || true
   # Scan ledger/progress hook: a re-scan loop must be backed by durable repeated table-read counts.
   # One sampled pass (symbol-gated binary, delayed start, inside the window). Use Toron unless the
   # sf=10 plan proof shows Rune is the only shape worth isolating.
   systemd-run --user --scope -p MemoryMax=4G -p MemoryHigh=4G -p TasksMax=512 \
     timeout 300 perf record -D 30000 -F 499 --call-graph dwarf \
       -o target/tpch/adr408_q03_sf10.perf.data -- \
       target/adr408_profile/release/toron-tpch perf-run-one --engine toron --query q03 \
         --mode strict --sf 10 --run-index 0 --data-source parquet --data-dir target/tpch/data || true
   perf report --stdio -i target/tpch/adr408_q03_sf10.perf.data --no-children --sort symbol \
     > target/tpch/adr408_q03_sf10_flat.txt
   ```
4. **Classify:** (a) partition/re-scan loop (ledger: repeated scans) / (b) materialization blowup (telemetry: giant
   build/selection volumes, RSS ramp) / (c) plan/exchange-aggregate term (SQL/Rune shape proof + frames) /
   (d) execution-model floor (linear-but-huge frames across decode/materialize with no loop). Record all evidence in
   the results doc before any fix.

## Phase 1 - the fork (Phase 0 decides)
- **Bounded fix** (classes (a)/(c), fenceable (b)): a generic, shape/size-fenced routing, planner, or placement
  correction at the named seam (403 pattern). No q03/table branch. Includes discriminating tests: the mis-routed shape
  takes the fixed path; the previously-correct shapes are unchanged.
- **Design spec** (class (d), unfenceable (b)): seal the results doc as the streaming-completion spec - the operators
  holding the floor, evidence shares, what must stream, how 407's build+probe partitioning folds in. No code.

## Phase 2 - re-measure (only if a fix lands)
```bash
# driver: q03 sf=10 under the same scope (runs=3 warmup=1 once it completes)
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 10 --mode strict --runs 3 --warmup 1 --query q03 \
    --engines toron,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr408_q03_sf10
# persisted correctness at sf=10 if it completes
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness --query q03 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data
# won ground: q03 sf=1 four-way unchanged
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr408_q03_sf1_control
# controls
for q in q04 q19 q08; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 3 --warmup 1 --query "$q" \
      --engines toron,duckdb --data-source parquet --data-dir target/tpch/data \
      --output-dir "target/tpch/perf_adr408_${q}_sf10"
done
# generalization signal: cheap bounded re-probes of q05/q10/q12 (single run each, RuntimeMaxSec=360s)
```
- Reach-board honesty: pass@compare, or a named + reproduced next wall. sf=1 four-way within band. Controls green.

## Verification (if code changes; else fmt/diff-check only, stated explicitly)
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED for an engine cut
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
find . -name '*.perf.data' -print
```
Note: cap rustc jobs (`CARGO_CGROUP_JOBS=4`); clean-rebuild hazard from 407 noted above.

## Deliverable (results doc)
`crates/toron/plans/impl/408_q03_sf10_time_cell_diagnostic_results.md`:
- Phase 0: clean-rebuild note, live probe row (wall/CPU/peak/row-or-timeout), sf=10 vs sf=1 plan proof diff,
  attribution evidence (timeout-safe scan counts, telemetry/progress rows, spill, sampled frames), classification -
  before any fix;
- **either** the bounded fix (seam, fence, discriminating tests) + q03 sf=10 reach-board move + sf=1 control +
  q04/q19/q08 controls + q05/q10/q12 generalization re-probe, **or** the sealed streaming-completion design spec;
- confirmation: no cap bump, no q03 branch, gates as applicable, no `.perf.data` committed, scope honesty.

## Risk / blast radius
- **Execution-model floor** -> the spec is the deliverable; feeds the big rock with evidence.
- **Plan flips at sf=10** -> binder-side fix path; cheaper than execution surgery.
- **Compounding causes** -> attribute shares; fix only the dominant fenceable one; name the rest.
- **In-memory ledgers vanish on timeout** -> add/use an off-by-default progress hook before classifying; missing ledger
  output is not evidence.
- **6-minute probe cost** -> ledgers first, one sampled pass, bounded runs.
- **Won-ground regression** -> sf=1 four-way control + q04/q19/q08 + full gate on any code change.

## Seal
`plan: adr 408 q03 sf10 time cell diagnostic` (the dual). Implementation (fix or spec + evidence):
`impl: adr 408 q03 sf10 time cell diagnostic`. Do not commit `.perf.data`.
