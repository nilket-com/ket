# ADR 417 - Implementation plan: scan-output accumulator family (q13/q17/q18/q21)

Pairs with `plans/adr/417_scan_output_accumulator_family.md`. Diagnostic-first, per-cell: classify each query's exact
recognizer miss (no single-family pre-classification), audit q13's lost ADR 369 coverage and classify q21 against its
ADR 372 history (new downstream scan vs lost eligibility), restore before inventing, and extend one recognizer only
where the miss is genuinely shared. ScanRequest parity is a hard requirement. Stop-line: consumers needing a general
pull interface -> seal the C design spec; no partial generic executor rewrite.

## Phase 0 - per-query classification (before any code)
1. **Reproduce all four** (fast: 0.5-6 s each; scratch-clean between; 415 `.meta` protocol):
   ```bash
   mkdir -p target/tpch/adr417
   for q in q13 q17 q18 q21; do
     unit="toron-adr417-${q}-pre"
     systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
       --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
       --property=RuntimeMaxSec=360s \
       --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH="target/tpch/adr417/${q}_scan_progress.jsonl" \
       /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query "$q" \
         --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
       > "target/tpch/adr417/${q}_pre.json" 2> "target/tpch/adr417/${q}_pre.err"
     { echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result --property=MemoryPeak; } \
       > "target/tpch/adr417/${q}_pre.meta"; systemctl --user reset-failed "$unit" 2>/dev/null || true
     rm -rf /tmp/toron_join_spill
   done
   ```
2. **Persisted-path plan proof + classification table:** instrument the exact persisted sf=10 `perf-run-one` bind
   with a temporary/off-by-default parent-path artifact. For the exact scan identified by the progress hook, record
   plan fingerprint + scan node + parent chain through the candidate recognizer. Then fill one row per query:
   overflowing table/scan | projection | immediate consumer | enclosing recognizer | exact recognizer miss. An
   in-memory fixture bind or a shape reconstructed from SQL does not satisfy this row.
3. **History audit:**
   - **q13 vs ADR 369:** read `plans/adr/369*`/`plans/impl/369*`; identify the streamed LeftOuter grouped recognizer;
     diff its expected shape against q13's current bound plan; name the drift source (which later ADR changed the
     shape or fence). If no retained artifact identifies one commit, record the narrowest proven commit interval and
     current failed recognizer condition; do not guess an ADR.
   - **q21 vs ADR 372:** read the 372 docs; determine whether today's wall is a new downstream scan (progress) or
     lost eligibility (regression), and name the change when proven. Otherwise record the narrowest proven interval
     and current failed condition rather than guessing.
4. Record everything before code.

## Phase 1 - restore / extend, per cell (only where Phase 0 clears)
- **Restore** (q13, possibly q21): fix the named recognizer miss - shape-match repair or fence correction - without
  regressing the ADR that introduced the drift (its tests stay green).
- **Extend** (q17/q18 if their consumers are streaming-capable): admit the topology at the existing recognizer;
  **one recognizer per genuinely-shared miss**, no umbrella.
- **Implementation cap:** land at most one bounded recognizer mechanism in 417. Multiple shape aliases may ride only
  when they enter the same existing machinery; a pure q13/q21 restoration may ride only if it adds no second
  execution mechanism. If the table names unrelated cuts, implement the highest-coverage bounded subset and split
  the rest into named follow-ons.
- **ScanRequest parity:** every bypass builds via `bound_scan_request`; projection, pushed predicates, row limit, row
  identity, cancellation, profiling preserved.
- **Rejected shapes:** scan spill without a pulling consumer; any partial generic pull-interface; cap nudges.
- Cells classified as pull-interface-required: **no code** - they feed the C-spec section of the results doc.

## Tests (discriminating, per admitted topology)
- **Materialized-vs-streamed parity:** same rows (and order semantics) as the vanilla arm for each newly admitted
  topology - fails if the streamed path drops/reorders/duplicates;
- **ScanRequest parity:** compare all observable request fields (projection, pushed predicate, row limit, row
  identity, profile hint, and other stable request metadata). Cancellation/trace handles remain structurally shared
  by requiring the bypass to call `bound_scan_request`, never reconstructing a request locally;
- **Restoration regression locks:** q13's recognizer catches the current shape (fails pre-417); the drift-source
  ADR's own tests stay green;
- **Recognizer fence:** topologies outside the admitted shape still take the vanilla arm (no over-admission);
- carry the streaming/join/spill/reservoir suites green.

## Phase 2 - re-measure (per-cell outcomes)
```bash
# drivers (bounded, hooks on for the scan-count/streamed proof), then per-cell:
#  - completing cells (except q17): persisted correctness --sf 10 (600s window)
#  - q17 if it reaches output: record reach_only + q17_sf10_oracle_policy_gap (live), and run its supported
#    oracle scale (sf=0.02) as SECONDARY evidence only
# controls: spill-bearing greens
for q in q03 q07 q09 q10 q12 q14 q16 q19 q20; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data \
    >> target/tpch/adr417/controls.out 2>&1
  rm -rf /tmp/toron_join_spill
done
# remaining greens cheap-optional; q03 sf=1 four-way if recognizer changes touch shared binder/exec code
```
- Per-cell primary: off the accumulator + pass@compare (q17: reach_only + policy gap + sf=0.02 secondary), or a
  named + reproduced next wall, or pull-interface classification -> C-spec.
- Mechanism proof per greened cell: the progress hook shows the overflowing scan streaming (no accumulator), scan
  counts sane (no re-scan pathology introduced).
- Controls: spill-bearing greens pass@compare; forensic bands (q03 233,338,304 / q10 215,703,848 / q12 159,510,976 /
  q19 51,471,552 / q07 134,732,480 / q09 108,992,256); q20 residency watch.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron streaming --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (executor cut); JOBS=4 if oomd
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast   # REQUIRED (full crate)
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)
`crates/toron/plans/impl/417_scan_output_accumulator_family_results.md`:
- Phase 0: the persisted-path parent-plan artifacts, four-row classification table, and the q13 (ADR 369) and q21
  (ADR 372) history audits - before code;
- per-cell outcomes: restoration/extension (recognizer, miss, fix, parity tests) with reach-board move + correctness
  provenance (q17: reach_only + policy gap + sf=0.02 secondary), **or** named next wall, **or** pull-interface
  classification;
- the C-spec section if any cell requires it (consumers, why per-recognizer work cannot admit them, what the pull
  interface must provide - the four-driver evidence base);
- controls + forensic bands; confirmation: no cap/scope change, ScanRequest parity held, no scan-spill-without-puller,
  full gates, no `.perf.data`, scope honesty (q02/q22/q05/q11 remain named).

## Risk / blast radius
- **Four different seams** -> per-cell honesty; subset outcomes acceptable.
- **Diagnostic expands into four cuts** -> one-mechanism implementation cap; split unrelated recognizers.
- **Deep q13 drift or q21 history ambiguity** -> the audit names the proven change boundary; restoration must not
  regress the change's win.
- **Order-bearing consumers** -> parity tests per topology before admission.
- **Walls behind walls** -> reach honesty.
- **Pull-interface required** -> C-spec sealed; no partial rewrite.

## Seal
`plan: adr 417 scan output accumulator family` (the dual). Implementation:
`impl: adr 417 scan output accumulator family`. Do not commit `.perf.data`.
