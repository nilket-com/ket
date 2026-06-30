# ADR 388 - Implementation plan: scan-output streaming for the multi-table inner-build family (q03/q05/q10)

Pairs with `plans/adr/388_scan_output_inner_build_family.md`. Engine cut: diagnose q03's distinct scan-output miss
(multi-table join + spilled inner build), extend the 387 scan-streaming + memory-aware gate to it, claim only measured
coverage. Record downstream spill before/after for wall-moves. q03 canary; no cap bump; full engine gate.

## Phase 0 - diagnose q03 (before code)
Run q03 at sf=10 with enough trace/local instrumentation to fill the producer table:
1. Which scan materializes into `scan output accumulator` (which table, where in the customer/orders/lineitem chain)?
2. Which consumer receives it (the spilled inner build / the multi-table join / the group)?
3. Did a streamed recognizer match and leave this edge materialized, or miss q03's multi-table-with-spilled-inner shape
   (387 covered only the bare/filtered Exchange(Join(0)) base)?
4. Persisted-Parquet vs lower-scale plan difference?
5. Which `ScanRequest` fields: projection, PBC, runtime filter, row identity, profile hint, morsel, row limit, cancel?
6. Memory-aware selector evidence: estimated scan-output bytes, cap, and selected path for q03 sf=10 and the sf=1
   control.
Record the q03 producer row before landing the fix.

## D1 - q03 canary (smallest shape-fenced extension)
- Reuse `bound_scan_request` + direct `storage.scan`/`ScanStream`; stream q03's measured scan edge into its
  multi-table-join/spilled-inner consumer; no `materialize_scan_stream -> Vec`.
- **Reuse the 387 memory-aware gate** (`reoriented_inner_stage_scan_build_exceeds_scan_output_cap`-style): sf=1/small
  scale stays materialized; stream only when memory-bound. Record the selector's estimate/cap/path in the results.
- Preserve output column order, residual/filter, projection, multi-table join + group semantics. Shape-fenced; no qNN.
- If q03 moves to a downstream producer, **record that producer + its spill_bytes (before/after)**; stop unless a
  measured sibling shares the shape.

## D2 - measured coverage + spill characterization
- Re-measure q03/q05/q10 live at sf=10: green / advanced / unchanged.
- For each **wall-moved** row, record the **downstream producer's `spill_bytes` before vs after** (386 baseline 1.43 GiB
  -> measured): same / increased / changed-shape, with the producer named. This characterizes the next cut.
- Extend beyond q03 only when Phase 0 shows q05/q10 share the miss / the same ScanRequest+consumer path.

## Tests
- streamed path == materialized path on a small synthetic plan matching q03's multi-table-inner-build shape (digest,
  column order, schema, residual/filter);
- ScanRequest parity covers every bound scan field used by the direct path;
- **memory-aware gate:** sf=1/small-scale stays materialized (a control proving the streamed path is not selected where
  materialization is cheaper);
- grouped/multi-table-join variant exercising the shared consumer if q05/q10 share the shape;
- prior greens (q04/q06/q14/q19) + q08 wall-moved state unchanged.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast       # REQUIRED
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01   # strict pass@compare
# fixed-cap sf=10 re-measure q03/q05/q10: producer + spill_bytes before/after; persisted sf=10 correctness if green
git diff --check
#   q03 miss diagnosed; memory-aware gate (sf=1 materialized); q03 green or named-downstream w/ spill before/after;
#   q05/q10 measured; ScanRequest parity; no cap bump; --no-fail-fast + toron-tpch green; no qNN
```

## Results doc
`crates/toron/plans/impl/388_scan_output_inner_build_family_results.md`:
- the q03 (and q05/q10) producer table: materializing scan, consumer, miss class, producer after;
- the cut (scan-streaming extension + memory-aware gate) + the sf=1 materialized-path control;
- q03/q05/q10 live re-measure: green / advanced / unchanged;
- **downstream spill_bytes before/after for wall-moved rows** (same/increased/changed-shape + named producer) - the
  next-cut characterization;
- memory-aware selector evidence: estimate/cap/path for the sf=10 canary and the sf=1 materialized-path control;
- persisted sf=10 correctness for greens; small-scale parity; ScanRequest parity;
- confirmation no cap bump, `--no-fail-fast` + `toron-tpch` green, no qNN, prior greens + q08 unchanged.

## Risk / blast radius
- **q03 miss differs from q05/q10** -> q03-only + split table.
- **Different spill-budget term exposed** -> the before/after spill bytes name the real next producer.
- **Small-scale perf regression** -> memory-aware gate + sf=1 control.
- **Plan/parity drift** -> shared builder + parity tests.
- **Regression** -> `--no-fail-fast` + `toron-tpch`.

## Seal
`impl: adr 388 scan output inner build family`. Do not commit wip drafts or large `.perf.data`.
