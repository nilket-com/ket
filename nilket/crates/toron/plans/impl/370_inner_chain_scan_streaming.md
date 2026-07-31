# ADR 370 - Implementation plan: B3-inner-chain scan-output miss (q07, diagnostic-first)

Pairs with `plans/adr/370_inner_chain_scan_streaming.md`. **Diagnostic-first**: the reoriented-inner-chain path already
streams its scans (`bound_scan_request`+`storage.scan`, streaming.rs:1900+), so 370 finds *why q07 doesn't reach it* and
removes that exact miss. q07 canary; q08/q09 only if they share the miss. Tight green; engine-cut seal gate.

## Phase 0 - the miss reason (the first deliverable; before any fix)
1. **Producer:** capture q07's exact `scan output accumulator` producer at sf=10 (which scan; bound plan + temporary
   trace; not the generic name).
2. **Miss class:** instrument/inspect `extract_reoriented_inner_chain` (streaming.rs:178) on q07's bound plan -
   **match-fail** (q07 never enters the streamed path) or **matched-but-one-scan-edge-materialized**.
3. **Plan-shape compare:** q07's persisted-Parquet (360) plan vs the in-memory plan 343/357 covered - record any
   shape difference (extra exchange, projection, the 2-nation/EXTRACT shape, etc.) that breaks the match.
4. Record the miss reason in the results doc. **No fix until this is named.**

## D1 - remove the measured miss (per the diagnosis)
- **Match-fail:** widen `extract_reoriented_inner_chain` to q07's shape (the minimal predicate change so q07 enters the
  existing streamed path); a shape-fence so it doesn't over-match.
- **Uncovered edge:** stream the named scan-edge via the shared `bound_scan_request` (the existing pattern).
- **Persisted-plan difference:** align the recognizer to the legitimate persisted plan shape. Do not reshape the
  persisted data surface merely to make the recognizer match; change the data surface only if Phase 0 proves the
  persisted plan is itself wrong.
- The fix must **remove the step-1 producer** (verified by re-measure). Topology-named; no qNN; no general recognizer.

## D2 - q08/q09 coverage (only if shared)
- Re-run q08/q09 after the q07 fix; if they share q07's miss (the 357 generalization covers them), claim them; else
  record the split (q08/q09 -> their own diagnosed miss, named follow-up). Claim only measured.

## Tests / safety net
- **q07 correctness:** strict `pass@compare` sf=0.01/sf=1; **persisted sf=10** `pass@compare` (365) only if green;
  dev parity (inner-chain streamed == materialized) sf=0.01.
- **Each newly-green cell:** persisted sf=10 `pass@compare`.
- **Recognizer fence (if widened):** a test that q07's shape now matches `extract_reoriented_inner_chain` and a
  non-matching shape still falls back (no over-match).
- **Miss-removed:** the step-1 producer is gone for q07 at sf=10 (live re-measure).
- **ScanRequest parity** (drift test); **no cap bump** (assert); **de-branding guard** (no qNN in core).

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01   # strict
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 1      # strict
# persisted sf=10 correctness (if green) + the live q07/q08/q09 re-measure (fixed 4 GB):
#   systemd-run --user --property=MemoryMax=4G --setenv=TORON_TPCH_DUCKDB_BIN=<duckdb> ... \
#     target/release/toron-tpch correctness --query q07 --sf 10 --data-source parquet --data-dir target/tpch/data
#   ... perf --sf 10 --query q07,q08,q09 --engines toron --data-source parquet --data-dir target/tpch/data ...
git diff --check
#   miss reason recorded; q07 producer removed -> green+persisted correctness OR named downstream wall; q08/q09 measured
#   or split; ScanRequest parity; --no-fail-fast green; no cap bump; no qNN
```

## Results doc
`crates/toron/plans/impl/370_inner_chain_scan_streaming_results.md`: **the q07 miss reason** (producer + match-fail vs
uncovered-edge vs persisted-plan-difference + the shape evidence) - the primary deliverable; the fix (recognizer widen
/ edge stream) + proof it removes the measured miss; q07 sf=10 green + persisted correctness OR named downstream wall;
q08/q09 coverage or the recorded split; strict + dev parity; confirmation no cap bump, temporary instrumentation
removed, `--no-fail-fast` green, no qNN.

## Risk / blast radius
- **Redundant stream** - the path already uses storage.scan; the diagnostic-first decision prevents adding a no-op.
- **Recognizer over-match (if widened)** - a fence test confirms non-matching shapes still fall back.
- **Miss not shared** - q07-only claim + split recorded.
- **Wall-move** - named downstream producer (advanced-not-green).
- **Regression** - `--no-fail-fast` required.

## Seal
`impl: adr 370 inner chain scan streaming`. Do not commit wip drafts.
