# ADR 371 - Implementation plan: B3-semi/anti scan-output miss (q04, diagnostic-first)

Pairs with `plans/adr/0371_semi_anti_scan_streaming.md`. **Diagnostic-first**: the 345 existence path already streams/
spills, so 371 names why q04's scan materializes before it and removes that exact miss. q04 canary; q18/q22 only if they
share the measured miss. The **producer table** is the coverage contract. Tight green; engine-cut seal gate.

## Phase 0 - name the q04 miss (before code)
1. **Producer:** capture q04's exact `scan output accumulator` producer at sf=10 (which scan/TableId; bound plan +
   temporary trace).
2. **Miss class:** inspect `extract_streaming_inner_prefix_semi_anti_chain` (streaming.rs:1127) /
   `decompose_streaming_existence_plan` (1079) on q04's bound plan - **recognizer miss** or
   **matched-but-scan-materialized** (370 pattern).
3. **Plan-shape compare:** q04 persisted-Parquet (360) plan vs the in-memory plan 345 covered.
4. Record the miss reason in the results doc. **No fix until named.**

## D1 - remove the measured miss (per the diagnosis)
- **Recognizer miss:** widen the existence recognizer to q04's shape (minimal, shape-fenced so it doesn't over-match).
- **Matched-but-scan-materialized:** stream the named scan-edge via the shared `bound_scan_request` + the existing 345
  existence path (the 370 pattern: prepare the downstream/probe scan through the streamed path).
- The fix must **remove the step-1 producer** (verified by re-measure). Topology-named; no qNN; no general recognizer.

## D2 - q18/q22 coverage (only if shared)
- Re-run q18/q22 after the q04 fix; fill the producer table. Claim a cell only if its measured miss matched q04's and
  the re-measure shows green or a named wall-move; else record the split.

## Tests / safety net
- **q04 correctness:** strict `pass@compare` sf=0.01/sf=1; **persisted sf=10** `pass@compare` (365) only if green;
  dev parity (semi/anti existence streamed == materialized) sf=0.01.
- **Each newly-green cell:** persisted sf=10 `pass@compare`.
- **Existence-streaming correctness:** a synthetic EXISTS (semi) + NOT-EXISTS (anti) streamed == materialized (digest,
  strict + dev), including empty-existence and all-match edges.
- **Recognizer fence (if widened):** q04's shape now matches; a non-matching shape still falls back (no over-match).
- **Miss-removed:** the step-1 producer gone for q04 at sf=10 (live re-measure).
- **ScanRequest parity** (drift test); **no cap bump** (assert); **de-branding guard** (no qNN in core).

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q04 --sf 0.01   # strict
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q04 --sf 1      # strict
# persisted sf=10 correctness (if green) + the live q04/q18/q22 re-measure (fixed 4 GB):
#   systemd-run --user --property=MemoryMax=4G --setenv=TORON_TPCH_DUCKDB_BIN=<duckdb> ... \
#     target/release/toron-tpch correctness --query q04 --sf 10 --data-source parquet --data-dir target/tpch/data
#   ... perf --sf 10 --query q04,q18,q22 --engines toron --data-source parquet --data-dir target/tpch/data ...
git diff --check
#   q04 miss named; producer table filled; q04 producer removed -> green+persisted correctness OR named existence-set
#   wall; q18/q22 measured or split; ScanRequest parity; --no-fail-fast green; no cap bump; no qNN
```

## Results doc
`crates/toron/plans/impl/0371_semi_anti_scan_streaming_results.md`: **the q04 miss reason** (producer + recognizer-miss
vs matched-but-scan-materialized + shape evidence); **the producer table** (query | semi/anti kind | scan-output
producer before | miss class | producer after | coverage verdict) filled live; the fix + proof it removes the measured
miss; q04 green + persisted correctness OR named existence-set wall; q18/q22 coverage or split; **q21 convergence note**
if q04/q18/q22 land on the existence-set producer; strict + dev parity; confirmation no cap bump, temporary
instrumentation removed, `--no-fail-fast` green, no qNN.

## Risk / blast radius
- **Wall-moves to existence-set spill** - expected/successful; producer table records it; q21 convergence re-prioritizes
  the deferred existence-set cut.
- **Miss not shared** - q04-only claim + split.
- **Recognizer over-match (if widened)** - fence test.
- **Silent answer change** - shared builder + persisted correctness + dev parity (EXISTS/IN/NOT-EXISTS).
- **Regression** - `--no-fail-fast` required.

## Seal
`impl: adr 371 semi anti scan streaming`. Do not commit wip drafts.
