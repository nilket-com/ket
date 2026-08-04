# ADR 369 - Implementation plan: scan-to-join streaming (q13 canary)

Pairs with `plans/adr/0369_scan_to_join_streaming.md`. A `crates/toron` engine cut: stream scan -> join -> group/agg
(q13 canary), reusing the shared `bound_scan_request` + the keyed join machinery + 368's grouped fold. 4-sub-bucket
coverage contract; tight green-definition (no join-output producer + persisted correctness); engine-cut seal gate.

## Phase 0 - the B3 coverage contract (before implementation)
- Classify the 8 B3 cells into: **B3-left-outer-simple** (q13), **B3-inner-chain** (q07/q08/q09-class),
  **B3-correlated** (q17), **B3-semi/anti-or-exists** (q04/q18/q22 members routing through existence/correlation).
  Record the table = the coverage contract.
- Confirm q13: `customer LEFT OUTER JOIN orders ON c_custkey=o_custkey AND o_comment NOT LIKE ... GROUP BY c_custkey`
  -> re-group; the wall is the scan output accumulator (scan materializes before the join).
- Name the exact q13 scan edge(s), join side(s) (preserved/build/probe), residual placement, and null-fill owner before
  implementation. LeftOuter semantics are preserved only if unmatched preserved-side rows and null-fill remain owned by
  the original preserved side.
- For each cell, predict: does scan->join streaming green it, or move it to a named join-output producer
  (867/969/2612/2684/2920) or another downstream producer (build/spill resident, exchange accumulator, aggregate
  accumulator)? Mark expected green vs expected wall-mover.

## D1 - q13 first, then measured extension
- Stream q13: scan(s) `ScanStream` (shared `bound_scan_request`) -> keyed LeftOuter(+residual) join -> grouped fold
  (368) -> re-group, bypassing `materialize_scan_stream -> Vec`. Residual (NOT LIKE) applied per-candidate; null-fill
  preserved; both group levels preserved.
- Extend to B3 sub-buckets the mechanism is **measured** to cover; cells whose join output materializes are recorded
  **advanced** against their named downstream producer (a successful wall-move, not green).
- Reuse the shared builder; topology-named; no qNN in core; no general join recognizer.

## Tests / safety net
- **q13 correctness:** strict `pass@compare` sf=0.01/sf=1; **persisted sf=10** `pass@compare` (365) only if green
  (scan-output gone AND no downstream producer); dev parity (streamed scan->join->group == materialized) sf=0.01.
- **Each newly-green B3 cell:** persisted sf=10 `pass@compare`.
- **Scan->join->group streaming correctness:** a synthetic scan -> LeftOuter(+residual) -> grouped -> re-group streamed
  == materialized (digest, strict + dev), including null-fill, the per-candidate residual, and both group levels.
- **ScanRequest parity:** the new edge uses `bound_scan_request`; the drift test passes.
- **Wall-mover honesty:** a cell that stops at a named downstream producer is recorded advanced-not-green (the live
  re-measure confirms scan-output gone + the named producer present).
- **No cap bump** (assert); **de-branding guard** (no qNN in core).

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q13 --sf 0.01   # strict
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q13 --sf 1      # strict
# persisted sf=10 correctness (if green) + the live B3 re-measure (fixed 4 GB), per query:
#   systemd-run --user --property=MemoryMax=4G --setenv=TORON_TPCH_DUCKDB_BIN=<duckdb> ... \
#     target/release/toron-tpch correctness --query q13 --sf 10 --data-source parquet --data-dir target/tpch/data
#   ... perf --sf 10 --query <each B3 cell> --engines toron --data-source parquet --data-dir target/tpch/data ...
git diff --check
#   B3 4-bucket coverage contract; q13 green (scan-output+downstream producers gone+persisted correctness) OR named
#   downstream wall; measured B3 coverage; ScanRequest parity; --no-fail-fast green; no cap bump; no qNN
```

## Results doc
`crates/toron/plans/impl/0369_scan_to_join_streaming_results.md`: the B3 4-sub-bucket coverage-contract table; the q13
fix (scan->join->group streaming) + its persisted sf=10 correctness OR the named downstream wall; the **live B3
re-measure** (per cell: green / advanced-to-named-producer / unchanged) - measured claim only; strict + dev parity; the
named next producer (join-output if it recurs, otherwise the measured downstream producer) and the next subset;
confirmation no cap bump, `--no-fail-fast` green, no qNN.

## Risk / blast radius
- **Join-output is the real wall** - cells advance to a named join-output producer; surfaces join-output streaming
  (q02 family) as the next target.
- **Another downstream producer is the real wall** - build/spill resident, exchange, or aggregate accumulation becomes
  the named next target instead of forcing the join-output hypothesis.
- **Overclaim** - coverage contract + tight green-definition + live re-measure.
- **Silent answer change** - shared builder + persisted correctness + dev parity (residual/LeftOuter/double-group).
- **Regression** - `--no-fail-fast` required.
- **No cap bump; no general recognizer.**

## Seal
`impl: adr 369 scan to join streaming`. Do not commit wip drafts.
