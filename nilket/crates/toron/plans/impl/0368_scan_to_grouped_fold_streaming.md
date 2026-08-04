# ADR 368 - Implementation plan: scan-to-grouped-fold streaming (q01 canary)

Pairs with `plans/adr/0368_scan_to_grouped_fold_streaming.md`. A `crates/toron` engine cut: stream scan -> exchange ->
grouped fold (q01 canary), reusing 367's scan-stream + 341's exchange-input + the grouped-fold primitives. Coverage
contract over the B2 cells; wall-mover gate; claim only measured; engine-cut seal gate.

## Phase 0 - the B2 coverage contract (before implementation)
- Classify the named B2 cells (from the 367 table) by exact grouped consumer shape: **B2-direct** (no exchange),
  **B2-exchange** (q01), **B2-mixed** (join+grouped). Record the table = the coverage contract.
- Confirm q01 is B2-exchange (`Scan -> Filter -> Exchange(returnflag,linestatus) -> GroupedAggregate`) and that the
  wall is the scan output accumulator (scan materializes before the exchange/fold), with a tiny group state.
- Identify which cells the scan -> exchange -> grouped-fold mechanism is expected to cover vs which stop at a named
  exchange wall (the gate).

## D1 - q01 first, then measured extension
- Stream q01: the scan `ScanStream` (shared `bound_scan_request`) feeds exchange partitioning by the group key, then
  `fold_bound_grouped_aggregate_batch` consumes the partitioned batches into the bounded group state, finalize via
  `finalize_bound_grouped_aggregate`. No `materialize_scan_stream -> Vec`.
- **Gate:** the green path partitions and folds without a returned exchange `Vec`. If the exchange output still
  materializes (the shuffle), q01 is a wall-mover -> record the named exchange wall, do not claim green.
- Extend to B2 cells the mechanism is measured to cover; B2-mixed / wall-moved cells are named, not overclaimed.
- Topology-named; no qNN in core; no general grouped recognizer.

## Tests / safety net
- **q01 correctness:** strict `pass@compare` sf=0.01/sf=1; **persisted sf=10** `pass@compare` (365) if green; dev
  (Float64) parity (streamed scan->exchange->grouped-fold == materialized) sf=0.01.
- **Each newly-green B2 cell:** persisted sf=10 `pass@compare`.
- **Grouped-fold streaming correctness:** a synthetic scan -> exchange -> grouped aggregate streamed == materialized
  (digest, strict + dev), including multi-batch input, multi-aggregate (SUM/AVG/COUNT), Float64-sensitive row visitation
  order within each partition, and final group output order.
- **ScanRequest parity:** the new edge uses `bound_scan_request`; the drift test passes.
- **Wall-mover honesty:** a cell that stops at the exchange is recorded advanced-not-green (a fixture or the live
  re-measure confirms the producer).
- **No cap bump** (assert the budgets unchanged); **de-branding guard** (no qNN in core).

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q01 --sf 0.01   # strict
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q01 --sf 1      # strict
# persisted sf=10 correctness (if green) + the live B2 re-measure (fixed 4 GB), per query:
#   systemd-run --user --property=MemoryMax=4G --setenv=TORON_TPCH_DUCKDB_BIN=<duckdb> ... \
#     target/release/toron-tpch correctness --query q01 --sf 10 --data-source parquet --data-dir target/tpch/data
#   ... perf --sf 10 --query <each B2 cell> --engines toron --data-source parquet --data-dir target/tpch/data ...
git diff --check
#   B2 coverage contract; q01 green+persisted-correctness OR named exchange wall; measured B2 coverage; ScanRequest
#   parity; --no-fail-fast green; no cap bump; no qNN in core
```

## Results doc
`crates/toron/plans/impl/0368_scan_to_grouped_fold_streaming_results.md`: the B2 coverage-contract table (cells ->
direct/exchange/mixed); the q01 fix (scan->exchange->grouped-fold) + its persisted sf=10 correctness OR the named
exchange wall (wall-mover); the **live B2 re-measure** (per cell: green / advanced / unchanged) - measured claim only;
strict + dev parity; the named next subset (B2-mixed, wall-moved, or the surfaced exchange producer); confirmation no
cap bump, `--no-fail-fast` green, no qNN in core.

## Risk / blast radius
- **Exchange is the real wall** - q01 advances (named exchange wall); recorded as wall-mover, surfacing exchange-output
  streaming as the next producer.
- **Overclaim** - coverage contract + live re-measure.
- **Silent answer change** - shared builder + persisted correctness + dev parity (multi-aggregate + group order).
- **Regression** - `--no-fail-fast` required.
- **No cap bump; no general recognizer.**

## Seal
`impl: adr 368 scan to grouped fold streaming`. Do not commit wip drafts.
