# ADR 367 - Implementation plan: scan-output streaming into non-join consumers (q06 canary)

Pairs with `plans/adr/367_scan_output_streaming_consumers.md`. A `crates/toron` engine cut: classify the 12 scan-output
rows by consumer shape (the coverage contract), stream q06's scan into the global-aggregate fold first, extend only as
measured, name the rest. ScanRequest parity (361); persisted correctness (365) on pass rows; no cap bump; engine-cut
seal gate.

## Phase 0 - the coverage contract (before implementation)
- For each of the 12 (`q01 q04 q06 q07 q08 q09 q13 q15 q17 q18 q20 q22`), read the bound plan and classify the
  scan->consumer edge into **B1 GlobalAggregate / B2 GroupedAggregate / B3 Exchange/Join (wall may move) / B4
  row-identity-or-runtime-filter-or-order beyond q06**. Record the table in the results doc - this is the coverage
  contract.
- Confirm q06 is B1 and why its scan doesn't already stream (fence too narrow / fold consumes a materialized scan).
- Identify which buckets q06's mechanism (scan `ScanStream` -> fold, via `bound_scan_request`) is expected to cover
  (likely B1, possibly B2); mark B3/B4 as out-of-scope-named.

## D1 - q06 first, then measured extension
- Route q06's scan as a `ScanStream` into the global-aggregate fold (extend the collapsing-global path / the scan-edge
  recognizer), bypassing `materialize_scan_stream -> Vec`. Reuse `bound_scan_request` (no new inline request).
- Extend to the buckets Phase 0 marks covered (B1, maybe B2); shape-fenced, no general recognizer.
- B3 (exchange/join) and B4 (identity/runtime-filter/order) and any uncovered cell: **do not implement here**; record as
  the named next subset.
- Topology-named; no qNN in core.

## Tests / safety net
- **q06 correctness:** strict `pass@compare` sf=0.01/sf=1; **persisted sf=10** `pass@compare` (365); dev parity
  (streamed-scan-into-fold == materialized) sf=0.01 both modes.
- **Each newly-greened cell:** persisted sf=10 `pass@compare` (not reach alone).
- **Scan-into-fold correctness:** a synthetic scan -> filter -> global aggregate streamed == materialized (digest).
- **ScanRequest parity:** the new edge uses `bound_scan_request`; the drift test passes.
- **Shape fence:** non-covered shapes (B3/B4) still materialize (no over-reach / no wall-move-claimed-as-green).
- **No cap bump:** the 1 GB accumulator budget unchanged (assert).
- **De-branding guard:** no qNN in core executor.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # MANDATORY (touches scan execution, 361 lesson)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q06 --sf 0.01   # strict
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q06 --sf 1      # strict
# persisted sf=10 correctness + the live family re-measure (fixed 4 GB), per query:
#   systemd-run --user --property=MemoryMax=4G --setenv=TORON_TPCH_DUCKDB_BIN=<duckdb> ... \
#     target/release/toron-tpch correctness --query q06 --sf 10 --data-source parquet --data-dir target/tpch/data
#   ... perf --sf 10 --query <each of the 12> --engines toron --data-source parquet --data-dir target/tpch/data ...
git diff --check
#   coverage contract recorded; q06 green + persisted correctness; measured family coverage; named next subset;
#   ScanRequest parity; --no-fail-fast green; no cap bump; no qNN in core
```

## Results doc
`crates/toron/plans/impl/367_scan_output_streaming_consumers_results.md`: the **coverage-contract table** (12 rows ->
B1/B2/B3/B4); the q06 fix (scan->fold streaming) + its persisted sf=10 correctness; the **live family re-measure** (per
query: green / advanced / unchanged after the cut) - the measured claim, not the contract max; the named next subset
(B3/B4/uncovered); strict + dev parity; confirmation no cap bump, `--no-fail-fast` green, no qNN in core.

## Risk / blast radius
- **Wall-moving (B3)** - the live re-measure classifies advanced-not-green; not claimed as covered.
- **Overclaim** - the coverage contract + measured re-run bound the claim.
- **Silent answer change** - shared builder + persisted correctness + dev parity.
- **Regression** - `--no-fail-fast` mandatory.
- **No cap bump; no general recognizer; no B3/B4 in scope** (named follow-ups).

## Seal
`impl: adr 367 scan output streaming consumers`. Do not commit wip drafts.
