# ADR 273 — Results: strict q06 correctness oracle (first answer-checked cell)

## Measured row
```
Reachability (unchanged):  q01/q03/q06/q14  dev=pass@execute  strict=pass@execute
Correctness (new):         q06 strict=pass@compare            (q01/q03/q14 unchecked)
```

The gauge gains a second axis. Toron's strict q06 executor output — `Decimal128(31,4)`, raw `i128` — **equals**
an independent oracle computed by traversing `tpchgen::LineItemGenerator` directly. Verified live:
```
tpch_correctness query=q06 sf=0.01 mode=strict phase=compare status=pass reason=none
```
This is the first proof a *result cell is right*, not just that a shape reaches the executor.

## The oracle (independent, exact)
`Σ (l_extendedprice.into_inner() × l_discount.into_inner())` as scale-4 `i128`, over rows matching the strict
q06 filters with the raw thresholds pinned against `tpchgen`'s **scale-2** `TPCHDecimal`:
- discount `into_inner()` in `5..=7` (`0.05..=0.07`; **not** the 100×-off `500..=700`),
- `(l_quantity as i128 * 100) < 2400` (`< 24`),
- `8766 <= l_shipdate.to_unix_epoch() < 9131` (`[1994-01-01, 1995-01-01)` as `Date32` day counts).

q06 is exactly comparable because the engine's Decimal `*` (ADR 263) yields the raw `i128` product at
`Decimal128(31,4)` and `SUM` carries that scale-4 accumulator — **no division, no rounding tolerance**. The
comparison is on the raw `i128` value + the `Decimal128(31,4)` type + nullness (never a formatted string), so a
future Decimal-widening change cannot silently pass. The oracle traverses the generator, **not** Toron's loaded
`RecordBatch` or executor output, so the check is genuinely independent.

## What was built (harness only, `crates/toron-tpch`)
- **Shared substrate** (`execute_query_with_tables`): the Load→Plan→Bind→Execute path, returning result
  batches or a deepest-phase stage error. Reachability (`run_query_with_tables`) adds the digest; correctness
  (`check_strict_q6`) adds the oracle compare — neither forks the execution path.
- **Oracle**: `strict_q6_oracle` (sum + matched-row count) over the per-row `q6_row_revenue` helper (the
  scaled-`i128` arithmetic + raw bounds, testable on crafted values).
- **Comparator**: `compare_strict_q6` (one column, ≤ 1 row, exact `Decimal128(31,4)`, raw equality).
- **Telemetry rail**: a distinct `CorrectnessAttempt` (a `tpch_correctness` human line, no digest — reusing
  `Attempt::human()` would mislabel it `tpch_reachability`); rows append to `target/tpch/correctness.jsonl`,
  rendered by `correctness_matrix`. `reachability.jsonl` is untouched.
- **CLI**: `correctness --query q06 [--sf]` and `correctness-matrix`. Only `q06`/`q6` is supported; any other
  query is a usage error (exit 2, no row, per ADR 261 CLI hygiene). A mismatch is `fail@compare` (no "wrong
  answer" recognized blocker); the oracle must match ≥ 1 row (no vacuous `0 == 0` pass).

## Tests (`tests/correctness_test.rs`)
- `strict_q6_matches_independent_oracle` — `check_strict_q6(0.01)` → `phase=compare status=pass`.
- `strict_q6_oracle_selects_nonempty_rows` — the oracle matches > 0 rows at sf=0.01 (non-vacuous guard).
- `q6_row_revenue_uses_scaled_i128_bounds` — discount bound `5..=7` (and `500` is rejected), quantity/shipdate
  bounds, scaled `i128` product — on crafted values.
- `compare_strict_q6_detects_raw_mismatch` — exact match `Ok`; off-by-one raw and non-null-vs-NULL `Err`.
- `correctness_matrix_is_separate_from_reachability` — a `fail` row renders `fail@compare`; a
  reachability-suite row is ignored by the correctness matrix.
- q01/q03/q06/q14 reachability tests stay green (the `execute_query_with_tables` refactor is transparent).

## Gate
fmt clean · `check -p toron-tpch --all-targets` clean · `test -p toron-tpch` green (correctness 5, harness 13,
architecture 10, probes) · live `correctness --query q06` → `pass@compare`, `correctness-matrix` renders q06
checked, `matrix` unchanged · `git diff --check` clean. Harness-only — no `toron` engine change (the oracle
found no mismatch).
