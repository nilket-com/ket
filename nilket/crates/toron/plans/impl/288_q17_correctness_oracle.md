# ADR 288 — Implementation plan: q17 correctness oracle

Pairs with `plans/adr/288_q17_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Function names are current-tree anchors.

## Phase 0 — measure and pin the live schema (don't hard-code blind)
Run strict q17 at sf=0.02 and **capture the actual output schema** (a temporary probe / focused helper printing
`batches[0].schema()`):
```
scripts/cargo-cgroup run -p toron-tpch -- run --query q17 --mode strict --sf 0.02 --allow-blocked
```
Grounded expectation: `avg_yearly` = `Decimal128(21, 6)` (the Decimal **division output type**). The D2 assertion
must pin the *measured* type; record it in the results doc. (Also confirm sf=0.02 is non-empty: ≥1 eligible part,
≥1 contributing lineitem, non-NULL value — grounded ~2 parts / ~6 rows / `3008928571` raw scale-6.)

## D1 — `strict_q17_oracle` (`crates/toron-tpch/src/lib.rs`)
An independent generator traversal returning the expected scalar (raw `i128` at the measured scale) + the
non-vacuity counts. A `Q17Oracle { value: Option<i128>, eligible_parts: usize, contributing_rows: usize }`.
- **Eligible parts:** `PartGenerator` rows with `p_brand=="Brand#23"` && `p_container=="MED BOX"` → set of
  `p_partkey`.
- **Per-partkey AVG:** one pass over `LineItemGenerator` accumulating `(sum_qty_raw, count)` per `l_partkey` (only
  partkeys that are eligible, to bound memory). `avg_raw = avg_decimal_raw(sum_qty_raw, count, …)` (the existing
  q01 helper — half-away, scale 2 → AVG scale); `threshold = 0.2 * avg_raw` (raw, scale-aware).
- **Contributing rows:** second pass over `LineItemGenerator` — for each item whose `l_partkey` is eligible and
  `l_quantity_raw < threshold` (scale-aligned compare), add `l_extendedprice_raw` (scale 2) to `sum_extprice`;
  count `contributing_rows`.
- **Final division:** `value = Some(half_away_div(sum_extprice, scale 2 → scale 6, divisor 7.0))` — a focused
  half-away helper aligned with the AVG helper's rounding policy. If no contributing rows, `value = None` (so
  the non-vacuity guard, not a bogus 0, catches the empty case).

## D2 — `check_strict_q17` + `compare_strict_q17` + wiring
Mirror `check_strict_q5`'s correctness-runner shape:
1. Add `Q17_CORRECTNESS_SCALE: f64 = 0.02`; execute
   `execute_query_with_tables(STRICT_Q17_SQL, q17_tables(true, Q17_CORRECTNESS_SCALE))`; on `StageError` return
   the classified row. The emitted `CorrectnessAttempt.scale_factor` is `0.02`.
2. `assert_strict_q17_schema(&batches)` — `avg_yearly` = the Phase-0-measured `Decimal128(21,6)`; drift →
   `Compare`/`Fail`.
3. Non-vacuity: `oracle.eligible_parts >= 1 && oracle.contributing_rows >= 1 && oracle.value.is_some()` → else
   `Fail` with the counts.
4. `compare_strict_q17(&batches, &oracle)`: exactly one row; `is_null` checked on the `avg_yearly` slot; the raw
   `i128` value equals `oracle.value`.
- `main.rs`: route `correctness --query q17` → `check_strict_q17` (strict-only). Because `sf=0.01` is a known
  masked-empty correctness path, q17 gets a query-specific scale policy: no `--sf` → `0.02`; explicit
  `--sf 0.02` accepted; any other explicit q17 `--sf` returns `harness_error` / exit 2 with no telemetry row.
- `correctness_matrix`: q17 renders `strict=pass@compare` from the emitted correctness row.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q17_matches_independent_oracle` — `check_strict_q17` returns `pass@compare` (64MB-stack thread pattern;
  q17 is deep).
- `strict_q17_oracle_is_non_vacuous` — `eligible_parts ≥ 1 && contributing_rows ≥ 1 && value.is_some()` at sf=0.02
  (proves we're not on the masked-empty path).
- q17 correctness scale policy — default command records `sf=0.02`; explicit `--sf 0.02` is accepted; explicit
  `--sf 0.01` is a harness usage error and writes no correctness row.
- **Truncation-vs-half-away** — a unit feeding the rounding helper a remainder-≥-half input and asserting it
  rounds **away from zero**, and a sibling asserting a truncating reference would differ (the operator's ask: the
  first division oracle must *prove* the rounding policy).
- **Injected mismatch** — `compare_strict_q17` rejects an off-by-one `avg_yearly`, a NULL `avg_yearly`, a
  wrong-precision schema, and a >1-row / 0-row batch.
- Existing correctness rows (q01/q03/q05/q06) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q17
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q17 --sf 0.02
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
Plus empirical: `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q17 --sf 0.01` exits 2 without
writing a row.

## Results doc
`crates/toron/plans/impl/288_q17_correctness_oracle_results.md`: the Phase-0-measured `avg_yearly`
`Decimal128(21,6)`; sf=0.02 non-empty counts (eligible parts / contributing rows / value); the half-away helper
shared across AVG + `/7.0`; q17 `strict=pass@compare`; the hollow-green reach cell now validated; confirmation
prior rows unchanged.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (a lying green): the truncation +
  injected-mismatch tests, the non-vacuity guard, and the Phase-0 schema pin are the defenses.
- **The division arithmetic must match the executor's `decimal_division_output_type` + half-away exactly** — if
  the oracle's scale/rounding differs, `compare_strict_q17` fails against the real engine output (which is the
  intended cross-check, but get it right via the shared helper, not a re-derived constant).
- sf is pinned to 0.02 by q17's correctness scale policy — don't let a caller pass the rail's 0.01
  (masked-empty).

## Seal
`impl: adr 288 q17 correctness oracle`. Do not commit wip drafts.
