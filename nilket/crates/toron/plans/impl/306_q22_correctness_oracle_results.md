# ADR 306 results - q22 correctness oracle

q22 is now answer-checked on the strict correctness rail:

```text
tpch_correctness query=q22 sf=0.01 mode=strict phase=compare status=pass reason=none elapsed_ms=3377.0
```

The reachability matrix is unchanged; q22 was already `pass@execute` after ADR 305. The correctness board now has
all currently green reach cells checked: q01/q02/q03/q04/q05/q06/q07/q08/q09/q10/q12/q14/q17/q20/q22.

## What landed

- `strict_q22_oracle`: an independent two-pass traversal over `CustomerGenerator` and `OrderGenerator`.
  - Pass 1 computes the global account-balance threshold over selected country codes and positive balances with
    `avg_decimal_raw(sum, count, 2, 6, 19)`.
  - Pass 2 applies the country-code filter, the scale-aligned `c_acctbal > AVG(c_acctbal)` comparison, and the
    `NOT EXISTS` anti-join, then groups by `cntrycode`.
- `assert_strict_q22_schema`: measured strict q22 output as
  `cntrycode Utf8`, `numcust Int64`, `totacctbal Decimal128(15,2)`.
- `compare_strict_q22`: exact row comparison by `cntrycode`, raw `i128` `totacctbal`, exact `numcust`, NULL
  rejection, duplicate rejection, and `ORDER BY cntrycode` validation.
- CLI route: `correctness --query q22`.

## Phase 0 measurements

Default `sf=0.01` is non-vacuous, so no q17-style scale override was needed:

```text
avg_threshold_raw = 5017600775   # Decimal128(19,6)
selected_rows = 73
with_orders_excluded = 117
groups = {
  "13": numcust 10, totacctbal_raw 7535929,
  "17": numcust 8,  totacctbal_raw 6228898,
  "18": numcust 14, totacctbal_raw 11107245,
  "23": numcust 5,  totacctbal_raw 4045886,
  "29": numcust 11, totacctbal_raw 8872285,
  "30": numcust 17, totacctbal_raw 12218933,
  "31": numcust 8,  totacctbal_raw 6631316,
}
```

The live data exercises both sides of the anti-join: 73 customers survive, and 117 otherwise-qualifying customers
are excluded because an order exists.

## Tests

`tests/correctness_test.rs` gained the q22 block:

- `strict_q22_matches_independent_oracle`
- `strict_q22_oracle_is_non_vacuous_at_default_scale`
- `strict_q22_output_schema_is_asserted`
- `q22_not_exists_anti_join_excludes_customers_with_orders`
- `q22_avg_threshold_boundary_is_strict`
- `q22_substring_prefix_and_in7_filter`
- `q22_correctness_mismatch_is_fail_at_compare`
- `q22_missing_extra_or_duplicate_cntrycode_is_fail`
- `q22_null_cell_is_fail_at_compare`
- `q22_order_violation_is_fail`

The synthetic tests pin the exact semantics that could otherwise hide behind a live green: anti-join inversion,
strict threshold comparison, character-position prefix extraction, exact raw totals, NULL rejection, duplicate
rejection, and ordering.

## Gate

```text
scripts/cargo-cgroup fmt
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q22
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q22 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
```

Observed:

```text
check -p toron-tpch --all-targets       clean
test -p toron-tpch                      189 passed, 0 failed
correctness --query q22                 pass@compare
correctness --query q22 --sf 0.01       pass@compare
correctness-matrix                      q22 strict=pass@compare
```

`git diff --check` is clean.

## Forward

The current green reach set is level with the correctness rail. The next cycle can reopen the reach frontier:
q11/q13/q15/q16/q18/q19/q21 remain unattempted, with q13's non-equi outer-join `ON` predicate and q19's
disjunctive predicate still the largest known cuts.
