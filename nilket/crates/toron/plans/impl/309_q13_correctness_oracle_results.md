# ADR 309 results - q13 correctness oracle

q13 is now answer-checked on the strict correctness rail:

```text
tpch_correctness query=q13 sf=0.01 mode=strict phase=compare status=pass reason=none elapsed_ms=5540.0
```

Reachability is unchanged: q13 was already `pass@execute` after ADR 308. The correctness board now levels with
reach at 16 / 16.

## Phase 0

Strict q13 output schema is:

```text
c_count   Int64
custdist  Int64
```

Default-scale oracle counters at `sf=0.01`:

```text
total_customers                1500
c_count groups                 33
qualifying_orders              14834
comment_excluded_orders        166
zero_count_customers           500
positive_count_customers       1000
no_order_customers             500
only_excluded_order_customers  0
```

The live data exercises the required q13 semantics: zero-count customers, positive-count customers, qualifying
orders, and comment-excluded orders are all present. The stricter residual-created zero-count subcase
(`only_excluded_order_customers`) is absent at this scale, so that boundary is pinned by the synthetic
`q13_outer_join_preservation_is_pinned` test rather than over-claimed as live evidence.

The oracle's grouped distribution:

```text
[(0, 500), (11, 68), (10, 64), (12, 62), (9, 62), (8, 61), (14, 54), (13, 52),
 (7, 49), (20, 48), (21, 47), (16, 46), (15, 45), (19, 44), (17, 41), (18, 38),
 (22, 33), (6, 33), (24, 30), (23, 27), (25, 21), (27, 17), (26, 15), (5, 14),
 (28, 6), (4, 6), (32, 5), (29, 5), (30, 2), (3, 2), (31, 1), (2, 1), (1, 1)]
```

## What landed

- `Q13Oracle` and `strict_q13_oracle`, computed directly from `CustomerGenerator` and `OrderGenerator`.
- An independent fixed-pattern matcher for `%special%requests%`; it is ordered and case-sensitive, and it accepts
  the zero-gap `specialrequests` case.
- `compare_strict_q13`, asserting schema, nullness, bijection by `c_count`, exact `custdist`, duplicate rejection,
  and `ORDER BY custdist DESC, c_count DESC`.
- `check_strict_q13` and CLI routing for `correctness --query q13`.

## Tests

`tests/correctness_test.rs` gained the q13 block:

- `strict_q13_matches_independent_oracle`
- `strict_q13_oracle_is_non_vacuous_at_default_scale`
- `strict_q13_output_schema_is_asserted`
- `q13_comment_pattern_matching_is_exact`
- `q13_outer_join_preservation_is_pinned`
- `q13_count_o_orderkey_counts_only_qualifying_orders`
- `q13_correctness_mismatch_is_fail_at_compare`
- `q13_missing_extra_or_duplicate_group_is_fail`
- `q13_null_cell_is_fail`
- `q13_order_violation_is_fail`

## Gate

```text
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q13
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

All green. The full `toron-tpch` suite includes 136 correctness tests after the q13 block landed.

## Forward

The board is level again. The next cycle can reopen the reach frontier: q11/q15/q16/q18/q19/q21 remain
unattempted.
