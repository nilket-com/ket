# ADR 303 Results — q09 correctness oracle

## Measured row

At `sf=0.01`:

```text
q09 strict = pass@compare
```

Live run:

```text
tpch_correctness query=q09 sf=0.01 mode=strict phase=compare status=pass reason=none elapsed_ms=3944.9
```

The correctness matrix now has q09 green:

```text
q09 strict=pass@compare
```

Reachability is unchanged:

```text
q09 dev=pass@execute strict=pass@execute
```

## Schema and Non-Vacuity

Strict q09's measured output schema is asserted before any value comparison:

```text
nation     Utf8
o_year     Int32
sum_profit Decimal128(33,4)
```

The independent oracle at `sf=0.01` is non-vacuous:

```text
groups=173
selected_rows=3223
negative_amount_rows=27
```

Live data exercises negative per-row profit, so the signed subtraction path is covered by both live data and the
synthetic sign test.

## Oracle

The oracle traverses tpchgen generators directly and computes:

```text
revenue_raw = l_extendedprice_raw * (100 - l_discount_raw)
cost_raw    = ps_supplycost_raw * (l_quantity * 100)
amount_raw  = revenue_raw - cost_raw
sum_profit[(nation, o_year)] += amount_raw
```

All values are raw signed scale-4 `i128`; there is no float conversion or tolerance. The `partsupp` lookup is keyed
by `(ps_partkey, ps_suppkey)`, the part filter is contains matching for `%green%`, and `o_year` uses a general
Date32-to-calendar-year helper matching `EXTRACT(year FROM o_orderdate)`.

## Coverage

- `strict_q09_matches_independent_oracle`
- `strict_q09_oracle_is_non_vacuous_at_default_scale`
- `strict_q09_output_schema_is_asserted`
- `q09_amount_is_revenue_minus_cost_signed`
- `q09_partsupp_composite_key_picks_right_cost`
- `q09_green_is_contains_match`
- `q09_order_year_extracts_general_date32_years`
- `q09_correctness_mismatch_is_fail_at_compare`
- `q09_missing_extra_or_duplicate_group_is_fail`
- `q09_null_cell_is_fail`
- `q09_order_violation_is_fail`

## Gate

Green after `fmt`:

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q09
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

The board is level again for measured cells: 14 reach cells and 14 correctness cells.
