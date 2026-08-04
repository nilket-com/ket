# ADR 312 results - q16 correctness oracle

## Outcome

q16 is now answer-checked on the strict correctness rail:

```text
tpch_correctness query=q16 sf=0.01 mode=strict phase=compare status=pass reason=none elapsed_ms=6160.7
```

Reachability is unchanged. The boards are level again:

```text
reach:       17 / 22
correctness: 17 / 22
```

`correctness-matrix` now renders:

```text
q16 strict=pass@compare
```

## Measured Shape

Strict q16's output schema is asserted before comparison:

```text
p_brand       Utf8
p_type        Utf8
p_size        Int32
supplier_cnt  Int64
```

The default `sf=0.01` oracle is non-vacuous:

```text
groups=296
kept_partsupp_rows=1196
dedup_groups=1
pruned_parts=1701
excluded_suppliers=0
```

The live data exercises `COUNT(DISTINCT)` deduplication and the brand/type/size filters. The supplier-comment
anti-join branch is not live at `sf=0.01`; this remains honest and is pinned by synthetic tests instead of being
marketed as live coverage.

## Implementation

Harness-only:

- `Q16Oracle` records per-group distinct supplier sets plus the non-vacuity metrics.
- `q16_oracle_from_rows` computes the oracle from independent `tpchgen` row traversal and also supports synthetic
  tests.
- `strict_q16_oracle` traverses `PartGenerator`, `PartSuppGenerator`, and `SupplierGenerator`.
- `compare_strict_q16` asserts schema, rejects NULL cells, compares exact `supplier_cnt`, rejects missing/extra
  or duplicate groups, and verifies `ORDER BY supplier_cnt DESC, p_brand, p_type, p_size`.
- `check_strict_q16` wires the strict correctness row.
- `main.rs` routes `correctness --query q16`.

No `crates/toron` engine code changed.

## Tests

`tests/correctness_test.rs` gained the q16 block:

- `strict_q16_matches_independent_oracle`
- `strict_q16_oracle_is_non_vacuous_at_default_scale`
- `strict_q16_output_schema_is_asserted`
- `q16_count_distinct_dedups_supplier_per_group`
- `q16_not_in_excludes_complaint_supplier`
- `q16_customer_complaints_matcher_is_positional`
- `q16_filter_predicates_are_exact`
- `q16_correctness_mismatch_is_fail_at_compare`
- `q16_missing_extra_or_duplicate_group_is_fail`
- `q16_null_cell_is_fail`
- `q16_four_key_order_violation_is_fail`

The q16 end-to-end correctness test uses an explicit 64MB test thread stack, matching the existing treatment for
deep multi-join correctness checks.

## Gate

Focused gates run during implementation:

```text
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch --test correctness_test q16 -- --nocapture
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q16 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
```

Full seal gate recorded in the impl commit.

## Forward Arc

The correctness rail is level with reach again. q11/q15 remain likely-green reach coverage; q19/q21 remain the
heavier reach frontier.
