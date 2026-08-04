# ADR 328 - Results: q19 correctness oracle

## Outcome

`q19` now answers against an independent strict oracle and passes compare at `sf=0.01`.

## Implementation Notes

- The oracle traverses `PartGenerator` and `LineItemGenerator` directly.
- Revenue is `SUM(l_extendedprice * (1 - l_discount))` over the qualifying `lineitem/part` rows.
- The strict output schema is `revenue` only.
- `revenue` is asserted as `Decimal128(32, 4)`, matching the executor shape.
- The comparator enforces schema, null rejection, and exact raw-value equality.
- The oracle records qualifying-row counts and branch hits so vacuity is measured on input rows, not output rows.

## Verification Snapshot

- `scripts/cargo-cgroup test -p toron-tpch --test correctness_test q19_`
- `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q19`
- `scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix`
- `scripts/cargo-cgroup run -p toron-tpch -- matrix`
- `scripts/cargo-cgroup check -p toron-tpch --all-targets`
- `scripts/cargo-cgroup test -p toron-tpch`
- `scripts/cargo-cgroup fmt`
- `git diff --check`

## Measured Rows

- `q19 strict=pass@compare`
- `correctness-matrix` row: `q19 strict=pass@compare`
- `matrix` unchanged: `q19 dev=pass@execute strict=pass@execute`

## Notes

The live strict oracle at `sf=0.01` is non-vacuous. The synthetic branch-coverage test also passed and
confirmed the OR-branch exclusions.
