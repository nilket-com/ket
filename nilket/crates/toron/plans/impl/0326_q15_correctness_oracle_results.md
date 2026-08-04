# ADR 326 results - q15 correctness oracle

## Outcome

`q15` strict now answers against an independent oracle and passes compare at `sf=0.01`.

## Implementation Notes

- The oracle traverses `SupplierGenerator` and `LineItemGenerator` directly.
- Revenue is grouped by `l_suppkey`, filtered to the `1996-01-01` through `1996-04-01` shipdate window.
- The strict output schema is `s_suppkey`, `s_name`, `s_address`, `s_phone`, `total_revenue`.
- `total_revenue` is asserted as `Decimal128(32, 4)`, matching the executor shape.
- The comparator enforces schema, null rejection, `ORDER BY s_suppkey`, exact row equality, and the non-vacuous guard.
- Synthetic fixtures cover the unique-max case, a real tie case with all max rows emitted, null rejection, and duplicate/order violations.

## Verification Snapshot

- `cargo test -p toron-tpch --test correctness_test q15_`
- `scripts/cargo-cgroup check -p toron-tpch --all-targets`
- `scripts/cargo-cgroup test -p toron-tpch --lib`
- `scripts/cargo-cgroup test -p toron-tpch --test correctness_test`
- `scripts/cargo-cgroup fmt -- --check`
- `git diff --check`

## Notes

The default-scale oracle is non-vacuous, and the comparator tests distinguish unique-max, tie, null, duplicate, and order-violation cases.
