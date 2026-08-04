# ADR 327 results - q18 correctness oracle

## Outcome

`q18` strict now answers against an independent oracle and passes compare at `sf=0.01`.

## Implementation Notes

- The oracle traverses `CustomerGenerator`, `OrderGenerator`, and `LineItemGenerator` directly.
- The grouped-membership set is built from `SUM(l_quantity) > 300` over `l_orderkey`.
- The strict output schema is `c_name`, `c_custkey`, `o_orderkey`, `o_orderdate`, `o_totalprice`, `sum`.
- `o_totalprice` and `sum` are asserted as `Decimal128(15, 2)`, matching the executor shape.
- The comparator enforces schema, null rejection, grouped-membership comparison by stable row identity, and
  `ORDER BY o_totalprice DESC, o_orderdate` without brittle tie-order assumptions.
- The default-scale oracle is non-vacuous, so no synthetic fallback was needed.
- Comparator tests cover schema drift, wrong values, null rejection, missing/extra/duplicate rows, and order/tie
  behavior.

## Verification Snapshot

- `scripts/cargo-cgroup test -p toron-tpch --test correctness_test q18`
- `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q18`
- `scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix`
- `scripts/cargo-cgroup check -p toron-tpch --all-targets`
- `scripts/cargo-cgroup test -p toron-tpch`
- `scripts/cargo-cgroup fmt`
- `scripts/cargo-cgroup fmt -- --check`
- `scripts/cargo-cgroup run -p toron-tpch -- matrix`
- `git diff --check`

## Notes

The canonical correctness board now shows `q18 strict=pass@compare`, and the reachability matrix remains
unchanged.
