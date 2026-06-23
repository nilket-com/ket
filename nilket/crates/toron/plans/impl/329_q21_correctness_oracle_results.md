# ADR 329 results - q21 correctness oracle

q21 correctness is now wired into the canonical board and passes at the default correctness scale.

## Verified outcomes
- `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21`
  - `tpch_correctness query=q21 sf=0.01 mode=strict phase=compare status=pass reason=none`
- `scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix`
  - `q21 strict=pass@compare`
- `scripts/cargo-cgroup run -p toron-tpch -- matrix`
  - `q21 dev=pass@execute strict=pass@execute`

## Oracle coverage
- The independent oracle traverses `SupplierGenerator`, `NationGenerator`, `OrderGenerator`, and `LineItemGenerator`.
- The synthetic fixture proves:
  - the qualifying late supplier is counted,
  - a second late supplier excludes the row through the `NOT EXISTS` residual,
  - a single-supplier order excludes the row through the `EXISTS` arm,
  - on-time and wrong-nation/wrong-status rows are excluded.
- The comparator checks the `(s_name, numwait)` schema, null rejection, `ORDER BY numwait DESC, s_name`, and the row-counting semantics for `numwait`.

## Test coverage
- `q21_oracle_counts_rows_and_discriminates_antijoins`
- `q21_executes_in_both_modes`
- `cargo check -p toron-tpch --all-targets`
- `scripts/cargo-cgroup test -p toron-tpch`

## Result
The canonical correctness board now includes q21 as `pass@compare`, and the full TPC-H correctness matrix remains green.
