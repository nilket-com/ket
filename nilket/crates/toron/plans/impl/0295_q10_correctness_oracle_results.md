# ADR 295 results — q10 correctness oracle

## Measured result
`q10 strict=pass@compare` at `sf=0.01`.

## Phase-0 facts
- Output schema:
  - `c_custkey Int64`
  - `c_name Utf8`
  - `revenue Decimal128(32,4)`
  - `c_acctbal Decimal128(15,2)`
  - `n_name Utf8`
  - `c_address Utf8`
  - `c_phone Utf8`
  - `c_comment Utf8`
- Non-vacuity at `sf=0.01`: `selected_rows=1259`, `rows=399`.

## Scope
Harness-only. The implementation adds the independent strict-q10 oracle, schema assertion, exact comparator, CLI
route, tests, and correctness-matrix row. Reachability is unchanged.

## Verification
- `scripts/cargo-cgroup fmt`
- `scripts/cargo-cgroup fmt -- --check`
- `scripts/cargo-cgroup check -p toron-tpch --all-targets`
- `scripts/cargo-cgroup test -p toron-tpch`
- `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q10`
- `scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix`
- `scripts/cargo-cgroup run -p toron-tpch -- matrix`
- `git diff --check`
