# ADR 307 results - tpch.q13 reachability

## Outcome

q13 is now in the reachability gauge with a precise measured red row:

```text
q13 dev=blocked(unsupported_non_equi_join_on@plan) strict=blocked(unsupported_non_equi_join_on@plan)
```

Live runs at `sf=0.01`:

```text
tpch_reachability query=q13 sf=0.01 mode=dev phase=plan status=blocked reason=unsupported_non_equi_join_on elapsed_ms=0.9 digest=none
tpch_reachability query=q13 sf=0.01 mode=strict phase=plan status=blocked reason=unsupported_non_equi_join_on elapsed_ms=0.7 digest=none
```

The first wall is frontend lowering of q13's `LEFT OUTER JOIN ... ON` clause:

```text
join ON only supports equality predicates
```

That is before binding, so the `orders` fixture deliberately did not grow `o_comment` in this cut. The q13
row measures the ON-residual gap, not a missing-column fixture gap.

## What landed

- `Q13_DEV_SQL` / `STRICT_Q13_SQL`, using q13's official customer-distribution shape without `LIMIT`.
- `q13_tables(strict, sf)` over the existing `customer` and `orders` fixtures.
- `run_dev_q13` / `run_strict_q13`, CLI `run --query q13`, and `status_rows` inventory wiring.
- `unsupported_non_equi_join_on` telemetry reason and allowlist registration.
- Harness tests pinning q13 at `Phase::Plan` and the classifier boundary.

No `crates/toron` engine code changed. Non-equality ON predicates remain the engine cut named by this row.

## Board

Reachability after the live run:

```text
q01 dev=pass@execute strict=pass@execute
q02 dev=pass@execute strict=pass@execute
q03 dev=pass@execute strict=pass@execute
q04 dev=pass@execute strict=pass@execute
q05 dev=pass@execute strict=pass@execute
q06 dev=pass@execute strict=pass@execute
q07 dev=pass@execute strict=pass@execute
q08 dev=pass@execute strict=pass@execute
q09 dev=pass@execute strict=pass@execute
q10 dev=pass@execute strict=pass@execute
q11 dev=not_run strict=not_run
q12 dev=pass@execute strict=pass@execute
q13 dev=blocked(unsupported_non_equi_join_on@plan) strict=blocked(unsupported_non_equi_join_on@plan)
q14 dev=pass@execute strict=pass@execute
q15 dev=not_run strict=not_run
q16 dev=not_run strict=not_run
q17 dev=pass@execute strict=pass@execute
q18 dev=not_run strict=not_run
q19 dev=not_run strict=not_run
q20 dev=pass@execute strict=pass@execute
q21 dev=not_run strict=not_run
q22 dev=pass@execute strict=pass@execute
```

Correctness matrix unchanged: q01/q02/q03/q04/q05/q06/q07/q08/q09/q10/q12/q14/q17/q20/q22 remain
`strict=pass@compare`; q11/q13/q15/q16/q18/q19/q21 remain unchecked.

## Gate

```text
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q13 --mode dev --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q13 --mode strict --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

Observed after implementation:

```text
check -p toron-tpch --all-targets       clean
test -p toron-tpch                      31 harness tests passed; full package suite passed
q13 dev                                 blocked(unsupported_non_equi_join_on@plan)
q13 strict                              blocked(unsupported_non_equi_join_on@plan)
matrix                                  q13 dev+strict blocked at plan
correctness-matrix                      unchanged
```

## Forward

The next engine cut is non-equality `ON` residual support, especially for outer joins where the residual cannot
be moved blindly into `WHERE` without changing null-extension semantics. q13's second surface, `COUNT` over a
derived aggregate, remains behind that wall.
