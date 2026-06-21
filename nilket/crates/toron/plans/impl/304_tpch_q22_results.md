# ADR 304 results - tpch.q22 reachability

## Outcome

q22 is now in the reachability gauge with a precise measured red row:

```text
q22 dev=blocked(unsupported_substring@plan) strict=blocked(unsupported_substring@plan)
```

Live runs at `sf=0.01`:

```text
tpch_reachability query=q22 sf=0.01 mode=dev phase=plan status=blocked reason=unsupported_substring elapsed_ms=1.2 digest=none
tpch_reachability query=q22 sf=0.01 mode=strict phase=plan status=blocked reason=unsupported_substring elapsed_ms=0.9 digest=none
```

The first wall is frontend lowering for SQL `SUBSTRING(c_phone FROM 1 FOR 2)`. A local phase probe reports the raw
shape as:

```text
unsupported expression: Substring { ... }
```

The classifier is deliberately narrow: it matches the `unsupported expression` + `substring` frontend-lowering
message and sits before the generic `unsupported_query_surface` bucket. An unrelated unsupported expression still
classifies as `unsupported_query_surface`.

## What landed

- `Q22_DEV_SQL` / `STRICT_Q22_SQL`, with the seven country-code literals inlined and no LIMIT.
- `q22_tables(strict, sf)` over only `customer` + `orders`; no new fixtures or columns.
- `run_dev_q22` / `run_strict_q22`, CLI `run --query q22`, and `status_rows` inventory wiring.
- `unsupported_substring` telemetry reason and allowlist registration.
- Harness tests pinning the q22 row at `Phase::Plan` and the classifier boundary.

No `crates/toron` engine code changed. SUBSTRING support remains ADR 305.

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
q13 dev=not_run strict=not_run
q14 dev=pass@execute strict=pass@execute
q15 dev=not_run strict=not_run
q16 dev=not_run strict=not_run
q17 dev=pass@execute strict=pass@execute
q18 dev=not_run strict=not_run
q19 dev=not_run strict=not_run
q20 dev=pass@execute strict=pass@execute
q21 dev=not_run strict=not_run
q22 dev=blocked(unsupported_substring@plan) strict=blocked(unsupported_substring@plan)
```

Correctness matrix unchanged: q01/q02/q03/q04/q05/q06/q07/q08/q09/q10/q12/q14/q17/q20 remain
`strict=pass@compare`; q11/q13/q15/q16/q18/q19/q21/q22 remain unchecked.

## Gate

```text
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q22 --mode dev --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q22 --mode strict --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
```

All passed.

## Forward

ADR 305: SUBSTRING expression support - frontend lowering, binding, and Arrow string-slice execution for
`SUBSTRING(x FROM start FOR len)`. Once it lands, q22 should re-measure the next wall behind SUBSTRING.
