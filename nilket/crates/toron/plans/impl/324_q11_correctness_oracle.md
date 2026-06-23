# ADR 324 — Implementation plan: q11 correctness oracle

Pairs with `plans/adr/324_q11_correctness_oracle.md`. Oracle-only cut in `toron-tpch` (+ a precise compare-failure
reason). No engine change — the fix is follow-on ADR 325. Canonical correctness row is **strict-only**.

## Phase 0 — confirm the mismatch + the reason (verify-first)
- Run a draft q11 oracle (correct threshold) vs q11 **strict** at sf=0.01. Confirm a mismatch: the engine returns
  more parts than the oracle (threshold collapsed to `value > 0`). Confirm at **default scale** (sf=0.01), not a
  tiny scale that might let all parts clear the real threshold.
- Confirm dev (control) matches the oracle (Float64 `0.0001` correct) — isolates the bug to the strict literal
  path. Dev is a control only; not a canonical row.
- Confirm the root cause is the literal scale (`decimal_literal_from_f64` rounding `0.0001 → 0.00`), so the reason
  `decimal_literal_scale` is accurate.

## D1 — the oracle (`crates/toron-tpch/src/lib.rs`)
- Independent traversal (pattern: `q04_oracle_from_rows`, `avg_decimal_raw`): `partsupp ⋈ supplier ⋈ nation`,
  `n_name = 'GERMANY'`; per `ps_partkey` `value = SUM(ps_supplycost * ps_availqty)` mirroring the engine's
  `Decimal128(26,2)` product + sum precision; global `threshold = SUM(...) * 0.0001` (the real 0.0001); emit
  `value > threshold` ordered `value DESC`.
- Assert schema first; check `is_null` on every Arrow slot; finalize Decimals via the precision-fit helpers so the
  only intended divergence is the literal.

## D2 — expected-red assertion + compare reason
- Return the precise compare reason **`decimal_literal_scale`** from the q11 correctness comparator. Do **not** add
  it to `RECOGNIZED_BLOCKERS` or `reason_for_error`: this is a `fail@compare`, not a blocked reachability row.
- The q11 correctness test **asserts the expected outcome**: strict = `fail@compare` with reason
  `decimal_literal_scale`. The test **passes by confirming the expected red** — no `#[ignore]`, no skipping. The
  correctness-matrix shows q11 strict = `fail(decimal_literal_scale@compare)`.
- Keep the canonical row **strict-only**; if dev is checked, it is an internal control assertion, not a matrix
  cell.

## D3 — non-vacuity (mandatory)
- Assert the oracle's threshold is **selective at sf=0.01**: the result is a **proper subset** of the German parts
  (`0 < oracle_rows < total_german_parts`). Without this the strict red is meaningless (an empty/all result would
  agree with the buggy engine for the wrong reason — the earlier empty-result trap).

## Tests
- **q11 oracle** — the traversal above; selectivity assertion (proper subset at sf=0.01); schema + nullness.
- **Expected-red** — q11 strict mismatches the oracle with reason `decimal_literal_scale`, asserted (suite green).
- **Dev control** (optional) — dev matches the oracle (isolates the bug), not a matrix cell.

## Phase 3 — re-run the expected-red command and matrix
```
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q11
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
```
- The q11 correctness command writes the row and exits non-zero because correctness failures are process failures;
  the implementation gate should assert that exit code is expected rather than treating it as a seal failure.
- q11 strict = `fail(decimal_literal_scale@compare)` (expected, asserted); other correctness cells unchanged; reach
  unchanged (22/22).

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
set +e
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q11
q11_status=$?
set -e
test "$q11_status" -eq 1
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/324_q11_correctness_oracle_results.md`: the oracle; the confirmed strict mismatch + the
`decimal_literal_scale` reason; the selectivity numbers at sf=0.01 (proper subset); confirmation the suite stays
green (expected red asserted), the rail stays strict-only, and reach + other correctness cells are unchanged;
ADR 325 named as the fix.

## Risk / blast radius
- **Don't smuggle the fix** — oracle-only; ADR 325 is the literal fix.
- **Vacuity** — the selectivity assertion is mandatory.
- **Mirror legitimate precision** — match the engine's product/sum Decimal precision; the sole mismatch is the
  literal.
- **Strict-only** — keep the canonical row strict; dev is a control.

## Seal
`impl: adr 324 q11 correctness oracle`. Do not commit wip drafts.
