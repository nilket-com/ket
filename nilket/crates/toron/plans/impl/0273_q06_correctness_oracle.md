# Implementation Plan — ADR 273: strict q06 correctness oracle

## Summary
Add the first answer-checked TPC-H cell: strict q06 compares Toron's executor output against an independent
scaled-`i128` oracle computed directly from `LineItemGenerator`. A separate correctness rail (log + matrix +
CLI + `phase=compare`); reachability `run`/`matrix` behavior is unchanged. Scope is `crates/toron-tpch` only
unless the oracle catches a real engine mismatch.

## Phase 0 — verify the board
`scripts/cargo-cgroup run -p toron-tpch -- matrix` (q01/q03/q06/q14 `pass@execute`) and
`run --query q06 --mode strict --allow-blocked` (carries a digest, no answer check yet).

## Phase 1 — factor execution so the comparison sees batches
In `lib.rs`, split the private runner so reachability and correctness share one execution substrate (never two
near-identical paths):
```
async fn execute_query_with_tables(sql, tables) -> Result<Vec<RecordBatch>, stage-error>
```
preserving the current staging (register descriptors → seed facts → `HarnessScan` → `SqlPlanner::plan` →
`bind(.., PlannerConfig::default())` → `Executor::new(storage).execute_bound(..)`). `run_query_with_tables`
calls it and computes the digest. Keep elapsed timing at the outer attempt layer, not in the shared helper.

## Phase 2 — independent q06 oracle
```
fn strict_q6_expected_raw(scale_factor: f64) -> Option<i128>
```
Traverse `LineItemGenerator::new(scale_factor, 1, 1)` (**not** `generate_strict_lineitem` — that would compare
Toron to its own loaded batch). Apply the strict-q06 filters with the pinned raw thresholds:
- `5 <= l_discount.into_inner() <= 7` (scale-2 `TPCHDecimal`: `0.05..=0.07`).
- `(l_quantity as i128 * 100) < 2400` (`< 24`).
- `8766 <= l_shipdate.to_unix_epoch() < 9131` (`[1994-01-01, 1995-01-01)` as `Date32` day counts).
Sum `l_extendedprice.into_inner() as i128 * l_discount.into_inner() as i128` (scale-4 raw). Return `Option` so
an empty result can represent SQL `SUM(empty) = NULL`. Also expose the matched-row count (or a sibling fn) for
the non-vacuous guard.

## Phase 3 — extract + compare Toron's q06 result
```
pub async fn check_strict_q6(scale_factor: f64) -> CorrectnessAttempt
```
Execute `STRICT_Q6_SQL` over `generate_strict_lineitem(scale_factor)` via the Phase-1 helper; require exactly
one column and ≤ 1 row; downcast to `Decimal128Array`; assert the type is `Decimal128(31,4)`; compare the raw
`i128` value and nullness against `strict_q6_expected_raw`. `phase=Compare status=Pass` on equality,
`status=Fail` with a short reason on mismatch. The proof is raw Decimal equality — the digest may be recorded
as context but is not the comparison.

## Phase 4 — correctness telemetry + CLI (separate from reachability)
- A `CorrectnessAttempt` emitting a distinct `tpch_correctness` line and appending to
  `target/tpch/correctness.jsonl` — **never** `reachability.jsonl`. Reuse the existing `Phase::Compare` /
  `Status` enums, but do not reuse `telemetry::Attempt::human()` because it names `tpch_reachability`.
- CLI: `correctness --query q06 [--sf]` and `correctness-matrix` (the matrix renders 22 rows,
  `q06 strict=pass@compare`, others `unchecked`). `--query` initially accepts only `q06`/`q6`; an unsupported
  query is a usage error (exit 2, no row), never a fake telemetry row. Preserve ADR 261 `--sf` hygiene
  (malformed / non-positive → exit 2, no row). No dev correctness cell.

## Phase 5 — tests (`crates/toron-tpch/tests/`)
- `strict_q6_matches_independent_oracle` — `check_strict_q6(0.01)` → `phase=Compare`, `status=Pass`.
- `strict_q6_oracle_selects_nonempty_rows` — the oracle matches ≥ 1 row at the default correctness scale
  (guards the threshold bug from re-creeping as a vacuous pass).
- `strict_q6_oracle_uses_scaled_i128` — exercises the oracle's predicate / revenue helper on crafted values:
  raw scaled multiplication, not `f64`; discount bound `5..=7`.
- `q6_correctness_mismatch_is_fail_at_compare` — compare with `expected_raw + 1` → `Status::Fail`, not
  `Blocked`, and no reachability blocker.
- `correctness_matrix_is_separate_from_reachability` — a correctness JSONL row does not alter `matrix_from_log`
  and renders only in the correctness matrix.
- Existing q01/q03/q06/q14 reachability tests stay green.

## Verification gate (harness-only, via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt   -p toron-tpch -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test  -p toron-tpch
scripts/cargo-cgroup run   -p toron-tpch -- correctness --query q06 --sf 0.01
scripts/cargo-cgroup run   -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run   -p toron-tpch -- matrix
git diff --check
```
If the oracle catches a real engine mismatch and the fix touches `toron`, widen to the full ADR 264 seal gate.

## Seal
`impl: adr 273 q06 correctness oracle`, `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
Add `crates/toron/plans/impl/0273_q06_correctness_oracle_results.md` in the impl commit: the correctness row,
the oracle formula + raw scale, the actual raw value/type observed from Toron, and confirmation that
reachability rows stay q01/q03/q06/q14 `pass@execute`.

## Risk notes
- **Threshold fidelity.** Discount is `5..=7` (scale-2), not `500..=700`; pin it + the non-vacuous guard — this
  is the exact error the oracle exists to prevent.
- **False independence.** Iterate `LineItemGenerator`, never `generate_strict_lineitem` / the executor output.
- **Polluting reachability.** Correctness rows go only to `correctness.jsonl`; the reachability matrix is
  unchanged.
- **Scale drift.** Compare raw i128 + `Decimal128(31,4)` type, never formatted strings.
- **Overclaiming.** A local strict-query oracle, not an official TPC-H result.
