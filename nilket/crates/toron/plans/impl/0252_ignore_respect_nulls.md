# Implementation Plan — ADR 252: IGNORE / RESPECT NULLS for Value Window Functions

## Summary

Carry `ignore_nulls: bool` on `WindowFunctionCall` + `BoundWindowCall` (default `false` = RESPECT).
Lowering maps the parsed `null_treatment` (SQL) / a `.ignore_nulls()` method (Rune) onto value calls;
ranking/aggregate/NTILE keep rejecting the modifier. `execute_window_value` skips NULLs in its target
navigation when `ignore_nulls`. No new node, no fragmenter change.

## Verify-first probe

1. Confirm `SELECT LAG(score, 1) IGNORE NULLS OVER (ORDER BY id) FROM wins` rejects today
   (`extract_value_function` `null_treatment` reject, `sql_planner.rs:4098`).
2. Confirm `execute_window_value` (`exec.rs:4481`) builds `targets: Vec<Option<usize>>` and reads the
   value via `sorted_value` — the single place null-skipping plugs in.
3. Confirm `BoundWindowCall` is in the execution plan (postcard preimage) so the field
   append needs a golden re-pin (grep the golden/wire tests for `BoundWindowCall` / window plan
   vectors).

## Phase 1 — IR

Add `pub ignore_nulls: bool` to `WindowFunctionCall` (`ir.rs:959`) and `BoundWindowCall`
(`ir/execution.rs:158`). Adding a field trips every constructor — `--all-targets` lists them; default
`false` at each non-value construction site (ranking/aggregate set `false`).

## Phase 2 — Lowering (SQL)

In `extract_value_function`: replace the `null_treatment.is_some()` reject with a map —
`Some(NullTreatment::IgnoreNulls) → true`, `Some(NullTreatment::RespectNulls) | None → false` — and
thread it into the constructed `WindowFunctionCall.ignore_nulls`. Keep the generic
`extract_window_call` `null_treatment` reject (`:3977`) for ranking/aggregate (non-value calls reject
both IGNORE and RESPECT). `FILTER` stays rejected.

## Phase 3 — Binder

Carry `call.ignore_nulls` through to the `BoundWindowCall` the Window arm builds (`binder.rs` Window
arm). No schema/type change — the output field is unchanged (the value type, nullable).

## Phase 4 — Executor (load-bearing)

Give `execute_window_value` an `ignore_nulls: bool` parameter (the exec dispatch passes
`call.ignore_nulls`). When `ignore_nulls`, skip `sorted_value.is_null(i)` rows while computing each
`targets[row]`:

- **LAG / LEAD**: walk from `row` toward the partition edge (back for LAG, forward for LEAD), counting
  only non-NULL rows; the `offset`-th non-NULL row is the target. Out of range → `None` (LEAD / no
  default) or the `Lag.default`. The existing partition clamp (`partition_start`/`partition_end`)
  bounds the walk.
- **FIRST_VALUE / NTH_VALUE**: scan **forward** from the slice start (`lo`) for the first / `n`-th
  non-NULL row. **LAST_VALUE**: scan **backward** from the slice end (`hi`) for the first non-NULL.
  None qualifies → `None`.

`RESPECT` (`ignore_nulls == false`) keeps the existing physical-row paths byte-for-byte (regression:
the 246 value tests stay green). Factor a small `nth_non_null_in(range, sorted_value, n, from_end)`
helper so the four frame cases share the scan.

## Phase 5 — Rune

Add `WindowFunctionCall::ignore_nulls(self) -> VmResult<WindowFunctionCall>` — sets the flag, errors
on a non-value call ("ignore_nulls applies only to lag/lead/first_value/last_value/nth_value"). Register
`m.associated_function("ignore_nulls", WindowFunctionCall::ignore_nulls)`.

## Phase 6 — Tests, guards, goldens, verification, seal

### Behavioral gates (`tests/window_test.rs`)

Use a value column with interleaved NULLs (inject via `CASE WHEN score = 10 THEN NULL ELSE score END`
over `wins`, or a small NULL-bearing fixture):
- `lag_ignore_nulls_skips_null_rows`
- `last_value_ignore_nulls_skips_trailing_nulls` (differs from RESPECT, which is NULL)
- `respect_nulls_is_default_unchanged` (plan + result identical to the no-modifier 246 path)
- `ignore_nulls_rejected_on_non_value_functions` (RANK / SUM / NTILE IGNORE NULLS reject) — add to
  `deferred_window_forms_stay_rejected` or a dedicated gate.

### Rune gate (`tests/rune_window_test.rs`)

- `rune_value_ignore_nulls_matches_sql` — `lag(col(v), 1).ignore_nulls()` parity with SQL.

### Distributed gate (`tests/distributed_process_test.rs`)

- `ignore_nulls_distributed_gate_is_value_discriminating` — over `set_left` (set_key has NULLs at ids
  2,4): a value function whose IGNORE-NULLS skip requires co-locating a scan-shared NULL row;
  `assert_cross_producer_cardinal_is_value_discriminating`. Fresh seed.

### Architecture guards (`tests/architecture/window_functions.rs`)

`adr252_window_call_carries_ignore_nulls`, `adr252_value_executor_skips_nulls_under_ignore`,
`adr252_ignore_nulls_scoped_to_value_functions`, `adr252_rune_value_ignore_nulls_method`,
`adr252_named_window_spec_merge_stays_deferred` (the 3496/3926 rejects are untouched — locks the 253
boundary). Reconcile the 246 `deferred_window_forms_stay_rejected` IGNORE-NULLS case (now lands on value
functions; keep the non-value rejects).

### Goldens

A `BoundWindowCall` field append shifts its postcard preimage *if* a pinned vector contains a Window
node — but 242/246 touched this type without a re-pin (likely window plans aren't pinned). `--lib` is
the signal: if it flags stale window-plan vectors, re-pin them in this cut; otherwise none needed.

### Verification gate (route every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets   # field-append constructor sweep
scripts/cargo-cgroup test  -p toron --test window_test
scripts/cargo-cgroup test  -p toron --test rune_window_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test ignore_nulls
scripts/cargo-cgroup test  -p toron --test architecture_test adr252
scripts/cargo-cgroup test  -p toron --test architecture_test        # FULL
scripts/cargo-cgroup test  -p toron --test distributed_process_test # FULL
scripts/cargo-cgroup test  -p toron --lib                           # GOLDEN RE-PIN: BoundWindowCall field
cargo fmt -p toron -- --check
git diff --check
```

### Seal

Commit the implementation as `impl: adr 252 ignore respect nulls` with the
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.

## Risk notes

- **Golden re-pin (conditional)**: a `BoundWindowCall` field append shifts the postcard preimage
  (positional) only if a window plan is pinned. 242/246 didn't need a re-pin; `--lib` is the signal —
  re-pin in this cut if it flags stale vectors (`#[serde(default)]` does not stabilize postcard).
- **RESPECT must be a true no-op**: the `ignore_nulls == false` paths must be byte-identical to 246 —
  the 246 value tests are the regression net; do not refactor the physical-row navigation.
- **Field-append sweep**: many `WindowFunctionCall` / `BoundWindowCall` construction sites (binder,
  exec, tests, benches); `--all-targets` catches each missing `ignore_nulls`.
- **Partition-clamped skip walk**: the LAG/LEAD non-NULL walk must stop at the partition edge, not run
  into the neighbouring partition — reuse `partition_start`/`partition_end`.
