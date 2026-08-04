# Implementation Plan — ADR 249: NTILE Window Bucketing

## Summary

Add `WindowFunction::Ntile { buckets: u64 }`: lowering parses `NTILE(n)` (positive integer
literal), binder emits a non-null Int64 column, executor computes the per-row bucket from
partition geometry. Frame-independent like ranking; no ORDER BY requirement (matches ROW_NUMBER).
No fragmenter change — single spec rides 241, multiple specs ride 248.

## Verify-first probe

1. Confirm `NTILE(2) OVER (ORDER BY score)` rejects today (the generic unsupported-function
   error; pinned in `deferred_window_forms_stay_rejected`).
2. Confirm `execute_window` exposes `partition_start[]` / `partition_end[]` in the per-call arm
   scope (`exec.rs:3734–3748`, used by the ranking arms at 3804) — the geometry NTILE needs.
3. Confirm ranking lowering does **not** require ORDER BY (no `order_by.is_empty()` reject for
   RowNumber/Rank/DenseRank), and that `execute_window` totalizes the partition sort with every
   column as a tiebreaker after the partition/order keys (`exec.rs:3687`) — together these make
   NTILE without an ORDER BY both permitted and deterministic, identical to ROW_NUMBER.

## Phase 1 — IR

Add `Ntile { buckets: u64 }` to `pub enum WindowFunction` (`src/ir.rs`), with a doc line. Adding
the variant trips every exhaustive `match` on `WindowFunction` — `--all-targets` catches them;
expect arms in the binder, executor dispatch, `window_function_default_name`, and the frame-reject
match.

## Phase 2 — Lowering (`src/sql_planner.rs`)

In `extract_window_call`, add an NTILE branch (the function name is `NTILE`, taking one argument —
so it is recognised before the no-args ranking check fails it):

- Parse a single argument as a **positive integer literal** → `buckets: u64`; reject missing /
  extra / non-literal / non-integer / `0` / negative (`"NTILE requires a positive integer bucket
  count"`), and reject `IGNORE NULLS` / `FILTER` (mirror the value-function rejects).
- NTILE takes no bound expression: `window_args = Vec::new()` (like ranking).
- Add `WindowFunction::Ntile { .. }` to the frame-reject `matches!(... RowNumber | Rank |
  DenseRank ...)` so an explicit frame on NTILE rejects (frame-independent).
- Add the `window_function_default_name` arm: `WindowFunction::Ntile { .. } => "ntile"`.
- Do **not** add an `order_by.is_empty()` reject (NTILE permits no ORDER BY — see the ADR
  decision point; if the council wants the stricter fence, it is one branch here).

## Phase 3 — Binder (`src/binder.rs`)

Add `WindowFunction::Ntile { .. }` to the ranking arm that builds a non-null Int64 output field
(`Field::new(&call.output_name, DataType::Int64, false)`); no bound argument program. Output facts
stay row-preserving (`PlanOptimizerFacts::composite_with_row`), unchanged.

## Phase 4 — Executor (`src/exec.rs`)

Add a `WindowFunction::Ntile { buckets }` arm in the per-call dispatch (alongside the ranking
arms). Compute a `Vec<i64>` of per-row buckets via a helper:

```
fn ntile_buckets(buckets: u64, partition_start: &[usize], partition_end: &[usize], rows: usize) -> Vec<i64>
```

For each row: `i = row - partition_start[row]`, `len = partition_end[row] - partition_start[row] + 1`,
`k = buckets`, then `base = len / k`, `rem = len % k`, and the ADR's branch formula. Push the
`Int64Array`. (`buckets >= 1` is guaranteed by the lowering, so `k` is never zero; the `else`
branch's `/ base` is only reached when `base >= 1`.)

## Phase 5 — Tests, guards, verification, seal

### Behavioral gates (`tests/window_test.rs`)

Reuse the `wins` fixture (grp a: scores 10,10,20; grp b: 5,5).
- `ntile_buckets_rows_uniformly` — use a 4-row partition or slice and assert the even split
  (`NTILE(2)` over 4 rows → `[1,1,2,2]`).
- `ntile_distributes_remainder_to_first_buckets` — `NTILE(3)` over 4 rows → `[1,1,2,3]`.
- `ntile_restarts_per_partition` — `NTILE(2) OVER (PARTITION BY grp ORDER BY score)`: grp a (3
  rows) → `[1,1,2]`, grp b (2 rows) → `[1,2]`.
- `ntile_more_buckets_than_rows` — `NTILE(10)` over grp b (2 rows) → `[1,2]`.
- `ntile_without_order_by_is_accepted` — `NTILE(2) OVER (PARTITION BY grp)` plans + executes (no
  reject), bucketing in the totalized order; locks the no-ORDER-BY decision (folded from Gemini iter-2).
- Reconcile `deferred_window_forms_stay_rejected` — drop the `NTILE(2)` case (it now plans); add
  `NTILE(0)` and `NTILE(score)` (non-literal) as the new rejects.

(Some gates need a 4-row partition for the clean `[1,1,2,2]` / `[1,1,2,3]` splits — use a whole-
input ORDER BY over 4 of the rows, or a small local fixture if `wins` does not divide cleanly.)

### Distributed gate (`tests/distributed_process_test.rs`)

- `ntile_distributed_gate_is_value_discriminating` — `SELECT set_left.set_key, NTILE(2) OVER
  (PARTITION BY set_left.set_key ORDER BY set_left.id) AS bucket FROM set_left`: rows sorted →
  `[(None,1),(None,2),(Some(7),1),(Some(9),1)]`; id 4's bucket 2 requires the exchange to
  co-locate + order ids 2,4. `assert_cross_producer_cardinal_is_value_discriminating`. Fresh seed.

### Architecture guards (`tests/architecture/window_functions.rs`)

- `adr249_window_function_has_ntile_variant`
- `adr249_ntile_is_partition_geometry_not_frame`
- `adr249_ntile_bucket_count_is_positive_integer_literal`
- `adr249_ntile_distributed_gate_is_value_discriminating`

Reconcile `adr241_window_function_enum_retains_ranking_variants` if its NTILE-deferred assertion
needs updating (it currently asserts `!value.contains("Ntile")` for ValueFunction — NTILE is a
`WindowFunction` variant, not a `ValueFunction`, so that assertion still holds; verify during impl).

### Verification gate (route every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets   # catches exhaustive-match arms for the new variant
scripts/cargo-cgroup test  -p toron --test window_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test ntile
scripts/cargo-cgroup test  -p toron --test architecture_test adr249
scripts/cargo-cgroup test  -p toron --test architecture_test        # FULL
scripts/cargo-cgroup test  -p toron --test distributed_process_test # FULL
scripts/cargo-cgroup test  -p toron --lib                           # serde/postcard: a new enum VARIANT
cargo fmt -p toron -- --check
git diff --check
```

`--lib` is a backstop: a new `WindowFunction` variant does not shift existing postcard goldens
(no value uses `Ntile`), but confirm rather than assume (per the 246 precedent).

### Seal

Commit the implementation as `impl: adr 249 ntile window bucketing` with the
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.

## Risk notes

- **Exhaustive matches**: adding the variant touches binder / exec / `window_function_default_name`
  / frame-reject; run `--all-targets` early (the 246 value-variant precedent).
- **Remainder rule**: the discriminating test is `NTILE(3)/4 → [1,1,2,3]`; a naive `i / (len/k)`
  gives `[1,1,3,4]` and fails it — the remainder-to-first-buckets branch is load-bearing.
- **No ORDER BY**: settled (operator-confirmed) — consistent with ROW_NUMBER, and deterministic
  via the `exec.rs:3687` sort totalization, not arbitrary. No new reject.
