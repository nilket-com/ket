# Impl Plan — ADR 152: Fixed-Duration Interval Delta Arithmetic

Pairs with `plans/adr/0152_fixed_duration_interval_delta_arithmetic.md`. **Toron-only**:
IR/scalar, SQL frontend, Rune, binder arithmetic, executor, PBC decline, output
rendering, tests. No Wendelon/fact/wire change. **Smaller than 151: no new
`Expr`/`BoundExprNode` node** — reuse `Add`/`Sub` + `Binary`. All anchors verified
on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Scalar | `ir.rs` `enum ScalarValue` (add `Interval(Option<i64>)` ns); `ScalarValue::data_type()` |
| Physical type | `DataType::Duration(TimeUnit::Nanosecond)` (in width table `binder.rs:396`; `DurationNanosecondArray`) |
| Arithmetic bind | `binder.rs:~7300` `bind_arithmetic` — `(false,false)` path `:7364`, common-cast `:7382-7383`; `arithmetic_output_type` (in `ir.rs`, imported `:32`) |
| Exec arithmetic | `exec.rs:2047` `evaluate_bound_binary` → `evaluate_arithmetic(op,l,r,left_type)` `:2069`; unbound `evaluate_unbound_arithmetic:4777`; checked-int arms `:5267+` |
| Precision reuse | `timestamp.rs` checked-divisible pattern (`checked_divisible_i64` is private today; expose crate-locally or add an interval-local equivalent), tick helpers `:176/:220` |
| Range-fact spend | `binder.rs` `range_scalar_for_fact` (consumes a folded `Timestamp` literal) |
| SQL lowering | `sql_planner.rs` `lower_expr`; reuse `lower_extract_datetime_field`'s `DateTimeField` |
| SQL AST walkers | `reject_nested_aggregate:179`, `expr_contains_aggregate_call:5681`, `expr_contains_subquery:5716`, `expr_contains_window_function:5748`, `expr_contains_nonaggregate_column_reference:5841` |
| Rune | `rune_engine.rs` (constructor + register `:940`) |
| PBC | `pbc.rs` decline arms `:143`, `:198` |
| Output render | `daemon.rs` restamp is timestamp-only (do **not** restamp duration); CLI/daemon duration rendering |
| ScalarValue match sweep | `rune_engine.rs`, `lib.rs`, `pbc.rs`, `ir/execution.rs`, `sql_planner.rs`, `coordinator/write.rs`, `binder.rs`, `exec.rs` |

## Phase 1 — `ScalarValue::Interval` + the match sweep (the E0004 bulk)

- `ir.rs`: add `ScalarValue::Interval(Option<i64>)` (signed ns); `data_type()` →
  `DataType::Duration(TimeUnit::Nanosecond)`; deterministic `Display`; structural
  eq/hash. Helpers: `interval_data_type()`, `nanos_per_timestamp_unit(unit) -> i64`.
  Keep `LogicalTypePhysical` unchanged (interval is not a durable logical-type
  physical in this ADR).
- **Sweep every exhaustive `ScalarValue` match** (the 8 files) — literal
  encode/decode (`BoundExprProgram.literals`, `append_literal`), `coordinator/write.rs`
  array builders, `sql_planner`/`pbc` literal handling. New variant trips `E0004`
  widely → `cargo check -p toron --all-targets` is the gate
  ([[feedback_verify_all_targets_on_new_ir_field]]).
- `range_scalar_for_fact`: `Duration`/interval is **not** a range-fact-spendable
  column type — return inert (`None`), not a panic.

## Phase 2 — SQL literal lowering (sql_planner.rs)

- Add `SqlExpr::Interval { value, leading_field, last_field, .. }` arm in `lower_expr`:
  value must be a literal; `last_field`/range/precision present → reject; unit from
  `leading_field` (a `DateTimeField`) or the simple string (`'7 days'`); fixed set
  (ns/us/ms/s/min/hour/day→86 400 s); reject week/month/quarter/year + compound
  strings + i64-ns overflow → `Expr::Literal(ScalarValue::Interval(Some(ns)))`.
- **Add interval traversal to the SQL AST walkers** (`reject_nested_aggregate:179`,
  `expr_contains_aggregate_call:5681`, `expr_contains_subquery:5716`,
  `expr_contains_window_function:5748`, `expr_contains_nonaggregate_column_reference:5841`):
  the interval `value` is an `Expr`, so walkers must recurse into it or reject
  deliberately. **Missing this recreates 151's `Extract`/`AtTimeZone` walker-miss
  (Codex).**

## Phase 3 — Rune constructors (rune_engine.rs)

`nanoseconds/microseconds/milliseconds/seconds/minutes/hours/days(n)` (checked
overflow) → `Expr::Literal(ScalarValue::Interval(Some(ns)))`; register (`:940`). Pin
`rune_interval_arithmetic_matches_sql`.

## Phase 4 — Binder: the no-cast temporal branch + fold (the heart, D5/D6/D7)

In `bind_arithmetic`'s `(false,false)` path (`binder.rs:7364`), **before** the
numeric `arithmetic_output_type` fallback:

```
if let Some(out) = temporal_arithmetic_output_type(op, &left_type, &right_type)? {
    // out = Timestamp(unit,"UTC") | Duration(Nanosecond); Err for explicit rejects
    if let (Some(l), Some(r)) = (literal_of(builder,left), literal_of(builder,right)) {
        return Ok(builder.append_literal(fold_temporal_arithmetic(op,l,r,&out)?)); // D7
    }
    return Ok(builder.append_binary(op, left, right, out));   // NO cast_if_needed
}
// else: existing numeric path (arithmetic_output_type + cast_if_needed) unchanged
```

- `temporal_arithmetic_output_type` encodes D4 (reject ts+ts, interval−ts, Date32±,
  interval*/, non-UTC/None tz, unsupported units, mixed-unit ts−ts; `Ok(None)` =
  not temporal → numeric path).
- `fold_temporal_arithmetic`: scalar math with **checked** ops + a checked-divisible
  ns→unit helper (D6 preserve-and-reject); literal null → typed null of `out`.
- **Do not** route temporal operands through `cast_if_needed`.
- Ensure `Duration(Nanosecond)` **comparison** binds (existing same-physical-type
  comparison path; verify it executes — `interval_comparison_orders_fixed_durations`).

## Phase 5 — Executor temporal arms (exec.rs)

In `evaluate_arithmetic` (called from `evaluate_bound_binary:2069`; takes `left_type`),
branch on temporal array types before the numeric path:
- `Timestamp{Ms,Us,Ns}Array ± DurationNanosecondArray` → ns→unit (checked_divisible),
  `checked_add/sub` per element → rebuild `Timestamp*Array.with_timezone("UTC")`;
- `DurationNanosecondArray ± DurationNanosecondArray` → checked → Duration;
- `Timestamp(u)Array − Timestamp(u)Array` (same unit) → per-element diff × (unit→ns)
  checked → `DurationNanosecondArray`;
- nulls per-element `Option`; mixed-unit unreachable (bind rejected). Add literal
  array construction for `ScalarValue::Interval`. Mirror in
  `evaluate_unbound_arithmetic:4777` **only if** Rune direct-eval routes there. **No
  Arrow Duration/Interval kernel.**

## Phase 6 — PBC decline (pbc.rs)

Decline a `Binary` whose operand/result type is `Duration(Nanosecond)` in
`compile_node` (`:143`) + `emit_node` unsupported arm (`:198`); reason `"fixed-
duration interval arithmetic is not PBC-eligible"`. No new opcode.

## Phase 7 — Output rendering (daemon.rs / cli.rs)

Verify `DurationNanosecondArray` renders deterministically in CLI/daemon SQL output;
add an explicit interval string format if needed. **Do not restamp duration arrays** —
151's restamp path is timestamp-metadata-only.

## Phase 8 — Guards (tests/architecture/temporal_interval.rs, adr152_*)

Source guards: `ScalarValue::Interval` physical = `Duration(Nanosecond)` (not Arrow
`Interval`); the no-cast temporal branch exists and `arithmetic_output_type` stays
numeric-only; literal fold present; SQL AST walkers recurse through `interval.value`;
PBC declines; no Arrow interval/duration-kernel import in the temporal path; no naive
timestamp; `day`→86 400 s; calendar/Date32 reject strings present; behavior-gate
names exist.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets     # MANDATORY — new ScalarValue
                                                      # variant ⇒ E0004 sweep
scripts/cargo-cgroup test -p toron temporal_interval
scripts/cargo-cgroup test -p toron --test architecture_test adr152
scripts/cargo-cgroup test -p toron --test non_aggregate_projection_accounting_test
scripts/cargo-cgroup test -p toron interval
git diff --check
```

## Risks

- **Common-cast trap** (D5) — routing `ts ± interval` through numeric `cast_if_needed`
  corrupts it to `ts ± ts`. The temporal branch must bypass it.
- **Missing const-fold** (D7) — without the literal fold, the headline filter sees
  `col > Binary(...)` and 150's fact never spends.
- **SQL walker miss** — `SqlExpr::Interval` carries an expression value; missing it
  in the AST walkers recreates 151's `Extract`/`AtTimeZone` failure.
- **ScalarValue E0004 breadth** — handle the new variant in every exhaustive match
  across 8 files; `--all-targets` is non-negotiable.
- **Precision silent-round** — ns→unit must be checked-divisible→error (D6), never a
  truncating cast.
- **i64-ns overflow** — checked everywhere; `ts(ms)−ts(ms)` of >292 y-apart instants
  errors (never wrong). `i128` is the noted follow-on.
- **Duration vs Interval Arrow type** — use `Duration(Nanosecond)`; `DataType::Interval`
  stays reserved for the deferred calendar cut.
- **Duration rendering** — verify user-facing SQL output before finalizing.

## Deferred (named)

Calendar intervals (Arrow `Interval`, reuse 151 civil path); `Date32 ± Interval`;
mixed-unit `ts − ts`; `Interval × numeric`; `CONVERT`/units; `SET TIME ZONE`; Date64.
