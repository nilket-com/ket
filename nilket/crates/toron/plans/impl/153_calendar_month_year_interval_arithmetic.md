# Impl Plan — ADR 153: Calendar Month-Year Interval Arithmetic

Pairs with `plans/adr/153_calendar_month_year_interval_arithmetic.md`. **Toron-only**:
IR/scalar, SQL frontend, Rune, binder arithmetic, executor, civil helpers, PBC decline,
tests. No Wendelon/fact/wire change. **No new IR node** — reuse `Add`/`Sub` + `Binary`
+ 151's `AtTimeZone`. All anchors verified on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Scalar | `ir.rs` `enum ScalarValue` (add `CalendarMonthInterval(Option<i32>)` months); `data_type()` → `Interval(IntervalUnit::YearMonth)` |
| Carrier array | Arrow `IntervalYearMonthArray` / `IntervalYearMonthBuilder` (i32 months) |
| 152 arithmetic seam | `binder.rs:7200` `temporal_arithmetic_output_type`, `:7324` `fold_temporal_arithmetic`, dispatch `:7676`, `is_interval_type:7294` |
| Frame carrier (151) | `AtTimeZone`: `append_at_time_zone` (`ir/execution.rs:547`), `evaluate_at_time_zone` (`exec.rs:2191`, `array.with_timezone(zone)`) |
| Exec frame read | timestamp array timezone metadata (`timezone() -> Option<&str>`) |
| Civil math | `timestamp.rs` (`utc_datetime_from_ticks:314`, `ticks_from_utc_datetime:270`, `from_local_datetime` policy `:80`/`:196`); chrono checked month add/subtract on local naive values (clamps last-day, `Option`) |
| Date32 civil | `date32.rs`; checked month add/subtract on `NaiveDate` |
| Exec arithmetic | `exec.rs:5248` `evaluate_arithmetic`; bound binary `exec.rs:2073` |
| SQL lowering | `sql_planner.rs` `lower_interval_literal` (152); AST walkers already recurse `interval.value` (152 — same `SqlExpr::Interval`, covered) |
| PBC | `pbc.rs` `binary_has_fixed_duration_interval_type` (152) — mirror for YearMonth |
| ScalarValue sweep | `rune_engine.rs`, `lib.rs`, `pbc.rs`, `ir/execution.rs`, `sql_planner.rs`, `coordinator/write.rs`, `binder.rs`, `exec.rs` |

## Phase 1 — Scalar + match sweep (E0004 bulk)

`ir.rs`: add `ScalarValue::CalendarMonthInterval(Option<i32>)`; `data_type()` →
`DataType::Interval(IntervalUnit::YearMonth)`; deterministic `Display` (e.g.
`"14 months"`); structural eq/hash. Helper `is_calendar_month_interval_type`. **Sweep
all 8 exhaustive `ScalarValue` matches** (literal encode/decode, `coordinator/write.rs`
builder — add `IntervalYearMonthBuilder` arm, `pbc`/`sql_planner` literal handling).
`cargo check -p toron --all-targets` is the gate
([[feedback_verify_all_targets_on_new_ir_field]]). `range_scalar_for_fact`: calendar
interval is not a spendable column type → inert.

## Phase 2 — SQL lowering (sql_planner.rs)

Extend `lower_interval_literal` to branch fixed-vs-calendar on the unit:
- `month`/`months`/`year`/`years` (+ `'1' MONTH`/`YEAR`), signed: integer × (year→12),
  checked i32 → `Expr::Literal(ScalarValue::CalendarMonthInterval(Some(months)))`;
- `day` and sub-day → **unchanged 152 fixed `Interval`**;
- reject `week`, `quarter`, compound strings, range/precision forms (named errors).
The AST walkers already recurse `interval.value` (152, same `SqlExpr::Interval`) — add a
guard asserting calendar literals are covered.

## Phase 3 — Rune (rune_engine.rs)

`months(n)`/`years(n)` (checked, year→12) → `Expr::Literal(CalendarMonthInterval)`;
register (`:940`). 152's `days(n)` stays fixed. Pin `rune_months_years_match_sql`.

## Phase 4 — Binder: calendar branch (the heart, D5/D6/D7/D8)

A **new calendar branch** in `bind_arithmetic` (`:7676`), beside 152's fixed-temporal
branch (disjoint by interval type). For `op ∈ {Add,Sub}` with a `CalendarMonthInterval`
operand and a `Timestamp`/`Date32`/`CalendarMonthInterval` operand:

- **Type/algebra** via `calendar_arithmetic_output_type(op, l, r)` (D5 reject set: cal−ts,
  ts+ts, cal±fixed, ×÷/numeric, comparison). `cal±cal` → cal (checked month add).
- **Timestamp ± cal:**
  - resolve effective zone **structurally** (operand `Expr::AtTimeZone(ts, z)` → `z`;
    else `ctx.session_timezone`) — 151's D3 pattern;
  - **literal × literal** → `fold_calendar_arithmetic` (civil math, chrono clamp, 151
    DST) → flat `Timestamp` literal (D8 selectivity);
  - **else (column)** → bind the ts operand, **wrap in `append_at_time_zone(effective_zone)`**
    so the array carries the frame (D6), `append_binary(op, wrapped_ts, interval,
    Timestamp(unit,"UTC"))`. The calendar arm **accepts a zone-stamped operand** — *not*
    152's UTC-only `timestamp_arithmetic_unit`.
- **Date32 ± cal:** no zone; literal→fold, else `append_binary` → `Date32`.

`fold_calendar_arithmetic`: chrono `checked_add_months`/`checked_sub_months` (clamps),
`from_local_datetime` (151), null → typed null. **Do not** route calendar operands
through `cast_if_needed`.

## Phase 5 — Executor (exec.rs) + civil helpers

`evaluate_arithmetic` (`:5248`) — dispatch on **array types** before numeric:
- `Timestamp{Ms,Us,Ns}Array ± IntervalYearMonthArray`: read **`ts_array.timezone()`** as
  the frame (D6 — never a session zone) → per element `add_calendar_months(tick, unit,
  frame_tz, ±months)` → rebuild `Timestamp*Array.with_timezone("UTC")`; nulls per-element;
- `Date32Array ± IntervalYearMonthArray`: `add_calendar_months_date32(days, ±months)`
  (zone-free) → `Date32Array`;
- `IntervalYearMonthArray ± IntervalYearMonthArray`: checked month add → YearMonth.
Add `IntervalYearMonthArray` literal construction. No Arrow calendar kernel.

`timestamp.rs`: `add_calendar_months(tick, unit, tz, months) -> Result<i64>` =
`utc_datetime_from_ticks → .with_timezone(tz) → naive_local()`; apply chrono checked
month add/subtract on the local naive value (add for nonnegative deltas, subtract for
negative deltas, with checked `i32::MIN` handling); resolve the result through 151's
`from_local_datetime` gap/fold policy; then `ticks_from_utc_datetime`. `date32.rs`:
`add_calendar_months_date32(days_i32, months) -> Result<i32>` via `NaiveDate` checked
month add/subtract + clamp.

## Phase 6 — PBC decline (pbc.rs)

Mirror 152's `binary_has_fixed_duration_interval_type` with a YearMonth check: decline a
`Binary` whose output/operand is `Interval(YearMonth)`, + the `CalendarMonthInterval`
literal arm; reason `"calendar interval arithmetic is not PBC-eligible"`.

## Phase 7 — Guards (tests/architecture/temporal_calendar.rs, adr153_*)

Source guards: scalar physical = `Interval(YearMonth)` (not `Duration`, not
`MonthDayNano`); calendar branch present + disjoint from 152's fixed branch; **exec
calendar arm reads timestamp array timezone metadata, never a session zone** (assert no
`session_timezone`/`PlannerConfig` in the calendar exec path); chrono checked month
add/subtract used; `from_local_datetime` reused; `INTERVAL '1 day'` still 152 fixed;
week/quarter/compound reject strings; PBC declines; behavior-gate names exist.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets   # MANDATORY — new ScalarValue variant
scripts/cargo-cgroup test -p toron temporal_calendar
scripts/cargo-cgroup test -p toron --test architecture_test adr153
scripts/cargo-cgroup test -p toron calendar
git diff --check
```

## Risks

- **Session-stateless violation** (D6) — the calendar exec arm must read the frame from
  `array.timezone()`, never reach for a session zone. The frame is placed by the
  bind-inserted `AtTimeZone` wrap (or an explicit `AT TIME ZONE`).
- **UTC-only check collision** — the calendar timestamp operand is deliberately
  zone-stamped (non-UTC); use the calendar accept-any-zone path, not 152's
  `timestamp_arithmetic_unit` UTC fence.
- **152 fixed-day regression** — `INTERVAL '1 day'` must keep emitting fixed `Duration`;
  a calendar-branch leak would break 152. Guard it.
- **Clamp/DST** — reuse chrono checked month add/subtract + 151 `from_local_datetime`
  verbatim; don't hand-roll month math.
- **ScalarValue E0004 breadth** — `--all-targets`.
- **Carrier confusion** — `Interval(YearMonth)`, never `Duration` (152) or `MonthDayNano`
  (deferred).

## Deferred (named)

Civil day/week syntax (retargets 152's fixed-day — needs the explicit fixed-vs-civil
decision); quarter (3-month desugar); compound `YEAR TO MONTH`; `MonthDayNano` mixing
(`calendar ± fixed`); calendar interval comparison/scaling; `CONVERT`/units; Date64.
