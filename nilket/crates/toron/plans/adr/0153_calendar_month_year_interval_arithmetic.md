# ADR 153: Calendar Month-Year Interval Arithmetic

**Status:** Promoted. **3-of-3 on topic/scope/carrier; the runtime-frame mechanism
promoted from the grounded Claude+Codex position (Gemini's impl under-specified it).**
ADR 152 built the Time dimension's **fixed** delta (`Interval`→`Duration(Nanosecond)`,
`INTERVAL '1 day'` = 86 400 s) but deliberately deferred **calendar** arithmetic.
ADR 153 adds the calendar delta — month/year — closing the temporal arc: the missing
piece for month-over-month / year-over-year analytics (151 gave `date_trunc('month')`;
you still can't build the comparison window without `+ INTERVAL '1 month'`).
**Toron-only.**

## Decisions

**D1 — Distinct calendar scalar.** `ScalarValue::CalendarMonthInterval(Option<i32>)` =
signed total **months**. Separate from 152's `ScalarValue::Interval(Option<i64>)`
(fixed nanoseconds).

**D2 — Arrow `Interval(YearMonth)` carrier (3-of-3).** `CalendarMonthInterval::data_type()`
→ `DataType::Interval(IntervalUnit::YearMonth)` (`IntervalYearMonthArray`, i32-backed).
**Disjoint** from 152's `Duration(Nanosecond)` (`is_interval_type:7294` stays fixed-only;
add `is_calendar_month_interval_type`). YearMonth makes day/nanos physically impossible
in v1 and the architecture guard crisp; `MonthDayNano` is the deferred carrier for the
later mixed-calendar cut. (Claude's iter-1 `MonthDayNano` lean conceded to Codex+Gemini.)

**D3 — Month/year literals only.** Accept `INTERVAL '1 month'`/`'12 months'`/`'1 year'`/
`'2 years'`/`'1' MONTH`/`'1' YEAR` (signed); **year → 12 months** (checked i32).
`day` and smaller units **continue to route to 152's fixed `Interval`** — `INTERVAL '1
day'` stays 86 400 s, never reinterpreted (the load-bearing 152 invariant: one SQL
spelling, one meaning). Reject `week`, `quarter`, compound strings (`'1 year 2 months'`),
range forms (`YEAR TO MONTH`), precision-bearing intervals. Rune adds `months(n)`/
`years(n)` beside 152's `days(n)` — the split *is* documentation
([[rune_is_primary_interface]]).

**D4 — Calendar intervals are construct-only deltas.** `Timestamp − Timestamp` remains
152's fixed elapsed `Duration(Nanosecond)` — no subtraction infers "one month" from
elapsed ticks. Calendar intervals come only from literals / Rune constructors.

**D5 — Algebra (extends 152's `temporal_arithmetic_output_type:7200`).**
- `Timestamp(UTC) ± CalendarMonthInterval → Timestamp(UTC)`;
- `CalendarMonthInterval + Timestamp(UTC) → Timestamp(UTC)` (add symmetry);
- `Date32 ± CalendarMonthInterval → Date32` (zone-free civil);
- `CalendarMonthInterval ± CalendarMonthInterval → CalendarMonthInterval`;
- **reject** `CalendarMonthInterval − Timestamp`, `Timestamp + Timestamp` (152),
  `CalendarMonthInterval ± fixed Interval` (distinct delta categories — no silent
  coercion), `Date32 ± fixed Interval` (152 deferral), and calendar interval
  comparison / multiplication / division / numeric scaling.

**D6 — Timestamp calendar arithmetic uses a bind-resolved civil frame; the runtime
never asks for session state. [the load-bearing decision; grounded Claude+Codex].**
Calendar timestamp math is zone-dependent, but the executor is **session-stateless**
(`evaluate_bound_program_without_storage` has only a bound program + a batch). So:
- **Effective zone resolved at bind** — explicit `AT TIME ZONE` on the timestamp operand
  if present, else the request `session_timezone` (structural, 151's D3 pattern);
- **carried to exec as Arrow tz metadata** by binding the timestamp operand through
  151's inserted `AtTimeZone(effective_zone)` lens (`array.with_timezone`, same ticks);
- **exec's calendar arm reads timestamp array timezone metadata** as the civil
  frame, does the month math there, and returns `Timestamp(unit, Some("UTC"))`.
The calendar binder arm therefore **accepts a zone-stamped (any-IANA) operand** and
treats its zone as the frame — *not* 152's UTC-only check. Gemini's iter-2 impl left
this "feed the session/explicit timezone" to exec, which (taken literally) repeats the
session-stateless violation caught in 152; the grounded mechanism is bind-carry.
*Fallback (named, not v1): a dedicated `CalendarArithmetic { op, ts, interval, zone }`
bound node with a baked zone string (151's Extract/DateTrunc pattern) if the
`AtTimeZone`-wrap ripples.* Date32 needs no frame.

**D7 — End-of-month clamp + DST reuse machinery (grounded).** `Timestamp + INTERVAL '1
month'`: `utc_datetime_from_ticks → .with_timezone(frame)` (151) → local naive
datetime → chrono checked month add/subtract (add for nonnegative deltas, subtract for
negative deltas, with checked `i32::MIN` handling). Chrono clamps missing target-month
days, so `Jan 31 + 1 month → Feb 28/29`, `2024-02-29 − 1 year → 2023-02-28` without
custom month math. Then resolve the new local datetime through 151's
`from_local_datetime` policy (gap→error, fold→earlier) → ticks. `checked_*` returns
`Option` → out-of-range errors. Date32 uses the same checked month-add/subtract policy
on `NaiveDate` directly (zone-free).

**D8 — Zone-dependent literal fold for selectivity.** Calendar folding depends on the
zone (unlike 152's zone-free fold), but the **effective zone is known at bind**, so
`WHERE event_time >= TIMESTAMPTZ '…' - INTERVAL '1 year'` folds to a UTC `Timestamp`
literal at bind and **spends 150's range facts** → broadcast (MoM/YoY). `Date32 ±
calendar` folds zone-free. The literal case folds directly (no `AtTimeZone` wrap); only
the runtime-column case wraps (D6).

**D9 — Toron-only; PBC declines; no Arrow calendar kernel.** No Wendelon/fact/wire/
opcode/session change. PBC declines `Interval(YearMonth)` literals and arithmetic.
Manual chrono-backed civil math (151's path), not Arrow's interval kernels.

## Headline gate

`relative_calendar_month_filter_spends_timestamp_range_fact`: effective-zone-resolved,
`WHERE event_time >= TIMESTAMPTZ '2026-01-31T00:00:00Z' - INTERVAL '1 month'` folds to a
UTC `Timestamp` literal, spends 150's fact, infers broadcast — fails on `main`.

## Supporting gates

- `timestamp_plus_one_month_clamps_end_of_month` (Jan 31 → Feb 28/29, chrono);
- `timestamp_calendar_arithmetic_reads_frame_from_array_metadata_not_session` (D6
  invariant — a plain column uses the bind-wrapped session zone; an explicit `AT TIME
  ZONE` column uses that zone);
- `timestamp_minus_one_year_uses_151_dst_gap_fold_policy`;
- `date32_plus_one_month_is_civil_and_zone_free`;
- `date32_minus_one_year_clamps_leap_day`;
- `calendar_interval_plus_calendar_interval_returns_calendar_interval`;
- `calendar_interval_minus_timestamp_rejects` / `timestamp_plus_timestamp_rejects`;
- `calendar_and_fixed_interval_mix_rejects`;
- `fixed_day_interval_remains_86400_seconds` (152 unbroken);
- `calendar_week_quarter_compound_reject_in_153`;
- `timestamp_minus_timestamp_still_returns_fixed_interval`;
- `rune_months_years_match_sql_calendar_literals`;
- architecture: fixed=`Duration(Nanosecond)`, calendar=`Interval(YearMonth)` (disjoint,
  not `MonthDayNano`); `arithmetic_output_type` stays numeric-only; **runtime has no
  session lookup** (frame from array metadata); no Arrow calendar kernel; PBC declines.

## Consensus trail

3-of-3 on topic, title, month/year-only scope (Codex's fixed-day grounding), the
`Interval(YearMonth)` carrier (Claude conceded `MonthDayNano`), zone-aware Timestamp via
151, Date32 zone-free, end-of-month clamping, literal fold for MoM/YoY selectivity,
`ts − ts` stays fixed, PBC-declined, Toron-only. **The runtime-frame mechanism (D6)** is
promoted from the **identical Claude+Codex** grounded position — bind-resolve the zone,
carry it via 151's `AtTimeZone` metadata, exec reads timestamp array timezone metadata
— over Gemini's under-specified "feed the timezone to exec" (which would repeat the
152 session-stateless violation). Calendar comparison deferred.

## Forward arc

`152 fixed-duration Interval → 153 calendar month/year (closes temporal) → (civil
day/week/quarter syntax · MonthDayNano mixing) · (measured-miss/skew evidence) ·
(CONVERT/units pivot) → Theta join-key intersection → join selectivity → reordering`.
