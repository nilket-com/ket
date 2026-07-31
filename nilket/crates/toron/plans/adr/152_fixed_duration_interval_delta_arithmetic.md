# ADR 152: Fixed-Duration Interval Delta Arithmetic

**Status:** Promoted. **Operator-locked title; Claude/Codex convergence; two
Gemini impl-level divergences recorded and resolved on grounding.** ADRs 149/150/151
made `Date32`/`Timestamp` filterable and computable, but left the Time dimension's
**delta** unbuilt: no `ScalarValue::Interval`, no `ts ± interval`, no `ts − ts`
(grounded — `exec` has arithmetic on numeric arrays only). So the #1 relative-time
filter — `event_time > … - INTERVAL '7 days'` — cannot bind. ADR 152 adds the
fixed-duration delta and its point/delta arithmetic: 151's named deferral, and
brainstorm-009's first **point/delta** instance on familiar ground (proving the
affine algebra a future `CONVERT`/units arc needs). **Toron-only.**

## Decisions

**D1 — `Interval` is the Time-delta scalar.** `ScalarValue::Interval(Option<i64>)`
= signed elapsed **nanoseconds**. A duration — not a timestamp point, not a civil
date, not a zone lens.

**D2 — Physical carrier is Arrow `Duration(Nanosecond)`, not `Interval`.**
`ScalarValue::Interval::data_type()` → `DataType::Duration(TimeUnit::Nanosecond)`
(already in the byte-width table, `binder.rs:396`; i64-backed
`DurationNanosecondArray`). Arrow distinguishes `Duration` (fixed elapsed i64) from
`Interval` (calendar months/days); a fixed-duration delta **is** `Duration`. This
gives the type-distinctness the algebra needs (`ts + Duration` ✓, `ts + Int64` ✗)
**and** maps the fixed/calendar split onto Arrow's own `Duration`/`Interval` split —
the deferred calendar cut will use `Interval`. Same SQL keyword, two honest types
by unit. (Resolves Gemini's iter-2 "add `DataType::Interval`" — that is the
*calendar* carrier, wrong for v1.)

**D3 — Fixed-duration literals only; classify over `DateTimeField` (151 reuse).**
Units: nanosecond, microsecond, millisecond, second, minute, hour, and **`day` =
exactly 86 400 s** (explicitly fixed, *not* civil — resolves Codex's smuggle-DST
flag by being loud). Reject `week`, `month`, `quarter`, `year`, compound ranges
(`DAY TO SECOND`), leading/fractional precision, compound strings (`'1 day 2
hours'`), and non-literal values. The unit arrives as `sqlparser`'s `DateTimeField`
— the *same enum* 151 maps — so fixed-vs-deferred is classified once over it.

**D4 — Point/delta algebra is bind-time typed.**
- `Timestamp(unit, UTC) ± Interval → Timestamp(unit, UTC)`;
- `Interval + Timestamp(unit, UTC) → Timestamp(unit, UTC)` (add only; `Interval −
  Timestamp` rejects);
- `Timestamp(unit, UTC) − Timestamp(unit, UTC) → Interval` (matching units;
  **mixed-unit rejects in v1**);
- `Interval ± Interval → Interval`; interval comparison orders fixed durations;
- **reject** `Timestamp + Timestamp` (point+point), `Date32 ± Interval` (D7),
  `Interval × / Interval`, and `Interval × numeric` (deferred).
`ts − ts` is **in** — it's `point − point = delta`, the defining equation the cut
exists to prove.

**D5 — No new IR node; a dedicated *no-cast* temporal branch. [load-bearing impl
correction].** Reuse `Expr::Add/Sub` + `BoundExprNode::Binary`. **But** the generic
arithmetic binder common-casts *both* operands to one output type (`cast_if_needed`,
`binder.rs:7382-7383`) — correct for numerics, **fatal for `Timestamp + Interval`**
(it would cast the Interval side to Timestamp). So 152 adds a temporal branch
*before* the numeric fallback: type the result via a temporal rule and
`append_binary` with operands kept at their **distinct** types (exec dispatches on
the real array types). `arithmetic_output_type` stays numeric-only.

**D6 — Precision is preserve-and-**reject**, never silent round.** `Timestamp(unit)
± Interval` returns `Timestamp(unit, Some("UTC"))` (150/151 invariant; no naive
timestamp). The interval's nanoseconds convert to the timestamp's unit via
**checked-divisible (reuse 150's checked-divisible helper pattern; expose it
crate-locally or add the interval-local equivalent)**; a non-representable
interval (e.g. `Timestamp(ms) + INTERVAL '1 microsecond'`) **errors** — literal
folding rejects at bind, runtime column arithmetic returns a typed execution error.
All arithmetic is **checked** (overflow → error, never wrap). (Resolves Gemini's
iter-2 "truncates/floors safely **or** errors" — silent truncation violates 150's
precision-loss discipline; reject is the only honest path.)

**D7 — Literal temporal arithmetic folds before selectivity (new, contained).** No
constant-fold pass exists today, so the temporal branch folds directly: when both
operands bind to literals, evaluate the scalar (null folds to a typed null) and emit
`Expr::Literal` instead of `Binary`. This makes `TIMESTAMPTZ '…' - INTERVAL '7 days'`
a flat `ScalarValue::Timestamp` that 150's `range_scalar_for_fact` spends → the
filtered dim broadcasts. Column ± interval stays a runtime `Binary`. **This is why
Interval is on the pushdown spine, not frontend sugar.**

**D8 — `Date32 ± Interval` deferred.** Date32 is civil; `Date32 + day` is civil
day-math and sub-day raises promote-to-Timestamp questions — it pairs with the
calendar-interval cut.

**D9 — Rune first-class constructors.** `nanoseconds/microseconds/milliseconds/
seconds/minutes/hours/days(n)` produce interval literals targeting the same IR/
binder path as SQL — the clean bridge to the future dimensioned-quantity arc
without overloading `CAST` ([[rune_is_primary_interface]]).

**D10 — Toron-only; PBC declines; no Arrow kernel.** No Wendelon producer, fact
schema, storage/PBC opcode, or session change. PBC declines interval-typed binary
nodes. Execution is **manual checked tick math**, *not* Arrow's `Duration`/`Interval`
arithmetic kernels (grounded: `exec` imports none; calendar kernels are zone-naive).

## Why this boundary

Fixed-duration arithmetic is **zone-free tick math** — +3600 s changes the epoch
ticks identically in every zone, needing **none** of 151's session-zone machinery.
Calendar arithmetic (+1 month, civil-day) is **zone-dependent** — it reopens 151's
`from_local_datetime` DST policy. Deferring calendar keeps v1 entirely off the
zone-semantics surface; the calendar cut later *reuses* 151's civil path.

## Known bound

i64 nanoseconds caps a single interval at ±~292 years; `Timestamp(ms) −
Timestamp(ms)` of instants >292 y apart overflows → **checked error, never wrong**
(`i128` backing is the noted follow-on if it ever bites). Realistic analytical
intervals are far inside this.

## Headline gate

`relative_interval_filter_spends_timestamp_range_fact`: `WHERE event_time >
TIMESTAMPTZ '2026-01-01T00:00:00Z' - INTERVAL '7 days'` folds to a `Timestamp`
literal at bind, spends 150's range fact, filtered dim infers broadcast — **fails
on `main`**.

## Supporting gates

- `timestamp_plus_interval_preserves_utc_instant_type`;
- `timestamp_minus_interval_preserves_utc_instant_type`;
- `timestamp_minus_timestamp_returns_interval_in_shared_unit`;
- `mixed_unit_timestamp_difference_rejects_in_v1`;
- `interval_plus_interval_returns_interval`;
- `interval_comparison_orders_fixed_durations`;
- `timestamp_plus_timestamp_rejects`;
- `interval_day_is_fixed_86400s_not_civil` (D3);
- `non_representable_interval_precision_rejects` (D6 preserve-and-reject);
- `calendar_month_year_interval_literal_rejects_in_v1` (D3 fence);
- `date32_plus_interval_rejects_in_v1` (D8);
- `rune_interval_arithmetic_matches_sql` (Rune-first);
- architecture: physical carrier is `Duration(Nanosecond)` not Arrow `Interval`;
  arithmetic is Toron-executed, PBC-declined, uses **no Arrow interval/duration
  kernel**, constructs no naive timestamp; `arithmetic_output_type` stays
  numeric-only; SQL AST walkers recurse through `interval.value`.

## Consensus trail

3-of-3 on topic, operator-locked title, `ScalarValue::Interval(Option<i64>)` nanos,
the point/delta algebra, fixed-only scope, `day=86400s`, the selectivity fold,
Rune-first, PBC decline, and calendar/Date32 deferral.

**Two Gemini impl-level divergences, resolved on grounding (promoted Claude+Codex):**
(1) **physical carrier** — Arrow `Duration(Nanosecond)` (Claude+Codex), not Gemini's
`DataType::Interval` (the calendar type, wrong for fixed v1); (2) **precision** —
preserve-and-**reject** (Claude+Codex, per 150's no-silent-round discipline), not
Gemini's "truncates/floors safely or errors." Both Gemini positions were
impl-level, grounded-against; Gemini's algebra/scope decisions fully converge.

## Forward arc

`151 temporal ops → 152 Interval (fixed-duration Time-delta) → (calendar intervals
· CONVERT/units arc) · (measured-miss → skew/quantile) → Theta join-key intersection
→ join selectivity → reordering`.
