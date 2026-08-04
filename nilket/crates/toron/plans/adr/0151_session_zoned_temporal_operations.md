# ADR 151: Session-Zoned Temporal Operations

**Status:** Promoted. **Operator-approved Claude/Codex convergence; Gemini
impl-mechanism dissent recorded.** ADRs 149/150 made `Date32` and `Timestamp`
first-class Toron scalars, unlocking temporal *filtering* and selectivity — Toron
can bind `event_time >= DATE '2026-01-01'` and spend min/max facts. But the
scalars are not *computable*: the #1 analytical follow-through to filtering
event-time is rolling it up (`date_trunc('day', event_time)` for time series,
`EXTRACT(year FROM …)` for grouping), and none of that binds today. ADR 151 makes
the landed temporal scalars computable via a narrow, hand-wired Toron expression
cut. **Toron-only; no Wendelon producer, fact schema, PBC opcode, persistent
session, or `SET TIME ZONE`.**

## Grounded gaps

- `sqlparser` 0.62 parses `SqlExpr::Extract` and `SqlExpr::AtTimeZone`, but
  Toron's `lower_expr` has no arms for them; `AtTimeZone` is referenced nowhere.
- Toron's expression IR is hand-wired (`Expr` `ir.rs:527`, `BoundExprNode`
  `ir/execution.rs:78`); there is **no** scalar-function framework. The only
  temporal node, `TimeBucket`, is experimental and **Int64-ms only**
  (`exec.rs:2118`) — it does not consume `Timestamp`, and floor-division is the
  wrong model for calendar truncation across DST/month/year.
- Arrow exposes `date_part` (honors timestamp tz metadata); no Arrow `date_trunc`
  kernel exists → truncation is a Toron helper.
- 150's daemon restamp rewrites **every** result timestamp to the session zone
  (`daemon.rs:1473`), which would erase an explicit `AT TIME ZONE` projection.

## Decision

SQL `EXTRACT(field FROM expr)`, `date_part('<field>', expr)` (alias),
`date_trunc('<field>', ts_expr)`, `ts_expr AT TIME ZONE '<IANA>'`, plus
first-class Rune helpers for each.

## The zone-resolution rule (semantic core)

`EXTRACT`/`date_trunc` on a `Timestamp` are **zone-dependent** (the hour/day/
month of an instant is only defined relative to a zone). Effective zone =
explicit `AT TIME ZONE '<IANA>'` on the operand if present, else the request
`session_timezone` (150). The operand stays a UTC instant — ticks never change.
`Date32` operations are **civil and zone-free** (the 149/150 two-category model).

## Decisions

**D1 — Hand-wired temporal IR; follow `TimeBucket`.** Add `Expr::{AtTimeZone,
Extract, DateTrunc}` + matching `BoundExprNode` variants + builder helpers, with
**typed field enums** (`TemporalExtractField`, `TemporalTruncField`) so bad
fields fail at lower/bind, not exec. `TimeBucket` is the single-input precedent
already threaded through every structural site; the diff adds an arm at each.
No generic dispatch layer.

**D2 — Bind-time zone resolution, baked into the bound node. [grounded 2-of-3].**
The executor is session-stateless: `evaluate_bound_program_without_storage`
(`exec.rs`) receives no `PlannerConfig`/`session_timezone`. So the effective zone
is resolved **at bind** — where the bind context already carries the resolved
`session_timezone: Tz` (`binder.rs:506`; `PlannerConfig::resolved_session_timezone`
`:97`) — and **baked into timestamp bound nodes as a string**:
`BoundExprNode::Extract { field, zone: Some(zone), input }` and `DateTrunc {
field, zone, input }`. Date32 `Extract` carries `zone: None`. Exec reads the
baked zone; **no new exec plumbing**. Matches 150's "resolved once, never
ambient at execution" doctrine.

**D3 — Zone detection is STRUCTURAL, not type-directed.** A landed `Timestamp` is
`Timestamp(unit, Some("UTC"))` (`timestamp.rs:40`), so a plain UTC column and `ts
AT TIME ZONE 'UTC'` are **type-identical** — yet `EXTRACT(HOUR FROM utc_col)`
under session `America/Chicago` must give the **Chicago** hour while `EXTRACT(HOUR
FROM utc_col AT TIME ZONE 'UTC')` must give UTC. Bind therefore resolves the zone
by **inspecting whether the operand expression is an explicit `Expr::AtTimeZone`**
(consume its zone, bind the inner timestamp), never by reading the input's
`DataType` zone tag. Otherwise → bake `ctx.session_timezone`. Consuming the
wrapper does **not** bypass the type rule: the inner expression must still bind
as `Timestamp`; `Date32 AT TIME ZONE ...` is invalid.

**D4 — `AT TIME ZONE` = zone annotation, UTC ticks unchanged.** Returns
`Timestamp(unit, Some("<IANA>"))` over the same UTC ticks (an Arrow tz-stamped
timestamp is still a UTC-epoch instant — tz is display metadata, so **not** a
naive instant; 150 not reopened). String-literal IANA only in v1; validated at
bind (`parse_session_timezone`); invalid/dynamic/fixed-offset → bind error. Two
roles, one node: (a) direct operand of a temporal op → its zone is the lens
(incl. explicit `'UTC'`); (b) standalone projection → survives to exec, which
stamps array tz metadata via `with_timezone` (identical ticks).

**D5 — `EXTRACT` / `date_part`.** Return `Int32`, nulls preserved. Timestamp:
evaluate the civil field in the baked effective zone. Date32: civil date fields,
zone-free. Date64 unsupported. Fields: `year, isoyear, quarter, month,
week/isoweek, day, dow, doy, hour, minute, second, millisecond, microsecond,
nanosecond`. Reject (named error): `epoch`, `timezone*`, `isodow` (Monday-zero
range mismatch with Arrow), `century/decade/millennium`, custom fields, and
time-of-day fields on Date32.

**D6 — `date_trunc`.** Over Timestamp, returns `Timestamp(unit, Some("UTC"))`
whose ticks are the start of the local bucket in the effective zone (a universal
UTC instant). Fields v1: `second, minute, hour, day, month, year` (`week`/
`quarter` deferred — week-start/ISO and quarter calendars are separate policy).
**Reuses 150's machinery verbatim** (`utc_datetime_from_ticks` → `.with_timezone`
→ truncate civil fields → `from_local_datetime` DST policy [gap→execution error,
fold→earlier instant, `timestamp.rs:77`] → `ticks_from_utc_datetime`), so it
cannot diverge from 150's DST decisions. **Not** `TimeBucket` floor-division.
Date32 `date_trunc` deferred.

**D7 — Output restamp retarget (`restamp_sql_result_timezones`, `daemon.rs:1473`,
called `:1135`; UTC fast-path `:979`).** Restamp only arrays whose zone is
`"UTC"`; preserve an explicit non-UTC zone from a top-level `AT TIME ZONE`
projection. Naive metadata still never constructed. A bare `date_trunc` result is
UTC and renders in the session zone unless wrapped in `AT TIME ZONE`.

**D8 — `TIME_BUCKET` stays fenced.** The experimental Int64-ms path is not
promoted to timestamp semantics; architecture-guarded experimental.

**D9 — Optimizer/PBC unchanged.** No new hard facts, `EmptyRelation`, range/NDV
spending; PBC **declines** all three new nodes (Toron-executed). Non-UTC
expression metadata is a render/operation annotation, never a column-data-type
authority — 150's `range_scalar_for_fact` UTC-**column** fence intact.

**D10 — Deferrals.** Interval literals + `ts ± interval` arithmetic (needs an
`Interval` scalar — the genuine boundary), `SET TIME ZONE`/REPL session, dynamic
zone expressions, fixed-offset shorthand, Date64 role, decimal, KLL/Count-Min/
heavy-hitter facts, hard-zero/snapshot.

## Headline gate

`extract_hour_from_timestamp_at_time_zone_uses_named_zone` — session
`America/New_York`, `EXTRACT(HOUR FROM event_time AT TIME ZONE 'America/Chicago')`
over a UTC `Timestamp` returns the Chicago hour, independent of the session zone.
Fails on `main` (`SqlExpr::AtTimeZone`/`Extract` unlowered).

## Supporting gates

- `rune_date_trunc_day_groups_event_time_in_session_zone` — Rune-first
  ([[rune_is_primary_interface]]; shared target is `BoundExprNode`);
- `extract_without_at_time_zone_uses_request_session_timezone`;
- `extract_utc_column_without_at_time_zone_uses_session_zone` — **D3 structural**;
- `extract_at_time_zone_utc_uses_utc_not_session_zone` — **D3 structural (other
  side)**;
- `date_part_alias_matches_extract`;
- `date_trunc_day_returns_utc_instant_for_session_local_midnight`;
- `date_trunc_day_at_time_zone_returns_utc_instant_for_local_midnight`;
- `date_trunc_day_across_dst_reuses_adr150_gap_fold_policy` — **fails if
  date_trunc is implemented as TimeBucket floor-division**;
- `at_time_zone_projection_survives_session_restamp` (D7);
- `at_time_zone_rejects_dynamic_or_invalid_zone`;
- `at_time_zone_rejects_date32_operand`;
- `date32_extract_is_civil_and_zone_free`;
- `date64_temporal_ops_remain_unsupported`;
- `timestamp_temporal_ops_do_not_write_hard_bounds_or_empty_relation`;
- `timestamp_range_facts_still_spend_only_utc_column_data_type` (150 intact);
- `legacy_time_bucket_int64_path_not_promoted_to_timestamp_semantics` (D8);
- `no_interval_arithmetic_in_151`, `no_set_time_zone_session_in_151`,
  `date64_remains_dark`;
- architecture: every new variant covered by the structural visitors / bound
  walkers / ordinal rewrite / serialization / exec dispatch.

## Consensus trail

3-of-3 on topic, scope (`EXTRACT`+`date_part`+`date_trunc`+`AT TIME ZONE`), the
zone-resolution rule, `date_trunc`≠`TimeBucket`, return types, field subsets,
restamp retarget, and all fences. Gemini supplied `AT TIME ZONE` from iter-0;
Codex supplied the non-naive UTC-instant fence, the restamp catch, and the
bound-walker grounding; Claude withdrew an iter-0 `AT TIME ZONE` defer after
grounding it is the per-operand zone selector, grounded `date_trunc`≠`TimeBucket`
(DST/month/year), and sharpened zone detection from type-directed to **structural**
(D3), adopted by Codex.

**Surfaced divergence (2-of-3, resolved on grounding):** the effective-zone
*resolution mechanism*. Claude + Codex resolve at **bind time** and bake the zone
into the bound node (D2), grounded by the **session-stateless executor**
(`evaluate_bound_program_without_storage` has no `PlannerConfig`). Gemini's iter-2
*impl* instead carried no zone on the bound node and injected `session_timezone`
at exec via an "if the input array lacks a timezone" check — **grounded-against
twice**: exec has no session zone to read, and the type-directed check is exactly
the D3 hole (a plain UTC column *has* `Some("UTC")`, so the session zone would
never inject → wrong hour). Gemini's *ADR prose* states the correct rule (explicit
else `PlannerConfig.session_timezone`); only the impl mechanism diverged.
Promoted: bind-time, structural.

## Forward arc

`150 Timestamp+session-tz → 151 session-zoned temporal ops → (measured-miss →
skew/quantile estimate quality) · (intervals → SET/REPL client) → Theta join-key
intersection → join selectivity → reordering`.
