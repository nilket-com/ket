# ADR 150: Timezone-Aware Timestamp Selectivity + Per-Request Session Timezone

**Status:** Promoted. **Operator-approved Claude/Codex convergence; Gemini dissent recorded.**
Event-time range filters (`WHERE event_time >= '2026-01-01'`) are the #1 analytical predicate
alongside dates, but they are **dark** in Toron: Wendelon already maintains Timestamp min/max
range facts for all three units (`ARROW_STATS_TAG_TIMESTAMP_{MILLISECOND,MICROSECOND,
NANOSECOND}`, `storage/stats.rs:400–406`), yet Toron's `ScalarValue` cannot express a
`Timestamp`, so timestamp predicates can't bind, execute, or reach the range-fact consumer.
ADR 150 makes **`Timestamp` a first-class UTC-instant Toron scalar** — the producer and 144's
integral range math are already done, so the engine half is a **scalar-foundation cut** (literal,
comparison, execution, display) mirroring 149's Date32. Folded in (operator's "one big cut"): a
**per-request session timezone** so the #1 bare-string event-time filter interprets in the
user's zone. **Toron-only; estimate-channel only; no Wendelon change.**

The operator's doctrine — *"a date without a timezone is a fundamental type error"* — is
sharpened to its precise target: the error is the **naive instant** (`Timestamp(_, None)`, a
wall-clock claiming to be absolute without a zone). A civil **date** (`Date32`, ADR 149) is a
*different category* — a calendar coordinate, legitimately zone-free ("born 1990-05-15" is the
same date everywhere). 150 keeps the **two-category model** (civil date vs. instant) and does
**not** reopen 149.

**Placement:** Toron scalar + SQL/Rune frontend + binder + executor + optimizer consumer +
SQL-wire request field + CLI client. No Wendelon producer/schema change.
**Predecessors:** 144 (range facts — timestamp tags produced but dark), 141 (equality NDV), 147
(typed `RangeScalar` + float range), 148 (IN-list), 149 (Date32 scalar foundation + the
`coerce_literal_value` target-gated coercion + the `range_scalar_for_fact` column-DataType
plumbing this cut reuses), 139/140 (the per-request `PlannerConfig` clone-and-stamp precedent
this cut's session-zone override follows), [[rune_is_primary_interface]].
**Migration:** `plans/impl/0150_timezone_aware_timestamp_selectivity.md`.

## Decision

**D1 — `ScalarValue::Timestamp { value: Option<i64>, unit: TimeUnit }`** — always a
UTC-normalized instant (the i64 is UTC ticks); `data_type()` → `Timestamp(unit, Some("UTC"))`.
Units **millisecond / microsecond / nanosecond** (all three produced). Naive
`Timestamp(_, None)` is **never constructed**. Update typed-null, structural eq/hash (value +
unit), literal arrays, result/system-row + write materialization, and **every exhaustive scalar
match** (compile-driven, verified `--all-targets`).

**D2 — Per-request session timezone (pinned, never ambient).** `session_timezone: String` (IANA
name, e.g. `America/Chicago`) rides on `ExecuteRequest` and the effective `PlannerConfig` for
that request. The coordinator stays **stateless**: the daemon plumbs the request zone into a
**per-request `PlannerConfig` override** before binding — the same clone-and-stamp shape the
139/140 broadcast-replay path already uses (`coordinator.rs:655`). The binder resolves the zone
to a `chrono_tz::Tz` **once per request** (invalid name → bind error); **no binder code reads
ambient OS-local timezone**. Defaults: CLI auto-detects the client's local zone once
(`iana-time-zone`, UTC fallback); daemon/non-CLI callers default `"UTC"`.

**D3 — Literal surface (three classes — bounded-lenient, the resolved divergence).**
(a) Explicit naive typed `TIMESTAMP '…'` (SQL `TimezoneInfo::None`/`WithoutTimeZone`) →
**reject** (the user named the naive type). (b) Explicit-offset / `TIMESTAMPTZ` / `WITH TIME
ZONE` → **accept, normalize to UTC ticks**. (c) A bare `Utf8` string, **target-gated** against a
supported `Timestamp(unit, Some("UTC"))` column → **interpret the wall-clock in the request
session zone → UTC instant** (149's exact `coerce_literal_value` target-gated, literal-only
mechanism — the zone is *declared*, never guessed); no timestamp target → stays `Utf8`/rejects.
Conversion to the target unit must be **exact** — a ns instant not representable in a ms/us
column is a **bind error**, never a rounded predicate.

> **Why bounded-lenient (grounded convergence):** the #1 event-time filter is `WHERE event_time >=
> '2026-01-01'` (bare string), so strict-offset-only makes the headline barely fire — the same
> tension ADR 149 resolved lenient. Codex iter-2 conceded bare-string-lenient; the fold then
> made the *session zone* the anchor, so lenient no longer "guesses UTC" (Codex's doctrine
> objection) — it resolves the literal in the declared session zone, exactly as type-directed
> coercion resolves any untyped literal from its target.

**D4 — DST policy.** Resolving a bare wall-clock in the session zone via `chrono-tz`
(`from_local_datetime` → `LocalResult`): **gap** (spring-forward, nonexistent) → **bind error**;
**fold** (fall-back, ambiguous) → **pick the earlier (pre-transition) instant**, documented;
**invalid zone name** → bind error before execution. Deterministic — the request carries the
zone, resolved once.

**D5 — Fact-spending uses the column `DataType` as tz authority.** `RangeFact` carries
tag/count/min/max but **no timezone**, and the producer tag `Timestamp(unit, _)` is tz-agnostic.
`range_scalar_for_fact` takes the bound column `DataType` and spends a timestamp fact only when:
tag-unit matches the column unit, the column is `Timestamp(unit, Some("UTC"))`, the literal is
the matching-unit UTC scalar, and min/max decode as signed 8-byte integral. `decode_range_scalar`
handles the three timestamp tags → `RangeScalar::Integral`. Naive/non-UTC columns, unit
mismatch, `Date32`/`Date64` tags, and raw `Int64` literals all **fail open**.

**D6 — Reuse 144/141/148; estimate-channel only.** Range → 144 discrete integral interpolation;
equality → 141 NDV; IN-list → 148 NDV with timestamp literal keys. Write **only**
`estimated_row_count` — never `row_upper_bound`/`byte_upper_bound`/`EmptyRelation`; 139/140
replay is the correctness net; 141 `min` combination.

**D7 — Execution + output rendering.** Timestamp literals/columns materialize as Arrow
`Timestamp*Array` (UTC metadata); equality/ordering via Arrow kernels once both operands are
typed timestamp arrays. **Output renders in the session zone:** the SQL edge/daemon restamps
output Timestamp columns' Arrow tz metadata to the request IANA zone (same UTC i64) before result IPC,
so the client `format_batches_for_display` renders in-zone — **requires arrow's `chrono-tz`
feature** (currently off; named-zone rendering is unproven without it). ISO-8601 with offset;
pin the CLI render surface (149 F1 lesson — assert `format_batches_for_display`, not `Display`).

**D8 — PBC declines.** A Timestamp predicate enters PBC pushdown only with proven
unit/tz-preserving evidence; v1 **declines** (filter runs in Toron) — **never** an untyped
`OP_PUSH_INT` that erases unit/tz. Mirrors 149's Date32 decline.

**D9 — Client: per-invocation, no `SET`/REPL.** `toron sql --timezone <IANA> <statement>`; no
flag → auto-detect client local zone (UTC fallback); every `Execute` stamps `session_timezone`.
**`SET TIME ZONE` and a persistent REPL session are out of scope** — in today's one-shot client
(`cli.rs:400 run_sql`) client-side `SET` is a no-op after process exit; a persistent interactive
session is a separate client-surface ADR.

**D10 — Date32 stays civil; Date64 deferred.** 149 not reopened (new fence: Date32 is not an
event-time day boundary — deriving a date from a timestamp needs a tz/calendar policy, out of
scope). `ARROW_STATS_TAG_DATE64` stays dark (civil = redundant with Date32; instant = category
error). **Rejects Gemini's unified-instant model** (Date32-as-session-midnight-instant), which
would make a civil date mean a different instant per session and cross-spend Date32⋈Timestamp
facts through an implicit calendar conversion.

**D11 — Dependencies.** `chrono-tz` (coordinator IANA resolution) and `iana-time-zone` (client
local detection) as **direct `crates/toron/Cargo.toml` deps** (not leaned through Wendelon's
graph) + arrow's **`chrono-tz` feature** (named-zone rendering).

## Headline gate
`timestamp_session_zone_range_filtered_dim_infers_broadcast_by_estimate` — a UTC
`Timestamp(ns)` dim with live min/max; a **Central-session** bare-string filter
(`WHERE event_time >= '2026-01-01'`, interpreted as `America/Chicago` → UTC instant) derives a
smaller `estimated_row_count` via integral interpolation → `BroadcastEstimated`. **Fails on
`main`** (no Timestamp scalar/session zone).

## Supporting gates
`execute_request_carries_session_timezone`;
`execute_request_session_timezone_reaches_binder` (a request zone binds a bare literal as that
zone, not the stored `self.config` default — the coordinator-override fence);
`cli_sql_timezone_flag_stamps_execute_request`;
`cli_sql_auto_detects_timezone_with_utc_fallback`;
`bare_string_timestamp_interpreted_in_session_zone` (CT vs ET give different instants — proves
the zone is load-bearing);
`timestamp_literal_zone_is_pinned_not_ambient`;
`explicit_offset_timestamp_literal_normalizes_to_utc`;
`explicit_naive_typed_timestamp_literal_rejected`;
`dst_gap_wall_clock_is_bind_error`; `dst_fold_picks_earlier_instant`;
`timestamp_unit_mismatch_fails_open_for_range_fact`;
`timestamp_naive_or_non_utc_column_fails_open`;
`timestamp_precision_loss_in_target_unit_is_bind_error`;
`timestamp_in_list_uses_existing_ndv_fact_and_writes_no_hard_bounds`;
`timestamp_output_renders_in_session_zone_with_offset` (CLI render surface);
`timestamp_pbc_declines_without_type_preserving_opcode`;
`date64_fact_remains_dark`; `date32_stays_civil_not_timestamp_day_boundary`.
Architecture: `adr150_timestamp_is_tz_anchored_and_estimate_only`; no Wendelon producer touched.

## Forward arc
`144 range → 147 float → 148 IN-list → 149 Date32 → **150 Timestamp + session timezone** →
(Date64/decimal breadth · persistent-session/`SET TIME ZONE` client ADR · `AT TIME ZONE`/temporal
arithmetic) → measured-skew sketches → Theta join-key intersection → join selectivity →
reordering`.

## Consensus trail
Folded from two threads on the operator's "one big cut": the **instant-selectivity engine**
(149 mirror — Claude+Codex converged early) and the **session timezone UX**. The operator's tz
steer was sharpened to *the naive instant is the error, the civil date is not* (two-category
model), settling the instants-only question. The literal-coercion divergence (Codex strict vs
Claude lenient) resolved bounded-lenient once the session zone became the anchor (Codex conceded
iter-2). Scope: Codex iter-2 UTC-only **superseded** by his iter-3 named-zone acceptance; client
shape narrowed to **per-invocation** (REPL/`SET` deferred — Codex+operator+Claude). The
coordinator **per-request `PlannerConfig` override** seam (Codex's catch) was grounded against
the landed 139/140 clone-and-stamp precedent (`coordinator.rs:655`). Grounding correction along
the way: ms/us/ns are *all* produced (Claude's iter-0 "ns-only" miss, conceded to Codex).
**Gemini's unified-instant model rejected** (reopens 149; civil date ≠ instant).
Operator-approved Claude/Codex convergence, with Gemini's dissent recorded rather than papered over.
