# Impl Plan — ADR 150: Timezone-Aware Timestamp Selectivity + Per-Request Session Timezone

**Migration for** `plans/adr/0150_timezone_aware_timestamp_selectivity.md`. **Toron-only** (scalar
+ SQL/Rune + binder + exec + optimizer consumer + SQL-wire request field + CLI). **No Wendelon
change** — ms/us/ns timestamp + Date64 tags already produced (`storage/stats.rs:400–408`).
Anchors verified against `main`. Largest cut in the 141→149 arc; the engine half mirrors 149,
the session-zone half is new plumbing.

## Surfaces
| Concern | Anchor |
|---|---|
| Scalar | `ir.rs` `ScalarValue` → `Timestamp { value: Option<i64>, unit: TimeUnit }`; `data_type()` |
| Parse/convert helper | new `timestamp.rs` (mirror `date32.rs`): `parse_rfc3339_to_utc`, `wall_clock_in_zone_to_utc`, `format_instant_in_zone` |
| SQL literal typing | `sql_planner.rs` `lower_expr` — `SqlExpr::TypedString` + `TimezoneInfo` (0.62 `{None,WithoutTimeZone,WithTimeZone,Tz}`) |
| Bare-string coercion | `binder.rs:7687` `coerce_literal_value` (add `(Utf8, Timestamp(_,UTC))` → session-zone parse) |
| DDL carrier | `sql_planner.rs:1206` `lower_create_table_data_type` — `Timestamp WITH TIME ZONE`/`TIMESTAMPTZ` → `Physical(Timestamp(ns, Some("UTC")))` |
| Exec arms | `exec.rs` literal-array / downcasts / group / partition-key / `system_table_array` (mirror Date32 sites `:869/1732/1819/3452/3692/4308`) |
| Write/CTAS | `coordinator/write.rs` `build_array` (mirror Date32) |
| Fact consumer | `binder.rs:4478` `decode_range_scalar` (3 ts tags); `range_scalar_for_fact` **+ column DataType param** (149 precedent) |
| Session zone — wire | `sql_wire.rs:59` `ExecuteRequest` → add `session_timezone: String` |
| Session zone — config | `binder.rs:71` `PlannerConfig` → add `session_timezone` (4th field; default `"UTC"`) |
| **Coordinator override (the seam)** | `coordinator.rs:655` — clone-and-stamp pattern (139/140 precedent); stamp `planner_config.session_timezone` from the request before the bind loop `:661` |
| Client | `cli.rs:400 run_sql` / `:538 run_sql_async` — `--timezone` flag + `iana-time-zone` detect |
| Rendering | `daemon.rs` result IPC encode path + `cli.rs:668 format_batches_for_display`; output-column tz-restamp |
| PBC | `pbc.rs:219 emit_literal` (decline) |
| Producer (unchanged) | `storage/stats.rs:400–406` ms/us/ns min/max already emitted |

## Dependencies (D11)
- `chrono-tz` — **direct** `crates/toron/Cargo.toml` dep (coordinator IANA → offset, DST-aware).
- `iana-time-zone` — **direct** dep (CLI client local-zone detection).
- arrow **`chrono-tz` feature** ON for the workspace `arrow` dependency (it enables
  `arrow-array/chrono-tz`; `arrow-cast` keeps its existing `prettyprint` feature) — currently
  off; required for named-zone *rendering*. Fixed-offset works without it, named zones do not.

## Phase 0 — failing gates first
Stage headline + supporting gates (adr). They fail on `main` for the coarse "no Timestamp"
reason; after impl they must fail for the *named* fence (naive-rejected, zone-pinned, etc.).

## Phase 1 — scalar + helpers
- `ScalarValue::Timestamp { value: Option<i64>, unit: TimeUnit }`; `data_type()` →
  `Timestamp(unit, Some("UTC".into()))`; typed-null; structural eq/hash (value+unit); literal
  array; `Display` → UTC ISO with `Z` (session-zone rendering is the *output path*, D7, not
  `Display`). **Every exhaustive match** (compile + `--all-targets`,
  [[feedback_verify_all_targets_on_new_ir_field]]).
- `timestamp.rs`: `wall_clock_in_zone_to_utc(&str, &Tz, TimeUnit) -> Result<i64>` (DST
  `LocalResult`: gap→Err, fold→earliest); `parse_rfc3339_to_utc(&str, TimeUnit) -> Result<i64>`
  (explicit offset); `format_instant_in_zone(i64, TimeUnit, &Tz) -> String`. Unit-exact;
  precision-loss → Err. Unit-test DST gap/fold edges + the three units.

## Phase 2 — session-zone plumbing (before literals — they depend on it)
- `ExecuteRequest.session_timezone: String`; `PlannerConfig.session_timezone: String`
  (default `"UTC"`).
- **The seam:** at the daemon SQL-wire request entry, plumb `session_timezone` into a
  per-request coordinator planner config. Use that effective config for **every bind in the
  request path**: catalog DDL, INSERT/write branches, and the read bind loop. The read loop can
  still reuse the `coordinator.rs:655` clone-and-stamp replay pattern; broadcast replay mutates
  the same effective request config rather than falling back to stored `self.config`.
- Non-SQL/internal coordinator entrypoints keep the `"UTC"` default unless they opt into an
  explicit planner-config override.
- Resolve the zone string → `chrono_tz::Tz` once at bind entry; invalid name → `BindingError`.

## Phase 3 — literals + coercion (D3/D4)
- `lower_expr` `SqlExpr::TypedString`: `TimezoneInfo::{WithTimeZone,Tz}` → `parse_rfc3339_to_utc`;
  `TimezoneInfo::{None,WithoutTimeZone}` → **reject** (`FrontendError`).
- `coerce_literal_value`: `(Utf8(Some(s)), Timestamp(unit, Some("UTC")))` →
  `wall_clock_in_zone_to_utc(s, session_zone, unit)`; `Err` → `BindingError`. **Literal-only +
  target-gated.** Thread the resolved `Tz` into the coercion context. Rune `lit_timestamp`
  (explicit offset only — programmatic, no ambient session zone).
- Pin: naive-typed-reject, offset-normalize, bare-string-in-zone (CT≠ET), precision-loss-error,
  zone-pinned-not-ambient.

## Phase 4 — DDL carrier + exec (D7)
- `lower_create_table_data_type`: `Timestamp WITH TIME ZONE`/`TIMESTAMPTZ` →
  `Physical(Timestamp(Nanosecond, Some("UTC")))`; bare `TIMESTAMP`/`WITHOUT TIME ZONE` → reject
  (naive column = type error). INSERT/CTAS materialize; info_schema → `TIMESTAMP WITH TIME ZONE`.
- exec Timestamp arms: literal-array (`Timestamp*Array::from(...).with_timezone("UTC")`),
  downcasts, projection, group/partition-key (distinct tag), write/system-table builders.
  **Comparison:** Arrow cmp kernels on matching `Timestamp*Array` — add an arm only if the
  compare path is hand-typed (149 showed per-type dispatch; verify).

## Phase 5 — selectivity consumer + PBC (D5/D6/D8)
- `decode_range_scalar`: `ARROW_STATS_TAG_TIMESTAMP_{MS,US,NS}` (8-byte signed) →
  `RangeScalar::Integral`. `range_scalar_for_fact(range, value, column_data_type)` — **add the
  column DataType param** (tz authority); spend only on unit + `Some("UTC")` column +
  matching-unit-UTC literal. Equality (141) / IN-list (148) timestamp literal keys
  (`InListLiteralKey::Timestamp { unit, value }`). Estimate-channel only (assert no hard bounds).
- `pbc.rs emit_literal`: `Timestamp(Some(_))` → decline (`unit/timezone-preserving PBC support`).

## Phase 6 — client (D9 — per-invocation, no REPL/SET)
- `iana-time-zone::get_timezone()` at `run_sql` startup → default `session_timezone` (UTC
  fallback). `--timezone <IANA>` flag overrides. Stamp every `Execute`. **No `SET TIME ZONE`,
  no REPL** (one-shot client; deferred to a client-surface ADR).

## Phase 7 — output rendering (D7)
- Enable workspace `arrow/chrono-tz`. The SQL edge/daemon restamps output Timestamp columns' tz
  metadata to the session zone (same UTC i64) before result IPC; client
  `format_batches_for_display` renders in-zone. Apply the restamp before both the audit
  fail-closed pre-encode path and the normal encode path, so the bytes audited and the bytes sent
  agree. Pin `timestamp_output_renders_in_session_zone_with_offset` on the render surface (149
  F1 lesson) — **named-zone render test, not just compile** (the feature can be silently off).

## Phase 8 — architecture guard + verification
- `tests/architecture/temporal.rs`: `adr150_*` guard (Timestamp tz-mandatory; naive rejected;
  PBC declines; column-DataType fact fence; `session_timezone` on ExecuteRequest+PlannerConfig +
  the coordinator stamp; Date32-still-civil; Date64-still-dark; no Wendelon producer touched).
```sh
scripts/cargo-cgroup test -p toron timestamp
scripts/cargo-cgroup test -p toron --test architecture_test adr150
scripts/cargo-cgroup test -p toron --test architecture_test adr144 adr148 adr149   # consumers + civil-date intact
scripts/cargo-cgroup check -p toron --all-targets
git diff --check
```

## Risk register
- **`ScalarValue::Timestamp` exhaustiveness** — widest match surface yet; `--all-targets` forces
  every arm (bench/DML/PBC/system-table).
- **The coordinator seam** — a field on `ExecuteRequest` that never reaches the binder is inert;
  `execute_request_session_timezone_reaches_binder` must prove the override fires, not the stored
  default. (Same lesson as 149's `range_scalar_for_fact` plumbing.)
- **Zone pinned, never ambient** — resolve once from the request; no `chrono::Local` at bind.
- **arrow `chrono-tz` feature** — named-zone rendering silently degrades to fixed-offset/error
  if the feature isn't truly enabled; verify with a named-zone render test.
- **DST gap/fold** — `LocalResult` handled explicitly (gap→Err, fold→earliest); an unwrap panics
  on real DST input.
- **Precision-loss** — ns→ms/us coercion rejects, never rounds.
- **Date32/Date64 fail-open** — Date32/Date64 tags must NOT spend timestamp facts (reject
  Gemini's cross-spend); pin `date32_stays_civil` + `date64_fact_remains_dark`.
