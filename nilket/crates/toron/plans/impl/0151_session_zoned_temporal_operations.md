# Impl Plan — ADR 151: Session-Zoned Temporal Operations

Pairs with `plans/adr/0151_session_zoned_temporal_operations.md`. **Toron-only**:
SQL frontend, Rune, IR, binder, executor, daemon restamp, PBC decline, tests. No
Wendelon/fact-schema/wire change. Recipe: **follow `TimeBucket`** — every site
below was grounded by tracing that node. All anchors verified to exist on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Logical IR | `ir.rs:527` `Expr`; pushdown-eligibility `ir.rs:602` |
| Bound IR | `ir/execution.rs:78` `BoundExprNode`; builder `:523`; `node_nullable` `:712`; `pretty_node` `:862`; children `:878` |
| Expr→bound bind | `binder.rs:6842` `bind_expr_into_program` (`Expr::TimeBucket` arm `:6945`) |
| Session zone (bind) | `binder.rs:97` `PlannerConfig::resolved_session_timezone`; bind ctx `session_timezone: Tz` `:506` |
| Bound walkers | `clone_bound_subgraph:3443`, `bound_node_children:3725`, `bound_subgraphs_structurally_equal*` (~`:3680`), `collect_program_node_columns:6540`, `rewrite_program_ordinals:6580`, `offset_bound_node:6721` |
| SQL lowering | `sql_planner.rs` `lower_expr` (`SqlExpr::IsNull` ~`:4104`); function arm `:4107`; validators `:4529`, `:5013` |
| Bound execution | `exec.rs` `evaluate_bound_program_without_storage` (bound arm region `:1182`); unbound `evaluate_expr` `:1071`; `evaluate_time_bucket:2118` |
| Timestamp helpers | `timestamp.rs` (`utc_datetime_from_ticks:220`, `ticks_from_utc_datetime:176`, `parse_wall_clock_timestamp:77` DST policy, `parse_session_timezone:54`) |
| Date32 helpers | `date32.rs` |
| Result restamp | `daemon.rs:1473` `restamp_sql_result_timezones` (called `:1135`; UTC fast-path `:979`) |
| Rune | `rune_engine.rs` (`time_bucket:624`, register `:940`) |
| PBC | `pbc.rs` (decline arms `:143`, `:198`) |
| Arch tests | `tests/architecture/temporal.rs` (new), `sql_planner_surface.rs` |

## Phase 0 — Failing gates first

Headline `extract_hour_from_timestamp_at_time_zone_uses_named_zone`; the two D3
structural gates; `date_trunc` day/hour with session + explicit-zone override;
the DST non-floor-division regression; the restamp-survival render test; the
Rune-first gate; the deferral/fence guards.

## Phase 1 — Typed field model

Crate-local `TemporalExtractField` + `TemporalTruncField` (in `ir.rs` or a small
temporal module). Parsers from sqlparser `DateTimeField` (`EXTRACT`), string
literals (`date_part`/`date_trunc`, Rune). Reject unsupported fields with named
errors (keep `isodow`/`epoch`/`timezone*`/custom, `week`/`quarter` trunc, Date64
out).

## Phase 2 — `Expr` (ir.rs)

- `:527` add `AtTimeZone(Box<Expr>, String)`, `Extract(TemporalExtractField,
  Box<Expr>)`, `DateTrunc(TemporalTruncField, Box<Expr>)`. (`date_part` lowers to
  `Expr::Extract`; no separate variant.)
- `:602` `is_pbc_pushdown_eligible` → add all three to the `=> false` arm (D9).
- The two recurse-into-child arms beside `TimeBucket` (`binder.rs:1700`
  `reject_storage_synthetic_returning_reference`, `:1760`
  `collect_logical_cast_names`).

## Phase 3 — `BoundExprNode` (ir/execution.rs) + bind (binder.rs)

Bound variants **carry the resolved zone** (D2):
`AtTimeZone { input, zone }`, `Extract { field, zone: Option<String>, input }`,
`DateTrunc { field, zone, input }`. `Extract` uses `Some(zone)` only for
Timestamp input; Date32 extraction is civil and uses `None`.

Thread through (each has a `TimeBucket` arm today):
- `:523` builder `append_*`; `:712` `node_nullable` (= input nullability);
  `:862` `pretty_node`; `:878` children;
- `clone_bound_subgraph:3443`, `bound_node_children:3725`,
  `bound_subgraphs_structurally_equal*` (compare field+zone option, recurse input),
  `collect_program_node_columns:6540`, `rewrite_program_ordinals:6580`,
  `offset_bound_node:6721`;
- `sql_planner.rs` structural hash/eq visitors + `sql_planner_surface.rs` guard.

**Bind (`bind_expr_into_program:6842`)** — the heart (D2/D3):
```
Expr::Extract(field, inner) | Expr::DateTrunc(field, inner) => {
    let (zone, op_expr, explicit_zone) = match inner.as_ref() {
        Expr::AtTimeZone(ts, z) => (validate_zone(z)?, ts.as_ref(), true),
        _                       => (ctx.session_timezone.to_string(), inner.as_ref(), false),
    };                         // NEVER read inner's DataType zone tag (D3)
    let input = bind(op_expr)?;
    match (op_kind, input.data_type, explicit_zone) {
        (Extract, Date32, false) => builder.append_extract(field, None, input, DataType::Int32),
        (Extract, Date32, true)  => error("AT TIME ZONE requires Timestamp"),
        (DateTrunc, Date32, true)  => error("AT TIME ZONE requires Timestamp"),
        (DateTrunc, Date32, false) => error("Date32 date_trunc deferred in 151"),
        (Extract, Timestamp(unit, _), _) =>
            builder.append_extract(field, Some(zone), input, DataType::Int32),
        (DateTrunc, Timestamp(unit, _), _) =>
            builder.append_date_trunc(field, zone, input, Timestamp(unit, Some("UTC"))),
        _ => error("unsupported temporal operand"),
    }
}
Expr::AtTimeZone(ts, z) => {               // standalone projection (D4 role b)
    let input = bind(ts)?;                 // require supported Timestamp unit
    builder.append_at_time_zone(validate_zone(z)?, input, Timestamp(unit, Some(z)))
}
```
Guardrails: never construct `Timestamp(_, None)`; non-UTC expr metadata never
reaches `range_scalar_for_fact`; Date32 extraction zone-free and rejects an
explicit `AT TIME ZONE` wrapper; Date64 rejected.

## Phase 4 — SQL lowering (sql_planner.rs)

- New top-level `lower_expr` arms (near `:4104`): `SqlExpr::Extract` (map field,
  reject unsupported) → `Expr::Extract`; `SqlExpr::AtTimeZone` (require
  string-literal zone) → `Expr::AtTimeZone`.
- In the `SqlExpr::Function` arm (`:4107`, after `TIME_BUCKET`, before
  `None => unsupported`): `DATE_TRUNC`/`DATE_PART` name arms (2 args,
  string-literal field) → `Expr::DateTrunc` / `Expr::Extract`.
- `:4529`/`:5013` validators: add the new variants to the recurse-into-child
  arms (GROUP BY/ORDER BY over a temporal op). No generic dispatcher.

## Phase 5 — Executor (exec.rs) — reads baked zone, no session state

Bound arms (`:1182` region):
- `AtTimeZone { input, zone }`: downcast `Timestamp{Ms,Us,Ns}Array`,
  `.with_timezone(zone)` (ticks/nulls unchanged).
- `Extract { field, zone: Some(zone), input }` over Timestamp: **per-element via a
  `timestamp.rs` helper with the explicit baked zone** (preferred — avoids
  selecting the zone from array metadata) → `Int32Array`. Restamp-then-`date_part`
  on the baked zone is an equivalent acceptable variant.
- `Extract { field, zone: None, input }` over Date32: `date32.rs` civil helper or
  Arrow `date_part`.
- `DateTrunc { field, zone, input }`: per-element `timestamp_trunc` (Phase 6),
  build a `Timestamp(unit, "UTC")` array.

Unbound `evaluate_expr:1071`: add arms **only if** Rune direct-eval routes
through it (trace the Rune test path); else rely on the bound path.
`evaluate_time_bucket:2118` untouched (D8).

## Phase 6 — Calendar helpers (timestamp.rs / date32.rs) — reuse 150

`pub(crate) fn timestamp_trunc(value: i64, unit, zone: Tz, field) -> Result<i64>`:
1. `utc_datetime_from_ticks(value, unit)` `:220`;
2. `.with_timezone(&zone)`;
3. zero lower civil fields per `field` (`second`…`year`);
4. `zone.from_local_datetime(naive)` — 150 policy: `Single` ok / `Ambiguous`→
   earlier / `None`→execution error (mirror `parse_wall_clock_timestamp:77`);
5. `ticks_from_utc_datetime(.., unit)` `:176`.

`pub(crate) fn timestamp_extract(value: i64, unit, zone: Tz, field) ->
Result<i32>`: steps 1–2 then read the civil field. Date32 civil extractor in
`date32.rs` (zone-free).

## Phase 7 — Daemon restamp retarget (daemon.rs:1473)

Per-array: `Some("UTC")` → restamp to session zone; `Some(non_utc)` → preserve;
`None` → must not occur (guard, don't create). Pin
`at_time_zone_projection_survives_session_restamp` on the **render** surface.

## Phase 8 — Rune (rune_engine.rs)

Mirror `time_bucket:624`/`:940`: `extract(field, expr)`, `date_part(field, expr)`,
`date_trunc(field, expr)`, `at_time_zone(expr, zone)` → same `Expr::*` → same
bound target. Pin `rune_date_trunc_day_groups_event_time_in_session_zone`.

## Phase 9 — PBC decline (pbc.rs)

Decline all three in `compile_node` (`:143`) + the `emit_node` unsupported arm
(`:198`). No new opcodes.

## Phase 10 — Architecture guards (tests/architecture/temporal.rs, adr151_*)

`SqlExpr::AtTimeZone`/`Extract` lowered; `date_part`/`date_trunc` lower via the
function arm; timestamp bound temporal nodes carry zone strings while Date32
`Extract` carries `None`; plain-UTC-col→session-zone; explicit
`AT TIME ZONE 'UTC'`→UTC; restamp preserves non-UTC; `TIME_BUCKET` experimental;
`AT TIME ZONE` rejects Date32; `range_scalar_for_fact` UTC-column fence; no
`SET`/interval; Date64 dark. Update `sql_planner_surface.rs` for the new `Expr`
variants.

## Verification

```sh
scripts/cargo-cgroup test -p toron temporal
scripts/cargo-cgroup test -p toron --test architecture_test adr151
scripts/cargo-cgroup test -p toron --test architecture_test sql_planner_surface
scripts/cargo-cgroup check -p toron --all-targets   # MANDATORY — new Expr/Bound
                                                    # variants trip E0004 in
                                                    # benches/examples cargo test skips
git diff --check
```
(Architecture target stem is `architecture_test`, not `architecture`.)

## Risks

- **Forgetting a bound walker** (clone/eq/children/ordinal/offset) → aggregate
  arg handling or boolean rewrite breaks silently. The Phase-3 list is exhaustive
  vs `TimeBucket`.
- **Restamp erasure** — failing Phase 7 makes `AT TIME ZONE` correct internally
  but render wrong.
- **Exec has no config** — zones MUST be baked at bind (D2).
- **TimeBucket trap** — floor-division is wrong across DST/month/year (D6).
- **Type-directed zone leak** — `Some("UTC")` is ordinary column metadata, not a
  signal to use UTC; only explicit `AtTimeZone` selects a non-session lens (D3).
- **Field aliases** — `dow`/`isodow`/`week`/`isoweek` off-by-one/policy: reject
  or pin explicitly.

## Deferred (named, not dropped)

`week`/`quarter` `date_trunc`; Date32 `date_trunc`; interval arithmetic;
`SET TIME ZONE`/REPL; dynamic/offset zones; Date64 role.
