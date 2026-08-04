# Toron ADR 228: Rune Optimizer Readout Handles

**Status:** Promoted. **Migration:** `plans/impl/0228_rune_optimizer_readout_handles.md`.
**Category:** Toron Rune frontend ergonomics — finite, lazy Rune handles over the existing
`information_schema.optimizer_*` readout tables. No optimizer policy, evidence recording,
durable key, system table, `SystemTableKind`, SQL grammar, executor, fragmenter, wire,
storage, daemon, or Wendelon change. Pure alias: user query plans byte-identical.

## Why this cut exists

The optimizer evidence arc (216→227) made the engine deeply inspectable via seven
`information_schema` readouts: `optimizer_decisions`, `optimizer_runtime_verdicts`,
`optimizer_evidence_status`, `optimizer_margin_advisory`, `optimizer_receipt_gates`,
`optimizer_decision_conflicts`, `optimizer_memo_admission`. Rune can query all of them today —
but only by tunneling through SQL strings:

```rune
ctx.sql("SELECT * FROM information_schema.optimizer_receipt_gates")
```

That is the wrong placement for the primary/cockpit interface ([[rune_is_primary_interface]]):
Rune is the observable/steerable surface, yet the gauges are reachable only as SQL strings.
The readouts already exist; the missing piece is a Rune-native authoring surface that returns
the same lazy `RuneDataFrame` as `ctx.table`/`ctx.sql`, without invoking the SQL parser or
changing any binder/runtime semantics. After the optimizer's plan-changing lane proved empty
(228 iter-0/1: relaxation premature, mixed-chain pushdown already covered by runtime filters,
counter-handoff externally blocked), this frontend placement is the grounded next cut.

## Grounding (verified)

- `RuneEngine` returns a `LogicalPlan` only — it does not bind, execute, resolve schema, or
  read `PlannerConfig`; ADR 079 guards pin this (`adr079_rune_dataframe_remains_lazy_logical_plan_handle`,
  `adr079_no_rune_local_optimizer_binder_or_executor`, rune_frontend.rs).
- `ToronContext::table(name)` lowers to `LogicalPlan::ScanNamed { table: TableRef::new(name),
  projection: None }` — intentionally one-part user-table syntax (rune_engine.rs:581).
- The binder enters `bind_information_schema_relation` only for schema-qualified scans —
  `is_information_schema_namespace(table)` (explain.rs:209) over a two-part
  `TableRef::with_schema("information_schema", table)` (catalog.rs:46).
- Existing Rune parity tests (`rune_queries_optimizer_*`) all use `ctx.sql(...)`, proving
  reachability but not Rune-native authoring.
- The readout manifest is **exactly seven** (the existing optimizer table constants in
  binder.rs). No new table is added; the probe-linearity cause distribution readout was *not*
  built (it is the named fallback), so it is not in the set.

## Decisions (promotion-locked)

**D1 — Finite optimizer readout handles.** Exactly seven associated functions on
`ToronContext`: `optimizer_decisions()`, `optimizer_runtime_verdicts()`,
`optimizer_evidence_status()`, `optimizer_margin_advisory()`, `optimizer_receipt_gates()`,
`optimizer_decision_conflicts()`, `optimizer_memo_admission()`. No generic
`ctx.information_schema(name)`, no schema-qualified overload of `ctx.table`, no arbitrary
system-table escape hatch — this is a cockpit surface, not a catalog API.

**D2 — Lazy `ScanNamed` lowering.** Each handle returns a `RuneDataFrame` whose plan is
`LogicalPlan::ScanNamed { table: TableRef::with_schema("information_schema",
"<existing_optimizer_table>"), projection: None }`. The helper path must not call
`SqlPlanner`/`plan_relation`, `bind`, an executor, runtime evidence, or any catalog API — it
only constructs a logical handle (ADR 079 boundary intact).

**D3 — Alias semantics only.** Schema, row builders, row order, projection behavior, evidence
snapshots, and `SystemTableKind` variants remain the existing information-schema
implementation. No new table, no readout-schema change.

**D4 — Ordinary Rune composition.** The return is a normal `RuneDataFrame`; `filter` /
`project` / `sort` / `limit` / `count` compose through the existing logical/binder path
(`ctx.optimizer_receipt_gates().filter(col("status").eq(lit_str("actionable")))`). No special
inspection language.

**D5 — Plan equivalence, not just row parity (the cardinal proof).** Each handle's raw
`LogicalPlan` equals the corresponding `ctx.sql("SELECT * FROM information_schema.<table>")`
relation **before binding** — the handle is a pure alias; if the plans diverge it has smuggled
in new semantics. Row parity under seeded evidence is the complement. `ctx.sql(...)` access is
retained as the compatibility/parity oracle, not removed.

**D6 — No-drift binding.** The seven handles use a private finite Rune manifest mirrored from
the existing binder table-name constants, and an architecture guard compares the two manifests
by source scan. The handle set cannot drift from the actual readout tables — no missing handle,
no extra, no stale name — without an explicit guard retarget. This keeps the cut alias-only
without making binder constants public.

**D7 — No optimizer/evidence behavior change.** Querying a handle is read-only and records no
optimizer evidence; representative non-readout user queries bind byte-identically before/after.

## Honesty gates (discriminating)

- `rune_optimizer_readout_handles_match_ctx_sql_plan` — every helper's `LogicalPlan` equals
  `ctx.sql("SELECT * FROM information_schema.<table>")` before binding (cardinal). Fails on `main`.
- `rune_optimizer_readout_handles_match_ctx_sql_rows` — seeded evidence; all seven return the
  same rows as the SQL form.
- `rune_optimizer_readout_handles_bind_to_existing_system_tables` — each binds to the expected
  existing `SystemTableKind`.
- `rune_optimizer_readout_handles_are_composable` — filter/project over a handle binds and
  matches the SQL relation's projection/filter.
- `rune_optimizer_readout_handles_are_lazy_logical_plans` — handles construct a plan without
  binding/executing (ADR 079).
- `rune_optimizer_readout_handles_cover_exactly_the_seven_readout_tables` — locks D6 (fails if
  a readout table has no handle or a handle has no table).
- `rune_optimizer_readout_handles_are_read_only` — querying a handle does not mutate evidence.
- `rune_optimizer_readout_handles_do_not_change_user_query_plans` — a representative non-readout
  Rune query binds byte-identically before/after.
- `ctx_table_remains_one_part_user_table_handle` — no schema-qualified escape hatch via
  `ctx.table`.

## Architecture guards (promotion-locked)

- `adr228_rune_optimizer_readout_handles_are_finite_seven`
- `adr228_rune_optimizer_handles_lower_to_schema_qualified_scan_named`
- `adr228_rune_optimizer_handles_do_not_use_sql_parser`
- `adr228_rune_engine_stays_lazy_no_binder_executor_config`
- `adr228_rune_optimizer_handles_mirror_table_constants_no_drift`
- `adr228_no_new_information_schema_table_or_system_table_kind`
- `adr228_no_runtime_evidence_or_policy_surface_change`
- `adr228_ctx_table_remains_one_part_and_ctx_sql_path_retained`

**Reconciled prior guards (named):**
- ADR 079 Rune laziness / no local binder-executor / no worker-side Rune surface → unchanged;
  the new helpers are allowed only as logical `ScanNamed` constructors.
- ADR 102 finite system-table + explicit-dispatch → unchanged (no new `SystemTableKind`).
- ADR 216 decision matrix → unchanged at 140/220.
- ADR 217/220/222/223/224/225 readout semantics → unchanged (helpers alias the existing tables).
- ADR 226 zero-admissible / no-generic-search, ADR 227 fence receipts → unchanged.

## Consensus (transparent)

3-of-3. The council reached this after the optimizer plan-changing fork proved empty (Claude
iter-0 triaged it: relaxation premature, mixed-chain pushdown covered by general runtime-filter
sideways-information-passing at every inner-equi join boundary, counter-handoff externally
blocked). Claude/Codex iter-1 raised a valid alternative — a probe-linearity cause-distribution
readout making ADR 227's "dominates in production" predicate durable; Claude conceded it was
the better-grounded *optimizer* cut and framed it as the terminating measurement. The operator
selected the **frontend rotation** instead: the gauges already exist; the primary Rune surface
should expose them without SQL strings. Codex authored the handle design; Claude contributed
the plan-equivalence cardinal gate and the no-drift binding; Gemini synthesized. The
cause-distribution readout remains the named fallback if a later cycle deliberately stays in
the probe-linearity arc.

## Forward arc

```text
227 probe-linearity fence receipts
-> 228 rune optimizer readout handles (this; the cockpit is first-class in Rune)
-> further Rune cockpit ergonomics only when a concrete handle/steering gap is grounded, or
   return to probe-linearity (the cause-distribution readout) once production evidence names a
   dominant fence cause.
```

## Deferred (named)

Probe-linearity cause distribution (the named fallback); any fragmenter relaxation / probe
physicalization; generic `information_schema` Rune access; schema-qualified `ctx.table`; a Rune
optimizer-trace DSL; new optimizer readout tables; SQL grammar changes; worker-side Rune
execution; any optimizer policy consumer.
