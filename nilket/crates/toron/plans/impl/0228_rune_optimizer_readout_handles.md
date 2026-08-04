# Impl Plan — Toron ADR 228: Rune Optimizer Readout Handles

Pairs with `plans/adr/0228_rune_optimizer_readout_handles.md`. Add seven finite, lazy Rune
`ToronContext` handles over the existing `information_schema.optimizer_*` readouts. **Pure
alias: no new table / `SystemTableKind` / evidence / durable key / plan semantics; ADR 079
rune-engine boundary intact (LogicalPlan only, no bind/execute/`PlannerConfig`).**

Touch surface:
- `rune_engine.rs` — seven `ToronContext` associated functions returning `RuneDataFrame` over
  `LogicalPlan::ScanNamed { table: TableRef::with_schema("information_schema", "<table>"),
  projection: None }`; register all seven in the Rune module.
- `binder/tests.rs` (and/or rune_engine tests) — plan-equality (cardinal), row-parity,
  composability, lazy, coverage, read-only, no-user-plan-change discriminators.
- `tests/architecture/rune_frontend.rs` — the eight `adr228_*` guards; keep the `adr079_*`
  boundary guards green.

No binder / explain / runtime_evidence / ir source change (the tables already exist); no SQL
grammar; no worker-side Rune surface.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| One-part handle precedent | `ToronContext::table(name)` → `LogicalPlan::ScanNamed { table: TableRef::new(name), projection: None }` (rune_engine.rs:581) |
| Two-part constructor | `TableRef::with_schema("information_schema", "<table>")` (catalog.rs:46) |
| Binder info-schema match | `is_information_schema_namespace(table)` (explain.rs:209), reached from binder.rs:913 |
| Readout table constants (compare handles, no drift) | the seven existing `INFORMATION_SCHEMA_OPTIMIZER_*_TABLE` constants in binder.rs |
| Rune module registration | the `m.function([...])` / `associated_function(...)` block (rune_engine.rs:~1138/1195) |
| ADR 079 boundary guards to keep green | `adr079_rune_dataframe_remains_lazy_logical_plan_handle` (rune_frontend.rs:67), `adr079_no_rune_local_optimizer_binder_or_executor` (213) |

## Phase 0 — Red discriminators

`rune_optimizer_readout_handles_match_ctx_sql_plan`,
`rune_optimizer_readout_handles_match_ctx_sql_rows`,
`rune_optimizer_readout_handles_are_composable`,
`rune_optimizer_readout_handles_cover_exactly_the_seven_readout_tables`. Red on `main` (no handles).

## Phase 1 — Handles (rune_engine.rs)

Seven associated functions on `ToronContext`, each lowering to a schema-qualified `ScanNamed`
and returning `RuneDataFrame`:

```text
optimizer_decisions            -> information_schema.optimizer_decisions
optimizer_runtime_verdicts     -> information_schema.optimizer_runtime_verdicts
optimizer_evidence_status      -> information_schema.optimizer_evidence_status
optimizer_margin_advisory      -> information_schema.optimizer_margin_advisory
optimizer_receipt_gates        -> information_schema.optimizer_receipt_gates
optimizer_decision_conflicts   -> information_schema.optimizer_decision_conflicts
optimizer_memo_admission       -> information_schema.optimizer_memo_admission
```

Drive the seven names from a private finite Rune manifest mirrored from the existing binder
constants, and guard the mirror with a source-scan comparison (D6 no-drift). Register all seven
in the Rune module beside `table`/`sql`.
`rune_engine.rs` must add **no** `bind`/executor/`PlannerConfig` import.

## Phase 2 — Tests + guards

Cardinal: each handle's `LogicalPlan` equals the `ctx.sql("SELECT * FROM
information_schema.optimizer_<table>")` relation **before binding**; rows identical under
seeded evidence; one composability case; the coverage test (handles ⟺ the seven constants);
lazy + read-only + no-user-plan-change. Add the eight `adr228_*` guards; confirm the `adr079_*`
boundary and the 102/216/226/227 locks stay green.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib rune_optimizer
scripts/cargo-cgroup test  -p toron --lib rune_queries_optimizer
scripts/cargo-cgroup test  -p toron --test architecture_test adr228
scripts/cargo-cgroup test  -p toron --test architecture_test adr079
scripts/cargo-cgroup test  -p toron --test architecture_test adr102
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. A handle plan differing from the SQL information-schema plan:
   `rune_optimizer_readout_handles_match_ctx_sql_plan` fails (smuggled semantics).
2. `rune_engine.rs` importing `PlannerConfig`/`bind`/executor:
   `adr228_rune_engine_stays_lazy_no_binder_executor_config` + the `adr079_*` guards fail.
3. A readout table with no handle, or a handle with no table:
   `rune_optimizer_readout_handles_cover_exactly_the_seven_readout_tables` fails.
4. A new `SystemTableKind` / table / evidence key:
   `adr228_no_new_information_schema_table_or_system_table_kind` +
   `adr228_no_runtime_evidence_or_policy_surface_change` fail.
5. A schema-qualified `ctx.table` escape hatch:
   `adr228_ctx_table_remains_one_part_and_ctx_sql_path_retained` fails.

## Hygiene

- No unrelated memo-engine work despite the existing `optimizer_memo_admission` table name; no
  version-suffix idiom; no new durable keys / goldens (alias-only — rows come from the
  existing tables; plans are `ScanNamed`, no EXPLAIN-render change).
- `--all-targets` after the Rune module additions.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Confirm `TableRef::with_schema("information_schema", …)` is the exact shape
   `is_information_schema_namespace` matches, so the handle plan is byte-identical to the SQL
   form (the plan-equality test is the check).
2. Single finite source for the seven Rune names — lean: keep a private Rune manifest and
   compare it to binder constants in the architecture guard. Making binder constants public is
   unnecessary surface for this alias cut.
3. Confirm the Rune registration idiom for an associated function taking no args beyond `self`.
