# Impl Plan — ADR 122: Rune `.join(...)`

**Migration for** `plans/adr/0122_rune_join.md`. **Scope:** Toron shared logical join construction
(`src/sql_planner.rs` helper extractions), Rune frontend (`src/rune_engine.rs` `.join` + 8 named
methods), behavior tests, and architecture guards. **Non-scope:** new IR; executor; PBC; Wendelon;
distributed scheduler; SQL grammar; broadcast/non-equi joins.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Rune dataframe API block | `crates/toron/src/rune_engine.rs::impl RuneDataFrame` (`:209-`) |
| Existing free column helpers | `rune_engine.rs` `col` (`:394`), `qcol` (`:400`), literal helpers |
| Existing predicate builders | `rune_engine.rs` `RuneExpr::eq` (`:600`), `RuneExpr::and` (`:630`), comparisons |
| Module registration | `rune_engine.rs::create_toron_module` |
| Logical join IR | `ir.rs` `LogicalPlan::Join { left, right, kind, on, strategy }` |
| Current join kinds (8 variants) | `ir.rs:603` `JoinKind::{Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, RightSemi, LeftAnti, RightAnti}` |
| Current join strategies | `ir.rs:615` `JoinStrategy::{HashPartitioned, Broadcast}` |
| SQL join predicate lowering | `sql_planner.rs::lower_join_on(&SqlExpr) -> Vec<(Expr, Expr)>` |
| SQL exchange-key derivation (free-helper-extract target) | `sql_planner.rs:4067 fn join_exchange_keys(&self, on: &[(Expr, Expr)], left, right)` — no `self.` state |
| SQL hash-join construction (refactor target) | `sql_planner.rs:2640-2670` |
| SQL derived-source reject (preserved, not imported into Rune) | `sql_planner.rs:2642` `"joins around derived tables are not supported"` |
| Binder hash-shape validation | `binder.rs::validate_strategy_shape` |
| Binder join key validation | `binder.rs::bind_join_key_pair` / `join_key_column_ref` |
| `plan_qualifiers` recursion (grounds D6) | `sql_planner.rs:6081` |
| `RelationScope::project` direct-column provenance (ADR 121 D2) | `binder.rs` |
| Join semantics regression suite | `crates/toron/tests/join_kind_test.rs` (eight variants plus nine SQL spelling cases) |
| Rune behavior suite | `crates/toron/tests/rune_relation_handles_test.rs` |
| Rune architecture guards | `crates/toron/tests/architecture/rune_frontend.rs` |

## Phase 0 — Write the failing gates first

Add 12 behavior tests to `crates/toron/tests/rune_relation_handles_test.rs`:

1. `rune_inner_join_alias_qcol_executes` — aliases both inputs, `qcol(...).eq(qcol(...))`, projects
   one qualified column from each side, executes; assert row values.
2. `rune_join_multi_column_predicate_decomposes_to_key_pairs` — build `eq(...).and(eq(...))`,
   inspect `LogicalPlan::Join.on` and both exchange key lists.
3. `rune_join_wraps_both_inputs_in_hash_exchanges` — outer `LogicalPlan::Join` over two
   `LogicalPlan::Exchange` children with the shared default join partition count.
4. `rune_join_named_methods_lower_to_existing_join_kinds` — `.join`, `.inner_join`, `.left_join`,
   `.right_join`, `.full_join`, `.left_semi_join`, `.right_semi_join`, `.left_anti_join`,
   `.right_anti_join` — each lowers to the expected `JoinKind`.
5. `rune_join_project_named_qualified_outputs_execute` — project qualified columns from both sides
   via `project_named`; assert output field names and row values.
6. `rune_join_then_take_remains_chainable` — `.join(...).take(n).project_named(...)` — at most `n`
   rows.
7. `rune_join_accepts_filtered_dataframe_inputs` — left input
   `ctx.table("sales").filter(col("status").eq(lit_str("open"))).alias("s")`; join against aliased
   `regions`; execute.
8. **`rune_join_accepts_sql_relation_input_when_keys_resolve`** — load-bearing positive gate for D6:
   `ctx.sql("SELECT id, region FROM sales").alias("s").join(...)` executes; pins SQL derived-table
   grammar reject was not imported into Rune values. Grounded in `plan_qualifiers` recursion +
   `RelationScope::project` direct-column provenance. **Do not** add a private `RuneDataFrame`
   "derived" flag.
9. `rune_join_unqualified_ambiguous_key_rejects` — two aliases of `sales` + `col("id").eq(col("id"))`
   → ambiguity error from binder.
10. `rune_join_computed_key_rejects` — `qcol("s","id").add(lit_i64(1)).eq(qcol("r","region"))` →
    existing computed-key rejection from binder.
11. `rune_join_predicate_without_equi_key_rejects` — `qcol("s","region").gt(qcol("r","region"))` →
    Rune bridge rejects before planning.
12. `rune_join_logical_identity_mismatch_rejects` — logical-ascribed keys with mismatched nominal
    identities → existing `NominalTypeMismatch`.

Architecture guards in `crates/toron/tests/architecture/rune_frontend.rs`:

- `adr122_rune_join_surface_exposed`
- `adr122_join_builder_is_pair_shaped_and_shared`
- `adr122_rune_predicate_bridge_is_narrow`
- `adr122_join_lowers_to_existing_logical_join`
- `adr122_join_inserts_terminal_hash_exchanges`
- `adr122_join_ir_and_strategy_surface_unchanged`
- `adr122_sql_derived_reject_not_imported_into_rune_join`
- `adr122_no_executor_pbc_wendelon_or_scheduler_change`

## Phase 1 — Extract shared pair-shaped join builder (D2, G2)

In `src/sql_planner.rs`, add the free helper near existing join lowering:

```rust
pub(crate) fn build_hash_join_plan(
    left: LogicalPlan,
    right: LogicalPlan,
    kind: JoinKind,
    on: Vec<(Expr, Expr)>,
) -> Result<LogicalPlan, FrontendError> {
    let (left_keys, right_keys) = derive_join_exchange_keys(&on, &left, &right)?;
    Ok(LogicalPlan::Join {
        left: Box::new(LogicalPlan::Exchange {
            input: Box::new(left),
            partition_count: DEFAULT_JOIN_PARTITION_COUNT,
            key: left_keys,
        }),
        right: Box::new(LogicalPlan::Exchange {
            input: Box::new(right),
            partition_count: DEFAULT_JOIN_PARTITION_COUNT,
            key: right_keys,
        }),
        kind,
        on,
        strategy: JoinStrategy::HashPartitioned,
    })
}
```

Refactor `sql_planner.rs:4067::join_exchange_keys(&self, ...)` to a free function:

```rust
fn derive_join_exchange_keys(
    on: &[(Expr, Expr)],
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Result<(Vec<Expr>, Vec<Expr>), FrontendError>;
```

Body is the existing side-inference logic verbatim — verified no `self.` state.

Refactor SQL's join loop at `:2640-2670` from inline construction to:

```rust
let (kind, on) = self.lower_join_operator(&join.join_operator)?;
plan = build_hash_join_plan(plan, right, kind, on)?;
```

**Do not** move SQL's `"joins around derived tables are not supported"` check at `:2642` into the
builder — it is a SQL table-factor boundary, not a logical-dataframe boundary.

**Do not** add a Rune-layer structural reject for `ctx.sql(...)` inputs. If the input preserves a
resolvable relation scope, the binder decides like any other `RuneDataFrame`.

Run `cargo test -p toron --test join_kind_test` after this phase before adding Rune methods —
behavior-preserving refactor of SQL JOIN lowering.

## Phase 2 — Add the Rune predicate bridge (D3, G3)

In `src/sql_planner.rs` (or a small shared frontend helper module), add:

```rust
pub(crate) fn expr_to_join_keys(on: &Expr) -> Result<Vec<(Expr, Expr)>, FrontendError> {
    match on {
        Expr::Eq(left, right) => Ok(vec![((**left).clone(), (**right).clone())]),
        Expr::And(left, right) => {
            let mut pairs = expr_to_join_keys(left)?;
            pairs.extend(expr_to_join_keys(right)?);
            Ok(pairs)
        }
        _ => Err(unsupported("JOIN predicate requires equality key predicates")),
    }
}
```

This helper is Rune-facing only. SQL keeps `lower_join_on` over the SQL parser AST.

## Phase 3 — Add `RuneDataFrame` join methods (D1, D4)

In `impl RuneDataFrame`:

```rust
pub fn join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self>;
pub fn inner_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self>;
pub fn left_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self>;
pub fn right_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self>;
pub fn full_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self>;
pub fn left_semi_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self>;
pub fn right_semi_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self>;
pub fn left_anti_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self>;
pub fn right_anti_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self>;
```

Add a private helper:

```rust
fn join_with_kind(
    left: LogicalPlan,
    right: LogicalPlan,
    kind: JoinKind,
    on: RuneExpr,
) -> VmResult<RuneDataFrame>;
```

Implementation requirements:
- call `expr_to_join_keys(&on.expr)`;
- call `build_hash_join_plan(left, right, kind, pairs)`;
- convert any `FrontendError` to the existing Rune panic-style error surface;
- **do not** bind, execute, optimize, inspect catalogs, or reject `ctx.sql(...)` dataframe inputs
  in Rune.

`join` and `inner_join` both delegate to `JoinKind::Inner`. The other named methods delegate to the
matching existing `JoinKind`. Register each method in `create_toron_module`. **Do not** add a string
`how` argument and **do not** register `JoinKind` as a Rune enum in this ADR.

## Phase 4 — Fixture updates

Extend the Rune relation test fixture with a second table:
- schema: `regions(region Utf8, label Utf8)`;
- descriptor: register in `catalog()` with a fresh `TableId`;
- storage: add its batch to the `FixtureScan` map for helpers that execute joins.

Prefer assertions stable under hash join output order: compare row sets, or sort with the existing
ordered-sort surface before asserting exact order.

## Phase 5 — Architecture guards (G1-G8)

Add ADR 122 guards to `tests/architecture/rune_frontend.rs`:

- **`adr122_rune_join_surface_exposed`** — `rune_engine.rs` contains `.join` and all eight named
  methods; module registers every method; module does not register `JoinKind` as a Rune type.
- **`adr122_join_builder_is_pair_shaped_and_shared`** — `sql_planner.rs` contains
  `pub(crate) fn build_hash_join_plan`; the helper signature contains `on: Vec<(Expr, Expr)>`; SQL
  join lowering calls `build_hash_join_plan`; `rune_engine.rs` calls `build_hash_join_plan`.
- **`adr122_rune_predicate_bridge_is_narrow`** — `expr_to_join_keys` handles `Expr::Eq` and
  `Expr::And`; SQL join lowering does not call `expr_to_join_keys`; `rune_engine.rs` does not
  contain another join predicate walker.
- **`adr122_join_lowers_to_existing_logical_join`** — builder source contains `LogicalPlan::Join`
  and `JoinStrategy::HashPartitioned`.
- **`adr122_join_inserts_terminal_hash_exchanges`** — builder source contains two
  `LogicalPlan::Exchange` constructors.
- **`adr122_join_ir_and_strategy_surface_unchanged`** — `ir.rs` still has exactly the eight
  expected `JoinKind` variants; `JoinStrategy` remains exactly `HashPartitioned` + `Broadcast`.
- **`adr122_sql_derived_reject_not_imported_into_rune_join`** — behavior test source contains
  `rune_join_accepts_sql_relation_input_when_keys_resolve`; no Rune join method contains
  `"joins around derived tables are not supported"`.
- **`adr122_no_executor_pbc_wendelon_or_scheduler_change`** — no ADR 122 marker in `exec.rs`,
  `pbc.rs`, `src/distributed`, or Wendelon source.

## Phase 6 — Verification

Focused commands:

```sh
cargo fmt
cargo test -p toron --test rune_relation_handles_test rune_join
cargo test -p toron --test join_kind_test
cargo test -p toron --test architecture_test adr122
cargo test -p toron --test architecture_test adr121
```

Full commands:

```sh
cargo test -p toron
cargo test -p wendelon
cargo check -p toron --all-targets
git diff --check
```

## Acceptance

All 12 behavior gates pass; the 8 `adr122_*` architecture guards pass; `join_kind_test.rs` remains
green after the shared-builder extraction; ADR 121 qualified-resolution regressions remain green;
ADR 119/120 scheduler guards remain unchanged; no executor / PBC / Wendelon / distributed scheduler
changes; full Toron + Wendelon suites + `--all-targets` green.
Migration: `plans/adr/0122_rune_join.md`.
