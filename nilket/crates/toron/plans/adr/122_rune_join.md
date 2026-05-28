# ADR 122: Rune `.join(...)`

**Status:** Promoted (plan). **Grounded operator promotion over a live peer-draft divergence** —
the council converged on the topic and the predicate-surface / key-pair-IR shape, while Codex's
final grounding resolved two remaining peer-draft errors: the join-kind count and whether SQL's
derived-source rejection should be imported into Rune. **Category:** Toron Rune frontend + shared
logical join construction.
**Predecessors:** 079 (Rune lazy relation handles), 115–120 (distributed barrier + streaming
substrate), 121 (general qualified relation-scope binding; Rune `alias` + `qcol`).
**Migration:** `plans/impl/122_rune_join.md`.

## Scale boundary

ADR 122 exposes Rune-authored equi-joins over Toron's existing hash-join substrate. It adds a Rune
join API, a small predicate-to-key-pair bridge, and a shared pair-shaped logical join builder used
by both SQL and Rune after each frontend has produced equi-key pairs. Non-scope: new join IR, new
`JoinKind`, new `JoinStrategy`, SQL grammar widening, executor changes, PBC changes, Wendelon
changes, distributed scheduler changes, broadcast planning, non-equi joins, qualified wildcards,
DML `RETURNING`, multi-upstream live pipelining.

## Doctrine — predicate surface, key-pair IR

> Rune feels like a dataframe language at the call site, but Toron keeps the join IR it already has.
> Rune authors a boolean equi-join predicate; a small bridge converts `Eq` and `And` expression
> trees into the existing `Vec<(Expr, Expr)>`; a shared builder owns the hash-exchange wrapping and
> `LogicalPlan::Join` construction.

The three flows are deliberately separate:

- **SQL parser path:** `JOIN … ON a = b AND c = d` → `Vec<(Expr, Expr)>` via the existing
  `lower_join_on` / `lower_join_operator`.
- **Rune frontend path:** `qcol("l", "a").eq(qcol("r", "b")).and(...)` → `Vec<(Expr, Expr)>` via the
  new `expr_to_join_keys` bridge.
- **Shared construction path:** `build_hash_join_plan(left, right, kind, pairs)` → exchange-wrapped
  `LogicalPlan::Join` with `JoinStrategy::HashPartitioned`.

This reconciles the council fork: Rune users do not write raw key-pair lists for common joins; the
reusable construction helper remains pair-shaped (not a second SQL parser); SQL and Rune share one
`LogicalPlan::Join` literal site.

## Grounding corrections (locked in council history)

- `JoinKind` has **eight** variants today: `Inner`, `LeftOuter`, `RightOuter`, `FullOuter`,
  `LeftSemi`, `RightSemi`, `LeftAnti`, `RightAnti`. ADR 122 exposes nine Rune methods total:
  `.join` defaults to Inner, and eight named methods cover every existing variant.
- `JoinStrategy` has `HashPartitioned` and `Broadcast`. ADR 122 constructs only `HashPartitioned`.
- SQL's `"joins around derived tables are not supported"` is a **SQL grammar/planner boundary**, not
  a logical-dataframe boundary. Rune joins operate on `RuneDataFrame` values; existing
  `plan_qualifiers` recursion (`sql_planner.rs:6081`) walks through `Filter`/`Project`/`Aggregate`/
  `Exchange`/`Sort`/`Take` and exposes `Alias` qualifiers; the binder's `RelationScope::project`
  (ADR 121 D2) preserves direct-column provenance. A Rune value like
  `ctx.sql("SELECT id, region FROM sales").alias("s")` therefore resolves `qcol("s", "region")`
  through the standard scope rules — **no separate derived-source rejection at the Rune layer.**

## Decisions (promotion-locked)

**D1 — Reuse existing join IR and execution semantics.** ADR 122 does not change `LogicalPlan::Join`,
`ExecutionPlan::Join`, `JoinKind`, `JoinStrategy`, executor kernels, fragmenter join handling, PBC,
or Wendelon.

**D2 — Shared pair-shaped builder.** Extract a free helper:

```rust
pub(crate) fn build_hash_join_plan(
    left: LogicalPlan,
    right: LogicalPlan,
    kind: JoinKind,
    on: Vec<(Expr, Expr)>,
) -> Result<LogicalPlan, FrontendError>;
```

The helper derives left/right exchange keys via the existing side-inference logic (extracted as a
free function `derive_join_exchange_keys`), wraps both inputs in terminal `LogicalPlan::Exchange`
nodes using the shared default join partition count, and returns
`LogicalPlan::Join { kind, on, strategy: JoinStrategy::HashPartitioned }`. The helper does not parse
boolean predicates and does not reject non-scan dataframe inputs.

**D3 — Rune predicate bridge.** Add one small bridge:

```rust
pub(crate) fn expr_to_join_keys(on: &Expr) -> Result<Vec<(Expr, Expr)>, FrontendError>;
```

It accepts only `Expr::Eq(left, right)` → one key pair and `Expr::And(left, right)` → recursive
concatenation. Everything else rejects as unsupported non-equi join predicate. SQL keeps its
existing parser-specific `lower_join_on` path; the bridge is for Rune only.

**D4 — Rune API surface.** Add `.join(...)` as inner join plus eight named methods for the existing
join kinds:

```rust
join(other, on)             -> Inner
inner_join(other, on)       -> Inner
left_join(other, on)        -> LeftOuter
right_join(other, on)       -> RightOuter
full_join(other, on)        -> FullOuter
left_semi_join(other, on)   -> LeftSemi
right_semi_join(other, on)  -> RightSemi
left_anti_join(other, on)   -> LeftAnti
right_anti_join(other, on)  -> RightAnti
```

No string `how` argument and no Rune-exposed `JoinKind` enum in V1. Named methods are the
least-surprising Rune surface and avoid making IR enum binding part of this ADR.

**D5 — `qcol` is the normal join-key spelling.** Rune joins author with relation aliases and
qualified columns:

```rust
ctx.table("sales").alias("s")
    .join(
        ctx.table("regions").alias("r"),
        qcol("s", "region").eq(qcol("r", "region")),
    )
```

Multi-column joins compose via `.and(...)`:

```rust
left.join(
    right,
    qcol("l", "tenant").eq(qcol("r", "tenant"))
        .and(qcol("l", "id").eq(qcol("r", "fk"))),
)
```

**D6 — Rune dataframe continuations may be join inputs.** Rune joins operate on `RuneDataFrame`
values, not SQL table factors. Filtered, projected, aliased, and `ctx.sql(...)` relation values may
be left or right inputs when the binder can resolve the key expressions. **This does not widen SQL's
derived-table grammar support** — SQL's `table_factor_is_derived` rejection at `sql_planner.rs:2642`
remains exactly as is for SQL-syntactic derived FROM-clauses; it is not imported into Rune. Grounded
in `plan_qualifiers` recursion + `RelationScope::project` direct-column provenance preservation;
the existing 121 gate `rune_qcol_project_named_over_joined_sql_executes` proves qualified Rune
projection over a `ctx.sql` plan, and 122 adds the positive join-input gate.

**D7 — Validation stays layered.** Rune validates only predicate shape through `expr_to_join_keys`.
Existing binder paths remain responsible for unknown qualifiers, ambiguous columns, computed join
keys, key type mismatches, unsupported float keys, and logical-identity mismatches. No duplicate
validation in Rune.

**D8 — No distributed scheduling change.** ADR 122 creates the authored multi-input workload that a
later ADR can use to relax the 119/120 multi-upstream live-pipelining barrier. It does not relax
that barrier; `adr119_multi_upstream_barrier_untouched` continues to pass.

## Behavior gates

- `rune_inner_join_alias_qcol_executes` — aliases both inputs, joins via `qcol(...).eq(qcol(...))`,
  projects qualified columns from both sides, executes; assert row values.
- `rune_join_multi_column_predicate_decomposes_to_key_pairs` — `.and(...)`-composed equality
  predicate lowers to the expected key-pair list.
- `rune_join_wraps_both_inputs_in_hash_exchanges` — logical plan has `Join` over two terminal
  `Exchange` children using the shared default join partition count.
- `rune_join_named_methods_lower_to_existing_join_kinds` — every Rune join method maps to the
  expected existing `JoinKind`.
- `rune_join_project_named_qualified_outputs_execute` — joined output can be renamed and projected
  with `project_named`.
- `rune_join_then_take_remains_chainable` — joined dataframe continues through normal Rune
  continuations.
- `rune_join_accepts_filtered_dataframe_inputs` — filtered/aliased inputs bind and execute when keys
  remain resolvable.
- **`rune_join_accepts_sql_relation_input_when_keys_resolve`** — the load-bearing positive gate for
  D6: `ctx.sql("SELECT id, region FROM sales").alias("s").join(...)` executes; pins that SQL's
  derived-table grammar reject was not imported into Rune values.
- `rune_join_unqualified_ambiguous_key_rejects` — two visible relations with `id` and
  `col("id").eq(col("id"))` reject through existing ambiguity handling.
- `rune_join_computed_key_rejects` — computed key expressions remain unsupported through existing
  binder checks.
- `rune_join_predicate_without_equi_key_rejects` — non-equality predicates reject through the Rune
  bridge.
- `rune_join_logical_identity_mismatch_rejects` — existing nominal join-key checks apply to
  Rune-authored joins.

Regressions: `join_kind_test.rs` remains green and continues to own join-kind semantics; ADR 121
qualified-resolution tests remain green; ADR 119/120 multi-upstream barrier guards remain
unchanged.

## Architecture guards

- **`adr122_rune_join_surface_exposed`** — `RuneDataFrame` exposes `.join` and all eight named join
  methods; the Rune module registers every method; no Rune `JoinKind` enum registration.
- **`adr122_join_builder_is_pair_shaped_and_shared`** — `sql_planner.rs` contains
  `pub(crate) fn build_hash_join_plan`; signature accepts `on: Vec<(Expr, Expr)>`; SQL join lowering
  calls it; `rune_engine.rs` calls it.
- **`adr122_rune_predicate_bridge_is_narrow`** — `expr_to_join_keys` handles only `Expr::Eq` and
  `Expr::And`; SQL join lowering does not call it; `rune_engine.rs` does not contain another join
  predicate walker.
- **`adr122_join_lowers_to_existing_logical_join`** — the shared builder source contains
  `LogicalPlan::Join` + `JoinStrategy::HashPartitioned`.
- **`adr122_join_inserts_terminal_hash_exchanges`** — the shared builder source contains two
  `LogicalPlan::Exchange` constructors.
- **`adr122_join_ir_and_strategy_surface_unchanged`** — `ir.rs` still has exactly the eight
  expected `JoinKind` variants; `JoinStrategy` remains exactly `HashPartitioned` and `Broadcast`.
- **`adr122_sql_derived_reject_not_imported_into_rune_join`** — behavior test source contains
  `rune_join_accepts_sql_relation_input_when_keys_resolve`; Rune join code does not contain the SQL
  derived-table reject string `"joins around derived tables are not supported"`.
- **`adr122_no_executor_pbc_wendelon_or_scheduler_change`** — no ADR 122 markers or join-authoring
  logic in `exec.rs`, `pbc.rs`, Wendelon, or distributed scheduler code.
- **Recipe:** `cargo test -p toron` + `cargo test -p wendelon` + `cargo test -p toron --test
  architecture_test adr122` + `cargo check -p toron --all-targets`.

## Council history

iter-0: 3-of-3 on topic A (Rune `.join(...)`). iter-1: API-shape fork — Claude/Gemini favored a
boolean `RuneExpr` predicate + shared construction helper; Codex initially favored direct key-pairs
to avoid a second join analyzer. iter-2 clarified the internal truth — SQL already produces
key-pairs before `LogicalPlan::Join`; the IR's `LogicalPlan::Join.on` is `Vec<(Expr, Expr)>`
(verified at `sql_planner.rs:4067`); the useful shared helper is pair-shaped. Codex's final pass
accepted predicate authoring at the Rune surface while keeping the reusable construction helper
pair-shaped, then resolved two live peer-draft errors by grounding:
- there are **eight** join kinds, not nine;
- Rune dataframe joins should **not** inherit SQL's derived-table grammar rejection because
  `plan_qualifiers` + `RelationScope::project` already handle direct-column Rune sources.
This was not a genuine 3-of-3 on every sub-decision; it was an operator promotion of the grounded
shape with the divergence surfaced explicitly, per council convention.

## What to do next

- **Multi-upstream live pipelining** — using Rune-authored joins as the customer; finally
  genuinely customer-ready (122 lands the customer the [[feedback_cut_must_deliver_the_name]]
  barrier-relaxation-needs-customer trap was waiting for).
- Optional richer join ergonomics only after usage proves the need.
- **Qualified wildcards and qualified DML `RETURNING`** — 121's biggest deferred follow-ons; require
  planner↔binder restructuring or `Wildcard` IR.
- **Broadcast joins / strategy selection** — the existing `JoinStrategy::Broadcast` variant has no
  current constructor; needs cost-based selection or syntactic hint.
- **SQL joins around derived tables / GROUP BY over joins** — the corresponding SQL syntactic
  rejections that 122 deliberately left in place.
- **Streamable `Sort`/`Aggregate` coverage** (120 V2).
- **Logical-type lifecycle** (`DROP`/`ALTER LOGICAL TYPE`) — tier pivot.

## Promotion criteria

All 12 behavior gates green; the 8 `adr122_*` architecture guards green; `join_kind_test.rs`
remains green after the shared-builder extraction; ADR 121 qualified-resolution regressions
remain green; ADR 119/120 scheduler guards unchanged; no executor / PBC / Wendelon / distributed
scheduler changes; full `cargo test -p toron` + `cargo test -p wendelon` + `cargo test -p toron
--test architecture_test adr122` + `cargo check -p toron --all-targets` green.
Migration: `plans/impl/122_rune_join.md`.
