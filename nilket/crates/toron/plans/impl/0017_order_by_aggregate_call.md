# Implementation Plan: ADR 017 — `ORDER BY <aggregate-call>` for Grouped Aggregate SQL

**Companion ADR:**
`plans/adr/0017_order_by_aggregate_call.md`

## Status

Promoted. Implemented.

This landed as a small single-crate composition: one rename pass, one
return-shape change, one function-signature extension, one
walker invocation, test coverage at four layers (planner /
binder / fragmenter / integration). No new IR variant, no wire
change, no operator surface change, no `bind_expr` arm, no
fragmenter detector change. No bench artifact.

## Goal

Land `GROUP BY ... ORDER BY <aggregate-call>` (with and without
`HAVING`) end-to-end:

- planner accepts the composition and lowers each
  `OrderByExpr.expr` matching an aggregate-call shape via the
  shared (renamed) walker against the grouped-projection scope;
- `lower_select` returns a private `LoweredSelect { plan,
  grouped_aggregate_scope }` struct; `lower_query` destructures
  the named fields and passes `grouped_aggregate_scope.as_ref()`
  as an `Option<&GroupedAggregateScope>` parameter to
  `lower_order_by`;
- the walker drops its `SqlExpr::CompoundIdentifier` reject arm
  (becomes shape-neutral over non-aggregate-call expression
  shapes); HAVING's qualified-reference reject moves into a
  small `lower_having_predicate` pre-pass with the unchanged
  reject string `"qualified HAVING column references are not
  supported"`. The walker takes a `clause: &'static str`
  parameter used only for ambiguity-message formatting at the
  resolver;
- `GroupedAggregateScope::resolve` returns a typed
  `ResolveOutcome` enum; the caller
  (`resolve_grouped_aggregate_reference`) formats the user-
  visible message with the clause label;
- the four-way ambiguity classification ADR 016 §4 fires for
  ORDER BY references identically (zero matches, multiple
  matches, one match with duplicated output name, one match
  with unique output name);
- the HAVING-only qualified-reference reject string
  `"qualified HAVING column references are not supported"` is
  preserved verbatim and continues to fire for `HAVING t.col`
  queries (now from the `lower_having_predicate` pre-pass
  rather than inside the walker); ORDER BY qualified column
  references continue to lower through `lower_expr`'s existing
  `CompoundIdentifier` arm at `sql_planner.rs:730-733`;
- fragmenter: no change. The ADR 015 detector and ADR 016
  detector extension already accept the resulting plan shapes;
- ADR 011's, ADR 015's, and ADR 016's existing fast paths are
  preserved verbatim;
- integration coverage: in-process digest parity for the
  ORDER BY-only and dual-rewrite cases; multi-process digest
  parity on a distributed cluster fixture.

## Substrate

- `crates/toron/src/sql_planner.rs`
  - `SqlPlanner::lower_query` (`sql_planner.rs:51-122`) — owns
    the existing `Sort + Exchange(1, [])` wrap when `ORDER BY`
    is present; calls `lower_order_by`. ADR 017 changes this to
    (a) destructure the named fields of the `LoweredSelect`
    struct returned from `lower_select` (`let LoweredSelect {
    plan, grouped_aggregate_scope } = ...;`),
    (b) pass `grouped_aggregate_scope.as_ref()` (which has type
    `Option<&GroupedAggregateScope>`) into `lower_order_by`.
  - `SqlPlanner::lower_select` (`sql_planner.rs:124+`) —
    return-type change from `Result<LogicalPlan,
    FrontendError>` to `Result<LoweredSelect, FrontendError>`,
    where `LoweredSelect` is a new private struct:
    `struct LoweredSelect { plan: LogicalPlan,
    grouped_aggregate_scope: Option<GroupedAggregateScope> }`.
    Grouped-aggregate branch sets
    `grouped_aggregate_scope: Some(scope)`; all other branches
    set `None`. Single caller (`lower_query`) destructures the
    named fields.
  - `SqlPlanner::lower_grouped_projection`
    (`sql_planner.rs:380+`) — produces
    `GroupedProjection { aggr_expr, having_scope }` today.
    Renames `having_scope` → `grouped_scope`. No behavior
    change.
  - `SqlPlanner::lower_order_by` (`sql_planner.rs:304-326`) —
    signature extension: add `scope: Option<&GroupedAggregate
    Scope>` parameter. When `Some`, each `OrderByExpr.expr`
    rewrites through `self.rewrite_grouped_aggregate_calls(...,
    "ORDER BY")` before `self.lower_expr(...)`. Existing checks
    (`interpolate`, `with_fill`, `exprs.is_empty()`) stay.
  - `SqlPlanner::rewrite_having_aggregate_calls`
    (`sql_planner.rs:432-461`) — renamed to
    `rewrite_grouped_aggregate_calls`. Adds
    `clause: &'static str` parameter (forwarded through
    recursion). The `SqlExpr::CompoundIdentifier` reject arm
    (`sql_planner.rs:456-458`) is **removed**; the walker
    becomes shape-neutral over non-aggregate-call expression
    shapes. HAVING's qualified-reference reject is relocated
    to `lower_having_predicate`'s pre-pass (see below).
  - `SqlPlanner::resolve_having_aggregate_reference`
    (`sql_planner.rs:463-473`) — renamed to
    `resolve_grouped_aggregate_reference`. Adds
    `clause: &'static str` parameter. Calls
    `scope.resolve(...)` returning `ResolveOutcome`; formats
    user-facing message with the clause label.
  - `SqlPlanner::lower_having_predicate`
    (`sql_planner.rs:423-430`) — call-site update for the
    renamed walker; passes `"HAVING"` as the clause label.
    Function name stays `lower_having_predicate` (clause-
    specific caller). **Adds a pre-pass** that walks the
    HAVING `SqlExpr` AST and rejects any
    `SqlExpr::CompoundIdentifier` it encounters with the
    unchanged string `"qualified HAVING column references
    are not supported"`. The pre-pass restores the user-
    visible HAVING reject behavior that the walker previously
    owned.
  - `HavingAggregateScope` struct (`sql_planner.rs:851-911`) —
    renamed to `GroupedAggregateScope`.
    `output_name_counts` field unchanged.
    `resolve` method returns `ResolveOutcome` instead of
    `Result<String, FrontendError>`.
  - `HavingAggregateEntry` (`sql_planner.rs:913-917`) — renamed
    to `GroupedAggregateEntry`.
  - `HavingAggregateSignature` (`sql_planner.rs:919-925`) —
    renamed to `GroupedAggregateSignature`. Variant set
    unchanged (`CountAll`, `Count(ColumnRef)`,
    `Sum(ColumnRef)`).
  - `having_aggregate_signature(...)` helper — renamed to
    `grouped_aggregate_signature(...)`.
  - `ResolveOutcome` enum — new private type:
    ```rust
    enum ResolveOutcome {
        Found(String),
        NotInSelect,
        Ambiguous,
    }
    ```
  - Walker qualified-references reject string at
    `sql_planner.rs:456-458` — **removed from the walker** and
    relocated to `lower_having_predicate`'s pre-pass with the
    unchanged text `"qualified HAVING column references are
    not supported"`. The grep-pinned string remains present in
    `sql_planner.rs`, just at a different site.
  - `lower_expr` catch-all at `sql_planner.rs:795` —
    unchanged. Still rejects aggregate-call shapes that the
    walker did not substitute (i.e., outside grouped queries
    where `lower_order_by` receives `scope = None`).
- `crates/toron/src/binder.rs` — no change. The
  `bind_comparison` literal coercion ADR 016 introduced
  remains; ORDER BY by aggregate output is a plain column ref
  in the sort-key position, no coercion needed.
- `crates/toron/src/distributed/fragment.rs` — no change.
  - `is_terminal_sort_over_grouped_aggregate` at
    `fragment.rs:562-632` already matches both `Sort(Exchange
    (Aggregate(Exchange)))` and `Sort(Exchange(Filter
    (Aggregate(Exchange))))` shapes. ADR 017 plans land in
    these shapes; detector inspection is shape-only,
    `SortExpr.expr` is cloned verbatim into
    `TerminalSortGather`.
- `crates/toron/src/exec.rs` — no change. `Sort` executor
  consumes whatever `SortExpr` produces; rewritten column-ref
  keys lower like any other.
- `crates/toron/src/ir/execution.rs` — no change. No new
  variant.
- `crates/toron/tests/group_by_test.rs` — ADR 017 adds planner
  + execution + fragmenter test cases for ORDER BY-only and
  dual-rewrite (HAVING + ORDER BY) compositions.
- `crates/toron/tests/sort_ordering_test.rs` — existing ADR
  010 / ADR 015 ordering coverage remains unchanged; ADR 017
  composition cases live in `group_by_test.rs` (HAVING is the
  closer-related context).
- `crates/toron/tests/distributed_process_test.rs` — ADR 017
  adds a multi-process `ORDER BY <agg>` fixture and a dual-
  rewrite fixture.
- `crates/toron/tests/architecture_test.rs` — ADR 017 adds:
  - **presence** guards for `"ORDER BY aggregate-call must
    appear in SELECT list"` and `"ambiguous ORDER BY
    aggregate-call"`;
  - **inherited** (ADR 016) **continues to hold**:
    `"qualified HAVING column references are not supported"`
    remains present in `sql_planner.rs` (the existing presence
    guard from ADR 016 keeps firing; ADR 017 does not relax
    it);
  - **inherited** (ADR 016): no `Expr::AggregateCall`
    handling in `bind_expr`;
  - **inherited**: no new `LogicalPlan::*` /
    `ExecutionPlan::*` / `FilterDisposition` / `MessageKind` /
    `TaskFailureKind` / `BudgetDimension` variant.

  No exact-helper-call-count guard is asserted for
  `rewrite_grouped_aggregate_calls`. Helper topology is private
  and a count guard would fail under harmless refactors (e.g.,
  extracting a helper that delegates to the walker).

## Files

Expected code changes:

- `crates/toron/src/sql_planner.rs`:
  - Rename pass (`HavingAggregate*` → `GroupedAggregate*`,
    walker / resolver / signature helpers, `having_scope` →
    `grouped_scope` field). The walker reject string is
    **not** renamed.
  - Walker `SqlExpr::CompoundIdentifier` arm removed; walker
    becomes shape-neutral.
  - `lower_having_predicate` gains a qualified-reference pre-
    pass with the unchanged reject string.
  - `LoweredSelect` private struct introduction.
  - `ResolveOutcome` enum introduction.
  - `GroupedAggregateScope::resolve` returns `ResolveOutcome`.
  - `resolve_grouped_aggregate_reference` and
    `rewrite_grouped_aggregate_calls` extended with
    `clause: &'static str` parameter.
  - `lower_select` return-type change to `LoweredSelect`.
  - `lower_query` named-field destructure + scope plumb into
    `lower_order_by`.
  - `lower_order_by` signature extension + walker invocation.
- `crates/toron/tests/group_by_test.rs`:
  - ORDER BY-only test cases (no HAVING).
  - Dual-rewrite test cases (HAVING + ORDER BY).
  - V1 exclusion regression tests.
  - Negative cases (ambiguity, qualified references,
    non-grouped, UnaryOp / arithmetic over aggregate refs).
  - Inherited-reject regression tests (joins under GROUP BY,
    DISTINCT, nested aggregates, aliased aggregates).
  - Case-insensitivity test.
- `crates/toron/tests/distributed_process_test.rs`:
  - Multi-process `ORDER BY <agg>` fixture.
  - Multi-process dual-rewrite fixture.
- `crates/toron/tests/architecture_test.rs`:
  - Add the five guards listed in Substrate above.

Updated documentation:

- `crates/toron/plans/adr/0017_order_by_aggregate_call.md`
- `crates/toron/plans/impl/0017_order_by_aggregate_call.md`
- `crates/toron/plans/wip/roadmap.md` — ADR 017 retires from
  "candidates" into "Implemented" with the cycle-shape
  triplet note.
- `~/.claude/projects/-home-me-work-nilket/memory/adr-015-
  cycle-shape.md` — update to record three instances of the
  cycle shape and the load-bearing property (source-level
  boundary clarity at named-reject granularity).

Not expected:

- new IR variants;
- new `FilterDisposition`, `JoinKind`, `JoinStrategy`,
  `MessageKind`, `TaskFailureKind`, `BudgetDimension`, or
  `TaskOutputBinding` variants;
- new `bind_expr` match arm;
- bench artifact;
- changes to `RuntimeFilterDescriptor`, `RuntimeFilterKind`,
  or the ADR 013/014 application hooks;
- changes to `TableQueryRequestMeta` or wire frame;
- planner cost-model knob;
- changes to ADR 011's aggregate-output naming, duplicate-name
  disambiguation, or `aggregate_field` contract;
- changes to ADR 015's detector site shape contract or ADR
  016's `Filter`-inside-gather contract;
- changes to `bind_comparison`'s literal coercion (ADR 016
  shipped this; ADR 017 inherits unchanged);
- changes to the executor's `Sort` dispatch.

## Phase 0 — Audit and shape decision

- **P0.1: `LoweredSelect` seam.** `lower_select` has a single
  caller at `sql_planner.rs:94` (`SetExpr::Select(select) =>
  self.lower_select(select)?`). Verified by grep. The
  `LoweredSelect`-struct return shape is contained. Named-
  field destructure (`LoweredSelect { plan,
  grouped_aggregate_scope }`) at the caller. Alternative
  shapes (tuple return, planner-local scope stack, threading
  via parameter) are rejected: a tuple makes the scope hand-
  off positional and easy to mis-thread; a stack for a one-
  use scope is heavier; forward-threading does not apply
  because the scope is not known until after `lower_select`
  returns.

- **P0.2: Rename scope.** Type and helper renames per ADR
  Decision §1 apply: `HavingAggregate*` types, walker /
  resolver / signature helper functions, and the
  `GroupedProjection` field (`having_scope` → `grouped_scope`).
  The walker reject string `"qualified HAVING column
  references are not supported"` is **not** renamed; it is
  relocated to `lower_having_predicate`'s pre-pass. Council
  policy: ship inline with the feature commit. Pre-cycle
  `chore:` slice is allowable but the default is single-
  coherent-slice.

- **P0.3: Walker recursion surface.** ADR 016's walker
  recurses through `SqlExpr::BinaryOp`, `IsNull`, `IsNotNull`,
  `Nested`, and previously rejected `SqlExpr::CompoundIdentifier`.
  ADR 017 removes the `CompoundIdentifier` reject arm; the
  walker becomes shape-neutral over non-aggregate-call shapes.
  ORDER BY accepts arbitrary `SqlExpr` in `OrderByExpr.expr`.
  The walker leaves non-matching shapes (including
  `CompoundIdentifier`) unchanged, so `ORDER BY sales.region`
  passes through to `lower_expr`'s existing
  `CompoundIdentifier` arm (`sql_planner.rs:730-733`) and
  resolves as a qualified column ref — ADR 015 behavior
  preserved. `ORDER BY -SUM(amount)` (UnaryOp wrapping
  Function) keeps the unrewritten `SqlExpr::Function`, which
  `lower_expr` rejects via catch-all (no UnaryOp arm in
  `lower_expr`). V1 exclusion §5 in the ADR. Likewise
  `ORDER BY SUM(amount) + 100` rewrites the inner function
  call but rejects on the surrounding `BinaryOp::Plus`
  arithmetic (no Plus arm in `lower_expr`). V1 exclusion §6.

- **P0.4: `ResolveOutcome` enum + clause-label parameter.**
  The walker and resolver carry `clause: &'static str` for
  user-facing message formatting only;
  `GroupedAggregateScope::resolve` returns
  `ResolveOutcome::{ Found(String), NotInSelect, Ambiguous }`.
  The caller (`resolve_grouped_aggregate_reference`) formats
  the message:

  ```rust
  match scope.resolve(&signature) {
      ResolveOutcome::Found(name) => Ok(name),
      ResolveOutcome::NotInSelect => Err(unsupported(format!(
          "{clause} aggregate-call must appear in SELECT list"
      ))),
      ResolveOutcome::Ambiguous => Err(unsupported(format!(
          "ambiguous {clause} aggregate-call"
      ))),
  }
  ```

  `&'static str` over an enum: two values, no behavior beyond
  message formatting. Internal planner parameter; no public
  API.

- **P0.5: `lower_order_by` signature.** Extended signature:

  ```rust
  fn lower_order_by(
      &self,
      order_by: &OrderBy,
      scope: Option<&GroupedAggregateScope>,
  ) -> Result<Vec<SortExpr>, FrontendError>
  ```

  When `scope.is_some()`, each `OrderByExpr.expr` rewrites
  through `self.rewrite_grouped_aggregate_calls(&expr.expr,
  scope_ref, "ORDER BY")` before `self.lower_expr(...)`. When
  `None`, existing behavior verbatim. Existing
  `interpolate` / `with_fill` / `exprs.is_empty()` checks
  stay.

- **P0.6: Detector inspection.** The fragmenter detector
  destructures `Sort { order_by, limit, offset, output_schema,
  input }` and clones `order_by` verbatim into the returned
  `TerminalSortGather` (`fragment.rs:626`). No code path
  branches on `SortExpr.expr` shape. Zero fragmenter change
  for ADR 017.

- **P0.7: Stage-dependency wiring (inherits ADR 015 / ADR
  016).** When `ORDER BY` is present, Stage 2 carries
  `upstream_stages: [Stage 1.id]`; Stage 1 carries
  `upstream_stages: [Stage 0.id]`. Terminal sort task carries
  `TaskOutputBinding::FinalResult`. ADR 017 plans land in this
  shape unchanged.

- **P0.8: Predicate resolution sanity.** The binder's
  `bind_expr` resolves a rewritten ORDER BY column-ref
  expression (post-rewrite:
  `SqlExpr::Identifier(Ident::new("sum"))`) against
  `Sort.input.schema()` via `resolve_column`. `Sort.input` is
  either `Aggregate` (no HAVING) or `Filter(Aggregate)` (with
  HAVING); both schemas equal the aggregate output schema
  (`Filter` preserves schema). Same code path ADR 015's sort-
  key binder uses.

- **P0.9: Case-insensitivity sanity.** Signature derivation
  runs through `lower_aggregate_function`, which normalizes
  the function name via `aggregate_function_name` (uppercase
  dispatch). `ORDER BY sum(amount)` over `SELECT SUM(amount)`
  matches case-insensitively. Phase 1F confirms via a planner
  test.

- **P0.10: Dual-rewrite scope read-only.** The
  `GroupedAggregateScope` is constructed once via
  `GroupedAggregateScope::from_projection` and consumed read-
  only through `&` references; no `&mut self` methods.
  Sequential reads from `lower_having_predicate` (inside
  `lower_select`) and `lower_order_by` (inside `lower_query`)
  are independent. Confirmed by reading the existing scope
  type's API surface; no `&mut` methods present.

- **P0.11: Architecture guards (semantic only).** Source-shape
  guards in `crates/toron/tests/architecture_test.rs`:
  - **Presence (new):** `"ORDER BY aggregate-call must appear
    in SELECT list"` and `"ambiguous ORDER BY aggregate-call"`
    appear in `sql_planner.rs`.
  - **Presence (inherited from ADR 016, continues to hold):**
    `"qualified HAVING column references are not supported"`
    remains present in `sql_planner.rs`. The string moves from
    inside the walker to `lower_having_predicate`'s pre-pass;
    the ADR 016 presence guard does not specify a line number
    and is unaffected.
  - **Inherited (ADR 016):** no `Expr::AggregateCall`
    handling in `bind_expr` body (source-level grep absence).
  - **Inherited:** no new `LogicalPlan::*` /
    `ExecutionPlan::*` / `FilterDisposition` /
    `MessageKind` / `TaskFailureKind` / `BudgetDimension`
    variant.

  **Not asserted:** exact helper-call-count for
  `rewrite_grouped_aggregate_calls`. Helper topology is
  private — a count guard would lock in the current
  call-graph shape and fail under harmless refactors
  (e.g., extracting a helper that delegates to the walker
  would change the count without changing semantics).

`// P0.x:` comments only where the audit finding is not
obvious from the code, per
`plans/conventions/no-planning-phase-names.md`.

## Phase 1 — Rename, return-shape change, walker plumb (six
slices)

`crates/toron/src/sql_planner.rs`. Phase 1 decomposes into six
slices that the implementer can keep visible in the diff via
careful staging. Each slice ships green on `cargo test --locked
-p toron`:

- **Slice 1A — Pure rename pass** (zero behavior diff). All
  `HavingAggregate*` → `GroupedAggregate*` renames; helper
  function renames (`having_aggregate_signature` →
  `grouped_aggregate_signature`, `rewrite_having_aggregate_
  calls` → `rewrite_grouped_aggregate_calls`,
  `resolve_having_aggregate_reference` →
  `resolve_grouped_aggregate_reference`);
  `GroupedProjection.having_scope` → `grouped_scope`. The
  reject string `"qualified HAVING column references are not
  supported"` is **not** renamed — it stays unchanged inside
  the walker for this slice and relocates to
  `lower_having_predicate` in Slice 1C. After this slice the
  entire existing suite passes unchanged (rename-only).

- **Slice 1B — `ResolveOutcome` enum + scope resolve refactor**.
  Introduce the `ResolveOutcome` enum.
  `GroupedAggregateScope::resolve(&self, signature)` returns
  `ResolveOutcome` instead of `Result<String, FrontendError>`.
  Add `resolve_grouped_aggregate_reference`'s
  `clause: &'static str` parameter, with message formatting
  per P0.4. `lower_having_predicate` updates to pass
  `"HAVING"` at this slice. ADR 016 tests continue to assert
  HAVING-prefixed messages.

- **Slice 1C — Walker `CompoundIdentifier` arm relocation +
  clause-label threading**. Two changes:
  - Remove the `SqlExpr::CompoundIdentifier` arm from
    `rewrite_grouped_aggregate_calls`. The walker becomes
    shape-neutral over non-aggregate-call expression shapes
    (`CompoundIdentifier` falls into the `_ => Ok(expr.clone())`
    catch-all).
  - Add a pre-pass to `lower_having_predicate` that walks the
    HAVING `SqlExpr` AST and rejects any
    `SqlExpr::CompoundIdentifier` it encounters with the
    unchanged string `"qualified HAVING column references are
    not supported"`. The pre-pass runs before the walker
    invocation.
  - Add `clause: &'static str` parameter to
    `rewrite_grouped_aggregate_calls`. Recursion forwards the
    label through `BinaryOp` / `IsNull` / `IsNotNull` /
    `Nested` arms. The `SqlExpr::Function` arm calls
    `resolve_grouped_aggregate_reference(function, scope,
    clause)`. `lower_having_predicate` continues to invoke
    with `"HAVING"`. User-visible HAVING reject behavior is
    byte-identical to ADR 016.

- **Slice 1D — `lower_select` return-shape change**. Introduce
  the `LoweredSelect` private struct:

  ```rust
  struct LoweredSelect {
      plan: LogicalPlan,
      grouped_aggregate_scope: Option<GroupedAggregateScope>,
  }
  ```

  Update `lower_select`'s signature to
  `Result<LoweredSelect, FrontendError>`. Update the return
  paths inside `lower_select` to construct `LoweredSelect {
  plan, grouped_aggregate_scope }` with the scope set
  appropriately. Update `lower_query`'s call site at
  `sql_planner.rs:94` to destructure named fields:

  ```rust
  let LoweredSelect { plan, grouped_aggregate_scope } = match &**body {
      SetExpr::Select(select) => self.lower_select(select)?,
      _ => return Err(unsupported("unsupported query body")),
  };
  ```

  `lower_order_by` still ignores the scope at this slice
  (signature not yet extended).

- **Slice 1E — `lower_order_by` signature extension**. Add
  `scope: Option<&GroupedAggregateScope>` parameter. Wire the
  walker invocation in the iteration loop. Update the call
  site in `lower_query` (single caller) to pass
  `grouped_aggregate_scope.as_ref()`. Pseudocode for the loop
  body:

  ```rust
  .map(|expr| {
      if expr.with_fill.is_some() {
          return Err(unsupported("ORDER BY WITH FILL is not supported"));
      }
      let lowered_expr = match scope {
          Some(scope_ref) => {
              let rewritten = self.rewrite_grouped_aggregate_calls(
                  &expr.expr,
                  scope_ref,
                  "ORDER BY",
              )?;
              self.lower_expr(&rewritten)?
          }
          None => self.lower_expr(&expr.expr)?,
      };
      Ok(SortExpr {
          expr: lowered_expr,
          descending: expr.asc == Some(false),
          nulls_first: expr.nulls_first.unwrap_or(false),
      })
  })
  ```

- **Slice 1F — Tests**. The new test cases for ORDER BY-only,
  dual-rewrite, V1 exclusions, and inherited rejects.
  Architecture-test additions for the new
  presence/count guards. All slices green on
  `cargo test --locked -p toron`.

Tests in `crates/toron/tests/group_by_test.rs` (added in 1F
or earlier slices where useful):

- **ORDER BY <agg>, no HAVING** —
  `SELECT region, SUM(amount) FROM sales GROUP BY region
  ORDER BY SUM(amount) DESC` lowers to `Sort(Exchange(1, [])
  (Aggregate(Exchange(K, [region])(Scan))))` with `order_by`
  containing `SortExpr(column "sum", desc=true,
  nulls_first=false)` plus ADR 010's total-order tie-break.
- **ORDER BY <agg> with LIMIT / OFFSET** —
  `... ORDER BY SUM(amount) DESC LIMIT 10 OFFSET 5` lowers
  with `limit = Some(10)`, `offset = 5`.
- **Dual-rewrite: HAVING + ORDER BY** —
  `... HAVING SUM(amount) > 1000 ORDER BY SUM(amount) DESC`
  lowers to `Sort(Exchange(1, [])(Filter(Aggregate(Exchange
  (K, [region])(Scan)))))`. Both predicates rewrite to the
  aggregate-output column ref.
- **Mixed: HAVING <agg> + ORDER BY <group-key>** —
  `... HAVING SUM(amount) > 1000 ORDER BY region` lowers
  without invoking the rewrite on the ORDER BY side; group-
  key reference resolves naturally.
- **Mixed: HAVING <group-key> + ORDER BY <agg>** —
  `... HAVING region = 'EMEA' ORDER BY SUM(amount)` lowers
  without invoking the rewrite on the HAVING side; ORDER BY
  rewrites.
- **Ordinal ORDER BY still works** —
  `... ORDER BY 2 DESC` continues to lower as ADR 015 today,
  unchanged. The walker doesn't fire (no `SqlExpr::Function`
  match).
- **Case-insensitivity** — `SELECT region, sum(amount) ...
  ORDER BY SUM(amount)` lowers identically to the
  uppercase variant.
- **Different-argument non-match** — over projection
  `(region, SUM(amount))`, `ORDER BY SUM(other) DESC`
  rejects with `"ORDER BY aggregate-call must appear in
  SELECT list"`.
- **V1 exclusion §1 (no GROUP BY, catch-all reject)** —
  `SELECT SUM(amount) FROM sales ORDER BY SUM(amount)`
  rejects via the generic `"unsupported expression"` catch-
  all. Test asserts failure *kind* (any unsupported-
  expression frontend error), not exact message — the catch-
  all is intentionally generic and ADR 017 does not pin its
  format.
- **V1 exclusion §2 (not in SELECT list)** —
  `SELECT region FROM sales GROUP BY region ORDER BY
  SUM(amount)` rejects with `"ORDER BY aggregate-call must
  appear in SELECT list"`.
- **V1 exclusion §3 (ambiguity)** — three forms:
  - `SELECT region, COUNT(*), COUNT(amount) FROM sales
    GROUP BY region ORDER BY COUNT(*)` — duplicated
    `"count"` output name → ambiguous.
  - `SELECT region, COUNT(*), COUNT(*) FROM sales GROUP BY
    region ORDER BY COUNT(*)` — two structural matches →
    ambiguous.
  - `SELECT count, COUNT(amount) FROM events GROUP BY count
    ORDER BY COUNT(amount)` — group-key collision →
    ambiguous.
- **V1 exclusion §4 (HAVING qualified ref preserved; ORDER
  BY qualified ref unchanged)** — two regression cases:
  - `SELECT region, COUNT(*) FROM sales GROUP BY region
    HAVING t.region = 'EMEA'` rejects with the unchanged
    `"qualified HAVING column references are not supported"`
    (relocated to the `lower_having_predicate` pre-pass; the
    user-visible message is byte-identical to ADR 016).
  - `SELECT region, COUNT(*) FROM sales GROUP BY region
    ORDER BY sales.region` lowers successfully (qualified
    ORDER BY column reference passes through the walker
    unchanged and resolves via `lower_expr`'s existing
    `CompoundIdentifier` arm — ADR 015 behavior preserved).
- **V1 exclusion §5 (UnaryOp over aggregate)** —
  `... ORDER BY -SUM(amount)` rejects via the catch-all
  (`lower_expr` has no UnaryOp arm).
- **V1 exclusion §6 (arithmetic over aggregate)** —
  `... ORDER BY SUM(amount) + 100` rewrites the
  `SUM(amount)` to a column ref then rejects on
  `BinaryOperator::Plus` (`lower_expr` has no Plus arm).
- **Inherited rejects §7-§10** — pin one regression case
  each (joins under GROUP BY, DISTINCT, nested aggregates,
  aliased aggregates).
- **No-`ORDER BY <agg>` fast paths unchanged** — ADR 011
  two-stage and ADR 015 three-stage plans produced unchanged
  when ORDER BY uses ordinals or group keys only.

## Phase 2 — Fragmenter inspection (zero-code)

`crates/toron/src/distributed/fragment.rs`: no changes.

`is_terminal_sort_over_grouped_aggregate` matches the shape
`Sort -> Exchange(1, []) -> ... -> Aggregate -> Exchange(K,
...) -> input`, with the optional `Filter` between the
terminal-gather `Exchange` and the `Aggregate` (ADR 016
extension). ADR 017's plans land in exactly these shapes with
rewritten `SortExpr.expr` values; the detector inspects node
identity, not sort-key expression shape.

Tests in `crates/toron/tests/group_by_test.rs`:

- **Detector match (no HAVING)** — construct
  `Sort(Exchange(1, [])(Aggregate(Exchange(K, [region])
  (Scan))))` with `SortExpr.expr = Expr::column("sum")` (the
  rewritten form). Fragment; assert two `StagePlan`s with the
  ADR 015 detector path firing.
- **Detector match (with HAVING)** — construct
  `Sort(Exchange(1, [])(Filter(Aggregate(Exchange(K,
  [region])(Scan)))))` with both `Filter.predicate` and
  `SortExpr.expr` referencing aggregate-output columns.
  Fragment; assert three `StagePlan`s with the ADR 016
  detector extension firing.
- **Negative-shape regression** — ADR 015 / ADR 016 negative
  shape tests stay green: `Sort(Exchange(1, [])(Project
  (Aggregate(Exchange))))` still rejects with
  `MultipleExchangeBoundaries`; `Sort(Exchange(1, [])(Filter
  (Project(Exchange))))` still rejects.

These tests prove the zero-fragmenter-code claim: a future
fragmenter refactor that adds a sort-key-expression gate to the
detector fails the ADR 017 SQL fixture (it would lower
successfully through the planner but reject at the fragmenter).

## Phase 3 — Verification across layers

**Binder coverage** (`group_by_test.rs`):

- `ORDER BY SUM(amount) DESC` over projection `(region,
  SUM(amount))` rewrites to a column ref against the aggregate
  output schema; binder resolves via existing `resolve_column`
  path. Assert the bound `SortExpr.expr` is the expected
  schema column.
- `ORDER BY region` resolves directly against the group-key
  field (no rewrite).
- Total-order tie-break: `ORDER BY SUM(amount)` over `(region,
  SUM(amount), COUNT(*))` results in the user key followed by
  every remaining output column ascending `NULLS LAST` per ADR
  010.

**Executor coverage** (`group_by_test.rs`):

- End-to-end execution of `SELECT region, SUM(amount) FROM
  sales GROUP BY region ORDER BY SUM(amount) DESC LIMIT 10`
  against an in-memory storage fixture returns the top 10
  regions by sum in descending order.
- Dual-rewrite execution: `... HAVING SUM(amount) > 1000
  ORDER BY SUM(amount) DESC LIMIT 10 OFFSET 1` returns the
  correct sliced + sorted + filtered output.
- `OFFSET 0` (default) composes correctly.

**In-process digest parity** (`group_by_test.rs`):

- The SQL `ORDER BY <agg>` path and a hand-built
  `Sort(Exchange(1, [])(Aggregate(Exchange(K, [region])
  (Scan))))` with `SortExpr.expr = Expr::column("sum")`
  produce digest-equal Arrow output.
- The dual-rewrite SQL path and a hand-built
  `Sort(Exchange(1, [])(Filter(Aggregate(Exchange(K,
  [region])(Scan)))))` produce digest-equal Arrow output.

**Multi-process digest parity** (`distributed_process_test.rs`):

- A two- and four-worker cluster running the `ORDER BY <agg>`
  fixture produces digest-equal results across worker counts.
- The dual-rewrite fixture produces deterministic, digest-
  equal output across worker counts.

## Phase 4 — Documentation and roadmap

Implementation promotion completes with:

- `plans/adr/0017_order_by_aggregate_call.md` Status flipped to
  "Promoted and implemented."
- this impl plan Status flipped similarly.
- `plans/wip/roadmap.md` ADR 017 entry retires from
  "candidates" into "Implemented" with the cycle-shape
  triplet note: ADR 015 (`Sort` + `Aggregate`), ADR 016
  (`Filter` over `Aggregate`), ADR 017 (`ORDER BY` aggregate-
  call). Three instances stabilize the cycle shape.
- Auto-memory `adr-015-cycle-shape` updates to record that the
  pattern now has three instances; the load-bearing property
  is "source-level boundary clarity at named-reject
  granularity, no silent acceptance."

## Verification

Minimum:

```text
cargo fmt --all --check
cargo check --workspace --locked
cargo test --locked -p toron --test group_by_test --no-fail-fast
cargo test --locked -p toron --test sort_ordering_test --no-fail-fast
cargo test --locked -p toron --test distributed_process_test --no-fail-fast
cargo test --locked -p toron --test architecture_test --no-fail-fast
git diff --check
```

Promotion:

```text
cargo test --locked --workspace --no-fail-fast
```

## Risks + mitigations

- **R1: Walker-arm relocation breaks user-visible HAVING
  reject behavior.** Removing the walker's
  `CompoundIdentifier` arm without adding the
  `lower_having_predicate` pre-pass would silently accept
  `HAVING t.col` — a behavior regression on ADR 016.
  Mitigation: Slice 1C performs the removal and the pre-pass
  addition atomically; ADR 016's HAVING-qualified-reference
  regression test continues to assert the byte-identical
  reject message. The presence guard at the architecture-
  test level continues to fire on the string (the string
  moves sites but stays in `sql_planner.rs`).
- **R2: Tuple-return ripple.** `lower_select` has one caller
  today (`lower_query`); verified by grep. A future caller
  would need to destructure. Mitigation: `lower_select` is
  private to `SqlPlanner`; the signature change is contained.
  Future ADRs adding callers pay the seam cost at that point.
- **R3: `ResolveOutcome` enum churn vs HAVING tests.** The
  refactor from `Result<String, FrontendError>` to
  `ResolveOutcome` runs in Slice 1B. ADR 016's HAVING tests
  assert on user-facing message text formatted by the caller;
  the caller now formats from `ResolveOutcome` with
  `clause = "HAVING"`. The output text remains
  byte-identical (`"HAVING aggregate-call must appear in
  SELECT list"`, `"ambiguous HAVING aggregate-call"`).
  Mitigation: Slice 1B includes a fixture test pinning the
  HAVING messages remain byte-identical.
- **R4: ADR 015 detector regression.** Extending the call
  graph through the walker must not change the no-`ORDER BY-
  <agg>` shape behavior. Mitigation: Phase 2 preserves ADR
  015 / ADR 016 negative-shape tests verbatim; the rewrite
  walker produces `SortExpr.expr` values the detector doesn't
  inspect.
- **R5: Dual-rewrite scope mutation.** Two walks against the
  same `GroupedAggregateScope` could in principle mutate
  state. Mitigation: P0.10 confirms the scope is constructed
  once via `from_projection` and consumed read-only through
  `&` references; no `&mut self` methods. Slice 1F adds a
  dual-rewrite test that pins independence.
- **R6: Catch-all reject for non-grouped ORDER BY agg
  references is ugly.** `"unsupported expression: Function
  { name: ObjectName([SUM]), args: ..., ...}"` is hostile to
  users. Mitigation: ADR §10 acknowledges; the catch-all is
  functionally correct (rejects the unsupported case), and
  lifting it into a named reject is non-load-bearing for
  cycle-shape integrity. Scalar-aggregate ORDER BY is a
  future ADR's surface.
- **R7: Helper-call-count guard would pin private topology.**
  An earlier draft proposed asserting `rewrite_grouped_
  aggregate_calls` is invoked at exactly two sites. Rejected:
  helper topology is private; a refactor that extracts a thin
  delegating helper would change the count without changing
  semantics, failing the guard spuriously. Mitigation: do not
  assert call-site counts. Semantic guards (presence of new
  reject strings, continued presence of the HAVING-qualified-
  reference reject string, no new IR / `bind_expr` /
  disposition variants) are the load-bearing set.
- **R8: ORDER BY <agg> over a future post-aggregate
  projection.** ADR 011 currently rejects aliases on
  aggregates and other post-aggregate projection shapes. A
  future ADR widening that surface would need to consult the
  scope's output-name multiset — the multiset becomes the
  authoritative output-name catalog. Mitigation: not ADR
  017's problem; flagged for ADR 018 (post-aggregate
  projection) consideration in the handoff section.
- **R9: ADR 016 test failures during 1A.** Tests that
  reference the renamed types / functions by name (e.g.,
  `HavingAggregateSignature::Sum(...)` in unit-test
  constructors, if any) need to update in 1A. Mitigation:
  `grep -rn "HavingAggregate" crates/toron/tests/` before
  Slice 1A; update all references in the same slice.

## Definition of done

- The renames in Slice 1A land: `HavingAggregate*` →
  `GroupedAggregate*`, walker / resolver / signature helpers
  renamed, `GroupedProjection.having_scope` →
  `grouped_scope`. The walker reject string is **not** renamed.
- Slice 1C: walker's `SqlExpr::CompoundIdentifier` arm is
  removed; the walker is shape-neutral over non-aggregate-
  call shapes. `lower_having_predicate` has a pre-pass that
  rejects qualified column refs with the unchanged string
  `"qualified HAVING column references are not supported"`.
- `LoweredSelect` private struct is introduced; `lower_select`
  returns `Result<LoweredSelect, FrontendError>`.
- `lower_query` destructures `LoweredSelect { plan,
  grouped_aggregate_scope }` and passes
  `grouped_aggregate_scope.as_ref()` into `lower_order_by`.
- `lower_order_by` accepts
  `scope: Option<&GroupedAggregateScope>` and invokes the
  walker per ORDER BY expr when `scope.is_some()`.
- `ResolveOutcome` enum is introduced and used by both
  `lower_having_predicate` and the new ORDER BY rewrite path.
- The walker and resolver carry a `clause: &'static str`
  parameter; ambiguity messages format with the clause label
  at the caller (not inside the scope).
- The two new named rejects (`"ORDER BY aggregate-call must
  appear in SELECT list"`, `"ambiguous ORDER BY aggregate-
  call"`) are in place and pinned by tests.
- `"qualified HAVING column references are not supported"`
  remains present in `sql_planner.rs` (now at
  `lower_having_predicate`'s pre-pass).
- `ORDER BY sales.region` lowers successfully — regression
  test pins ADR 015 behavior preservation.
- ADR 016's HAVING-prefixed reject messages remain byte-
  identical after the `ResolveOutcome` refactor and the
  walker-arm relocation.
- Fragmenter requires zero new code; ADR 015 / ADR 016
  detectors fragment ADR 017 plans correctly. Verified by
  hand-built detector tests.
- ADR 011 / ADR 015 / ADR 016 fast paths preserved.
- Every V1 exclusion (§1-§10) has a pinned regression case.
- In-process digest parity holds between SQL and a hand-built
  logical plan for ORDER BY-only and dual-rewrite cases.
- Multi-process digest parity holds across worker counts on
  the ORDER BY-only and dual-rewrite fixtures.
- Architecture guards green (semantic guards per P0.11; no
  helper-call-count guard asserted).
- Full Toron + Wendelon workspace test suites green.
