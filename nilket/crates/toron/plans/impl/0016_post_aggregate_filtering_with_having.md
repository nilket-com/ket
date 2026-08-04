# Implementation Plan: ADR 016 — Post-Aggregate Filtering with `HAVING`

**Companion ADR:**
`plans/adr/0016_post_aggregate_filtering_with_having.md`

## Status

Promoted and implemented.

This is a small cross-file composition: one planner
reject removed, one planner-side rewrite added, one
fragmenter detector extended, test coverage at four
layers (planner / binder / fragmenter / integration).
No new IR variant, no wire change, no operator surface
change, no `bind_expr` aggregate-call arm. A narrow
`bind_comparison` numeric-literal coercion is part of
the shipped binder behavior.

## Goal

Land `GROUP BY ... HAVING ...` (with and without
`ORDER BY`) end-to-end:

- planner accepts the composition and lowers to
  `Filter(<rewritten>)(Aggregate(Exchange(K,
  group_expr)(...)))` or, with `ORDER BY`,
  `Sort(Exchange(1, [])(Filter(<rewritten>)
  (Aggregate(Exchange(K, group_expr)(...)))))`;
- the planner-side rewrite of `HAVING` aggregate-
  call references matches on a planner-local
  structural signature derived from the lowered
  aggregate output (not on raw
  `sqlparser::ast::Function` AST), keyed on
  `ColumnRef` (see P0.2), making the match
  case-insensitive and argument-normalized;
- binder resolves the rewritten predicate against
  the aggregate output schema via the existing
  `bind_expr` + `resolve_column` path, with
  comparison literals coerced in `bind_comparison`
  after expression types are known; no new
  `bind_expr` arm;
- fragmenter Site 1 (no `ORDER BY`) requires zero
  new code — the generic single-exchange path
  fragments `Filter(Aggregate(Exchange_K(scan)))`
  cleanly via the existing recursive descent
  through `Filter | Project | Aggregate | Sort`;
- fragmenter Site 2 (with `ORDER BY`) extends the
  ADR 015 detector at `fragment.rs:561` to
  recognize an optional `Filter` between the
  terminal-gather `Exchange` and the `Aggregate`;
- ADR 011's and ADR 015's existing fast paths are
  preserved verbatim;
- integration coverage: in-process digest parity
  with a hand-built logical plan and multi-process
  digest parity on a distributed cluster fixture.

## Substrate

- `crates/toron/src/sql_planner.rs`
  - `SqlPlanner::lower_select` (line 122+) — owns
    the `"HAVING is not supported"` reject at line
    171-172 that ADR 016 removes. The exhaustive
    `Select` destructure (lines 123-144) binds
    `having: Option<Expr>` at line 138; ADR 016
    routes the field to the new lowering path
    rather than the reject.
  - `lower_grouped_projection` (line 363+) —
    produces the `Vec<(AggregateFunction,
    Vec<Expr>)>` for the `Aggregate` plan. ADR 016
    pairs each lowered aggregate with its
    `aggregate_field` output name to build the
    rewrite map; the rewrite-map construction
    helper shape (sibling function vs signature
    extension) is Phase 1's implementation choice.
  - `lower_aggregate_function` (line 403+) — the
    case-normalization site whose lowered
    `(AggregateFunction, Vec<Expr>)` output is the
    raw material the planner derives the
    `HavingAggregateSignature` from. Returns
    `(AggregateFunction, Vec<Expr>)` from a
    `sqlparser::ast::Function`, normalizing the
    function name via
    `aggregate_function_name`'s uppercase
    dispatch.
  - `aggregate_function_name` — detects aggregate-
    call shape in the `HAVING` AST before invoking
    `lower_aggregate_function`. Returns
    `Some("COUNT" | "SUM" | "LINEAR_REGRESS" |
    "APPROX_QUANTILE")` for aggregate calls,
    `None` otherwise.
  - `lower_query` (line 49+) — owns the existing
    `Sort + Exchange(1, [])` wrap when `ORDER BY`
    is present. Unchanged; the wrap composes over
    the Filter naturally.
- `crates/toron/src/binder.rs`
  - `bind_expr` (line 1131+) — resolves predicates
    against an input schema; no
    `Expr::AggregateCall` arm and ADR 016 does not
    add one.
  - `bind_comparison` — coerces an `Int32` numeric
    literal to `Int64` or `Float64` when the
    opposite side is a typed, non-literal expression
    of that type. This handles aggregate-output
    comparisons such as `HAVING SUM(amount) > 5`;
    literal-vs-literal comparisons are unchanged.
  - `Filter` binder (lines 78-100) — selects
    `FilterDisposition::EvaluateInToron` for
    predicates that cannot push to PBC. The post-
    aggregate filter naturally takes
    `EvaluateInToron`; no disposition or
    `bind_expr` code change.
  - `aggregate_field` (lines 1287-1301) —
    generates `"count"`, `"sum"`, `"linreg"`,
    `"quantile"`. Source of the rewrite map's
    output field names.
- `crates/toron/src/distributed/fragment.rs`
  - `fragment_plan` generic non-join path (lines
    88-162) — fragments `Filter(Aggregate
    (Exchange_K(scan)))` via the
    `count_exchanges` / `find_exchange` /
    `replace_exchange_with_scan` recursion through
    `Filter | Project | Aggregate | Sort` (lines
    825-857, 901-965). **Zero change for Site 1.**
  - `is_terminal_sort_over_grouped_aggregate`
    (line 561) and
    `fragment_terminal_sort_over_grouped_aggregate`
    (line 621) — ADR 015's detector and three-
    stage builder. ADR 016 extends both.
- `crates/toron/src/exec.rs`
  - `ExecutionPlan::Filter` dispatch (lines
    494-516) — input-polymorphic; calls
    `execute_bound(input)` with no type gate. No
    change.
- `crates/toron/src/ir/execution.rs`
  - `ExecutionPlan::Filter` variant (lines 81-85)
    with `disposition: FilterDisposition` field.
    `FilterDisposition` enum (lines 57-60) with
    `FullyPushed` and `EvaluateInToron`. No
    change.
- `crates/toron/tests/group_by_test.rs` — ADR 016
  adds planner + execution + fragmenter test
  cases, including `HAVING + ORDER BY`.
- `crates/toron/tests/sort_ordering_test.rs` —
  existing ADR 015 ordering coverage remains
  unchanged; ADR 016 composition cases live in
  `group_by_test.rs`.
- `crates/toron/tests/distributed_process_test.rs`
  — ADR 016 adds the distributed-cluster `HAVING`
  fixture.
- `crates/toron/tests/architecture_test.rs` —
  ADR 016 adds a guard for the absence of
  `"HAVING is not supported"`.

## Files

Expected code changes:

- `crates/toron/src/sql_planner.rs` — remove the
  `having.is_some()` reject at line 171-172; add
  the `HAVING` lowering branch in
  `lower_select`'s grouped-aggregate path; add
  the rewrite-map construction (sibling helper or
  signature extension per Phase 1 choice); add
  the `"HAVING without GROUP BY is not
  supported"` reject (V1 exclusion §1); add the
  `"HAVING aggregate-call must appear in SELECT
  list"` reject (V1 exclusion §2).
- `crates/toron/src/distributed/fragment.rs` —
  extend the detector's return struct with an
  `Option<(ExecutionExpr, FilterDisposition)>`
  having field; extend
  `is_terminal_sort_over_grouped_aggregate` to
  match an optional `Filter` between the
  terminal-gather `Exchange` and the `Aggregate`;
  extend the builder to wrap `aggregate_plan` in
  `ExecutionPlan::Filter` when the field is
  `Some`.
- `crates/toron/src/binder.rs` — add the narrow
  `bind_comparison` numeric-literal coercion for
  typed non-literal comparison operands.
- `crates/toron/tests/group_by_test.rs` —
  planner, binder, fragmenter, and execution
  coverage, including `HAVING + ORDER BY`
  composition cases.
- `crates/toron/tests/distributed_process_test.rs`
  — multi-process digest parity fixture.
- `crates/toron/tests/architecture_test.rs` —
  source-shape guard pinning the absence of the
  `"HAVING is not supported"` reject string and
  the absence of any new variant.

Expected documentation:

- `crates/toron/plans/adr/0016_post_aggregate_filtering_with_having.md`
- `crates/toron/plans/impl/0016_post_aggregate_filtering_with_having.md`
- `crates/toron/plans/wip/roadmap.md`

Not expected:

- new IR variants;
- new `FilterDisposition`, `JoinKind`,
  `JoinStrategy`, `MessageKind`,
  `TaskFailureKind`, `BudgetDimension`, or
  `TaskOutputBinding` variants;
- new `bind_expr` match arm;
- bench artifact;
- changes to `RuntimeFilterDescriptor`,
  `RuntimeFilterKind`, or the ADR 013/014
  application hooks;
- changes to `TableQueryRequestMeta` or wire
  frame;
- planner cost-model knob;
- changes to ADR 011's aggregate-output naming,
  duplicate-name disambiguation, or
  `aggregate_field` contract;
- changes to ADR 015's detector site shape
  contract beyond the optional-`Filter`
  extension.

## Phase 0 — Audit and shape decision

- **P0.1: Projection metadata.** Extend
  `lower_grouped_projection` to return a private
  `GroupedProjection` struct carrying both the
  lowered `aggr_expr` and a `HavingAggregateScope`
  recording each projected aggregate's structural
  signature, generated output name, and the
  collection of all output names visible in the
  aggregate output schema (group-key names plus
  aggregate field names — see P0.3). Signature
  extension is the recommended path: projection
  validation and `HAVING` metadata generation share
  one pass, and the call site (a single caller in
  `lower_select`) is updated cleanly. A sibling
  helper walking the same projection slice is an
  acceptable alternative; the contract is the
  metadata content, not the helper shape. Internal
  helper names (`GroupedProjection`,
  `HavingAggregateScope`,
  `HavingAggregateSignature`, `lower_having_expr`,
  `grouped_aggregate_output_name`) are illustrative
  — not pinned by the architecture guards.

- **P0.2: Structural signature key.** `Expr` does
  not derive `PartialEq` (`ir.rs:68`) and
  `AggregateFunction` derives `PartialEq` but not
  `Eq` / `Hash` (`ir.rs:107`). The match key
  therefore cannot be `(AggregateFunction,
  Vec<Expr>)` directly. Introduce a private
  planner-local signature enum keyed on `ColumnRef`
  (which already derives `PartialEq, Eq, Hash` at
  `ir.rs:46-50`), narrow to the V1 grouped subset:

  ```rust
  #[derive(Clone, Debug, PartialEq, Eq, Hash)]
  enum HavingAggregateSignature {
      CountAll,
      Count(ColumnRef),
      Sum(ColumnRef),
  }
  ```

  `lower_aggregate_function`'s `(AggregateFunction,
  Vec<Expr>)` output maps to this signature when
  the lowered arg is `Expr::Column(ColumnRef)`
  (the V1 case for `COUNT(<col>)` / `SUM(<col>)`).
  Aggregate calls whose lowered args fall outside
  this shape (`COUNT(<literal>)`, etc.) take the
  "no signature match" path → "must appear in
  SELECT list". This avoids widening IR-level
  `Expr` equality semantics.

- **P0.3: Output-name mapping and scope.**
  Generated output names match
  `binder::aggregate_field`'s V1 subset
  (`CountAll` → `"count"`, `Count` →
  `"count"`, `Sum` → `"sum"`); add a private
  planner-local helper rather than making
  `binder::aggregate_field` public. The
  `HavingAggregateScope` records, for each
  projected aggregate, its `HavingAggregateSignature`
  and its generated output name; **it also records
  the set of group-key output names** so the
  ambiguity check (P0.4) catches group-key /
  aggregate-name collisions, not only
  aggregate-output collisions.

- **P0.4: Four-way ambiguity classification.**
  `lower_having_expr` classifies each aggregate-
  call reference per ADR §4. "Output name
  duplicated in scope" means the would-be
  substituted name appears more than once across
  the full aggregate output schema — *group-key
  names plus aggregate field names*, not only
  the projected aggregates:

  ```text
  zero structural matches              -> Unsupported("HAVING aggregate-call must appear in SELECT list")
  more than one structural match       -> Unsupported("ambiguous HAVING aggregate-call")
  one match + output name appears >1   -> Unsupported("ambiguous HAVING aggregate-call")
    times in full output schema
  one match + output name appears      -> Expr::column(output_name)
    exactly once
  ```

  Three test fixtures pin the policy:
  - `(COUNT(*), COUNT(amount))` ambiguous on
    duplicate aggregate output name `"count"`;
  - `(COUNT(*), COUNT(*))` ambiguous on two
    structural matches;
  - `SELECT count, COUNT(amount) FROM events
    GROUP BY count HAVING COUNT(amount) > 0`
    ambiguous on group-key/aggregate-name
    collision (group key `count` collides with
    `COUNT(amount)`'s generated `"count"`).

- **P0.5: Detector extension placement.** The
  optional-`Filter` match is inside
  `is_terminal_sort_over_grouped_aggregate`'s
  predicate body, not a separate detector. The
  narrow detector still fires *before* the
  generic non-join multi-exchange reject in
  `fragment_plan` (ADR 015 P0.2). The broader
  reject protects every other non-join two-
  exchange shape from silent acceptance; the
  narrow detector remains the single permitted
  exception (now with an optional `Filter` in one
  specific position).

- **P0.6: Stage-dependency wiring (inherits
  ADR 015).** When `ORDER BY` is present, Stage 2
  carries `upstream_stages: [Stage 1.id]`;
  Stage 1 carries `upstream_stages: [Stage 0.id]`.
  Terminal sort task carries
  `TaskOutputBinding::FinalResult`. Filter inside
  the aggregate consumer's plan is executed
  inline within Stage 1's task; no new stage
  barrier.

- **P0.7: Predicate resolution sanity.** The
  binder's `bind_expr` resolves the rewritten
  predicate (post-rewrite: a plain column-ref
  expression) against `input.schema()` via
  `resolve_column`. With the planner inserting
  `LogicalPlan::Filter { input:
  Aggregate { ... }, predicate: <rewritten> }`,
  the input schema is `Aggregate.output_schema =
  aggregate_schema(group_expr, aggr_expr)` —
  same schema ADR 015's sort-key binder already
  exercises. Phase 1 confirms via a unit-level
  binder test that `HAVING SUM(amount) > 100`
  over `(region, SUM(amount))` rewrites to a
  reference to the aggregate-output field at
  index 1 and resolves cleanly.

  The same binder pass owns numeric literal
  coercion for comparisons. `SUM(amount)` may
  bind to `Int64` while `5` parses as `Int32`;
  `bind_comparison` coerces that `Int32` literal
  to the typed non-literal operand's `Int64` or
  `Float64` type. Literal-vs-literal comparisons
  stay unchanged, and no aggregate-call handling
  is added to `bind_expr`.

- **P0.8: Case-insensitivity sanity.** The
  signature derivation runs through
  `lower_aggregate_function`, which normalizes
  the function name via `aggregate_function_name`
  (uppercase dispatch). Phase 1 confirms
  via a planner test that `SELECT sum(amount)
  FROM t GROUP BY r HAVING SUM(amount) > 100`
  lowers identically to the uppercase variant.

- **P0.9: `FilterDisposition` selection.** The
  PBC-push flow at `binder.rs:83-93` calls
  `bound_predicate.pbc_eligible()` →
  `compile_pbc(...)` → `attach_pbc_program(input,
  program)`. For a post-aggregate `HAVING`
  filter, eligibility may pass and compilation
  may succeed (column-ref + comparison + literal
  is structurally PBC-friendly), but
  `attach_pbc_program` returns `Err(input)`
  because the filter's input is an
  `ExecutionPlan::Aggregate`, not a `Scan`. The
  binder branches at line 87 hand the predicate
  to `EvaluateInToron`. No disposition or
  `bind_expr` code change.

- **P0.10: Architecture guards.** Source-shape
  guards in
  `crates/toron/tests/architecture_test.rs`:
  - No `"HAVING is not supported"` reject
    string survives in `sql_planner.rs` — pins
    the absence of the ADR 016-removed reject
    (mirrors ADR 015's
    `"GROUP BY with ORDER BY is not supported"`
    absence guard).
  - The planner-owned qualified-reference reject
    string remains present.
  - No new `LogicalPlan::*` or
    `ExecutionPlan::*` variant.
  - No new `FilterDisposition` variant.
  - No new `bind_expr` match arm (asserted by
    source-level grep absence of
    `Expr::AggregateCall` handling in
    `bind_expr`'s body).
  - Planner-local `aggregate_output_name` and
    binder-local `aggregate_field` continue to
    agree on generated aggregate output names for
    every `AggregateFunction` variant.
  - No new `MessageKind`, `TaskFailureKind`,
    `BudgetDimension`.

`// P0.x:` comments only where the audit finding
is not obvious from the code, per
`plans/conventions/no-planning-phase-names.md`.

## Phase 1 — Planner unblock and rewrite

`crates/toron/src/sql_planner.rs`:

- Remove the `having.is_some()` reject at line
  171-172. The `having: Option<Expr>` binding in
  the exhaustive destructure stays at line 138.
- In `lower_select`'s grouped-aggregate branch
  (lines 236-251), after constructing
  `aggr_expr` via `lower_grouped_projection`, if
  `having` is `Some`:
  - Build the `HavingAggregateScope` via
    `lower_grouped_projection`'s extended return
    (per P0.1), pairing each lowered aggregate's
    `HavingAggregateSignature` (per P0.2) with
    its planner-local generated output name (per
    P0.3).
  - Walk the `having` `SqlExpr` AST recursively.
    For each `SqlExpr::Function(func)`:
    - Probe
      `aggregate_function_name(&func.name)`. If
      `None`, leave the node unchanged.
    - If `Some`, call
      `self.lower_aggregate_function(func,
      AggregateSqlMode::Grouped)`. On lowering
      error, surface the error. On success,
      classify via the four-way ambiguity
      policy (P0.4): zero matches →
      `"HAVING aggregate-call must appear in
      SELECT list"`; multiple matches or
      duplicate output name →
      `"ambiguous HAVING aggregate-call"`;
      one unique match → substitute
      `SqlExpr::Identifier(Ident::new
      (output_field_name))`.
  - Lower the rewritten `SqlExpr` via
    `self.lower_expr(...)` to produce the
    `LogicalPlan::Filter` predicate.
  - Construct `LogicalPlan::Filter { input:
    Box::new(LogicalPlan::Aggregate { ... }),
    predicate: lowered_predicate }` and return
    it instead of the bare
    `LogicalPlan::Aggregate`.
- If `having.is_some() && group_keys.is_none()`,
  return `unsupported("HAVING without GROUP BY
  is not supported")` (V1 exclusion §1).

Tests in `crates/toron/tests/group_by_test.rs`:

- **Aggregate-call rewrite, canonical case** —
  `SELECT region, COUNT(*) FROM sales GROUP BY
  region HAVING COUNT(*) > 5` lowers to
  `Filter(<col_ref_to_"count"_field> > 5)
  (Aggregate(Exchange(K, [region])(Scan)))`.
- **Aggregate-call rewrite with ORDER BY** —
  `SELECT region, SUM(amount) FROM sales GROUP
  BY region HAVING SUM(amount) > 1000 ORDER BY
  2 DESC LIMIT 10 OFFSET 1` lowers to
  `Sort(Exchange(1, [])(Filter(Aggregate
  (Exchange(K, [region])(Scan)))))` with
  `limit = Some(10)`, `offset = 1`.
- **Group-key predicate, no rewrite** —
  `HAVING region = 'EMEA'` lowers without
  triggering the rewrite (group-key column ref
  resolves naturally against
  `Aggregate.output_schema`).
- **Case-insensitivity** — `SELECT region, sum
  (amount) FROM sales GROUP BY region HAVING
  SUM(amount) > 1000` lowers identically to the
  uppercase-projection variant.
- **Different-argument non-match** — over
  projection `(region, SUM(amount))`,
  `HAVING SUM(other_amount) > 1000` rejects
  with `"HAVING aggregate-call must appear in
  SELECT list"`. The HAVING signature `Sum
  (other_amount)` finds zero structural matches
  against the projection's `Sum(amount)`
  signature — different lowered `ColumnRef`
  arg. (Arithmetic-in-aggregate-arg cases like
  `SUM(amount + 0)` are not used as the
  normalization test: `lower_expr` has no
  arithmetic variants in `Expr` (`ir.rs:68-84`),
  so `lower_aggregate_function` rejects on arg
  lowering before the rewrite classification —
  a different reject path.)
- **V1 exclusion §1** — `SELECT COUNT(*) FROM
  sales HAVING COUNT(*) > 5` rejects with
  `"HAVING without GROUP BY is not supported"`.
- **V1 exclusion §2** — `SELECT region FROM
  sales GROUP BY region HAVING SUM(amount) >
  100` rejects with `"HAVING aggregate-call
  must appear in SELECT list"` — `SUM(amount)`
  is a recognized aggregate
  (`aggregate_function_name` returns `Some
  ("SUM")` at `sql_planner.rs`), lowers
  cleanly, finds zero structural matches in
  the projection's `HavingAggregateScope`, and
  takes the V1 exclusion §2 reject path.
  (Unrecognized function names such as `AVG`
  return `None` from `aggregate_function_name`
  and fall through unchanged to `lower_expr`,
  taking the existing unsupported-expression
  reject — that is a different path; the V1
  exclusion §2 test must use a recognized
  aggregate.)
- **V1 exclusion §3 (strict ambiguity)** — three
  forms reject with `"ambiguous HAVING
  aggregate-call"`:
  - `SELECT region, COUNT(*), COUNT(amount) FROM
    sales GROUP BY region HAVING COUNT(*) > 5`
    (one structural match, but `COUNT(*)` and
    `COUNT(amount)` both generate `"count"` →
    duplicated aggregate-output name).
  - `SELECT region, COUNT(*), COUNT(*) FROM
    sales GROUP BY region HAVING COUNT(*) > 5`
    (two structural matches).
  - `SELECT count, COUNT(amount) FROM events
    GROUP BY count HAVING COUNT(amount) > 0`
    (one structural match, but group-key
    `count` collides with `COUNT(amount)`'s
    generated `"count"`).
- **V1 exclusion §4** — `SELECT region,
  COUNT(*) FROM sales GROUP BY region HAVING
  t.region = 'EMEA'` rejects in the planner
  before lowering with `"qualified HAVING column
  references are not supported"` (aggregate
  output schema is unqualified, and V1 keeps
  qualifier handling out of the generic binder
  path).
- **`HAVING 2 > 100` literal predicate** —
  `SELECT region, COUNT(*) FROM sales GROUP BY
  region HAVING 2 > 100` lowers as a literal
  predicate (`2 > 100`, always false). Not an
  ordinal reference; the resulting plan
  filters out every row.
- **Generated-name form** — `SELECT region,
  COUNT(*) FROM sales GROUP BY region HAVING
  count > 0` resolves through the existing
  `resolve_column` against the aggregate output
  schema (the rewrite fires only on
  `SqlExpr::Function`). Duplicate-output-name
  cases inherit existing binder ambiguity
  behavior.
- **Inherited rejects §5-§10** — pin each with a
  regression case verifying ADR 016 did not
  slacken them:
  - `HAVING` over joins → `"GROUP BY over
    joins is not supported"`.
  - `HAVING` over `DISTINCT` → `"DISTINCT is
    not supported"`.
  - Computed `HAVING` predicate (`HAVING SUM
    (amount) + 100 > 1000`) → existing
    `bind_expr` arithmetic-on-column-ref
    reject.
  - Nested aggregate (`HAVING SUM(COUNT(*)) >
    5`) → existing `lower_aggregate_function`
    nested reject.
  - `HAVING raw_col > 5` referencing a non-
    output column → existing `resolve_column`
    "column not found".
  - `HAVING` with an aliased aggregate's alias
    → ADR 011's alias reject fires before
    HAVING lowering.
- **No-`HAVING` fast paths unchanged** — ADR 011
  two-stage and ADR 015 three-stage plans
  produced unchanged when `having.is_none()`.

## Phase 2 — Fragmenter detector extension

`crates/toron/src/distributed/fragment.rs`:

Extend `is_terminal_sort_over_grouped_aggregate`'s
return struct with an `Option<(ExecutionExpr,
FilterDisposition)>` having field.

Extend `is_terminal_sort_over_grouped_aggregate`
(`fragment.rs:561`):

- Match the `Sort -> Exchange(1, []) -> ...`
  prefix as today.
- Peek the node between the terminal-gather
  `Exchange` and the `Aggregate`. If it is
  `ExecutionPlan::Filter { input, predicate,
  disposition }`, extract `(predicate,
  disposition)` into the having field, descend
  through the filter's input, and require an
  `ExecutionPlan::Aggregate` directly under it.
  If it is `ExecutionPlan::Aggregate` directly,
  the having field is `None` and the detector
  proceeds as today.
- Any other intervening node (e.g. `Project`,
  `Sort`, nested `Filter`) returns `None` —
  detector remains narrow; the generic non-
  join multi-exchange reject covers the
  rejection path.

Extend `fragment_terminal_sort_over_grouped_aggregate`
(`fragment.rs:621`):

- When the having field is `Some`, wrap the
  constructed `aggregate_plan` (lines 647-652)
  in `ExecutionPlan::Filter { input:
  Box::new(aggregate_plan), predicate,
  disposition }` before binding it to the
  aggregate consumer task.
- Stage 0 (producer), Stage 1 task bindings
  (`ShuffleConsumer` for the group-keyed
  exchange, `ShuffleProducer` for the terminal-
  gather exchange), and Stage 2 (terminal sort)
  are all unchanged.

Tests in `crates/toron/tests/group_by_test.rs`:

- **Site 1 (no `ORDER BY`)** — construct
  `Filter(Aggregate(Exchange))` directly,
  fragment, assert two `StagePlan`s with the
  consumer's plan equal to
  `Filter(Aggregate(Scan))`. The Site 1 zero-
  code claim is verified by this test directly
  — a future fragmenter refactor that breaks
  the generic-path recursion fails this test
  loudly.
- **Site 2 (with `ORDER BY`)** — the extended
  detector fires; three `StagePlan`s emit:
  - Stage 0: producer, group-keyed shuffle,
    same as ADR 015.
  - Stage 1: aggregate consumer, plan is
    `Filter(Aggregate(Scan))`, output is
    `ShuffleProducer(partition_count = 1,
    key = [])`.
  - Stage 2: terminal sort, `upstream_stages
    == [Stage 1.id]`, output is
    `FinalResult`.
- **Negative-shape control: Project between
  exchanges** — `Sort(Exchange(1, [])(Project
  (Aggregate(Exchange(K, group_expr)
  (Scan)))))` still rejects with
  `MultipleExchangeBoundaries`.
- **Negative-shape control: Filter without
  Aggregate** — `Sort(Exchange(1, [])(Filter
  (Project(Exchange(K, key)(Scan)))))` still
  rejects with `MultipleExchangeBoundaries`.
- **ADR 015 detector regression** — the
  original no-`Filter` `Sort(Exchange(1, [])
  (Aggregate(Exchange(K, group_expr)
  (Scan))))` shape still matches and fragments
  identically to ADR 015's test expectations.

## Phase 3 — Verification across layers

**Binder coverage**
(`group_by_test.rs`):

- `HAVING SUM(amount) > 1000` rewrites to a
  reference to the aggregate-output field for
  `SUM(amount)`; binder resolves it via
  existing `resolve_column` path.
- `HAVING region = 'EMEA'` resolves directly
  against the group-key field (no rewrite).
- `HAVING COUNT(other) > 5` over projection
  `(region, COUNT(*), COUNT(other))` rejects
  as ambiguous. The structural match
  `(Count, other)` is unique, but both
  projection aggregates produce generated
  output name `"count"`, so the output-name-
  in-scope count is 2. Per the four-way
  classification (P0.4), this rejects with
  `"ambiguous HAVING aggregate-call"`, not
  silent first-match.
- `HAVING SUM(amount) > 5` over projection
  `(region, COUNT(*), SUM(amount))` rewrites
  successfully — one structural match
  `(Sum, amount)` with the unique output name
  `"sum"` in scope.
- The right-hand literal in `HAVING SUM(amount)
  > 5` binds as the aggregate output type when
  `SUM(amount)` is `Int64`; literal-vs-literal
  comparisons remain unchanged.
- Disposition is `EvaluateInToron`; no PBC-
  push attempt succeeds. Phase 3 explicitly
  asserts this on at least one fixture.

**Executor coverage** (`group_by_test.rs`):

- End-to-end execution of `SELECT region,
  SUM(amount) FROM sales GROUP BY region
  HAVING SUM(amount) > 1000 ORDER BY 2 DESC
  LIMIT 10` against an in-memory storage
  fixture returns only the top 10 regions
  whose total sum exceeds 1000, in descending
  order.
- `LIMIT` + `OFFSET` compose with `HAVING`.
- Filter happens *after* aggregation: with a
  fixture where individual rows exceed the
  threshold but their group sum does not, the
  group is excluded.

**In-process digest parity**
(`group_by_test.rs`):

- The SQL `HAVING` path and a hand-built
  `LogicalPlan::Filter(<predicate>)
  (LogicalPlan::Aggregate(LogicalPlan::
  Exchange(K, group_expr)(Scan)))` produce
  digest-equal Arrow output for the same
  fixture.
- The SQL `HAVING + ORDER BY` path and a hand-
  built `Sort(Exchange(1, [])(Filter
  (Aggregate(Exchange(K, group_expr)
  (Scan)))))` produce digest-equal Arrow
  output.

**Multi-process digest parity**
(`distributed_process_test.rs`):

- A two- and four-worker cluster running the
  same `HAVING` fixture produces digest-equal
  results across worker counts.
- The same composition with `ORDER BY +
  LIMIT + OFFSET` produces deterministic,
  digest-equal output.

## Phase 4 — Documentation and roadmap

Implementation promotion completed with:

- `plans/adr/016_post_aggregate_filtering_with_
  having.md` Status flipped to "Promoted and
  implemented.";
- this impl plan Status flipped similarly;
- `plans/wip/roadmap.md` ADR 016 section
  retired from "Promoted: ADR 016 plan"
  scaffolding and reframed forward-looking to
  ADR 017 candidates;
- no wip iteration artifacts to retire here
  (the plan commit already retired them).

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

- **R1: Case-sensitivity / argument-shape drift
  between projection and HAVING.** If the
  rewrite matched on raw
  `sqlparser::ast::Function` `PartialEq`,
  `HAVING sum(amount)` over `SELECT SUM(amount)`
  would byte-mismatch and reject as "not in
  SELECT list" despite being the same call.
  Mitigation: the rewrite matches on a planner-
  local structural signature
  (`HavingAggregateSignature`, keyed on
  `ColumnRef`) derived from
  `lower_aggregate_function`'s lowered output,
  canonicalizing case and normalizing args.
  Phase 1's case-insensitivity test pins the
  behavior.
- **R2: Detector extension widens the multi-
  exchange gate accidentally.** The extended
  detector must match the inner node as exactly
  `Aggregate` when the optional `Filter` is
  present; admitting a `Project` or other node
  would silently route non-aggregate two-
  exchange plans through the new path.
  Mitigation: Phase 2's two negative-shape
  control cases (`Project` between exchanges,
  `Filter`-without-`Aggregate`) are required,
  not optional. Phase 0 P0.10 architecture guard
  pins the detector shape at source level.
- **R3: ADR 015 detector regression.**
  Extending the detector to optionally match
  `Filter` must not change the no-`Filter`
  matching behavior. Mitigation: Phase 2
  preserves ADR 015's test cases unchanged; the
  optional-`Filter` extension only adds cases.
- **R4: ADR 011 / ADR 015 reject regression.**
  Removing the `HAVING` reject from
  `lower_select` could inadvertently slacken
  an ADR 011 reject if the outer reject was
  load-bearing for any case. Mitigation:
  Phase 1's ADR 011 / ADR 015 regression cases
  (`DISTINCT`, aliases, computed keys,
  reordered keys, joins under `GROUP BY`,
  no-`HAVING` paths) pin every existing
  reject.
- **R5: Duplicate-aggregate-name silent first-
  match.** A naive rewrite picks the first
  matching projection aggregate on duplicates,
  hiding ambiguity from the user. Mitigation:
  the four-way ambiguity policy (P0.4) rejects
  with `"ambiguous HAVING aggregate-call"` when
  multiple structural matches exist, when a
  single structural match has an aggregate-output
  name duplicated by another projection aggregate,
  OR when a single structural match collides with
  a group-key output name. Phase 1's three §3
  test cases (`(COUNT(*), COUNT(amount))`,
  `(COUNT(*), COUNT(*))`, and `SELECT count,
  COUNT(amount) FROM events GROUP BY count
  HAVING COUNT(amount) > 0`) pin all three arms.
- **R6: `EvaluateInToron` disposition assumed
  but not asserted.** Mitigation: Phase 3's
  binder coverage explicitly asserts the
  disposition is `EvaluateInToron` for a
  representative `HAVING` fixture.
- **R7: Site 1 zero-code claim breaks under
  future fragmenter refactor.** If a later
  fragmenter change adds a node-type gate to
  `find_exchange` / `count_exchanges` /
  `replace_exchange_with_scan` that excludes
  `Filter`, Site 1 silently breaks.
  Mitigation: Phase 2's Site 1 test directly
  constructs `Filter(Aggregate(Exchange))` and
  asserts the produced consumer plan is
  `Filter(Aggregate(Scan))`. A future refactor
  that breaks this assertion fails loudly.

## Definition of done

- The `"HAVING is not supported"` reject in
  `SqlPlanner::lower_select` is removed.
- The `"HAVING without GROUP BY is not
  supported"` reject is in place for the
  scalar-aggregate V1 exclusion.
- The `"HAVING aggregate-call must appear in
  SELECT list"` reject is in place for V1
  exclusion §2.
- `lower_select` lowers `GROUP BY ...
  HAVING ...` to `Filter(<rewritten>)
  (Aggregate(Exchange(K, group_expr)(Scan)))`;
  with `ORDER BY`, wraps in `Sort(Exchange(1,
  []))` per ADR 015.
- The planner-side rewrite matches on a
  planner-local structural signature derived
  from the lowered aggregate output (a private
  enum keyed on `ColumnRef`, narrow to the V1
  grouped subset — `CountAll`,
  `Count(ColumnRef)`, `Sum(ColumnRef)`),
  case-insensitive and argument-normalized via
  `lower_aggregate_function`.
- Binder resolves the rewritten predicate
  against the aggregate output schema via
  existing `bind_expr` + `resolve_column`
  paths; `bind_comparison` performs the narrow
  numeric literal coercion for typed non-literal
  comparison operands; no new `bind_expr` match
  arm.
- `Filter` disposition is `EvaluateInToron`
  for post-aggregate predicates; no new
  disposition variant.
- Fragmenter Site 1 (no `ORDER BY`) requires
  zero new fragmenter code; verified by a
  Site 1 test that exercises the generic
  single-exchange path.
- Fragmenter Site 2 (with `ORDER BY`)
  extends the detector and three-stage
  builder to fold the filter into the
  aggregate consumer stage.
  `Stage 2.upstream_stages == [Stage 1.id]`
  preserved.
- Two negative-shape control cases
  (`Project` between exchanges, `Filter`-
  without-`Aggregate`) still reject with
  `MultipleExchangeBoundaries`.
- ADR 011's and ADR 015's existing fast
  paths are preserved.
- Every V1 exclusion in the companion ADR's
  V1 exclusion list (§1-§10) has a pinned
  regression case.
- In-process digest parity holds between SQL
  and a hand-built logical plan.
- Multi-process digest parity holds across
  worker counts on the `HAVING` fixture.
- Architecture guards green (no `"HAVING is
  not supported"` string in `sql_planner.rs`;
  no new IR variant; no new
  `FilterDisposition`, `MessageKind`,
  `TaskFailureKind`, `BudgetDimension`; no
  new `bind_expr` match arm; planner/binder
  aggregate output-name mapping stays paired).
- Full Toron + Wendelon workspace test
  suites green.
