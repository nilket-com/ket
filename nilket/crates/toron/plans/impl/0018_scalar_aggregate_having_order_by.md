# Implementation Plan: ADR 018 — Scalar-Aggregate `HAVING` / `ORDER BY` SQL

**Companion ADR:**
`plans/adr/0018_scalar_aggregate_having_order_by.md`

## Status

Promoted. Implemented.

This is the smallest implementation in the cycle so far:
one conditional gate-widening, one scope construction, one
inlined HAVING-attachment block in the global-aggregate
branch, and three test sections (planner, fragmenter,
distributed). No helper extraction. No grouped-branch
refactor. No new IR variant, no wire change, no operator
surface change, no `bind_expr` arm, no fragmenter detector
change, no `lower_order_by` signature change, no walker /
scope / message-helper change. No bench artifact.

## Goal

Land scalar-aggregate `HAVING` and `ORDER BY` end-to-end:

- planner accepts the three canonical scalar patterns
  (HAVING-only, ORDER BY-only, dual rewrite) and lowers
  each via the existing ADR 016 / ADR 017 substrate;
- `lower_select`'s global-aggregate branch constructs
  `GroupedAggregateScope::from_projection(&[], &aggr_expr)`
  and inlines the optional HAVING-attachment, returning
  `LoweredSelect { plan, grouped_aggregate_scope: Some(scope) }`;
- the grouped-aggregate branch at `sql_planner.rs:241-277`
  is byte-identical pre- and post-ADR-018;
- the `HAVING`-without-`GROUP BY` reject conditions on the
  absence of an aggregate projection; reject string lifts
  to `"HAVING requires GROUP BY or an aggregate
  projection"`;
- `lower_order_by` runs the existing walker against the
  new `Some(scope)` for scalar aggregates with `ORDER BY`
  referencing an aggregate call — zero signature change;
- `reject_qualified_having_column_references` pre-pass
  runs symmetrically;
- `bind_comparison` numeric-literal coercion (ADR 016
  inheritance) composes unchanged;
- fragmenter: no change. HAVING-only routes via
  `single_task_stage`; ordered scalar (with or without
  HAVING) routes via the generic one-exchange
  producer/consumer two-stage path;
- ADR 010 / ADR 011 / ADR 015 / ADR 016 / ADR 017 fast
  paths unchanged;
- in-process digest parity for the HAVING-only and dual-
  rewrite patterns; multi-process digest parity for the
  ordered cases.

## Substrate

- `crates/toron/src/sql_planner.rs`
  - `SqlPlanner::lower_select` at `sql_planner.rs:127-321`
    — two surgical changes:
    - line 193-195: condition the `HAVING`-without-`GROUP
      BY` reject on `!projection_contains_aggregate_call
      (projection)`; lift reject string.
    - line 280-291 (global branch): construct scope, inline
      optional HAVING-attachment, return `Some(scope)`.
  - The grouped branch at `sql_planner.rs:241-277` is
    explicitly NOT modified. Existing test corpus is the
    regression bar.
  - All other `sql_planner.rs` surfaces unchanged. No
    helper extraction; no rename.
- `crates/toron/src/binder.rs` — no change. The rewritten
  scalar HAVING predicate `Expr::Gt(Expr::column("sum"),
  Literal(1000))` binds via the inherited ADR 016
  `bind_comparison` numeric-literal coercion.
- `crates/toron/src/exec.rs` — no change.
  `execute_bound`'s `Aggregate` arm at
  `exec.rs:537-554` already branches on
  `group_expr.is_empty()`.
- `crates/toron/src/distributed/fragment.rs` — no change.
  HAVING-only `Filter(Aggregate(scan))` has zero exchanges
  and routes via `single_task_stage`; ordered scalar
  `Sort(Exchange(1, [])(...))` has one exchange and routes
  via the generic producer/consumer path at
  `fragment.rs:88-162`. `is_terminal_sort_over_grouped_
  aggregate` correctly returns `None` for all scalar
  shapes (its inner-Exchange destructure fails).
- `crates/toron/src/ir.rs` — no change.
- `crates/toron/tests/group_by_test.rs` — ADR 018 adds
  planner + executor test cases for the three canonical
  scalar patterns, V1 exclusion regressions, the §12
  inherited-empty-input pin, and a grouped-branch
  no-change regression case.
- `crates/toron/tests/distributed_process_test.rs` — ADR
  018 adds two multi-process fixtures (ordered scalar
  with and without HAVING) to pin digest parity across
  worker counts.
- `crates/toron/tests/architecture_test.rs` — ADR 018 adds
  `adr018_scalar_aggregate_having_order_by_guards_are_
  narrow` test with the presence/absence guards listed
  in P0.7.

## Files

Expected code changes:

- `crates/toron/src/sql_planner.rs`:
  - Conditional reject lift at line 193-195.
  - Global branch: construct scope, inline HAVING-
    attachment, return `Some(scope)`.
- `crates/toron/tests/group_by_test.rs`:
  - Three canonical-pattern test cases (HAVING-only,
    ORDER BY-only, dual rewrite).
  - V1 exclusion regression tests (ambiguity, not-in-
    select, qualified-reference pre-pass, no-aggregate-
    no-GROUP-BY still rejects, UnaryOp / arithmetic over
    aggregate calls rejects, experimental aggregates not
    accepted in scalar HAVING/ORDER BY).
  - Empty-input regression (pins inherited behavior).
  - Grouped-branch reject-ordering preservation regression
    for `SELECT region FROM sales GROUP BY region HAVING
    SUM(amount) > 100`.
  - In-process executor digest parity for the dual-
    rewrite pattern.
  - Fragmenter routing tests: HAVING-only fragments as
    single-task-stage; ORDER BY fragments as the generic
    two-stage producer/consumer path.
- `crates/toron/tests/distributed_process_test.rs`:
  - Multi-process fixture: ordered scalar aggregate
    `SELECT SUM(amount) FROM sales ORDER BY SUM(amount)
    DESC LIMIT 1` produces byte-identical digest across
    two- and four-worker counts.
  - Multi-process fixture: ordered scalar dual-rewrite
    `SELECT SUM(amount) FROM sales HAVING SUM(amount) > 0
    ORDER BY SUM(amount) DESC LIMIT 1` produces byte-
    identical digest across worker counts.
- `crates/toron/tests/architecture_test.rs`:
  - `adr018_scalar_aggregate_having_order_by_guards_are_
    narrow` test.

Updated documentation:

- `crates/toron/plans/adr/018_scalar_aggregate_having_
  order_by.md` (this file's companion ADR).
- `crates/toron/plans/impl/018_scalar_aggregate_having_
  order_by.md` (this file).
- `crates/toron/plans/wip/roadmap.md` — ADR 018 retires
  from "candidates" into "Implemented" with the cycle-
  shape quadruplet note; "ADR 019 candidates" section
  seeded from the ADR 019 handoff.
- Auto-memory `adr-015-cycle-shape` updates to four
  instances and reaffirms the load-bearing property.

Not expected:

- new IR variants;
- new `FilterDisposition`, `JoinKind`, `JoinStrategy`,
  `MessageKind`, `TaskFailureKind`, `BudgetDimension`, or
  `TaskOutputBinding` variants;
- new `bind_expr` match arm;
- bench artifact;
- new `attach_having_filter` helper or any other shared
  helper for HAVING-attachment;
- changes to `RuntimeFilterDescriptor`, wire frame,
  cost-model knob, ADR 011 aggregate-output naming, ADR
  015 detector site, ADR 016 `Filter`-inside-gather
  contract, ADR 017 substrate (walker / scope /
  `ResolveOutcome` / pre-pass / message helpers / clause-
  label threading), `bind_comparison` coercion;
- changes to executor's `Aggregate` / `Filter` / `Sort`
  dispatch;
- changes to `lower_order_by`'s signature;
- changes to the grouped-aggregate branch of
  `lower_select` (`sql_planner.rs:241-277`).

## Phase 0 — Audit and shape decision

- **P0.1: Scope generalization.**
  `GroupedAggregateScope::from_projection` at
  `sql_planner.rs:885-912` accepts `group_keys: &[GroupKey]`;
  passing `&[]` is valid. Zero group-key entries flow
  through the loop unchanged; `output_name_counts`
  populates with only aggregate names. Generalization is
  mechanical.

- **P0.2: Fragmenter routing — three cases.**
  `lower_query` at `sql_planner.rs:101-121` unconditionally
  wraps `Sort` over `Exchange(partition_count: 1, key:
  [])` when `order_by.is_some()`. So ordered scalar
  queries land with one exchange, not zero. Routing:
  - HAVING-only `Filter(Aggregate(scan))` — zero exchanges,
    `single_task_stage` at `fragment.rs:467-489`.
  - ORDER BY-only `Sort(Exchange(1, [])(Aggregate(scan)))`
    — one exchange, generic two-stage path at
    `fragment.rs:88-162`.
  - Dual rewrite `Sort(Exchange(1, [])(Filter(Aggregate
    (scan))))` — one exchange, generic two-stage path.
  No new detector. The generic one-exchange path is
  existing code.

- **P0.3: Executor composition.** `execute_bound`'s
  `Aggregate` arm at `exec.rs:537-554` dispatches
  `bound_global_aggregate` when `group_expr.is_empty()`.
  `Filter` and `Sort` compose on the single-row scalar
  output.

- **P0.4: Single feature commit.** ADR 018 ships as one
  commit (`feat: ADR 018 scalar-aggregate HAVING / ORDER
  BY`). No `chore:` slice, no helper extraction, no
  grouped-branch refactor. The reject-lift and the global-
  branch scope+HAVING wiring must land together to avoid
  any transitional silent-drop window where the gate is
  widened but the branch ignores HAVING.

- **P0.5: Pre-pass symmetry.** Automatic. The global
  branch invokes `lower_having_predicate` (via the inlined
  block), which runs
  `reject_qualified_having_column_references` before the
  walker. Zero code change required for pre-pass behavior.

- **P0.6: `lower_order_by` signature.** Unchanged. ADR
  017's `scope: Option<&GroupedAggregateScope>` parameter
  receives `Some(scope)` for scalar aggregates with
  `ORDER BY <aggregate-call>`.

- **P0.7: Architecture guards (semantic only).**
  - **Presence (new):** `"HAVING requires GROUP BY or an
    aggregate projection"` appears in `sql_planner.rs`.
  - **Absence (new):** `"HAVING without GROUP BY is not
    supported"` no longer appears in `sql_planner.rs`.
  - **Inherited (ADR 016 / ADR 017):** five walker / scope
    / pre-pass reject strings remain:
    `"qualified HAVING column references are not
    supported"`, `"HAVING aggregate-call must appear in
    SELECT list"`, `"ambiguous HAVING aggregate-call"`,
    `"ambiguous ORDER BY aggregate-call"`,
    `"ORDER BY aggregate-call must appear in SELECT
    list"`.
  - **Inherited:** no `Expr::AggregateCall` handling in
    `bind_expr`; no new `FilterDisposition` /
    `MessageKind` / `TaskFailureKind` / `BudgetDimension`
    / IR variant.

- **P0.8: `bind_comparison` inheritance.** The rewritten
  HAVING expression `Expr::Gt(Expr::column("sum"),
  Literal(1000))` binds via the inherited ADR 016
  coercion. Zero binder code change.

- **P0.9: Grouped branch byte-identical.** The grouped
  branch at `sql_planner.rs:241-277` is explicitly NOT
  refactored. Existing `having_predicate` materialization
  order (HAVING lowered before `aggr_expr.is_empty()`
  check) is preserved. Reject-ordering for malformed
  grouped inputs remains as today.

- **P0.10: Inlining over helper extraction.** With only
  one new HAVING-attachment caller (the global branch),
  a shared helper does not earn the named-seam value that
  the cycle's source-level boundary clarity property would
  otherwise favor. Inline the `if let Some(having)` block
  directly. When a future ADR (likely post-aggregate
  projection) adds a third caller, the helper extraction
  becomes worthwhile and that ADR's review can ratify any
  grouped-branch refactor explicitly.

## Phase 1 — Implementation

`crates/toron/src/sql_planner.rs`. One commit:
`feat: ADR 018 scalar-aggregate HAVING / ORDER BY`.

### Step 1: conditional reject lift at `sql_planner.rs:193-195`

Replace the unconditional HAVING-without-GROUP-BY reject
with:

```rust
if having.is_some()
    && group_keys.is_none()
    && !projection_contains_aggregate_call(projection)
{
    return Err(unsupported(
        "HAVING requires GROUP BY or an aggregate projection",
    ));
}
```

### Step 2: global branch scope + inline HAVING at `sql_planner.rs:280-291`

Replace:

```rust
if projection_contains_aggregate_call(projection) {
    let aggr_expr = self.lower_global_aggregate_projection(projection)?;
    return Ok(LoweredSelect {
        plan: LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_expr: vec![],
            aggr_expr,
        },
        grouped_aggregate_scope: None,
    });
}
```

with:

```rust
if projection_contains_aggregate_call(projection) {
    let aggr_expr = self.lower_global_aggregate_projection(projection)?;
    let scope = GroupedAggregateScope::from_projection(&[], &aggr_expr);
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(plan),
        group_expr: vec![],
        aggr_expr,
    };
    let plan = if let Some(having) = having.as_ref() {
        let predicate = self.lower_having_predicate(having, &scope)?;
        LogicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        }
    } else {
        aggregate
    };
    return Ok(LoweredSelect {
        plan,
        grouped_aggregate_scope: Some(scope),
    });
}
```

### Step 3: tests in `group_by_test.rs`

- `scalar_aggregate_having_lowers_to_filter_over_aggregate`:
  `SELECT SUM(amount) FROM sales HAVING SUM(amount) > 1000`
  lowers to `Filter { predicate, input: Box::new(Aggregate
  { group_expr: [], aggr_expr: [(Sum, ...)] }) }`.

- `scalar_aggregate_order_by_lowers_to_sort_over_exchange_over_aggregate`:
  `SELECT SUM(amount) FROM sales ORDER BY SUM(amount) DESC`
  lowers to `Sort { order_by: [SortExpr { expr:
  Expr::column("sum"), descending: true, ... }], input:
  Box::new(Exchange { partition_count: 1, key: [], input:
  Box::new(Aggregate { group_expr: [], ... }) }) }`.
  Asserts the one-exchange shape explicitly so a future
  change to the `Sort` wrap is caught.

- `scalar_aggregate_dual_rewrite_having_and_order_by`:
  `SELECT SUM(amount) FROM sales HAVING SUM(amount) > 1000
  ORDER BY SUM(amount) DESC LIMIT 1` lowers to
  `Sort { ..., input: Box::new(Exchange { ..., input:
  Box::new(Filter { predicate, input: Box::new(Aggregate
  { group_expr: [], ... }) }) }) }`.

- `scalar_aggregate_having_ambiguity_is_rejected`:
  `SELECT COUNT(*), COUNT(*) FROM events HAVING COUNT(*) > 0`
  rejects with `"ambiguous HAVING aggregate-call"`.

- `scalar_aggregate_order_by_ambiguity_is_rejected`:
  `SELECT COUNT(*), COUNT(*) FROM events ORDER BY COUNT(*)`
  rejects with `"ambiguous ORDER BY aggregate-call"`.

- `scalar_aggregate_order_by_not_in_select_rejects`:
  `SELECT COUNT(*) FROM events ORDER BY SUM(amount)`
  rejects with `"ORDER BY aggregate-call must appear in
  SELECT list"`.

- `scalar_aggregate_having_not_in_select_rejects`:
  `SELECT COUNT(*) FROM events HAVING SUM(amount) > 0`
  rejects with `"HAVING aggregate-call must appear in
  SELECT list"`.

- `scalar_aggregate_having_qualified_reference_rejects`:
  `SELECT SUM(amount) FROM sales HAVING t.amount > 0`
  rejects with `"qualified HAVING column references are
  not supported"`. Confirms pre-pass symmetry.

- `having_without_group_by_and_without_aggregate_still_rejects`:
  `SELECT region FROM sales HAVING region = 'EMEA'`
  rejects with `"HAVING requires GROUP BY or an aggregate
  projection"`.

- `scalar_aggregate_order_by_unary_op_over_aggregate_rejects`:
  `SELECT SUM(amount) FROM sales ORDER BY -SUM(amount)`
  rejects via the catch-all. Pins V1 exclusion §6.

- `scalar_aggregate_order_by_arithmetic_over_aggregate_rejects`:
  `SELECT SUM(amount) FROM sales ORDER BY SUM(amount) + 100`
  rejects via the catch-all on the surrounding `BinaryOp`.

- `scalar_aggregate_having_experimental_aggregate_rejects`:
  `SELECT LINEAR_REGRESS(x, y) FROM t HAVING LINEAR_REGRESS
  (x, y) > 0` rejects because
  `grouped_aggregate_signature` returns `None` for
  `LinearRegress`, triggering the "must appear in SELECT
  list" reject. Pins V1 exclusion §7.

- `grouped_branch_reject_ordering_is_preserved`:
  `SELECT region FROM sales GROUP BY region HAVING SUM
  (amount) > 100` rejects with `"HAVING aggregate-call
  must appear in SELECT list"` (the existing pre-ADR-018
  message). Pins P0.9 — confirms the grouped branch is
  unchanged.

- `scalar_aggregate_having_executes_with_one_row_output`:
  end-to-end execution of `SELECT SUM(amount) FROM sales
  HAVING SUM(amount) > 0 ORDER BY SUM(amount) DESC LIMIT 1`
  against a fixture where the aggregate exceeds the
  predicate; returns one row.

- `scalar_aggregate_having_filters_to_zero_rows_when_predicate_false`:
  against a fixture where SUM(amount) ≤ 0; returns zero
  rows.

- `scalar_aggregate_having_empty_input_pins_inherited_behavior`:
  `SELECT SUM(amount) FROM empty_fixture HAVING SUM(amount)
  > 0` returns whatever the inherited
  `bound_global_aggregate` produces. Current behavior
  materializes NULL for empty-input SUM and the HAVING
  comparison does not keep the row. The test asserts the
  observed inherited behavior; a future kernel-graduation
  ADR that changes empty-input aggregate semantics must
  re-pin this assertion explicitly.

- `scalar_aggregate_having_order_by_sql_matches_hand_built_plan`:
  digest parity between the SQL path and a hand-built
  `Sort(Exchange(1, [])(Filter(Aggregate { group_expr: []
  })))` plan.

- `scalar_aggregate_having_only_fragments_to_single_task_stage`:
  construct `Filter(Aggregate { group_expr: [] }(scan))`,
  pass to `fragment_plan`, assert one `StagePlan` with one
  `TaskPlan` (`partition: None`,
  `output: TaskOutputBinding::FinalResult`). Confirms
  `exchange_count == 0` arm fires.

- `scalar_aggregate_ordered_fragments_to_generic_two_stage`:
  construct `Sort(Exchange(1, [])(Aggregate { group_expr:
  [] }(scan)))`, pass to `fragment_plan`, assert two
  `StagePlan`s with the producer/consumer shape. Confirms
  the generic one-exchange path fires and
  `is_terminal_sort_over_grouped_aggregate` correctly
  returns `None` for the scalar shape (grouped-detector
  negative-shape control).

### Step 4: distributed fixtures in `distributed_process_test.rs`

- `scalar_aggregate_ordered_process_digest_is_stable_across_worker_counts`:
  fixture-driven test for `SELECT SUM(val) FROM fixture
  ORDER BY SUM(val) DESC LIMIT 1` across two- and four-
  worker process configurations. Asserts byte-identical
  normalized digest. Exercises the producer/consumer
  fragmenter route end-to-end.

- `scalar_aggregate_having_ordered_process_digest_is_stable_across_worker_counts`:
  same fixture for `SELECT SUM(val) FROM fixture HAVING
  SUM(val) > 0 ORDER BY SUM(val) DESC LIMIT 1`. Asserts
  byte-identical normalized digest across worker counts.

### Step 5: architecture-test addition in `architecture_test.rs`

```rust
#[test]
fn adr018_scalar_aggregate_having_order_by_guards_are_narrow() {
    let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
        .expect("read SQL planner source");
    assert!(planner.contains("HAVING requires GROUP BY or an aggregate projection"));
    assert!(!planner.contains("HAVING without GROUP BY is not supported"));
    // Inherited from ADR 016 / 017 — continued presence:
    assert!(planner.contains("qualified HAVING column references are not supported"));
    assert!(planner.contains("HAVING aggregate-call must appear in SELECT list"));
    assert!(planner.contains("ambiguous HAVING aggregate-call"));
    assert!(planner.contains("ambiguous ORDER BY aggregate-call"));
    assert!(planner.contains("ORDER BY aggregate-call must appear in SELECT list"));
    // Inherited continued: no Expr::AggregateCall in bind_expr.
    let binder = std::fs::read_to_string(manifest_dir().join("src/binder.rs"))
        .expect("read binder source");
    let bind_expr = binder
        .split("fn bind_expr")
        .nth(1)
        .and_then(|rest| rest.split("fn bind_comparison").next())
        .expect("bind_expr block");
    assert!(!bind_expr.contains("AggregateCall"));
}
```

Verification: `cargo test --locked -p toron`.

## Phase 2 — Fragmenter inspection (zero-code)

`crates/toron/src/distributed/fragment.rs`: no changes.

Two tests in `group_by_test.rs` (in the same commit) pin
the three-route behavior:

- `scalar_aggregate_having_only_fragments_to_single_task_stage`
  pins the zero-exchange route for HAVING-only.
- `scalar_aggregate_ordered_fragments_to_generic_two_stage`
  pins the one-exchange producer/consumer route for the
  ordered cases AND the negative-shape behavior of the
  grouped detector (it does NOT fire for scalar shapes).

A future fragmenter refactor that adds a scalar-shape
detector arm would fail one of these regression tests.

## Phase 3 — Verification across layers

**Binder coverage** (`group_by_test.rs`):

- Rewritten ORDER BY column references resolve against
  `Sort.input.schema()`. With the inner `Exchange(1, [])`
  preserving schema, the binder reaches the aggregate
  output schema as before.
- Rewritten HAVING comparison binds via the inherited
  `bind_comparison` numeric-literal coercion (ADR 016).
- ADR 010 total-order tie-break composes unchanged.

**Executor coverage** (`group_by_test.rs`):

- Per Step 3's executor test additions.

**Fragmenter coverage** (`group_by_test.rs`):

- Per Phase 2's two routing tests.

**Distributed coverage** (`distributed_process_test.rs`):

- Per Step 4's two multi-process fixtures. Ordered scalar
  exercises the one-exchange producer/consumer route end-
  to-end across worker counts.

**In-process digest parity** (`group_by_test.rs`):

- Per Step 3's `_sql_matches_hand_built_plan` test.

## Phase 4 — Documentation and roadmap

- ADR + impl plan promoted to
  `plans/adr/0018_scalar_aggregate_having_order_by.md` and
  `plans/impl/0018_scalar_aggregate_having_order_by.md`
  with status "Promoted. Implemented.".
- `plans/wip/roadmap.md` ADR 018 entry retires from
  "candidates" into "Implemented" with the cycle-shape
  quadruplet note: ADR 015 (`Sort` + `Aggregate`), ADR 016
  (`Filter` over `Aggregate`), ADR 017 (`ORDER BY`
  aggregate-call), ADR 018 (scalar-aggregate `HAVING` /
  `ORDER BY`). Four instances stabilize the cycle shape
  across grouped and scalar surfaces. Adds an "ADR 019
  candidates" section seeded from the ADR 019 handoff.
- Auto-memory `adr-015-cycle-shape` updates to record
  four instances and reaffirm the load-bearing property
  (source-level boundary clarity at named-reject
  granularity, no silent acceptance, no silent drop).
- Iter artifacts retire upon promotion; git holds the
  narrative.

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

- **R1: Transitional silent-drop window.** If the gate
  widening (Step 1) lands without the global-branch
  scope+HAVING (Step 2), scalar HAVING queries pass the
  gate but the global branch ignores `having` and silently
  drops the clause. Mitigation per P0.4: ship Steps 1 and
  2 in one atomic commit.
- **R2: `ResolveOutcome::Ambiguous` arm under empty
  group-keys.** Code path is identical; the unreachable
  group-key-collision branch remains exercised only by
  ADR 016 grouped tests. Step 3's ambiguity tests pin the
  reachable arms.
- **R3: Existing tests assert HAVING-without-GROUP-BY
  rejection text.** Mitigation: grep before the commit
  (`grep -rn "HAVING without GROUP BY" crates/toron/`);
  update assertion targets to the new string for tests
  that still reject; drop or relax assertions for tests
  that now lower successfully. The grouped-aggregate test
  corpus is unaffected (grouped branch is unchanged).
- **R4: Empty-input semantics pin inherited behavior.**
  V1 exclusion §12 explicitly names this distinction.
  The `_pins_inherited_behavior` test signals the intent
  — a future kernel-graduation ADR that changes empty-
  input aggregate semantics must re-pin the assertion.
- **R5: `lower_order_by` does not invoke the walker for
  non-aggregate ORDER BY exprs on scalar aggregates.**
  ORDER BY a literal or ordinal in a scalar-aggregate
  query continues to flow through `lower_expr` directly.
  Existing behavior, no change. Regression test:
  `SELECT SUM(amount) FROM sales ORDER BY 1 DESC` lowers
  correctly without invoking the walker.
- **R6: `bind_comparison` coercion under empty input.**
  Coercion happens at type level (column type vs literal
  type), not value level. No risk.
- **R7: Distributed fixture flakiness.** The ordered
  scalar fixtures exercise the producer/consumer
  fragmenter route — fragmenter and distributed harness
  are already proven on ADR 015 / 016 / 017 grouped
  fixtures; the scalar variant uses the same harness with
  a different plan shape. If the new fixture exhibits any
  digest non-determinism, the bug is in shared distributed
  infrastructure and surfaces here as an early signal
  rather than later in a kernel-graduation ADR.
- **R8: Helper extraction deferred — future post-aggregate
  projection ADR may want it.** If ADR 019 (or whichever
  ADR adds a third HAVING-attachment caller) wants a
  shared helper, that ADR can extract it then. ADR 018's
  inlining is intentionally local. No mitigation needed
  unless the deferred helper extraction creates an
  obvious code-duplication wart at ADR 019 time, which is
  ratifiable then.

## Definition of done

- Single commit (`feat:`): conditional reject lift; new
  string presence guard fires; old string absence guard
  fires; global branch constructs scope and inlines
  HAVING-attachment; returns `Some(scope)`.
- Grouped branch byte-identical pre- and post-ADR-018;
  `grouped_branch_reject_ordering_is_preserved` regression
  test fires.
- `lower_order_by` receives `Some(scope)` for scalar
  aggregates with `ORDER BY <aggregate-call>`; walker
  fires; rewritten column refs flow to `lower_expr`.
- `reject_qualified_having_column_references` pre-pass
  runs symmetrically.
- `bind_comparison` numeric-literal coercion composes
  unchanged.
- Three canonical scalar-aggregate patterns lower,
  fragment, and execute end-to-end.
- All twelve V1 exclusion items have appropriate
  regression coverage: §1 by inheritance and fragmenter
  routing tests; §2–§7 by direct regression cases; §8–§11
  by inheritance; §12 by the explicit
  `_pins_inherited_behavior` test.
- HAVING-only fragments via `single_task_stage`; ordered
  scalar fragments via the generic two-stage route.
  Pinned by `group_by_test.rs` fragmenter routing tests.
- Multi-process digest parity holds for both ordered
  scalar fixtures across two- and four-worker counts.
- Architecture-test additions land; presence/absence
  guards fire correctly.
- In-process digest parity holds.
- Full Toron + Wendelon workspace test suites green.
- Memory file `adr-015-cycle-shape` updated to four
  instances.
- Roadmap entry retired into "Implemented" with
  quadruplet note; "ADR 019 candidates" section seeded.
