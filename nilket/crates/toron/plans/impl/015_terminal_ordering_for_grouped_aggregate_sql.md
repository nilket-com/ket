# Implementation Plan: ADR 015 — Terminal Ordering for Grouped Aggregate SQL

**Companion ADR:**
`plans/adr/015_terminal_ordering_for_grouped_aggregate_sql.md`

## Status

Promoted and implemented.

This is a small cross-file composition: one planner reject removed,
one narrow fragmenter detector added ahead of an existing reject,
test coverage at four layers (planner / binder / fragmenter /
integration). No new IR variant, no wire change, no operator
surface change.

## Goal

Land `GROUP BY ... ORDER BY` end-to-end:

- planner accepts the composition and lowers to
  `Sort(Exchange(1, [])(Aggregate(Exchange(K, group_expr)(...))))`;
- binder resolves sort keys (names + 1-based ordinals) against the
  `Aggregate` output schema via ADR 010's existing `bind_sort_keys`
  path;
- fragmenter recognizes the nested shape and emits three
  `StagePlan`s (producers / aggregate consumers / terminal sort),
  with `Stage 2.upstream_stages == [Stage 1.id]`;
- ADR 011's no-`ORDER BY` fast path and every existing reject are
  preserved;
- integration coverage: in-process digest parity with a hand-built
  logical plan, and multi-process digest parity on a distributed
  cluster fixture.

## Substrate

- `crates/toron/src/sql_planner.rs`
  - `SqlPlanner::lower_query` — owns the
    `"GROUP BY with ORDER BY is not supported"` reject that ADR 015
    removes.
  - `is_grouped_aggregate_plan` — `LogicalPlan` shape primitive
    whose only present caller is the reject ADR 015 removes. The
    fragmenter does not depend on it. P0.1 decides keep-or-remove
    based on whether a non-reject caller naturally remains after
    Phase 1.
- `crates/toron/src/binder.rs`
  - `bind_sort_keys` — already resolves against `input.schema()`;
    no code change. `aggregate_field` produces the generated
    aggregate field names (`count`, `sum`, `linreg`, `quantile`).
- `crates/toron/src/distributed/fragment.rs`
  - `terminal_sort_gather` — the `contains_join(input)` gate stays;
    ADR 015 does not generalize this helper.
  - `fragment_plan`'s generic non-join multi-exchange reject —
    ADR 015 inserts the new narrow detector **before** this site.
  - The new detector matches exactly the nested shape, builds the
    three-stage plan, and short-circuits the reject. Anything else
    with two non-join exchanges continues to reject.
- `crates/toron/tests/group_by_test.rs`
  - Existing ADR 011 coverage; ADR 015 adds planner + execution +
    fragmenter test cases here.
- `crates/toron/tests/sort_ordering_test.rs`
  - Existing ADR 010 coverage; ADR 015 adds cross-composition
    cases that exercise sort-key binding against `Aggregate`
    output.
- `crates/toron/tests/distributed_process_test.rs`
  - Existing multi-process digest harness; ADR 015 adds the
    distributed-cluster fixture.

## Files

Expected code changes:

- `crates/toron/src/sql_planner.rs` — remove the `lower_query`
  reject; resolve `is_grouped_aggregate_plan` per P0.1; no
  crate-public helper is introduced.
- `crates/toron/src/distributed/fragment.rs` — new
  `ExecutionPlan`-level narrow detector + three-stage builder,
  placed before the generic multi-exchange reject in
  `fragment_plan`.
- `crates/toron/tests/group_by_test.rs` — planner, binder,
  fragmenter, and execution coverage.
- `crates/toron/tests/sort_ordering_test.rs` — cross-composition
  sort-key cases.
- `crates/toron/tests/distributed_process_test.rs` — multi-process
  digest parity fixture.
- `crates/toron/tests/architecture_test.rs` — source-shape guard
  pinning the narrow detector (does not promote to "any two-exchange
  shape").

Expected documentation:

- `crates/toron/plans/adr/015_terminal_ordering_for_grouped_aggregate_sql.md`
- `crates/toron/plans/impl/015_terminal_ordering_for_grouped_aggregate_sql.md`
- `crates/toron/plans/wip/roadmap.md`

Not expected:

- new IR variants (`LogicalPlan::Sort` / `::Aggregate` /
  `::Exchange` unchanged);
- new `JoinKind`, `JoinStrategy`, `MessageKind`,
  `TaskFailureKind`, `BudgetDimension`, or `TaskOutputBinding`
  variants;
- bench artifact (no operator, no perf claim);
- changes to `RuntimeFilterDescriptor`, `RuntimeFilterKind`, or
  the ADR 013/014 application hooks;
- changes to `TableQueryRequestMeta` or wire frame;
- planner cost-model knob;
- changes to ADR 011's aggregate-output naming or
  duplicate-name disambiguation behavior.

## Phase 0 — Audit and shape decision

- **P0.1: Shape primitive ownership.** The fragmenter adds a
  separate `ExecutionPlan`-level detector in `fragment.rs`; no
  crate-public helper is introduced and the fragmenter does not
  depend on `sql_planner.rs`. The planner-side
  `is_grouped_aggregate_plan` helper's only current caller is the
  reject Phase 1 removes — after that change, if no other caller
  naturally exists, the helper is removed alongside its reject. If
  a non-reject caller does exist or naturally arises in Phase 1
  (e.g. an asserts-shape test fixture in `group_by_test.rs`), the
  helper may stay. The contract is reject-removal and detector
  shape, not the helper's continued existence.

- **P0.2: Detector placement.** The narrow detector fires
  *before* the generic non-join multi-exchange reject in
  `fragment_plan`. The order matters: the broader reject protects
  every other non-join two-exchange shape from silent acceptance;
  the narrow detector is the single permitted exception. Document
  this ordering with a one-line code comment at the detector site
  explaining *why* the placement is load-bearing.

- **P0.3: Stage-dependency wiring.** Stage 2 carries
  `upstream_stages: [Stage 1.id]`. Stage 1 carries
  `upstream_stages: [Stage 0.id]`. The terminal sort task has
  `TaskOutputBinding::FinalResult`; Stages 0 and 1 use
  `ShuffleProducer` / `ShuffleConsumer` bindings as ADR 011
  established. Reuse of the existing `terminal_sort_stage`
  builder vs a parallel builder is implementation choice; the
  contract is the stage shape and dependencies in Phase 2.

- **P0.4: Sort-key resolution sanity.** ADR 010's `bind_sort_keys`
  resolves against `output_schema = input.schema()`. With the
  planner reject removed, the sort input is the `Exchange` above
  the `Aggregate`, whose schema is `aggregate_schema(group_expr,
  aggr_expr)`. Confirm via a unit-level binder test that an
  ordinal sort key on `(region, SUM(amount))` resolves to ordinal
  index 1 (zero-based) — the aggregate field, not the source
  column.

- **P0.5: Helper reuse vs split.** `terminal_sort_gather` stays
  gated on `contains_join(input)`. The new detector is its own
  match in `fragment_plan`. The `terminal_sort_stage` *builder*
  may be reusable across the two callers (join terminal sort +
  aggregate terminal sort); confirm by inspection. If the builder
  needs different scan-metadata handling for the aggregate case,
  factor a parallel builder rather than parameterize the existing
  one. Reuse is not a contract requirement.

- **P0.6: Architecture guards.** Source-shape guards in
  `crates/toron/tests/architecture_test.rs`:
  - No "any two-exchange" widening of the fragmenter — the new
    detector must match the exact nested shape, not relax the
    generic non-join multi-exchange reject globally.
  - No `"GROUP BY with ORDER BY is not supported"` reject string
    survives in `sql_planner.rs` — pins the absence of the
    ADR 015-removed reject rather than tying the guard to a
    specific helper's continued existence.
  - No new `LogicalPlan::*` or `ExecutionPlan::*` variant.
  - No new `MessageKind`, `TaskFailureKind`, `BudgetDimension`.

`// P0.x:` comments only where the audit finding is not obvious
from the code, per `plans/conventions/no-planning-phase-names.md`.

## Phase 1 — Planner unblock

`crates/toron/src/sql_planner.rs`:

- Remove the grouped-aggregate `ORDER BY` reject in
  `SqlPlanner::lower_query`. The surrounding `if let Some(order_by)
  = order_by { ... }` arm now applies its existing
  `Sort + Exchange(1, [])` wrap to grouped aggregate plans
  alongside non-aggregate plans.
- Resolve `is_grouped_aggregate_plan` per P0.1: if removing the
  reject leaves the helper with no caller, remove the helper
  alongside; if a non-reject caller naturally remains or arises in
  Phase 1, keep it and add a one-line doc-comment if absent. Do
  not preserve dead private code, and do not invent an artificial
  caller to justify keeping it.

Tests in `crates/toron/tests/group_by_test.rs`:

- `SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY
  region` lowers to
  `Sort(Exchange(1, [])(Aggregate(Exchange(K, [region])(Scan))))`.
- `SELECT region, SUM(amount) FROM sales GROUP BY region ORDER
  BY 2 DESC LIMIT 10 OFFSET 1` lowers with the same shape,
  `limit = Some(10)`, `offset = 1`, and sort key bound to the
  aggregate-output ordinal.
- `... ORDER BY SUM(amount)` rejects as unsupported. The exact
  rejection layer (planner, binder, or expression-lowering) is
  whichever path naturally rejects aggregate-call expressions
  today; the test pins the reject, not the layer.
- ADR 011 rejects remain: `GROUP BY` over joins, `HAVING`,
  `DISTINCT`, reordered keys, aliases, computed keys — pin each
  with one regression case.
- The no-`ORDER BY` fast path is unchanged: `SELECT region,
  COUNT(*) FROM sales GROUP BY region` lowers to
  `Aggregate(Exchange(K, [region])(Scan))` with no surrounding
  `Sort(Exchange(1, []))`.

## Phase 2 — Fragmenter narrow detector

`crates/toron/src/distributed/fragment.rs`:

In `fragment_plan`, before the generic non-join multi-exchange
reject:

```rust
// ADR 015: terminal sort over grouped aggregate has exactly
// two exchanges and no join. This narrow detector matches the
// composed shape only; every other non-join two-exchange shape
// continues to reject below.
if let Some(detected) = is_terminal_sort_over_grouped_aggregate(&plan) {
    return fragment_terminal_sort_over_grouped_aggregate(
        config, detected,
    );
}
```

Add:

- `fn is_terminal_sort_over_grouped_aggregate(plan: &ExecutionPlan)
  -> Option<TerminalSortOverAggregate>` — matches exactly
  `Sort(Exchange(partition_count = 1, key = [])(Aggregate(Exchange
  (partition_count = K, key = group_expr)(input))))` and extracts
  `(order_by, limit, offset, output_schema, aggregate_fields,
  group_expr, exchange_partition_count_K, inner_input)`.
- `fn fragment_terminal_sort_over_grouped_aggregate(config,
  detected) -> Result<Vec<StagePlan>, FragmentError>` — emits
  three stages:
  - **Stage 0:** scan + projection over `inner_input`,
    `ShuffleProducer` for the group-keyed exchange.
  - **Stage 1:** `ShuffleConsumer` for the group-keyed exchange
    on each partition; `Aggregate` computation; `ShuffleProducer`
    for the terminal-gather exchange.
  - **Stage 2:** `ShuffleConsumer` for the terminal-gather
    exchange (partition 0); `Sort` task; `TaskOutputBinding::
    FinalResult`. `upstream_stages == [Stage 1.id]`.

Internal helper names are illustrative; the load-bearing contract
is the detector shape, the stage dependencies, and the
negative-shape preservation.

The detector is conservative: any mismatch (non-`Sort` outer,
wrong partition counts, non-empty terminal key, non-`Aggregate`
between exchanges) returns `None`, and control falls through to
the existing multi-exchange reject. Plans containing a join are
already routed to `fragment_join_plan` ahead of exchange counting,
so a join under the aggregate never reaches this detector path;
the `GROUP BY` over joins reject is owned by the SQL planner
(ADR 011) for the SQL path and by the join fragmenter routing for
hand-built execution plans.

Tests in `crates/toron/tests/group_by_test.rs`:

- The composition lowers and fragments into exactly three
  `StagePlan`s.
- Stage 0 has `ShuffleProducer(partition_count = K, key =
  [region])`.
- Stage 1 has one `ShuffleConsumer` reading from Stage 0 and one
  `ShuffleProducer(partition_count = 1, key = [])`.
- Stage 2 has `upstream_stages == [Stage 1.id]`, exactly one
  `ShuffleConsumer`, and `TaskOutputBinding::FinalResult`.
- Stage 2 task count is exactly one.
- **Negative-shape control case** — a constructed
  `Sort(Exchange(1, [])(Project(Exchange(K, key)(Scan))))`
  (project, not aggregate, between the two exchanges) still
  rejects with `FragmentError::MultipleExchangeBoundaries`. The
  detector is narrow.

## Phase 3 — Verification across layers

**Binder coverage** (`crates/toron/tests/sort_ordering_test.rs`
or `group_by_test.rs`, whichever fits the existing fixture
style):

- `ORDER BY <group_key_name>` resolves against the group-key
  column.
- `ORDER BY <generated_aggregate_name>` resolves against the
  aggregate column when the name is unambiguous (single
  aggregate of that function).
- `ORDER BY <ordinal>` resolves against the corresponding output
  column; ordinal 1 = first group key, ordinal `N+1` = first
  aggregate.
- Duplicate aggregate names produce the existing ambiguous-name
  error rather than silently selecting a field.
- `ASC` / `DESC` / `NULLS FIRST` / `NULLS LAST` all carry through
  per ADR 010.

**Executor coverage** (`group_by_test.rs`):

- End-to-end execution of `SELECT region, SUM(amount) FROM sales
  GROUP BY region ORDER BY 2 DESC LIMIT 10` against an in-memory
  storage fixture returns the top 10 regions by aggregate sum,
  in descending order.
- `LIMIT` and `OFFSET` compose: `ORDER BY 2 DESC LIMIT 5 OFFSET
  2` skips two rows and returns the next five.
- Sort happens *after* aggregation: with a fixture where row
  order in the source contradicts grouped order, the output
  reflects grouped-aggregated ordering.

**In-process digest parity** (`group_by_test.rs`):

- The SQL path and a hand-built `LogicalPlan::Sort(Exchange(1,
  [])(Aggregate(Exchange(K, group_expr)(Scan))))` produce
  digest-equal Arrow output for the same fixture.

**Multi-process digest parity** (`distributed_process_test.rs`):

- A two- and four-worker cluster running the same ordered
  grouped aggregate fixture produces digest-equal results across
  worker counts. Deterministic stage barrier proven by the
  digest-equality, not by stage-shape assertion at this layer
  (stage-shape is Phase 2's responsibility).

## Phase 4 — Documentation and roadmap

Implementation promotion completed with:

- `plans/adr/015_terminal_ordering_for_grouped_aggregate_sql.md`
  Status flipped to "Promoted and implemented.";
- this impl plan Status flipped similarly;
- `plans/wip/roadmap.md` ADR 014 section retired and ADR 015
  section reframed forward-looking to ADR 016 candidates;
- WIP iteration artifacts for ADR 015
  (`*_iter_*_adr_015.md`, `*_iter_*_impl_015.md`) retired in the
  promotion commit — git holds the council narrative.

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

- **R1: Detector accidentally widens the multi-exchange gate.**
  The new detector must match the exact nested shape; a sloppy
  predicate that admits e.g. `Sort(Exchange(1, [])(Project(
  Exchange(K, key)(Scan))))` would silently route non-aggregate
  two-exchange plans through the new path. Mitigation: Phase 2's
  negative-shape control case (`Project` between exchanges still
  rejects) is required, not optional. Phase 0 P0.6 architecture
  guard pins the detector shape at source level.
- **R2: Stage-barrier wiring forgets the upstream dependency.**
  If Stage 2 listed only Stage 0 as upstream (or no upstream),
  the scheduler could attempt the final sort before any
  aggregate task published its manifest, producing partial
  output. Mitigation: Phase 2's `Stage 2.upstream_stages ==
  [Stage 1.id]` assertion is the required gate; the integration
  digest-parity tests in Phase 3 fail loudly under partial
  aggregate observation.
- **R3: Sort-key binder regression on the non-aggregate path.**
  ADR 010's path is untouched in code, but a regression in
  shared sort-key binding helpers would surface here first.
  Mitigation: Phase 3 binder coverage explicitly exercises both
  the existing non-aggregate `ORDER BY` (regression check) and
  the new grouped-aggregate `ORDER BY`.
- **R4: ADR 011 reject regression.** Removing the planner
  reject from `lower_query` could inadvertently slacken an
  ADR 011 reject if `lower_select` relied on the outer reject
  for any case. Mitigation: Phase 1's ADR 011 regression cases
  (`HAVING`, `DISTINCT`, joins under `GROUP BY`, aliases,
  computed keys, reordered keys) pin every existing reject.
- **R5: Duplicate-name behavior changes.** ADR 015 does not pin
  a new naming contract; the binder's existing behavior carries
  forward. Mitigation: Phase 3 binder coverage includes a
  multi-aggregate duplicate-name fixture asserting the existing
  ambiguous-name error path.

## Definition of done

- The grouped-aggregate `ORDER BY` reject in
  `SqlPlanner::lower_query` is removed. The
  `is_grouped_aggregate_plan` helper is removed alongside if no
  caller naturally remains, or stays if Phase 1 surfaces one.
- Planner lowers `GROUP BY ... ORDER BY` to
  `Sort(Exchange(1, [])(Aggregate(Exchange(K, group_expr)
  (Scan))))`.
- Binder resolves sort keys against the `Aggregate` output
  schema by name and 1-based ordinal; duplicate aggregate names
  retain the existing ambiguous-name behavior.
- Fragmenter recognizes the nested shape and emits exactly three
  `StagePlan`s with `Stage 2.upstream_stages == [Stage 1.id]`.
- A non-aggregate two-exchange shape (e.g. project between
  exchanges) still rejects with `MultipleExchangeBoundaries` —
  the detector is narrow.
- ADR 011's one-exchange fast path is preserved when no
  `ORDER BY` is present.
- Every existing ADR 011 reject is preserved.
- In-process digest parity holds between SQL and a hand-built
  logical plan.
- Multi-process digest parity holds across worker counts on the
  ordered grouped aggregate fixture.
- Architecture guards green.
- Full Toron + Wendelon workspace test suites green.
