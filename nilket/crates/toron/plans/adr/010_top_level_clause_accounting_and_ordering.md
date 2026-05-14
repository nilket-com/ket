# ADR 010: Top-Level SQL Clause Accounting and Deterministic Result Ordering

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion implementation plan:**
`plans/impl/010_top_level_clause_accounting_and_ordering.md`

## Status

Promoted to `plans/adr/` and implemented. No bench-results
artifact — ADR 010 introduces a new operator but makes no
performance claim; promotion evidence is digest parity
(canonical + ordered) plus stage-shape inspection against
hand-written `Sort` plans plus preserved/updated architecture
guards (see "Promotion evidence").

Three-way AI Swarm Council convergence reached across three
topic-iteration rounds and two ADR-drafting rounds
(`plans/wip/{claude,codex,gemini}_*_010_*.md`). Codex iter-2
approved this draft as the promotion base; Gemini iter-2 ratified
the same contract. Iteration artifacts retired — git holds the
narrative.

Load-bearing antecedents: Codex's clause-accounting broadening,
the three-bucket field model, the output-ordinal physical key
shape, the bind-time tie-break, the fragmenter stage-shape gates,
the final policy locks, and the broadcast-join composition
catch; Gemini's ordered-digest determinism check, the
`descending` `SortOptions`-aligned flag, and D1–D6 ratification;
Claude's missing-candidate selection argument, the S1
output-schema key scope, the S2 compile-time accounting guard,
the S3 fragmenter-boundary correction, and the consolidated
contract block.

This ADR is a **frontend-planner surface extension plus a new
operator** that closes a live V1 "First Query with Integrity"
correctness defect: `SqlPlanner::lower_query` and
`SqlPlanner::lower_select` silently discard every top-level
`Query` / `Select` clause they do not explicitly handle —
including `ORDER BY`, `LIMIT`, `OFFSET`, and `DISTINCT`.
`SELECT * FROM t ORDER BY x` parses, lowers, binds, distributes,
and executes today **with the `ORDER BY` dropped on the floor**.
ADR 010 makes every parsed top-level field either intentionally
lowered, intentionally rejected, or intentionally bound-and-
ignored as parser metadata, and implements deterministic
`ORDER BY` / `LIMIT` / `OFFSET` as the feature that earns the
ADR.

## Goal

Two inseparable halves:

1. **Clause accounting (the integrity fix).** No accepted
   top-level SQL query may contain a parsed `sqlparser` `Query`
   or `Select` *field* that the planner silently ignores. Every
   field is *accounted for* — lowered intentionally, rejected
   intentionally through `FrontendError::Unsupported` when
   present, or explicitly bound and documented as non-semantic
   parser metadata. The guarantee is **field-level**: it covers
   whether each `Query` / `Select` field is handled, not whether
   every *item within* a lowered field is itself fully
   accounted. `Select.projection` keeps its ADR 008 / 009
   handling unchanged; projection-*item* gaps that predate
   ADR 010 — mixed aggregate / non-aggregate projection,
   wildcard mixed with explicit items — are named deferred work,
   not closed here (see §"Not in scope").
2. **Deterministic result ordering (the feature).** Recognize
   top-level `ORDER BY`, `ORDER BY ... LIMIT n`,
   `ORDER BY ... OFFSET n`, and `ORDER BY ... LIMIT n OFFSET n`,
   and lower them to a new `Sort` operator with a **total-order
   determinism guarantee** and a single terminal gather-sort
   distributed shape.

After ADR 010, Toron may still reject a great deal of SQL — but
it no longer *accepts* top-level SQL syntax while silently
discarding part of the user's query.

## Scale boundary

**Primary:** frontend planner + operator-level executor. The
contract change spans `SqlPlanner::lower_query` /
`SqlPlanner::lower_select` (clause accounting + the `ORDER BY`
rewrite), a new `LogicalPlan::Sort` / `ExecutionPlan::Sort` IR
variant, the binder path that validates and lowers it, and a new
`SortExec` operator.

**Secondary:** distributed fragmenter. `ORDER BY` above a
distributed join requires a terminal single-partition
gather-sort stage. This is *narrower* than distributed sort — one
final task sorts the complete result, no new exchange mode — but
it is not zero, and it is **required**, not optional: the current
join fragmenter treats the first join as the terminal consumer
and emits join-consumer tasks as final-result tasks. ADR 010
teaches it the post-join gather boundary so the join-consumer
stage publishes shuffle output to a terminal single-task sort
stage. The path is **strategy-agnostic** — it covers both
`HashPartitioned` and `Broadcast` join consumers, because SQL
lowering emits the same `Sort(Exchange₁(…))` shape regardless of
join strategy. This is the largest real implementation risk
(Phase 0 P0.9, impl Phase 4), gated by stage-shape inspection,
not output digest alone.

**Not in scope:**
- No wire change. No new `MessageKind`, `TaskFailureKind`,
  `BudgetDimension`, telemetry field, or Wendelon scan-contract
  change. `ExecutionPlan::Sort` rides the existing `TaskSpec` /
  partition-store machinery.
- No range partitioning, no distributed merge sort. V1 is one
  terminal gather-sort; distributed sort is a V1.x
  evidence-driven follow-on.
- No external merge sort / sort spill. V1 sorts in memory under
  the existing budget envelope and rejects over budget.
- No sort-merge join. ADR 010 is the *ordering-contract
  antecedent* a future sort-merge-join ADR builds on.
- No top-K / `LIMIT` pushdown into Wendelon (Brainstorm 003).
- No `DISTINCT` implementation — `DISTINCT` is an aggregate/dedup
  contract. ADR 010 closes its silent drop by loud-rejecting it;
  a later aggregate-path ADR implements it.
- No `GROUP BY` / `HAVING` implementation — same posture.
- No projection-*item* accounting. ADR 010's accounting
  invariant is field-level; `Select.projection` keeps its
  ADR 008 / 009 handling. Pre-existing projection-item gaps —
  mixed aggregate / non-aggregate projection, wildcard mixed
  with explicit items — are a named deferred item
  (aggregate-path-adjacent work), not closed by ADR 010.
- No `ORDER BY` on a non-output column, no projection-alias or
  computed `ORDER BY` expressions, no `ORDER BY` in subquery
  bodies, no set-operation ordering, no window functions, no
  `QUALIFY`, no `WITH` / CTEs.
- No cost-based decision about sort placement.

## V1 contract summary

- **Clause accounting, three buckets.** `lower_query` and
  `lower_select` destructure the `sqlparser` `Query` and `Select`
  structs **exhaustively, with no `..` rest pattern**. Every
  field falls into exactly one bucket:
  - **semantic-lowered** — `Query.body`, `Query.order_by`,
    `Query.limit`, `Query.offset`, `Select.from`,
    `Select.selection`, `Select.projection`;
  - **semantic-rejected-when-present** — every other semantic
    clause (`Select.distinct`, `group_by`, `having`, `qualify`,
    `top`, `into`, `prewhere`, `lateral_views`, `cluster_by`,
    `distribute_by`, `sort_by`, `named_window`,
    `value_table_mode`, `connect_by`; `Query.with`, `limit_by`,
    `fetch`, `locks`, `for_clause`, `settings`,
    `format_clause`). Rejection fires only when *present and
    non-empty* (`Some(_)` / non-empty `Vec` / non-default
    `GroupByExpr`);
  - **parser-metadata-ignored** — non-semantic parser
    bookkeeping (`Select.select_token`,
    `Select.top_before_distinct`, `Select.window_before_qualify`),
    explicitly bound and commented as carrying no clause
    semantics.

  A `sqlparser` upgrade that adds any field is a *compile error*
  in Toron, forcing a deliberate bucket assignment.
- **`ORDER BY` → `LogicalPlan::Sort`.** A top-level `ORDER BY` is
  lowered to a new `Sort` operator above the `Project`, over a
  terminal `Exchange { partition_count: 1, key: [] }` the planner
  emits explicitly. Supported forms: `ORDER BY ...`,
  `ORDER BY ... LIMIT n`, `ORDER BY ... OFFSET n`,
  `ORDER BY ... LIMIT n OFFSET n`, with `n` a non-negative
  integer literal.
- **Bare `LIMIT` / `OFFSET` / `FETCH` reject.** A bare `LIMIT` in
  a distributed engine returns arbitrary rows. Because bare
  `LIMIT` rejects, ADR 010 needs no standalone `Limit` IR
  variant.
- **`ORDER BY` key surface, locked.** Keys resolve against the
  query's **output schema** only; `Sort` sits above `Project`.
  - output column names — **supported**;
  - **1-based positive integer ordinals** (`ORDER BY 1`) —
    **supported**, resolved against the output schema by
    position. `ORDER BY 0`, negative ordinals, out-of-range
    ordinals, non-integer literals, and expressions masquerading
    as ordinals **reject**;
  - projection aliases — **rejected** (SQL projection aliases are
    not generally supported in Toron today);
  - computed order expressions (`ORDER BY a + b`) — **rejected**;
  - `ORDER BY` on a column not in the output — **rejected**.
- **Total order.** The effective sort key is the user's
  `ORDER BY` expressions followed by an implicit deterministic
  tie-break: every remaining output column in ordinal order,
  ascending, `NULLS LAST`. Tie-break keys are **appended at bind
  time** so `ExecutionPlan::Sort.order_by` is the complete,
  self-contained key list.
- **Null policy.** `NULLS FIRST` / `NULLS LAST` are supported and
  lowered to the physical null flag. **When omitted, the default
  is `NULLS LAST`** — for both ascending and descending user keys
  — pinned explicitly rather than inheriting Arrow's
  Spark-oriented `SortOptions` default.
- **Float sort keys.** `Float32` and `Float64` sort keys are
  **supported** in V1. Ordering follows Arrow 58's IEEE 754
  total order. Unlike ADR 005 float *join* keys (rejected
  pending canonical encoding, because *equality* semantics are
  unsound), a *sort* key needs only a stable total order, which
  Arrow provides. The exact ordering of the edge cases Toron
  cares about — `NaN`, `-0.0`, `0.0`, ordinary values, nulls —
  is pinned by the `sort_float_keys_follow_arrow_total_order`
  gate, not narrated in prose and not tied to a version string.
- **Single terminal gather-sort.** The planner emits the terminal
  `Exchange { partition_count: 1 }` explicitly below the `Sort`;
  the fragmenter never infers a hidden gather. The post-join
  gather-sort path is strategy-agnostic (hash-partitioned and
  broadcast).
- **In-memory sort, reuse budget path.** `SortExec` materializes
  and sorts under the existing budget envelope, reserving against
  `BudgetDimension::Memory`. Over-budget execution returns the
  existing `ExecutionError::BudgetExhausted`, mapping to the
  existing `TaskFailureKind::BudgetExhausted { dimension:
  BudgetDimension::Memory, .. }`. No sort spill, no new
  `TaskFailureKind`, no new `BudgetDimension`.
- **`DISTINCT`, `GROUP BY`, `HAVING`, `WITH`, and all
  dialect-specific top-level clauses loud-reject** through the
  existing `FrontendError::Unsupported` path.
- **No semantic fallback.** Every shape outside the supported
  class continues through `FrontendError::Unsupported`. ADR 010
  can only widen what executes correctly and convert silent drops
  to loud rejections.
- **New contract surface, scoped.** New IR variant
  (`LogicalPlan::Sort` / `ExecutionPlan::Sort`), new `SortExec`
  operator, updated IR variant-count architecture guard, new
  compile-time clause-accounting source guard. No new wire,
  `TaskFailureKind`, `BudgetDimension`, `JoinKind`,
  `JoinStrategy`, `MessageKind`, or Wendelon surface.

## Context

The roadmap's post-ADR-009 candidate list (A multi-tenant
governance, B wire efficiency, C V1.x runtime-filter extensions,
D worker-side Rune UDFs, E executor upgrade) was authored at the
ADR 009 planning session. Every entry is post-V1 (A), evidence-
blocked on the pending ADR 006 Wendelon production row (B, C), or
reserved/demoted (D, E). Reading that list alone, the council's
first instinct (Gemini iter-0, Codex iter-0) was to suspend
ADR 010 and pivot to the Wendelon-side ADR 006 work.

The roadmap's own discipline overrides that instinct: *"Between
sessions the roadmap lags the code on purpose,"* and *"Evidence
drives scope."* The A–E list is not wrong — it is incomplete,
authored before anyone read `SqlPlanner::lower_query`:

```rust
fn lower_query(&self, query: &Query) -> Result<LogicalPlan, FrontendError> {
    match &*query.body {
        SetExpr::Select(select) => self.lower_select(select),
        _ => Err(unsupported("unsupported query body")),
    }
}
```

`query.order_by`, `query.limit`, `query.offset`, `query.fetch`,
`query.with` are never inspected. `lower_select` never inspects
`select.distinct`, `select.group_by`, `select.having`,
`select.qualify`, and a dozen more. ADR 009 added careful
rejection for these shapes *inside supported `EXISTS` subquery
bodies* (`validate_exists_query_shape` /
`validate_exists_select_shape`) — but the **top-level query path
has no equivalent guard**. The result is a silent-wrong-results
defect on the V1 SQL surface: `SELECT * FROM t ORDER BY x`
returns unordered rows with no error.

This is the failure mode the roadmap's standing guardrail names:
*"Silently-wrong-results is the failure mode ADR 002 paid
complexity to prevent."* It is also a direct violation of the V1
target — "First Query with Integrity" plus "byte-identical
normalized output." The roadmap flags the missing ordering
contract twice without connecting it to a live bug: the V1.x
"Sort-merge join" item notes *"no `ORDER BY` semantics pinned in
Toron yet,"* and the V1 target paragraph demands integrity.

The council scored this candidate against A / B / C and selected
it: it is V1 (a correctness bug, not a missing feature), pure-
Toron with zero Wendelon dependency, does not touch the runtime-
filter path, and does not front-run the ADR 006 selective-bench
evidence. ADR 009's precedent — a frontend-correctness ADR
promoted on digest-parity evidence with no bench artifact —
applies structurally unchanged. The ADR 006 Wendelon production
row stays parallel work and feeds ADR 011 / B–C selection.

## Decision

### 1. Clause accounting via exhaustive three-bucket destructure

`lower_query` and `lower_select` destructure the `sqlparser`
`Query` and `Select` structs with **no `..` rest pattern**. Every
field is bound by name and assigned to exactly one bucket:
semantic-lowered, semantic-rejected-when-present (only when
`Some(_)` / non-empty / non-default), or parser-metadata-ignored
(explicitly bound and commented as carrying no clause semantics).

The no-`..` discipline is the load-bearing accounting guard: a
`sqlparser` upgrade that adds any `Query` or `Select` field
becomes a compile error, forcing a deliberate bucket assignment.
This is the ADR 010 analog of ADR 007's
`Context::with_default_modules` allow-list discipline. The
runtime behavior is pinned by a hard-gate table test; the
compile-time destructure plus a source-level architecture guard
(rejecting `..` in these two functions, with an explicit
allow-list for the named parser-metadata fields) make the
invariant survive dependency bumps.

The accounting is **field-level by construction** — it asserts
each `Query` / `Select` field is lowered, rejected, or
metadata-ignored. It does *not* assert that every item inside a
lowered field is fully handled: `Select.projection` is in the
semantic-lowered bucket and keeps its ADR 008 / 009 lowering
unchanged. Pre-existing projection-item gaps (mixed
aggregate / non-aggregate projection, wildcard mixed with
explicit items) are out of scope and named deferred work — see
§"Not in scope". Closing them is aggregate-path-adjacent and
would widen ADR 010 past its narrow contract.

### 2. `ORDER BY` rewrite shape

In `SqlPlanner::lower_query`, after `lower_select` produces the
select-level plan, a present `query.order_by` is lowered to:

```rust
LogicalPlan::Sort {
    input: Box<LogicalPlan::Exchange {
        input: <select-level plan>,
        partition_count: 1,
        key: vec![],          // gather, not repartition
    }>,
    order_by: Vec<SortExpr>,  // SortExpr { expr, descending, nulls_first }
    limit:  Option<usize>,    // from ORDER BY ... LIMIT n
    offset: usize,            // from ORDER BY ... OFFSET n, default 0
}
```

`Sort` sits **above `Project`** (Decision §5). The planner emits
the terminal `Exchange { partition_count: 1, key: [] }`
explicitly — matching the existing pattern where SQL join
lowering emits explicit hash-partitioned exchanges, and giving
the fragmenter a concrete exchange boundary to split rather than
inferring a hidden gather. ADR 003's strict stage barrier
guarantees the `Sort` task starts only after every upstream
partition manifest is accepted.

### 3. Physical key binding and bind-time tie-break

The binder validates each `SortExpr` against the `Sort` input's
output schema and lowers to output ordinals:

```rust
ExecutionPlan::Sort {
    input: Box<ExecutionPlan>,
    order_by: Vec<ExecutionSortKey>,   // complete: user keys + tie-break
    limit: Option<usize>,
    offset: usize,
    output_schema: SchemaRef,
}

ExecutionSortKey {
    ordinal: ColumnOrdinal,
    descending: bool,      // aligns with Arrow SortOptions
    nulls_first: bool,
}
```

The `descending` flag is chosen over `ascending` to map directly
onto Arrow's `SortOptions { descending, nulls_first }` — an
implementation-naming choice, not a contract issue; the contract
is `ASC` / `DESC` support plus explicit null policy.

Because keys resolve against the output schema, `SortExec` needs
no general `ExecutionExpr` evaluation in V1 — only ordinal column
comparison. The **implicit tie-break keys are appended at bind
time**: for every output ordinal not already present in the user
key set, the binder appends `ExecutionSortKey { ordinal,
descending: false, nulls_first: false }` (ascending, `NULLS
LAST`). This makes `ExecutionPlan::Sort.order_by` the complete,
self-contained, inspectable key list and keeps `SortExec` simple.

### 4. Total-order determinism contract

The effective sort comparator is the user's `ORDER BY`
expressions (each with its resolved `descending` / `nulls_first`
policy) followed by every remaining output column in ordinal
order, ascending, `NULLS LAST`. This makes the emitted row
sequence a deterministic function of the row *set* — two rows
compare unequal unless identical across the entire output schema,
and identical rows are by definition indistinguishable. The
contract is a **total order on output rows**, not a partial order
with arbitrary tie resolution.

This composes cleanly with ADR 003 §13 canonical normalization,
orthogonally: ADR 003's digest (concat → full-row sort → reslice
→ digest) proves *set equality* and is order-insensitive by
construction — unchanged for an ordered query. ADR 010 adds an
**ordered digest** (hash of the emitted row sequence, no
pre-sort) to prove *sequence equality*. Promotion gates assert
both.

### 5. `ORDER BY` key scope: output schema only

`ORDER BY` keys must resolve against the query's **output
schema**, by output column name or by 1-based positive integer
ordinal. `Sort` sits unambiguously above `Project`. The Decision
§4 tie-break surface is then exactly the output columns (no
hidden-column machinery); `SELECT a FROM t ORDER BY b` with `b`
unprojected **rejects** in V1. The full key-surface enumeration
is in the V1 contract summary; supporting input-only keys forces
the sort-below-`Project` + hidden-column design, reserved for
V1.x.

### 6. Bare `LIMIT` / `OFFSET` / `FETCH` reject

`LIMIT`, `OFFSET`, `FETCH FIRST/NEXT`, `WITH TIES`, `PERCENT`,
and dialect `LIMIT BY` without an accompanying `ORDER BY` are
**unsupported** in V1. Deterministic top-N requires
`ORDER BY ... LIMIT n`. Because bare `LIMIT` rejects, ADR 010
needs no standalone `Limit` IR variant.

### 7. Distributed shape: single terminal gather-sort, strategy-agnostic

V1 distributed ordering is one terminal `Exchange
{ partition_count: 1 }` followed by one `Sort`. The full result
set transits a single final task. This avoids range-partition
exchange, ordered-merge operators, skew mitigation, and cost-
model work.

The fragmenter must produce a terminal gather-sort stage whenever
the select-level plan is distributed — and this is **required,
not optional**. For a plan containing a distributed join, the
shape is `Sort(Exchange₁(Project(Join(…))))`; replacing only the
first join is insufficient — each join consumer would otherwise
run its own local sort wrapper and publish independent final
results. The fragmenter splits the terminal single-partition
exchange above the join into a real post-join gather stage
feeding one final sort task whose output is `FinalResult`.

The path is **strategy-agnostic**: it covers both
`HashPartitioned` and `Broadcast` join consumers, because SQL
lowering emits the same `Sort(Exchange₁(…))` shape regardless of
join strategy. A broadcast ordered join must not be left silently
wrong — it routes through the same shared terminal gather-sort
path. This is gated by **stage-shape inspection** (hard gates
13–16), not output digest alone.

### 8. Budget and spill: in-memory, reject over budget

`SortExec` materializes its input and sorts in memory, reserving
its buffer against `BudgetDimension::Memory` via the existing
budget envelope. Over-budget execution returns the existing
`ExecutionError::BudgetExhausted`; the distributed task path maps
it to the existing `TaskFailureKind::BudgetExhausted { dimension:
BudgetDimension::Memory, requested, available }`. ADR 010 ships
**no external merge sort and no sort spill** — a V1.x operator
ADR, analogous to the hash-spill extensions reserved after
ADR 004. **No new `TaskFailureKind`, no new `BudgetDimension`** —
both already exist and are reused; ADR 003's 7-variant guard
stays green. Phase 0 P0.8 pins the memory-estimate and
reservation mechanics, not the existence of the failure path.

### 9. NULL, float, and literal handling

- **NULL ordering.** `NULLS FIRST` / `NULLS LAST` lower to the
  physical `nulls_first` flag. **When omitted, the default is
  `NULLS LAST`** for both ascending and descending user keys;
  implicit tie-break keys are ascending `NULLS LAST`. Pinned
  explicitly, not inherited from Arrow's `SortOptions` default.
- **Float sort keys.** `Float32` and `Float64` sort keys are
  supported, ordered by Arrow 58's IEEE 754 total order. The
  exact edge-case ordering is pinned by the
  `sort_float_keys_follow_arrow_total_order` gate covering `NaN`,
  `-0.0`, `0.0`, ordinary values, and nulls — not by version
  string.
- **`LIMIT` / `OFFSET` literals.** `n` is a non-negative integer
  literal; expressions, parameters, decimals, and negative
  literals reject. Phase 0 P0.6 pins overflow behavior.

### 10. Unsupported shapes reject — no semantic fallback

Every shape outside the supported class continues through
`FrontendError::Unsupported`. A rejected shape behaves
byte-identically to today's parse-then-reject — except clauses
which today are *silently dropped* now *loudly reject*. No new
error variant — the existing unsupported-expression class carries
every rejection (ADR 008 P0.8 / ADR 009 discipline).

### 11. Surface invariants

`LogicalPlan` gains exactly one variant (`Sort`);
`ExecutionPlan` gains exactly one variant (`Sort`).
`LogicalPlan::Join` / `ExecutionPlan::Join` stay single-variant;
`JoinKind` stays eight; `JoinStrategy` stays two;
`TaskFailureKind` stays seven; `BudgetDimension` stays seven; no
`MessageKind`, telemetry field, or Wendelon scan-contract change.
ADR 010 **updates** the IR variant-count architecture guard
intentionally for `Sort` and **adds** the compile-time
clause-accounting source guard; it asserts the ADR 003 / 004 /
008 guard set otherwise stays green.

## Hard gates

### Ordering correctness (1-7)

1. `top_level_order_by_lowers_to_sort_operator`.
2. `order_by_executes_in_expected_order` — ordered-digest parity
   against a hand-built `Sort` plan.
3. `order_by_limit_offset_truncates_after_sort`.
4. `order_by_with_ties_is_total_and_reproducible` — ordered-digest
   parity across repeated runs.
5. `sort_default_null_policy_is_nulls_last`.
6. `sort_explicit_nulls_first_last_override_default`.
7. `sort_float_keys_follow_arrow_total_order` — `NaN`, `-0.0`,
   `0.0`, ordinary values, nulls.

### Binder / key scope (8-12)

8. `sort_key_resolves_against_output_schema`.
9. `order_by_positive_integer_resolves_output_ordinal`.
10. `order_by_zero_negative_or_out_of_range_ordinal_rejects`.
11. `sort_key_rejects_non_output_alias_and_computed_keys` —
    includes `SELECT a FROM t ORDER BY b`.
12. `sort_tie_break_keys_are_appended_by_output_ordinal_at_bind_time`.

### Distributed parity + stage shape (13-17)

13. `order_by_above_hash_join_has_terminal_single_task_sort_stage`
    — exactly one final task carrying `ExecutionPlan::Sort`,
    output `FinalResult`; one more stage than the unordered
    hash-join baseline.
14. `post_join_gather_stage_consumes_all_join_partitions` — the
    join-consumer stage publishes shuffle output to the terminal
    gather, not independent final results.
15. `order_by_above_broadcast_join_uses_terminal_gather_sort` —
    the strategy-agnostic path; a broadcast ordered join is not
    left silently wrong.
16. `ordered_digest_matches_single_process_and_distributed`.
17. `canonical_digest_still_matches_for_ordered_query` — ADR 003
    §13 set-equality digest unaffected by `Sort`.

### Clause accounting — no silent drop (18-23)

18. `limit_without_order_by_is_unsupported`.
19. `offset_without_order_by_is_unsupported`.
20. `fetch_and_limit_by_are_unsupported`.
21. `distinct_is_unsupported_not_dropped`.
22. `group_by_having_with_are_unsupported_not_dropped`.
23. `dialect_select_and_query_clauses_are_unsupported_not_dropped`
    — table test: `TOP`, `INTO`, `PREWHERE`, `QUALIFY`, named
    windows, `CLUSTER BY`, `DISTRIBUTE BY`, `SORT BY`,
    `connect_by`, `value_table_mode`, locks, settings, format
    clauses.

### Regression / architecture (24-26)

24. `existing_sql_queries_byte_identical_after_clause_accounting`
    — runs first (ADR 009 R1 posture).
25. `architecture_guards_unchanged_plus_sort_variant` —
    `logical_plan` / `execution_plan` variant-count guards
    updated intentionally for `Sort`; `Join` single-variant,
    `JoinKind` eight-variant, `JoinStrategy` two-variant,
    `TaskFailureKind` seven-variant, `BudgetDimension`
    seven-variant, and message/wire-surface guards all stay
    green; a source-level guard asserts no `..` rest pattern in
    `lower_query` / `lower_select` (parser-metadata allow-list
    permitted).
26. `sort_over_budget_fails_cleanly` — over-budget `SortExec`
    returns `ExecutionError::BudgetExhausted` mapping to
    `TaskFailureKind::BudgetExhausted`; no new variant.

## Promotion evidence

ADR 010 introduces a new operator but makes **no performance
claim** — it closes a correctness defect. Promotion evidence is
**digest parity** in two forms — the ADR 003 §13 canonical digest
(set equality, gate 17) and a new **ordered digest** (emitted-
sequence equality, gates 2, 4, 16) against hand-built `Sort`
plans — plus **stage-shape inspection** for the distributed
gather-sort (gates 13–15) and the updated/preserved architecture
guards (gate 25). No bench-results artifact — structurally
identical to ADR 009's promotion shape.

## Verification commands at promotion

```bash
cargo fmt -p toron --check
cargo check --workspace
cargo test -p toron --no-fail-fast
cargo test -p toron --test architecture_test --no-fail-fast
cargo test -p toron --test sort_ordering_test --no-fail-fast
cargo test -p toron --test clause_accounting_test --no-fail-fast
cargo test -p toron --test join_kind_test --no-fail-fast
cargo test -p toron --test distributed_process_test --no-fail-fast
cargo test --workspace --no-fail-fast
git diff --check
```

## Open questions

All policy forks are locked (Decision §1–§11). The residual items
are true Phase 0 implementation-audit details, recorded in the
impl plan — none is a contract fork:

- exact `sqlparser` 0.54 `Query` / `Select` field inventory and
  bucket assignment;
- exact `OrderByExpr` / `OrderBy` container field shape;
- exact `LIMIT` / `OFFSET` literal AST shape and overflow
  behavior;
- exact `SortExec` memory estimator for the
  `BudgetDimension::Memory` reservation;
- exact source-guard implementation for the no-`..` discipline;
- exact fragmenter stage / exchange IDs and replacement helpers
  for the post-join gather-sort (the *requirement* is settled;
  only the code mechanics are audited).

## What Not To Do

- **Don't let `lower_query` / `lower_select` destructure with
  `..`.** The exhaustive no-rest-pattern destructure is the
  load-bearing clause-accounting guard.
- **Don't treat parser-metadata fields as rejectable clauses.**
  `select_token`, `top_before_distinct`, `window_before_qualify`
  are bound-and-ignored bookkeeping.
- **Don't reject an absent clause.** Rejection fires only on a
  *present and non-empty* unsupported clause.
- **Don't add a standalone `Limit` IR variant.** Bare `LIMIT`
  rejects; `limit` / `offset` are fields on `Sort`.
- **Don't ship a partial-order `ORDER BY`.** Total order, always.
- **Don't place `Sort` below `Project`.** Keys resolve against
  the output schema; sort-below-project forces hidden-column
  machinery.
- **Don't support `ORDER BY` on non-output / alias / computed
  keys in V1.** Named, deferred extensions.
- **Don't construct tie-break keys inside `SortExec`.** Append
  them at bind time.
- **Don't let the fragmenter infer a hidden gather.** The planner
  emits the terminal `Exchange` explicitly.
- **Don't leave a broadcast ordered join silently wrong.** The
  post-join terminal gather-sort path is strategy-agnostic.
- **Don't prove the distributed gather-sort with output digest
  alone.** Stage-shape inspection is required.
- **Don't implement `DISTINCT` / `GROUP BY` / `HAVING` in
  ADR 010.** Aggregate-path contracts; ADR 010 loud-rejects only.
- **Don't ship distributed range sort or sort spill.** V1.x ADRs.
- **Don't add a new `TaskFailureKind` or `BudgetDimension`.**
  `TaskFailureKind::BudgetExhausted` and `BudgetDimension::Memory`
  already exist and are reused.
- **Don't use Arrow's `lexsort_to_indices` `limit` argument as a
  substitute for Toron's `LIMIT`.** Toron applies `OFFSET` then
  `LIMIT` *after* the full logical order is established.
- **Don't borrow SQL `FETCH` vocabulary for the IR.** The field
  is `limit`.
- **Don't make a cost claim.** No performance gate, no bench
  artifact.
- **Don't silently ignore `OrderByExpr` modifiers.** `WITH FILL`
  and unsupported modifiers reject loudly.

## Rejected alternatives

- **Suspend ADR 010 / pivot to Wendelon.** Considered (Gemini
  iter-0, Codex iter-0); rejected. The A–E candidate list was
  authored before the silent-drop defect was visible; the
  roadmap's "evidence drives scope" discipline says the council
  re-derives scope against current code. The Wendelon ADR 006
  production row stays parallel work.
- **"ORDER BY only" title (Claude topic iter-0).** Superseded by
  Codex's clause-accounting broadening — fixing four named
  clauses leaves the *class* open.
- **Two-bucket clause accounting (lower / reject).** Considered
  (Claude ADR iter-0); rejected (Codex iter-1). `sqlparser` 0.54
  `Select` carries non-semantic parser-metadata fields; routing
  them to "unsupported" overclaims that every field is a
  user-visible clause decision. Three buckets is the honest
  model.
- **`LIMIT` without `ORDER BY` with an implicit total-order
  tie-break.** Considered; rejected (Gemini topic iter-1). A bare
  `LIMIT` that silently triggers a full-dataset sort hides a
  performance cliff.
- **`ORDER BY` keys resolvable against pre-projection input.**
  Considered (Gemini ADR iter-0, Codex topic iter-1); rejected
  (Claude topic iter-2 S1, Codex ADR iter-1). It forces `Sort`
  below `Project` plus hidden-column machinery.
- **Tie-break keys constructed inside `SortExec`.** Considered
  (Claude ADR iter-0); rejected (Codex iter-1) in favor of
  bind-time appending — the physical plan is then self-contained
  and inspectable.
- **Reject float sort keys in V1.** Considered (Claude ADR iter-0
  P0.5 fork); rejected (Codex iter-2). A sort key needs only a
  stable total order, which Arrow's IEEE 754 total order
  provides — distinct from float *join* keys, which need sound
  *equality* semantics and stay rejected.
- **Defer `ORDER BY` ordinals to V1.x.** Considered (Claude ADR
  iter-0 P0.7 fork); rejected (Codex iter-2). 1-based positive
  output ordinals resolve against the output schema by position
  with no alias or hidden-column machinery — they are in the
  same "output-schema-only" class as column names.
- **Distributed range sort in V1.** Rejected — range-partition
  exchange, ordered-merge operator, skew mitigation, and cost
  model are each their own scope.
- **External merge sort / sort spill in V1.** Rejected — mirrors
  ADR 004's one-way-spill posture.
- **Clause accounting as a table test only.** Rejected (Claude
  topic iter-2 S2) — a table test catches only enumerated
  clauses; the compile-time no-`..` destructure catches a clause
  nobody has seen yet.
- **New `TaskFailureKind::SortMemoryExhausted`.** Rejected — sort
  memory exhaustion is a budget event;
  `TaskFailureKind::BudgetExhausted { dimension:
  BudgetDimension::Memory, .. }` already exists.
- **Leaving broadcast ordered joins out of the fragmenter path.**
  Considered; rejected (Codex iter-2). SQL lowering emits the
  same `Sort(Exchange₁(…))` shape regardless of join strategy;
  the post-join terminal gather-sort path is shared and
  strategy-agnostic rather than hash-partitioned-only.
- **Folding `DISTINCT` into ADR 010.** Rejected — `DISTINCT` is
  `GROUP BY <all output columns>` with no aggregates, an
  aggregate-path concern.
- **`SortExec` in a new `src/exec/sort.rs` module.** Deferred to
  implementer discretion (Codex iter-1) — `SortExec` starts in
  `exec.rs` near the aggregate/join helpers; a module split is
  out-of-scope churn unless file size forces it.

## Council convergence summary

Three-way agreement reached at topic iter-2 and carried through
two ADR-drafting rounds. Codex iter-2 approved this draft as the
promotion base; Gemini iter-2 ratified the same contract.

| Decision                                                  | Status    |
|-----------------------------------------------------------|-----------|
| Select ADR 010 from outside the roadmap A–E list          | converged |
| Selection basis: code evidence (silent-drop defect)       | converged |
| Title: clause accounting + deterministic result ordering  | converged |
| Scale boundary: planner + executor; secondary fragmenter  | converged |
| Three-bucket clause accounting (lower/reject/metadata)    | converged |
| Enforced by compile-time no-`..` destructure + source guard | converged |
| `ORDER BY` → new `LogicalPlan::Sort` / `ExecutionPlan::Sort` | converged |
| Bare `LIMIT` / `OFFSET` / `FETCH` reject                  | converged |
| `ORDER BY` keys: output names + 1-based positive ordinals | converged |
| Alias / computed / non-output keys reject                 | converged |
| Total-order determinism; tie-break appended at bind time  | converged |
| Physical keys are output `ColumnOrdinal`s, `descending` flag | converged |
| Default null policy `NULLS LAST`                          | converged |
| Float sort keys supported via Arrow IEEE 754 total order  | converged |
| Planner emits the terminal gather explicitly              | converged |
| Single terminal gather-sort; no distributed range sort    | converged |
| Post-join gather-sort required, strategy-agnostic         | converged |
| Fragmenter path gated by stage-shape inspection           | converged |
| In-memory sort; reuse `BudgetExhausted`; no new variant    | converged |
| `DISTINCT` / `GROUP BY` / `HAVING` loud-reject, not impl   | converged |
| Ordered digest + canonical digest both gated              | converged |
| No new wire / `TaskFailureKind` / `BudgetDimension` / Wendelon | converged |
| Digest parity + stage shape = promotion evidence, no bench | converged |
| ADR 006 Wendelon row stays parallel, feeds ADR 011        | converged |

All policy forks are locked. Residual items are Phase 0
implementation-audit details, recorded in the impl plan.
