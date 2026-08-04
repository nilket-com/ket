# ADR 016: Post-Aggregate Filtering with `HAVING`

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Merge of Claude / Codex / Gemini iter-5
final positions.
**Companion implementation plan:**
`plans/impl/0016_post_aggregate_filtering_with_having.md`

## Status

Promoted and implemented.

Three-way AI Swarm Council convergence reached across
five iteration rounds. Iteration artifacts retire when
this plan is promoted — git holds the narrative.

ADR 016 owns no independent bench artifact. It is a
second instance of the ADR 015 cycle shape (pure
composition, narrow reject removal, narrow detector
extension, reject-string absence as architecture guard)
applied to a different surface: one new logical position
(post-aggregate `Filter`) plus a localized planner-side
rewrite of `HAVING` aggregate-call references. No new
operator and no performance claim warrant a bench row.

Load-bearing antecedents: Claude iter-0's framing of
`HAVING` as the printed ADR 016 candidate whose cycle
shape echoes ADR 015 verbatim; Claude iter-1 binder
finding that `bind_expr` has no `Expr::AggregateCall`
arm — forcing the design choice between three shapes
(group-keys-only, binder widening, planner pre-rewrite);
Codex iter-2 first-principles redline of Shape B (binder
widening) showing it does not avoid structural-match
logic, only relocates it, and `bind_expr` lacks
projection context today; Claude iter-2 Shape C lock
(planner pre-rewrite of `HAVING` AST, echoing ADR 009's
"targeted SQL-AST rewrite over expanding binder
surface"); Claude iter-2 fragmenter cost sharpening
(Site 1 needs zero new code because `find_exchange:
836-839` / `count_exchanges:850-853` /
`replace_exchange_with_scan:904-945` all recurse through
`Filter | Project | Aggregate | Sort`; only the ADR 015
detector at `fragment.rs:561` extends);
`FilterDisposition::EvaluateInToron` selection (post-
aggregate predicates never PBC-push naturally); Filter
inside the terminal-gather Exchange for wire efficiency
and stage symmetry; Codex iter-3 structural redline on
the detector's negative-shape control cases (`Project`
between exchanges, `Filter` without `Aggregate`); Claude
iter-4 rewrite-match key shift away from raw
`sqlparser::ast::Function` `PartialEq` (verified
byte-equal on `name: ObjectName`, case-sensitive)
toward a planner-local structural signature derived
from the lowered aggregate output via
`lower_aggregate_function`, canonicalizing case and
normalizing arguments; Gemini iter-5
V1 exclusion split (ADR-016-specific exclusions §1-§3
versus inherited rejects §4-§9), the layer-partitioned
promotion-evidence list, and the qualified-names-in-
`HAVING` exclusion (aggregate output schema is
unqualified); Codex iter-5 strict ambiguity policy
(reject when one structural match exists but the
generated output name is duplicated — duplicate
generated names produce ambiguous rewrites and must not
silently pick the first projection aggregate); Codex
iter-5 final redlines on signature-extension of
`lower_grouped_projection` (over a sibling helper), the
`HAVING 2 > 100` literal-predicate clarification
(standard SQL has no `HAVING <ordinal>` form), and the
load-bearing-surface discipline (don't pin internal
helper names in the contract; the contract is the
derived-from-lowered-semantics match key, the four-way
ambiguity classification, the detector-shape negative
cases, and the reject-string-absence architecture
guards).

## Goal

Enable post-aggregate predicate filtering on grouped
aggregate SQL via the `HAVING` clause, by adding a
post-aggregate `LogicalPlan::Filter` position above
`Aggregate` and a localized planner-side rewrite of
aggregate-call references in the `HAVING` predicate
against projection-output field names.

ADR 011 introduced single-table grouped aggregation;
ADR 015 introduced the nested
`Sort(Exchange(Aggregate(Exchange(...))))` shape for
`GROUP BY ... ORDER BY`. Both shipped with `HAVING`
explicitly rejected at the planner. ADR 016 removes
that reject with no new IR variant, no new wire frame,
no new `bind_expr` aggregate-call arm, no new
`FilterDisposition`, and no new fragmenter recognition
site (only an extension of the ADR 015 detector).

## Scale boundary

**Primary:** Toron planner and distributed fragmenter
composition over already-shipped operators.

In scope:

- removing the `"HAVING is not supported"` reject in
  `SqlPlanner::lower_select`;
- a planner-side rewrite of `HAVING` aggregate-call
  references against the projection's lowered
  aggregate calls, matching on a planner-local
  structural signature derived from the lowered
  aggregate output (see §2), and substituting
  column references that resolve via existing
  `bind_expr` paths;
- inserting a post-aggregate `LogicalPlan::Filter`
  above `Aggregate` (and inside the terminal-gather
  `Exchange` when `ORDER BY` is present);
- extending the ADR 015 fragmenter detector at
  `distributed/fragment.rs` to recognize an optional
  `Filter` between the terminal-gather `Exchange`
  and the `Aggregate`, folding the filter into the
  aggregate consumer stage;
- planner / binder / fragmenter / integration test
  coverage and source-shape architecture guards.

Explicitly unchanged:

- ADR 010's `Sort` operator surface, sort-key binder
  semantics, and terminal-gather shape;
- ADR 011's `Aggregate` operator surface, projection-
  shape contract, hash-partitioned exchange, one-
  exchange fast path;
- ADR 015's nested-shape detector, three-stage
  decomposition, and stage-dependency invariants
  (extension only, not contract change);
- the `Filter` operator surface, `FilterDisposition`
  variant set, and `bind_expr` expression set;
- the table-query wire frame, runtime-filter
  descriptor surface, PBC bytecode, and the
  ADR 013/014 application hooks;
- `JoinKind`, `JoinStrategy`, scheduler-barrier
  semantics, `TaskSpec`, `TaskFailureKind`,
  `BudgetDimension`, `MessageKind`,
  `TaskOutputBinding` variants;
- `RuntimeFilterDescriptor`, `RuntimeFilterKind`,
  and every ADR 006/013/014 carrier-and-application
  contract;
- the existing duplicate-name behavior of the binder
  and ADR 011's `aggregate_field` naming.

No new IR variant, no new fragmenter recognition site,
no new shuffle binding, no new stage role, no new
failure kind, and no new wire surface is reserved
here. Every extension below (`ORDER BY <aggregate-
call>`, `DISTINCT`, `HAVING` over joins, scalar-
aggregate `HAVING`, computed `HAVING` predicates,
post-aggregate projection, two-phase aggregation, new
aggregate functions, broadcast / outer / semi / anti /
sort-merge / non-equi join surface) is a separate
future ADR.

## Context

Three load-bearing antecedents:

- **ADR 011 — Aggregate Projection Accounting and
  `GROUP BY`.** Single-table grouped aggregation
  distributes as `Aggregate(Exchange(K,
  group_expr)(scan))`; projection lists group keys
  first then aggregate calls; `HAVING`, `DISTINCT`,
  joins under `GROUP BY`, aliases on aggregates, and
  computed group keys are explicitly rejected.
- **ADR 015 — Terminal Ordering for Grouped
  Aggregate SQL.** Composes ADR 010 and ADR 011 into
  `Sort(Exchange(1, [])(Aggregate(Exchange(K,
  group_expr)(scan))))`; a narrow fragmenter
  detector evaluates ahead of the generic non-join
  multi-exchange reject; three-stage `StagePlan`
  decomposition with the terminal sort stage
  barrier.
- **ADR 009 — Correlated `EXISTS` Decorrelation.**
  Established the council's "targeted SQL-AST
  rewrite in `SqlPlanner::lower_select`, reusing the
  logical and physical contract unchanged" posture
  for SQL-surface extensions that do not warrant a
  new IR or `bind_expr` projection-context seam.

The reject site:

- `SqlPlanner::lower_select` at
  `sql_planner.rs:171-172` —
  `if having.is_some() { return Err(unsupported
  ("HAVING is not supported")); }`. The exhaustive
  destructure at lines 123-144 binds
  `having: Option<Expr>` at line 138 (no `..` rest
  pattern per ADR 010's invariant). No binder-side
  reject; no executor-side reject. The planner
  reject is the *only* thing preventing
  `Filter(Aggregate(...))` from being produced
  today.

The fragmenter recognizes `Filter` in unary-descent
positions already: `find_exchange:836-839`,
`count_exchanges:850-853`, and
`replace_exchange_with_scan:904-945` all match
`ExecutionPlan::Filter | Project | Aggregate | Sort`
with identical union patterns. The generic non-join
single-exchange path at `fragment.rs:88-162` fragments
`Filter(Aggregate(Exchange_K(scan)))` cleanly once
produced; the only fragmenter change ADR 016 requires
is at the ADR 015 detector site (`fragment.rs:561`).

## Decision

### 1. Remove the planner reject

In `SqlPlanner::lower_select`, remove the
`having.is_some()` reject at `sql_planner.rs:171-172`.
The `having: Option<Expr>` binding in the exhaustive
destructure stays; ADR 016's lowering path takes over
when the field is `Some`.

When `having` is `Some` and `group_keys` is `Some`,
the planner produces:

```text
Filter(<rewritten_predicate>)
  Aggregate(Exchange(K, group_expr)
    <single-table input>)
```

When `order_by` is also present, `lower_query`'s
existing `Sort + Exchange(1, [])` wrap composes
naturally over the `Filter`:

```text
Sort(Exchange(1, [])
  Filter(<rewritten_predicate>)
    Aggregate(Exchange(K, group_expr)
      <single-table input>))
```

When `having` is `Some` and `group_keys` is `None`,
reject with `"HAVING without GROUP BY is not
supported"` (V1 exclusion §1).

### 2. Planner-side rewrite on a derived structural signature

The binder's `bind_expr` has no `Expr::AggregateCall`
match arm (`binder.rs:1131+`). Rather than widen
`bind_expr` (a new projection-context seam coupled
to aggregate-call binding), ADR 016 rewrites the
`HAVING` AST at the planner before lowering,
echoing ADR 009's posture.

**Match key:** the rewrite operates on a planner-
local structural signature derived from the lowered
aggregate semantics, not on raw `sqlparser::ast::
Function` AST. `Function`'s derived `PartialEq` is
byte-equal on all fields including `name:
ObjectName`, which preserves user casing —
`HAVING sum(amount)` would byte-mismatch `SELECT
SUM(amount)` despite being the same call.
`lower_aggregate_function` at `sql_planner.rs:
403-433` normalizes the function name via
`aggregate_function_name` (uppercase dispatch) and
lowers the args; the planner derives a private
signature from that lowered output. `Expr` does not
derive `PartialEq` (`ir.rs:68`) and
`AggregateFunction` derives `PartialEq` but not
`Eq` / `Hash` (`ir.rs:107`), so direct `(Aggregate
Function, Vec<Expr>)` equality is not available;
the signature is keyed on `ColumnRef` (`ir.rs:46`
derives `PartialEq, Eq, Hash`) narrow to the V1
grouped subset (`CountAll`, `Count(ColumnRef)`,
`Sum(ColumnRef)`). The companion impl plan owns
the signature type's name and exact shape; the
load-bearing contract is the derived-from-lowered-
semantics property: matching is case-insensitive
and argument-normalized.

**Mechanism:**

1. Collect the projection's signatures alongside
   their generated output field names.
   `lower_grouped_projection` (`sql_planner.rs:363`)
   already lowers each projected aggregate via
   `lower_aggregate_function`; the planner derives
   the signature from the lowered output and pairs
   it with the output field name produced by the
   V1 mapping (`CountAll`/`Count` → `"count"`,
   `Sum` → `"sum"`, matching the existing
   `binder::aggregate_field` private contract).
2. Walk the `HAVING` `SqlExpr` AST. For each
   `SqlExpr::Function(func)`:
   - Probe `aggregate_function_name(&func.name)`.
     If `None`, leave the node unchanged — non-
     aggregate calls fall through to `lower_expr`'s
     existing reject path.
   - If `Some`, call `lower_aggregate_function(func,
     AggregateSqlMode::Grouped)`. On lowering
     error, surface the error. On success, derive
     the signature from the lowered output. If the
     lowered args fall outside the V1 column-ref
     shape (e.g. a non-column arg), no signature
     can be formed; the call takes the "no match"
     path. Otherwise classify per §4.
   - On a unique match, substitute
     `SqlExpr::Identifier(Ident::new
     (output_field_name))`. On no match, return
     `unsupported("HAVING aggregate-call must
     appear in SELECT list")`.
3. Lower the rewritten `SqlExpr` via `lower_expr`.
   The binder sees a plain column reference and
   resolves it against `Aggregate.output_schema`
   via the existing `resolve_column` path
   (`binder.rs:78-100`).

Group-key references in `HAVING` (e.g. `HAVING
region = 'EMEA'`) require no rewrite — they resolve
against the aggregate output schema by name
directly, the same mechanism ADR 015's sort-key
binder uses.

The rewrite is consulted only at the `HAVING`
lowering site. `ORDER BY <aggregate-call>` continues
to reject per ADR 015's V1 exclusion; closing it is
an ADR 017 candidate that extends the same rewrite
to `lower_order_by`.

### 3. Sort-key resolution inherits ADR 015 verbatim

When `HAVING` and `ORDER BY` compose, the sort input
schema is `Filter.input.schema() ==
Aggregate.output_schema` (`Filter` preserves
schema). ADR 010's sort-key binder resolves against
the Filter's input schema unchanged. No binder
change.

The ordinal-based top-N pattern from ADR 015
(`ORDER BY 2 DESC LIMIT 10`) composes with `HAVING`
directly:

```sql
SELECT region, SUM(amount)
FROM sales
GROUP BY region
HAVING SUM(amount) > 1000
ORDER BY 2 DESC
LIMIT 10
OFFSET 1
```

### 4. Four-way ambiguity classification

The rewrite classifies each `HAVING` aggregate-call
reference against the projection's lowered aggregates
and the **full aggregate output schema** (group-key
names *plus* aggregate field names):

```text
zero structural matches              -> reject: "HAVING aggregate-call must appear in SELECT list"
more than one structural match       -> reject: "ambiguous HAVING aggregate-call"
one structural match,                -> reject: "ambiguous HAVING aggregate-call"
  output name appears >1 time in
  full output schema
one structural match,                -> rewrite to Expr::column(output_name)
  output name appears exactly once
```

Concretely:

- `HAVING COUNT(other) > 5` over projection
  `(region, COUNT(*), COUNT(other))` — one structural
  match (`(Count, [other])`), but both `COUNT(*)` and
  `COUNT(other)` generate the field name `"count"`,
  so the output-name count in scope is 2. **Rejects
  as ambiguous.**
- `HAVING COUNT(*) > 5` over projection `(region,
  COUNT(*), COUNT(*))` — two structural matches.
  **Rejects as ambiguous.**
- `SELECT count, COUNT(amount) FROM events GROUP BY
  count HAVING COUNT(amount) > 0` — one structural
  match (`(Count, [amount])`), but the would-be
  substituted name `"count"` also appears as a
  group-key output (`count`); the full-schema
  count is 2. **Rejects as ambiguous.** Group-key
  collisions with aggregate-generated names are in
  scope, not silently delegated to the binder.
- `HAVING SUM(amount) > 100` over `(region,
  SUM(amount))` — one structural match, output name
  `"sum"` is unique in scope. **Rewrites
  successfully.**
- `HAVING COUNT(amount) > 5` over `(region,
  SUM(amount), COUNT(amount))` — one structural
  match, output name `"count"` is unique in scope.
  **Rewrites successfully.**

Standard SQL has no `HAVING <ordinal>` form; ADR 016
does not invent one. `HAVING 2 > 100` is interpreted
as a literal predicate (`2 > 100`, always false),
not an ordinal reference to the second output
column. The ambiguous-aggregate workaround is to
restructure the query to eliminate the duplicate.

The generated-name form (`HAVING count > 1` instead
of `HAVING COUNT(*) > 1`) is not a rewrite path —
the rewrite fires only on `SqlExpr::Function` nodes.
A bare identifier `count` resolves through the
existing binder against the aggregate output schema
and inherits the existing duplicate-name binder
ambiguity behavior verbatim.

### 5. Fragmenter — Site 1 needs zero new code; Site 2 extends ADR 015 detector

**Site 1 (no `ORDER BY`).** The non-`ORDER BY`
`HAVING` case (`Filter(Aggregate(Exchange_K
(group_expr)(scan)))`) requires zero new fragmenter
code. The generic single-exchange path at
`fragment.rs:88-162`:

- counts the one group-key exchange via
  `count_exchanges`, which recurses through
  `Filter | Project | Aggregate | Sort`
  (`fragment.rs:850-853`);
- locates the exchange via `find_exchange`, same
  recursion (`fragment.rs:836-839`);
- builds the consumer plan via
  `replace_exchange_with_scan`, same recursion
  (`fragment.rs:901-965`), producing
  `Filter(Aggregate(Scan))` as the consumer task
  plan;
- executes the consumer task via the polymorphic
  `Filter` executor at `exec.rs:494-516`, which
  calls `execute_bound(input)` recursively with no
  type gate.

**Site 2 (with `ORDER BY`).** The `ORDER BY` case
extends the ADR 015 detector at `fragment.rs:561`.
The current detector matches `Sort(Exchange(1, [])
(Aggregate(Exchange(K, group_expr)(input))))`;
ADR 016 extends it to match an optional `Filter`
between the terminal-gather `Exchange` and the
`Aggregate`:

```text
Sort(Exchange(1, [])
  Filter?(<predicate>, <disposition>)
    Aggregate(Exchange(K, group_expr)
      <single-table input>))
```

On match, the detector's return struct carries an
optional `having: Option<(ExecutionExpr,
FilterDisposition)>`; the three-stage builder wraps
the constructed `aggregate_plan` in
`ExecutionPlan::Filter` when the predicate is
`Some`. Stage 0 (producer), Stage 1 task bindings
(`ShuffleConsumer` for the group-keyed exchange,
`ShuffleProducer` for the terminal-gather
exchange), and Stage 2 (terminal sort) are all
unchanged.

The detector remains narrow: a `Filter` between the
terminal-gather `Exchange` and any non-`Aggregate`
input, or a non-`Filter` non-`Aggregate` node in
the same position, falls through to the generic
non-join multi-exchange reject. Anything else with
two non-join exchanges continues to reject.

### 6. `FilterDisposition::EvaluateInToron`

The `ExecutionPlan::Filter` variant carries a
`FilterDisposition` (`ir/execution.rs:57-60`) with
two values: `FullyPushed` (predicate compiled to
PBC and attached to a `Scan`'s `pbc_program`) and
`EvaluateInToron` (per-row evaluation in Toron).
For a post-aggregate `HAVING` filter, the binder
flow at `binder.rs:83-93` runs
`pbc_eligible() → compile_pbc(...) →
attach_pbc_program(input, program)`. Eligibility
and compilation may succeed (column-ref +
comparison + literal is structurally PBC-friendly),
but `attach_pbc_program` returns `Err(input)`
because the filter's input is an `ExecutionPlan::
Aggregate`, not a `Scan` — attachment is the step
that fails, not compilation. The disposition then
resolves to `EvaluateInToron`. No new variant; no
new binder arm.

### 7. Filter inside the terminal-gather Exchange

For `HAVING + ORDER BY`, the planner emits the
`Filter` *inside* the terminal-gather `Exchange`
(§5 Site 2 shape), not outside. Two reasons:

1. **Wire efficiency.** Filter reduces row count
   before the terminal-gather shuffle.
2. **Stage symmetry.** Filter folds into Stage 1
   (aggregate consumer); Stage 2 stays a pure sort
   task, preserving the ADR 015 three-stage shape.

The outside-gather form is not produced by the
planner and not recognized by the fragmenter.

### 8. Preserved fast paths

When the query has no `HAVING`, ADR 011's two-stage
and ADR 015's three-stage shapes are unchanged.

When the query has `HAVING` but no `GROUP BY`, the
planner rejects with `"HAVING without GROUP BY is
not supported"` (V1 scalar-aggregate exclusion).

A regression test pins each preserved fast path.

### 9. No new surface

ADR 016 introduces no new IR variant; `JoinKind`,
`JoinStrategy`, `MessageKind`, `TaskFailureKind`,
`BudgetDimension`, `FilterDisposition`, scheduler
barrier kind, or `TaskOutputBinding` variant; wire
frame, runtime-filter descriptor field, or PBC
opcode; aggregate function, `aggregate_field`
contract, or aggregate-output schema; `bind_expr`
match arm; or planner cost-model knob.

The composition rides on the existing
`LogicalPlan::Filter` / `ExecutionPlan::Filter`
variants and the existing `EvaluateInToron`
disposition.

### 10. Numeric comparison literal coercion

`HAVING SUM(amount) > 5` compares an aggregate output
whose type is known only after binding against an SQL
integer literal parsed as `ScalarValue::Int32`. The
planner-side rewrite cannot safely coerce that literal:
`SUM`'s output type depends on the bound input type and
there is no catalog-typed expression available at the
SQL-AST rewrite layer.

ADR 016 therefore permits one narrow binder behavior:
`bind_comparison` may coerce an `Int32` literal to
`Int64` or `Float64` when the opposite side of the
comparison is a typed, non-literal expression of that
type. Literal-vs-literal comparisons are unchanged. This
does not add a `bind_expr` match arm, does not teach
`bind_expr` about aggregate calls, and does not create
projection-context lookup in the binder.

## V1 exclusions

**ADR 016-specific exclusions** (new rejects that
ADR 016 owns):

1. **Scalar-aggregate `HAVING`** (no `GROUP BY`).
   New reject string: `"HAVING without GROUP BY is
   not supported"`. Reserved for the ADR that ships
   distributed scalar aggregates.
2. **`HAVING` aggregate-call references not in the
   `SELECT` list.** Toron V1 restriction (no hidden
   aggregate columns). Standard SQL and most engines
   (Postgres, BigQuery, Snowflake, MySQL) permit
   `HAVING COUNT(*) > N` without projecting
   `COUNT(*)`; ADR 016 chooses the narrower surface
   to avoid implicit projection lifting or a
   separate hidden-output schema concept. Either
   widening is a future ADR. New reject string:
   `"HAVING aggregate-call must appear in SELECT
   list"`.
3. **Ambiguous `HAVING` aggregate-call references.**
   New reject string: `"ambiguous HAVING aggregate-
   call"`. Fires per the four-way classification in
   §4 — multiple structural matches, or one match
   with a duplicated generated output name.
   Standard SQL has no `HAVING <ordinal>` form;
   ADR 016 does not invent one.
4. **Qualified column references in `HAVING`.**
   The aggregate output schema is unqualified;
   `HAVING t.region = 'EMEA'` is rejected by the
   planner before lowering with `"qualified HAVING
   column references are not supported"`. The
   explicit planner reject keeps the V1 surface
   narrow without relying on qualifier handling in
   the generic binder path.

**Inherited rejects** (preserved verbatim from
prior ADRs; pinned by regression case but not new
V1 surface for ADR 016):

5. **`HAVING` over joins** — inherits ADR 011's
   `"GROUP BY over joins is not supported"`
   (`sql_planner.rs:202-204`).
6. **`HAVING` over `DISTINCT`** — inherits ADR 011's
   `"DISTINCT is not supported"`
   (`sql_planner.rs:147-149`).
7. **Computed / arithmetic `HAVING` predicates** —
   `bind_expr` supports comparisons and boolean ops
   over column refs and literals; arithmetic on
   column refs remains unsupported.
8. **Nested aggregate calls** — `lower_aggregate_
   function` already rejects nested aggregates.
9. **`HAVING` referencing non-output columns** —
   the binder's `resolve_column` reject is
   standard-SQL semantics, preserved.
10. **`HAVING` on aliased aggregates** — ADR 011
    already rejects aliases on aggregates.

## Consequences

- Analytical post-aggregate filtering is supported:
  `SELECT region, SUM(amount) FROM sales GROUP BY
  region HAVING SUM(amount) > 1000 ORDER BY 2 DESC
  LIMIT 10` lowers, fragments, and runs against the
  live in-process Wendelon harness.
- The fragmenter extends one detector; the generic
  non-join single-exchange path handles the
  no-`ORDER BY` case unchanged.
- ADR 011's narrowness is preserved: `DISTINCT`,
  computed keys, aliases, post-aggregate
  projection, `GROUP BY` over joins, scalar
  aggregates still reject. ADR 016 is composition
  over the *current* ADR 011 + ADR 015 surface,
  not a widening of it.
- ADR 015's `ORDER BY <aggregate-call>` V1
  exclusion is preserved verbatim; closing it is
  an ADR 017 candidate via the same planner-
  rewrite pattern at `lower_order_by`.

## Promotion evidence

Implementation promotion requires:

1. **Planner** —
   - `SELECT region, SUM(amount) FROM sales GROUP
     BY region HAVING SUM(amount) > 1000` lowers
     to `Filter(<rewritten_predicate>)(Aggregate
     (Exchange(K, [region])(Scan)))`.
   - `... ORDER BY 2 DESC LIMIT 10 OFFSET 1`
     lowers to `Sort(Exchange(1, [])(Filter
     (Aggregate(Exchange(K, [region])(Scan)))))`
     with `limit = Some(10)`, `offset = 1`.
   - `HAVING <group-key> = <literal>` lowers
     without invoking the rewrite.
   - Case-insensitivity: `SELECT sum(amount) FROM
     ... HAVING SUM(amount) > 100` lowers
     identically to the uppercase variant.
   - V1 exclusion items §1-§4 each reject with a
     stable, user-readable error.
   - V1 exclusion items §5-§10 each remain
     rejected by their pre-existing inherited
     path.
   - The reject string `"HAVING is not supported"`
     is absent from `sql_planner.rs`.
   - ADR 011 / ADR 015 fast paths unchanged.

2. **Binder / executor** —
   - The rewritten predicate resolves via existing
     `bind_expr` + `resolve_column` paths against
     `Aggregate.output_schema`.
   - Comparisons may coerce an `Int32` numeric
     literal to `Int64` or `Float64` in
     `bind_comparison` when the opposite side is a
     typed, non-literal expression. Literal-vs-
     literal comparisons are unchanged.
   - `Filter` disposition is `EvaluateInToron`.
     For post-aggregate predicates, `attach_pbc_
     program` returns `Err(input)` because the
     filter input is `ExecutionPlan::Aggregate`,
     not `Scan` — attachment is the step that
     fails (see §6).
   - Aggregate consumer executes
     `Filter(Aggregate(Scan))` in one task.
   - Sort ordinals continue to resolve against the
     full aggregate output schema after Filter
     (Filter does not reduce schema).
   - Duplicate-aggregate-name `HAVING` references
     reject as ambiguous per the four-way
     classification in Decision §4; no silent
     first-match resolution.

3. **Fragmenter** —
   - Non-`ORDER BY` `HAVING`: zero new fragmenter
     code. Generic single-exchange path fragments
     `Filter(Aggregate(Exchange))` into two
     `StagePlan`s.
   - With `ORDER BY`: the extended detector at
     `fragment.rs:561` matches the nested shape
     with optional `Filter`, emits three
     `StagePlan`s with the filter folded into
     Stage 1. `Stage 2.upstream_stages == [Stage
     1.id]`.
   - **Negative-shape control:** a constructed
     `Sort(Exchange(1, [])(Project(Aggregate
     (Exchange(K, group_expr)(Scan)))))` still
     rejects with `MultipleExchangeBoundaries`.
   - **Filter-without-aggregate negative case:** a
     constructed `Sort(Exchange(1, [])(Filter
     (Project(Exchange(K, key)(Scan)))))` still
     rejects with `MultipleExchangeBoundaries`.

4. **Integration** —
   - In-process SQL result digest-matches a hand-
     built logical plan with the same
     `Filter(Aggregate(Exchange))` shape.
   - Multi-process distributed execution returns
     deterministic, digest-equal results for the
     `HAVING` fixture across multiple worker
     counts.
   - `LIMIT` + `OFFSET` + `HAVING` + `ORDER BY`
     compose correctly under the terminal sort
     stage.

No bench artifact. ADR 016 makes no performance
claim; it unlocks a composed SQL surface with
correctness evidence.

## What not to do

- Do not widen `bind_expr` to learn
  `Expr::AggregateCall` resolution. ADR 016 is a
  planner-side rewrite (Shape C), echoing
  ADR 009's posture.
- Do not move aggregate-output type inference into
  the SQL-AST rewrite. Numeric literal coercion for
  comparisons belongs in `bind_comparison` after the
  expression types are known.
- Do not match `HAVING` aggregate-call references
  on raw `sqlparser::ast::Function` `PartialEq`.
  The match key is a planner-local structural
  signature derived from the lowered aggregate
  output (case-insensitive, argument-normalized
  via `lower_aggregate_function`); see §2.
- Do not lift `HAVING` aggregate-call references
  into the `SELECT` list when missing. Most SQL
  engines permit this; ADR 016 chooses the narrower
  Toron V1 surface (no hidden aggregate columns) to
  avoid implicit projection rewriting. Lifting is
  a candidate for a future ADR.
- Do not invent a `HAVING <ordinal>` form.
  Standard SQL has no such form; `HAVING 2 > 100`
  is a literal predicate (`2 > 100`), not an
  ordinal reference. Ambiguous aggregate
  references reject per §4 — the user restructures
  the query.
- Do not close ADR 015's `ORDER BY <aggregate-
  call>` V1 exclusion as a side effect. ADR 017
  candidate via the same rewrite at
  `lower_order_by`.
- Do not widen ADR 011's `GROUP BY over joins`,
  `DISTINCT`, alias, or computed-key rejects.
  ADR 016 is composition, not widening.
- Do not enable scalar-aggregate `HAVING`.
  Scalar aggregates are the surface of their own
  ADR.
- Do not add a new `FilterDisposition` variant
  for post-aggregate predicates.
  `EvaluateInToron` applies; `attach_pbc_program`
  fails on `Aggregate` input (see §6).
- Do not place the post-aggregate `Filter`
  outside the terminal-gather `Exchange`. Filter
  inside the gather is the load-bearing physical
  shape.
- Do not widen the fragmenter's multi-exchange
  guard beyond the one ADR 015 detector
  extension. The detector remains narrow; the
  `Filter`-without-`Aggregate` and `Project`-
  between-exchanges negative cases stay
  rejected.
- Do not introduce new IR variants, wire frames,
  message kinds, failure kinds, or budget
  dimensions for this composition.
- Do not add a planner cost-model opt-in or
  knob; ADR 016 is unconditional once the
  planner reject is removed.
- Do not pin internal helper names in the
  contract (rewrite-map builder, detector struct
  field names, etc.). The load-bearing surfaces
  are the planner-local structural signature
  derived from the lowered aggregate output as
  the match key (§2), the four-way ambiguity
  classification (§4), the negative-shape
  detector cases (§5), and the reject-string-
  absence architecture guards.

## ADR 017 handoff

ADR 017 selection starts from the adjacent SQL-
surface candidates remaining after ADR 016:

- `ORDER BY <aggregate-call>` — natural symmetric
  extension; same planner-rewrite pattern at
  `lower_order_by`. ADR 015's V1 exclusion this
  would close.
- `DISTINCT` — `GROUP BY <all output columns>`
  with no aggregates; lowering question is
  whether to reuse the `Aggregate` path or stand
  on its own.
- Post-aggregate projection (aliases, computed
  expressions on aggregates, key subsets,
  reordered keys) — each its own decision-matrix
  item.
- `GROUP BY` over joins — composes ADR 008 +
  ADR 011; fragmenter-routing question is its
  own ADR.

The roadmap's `ADR 017 candidates` section owns
the forward-looking detail; code-evidence
defects (silent-drop, inert-feature) continue to
outrank the printed list when one surfaces.
