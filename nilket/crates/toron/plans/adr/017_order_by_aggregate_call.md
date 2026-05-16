# ADR 017: `ORDER BY <aggregate-call>` for Grouped Aggregate SQL

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Merge of Claude iter-2 / iter-3 / iter-4
final positions.
**Companion implementation plan:**
`plans/impl/017_order_by_aggregate_call.md`

## Status

Promoted. Implementation pending.

Three-way AI Swarm Council convergence reached across four
iteration rounds. Iter-0 nominated `ORDER BY <aggregate-call>`
as the lead candidate alongside an evidence-hunt sweep for
code-evidence defects. Iter-1 ground-truth-verified that the
sole code-evidence candidate (`Function`-modifier silent-drop
in `lower_aggregate_function`) does not exist —
`aggregate_function_args` at `sql_planner.rs:1034-1063`
already rejects `over`, `filter`, `null_treatment`,
`within_group`, `parameters`, `uses_odbc_syntax`,
`duplicate_treatment`, and `clauses` with `"aggregate function
modifiers are not supported"`. The roadmap's "code-evidence
defects outrank the printed list" rule had no signal to fire
on. Iter-2 / iter-3 / iter-4 converged on the canonical
contract shape. Iteration artifacts retire when this plan is
promoted — git holds the narrative.

ADR 017 owns no independent bench artifact. It is the third
instance of the composition cycle established by ADR 015
(`Sort` over `Aggregate`) and proved by ADR 016 (`Filter` over
`Aggregate`); the load-bearing property of the cycle shape —
source-level boundary clarity at named-reject granularity, no
silent acceptance, no silent drop — is reaffirmed.

Load-bearing antecedents: ADR 010 (`Sort` operator surface,
total-order tie-break, output-schema-only sort keys); ADR 011
(grouped aggregate, single-table-only, projection-shape
contract); ADR 015 (terminal-gather shape, narrow fragmenter
detector ahead of the generic non-join multi-exchange reject,
`ORDER BY <aggregate-call>` explicitly excluded as V1); ADR 016
(planner-side rewrite walker, `GroupedAggregateScope` —
formerly `HavingAggregateScope` — keyed on
`HavingAggregateSignature`, four-way ambiguity classification,
`Filter` placement inside the terminal-gather `Exchange`,
`FilterDisposition::EvaluateInToron` for post-aggregate
predicates).

## Goal

Enable `ORDER BY` keys that reference projected aggregate calls
by name for queries that have a `GROUP BY` clause. The canonical
analytical pattern:

```sql
SELECT region, SUM(amount)
FROM sales
GROUP BY region
HAVING SUM(amount) > 1000
ORDER BY SUM(amount) DESC
LIMIT 10
OFFSET 1
```

Today this query rejects at `sql_planner.rs:795` in the generic
`"unsupported expression"` arm of `lower_expr` because
`SqlExpr::Function(SUM(amount))` has no `lower_expr` arm and
only ordinal / column-name keys are accepted in `ORDER BY`. ADR
017 lifts that aggregate-call shape into the same planner-side
rewrite walker ADR 016 introduced for `HAVING`, sharing one
projection-derived scope across both clauses.

## Scale boundary

**Primary:** Toron planner. SQL-AST rewrite + a contained
`LoweredSelect`-struct seam between `lower_select` and
`lower_query`.

In scope:

- introducing a private `LoweredSelect { plan,
  grouped_aggregate_scope }` struct as `lower_select`'s return
  value (named-field destructure at the single caller);
- plumbing the scope through `lower_query` into a new parameter
  on `lower_order_by`;
- invoking the renamed planner-side walker at each `OrderByExpr.
  expr` when scope is present;
- a rename pass making the walker / scope / signature clause-
  neutral: `HavingAggregateScope` → `GroupedAggregateScope`,
  `HavingAggregateEntry` → `GroupedAggregateEntry`,
  `HavingAggregateSignature` → `GroupedAggregateSignature`,
  `having_aggregate_signature` → `grouped_aggregate_signature`,
  `rewrite_having_aggregate_calls` →
  `rewrite_grouped_aggregate_calls`,
  `resolve_having_aggregate_reference` →
  `resolve_grouped_aggregate_reference`,
  `GroupedProjection.having_scope` field → `grouped_scope`. The
  walker reject string `"qualified HAVING column references are
  not supported"` is **not** renamed; HAVING's qualified-
  reference reject moves out of the walker into a small
  `lower_having_predicate` pre-pass so the walker is shape-
  neutral and ORDER BY's `CompoundIdentifier` path (e.g.,
  `ORDER BY sales.region`) is unchanged from ADR 015;
- a typed `ResolveOutcome` enum return from
  `GroupedAggregateScope::resolve`, with message formatting in
  the (renamed) caller — eliminates the need to encode the
  clause label inside `GroupedAggregateScope`;
- a `clause: &'static str` parameter on the walker and resolver,
  used only for `unsupported(...)` message formatting at the
  call sites;
- two new named rejects for the ORDER BY ambiguity classes
  (`"ORDER BY aggregate-call must appear in SELECT list"`,
  `"ambiguous ORDER BY aggregate-call"`);
- planner / binder / fragmenter / integration test coverage,
  including the dual-rewrite (`HAVING <agg> + ORDER BY <agg>`)
  composition;
- semantic architecture guards: presence of the two new ORDER
  BY reject strings, continued presence of the unchanged
  HAVING-qualified-reference reject string, no new IR /
  `bind_expr` / disposition variant. No exact helper-call-
  count guard is asserted; helper topology is private and
  refactor-friendly.

Explicitly unchanged:

- ADR 010's `Sort` operator surface, sort-key binder semantics,
  total-order tie-break appending, and terminal-gather shape;
- ADR 011's `Aggregate` operator surface, projection-shape
  contract, hash-partitioned exchange, one-exchange fast path;
- ADR 015's nested-shape detector at `fragment.rs:562-632` and
  the three-stage `StagePlan` decomposition (no detector
  change);
- ADR 016's `Filter` placement inside the terminal-gather
  `Exchange`, `FilterDisposition::EvaluateInToron` for post-
  aggregate predicates, the four-way ambiguity classification
  (inherited verbatim; applied symmetrically to ORDER BY), and
  the `bind_comparison` numeric-literal coercion;
- the `Filter` operator surface, `FilterDisposition` variant
  set, and `bind_expr` expression set (no new
  `Expr::AggregateCall` arm);
- the table-query wire frame, runtime-filter descriptor surface,
  PBC bytecode, and the ADR 013/014 application hooks;
- `JoinKind`, `JoinStrategy`, scheduler-barrier semantics,
  `TaskSpec`, `TaskFailureKind`, `BudgetDimension`,
  `MessageKind`, `TaskOutputBinding` variants;
- the existing duplicate-name behavior of the binder and ADR
  011's `aggregate_field` naming.

No new IR variant, no new fragmenter recognition site, no new
shuffle binding, no new stage role, no new failure kind, no new
wire surface. Every extension named in ADR 016's handoff
(`DISTINCT`, `HAVING` over joins, scalar-aggregate `HAVING` /
`ORDER BY`, computed `HAVING` / `ORDER BY` predicates, post-
aggregate projection, two-phase aggregation, new aggregate
functions, broadcast / outer / semi / anti / sort-merge / non-
equi join surface) is a separate future ADR.

## Cycle shape

ADR 017 is the third instance of the composition cycle. The
load-bearing property across the three:

> **Source-level boundary clarity.** Every SQL surface either
> lowers to an existing operator chain or rejects through a
> deterministic planner reject. No silent acceptance. No silent
> drop. The reject is typically a narrow named string for the
> common analytical shapes; deliberately generic catch-all
> rejects (`"unsupported expression"`) are acceptable for
> shapes a future ADR will lift into a dedicated surface (e.g.,
> scalar-aggregate `ORDER BY` per §10).

Direction of motion (remove vs introduce a reject) is not load-
bearing; boundary clarity is. The three instances:

- **ADR 015 (`Sort` over `Aggregate`):** removed one wide reject
  (`"GROUP BY with ORDER BY is not supported"`); added one
  narrow fragmenter detector ahead of the generic non-join
  multi-exchange reject. No new IR.
- **ADR 016 (`Filter` over `Aggregate`):** removed one wide
  reject (`"HAVING is not supported"`); introduced three narrow
  named rejects; extended ADR 015's detector with one optional
  `Filter` position; introduced the planner-side walker
  substrate. No new IR.
- **ADR 017 (`Sort` over `Filter`-or-`Aggregate` with rewritten
  keys):** introduced two narrow named rejects; refactored the
  walker to be shape-neutral (its prior `CompoundIdentifier`
  reject arm moves to a `lower_having_predicate` pre-pass) so
  the same walker body serves both clauses without changing
  ORDER BY's qualified-reference behavior; reused the walker
  via one new caller in `lower_order_by`. No detector change.
  No new IR.

The architecture guards are semantic: presence of the new ORDER
BY reject strings, continued presence of the HAVING-qualified-
reference reject string, no new IR / `bind_expr` /
`FilterDisposition` variant. Exact helper-call counts are not
pinned — helper topology is private and a count guard would
fail under harmless refactors.

## Context

The rewrite-walker site:

- `SqlPlanner::rewrite_having_aggregate_calls` at
  `sql_planner.rs:432-461` — match arms for
  `SqlExpr::Function(f)` when
  `aggregate_function_name(&f.name).is_some()`,
  `SqlExpr::BinaryOp`, `IsNull`, `IsNotNull`, `Nested`, and
  `CompoundIdentifier` (reject with the qualified-references
  message). All other shapes pass through unchanged. The
  recursion is correct for ORDER BY predicates as much as for
  HAVING predicates — both clauses accept arbitrary `SqlExpr`
  from sqlparser.

The scope site:

- `HavingAggregateScope` at `sql_planner.rs:851-911` — records
  each projected aggregate's `HavingAggregateSignature` (enum
  keyed on `ColumnRef`: `CountAll`, `Count(ColumnRef)`,
  `Sum(ColumnRef)`), generated output name, and the full
  output-name multiset (group keys + aggregate fields) for
  ambiguity detection. `from_projection` constructs once;
  `resolve` reads; no `&mut self` methods.

The rejection site ADR 017 addresses:

- `SqlPlanner::lower_expr` at `sql_planner.rs:769-795` — only
  matches `SqlExpr::Function(f)` for `TIME_BUCKET`. Every other
  function call, including aggregate calls, falls through to
  `_ => Err(unsupported(format!("unsupported expression:
  {sql_expr:?}")))` at line 795. There is no `"ORDER BY <agg>
  is not supported"` named reject string to grep for or
  remove — the failure path is generic.

The fragmenter detector:

- `is_terminal_sort_over_grouped_aggregate` at
  `fragment.rs:562-632` destructures `Sort { order_by, limit,
  offset, output_schema, input }` and clones `order_by`
  verbatim into the returned `TerminalSortGather`. No code path
  inspects `SortExpr.expr` shape. ADR 017's rewritten ORDER BY
  column refs flow through unchanged — zero fragmenter change.

The single caller of `lower_select`:

- `SqlPlanner::lower_query` at `sql_planner.rs:94`. Verified by
  grep — the `LoweredSelect` seam is contained to one caller.

## Decision

### 1. Walker / scope rename: clause-neutral; reject relocates

The walker is now shared across two callers. Apply the type /
helper-function renames listed in §"Scale boundary". The walker
*body* changes in one place: the `SqlExpr::CompoundIdentifier`
arm at `sql_planner.rs:456-458` is **removed**. The walker
becomes shape-neutral — non-aggregate-call expression shapes
pass through unchanged.

HAVING's qualified-reference reject does not move into a
clause-conditional walker arm; it moves into a small pre-pass
inside `lower_having_predicate` that walks the `HAVING` AST and
rejects any `SqlExpr::CompoundIdentifier` it sees with the
unchanged string `"qualified HAVING column references are not
supported"`. User-visible HAVING behavior is byte-identical to
ADR 016.

ORDER BY's qualified-reference behavior stays on ADR 015's
path: `lower_order_by` invokes the walker (which leaves
`CompoundIdentifier` alone), then passes the result to
`lower_expr`, which resolves `SqlExpr::CompoundIdentifier`
via its existing `qualified_column` arm at
`sql_planner.rs:730-733`. `ORDER BY sales.region` continues to
work exactly as before ADR 017.

`lower_having_predicate` retains its `HAVING`-prefixed name —
it remains the HAVING-specific caller of the renamed walker,
now also the home of the qualified-reference pre-pass.

### 2. `lower_select` returns a private `LoweredSelect` struct

Introduce a private struct:

```rust
struct LoweredSelect {
    plan: LogicalPlan,
    grouped_aggregate_scope: Option<GroupedAggregateScope>,
}
```

Change `lower_select`'s return type from
`Result<LogicalPlan, FrontendError>` to
`Result<LoweredSelect, FrontendError>`.

- The grouped-aggregate branch returns
  `LoweredSelect { plan, grouped_aggregate_scope:
  Some(grouped_projection.grouped_scope) }`.
- Every other branch returns
  `LoweredSelect { plan, grouped_aggregate_scope: None }`.

A named struct over a tuple: two fields whose intent matters at
the read site (`plan` vs `grouped_aggregate_scope`); named-
field destructure makes the scope hand-off self-documenting and
avoids positional-argument bugs in any future caller.

Single caller (`lower_query` at `sql_planner.rs:94`)
destructures the named fields and consumes the scope when
invoking the new `lower_order_by` parameter (§3):

```rust
let LoweredSelect { plan, grouped_aggregate_scope } = match &**body {
    SetExpr::Select(select) => self.lower_select(select)?,
    _ => return Err(unsupported("unsupported query body")),
};
```

### 3. `lower_order_by` accepts the scope; invokes the walker

Extend `lower_order_by` (`sql_planner.rs:304-326`) with a
`scope: Option<&GroupedAggregateScope>` parameter. When `Some`:

- For each `OrderByExpr` in `order_by.exprs`:
  - Rewrite `expr.expr` via
    `self.rewrite_grouped_aggregate_calls(&expr.expr, scope_ref,
    "ORDER BY")?`.
  - Substitute the rewritten `SqlExpr` for the original before
    lowering via `self.lower_expr(...)`.
- The existing rejects on `interpolate.is_some()` and
  `with_fill.is_some()` stay verbatim.
- Existing field handling (`asc`, `nulls_first`) unchanged.

When `None`: existing behavior — no walker invocation, each
`expr.expr` lowers directly through `lower_expr`. The catch-all
reject at line 795 continues to fire for unsupported expression
shapes including aggregate-call references over non-grouped
queries.

### 4. Walker signature: clause-label threading

Add a `clause: &'static str` parameter to the walker and the
resolver:

```rust
fn rewrite_grouped_aggregate_calls(
    &self,
    expr: &SqlExpr,
    scope: &GroupedAggregateScope,
    clause: &'static str,  // "HAVING" or "ORDER BY"
) -> Result<SqlExpr, FrontendError>

fn resolve_grouped_aggregate_reference(
    &self,
    function: &Function,
    scope: &GroupedAggregateScope,
    clause: &'static str,
) -> Result<String, FrontendError>
```

Recursion forwards `clause` through the structural arms;
`resolve_grouped_aggregate_reference` formats the user-facing
reject messages with `clause` interpolated.

`&'static str` over an enum: two values, no behavior beyond
message formatting. An enum is over-engineered. ADR 016 hard-
coded the strings inline; ADR 017 extracts the variable into a
parameter, no further abstraction.

### 5. `GroupedAggregateScope::resolve` returns `ResolveOutcome`

Refactor `GroupedAggregateScope::resolve` from
`Result<String, FrontendError>` to a typed enum:

```rust
enum ResolveOutcome {
    Found(String),
    NotInSelect,
    Ambiguous,
}
```

The (renamed) caller `resolve_grouped_aggregate_reference`
formats the reject message with the clause label:

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

Two benefits: the scope stops embedding clause-aware message
text (it was already clause-specific via the `"HAVING"` literal
in ADR 016); test coverage on the scope's classification logic
no longer needs to grep error messages.

### 6. Four-way ambiguity classification — inherited

ADR 016 §4's four-way classification applies to ORDER BY
references against the same `GroupedAggregateScope`:

```text
zero structural matches            -> "ORDER BY aggregate-call must appear in SELECT list"
more than one structural match     -> "ambiguous ORDER BY aggregate-call"
one match, output name dup'd in    -> "ambiguous ORDER BY aggregate-call"
  full output schema
one match, output name unique      -> Expr::column(output_name)
```

Concretely:

- `ORDER BY COUNT(other)` over projection `(region, COUNT(*),
  COUNT(other))` — unique structural match, but both projection
  aggregates generate `"count"`. **Rejects as ambiguous.**
- `ORDER BY COUNT(*)` over `(region, COUNT(*), COUNT(*))` — two
  structural matches. **Rejects as ambiguous.**
- `SELECT count, COUNT(amount) FROM events GROUP BY count
  ORDER BY COUNT(amount)` — unique structural match, but the
  would-be substituted name `"count"` collides with the group-
  key output. **Rejects as ambiguous.**
- `ORDER BY SUM(amount) DESC` over `(region, SUM(amount))` —
  unique structural match, output name `"sum"` unique in scope.
  **Rewrites successfully.**

Standard SQL has no `ORDER BY <ordinal-as-aggregate-spell>`
form to disambiguate. ADR 010's existing `ORDER BY <ordinal>`
remains the ambiguity workaround. The walker fires only on
`SqlExpr::Function`; literal integers pass through unchanged.

### 7. Sort-key resolution inherits ADR 010 / 015 verbatim

After the walker substitutes `SqlExpr::Function(SUM(amount))` →
`SqlExpr::Identifier(Ident::new("sum"))`, the binder's existing
sort-key resolution against `Sort.input.schema()` resolves the
identifier to the aggregate output column.

When `HAVING` is present, `Sort.input` is `Filter(Aggregate)`;
`Filter` preserves schema, so resolution still targets
`Aggregate.output_schema`. ADR 016 already proved this.

ADR 010's total-order tie-break (append every remaining output
column ascending `NULLS LAST` at bind time) composes unchanged.

### 8. Fragmenter — zero change

`Sort(Exchange(1, [])(Aggregate(Exchange(K, group_expr)(scan))))`
is ADR 015's detector target. `Sort(Exchange(1, [])(Filter
(Aggregate(Exchange(K, group_expr)(scan)))))` is ADR 016's
detector-extension target. ADR 017's rewritten plans produce
exactly these two shapes — only the sort keys' expression
forms differ (column ref to aggregate output instead of literal
ordinal). `is_terminal_sort_over_grouped_aggregate` inspects
shape, not the sort-key expression. No detector change.

Negative-shape control: ADR 015's and ADR 016's
`MultipleExchangeBoundaries` rejects for `Project` between
exchanges and `Filter`-without-`Aggregate` still fire; ADR 017
adds regression cases to pin.

### 9. Dual-rewrite composition

`HAVING <agg> + ORDER BY <agg>` is a novel composition not
proved by ADR 015 or ADR 016. The flow:

1. `lower_select` lowers projection (constructs the
   `GroupedAggregateScope`).
2. `lower_select` processes `HAVING`: invokes
   `rewrite_grouped_aggregate_calls(&having, &scope, "HAVING")`;
   lowers the rewritten predicate; constructs
   `Filter(<rewritten>)(Aggregate(...))`.
3. `lower_select` returns `LoweredSelect { plan,
   grouped_aggregate_scope: Some(scope) }` to `lower_query`.
4. `lower_query` destructures the named fields and invokes
   `lower_order_by(order_by, grouped_aggregate_scope.as_ref())`.
   The walker runs against the same scope on each ORDER BY
   expr with `clause = "ORDER BY"`.
5. The resulting `Sort + Exchange(1, [])` wraps the
   `Filter(Aggregate(...))` body.

Two reads of the same `&GroupedAggregateScope` from two
sequential rewrite walks. No temporal coupling, no mutation —
the scope is constructed once and consumed read-only. The two
walks are independent in the dataflow sense.

Resulting plan shape, identical to ADR 016's terminal form
with column-ref sort keys:

```text
Sort(Exchange(1, [])
  Filter(<HAVING_rewritten>)
    Aggregate(Exchange(K, group_expr)
      <single-table input>))
```

A pinned integration test exercises this end-to-end.

### 10. Catch-all reject for non-grouped ORDER BY aggregate
references

When a query has `ORDER BY <aggregate-call>` but no `GROUP BY`,
`lower_select` returns `scope = None`. `lower_order_by` does
not invoke the walker; `lower_expr` hits the catch-all reject
at line 795.

ADR 017 does not lift this case into a named reject. The catch-
all message is ugly (`"unsupported expression: Function { name:
ObjectName([SUM]), args: ..., ...}"`) but functional and
honest — no widening of the surface, no silent acceptance.
Scalar-aggregate `ORDER BY` is a future ADR's surface; it would
lift this case along with scalar-aggregate `HAVING` (ADR 016
V1 exclusion §1) and other scalar-aggregate concerns under a
single contract.

### 11. No new surface

ADR 017 introduces no new IR variant; no new `JoinKind`,
`JoinStrategy`, `MessageKind`, `TaskFailureKind`,
`BudgetDimension`, `FilterDisposition`, scheduler barrier kind,
`TaskOutputBinding` variant; no new wire frame, runtime-filter
descriptor field, PBC opcode; no new aggregate function,
`aggregate_field` contract, or aggregate-output schema; no new
`bind_expr` match arm; no new planner cost-model knob; no
fragmenter recognition site; no new architecture guard *class*
(only new instances of the absence / presence / count guard
classes already established by ADR 010 / ADR 016).

## V1 exclusions

**ADR 017-specific exclusions:**

1. **`ORDER BY <aggregate-call>` without `GROUP BY`** —
   catch-all rejects via the generic `"unsupported expression"`
   path; not lifted into a named reject (§10). Future scalar-
   aggregate ADR's surface.
2. **`ORDER BY <aggregate-call>` not in `SELECT` list.** New
   reject: `"ORDER BY aggregate-call must appear in SELECT
   list"`. Parallel to ADR 016 §2.
3. **Ambiguous `ORDER BY <aggregate-call>` references.** New
   reject: `"ambiguous ORDER BY aggregate-call"`. Fires per the
   four-way classification, against the same full aggregate
   output schema as ADR 016 §4 (group-key names + aggregate
   field names).
4. **Qualified column references in `HAVING`.** Preserved
   verbatim from ADR 016. `lower_having_predicate`'s pre-pass
   rejects any `SqlExpr::CompoundIdentifier` in the HAVING AST
   with the unchanged string `"qualified HAVING column
   references are not supported"`. ORDER BY qualified column
   references (e.g., `ORDER BY sales.region`) continue to lower
   through `lower_expr`'s existing `CompoundIdentifier` arm
   unchanged from ADR 015 / ADR 010 — not in scope for ADR 017
   reject behavior.
5. **`ORDER BY -SUM(amount)`** and other `UnaryOp` shapes over
   aggregate calls. Walker does not recurse through
   `SqlExpr::UnaryOp`; the wrapped function is not substituted;
   `lower_expr` has no `UnaryOp` arm and rejects via catch-all.
   Existing behavior; future ADR if evidence justifies.
6. **`ORDER BY SUM(amount) + 100`** and other arithmetic over
   aggregate-call references. Walker substitutes the function
   call; `lower_expr` rejects the surrounding arithmetic via
   existing `BinaryOperator` reject (no Plus/Minus/Mul arm).
   Same as ADR 016 §7.

**Inherited rejects** (preserved verbatim from prior ADRs;
pinned by regression case but not new V1 surface):

7. **`ORDER BY` over joined queries** — inherits ADR 011's
   `"GROUP BY over joins is not supported"` reject.
8. **`ORDER BY` over `DISTINCT`** — inherits ADR 011's
   `"DISTINCT is not supported"`.
9. **Nested aggregate calls in ORDER BY** —
   `lower_aggregate_function` already rejects nested
   aggregates (called by the walker via
   `resolve_grouped_aggregate_reference`).
10. **`ORDER BY` aliased aggregates** — ADR 011 already rejects
    aliases on aggregates; the alias never reaches the
    projection.

## Consequences

- The canonical analytical pattern lowers, fragments, and runs:
  `SELECT region, SUM(amount) FROM sales GROUP BY region HAVING
  SUM(amount) > 1000 ORDER BY SUM(amount) DESC LIMIT 10 OFFSET 1`.
- ADR 015's `ORDER BY <aggregate-call>` V1 exclusion is closed.
- The walker is now a shared, shape-neutral artifact reused
  by `lower_having_predicate` and `lower_order_by`. Helper
  topology stays private; architecture guards are semantic
  (named-reject presence, no new IR / `bind_expr` / disposition
  variant) rather than call-site counts.
- ADR 016's narrowness preserved: `DISTINCT`, computed keys,
  aliases, post-aggregate projection, `GROUP BY` over joins,
  scalar aggregates still reject. ADR 017 is composition over
  the *current* ADR 011 + ADR 015 + ADR 016 surface.
- The composition cycle stabilizes with three instances. The
  shape — composition-only seam removal / addition, no new IR /
  wire / failure surface, reject-string presence/absence as
  architecture guard — is now a documented pattern with three
  proofs.

## Promotion evidence

Implementation promotion requires:

1. **Planner** —
   - `SELECT region, SUM(amount) FROM sales GROUP BY region
     ORDER BY SUM(amount) DESC LIMIT 10` lowers to `Sort(Exchange
     (1, [])(Aggregate(Exchange(K, [region])(Scan))))` with
     `order_by = [SortExpr(column "sum", desc=true, nulls_first
     =false), ...total-order-tie-break...]`, `limit = Some(10)`,
     `offset = 0`.
   - Dual-rewrite composition: `... HAVING SUM(amount) > 1000
     ORDER BY SUM(amount) DESC LIMIT 10 OFFSET 1` lowers to
     `Sort(Exchange(1, [])(Filter(Aggregate(Exchange(K,
     [region])(Scan)))))` with both predicates rewriting to the
     aggregate-output column reference.
   - `ORDER BY 2 DESC` (ordinal form) continues to lower
     unchanged; the walker doesn't fire (no
     `SqlExpr::Function` match).
   - `ORDER BY region` (group-key reference) lowers without
     invoking the rewrite.
   - Case-insensitivity: `SELECT region, sum(amount) ... ORDER
     BY SUM(amount)` lowers identically to the uppercase
     variants.
   - V1 exclusion items §2-§4 reject with stable, user-readable
     errors.
   - V1 exclusion items §5-§6 reject via existing paths.
   - V1 exclusion items §7-§10 each remain rejected by their
     pre-existing inherited path.
   - The renamed walker is reused by `lower_having_predicate`
     and `lower_order_by`. (No exact call-site count is
     asserted; helper topology is private.)
   - The string `"qualified HAVING column references are not
     supported"` remains present in `sql_planner.rs` — now at
     `lower_having_predicate`'s pre-pass rather than inside the
     walker — and continues to fire for `HAVING t.col` queries.
   - `ORDER BY sales.region` (qualified column reference,
     non-aggregate) lowers unchanged from ADR 015 — through
     `lower_expr`'s `CompoundIdentifier` arm.
   - ADR 011 / ADR 015 / ADR 016 fast paths unchanged.

2. **Binder / executor** —
   - Rewritten ORDER BY column references resolve via existing
     `bind_expr` + `resolve_column` paths against
     `Sort.input.schema()`, which equals `Filter(Aggregate)`'s
     output schema (== `Aggregate.output_schema`) when HAVING
     present, else `Aggregate.output_schema` directly.
   - No new `bind_expr` arm.
   - ADR 010's total-order tie-break composes unchanged.
   - ADR 016's `bind_comparison` numeric-literal coercion
     unchanged.

3. **Fragmenter** —
   - `Sort(Exchange(1, [])(Aggregate(Exchange)))` fragments via
     ADR 015's detector unchanged.
   - `Sort(Exchange(1, [])(Filter(Aggregate(Exchange))))`
     fragments via ADR 016's detector extension unchanged.
   - **Negative-shape control:** ADR 015's and ADR 016's
     negative-shape rejects (`Project` between exchanges,
     `Filter`-without-`Aggregate`) still fire with
     `MultipleExchangeBoundaries`. Regression cases pin.

4. **Integration** —
   - In-process SQL result digest-matches a hand-built logical
     plan with the same `Sort(Exchange(Aggregate(Exchange)))`
     (no HAVING) and `Sort(Exchange(Filter(Aggregate
     (Exchange))))` (with HAVING) shapes.
   - Multi-process distributed execution returns deterministic,
     digest-equal results for the dual-rewrite fixture across 2-
     and 4-worker counts.
   - `LIMIT + OFFSET + HAVING + ORDER BY <aggregate-call>`
     composes correctly under the terminal sort stage.

No bench artifact. ADR 017 makes no performance claim; it
unlocks a composed SQL surface with correctness evidence.

## What not to do

- Do not widen `bind_expr` to learn `Expr::AggregateCall`
  resolution. ADR 017 is a planner-side rewrite, echoing ADR
  016's posture (Shape C).
- Do not match `ORDER BY` aggregate-call references on raw
  `sqlparser::ast::Function` `PartialEq`. ADR 016's lesson
  (byte-equal preserves user casing; `HAVING sum(amount)` would
  mismatch `SELECT SUM(amount)`) applies verbatim to ORDER BY.
  The match key remains the planner-local
  `GroupedAggregateSignature` derived from
  `lower_aggregate_function`'s lowered output.
- Do not invent an `ORDER BY <ordinal-as-aggregate-spell>`
  form. Standard SQL has none; ADR 010's existing
  `ORDER BY <ordinal>` is the ambiguity workaround.
- Do not lift `ORDER BY` aggregate-call references into the
  `SELECT` list when missing. Same Toron V1 narrowness as ADR
  016 §2; a future ADR can widen.
- Do not lift the §10 catch-all reject for non-grouped ORDER
  BY aggregate references into a named string. The catch-all
  is functionally correct; scalar-aggregate ORDER BY is a
  future ADR's surface.
- Do not widen ADR 011's `GROUP BY over joins`, `DISTINCT`,
  alias, or computed-key rejects.
- Do not add a new `FilterDisposition` variant or any other
  IR / wire / failure-kind / budget-dim surface.
- Do not extend the walker to recurse through
  `SqlExpr::UnaryOp` or other shapes outside ADR 016's
  recursion set. The recursion surface is ADR 016's; widening
  is a separate decision.
- Do not split ADR 017 into "rename chore" + "feature ADR"
  unless the council explicitly prefers it. The rename and
  the walker's second caller are part of the same contract
  decision (walker becomes clause-neutral); splitting them
  obscures the rationale at review time. Default is in-
  feature single commit; pre-cycle `chore:` slice is
  acceptable under the architecture guards.
- Do not pin the renamed-helper names or exact call-site counts
  in the contract. Load-bearing surfaces are: the shape-neutral
  walker reused by `lower_having_predicate` and
  `lower_order_by`, the four-way ambiguity classification
  applied consistently across both clauses, the semantic
  reject-string presence guards, and the `LoweredSelect` seam
  between `lower_select` and `lower_query`.
- Do not make the walker's `CompoundIdentifier` reject clause-
  conditional. The cleaner separation is: walker normalizes
  aggregate-call references (shape-neutral); each clause
  caller imposes its own pre-walker validation. HAVING's
  qualified-reference reject lives at
  `lower_having_predicate`'s pre-pass; ORDER BY imposes no
  such pre-pass because qualified ORDER BY references are
  supported (ADR 015 inheritance).
- Do not encode the clause label inside
  `GroupedAggregateScope`. The scope is clause-neutral; the
  label lives at the caller and is threaded as a parameter to
  the walker and resolver for message formatting only. The
  `ResolveOutcome` enum is the seam.

## ADR 018 handoff

ADR 018 selection starts from the post-ADR-017 remainder:

- **`DISTINCT`** — `GROUP BY <all output columns>` with no
  aggregates. Requires loosening ADR 011's zero-aggregate
  reject; not pure composition. Own ADR with its own decision
  matrix.
- **Post-aggregate projection** (aliases, computed expressions
  on aggregates, key subsets, reordered keys). Own ADR; widens
  ADR 011's projection contract. ADR 017's
  `GroupedAggregateScope` output-name multiset becomes the
  authoritative output-name catalog when this widens.
- **`GROUP BY` over joins** — composes ADR 008 + ADR 011.
  Fragmenter-routing question is its own ADR.
- **Scalar-aggregate `HAVING` / `ORDER BY`** — lifts the
  catch-all rejects ADR 016 §1 and ADR 017 §10 left in place.
  Own ADR.
- **V1.x runtime-filter extensions** (Candidate C cluster) —
  each its own narrow ADR; promotion still requires per-
  extension evidence.

**Code-evidence defects continue to outrank the printed list
when one surfaces.** Iter-1's evidence-hunt sweep falsified the
only candidate that surfaced this cycle
(`Function`-modifier accounting was already loud-rejected by
`aggregate_function_args`); the lesson is that evidence-hunt
sweeps need to trace the call graph, not stop at the top-level
destructure.

The roadmap's `ADR 018 candidates` section owns the forward-
looking detail; code-evidence defects (silent-drop, inert-
feature) continue to outrank the printed list when one
surfaces.
