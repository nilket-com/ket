# ADR 023: Post-Aggregate Projection

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Companion implementation plan:**
`plans/impl/023_post_aggregate_projection.md`

## Status

Promoted.

First substrate-and-surface ADR under the post-ADR-018 cycle
after four kernel-domain ADRs in a row (019 MIN/MAX, 020 AVG,
021 catalog refactor, 022 Float MIN/MAX). Decouples user-facing
SELECT projection from the `Aggregate` operator's natural
output schema. Lifts the strict-positional grouped-projection
reject (`sql_planner.rs:432, 437, 450, 1101`) by composing
existing `LogicalPlan::Project` over `LogicalPlan::Aggregate`
with an aggregate-output catalog.

Three-way council convergence reached across four iterations:

- **Lead.** Post-aggregate projection. (Claude / Codex /
  Gemini iter-0.)
- **Catalog-first triple-pass planner.** Discovery →
  Construction → Rewrite. (Codex iter-0; Gemini iter-1; Claude
  iter-1.)
- **Plan shape.** `Sort? → Exchange(1, []) → Project? →
  Filter? → Aggregate → Exchange(N, group_keys) → inner`. Both
  `Project` and `Filter` are optional; both execute on the
  aggregate-consumer task (Stage 1) before publishing to the
  terminal exchange. (Claude iter-1 concession on fragmenter
  posture; Codex iter-1 ratification; Gemini iter-1.)
- **HAVING aggregate references not in SELECT — enabled.**
  `Aggregate` computes `union(SELECT, HAVING)`; `Filter`
  consumes the full set; `Project` drops HAVING-only ones from
  final output. (Codex iter-1 reversal; Gemini iter-0+; Claude
  iter-2 re-flip.)
- **ORDER BY hidden aggregate references — rejected.** Sort
  operates on post-Project schema; supporting hidden ORDER BY
  aggregates requires a different plan-shape decision
  (Project-above-Sort or hidden sort columns). An aggregate
  call is ORDER-BY-visible only when it is a bare aggregate
  SELECT output; aggregate requirements used only inside a
  computed SELECT expression stay hidden after Project. (Codex
  iter-1 discipline; Claude iter-2 ratification; Gemini iter-3
  silent → resolved.)
- **Computed projection limited to the existing `Expr`
  surface.** Today's `Expr` has comparison, boolean,
  null-predicate, nested, literal, column, and `TIME_BUCKET`
  forms. Arithmetic (`+`, `-`, `*`, `/`) does not exist yet;
  examples like `SUM(amount) * 1.1` or `SUM(x) + COUNT(*)`
  require a scalar arithmetic IR ADR. (Codex iter-1 code-
  grounded limit; Claude iter-2 ratification; Gemini iter-3
  retained generic phrasing — recorded dissent.)
- **`AggregateOutputCatalog` (renamed from
  `GroupedAggregateScope`).** ADR 021's `AggregateKernelSpec`
  catalog (stable kernel metadata) and ADR 023's
  `AggregateOutputCatalog` (per-query name binding) are
  distinct concepts at adjacent layers; this ADR pins the
  distinction. (Codex + Gemini name; recorded Claude dissent
  on `PostAggregateScope`.)
- **`LogicalPlan::Project` gains explicit output names** —
  `exprs: Vec<Expr>` plus `output_names: Vec<Option<String>>`
  with `output_names.len() == exprs.len()` invariant; `None`
  preserves derived-name behavior; `Some(name)` carries a
  SELECT alias. No new IR variant; no `Expr::Alias`. (Codex
  iter-2/4; Claude iter-3 concession.)
- **`GroupedAggregateSignature` unchanged.** The finite
  column-based enum (`CountAll`, `Count(ColumnRef)`,
  `Sum(ColumnRef)`, `Min(ColumnRef)`, `Max(ColumnRef)`,
  `Avg(ColumnRef)`) is preserved. Widening to
  `(AggregateFunction, Vec<Expr>)` is blocked today by
  `AggregateFunction` not deriving `Eq`/`Hash` (contains
  `ApproxQuantile(f64)`) and `Expr` not deriving `Eq`/`Hash`
  (contains `ScalarValue::Float32`/`Float64`). A future ADR
  bundles the derive refactor with computed-aggregate-argument
  signature widening. (Codex iter-1 code-grounded block; 3/3
  by iter-3.)
- **Non-aggregate alias enablement.** `SELECT amount AS a
  FROM t` accepted post-ADR-023. Once `LogicalPlan::Project`
  carries `output_names`, the implementation cost of
  preserving the non-aggregate alias reject exceeds the cost
  of accepting it. (Codex iter-2; Claude iter-1; 3/3.)
- **No aggregate row-path changes.** `AggState`, kernel
  surfaces, `STABLE_AGGREGATE_KERNELS`, runtime-filter
  descriptors, Wendelon protocols all unchanged. The only
  executor touchpoint is Project output naming in the unbound
  direct-executor path. (3/3.)

Load-bearing antecedents: ADR 010 (terminal Sort wrap;
ordering contract), ADR 011 (aggregate projection accounting;
GROUP BY reachability), ADR 012 (mixed-wildcard projection
accounting), ADR 015 (cycle property — reject-removal + walker
reuse + reject-string-absence guard), ADR 016 (HAVING; Filter
placement; `aggregate_call_not_in_select_message`), ADR 017
(ORDER BY aggregate-call walker; clause-neutral signature
resolver), ADR 018 (scalar-aggregate HAVING/ORDER BY; the
12-of-17 scalar-path callers of `GroupedAggregateScope` that
made the rename load-bearing), ADR 021 (`AggregateKernelSpec`
catalog; output-name authority via `output_name`), ADR 022
(`AggState: Copy` invariant; row-update body invariance).

ADR 023 instances ADR 015's cycle property at the **planner-
projection layer**: every grouped-aggregate or scalar-aggregate
SELECT statement either lowers to a `Project?`-above-`Filter?`-
above-`Aggregate` plan (with `Project` emitted per the four-
condition predicate below) or rejects via one of two named
structural rejects (HAVING alias visibility; ORDER BY hidden
aggregate). The architecture guard machine-checks
reject-string absence for the removed strict-positional reject
and reject-string presence for the two new structural rejects.
Fifth instance of the ADR 015 cycle (after ADR 016, 017, 018,
019/022 — the kernel-domain instances).

## Goal

Decouple `Aggregate` operator output from the final SELECT
projection. Enable aliased, reordered, subsetted, and current-
`Expr`-surface-computed projections over grouped and scalar
aggregate output. Land the first downstream user of the ADR
015 / 016 / 017 / 018 cycle's fifth substrate-and-surface
instance without:

- new IR variants
- new fragmenter detector (one existing detector widens)
- new `AggregateOutputRule` variants (ADR 021 surface
  unchanged)
- new `StableAggregateKernel` rows (ADR 022 surface unchanged)
- new `AggState` variants or `Copy` removal (ADR 022 invariant
  unchanged)
- new `Expr` variants (scalar arithmetic deferred)
- new `AggregateFunction` variants (computed-aggregate-
  argument signature widening deferred)
- new cross-ring engagement (Wendelon AGG protocol unchanged;
  ADR 02X still parked)

Concretely:

- `SELECT region AS r, COUNT(*) AS n FROM sales GROUP BY
  region` binds + executes; output columns named `r` and `n`.
- `SELECT region FROM sales GROUP BY region HAVING SUM(amount)
  > 100` binds + executes; output column is `region` only;
  `SUM(amount)` is computed by Aggregate, consumed by Filter,
  dropped by Project.
- `SELECT COUNT(*) AS n, region AS r FROM sales GROUP BY
  region ORDER BY n DESC` binds + executes; aggregate-first
  projection with alias-targeted ORDER BY.
- `SELECT region, SUM(amount) > 10 AS large FROM sales GROUP
  BY region` binds + executes; computed comparison over
  aggregate output uses the existing `Expr::Gt` surface.
- `SELECT amount AS a FROM sales` binds + executes;
  non-aggregate alias enablement companion.
- `SELECT SUM(x) AS total FROM sales ORDER BY total` and
  `... ORDER BY SUM(x)` both bind to a sort key on the
  projected `total` column because `SUM(x)` is a bare aggregate
  SELECT output.
- All ADR 022's float MIN/MAX semantics and ADR 021's catalog
  contract remain byte-identical.
- Currently-accepted queries produce byte-identical
  `ExecutionPlan` digests pre/post ADR 023 (identity-shape
  `Project` suppression).

## Scale boundary

**Primary:** Toron operator-level — SQL planner + binder +
fragmenter detector. **Local planner-side rewrite + one IR
field addition on `LogicalPlan::Project` + one fragmenter
detector widening.**

In scope:

- `crates/toron/src/ir.rs`: `LogicalPlan::Project` gains
  `output_names: Vec<Option<String>>`.
- `crates/toron/src/sql_planner.rs`: `GroupedAggregateScope` →
  `AggregateOutputCatalog`; new SELECT+HAVING aggregate
  discovery walker; strict-positional reject removed; final
  SELECT projection lowered through the catalog; conditional
  `Project` emission per the four-condition predicate; two new
  reject strings for HAVING alias visibility and ORDER BY
  hidden aggregate.
- `crates/toron/src/binder.rs`: `Project` binding consumes
  `output_names[i].as_deref()` for explicit alias; falls back
  to derived names when `None`; constructs
  `ExecutionPlan::Project.output_schema` accordingly.
- `crates/toron/src/exec.rs`: unbound direct-executor `Project`
  path consumes `output_names` when present; falls back to
  `col_i` derivation when `None`.
- `crates/toron/src/distributed/fragment.rs`:
  `is_terminal_sort_over_grouped_aggregate` peels optional
  `[Project]` between gather `Exchange(1)` and optional
  `[Filter]`; `fragment_terminal_sort_over_grouped_aggregate`
  packages the `Project` into the aggregate-consumer task
  (Stage 1).
- `crates/toron/tests/architecture_test.rs`: existing guards
  source-read updates (rename, detector body); five new guards
  (rename completion, strict-projection reject absence,
  `output_names` field presence, detector-shape peel order,
  diagnostic preservation).
- `crates/toron/tests/group_by_test.rs`: positive + negative
  fixture suite (see §Surface matrix).
- `crates/toron/tests/distributed_process_test.rs`: one
  alias + reorder + HAVING + ORDER BY composition across
  2-worker and 4-worker configurations.

Explicitly unchanged:

- **`LogicalPlan` and `ExecutionPlan` variant sets.** One
  field added to `LogicalPlan::Project`; no new variants.
  ADR 008 `Join` single-variant guard precedent.
- **`AggregateFunction` enum.** Six entries; experimental
  set unchanged.
- **`StableAggregateKernel` enum and `STABLE_AGGREGATE_KERNELS`
  table** (six rows; ADR 021/022 surface).
- **`AggState` enum** (13 variants; ADR 022 surface; `Copy`
  preserved).
- **Aggregate row-update bodies, kernel surfaces, finalize
  helpers, scalar / grouped dispatch.** Byte-identical.
- **Runtime-filter descriptors and any Wendelon protocol
  surface.**
- **Fragmenter recognition sites** other than
  `is_terminal_sort_over_grouped_aggregate`.
- **`ExecutionPlan` digests for queries currently bound and
  accepted.**
- **Brainstorm seams** (none touched, none closed).
- **SoA-readiness invariants** (ADR 021; see §SoA-readiness
  check).

No new IR variant. No new fragmenter recognition site. No new
catalog row. No new `AggregateOutputRule` variant. No new
`AggState` variant. No `AggState: Copy` removal. No row-path
dyn dispatch. No cross-ring engagement.

## Plan shape

```text
Sort?                       # ADR 010 terminal ordering wrap (optional)
  Exchange(1, [])           # ADR 010 single gather (optional, only when Sort)
    Project?                # NEW: post-aggregate shaping (optional)
      Filter?               # ADR 016 HAVING (optional)
        Aggregate           # ADR 011 grouped or scalar
          Exchange(N, group_keys)   # ADR 011 grouped shuffle (grouped only)
            input
```

Scalar aggregate uses the same shape with `group_expr = []`
and no grouped-shuffle exchange below the `Aggregate`.

The aggregate-consumer task (Stage 1) executes
`Aggregate → Filter? → Project?` and publishes final
SELECT-shaped rows to the terminal exchange. The terminal
sort stage (Stage 2; when present) reads post-projection rows
and sorts on the final projected schema.

## Project emission predicate

The post-aggregate `Project` is emitted when ANY of:

1. **(a) Alias present.** Any SELECT item has an alias
   (`output_name == Some(_)`).
2. **(b) Computed expression.** Any SELECT item is a computed
   expression — not a bare column reference or bare aggregate
   call — within the existing `Expr` surface (comparison,
   boolean, null-predicate, nested, literal, column,
   `TIME_BUCKET`).
3. **(c) Reorder or subset.** The SELECT projection list
   differs in order or membership from the `Aggregate`'s
   natural output schema (`[group_keys..., aggregates...]` in
   GROUP BY clause order then discovery order).
4. **(d) HAVING-only aggregate.** HAVING references an
   aggregate signature absent from the SELECT projection. The
   `Aggregate` must compute it for `Filter`, but it must be
   dropped from final output.

If none of (a)–(d) hold, `Project` is omitted and the plan
shape collapses to `Filter? → Aggregate → Exchange(N) → input`
exactly as before ADR 023. The bound `ExecutionPlan` digest is
byte-identical to pre-ADR-023 for these queries.

## Catalog specification

`AggregateOutputCatalog` is a per-query lookup table. Distinct
from ADR 021's `AggregateKernelSpec` catalog (stable kernel
metadata at module-scope); this ADR pins the layering:

- **ADR 021** `AggregateKernelSpec` — stable aggregate kernel
  metadata table, lives in `aggregate_kernel.rs`,
  module-private, owns kernel function name / input domain /
  output rule / nullability / grouped support.
- **ADR 023** `AggregateOutputCatalog` — per-query name
  binding scope, lives in `sql_planner.rs`, constructed during
  `lower_select` for each grouped-or-scalar aggregate query,
  owns group-key bindings + aggregate signature → output
  ordinal + final projected names.

Catalog responsibilities:

- Deterministic aggregate discovery order (SELECT then HAVING
  in document order; duplicates collapse to the first
  occurrence's ordinal).
- Aggregate signature de-duplication keyed by
  `GroupedAggregateSignature` (existing finite enum).
- Group-key resolution against aggregate output ordinals.
- Final SELECT projection expression + output name list.
- HAVING rewrite bindings (natural aggregate-output names).
- ORDER BY rewrite bindings (final projected names for aliases
  and bare aggregate SELECT outputs; no discovery contribution).

Pure-data discipline preserved (ADR 021 §Catalog shape): no
function pointers and no trait objects. The catalog stores
ordinary `HashMap` / `Vec` values and may own existing `Expr`
trees already used by the planner.

## Aggregate discovery rule

Aggregate requirements are collected from:

```text
SELECT projection + HAVING clause
```

They are NOT collected from ORDER BY. The discovery walker
traverses `sqlparser::ast::Expr` over the SELECT projection
list, then the HAVING clause, in document order. Each
stable-aggregate `Function` call lowers to a
`GroupedAggregateSignature`; duplicate signatures collapse to
the first occurrence's ordinal. The walker handles the
existing `lower_expr` expression coverage (Function, BinaryOp,
IsNull, IsNotNull, Nested, Identifier, CompoundIdentifier);
expression shapes not in the existing `Expr` surface reject
through the existing unsupported-expression path.

This rule is deliberate. HAVING-only aggregates are accepted
(Aggregate computes, Filter consumes, Project drops). ORDER
BY hidden aggregates are rejected because Sort runs above
`Project` over the final projected schema; supporting them
requires a different plan-shape decision (Project-above-Sort
or hidden sort columns) deferred to a future ADR.

## Clause resolution semantics

**SELECT projection:**

- May alias group keys and aggregate calls via
  `SelectItem::ExprWithAlias`.
- May reorder or subset group keys when at least one stable
  aggregate exists in SELECT or HAVING (ADR 011's zero-
  aggregate `GROUP BY` reject preserved; that case is
  `DISTINCT` territory).
- May reference aggregate calls in computed expressions only
  within the current `Expr` surface
  (`SELECT region, SUM(x) > 10 AS large` accepted;
  `SELECT region, SUM(x) * 1.1` rejected via the existing
  unsupported-expression path).
- May alias non-aggregate columns (`SELECT amount AS a FROM
  sales` accepted).

**HAVING:**

- Resolves aggregate calls against the natural Aggregate
  output schema (existing ADR 016 surface).
- May reference aggregate calls not in SELECT (Aggregate
  computes; Project drops).
- Does NOT resolve SELECT aliases. `HAVING total > 10` after
  `SELECT SUM(x) AS total` rejects with the diagnostic:
  ```
  HAVING does not resolve SELECT aliases; spell the aggregate explicitly
  ```

**ORDER BY:**

- Resolves names against the final projected output schema
  (alias if any, else natural).
- May reference an aggregate call by spelling it again if the
  same signature appears in SELECT as a bare aggregate output.
  An aggregate call used only inside a computed SELECT
  expression, such as `SUM(x) > 10 AS large`, is not itself a
  final projected output.
- Does NOT contribute aggregate discovery. Hidden aggregate
  calls in ORDER BY reject with the diagnostic:
  ```
  ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list
  ```

## Project IR shape

`LogicalPlan::Project` extends:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    // ... other variants unchanged ...
    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<Expr>,
        output_names: Vec<Option<String>>,   // NEW
    },
    // ... other variants unchanged ...
}
```

Invariant: `exprs.len() == output_names.len()`. Enforced by:

- a small Project-construction helper, or `debug_assert!` at any
  remaining direct construction sites.
- Binder-side length validation with `FrontendError` rejection
  on mismatch (internal-error class).
- Architecture guard
  `logical_project_carries_output_names` source-reads the field
  declaration so the metadata field cannot disappear silently.

`None` preserves derived-name behavior; the binder falls back
to existing `schema_for_exprs` naming. `Some(name)` carries a
SELECT alias.

`ExecutionPlan::Project` unchanged — already carries
`output_schema: SchemaRef`. The binder constructs the schema
from the per-expression name (`output_names[i]` or derived).

No `Expr::Alias`. Aliases are schema metadata, not expression
semantics; keeping them on `Project` avoids forcing filter /
scan / sort / expression rewrite paths to carry a concept
they don't use.

## Fragmenter detector widening

`is_terminal_sort_over_grouped_aggregate`
(`crates/toron/src/distributed/fragment.rs:562-632`) widens to
recognize one optional `[Project]` peel between the terminal
gather `Exchange(1, [])` and the existing optional
`[Filter]` peel:

```text
Sort
  Exchange(1, [])
    Project?       <- NEW optional peel
      Filter?      <- existing optional peel
        Aggregate
          Exchange(N, group_keys)
            inner
```

Detector peel order is exact (Project before Filter); the
architecture guard
`adr023_terminal_sort_detector_peels_project_then_filter`
asserts the order. `fragment_terminal_sort_over_grouped_aggregate`
packages the optional `Project` into the aggregate-consumer
task alongside the optional `Filter`; the consumer task
publishes final SELECT-shaped rows to the terminal exchange.

No new detector, no new fragmenter recognition site, no
routing-decision change. ADR 015's "one narrow detector for
the cycle" property preserved with one detector-body
widening.

## Architecture guards

| Guard | Scope | Change |
|---|---|---|
| ADR 015: `grouped_aggregate_sql_reachable_via_terminal_sort_fragmenter` | detector body source-read | Adjusts; source-read recognizes the new `Project?` peel position |
| ADR 016: HAVING fragmenter Filter placement | detector + reject-string list | Adjusts for Filter-below-Project relative position |
| ADR 017: ORDER BY aggregate-call walker | clause-neutral resolver source-read | Preserved (resolver now produces projected names for bare aggregate SELECT outputs) |
| ADR 018: scalar-aggregate HAVING / ORDER BY | scalar branch source-read | Adjusts for catalog rename only |
| ADR 021: `aggregate_kernel_catalog_guards_are_narrow` | kernel catalog source-read | Preserved (no kernel-spec change) |
| ADR 022: `AggState` variant-count = 13 | exec.rs source-read | Preserved |
| **NEW** `aggregate_output_catalog_replaces_grouped_aggregate_scope` | rename completion | `sql_planner.rs` no longer contains `GroupedAggregateScope` |
| **NEW** `adr023_strict_projection_reject_is_absent` | reject-string absence | `"GROUP BY projection must list all keys in order before aggregates"` absent from `sql_planner.rs` (ADR 015 cycle pattern) |
| **NEW** `logical_project_carries_output_names` | IR field shape | `ir.rs` source-read confirms `output_names: Vec<Option<String>>` on `LogicalPlan::Project` |
| **NEW** `adr023_terminal_sort_detector_peels_project_then_filter` | detector-shape pin | `distributed/fragment.rs` source-read confirms peel order (Project before Filter) |
| **NEW** `adr023_diagnostics_are_preserved` | reject-string presence | Source-read finds the two new diagnostic strings (HAVING alias; ORDER BY hidden aggregate) |

No new architecture-test machinery. Source-read + string-
absence + string-presence pattern matches ADR 015 / 022.

## What Not To Do

- **Don't introduce a new IR variant.** `LogicalPlan::Project`
  and `LogicalPlan::Aggregate` compose; the change is one IR
  field on `Project`. ADR 004 single-variant `Join` guard
  precedent.
- **Don't add `Expr::Alias`.** Aliases are schema metadata; on
  `Project` only.
- **Don't widen `GroupedAggregateSignature` to
  `(AggregateFunction, Vec<Expr>)` in ADR 023.** Blocked by
  derive-Eq/Hash refactor on `AggregateFunction` and `Expr`;
  ADR 023 doesn't need it.
- **Don't add arithmetic to `Expr`.** Scalar arithmetic IR
  (`+`, `-`, `*`, `/`, `CASE`) is a separate ADR. ADR 023's
  computed projection is bounded to the existing `Expr`
  surface.
- **Don't enable HAVING alias visibility.** `HAVING total > 10`
  after `SELECT SUM(x) AS total` rejects. Separate SQL-compat
  ADR.
- **Don't allow ORDER BY hidden aggregate discovery.**
  Asymmetric with HAVING by design; separate plan-shape ADR.
- **Don't promote `DISTINCT` under ADR 023.** `GROUP BY <all
  output columns>` with no aggregates is `DISTINCT` territory;
  loosens ADR 011's zero-aggregate reject; own ADR.
- **Don't engage cross-ring AGG pushdown.** Wendelon AGG
  protocol surface unchanged; ADR 02X stays parked.
- **Don't synthesize an `Exchange` in the fragmenter** (ADR
  004 / 010 / 015 cycle invariant). ADR 023 detector peels one
  more optional operator; emits no operators.
- **Don't add a new detector** for the new shape. ADR 015's
  "one narrow detector" property is load-bearing.
- **Don't drop the strict-positional reject from
  `sql_planner.rs:1101`** (the second instance outside
  `lower_grouped_projection`). All four reject sites
  (`:432, :437, :450, :1101`) must come out together; the
  architecture guard's reject-string absence is the gate.
- **Don't preserve the non-aggregate alias reject.** Once
  `output_names` exists on `Project`, the cost of preserving
  the reject exceeds the cost of accepting it (Codex iter-2
  §D4; 3/3).
- **Don't change `ExecutionPlan::Project` shape.** It already
  carries `output_schema: SchemaRef`; the binder converts
  `output_names` into the schema at bind time.

## Recorded dissent

Two non-blocking dissents, ratified by 2-of-3 majority per
ADR 022 enumerated-error-string precedent:

- **Name:** Gemini iter-0+1+2+3 and Codex iter-2/4 carry
  `AggregateOutputCatalog`. Claude iter-0/2 preferred
  `PostAggregateScope` (concern about "catalog" overload with
  ADR 021's `AggregateKernelSpec`). Codex iter-1 reversed to
  `PostAggregateScope`; Codex iter-2/4 reversed back. The
  council ratifies `AggregateOutputCatalog` with this ADR
  pinning the layering distinction (§Catalog specification).
- **Computed projection scope language:** Gemini iter-1/2
  surfaced arithmetic examples (`SUM(amount) * 1.1`,
  `SUM(amount) + COUNT(*)`) as in-scope. Codex iter-1 + Claude
  iter-2 narrowed to the existing `Expr` surface (no
  arithmetic operators today). Gemini iter-3 retained generic
  "computed projection" phrasing but did not push back on the
  surface narrowing. Council ratifies the narrowed surface;
  arithmetic deferred to a scalar arithmetic IR ADR.

A process observation: the name decision flipped twice across
iterations. Future ADR cycles where two choices stand on
equal technical ground should either hard-pin in iter-0 with
a coin-flip rationale or defer to ADR-draft authoring;
rotating council iterations on non-load-bearing names is a
process inefficiency.

## SoA-readiness check

ADR 021 § SoA / SIMD forward-compatibility established four
invariants that subsequent kernel ADRs (022) honored. ADR 023
preserves all four:

1. **Catalog is pure metadata (no fn pointers in spec).**
   `AggregateOutputCatalog` is a per-query planner accounting
   object stored as ordinary `HashMap` / `Vec` values; no fn
   pointers and no trait objects. It may own existing planner
   `Expr` trees, which already contain boxed subexpressions.
   ADR 021's module-scope `AggregateKernelSpec` catalog stays
   pure-data; the new per-query catalog matches the
   discipline.
2. **`StableAggregateKernel` `#[repr(u8)]` plan-payload
   shape.** Untouched. Six catalog rows preserved.
3. **Centralized dispatch points as SoA rewrite seams.**
   `init_bound_state_for_call` and
   `finalize_scalar_for_call` are unchanged. ADR 023's
   executor touchpoints are limited to the `Project`
   operator path (`binder.rs:104` and `exec.rs:224`); the
   aggregate row-update path is byte-identical to ADR 022.
4. **No row-path dyn dispatch.** The new `Project` operator
   sits downstream of `Aggregate` and operates on bounded
   post-aggregate output (one row per group / scalar). Row
   evaluation goes through the unchanged `ExecutionExpr`
   path; no dyn dispatch added. The new `output_names:
   Vec<Option<String>>` field is bind-time metadata
   consumed only by the binder when constructing
   `ExecutionPlan::Project.output_schema`; at execution time
   the schema is fixed.

**Positive alignment.** Shape A's parallel-vector form
(`exprs: Vec<Expr>` + `output_names: Vec<Option<String>>`)
is mildly SoA-friendlier than the rejected Shape B
(`Vec<ProjectionItem { expr, name }>`). The `Project`
operator is not the hot row-path (aggregate accumulator is);
the alignment is a secondary positive rather than a
load-bearing decision.

No SoA invariant closes; no SoA invariant gains new
forward-work obligations beyond what ADR 021 / 022 already
established.

## Brainstorm-seam touch check

ADR 023 touches none of the load-bearing brainstorm seams:

- **Brainstorm 001 (Cranelift JIT):** the new `Project` above
  `Aggregate` is a row-batch transform on Aggregate's small
  output (one row per group / scalar). `LogicalPlan::Project
  { exprs, output_names }` is the JIT seam shape already; the
  `output_names` payload is metadata, not expression
  evaluation surface.
- **Brainstorm 002 (WeakControlRing):** no coordinator-state /
  registry / lease touch.
- **Brainstorm 003 (Disaggregated Compute):** fragmenter
  routing unchanged; the Project executes on the aggregate-
  consumer task alongside Filter — exactly the "compute-heavy
  consumer" asymmetry pattern Brainstorm 003 contemplates.
  Seam preserved by inaction.
- **Brainstorm 004 (Memory Arenas):** the new `output_names:
  Vec<Option<String>>` field on `LogicalPlan::Project` adds
  bounded heap allocation per Project node; arena-compatible
  at the same shape as `exprs: Vec<Expr>`.

No seam closes; no seam touches beyond ADR 021's existing
surface.

## Definition of done

- `LogicalPlan::Project` carries `output_names:
  Vec<Option<String>>` with the length invariant enforced.
- `sql_planner.rs` references `AggregateOutputCatalog`;
  `GroupedAggregateScope` not present.
- `lower_grouped_projection`'s strict-positional reject string
  is absent from `sql_planner.rs`; the four reject sites are
  removed.
- Aggregate discovery is SELECT-then-HAVING (document order;
  dedup by first occurrence); ORDER BY does not contribute.
- `LogicalPlan::Project` is emitted above `Aggregate` (and
  above `Filter` when present) iff the four-condition
  predicate holds; otherwise omitted (identity-shape
  preservation).
- HAVING rewrite resolves aggregate calls against natural
  Aggregate output names; HAVING alias visibility rejected
  with the diagnostic.
- ORDER BY rewrite resolves names against the final projected
  output schema and bare aggregate SELECT outputs; ORDER BY
  hidden aggregate rejected with the diagnostic.
- Non-aggregate `SelectItem::ExprWithAlias` accepted; aliases
  flow through `output_names`.
- `is_terminal_sort_over_grouped_aggregate` peels optional
  `[Project]` then optional `[Filter]`; aggregate-consumer
  task packages both.
- Five new architecture guards green; six existing guards
  green after adjustment.
- `tests/group_by_test.rs` covers the positive + negative
  surface matrix (below).
- `tests/distributed_process_test.rs` covers one composition
  fixture across 2 and 4 workers.
- `ExecutionPlan` digests for currently-accepted queries
  byte-identical pre/post ADR 023.
- `cargo fmt --check` clean; `cargo test -p toron` green.

## Surface matrix

### Positive (must bind + execute)

```sql
-- Aliases
SELECT amount AS a FROM sales
SELECT COUNT(*) AS n FROM sales
SELECT region AS r, COUNT(*) AS n FROM sales GROUP BY region

-- Reorder / subset
SELECT COUNT(*) AS n, region AS r FROM sales GROUP BY region
SELECT region, COUNT(*) FROM sales GROUP BY region, status
SELECT status, region, COUNT(*) FROM sales GROUP BY region, status

-- Existing-Expr-surface computed projection
SELECT region, SUM(amount) > 10 AS large FROM sales GROUP BY region

-- HAVING hidden aggregate
SELECT region FROM sales GROUP BY region HAVING SUM(amount) > 100

-- ORDER BY visible alias / visible aggregate
SELECT SUM(amount) AS total FROM sales ORDER BY total
SELECT SUM(amount) AS total FROM sales ORDER BY SUM(amount)
SELECT SUM(amount) AS total FROM sales HAVING SUM(amount) > 10
SELECT region AS r, COUNT(*) AS n FROM sales GROUP BY region ORDER BY n DESC
```

### Negative (must reject with stable diagnostics)

```sql
-- DISTINCT territory (deferred)
SELECT region FROM sales GROUP BY region
SELECT DISTINCT region, COUNT(*) FROM sales GROUP BY region

-- Hidden ORDER BY aggregate (new structural reject)
SELECT region FROM sales GROUP BY region ORDER BY SUM(amount)

-- HAVING alias visibility (new structural reject)
SELECT SUM(amount) AS total FROM sales HAVING total > 10

-- Scalar arithmetic (existing unsupported-expression reject)
SELECT SUM(amount) + 1 FROM sales
SELECT region, SUM(amount) * 1.1 FROM sales GROUP BY region
SELECT region, SUM(amount) + COUNT(*) FROM sales GROUP BY region

-- Computed aggregate arguments (existing reject)
SELECT SUM(amount + tax) FROM sales

-- GROUP BY over joins (existing reject)
SELECT region, COUNT(*) FROM sales JOIN events ON sales.id = events.id GROUP BY region
```

## Future work

- **Scalar arithmetic IR.** `+`, `-`, `*`, `/`, `CASE`, etc.
  Precondition for general computed post-aggregate projection.
  Independent ADR; touches `Expr`, `ExecutionExpr`, binder
  numeric promotion, executor evaluation. Multiple downstream
  surfaces unblock when this lands.
- **HAVING alias visibility.** `HAVING <select-alias>` form.
  Separate SQL-compat ADR.
- **ORDER BY hidden aggregate keys.** Project-above-Sort or
  hidden sort columns; separate plan-shape ADR.
- **Computed aggregate arguments.** `SUM(x + y)`. Bundled with
  derive-Eq/Hash refactor on `AggregateFunction` and `Expr`,
  plus `GroupedAggregateSignature` widening to
  `(AggregateFunction, Vec<Expr>)`.
- **DISTINCT.** `GROUP BY <all output columns>` with no
  aggregates; loosens ADR 011's zero-aggregate reject.
- **Utf8 / Decimal MIN / MAX.** Kernel-domain widening; ADR
  022 substrate.
- **Cross-ring AGG consumer wiring (ADR 02X).** Inherits ADR
  022's Float MIN/MAX divergence-check forward work.
- **Kahan-Neumaier stability.** SUM / AVG precision.
- **Kernel graduation.** `LinearRegress` / `ApproxQuantile`
  bound/unbound parity.
