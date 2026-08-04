# ADR 027: `ORDER BY` Computed Expressions over Aggregate Output

## Status

Promoted. Three-way council convergence (Claude / Codex /
Gemini) reached across three iterations. Iter-0 topic split
(Claude led with computed-aggregate-arguments substrate;
Codex + Gemini led with this ratification); Claude conceded
the topic selection at iter-1 with the deferred substrate
carried forward as the ADR 028 candidate. Substantive shape
converged at iter-2 on D1-D10 (D2's `validate_hidden_sort_key_refs`
visible-output validator promoted from Codex iter-1's
correctness clarification into a numbered decision). Iter-3
was a clean cosmetic alignment pass with zero precision
items — the smaller surface (pure planner-shape ratification,
no IR variant, no kernel work) left nothing to refine after
iter-2. ADR 015 cycle's ninth instance (after 016 / 017 /
018 / 019 / 022 / 023 / 024 / 025 / 026).

## Scale boundary

**Primary:** Toron SQL planner plan-shape ratification for
top-level `ORDER BY` on aggregate queries. Generalizes the
existing ADR 025 / 026 hidden-projection mechanism from the
cast-family gate to all non-direct sort keys, with a new
recursive validator (D2) that pins raw-input-column leakage
prevention at design time.

No new IR variant. No new operator. No new kernel. No
fragmenter change. No Eq/Hash refactor. No
`GroupedAggregateSignature` widening. No cross-ring
engagement. No `LogicalPlan` / `ExecutionPlan` variant
addition. No new `ExecutionError` / `TaskFailureKind`
variant. The `ExecutionSortKey` ordinal-only contract from
ADR 010 is preserved.

## Context

ADR 025 introduced the hidden-projection mechanism in
`sql_planner.rs` to handle:

```sql
SELECT region, SUM(amount)
FROM sales
GROUP BY region
ORDER BY CAST(SUM(amount) AS FLOAT32) DESC
```

The plan shape materializes the computed sort key as a
hidden column below the terminal Sort and strips it above:

```text
Project(visible outputs)
  -> Sort(by ordinal keys)
    -> Exchange(1, [])
      -> Project(visible outputs + hidden order keys)
        -> aggregate-query plan
```

ADR 026 D10 extended the gate from `Cast` to `Cast | TryCast`
via a one-helper rename (`expr_contains_cast` →
`expr_contains_cast_family`). The mechanism proper
(`add_hidden_sort_projection`, `visible_output_names`,
`unique_hidden_sort_name`, `__toron_order_key_{idx}` naming,
duplicate-name detection, final-projection stripping) was
shipped under ADR 025 but never ratified at the policy
level.

The remaining asymmetry is user-visible:

```sql
ORDER BY CAST(SUM(amount) AS FLOAT32) DESC    -- works
ORDER BY TRY_CAST(SUM(amount) AS FLOAT32) DESC -- works (ADR 026 D10)
ORDER BY SUM(amount) + 100 DESC                -- rejects via bind_sort_key_ordinal catchall
ORDER BY SUM(amount) * 1.5 DESC                -- rejects
ORDER BY 1 + 1                                 -- rejects
```

All three rejected forms are valid `Expr` shapes that
already exist in the IR. ADR 027 ratifies the
hidden-projection mechanism as the general path for
**non-direct** computed `ORDER BY` keys over the visible
aggregate output schema, generalizing the gate beyond the
cast family while adding a critical safety boundary: raw
input columns must not leak into the post-aggregate
projection.

## Decision

### D1 — Generalize the gate; remove `expr_contains_cast_family`

Replace the cast-family recursive gate with the inline
non-direct-key trigger:

```rust
fn sort_needs_hidden_projection(order_by: &[SortExpr]) -> bool {
    order_by
        .iter()
        .any(|key| !sort_key_is_direct_output_reference(&key.expr))
}

fn sort_key_is_direct_output_reference(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(_))
}
```

`expr_contains_cast_family` is removed outright — **not
renamed**. Recursive expression inspection now lives in the
D2 validator; keeping a renamed trigger helper would
preserve the old variant-specific model.

Direct output columns (`ORDER BY region`) and ordinals
(`ORDER BY 1`) continue to bypass hidden projection. Plan
digests for these queries remain byte-identical
pre/post-ADR-027.

### D2 — Validate hidden sort-key references

Before appending a hidden sort key, every column reference
inside the computed expression must be an unqualified
visible output column.

```rust
fn validate_hidden_sort_key_refs(
    expr: &Expr,
    visible_names: &HashSet<String>,
) -> Result<(), FrontendError> {
    match expr {
        Expr::Column(column) => {
            if column.qualifier.is_some()
                || !visible_names.contains(&column.name.0)
            {
                return Err(unsupported(
                    "ORDER BY computed aggregate expression must reference visible output columns",
                ));
            }
            Ok(())
        }
        Expr::Literal(_) => Ok(()),
        Expr::Cast(inner, _) | Expr::TryCast(inner, _) => {
            validate_hidden_sort_key_refs(inner, visible_names)
        }
        Expr::Add(left, right)
        | Expr::Sub(left, right)
        | Expr::Mul(left, right)
        | Expr::Div(left, right)
        | Expr::Eq(left, right)
        | Expr::Neq(left, right)
        | Expr::Lt(left, right)
        | Expr::Lte(left, right)
        | Expr::Gt(left, right)
        | Expr::Gte(left, right)
        | Expr::And(left, right)
        | Expr::Or(left, right) => {
            validate_hidden_sort_key_refs(left, visible_names)?;
            validate_hidden_sort_key_refs(right, visible_names)
        }
        Expr::Not(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::TimeBucket(_, inner) => {
            validate_hidden_sort_key_refs(inner, visible_names)
        }
    }
}
```

Rules (3/3 ratified):

- `Expr::Column.qualifier.is_some()` rejects (e.g.,
  `sales.region + 1`).
- `Expr::Column.name` must be in the visible-output set;
  otherwise rejects (e.g., `amount + 1` over `SELECT region,
  SUM(amount) FROM sales GROUP BY region` — `amount` is a
  raw input column, not visible output).
- `Expr::Literal` accepts unconditionally (D9).
- Recursion is exhaustive over the current `Expr` surface.
  Future expression variants force a compiler-exhaustiveness
  catch here.

Stable diagnostic for both qualified-column and
non-visible-column rejects:

```text
ORDER BY computed aggregate expression must reference visible output columns
```

This is the safety boundary that prevents raw input columns
from being materialized above an aggregate — a class-of-bug
hazard that would otherwise emerge silently as soon as the
gate widens beyond cast-family.

### D3 — Preserve ADR 023's hidden-aggregate-key reject

Do not discover aggregate calls inside `ORDER BY`. ADR 023's
`AggregateOutputCatalog::resolve_for_order_by` remains the
gate. These continue to reject:

```sql
SELECT region FROM sales GROUP BY region ORDER BY SUM(amount)
SELECT region FROM sales GROUP BY region ORDER BY SUM(amount) + 1
```

Diagnostic unchanged:

```text
ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list
```

### D4 — Preserve ADR 024 D6's computed-aggregate-argument reject

Do not change `validate_aggregate_argument`. Do not widen
`GroupedAggregateSignature`. Do not add Eq/Hash substrate
work. These continue to reject:

```sql
SELECT SUM(amount + tax) FROM sales
SELECT SUM(CAST(amount AS BIGINT)) FROM sales
SELECT SUM(TRY_CAST(amount AS BIGINT)) FROM sales
```

Diagnostic unchanged:

```text
aggregate arguments must be simple column references
```

Computed aggregate arguments move forward as the ADR 028
candidate (substrate-introducing).

### D5 — Keep Sort ordinal-only

`ExecutionSortKey` remains ordinal-only.
`bind_sort_key_ordinal` keeps its catch-all diagnostic for
sort keys that escape the hidden-projection mechanism (e.g.,
non-aggregate computed `ORDER BY` per D10):

```text
ORDER BY keys must reference output columns or positive ordinals
```

ADR 027 materializes computed keys into hidden columns
rather than teaching the Sort operator to evaluate
expressions.

### D6 — Ratify the hidden-projection contract

The ADR 025-shipped mechanism becomes contractual:

- Hidden column names start as `__toron_order_key_{idx}`,
  where `idx` is the 0-based index of the sort key in the
  `ORDER BY` list.
- `unique_hidden_sort_name` appends numeric suffixes on
  collision with visible output names.
- Duplicate visible output names reject **before** any
  hidden keys are appended.
- Final `Project` restores only the visible-output schema
  and strips every hidden key.

Duplicate-name diagnostic unchanged:

```text
ORDER BY computed aggregate expression requires unique output names
```

The broad duplicate-name reject is intentional. It keeps
final projection restoration simple and avoids adding
context-sensitive name-ambiguity rules in ADR 027.

### D7 — Top-level ORDER BY alias visibility; HAVING alias reject preserved

SELECT aliases are visible output names for top-level
`ORDER BY`:

```sql
SELECT region, SUM(amount) AS s
FROM sales
GROUP BY region
ORDER BY s + 1 DESC      -- OK; `s` is a visible output name
ORDER BY s / 10 DESC     -- OK
```

ADR 023's HAVING-alias reject remains unchanged:

```text
HAVING does not resolve SELECT aliases; spell the aggregate explicitly
```

HAVING-alias visibility is a separate name-resolution policy
ADR; ADR 027 does not relax it.

### D8 — Expression surface = the current `Expr` surface

ADR 027 supports whatever already lowers to `Expr`:

- visible output columns and literals;
- ADR 024 arithmetic (`+`, `-`, `*`, `/`);
- ADR 025 `Cast`;
- ADR 026 `TryCast` (and `SafeCast` alias);
- comparisons, boolean operators, null predicates,
  `Not`, `TimeBucket`.

Unary negation, modulo, `CASE`, `COALESCE`, `IFNULL`,
`NULLIF` remain future expression ADRs. Once they lower to
`Expr`, they inherit ADR 027's hidden sort-key path
automatically. The D2 validator's exhaustive `Expr` match
forces a compiler update at the same time, locking the
safety boundary in step.

### D9 — Literal-only computed sort keys are allowed

Literal-only expressions contain no column references, so
the D2 validator accepts them:

```sql
SELECT SUM(amount) FROM sales ORDER BY 1 + 1
SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY 1 + 1, region
```

They materialize as hidden constants. All rows tie on that
key, and existing secondary-key / tie-break behavior decides
the result. The shape is harmless and would require
ad-hoc literal-only special-casing to reject — not worth the
complexity.

### D10 — Non-aggregate computed `ORDER BY` remains out of scope

The existing aggregate-output-catalog gate stays:

```rust
aggregate_output_catalog.is_some() && sort_needs_hidden_projection(&order_by)
```

So this continues to route through the ADR 010 sort path
and remains out of scope:

```sql
SELECT amount FROM sales ORDER BY amount + 1
```

`bind_sort_key_ordinal`'s catch-all rejects it; a dedicated
future ADR would loosen this.

## Reject removal (ADR 015 cycle's ninth instance)

The current `ORDER BY SUM(amount) + 100`-style queries
reject through `bind_sort_key_ordinal`'s catch-all; ADR 027
lifts that path into the hidden-projection mechanism.

Structural rejects preserved (cycle's "narrow detector
before generic guard" property):

- ADR 023 hidden-aggregate-key reject preserved (D3).
- ADR 024 D6 computed-aggregate-argument reject preserved
  (D4).
- `bind_sort_key_ordinal` catch-all preserved for
  non-aggregate query sort keys (D10).
- **NEW** raw-input-column reject for computed aggregate
  `ORDER BY` (D2).
- **NEW** qualified-column reject for computed aggregate
  `ORDER BY` (D2).
- Duplicate visible output names reject preserved (D6).

## Architecture guard adjustments

| Guard | Status |
|---|---|
| `adr025_order_by_cast_uses_hidden_sort_projection` | **Adjusted.** Drop `expr_contains_cast_family` source-read; assert the generalized gate trigger and verify `validate_hidden_sort_key_refs` covers the `Cast` arm. Keep Project insertion, hidden-naming, and final-projection assertions. |
| `adr026_try_cast_order_by_uses_hidden_sort_projection` | **Adjusted.** Same shape; verify `TryCast` arm in the validator. |
| All other ADR 024 / 025 / 026 guards | Preserved. |
| `bind_sort_key_ordinal` catch-all diagnostic guard | Preserved. |
| **NEW** `adr027_hidden_sort_projection_gate_is_generalized` | Source-reads `sort_needs_hidden_projection` for `!sort_key_is_direct_output_reference(&key.expr)`; asserts `fn expr_contains_cast_family` is absent from `sql_planner.rs`. |
| **NEW** `adr027_hidden_sort_key_refs_are_visible_outputs` | Source-reads `validate_hidden_sort_key_refs` for: qualifier rejection, visible-name membership, stable diagnostic, `Literal` acceptance, and exhaustive recursion through every current `Expr` variant (Cast, TryCast, arithmetic, comparisons, boolean, null predicates, `Not`, `TimeBucket`). |
| **NEW** `adr027_hidden_sort_projection_contract_is_pinned` | Source-reads `add_hidden_sort_projection` + companions for: `validate_hidden_sort_key_refs` call site, `__toron_order_key_` naming, duplicate-name diagnostic, `FinalProjection` construction, and preserved `visible_output_names` / `has_duplicate_names` / `unique_hidden_sort_name` helpers. |

Total: 2 adjusted, 3 new architecture guards.

## SoA-readiness check

ADR 021's four invariants preserved:

1. Catalog stays pure metadata (no catalog touch).
2. `StableAggregateKernel` `#[repr(u8)]` unchanged.
3. Centralized dispatch seams unchanged; hidden-key
   columns evaluate via the row-layer expression evaluator
   already shipped by ADR 024 / 025 / 026.
4. No row-path dyn dispatch.

The synthetic-column injection is purely a planner-shape
transformation; runtime execution sees an existing
`Project` + `Sort` operator pair with normal column inputs.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT):** Hidden-key expressions
  evaluate via the existing per-pair kernel dispatch; JIT
  specialization seam preserved.
- **Brainstorm 002 (WeakControlRing):** Untouched.
- **Brainstorm 003 (Disaggregated Compute):** Fragmenter
  unchanged.
- **Brainstorm 004 (Memory Arenas):** Hidden-projection
  rewrite creates two new `LogicalPlan` nodes inside the
  existing `Box`-based IR; arena-compatible.

No seam closes.

## What Not To Do

- Don't rename `expr_contains_cast_family` — it's **removed**
  outright (D1). Recursive inspection moves into the D2
  validator.
- Don't bundle computed aggregate arguments (`SUM(x + y)`,
  `SUM(TRY_CAST(amount AS BIGINT))`); that's the ADR 028
  candidate (substrate-introducing).
- Don't bundle hidden-aggregate `ORDER BY` keys
  (`ORDER BY SUM(amount)` where `SUM(amount)` not in
  `SELECT`); separate plan-shape ADR.
- Don't widen `ExecutionSortKey` to carry expressions (D5).
- Don't change `validate_aggregate_argument` (D4).
- Don't add new expression IR variants (D8).
- Don't allow raw input columns in hidden computed sort
  keys (D2).
- Don't accept qualified column references in hidden sort
  keys (D2).
- Don't loosen ADR 023's HAVING-alias reject (D7).
- Don't add new PBC opcodes.
- Don't change Cast / TryCast PBC ineligibility (ADR 026 D9
  preserved).
- Don't remove ADR 025 / 026 hidden-sort-key guards — they
  adjust, not delete.
- Don't add aggregate-time expression evaluation.
- Don't reject literal-only computed sort keys (D9).
- Don't widen the hidden-projection gate to non-aggregate
  queries (D10).
- Don't catch the validator error and continue; the
  recursive walker propagates the diagnostic via `?`.

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Plan digest regression for direct-ORDER-BY queries | D1 bypasses hidden projection for `Column` / `Literal` keys; plans byte-identical pre/post. Phase 5 spot-checks five queries. |
| R2 | `expr_contains_cast_family` removal breaks ADR 025 / 026 guards | Phase 4 adjusts both guards to assert the generalized gate + validator arms. |
| R3 | Validator misses a recursion site | Exhaustive `Expr` match; compiler exhaustiveness catches future variant additions. New `adr027_hidden_sort_key_refs_are_visible_outputs` guard pins every variant arm. |
| R4 | Raw-input column slips through via synthetic-name collision | D6 `has_duplicate_names` + `unique_hidden_sort_name` collision suffixing + D2 validator at the gate entry. |
| R5 | Hidden `Project`'s column ordering disrupts ADR 023 catalog | Final `Project` restores visible-output schema; catalog binding unchanged. |
| R6 | Cast/TryCast regress while removing the helper | Adjusted ADR 025 / 026 guards + regression fixtures from those ADRs. |
| R7 | Multi-key `ORDER BY` misroutes direct keys | Direct keys flow through unchanged inside the projection sandwich; D1 trigger is `any(...)` so the mechanism activates iff at least one key is non-direct. |
| R8 | Literal-only computed sort keys trigger hidden projection unnecessarily | D9 accepts them as harmless constants; tie-break behavior preserved. |
| R9 | Future expression families silently inherit ADR 027 acceptance | D8 documents the contract; new IR ADRs add their arm to the validator and the compiler enforces it. |
| R10 | Validator HashSet construction cost | O(visible-output-count) at the gate entry; scope-local. |
| R11 | Non-aggregate computed `ORDER BY` accidentally triggers hidden projection | D10 + `aggregate_output_catalog.is_some()` guard. |
| R12 | Helper removal cascades to unknown callers | Phase 0 baseline review verifies `expr_contains_cast_family` has exactly one production caller. |
| R13 | Diagnostic uniformity loses debugging context for qualified-vs-missing-column cases | D2 explicit on a shared diagnostic; the underlying `column.qualifier.is_some()` vs `!visible_names.contains(...)` distinction is observable in source but not surfaced to the user (consistent with cycle convention). |

## Recorded dissent

**None at iter-3.** All iter-1 / iter-2 open questions
resolved at iter-2. Iter-0 topic split (Claude:
computed-aggregate-args substrate vs Codex+Gemini: this
ratification) resolved 2-of-3 in favor of this topic; Claude
conceded at iter-1 with the deferred substrate carried to
the ADR 028 candidate.

## Process observations

### Iter-3 had zero precision items

ADR 027 substantively converged at iter-2 on D1-D10. Iter-3
was a clean cosmetic alignment pass with **zero precision
items** — tighter convergence than ADR 026 (which had three
iter-3 precision items: logical-IR `is_pbc_pushdown_eligible`
arm, binder ancillary helpers, hidden-projection guard
naming).

Why ADR 027 had zero iter-3 precision items:

- **Smaller surface.** Pure planner-shape ratification; no
  IR variant addition; no kernel work; no metadata helper
  updates.
- **Existing mechanism reuse.** ADR 025's helpers
  (`visible_output_names`, `has_duplicate_names`,
  `unique_hidden_sort_name`, `add_hidden_sort_projection`)
  all preserved verbatim. Only the gate widens.
- **Smaller decision matrix.** ADR 026 had eleven numbered
  decisions; ADR 027 has ten with most being preservation
  decisions (D3-D7, D10) rather than substrate decisions.

### Substrate-downstream cadence holds across three consecutive cycles

| ADR | Substrate-introducing? | Iterations | Flip-flops |
|---|---|---|---|
| ADR 023 | Yes (Project field addition) | 4 + consolidation | 1 (name) |
| ADR 024 | Yes (5 IR variants) | 4+ | Multi (Cast IR scope) |
| ADR 025 | No (downstream of 024) | 2 | None |
| ADR 026 | No (downstream of 025) | 3 | None (iter-3 precision items only) |
| **ADR 027** | **No (downstream of 025 / 026)** | **3** | **0 (iter-0 topic split + iter-1 concession resolved cleanly)** |
| ADR 028 (anticipated) | Yes (computed-aggregate-args substrate) | Expect 4+ | Expect at least one |

The substrate-downstream cadence (2-3 iterations with 0-1
flip-flops) holds across three consecutive cycles. ADR 028
will be the next test of the substrate-introducing cadence.

### Topic-selection split pattern matures

ADR 027's iter-0 split (Claude's computed-aggregate-args
lead vs Codex + Gemini's ORDER BY ratification) followed
the ADR 020 pattern cleanly:

| ADR | Split | Resolution | Deferred substrate landed as |
|---|---|---|---|
| ADR 020 | AVG-alone (2) vs AVG+refactor bundle (1) | 2-of-3 wins | ADR 021 (kernel catalog refactor) |
| ADR 027 | ORDER BY ratification (2) vs computed-aggregate-args (1) | 2-of-3 wins | ADR 028 candidate |

Pattern observation: when the roadmap explicitly orders
candidates by class (code-evidence-driven vs
workload-evidence-pressured vs substrate-introducing), the
council's default is to pick the higher-ordered candidate
first. The minority position has a natural next-cycle home.

### Codex correctness-clarification streak (fourth consecutive cycle)

D2's `validate_hidden_sort_key_refs` validator is the fourth
consecutive cycle where a Codex iter-1/2 correctness
clarification surfaces a load-bearing safety boundary at
design time:

| ADR | Codex correctness contribution |
|---|---|
| ADR 024 iter-1 | Aggregate-argument shape guard (column-only) |
| ADR 025 iter-2 | Identity-Cast PBC boundary + float-to-int exclusive upper bound |
| ADR 026 iter-1 / iter-2 (D11) | Inner Cast errors propagate, not swallowed by outer TryCast |
| **ADR 027 iter-1 / iter-2 (D2)** | **Visible-output validator (raw-input-column leakage prevention)** |

This is now a stable design-time feature of the cycle.
Worth recording as a process invariant: the council's
iter-1 correctness-review pass reliably surfaces a
class-of-bug safety boundary before implementation.

## Future work

ADR 028+ candidates (ordered by composes-with-substrate
gravity, with computed-aggregate-args promoted to lead per
the iter-1 topic concession):

- **Computed aggregate arguments** (`SUM(x + y)`,
  `SUM(TRY_CAST(amount AS BIGINT))`). Requires derive-Eq/Hash
  refactor on `AggregateFunction` (blocked by
  `ApproxQuantile(f64)`) and `Expr` (blocked by
  `ScalarValue::Float32`/`Float64`) plus
  `GroupedAggregateSignature` widening to
  `(AggregateFunction, Vec<Expr>)`. **Substrate-introducing**
  — expect 4+ iterations with at least one flip-flop per the
  cadence hypothesis. My iter-0 doc becomes the seed for
  this cycle.
- **Hidden-aggregate `ORDER BY` keys.** `ORDER BY SUM(amount)`
  where `SUM(amount)` is not in `SELECT`. Plan-shape ADR;
  loosens ADR 023's hidden-aggregate-key reject.
- **Non-aggregate computed `ORDER BY`.** D10 deferred.
  Generalizes the mechanism off the
  `aggregate_output_catalog.is_some()` gate.
- **HAVING alias visibility.** Carried from ADR 023 D7.
- **Utf8 ↔ numeric `CAST` + `TRY_CAST`.** Parsing semantics
  ADR.
- **Boolean ↔ numeric `CAST` + `TRY_CAST`.** Truth-table
  semantics.
- **Decimal `CAST` + `TRY_CAST`.** V1.x Decimal numeric-types
  ADR.
- **Date / Timestamp `CAST` + `TRY_CAST`.** Temporal-conversion
  ADR.
- **Wendelon PBC Cast / TryCast opcode extension.** Cross-ring
  ADR.
- **PostgreSQL `expr::type` shorthand.** SQL-compat ADR.
- **Unary negation (`-x`), modulo (`%`), `CASE`,
  `COALESCE` / `IFNULL` / `NULLIF`.** Sibling ADR 024 cycles.
- **`SMALLINT` / `TINYINT` → Int8 / Int16.** V1.x numeric-types
  ADR.
- **Utf8 / Decimal `MIN`/`MAX`, Kahan-Neumaier stability,
  `DISTINCT`.** Carried from ADR 020 / 022.
- **Cross-ring AGG consumer wiring (ADR 02X).** Carried.
- **Kernel graduation** (`LinearRegress` / `ApproxQuantile`).
  Carried.
