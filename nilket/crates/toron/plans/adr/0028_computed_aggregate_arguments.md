# ADR 028: Computed Aggregate Arguments — `SUM(x + y)` / `SUM(TRY_CAST(amount AS BIGINT))`

## Status

Promoted. Three-way council convergence (Claude / Codex /
Gemini) reached across five iterations. Substantive shape
converged at iter-3 on D1-D11; iter-4 reopened D2's
**implementation shape** (not its semantic content) under
user-evidence pressure ("Box everything") and refined the
materialized `AggregateArgKey` tree to a visitor-based
hashing approach on `Vec<Expr>`; iter-5 folded in four
Codex precision items (P1-P4) including a load-bearing
reflexivity correctness fix on `data_type_structural_eq`.
ADR 015 cycle's tenth instance; **first substrate-
introducing ADR since ADR 024** after three substrate-
downstream cycles (025 / 026 / 027).

## Scale boundary

**Primary:** Toron SQL aggregate-call discovery + aggregate
signature representation + planner-private structural-
identity layer. Surgical surface: lift the column-only
argument reject; replace `GroupedAggregateSignature` with
a struct keyed by `StableAggregateKernel + Vec<Expr>` with
custom planner-private `PartialEq + Eq + Hash` impls
implemented via visitor functions over the existing `Expr`
tree.

No new IR variant. No new operator. No new kernel. No
fragmenter detector change. No PBC opcode change. No
cross-ring engagement. No `LogicalPlan` / `ExecutionPlan`
variant addition. No new `ExecutionError` / `TaskFailureKind`
variant. No `AggState` shape change. No `StableAggregateKernel`
row change. No hidden pre-aggregate `Project` node.
**No parallel key types** (`AggregateArgKey` / `ScalarKey`
/ `DataTypeKey` / `DurationKey` rejected at iter-4 in
favor of visitor approach). No public `Eq + Hash` on
`AggregateFunction` / `Expr` / `ScalarValue`.

## Context

ADR 024 introduced scalar arithmetic and `Cast` IR with a
preserved aggregate-argument reject in D6:

```text
aggregate arguments must be simple column references
```

ADR 025 (16-cell strict Cast lattice), ADR 026 (TryCast /
SafeCast twin), and ADR 027 (hidden-projection mechanism
for computed ORDER BY) each explicitly preserved this
reject, naming computed-aggregate-args as the next
substrate cycle. ADR 027's iter-1 topic concession
carried computed-aggregate-args forward as the ADR 028
lead.

The user-visible asymmetry: post-ADR-027, users can write
`amount + tax` in `SELECT`, `WHERE`, and computed
`ORDER BY` keys — but the column-only rule still blocks
the same expression inside `SUM(...)`. ADR 028 closes the
asymmetry for the stable aggregate kernel set.

The substrate work this requires:

- `validate_aggregate_argument` widens from column-only
  to "the lowered `Expr` surface" with nested-aggregate
  rejection at SQL-walk time.
- Aggregate-call structural identity (used by
  `AggregateOutputCatalog::resolve_for_*` for
  projection / HAVING / ORDER BY dedup) needs to handle
  expression-bearing signatures, not just `(kernel,
  ColumnRef)` finite arms.

The load-bearing decision: **where does the structural-
identity machinery live, and at what trait-surface cost?**

## Decision

### D1 — Lift the column-only aggregate-argument reject

The stable aggregate kernel family (`CountAll` plus
one-arg `Count`, `Sum`, `Min`, `Max`, `Avg`) accepts
the currently-lowered `Expr` surface as arguments:

```sql
SELECT SUM(amount + 1) FROM sales
SELECT COUNT(amount + 1) FROM sales
SELECT AVG(price * 1.1) FROM sales
SELECT MIN(amount + id), MAX(amount + id) FROM sales
SELECT SUM(TRY_CAST(amount AS BIGINT)) FROM sales
SELECT SUM(CAST(amount AS BIGINT)) FROM sales
SELECT region, SUM(amount + 1)
  FROM sales GROUP BY region
  HAVING SUM(amount + 1) > 10
  ORDER BY SUM(amount + 1) DESC
```

Accepted expression families mirror the current
`lower_expr` surface:

- columns and literals;
- ADR 024 arithmetic (`+`, `-`, `*`, `/`);
- ADR 025 `Cast`;
- ADR 026 `TryCast` / `SafeCast`;
- comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`);
- boolean operators currently lowered from SQL (`AND`, `OR`);
- null predicates (`IS NULL`, `IS NOT NULL`);
- `TimeBucket`.

`COUNT(*)` retains its zero-argument wildcard form (D6).
Experimental kernels (`LinearRegress`,
`ApproxQuantile`) do not participate in grouped
aggregate signature matching via D2 type segregation;
their existing behavior is otherwise unchanged.

### D2 — Visitor-based planner-private signature key

The aggregate-call structural-identity surface lives
planner-private. `GroupedAggregateSignature` becomes a
struct keyed by `StableAggregateKernel + Vec<Expr>` with
custom `PartialEq + Eq + Hash` impls implemented via
visitor functions over the existing `Expr` tree:

```rust
#[derive(Debug, Clone)]
struct GroupedAggregateSignature {
    kernel: StableAggregateKernel,
    args: Vec<Expr>,
}

impl PartialEq for GroupedAggregateSignature {
    fn eq(&self, other: &Self) -> bool {
        self.kernel == other.kernel
            && self.args.len() == other.args.len()
            && self.args.iter().zip(&other.args)
                .all(|(a, b)| expr_structural_eq(a, b))
    }
}

impl Eq for GroupedAggregateSignature {}

impl Hash for GroupedAggregateSignature {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kernel.hash(state);
        self.args.len().hash(state);  // length-hash prevents prefix collisions
        for arg in &self.args {
            hash_expr_structural(arg, state);
        }
    }
}
```

Six planner-private visitor helpers in `sql_planner.rs`:

- `hash_expr_structural(&Expr, &mut impl Hasher)` —
  exhaustive over current `Expr`, documented variant-
  discriminator order.
- `expr_structural_eq(&Expr, &Expr) -> bool` —
  exhaustive, pair-symmetric.
- `hash_scalar_structural(&ScalarValue, &mut impl Hasher)`
  — bit-pattern via `Option::map(f32::to_bits)` /
  `Option::map(f64::to_bits)` for `Float32` / `Float64`
  arms.
- `scalar_structural_eq(&ScalarValue, &ScalarValue) -> bool`
  — bit-pattern comparison for float arms.
- `hash_data_type_structural(&DataType, &mut impl Hasher)`
  — explicit primitive arms (`Int32` / `Int64` /
  `Float32` / `Float64`) + `format!("{other:?}")`
  defensive fallback (P3 consistency with reflexive Eq).
- `data_type_structural_eq(&DataType, &DataType) -> bool`
  — `left == right` reflexive delegation to Arrow's
  derived `PartialEq` (P3 correctness — `matches!` with
  arm restrictions would violate `Eq` reflexivity for
  hand-built `Expr::Cast(_, DataType::Utf8)` values).

**Invariants:**

- Public `AggregateFunction` / `Expr` / `ScalarValue`
  remain non-`Eq` / non-`Hash`. Visitors are standalone
  planner-private helpers, NOT trait impls on IR types.
- `StableAggregateKernel` (already `Eq + Hash`) keys
  function side, sidestepping the
  `AggregateFunction::ApproxQuantile(f64)` float-derive
  blocker and type-segregating experimental kernels.
- Zero parallel-IR allocation: `Vec<Expr>` is the
  existing logical-plan clone; no `Box<AggregateArgKey>`
  per internal node.
- Structural equality is post-lowering (D4): operand
  order preserved.
- Float bit-pattern handling encapsulated planner-
  private (NaN canonicalization invisible to public IR).
- Compiler-exhaustiveness enforces visitor-arm coverage;
  future `Expr` variants force deliberate updates at
  build time.

### D3 — Aggregate-domain type checks stay in the binder

The SQL planner accepts computed `Expr` argument shapes;
the binder validates per-kernel type-domain at bind time:

```sql
COUNT(amount > 0)             -- OK: COUNT accepts any non-null expression
SUM(amount > 0)               -- TypeMismatch: Boolean not SumNumeric
AVG(TRY_CAST(x AS INT))       -- OK if x in scope and TRY_CAST binds
MIN(price * 1.1)              -- OK; output type follows bound expression type
SUM(CAST(region AS BIGINT))   -- Cast fails at bind (Utf8 not in 16-cell lattice)
```

ADR 019 / 020 / 021 domain ownership preserved:
planner discovers shape; binder validates type;
kernels receive already-bound `ArrayRef`s.

### D4 — Signature equality is post-lowering structural

No algebraic normalization. These are distinct
signatures:

```sql
SUM(amount + tax)              -- distinct
SUM(tax + amount)              -- distinct
SUM(CAST(amount AS INT))       -- distinct from SUM(amount)
SUM(amount + 0)                -- distinct from SUM(amount)
SUM(amount)                    -- distinct
```

Mirrors ADR 025 D8's "no identity-Cast elision"
discipline. The aggregate catalog matches exactly what
the planner lowered, not what a future algebraic
optimizer might prove equivalent.

### D5 — Nested aggregate calls reject at SQL-walk time

Aggregate calls do NOT appear in Toron's logical `Expr`
enum; aggregate extraction happens during the SQL
aggregate-discovery walk. The natural reject point is
**before** lowering to `Expr`, via a SQL-AST walker that
matches `sqlparser::ast::Expr::Function` against
the existing aggregate-function name resolver:

```sql
SELECT SUM(COUNT(*)) FROM sales              -- reject
SELECT SUM(amount + SUM(tax)) FROM sales     -- reject
SELECT COUNT(AVG(price)) FROM sales          -- reject
SELECT SUM(CAST(SUM(x) AS BIGINT)) FROM sales -- reject (nested under Cast)
```

Stable diagnostic:

```text
aggregate arguments must not contain aggregate calls
```

Walker recurses through `BinaryOp`, `UnaryOp` (including
SQL `NOT`), `Cast`, `IsNull` / `IsNotNull`, `Nested`,
and `Function` arg lists.

### D6 — `COUNT(*)` is zero-arg signature; logical placeholder unchanged

`grouped_aggregate_signature`'s `AggregateArity::Zero`
branch produces:

```rust
GroupedAggregateSignature {
    kernel: StableAggregateKernel::CountAll,
    args: Vec::new(),
}
```

**regardless of input args.** This decouples
`CountAll`'s structural identity from
`lower_count_aggregate`'s internal synthetic
`Expr::column("count")` placeholder, which is
**preserved verbatim** in the `LogicalPlan` shape so
that column-only-arg plan digests remain byte-identical
(D10).

### D7 — Output names stay kernel names; no HAVING alias visibility

Aggregate output names remain the stable kernel-name
set: `sum`, `count`, `min`, `max`, `avg`.

Do **not** introduce formatted / hashed computed-
expression output names. Duplicate natural-name
behavior remains the existing ambiguity mechanism:

```sql
SELECT SUM(amount + 1), SUM(amount + 2)
FROM sales
HAVING SUM(amount + 1) > 0
```

Ambiguous unless an existing projection / alias path
disambiguates. ADR 028 does NOT introduce HAVING alias
visibility (ADR 023 D7 reject preserved) or
expression-derived output names.

### D8 — Row-layer executor seam reused

The existing row-layer executor already evaluates
`Vec<Expr>` / `Vec<ExecutionExpr>` aggregate arguments
per-batch before kernel accumulation:

```rust
// unbound grouped path
self.evaluate_expr(&args[0], batch)?

// bound grouped path
self.evaluate_bound_expr(&call.args[0], batch)?
```

No new operator. No hidden pre-aggregate
`LogicalPlan::Project` node. No fragmenter detector
change. No `AggregateExec` rewiring. No kernel-side
change.

The implementation surface is SQL-side validation +
signature substrate + architecture guards + fixtures.

### D9 — Null / error / PBC semantics inherit through composition

Computed aggregate arguments inherit all upstream
semantics:

- **ADR 022 sticky-NaN:** `MIN(price * 1.1)` over
  Float64 with NaN → `*` kernel's NaN propagation
  reaches MIN, which applies float-aware sticky-NaN.
- **ADR 024 arithmetic loud-failure:** `SUM(amount + tax)`
  where the sum overflows → arithmetic-overflow Kernel
  error before SUM sees any rows.
- **ADR 024 D8 integer div/0 null:** `SUM(a / b)` where
  `b = 0` for some rows → those rows become NULL; SUM
  inherits null-handling.
- **ADR 025 strict Cast:** `SUM(CAST(amount AS INT))`
  where the cast fails → `cast failed: ...` Kernel
  error.
- **ADR 026 TryCast fail-to-null:** `SUM(TRY_CAST(amount
  AS BIGINT))` where some rows fail → those rows
  become NULL; SUM null-handling applies.
- **ADR 026 D11 inner-Cast-error propagation:** `SUM(TRY_CAST(CAST(amount AS INT) AS BIGINT))`
  — inner CAST overflow propagates as Kernel error.

**PBC inheritance:** All accepted `Expr` argument
families (arithmetic, Cast, TryCast) are PBC-ineligible
per ADR 024 D9 / ADR 025 / ADR 026 D9. Aggregates with
computed arguments inherit the ineligibility — they
stay Toron-resident, never Wendelon-pushed. PBC
diagnostic unchanged.

No new error pathway. No aggregate-specific error
wrapping.

### D10 — Column-only-arg plan-digest backward compat is automatic

`LogicalPlan::Aggregate.aggr_expr` already carries
`Vec<(AggregateFunction, Vec<Expr>)>`. Bare `SUM(amount)`
lowers to `(AggregateFunction::Sum, vec![Expr::Column(...)])`
pre- and post-ADR-028 — byte-identical. Plan digests
for column-only-argument queries remain byte-identical
without explicit preservation work.

The substrate change (D2) is to a planner-private
signature-key shape used inside `AggregateOutputCatalog`
for catalog lookup; it does NOT participate in plan
digests because the signature is not part of the
serialized `LogicalPlan` surface.

### D11 — Qualified-reference policy unchanged

ADR 028 does not change SQL name-resolution policy.
Existing qualified-reference rejects in aggregate
projection and HAVING continue to fire where they fire
today. ORDER BY aggregate-call matching continues
through `resolve_for_order_by`; a qualified argument
that does not structurally match the visible SELECT
aggregate remains `NotInSelect` / `Ambiguous`.

## Reject removal (ADR 015 cycle's tenth instance)

`validate_aggregate_argument`'s column-only catch-all
reject is lifted for the stable kernel set. The old
diagnostic:

```text
aggregate arguments must be simple column references
```

goes away for computed `Expr` arguments. Structural
rejects preserved:

- Nested aggregate calls reject at SQL-walk time (D5).
- Experimental-kernel computed args reject via D2 type
  segregation (`stable_kernel_for_function` returns
  `None`).
- Out-of-domain type mismatches reject at the binder
  (D3).
- Out-of-lattice Cast / TryCast types reject at lowering
  (preserved from ADR 025 / 026).
- Qualified-reference rejects preserved (D11).

New reject added:

- Nested aggregate diagnostic (D5):
  ```text
  aggregate arguments must not contain aggregate calls
  ```

## Architecture guard adjustments

| Guard | Status |
|---|---|
| ADR 024 D6 column-only-rule guard (whichever guard pins the column-only diagnostic) | **Adjusted.** Drop column-only sub-assertion; keep nested-aggregate + scope-binding sub-assertions; positive absence assertion for the column-only diagnostic string in `sql_planner.rs`. |
| ADR 020 AVG signature-shape guard | **Adjusted.** Stop looking for removed `GroupedAggregateSignature::Avg(ColumnRef)` enum variant; pin `StableAggregateKernel::Avg` + natural output name `"avg"`. |
| All other ADR 020 / 022 / 023 / 024 / 025 / 026 / 027 guards | Preserved. |
| **NEW** `adr028_aggregate_arguments_accept_computed_expressions` | Source-reads `validate_aggregate_argument`: column-only diagnostic absent; nested-aggregate diagnostic present; walker calls `reject_nested_aggregate`. |
| **NEW** `adr028_aggregate_signature_uses_planner_private_visitors` | Source-reads `struct GroupedAggregateSignature { kernel: StableAggregateKernel, args: Vec<Expr> }`; custom `impl PartialEq` + `impl Hash`; 6 visitor helpers present; `args.len().hash(state)` length-hash present; **absence** of `AggregateArgKey` / `ScalarKey` / `DataTypeKey` / `DurationKey` types. |
| **NEW** `adr028_aggregate_signature_visitors_are_exhaustive` | Source-reads `hash_expr_structural` AND `expr_structural_eq` (P4 dual coverage) for every current `Expr` variant arm. |
| **NEW** `adr028_aggregate_signature_visitors_preserve_float_bits` | Source-reads `hash_scalar_structural` + `scalar_structural_eq`: `f32::to_bits` / `f64::to_bits` present; per-`ScalarValue`-variant arm coverage. |
| **NEW** `adr028_data_type_structural_eq_is_reflexive` | **P3 correctness boundary.** Source-reads `fn data_type_structural_eq` for `left == right` body; asserts **absence** of `matches!` arm-restriction patterns. Source-reads `hash_data_type_structural` for primitive arms + defensive `format!("{other:?}")` fallback. |
| **NEW** `adr028_aggregate_signature_count_all_is_zero_arg` | Source-reads `grouped_aggregate_signature` for `AggregateArity::Zero => args: Vec::new()`; source-reads `lower_count_aggregate` for synthetic `Expr::column("count")` placeholder preservation. |
| **NEW** `adr028_nested_aggregate_rejects_at_sql_walk` | Source-reads `reject_nested_aggregate` walker arms (BinaryOp, UnaryOp, Cast, IsNull, IsNotNull, Nested, Function); stable diagnostic present. |
| **NEW** `adr028_experimental_kernels_excluded_from_grouped_signatures` | Source-reads `aggregate_kernel.rs::stable_kernel_for_function`: `ApproxQuantile(_) => None` + `LinearRegress => None`; `grouped_aggregate_signature` calls `aggregate_kernel::stable_kernel_for_function(function)?`. |
| **NEW** `adr028_aggregate_output_names_stay_kernel_names` | Source-reads `aggregate_output_name` body: kernel-name strings present; absence of `format!`. |
| **NEW** `adr028_binder_executor_aggregate_args_preserved` | Source-reads `bind_expr`, `evaluate_expr`, `evaluate_bound_expr` aggregate-argument callsites. |
| **NEW** `adr028_public_ir_remains_non_eq_non_hash` | Source-reads `ir.rs` for absence of `Eq` / `Hash` derives and explicit `impl Hash for ...` / `impl Eq for ...` blocks on `AggregateFunction`, `Expr`, and `ScalarValue`. |

Total: **2 adjusted**, **11 new** architecture guards.

## SoA-readiness check

ADR 021's four invariants preserved:

1. Catalog stays pure metadata — no
   `StableAggregateKernel` row change.
2. `#[repr(u8)]` on `StableAggregateKernel` unchanged.
3. Centralized dispatch seams unchanged — computed
   args evaluate via existing row-layer evaluator;
   per-kernel dispatch operates on already-bound
   `ArrayRef`s.
4. No row-path dyn dispatch.

The new structural-identity surface (D2) is planner-
side only and adds no row-path obligations. JIT
specialization (Brainstorm 001) inherits the existing
per-kernel seam.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT).** Visitor approach
  preserves minimal-trait IR surface JIT prefers; no
  trait commitments added.
- **Brainstorm 002 (WeakControlRing).** Untouched.
- **Brainstorm 003 (Disaggregated Compute).** Untouched
  unless PBC partial-pushdown of computed-arg
  aggregates lands (future ADR; out of scope).
- **Brainstorm 004 (Memory Arenas).** **Strictly
  Brainstorm-004-friendly.** When `Expr` eventually
  migrates to arena/index form, the visitors trivially
  follow (match arms change once per visitor); no
  parallel arena migration of a materialized
  `AggregateArgKey` tree needed. The iter-4 refinement
  avoided *adding* a Box-everywhere substrate parallel
  to `Expr`. **The case for a Brainstorm 004 arena ADR
  is now stronger** — multiple cycles (ADR 023 / 024 /
  025 / 026 / 028 iter-3 → iter-4) have surfaced the
  Box-everywhere pattern.

No seam closes. **Brainstorm 004 strengthens as a
candidate post-ADR-028.**

## What Not To Do

- Don't implement public `Eq + Hash` on
  `AggregateFunction`, `Expr`, or `ScalarValue` (D2).
- **Don't introduce parallel key types**
  (`AggregateArgKey` / `ScalarKey` / `DataTypeKey` /
  `DurationKey`) — iter-4 refinement removed these.
- **Don't make `data_type_structural_eq` non-reflexive**
  (P3 correctness). Use `left == right`; do NOT use
  `matches!` with arm restrictions.
- Don't put visitor functions in `ir.rs` — they live
  in `sql_planner.rs` as planner-private helpers.
- Don't add a new aggregate kernel.
- Don't add a new `Expr` IR variant.
- Don't widen `AggState` shape or `StableAggregateKernel`
  row layout.
- Don't add `FILTER (WHERE ...)` clause support.
- Don't bundle `DISTINCT` aggregate arguments.
- Don't bundle nested aggregate calls.
- Don't make experimental kernels grouped-signature
  eligible (D2 type segregation enforces this).
- Don't change ADR 022's sticky-NaN policy under
  computed-argument paths.
- Don't change ADR 024 / 025 / 026 PBC ineligibility.
- Don't break plan-digest backward compat for column-
  only-argument queries.
- Don't add a hidden pre-aggregate
  `LogicalPlan::Project` node (D8).
- Don't churn the executor's row-layer evaluator seam.
- Don't introduce HAVING alias visibility (D7).
- Don't introduce expression-derived aggregate output
  names (D7).
- **Don't remove `lower_count_aggregate`'s synthetic
  `Expr::column("count")` placeholder** (D6).
- Don't introduce algebraic / Cast normalization of
  signatures (D4).
- Don't change SQL qualified-reference policy (D11).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Plan-digest regression for column-only-arg queries | D10 automatic via `LogicalPlan` shape preservation; D6 preserves `COUNT(*)` placeholder; spot-check fixture. |
| R2 | Visitor drift if `Expr` adds variants | Exhaustive `match` in both visitors; `adr028_aggregate_signature_visitors_are_exhaustive` guard pins dual coverage. |
| R3 | Nested-aggregate detector misses a SQL-walk arm | `adr028_nested_aggregate_rejects_at_sql_walk` guard pins arms. |
| R4 | Computed argument over aggregate accidentally accepted | Phase 6 fixture (`SUM(CAST(SUM(x) AS BIGINT))`). |
| R5 | Aggregate-arg expression error propagation diverges | D8 + Phase 4 audit — single path through row-layer evaluator. |
| R6 | Experimental-kernel surprises | D2 type segregation; guard. |
| R7 | Wide Eq/Hash surface complicates Brainstorm 001 / 004 | D2 visitor approach; `adr028_public_ir_remains_non_eq_non_hash` guard. |
| R8 | ADR 022 sticky-NaN composition gap in computed-arg float aggregates | D9 inheritance; fixtures cover. |
| R9 | PBC residual contract breaks | D9; no PBC change. |
| R10 | Architecture-guard adjustment cascade | 2 adjusted guards. |
| R11 | `CountAll` placeholder hashing leak | D6; guard. |
| R12 | Output-name policy drift | D7; guard. |
| R13 | `lower_count_aggregate` placeholder accidentally removed | Guard asserts placeholder preservation. |
| R14 | `hash_scalar_structural` / `scalar_structural_eq` mirror drift if `ScalarValue` adds variants | Exhaustive `match`; compiler enforces. |
| R15 | `data_type_structural_eq` becomes non-reflexive | **`adr028_data_type_structural_eq_is_reflexive` guard pins `left == right` and absence of `matches!` arm-restriction patterns.** |
| R16 | `hash_data_type_structural` fast-path drifts vs Eq | Defensive `format!("{other:?}")` fallback keeps `Hash` consistent with reflexive `Eq`. |
| R17 | Resolver `match` on old `GroupedAggregateSignature` enum variants | Phase 3 audit; resolvers compare by `&` reference. |
| R18 | Walker recursion misses `SqlExpr::Function` non-aggregate args | `reject_nested_aggregate` explicit `Function` arm recurses. |
| R19 | `validate_aggregate_argument` callers outside lower_* helpers | Phase 0 baseline audit. |
| R20 | Implementer accidentally widens public IR derives | `adr028_public_ir_remains_non_eq_non_hash` guard. |
| R21 | Visitor walk cost per probe | Catalog is `Vec<AggregateOutputEntry>` linear scan today; cost is structural equality over a tiny catalog. `Hash` impl preserved for forward-compat if catalog moves to a map. |
| R22 | Hash/Eq discriminator-order drift | Guard pins both visitors per `Expr` variant arm; per-arm dual coverage. |
| R23 | `args.len()` not hashed → prefix collision | `args.len().hash(state)` in signature `Hash` impl; guard pins. |

## Recorded dissent

**None at iter-5.** All iter-0 / iter-1 / iter-2 / iter-3
/ iter-4 open questions resolved. Two flip-flops
recorded:

- **iter-3 D2 P1 vs P2 flip-flop:** Gemini iter-0 /
  iter-1 / iter-2 advocated public manual `Eq + Hash`
  on `AggregateFunction` / `Expr` / `ScalarValue`;
  retracted at iter-3 in favor of Claude / Codex's
  planner-private signature key. Third consecutive
  cycle of Gemini iter-1/2 "wider trait surface"
  retraction (ADR 025 Dissent B, ADR 026
  `null_on_failure`, ADR 028 D2 P2).
- **iter-4 D2 realization flip-flop (user-evidence-
  pressured):** iter-3 ratified D2 P1 with a
  materialized `AggregateArgKey` tree; iter-4 refined
  to a visitor-based hashing approach on `Vec<Expr>`
  after user evidence flagged the "Box everything"
  concern at merge staging. Codex + Gemini iter-4
  endorsed the refinement; Codex iter-4 added 4
  precision items (P1-P4) including a load-bearing
  reflexivity correctness fix on `data_type_structural_eq`
  (P3) folded into iter-5.

## Process observations

### Substrate-introducing cadence with one user-evidence-pressured refinement

ADR 028 took 5 iterations with 2 flip-flops. The
substrate-introducing cadence hypothesis predicted 4+
iters with at least one flip-flop. The user-evidence-
pressured iter-4 refinement extended the cycle by one
iteration but produced a strictly-better realization of
D2 P1.

| Iter | Convergence event |
|---|---|
| iter-0 | 3/3 topic convergence; D2 P1 vs P2 split surfaces. |
| iter-1 | 12+ points of substantive iter-1 convergence; Gemini iter-1 impl self-correction on D5 placement. |
| iter-2 | Dual ADR + impl drafts; D2 split persists. |
| iter-3 | Gemini retracts P2 → P1; full 3/3 on D1-D11; two Codex iter-3 precision items. |
| iter-4 | User-evidence-pressured D2 reopen at staging; materialized tree → visitor approach; Codex + Gemini endorse. |
| iter-5 | Codex P1-P4 precision items folded in; P3 is load-bearing correctness fix. Ready for merge. |

### Codex correctness-clarification streak — fifth consecutive cycle

| ADR | Codex correctness contribution | Surface iter |
|---|---|---|
| ADR 024 | Aggregate-argument shape guard | iter-1 |
| ADR 025 | Identity-Cast PBC + float-to-int bound | iter-2 |
| ADR 026 | Inner-Cast-error propagation (D11) | iter-1 |
| ADR 027 | Visible-output validator (D2) | iter-1 |
| **ADR 028** | **Planner-private signature key (iter-0) + `COUNT(*)` placeholder preservation (iter-3) + `data_type_structural_eq` reflexivity (iter-5 P3)** | **iter-0 + iter-3 + iter-5** |

ADR 028's pattern is the most distributed surfacing of
Codex correctness items across the cycle (iter-0 +
iter-3 + iter-5). Substrate-introducing scope
correlates with multiple correctness-pass surfaces.

### Gemini "wider trait surface" retraction stable pattern (third consecutive cycle)

| ADR | Gemini iter-0/1/2 position | Iter-2/3 retraction |
|---|---|---|
| ADR 025 | Arrow-kernel-delegation (broader scope) | Manual per-pair kernels |
| ADR 026 | `null_on_failure: bool` flag on strict kernels | Separate `try_cast_*` kernels |
| **ADR 028** | **Public manual `Eq + Hash` on IR** | **Planner-private signature key** |

Now a 3-of-3 confirmed pattern: Gemini's broader-trait-
surface iter-1/2 positions reliably retract at iter-2/3
as implementation-precision implications surface. Worth
recording as a stable process feature.

### User-evidence-pressured iter-4 is a new process variant

ADR 028 iter-4 is the **first user-evidence-pressured
substrate refinement at staging time** in the cycle's
history. Prior iter-4+ cycles (ADR 023, ADR 024) were
internally driven. The "Box everything" observation at
iter-3 merge staging triggered a scoped re-open of D2's
implementation shape (D1, D3-D11 stayed locked).

**Recorded as a process invariant:** user-evidence
concerns at staging time can re-open a single load-
bearing decision without forcing the entire ADR back
to iter-0. The cycle's discipline scoped the re-open
to D2's realization; total cycle stretch was ~1.5
iterations.

### Topic-concession-compresses-next-cycle pattern at 2-of-2 confirmed

ADR 020 → 021 (computed kernel-catalog substrate
deferred at 020 → led 021): 2 iters with topic
agreement at iter-0.

ADR 027 → 028 (computed-aggregate-args deferred at 027
→ led 028): unanimous topic agreement at iter-0,
strong iter-1 convergence (12+ points), 5-iter total
with two flip-flops (one internal, one user-evidence).

The pattern compresses iter-0 topic-debate but doesn't
shortcut substrate-introducing convergence work or
user-evidence-pressured refinements.

### "Box everything" pattern is now a recurring cycle property

Toron's expression IR uses Box-everywhere as the
default substrate. ADR 023 added `LogicalPlan::Project`
with `Box<LogicalPlan>`. ADR 024 added five `Expr`
variants with `Box<Expr>`. ADR 025 / 026 added Cast /
TryCast with the same shape. ADR 027's hidden-projection
mechanism added more. ADR 028 iter-3 proposed extending
the pattern; ADR 028 iter-4 refined to avoid the
extension.

**Brainstorm 004 (Memory Arenas) has a stronger seam
case after ADR 028.** Multiple ADRs have surfaced the
Box-everywhere pattern; the cumulative pressure
suggests an arena-introducing ADR cycle is well-
prepared for when workload evidence triggers it.
Recorded in roadmap as ADR 029+ candidate.

## Future work

ADR 029+ candidates (composes-with-substrate gravity):

- **Brainstorm 004 (Memory Arenas) ADR.** Multiple
  cycles now bumping against Box-everywhere; ADR 028
  iter-4's user-evidence pressure may be a strong
  trigger signal.
- **PBC partial-pushdown of computed-arg aggregates**
  (cross-ring; workload-evidence-pressured). Now that
  computed-arg aggregates exist, the cross-ring ADR
  can consider arithmetic / Cast / TryCast PBC opcodes.
- **Hidden-aggregate `ORDER BY` keys** (ADR 023 D3 /
  ADR 027 D3 deferred). Composes with ADR 028
  signature substrate.
- **Non-aggregate computed `ORDER BY`** (ADR 027 D10
  deferred).
- **`DISTINCT` aggregate arguments.**
- **`FILTER (WHERE ...)` clause on aggregate calls.**
- **Window aggregates (`OVER`).**
- **Aggregate-local `ORDER BY` inside the call.**
- **HAVING alias visibility** (ADR 023 D7 deferred).
- **Utf8 / Boolean / Decimal / Date CAST + TRY_CAST**
  (carried from ADR 026; computed-aggregate-argument
  acceptance inherits via visitor exhaustive-match
  once they lower).
- **Unary negation (`-x`), modulo (`%`), `CASE`,
  `COALESCE` / `IFNULL` / `NULLIF`** — ADR 024 sibling
  cycles. Inherit ADR 028 acceptance via visitor
  exhaustive-match (compile-time enforced).
- **Algebraic / Cast normalization of signatures** (D4
  deferred).
- **Public `Eq + Hash` on `AggregateFunction` / `Expr`**
  — if a future cost-based optimizer or expression-
  keyed cache genuinely needs it.
- **Kernel graduations** (`LinearRegress`,
  `ApproxQuantile`).
- **Wendelon AGG consumer wiring (ADR 02X).**
- **Nested aggregate calls** (deeply deferred).

## Definition of Done

- [ ] `validate_aggregate_argument` accepts lowered
      `Expr` surface; column-only diagnostic
      `"aggregate arguments must be simple column
      references"` removed from `sql_planner.rs`.
- [ ] `reject_nested_aggregate` SQL-walk walker added;
      stable diagnostic `"aggregate arguments must not
      contain aggregate calls"` present; exhaustive
      over SQL expression families.
- [ ] Existing aggregate-function name resolver reused
      for nested-aggregate detection.
- [ ] `GroupedAggregateSignature` struct `{ kernel:
      StableAggregateKernel, args: Vec<Expr> }` with
      planner-private `impl PartialEq` + `impl Eq` +
      `impl Hash`.
- [ ] **No `AggregateArgKey` / `ScalarKey` /
      `DataTypeKey` / `DurationKey` types in
      `sql_planner.rs`.**
- [ ] 6 visitor helpers present:
      `hash_expr_structural`, `expr_structural_eq`,
      `hash_scalar_structural`, `scalar_structural_eq`,
      `hash_data_type_structural`,
      `data_type_structural_eq`.
- [ ] `hash_expr_structural` + `expr_structural_eq`
      exhaustive over current `Expr` (20 variants);
      shared discriminator-order.
- [ ] `hash_scalar_structural` + `scalar_structural_eq`
      exhaustive over `ScalarValue`;
      `f32::to_bits()` / `f64::to_bits()` for float
      arms.
- [ ] `data_type_structural_eq` body is `left == right`
      (P3 reflexive); no `matches!` arm-restriction.
- [ ] `hash_data_type_structural` primitive arms +
      `format!("{other:?}")` fallback.
- [ ] `args.len().hash(state)` in signature `Hash`
      impl (P4 length-hash).
- [ ] `stable_kernel_for_function` returns `Some(...)`
      for stable kernels, `None` for experimental.
- [ ] `grouped_aggregate_signature`'s
      `AggregateArity::Zero` branch produces `args:
      Vec::new()` regardless of input.
- [ ] `lower_count_aggregate`'s synthetic
      `Expr::column("count")` placeholder for
      `COUNT(*)` preserved verbatim.
- [ ] `lower_sum_aggregate` / `lower_min_aggregate` /
      `lower_max_aggregate` / `lower_avg_aggregate`
      accept computed `Expr` args.
- [ ] `aggregate_output_name` returns kernel-name-only
      strings.
- [ ] `AggregateOutputCatalog::resolve_for_*` composes
      unchanged.
- [ ] Binder per-kernel type-domain validation
      preserved verbatim.
- [ ] Row-layer executor seam unchanged; no
      `LogicalPlan::Project` between input and
      `Aggregate`.
- [ ] PBC declines aggregates with computed args.
- [ ] Column-only-argument plan digests byte-identical
      pre/post-ADR-028.
- [ ] Experimental kernels remain excluded from grouped
      aggregate signature matching via D2 type
      segregation.
- [ ] Public `AggregateFunction` / `Expr` /
      `ScalarValue` remain non-`Eq` / non-`Hash`.
- [ ] **11** new ADR 028 architecture guards pass.
- [ ] **2** existing guards adjusted (ADR 024 D6 +
      ADR 020 AVG).
- [ ] Positive / negative / matching-ambiguity /
      regression / plan-digest / compositional
      fixtures pass.
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.
