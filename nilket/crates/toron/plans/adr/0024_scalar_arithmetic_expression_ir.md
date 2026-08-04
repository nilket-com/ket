# ADR 024: Scalar Arithmetic Expression IR

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Companion implementation plan:**
`plans/impl/0024_scalar_arithmetic_expression_ir.md`

## Status

Promoted.

First substrate-and-surface ADR after ADR 023 that lifts a
specific user-facing reject named in ADR 023's negative-case
fixture set. ADR 023 bounded computed projection to the
existing `Expr` surface and explicitly deferred arithmetic to
"a scalar arithmetic IR ADR." This is that ADR.

Three-way council convergence reached across four iterations.
The Cast IR implementation-shape decision flipped across the
cycle (rotating multiple times across Codex iter-0/1/2/3/4
and Claude iter-0/1/2/3/4); convergence ratified by iter-4
with full 3/3 agreement on every load-bearing decision.

Load-bearing decisions:

- **Lead.** Scalar Arithmetic Expression IR. (Claude / Codex /
  Gemini iter-0.)
- **Four binary arithmetic operators: `+`, `-`, `*`, `/`.**
  (Claude / Gemini iter-0; Codex iter-1 concession from
  iter-0 defer-division position.)
- **Primitive numeric domain.** `Int32`, `Int64`, `Float32`,
  `Float64`. No Decimal, Date/Timestamp, Boolean, Utf8.
- **Conservative-precise promotion lattice.** Pure-Int
  preserves width (`Int32 + Int32 → Int32`); pure-Float
  preserves width (`Float32 + Float32 → Float32`); mixed
  Int+Float always promotes to `Float64`. (3/3 by iter-2
  after Codex+Claude pushback on Gemini iter-1's
  `Int64+Float32 → Float32` cell; Gemini iter-4 ratifies.)
- **Cast IR variant — YES, binder-only widening.** Four
  widening Cast conversions in scope: `Int32 → Int64`,
  `Int32 → Float64`, `Int64 → Float64`,
  `Float32 → Float64`. No SQL `CAST(x AS T)` lowering. (3/3
  by iter-4 after the cycle's longest flip-flop;
  recorded in §Recorded dissent.)
- **Per-lattice division output.** `Div` uses the same lattice
  as `Add`/`Sub`/`Mul`. `Int64 / Int64 → Int64` (truncating
  integer division per PostgreSQL/DuckDB/SQLite SQL norm).
  (3/3 by iter-4; Codex iter-2 dissent on always-Float64
  retracted iter-3/4.)
- **Checked integer overflow returns `ExecutionError::Kernel`.**
  Reuse existing variant with stable message format
  `"arithmetic overflow: {Op}({Type})"`; no new variant.
  ADR 020 SUM precedent extended. (3/3 by iter-4 after Gemini
  iter-4 ratified checked overflow.)
- **Division-by-zero policy: integer → NULL; float → IEEE 754.**
  Integer zero divisor returns NULL; integer division overflow
  remains checked overflow. Float `/0` uses direct `/` →
  ±Infinity or NaN. (3/3 by iter-4.)
- **Critical aggregate-argument shape guard.** Once
  `Expr::Add` etc. exist, the accidental
  `lower_expr`-unsupported guard that today rejects
  `SUM(amount + tax)` disappears. ADR 024 adds explicit
  aggregate-argument shape validation in `lower_*_aggregate`
  helpers; diagnostic:
  `"aggregate arguments must be simple column references"`.
  (3/3 by iter-2 — Codex critical guard; ratified through
  iter-4.)
- **Computed aggregate arguments stay rejected.** ADR 023's
  `GroupedAggregateSignature` finite enum preserved;
  `(AggregateFunction, Vec<Expr>)` widening deferred along
  with derive-Eq/Hash refactor.
- **Wendelon PBC arithmetic and Cast out of scope.** Both
  PBC-ineligible; `PbcCompiler` declines explicitly. WHERE
  arithmetic evaluates as Toron residual. (All three iter-0.)
- **No `LogicalPlan` / `ExecutionPlan` variant change.** IR
  extension at the `Expr` / `ExecutionExpr` level only.
- **No fragmenter detector change.** ADR 023's
  `is_terminal_sort_over_grouped_aggregate` preserved.
- **No `AggState`, `STABLE_AGGREGATE_KERNELS`,
  `AggregateOutputCatalog`, `AggregateOutputRule`,
  `GroupedAggregateSignature` change.** ADR 021/022/023
  surfaces byte-identical.
- **No new `TaskFailureKind` variant.** ADR 003's 7-variant
  contract preserved.
- **Brainstorm seams untouched** with positive Brainstorm 001
  alignment: explicit Cast nodes + 16 per-(op, type) kernels
  are canonical JIT specialization seams.
- **SoA-readiness invariants preserved.** ADR 021's four
  invariants all hold; Cast IR strengthens the JIT seam
  shape.

Load-bearing antecedents: ADR 010 (terminal Sort wrap;
output-schema-only sort keys), ADR 011 (aggregate projection
accounting), ADR 020 (SUM `checked_add` precedent;
`ExecutionError::Kernel` overflow message shape), ADR 022
(sticky-NaN / IEEE 754 discipline; "silencing hides bad data"),
ADR 023 (post-aggregate projection;
`select_expr_is_computed` already returns true for arithmetic
SQL AST; `discover_aggregate_calls` walker already recurses
through BinaryOp; `LogicalPlan::Project.output_names` field).

ADR 024 instances ADR 015's cycle property at the **expression-
IR layer**. The removed reject scope is "BinaryOperator::Plus /
Minus / Multiply / Divide" falling into `lower_expr`'s
unsupported-expression catch-all; the new structural reject is
the aggregate-argument shape validation. Sixth instance of the
ADR 015 cycle (after 016 HAVING, 017 ORDER BY aggregate-call,
018 scalar-aggregate, 019/022 kernel-domain, 023 post-
aggregate projection).

## Goal

Lift the scalar arithmetic surface ADR 023 explicitly named-
deferred:

```sql
SELECT SUM(amount) + 1 FROM sales
SELECT region, SUM(amount) * 1.1 FROM sales GROUP BY region
SELECT region, SUM(amount) + COUNT(*) FROM sales GROUP BY region
SELECT amount + tax AS gross FROM sales
SELECT * FROM sales WHERE amount + tax > 100
SELECT SUM(amount) / COUNT(*) AS ratio FROM sales
```

Concretely, post-ADR-024:

- All six examples above bind + execute.
- Arithmetic in SELECT (top-level and post-aggregate),
  HAVING, and WHERE all lower and evaluate.
- Aggregate function arguments still require column
  references or `*`; `SUM(amount + tax)` still rejects with
  a stable diagnostic.
- Plan digests for queries currently bound (no arithmetic)
  remain byte-identical.
- Wendelon PBC compiler still doesn't see arithmetic
  opcodes; WHERE arithmetic evaluates Toron-residual.

Bounded by:

- No new IR variants beyond five (`Cast` + four arithmetic
  ops). No SQL `CAST(x AS T)` syntax exposed.
- No new `ExecutionError` variant, no new `TaskFailureKind`
  variant, no new `AggregateOutputRule` variant, no new
  `StableAggregateKernel` row.
- No `AggState` change; ADR 022 `Copy` preserved.
- No fragmenter detector change.
- No cross-ring engagement; ADR 02X stays parked.
- No `GroupedAggregateSignature` widening; computed
  aggregate arguments stay deferred.
- No unary negation, modulo, bitwise ops, string concat,
  Decimal, Date/Timestamp arithmetic, CASE, COALESCE, NULLIF.

## Scale boundary

**Primary:** Toron expression layer — `Expr` / `ExecutionExpr`
IR widening, binder type-promotion with Cast injection,
executor evaluation. **Local IR + binder + executor surface
expansion; no fragmenter or cross-ring engagement.**

In scope:

- `crates/toron/src/ir.rs`: `Expr` gains five arithmetic
  variants (Cast + Add/Sub/Mul/Div).
- `crates/toron/src/ir/execution.rs`: `ExecutionExpr` gains
  five symmetric variants; `data_type()`, `nullable()`,
  `pbc_eligible()` extend.
- `crates/toron/src/sql_planner.rs::lower_expr`: four
  `BinaryOperator` arms (Plus, Minus, Multiply, Divide) lower
  to `Expr::Add`/`Sub`/`Mul`/`Div`. `SqlExpr::Cast`
  continues to reject through the existing unsupported-
  expression path.
- `crates/toron/src/sql_planner.rs::lower_*_aggregate`:
  aggregate-argument shape validation prevents computed
  aggregate arguments.
- `crates/toron/src/binder.rs::bind_expr`: arithmetic
  expression binding via `bind_arithmetic` helper. The helper
  computes the lattice common type, injects
  `ExecutionExpr::Cast` nodes for operands that don't match
  the common type, and emits the arithmetic
  `ExecutionExpr`. Non-numeric operands reject with stable
  diagnostic. Cast binding validates widening-only conversions.
- `crates/toron/src/exec.rs`: 16 same-type arithmetic kernels
  (4 ops × 4 types) + 4 Cast evaluator paths (the four
  widening conversions). Total 20 evaluator paths.
  Checked integer overflow returns `ExecutionError::Kernel`.
  Integer zero divisor returns NULL; integer divide overflow
  returns `ExecutionError::Kernel`; float division-by-zero
  follows IEEE 754. Both bound and unbound executor paths
  covered.
- PBC compiler (wherever `PbcCompiler::compile_expr` lives):
  arithmetic AND Cast `ExecutionExpr` variants explicitly
  decline PBC compilation.
- `crates/toron/tests/architecture_test.rs`: seven new guards
  + variant-count assertion bumps on `Expr` and
  `ExecutionExpr`.
- `crates/toron/tests/group_by_test.rs`,
  `non_aggregate_projection_accounting_test.rs`,
  `aggregate_projection_accounting_test.rs`: positive +
  negative fixture suite (see §Surface matrix).
- `crates/toron/tests/distributed_process_test.rs`: one
  composition fixture — grouped post-aggregate arithmetic
  query across 2 and 4 workers, digest parity gate.

Explicitly unchanged:

- **`LogicalPlan` and `ExecutionPlan` variant sets.** ADR 008
  single-variant `Join` guard, ADR 023's
  `LogicalPlan::Project.output_names`, all preserved.
- **`AggregateFunction` enum.** Shape unchanged: stable
  catalog functions plus existing experimental aggregate
  entries remain as-is.
- **`StableAggregateKernel`, `AggregateOutputRule`,
  `STABLE_AGGREGATE_KERNELS`.** ADR 021/022 surface byte-
  identical.
- **`AggState` enum.** ADR 022's 13 variants; `Copy`
  preserved.
- **`GroupedAggregateSignature`.** ADR 023's finite column-
  based enum preserved.
- **`AggregateOutputCatalog`.** ADR 023 surface preserved;
  arithmetic in post-aggregate projection composes naturally
  through the existing four-condition `Project` emission
  predicate.
- **Fragmenter detector and routing.** ADR 023's
  `is_terminal_sort_over_grouped_aggregate` preserved with
  exact peel order.
- **Wendelon AGG protocol and PBC bytecode.** PBC sees no new
  opcodes.
- **`TaskFailureKind`.** ADR 003's 7 variants preserved.
- **Brainstorm seams.** None touched (positive Brainstorm 001
  alignment).
- **SoA-readiness invariants.** ADR 021's four invariants all
  preserved.
- **Plan digests for queries currently bound.** Arithmetic
  surface is additive; existing queries produce byte-
  identical plans pre/post ADR 024.

No new IR variant beyond five. No new fragmenter recognition
site. No new `ExecutionError` variant. No new
`TaskFailureKind` variant. No new architecture-test machinery
beyond source-read guards.

## Decisions (council-ratified)

### D1 — Four arithmetic ops + binder-only widening Cast

```rust
pub enum Expr {
    // ... existing variants ...
    Cast(Box<Expr>, DataType),        // NEW (binder-only widening)
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
}
```

Symmetric `ExecutionExpr` variants.

**Cast guardrails** (3/3 by iter-4):
- `SqlExpr::Cast` rejected by `SqlPlanner::lower_expr` (no
  SQL CAST surface).
- `Cast` nodes inserted only by the binder during arithmetic
  operand promotion.
- `Cast` only widens numeric values; the four conversions in
  scope are `Int32 → Int64`, `Int32 → Float64`,
  `Int64 → Float64`, `Float32 → Float64`. Narrowing rejects.
- `Cast` is not PBC-eligible.
- Architecture guard `adr024_cast_is_binder_only_widening`
  asserts no SQL Cast lowering AND the four widening
  conversions.

Deferred to follow-on ADRs: unary negation (`-x`), integer
modulo (`%`), SQL `CAST(x AS T)` syntax, CASE, COALESCE,
bitwise ops, string concatenation, Decimal arithmetic,
Date/Timestamp arithmetic.

### D2 — Conservative-precise promotion lattice

| Left ↓ / Right → | Int32 | Int64 | Float32 | Float64 |
|---|---|---|---|---|
| **Int32**   | Int32   | Int64   | **Float64** | Float64 |
| **Int64**   | Int64   | Int64   | **Float64** | Float64 |
| **Float32** | **Float64** | **Float64** | Float32 | Float64 |
| **Float64** | Float64 | Float64 | Float64 | Float64 |

Bolded cells: mixed Int+Float always promotes to Float64.

Reasons:
- **Pure-Int preserves width.** `Int32 + Int32 → Int32`
  matches user expectation; checked overflow catches edge
  cases.
- **Pure-Float preserves width.** `Float32 + Float32 →
  Float32` keeps Float32 columns at Float32 precision.
- **Mixed Int+Float → Float64.** Float32's 24-bit mantissa
  can't represent Int32 fully (32 bits) or Int64 fully
  (53+ bits needed). Promoting to Float64 is the safe
  choice.

Same lattice applies to Add, Sub, Mul, Div.

Architecture guard `adr024_arithmetic_promotion_lattice_is_narrow`
source-reads the exact 16-cell table.

### D3 — Cast injection logic

`bind_arithmetic` algorithm:
1. Bind left and right operands recursively.
2. Compute `output_type = arithmetic_output_type(left.dt,
   right.dt)`. Reject if non-numeric or no common type
   exists.
3. For each operand whose type differs from `output_type`,
   wrap in `ExecutionExpr::Cast(operand, output_type)`. The
   wrapped Cast must be one of the four widening conversions;
   the lattice ensures this is never violated.
4. Emit the arithmetic `ExecutionExpr` with both operands now
   at `output_type`.

Result: arithmetic kernels see matched-type operands
exclusively. No mixed-type runtime dispatch; SoA / JIT seams
preserved.

### D4 — Checked overflow returns `ExecutionError::Kernel`

Integer arithmetic overflow on the result type returns:

```rust
ExecutionError::Kernel {
    message: "arithmetic overflow: Add(Int32)".to_string(),
}
```

Message format: `"arithmetic overflow: {Op}({Type})"`. Integer
division also detects the `MIN_VALUE / -1` overflow and
returns the same kernel error shape. Zero divisor is not
treated as overflow; D5 maps it to NULL.

No new `ExecutionError::Overflow` variant; reuses the
existing `ExecutionError::Kernel { message }` shape per
ADR 020 SUM precedent
(`ExecutionError::Kernel { message: "SUM(Int) overflow" }`).

Wire-level mapping: `TaskFailureKind::TaskFailed { retryable:
false, reason: "Arithmetic::Overflow" }` per ADR 007 reason-
string precedent.

**Float arithmetic** does not check overflow; produces
`±Infinity` per IEEE 754. NaN propagation per IEEE 754
(composes with ADR 022 sticky-NaN philosophy).

### D5 — Division-by-zero policy

Integer division by zero (when result type is `Int32` or
`Int64`) returns NULL. Float division by zero (when result
type is `Float32` or `Float64`) follows IEEE 754
(`±Infinity` for non-zero/zero, `NaN` for zero/zero).

Rationale:
- **Integer NULL** matches DuckDB and SQLite precedent.
- **Float IEEE** is the only sensible Float64 behavior;
  composes with ADR 022 sticky-NaN.
- **Asymmetric per result type** matches the
  representation: integers have no Inf; floats do.

Implementation: integer division kernel checks the divisor
first. Zero appends NULL. Non-zero division then uses
`checked_div`; `None` at that point is integer overflow
(`MIN_VALUE / -1`) and returns `ExecutionError::Kernel`.
Float division uses direct `/` with IEEE semantics.

**Integer truncation note.** `Int64 / Int64 → Int64`
truncates (integer division). PostgreSQL/DuckDB/SQLite all
truncate. Users who want non-truncating division compose
with a Float literal (`SUM(amount) * 1.0 / COUNT(*)`) or
use the future SQL `CAST(x AS Float64) / y` once the CAST
ADR lands.

### D6 — Aggregate-argument shape guard

`lower_*_aggregate` helpers in `sql_planner.rs` add explicit
argument-shape validation via `validate_aggregate_argument`
helper:

```rust
fn validate_aggregate_argument(
    arg: &FunctionArg,
    allow_wildcard: bool,
) -> Result<&FunctionArg, FrontendError> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if allow_wildcard => Ok(arg),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Identifier(_)))
        | FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::CompoundIdentifier(_))) => Ok(arg),
        _ => Err(unsupported(
            "aggregate arguments must be simple column references"
        )),
    }
}
```

Diagnostic: `"aggregate arguments must be simple column
references"`. Architecture guard
`adr024_aggregate_arguments_remain_column_only` source-reads
the rejection and asserts the diagnostic.

Critical: without this guard, `SUM(amount + tax)` would
accidentally start binding once `Expr::Add` exists. The
implementation MUST preserve the column-only invariant on
aggregate arguments until a future ADR widens
`GroupedAggregateSignature`.

### D7 — Wendelon PBC arithmetic and Cast out of scope

`ExecutionExpr::pbc_eligible()` returns `false` for
arithmetic AND Cast variants. `PbcCompiler::compile_expr`
adds explicit `Err(...)` branches. Storage-boundary pushdown
declines; predicates fall through to Toron residual
evaluation via existing `FilterDisposition::EvaluateInToron`.

Architecture guard `adr024_pbc_compiler_declines_arithmetic_and_cast`
source-reads the compiler for the decline branches.

Cross-ring PBC arithmetic pushdown is a separable follow-on
ADR; promotes on workload evidence.

## Plan emission and ADR 023 composition

ADR 023's `Project` emission predicate composes naturally:

- **(b) Computed expression** fires when any SELECT item is a
  computed expression in the existing `Expr` surface. Post-
  ADR-024, arithmetic SQL like `SUM(amount) * 1.1` triggers
  the predicate; `LogicalPlan::Project` is emitted above
  `Aggregate`. Arithmetic kernels evaluate the projection on
  the aggregate-consumer task (Stage 1).
- **HAVING with arithmetic** (e.g., `HAVING SUM(amount) +
  COUNT(*) > 10`): ADR 023's `discover_aggregate_calls`
  walker recurses through `SqlExpr::BinaryOp`; aggregate
  calls discovered; arithmetic lowers to `Expr::Add` etc.;
  `Filter` predicate evaluates on Aggregate output schema.
- **WHERE with arithmetic** (`WHERE amount + tax > 100`):
  lowered to `Expr::Gt(Expr::Add(...), Expr::Literal(100))`;
  PBC ineligibility forces `FilterDisposition::EvaluateInToron`.
- **ORDER BY arithmetic**: ADR 010's output-schema-only sort
  key rule preserved; users project the arithmetic with an
  alias (`SELECT a + b AS sum ... ORDER BY sum`).

## Architecture guards

| Guard | Scope | Status |
|---|---|---|
| ADR 015 cycle property | preserved | Reject-removal pattern; four arithmetic ops out of `lower_expr` catch-all |
| ADR 020 SUM overflow message | preserved | Existing `"SUM(Int) overflow"` still fires; new arithmetic messages use the same `ExecutionError::Kernel` shape |
| ADR 022 `AggState` variant count | preserved | No AggState change |
| ADR 023 `output_names` field | preserved | Project IR unchanged beyond what ADR 023 added |
| ADR 023 detector peel order | preserved | Fragmenter unchanged |
| ADR 023 diagnostics | preserved | No regression |
| **NEW** `expr_variant_count_includes_arithmetic_and_cast` | `ir.rs` source-read | Asserts the five new variants present (Cast, Add, Sub, Mul, Div) |
| **NEW** `execution_expr_variant_count_includes_arithmetic_and_cast` | `ir/execution.rs` source-read | Symmetric |
| **NEW** `adr024_arithmetic_promotion_lattice_is_narrow` | `binder.rs` source-read | Asserts the 16-cell promotion table is exactly the 4×4 numeric primitive grid (D2); no Decimal/Date/Boolean/Utf8/unsigned arms |
| **NEW** `adr024_arithmetic_in_sql_planner_lowers_to_ir` | `sql_planner.rs::lower_expr` source-read | Four `BinaryOperator` arms present (Plus, Minus, Multiply, Divide) |
| **NEW** `adr024_integer_overflow_returns_kernel_error` | `exec.rs` source-read | `checked_*` usage, explicit zero-divisor handling before integer division, and stable `"arithmetic overflow: "` diagnostic |
| **NEW** `adr024_aggregate_arguments_remain_column_only` | `sql_planner.rs::lower_*_aggregate` source-read | Asserts the column-only validation + diagnostic |
| **NEW** `adr024_cast_is_binder_only_widening` | `sql_planner.rs::lower_expr` + `binder.rs` source-read | Asserts no `SqlExpr::Cast` lowering site; asserts Cast evaluator covers only the four widening conversions |
| **NEW** `adr024_pbc_compiler_declines_arithmetic_and_cast` | PBC compiler source-read | Asserts explicit decline branches for the five new variants |

Reject-string-presence: D4 overflow message, D6 aggregate
argument message, D5 (implicit — no error path for /0)
preserved by tests and guards.

## What Not To Do

- **Don't expose SQL `CAST(x AS T)` syntax.** ADR 024 keeps
  the planner rejecting `SqlExpr::Cast`. SQL CAST is a future
  ADR with its own narrowing/saturating decision matrix.
- **Don't introduce narrowing Cast.** The binder injects only
  the four widening conversions; narrowing rejects at bind
  time. Future SQL CAST ADR adds narrowing semantics.
- **Don't add unary negation, modulo, bitwise ops, string
  concat, Decimal arithmetic, Date/Timestamp arithmetic,
  CASE, COALESCE, NULLIF.** Each is its own follow-on ADR.
- **Don't widen `GroupedAggregateSignature` to support
  `SUM(x + y)`.** Computed aggregate arguments require the
  derive-Eq/Hash refactor + signature widening; sibling ADR.
- **Don't drop the column-only aggregate argument guard.**
  Without it, `SUM(amount + tax)` accidentally binds once
  arithmetic lowers; the `GroupedAggregateSignature` enum
  can't represent the computed argument cleanly.
- **Don't add a new `ExecutionError` variant for arithmetic
  overflow.** Reuse `ExecutionError::Kernel` per ADR 020
  precedent.
- **Don't add a new `TaskFailureKind` variant.** ADR 003's
  7-variant contract preserved.
- **Don't use wrapping or saturating arithmetic for
  integers.** Checked overflow is the contract; silent
  corruption is the failure mode the cycle exists to
  prevent.
- **Don't push arithmetic or Cast to Wendelon PBC.** Cross-
  ring arithmetic opcodes are a follow-on ADR.
- **Don't widen `lower_expr` to cover non-arithmetic
  `SqlExpr` variants.** The unsupported-expression catch-all
  remains for all other SQL surface shapes.
- **Don't synthesize a new fragmenter recognition site.**
  ADR 015's "one narrow detector" property preserved.
- **Don't accept ORDER BY on arbitrary computed sort keys.**
  ADR 010's output-schema-only rule preserved.
- **Don't change division output type to Float64 always.**
  Per-lattice Div matches SQL norms (PostgreSQL/DuckDB/SQLite
  all truncate integer division).
- **Don't expose Cast through any user-facing surface.**
  Binder-only insertion is the contract.

## Recorded dissent

All previously-recorded iter-2/3 dissents resolved by iter-4:

- **Cast IR variant** — multi-iteration flip-flop resolved
  3/3 Yes at iter-4. Codex iter-0 No → iter-1 No → iter-2
  Yes → iter-3 No → iter-4 Yes; Claude iter-0 No → iter-1
  Yes → iter-2 No → iter-3 Yes → iter-4 Yes; Gemini stable
  Yes from iter-1.
- **Overflow semantics** — Gemini iter-2/3 wrapping dissent
  resolved iter-4; ratified checked overflow.
- **Division-by-zero policy** — Gemini iter-2/3
  error-everywhere dissent resolved iter-4.
- **Div output type** — Codex iter-2 Float64-always dissent
  retracted iter-3/4.

**No substantive dissent remains at iter-4.**

### Process observation

The Cast IR multi-flip-flop is the second cycle exhibiting
an implementation-shape rotation pattern (after ADR 023's
name `PostAggregateScope` vs `AggregateOutputCatalog`
decision flipping twice).

ADR 023 §"Recorded process note" first named the pattern;
ADR 024 confirms: the pattern recurs **when the choice is
implementation-shape, not contract-shape** — both
Cast-IR-Yes (with binder-only guardrails) and Cast-IR-No
(with kernel-internal type promotion) implement the same
observable semantics; both preserve SoA-readiness; both
satisfy the narrow-ADR discipline.

For future cycles, the recurring lesson is to hard-pin
implementation-shape decisions in iter-0 with an explicit
"both choices technically equivalent; default to X unless
new technical argument surfaces" rationale. The council can
revisit if a load-bearing argument emerges, but the default
prevents rotation across iterations.

## SoA-readiness check

ADR 021's four invariants preserved:

1. **Catalog stays pure metadata.** No catalog change in ADR
   024. ADR 023's `AggregateOutputCatalog` and ADR 021's
   `AggregateKernelSpec` both unchanged.
2. **`StableAggregateKernel` `#[repr(u8)]`** unchanged.
3. **Centralized dispatch seams.** Per-(op, output-type)
   arithmetic kernel selection + per-conversion Cast paths
   are localized match-on-DataType dispatch in
   `evaluate_bound_expr` and `evaluate_expr`. SoA layout
   specialization seam shape preserved.
4. **No row-path dyn dispatch.** Per-(op, output-type)
   arithmetic kernels and per-conversion Cast paths are
   explicit functions; no `&dyn` / `Box<dyn>`. The Cast IR
   variant makes specialization seams visible at the IR
   layer — explicit type-conversion nodes the JIT compiler
   can target without runtime type dispatch.

**Positive alignment.** The 16 same-type arithmetic kernels
+ 4 Cast evaluator paths are the canonical SoA / SIMD / JIT
specialization shape. Future Cranelift JIT compilation
(Brainstorm 001) specializes each kernel + each Cast
conversion for its specific (op, type) tuple; the IR shape
already exposes these as distinct dispatch points. Explicit
Cast nodes make specialization boundaries visible.

## Brainstorm-seam touch check

ADR 024 touches none of the load-bearing brainstorm seams;
mild positive Brainstorm 001 alignment:

- **Brainstorm 001 (Cranelift JIT):** **Positive alignment.**
  The 16 same-type arithmetic kernels + 4 Cast evaluator
  paths are the canonical JIT specialization shape. Explicit
  Cast nodes (vs runtime type-dispatching inside kernels)
  make specialization seams visible.
- **Brainstorm 002 (WeakControlRing):** No coordinator-state
  touch.
- **Brainstorm 003 (Disaggregated Compute):** Fragmenter
  unchanged. Arithmetic and Cast execute wherever the
  containing expression executes — Stage 0 producers for
  non-aggregate filters/projections; Stage 1 aggregate
  consumers for post-aggregate arithmetic (per ADR 023).
- **Brainstorm 004 (Memory Arenas):** Five new `Expr` /
  `ExecutionExpr` variants each carry `Box<Expr>` operands;
  arena-compatible at the same shape as existing variants.

No seam closes; no seam touches beyond ADR 023's existing
surface.

## Definition of done

1. `Expr` enum has five new variants: `Cast(Box<Expr>,
   DataType)`, `Add`/`Sub`/`Mul`/`Div(Box<Expr>, Box<Expr>)`.
2. `ExecutionExpr` enum has five symmetric variants;
   `data_type()` returns the cast target type / lattice
   output; `nullable()` propagates correctly (Div integer
   nullable to account for /0 → NULL); `pbc_eligible()`
   returns false for all five.
3. `sql_planner.rs::lower_expr` has four `BinaryOperator`
   arms (Plus, Minus, Multiply, Divide); `SqlExpr::Cast`
   continues to reject through the unsupported-expression
   path.
4. Aggregate-argument shape guard rejects computed args with
   `"aggregate arguments must be simple column references"`.
5. `binder.rs::bind_arithmetic` helper applies the 16-cell
   promotion lattice; injects `ExecutionExpr::Cast` nodes
   for operand promotion; non-numeric operands reject.
6. `binder.rs::bind_expr` Cast arm validates widening-only
   conversions.
7. `exec.rs`: 16 per-(op, output-type) arithmetic kernels +
   4 Cast evaluator paths covering Int32, Int64, Float32,
   Float64; integer kernels use `checked_*`; integer
   division-by-zero returns NULL; integer divide overflow
   returns `ExecutionError::Kernel`; float kernels follow
   IEEE 754.
8. `PbcCompiler::compile_expr` declines arithmetic AND Cast
   explicitly.
9. Seven new architecture guards green; existing ADR 015 /
   020 / 022 / 023 guards preserved.
10. Surface matrix (positive + negative + error) tested in
    `group_by_test.rs`,
    `non_aggregate_projection_accounting_test.rs`,
    `aggregate_projection_accounting_test.rs`.
11. Distributed-process-test composition fixture green at 2
    and 4 workers; digest parity asserted.
12. Plan digests for currently-accepted queries byte-
    identical pre/post ADR 024.
13. `cargo fmt --check` clean; `cargo test -p toron` green.

## Surface matrix

### Positive (must bind + execute)

```sql
-- Top-level arithmetic in SELECT
SELECT amount + id AS gross FROM sales
SELECT amount - id AS delta FROM sales
SELECT amount * 2 AS doubled FROM sales
SELECT amount / 2 AS half FROM sales

-- Post-aggregate arithmetic
SELECT region, SUM(amount) + COUNT(*) AS score FROM sales GROUP BY region
SELECT region, SUM(amount) * 1.1 AS adjusted FROM sales GROUP BY region
SELECT SUM(amount) + 1 AS bumped FROM sales
SELECT SUM(amount) / COUNT(*) AS ratio FROM sales

-- Arithmetic in HAVING
SELECT region FROM sales GROUP BY region HAVING SUM(amount) + COUNT(*) > 10
SELECT SUM(amount) AS total FROM sales HAVING SUM(amount) * 2 > 100

-- Arithmetic in WHERE (Toron residual; no PBC push)
SELECT * FROM sales WHERE amount + tax > 100

-- Mixed numeric type arithmetic (lattice exercises)
SELECT int32_col + int64_col FROM sales        -- → Int64; binder Cast(Int32→Int64)
SELECT int32_col + float32_col FROM sales       -- → Float64; binder Cast×2
SELECT int64_col + float32_col FROM sales       -- → Float64
SELECT float32_col + float64_col FROM sales     -- → Float64
SELECT int32_col + float64_col FROM sales       -- → Float64
```

### Negative (must reject)

```sql
-- Computed aggregate arguments — ADR 024 critical guard preserves
SELECT SUM(amount + tax) FROM sales
SELECT COUNT(amount + tax) FROM sales
SELECT region, SUM(amount + tax) FROM sales GROUP BY region
SELECT MIN(amount * 2) FROM sales
SELECT AVG(price * quantity) FROM sales

-- Out-of-scope ops
SELECT -amount FROM sales                       -- unary negation
SELECT amount % 2 FROM sales                    -- modulo
SELECT amount & 0xFF FROM sales                 -- bitwise
SELECT CAST(amount AS Float64) FROM sales       -- SQL CAST
SELECT CASE WHEN x > 0 THEN 1 ELSE 0 END FROM sales  -- CASE

-- Out-of-scope domains
SELECT region + amount FROM sales               -- Utf8 + numeric
SELECT amount + 'x' FROM sales                  -- numeric + Utf8
SELECT decimal_col + 1 FROM sales               -- Decimal (future ADR)
SELECT date_col + 1 FROM sales                  -- Date arithmetic (future ADR)

-- ORDER BY arithmetic without projection alias (ADR 010 preserved)
SELECT * FROM sales ORDER BY amount + tax

-- Existing rejects preserved
SELECT region, COUNT(*) FROM sales s JOIN d ON s.id = d.id GROUP BY region
```

### Error fixtures (runtime)

```sql
-- Integer overflow → ExecutionError::Kernel
"arithmetic overflow: Add(Int32)" -- e.g., Int32::MAX + 1
"arithmetic overflow: Div(Int32)" -- e.g., fixture min_i32 / neg_one

-- Integer divide-by-zero → NULL
SELECT 1 / 0 FROM sales                  -- result: NULL
SELECT amount / quantity FROM sales WHERE quantity = 0
                                          -- result column has NULL rows

-- Float divide-by-zero → IEEE 754
SELECT 1.0 / 0.0 FROM sales              -- result: +Infinity
SELECT 0.0 / 0.0 FROM sales              -- result: NaN

-- NULL propagation (nullable-column or constructed-array fixtures;
-- SQL NULL literal support remains out of scope)
SELECT nullable_amount + 1 FROM sales    -- NULL input rows produce NULL
SELECT 1 + nullable_amount FROM sales    -- NULL input rows produce NULL
```

## Future work

ADR 025+ candidates surfaced by ADR 024:

- **SQL `CAST(x AS T)` syntax.** Now has a clean substrate
  to extend (the `Cast` IR variant is in place; future ADR
  adds narrowing/saturating semantics + SQL planner
  lowering). Likely the next-most-needed surface.
- **Computed aggregate arguments** (`SUM(x + y)`). Requires
  derive-Eq/Hash refactor on `AggregateFunction` and `Expr`
  + `GroupedAggregateSignature` widening to
  `(AggregateFunction, Vec<Expr>)`. Composes naturally with
  ADR 024's arithmetic once the signature substrate exists.
- **Unary negation** (`-x`). Small surface; could bundle
  with modulo.
- **Integer modulo** (`%`). Has its own /0 decision (matches
  Div).
- **`CASE WHEN ... THEN ... ELSE ... END`.** Substantial own
  decision matrix.
- **`COALESCE` / `IFNULL` / `NULLIF`.** Null-handling
  functions.
- **Wendelon PBC arithmetic and Cast opcode extension.**
  Cross-ring ADR; promotes on workload evidence.
- **HAVING alias visibility, ORDER BY hidden aggregate
  keys** (ADR 023 named-deferred siblings).
- **Utf8 MIN/MAX, Decimal arithmetic, Date/Timestamp
  arithmetic.** Each its own kernel-domain widening.
- **Kahan-Neumaier stability ADR, DISTINCT, AVG window,
  GROUP BY over joins, kernel graduation, cross-ring AGG
  consumer wiring.** Carried from ADR 023.
