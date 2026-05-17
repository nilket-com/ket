# ADR 026: `TRY_CAST(x AS T)` / `SAFE_CAST(x AS T)` — Error-Tolerant Numeric Primitive Cast

## Status

Promoted. Three-way council convergence (Claude / Codex /
Gemini) reached across three iterations. Substantive shape
converged at iter-2 on D1-D10; D11 (inner Cast errors
propagate, not swallowed) promoted from Codex iter-1's
clarification into a numbered decision at iter-2; iter-3
closed three implementation-precision items (logical-IR
`is_pbc_pushdown_eligible` arm, binder ancillary helpers,
hidden-projection guard naming). ADR 015 cycle's eighth
instance (after 016 / 017 / 018 / 019 / 022 / 023 / 024 /
025).

## Scale boundary

**Primary:** Toron expression layer — SQL planner lowering
+ binder TryCast path + executor TryCast evaluator + PBC
decline widening + ADR 025 hidden-sort-key gate extension
(D10). Local SQL surface expansion + new IR variant (twin
to ADR 025 `Cast`) + 6 new fail-to-null kernels (paired
with 6 reused ADR 025 strict kernels for infallible cells).
No fragmenter change, no cross-ring engagement, no
`LogicalPlan` / `ExecutionPlan` variant addition.

## Context

ADR 025 promoted strict SQL `CAST(x AS T)` for the four
primitive numeric types under a 16-cell conversion lattice
with loud-failure error-on-overflow semantics. ADR 025's
D4 explicitly rejected both `TRY_CAST(x AS T)` and
`SAFE_CAST(x AS T)` with stable per-kind diagnostics:

```text
unsupported cast syntax: TRY_CAST; only CAST(... AS ...) is supported
unsupported cast syntax: SAFE_CAST; only CAST(... AS ...) is supported
```

These rejects are pinned by ADR 025's
`adr025_cast_syntax_variants_are_rejected` architecture
guard. Users hitting `cast failed: Float64 to Int32: ...`
from ADR 025's strict policy want `TRY_CAST` as the
natural escape valve for analytical queries over imperfect
data — bad rows should become NULL instead of aborting the
query.

ADR 026 lifts the two named rejects and adds the
error-tolerant variant under the same 16-cell numeric
primitive lattice. ADR 025's strict Cast contract is
preserved verbatim; ADR 026 adds the parallel tolerant
path with surgical null discipline.

## Decision

### D1 — IR shape: separate `Expr::TryCast` variant (twin to Cast)

Add new IR variants paralleling ADR 024's Cast:

```rust
// crates/toron/src/ir.rs
pub enum Expr {
    Cast(Box<Expr>, DataType),
    TryCast(Box<Expr>, DataType),   // NEW
    Add(Box<Expr>, Box<Expr>),
    // ... existing variants from ADR 024
}

// crates/toron/src/ir/execution.rs
pub enum ExecutionExpr {
    Cast(Box<ExecutionExpr>, DataType),
    TryCast(Box<ExecutionExpr>, DataType),  // NEW
    // ...
}
```

Reasoning (3/3 ratified):

- **Plan-digest preservation for non-TryCast queries.**
  Pure-addition variant; existing `Cast` match arms stay
  byte-identical.
- **Match-arm legibility.** PBC, executor, plan-digest,
  binder all gain explicit TryCast arms — visible policy
  separation.
- **Nullability contract divergence.**
  `Cast.nullable() = inner.nullable()` (ADR 025 D7);
  `TryCast.nullable() = true` (always, D5). Twin variants
  expose the contract difference in source.
- **Per-variant PBC discipline.** Both Cast and TryCast
  PBC-ineligible today; a future cross-ring ADR could
  promote one without the other.
- **SoA / JIT seam preservation.** Per-(source, target)
  TryCast paths are the natural JIT specialization shape.

Trade-off: Expr enum 9 → 10; ExecutionExpr 9 → 10.
Minimal addition for semantically distinct nullability +
failure policy.

ADR 024's exhaustive `Expr::is_pbc_pushdown_eligible()`
helper adds the arm:

```rust
Self::TryCast(_, _) => false
```

Mechanically required by Rust compiler exhaustiveness;
called out explicitly so the implementation phase doesn't
miss it.

### D2 — Failure modes that null

| ADR 025 strict Cast failure | ADR 026 TryCast behavior |
|---|---|
| Integer narrowing overflow (`Int64::MAX → Int32`) | NULL |
| Float → Int overflow (ADR 025 D9 exclusive-upper-bound) | NULL |
| Float NaN → Int | NULL |
| Float ±Inf → Int | NULL |
| Float64 → Float32 finite overflow (`x.is_finite() && x.abs() > f32::MAX as f64`) | NULL |

Non-failure preservations (identical to ADR 025):

| ADR 025 success | ADR 026 TryCast behavior |
|---|---|
| Identity (no-op clone) | Identity |
| Widening (lossless) | Widening |
| Float64 → Float32 NaN/±Inf preserve | NaN/±Inf preserve |
| Float → Int truncation toward zero (in range) | Truncation toward zero |
| Int → Float lossy mantissa precision | Lossy mantissa precision |
| Int64 → Float32 saturation to ±Float32::INFINITY | Saturation to ±Float32::INFINITY |
| Null input → null output | Null input → null output |

**Contract:** "Every ADR 025 strict Cast data-domain
error becomes NULL when the same source/target pair is
evaluated by TRY_CAST; every ADR 025 strict Cast success
stays identical."

### D3 — SQL surface: `TRY_CAST` and `SAFE_CAST` bundle as aliases

Both `CastKind::TryCast` and `CastKind::SafeCast` lower
to the same `Expr::TryCast`:

```rust
CastKind::TryCast | CastKind::SafeCast => Ok(Expr::TryCast(
    Box::new(self.lower_expr(expr)?),
    lower_cast_target_type(data_type)?,
))
```

Semantic equivalence: BigQuery `SAFE_CAST` ≡ DuckDB /
Snowflake `TRY_CAST` for the numeric primitive scope.
Both produce null-on-failure with the same five
data-domain failure modes (D2).

Spelling-neutral diagnostic at the binder (the binder no
longer knows which spelling the user wrote once both kinds
collapse to `Expr::TryCast`):

```text
unsupported TRY_CAST/SAFE_CAST: {source:?} to {target:?}; only Int32, Int64, Float32, and Float64 are supported
```

### D4 — `::` (DoubleColon) and `FORMAT` clause stay rejected

ADR 026 removes only ADR 025's `TRY_CAST` and `SAFE_CAST`
syntax rejects. Still rejected:

- `amount::BIGINT` (PostgreSQL `expr::type` shorthand) →
  `"unsupported cast syntax: ::; only CAST(... AS ...) is supported"`.
- `CAST(... FORMAT ...)`,
  `TRY_CAST(... FORMAT ...)`,
  `SAFE_CAST(... FORMAT ...)` →
  `"CAST FORMAT clause is not supported"`.
- Unsupported target types (`VARCHAR`, ambiguous `FLOAT`,
  `DECIMAL`, `BOOLEAN`, temporal) keep ADR 025's target
  diagnostics via the shared `lower_cast_target_type`
  helper.

ADR 025's `adr025_cast_syntax_variants_are_rejected`
guard renames to
`adr026_cast_syntax_doublecolon_and_format_remain_rejected`.

### D5 — Output nullability: always nullable

`TryCast.nullable() = true` regardless of
`inner.nullable()`. This is the key divergence from
ADR 025 D7 passthrough.

```rust
ExecutionExpr::Cast(inner, _) => inner.nullable()
ExecutionExpr::TryCast(_, _) => true
```

Identity `TRY_CAST(id AS INT)` over a non-nullable
`Int32` column may produce an Arrow array with no actual
nulls, but the expression schema remains nullable. The
contract reflects: "this is the null-on-failure operator;
downstream operators must handle null."

### D6 — Aggregate-argument shape: TryCast continues to reject

ADR 024 D6's column-only rule preserved:

```sql
SUM(TRY_CAST(amount AS BIGINT))    -- rejects
COUNT(SAFE_CAST(id AS BIGINT))     -- rejects
AVG(TRY_CAST(price AS DOUBLE))     -- rejects
```

Diagnostic unchanged:

```text
aggregate arguments must be simple column references
```

Relaxation belongs to the future computed-aggregate-
argument ADR (derive-Eq/Hash refactor + `GroupedAggregateSignature`
widening).

Post-aggregate TryCast IS allowed (composes with ADR 023):

```sql
SELECT TRY_CAST(SUM(amount) AS Int32) FROM sales            -- OK; nullable Int32
SELECT region, TRY_CAST(SUM(amount) AS Int32)               -- OK
FROM sales GROUP BY region
SELECT region FROM sales GROUP BY region                    -- OK
  HAVING TRY_CAST(SUM(amount) AS Int32) IS NOT NULL
```

### D7 — Identity TryCast: NO bind-time elision

Parallel to ADR 025 D8: preserve `ExecutionExpr::TryCast(inner,
T)` even when `source == T`. Eliding identity TryCast at
bind time would let `WHERE TRY_CAST(id AS INT) = 1`
collapse to `id = 1` and become PBC-pushable.

```rust
fn bind_try_cast(
    input_schema: &SchemaRef,
    config: &PlannerConfig,
    inner: &Expr,
    target: &DataType,
) -> Result<ExecutionExpr, BindingError> {
    let inner = bind_expr(inner, input_schema, config)?;
    let source = inner.data_type();
    if !is_supported_explicit_numeric_cast(&source, target) {
        return Err(BindingError::TypeMismatch {
            message: format!(
                "unsupported TRY_CAST/SAFE_CAST: {source:?} to {target:?}; only Int32, Int64, Float32, and Float64 are supported"
            ),
        });
    }
    // INTENTIONALLY NO identity elision here.
    // Same PBC-boundary reasoning as ADR 025 D8.
    Ok(ExecutionExpr::TryCast(Box::new(inner), target.clone()))
}
```

Architecture guard `adr026_try_cast_policy_is_separate`
source-reads `bind_try_cast` body with negative
assertions for the absence of `if source == *target` and
`if source == target`.

The evaluator may still short-circuit identity at
execution time (`Ok(array.clone())`) — only the bind-time
elision is forbidden.

### D8 — Executor strategy: 6 fail-to-null kernels + 6 strict reuses

`evaluate_try_cast` mirrors `evaluate_cast`'s 16-arm
dispatch. The 6 **infallible** non-identity cells reuse
ADR 025 strict kernels directly; the 6 **fallible** cells
get new `try_cast_*` kernels:

```rust
fn evaluate_try_cast(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, ExecutionError> {
    if array.data_type() == target {
        return Ok(array.clone());  // identity short-circuit
    }
    match (array.data_type(), target) {
        // 6 infallible cells — reuse ADR 025 strict kernels directly
        (DataType::Int32, DataType::Int64) => cast_i32_to_i64(array),
        (DataType::Int32, DataType::Float32) => cast_i32_to_f32(array),
        (DataType::Int32, DataType::Float64) => cast_i32_to_f64(array),
        (DataType::Int64, DataType::Float32) => cast_i64_to_f32(array),
        (DataType::Int64, DataType::Float64) => cast_i64_to_f64(array),
        (DataType::Float32, DataType::Float64) => cast_f32_to_f64(array),
        // 6 fallible cells — new fail-to-null kernels
        (DataType::Int64, DataType::Int32) => try_cast_i64_to_i32(array),
        (DataType::Float64, DataType::Float32) => try_cast_f64_to_f32(array),
        (DataType::Float32, DataType::Int32) => try_cast_f32_to_i32(array),
        (DataType::Float32, DataType::Int64) => try_cast_f32_to_i64(array),
        (DataType::Float64, DataType::Int32) => try_cast_f64_to_i32(array),
        (DataType::Float64, DataType::Int64) => try_cast_f64_to_i64(array),
        (source, target) => Err(cast_failed(source, target, "unsupported combination (binder gate failure)")),
    }
}
```

**Why separate fail-to-null kernels** over a flag-on-strict-
kernel approach:

- **Surgical null discipline.** Only the 5 documented
  data-domain failure modes produce NULL.
- **No accidental error swallowing.** Internal invariant
  failures (array type mismatches, binder-gate bugs)
  still return `ExecutionError`.
- **Architecture-guard simplicity.** Source-reading
  `try_cast_*` bodies for absence of `cast_failed(`
  calls is precise.
- **No "stop at first failure" pathology.** A naive
  `evaluate_try_cast` that calls `evaluate_cast` and
  catches the result would error out at the first bad
  row instead of producing per-row NULLs.

**Do not implement TryCast by wrapping `evaluate_cast`
with catch-and-null.** This is explicitly forbidden — it
would (a) error out at the first bad row instead of per-
row NULLs, (b) risk swallowing internal executor bugs
unrelated to data-domain failures, (c) violate D11 by
nulling inner child errors that should propagate.

Example `try_cast_f64_to_i32` kernel:

```rust
fn try_cast_f64_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
    let array = as_float64(array, "TryCast source")?;
    let values = array
        .iter()
        .map(|value| -> Option<i32> {
            let value = value?;  // null → null
            if value.is_nan() { return None; }
            if value.is_infinite() { return None; }
            let trunc = value.trunc();
            if !(F64_I32_LOWER <= trunc && trunc < F64_I32_UPPER) {
                return None;
            }
            Some(trunc as i32)
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}
```

Reuses from ADR 025: `F64_I32_LOWER`/`F64_I32_UPPER` /
`F64_I64_LOWER`/`F64_I64_UPPER` constants; truncation
toward zero via `value.trunc()`; f64-conversion before
range check for `Float32 → Int*` (ADR 025 D9).

### D9 — PBC ineligibility: widened diagnostic

`ExecutionExpr::TryCast` is PBC-ineligible. ADR 024 +
ADR 025's pbc.rs diagnostic widens:

```rust
// Before (ADR 024 / 025):
"arithmetic and Cast are not PBC-eligible"

// After (ADR 026):
"arithmetic, Cast, and TryCast are not PBC-eligible"
```

Both `ExecutionExpr::Cast` and `ExecutionExpr::TryCast`
decline PBC. ADR 024's
`adr024_scalar_arithmetic_guards_are_narrow` guard
source-read updates to the widened string.

Two new integration tests parallel to ADR 025:
- `pbc_declines_try_cast_filter` — `WHERE TRY_CAST(amount
  AS BIGINT) IS NOT NULL` evaluates as Toron residual.
- `pbc_declines_identity_try_cast_filter` — `WHERE
  TRY_CAST(id AS INT) = 1` (identity TryCast on `Int32`
  source) stays residual; proves D7 end-to-end.

### D10 — Cast-family hidden sort-key gate extension

ADR 025's implementation added `expr_contains_cast`
helper + `sort_needs_hidden_projection` gate to handle
`ORDER BY CAST(SUM(amount) AS Float32) DESC` via a
hidden-projection mechanism over aggregate output. ADR
026 extends the gate to the cast family:

```rust
// Before (ADR 025 implementation):
fn expr_contains_cast(expr: &Expr) -> bool {
    match expr {
        Expr::Cast(_, _) => true,
        // ... recursion ...
    }
}

// After (ADR 026 D10):
fn expr_contains_cast_family(expr: &Expr) -> bool {
    match expr {
        Expr::Cast(_, _) | Expr::TryCast(_, _) => true,
        // ... recursion ...
    }
}
```

Helper rename; single caller in
`sort_needs_hidden_projection` updates to the renamed
helper. The mechanism proper (`add_hidden_sort_projection`,
`visible_output_names`, `unique_hidden_sort_name`, hidden-
column naming `__toron_order_key_{idx}`) stays untouched.

**Narrow extension principle:** ADR 026 ONLY extends the
gate from `Cast` to `Cast | TryCast`. The broader
"ORDER BY computed expressions on aggregate output"
ratification — generalizing the gate to arithmetic, unary
negation, and future computed-expression families — is a
separate future ADR.

Architecture guard
`adr026_try_cast_order_by_uses_hidden_sort_projection`
extends ADR 025's source-read coverage for the hidden-
projection mechanism from the strict-Cast gate to the
cast-family gate. ADR 025 already pins the Cast-only path
with `adr025_order_by_cast_uses_hidden_sort_projection`;
ADR 026 adds the TryCast composition case while leaving
broader computed-expression coverage for a future
dedicated ADR.

### D11 — Inner strict Cast errors propagate (NOT swallowed by outer TryCast)

`TRY_CAST` nulls failures from **its own** per-pair
conversion. It does NOT catch errors raised while
evaluating its child expression.

```sql
SELECT TRY_CAST(CAST(big_amount AS INT) AS BIGINT) FROM sales
-- big_amount = Int64 value > i32::MAX
-- Inner strict CAST fails with "cast failed: Int64 to Int32: ..."
-- Outer TryCast does NOT null this error
-- Query fails with the inner Cast's ExecutionError::Kernel
```

**Why this matters:**

- "TryCast nulls Cast failures" (D2) means TryCast nulls
  ITS OWN per-pair Kernel-error failure modes (the 5
  documented data-domain modes), not arbitrary child
  errors.
- The natural user pattern "explicit strict then
  tolerate" requires the inner failure to propagate.
- A naive `evaluate_try_cast` that catches
  `ExecutionError::Kernel` from a strict child would
  silently null inner failures too — explicitly forbidden
  by D8.

**Implementation guarantee.** The evaluator dispatch
takes the already-evaluated child array; child evaluation
happens upstream under its own strict policy:

```rust
ExecutionExpr::TryCast(inner, target) => {
    let arr = self.evaluate_bound_expr(inner, batch)?;  // ? propagates child errors
    evaluate_try_cast(&arr, target)
}
```

The `?` operator on the inner call is what enforces D11.
If the child errors, the error propagates before
TryCast's per-pair kernel runs. This is the natural Rust
error-propagation pattern; D11 is structurally enforced
rather than requiring a dedicated architecture guard.

**Fixture** `try_cast_over_strict_cast_propagates_inner_error`:
```sql
SELECT TRY_CAST(CAST(amount * 100000 AS INT) AS BIGINT) FROM sales
```
where `amount * 100000` triggers ADR 024 arithmetic
overflow OR the inner CAST overflow. Expected: query
fails with the inner Kernel-error diagnostic (arithmetic
overflow or cast failed); the outer TryCast does NOT null
this.

## Reject removal (ADR 015 cycle's eighth instance)

Two ADR 025 reject arms convert to one lower arm:

```rust
// Before (ADR 025):
CastKind::TryCast => Err(unsupported("unsupported cast syntax: TRY_CAST; ...")),
CastKind::SafeCast => Err(unsupported("unsupported cast syntax: SAFE_CAST; ...")),

// After (ADR 026):
CastKind::TryCast | CastKind::SafeCast => Ok(Expr::TryCast(
    Box::new(self.lower_expr(expr)?),
    lower_cast_target_type(data_type)?,
)),
```

Structural rejects preserved (cycle's "narrow detector
before generic guard" property):
- `CastKind::DoubleColon` reject preserved (D4).
- `format.is_some()` reject preserved (D4).
- Out-of-lattice source/target types reject — at binder
  layer for invalid sources, at planner layer
  (`lower_cast_target_type`) for invalid targets.

## Architecture guard adjustments

| Guard | Status |
|---|---|
| `adr024_scalar_arithmetic_guards_are_narrow` | **Adjusted.** Bump Expr/ExecutionExpr variant lists to include `TryCast`. Update PBC diagnostic string assertion to `"arithmetic, Cast, and TryCast are not PBC-eligible"`. Add `Expr::is_pbc_pushdown_eligible` TryCast arm assertion. |
| `adr025_sql_cast_lowers_via_planner` | Preserved. |
| `adr025_sql_cast_target_type_normalization` | Preserved (TryCast reuses `lower_cast_target_type`). |
| `adr025_explicit_cast_policy_is_separate_from_implicit_widening` | Preserved. |
| `adr025_cast_supports_numeric_primitive_lattice` | Preserved (`evaluate_cast` body unchanged). |
| `adr025_cast_failure_returns_kernel_error_with_prefix` | Preserved (`cast_failed` helper unchanged). |
| `adr025_cast_syntax_variants_are_rejected` | **Renamed → `adr026_cast_syntax_doublecolon_and_format_remain_rejected`.** Drop TRY_CAST + SAFE_CAST reject sub-assertions; keep `::` + FORMAT; add positive assertion that TRY_CAST + SAFE_CAST now lower. |
| `adr025_float_to_int_uses_exclusive_upper_bound` | Preserved. |
| `pbc_declines_cast_filter` | Preserved. |
| `pbc_declines_identity_cast_filter` | Preserved. |
| `adr026_try_cast_ir_surface_is_narrow` | NEW. Source-reads `Expr::TryCast` + `ExecutionExpr::TryCast` + `data_type()` + `nullable()` always-true (D5) + `pbc_eligible()` false (D9) + `Expr::is_pbc_pushdown_eligible` TryCast arm. |
| `adr026_try_cast_lowers_via_planner` | NEW. Source-reads `lower_expr` for `CastKind::TryCast | CastKind::SafeCast =>` lower arm. |
| `adr026_try_cast_policy_is_separate` | NEW. Source-reads `bind_try_cast` for `is_supported_explicit_numeric_cast` call + always-nullable output + **absence of identity elision** (D7 negative assertion). |
| `adr026_try_cast_supports_numeric_primitive_lattice` | NEW. Source-reads `evaluate_try_cast` for 12-arm dispatch (6 strict-reuse + 6 fail-to-null) + identity short-circuit. |
| `adr026_try_cast_nulls_only_data_domain_failures` | NEW. Source-reads 6 `try_cast_*` kernel bodies for absence of `cast_failed(` + presence of `return None` + F64_*_UPPER reuse (ADR 025 D9). |
| `adr026_try_cast_pbc_declines` | NEW. Source-reads PBC decline arm for widened diagnostic + `ExecutionExpr::TryCast` arm. |
| `adr026_try_cast_order_by_uses_hidden_sort_projection` | NEW. Extends ADR 025's hidden-sort-key source-read coverage from Cast-only to Cast-family. Pins `expr_contains_cast_family` rename + `Cast | TryCast` body match + `sort_needs_hidden_projection` caller update. |
| `pbc_declines_try_cast_filter` | NEW integration test. |
| `pbc_declines_identity_try_cast_filter` | NEW integration test (D7 end-to-end). |
| `order_by_try_cast_aggregate_uses_hidden_projection` | NEW integration test (D10 end-to-end). |

Total: 1 adjusted, 1 renamed, 7 new architecture guards
+ 3 new integration tests.

## SoA-readiness check

ADR 021's four invariants preserved:

1. Catalog stays pure metadata (no catalog touch).
2. `StableAggregateKernel` `#[repr(u8)]` unchanged.
3. Centralized dispatch seams: per-(source, target)
   TryCast path dispatch is a localized `match` in
   `evaluate_try_cast`.
4. No row-path dyn dispatch: per-pair kernels are
   explicit functions.

The 6 new `try_cast_*` kernels follow the same per-pair
specialization shape as ADR 025's 12 strict `cast_*`
kernels. The 6 strict-kernel-reuse cells are pure
function calls — JIT specialization seam preserved.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT):** Per-pair TryCast
  kernels are canonical JIT specialization shape, same
  as ADR 025's per-pair Cast kernels. Positive alignment.
- **Brainstorm 002 (WeakControlRing):** Untouched.
- **Brainstorm 003 (Disaggregated Compute):** Fragmenter
  unchanged.
- **Brainstorm 004 (Memory Arenas):** New
  `TryCast(Box<Expr>, DataType)` shape is arena-
  compatible.

No seam closes; no seam touches beyond ADR 025's
existing surface.

## What Not To Do

- Don't add TryCast to PBC.
- Don't loosen the aggregate-argument shape guard.
- Don't unify Cast and TryCast (D1).
- Don't add `TRY_CAST(... FORMAT ...)` or `expr::?type`
  (D4).
- Don't expand the conversion lattice in ADR 026 — Utf8 /
  Boolean / Decimal / Date TRY_CAST belong to future
  ADRs.
- Don't change Cast's nullability passthrough (ADR 025
  D7 stays).
- Don't introduce a new `ExecutionError` variant.
- Don't implement TryCast by wrapping `evaluate_cast`
  with catch-and-null (D8 + D11). Would (a) stop at
  first failure, (b) swallow internal executor bugs, (c)
  violate D11 by nulling inner child errors.
- Don't use a `null_on_failure: bool` flag on the strict
  kernels (recorded iter-1 dissent — retracted at iter-3
  in favor of separate kernels).
- Don't widen ADR 025's hidden-sort-key gate beyond
  `Cast | TryCast` (D10). The general "ORDER BY computed
  expressions" cycle remains separate.
- Don't retroactively ratify the full ADR 025 hidden-
  projection mechanism in ADR 026 — D10 extends one
  helper for one composability case; the broader
  retroactive ratification belongs to its own ADR cycle.
- Don't swallow inner Cast errors in TryCast (D11).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Plan digest regression for no-TryCast queries | Additive surface; spot-check 5 queries against pre-patch digests. |
| R2 | ADR 025 `cast_syntax_variants_are_rejected` guard breaks | Renamed + sub-assertions adjusted. |
| R3 | Per-pair `try_cast_*` kernels duplicate strict kernel logic | Only 6 fallible cells; 6 infallible reuse strict kernels. |
| R4 | Always-nullable output for TryCast surprises users | D5 documented; fixtures pin behavior. |
| R5 | Aggregate-argument guard over-rejects | Preserved column-only rule. |
| R6 | New IR variant breaks pattern exhaustiveness | Implementation phase enumerates `collect_expr_columns` + `rewrite_expr_ordinals` arms explicitly; compiler exhaustiveness catches at build time. |
| R7 | PBC diagnostic widening breaks ADR 024 guard | `adr024_scalar_arithmetic_guards_are_narrow` string assertion updated. |
| R8 | `evaluate_try_cast` divergence from `evaluate_cast` | Per-pair fixtures pair each strict failure with corresponding tolerant null. |
| R9 | Identity TryCast PBC-pushdown leak | D7 preserves `ExecutionExpr::TryCast` at bind time; `adr026_try_cast_policy_is_separate` negative source-read + `pbc_declines_identity_try_cast_filter` end-to-end. |
| R10 | D11 inner Cast error swallowed by outer TryCast | D11 explicit + `try_cast_over_strict_cast_propagates_inner_error` fixture + structural `?` operator pattern in evaluator dispatch. |
| R11 | ADR 025 hidden-sort-key gate doesn't recognize TryCast | D10 extends gate; new architecture guard + integration test. |
| R12 | Spelling-neutral diagnostic loses debugging context | `TRY_CAST/SAFE_CAST` form covers both. |
| R13 | Walker recursion misses `CastKind::TryCast` | Audit confirms ADR 025 walkers kind-agnostic (`SqlExpr::Cast { expr, .. }` matches all kinds). |
| R14 | `Expr::is_pbc_pushdown_eligible` arm missed | Implementation phase explicit; compiler exhaustiveness catches at build time. |
| R15 | `collect_expr_columns` / `rewrite_expr_ordinals` arms missed | Implementation phase explicit; compiler exhaustiveness catches. |
| R16 | `null_on_failure: bool` flag drift in future refactor | Architecture guard for `try_cast_*` absence of `cast_failed(` prevents accidental regression. |
| R17 | `SAFE_CAST` non-numeric semantics surprise | D3 + D7 scope explicit for numeric domain; non-numeric semantics deferred to future Utf8 / Boolean ADRs. |
| R18 | TryCast over TryCast composition | Natural recursion via binder + evaluator; works structurally without special casing. |
| R19 | Hidden-projection mechanism guard naming confusion | `adr026_try_cast_order_by_uses_hidden_sort_projection` is a NEW ADR 026 extension guard, not a rename of ADR 025's existing `adr025_order_by_cast_uses_hidden_sort_projection` guard. |
| R20 | `expr_contains_cast` callers outside `sort_needs_hidden_projection` exist | Single-caller audit before rename — confirmed during implementation. |

## Process observations

**Iter-3 closed three implementation-precision items.**
ADR 026's substantive shape converged at iter-2 on D1-D10;
iter-3 was a cosmetic alignment pass that nonetheless
caught three implementation-detail items that didn't fit
the iter-2 contract frame:

1. **`Expr::is_pbc_pushdown_eligible()` arm** (Codex
   iter-3 §D1) — adding `Self::TryCast(_, _) => false`
   to the ADR 024 logical-IR helper. Mechanically
   required; called out explicitly.
2. **Binder ancillary helpers** (Codex iter-3 impl) —
   `collect_expr_columns` and `rewrite_expr_ordinals`
   need TryCast arms. Implicit in iter-2 "audit all
   match sites" but named explicitly at iter-3.
3. **Hidden-projection guard naming** — ADR 025 already
   has `adr025_order_by_cast_uses_hidden_sort_projection`
   for the Cast-only path. ADR 026 should not describe
   its guard as the first source-read for the mechanism
   or as a rename. `adr026_try_cast_order_by_uses_hidden_sort_projection`
   is a new extension guard for the cast-family case.

**Codex-iter-1 correctness-clarification pattern recurs.**
Third consecutive ADR cycle where a Codex iter-1/2
correctness argument surfaces a load-bearing semantic
boundary at design time:
- **ADR 024 iter-1**: aggregate-argument shape guard
  prevents `SUM(amount + tax)` accidentally binding.
- **ADR 025 iter-2**: identity-Cast PBC boundary +
  float-to-int exclusive upper bound.
- **ADR 026 iter-1 / iter-2 (D11)**: inner Cast errors
  propagate, not swallowed.

In each case, the council's design-time correctness
review surfaces a class-of-bug boundary at design time.

**Substrate-downstream cadence holds.** ADR 025: 2
iterations; ADR 026: 3 iterations (substantive iter-2,
polish iter-3). Confirms the cadence hypothesis from
ADR 025: substrate-downstream ADRs converge in 2-3
iterations; substrate-introducing ADRs (the next cycle's
likely lead — computed aggregate arguments — is one)
expect 4+ iterations with at least one flip-flop.

**One implementation-shape dissent retracted at iter-3.**
Gemini iter-1/iter-2 advocated `null_on_failure: bool`
flag on the strict kernels. Codex + Claude prefer
separate `try_cast_*` kernels. Gemini iter-3 retracted
the dissent — his iter-3 ADR explicitly says "no
`null_on_failure` flags" under §Executor Policy. Pattern
matches ADR 025 Dissent B retraction (Arrow-kernel
delegation → manual per-pair). The cycle's design-time
correctness focus pulls implementation-shape dissents
toward convergence as architecture-guard precision
implications become clearer.

## Future work

ADR 027+ candidates (ordered by composes-with-substrate
gravity):

- **ORDER BY computed-expressions-on-aggregate-output
  ratification ADR.** Codify the full ADR 025 hidden-
  projection helpers (general computed-expression gate
  beyond cast family; full mechanism architecture guards;
  hidden-column naming convention; duplicate-name
  detection). Code-evidence-driven candidate.
- **Computed aggregate arguments** (`SUM(x + y)`,
  `SUM(TRY_CAST(amount AS BIGINT))`). Requires
  derive-Eq/Hash refactor on `AggregateFunction` (blocked
  by `ApproxQuantile(f64)`) and `Expr` (blocked by
  `ScalarValue::Float32/Float64`) plus
  `GroupedAggregateSignature` widening to
  `(AggregateFunction, Vec<Expr>)`. Substrate-introducing
  ADR (expect 4+ iterations).
- **Utf8 ↔ numeric CAST + TRY_CAST.** Parsing semantics
  ADR — locale, whitespace handling, scientific notation,
  what counts as a parse failure. Extends both lattices
  (strict + tolerant) simultaneously.
- **Boolean ↔ numeric CAST + TRY_CAST.** Truth-table
  semantics ADR (dialect-specific spellings: `'t'` /
  `'1'` / `'yes'`).
- **Decimal CAST + TRY_CAST.** V1.x Decimal numeric-
  types ADR.
- **Date / Timestamp CAST + TRY_CAST.** Temporal-
  conversion ADR.
- **Wendelon PBC Cast and TryCast opcode extension.**
  Cross-ring ADR; promotes on workload evidence.
- **PostgreSQL `expr::type` shorthand.** SQL-compat ADR.
- **`SMALLINT` / `TINYINT` → Int8 / Int16 numeric-type
  expansion.** V1.x numeric-types ADR.
- **HAVING alias visibility, ORDER BY hidden aggregate
  keys.** Carried from ADR 023.
- **Unary negation (`-x`), modulo (`%`), `CASE`,
  `COALESCE` / `IFNULL` / `NULLIF`.** Carried from
  ADR 024 future-work.
- **Utf8 / Decimal MIN/MAX, Kahan-Neumaier stability,
  `DISTINCT`.** Carried from ADR 022 / 020.
- **Cross-ring AGG consumer wiring (ADR 02X).** Carried.
- **Kernel graduation** (`LinearRegress` /
  `ApproxQuantile`). Carried.
