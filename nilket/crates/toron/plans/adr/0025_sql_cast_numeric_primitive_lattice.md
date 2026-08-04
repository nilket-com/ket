# ADR 025: SQL `CAST(x AS T)` Syntax — Numeric Primitive Lattice

## Status

Promoted. Three-way council convergence (Claude / Codex /
Gemini) reached across two iterations on the numeric-only
16-cell primitive lattice scope and manual-kernel
implementation shape, with two correctness corrections
ratified at iter-2 (no identity-cast elision in
`bind_explicit_cast`; exclusive upper bound for float-to-int
range check). ADR 015 cycle's seventh instance (after
016 / 017 / 018 / 019 / 022 / 023 / 024).

## Scale boundary

**Primary:** Toron expression layer — SQL planner lowering
+ binder Cast extension + executor Cast evaluator extension.
Local SQL surface expansion + per-(source, target) kernel
expansion; no IR variant addition, no fragmenter change, no
cross-ring engagement, no `LogicalPlan` / `ExecutionPlan`
variant addition.

## Context

ADR 024 promoted scalar arithmetic by adding five IR
variants (`Cast` + `Add` / `Sub` / `Mul` / `Div`) under a
narrow primitive numeric domain (`Int32`, `Int64`,
`Float32`, `Float64`). ADR 024's Cast variant was
deliberately **binder-only widening**: the binder injects
Cast nodes for arithmetic-operand promotion across the four
widening conversions (`Int32→Int64`, `Int32→Float64`,
`Int64→Float64`, `Float32→Float64`), but SQL `CAST(x AS T)`
syntax stayed rejected through `lower_expr`'s unsupported-
expression catchall.

ADR 024's `arithmetic_bind_injects_only_widening_casts`
test explicitly pinned the `CAST(amount AS BIGINT)` reject
as a named-deferred fixture. This is the first user-facing
surface a query author hits when they want explicit type
coercion — `SUM(amount) / CAST(COUNT(*) AS Float64)` for
clarity, `CAST(int_col AS Float64)` to work around
ADR 024's integer-truncation behavior on `Div`, or
`CAST(int_col AS REAL)` when an explicit lossy precision
tradeoff is desired.

ADR 025 lifts the SQL CAST reject and extends the
conversion lattice from 4 cells (ADR 024 widening) to 16
cells (the full numeric primitive matrix).

## Decision

### D1 — Conversion lattice: 16-cell numeric primitive matrix

The explicit cast lattice covers all 16 source/target pairs
over `Int32 | Int64 | Float32 | Float64`:

| Source ↓ / Target → | Int32 | Int64 | Float32 | Float64 |
|---|---|---|---|---|
| **Int32**   | identity | widening (ADR 024) | int→float (NEW) | widening (ADR 024) |
| **Int64**   | narrowing (NEW) | identity | int→float (NEW) | widening (ADR 024) |
| **Float32** | float→int (NEW) | float→int (NEW) | identity | widening (ADR 024) |
| **Float64** | float→int (NEW) | float→int (NEW) | narrowing (NEW) | identity |

Cell categories:

| Category | Cells | Behavior |
|---|---|---|
| Identity | 4 (diagonal) | Preserved as `ExecutionExpr::Cast` at bind time (D8); short-circuit at evaluation via `Ok(array.clone())` |
| Widening (ADR 024 inherited) | 4 | Lossless; preserved unchanged |
| Integer narrowing | 1 (`Int64→Int32`) | Per-row checked range; overflow → `ExecutionError::Kernel` |
| Float narrowing | 1 (`Float64→Float32`) | Preserve NaN/Inf; finite overflow → `ExecutionError::Kernel` |
| Float → Int | 4 | Reject NaN/Inf; exclusive-upper-bound range check (D9); truncate toward zero |
| Int → Float lossy | 2 (`Int32→Float32`, `Int64→Float32`) | Direct `as f32`; precision loss is explicit user intent |

Total: 4 + 4 + 1 + 1 + 4 + 2 = 16. ADR 025 adds 8 new
evaluator paths (16 total − 4 ADR 024 widening − 4 identity
short-circuits).

**Out of scope** (3/3 ratified deferrals):

- Decimal CAST (V1.x ADR).
- Date / Timestamp / Time / Interval CAST.
- Utf8 ↔ numeric CAST (separate parsing ADR).
- Boolean ↔ numeric CAST.
- Float16, BFloat16, UInt8/16/32/64.
- `SMALLINT`, `TINYINT` (Toron doesn't expose Int16 / Int8).

### D2 — Overflow / domain-violation: `ExecutionError::Kernel` with stable `"cast failed: {Source} to {Target}"` prefix

Reuse `ExecutionError::Kernel { message }` per ADR 020 SUM
/ 022 sticky-NaN / 024 arithmetic-overflow precedent. No
new error variant.

Diagnostic shape:

```rust
ExecutionError::Kernel {
    message: format!("cast failed: {source:?} to {target:?}: {cause}"),
}
```

Stable prefix `"cast failed: "` (13 bytes ASCII) is the
architecture-guard contract; suffix is informative-only.

Cases (all → Kernel error):

- Integer narrowing overflow (`Int64 → Int32` outside `i32::MIN..=i32::MAX`).
- Float → Int overflow (per D9 exclusive upper bound).
- Float NaN → Int.
- Float ±Inf → Int.
- Float64 → Float32 finite overflow (`x.is_finite() && x.abs() > f32::MAX as f64`).

Cases (NOT errors):

- Float64 → Float32 NaN / ±Inf: preserved (IEEE 754 flow-
  through via `as f32`).
- Int → Float lossy precision (mantissa truncation): preserved.
- `Int64 → Float32` is finite for every `i64` value because
  the `i64` range is far below `f32::MAX`; precision loss is
  still accepted as explicit user intent.
- Null input → null output (mask propagation, no kernel
  invocation).

**Float → Int rounding** is truncation toward zero
(`CAST(3.7 AS Int32) = 3`, `CAST(-3.7 AS Int32) = -3`).
ADR 025 chooses Rust-style truncation explicitly; SQL engine
behavior varies, so this is a Toron contract rather than a
dialect-compatibility claim.

### D3 — SQL type name normalization

`lower_cast_target_type` helper maps `sqlparser::ast::DataType`
to Toron `DataType` for the four numeric primitives:

| SQL spelling | Toron target |
|---|---|
| `INTEGER`, `INT`, `INT4`, `INT32` | `Int32` |
| `BIGINT`, `INT64` | `Int64` |
| `REAL`, `FLOAT4`, `FLOAT32` | `Float32` |
| `DOUBLE`, `DOUBLE PRECISION`, `FLOAT8`, `FLOAT64` | `Float64` |

Rejected with stable diagnostic
`"unsupported CAST target type: {sql_type_name}"`:

- `FLOAT`, `FLOAT(n)` — precision-parameter ambiguous
  across dialects (DuckDB / SQLite / MySQL / PostgreSQL
  differ); defer to a future ADR that picks a deliberate
  mapping.
- `SMALLINT`, `TINYINT`, unsigned aliases — Toron doesn't
  expose Int16 / Int8.
- `DECIMAL(p,s)`, `NUMERIC(p,s)` — V1.x Decimal ADR.
- `DATE`, `TIMESTAMP`, `TIME`, `INTERVAL` — temporal
  conversion ADR.
- `VARCHAR`, `TEXT`, `CHAR`, `STRING` — Utf8 parsing ADR.
- `BOOLEAN`, `BOOL` — truth-table semantics ADR.
- `BINARY`, `BYTEA`, list / struct / map — structured-type
  support out of scope.

### D4 — `TRY_CAST` / `SAFE_CAST` / `::` / `FORMAT` rejected

All four reject with per-kind stable diagnostics:

```text
unsupported cast syntax: TRY_CAST; only CAST(... AS ...) is supported
unsupported cast syntax: SAFE_CAST; only CAST(... AS ...) is supported
unsupported cast syntax: ::; only CAST(... AS ...) is supported
CAST FORMAT clause is not supported
```

`TRY_CAST` is the natural follow-on ADR (reuses ADR 025
lattice with `nullable_on_failure = true`). Each follow-on
ADR carries its own decision matrix (what counts as a
parse failure for `TRY_CAST`, BigQuery vs PostgreSQL
`SAFE_CAST` semantics, what format-string flavors
`FORMAT` accepts).

### D5 — Cast in WHERE: Toron-residual evaluation (preserved from ADR 024 D7)

`ExecutionExpr::Cast` remains PBC-ineligible. ADR 024's
`PbcCompiler` decline arm carries forward unchanged.

```sql
SELECT * FROM sales WHERE CAST(amount AS BIGINT) > 1000
```

evaluates as `FilterDisposition::EvaluateInToron`. The
PBC pushdown path declines Cast with the existing ADR 024
diagnostic.

D8's no-identity-elision rule **strengthens** this
contract: `WHERE CAST(id AS INT) = 1` (where `id` is
already `Int32`) also stays residual, because the binder
preserves the `ExecutionExpr::Cast(id, Int32)` wrapper
rather than collapsing to `id = 1`.

### D6 — Cast inside aggregate arguments: continues to reject

ADR 024 D6's aggregate-argument shape guard is preserved:

```sql
SUM(CAST(amount AS BIGINT))   -- rejects
COUNT(CAST(id AS BIGINT))     -- rejects
AVG(CAST(price AS DOUBLE))    -- rejects
```

Diagnostic (unchanged):

```text
aggregate arguments must be simple column references
```

A `CAST(...)` is a computed expression for aggregate-
argument purposes even when it wraps a bare column.
Relaxation belongs to the computed-aggregate-argument ADR
(sibling ADR with derive-Eq/Hash refactor on
`AggregateFunction` and `Expr` plus
`GroupedAggregateSignature` widening).

Post-aggregate CAST IS allowed (composes with ADR 023
post-aggregate projection):

```sql
SELECT CAST(SUM(amount) AS DOUBLE) FROM sales            -- OK
SELECT region, CAST(SUM(amount) AS DOUBLE)               -- OK
FROM sales GROUP BY region
SELECT region FROM sales GROUP BY region                 -- OK
  HAVING CAST(SUM(amount) AS Int32) > 100
SELECT region, SUM(amount) FROM sales GROUP BY region    -- OK
  ORDER BY CAST(SUM(amount) AS Float32) DESC
```

### D7 — Cast nullability: passthrough

`Cast.nullable() = inner.nullable()`. Cast either succeeds
(producing the converted value, retaining the input null
mask) or returns Kernel error. No NULL production from
Cast in ADR 025; null input → null output via mask
propagation (the kernel is not invoked on null slots).

`TRY_CAST` will be the nullable-on-failure variant in a
follow-on ADR.

### D8 — Binder policy split: `bind_explicit_cast` vs `cast_if_needed` — NO identity-cast elision

The binder maintains two clearly named helpers with
disjoint policy lattices:

| Helper | Policy | Invoked by |
|---|---|---|
| `cast_if_needed(expr, target)` | widening-only (4 ADR 024 cells) | `bind_arithmetic` for operand promotion |
| `bind_explicit_cast(inner, target)` | full 16-cell numeric lattice; **no identity-cast elision** | binder's `Expr::Cast` dispatch arm |
| `is_widening_cast(source, target)` | matches `cast_if_needed`'s 4-cell lattice | helper used by `cast_if_needed` |
| `is_supported_explicit_numeric_cast(source, target)` | matches `bind_explicit_cast`'s 16-cell lattice | helper used by `bind_explicit_cast` |

**The no-identity-elision rule is load-bearing.**
`bind_explicit_cast` always returns
`ExecutionExpr::Cast(Box::new(bound_inner), target)`,
including for `source == target`. Eliding identity at bind
time would let user-written CAST predicates lose their
`ExecutionExpr::Cast` marker — `WHERE CAST(id AS INT) = 1`
would bind to `id = 1`, indistinguishable from a no-CAST
predicate, and `PbcCompiler` could push it down to
Wendelon, violating the contract that user-visible
`CAST(...)` stays as a Toron residual.

The identity short-circuit lives in `evaluate_cast`:

```rust
if inner.data_type() == target {
    return Ok(inner.clone());  // ref-count bump, no element copy
}
```

`adr025_explicit_cast_policy_separate` architecture guard
source-reads both helper bodies and includes negative
assertions:

```rust
let bind_explicit = extract_fn_body(&src, "bind_explicit_cast");
assert!(
    !bind_explicit.contains("if source == *target"),
    "bind_explicit_cast must not elide identity Cast — preserves PBC boundary",
);
```

### D9 — Float-to-int boundary rule: exclusive upper bound

Float-to-int range checks use exclusive upper bounds:

```rust
const F64_I32_LOWER: f64 = i32::MIN as f64;        // -2147483648.0
const F64_I32_UPPER: f64 = 2147483648.0;           // i32::MAX + 1, exact in f64
const F64_I64_LOWER: f64 = i64::MIN as f64;        // -9.22e18
const F64_I64_UPPER: f64 = 9223372036854775808.0;  // i64::MAX + 1, exact in f64

// kernel: cast_f64_to_i32
let trunc = value.trunc();
if !(F64_I32_LOWER <= trunc && trunc < F64_I32_UPPER) {
    return Err(cast_failed(...));
}
```

`i32::MAX as f32 = 2147483648.0` (one above `i32::MAX`
because f32 cannot represent `i32::MAX = 2147483647`
exactly). Similarly `i64::MAX as f64 = 9223372036854775808.0`
(one above `i64::MAX`). An inclusive upper bound
(`trunc <= i32::MAX as f32`) would miss the boundary
value and violate ADR 025's checked-cast contract.

For `Float32 → Int32` and `Float32 → Int64`, the kernel
converts the source value to f64 before the range check
to avoid f32 rounding traps:

```rust
let trunc_f64 = (value as f64).trunc();
if !(F64_I32_LOWER <= trunc_f64 && trunc_f64 < F64_I32_UPPER) { /* error */ }
```

`adr025_float_to_int_uses_exclusive_upper_bound` guard
source-reads the comparator shape across the four
float-to-int kernels.

### D10 — Narrow hidden sort-key projection for `ORDER BY CAST(<selected aggregate>)`

ADR 025 allows post-aggregate CAST in `ORDER BY` when the
wrapped aggregate call is already selected:

```sql
SELECT region, SUM(amount)
FROM sales
GROUP BY region
ORDER BY CAST(SUM(amount) AS Float32) DESC
```

Toron's existing `SortExpr` binding contract sorts only by
output columns or positive ordinals. To keep that contract
unchanged, ADR 025 uses a narrow planner rewrite when an
aggregate query's `ORDER BY` key contains `Cast` after
aggregate-call rewriting:

1. Insert a Project below Sort that preserves the visible
   output columns and appends hidden computed sort keys.
2. Rewrite the Sort keys to reference the hidden projected
   columns by generated names (`__toron_order_key_{idx}`).
3. Insert a final Project above Sort that drops the hidden
   columns and restores the visible output names.

This is deliberately **not** the general hidden-aggregate-
keys ADR. The trigger is narrow (`expr_contains_cast`) so
existing computed ORDER BY behavior, including arithmetic-
only expressions, remains byte-for-byte governed by the
pre-ADR-025 path. Broader support for arbitrary hidden
ORDER BY expressions (`ORDER BY SUM(x) + 1`,
`ORDER BY -SUM(x)`, alias visibility, duplicate visible
name policy, and generated-name stability beyond this
local path) remains in the future ORDER BY hidden aggregate
keys ADR.

The rewrite rejects duplicate visible output names with a
loud unsupported diagnostic rather than guessing which
visible column to restore. That keeps the helper local and
reviewable until the dedicated plan-shape ADR decides the
general policy.

## Reject removal (ADR 015 cycle's seventh instance)

ADR 015 cycle's seventh reject-removal instance (after
016 / 017 / 018 / 019 / 022 / 023 / 024) at the SQL CAST
surface layer:

```text
SqlExpr::Cast → unsupported-expression catch-all in lower_expr
```

ADR 024's `cast_is_binder_only_widening` guard source-read
asserted no `SqlExpr::Cast` lowering site existed. ADR 025
inverts: a `SqlExpr::Cast { kind: CastKind::Cast, .. }`
lowering arm MUST exist with `lower_cast_target_type`
normalization.

The cycle's "narrow detector before generic guard"
property holds: each new reject diagnostic source-reads
its specific arm (D4 syntax rejects, D3 target-type
rejects), not a wide-blast assertion.

## Architecture guard adjustments

| Guard | Status |
|---|---|
| `adr024_scalar_arithmetic_guards_are_narrow` | **Adjusted.** Drop "no `SqlExpr::Cast` lowering site" sub-assertion. Keep arithmetic shape, error variant, etc. |
| `adr024_cast_is_binder_only_widening` | **Renamed → `adr025_binder_arithmetic_cast_remains_widening_only`.** Drops SQL-lowering-absence sub-assertion; keeps `cast_if_needed` / `is_widening_cast` 4-cell lattice assertion. |
| `cast_pbc_eligible_returns_false` | Preserved (ADR 024). |
| `arithmetic_promotion_lattice_is_narrow` | Preserved (ADR 024). |
| `aggregate_arguments_remain_column_only` | Preserved (ADR 024). |
| `adr025_sql_cast_lowers_via_planner` | NEW. Source-reads `lower_expr` for `SqlExpr::Cast { kind: CastKind::Cast, .. }`. |
| `adr025_sql_cast_target_type_normalization` | NEW. Source-reads `lower_cast_target_type` for the 4 primitive mappings + reject arms. |
| `adr025_explicit_cast_policy_separate` | NEW. Source-reads both binder helpers; asserts policy separation **and absence of identity-cast elision** (D8). |
| `adr025_cast_supports_numeric_primitive_lattice` | NEW. Source-reads `evaluate_cast` for the 16-arm dispatch. |
| `adr025_cast_failure_returns_kernel_error_with_prefix` | NEW. Source-reads `cast_failed` for `"cast failed: "` prefix + `ExecutionError::Kernel`. |
| `adr025_try_cast_safe_cast_doublecolon_reject` | NEW. Source-reads `lower_expr` for `CastKind::TryCast/SafeCast/DoubleColon` reject arms. |
| `adr025_float_to_int_uses_exclusive_upper_bound` | NEW. Source-reads `cast_*_to_i*` kernels for exclusive-upper-bound comparators (D9). |
| `adr025_order_by_cast_uses_hidden_sort_projection` | NEW. Source-reads the D10 hidden Project → Sort → Project helper path and narrow `expr_contains_cast` gate. |

Total: 2 adjusted (1 renamed), 8 new guards.

## SoA-readiness check

ADR 021's four invariants preserved:

1. Catalog stays pure metadata (no catalog touch).
2. `StableAggregateKernel` `#[repr(u8)]` unchanged.
3. Centralized dispatch seams: per-(source, target) Cast
   path dispatch is a localized `match` in `evaluate_cast`.
4. No row-path dyn dispatch: per-pair kernels are explicit
   functions.

The 16 per-(source, target) Cast paths follow the same JIT
specialization shape as ADR 024's 16 arithmetic kernels.
Manual per-pair kernels (vs. `arrow_cast::cast_with_options`
delegation) preserve the SoA layout specialization seam
explicitly in source.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT):** 16 per-(source,
  target) Cast paths are the canonical JIT specialization
  shape for type conversion. Positive alignment, same as
  ADR 024.
- **Brainstorm 002 (WeakControlRing):** Untouched.
- **Brainstorm 003 (Disaggregated Compute):** Fragmenter
  unchanged. Cast executes wherever the containing
  expression executes.
- **Brainstorm 004 (Memory Arenas):** No new IR variant;
  existing `Cast(Box<Expr>, DataType)` shape is arena-
  compatible.

No seam closes; no seam touches beyond ADR 024's existing
surface.

## What Not To Do

- Don't add `TRY_CAST` / `SAFE_CAST` / `::` / `FORMAT` in
  ADR 025 (D4).
- Don't widen scope to Utf8 ↔ numeric or Utf8 ↔ Boolean.
- Don't widen the numeric domain beyond `Int32 | Int64 |
  Float32 | Float64`.
- Don't change Cast's PBC-ineligibility.
- Don't loosen the aggregate-argument shape guard.
- Don't change Cast nullability to "nullable on overflow."
- Don't add a new `ExecutionError` variant.
- Don't add saturating Cast semantics.
- Don't add float-rounding-style CAST (`CAST(3.7 AS Int32)
  = 3`, not `4`).
- Don't accept bare `FLOAT` / `FLOAT(n)`.
- Don't merge `cast_if_needed` and `bind_explicit_cast`.
- Don't use `arrow_cast::cast_with_options` for the
  primary execution path.
- **Don't elide identity Cast in `bind_explicit_cast`**
  (D8). Identity short-circuit lives in `evaluate_cast`
  only.
- **Don't use inclusive upper bound for float-to-int
  range check** (D9).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Plan digest regression for no-CAST queries | Additive surface; no-CAST corpus digests byte-identical pre/post. |
| R2 | ADR 024 `cast_is_binder_only_widening` guard breaks | Renamed to `adr025_binder_arithmetic_cast_remains_widening_only`; SQL-lowering-absence sub-assertion dropped. |
| R3 | Float → Int overflow check costs per-row branch | Acceptable; future SoA / SIMD vectorizes. |
| R4 | NaN / Inf → Int error halts query with bad data | Documented; `TRY_CAST` follow-on provides null-on-failure. |
| R5 | SQL type name normalization misses an alias | Fixture enumerates supported spellings; new aliases extend additively. |
| R6 | Aggregate-argument guard over-rejects | Guard preserves column-only rule; CAST is computed expression. |
| R7 | Cast IR variant proliferation when future ADRs add Decimal / Date CAST | Single `Cast(Box<Expr>, DataType)` accommodates any target type. |
| R8 | `is_widening_cast` accidentally widened by ADR 025 patch | `adr025_explicit_cast_policy_separate` uses positive + negative source-reads. |
| R9 | PBC compiler arms need updating | No update needed; `pbc_eligible() = false` for all Cast variants. |
| R10 | `exec.rs` LOC inflation | Co-locate in `exec/cast.rs` if needed; doesn't change architecture guard structure. |
| R11 | Float64 → Float32 finite-overflow check non-obvious | Comment kernel + positive (NaN, Inf preserved) and negative (`1e40 → Float32`) boundary fixtures. |
| R12 | Identity Cast PBC-pushdown leak | D8 preserves `ExecutionExpr::Cast`; `pbc_declines_identity_cast_filter` integration test exercises end-to-end. |
| R13 | Float-to-int off-by-one at `i32::MAX as f32 = 2147483648.0` | D9 exclusive upper bound; boundary fixtures pin behavior. |
| R14 | `sqlparser` `CastKind::DoubleColon` variant availability | Verify pinned version; catchall arm handles absence with same diagnostic format. |
| R15 | Walker recursion misses a `SqlExpr::Cast` site | Phase 1 enumerates every walker; HAVING / ORDER BY / WHERE CAST integration tests exercise paths. |
| R16 | D10 hidden sort-key helper becomes accidental general ORDER BY policy | Guard pins narrow `expr_contains_cast` trigger; broader hidden-key decisions remain future ADR work. |

## Process observations

**Two corrections ratified at iter-2** that the council
caught at design time rather than after implementation:

1. **No identity-cast elision in `bind_explicit_cast`**
   (D8). Codex iter-2 identified that eliding
   `CAST(int_col AS Int32)` at bind time would let user-
   written CAST predicates slip into PBC pushdown,
   violating the Toron-residual contract for user-visible
   CAST. Claude iter-1 had elision in `bind_explicit_cast`;
   Gemini iter-2 also had elision. Both yielded to the
   single correctness argument; ratified 3/3 against
   elision in iter-2.
2. **Exclusive upper bound for float-to-int range check**
   (D9). Codex iter-2 identified that `i32::MAX as f32 =
   2147483648.0` (one above MAX due to f32 rounding); an
   inclusive `trunc <= i32::MAX as f32` check would miss
   the boundary value. The exclusive `trunc <
   F64_I32_UPPER` form (with `UPPER` = `i32::MAX + 1`
   exact) rules out the off-by-one. Iter-1 left this as an
   open question; iter-2 resolved it concretely.

**Convergence cadence.** ADR 025 converged in 2 iterations
versus ADR 023's 4+ and ADR 024's 4+ with multi-direction
flip-flops on Cast IR scope. **Hypothesis:** substrate-
downstream ADRs (reuse existing IR substrate, no new
variant) converge faster than substrate-introducing ADRs
(add an IR variant or reshape one). ADR 025 inherits the
`Cast(Box<Expr>, DataType)` IR shape from ADR 024
unchanged; council decisions are entirely at the SQL
surface + binder policy + executor kernel layer, which has
narrower decision space than IR variant shape.

For future cycles: when the ADR is substrate-downstream,
2-iteration cycles with one correctness-correction round
are realistic. When substrate-introducing, expect 4+
iterations with at least one shape/scope flip-flop.

**One scope reversal recorded.** Gemini's iter-0 / iter-1
position included Utf8 ↔ numeric and Utf8 ↔ Boolean
conversions implemented via `arrow_cast::cast_with_options
{ safe: false }`. Gemini iter-2 fully retracted both
positions in favor of the 2-of-3 majority (numeric-only,
manual kernels) shape. The retraction was clean — Gemini
documented the reversals explicitly rather than carrying
silent dissent.

## Future work

ADR 026+ candidates (ordered by composes-with-substrate
gravity):

- **`TRY_CAST(x AS T)`** — error-tolerant variant. Reuses
  ADR 025 lattice with `nullable_on_failure = true`.
  Lowest-friction substrate-downstream extension after
  ADR 025.
- **Computed aggregate arguments** (`SUM(x + y)`,
  `SUM(CAST(amount AS Int64))`). Requires derive-Eq/Hash
  refactor on `AggregateFunction` (blocked by
  `ApproxQuantile(f64)`) and `Expr` (blocked by
  `ScalarValue::Float32/Float64`) plus
  `GroupedAggregateSignature` widening to
  `(AggregateFunction, Vec<Expr>)`. Composes naturally
  with ADR 024 + 025 once the signature substrate exists.
- **Utf8 ↔ numeric CAST.** Parsing semantics ADR — locale,
  whitespace handling, scientific notation, what counts
  as a parse failure.
- **Boolean ↔ numeric CAST.** Truth-table semantics ADR
  (dialect-specific spellings: `'t'` / `'1'` / `'yes'`).
- **Decimal CAST.** Composes with V1.x Decimal numeric-
  types ADR.
- **Date / Timestamp CAST.** Temporal arithmetic /
  conversion ADR.
- **Wendelon PBC Cast opcode extension.** Cross-ring ADR;
  promotes on workload evidence.
- **PostgreSQL `expr::type` shorthand.** SQL-compat ADR.
- **`SMALLINT` / `TINYINT` → Int8 / Int16 numeric-type
  expansion.** Composes with V1.x numeric-types ADR.
- **Unary negation (`-x`), modulo (`%`), `CASE`,
  `COALESCE` / `IFNULL` / `NULLIF`.** Carried from ADR 024
  future-work.
- **HAVING alias visibility, general ORDER BY hidden
  aggregate keys.** Carried from ADR 023. ADR 025 D10 only
  handles the narrow CAST-bearing selected-aggregate path.
- **Utf8 / Decimal MIN/MAX, Kahan-Neumaier stability,
  `DISTINCT`.** Carried from ADR 022 / 020 future-work.
- **Cross-ring AGG consumer wiring (ADR 02X).** Carried.
- **Kernel graduation** (`LinearRegress` /
  `ApproxQuantile`). Carried.
