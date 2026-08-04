# ADR 022: Float MIN / MAX Aggregate Kernels

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Companion implementation plan:**
`plans/impl/0022_float_min_max_aggregate_kernels.md`

## Status

Promoted.

First downstream user of the ADR 021 aggregate-
kernel-catalog spine. Honors Gemini's recorded ADR
021 dissent ("Float MIN/MAX as a sticky-NaN proof
case") while leaving Gemini's other ADR 021 dissent
(trait-dispatch) on the recorded shelf per the
SoA-readiness argument.

Three-way council convergence reached across four
iterations:

- **Sticky-NaN policy.** Claude + Codex + Gemini
  iter-0. Any NaN input produces NaN output for both
  MIN and MAX. Matches ADR 020 AVG precedent; avoids
  the incoherent query surface where `AVG(x)` returns
  NaN but `MIN(x)` silently skips the same value.
- **Deterministic signed-zero policy.** Claude +
  Codex + Gemini iter-0. `-0.0 < +0.0` as a total
  order via `f32::total_cmp` / `f64::total_cmp`.
  Reuses the ADR 010 float total-order precedent from
  `Sort`; keeps plan digests from depending on
  helper NaN / tie behavior outside this ADR's
  policy.
- **Widen-don't-add input-domain shape.** Claude +
  Codex + Gemini iter-0. Rename
  `AggregateInputDomain::IntMinMax` →
  `OrderedMinMax`; widen acceptance to
  `Int32 | Int64 | Float32 | Float64`. No new
  `StableAggregateKernel` row; no new
  `AggregateOutputRule` variant (`SameAsInput` covers
  Float identity output); catalog row count stays
  six.
- **Enumerated error strings.** Codex iter-1 dissent
  against the shorter `"numeric"` wording; ratified
  iter-2. MIN / MAX still rejects unsigned integer
  widths (which SUM / AVG accept today) and other
  out-of-scope domains such as Decimal, Date /
  Timestamp, Boolean, and Utf8. The enumerated
  form is more brittle (a future Decimal MIN / MAX
  ADR must update the strings) but that is the right
  kind of brittleness: it forces visible contract
  updates when the domain widens.
- **Narrow Float32 literal coercion companion.**
  Codex iter-0 surfaced as in-scope adjacent gap;
  Claude + Gemini ratified iter-1. Without it,
  `HAVING MIN(f32_col) > 0` fails to bind because
  `coerce_literal_value` lacks Float32 target arms.
  Four arms added: `Int32 → Float32`, `Int64 →
  Float32`, `Int64 → Float64`, `Float64 → Float32`.
- **NaN payload non-contractual.** Codex iter-0
  raised; ratified iter-1. Tests assert `is_nan()`
  predicate equality, not bit identity. NaN bit
  pattern is implementation-defined per IEEE 754.
- **`AggState: Copy` preserved; 9 → 13 variants.**
  ADR 021 invariant honored. `Option<f32>` and
  `Option<f64>` are `Copy`.
- **No row-path trait dispatch.** ADR 021's catalog-
  only mechanism stands; row-update bodies remain
  explicit; SoA-readiness preserved.
- **No cross-ring engagement.** Wendelon AGG protocol
  exposes type-agnostic `Min { column }` / `Max {
  column }` function tags with no Float NaN policy
  pinned at the wire layer; ADR 02X (cross-ring AGG
  consumer wiring) inherits divergence-check work.
- **Utf8 MIN / MAX deferred.** Different decision
  matrix (collation, locale, `AggState: Copy`
  removal). Future ADR.

Load-bearing antecedents: ADR 010 (float sort-key
total order), ADR 015 (cycle's source-level boundary
clarity property), ADR 019 (first MIN / MAX kernel
ADR; integer-only `IntMinMax` domain), ADR 020
(sticky-NaN precedent for AVG; cross-kernel policy
consistency), ADR 021 (private catalog spine,
`AggregateInputDomain` enum, `AggregateOutputRule`
enum, `AggState: Copy` invariant, executor-boundary
discipline).

## Goal

Widen `MIN` and `MAX` to accept `Float32` and
`Float64` inputs under a deterministic sticky-NaN
total-order policy. Land the first downstream user
of the ADR 021 aggregate-kernel-catalog spine
without new IR variants, new `StableAggregateKernel`
rows, new `AggregateOutputRule` variants, fragmenter
changes, or cross-ring engagement.

Concretely:

- `MIN(Float32 | Float64)` and `MAX(Float32 |
  Float64)` bind successfully through the standard
  catalog path; output type is identity
  (`SameAsInput`).
- Per-row accumulation propagates NaN (sticky); ties
  with signed zero resolve as `-0.0 < +0.0`
  deterministically.
- `HAVING MIN(f32_col) > 0` and `HAVING MIN(f32_col)
  > 0.0` both bind through a narrow literal-coercion
  companion in `binder.rs`.
- Binder rejects unsigned integers, Decimal,
  Float16, Date / Timestamp, Boolean, and Utf8 with
  an enumerated error string naming the accepted
  set.
- All existing integer MIN / MAX behavior remains
  byte-for-byte unchanged.
- ADR 019 and ADR 021 architecture guards adjust to
  read through the renamed `OrderedMinMax` domain;
  ADR 021's `AggState: Copy` invariant is preserved.

ADR 022 instances ADR 015's cycle property at the
**input-domain layer**: every Float32 / Float64 MIN
/ MAX call either lowers to a deterministic
sticky-NaN total-order accumulator or rejects via
the enumerated `"expects Int32, Int64, Float32, or
Float64"` error string. Architecture guard machine-
checks the enumerated set.

## Scale boundary

**Primary:** Toron operator-level kernels. **Local
aggregate-kernel widening with one binder coercion
companion.**

In scope:

- `aggregate_kernel.rs`: rename
  `AggregateInputDomain::IntMinMax` →
  `OrderedMinMax`; widen
  `is_int_min_max_input` → `is_ordered_min_max_input`
  body to admit `Int32 | Int64 | Float32 | Float64`;
  update `STABLE_AGGREGATE_KERNELS` MIN and MAX rows
  to reference the renamed variant.
- `exec.rs`: four new `AggState` variants
  (`MinFloat32`, `MinFloat64`, `MaxFloat32`,
  `MaxFloat64`); `AggState: Copy` preserved.
- `exec.rs`: Float arms in
  `min_state_for_data_type`,
  `max_state_for_data_type`, `update_min_state`,
  `update_max_state`, `min_max_state_array`, and
  `min_max_column_from_groups`. Two new helper-
  function pairs (`f64_min_sticky_total` /
  `f64_max_sticky_total`; mirror f32) implementing
  sticky-NaN + signed-zero via `total_cmp`.
- `exec.rs`: state-match exhaustiveness sweep across
  scalar dispatch (`exec.rs:1100-1116`,
  `exec.rs:1142-1149`) and grouped dispatch
  (`exec.rs:1255-1297`); no `_ =>` wildcards.
- `exec.rs`: cfg(test) legacy unbound grouped
  aggregate path becomes type-aware for MIN / MAX by
  initializing `init_states` from observed aggregate
  array types and by tracking per-aggregate output
  types through `legacy_aggregate_output_type`.
  This keeps the legacy direct-executor path aligned
  with `SameAsInput` for Float inputs.
- `binder.rs`: four new arms in `coerce_literal_value`
  (`Int32 → Float32`, `Int64 → Float32`,
  `Int64 → Float64`, `Float64 → Float32`); enumerated
  MIN / MAX type-mismatch error strings.
- `tests/architecture_test.rs`: ADR 019 guard
  adjustment for `OrderedMinMax`; ADR 021 guard
  preserved-string list update for new MIN / MAX
  error wording; `AggState` variant-count assertion
  bumps from 9 to 13.
- Regression coverage in `tests/group_by_test.rs`
  covering sticky-NaN, signed-zero, empty / all-NULL,
  Float32 HAVING coercion, and Float32 / Float64
  identity outputs. The coverage is consolidated
  there to reuse the shared sales-table fixture.
- `tests/distributed_process_test.rs`: one grouped
  Float64 MIN / MAX fixture across 2-worker and
  4-worker configurations (finite values only).
- `benches/storage_execution.rs`: rows
  `min_float64`, `max_float64`, `min_nullable_f32`,
  `group_float_min_max`; no minimum-speedup gate.
- Optional performance-report artifact may be
  captured later from the storage benchmark rows;
  the ADR 022 implementation gate is the bench
  target compiling with those rows.

Explicitly unchanged:

- **`AggregateFunction` IR enum.** No new variant;
  the existing `Min` / `Max` variants are
  polymorphic over input type via the catalog
  rejector.
- **`AggregateOutputRule` enum.** `SameAsInput`
  already covers Float identity output.
- **`StableAggregateKernel` enum.** Six rows
  preserved.
- **`STABLE_AGGREGATE_KERNELS` row count.** Six.
- **`AggState: Copy`.** Preserved (`Option<f32>` and
  `Option<f64>` are Copy).
- **Row-update bodies for integer MIN / MAX.**
  Byte-identical.
- **AVG / SUM / COUNT kernel surfaces.** Unchanged.
- **Plan digests for integer-only MIN / MAX queries.**
  Byte-identical (the `OrderedMinMax` rename is
  internal; no observable name change).
- **`AggregateFunction` planner SQL-name recognition.**
  Unchanged.
- **Fragmenter recognition, shuffle binding, stage
  roles, runtime-filter descriptors, PBC bytecode,
  wire surfaces.** All unchanged.
- **Wendelon AGG protocol consumer surface.** ADR
  02X territory; no engagement here.
- **Experimental kernel gates.** `LinearRegress`,
  `ApproxQuantile(f64)` unchanged.

No new IR variant. No new fragmenter recognition
site. No SQL surface widening beyond input-type
acceptance. No cross-ring engagement. No `AggState:
Copy` removal. No row-path dyn dispatch. No general
expression-coercion ADR.

## Sticky-NaN total-order contract

**Policy.** Let `S` be the multiset of non-NULL input
values for a MIN or MAX aggregate over a column. NULL
inputs are skipped (existing behavior). For `S =
∅`, output is NULL (existing behavior). Otherwise:

- If any `x ∈ S` satisfies `x.is_nan()`, the output
  is `NaN` (NaN payload non-contractual).
- Otherwise, the output is the `total_cmp`-minimum
  (for MIN) or `total_cmp`-maximum (for MAX) of `S`.
  `total_cmp` is the IEEE 754 totalOrder predicate
  as implemented by `f32::total_cmp` /
  `f64::total_cmp` in Rust std; in particular it
  orders `-0.0 < +0.0` strictly.

**Composition with NULL.** NULL skip is independent
of NaN stickiness. A column containing
`[1.0, NULL, NaN, 2.0]` evaluates as MIN(`{1.0,
NaN, 2.0}`) → NaN (sticky); MAX(`{1.0, NaN, 2.0}`)
→ NaN (sticky). A column of all NULLs evaluates to
NULL. A column of all NaNs evaluates to NaN.

**Cross-kernel consistency.** Sticky-NaN applies to
MIN, MAX, and AVG (ADR 020). SUM is also sticky for
Float inputs (naive `f64` arithmetic propagates NaN
through `+=`). COUNT is NaN-agnostic. The same query
computing `AVG(x), MIN(x), MAX(x)` over a NaN-
containing column returns `(NaN, NaN, NaN)` — no
kernel-internal inconsistency.

**Why not skip-NaN.** Three counter-arguments
rejected:

1. *DuckDB / Postgres precedent skip NaN.* Counter:
   ADR 020 AVG already pinned sticky-NaN; a skip-NaN
   MIN / MAX inside the same kernel family creates
   intra-query incoherence.
2. *Skip-NaN closer to ANSI SQL.* Counter: ANSI SQL
   does not define NaN handling for arithmetic types;
   the choice is implementation-defined.
3. *NaN is a data-quality signal users want
   silenced.* Counter: silencing hides bad data;
   sticky-NaN surfaces it deterministically. Users
   who want skip-NaN can filter `WHERE NOT
   is_nan(col)` explicitly.

## Catalog specification

Six `STABLE_AGGREGATE_KERNELS` rows preserved. ADR
022 modifies two:

| `kind` | `sql_name` | `output_name` | `arity` | `input_domain` | `output_rule` | `nullable` | `grouped_supported` |
|--------|------------|---------------|---------|----------------|---------------|------------|---------------------|
| `Min`  | `MIN`      | `min`         | `One`   | `OrderedMinMax`| `SameAsInput` | `true`     | `true`              |
| `Max`  | `MAX`      | `max`         | `One`   | `OrderedMinMax`| `SameAsInput` | `true`     | `true`              |

Other four rows (`CountAll`, `Count`, `Sum`, `Avg`)
byte-identical to ADR 021.

`AggregateInputDomain` enum:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateInputDomain {
    Any,             // COUNT(expr)
    SumNumeric,      // SUM
    AvgNumeric,      // AVG
    OrderedMinMax,   // MIN, MAX  (was: IntMinMax in ADR 021)
}
```

Acceptance predicate:

```rust
fn is_ordered_min_max_input(input: &DataType) -> bool {
    matches!(
        input,
        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
    )
}
```

Pure-data discipline preserved (ADR 021 §Catalog
shape): no function pointers, no `&dyn`, no `Box`.
The variant rename is mechanical.

## Executor semantics

`AggState` widens from 9 → 13 variants (`AggState:
Copy` preserved):

```rust
#[derive(Debug, Clone, Copy)]
enum AggState {
    Count(i64),
    SumInt { total: i128, seen: bool },
    SumFloat { total: f64, seen: bool },
    MinInt32 { value: Option<i32> },
    MinInt64 { value: Option<i64> },
    MinFloat32 { value: Option<f32> },   // NEW
    MinFloat64 { value: Option<f64> },   // NEW
    MaxInt32 { value: Option<i32> },
    MaxInt64 { value: Option<i64> },
    MaxFloat32 { value: Option<f32> },   // NEW
    MaxFloat64 { value: Option<f64> },   // NEW
    AvgInt { total: i128, count: u64 },
    AvgFloat { total: f64, count: u64 },
}
```

Two pairs of helper functions co-located with the
existing `update_min_state` / `update_max_state` in
`exec.rs`:

```rust
// total_cmp pins signed-zero ordering; the explicit NaN branch
// preserves ADR 022 sticky-NaN semantics.

#[inline]
fn f64_min_sticky_total(current: f64, candidate: f64) -> f64 {
    if current.is_nan() || candidate.is_nan() {
        return f64::NAN;
    }
    match current.total_cmp(&candidate) {
        std::cmp::Ordering::Greater => candidate,
        _ => current,
    }
}

#[inline]
fn f64_max_sticky_total(current: f64, candidate: f64) -> f64 {
    if current.is_nan() || candidate.is_nan() {
        return f64::NAN;
    }
    match current.total_cmp(&candidate) {
        std::cmp::Ordering::Less => candidate,
        _ => current,
    }
}
```

Symmetric `f32_min_sticky_total` /
`f32_max_sticky_total` via `f32::total_cmp`.

**Why `total_cmp` inside the helper, not `f64::min` /
`f64::max`:** `f64::min` / `f64::max` follow IEEE
754-2008 `minNum` / `maxNum` semantics — when one
argument is NaN, return the non-NaN argument. This
is the OPPOSITE of sticky-NaN; using these helpers
would silently mask the NaN policy. `total_cmp`
gives signed-zero ordering (`-0.0 < +0.0`) for free
without an explicit `is_sign_negative()` branch.
The two concerns (sticky-NaN, signed-zero) are
layered cleanly: NaN check first, then total-order
ordering. Each line maps to one IEEE 754 property.

**Why helpers rather than inline bodies:** keeps
update arms short; centralizes the sticky-NaN check
once per type; reduces risk of accidental
`f64::min` substitution because the helpers are
structurally distinct (return `f64`, take `(f64,
f64)`, named for policy not operation).

Row-update arms become four-line bodies:

```rust
AggState::MinFloat64 { value } => {
    let candidate = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ExecutionError::InvalidExpression {
            message: "MIN(Float64) array type mismatch".to_string(),
        })?
        .value(row);
    *value = Some(value.map_or(candidate, |current| {
        f64_min_sticky_total(current, candidate)
    }));
    Ok(())
}
```

First-non-null seeds the state with `candidate`
directly (NaN or finite). Subsequent values route
through the helper. `Option::map_or` handles the
first-non-null / subsequent-non-null distinction
without redundant `is_some` checks.

`min_state_for_data_type` and
`max_state_for_data_type` add `Float32` and
`Float64` arms producing the four new variants.
`min_max_state_array` (scalar finalize) and
`min_max_column_from_groups` (grouped finalize) add
matching Float arms.

State-match exhaustiveness sweep across scalar
dispatch (`exec.rs:1100-1116`,
`exec.rs:1142-1149`) and grouped dispatch
(`exec.rs:1255-1297`): every `AggState::MinInt32 |
AggState::MinInt64` pattern extends to include the
two new Float MIN variants; every `AggState::MaxInt32
| AggState::MaxInt64` pattern extends to include the
two new Float MAX variants. Wildcards (`_ => ...`)
forbidden — compiler-caught omissions.

## Binder coercion companion

ADR 022 widens MIN / MAX output type to Float32 /
Float64. Without companion coercion, `HAVING
MIN(f32_col) > 0` (where `0` is
`ScalarValue::Int32`) would fail to bind.
`coerce_literal_value` (`binder.rs:1296`) extends
with four arms:

```rust
fn coerce_literal_value(value: ScalarValue, target_type: &DataType) -> ScalarValue {
    match (value, target_type) {
        // Pre-existing (ADR < 022):
        (ScalarValue::Int32(v), DataType::Int64) => ScalarValue::Int64(v.map(i64::from)),
        (ScalarValue::Int32(v), DataType::Float64) => ScalarValue::Float64(v.map(f64::from)),
        // ADR 022 additions:
        (ScalarValue::Int32(v), DataType::Float32) => ScalarValue::Float32(v.map(|i| i as f32)),
        (ScalarValue::Int64(v), DataType::Float32) => ScalarValue::Float32(v.map(|i| i as f32)),
        (ScalarValue::Int64(v), DataType::Float64) => ScalarValue::Float64(v.map(|i| i as f64)),
        (ScalarValue::Float64(v), DataType::Float32) => ScalarValue::Float32(v.map(|f| f as f32)),
        (value, _) => value,
    }
}
```

**Arm justification:**

- `Int32 → Float32`: `HAVING MIN(f32_col) > 0`.
- `Int64 → Float32`: `HAVING MIN(f32_col) >
  1000000000`. Lossy at the precision boundary
  (Float32 mantissa is 24 bits); the cast follows
  Rust's `as` semantics, which saturate to ±∞ on
  overflow.
- `Int64 → Float64`: `HAVING MIN(f64_col) >
  10000000000`. Lossless for values up to 2^53.
- `Float64 → Float32`: `HAVING MIN(f32_col) > 0.0`
  (SQL `0.0` lowers to `ScalarValue::Float64` by
  default). Lossy at the precision boundary.

**Scope discipline.** This is NOT a general
expression-coercion ADR. The four new arms are the
minimum-cover set to keep the widened MIN / MAX SQL
surface reachable through standard HAVING / WHERE /
ORDER BY literal comparisons. Column-to-column
coercion, join-key widening, and broad numeric
promotion lattices remain separable future ADRs.

## Binder error strings

ADR 021 marked the load-bearing strings:

```
MIN expects Int32 or Int64 input, got {data_type:?}
MAX expects Int32 or Int64 input, got {data_type:?}
```

ADR 022 updates to enumerated form:

```
MIN expects Int32, Int64, Float32, or Float64 input, got {data_type:?}
MAX expects Int32, Int64, Float32, or Float64 input, got {data_type:?}
```

**Why enumerated over `"numeric"`** (Codex iter-1
position, ratified iter-2): MIN / MAX after ADR 022
still rejects unsigned integer widths
(`UInt8..UInt64`) even though SUM / AVG accept them
today. It also rejects other out-of-scope domains
such as Decimal128 / Decimal256, Float16,
Date / Timestamp, Boolean, and Utf8. A `"numeric"`
error string would overclaim acceptance at the exact
moment the binder is explaining a rejection. The
enumerated form is
more brittle (a future Decimal MIN / MAX ADR must
update the strings) but that is the right kind of
brittleness — it forces visible contract updates
when the domain widens.

Arity strings unchanged:

```
MIN requires one argument
MAX requires one argument
```

Executor-side strings (`"MIN does not support input
type {data_type:?}"`) preserved — their reachability
narrows after binder accepts Float32 / Float64, but
they remain reachable for genuinely unsupported
types that bypass the binder gate (impossible in
production paths; defense-in-depth).

## Architecture guard

`tests/architecture_test.rs` changes:

- **Adjust** `adr019_min_max_aggregate_kernels_guards_are_narrow`:
  - `MIN.input_domain` assertion: `IntMinMax` →
    `OrderedMinMax`.
  - `MAX.input_domain` assertion: same.
  - Add: `is_ordered_min_max_input` body source-read
    asserts admitted types (`Int32`, `Int64`,
    `Float32`, `Float64`) and a representative
    sample of rejected types (e.g. `UInt32`,
    `Decimal128`, `Date32`, `Boolean`, `Utf8`)
    remains absent.
  - Existing assertions for `output_rule
    == SameAsInput`, `nullable == true`,
    `grouped_supported == true` preserved.
- **Adjust** `aggregate_kernel_catalog_guards_are_narrow`:
  - Catalog row count remains exactly six.
  - Catalog source-read asserts `OrderedMinMax`
    present; `IntMinMax` not present.
  - Preserved-string list updates: remove
    `"MIN expects Int32 or Int64 input"` /
    `"MAX expects Int32 or Int64 input"`; add the
    enumerated wording.
  - `AggState` source-read asserts `derive(...Copy...)`
    still present; variant-count assertion bumps
    from 9 to 13.
- **No change** to
  `bind_expr_remains_aggregate_free` (ADR 021).
- **No new architecture guard.** Sticky-NaN and
  signed-zero behavior assertions live in
  `tests/group_by_test.rs` (semantic regression
  tests), not architecture tests.

## Cycle property at the input-domain layer

ADR 015 established the cycle's source-level boundary
clarity property; ADR 019 extended it to the kernel
surface; ADR 020 was the second kernel-surface
instance; ADR 021 generalized to the metadata layer.

ADR 022 instances the property at the **input-
domain layer**: every Float32 / Float64 MIN / MAX
call either lowers to a deterministic sticky-NaN
total-order accumulator or rejects via the
enumerated `"expects Int32, Int64, Float32, or
Float64"` error string. The architecture guard
machine-checks the enumerated set; tests at the
semantic boundary (`tests/group_by_test.rs`)
machine-check the NaN / signed-zero behavior.

## Migration strategy

Five phases, materialized in
`plans/impl/0022_float_min_max_aggregate_kernels.md`:

1. **Phase 1 — Catalog widening.** Rename
   `IntMinMax` → `OrderedMinMax`; widen acceptance
   predicate; update catalog rows.
2. **Phase 2 — Executor widening.** Add four
   `AggState` variants, two helper-function pairs,
   Float arms in all six executor-side MIN / MAX
   helpers; state-match exhaustiveness sweep.
3. **Phase 3 — Binder companion.** Enumerated error
   strings; four new `coerce_literal_value` arms.
4. **Phase 4 — Architecture guard adjustment.** ADR
   019 + ADR 021 guards adjust.
5. **Phase 5 — Verification + bench.** Expanded
   `group_by_test.rs`; new `distributed_process_test.rs`
   fixture; storage benchmark rows.

## Definition of done

- `crates/toron/src/aggregate_kernel.rs` has
  `AggregateInputDomain::OrderedMinMax` (was
  `IntMinMax`); `is_ordered_min_max_input` admits
  `Int32 | Int64 | Float32 | Float64`; catalog
  MIN / MAX rows reference the renamed variant;
  row count remains six.
- `crates/toron/src/exec.rs` has 13 `AggState`
  variants (was 9); `AggState: Copy` preserved;
  Float arms in all six executor-side helpers;
  state-match sweep complete; `f32`/`f64`
  `_sticky_total` helpers present.
- `crates/toron/src/binder.rs` has four new
  `coerce_literal_value` arms; MIN / MAX error
  strings enumerated.
- `tests/architecture_test.rs` ADR 019 + ADR 021
  guards updated; `AggState` variant count
  assertion = 13.
- `tests/group_by_test.rs` covers sticky-NaN scalar
  + grouped, signed-zero scalar, empty / all-NULL,
  Float32 HAVING coercion (both `> 0` and `> 0.0`),
  and Float32 + Float64 identity-output symmetry.
- `tests/distributed_process_test.rs` includes a
  Float64 grouped MIN / MAX fixture across 2 and
  4 workers using only finite values.
- `benches/storage_execution.rs` includes
  `min_float64`, `max_float64`,
  `min_nullable_f32`, and `group_float_min_max`
  rows.
- Plan digests for integer-only MIN / MAX queries
  byte-identical to pre-ADR-022.
- All existing tests green:
  `architecture_test.rs`, `group_by_test.rs`,
  `distributed_process_test.rs`,
  `aggregate_projection_accounting_test.rs`,
  binder / planner test suites.

## Recorded dissent

**None substantive.** Three-way council convergence
across four iteration rounds on every load-bearing
decision. One resolved divergence (error-string
wording: Claude / Gemini iter-1 `"numeric"` vs
Codex iter-1 enumerated) ratified in Codex's favor
with documented rationale.

The ADR 021 Gemini trait-dispatch dissent
(`AggregateKernel::accumulate(state, array, row)`)
is NOT re-raised by ADR 022; the SoA-readiness
argument from ADR 021 § SoA / SIMD forward
compatibility stands. Any future kernel needing
polymorphic accumulation should make the case
alongside SoA plan-layout evidence per ADR 021
§Future work § Trait re-introduction pressure.

## Future work

- **ADR 023 — Post-aggregate projection.**
  `GroupedAggregateScope` naming-asymmetry signal
  (17 references; 12 in scalar paths post-ADR-018).
  Renames the scope, lifts aliased / computed-
  expression / key-subset / reordered-keys surfaces.
  Independent of ADR 022's kernel-domain widening;
  promotes as the substrate-downstream lead.
- **Utf8 MIN / MAX.** First ADR that would change
  `AggState: Copy`. Bundleable with collation /
  locale decision matrix, or kept bytewise-only.
- **Decimal MIN / MAX.** Composes with the V1.x
  Decimal numeric-types ADR; preserves total-order
  property; `OrderedMinMax` body extends with two
  new arms (`Decimal128`, `Decimal256`).
- **Cross-ring AGG consumer wiring (ADR 02X).**
  ADR 02X must check and align Wendelon-side Float
  MIN / MAX semantics before engaging cross-ring
  aggregate pushdown for these kernels. Wendelon
  protocol layer is type-agnostic; the divergence-
  check responsibility falls on ADR 02X. If
  mismatch surfaces, ADR 02X reconciles (either
  align Wendelon executor or refuse Float MIN / MAX
  pushdown).
- **Float16 / general numeric coercion lattice.**
  Out of ADR 022 scope; promotes under a dedicated
  numeric-coercion ADR if evidence surfaces.
- **General SoA / SIMD plan layout.** Speculative
  (Brainstorm 004). ADR 021's SoA-readiness
  invariants (catalog as pure metadata, no row-path
  dyn dispatch) extend through ADR 022 unchanged.

## Brainstorm-seam touch check

ADR 022 touches none of the load-bearing
brainstorm seams:

- **Brainstorm 001 (Cranelift JIT):** the explicit
  branch shape in `f64_min_sticky_total` is JIT-
  friendly — visible NaN check, single `total_cmp`
  call, simple Ordering match. More JIT-friendly
  than `f64::min` (which compiles to `minsd` with
  IEEE-mandated semantics the sticky branch would
  have to override).
- **Brainstorm 002 (WeakControlRing):** no
  coordinator-state, registry, or lease touch.
- **Brainstorm 003 (disaggregated compute roles):**
  no fragmenter / router / NodeRoleSet touch.
- **Brainstorm 004 (memory arenas):** four new
  `AggState` variants stay ≤ 16 bytes each;
  arena-allocatable per the same shape as existing
  variants. Catalog spec stays pure-data.

No seam closes; no seam touches beyond ADR 021's
existing surface.
