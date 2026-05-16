# ADR 021: Aggregate Kernel Catalog Refactor

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Companion implementation plan:**
`plans/impl/021_aggregate_kernel_catalog_refactor.md`

## Status

Promoted + implemented.

Internal substrate ADR. Centralizes stable aggregate
metadata into a single private catalog after ADR 019 and
ADR 020 widened the local aggregate kernel surface to the
basic analytical set (`COUNT`/`SUM`/`MIN`/`MAX`/`AVG`).
Three-way council convergence reached across four
iterations:

- **Catalog-only mechanism; no runtime trait dispatch.**
  Codex + Claude 2-of-3 majority across iter-0 through
  iter-3. Stable aggregate metadata moves to one
  authoritative `AggregateKernelSpec` table; planner,
  binder, and executor-adjacent helpers delegate to it.
  Per-row accumulation, `AggState` representation, and
  per-kernel state-machine semantics stay explicit in
  `exec.rs`. Gemini iter-0/1/2/3 dissented in favor of a
  runtime `AggregateKernel` trait with row-path
  `accumulate(state, array, row)` dispatch; the dissent
  is recorded for future ADRs that could re-raise
  trait-dispatch with a different evidence base
  (e.g., a kernel needing polymorphic accumulation
  alongside SoA plan-layout evidence).
- **Refactor-alone scope; no Float MIN/MAX bundle.**
  Codex + Claude 2-of-3 majority. Float MIN/MAX (with
  sticky-NaN policy ratification) ships as ADR 022
  against the new spine. Gemini iter-0/1/2/3 advocated
  bundling Float MIN/MAX with the refactor as a
  "sticky-NaN proof case"; dissent recorded.
- **Pure-data spec, not plan payload.** Codex iter-3
  argument, ratified iter-4: `AggregateKernelSpec` stores
  only `Copy`-able metadata (no function pointers, no
  trait objects, no executor references). Output-type
  resolution is driven by an `AggregateOutputRule` enum
  (`Int64`, `Float64`, `SameAsInput`, `SumNumeric`)
  rather than fn pointers. Compact tagged shape composes
  cleanly with future Structure-of-Arrays plan layouts
  (Brainstorm 004 memory-arena seam — see §SoA / SIMD
  forward compatibility).

ADR 021 also pins three smaller decisions:

- **`StableAggregateKernel` compact tag.** Stable kernels
  carry a `#[repr(u8)]` Copy enum tag distinct from
  `AggregateFunction`. Two-step lookup:
  `AggregateFunction → Option<StableAggregateKernel> →
  &'static AggregateKernelSpec`. Experimental kernels
  (`LinearRegress`, `ApproxQuantile(_)`) return `None`
  from the first step — they stay outside the stable
  catalog's exhaustiveness contract. The compact tag
  matches the future SoA plan-payload shape
  (`aggregate_kernel_ids: [Sum, Avg, Count]` arrays).

- **Architecture guard renamed and broadened.** The ADR
  020 guard `aggregate_function_enum_guards_are_narrow`
  becomes `aggregate_kernel_catalog_guards_are_narrow`
  with source-read assertions over `aggregate_kernel.rs`,
  the catalog row exhaustiveness, the
  `mod aggregate_kernel;` (not `pub mod`) privacy
  invariant, and the `AggState: Copy` preservation. A
  companion `bind_expr_remains_aggregate_free` guard adds
  the ADR 015 cycle's reject-string-absence pattern to
  the aggregate surface.
- **Bench artifact: no minimum-speedup gate.** Refactor
  ADR; expectation is no regression, not improvement.
  Digest parity asserted; aggregate rows recorded; >10%
  per-row regression triggers investigation before
  merge.

Implementation landed in the ADR 021 pass: the private
`aggregate_kernel.rs` catalog is wired through planner,
binder, and executor-adjacent metadata helpers; the row
update/finalization state machines remain in `exec.rs`.
Bench artifact:
`plans/impl/021_aggregate_kernel_catalog_refactor_bench_results.md`.

Load-bearing antecedents: ADR 011 (`Aggregate` operator
surface, `AggState` enum, SUM/COUNT kernel substrate),
ADR 015 (cycle's source-level boundary clarity property —
extended to kernel surface by ADR 019, generalized here
to the metadata layer), ADR 016 (HAVING walker substrate),
ADR 018 (scalar-aggregate surface), ADR 019 (MIN/MAX
kernel-catalog widening, identity-type output, `Option<T>`
state pattern; first instance of the kernel-catalog
boundary-clarity property), ADR 020 (AVG kernel-catalog
widening, `(total, count)` multi-state accumulator,
sticky-NaN, `SumValue → NumericValue` rename; ADR
020 §V1 §9 records Gemini's bundling dissent and names
the kernel-registry refactor as ADR 021's lead).

## Goal

Centralize stable aggregate metadata into a single
private catalog. Remove the parallel match-arm drift
across `sql_planner.rs`, `binder.rs`, and `exec.rs` while
preserving every per-row state-machine, every error
string, every plan digest, and every executor behavior
byte-for-byte. The refactor is **observationally
invisible**: no SQL output changes, no plan digest
changes, no IR shape change, no Wendelon-side change.

Concretely:

- Six stable kernels (`COUNT(*)`, `COUNT(expr)`, `SUM`,
  `MIN`, `MAX`, `AVG`) share one metadata source.
- Planner SQL-name recognition, output-name lookup,
  binder arity/input-domain/output-type validation, and
  binder output-field construction all delegate to the
  catalog.
- Executor `init_bound_states` delegates through a single
  `init_bound_state_for_call` helper; row-update bodies
  and per-kernel state helpers stay byte-identical.
- Experimental kernels (`LinearRegress`,
  `ApproxQuantile(f64)`) remain explicitly outside the
  stable catalog; their SQL-name recognition routes
  through a small adjacent
  `experimental_aggregate_sql_name` helper. The existing
  binder-side `allow_experimental_kernels` gate remains
  the authority for execution.
- A new architecture guard asserts catalog exhaustiveness
  and module privacy; the ADR 015 reject-string-absence
  pattern extends to the aggregate surface.

The catalog establishes the substrate that ADR 022
(Float MIN/MAX with NaN policy), future kernel
graduations, and any later SoA plan-layout refactor
build on without rewriting ~8 dispatch sites.

## Scale boundary

**Primary:** Toron planner + binder + executor metadata.
**Internal aggregate-kernel-catalog substrate.**

In scope:

- New private module `crates/toron/src/aggregate_kernel.rs`
  containing `StableAggregateKernel`, `AggregateArity`,
  `AggregateInputDomain`, `AggregateOutputRule`,
  `AggregateKernelSpec`, the `STABLE_AGGREGATE_KERNELS`
  static slice (six rows), and helper API
  (`stable_kernel_for_function`, `stable_spec`,
  `stable_spec_for_function`,
  `stable_aggregate_sql_name`,
  `experimental_aggregate_sql_name`,
  `stable_output_data_type`, `stable_output_field`).
- `mod aggregate_kernel;` declaration in `lib.rs`
  (private; not `pub mod`).
- `sql_planner.rs` planner-metadata delegation
  (`aggregate_function_name`, `aggregate_output_name`).
- `binder.rs` binder-metadata delegation
  (`bind_aggregate_call` arity/input-domain/output-type
  paths, `aggregate_field`, `aggregate_function_keyword`).
  The three private helpers `sum_output_data_type`,
  `min_max_output_data_type`, and `avg_output_data_type`
  are removed (logic folded into `AggregateOutputRule`
  matching).
- `exec.rs` executor migration: introduce
  `init_bound_state_for_call`; `init_bound_states`
  delegates through it. Optional scalar/grouped
  finalization centralization
  (`finalize_scalar_for_call`,
  `finalize_grouped_for_call`) only if local and clearly
  readable as direct matches over `AggregateFunction` /
  `AggState`. No row-path trait dispatch under any
  circumstances.
- cfg(test) helpers `aggregate_state_keyword` and
  `aggregate_state_output_name`: delegate to catalog or
  remove.
- Architecture guard updates:
  `aggregate_function_enum_guards_are_narrow` replaced
  with `aggregate_kernel_catalog_guards_are_narrow`;
  `bind_expr_remains_aggregate_free` added. ADR 019 and
  ADR 020 guards adjusted to read through the catalog
  rather than asserting parallel planner/binder match
  arms.
- Bench artifact at
  `plans/impl/021_aggregate_kernel_catalog_refactor_bench_results.md`
  recording per-aggregate-row digest parity and timings.

Explicitly unchanged:

- **`AggState`.** Variants, derives, layout, `Copy`-ness
  — all preserved. `AggState` stays in `exec.rs`.
- **Row-update bodies.** `add_sum_value`,
  `add_avg_value`, `update_min_state`, `update_max_state`
  are byte-identical pre- and post-refactor.
- **Per-kernel state helpers.** `sum_state_for_data_type`,
  `min_state_for_data_type`, `max_state_for_data_type`,
  `avg_state_for_data_type` stay in `exec.rs` with
  unchanged bodies.
- **Per-kernel finalize helpers.** `sum_state_array`,
  `min_max_state_array`, `avg_state_array` stay in
  `exec.rs` with unchanged bodies; the centralized
  `finalize_scalar_for_call` (if introduced) is a thin
  match.
- **`NumericValue`.** The ADR 020 rename remains; no
  further reshaping.
- **Plan digests.** Byte-for-byte unchanged across the
  test suite.
- **SQL behavior.** No new aggregate functions, no new
  type domains, no new error paths, no acceptance-domain
  expansion or contraction.
- **Binder error strings (load-bearing).**
  Preserved byte-for-byte:
  `SUM requires one argument`,
  `SUM expects numeric input`,
  `MIN requires one argument`,
  `MIN expects Int32 or Int64 input`,
  `MAX requires one argument`,
  `MAX expects Int32 or Int64 input`,
  `AVG requires one argument`,
  `AVG expects numeric input`.
  Plus the planner's `unsupported aggregate function`
  catch-all.
- **`AggregateFunction` IR enum.** Variants unchanged
  (8 variants from ADR 020 stay).
- **`GroupedAggregateSignature` enum.** Stays explicit —
  aggregate-call rewrite behavior, not metadata.
- **Experimental kernel gates.** `LinearRegress`,
  `ApproxQuantile(f64)` remain bound-executor
  `NotImplemented`; `allow_experimental_kernels = false`
  default; grouped support rejected.
- **Wendelon AGG protocol consumer surface.** Separate
  future ADR (ADR 02X) — cross-ring engagement is out of
  scope.
- **Fragmenter recognition, shuffle binding, stage roles,
  failure kinds, wire surfaces, runtime-filter
  descriptors, PBC bytecode.** All unchanged.

No new IR variant. No new fragmenter recognition site.
No SQL surface change. No cross-ring engagement. No
`AggState: Copy` removal. No row-path dyn dispatch.

## Catalog shape

```rust
// crates/toron/src/aggregate_kernel.rs

use arrow::datatypes::{DataType, Field};
use crate::ir::{AggregateCall, AggregateFunction};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StableAggregateKernel {
    CountAll = 0,
    Count    = 1,
    Sum      = 2,
    Min      = 3,
    Max      = 4,
    Avg      = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateArity {
    Zero,        // COUNT(*)
    One,         // COUNT(expr), SUM, MIN, MAX, AVG
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateInputDomain {
    Any,             // COUNT(expr)
    SumNumeric,      // SUM
    AvgNumeric,      // AVG
    IntMinMax,       // MIN, MAX
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateOutputRule {
    Int64,           // COUNT(*), COUNT(expr)
    Float64,         // AVG
    SameAsInput,     // MIN, MAX
    SumNumeric,      // SUM (Int* → Int64; Float* → Float64)
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AggregateKernelSpec {
    pub(crate) kind: StableAggregateKernel,
    pub(crate) sql_name: &'static str,
    pub(crate) output_name: &'static str,
    pub(crate) arity: AggregateArity,
    pub(crate) input_domain: AggregateInputDomain,
    pub(crate) output_rule: AggregateOutputRule,
    pub(crate) nullable: bool,
    pub(crate) grouped_supported: bool,
}

pub(crate) static STABLE_AGGREGATE_KERNELS: &[AggregateKernelSpec] = &[
    // CountAll, Count, Sum, Min, Max, Avg in StableAggregateKernel order.
    // Six specs; impl plan §Phase 1 lists field values.
];
```

**Pure-data discipline.** The spec stores only `Copy`
metadata — no function pointers, no `&dyn`, no `Box`, no
references to executor types. Source-based architecture
guards can inspect every field; future plan
representations can carry the `StableAggregateKernel` tag
(one `u8`) as compact plan payload without dragging spec
references into plan nodes.

Helper API (private to the crate):

```rust
pub(crate) fn stable_kernel_for_function(
    function: &AggregateFunction,
) -> Option<StableAggregateKernel>;

pub(crate) fn stable_spec(kernel: StableAggregateKernel)
    -> &'static AggregateKernelSpec;

pub(crate) fn stable_spec_for_function(
    function: &AggregateFunction,
) -> Option<&'static AggregateKernelSpec>;

pub(crate) fn stable_aggregate_sql_name(upper: &str)
    -> Option<&'static str>;

pub(crate) fn experimental_aggregate_sql_name(upper: &str)
    -> Option<&'static str>;

pub(crate) fn stable_output_data_type(
    function: &AggregateFunction,
    input: Option<&DataType>,
) -> Option<DataType>;

pub(crate) fn stable_output_field(call: &AggregateCall)
    -> Option<Field>;
```

`stable_output_data_type` implements output resolution by
matching on `AggregateOutputRule`. The full body is in
the impl plan; the contract is:

- `Int64` → output `DataType::Int64` regardless of input.
- `Float64` → output `DataType::Float64` regardless of
  input.
- `SameAsInput` → clone the input type (None if no input
  provided).
- `SumNumeric` → match input: Int8/Int16/Int32/Int64
  and UInt8/UInt16/UInt32/UInt64 → Int64; Float32/Float64
  → Float64; otherwise None.

`AggregateInputDomain::SumNumeric` and
`AggregateInputDomain::AvgNumeric` reproduce the existing
binder helper domains byte-for-byte: signed and unsigned
integer widths listed above, plus Float32 and Float64.
ADR 021 does not widen or narrow the type domain.

## COUNT family shape

`COUNT(*)` and `COUNT(expr)` are **separate catalog
rows** sharing one SQL name (`COUNT`) and one output name
(`count`). They differ in arity (`Zero` vs `One`) and in
binding behavior. `CountAll` produces zero bound
`AggregateCall` args after binder binding, but the current
planner-lowered logical aggregate still carries its
existing placeholder `Expr::column("count")`; ADR 021
preserves that shape unless a separate digest-neutral
cleanup proves otherwise. `Count` binds one argument. The
architecture guard explicitly asserts that the SQL-name
set has five unique strings while the catalog has six rows.

Planner SQL-name recognition treats `COUNT` as one stable
token (both `CountAll` and `Count` answer
`stable_aggregate_sql_name("COUNT") = Some("COUNT")`).
Post-lowering, the specific `CountAll` vs `Count` row
resolves via `stable_spec_for_function`.

## Experimental kernels

`LinearRegress` and `ApproxQuantile(f64)` stay outside
the stable catalog:

- `stable_kernel_for_function(LinearRegress)` returns
  `None`.
- `stable_kernel_for_function(ApproxQuantile(_))` returns
  `None`.
- They do not count toward stable-catalog exhaustiveness.
- Grouped support remains rejected.
- Bound executor returns `NotImplemented`.
- `allow_experimental_kernels = false` default unchanged.

A small adjacent helper `experimental_aggregate_sql_name`
recognizes `"LINEAR_REGRESS"` and `"APPROX_QUANTILE"` —
purely for planner SQL-name routing. The helper does not
imply grouped support or execution support, and does not
participate in the catalog-exhaustiveness architecture
guard.

This shape avoids the ADR 020 iter-1 `AggregateFunctionKind`
proposal (data-stripped classification of all kernels):
since experimental kernels are outside the catalog, the
stable side doesn't need to data-strip
`ApproxQuantile(f64)`. `StableAggregateKernel` is
stable-only.

## Executor boundary

`AggState` remains private to `exec.rs` and remains
`Copy`. The catalog never references `AggState`.

ADR 021 introduces one new helper in `exec.rs`:

```rust
fn init_bound_state_for_call(call: &AggregateCall)
    -> Result<AggState, ExecutionError>
```

with a single match dispatching to the existing
`*_state_for_data_type` helpers. `init_bound_states`
maps over `init_bound_state_for_call`.

Optionally — implementer's choice if it improves
readability locally — `finalize_scalar_for_call` and
`finalize_grouped_for_call` centralize the scalar
(lines 968-1010) and grouped (lines 304-372) dispatch
matches. If introduced, they remain direct matches over
`AggregateFunction` and `AggState`. **No row-path trait
dispatch under any circumstance.** No `dyn`, no `Box`,
no function pointers, no ZST trait objects.

The following row-path and state-machine code stays
explicit in `exec.rs`, byte-identical pre- and
post-refactor:

- COUNT row counting.
- SUM `checked_add` overflow discipline.
- AVG `(total, count)` accumulation and sticky-NaN
  behavior.
- MIN/MAX integer comparison behavior.
- Scalar and grouped row loops.
- `add_sum_value`, `add_avg_value`, `update_min_state`,
  `update_max_state`.
- `sum_state_for_data_type`, `min_state_for_data_type`,
  `max_state_for_data_type`, `avg_state_for_data_type`.
- `sum_state_array`, `min_max_state_array`,
  `avg_state_array`.
- `NumericValue` and its extraction paths.

## SoA / SIMD forward compatibility

ADR 021 explicitly considered Structure-of-Arrays plan
layouts and SIMD-aligned aggregate hot loops as a
forward-compatibility concern (Brainstorm 004 memory-
arena seam). The catalog shape composes cleanly with
that direction; this section records the reasoning so
future ADRs can reference it.

**Catalog is metadata, not plan payload.** Future SoA
plan representations carry compact kernel-id tags and
column ordinals — not `&AggregateKernelSpec` pointers,
not function pointers, not trait objects:

```text
aggregate_kernel_ids: [Sum, Avg, Count]   // StableAggregateKernel as u8
input_ordinals:        [3,   4,   5]
output_ordinals:       [0,   1,   2]
flags:                 [...]
```

`#[repr(u8)] StableAggregateKernel` is exactly the shape
such arrays want to hold. The spec table is a side-table
indexed by kernel id; plan nodes reference kernel ids, not
specs.

**Centralized dispatch points are SoA rewrite seams.**
`init_bound_state_for_call` and (if introduced)
`finalize_scalar_for_call` / `finalize_grouped_for_call`
are exactly the points a future SoA refactor rewrites to
swap per-kernel-instance `AggState` for columnar partial
aggregates indexed by group. Without ADR 021, an SoA
refactor rewrites ~8 dispatch sites; with it, 1-3.

**SoA-blocking constraints stay unpinned.** `AggState:
Copy` is preserved (not contracted as load-bearing);
`AggState` stays in `exec.rs` (not pulled into the
catalog where its shape would couple to metadata); row-
update bodies stay explicit (no dyn dispatch in the hot
path — which is the real SoA blocker, since SIMD wants
tight inner loops, not trait calls).

**Static-catalog + arena-allocated plans (Brainstorm 004
seam).** The catalog's static specs live in `.rodata`.
Arena-allocated `ExecutionPlan` nodes that reference
kernel ids do so by `u8` value; specs are not arena-
allocated. There is no co-allocation constraint a future
arena ADR would have to undo.

**Gemini's iter-3 trait-dispatch position, viewed through
the SoA lens.** Gemini's proposed
`AggregateKernel::accumulate(state, array, row)` would
bake per-row scalar dispatch into the ADR 021 contract.
That would make a future SoA refactor harder — the trait
would need either replacement or wide generalization to
admit batched/columnar accumulation. Codex's "no dynamic
dispatch in row-update loops" position is therefore an
SoA-readiness guard, even though Codex's stated
motivation was audit clarity. The two arguments converge
on the same row-path shape.

**ADR 021 does not implement SoA plan layouts.** It
records that the catalog composes cleanly with that
future direction. Any future SoA-prep or batched-
accumulator ADR can reference this section as the
substrate-readiness rationale.

## Architecture guard

`tests/architecture_test.rs` change:

- **Remove** `aggregate_function_enum_guards_are_narrow`
  (the ADR 020 enum-count guard).
- **Add** `aggregate_kernel_catalog_guards_are_narrow`:
  - Source-reads `src/aggregate_kernel.rs` and asserts
    `STABLE_AGGREGATE_KERNELS` has exactly six rows.
  - Asserts the six catalog rows are exactly `CountAll`,
    `Count`, `Sum`, `Min`, `Max`, `Avg` in that order.
  - Asserts `COUNT` is one SQL name but two catalog rows.
  - Asserts unique SQL names number exactly five.
  - Asserts `LinearRegress` and `ApproxQuantile` are not
    present inside the stable catalog rows; they may appear
    only in the adjacent experimental helper / lookup arms.
  - Source-reads `src/lib.rs`: asserts
    `mod aggregate_kernel;` exists and
    `pub mod aggregate_kernel` does not.
  - Source-reads `src/sql_planner.rs`: asserts the
    `unsupported aggregate function` reject string still
    exists.
  - Source-reads `src/exec.rs`: asserts the `AggState`
    definition still derives `Copy`.
- **Add** `bind_expr_remains_aggregate_free` (ADR 015
  reject-string-absence pattern at the aggregate
  surface): source-reads `src/binder.rs`, locates
  `bind_expr` body, asserts the body contains neither
  `AggregateFunction` nor `AggregateCall`.

ADR 019 and ADR 020 guards adjust to read through the
catalog. They retain their invariants (MIN/MAX
`IntMinMax` input domain, identity output; AVG
`AvgNumeric` input domain, `Float64` output) but assert
those via catalog rows rather than via parallel
planner/binder match arms removed by ADR 021.

## Cycle property at the metadata layer

ADR 015 established the cycle's source-level boundary
clarity property: every reachable SQL surface either has
a deterministic planner / binder behavior or rejects via
a named reject string. ADR 019 extended it to the kernel
surface (every aggregate function either lowers to a
known kernel arm or rejects via the
`unsupported aggregate function` catch-all). ADR 020 was
the second kernel-surface instance.

ADR 021 generalizes the property to the **metadata
layer**: every fact a planner / binder / executor needs
about a stable aggregate kernel comes from one
authoritative source. The architecture guard's source-
read assertions make this property machine-checked.

The stable "two-step lookup" (function → tag → spec)
preserves the existing split: stable kernels resolve
through the catalog; experimental kernels stay outside the
catalog and route through their adjacent SQL-name helper
before the binder applies the existing experimental gate;
unknown functions still reach the planner's
`unsupported aggregate function` catch-all.

## Migration strategy

Six phases, materialized in
`plans/impl/021_aggregate_kernel_catalog_refactor.md`:

1. **Phase 1 — Private catalog.** Land
   `aggregate_kernel.rs`. Verify type domains against
   existing binder helpers before pinning catalog
   contents. No call-site changes.
2. **Phase 2 — Planner migration.**
   `aggregate_function_name`, `aggregate_output_name`
   delegate to catalog. Distributed digest tests confirm
   plan digests unchanged.
3. **Phase 3 — Binder migration.** `bind_aggregate_call`
   reads spec; the three private `*_output_data_type`
   helpers removed. Error strings preserved byte-for-byte.
4. **Phase 4 — Executor cleanup.**
   `init_bound_state_for_call` introduced; row-update
   bodies untouched. Optional scalar/grouped finalize
   centralization.
5. **Phase 5 — Architecture guard.**
   `aggregate_kernel_catalog_guards_are_narrow` replaces
   the ADR 020 enum-count guard;
   `bind_expr_remains_aggregate_free` added. ADR 019 /
   ADR 020 guards adjusted to read through the catalog.
6. **Phase 6 — Verification + bench.** Full test matrix;
   bench artifact at
   `plans/impl/021_aggregate_kernel_catalog_refactor_bench_results.md`.

## Definition of done

- `crates/toron/src/aggregate_kernel.rs` exists with the
  catalog and full helper API.
- `mod aggregate_kernel;` declared in `lib.rs` (private).
- Planner metadata delegates to catalog; plan digests
  byte-for-byte unchanged.
- Binder metadata delegates to catalog; error strings
  preserved byte-for-byte; the three private
  `*_output_data_type` helpers removed.
- Executor `init_bound_states` delegates through
  `init_bound_state_for_call`; row-update bodies and
  `AggState` byte-identical; `AggState: Copy` preserved.
- `aggregate_kernel_catalog_guards_are_narrow` replaces
  `aggregate_function_enum_guards_are_narrow`;
  `bind_expr_remains_aggregate_free` added; ADR 019 /
  ADR 020 guards read through the catalog.
- All existing tests green: `group_by_test.rs`,
  `architecture_test.rs`, `distributed_process_test.rs`,
  `aggregate_projection_accounting_test.rs`,
  binder/planner test suites.
- Bench artifact records digest parity and per-aggregate-
  row timings; no row regresses > 10%.
- No SQL behavior change. No IR shape change. No
  fragmenter detector change. No cross-ring engagement.

## Recorded dissent

Gemini iter-0 through iter-3 consistently advocated:

- A runtime `AggregateKernel` trait with row-path
  `accumulate(state, array, row)` dispatch.
- Bundling Float MIN/MAX with the refactor as a
  "sticky-NaN proof case."
- "30-50% body-line reduction" via trait dispatch.

Codex + Claude 2-of-3 majority across all four
iterations landed on:

- Catalog-only mechanism; no runtime trait dispatch.
- Refactor-alone scope; Float MIN/MAX deferred to
  ADR 022 against the new spine.
- Realistic line reduction ~15-20% across metadata
  helpers; row-path bodies intentionally preserved per
  audit-clarity and SoA-readiness arguments (see §SoA /
  SIMD forward compatibility).

The dissent is recorded so future ADRs can re-raise the
trait-dispatch question with a different evidence base.
Notably, the SoA / SIMD forward-compat consideration
(council-converged) further reinforces the no-row-path-
dyn-dispatch decision — any future kernel needing
polymorphic accumulation should make the case alongside
SoA plan-layout evidence.

## Future work

- **ADR 022 — Float MIN/MAX (with sticky-NaN policy).**
  First downstream user of the catalog spine. Adds a
  Float input-domain variant (likely `FloatMinMax` or
  extension of `IntMinMax`); reuses
  `AggregateOutputRule::SameAsInput`; ratifies the
  Float32/Float64 NaN-policy decision Gemini iter-0
  through iter-3 wanted bundled here.
- **ADR 023 — Post-aggregate projection.** Naming-
  asymmetry signal on `GroupedAggregateScope` (17
  references; 12 in scalar paths post-ADR-018). Renames
  the scope, lifts aliased / computed-expression / key-
  subset / reordered-keys surfaces. Larger scope than
  ADR 021; depends on the catalog for output-name
  authority.
- **Cross-ring AGG consumer wiring (ADR 02X).** Engages
  the Wendelon AGG protocol; lifts ADR 011/019/020's
  per-partition scalar ceiling; bench artifact required
  (cross-ring perf characteristic). Catalog provides the
  Toron-side metadata authority the wire surface
  references.
- **Kernel graduations.** `LinearRegress` and
  `ApproxQuantile` each promote under their own ADR. The
  catalog's `experimental_aggregate_sql_name` helper is
  the aggregate-kernel substrate; graduation moves a
  kernel from experimental-helper to stable-catalog row.
  `TimeBucket` remains a scalar expression/PBC concern,
  not an aggregate-catalog row.
- **SoA / SIMD plan layout.** Speculative. See §SoA /
  SIMD forward compatibility for the substrate-
  readiness rationale.
