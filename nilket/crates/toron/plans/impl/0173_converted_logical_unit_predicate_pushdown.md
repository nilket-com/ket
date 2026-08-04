# Impl Plan - ADR 173: Converted Logical-Unit Predicate Pushdown

Pairs with `plans/adr/0173_converted_logical_unit_predicate_pushdown.md`.

A bind-time predicate-planning change: normalize a linear converted comparison
(`col * factor cmp const`) into a conservative bare-column pre-filter
(`col cmp const'`) that the existing PBC compiler accepts, keeping the original
CONVERT predicate as the exact Toron residual. No PBC opcode, scan-request,
storage, descriptor, executor, fact schema, or fact producer change; the existing
range-selectivity consumer gets an authenticated converted source-atom hook.

## Anchors

| Concern | Anchor |
|---|---|
| Pushdown planner | `predicate_pushdown_plan` (`binder.rs:3030`), `partition_bound_predicate` (`binder.rs:3091`), `plan_filter_with_predicate_pushdown` |
| PBC compile entry | `compile_pbc` (`binder.rs:2114`); attach `attach_pbc_program` (`binder.rs:2127`) |
| PBC arithmetic rejection (stays) | `pbc.rs:107`, `pbc.rs:353` (`Add|Sub|Mul|Div` → `ExpressionShapeUnsupported`) |
| Pushdown plan struct | `PredicatePushdownPlan { pushed_predicate, residual_predicate, pbc_program, profile_hint }` |
| Bound expr graph + literal read | `BoundExprNode::{Binary{op,left,right}, Column(ResolvedColumn), Literal(u32)}` (`ir/execution.rs:81`); `BoundExprProgram::literal(id) -> Option<&ScalarValue>` (`:608`), `append_literal` (`:433`) |
| CONVERT desugar shape | `bind_sql_convert` (`binder.rs:9059`): linear → `Mul(col, factor)`; affine → `Add(Mul(col,factor), shift)` |
| Logical type metadata | `BoundExprProgram::node_logical_type` (`ir/execution.rs:596`); CONVERT stamps the target logical type (`binder.rs:9158`) |
| Range selectivity | `filter_range_selectivity_estimated_row_count` (`binder.rs:5065`), called before pushdown planning (`binder.rs:632` before `:639`) |
| Behavior tests | `crates/toron/tests/logical_type_ddl_test.rs`, predicate-pushdown test module |
| Architecture tests | `crates/toron/tests/architecture/predicate_pushdown.rs`, `nominal_type_identity.rs` |

## Phase 1 - Recognize the authenticated linear-converted shape

Add a detector over a comparison conjunct's `BoundExprProgram` nodes:

```rust
struct LinearConvertedComparison {
    column: ResolvedColumn,   // base scan column
    source_logical: LogicalTypeId,
    target_logical: LogicalTypeId,
    factor: f64,              // positive
    op: BoundBinaryOp,        // <, <=, >, >= (mirrored if const is on the left)
    constant: f64,
}

fn match_linear_converted_comparison(
    program: &BoundExprProgram,
    root: BoundExprId,
    logical_types: &LogicalTypeRegistry,
) -> Option<LinearConvertedComparison>
```

Match `Binary{cmp, A, B}` where `cmp ∈ {Lt,Lte,Gt,Gte}` and exactly one of `A`/`B`
is `Binary{Mul, Column(c), Literal(f)}` (or `Binary{Mul, Literal(f), Column(c)}`)
with `c` a base column and both `f` and the opposite side `Float64` literals (read
via `program.literal(id)` → `ScalarValue::Float64(Some(_))`). If the constant is on
the left, mirror `cmp`.

Then authenticate that this arithmetic is a CONVERT lowering, not naked arithmetic:

- read `program.node_logical_type(mul_node)` for the target logical type;
- read `program.node_logical_type(column_node)` for the source logical type;
- resolve both specs through `LogicalTypeResolver::with_registry(logical_types)`;
- require both specs to have units, matching dimension, matching quantity kind, and
  `Float64` physical type;
- compute the expected factor with the same helper used by `bind_sql_convert`;
- require the observed factor's bits to match the expected factor's bits.

**Reject** if the `Mul` is wrapped in `Add` (affine shift — deferred D6), if either
literal is not finite `Float64`, if the factor is not finite and positive, if the
column is not a base scan column, if more than one column is involved, or if the
metadata proof fails.

Refactor the existing CONVERT factor computation into a shared helper before adding
the recognizer, so binder and optimizer do not drift.

## Phase 2 - Derive the conservative bare-column conjunct

```rust
fn derive_pushable_bare_predicate(m: &LinearConvertedComparison) -> BoundExprProgram
```

- `const' = m.constant / m.factor` (f64), declining if the result is non-finite;
- nudge outward by one ULP so the pushed predicate is a superset:
  - `Gt`/`Gte`: `const' = next_float_down(const')` (include more);
  - `Lt`/`Lte`: `const' = next_float_up(const')`;
- build a `BoundExprProgram` for `Column(m.column) m.op Literal(const')` via
  `append_literal(ScalarValue::Float64(Some(const')))` + the column node — a
  bare-column comparison (PBC-eligible).

Use `f64::next_down`/`f64::next_up` if available in the active toolchain, or local
bit-based helpers with tests for `0.0`, `-0.0`, finite extrema, NaN, and infinity.

`factor > 0` is authenticated from CONVERT metadata, so `m.op` is never flipped by
the division.

## Phase 3 - Integrate into the pushdown planner

Thread `logical_types` into `predicate_pushdown_plan`. In `predicate_pushdown_plan`
(`binder.rs:3030`), after `partition_bound_predicate` (`3091`):

- collect D1-shaped conjuncts from the **whole** predicate (they currently sit in
  `residual`) using `match_linear_converted_comparison(..., logical_types)`;
- build derived bare-column conjuncts (Phase 2);
- `pbc_input = AND(existing_pushed_conjuncts, derived_conjuncts)`;
- `compile_pbc(pbc_input)`; on success attach via `attach_pbc_program`; on failure
  fall back to today's behavior;
- **residual = the original predicate minus only the conjuncts that were exactly
  pushable before** — the converted conjuncts always stay in the residual (their
  push is approximate).

Keep `attach_pbc_program` and the scan plumbing unchanged. The derived conjuncts
exist only inside the PBC program; they are never the residual.

## Phase 4 - Reconnect range selectivity explicitly

Update `filter_range_selectivity_estimated_row_count` to receive logical type
context, or add an adjacent helper that can access it from the caller. In its
top-level conjunct walk, use the existing atom first and then the converted atom:

```rust
let atom = range_atom(predicate, conjunct, input)
    .or_else(|| converted_linear_range_atom(predicate, conjunct, input, logical_types));
```

The converted atom returns the source column ordinal and the widened source
threshold, so existing `column_range_for_input`, range distribution, and
`estimated_rows_for_range` logic can be reused unchanged.

This is not optional for the ADR headline. In the current bind flow, range
selectivity is computed before `plan_filter_with_predicate_pushdown`; a PBC-only
implementation would prune storage at execution time but still miss source range
facts when choosing plans.

## Phase 5 - Behavioral tests

Fixture: a base `Float64` unit column (`distance_mile : Mile`, with `Kilometer`
defined), source-column range facts for `distance_mile`, and a non-unit column
(`region`).

- `converted_linear_predicate_pushes_as_bare_column` - `WHERE
  CONVERT(distance_mile, Kilometer) > 10.0` attaches a PBC program over
  `distance_mile`; assert `Scan.pbc_program.is_some()` and correct rows.
- `converted_pushdown_keeps_exact_residual` - a value inside the ULP widening is
  pre-filter-passed but residual-dropped; final result equals the unpushed result.
- `linear_convert_pushdown_matches_unpushed_results` - parametric over
  `{<,<=,>,>=}` and several constants: pushed == unpushed rows.
- `converted_linear_predicate_spends_source_range_fact` - converted filter over
  `distance_mile` changes estimated row count through the `distance_mile` range
  fact path.
- `mixed_predicate_pushes_converted_and_bare_conjuncts` - converted bound + `region
  = 'us'` both in the PBC program; residual re-checks the converted conjunct.
- `affine_point_converted_predicate_stays_residual` - affine CONVERT predicate: no
  PBC program; correct result.
- `equality_on_converted_column_stays_residual` - `CONVERT(...) = k`: no push;
  correct.
- `naked_arithmetic_scaled_predicate_stays_residual` - `distance_mile * 1.609344 >
  10.0` without CONVERT logical metadata does not push.
- `converted_pushdown_rejects_factor_metadata_mismatch` - mismatched factor/metadata
  does not produce a derived pushed predicate.
- `non_converted_predicate_pushdown_unchanged` - regression on existing
  bare-column/PBC predicates.
- `rune_converted_predicate_pushes` - same filter via Rune pushes identically.

Reuse the existing predicate-pushdown harness (asserting `pbc_program`/scan shape).
Note the `MultiTableStorageScan` mock drops the pushed predicate, so assert *that
the PBC program is attached* and row correctness via the residual, per the known
mock caveat.

## Phase 6 - Architecture guards

In `predicate_pushdown.rs` / `nominal_type_identity.rs`:

- `adr173_converted_linear_predicate_normalized_before_pbc` - the planner matches +
  derives bare-column conjuncts from `col * factor cmp const` before `compile_pbc`.
- `adr173_convert_pushdown_is_metadata_authenticated` - the matcher checks
  source/target logical unit metadata and the shared CONVERT factor helper, not just
  a `Mul` node shape.
- `adr173_pbc_compiler_stays_arithmetic_free` - `pbc.rs` still returns
  `ExpressionShapeUnsupported` for `Add|Sub|Mul|Div` (reuse the existing assertion).
- `adr173_converted_pushdown_keeps_exact_residual` - the derive step does not remove
  converted conjuncts from the residual.
- `adr173_range_selectivity_uses_converted_source_atoms` - the range estimator calls
  a converted source-atom helper with logical type context.
- `adr173_affine_and_equality_pushdown_deferred` - the matcher rejects the
  `Add(Mul,…)` (affine) shape and `Eq`/`Neq`.
- `adr173_no_pbc_opcode_or_storage_wire_surface` - no scan-request/PBC/storage
  field added.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test converted_linear_predicate_pushes_as_bare_column
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test linear_convert_pushdown_matches_unpushed_results
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test converted_pushdown_keeps_exact_residual
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test converted_linear_predicate_spends_source_range_fact
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test
scripts/cargo-cgroup test -p toron --test architecture_test adr173
scripts/cargo-cgroup test -p toron --test architecture_test predicate_pushdown
scripts/cargo-cgroup test -p toron --test architecture_test
git diff --check
```

## Risks

- **Superset direction.** The ULP nudge must widen, not narrow, per operator; a
  wrong-direction nudge silently drops boundary rows. Pin with
  `converted_pushdown_keeps_exact_residual` at an exact boundary value.
- **Residual must stay exact.** Never remove a converted conjunct from the residual;
  the pushed derived conjunct is approximate.
- **Naked arithmetic must not be swept in.** Shape-only matching of `col * factor`
  would also catch user arithmetic, including negative factors. Authenticate source
  and target logical-unit metadata and factor equality before deriving a pushdown.
- **Base-column-only.** Only push when the `Mul`'s column operand is a base scan
  column; a converted expression over a computed column must not push.
- **Selectivity order.** Range selectivity runs before pushdown planning. Keep the
  converted source-atom path in the estimator, not only in PBC derivation.
- **Mock predicate drop.** `MultiTableStorageScan` mock ignores the pushed
  predicate; assert attachment + residual-correct rows, not mock row counts.
- **Affine shape.** Ensure `Add(Mul(col,factor), shift)` is rejected by the matcher
  (deferred), not mis-parsed as `col * factor`.
- **Factor sign.** Guaranteed positive by 167; assert no direction flip anyway.

## Deferred

Affine-point converted pushdown; bracketed `=`/`!=` converted pushdown; teaching PBC
linear-transform shapes directly; unit display formatting; cross-unit point delta.
