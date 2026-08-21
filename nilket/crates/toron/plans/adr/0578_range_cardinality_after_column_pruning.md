# ADR 578 - Range cardinality after column pruning

**Status:** Proposed 2026-08-21. Repairs the ADR 0569 range-source invariant
exposed by `ctx.range(N).aggregate([], [count_all()])`.

## Context

ADR 0569 defines `Range` as a generated relation of `count` non-null `Int64`
ordinals. Its executor assumes that schema unconditionally: it synthesizes one
ordinal array and uses `expect("range schema is the one ordinal column")` when
building each batch.

That assumption conflicts with the required-column optimizer. A global
`COUNT(*)` reads row cardinality, not a value column, so
`optimize_for_required` correctly projects a `Range` input to an empty schema.
The executor then supplies one array for zero fields and panics. This is a
user-reachable query failure, not malformed SQL and not a condition that may
be converted into a generic execution error.

Arrow record batches have an explicit row-count option precisely for this
case: a zero-field batch can represent N rows when constructed with
`RecordBatchOptions::with_row_count(Some(N))`. CountAll already uses
`batch.num_rows()` in both the unbound and bound aggregate paths. The missing
concept is therefore not cardinality in the IR; it is that a physical Range
may retain its rows after all value columns are pruned.

## Decision

### 1. A pruned Range may have zero value columns while retaining cardinality

The logical range relation remains one ordinal column. The bound execution
plan may have either of two schemas:

- the canonical non-null `Int64` `ordinal` column, when a consumer needs it;
- zero columns, when required-column pruning proves no consumer needs a
  value, while the row count still matters.

No third Range schema is meaningful. A wire-delivered plan with any other
schema fails closed with a typed execution error; it never reaches an
`expect`, silently manufactures a column, or treats an arbitrary field as an
ordinal.

### 2. Zero-column Range batches carry row count explicitly

For the canonical schema, the executor emits the existing ordinal arrays. For
the empty schema, it emits batches with no arrays and an explicit row count.
Both forms use the existing 8,192-row batch partitioning, so an aggregate,
filter-free cardinality consumer sees exactly `count` rows without materializing
an unused `Int64` array.

This is physical representation, not a second cardinality field on
`ExecutionPlan::Range`: `count` remains the source's single cardinality fact,
and `RecordBatch::num_rows()` is the execution representation every existing
consumer already understands.

### 3. The malformed-schema boundary is executable, not aspirational

The binder constructs only the two admitted shapes. Execution nevertheless
validates because a plan can arrive without binding. A malformed range schema
must return `ExecutionError::InvalidExpression` naming the expected canonical
or zero-column shapes. It must never panic.

## Acceptance

- `ctx.range(4).aggregate([], [count_all()])` returns one row with count 4;
  it must not panic. This is the direct regression.
- The same query over `range(0)` returns one row with count 0, proving the
  zero-column representation distinguishes an empty relation from a global
  aggregate's one zero result.
- Binding the keyless CountAll plan leaves the Range schema empty, proving the
  success is cardinality-preserving pruning rather than a hidden retained
  ordinal.
- A direct execution control for a zero-column Range observes its batches'
  row counts summing exactly to `count`, including a count that crosses the
  8,192-row boundary.
- A direct wire-shaped Range with a non-empty, non-canonical schema returns a
  typed `InvalidExpression` error and never panics. The control is executed,
  not merely a schema helper unit test.
- A value consumer such as `seeded_random(seed, ordinal)` still receives the
  canonical ordinal arrays and produces the ADR 0569 witnesses unchanged.

## Guardrails

`synthesize_range_batches` owns the two physical shapes. It must not accept a
general projection list or infer cardinality from a column; those would turn a
closed generated source into a generic scan. The range cap and its checked
byte facts remain unchanged.

The regression must invoke the normal Rune/binder path as well as the direct
malformed-plan path. Testing only one would either miss pruning or miss the
wire safety boundary.

## Risks and deferrals

Zero-field batches are valid Arrow but uncommon in this executor. This cut is
scoped to Range and CountAll's existing row-count contract; it does not claim
that arbitrary operators accept zero fields. Any new operator that admits a
columnless input must demonstrate that it consumes cardinality rather than a
column.

This does not lift ADR 0569's materializing cardinality cap, add a streaming
range source, or change projection pruning for ordinary scans.

## Forward

After this repair, the ADR 0577 ignored transport exhibit may return to
`count_all()` rather than carrying a projected random column solely to avoid a
Range panic. The exhibit remains a transport test; that cleanup is not a
prerequisite for this record.
