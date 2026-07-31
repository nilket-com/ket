# ADR 475 - hoist the numeric downcast out of the per-row loop

ADR 474 named the owner of the m02 20×: `numeric_value` (`aggregate.rs:2602`) runs a
chain of up to eleven `as_any().downcast_ref::<T>()` type tests **per row**, called
from plain `for row in 0..batch.num_rows()` loops. Resolve the array's concrete type
once per array and run a typed accumulation loop. Behaviour-preserving by
construction; the payoff is already quantified.

## Why this is the cut

- Profile (symbolized, `[profile.profiling]`): `numeric_value` 14.5% self,
  `fold_global_aggregate_state` 5.9%, with `<T as Any>::type_id` 2.5% and
  `PrimitiveArray::as_any` 2.1% appearing as their own line items — dispatch
  machinery only surfaces that way when it runs per value.
- Subtractions agree: accumulation costs 25 ns/row (decimal) and 11 ns/row (i64)
  while the Arrow-kernel multiply on the same rows costs 3.8 ns/row.
- The decimal penalty (14.3 ns/row) is explained by **chain position** —
  `Decimal128Array` is the last of eleven tests — not by decimal arithmetic.

## Decision

Introduce a `NumericArrayReader` that performs the existing type resolution **once
per array**, mirroring `numeric_value`'s type set, conversions, and error text
exactly. Hot loops hoist the reader out of the row loop and call a typed
`value(row)`; `numeric_value` is retained for its non-hot callers and re-expressed
through the same resolution so the two cannot drift.

**Invariants preserved, each pinned by an existing or added test:**

- **Nulls**: `is_null(row)` continues to yield "no contribution"; all-null and empty
  inputs keep today's outputs (SUM → null, COUNT → 0, AVG → null).
- **Decimal**: values still enter the accumulator as raw unscaled `i128` and SUM
  finalize still re-applies `(precision, scale)` (ADR 263); overflow behaviour
  unchanged (`checked_*` paths untouched).
- **Parity**: grouped and global aggregate paths take the same reader; a divergence
  between them is a defect this cut must not introduce.
- **Output types** unchanged — this cut touches value extraction only, never
  finalize.
- **Unsupported types** still produce the identical `InvalidExpression` message.

## Acceptance

- Full engine suite and architecture green; the 22-query correctness sweep green on
  parquet (armed source, per the ADR 465 rule).
- The ladder re-run: m02/m07/m08/m09/m10 medians and their subtractions recorded
  before and after, with the accumulate-per-row figure as the headline.
- No wall claim beyond the ladder's own arithmetic; the TPC-H board is re-measured
  separately if wanted, not asserted here.

## Guardrails

- **Semantics first.** A faster accumulator that changes a null, a scale, or an
  overflow is a regression regardless of its speed.
- **`stable_file_hash` stays out of the interpretation** — it is harness startup
  cost (58% of the profiled *process*, zero of `execute_ms`).
- No parallelism, no kernel rewrite beyond hoisting; if a full Arrow-kernel
  `sum` path is warranted, it is a separate cut with its own evidence.

## Forward

m06's 18.6× is the next target and likely shares this substrate through join probe
rows. If it does, the same hoist applies there.
