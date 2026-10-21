# ADR 498 - m04 low-cardinality grouped COUNT attribution

The corrected post-497 primitive-ladder ranking makes m04 the largest recoverable
one-thread gap: `SELECT l_returnflag, COUNT(1) FROM lineitem GROUP BY l_returnflag`
takes about 2,015 ms in Toron versus 186 ms in DuckDB 1t at sf=8. m06 remains an
execution target, but its corrected 1t gap is about 1,032 ms and is second priority.

## Boundary

m04 is not ADR 495's compact Int64 path and not ADR 497's join probe path. It is the
streaming generic grouped fold: one non-null UTF-8 key with three observed groups and
one row-count aggregate. Every lineitem row currently evaluates the key, finds one of
three hash-table entries, and increments its `AggState::Count`.

The nearby m07/m08 bare COUNT results corroborate a broad per-row substrate cost, but
do not establish which part of m04 is recoverable. The small group cardinality rules
out generic high-cardinality state growth as the first hypothesis.

## Decision

Before choosing a kernel, run m04 with the existing
`TORON_GENERIC_GROUPED_FOLD_PROFILE_DIR` receipt enabled and quantify:

- UTF-8 group-expression evaluation;
- grouped-fold time, lookup/probe counts, and occupied-hit rate;
- aggregate updates; and
- finalize time and resident state.

Compare the profiled m04 result with m07/m08 on the same strict sf=8, persisted
Parquet, 2 GiB task / 2 GiB spill-reserve, release coordinate. The next implementation
must be selected from the dominant measured bucket, not from the low-cardinality shape
alone. A UTF-8-specialized three-key dispatch is admissible only if it preserves the
generic semantics for nulls and arbitrary returnflag values; otherwise scan/decode or
generic fold dispatch remains the target.

## Reference rail

Any primitive ladder including DuckDB now requires
`TORON_TPCH_DUCKDB_THREADS=1`; an unset or non-`1` value is a harness error. This closes
the post-497 re-pin's parallel-reference confound before the new evidence is taken.

## Acceptance

The eventual m04 change must keep the digest, three output rows, and zero spill
unchanged. Its profile must show the targeted bucket shrinking on paired runs; a wall
improvement without that movement is not sufficient. The refreshed ladder must retain
explicit DuckDB thread provenance of `"1"` and label Polars separately as a one-thread
reference when present.
