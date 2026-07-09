# ADR 406 results - Int32 join primitive table generalization

## Summary

Implemented the Int32 follow-on from 405 by parameterizing the primitive join build/probe table:

```text
PrimitiveBuildTable<K>
```

is now used for both `i32` and `i64` keys. `JoinKeyPlan::Int32 && !null_safe_keys` selects
`PrimitiveBuildTable<i32>`, while `JoinKeyPlan::Int64 && !null_safe_keys` continues to use the 405 primitive path.
`Generic` and null-safe joins stay on the existing `HashMap<JoinKey, BuildRowList, RandomState>` fallback.

This is **not** a benchmark/perf ADR. Toron's TPC-H schema has no Int32 join keys: all TPC-H key columns used for joins
are `Int64`. So there is no q03/qNN performance result to bank. The result is behavior-neutral coverage and mechanism
consistency for non-TPC-H Int32 joins.

## Implementation

- `JoinBuildRowsInner` now has three representations:
  - `Hash(HashMap<JoinKey, BuildRowList, RandomState>)`
  - `Int32(PrimitiveBuildTable<i32>)`
  - `Int64(PrimitiveBuildTable<i64>)`
- The former Int64-only open-addressing table is now `PrimitiveBuildTable<K>`.
- `new_int32_join_build_rows(capacity)` mirrors `new_int64_join_build_rows(capacity)`.
- `build_in_memory_join_rows` selects the primitive table for:

```text
(JoinKeyPlan::Int32, false) -> PrimitiveBuildTable<i32>
(JoinKeyPlan::Int64, false) -> PrimitiveBuildTable<i64>
_                         -> HashMap fallback
```

- `get_int32(i32)` mirrors `get_int64(i64)` and avoids constructing `JoinKey::Int32` on the central probe path and the
  direct streaming left-outer probe path.
- `BuildRowList` stays the table value, so duplicate insertion order, probe-row output order, and the matched-build
  bitmap semantics are unchanged.

## No TPC-H perf driver

Schema grounding from `crates/toron-tpch/src/lib.rs`:

- join-key fields such as `l_orderkey`, `o_orderkey`, `c_custkey`, `p_partkey`, `s_suppkey`, `n_nationkey`,
  `n_regionkey`, `ps_partkey`, and `ps_suppkey` are `DataType::Int64`;
- `DataType::Int32` appears on attributes/derived outputs such as `p_size`, `o_shippriority`, `ps_availqty`, and year
  projections, not as TPC-H join keys.

Therefore 406 does not include a four-way perf table or a claimed query speedup.

## Correctness gates

Focused join-build table tests:

```text
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron join_build_rows --lib -- --nocapture
```

Result: `13 passed`.

New Int32 locks:

- `int32_join_build_rows_are_fenced_to_non_null_safe_int32_plan`
- `int32_join_build_rows_preserve_duplicate_probe_order_and_null_routing`
- `int32_join_build_rows_match_hash_fallback_for_join_kinds`
- `int32_join_build_rows_preserve_memory_guard_decision`
- `int32_join_build_rows_keep_spare_bucket_for_absent_probe`

Existing Int64 locks from 405 still pass, as do the shared unique-row, duplicate-overflow, generic-order, and
preallocation tests.

Broader join unit surface:

```text
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron join --lib
```

Result: `184 passed`.

Additional gates:

```text
scripts/cargo-cgroup fmt -- --check
git diff --check
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup check -p toron --all-targets
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron --test architecture_test
CARGO_BUILD_JOBS=12 cargo test -p toron --no-fail-fast
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron-tpch --no-fail-fast
```

Results:

- format and diff checks passed;
- `check -p toron --all-targets` passed;
- architecture passed: `2079 passed`;
- `toron --no-fail-fast` passed;
- `toron-tpch --no-fail-fast` passed.

## Hygiene

- No cap bump.
- No qNN or TPC-H-specific branch.
- Null-safe Int32 remains on the hash-map fallback.
- The existing Int64 primitive path remains covered by the 405 tests.
- No `.perf.data` artifact is part of this change.

## Verdict

406 lands as an honest behavior-neutral generalization: the primitive join table now covers both single-column primitive
integer key plans (`Int32` and `Int64`) while preserving the 405 contract and keeping the generic/null-safe fallback
intact. The next performance-backed join-table work is radix/partitioned representation for the Int64-heavy paths, not
more Int32 benchmarking.
