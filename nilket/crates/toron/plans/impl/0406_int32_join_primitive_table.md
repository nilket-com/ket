# ADR 406 - Implementation plan: Int32 join primitive table generalization

Pairs with `plans/adr/0406_int32_join_primitive_table.md`. Generalize 405's Int64 primitive build/probe table to cover
`JoinKeyPlan::Int32 && !null_safe_keys`, using a shared `PrimitiveBuildTable<K>` implementation. This is coverage and
mechanism consistency only; TPC-H has no Int32 join-key perf driver.

## Phase 0 - grounding

1. Confirm TPC-H schema has no Int32 join keys:
   - key fields such as `l_orderkey`, `o_orderkey`, `c_custkey`, `*_partkey`, `*_suppkey`, `*_nationkey`,
     `*_regionkey` are `DataType::Int64`;
   - `Int32` fields are attributes/derived outputs, so there is no qNN metric to bank.
2. Confirm the 405 seam:
   - `JoinBuildRows` owns the table representation;
   - `build_in_memory_join_rows` chooses the table from `JoinKeyPlan` + `null_safe_keys`;
   - `probe_in_memory_join_rows` has primitive Int32/Int64 arms;
   - the direct streaming left-outer probe path also has primitive Int32/Int64 arms.

## Phase 1 - implementation

- Convert the Int64-only table into `PrimitiveBuildTable<K>` where `K: Copy + Eq + Hash`.
- Add `JoinBuildRowsInner::Int32(PrimitiveBuildTable<i32>)`.
- Add `new_int32_join_build_rows(capacity)`.
- Select it in `build_in_memory_join_rows` only for `(JoinKeyPlan::Int32, false)`.
- Add `get_int32(i32)` and use it in:
  - the central Int32 arm of `probe_in_memory_join_rows`;
  - the direct streaming left-outer Int32 probe path.
- Keep:
  - `Generic` and null-safe paths on the hash-map fallback;
  - existing `BuildRowList` values;
  - existing memory guard / cap behavior.

## Tests

Mirror the 405 Int64 locks for Int32:

- fence: Int32 non-null-safe uses the primitive table; Int32 null-safe and Generic stay on hash-map fallback;
- duplicate/probe order and null routing;
- all eight join kinds match the hash fallback row-for-row;
- memory guard equivalence under an over-cap build;
- spare-bucket termination for absent probes;
- existing Int64 tests continue to pass.

## Verification

```text
cargo fmt --all
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron join_build_rows --lib -- --nocapture
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup fmt -- --check
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup check -p toron --all-targets
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron --test architecture_test
git diff --check
```

If any test or compile failure shows the generic table changed Int64 semantics, fix that before sealing. No perf run is
required for acceptance because there is no Int32 TPC-H driver.

## Deliverable

`plans/impl/0406_int32_join_primitive_table_results.md` records:

- the no-perf-driver schema grounding;
- the shared `PrimitiveBuildTable<K>` implementation;
- the mirrored Int32 correctness tests and broader join surface result;
- explicit no perf claim / no qNN claim;
- hygiene: no cap bump, no committed `.perf.data`, no qNN branding.

## Seal

`plan: adr 406 int32 join primitive table` if committed as a plan first. Implementation:
`impl: adr 406 int32 join primitive table`.
