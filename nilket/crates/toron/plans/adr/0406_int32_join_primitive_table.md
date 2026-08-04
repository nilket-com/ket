# ADR 406 - Int32 join build/probe primitive table generalization

## Context

405 landed the fenced primitive join table for `JoinKeyPlan::Int64 && !null_safe_keys`. It removed the enum-key/hashbrown
representation from the q03 Int64 build/probe path and banked a measured q03 sf=1 win. 405 also named `Int32` as the
small follow-on once the primitive-table contract was proven.

The important grounding for 406: **TPC-H does not exercise Int32 join keys in Toron's schema**. All TPC-H join-key
columns are `Int64` (`l_orderkey`, `o_orderkey`, `c_custkey`, `*_partkey`, `*_suppkey`, `*_nationkey`,
`*_regionkey`). The `Int32` columns are attributes such as `p_size`, `o_shippriority`, `ps_availqty`, and derived year
outputs. So 406 has no honest benchmark driver and must not claim a q03/qNN performance win.

406 generalizes the 405 table contract to `Int32`: one primitive open-addressing table implementation parameterized over
`i32`/`i64`, selected only for non-null-safe primitive key plans. `Generic` and null-safe joins remain on the existing
`HashMap<JoinKey, BuildRowList, RandomState>` fallback.

## Decision

### The cut - coverage/generalization, not perf
- Replace the Int64-only table type with `PrimitiveBuildTable<K>`, instantiated for `i32` and `i64`.
- Select the primitive table for:
  - `JoinKeyPlan::Int32 && !null_safe_keys`
  - `JoinKeyPlan::Int64 && !null_safe_keys`
- Keep `Generic` and all null-safe joins on the existing hash-map path.
- Keep `BuildRowList` as the value representation, preserving duplicate insertion order and the existing
  outer/semi/anti matched-build bitmap contract.
- Add `get_int32(i32)` so Int32 probes avoid constructing `JoinKey::Int32`, mirroring 405's `get_int64(i64)`.

### Stop-line
If the Int32 primitive path cannot preserve the 405 contract with the same behavior-neutral surface - duplicate order,
null routing, all join kinds, guard decision, spare-bucket termination - do not ship a partial Int32 fast path. Leave
Int32 on the hash-map fallback and seal the failure as a diagnostic. There is no benchmark-driven reason to force it.

## Acceptance

- **No perf claim:** results explicitly state that TPC-H does not exercise Int32 join keys; no q03/qNN win is banked.
- **Fence honored:** `Int32 && !null_safe_keys` selects the primitive table; `Int32 && null_safe`, `Generic`, and
  mismatched-key paths stay on the hash-map fallback. Int64 behavior remains the 405 primitive path.
- **Behavior-neutral Int32 tests:** mirrored locks for duplicate/probe order, null routing, all eight join kinds vs the
  hash fallback, memory-guard equivalence, and spare-bucket termination.
- **Shared mechanism:** Int32 and Int64 use the same `PrimitiveBuildTable<K>` implementation; no qNN or schema-specific
  branch.
- **No regression:** the broader join unit surface stays green; no cap bump; no plan-shape changes.
- **Hygiene:** formatted, `git diff --check`, no committed `.perf.data`, de-branded.

## Guardrails

- The primitive path is selected only by `JoinKeyPlan` and `null_safe_keys`, not by query name or table/schema.
- Output determinism remains probe-row driven; the primitive table is never an output iteration source.
- The memory guard remains the existing `estimated_build_hash_bytes` guard; 406 must not admit a build that the current
  guard would spill/reject.
- The Int32 branch is coverage of a generic executor path. Because there is no TPC-H perf signal, any result doc that
  reads as a performance bank is wrong.

## Risks

- **False perf framing:** the main risk is overclaiming. TPC-H has no Int32 join-key row, so the only valid result is
  coverage/no-regression.
- **Generic-table drift:** parameterizing the table could accidentally change Int64 behavior. The existing 405 Int64
  tests must continue to pass.
- **Null-safe leak:** null-safe Int32 must remain on the hash-map/generic fallback. A primitive null-safe path is out of
  scope.
- **Absent-probe termination:** the open-addressing table relies on spare buckets; mirrored Int32 spare-bucket coverage
  must lock the invariant.

## Forward Arc

With Int32 covered, the primitive-table story is complete for single-column primitive keys. The next performance-backed
join-table mechanism is not another Int32 pass; it is radix/partitioned join-table work for the Int64-heavy paths, or a
separate execution-model cut for the sf=10 time cells. 406 should therefore seal as a small coverage/generalization ADR.

## Grounding evidence

405 primitive table and contract: `crates/toron/src/exec/join/build_rows.rs`, `build_in_memory_join_rows` in
`crates/toron/src/exec/join.rs`, and the 405 results doc. TPC-H schema grounding:
`crates/toron-tpch/src/lib.rs` declares TPC-H join-key columns (`l_orderkey`, `o_orderkey`, `c_custkey`, `*_partkey`,
`*_suppkey`, `*_nationkey`, `*_regionkey`) as `DataType::Int64`; `Int32` fields are non-key attributes/derived outputs.
