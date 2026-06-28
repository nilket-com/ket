# ADR 364 - partitioned streamed inner-build spill (bound q21's resident index)

## Context

The q21 closer (one more before 359-redux). ADR 363 cleared q21's spill-volume; q21 now hits the **streamed inner join
build spill resident state** (memory_budget_exhausted, **11,656 B over 1 GB** - the 349 index family). Bound the
resident path by **grace/partitioning the streamed inner build**, reusing the 349 *primitives* (not the LeftOuter
visitor). Narrow path; whole resident path bounded; **no cap bump**; sf=10-durable by design. **No shared substrate -
factor later only after a third caller proves it.**

## Decision

### Reuse the primitives, not the visitor
- **Reuse:** `streamed_spill_partition_count` (join.rs:2126), `select_join_partition_batch` (2139), the existing
  `SpilledInnerJoinBuild`, and 363's **projected write / key-remap / schema-restore** materialization.
- **Do NOT reuse** `visit_partitioned_spilled_left_outer_join_chunks` - it is LeftOuter output/null-fill specific. 364
  adds a **narrow partitioned streamed-inner-build path** that keeps 363's projection/key-remap/schema-restore behavior
  (inner-join semantics, not null-filled outer).

### Phase 0 (load-bearing) - measure resident terms + record the partition topology
- Measure q21's reoriented inner-build resident: **index bytes, writer/page state, retained inputs, build map** (+ the
  concat/input copy). Confirm `index_bytes` is the dominant data-proportional term.
- **Record the partition topology.** For a streamed *scan* build, default to the simple bounded path:
  **re-scan once per active partition, holding only one partition's index resident** at a time. The alternative
  (one-pass partition staging) is allowed **only if Phase 0 proves it does not hold all partition indexes resident**.
- **Partition count is computed against the effective resident-state cap the failing guard compares to**:
  `join_spill_retained_footprint_memory_cap(task_budget.memory.get())` (~1 GB for q21), **not blindly the raw 64 MB
  task-memory input** - unless Phase 0 proves the stricter target is intentional. (Mismatching the cap mis-sizes the
  partition count.)

### The fix
A narrow partitioned streamed-inner-build path: partition by key hash via `streamed_spill_partition_count` /
`select_join_partition_batch` against the resident-state cap; process **one active partition at a time** (re-scan per
partition by default), building only that partition's projected (363) `SpilledInnerJoinBuild`, materializing matches
with the 363 key-remap/schema-restore, then dropping it before the next. Only the active partition's index + build map
+ input is ever resident.

## Acceptance
- q21 sf=10 (fixed 4 GB): **green** (`pass@execute`) via the partitioned resident path, **OR** a named next wall (332)
  - if it advances, that is the real remaining q21 term.
- **No full build index resident**; **no multiple partition indexes resident** unless Phase 0 bounds it by measurement;
  **resident state bounded by design at sf=10 scale** (synthetic isolated-operator at sf=10, or a bounded-by-design
  partition-count argument - mirroring 349's regression test).
- **No cap bump** - the resident/memory budget unchanged; partition count sized to the resident-state cap.
- **Correctness:** q21 `pass@compare` strict at sf=0.01/sf=1; **dev (Float64) parity** via a focused parity test
  (partitioned == un-partitioned inner build), not `correctness --mode dev`.
- **Seal gate includes `scripts/cargo-cgroup test -p toron --no-fail-fast`** (engine cut - the 361 lesson).
- **De-branded core** (topology-named; no q21/qNN in `crates/toron/src`); behavior-neutral at sf=1; q14/q18 + green
  cells unchanged.

## Guardrails
- **Primitives only, narrow path** - reuse partition-count/select + SpilledInnerJoinBuild + 363's projected
  materialization; NOT the LeftOuter visitor; no shared substrate (factor only after a third caller).
- **Bounded topology** - one active partition's index resident at a time (re-scan per partition default); one-pass
  staging only if proven not to hold all indexes.
- **Cap-matched partition count** - sized against the resident-state guard's cap, not the raw 64 MB.
- **Whole resident path bounded** - index + build map + input per partition (not index-only).
- **sf=10-durability proven, not asserted; no cap-bump; correctness strict + dev-parity; --no-fail-fast mandatory;
  de-branded core.**

## Risks
- **Visitor mis-reuse** - the LeftOuter visitor's null-fill would corrupt inner-join output; 364 keeps inner semantics
  via 363's materialization, not the visitor.
- **All-partition-index residency** - one-pass staging holding every index; default re-scan-per-partition avoids it,
  one-pass only if proven bounded.
- **Cap mismatch** - partition count vs the wrong cap mis-sizes partitions; size to the resident-state cap.
- **Projection/key-remap drift under partitioning** - the dev/strict parity gates + a partitioned-build correctness
  test.
- **Engine-cut regression** - `--no-fail-fast` mandatory (the 361 lesson).

## Forward arc
364 (q21 green, or its real remaining term) -> **359-redux** (the broader sf=10 atlas with the complete lever kit:
scan-streaming, column-projection, grace-partition/resident-bounding) -> sf=10 fix arc -> sf=100 / Rune. A third
partitioned-spill caller (likely surfaced by 359-redux) is the trigger to factor the shared substrate - not before.
