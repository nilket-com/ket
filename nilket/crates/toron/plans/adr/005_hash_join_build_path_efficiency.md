# ADR 005: Toron Hash-Join Build Path Efficiency

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion implementation plan:**
`plans/impl/005_hash_join_build_path_efficiency.md`

## Status

Promoted to `plans/adr/` and implemented. Promotion bench evidence
landed at
`plans/impl/005_hash_join_build_path_efficiency_bench_results.md`.

Builds on four WIP iteration rounds across the council
(`plans/wip/{claude,codex,gemini}_{adr,impl}_005.md`). Three-way
convergence reached on every contract surface by iter-4; iteration
artifacts retired (git holds the narrative). Codex's evidence-ordered
profiling discipline and miss-recording rigor, Gemini's brevity and
the type-tag-invariant hard-gate framing, and Claude's three-band
conditional ADR 006 numerics and load-bearing rationale are the
load-bearing antecedents.

This ADR is a **narrow optimization contract**, not a new relational
semantics, wire, scheduler, or executor-model contract. It rewrites
the worker-local hash-join build store inside `execute_inner_join`
to reduce the dominant `build_ms` bucket revealed by ADR 004's
post-promotion telemetry. Every ADR 002 / 003 / 004 hard gate and
architecture guard G1-G18 must continue to pass byte-identically.

It does **not** decide outer / semi / anti / sort-merge / non-equi
joins (V1.x — separate ADRs), runtime / Bloom filter pushdown (own
cross-ring ADR), worker-internal sharded or morsel-parallel build
(V1.x — own determinism + budget evidence), executor model upgrade
(rejected outright by ADR 004 telemetry), or the residual
`framework_overhead_ms` runtime-plumbing question (deferred to a
conditional ADR 006 governed by the rerun outcome).

## V1 contract summary

- **Scope (three work items).** (a) Join-key fast path for
  single-column fixed-width primitive keys. (b) Allocation reduction
  via inline-first row-list + pre-sized hash map. (c) Batch-oriented
  build loop replacing per-row dynamic dispatch.
- **Fast-path eligibility V1.** Single-column `Int64` and `Int32`
  join keys, nullable or non-nullable. Other key shapes already
  supported by the canonical encoder — composite keys plus
  single-column `Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`,
  `UInt64`, `Float32`, `Float64`, `Utf8` — remain on the generic
  canonical-byte path. Phase 0 profiler evidence may justify
  extending the fast-path set within this ADR to other primitive
  integer types **already supported by the canonical encoder**;
  otherwise extension is V1.x. `Date`/`Timestamp`/`Decimal` and any
  other type not already canonical-encodable is out of scope —
  encoder extension is its own work.
- **Type-tag invariant (load-bearing).** Fast-path key identity
  includes the logical Arrow type. `Int32(1)` and `Int64(1)` are
  distinct build-side keys; the encoder must not let logical types
  collide. Unit-test enforced.
- **Allocation reduction.** Replace `HashMap<Vec<u8>, Vec<u32>>`
  row-list shape with an inline-first representation. Store the first
  build-row index inline; allocate overflow only for duplicate keys.
  Pre-size the hash map from `left_batch.num_rows()` or a conservative
  non-null estimate.
- **Batch-oriented build loop.** Downcast key arrays to concrete
  primitive Arrow types once at operator setup. Walk typed values
  directly for fast-path keys; per-row dynamic dispatch is removed
  on the hot path. Per-row (or per-small-batch) memory-reservation
  check is preserved — vectorized build does not bypass the
  spill-engagement decision point.
- **Exchange-partition hash separation.** ADR 003 §5 partition
  assignment `xxh3_64(canonical_encoded_bytes) mod partition_count`
  is **unchanged**. The join-build fast-path key is operator-local;
  helper names are semantic ("join build key" vs "exchange partition
  hash"), never a generic "fast hash" that blurs them.
- **Spill compatibility.** ADR 004 §7 spill behavior preserved
  byte-identically. The implementation may either keep the spilled
  build path on the canonical-byte map or teach the optimized build
  map to serialize deterministic canonical keys into encrypted spill
  pages. Either choice preserves `BudgetDimension::Memory` /
  `BudgetDimension::SpillBytes` accounting, encrypted-page cleanup,
  and typed terminal failures for cap exhaustion.
- **Worker-internal parallel build: out of scope.** Sharded or
  morsel-parallel build would introduce concurrent-insertion
  determinism, per-thread memory accounting, and allocator questions
  that need their own evidence. V1.x.
- **Profiling calibration.** Before implementation, capture a local
  profiler baseline against the 10M × 1M hash-partitioned join,
  preferably at 4 workers (middle of the 2/4/8 sweep), or a
  representative single-node fixture if distributed profiling is not
  practical. `samply`, `perf`, or an equivalent sampling profiler.
  The profile orders the three work items inside this ADR; it does
  not widen scope.
- **`build_ms` reduction target (default).** `≥30%` at 8 workers
  versus the ADR 004 baseline (`2581.550 ms → ≤1807.085 ms`). The
  implementation plan may pin a different number with council
  readout; the default holds otherwise.
- **Conditional ADR 006 rule (three bands).**
  - **Operator-adjacent:** `framework_overhead_ms` drops `≥50%` of
    the proportional reduction of `build_ms` → ADR 006 does **not**
    promote runtime-plumbing investigation.
  - **Flat:** `framework_overhead_ms` drops `≤20%` of proportional
    reduction → ADR 006 promotes with sharp scope on async-runtime
    poll cost, allocator churn beyond ADR 005's wins, lock contention
    beyond ADR 004's telemetry-lock bucket, and scheduler/registry
    plumbing.
  - **Gray zone (20% < drop < 50%):** brief council readout decides.
  - **Both fail to move:** ADR 005's optimization missed the real
    bottleneck; council reopens scope rather than treating residual
    plumbing as proven.
- **No new contract surface.** No new `MessageKind`, IR variant,
  `BudgetDimension`, task telemetry field, public API, or Wendelon
  scan contract. No new architecture guard by default; conditional
  escape if shared-helper extraction makes drift plausible (a
  `partition_key_hash` call-target grep guard).
- **Correctness preservation.** Output digest for the 10M × 1M
  canonical row remains
  `2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289`.

## Context

ADR 004 delivered hash-partitioned and bounded broadcast inner
equi-joins with bench telemetry that attributes task time across
shuffle pull/decode, build, probe, spill, output publish, batch
encode/send, and framework residual buckets.

The post-promotion 10M × 1M hash-partitioned rerun at 8 workers
(`task_wall_ms = 5416.577`) produced this attribution:

| Phase | Cumulative ms | % of wall |
|---|---:|---:|
| build | 2581.550 | 47.7% |
| framework_overhead (residual) | 2042.549 | 37.7% |
| output_publish | 201.890 | 3.7% |
| batch_send | 180.547 | 3.3% |
| probe | 162.982 | 3.0% |
| shuffle_pull_decode | 132.865 | 2.5% |
| batch_encode | 114.017 | 2.1% |
| task_setup | 0.177 | ~0% |
| telemetry_lock | 0.000 | ~0% |
| spill | 0.000 | 0% (no-spill workload) |

Probe + output combined is ~7% of wall, demoting the original
streaming push-based executor hypothesis. The dominant sharp bucket
is the hash-join build phase. The residual is real but unfocused
(38% after sub-attribution).

The current build path in `crates/toron/src/exec.rs`
(`execute_inner_join`) is intentionally simple:

- concatenate left input batches into one build `RecordBatch`;
- derive key arrays from join-key ordinals;
- for every build row, allocate a fresh canonical `Vec<u8>` key via
  `join_key_bytes`;
- insert into `HashMap<Vec<u8>, Vec<u32>>`;
- allocate a row-list `Vec<u32>` for every distinct key on first
  insertion;
- probe by building another fresh key `Vec<u8>` per probe row.

On a 10M-row mostly-unique build side, this shape creates allocator
pressure and scalar per-row work before the hash table has done any
meaningful relational work.

## Decision

ADR 005 optimizes the worker-local hash-join build path while
preserving ADR 004's external behavior. The implementation must
keep:

- `build = left` semantics from ADR 004;
- inner equi-join output semantics, including duplicate preservation;
- NULL join-key behavior: any NULL key component is non-matching;
- deterministic digest behavior across process loss and rerun;
- ADR 003 / 004 budget, spill, replay, and task-failure taxonomy;
- exchange partitioning semantics and `xxh3_64` canonical
  partition-key encoding.

The implementation may change private in-memory data structures
inside the join operator.

### 1. Join-key fast path

Introduce a private build/probe key representation that avoids
allocating a canonical byte vector for single-column fixed-width
primitive keys. The initial required fast path is:

- single-column `Int64` join keys, nullable or non-nullable;
- single-column `Int32` join keys, nullable or non-nullable.

The fast path must preserve the same equality domain as the generic
canonical encoder by including the logical type tag in the key
identity. It must not reinterpret raw primitive bytes in a way that
lets different logical types collide.

Eligibility is detected once per join at operator setup (not per
row): exactly one ordinal in each of `left_keys`/`right_keys`, plus
matching fixed-width primitive `DataType` on both sides. The
`DataType` is resolved from each child operator's output schema
indexed by the join-key `ColumnOrdinal` — the actual array type seen
by `execute_inner_join`, not the source-table type. So a projected
`Int32 → Int64` key classifies by its bound output `DataType`.

Composite keys plus single-column `Int8`, `Int16`, `UInt8`, `UInt16`,
`UInt32`, `UInt64`, `Float32`, `Float64`, and `Utf8` (the canonical
encoder's existing supported set) remain on the generic
canonical-byte path. `Date`/`Timestamp`/`Decimal` and any other type
not already canonical-encodable is out of scope — canonical encoder
extension is its own future work.

### 2. Allocation reduction

Replace `HashMap<Vec<u8>, Vec<u32>>` row-list shape with:

- **Inline-first row representation.** Store the first build-row
  index (a `u32`) inline in the hash-map value. Allocate overflow
  storage only for duplicate build keys.
- **Pre-sized hash map.** Pre-size from `left_batch.num_rows()` or
  a conservative non-null estimate. Avoids rehashing during the
  build loop.

A mostly-unique 10M-row build creates ~10M allocator allocations for
row-list `Vec<u32>` (one per distinct key). Inline-first storage
eliminates the dominant case entirely; overflow allocation only on
duplicates.

### 3. Batch-oriented build loop

- **Once-per-task downcast.** Downcast key arrays to their concrete
  primitive types once at operator setup, before the build/probe
  loops begin.
- **Typed inner loop.** Walk typed Arrow primitive values directly
  for fast-path keys. No per-row trait-object dispatch.
- **Per-row reservation preserved.** The memory-reservation check
  before each insertion remains per-row (or per-small-batch);
  batch-oriented build does not bypass the spill-engagement decision
  point (ADR 004 §7).
- **NULL-key filtering inlined.** NULL-keyed build rows are skipped;
  NULL-keyed probe rows produce no matches.

Worker-internal sharded or morsel-parallel build is **out of scope**
for this ADR. Concurrent insertion would introduce sharded-table
determinism, per-thread memory accounting, and allocator questions
that need their own evidence.

### 4. Spill compatibility

ADR 004 §7 spill behavior remains correct and typed. The
implementation may either:

- keep the spilled build path on the existing canonical-byte map for
  this ADR; or
- teach the optimized build map to serialize deterministic canonical
  keys into encrypted spill pages.

Either choice preserves:

- `BudgetDimension::Memory` accounting for resident spill state
  (bucket index, LRU cache, scratch);
- `BudgetDimension::SpillBytes` accounting for encrypted pages;
- encrypted spill page cleanup on task termination;
- typed terminal failures for cap exhaustion.

The no-spill 10M × 1M bench is the promotion workload; spill
regression is a hard correctness gate.

### 5. Exchange-partition hash separation

ADR 003 §5 partition assignment uses
`xxh3_64(canonical_encoded_bytes) mod partition_count` and is the
cross-system contract for fragments, manifests, partition pulls, and
worker replacement. ADR 005's fast-path key representation is
operator-local. It must not replace, wrap, or share a generic "fast
hash" helper with `partition_key_hash`. New helper names must make
the boundary explicit: join-build key identity is separate from
exchange-partition hashing.

### 6. Profiling calibration

Before implementation, capture a local profiler baseline against the
10M × 1M hash-partitioned join — preferably at 4 workers (middle of
the 2/4/8 sweep), or a representative single-node in-memory fixture
if distributed profiling is not practical. `samply`, `perf`, or an
equivalent sampling profiler is sufficient. The bench-results
artifact records the profiler command, machine note, dominant hot
path, and (optionally) a flamegraph/profile file reference.

The profiler does **not** widen the ADR scope. It orders the
already-selected work:

- key encoding and hashing dominant → prioritize the fixed-width
  fast path;
- allocator / `Vec` growth dominant → prioritize row-list and
  hash-map layout;
- cache misses / branch misses dominant → prioritize hash table
  layout;
- diffuse profile → all three Phase 1/2 work items are reasonable
  to ship together; escalate to council only on a surprising
  dominator.

## Hard gates

ADR 005 is acceptable only if all of these hold. The gates split into
**correctness preservation** and **bench evidence**.

### Correctness preservation

1. `cargo test -p toron` passes.
2. `cargo test --workspace` passes.
3. **Existing architecture guards G1-G18 pass unchanged.** No new
   guard is added by default; conditional escape per hard gate 14.
4. ADR 004 distributed process SIGKILL replay tests produce
   byte-identical normalized digests.
5. Hash-partitioned and broadcast ADR 004 correctness fixtures
   remain digest-identical. Output digest for the 10M × 1M canonical
   row remains
   `2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289`.
6. NULL join keys still do not match, including under the fast path.
7. **Type-tag invariant.** `Int32(1)` and `Int64(1)` join keys are
   distinct in the build hash table. Unit-test enforced.
8. Duplicate build keys still emit all matching rows in stable
   probe-driven order.
9. Composite join keys remain supported through the generic path.
10. `partition_key_hash` and exchange bucket assignment remain
    canonical and unchanged for **all** ADR 004 supported key types
    (not just the fast-path set).
11. Spill engagement, spill exhaustion, and resident-spill memory-cap
    tests remain typed and terminal.
12. No public API, wire, scheduler, task identity, `TaskInputBinding`,
    or Wendelon scan contract changes.
13. **No new contract surface.** No new `MessageKind`, IR variant,
    `BudgetDimension`, task telemetry field, or Wendelon scan
    contract. Semantic identifier names per
    `no-planning-phase-names.md`.
14. **No new architecture guard required**, unless implementation
    extracts a shared hash-helper surface that makes
    exchange-partition hash drift plausible. In that case a
    `partition_key_hash` call-target grep guard is added in
    `tests/architecture_test.rs`. Default expectation: no guard
    needed.

### Bench evidence

15. **Rerun digest equality.** ADR 004 10M × 1M hash-partitioned
    join rerun at 2/4/8 workers produces the same digest as the
    baseline.
16. **`build_ms` reduction.** Default target is `≥30%` reduction at
    8 workers versus the ADR 004 baseline
    (`2581.550 ms → ≤1807.085 ms`). A miss with isolated real
    reduction is acceptable if the bench artifact records the gap
    explicitly (no softening into a vague "improved" claim) and the
    council decides whether to accept the narrower win or reopen
    scope. Digest equality remains stronger than a single timing
    number. A wall-clock-only improvement without `build_ms`
    movement is **not** sufficient.

### Soft gates

- Worker-count scaling improves; speedup per doubling approaches 1.5×.
- `framework_overhead_ms` delta recorded (resolves conditional
  ADR 006 rule).
- Computed build rows/sec recorded in the bench artifact
  (`telemetry_build_rows / build_ms`) for both baseline and
  post-change rows.
- Post-optimization profiler note archived alongside bench artifact;
  flamegraph file reference if available.

## Bench evidence

The promotion bench must rerun ADR 004's 10M × 1M hash-partitioned
2/4/8 worker sweep:

```text
cargo bench -p toron --bench distributed_inner_equi_join \
    -- --distributed-inner-equi-join-only --strategy=hash \
    --workers=2,4,8 --build-rows=10000000 --probe-rows=1000000
```

The bench artifact at
`plans/impl/005_hash_join_build_path_efficiency_bench_results.md`
must record, at minimum:

- ADR 004 baseline table (2/4/8 workers);
- ADR 005 post-change rows (2/4/8 workers);
- digest comparison;
- `build_ms`, `framework_overhead_ms`, `wall_ms`, `task_wall_ms`,
  `shuffle_pull_decode_ms`, `probe_ms`, `output_publish_ms`,
  `batch_encode_ms`, `batch_send_ms` for each row;
- explicit 8-worker comparison against `≤1807.085 ms` (hit/miss with
  actual delta);
- computed build rows/sec (`telemetry_build_rows / build_ms`) for
  baseline and post-change rows;
- profiler command, machine note, dominant hot path (baseline and
  post-change), flamegraph/profile file references when available;
- short interpretation: did residual framework overhead move with
  build work, or stay flat?
- explicit conditional ADR 006 band: operator-adjacent / gray zone /
  flat / miss+reopen if neither build nor residual moved.

The primary success signal is a clear reduction in `build_ms` versus
the baseline. Wall-clock should improve or remain within bench noise.
`framework_overhead_ms` is diagnostic, not primary. The load-bearing
comparison is phase attribution plus digest equality.

## Conditional ADR 006 rule

The bench rerun resolves the residual interpretation. Three bands,
pinned at ADR level to avoid council mini-session on the readout:

- **Operator-adjacent (≥50%).** `framework_overhead_ms` drops at
  least 50% of the proportional reduction of `build_ms` (e.g., if
  `build_ms` drops 30%, residual drops ≥15%). The previous residual
  included operator-adjacent allocation/runtime work. ADR 006 does
  **not** promote a runtime-plumbing investigation.
- **Flat (≤20%).** `framework_overhead_ms` drops at most 20% of the
  proportional reduction. The residual is genuine runtime plumbing.
  ADR 006 **promotes** with sharp scope on async-runtime poll cost,
  allocator churn beyond ADR 005's wins, lock contention beyond
  ADR 004's telemetry-lock bucket, and scheduler/registry plumbing.
- **Gray zone (20% < drop < 50%).** Brief council readout decides
  the ADR 006 framing.
- **Both fail to move.** ADR 005's attempted optimization did not
  hit the real bottleneck; the council reopens scope rather than
  treating residual plumbing as proven.

## Rationale

### Why fast-path eligibility is conservative

`Int64` and `Int32` cover the first-order analytical join-key cases:
primary keys, foreign keys, integer IDs, and integer-encoded
dimensions. Each extension adds a parity-test obligation and a
logical-type-tag verification. V1 commits to the two that matter
most; Phase 0 profiler evidence may justify extending cleanly within
this ADR, but the default contract stays narrow.

### Why the type-tag invariant is load-bearing

The fast path bypasses canonical byte encoding. If the encoder hashes
raw primitive bytes without distinguishing logical type, `Int32(1)`
produces the same hash as `Int64(1)` (both are little-endian
`01 00 00 00 ...`). A join on mixed-type columns (rare but legal:
same *data* type, different *logical* types from coercion or
explicit `CAST`) would silently produce wrong matches. The invariant:
fast-path key identity includes the logical type — via type
discriminant in the hash input or a typed enum variant in equality.

### Why allocation reduction is its own scope item

A 10M-row mostly-unique build creates ~10M allocator allocations for
row-list `Vec<u32>` (one per distinct key). Inline-first storage
removes that cost for the dominant one-row-per-key case while keeping
duplicate preservation precise. This is a data-structure choice
orthogonal to whether the build loop is per-row or batch-oriented;
both wins compose.

### Why exchange-partition hash stays separate

ADR 003 §5 pinned `xxh3_64(canonical_encoded_bytes)` for partition
assignment. Workers, fragments, manifests, and the partition store
all depend on consistent partition assignment. The join-build hash
is operator-local; mixing the two via a "shared fast hash" helper
would invite drift.

### Why no new architecture guard by default

ADR 005 adds no new contract surface. The type-tag invariant is
unit-test enforced. The exchange-partition separation is a
non-action. The fast-path eligibility set is enum-bounded and
grep-checkable at impl-plan level. The conditional escape (hard
gate 14) covers the case where a shared helper extraction makes
drift plausible.

### Why streaming push-based executor is rejected outright

Probe + output combined at ~7% of wall (post-ADR-004 telemetry). A
Volcano → push-based rewrite is a substantial substrate change
addressing ~0% of measured bottleneck. "Rejected" (not "deferred")
because the evidence shape explicitly demoted this candidate. If a
future workload shows probe-side dominance, the rejection unblocks.

### Why no new telemetry

ADR 004 already shipped the required columns. Adding more telemetry
now would turn ADR 005 back into attribution work. The selected
bucket is already sharp enough: `build_ms`.

## What Not To Do

- **Do not change `partition_key_hash` or any exchange-partition
  path.** ADR 003 §5 contract holds.
- **Do not let fast-path encoding lose the logical-type tag.**
  `Int32(1)` and `Int64(1)` must remain distinct build-side keys.
- **Do not regress digest equality** on the ADR 004 fixtures.
- **Do not bypass the per-row memory-reservation check during
  batch-oriented insertion.** Spill engagement decision preserved.
- **Do not parallelize hash-table build inside a worker.** V1.x.
- **Do not introduce new architecture guards** unless shared-helper
  extraction makes drift plausible.
- **Do not bundle runtime-plumbing investigation into ADR 005.**
  Conditional ADR 006 is the right placement.
- **Do not rewrite the executor model.** Rejected outright.
- **Do not name helper APIs generically** (e.g., "fast hash") in a
  way that blurs join-build key identity and exchange-partition
  hashing.
- **Do not treat a wall-clock-only improvement as sufficient** —
  digest equality and phase attribution are stronger than a single
  timing number; `build_ms` must move.
- **Do not soften a `build_ms` target miss** into a vague
  "improved" narrative in the bench artifact.
- **Do not skip the 10M × 1M 2/4/8 rerun.**

## Rejected and Deferred

### Streaming push-based executor

Rejected outright. Probe + output is ~7% of wall; rewriting the
Volcano model addresses ~0% of the measured bottleneck. If a future
workload shows probe-side dominance, the rejection unblocks.

### Residual runtime plumbing

Deferred to conditional ADR 006. After ADR 004's sub-attribution
moved batch encode/send, decode, dispatch wait, and telemetry-lock
out of the residual, the remaining 38% is still too broad for a
narrow ADR. The conditional rule above resolves whether ADR 006
promotes runtime-plumbing investigation.

### Runtime / Bloom filter pushdown

Deferred. Runtime filters may be valuable, but ADR 004's scaling
evidence points first at local build-path cost, not at Wendelon-side
probe reduction. Cross-ring contract surface deserves its own ADR.

### Concurrent / sharded build

Deferred to V1.x. Worker-internal concurrent build adds
sharded-table determinism, per-thread memory accounting, and
allocator questions that need their own evidence.

### Hash function change

Rejected. Exchange partition assignment is pinned to `xxh3_64` over
canonical key bytes. Any operator-local hash-table choice must
preserve that contract boundary.

### Outer / semi / anti / sort-merge / non-equi joins

Out of scope. Each is its own future ADR.

### Probe-side vectorization

Deferred. Probe at 3% of wall; not worth scope.

### Output-publish optimization

Deferred. Output at 3.7% of wall.

### Fast-path extension beyond `Int64`/`Int32`

Deferred to V1.x or own follow-on ADR with parity-test evidence,
unless Phase 0 profiler evidence justifies extension within this
ADR.

### JIT-compiled hash kernels (Brainstorm 001)

Reserved for post-V1 deliberation when bench evidence quantifies
the interpretation overhead.

## Council decisions

Three-way convergence at iter-4. Iteration artifacts retired (git
holds the narrative).

1. **ADR 005 = Hash-Join Build Path Efficiency.** Three-way iter-0
   from ADR 004 post-promotion telemetry.
2. **Scope: three work items** (fast-path encoding, allocation
   reduction, batch-oriented build loop). Three-way iter-1.
3. **Fast-path eligibility V1: `Int64` and `Int32` only.**
   Conservative default; extension within this ADR allowed only if
   Phase 0 evidence justifies and parity tests are cheap.
4. **Type-tag invariant: load-bearing** (hard gate 7, unit-test
   enforced).
5. **Exchange-partition hash separation preserved** (hard gate 10).
6. **Streaming push-based executor: rejected outright.**
7. **Runtime residual plumbing: deferred to conditional ADR 006.**
8. **No new instrumentation / no new contract surface.** Hard gates
   12-13.
9. **G1-G18 architecture guards pass unchanged.** Hard gate 3.
10. **Layout choice: Phase 0 calibration, not ADR text.**
11. **`build_ms` reduction threshold: default `≥30%` at 8 workers**
    (hard gate 16).
12. **Conditional ADR 006 rule: three bands** (≥50%
    operator-adjacent; ≤20% flat; 20-50% gray).
13. **Profiling calibration in ADR body** (§6), with capture
    preferably at 4 workers or a representative single-node fixture.
14. **Conditional architecture guard** (hard gate 14) — no new
    guard unless shared-helper extraction makes drift plausible.
15. **Wall-clock-only improvement is not sufficient** (hard gate
    16; What Not To Do).
16. **Miss-recording discipline** — bench artifact records gaps
    explicitly, no softening narrative.
17. **Worker-internal parallel build out of scope** with explicit
    reasoning (§3).
18. **Bench artifact records dominant-hot-path note** as primary
    profiling evidence; flamegraph reference optional.
