# Implementation Plan: ADR 005 — Hash-Join Build Path Efficiency

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion ADR:** `plans/adr/005_hash_join_build_path_efficiency.md`
**Bench-results artifact (post-promotion):**
`plans/impl/005_hash_join_build_path_efficiency_bench_results.md`

## Inherits substrate

- ADR 001 / 002 / 003 / 004 promoted contracts; all hard gates and
  architecture guards G1-G18 must continue to pass byte-identically.
- `plans/impl/004_distributed_inner_equi_join_bench_results.md`
  records the post-promotion telemetry rerun rows that serve as the
  ADR 005 baseline for comparison.
- `crates/toron/src/exec.rs` (function `execute_inner_join`) — the
  current build path lives here.
- `crates/toron/src/distributed/task.rs` — `TaskTelemetryPhases`
  shipped by the ADR 004 telemetry slice. ADR 005 reads existing
  columns; adds none.
- `crates/toron/benches/distributed_inner_equi_join.rs` — bench
  harness already records the columns ADR 005 needs.
- `crates/wendelon/plans/conventions/` — semantic names,
  newtype-units, no-premature-backward-compat,
  no-planning-phase-names.

## Goal

Reduce ADR 004's dominant `build_ms` bucket for hash-partitioned
inner equi-joins by removing avoidable allocation and scalar dispatch
in the worker-local hash-join build path, without changing join
semantics, distributed runtime contracts, or spill failure taxonomy.

After ADR 005, cumulative `build_ms` for the 10M × 1M
hash-partitioned `INNER JOIN` at 8 workers is statistically
significantly lower than `2581.550 ms` (default target
`≤1807.085 ms`, a `≥30%` reduction). All ADR 004 correctness
fixtures pass byte-identically. The post-rerun
`framework_overhead_ms` delta resolves the conditional ADR 006
promotion rule.

## Baseline (from ADR 004 telemetry rerun)

10M × 1M hash-partitioned join, framework-overhead sub-attribution
rerun. Rows ADR 005 compares against:

| Workers | build_ms | framework_overhead_ms | wall_ms |
|---|---:|---:|---:|
| 2 | 3199.407 | 2370.266 | 6944.653 |
| 4 | 2916.464 | 2224.603 | 6225.153 |
| 8 | 2581.550 | 2042.549 | 5451.372 |

Output digest (preserved unchanged):
`2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289`.

Build rows/sec is **not** pre-pinned here. The bench artifact
computes both baseline and post-change throughput from
`telemetry_build_rows / build_ms` per worker count.

## Implementation contract

- The build-side hash table internals in `execute_inner_join` may be
  rewritten to reduce per-row allocation and dispatch.
- The fast-path key representation produces **equality + output
  parity** with the generic canonical path for the inputs it accepts
  (same matched-row sets, same probe-driven order). It does **not**
  need byte-identical hash bytes — the operator-local hash is
  separate from the canonical exchange-partition hash by design
  (§Decision.5 in the ADR). The fast path **preserves logical-type
  identity**: `Int32(1)` and `Int64(1)` are distinct build-side keys.
- The generic canonical-byte path remains the fallback for composite
  keys plus single-column key types **already supported by the
  canonical encoder**: `Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`,
  `UInt64`, `Float32`, `Float64`, `Utf8`. ADR 005 does **not** add
  canonical encoder support for new types; `Date`/`Timestamp`/
  `Decimal` etc. remain unsupported by the join-key path, with the
  same typed error
  (`ExecutionError::InvalidExpression { message: "unsupported ..." }`)
  the encoder already produces.
- Exchange-partition assignment (`partition_key_hash`, `xxh3_64`,
  ADR 003 §5) is **unchanged**. Build-path fast hashing is
  operator-local; the two paths are named by semantic purpose, not
  by a shared "fast hash" helper.
- ADR 004 §7 spill invariants preserved byte-identically: spill
  engagement triggers, bucket → page-range index, memory-budget
  accounting for in-memory state, spill-bytes accounting for
  encrypted pages, typed terminal failures, cleanup on task
  termination.
- The post-ADR-005 rerun records `build_ms`, `framework_overhead_ms`,
  `wall_ms` for 10M × 1M 2/4/8 workers; compares against ADR 004
  baseline above.
- No new `MessageKind`, IR variant, wire surface, architecture
  guard, `BudgetDimension`, task telemetry field, public API, or
  Wendelon scan contract.

## Phase plan

### Phase 0 — Profiling, baseline pinning, calibration

No production code. Outputs documented in the bench-results
artifact's Phase 0 audit section.

- **P0.1: Profiler tooling.** Recommended default `samply`
  (lightweight, cross-platform, Firefox-profiler-compatible output).
  Fallback `perf` + `flamegraph` if `samply` doesn't fit the harness.
  The artifact needs the profiler command, machine note, profile-file
  reference or summary, and the dominant hot path. The profiling
  fixture and command must be reproducible enough to rerun after
  implementation; the post-change profile reuses the same shape
  where possible.
- **P0.2: Baseline profile capture.** Profile the 10M × 1M
  hash-partitioned join at 4 workers (middle of the 2/4/8 sweep), or
  a representative single-node fixture if distributed profiling is
  not practical. Identifies what dominates `build_ms`:
  - Key encoding / hashing dominant → prioritize fast-path encoding.
  - Allocator / `Vec` growth dominant → prioritize row-list and
    map layout (inline-first, pre-sized).
  - Cache misses / branch misses dominant → prioritize hash table
    layout.
  - Diffuse profile → all three Phase 1/2 work items are reasonable
    to ship together; escalate to council only on a surprising
    dominator.
- **P0.3: Baseline pinned.** Record the baseline table above in the
  bench-results artifact. Do not hardcode rows/sec; compute in the
  artifact from `telemetry_build_rows / build_ms`.
- **P0.4: `build_ms` reduction threshold + conditional ADR 006
  thresholds.** Default thresholds (override only with council
  readout):
  - **`build_ms` reduction target.** `≥30%` at 8 workers
    (`2581.550 → ≤1807.085 ms`).
  - **"Operator-adjacent" residual drop.** `framework_overhead_ms`
    drops `≥50%` of the proportional reduction of `build_ms` →
    ADR 006 does not promote a runtime-plumbing investigation.
  - **"Flat" residual.** `framework_overhead_ms` drops `≤20%` of
    proportional reduction → ADR 006 promotes with sharp scope.
  - **Gray zone (20% < drop < 50%).** Brief council readout
    interprets.
- **P0.5: Fast-path eligibility set.** V1 contract: single-column
  `Int64` and `Int32`. Phase 0 may evaluate whether to extend within
  this ADR to other primitive integer types **already supported by
  the canonical encoder** (`Int8`, `Int16`, `UInt8`, `UInt16`,
  `UInt32`, `UInt64`) based on profiler evidence + parity-test cost.
  Default: ship `Int64/Int32` only. `Date`/`Timestamp`/`Decimal`
  and other not-yet-canonical-encodable types are explicitly out of
  scope — canonical encoder extension is its own future work
  (`Float32`/`Float64` join keys are also deferred per the floating-
  point canonical-encoding ADR).
- **P0.6: Hash table layout decision.** Pick one layout candidate
  (open addressing with linear probing, Robin Hood probing,
  cache-aligned bucket chaining, or two-level radix) based on
  profiler output. Do not pre-pin an algorithm before profiling.
  Keep the layout private to the join operator — not a generic
  substrate helper. An A/B candidate is allowed if profiler evidence
  is ambiguous; the bench rerun resolves.
- **P0.7: Rerun harness command pinned.**
  ```
  cargo bench -p toron --bench distributed_inner_equi_join \
      -- --distributed-inner-equi-join-only --strategy=hash \
      --workers=2,4,8 --build-rows=10000000 --probe-rows=1000000
  ```

### Phase 1 — Join-key fast path + allocation reduction

Files:
- `crates/toron/src/exec.rs` (extend `execute_inner_join`).
- `crates/toron/src/exec/hash_key.rs` (new, optional) — if the
  fast-path encoder grows beyond a few inline lines.
- `crates/toron/tests/hash_join_fast_path_parity_test.rs` (new) —
  fast-path output/equality parity tests.

Work:

**A — Join-key fast path:**

- Introduce a private build/probe key representation. Likely shape:
  `enum JoinKey { Int64(i64), Int32(i32), Generic(Vec<u8>) }` where
  `Int64`/`Int32` variants embed logical type in discriminant —
  `Int32(1)` and `Int64(1)` are distinct by `PartialEq`/`Hash`.
  `Generic` carries canonical-encoded bytes for non-fast-path keys.
  A `JoinKeyRef<'_>` borrowed counterpart enables probe-side lookup
  without allocation on fast-path keys.
- Detect fast-path eligibility once per join at operator setup (not
  per row): exactly one ordinal in each of `left_keys` /
  `right_keys` (both are `Vec<ColumnOrdinal>` on
  `ExecutionPlan::Join`), plus matching fixed-width primitive
  `DataType` on both sides. The `DataType` is resolved from each
  child operator's output schema indexed by the join-key ordinal —
  the actual key-array `DataType` seen by `execute_inner_join`, not
  the source-table type. A projected/coerced `Int32 → Int64` key
  classifies by its bound output `DataType`.
- Downcast key arrays to concrete primitive Arrow types once before
  the build/probe loops begin.
- For fast-path keys, build directly from typed Arrow primitive
  values; skip rows whose validity bitmap marks the key NULL.
- For generic keys, keep `join_key_bytes` unchanged.

**B — Allocation reduction:**

- Replace `HashMap<Vec<u8>, Vec<u32>>` row-list shape with
  inline-first: store first row index inline; allocate overflow
  `Vec<u32>` only for duplicate keys.
- Pre-size hash map from `left_batch.num_rows()` (or conservative
  non-null estimate) before insertion begins.
- The hash map's key type becomes the new `JoinKey` representation.
  For fast-path joins this is a small enum variant (no allocation
  per insert); for generic joins it's still `Vec<u8>` but only paid
  on the generic path.

Tests (in `hash_join_fast_path_parity_test.rs` unless noted):

- `fast_path_parity_int64` — fast-path produces the same matched-row
  set and probe-driven order as the generic canonical path for
  `Int64` inputs (output parity, not hash-byte parity — the
  operator-local hash is separate from the canonical encoder by
  design).
- `fast_path_parity_int32` — same for `Int32` (includes duplicate
  build keys and NULLs).
- `fast_path_type_tag_invariant_int32_vs_int64_distinct` —
  `JoinKey::Int32(1)` and `JoinKey::Int64(1)` produce distinct hash
  + equality. **Critical correctness test.**
- `fast_path_disabled_for_multi_column_key` — `(region,
  customer_id)` composite goes through generic path byte-identically
  (reuses ADR 004 composite-key fixture).
- `fast_path_disabled_for_utf8_key` — UTF-8 string key goes through
  generic path.
- `fast_path_disabled_for_unsupported_primitive` — `UInt64` (or
  whichever types are excluded by P0.5) goes through generic path.
- `fast_path_eligibility_uses_child_output_schema` — a projected /
  coerced `Int32 → Int64` key classifies as `Int64` per the child
  operator's output `DataType` (resolved from the schema indexed by
  the join-key `ColumnOrdinal`), not the source-table type.
- `fast_path_preserves_null_key_drop` — NULL-keyed build rows
  skipped; NULL-keyed probe rows produce no matches.
- `fast_path_preserves_duplicate_preservation` — duplicate build
  keys emit all matching rows in stable probe-driven order.
- `row_list_inline_first_no_overflow_for_unique_keys` — unique-key
  build avoids overflow allocation entirely.
- `row_list_overflow_for_duplicate_keys` — duplicate keys allocate
  overflow and preserve insertion order.
- `hash_map_pre_size_avoids_rehash` — observable via no rehash
  events during build on a known-cardinality fixture.
- `partition_assignment_unchanged_for_fast_path_types` —
  `partition_key_hash` for `Int64`/`Int32` keys remains
  byte-identical to the canonical path. **Critical separation
  test.**

### Phase 2 — Batch-oriented build loop + hash table layout

Files:
- `crates/toron/src/exec.rs` (rewrite build loop internals).
- `crates/toron/src/exec/hash_table.rs` (new, optional) — if the
  layout grows beyond a single struct.

Work:

- Exploit the once-per-task downcast from Phase 1: walk typed Arrow
  primitive values directly for fast-path keys; no per-row
  trait-object dispatch.
- Preserve per-row reservation check before insertion. A
  per-small-batch reservation is acceptable if it remains
  conservative enough to trigger spill before exceeding
  `memory_per_task_bytes`; batch-oriented build does not bypass
  the spill-engagement decision point (ADR 004 §7).
- Preserve per-row NULL filtering (inline validity-bitmap check).
- Implement P0.6's chosen layout. The layout is private to the join
  operator. Memory accounting for the new layout's resident state
  (bucket index, scratch, any caches) reserves against
  `memory_per_task_bytes` per ADR 004 §7.
- Adjust `estimated_build_hash_bytes` (or equivalent pre-flight
  memory estimate) for the new layout. The estimate must remain
  conservative — over-estimate triggers spill earlier (safe);
  under-estimate could exceed `memory_per_task_bytes` silently
  (unsafe).
- **Do not parallelize the build table inside a worker in this
  ADR.** Sharded concurrent insertion needs its own determinism and
  budget accounting work; deferred to V1.x.

Tests:

- `batch_build_loop_processes_arrow_arrays_directly` — no per-row
  `downcast` calls during the build loop on a fast-path fixture
  (call-counter or compilation-style assertion; falls back to
  timing comparison if a counter is awkward).
- `hash_table_layout_round_trip` — insert N rows; read back same
  rows with mixed unique and duplicate keys.
- `hash_table_layout_memory_accounting` — resident state sum within
  configured cap; over-cap promotes to spill.
- `batch_build_preserves_null_key_semantics` — mixed null/non-null
  batch; only non-null-keyed rows enter the table.
- `batch_build_spill_engagement` — mid-batch insertion that would
  exceed memory cap triggers spill promotion; bucket state after
  promotion is correct.
- `batch_build_digest_stable` — same input produces digest-equal
  output after canonical normalization across multiple runs.
- `fast_path_spill_digest_matches_in_memory` — if the fast-path
  representation can reach the spill writer, spilled and in-memory
  execution produce the same normalized digest for an `Int64`
  single-key fixture.
- **Every existing ADR 004 hash-join test reruns and passes
  byte-identically**: spill engagement, spill exhaustion, NULL
  semantics, multi-column key, broadcast strategy. Load-bearing
  regression check.

### Phase 3 — Spill preservation

Files:
- `crates/toron/src/exec.rs`

Work:

- Keep the existing spilled path correct while the in-memory fast
  path lands.
- If optimized storage is shared with spill, provide deterministic
  canonical serialization before writing encrypted spill pages.
- Revisit `estimated_build_hash_bytes` so the spill decision does
  not undercount the new in-memory layout.
- Keep cache/index/writer scratch accounting under ADR 004's
  memory rules.

Tests:

- Existing spill tests remain hard gates:
  - `execute_bound_inner_join_spills_build_when_memory_cap_is_low`
  - `execute_bound_inner_join_spill_cap_exhaustion_is_typed`
  - `execute_bound_inner_join_spill_index_counts_against_memory_cap`
- Add `fast_path_spill_digest_matches_in_memory` (also listed in
  Phase 2) if the fast-path representation can reach the spill
  writer.

### Phase 4 — Exchange hash compatibility

Files:
- `crates/toron/src/exec.rs`
- `crates/toron/tests/architecture_test.rs` if a structural guard
  becomes warranted.

Work:

- Do not replace `partition_key_hash` with the join-build hash-table
  representation.
- Keep exchange partition assignment tied to canonical
  `encode_partition_key_value` and `xxh3_64`.
- If helper extraction creates a tempting shared hash API, name the
  APIs by semantic purpose: "exchange partition hash" vs "join
  build key" — never a generic "fast hash" that blurs the contracts.

Tests:

- Keep `exchange_partition_bucket_uses_stable_xxh3_64_encoding`.
- Add a regression test if new helper boundaries make it possible
  for join fast-path hashing to affect exchange partitioning.
- If a shared helper is introduced, add an architecture-test grep
  guard proving `partition_key_hash` still calls canonical
  `encode_partition_key_value` rather than the join-build fast path
  (ADR hard gate 14's conditional escape).

### Phase 5 — Telemetry rerun + promotion

Files:
- `plans/impl/005_hash_join_build_path_efficiency_bench_results.md`
  (new) — bench artifact.

Work:

- Run the full hard-gate suite:
  ```
  cargo fmt -p toron --check
  cargo check --workspace
  cargo test -p toron
  cargo test -p toron --test architecture_test
  cargo test -p toron --test distributed_process_test
  cargo test -p toron --test wendelon_storage_execution_test
  cargo test -p toron --test hash_join_fast_path_parity_test
  cargo test --workspace
  cargo bench -p toron --bench distributed_inner_equi_join --no-run
  cargo bench -p toron --bench distributed_inner_equi_join -- --quick --distributed-inner-equi-join-only
  git diff --check
  ```
  Notes:
  - `cargo test -p toron` exercises the in-crate join failure /
    determinism / spill tests in `crates/toron/src/exec.rs`
    (`execute_bound_inner_join_*`), including
    `execute_bound_inner_join_spills_build_when_memory_cap_is_low`,
    `execute_bound_inner_join_spill_cap_exhaustion_is_typed`, and
    `execute_bound_inner_join_spill_index_counts_against_memory_cap`
    (ADR 004 Phase 5 gates).
  - Architecture guards G1-G18 must pass unchanged via
    `architecture_test` (ADR hard gate 3).
  - `distributed_process_test` covers the SIGKILL replay path
    (ADR hard gate 4).
  - `hash_join_fast_path_parity_test` is the Phase 1 new test file
    introduced by ADR 005.
- Re-run the promotion-scale bench (P0.7).
- Record post-ADR-005 rerun rows alongside the ADR 004 baseline.
- Capture post-optimization profiler note (same profiler / fixture
  as P0.2). Flamegraph file reference if available.
- Compute and record deltas in the bench artifact:
  - Δ`build_ms` per worker count (target ≥30% at 8 workers per P0.4).
  - Δ`framework_overhead_ms` per worker count (resolves conditional
    ADR 006).
  - Δ`wall_ms` per worker count.
  - Computed build rows/sec (`telemetry_build_rows / build_ms`) for
    both baseline and post-change rows.
  - Speedup per doubling vs baseline.
- Write the conditional ADR 006 readout:
  - "Operator-adjacent" (≥50% proportional residual drop) →
    ADR 006 doesn't promote.
  - "Flat" (≤20% proportional residual drop) → ADR 006 promotes
    with sharp scope.
  - "Gray zone" → brief council session.

Bench artifact required contents:

- ADR 004 baseline table (2/4/8 workers).
- ADR 005 post-change rows (2/4/8 workers).
- Digest comparison.
- `build_ms`, `framework_overhead_ms`, `wall_ms` delta table.
- Explicit 8-worker comparison against `≤1807.085 ms` target (hit
  or miss; if miss, the actual delta is recorded — no softening
  into a vague "improved" narrative).
- Computed build rows/sec for baseline and post-change rows.
- Profiler command, machine note, baseline and post-change profile
  notes (dominant hot path each).
- Short interpretation: did residual framework overhead move with
  build work, or stay flat?
- Explicit conditional ADR 006 band: operator-adjacent / gray
  zone / flat / miss+reopen if neither build nor residual moved.

Suggested commit slices:

1. `chore: adr 005 prep — roadmap honesty + Phase 0 profiling notes`
2. `plan: adr 005 toron hash-join build path efficiency`
3. `feat: adr 005 phase 1 — join-key fast path + allocation
   reduction`
4. `feat: adr 005 phase 2 — batch-oriented build loop + hash table
   layout`
5. `feat: adr 005 phase 3 — telemetry rerun + promotion` (lands
   `bench_results.md`; resolves conditional ADR 006; marks ADR 005
   shipped on roadmap)

If hard gates pass and `build_ms` reduction hits the calibrated
threshold, promote. If reduction misses:

1. **Accept narrower reduction.** Record actual delta; close ADR
   005 with measured win; build stays on soft-gate watch list.
2. **Scope down.** Ship Phase 1 only (fast-path + allocation
   reduction); defer Phase 2 to ADR 005.x.
3. **Shelve.** Revert; document the gap; reopen when evidence
   redirects.

## Files touched

Implementation:

- `crates/toron/src/exec.rs` — `execute_inner_join` build/probe
  rewrite; private `JoinKey` representation; inline-first row-list;
  pre-sized hash map; batch-oriented inner loop; layout integration.
- `crates/toron/src/exec/hash_key.rs` (new, optional) — fast-path
  key representation if it grows beyond inline.
- `crates/toron/src/exec/hash_table.rs` (new, optional) — layout
  module if it grows beyond inline.
- `crates/toron/tests/hash_join_fast_path_parity_test.rs` (new) —
  Phase 1 parity tests.
- (Possibly) `crates/toron/tests/hash_join_layout_test.rs` (new) —
  Phase 2 layout tests, if a separate module warrants its own test
  file.

Bench / evidence:

- `crates/toron/benches/distributed_inner_equi_join.rs` — no change
  required (ADR 004 telemetry columns are already there).
- `plans/impl/005_hash_join_build_path_efficiency_bench_results.md`
  (new) — Phase 5 artifact.

Roadmap:

- `plans/wip/roadmap.md` — record ADR 005 promotion; resolve
  conditional ADR 006 rule; reframe ADR 006 candidates per rerun
  outcome.

## Out of scope (V1.x or later)

- Concurrent / parallel hash table build (worker-internal morsel
  parallelism).
- JIT-compiled hash kernels (Brainstorm 001).
- Runtime / Bloom filter pushdown (cross-ring Wendelon contract).
- Outer / semi / anti / sort-merge / non-equi joins.
- Multi-way join planning (requires cost model).
- Cost-based build/probe swap (requires Wendelon stats + Toron
  cost model).
- Streaming push-based executor model rewrite (rejected outright).
- Hash spill optimization (page size, cache policy, demotion).
- Probe-side vectorization (probe at 3% of wall).
- Output-publish optimization (output at 3.7% of wall).
- Fast-path extension beyond `Int64`/`Int32` if Phase 0 doesn't
  justify it in this ADR.
- Runtime plumbing investigation — conditional ADR 006.

## Risks and mitigations

- **R1: Type-tag invariant drift.** Fast-path key encoding collapses
  logical types on equality (e.g., a refactor hashes raw primitive
  bytes without type discriminant).
  *Mitigation:*
  `fast_path_type_tag_invariant_int32_vs_int64_distinct` unit test;
  CI runs it on every commit.

- **R2: Exchange-partition hash drift.** A "fast hash" helper
  extracted from build-path code accidentally gets used by
  `partition_key_hash`, breaking ADR 003 §5 partition assignment
  consistency.
  *Mitigation:*
  `partition_assignment_unchanged_for_fast_path_types` test;
  semantic helper names ("join build key" vs "exchange partition
  hash"); architecture-test grep guard if helper extraction makes
  it temptingly easy (ADR hard gate 14's conditional escape).

- **R3: Memory under-accounting.** New layout's resident state
  (bucket index, scratch, caches) leaks bytes past the per-task
  memory cap because `estimated_build_hash_bytes` is not updated
  for the new shape.
  *Mitigation:* ADR 004 Phase 5 memory accounting tests rerun on
  the new layout; Phase 2 includes explicit estimate update;
  over-estimate is the safe direction.

- **R4: Spill behavior regression.** Optimized in-memory store
  reaches the spill writer without producing the canonical key
  bytes the spill format expects.
  *Mitigation:* either keep spilled path on canonical bytes (Phase
  1 option), or serialize canonical bytes before write (Phase 2
  option). `fast_path_spill_digest_matches_in_memory` test; ADR
  004 spill tests remain hard gates.

- **R5: Benchmark overfitting to Int64.** The 10M × 1M fixture is
  Int64-heavy; ADR 005 might optimize for Int64 in a way that
  regresses composite or string key joins.
  *Mitigation:* generic path is preserved unchanged; the
  composite-key fixture from ADR 004 hard gate 7 must continue
  passing byte-identically.

- **R6: `build_ms` reduction misses threshold.** Phase 5 rerun
  shows reduction below P0.4's pin.
  *Mitigation:* three council paths pinned in advance (accept
  narrower / scope down / shelve). The framework for the decision
  exists; no new planning round needed.

- **R7: Residual stays flat AND build doesn't drop meaningfully.**
  Worst case — ADR 005 doesn't fire its primary signal AND doesn't
  fire the conditional ADR 006 signal.
  *Mitigation:* this is the "shelve" path. The bench artifact
  records what happened; the council reopens scope.

- **R8: Phase 0 profiler doesn't identify a clear bottleneck.**
  Flamegraph shows diffuse cost rather than a dominant hot path.
  *Mitigation:* all three scope items (fast path, allocation
  reduction, batch loop) attack the three most common causes;
  implementing all three even with diffuse evidence is reasonable.
  Escalate to council only on a surprising dominator.

- **R9: Bench result digest mismatch.** Output digest changes from
  `2fe6c8d8...` due to insertion-order leak through canonical
  normalization.
  *Mitigation:* digest equality is a Phase 5 hard gate; any drift
  triggers investigation rather than promotion.

- **R10: Fast-path eligibility set ambiguity at runtime.** `Int32`
  columns coerced to `Int64` by a previous `Project` produce keys
  that should fast-path as `Int64` but the operator setup classifies
  them as `Int32` (or vice versa).
  *Mitigation:* eligibility check resolves `DataType` from the child
  operator's output schema indexed by the join-key `ColumnOrdinal`,
  not the source-table type;
  `fast_path_eligibility_uses_child_output_schema` test.

- **R11: Threshold overfitting.** The default `≥30%` `build_ms`
  reduction is a promotion calibration, not a license to distort
  the implementation around one bench row.
  *Mitigation:* digest equality and phase attribution remain
  stronger than a single timing number. The bench artifact records
  the full delta table; the council may accept a narrower real win.

- **R12: Shared-helper drift.** A convenience helper named too
  broadly (e.g., "fast hash") could blur exchange-partition hashing
  with join-build key hashing, silently breaking ADR 003 §5.
  *Mitigation:* semantic helper names; if helper extraction creates
  a tempting shared surface, add an architecture-test grep guard
  proving `partition_key_hash` still calls canonical
  `encode_partition_key_value` rather than the join-build fast path
  (ADR hard gate 14's conditional escape).

- **R13: Wall-clock-only narrative.** Promotion is tempting if
  `wall_ms` drops but `build_ms` doesn't move — a coincidence
  rather than a measured win against the targeted bucket.
  *Mitigation:* ADR hard gate 16 explicitly rejects
  wall-clock-only promotion; the bench artifact records `build_ms`
  movement as the primary signal.

## Definition of done

- All 7 Phase 0 pins recorded in the bench-results artifact.
- Phase 0 P0.2 baseline profiler note archived; flamegraph file
  reference if available.
- `JoinKey` representation lands; fast-path eligibility for
  single-column `Int64` and `Int32`; type-tag invariant enforced.
- Row-list inline-first representation lands; pre-sized hash map.
- Batch-oriented build loop lands; per-row reservation check
  preserved.
- Hash table layout (Phase 0 P0.6 chosen candidate) lands; memory
  accounting preserved.
- Every ADR 002 / 003 / 004 hard gate passes byte-identically.
  Architecture guards G1-G18 unchanged.
- `partition_key_hash` byte-identical for all ADR 004 supported
  key types.
- Output digest equality preserved (`2fe6c8d8...`).
- Post-ADR-005 rerun rows at 2/4/8 workers recorded alongside
  ADR 004 baseline.
- `build_ms` reduction meets Phase 0 P0.4 threshold (default
  `≥30%` at 8 workers), or the bench artifact records the explicit
  gap (no softening into a vague "improved" claim) and the council
  selects accept-narrower / scope-down / shelve.
- `framework_overhead_ms` delta recorded; conditional ADR 006
  promotion rule resolved (band named).
- Post-optimization profiler note captured.
- Bench artifact records baseline, rerun, deltas (including
  computed build rows/sec), profiler references, conditional
  ADR 006 readout.
- Roadmap updated to record ADR 005 promotion + ADR 006 resolution.
