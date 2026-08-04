# Implementation Plan: ADR 006 — Runtime Filter Pushdown

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion ADR:** `plans/adr/0006_runtime_filter_pushdown.md`
**Bench-results artifact (post-promotion):**
`plans/impl/0006_runtime_filter_pushdown_bench_results.md`

## Inherits substrate

- ADR 001 / 002 / 003 / 004 / 005 promoted contracts; all hard
  gates and architecture guards G1-G18 must continue to pass
  byte-identically.
- `plans/impl/0005_hash_join_build_path_efficiency_bench_results.md`
  is the baseline reference for the ADR 005 canonical 10M × 1M
  hash-partitioned row.
- `crates/toron/src/distributed/fragment.rs` — current hash-join
  fragmenter; this is where the two-wave stage-order change lands.
- `crates/toron/src/distributed/scheduler.rs` — coordinator
  merge of build-side fragments + barrier + probe-wave dispatch.
  (There is no `distributed/coordinator.rs`; coordinator
  responsibilities live in `scheduler.rs` and `server/mod.rs`.)
- `crates/toron/src/server/mod.rs` — task dispatch surface
  (~line 1064); existing `TaskSpec` carries the binding the
  merged filter rides on.
- `crates/toron/src/distributed/task.rs` —
  `PartitionManifest`, `TaskCompletionOutput::ShuffleProducer`,
  `TaskCompletePayload`, `TaskInputBinding::Scan`. The new
  `runtime_filter_fragment` lands on `PartitionManifest`; the
  new `runtime_filter` carrier lands on `TaskInputBinding::Scan`.
- `crates/toron/src/storage.rs` — `ScanRequest` optional
  `runtime_filter: Option<RuntimeFilterDescriptor>` field
  (the wire-level surface to Wendelon).
- `crates/toron/src/exec.rs` — build-side filter generation
  hook in the exchange-producer execution path (not in
  `execute_inner_join`); worker-side threading of
  `TaskInputBinding::Scan.runtime_filter` into
  `ScanRequest.runtime_filter` (~line 460).
- Shared cross-ring schema crate — to be selected in Phase 1
  (default: extend an existing Wendelon-published schema crate;
  alternative: minimal `nilket-shared` common module). Final
  choice does not change the contract; it changes only the
  module path of `RuntimeFilterDescriptor`.
- `crates/toron/benches/distributed_inner_equi_join.rs` — extend
  for runtime-filter-enabled and dual-shape benchmarks.
- `crates/wendelon/plans/conventions/` — semantic names,
  newtype-units, no-premature-backward-compat,
  no-planning-phase-names.

## Goal

Reduce the post-ADR-005 bytes-movement cluster (~34% of wall at
8 workers on the 10M × 1M canonical row) for selective
hash-partitioned inner equi-joins by emitting advisory Bloom
runtime filters during the build wave and applying them at the
Wendelon storage boundary before probe-side bytes hit the wire.

ADR 006 has a **two-stage completion criterion**:

- **Toron-side complete (ADR 006 promotion).** Toron emits
  build-wave Bloom fragments, the coordinator merges them, the
  merged filter rides `TaskInputBinding::Scan` to the worker,
  and the worker threads it onto `ScanRequest.runtime_filter`.
  ADR 005 canonical 10M × 1M row remains digest-identical with
  filters enabled or disabled. The Wendelon-disabled gate
  (hard gate 21) proves
  filter-enabled-with-Wendelon-disabled is byte-identical to
  filter-disabled. The non-selective guard row stays within
  10% of ADR 005's `build_ms` post-promotion baseline. This
  is the **Toron-side promotion bar** and the minimum for
  ADR 006 to ship.
- **Wendelon-enabled bench evidence (post-promotion follow-up).**
  Once Wendelon-side application ships, the selective bench
  row shows the `≥50%` bytes-movement cluster reduction versus
  filter-disabled at 8 workers. This row lands as an update
  to the bench-results artifact, not blocking ADR 006
  promotion. Hard gate 18's `≥50%` target is a
  **Wendelon-enabled target**, not a Toron-side promotion
  blocker.

The split is structurally required by the advisory invariant:
Toron's contract is correct whether or not Wendelon applies the
filter; the perf win materializes only when Wendelon applies it.
The bench artifact records both gates explicitly.

## Scale boundary

**Primary:** cross-ring (Toron ↔ Wendelon storage layer).
**Secondary:** scheduler / fragmenter (cluster-wide) and
transport (`PartitionManifest`, `ScanRequest`). ADR 006 is the
first implementation plan drafted under the council's
scale-boundary convention.

## Baseline (from ADR 005 promotion)

ADR 005 post-promotion 10M × 1M hash-partitioned, 8 workers:

| Phase | ms | % of wall |
|---|---:|---:|
| build | 542.787 | 33.1% |
| framework_overhead | 432.429 | 26.4% |
| batch_send | 185.123 | 11.3% |
| shuffle_pull_decode | 156.441 | 9.6% |
| output_publish | 143.318 | 8.8% |
| probe | 80.191 | 4.9% |
| batch_encode | 73.630 | 4.5% |

Bytes-movement cluster (`batch_send` + `shuffle_pull_decode` +
`output_publish` + `batch_encode`) = 558.512 ms (34.1%).

Canonical digest (preserved unchanged):
`2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289`.

## Implementation contract

- The build-side hash-join exchange producer may publish optional
  Bloom filter fragments on its `PartitionManifest`.
- The coordinator deterministically merges compatible fragments
  from accepted build-stage attempts only, then injects the
  merged descriptor into each probe-side task's
  `TaskInputBinding::Scan.runtime_filter` at probe-wave
  dispatch. The worker reads the binding inside
  `Executor::execute_bound` and threads the descriptor onto the
  `ScanRequest.runtime_filter` it constructs for the
  `ExecutionPlan::Scan` variant. (Coordinator does **not**
  construct `ScanRequest` directly.)
- Wendelon may apply the filter as a row-level reject; falling
  back is byte-identical to filter-disabled.
- All ADR 002 / 003 / 004 / 005 hard gates pass byte-identically.
- The ADR 005 canonical 10M × 1M row remains digest-identical
  with filters enabled or disabled.
- No new `MessageKind`, `JoinKind`, IR variant, `BudgetDimension`,
  task telemetry field, architecture guard (by default), or
  Wendelon scan contract beyond the **three additive optional
  fields**: `runtime_filter_fragment` on `PartitionManifest`,
  `runtime_filter` on `TaskInputBinding::Scan`, and
  `runtime_filter` on `ScanRequest`.
- Filter-disabled queries retain today's concurrent-producer
  stage graph unchanged.

## Phase plan

### Phase 0 — Calibration and fixture scaffolding

No production code. Outputs documented in the bench-results
artifact's Phase 0 audit section.

- **P0.1: Selectivity fixture design.** Define the selective
  star-schema-like fixture as **small dimension × large fact**.
  Realistic shape: build = 10K–100K rows (small dimension),
  probe = 100M rows (fact table), match rate driven by the
  dimension-key population over the fact-table key range
  (~1–10% match rate). Pin row counts so the selectivity
  profile is reproducible across runs. Record the fixture
  seed and key distribution. The build cardinality is
  load-bearing for Bloom calibration (P0.2) — large-build
  selective joins are a V1.x consideration; ADR 006 V1
  optimizes the small-dimension case.
- **P0.2: Bloom parameter calibration (adaptive sizing).** Pin
  the V1 sizing curve, not a single byte budget:
  - **Target FP rate:** 1% at the observed distinct build-key
    count `n`.
  - **Optimal bit count formula:**
    `m = ceil(-n · ln(p) / (ln 2)²)` ≈ `9.59 · n` for p = 0.01.
  - **Optimal hash count formula:** `k = round((m/n) · ln 2)`
    ≈ `7` for p = 0.01.
  - **Byte budget bounds:**
    - Minimum **16 KiB** serialized payload (sufficient for
      `n ≤ 13K` at 1% FP).
    - Cap **1 MiB** serialized payload (sufficient for
      `n ≤ ~870K` at 1% FP).
  - **At-cap behavior:** when `n` exceeds the cap's threshold,
    measured FP rate degrades above 1%. The filter remains
    advisory and correct; selectivity reduces. ADR 006 does
    **not** error on at-cap; the at-cap FP rate is recorded
    in the bench artifact.
  - **Calibration audit:** Phase 0 records, for the selective
    fixture's chosen build cardinality, the computed
    `(bit_count, hash_count, byte_budget)`, the measured FP
    rate on the fixture's probe distribution, and whether the
    cap binds. The fixture-derived `(bit_count, hash_count)`
    are the V1 defaults the ADR 006 §6 descriptor carries
    for that workload; runtime sizing per-join follows the
    same formula against the actual observed `n`.
- **P0.3: Non-selective guard fixture.** Define the
  non-selective guard fixture (10M × 10M or 10M × 1M with ~100%
  match rate). Same canonical encoder, different key
  distribution.
- **P0.4: Wendelon-side coordination check.** Confirm whether
  Wendelon-side application work is in flight or scheduled. If
  yes, ADR 006 promotes against end-to-end bench evidence. If
  no, ADR 006 promotes against the Wendelon-disabled gate (hard
  gate 21) and the full reduction row lands in a bench-results
  update later.
- **P0.5: Rerun harness command pinned.**
  ```
  cargo bench -p toron --bench distributed_inner_equi_join \
      -- --distributed-inner-equi-join-only \
         --strategy=hash --workers=2,4,8 \
         --runtime-filter={enabled,disabled} \
         --build-rows=<P0.1> --probe-rows=<P0.1>
  ```

### Phase 1 — Cross-ring descriptor + schema location

Files:
- Shared cross-ring schema crate — to be chosen Phase 1 (default:
  extend an existing Wendelon-published schema crate; alternative:
  new minimal `nilket-shared`).
- `crates/toron/Cargo.toml` and / or `crates/wendelon-*/Cargo.toml`
  for the dependency wiring.
- `crates/toron/tests/architecture_test.rs` — allow the shared
  schema boundary in the import allowlist if a new crate is
  introduced; otherwise no change.

Work:

- **Decide crate location.** Default recommendation: extend an
  existing Wendelon-published schema crate by one descriptor
  type. Alternative: create `nilket-shared` with this as its
  initial inhabitant. Final decision recorded in the bench
  artifact's Phase 1 audit. The contract is identical either
  way; this is a module-path decision.
- **Define `RuntimeFilterDescriptor`** per ADR 006 §3:
  ```
  pub struct RuntimeFilterDescriptor {
      pub key_encoding_version: u32,
      pub build_key_schema: BuildKeySchema, // or LogicalTypeVec
      pub probe_key_plan: ProbeKeyEncodingPlan,
      pub hash_seed: u64,
      pub bit_count: u32,
      pub hash_count: u8,
      pub byte_budget: u32,
      pub target_fp_rate_bp: u16, // basis points
      pub payload: Bytes,
      pub probe_descriptor_fingerprint: PhysicalDescriptorFingerprint,
  }
  ```
  Field shapes are illustrative; final types follow the chosen
  schema-crate conventions.
- **Define `RuntimeFilterFragment`** (Toron-internal type that
  wraps the descriptor plus build-side provenance for merge):
  fragment payload + merge-key (bit_count, hash_count, hash_seed,
  key_encoding_version) for compatibility checking.

Tests:

- `runtime_filter_descriptor_round_trips` — serialize / deserialize
  parity.
- `runtime_filter_descriptor_versioning` — older versions decode
  with fallback semantics.
- `runtime_filter_fragment_merge_compatibility` — compatible
  fragments merge; incompatible fragments produce a typed
  drop-the-filter signal, not a panic or query failure.
- `architecture_test` allowlist updated only if a new crate is
  introduced.

### Phase 2 — Build-side fragment generation

Files:
- `crates/toron/src/exec.rs` — exchange-producer execution path.
- `crates/toron/src/distributed/task.rs` — `PartitionManifest`
  gains optional `runtime_filter_fragment` field.
- `crates/toron/src/exec/runtime_filter.rs` (new) — Bloom
  generation helpers, semantic name per the helper-naming rule.

Work:

- **Determine filter eligibility at task setup.** A build-side
  exchange-producer task is filter-eligible when:
  - the parent join is `Inner` + `HashPartitioned`;
  - join keys are **single-column** (composite keys are V1.x;
    fall through to concurrent stage graph);
  - the bound key `DataType` is in the **V1 narrow set**:
    `Int8`/`Int16`/`Int32`/`Int64`, `UInt8`/`UInt16`/`UInt32`/
    `UInt64`, `Utf8`. **`Float32`/`Float64` are excluded in V1**
    (deferred behind the floating-point canonical-encoding ADR
    — NaN/signed-zero/subnormal semantics); **`Date`/`Timestamp`/
    `Decimal` are excluded** (canonical encoder doesn't support
    them yet).
- **Generate the Bloom filter** during the build-side scan,
  accumulating canonical filter-key bytes (reusing ADR 003 §13
  normalization and ADR 005 type-tag invariant). Bloom
  parameters come from the calibrated defaults (P0.2).
- **Attach the fragment to `PartitionManifest`.** Add optional
  `runtime_filter_fragment: Option<RuntimeFilterFragment>`. Wire
  through `TaskCompletionOutput::ShuffleProducer { manifest }`.
  Existing barrier semantics unchanged.
- **Filter-key helper naming** (architecture invariant): name
  the helper for "runtime filter key" semantically. Do **not**
  reuse `partition_key_hash` or extract a shared "fast hash"
  helper. ADR 006 hard gate 15.

Tests:

- `runtime_filter_generation_no_false_negatives` — every
  build-key inserted is reported present by the generated
  filter. **Critical correctness test** (ADR 006 hard gate 10).
- `runtime_filter_generation_preserves_type_tag` — `Int32(1)`
  and `Int64(1)` produce different filter membership. Generalizes
  ADR 005 type-tag test (ADR 006 hard gate 14).
- `runtime_filter_generation_fp_rate_within_target` — on the
  calibrated fixture (P0.2), measured false-positive rate is
  within target ± noise budget.
- `runtime_filter_generation_eligibility_inner_hash_only` — outer
  joins, broadcast strategy, and composite-key joins do not
  generate fragments (concurrent stage graph preserved).
- `runtime_filter_generation_eligibility_excludes_floats` —
  `Float32` and `Float64` single-column joins do not generate
  fragments in V1 (deferred behind the floating-point
  canonical-encoding ADR).
- `runtime_filter_generation_eligibility_excludes_unsupported_types` —
  `Date32`, `Date64`, `Timestamp*`, and `Decimal` single-column
  joins do not generate fragments (canonical encoder unsupported).
- `runtime_filter_disabled_concurrent_graph_preserved` — when
  filtering is disabled, `PartitionManifest` carries no fragment
  and the producer stage graph is byte-identical to ADR 005's.
- `partition_manifest_round_trip_with_and_without_fragment` —
  serialization parity with the new optional field.
- `partition_assignment_unchanged_for_filter_eligible_types` —
  `partition_key_hash` is byte-identical to ADR 005 baseline.

### Phase 3 — Coordinator merge + two-wave stage dispatch

Files:
- `crates/toron/src/distributed/fragment.rs` — fragmenter
  decision for two-wave vs concurrent stage graph.
- `crates/toron/src/distributed/scheduler.rs` — fragment
  collection + merge; barrier semantics for the two-wave shape;
  probe-wave dispatch injects merged filter into outbound
  `TaskInputBinding::Scan.runtime_filter`. (There is no
  `distributed/coordinator.rs`; coordinator responsibilities
  live in `scheduler.rs` and `server/mod.rs`.)
- `crates/toron/src/server/mod.rs` — task dispatch path
  (~line 1064); the merged filter rides through the existing
  `TaskSpec` dispatch surface via `TaskInputBinding::Scan`.

Work:

- **Fragmenter decides two-wave vs concurrent at plan-bind time.**
  Filter-enabled hash-partitioned inner joins produce a stage
  graph where the probe-side exchange producer has the build-side
  exchange producer as an `upstream_stages` entry. Filter-disabled
  joins preserve today's `upstream_stages = []` for both
  producers.
- **Coordinator collects fragments** as build-stage task manifests
  arrive. Merge happens at build-stage acceptance — when ADR 003
  task acceptance fires for a build-stage attempt, the fragment
  is folded into the running merged filter for that join.
- **Merge semantics.** Bitwise OR of payload bytes when all
  compatibility fields match (`bit_count`, `hash_count`,
  `hash_seed`, `key_encoding_version`). Incompatible fragments
  drop the merged filter for the entire join, silently (no wire
  error). Compatibility checking happens before XOR/OR — never
  blend incompatible bits.
- **Stale-attempt handling.** Only fragments from
  accepted-by-ADR-003 attempts participate in the merge.
  Reaccepted-after-retry attempts produce reproducible fragments
  given the same task seed (build-side task input is canonical;
  the filter is deterministic over its input).
- **Build-wave barrier.** Probe-side exchange-producer dispatch
  is gated on build-wave completion (`StagePlan::upstream_stages
  = [build_stage]`). The barrier reuses ADR 003's multi-upstream
  barrier semantics (G18); no new barrier kind.
- **Filter injection into probe-side `TaskInputBinding::Scan`.**
  When the build-wave barrier opens, the coordinator records the
  merged filter (or the drop-the-filter signal) and injects it
  into each probe-side task's `TaskInputBinding::Scan.runtime_filter`
  before dispatching the `TaskSpec`. The coordinator stops here;
  it does **not** construct `ScanRequest` directly. Worker-side
  threading from the binding to `ScanRequest.runtime_filter` is
  Phase 4 work.

Tests:

- `two_wave_stage_graph_for_filter_enabled_inner_hash_join` —
  fragmenter produces probe-with-build-upstream when filter
  enabled; produces concurrent producers when disabled.
- `coordinator_merge_order_invariant` — merging fragments in
  permutation order produces byte-identical merged filter.
- `coordinator_merge_drops_incompatible_fragments` — incompatible
  fragments silently drop the filter; query proceeds correctly.
- `coordinator_merge_excludes_stale_attempts` — fragments from
  failed / rejected attempts do not participate; reaccepted
  attempts produce reproducible merged filter.
- `probe_dispatch_blocks_on_build_wave_barrier` — probe-side
  tasks do not dispatch before build-wave acceptance. **ADR 006
  hard gate 13.**
- `coordinator_injects_merged_filter_into_probe_task_binding` —
  the merged descriptor reaches the dispatched
  `TaskInputBinding::Scan.runtime_filter` on each probe-side
  task. (End-to-end threading through to `ScanRequest` is
  Phase 4's `probe_scan_request_carries_merged_filter`.)
- `filter_disabled_path_byte_identical_to_adr_005` — the
  filter-disabled execution path produces byte-identical
  manifests, scan requests, and digests to ADR 005's baseline.
- `worker_loss_retry_preserves_merged_filter_determinism` —
  worker SIGKILL on a build-stage task; reaccepted retry
  produces identical merged filter; ADR 004 SIGKILL replay
  semantics preserved.

### Phase 4 — Probe-wave carrier + scan-time attachment + Wendelon-side coordination

Files:
- `crates/toron/src/distributed/task.rs` — `TaskInputBinding::Scan`
  gains `runtime_filter: Option<RuntimeFilterDescriptor>`.
- `crates/toron/src/distributed/scheduler.rs` and
  `crates/toron/src/server/mod.rs` — probe-side dispatch
  injects the merged filter into outbound `TaskInputBinding::Scan`
  bindings. **Coordinator does not construct `ScanRequest`
  directly** — it only puts the filter onto the binding the
  worker will read.
- `crates/toron/src/exec.rs` — `Executor::execute_bound` reads
  the optional filter from `TaskInputBinding::Scan` (the path
  threaded through task execution context) and populates
  `ScanRequest.runtime_filter` when constructing the request
  for the `ExecutionPlan::Scan` variant (~line 460).
- `crates/toron/src/storage.rs` — `ScanRequest` gains optional
  `runtime_filter: Option<RuntimeFilterDescriptor>` as the
  wire-level field to Wendelon.
- (Wendelon-side) — Wendelon's scan path validates the descriptor
  against the probe-side `physical_descriptor_fingerprint`,
  applies row-level reject, falls back silently on mismatch.
  Lives in the Wendelon repo and is tracked separately; ADR 006
  pins the contract surface only.

Work (Toron-side):

- **Extend `TaskInputBinding::Scan`** with optional
  `runtime_filter: Option<RuntimeFilterDescriptor>`. This is
  the load-bearing carrier from coordinator merge state into
  worker scan execution — `ScanRequest` is constructed on the
  worker, not the coordinator, so the descriptor must ride
  the binding to reach the worker.
- **Wire probe dispatch.** When the coordinator opens the
  build-wave barrier and has a merged filter for a probe-side
  task's join, the merged filter is placed onto the task's
  `TaskInputBinding::Scan.runtime_filter` field before
  dispatch. Filter-disabled paths leave the field `None`.
- **Worker-side threading.** `Executor::execute_bound` reads
  the filter from the active task's binding when processing
  `ExecutionPlan::Scan` and threads it into the constructed
  `ScanRequest.runtime_filter`. The plan IR itself
  (`ExecutionPlan::Scan`) does **not** carry the filter —
  the filter is a runtime artifact injected per task instance
  at coordinator-dispatch time, not a plan-bind-time decision.
- **Add optional `runtime_filter` field to `ScanRequest`.**
  Serialization parity preserved; pre-ADR-006 Wendelon clients
  accept requests without the field.
- **Mock Wendelon application in Toron's test harness.** A
  test-only `StorageScan` implementation that consumes the
  descriptor, computes per-row filter-key hashes per the
  descriptor's encoding plan, and rejects rows whose hash bits
  aren't all set. Used for Phase 4 / 5 tests and the
  Wendelon-disabled gate (hard gate 21).

Work (Wendelon-side, tracked separately):

- Wendelon's scan path consumes `runtime_filter` when present.
- Validates against probe-side
  `physical_descriptor_fingerprint`.
- Row-level reject before Arrow batch emission.
- Falls back silently on mismatch / unsupported.

Tests:

- `scan_request_round_trip_with_and_without_runtime_filter` —
  serialization parity.
- `task_input_binding_scan_round_trip_with_and_without_runtime_filter` —
  binding-level serialization parity.
- `probe_task_binding_carries_merged_filter_to_executor` —
  the coordinator-injected descriptor reaches the worker's
  `TaskInputBinding::Scan`.
- `executor_threads_runtime_filter_from_binding_to_scan_request` —
  `Executor::execute_bound` reads the binding's filter and
  populates `ScanRequest.runtime_filter` when constructing
  the request for `ExecutionPlan::Scan`.
- `probe_scan_request_carries_merged_filter` — end-to-end
  attachment under filter-enabled execution (the load-bearing
  Toron-side path test, covering coordinator merge through
  binding through worker through `ScanRequest`).
- `mock_wendelon_applies_row_level_reject` — using the test
  harness mock, a selective fixture emits fewer rows when the
  filter is provided.
- `mock_wendelon_silently_drops_on_fingerprint_mismatch` — the
  scan succeeds and emits all rows when the descriptor's
  fingerprint doesn't match the probe-side descriptor.
- `mock_wendelon_application_byte_identical_to_filter_disabled_on_full_match` —
  if the filter is all-positive, the scan emits the same rows
  as without the filter (correctness invariant under
  degenerate case).

### Phase 5 — End-to-end correctness + telemetry rerun

Files:
- `crates/toron/tests/runtime_filter_pushdown_test.rs` (new) —
  end-to-end correctness gates.
- `plans/impl/0006_runtime_filter_pushdown_bench_results.md`
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
  cargo test -p toron --test runtime_filter_pushdown_test
  cargo test --workspace
  cargo bench -p toron --bench distributed_inner_equi_join --no-run
  cargo bench -p toron --bench distributed_inner_equi_join -- --quick --distributed-inner-equi-join-only
  git diff --check
  ```
  Notes:
  - `cargo test -p toron` exercises in-crate inner-join
    correctness fixtures and the ADR 005 `execute_bound_inner_join_*`
    tests; Phase 3's filter-disabled gate ensures byte-identical
    behavior for those.
  - `distributed_process_test` covers SIGKILL replay
    (Phase 3's `worker_loss_retry_preserves_merged_filter_determinism`
    cross-validates).
  - `runtime_filter_pushdown_test` is the Phase 5 new test file —
    end-to-end correctness, fallback, fingerprint mismatch,
    no-false-negative.
- **Selective bench rerun.** Phase 0 selective fixture at
  2/4/8 workers, filter-enabled + filter-disabled, with and
  without the Wendelon mock applying the filter.
- **Non-selective guard rerun.** Phase 0 guard fixture at
  8 workers, filter-enabled + filter-disabled.
- **ADR 005 canonical rerun.** 10M × 1M hash-partitioned,
  filter-enabled + filter-disabled. Digest equality is the
  primary signal.
- Compute and record deltas in the bench artifact:
  - Δbytes-movement cluster per worker count, selective fixture
    (target ≥50% reduction filter-enabled-with-Wendelon vs
    filter-disabled).
  - Δ`shuffle_pull_decode_ms` and Δ`batch_send_ms` per worker
    count, selective fixture.
  - Computed shuffle bytes / probe row (baseline and
    post-change) on the selective fixture.
  - Non-selective guard `build_ms` delta vs ADR 005 baseline
    (target within 10%).
  - ADR 005 canonical digest equality.
  - Phase 0 calibration audit (selectivity profile, Bloom
    parameter calibration).
  - Profiler note if available; carry forward the ADR 005
    profiler ptrace/Yama caveat if still in place.
  - Wendelon-disabled gate evidence (filter-enabled-without-
    Wendelon byte-identical to filter-disabled).

End-to-end correctness tests (in
`runtime_filter_pushdown_test.rs` unless noted):

- `end_to_end_selective_join_emits_byte_identical_results_with_filter_enabled` —
  the load-bearing correctness gate. ADR 006 hard gate 6.
- `end_to_end_filter_dropped_on_fingerprint_mismatch_is_byte_identical` —
  ADR 006 hard gate 8.
- `end_to_end_saturated_filter_emits_byte_identical_results` —
  ADR 006 hard gate 9.
- `end_to_end_no_false_negatives_on_random_keys` — ADR 006
  hard gate 10.
- `end_to_end_two_wave_barrier_enforced` — probe-side dispatch
  blocks on build-wave acceptance. ADR 006 hard gate 13.
- `end_to_end_filter_disabled_digest_equals_adr_005` —
  ADR 005 canonical digest with filter disabled.
- `end_to_end_wendelon_disabled_byte_identical_to_filter_disabled` —
  Wendelon-disabled gate. ADR 006 hard gate 21.

Bench artifact required contents:

- ADR 005 baseline table (2/4/8 workers, 10M × 1M).
- ADR 006 selective row, filter-enabled and filter-disabled,
  Wendelon-enabled and Wendelon-disabled.
- ADR 006 non-selective guard row.
- ADR 005 canonical row rerun (digest comparison).
- Bytes-movement cluster delta (selective row).
- Shuffle bytes / probe row computation (selective row,
  baseline and post-change).
- Phase 0 calibration audit.
- Profiler notes (forwarded ADR 005 caveat if applicable).
- Wendelon-disabled gate evidence.
- Conditional readout: if selective ≥50% miss or non-selective
  guard regresses beyond 10%, record explicit gap and council
  decision path (accept-narrower / scope-down / shelve).

Suggested commit slices:

1. `chore: adr 006 prep — roadmap honesty + Phase 0
   fixture/calibration notes`
2. `plan: adr 006 runtime filter pushdown`
3. `feat: adr 006 phase 1 — cross-ring runtime filter descriptor`
4. `feat: adr 006 phase 2 — build-side bloom filter generation +
   partition manifest extension`
5. `feat: adr 006 phase 3 — coordinator merge + two-wave stage
   dispatch`
6. `feat: adr 006 phase 4 — probe-side scan-request attachment +
   mock wendelon application`
7. `feat: adr 006 phase 5 — telemetry rerun + promotion` (lands
   `bench_results.md`; marks ADR 006 shipped on roadmap;
   Wendelon-side end-to-end row arrives in a follow-up bench
   update if the Wendelon-side application post-dates Toron-side
   promotion)

**Toron-side promotion** lands when the Toron-side blockers
pass: correctness gates 1-17, non-selective guard within 10% of
ADR 005 baseline (gate 19), ADR 005 canonical digest equality
(gate 20), and the Wendelon-disabled gate (gate 21). The
selective ≥50% row (gate 18) is a Wendelon-enabled target and
ships in a follow-up bench-results update when Wendelon-side
application is in.

If the non-selective guard regresses beyond 10% at Toron-side
promotion time, or if the Wendelon-enabled selective row later
misses ≥50%:

1. **Accept narrower reduction.** Record actual delta; close
   ADR 006 with measured win; bytes-movement cluster stays on
   soft-gate watch list for ADR 007.
2. **Scope down.** Narrow filter eligibility (e.g., drop
   `Utf8` from V1 eligibility, ship integer keys only); defer
   broader eligibility to V1.x.
3. **Shelve.** Revert; document the gap; reopen when evidence
   redirects (likely toward Topic E wire efficiency or
   evidence-redirecting workload changes).

A non-selective guard regression at Toron-side promotion time
is a blocker (one of paths 1-3 above must fire before promotion).
A Wendelon-enabled selective miss arrives later and triggers
the same three paths as a follow-up council decision, not a
retroactive de-promotion.

## Files touched

Implementation:

- Shared cross-ring schema crate (extended or new) —
  `RuntimeFilterDescriptor`.
- `crates/toron/src/distributed/task.rs` — `PartitionManifest`
  optional `runtime_filter_fragment` field;
  `TaskInputBinding::Scan` optional `runtime_filter` field
  (the load-bearing carrier from coordinator merge state to
  worker scan execution).
- `crates/toron/src/exec.rs` — build-side filter generation
  hook in the exchange-producer execution path; worker-side
  threading of `TaskInputBinding::Scan.runtime_filter` into
  `ScanRequest.runtime_filter` at ~line 460.
- `crates/toron/src/exec/runtime_filter.rs` (new) — Bloom
  generation helpers, eligibility detection, adaptive sizing.
- `crates/toron/src/distributed/fragment.rs` — two-wave stage
  graph for filter-enabled hash-partitioned inner joins.
- `crates/toron/src/distributed/scheduler.rs` — fragment
  collection + merge; barrier semantics for the two-wave
  shape; probe-wave dispatch injects merged filter into
  outbound `TaskInputBinding::Scan` bindings.
- `crates/toron/src/server/mod.rs` — task dispatch path
  (~line 1064 where `TaskSpec` is sent); the merged filter
  rides through the existing `TaskSpec` dispatch surface via
  `TaskInputBinding::Scan`.
- `crates/toron/src/storage.rs` — `ScanRequest` optional
  `runtime_filter` field (the wire-level surface to Wendelon).
- `crates/toron/tests/runtime_filter_pushdown_test.rs` (new) —
  end-to-end correctness tests.

Bench / evidence:

- `crates/toron/benches/distributed_inner_equi_join.rs` —
  extend with `--runtime-filter={enabled,disabled}` and the
  selective + non-selective fixtures.
- `plans/impl/0006_runtime_filter_pushdown_bench_results.md`
  (new) — Phase 5 artifact.

Architecture:

- `crates/toron/tests/architecture_test.rs` — allowlist update
  only if a new shared crate is introduced (Phase 1 default
  recommendation is no new crate). Conditional grep guard for
  runtime-filter-vs-exchange-partition hash drift if shared
  helper extraction makes drift plausible (ADR 006 hard gate
  15; default expectation: no new guard).

Wendelon-side (tracked separately):

- Wendelon scan path runtime-filter consumption and
  application (row-level reject) — ships under Wendelon's
  ADR process.

Roadmap:

- `plans/wip/roadmap.md` — record ADR 006 promotion; reframe
  ADR 007 candidates per the rerun outcome (selective ≥50%
  hit → ADR 007 candidates pick from B/C/D/E and V1.x runtime-
  filter extensions; selective miss → ADR 007 reopens
  evidence-driven scope).

## Out of scope (V1.x or later)

- Exact `InList` filters.
- Cost-based filter selection / planner-stats opt-in.
- Runtime filters for outer / semi / anti / sort-merge /
  non-equi joins.
- Runtime filters for broadcast joins.
- Row-group skip heuristics beyond independently-sound cases.
- Wendelon-side internal scan-engine changes (Wendelon ADRs).
- Concurrent / sharded coordinator merge (single-coordinator
  merge is fine for V1).
- Streaming partial-filter publication (build-wave fragments
  publish at task completion; partial / streaming is V1.x).

## Risks and mitigations

- **R1: No-false-negative violation.** Bloom-generation bug
  causes a probe row to be rejected when its key actually
  matches a build key. Catastrophic correctness violation
  (query returns wrong results).
  *Mitigation:* `runtime_filter_generation_no_false_negatives`
  + `end_to_end_no_false_negatives_on_random_keys` as hard
  gates. CI runs on every commit. Conservative Bloom parameter
  calibration (target FP rate is bounded but never zero, by
  design; false negatives must always be zero).

- **R2: Stage-order drift.** A future refactor reverts the
  two-wave shape (e.g., a "performance fix" that runs probe
  producers concurrently for non-selective workloads) silently
  breaks the filter contract.
  *Mitigation:*
  `probe_dispatch_blocks_on_build_wave_barrier` and
  `end_to_end_two_wave_barrier_enforced` are hard gates;
  fragmenter test pins the two-wave graph shape for
  filter-enabled queries.

- **R3: Runtime-filter hash vs exchange-partition hash drift.**
  A shared-helper extraction lets join filter hashing affect
  exchange partition assignment.
  *Mitigation:* semantic helper names ("runtime filter key" vs
  "exchange partition hash"); architecture-test grep guard
  conditional on shared-helper extraction (ADR 006 hard gate
  15);
  `partition_assignment_unchanged_for_filter_eligible_types`
  test.

- **R4: Type-tag invariant violation.** Filter-key encoding
  loses logical-type identity; `Int32(1)` and `Int64(1)`
  collide. Selective bench may show good numbers; semantics
  silently wrong on mixed-type joins.
  *Mitigation:* `runtime_filter_generation_preserves_type_tag`
  unit test; generalizes ADR 005 hard gate 7 enforcement.

- **R5: Non-selective workload regression.** Two-wave barrier
  cost erases ADR 005's `build_ms` win on workloads where the
  filter doesn't help. ADR 006 ships a perf win on selective
  workloads but causes a perf regression on non-selective
  ones.
  *Mitigation:* non-selective guard hard gate (gate 19);
  `≤10%` `build_ms` regression target at 8 workers vs ADR 005
  baseline. If exceeded, council decides accept-narrower /
  scope-down (narrow filter eligibility) / shelve.

- **R6: Wendelon-side never lands.** Toron ships filter
  emission but Wendelon-side application is indefinitely
  blocked.
  *Mitigation:* advisory invariant makes split-shipment safe;
  Toron-only execution is byte-identical to filter-disabled.
  ADR 006 promotes against the Wendelon-disabled gate; the
  full bench row arrives in a bench update later. If the
  Wendelon-side work never lands, the contract has zero
  performance value but zero correctness cost — the council
  reopens scope at that point. R6 is a scheduling risk, not a
  correctness risk.

- **R7: Stale-attempt fragment leakage.** A failed-but-retried
  build-stage task's fragment leaks into the merge, producing
  a non-reproducible merged filter.
  *Mitigation:*
  `coordinator_merge_excludes_stale_attempts` test; merge
  consumes only ADR-003-accepted attempt fragments; SIGKILL
  replay determinism gate
  (`worker_loss_retry_preserves_merged_filter_determinism`).

- **R8: Incompatible-fragment merge silently corrupts.** Two
  fragments with mismatched parameters merged via XOR/OR
  produce a Bloom filter that isn't a valid Bloom over either
  input.
  *Mitigation:* compatibility checking happens **before**
  merge bit-ops; incompatible fragments drop the merged filter
  for the whole join (silent drop preserves advisory
  invariant); `coordinator_merge_drops_incompatible_fragments`
  test.

- **R9: Selective benchmark overfit.** The fixture is
  artificially constructed; real-world selectivity profiles
  vary widely.
  *Mitigation:* the selective fixture (P0.1) is one
  representative shape, not the only one. ADR 006 promotion
  cites the fixture's reduction; bench artifact records the
  selectivity profile so future workloads can be compared
  apples-to-apples.

- **R10: Cross-ring crate ownership churn.** Choosing
  `nilket-shared` and later regretting it (or vice versa)
  creates a migration cost.
  *Mitigation:* the descriptor is a single type; migration is
  a module path change, not a contract change. Phase 1's
  default recommendation (extend an existing
  Wendelon-published schema crate) avoids spawning a new crate
  before multi-consumer evidence justifies it.

- **R11: Probe-side `physical_descriptor_fingerprint`
  mismatch on the common case.** If the descriptor's
  fingerprint check is too strict, common reroute/retry paths
  silently drop the filter and the bench shows no
  improvement.
  *Mitigation:*
  `end_to_end_filter_dropped_on_fingerprint_mismatch_is_byte_identical`
  test pins the correctness path; bench artifact records
  how often the fingerprint check drops the filter on the
  selective fixture (should be ~0 under stable conditions).

- **R12: Bloom parameter miscalibration.** P0.2 calibration is
  off; the 64 KiB budget produces a much-higher-than-1% FP
  rate at the actual build cardinality, weakening the filter
  to near-uselessness.
  *Mitigation:* `runtime_filter_generation_fp_rate_within_target`
  test on the calibrated fixture; bench artifact records
  measured FP rate; if off-target, Phase 0 recalibration
  before promotion.

## Definition of done

- All 5 Phase 0 pins recorded in the bench-results artifact.
- Phase 0 selectivity fixture + Bloom calibration documented.
- Shared cross-ring `RuntimeFilterDescriptor` lands in the
  chosen schema location; descriptor round-trip + versioning
  tests pass.
- Build-side filter generation lands; no-false-negative test
  + type-tag-invariant test enforced.
- `PartitionManifest` carries optional
  `runtime_filter_fragment` field; serialization parity
  preserved.
- Coordinator merge lands with order-invariance,
  stale-attempt exclusion, incompatible-fragment silent drop,
  and worker-loss retry determinism.
- Two-wave stage graph lands for filter-enabled
  hash-partitioned inner joins; filter-disabled path
  byte-identical to ADR 005.
- **Probe-wave carrier chain lands end-to-end.**
  `TaskInputBinding::Scan` gains optional `runtime_filter`
  field (the coordinator-to-worker carrier); coordinator
  injects the merged filter onto the binding at probe-wave
  dispatch; `Executor::execute_bound` reads the binding's
  filter and threads it onto `ScanRequest.runtime_filter`
  at `exec.rs:460`; `ScanRequest` gains the optional
  `runtime_filter` field (wire surface to Wendelon); mock
  Wendelon application validates row-level reject.
- Probe-side dispatch blocks on build-wave barrier
  (ADR 006 hard gate 13).
- Every ADR 002 / 003 / 004 / 005 hard gate passes
  byte-identically. Architecture guards G1-G18 unchanged.
- ADR 005 canonical 10M × 1M row digest-identical with
  filters enabled or disabled.
- **Toron-side promotion bar (blocking).** Non-selective
  guard bench shows `build_ms` within 10% of ADR 005
  post-promotion baseline at 8 workers. ADR 005 canonical
  row digest-identical filter-enabled vs filter-disabled.
  Wendelon-disabled gate (hard gate 21) shows
  filter-enabled-with-Wendelon-disabled byte-identical to
  filter-disabled.
- **Wendelon-enabled bench evidence (post-promotion
  follow-up, not blocking).** Once Wendelon-side application
  ships, the selective bench (Phase 0 fixture) shows ≥50%
  bytes-movement cluster reduction filter-enabled vs
  filter-disabled at 8 workers — or the bench-results
  update records the explicit gap and the council selects
  accept-narrower / scope-down (narrow filter eligibility) /
  shelve. The selective row is a target, not a Toron-side
  promotion blocker.
- Bench artifact records baseline, rerun, deltas, computed
  shuffle bytes / probe row, Phase 0 calibration, profiler
  references, Wendelon-disabled gate evidence, and
  conditional readout.
- Roadmap updated to record ADR 006 promotion + ADR 007
  candidate reorganization.
