# Implementation Plan: ADR 004 — Toron Distributed Inner
Equi-Join

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion ADR:**
`plans/adr/004_distributed_inner_equi_join.md`
**Bench-results artifact (promotion-scale evidence recorded):**
`plans/impl/004_distributed_inner_equi_join_bench_results.md`

## Inherits substrate

- `plans/adr/001_toron_foundation_and_engine_contract.md` —
  greenfield Arrow-native engine, `StorageScan` boundary,
  single Toron crate, worker-local `MorselId`.
- `plans/adr/002_toron_storage_backed_execution_contract.md`
  — schema-bound `ExecutionPlan`, coordinator-owned
  `LogicalPlan` → `ExecutionPlan` binding, descriptor
  fingerprint pin protocol, structured error taxonomy,
  canonical record-batch normalization, V1 grouped aggregate
  contract, architecture guards G1-G7.
- `plans/adr/003_toron_distributed_runtime_and_data_movement.md`
  — `TaskSpec` / `TaskIdentity` / `TaskInputBinding`,
  `WireEnvelope` + twelve `MessageKind` variants,
  `PartitionAddress` and pull-addressed partition store,
  `StaticWorkerRegistry`, strict stage barriers,
  `QueryBudget` and `BudgetDimension` taxonomy, ADR 003 §13
  canonical normalization with `SortPolicy::FullRowLex`,
  per-attempt FSM + per-task acceptance index, M10
  AES-256-GCM-SIV encrypted spill substrate, architecture
  guards G8-G15.
- `plans/impl/003_distributed_runtime_bench_results.md` —
  multi-process worker harness (`toron_fixture_worker`
  binary; `tokio::process::Child::kill` for SIGKILL),
  framed TCP transport via `ToronTransport`,
  `EncryptedSpillPartitionStore` reusing M10 substrate.
- `crates/wendelon/plans/conventions/` — newtype-units,
  no-version-suffixes, no-planning-phase-names,
  no-premature-backward-compat, no-project-name-in-protocols,
  capability-vs-feature-naming.

## Status

Promoted to `plans/impl/`. Implementation-complete with
promotion-scale evidence recorded in
`plans/impl/004_distributed_inner_equi_join_bench_results.md`.
Hard-gate bench rows passed; the worker-scaling soft gate
miss is recorded for ADR 005 planning.

## Implementation contract

- `LogicalPlan::Join { left, right, kind: JoinKind, on:
  Vec<(Expr, Expr)>, strategy: JoinStrategy }` is the
  logical surface; lowers under ADR 002's binder under the
  pinned descriptor.
- `ExecutionPlan::Join { left, right, kind, left_keys:
  Vec<ColumnOrdinal>, right_keys: Vec<ColumnOrdinal>,
  strategy, output_schema }` is the bound surface.
- `JoinKind { Inner }` single variant; `JoinStrategy
  { HashPartitioned, Broadcast }` two variants. Architecture
  guards G16, G17 enforce.
- Strategy selection in V1: SQL planner defaults to
  `HashPartitioned`; programmatic callers (Rune programs,
  test fixtures, benches, external API consumers) can emit
  `Broadcast` when they have cardinality knowledge. SQL
  hint syntax and statistics-driven selection are V1.x.
- Build side = syntactic `left` for both strategies.
  `BroadcastBuildSidePlacement` is a **structural** binder
  check (not stats-driven intent inference): under
  `JoinStrategy::Broadcast`, the left subtree must
  terminate in the single-partition publish shape
  (`Exchange { partition_count: 1 }` or
  `BroadcastPublish`). Any missing or wrong-count left
  publish shape rejects; any right-side publish shape
  rejects as ambiguous.
- `TaskInputBinding::Scan` extended from fieldless to
  `Scan { share: Option<ScanShare> }` where `ScanShare =
  { worker_index: u32, worker_count: u32 }`. Existing
  source-stage scans become `Scan { share: None }`.
  Broadcast probe scan tasks carry `Scan { share:
  Some(ScanShare { worker_index, worker_count }) }`;
  worker `w_i` scans tablets satisfying
  `xxh3_64(tablet_id) mod worker_count == worker_index`.
  Mechanical variant change per
  `no-premature-backward-compat.md`; no compat shim.
- Logical column references qualifier-aware:
  `Expr::Column(ColumnRef)` where `ColumnRef = { qualifier:
  Option<TableRef>, name: ColumnName }`. Binder maintains
  separate scopes for left and right join inputs; rejects
  ambiguous unqualified refs with typed
  `BindError::AmbiguousColumnRef`. Duplicate output column
  names **permitted** at the bound-schema level; downstream
  disambiguates by ordinal; SQL planner enforces naming
  discipline at the SQL surface.
- `StagePlan::upstream_stage: Option<StageId>` renamed
  mechanically to `upstream_stages: Vec<StageId>` per
  `no-premature-backward-compat.md`. No compat shim.
  Scheduler strict-barrier check iterates the vec; G18
  enforces.
- Hash-partitioned fragmenter emits 3-stage DAG: build-
  partition (left), probe-partition (right), join. Join
  stage has two upstreams. Planner emits explicit
  `Exchange` above each join input; fragmenter rejects
  non-`Exchange` inputs with
  `FragmentError::JoinInputNotExchanged`.
- Broadcast fragmenter emits 2-stage DAG: build-publish
  (left, single partition), probe-and-join (per-worker
  probe scan share + pulled build manifest). Phase 0
  P0.2 decides between reusing `Exchange { partition_count:
  1 }` vs introducing `BroadcastPublish` operator.
- Broadcast probe scan-share assignment is deterministic:
  `xxh3_64(source_tablet_id) mod worker_count`. Phase 0
  P0.6 pins the rule.
- Three new `QueryBudget` fields gate broadcast admission:
  `broadcast_build_bytes_per_task` (default 64 MiB),
  `broadcast_replicated_bytes_per_query` (default 1 GiB),
  `memory_per_task_bytes` (ADR 003 inherited). New
  `BudgetDimension::BroadcastReplication` variant.
  Broadcast-specific byte caps use this dimension; the
  existing task-memory cap still fails as
  `BudgetExhausted { dimension: Memory }`.
- Build-side memory reserves against
  `QueryBudget::memory_per_task_bytes`. Reservation failure
  → `BudgetExhausted { dimension: Memory }` only if spill
  cannot initialize; otherwise promotes one-way to encrypted
  spill.
- Build-side encrypted hash spill: AES-256-GCM-SIV via the
  M10 substrate; one spill file per join task; per-page
  nonce from `SpillId + page_index`; hash-bucket → page
  range index (random-access); LRU page cache; **all
  spill-resident in-memory state — bucket index, LRU
  decrypted-page cache, spill-writer buffers, per-bucket
  scratch — reserves against `memory_per_task_bytes`**
  (the cache is part of the task memory budget, not
  additional to it); spill bytes on disk reserve against
  `spill_bytes_reserved_per_query`; spill-cap exhaustion
  fails terminal `BudgetExhausted { dimension:
  SpillBytes }`; memory-cap exhaustion under index+cache
  pressure fails terminal
  `BudgetExhausted { dimension: Memory }`.
- Zero new `MessageKind` variants. Hash-partitioned join
  task carries two `TaskInputBinding::ShuffleConsumer`
  entries; worker discriminates build vs probe by matching
  `producer_stage_id` against the bound `Join` operator's
  left / right subtree terminal `Exchange` stages.
  Broadcast join task carries one `TaskInputBinding::Scan
  { share: Some(_) }` (probe) + one
  `TaskInputBinding::ShuffleConsumer` (build); worker
  discriminates by **binding variant**: `Scan` is probe,
  `ShuffleConsumer` is build. Architecture-guarded that
  broadcast tasks carry exactly one of each.
- Floating-point join keys, non-equi residuals, computed-
  expression keys rejected at bind with typed errors.
- All 21 hard gates pass; soft gates met or explicitly
  justified.
- ADR 003 residual counters plus executor/join phase spans
  are stacked into the staged ADR 004 promotion work so ADR
  005 starts from attributed phase evidence rather than
  wall-clock only.

## Goal

After ADR 004, Toron executes distributed inner equi-joins
under two strategies (`HashPartitioned` correctness baseline,
`Broadcast` cap-gated optimization), survives producer-loss
on either side via ADR 003 §8 invalidation, spills build-
side state to encrypted random-access spill when memory
caps, fails terminal on spill-cap or broadcast-cap
exhaustion, rejects malformed join queries at bind with
typed errors, and produces byte-identical normalized output
across independent runs.

## Phase plan

Each phase ships a runnable slice with fixture-backed tests.
Framework-only work does not count as complete (ADR 002 /
ADR 003 discipline, Wendelon ADR 070 precedent).

### Phase 0 — Audit, calibration, council pins

Pre-implementation pass. No code lands. Deliverable: a
"Phase 0 audit" section in the bench-results artifact pinning
each item.

- **P0.1: Fragmenter audit.**
  `crates/toron/src/distributed/fragment.rs` currently
  rejects multi-exchange plans with
  `FragmentError::MultipleExchangesUnsupported`. Document
  the rewrite for both strategies: hash-partitioned three-
  stage DAG with multi-upstream join consumer; broadcast
  two-stage DAG with single-partition build publish.
  Confirm regression-free for ADR 003 single-exchange GROUP
  BY.

- **P0.2: Broadcast publish operator decision.** Recommended
  default: reuse `Exchange { partition_count: 1 }` (fewer
  surfaces, identical wire shape). If `BroadcastPublish`
  wins (e.g., for cleaner fragmenter dispatch), fragmenter
  handles two operator shapes and planner emits
  appropriately.

- **P0.3: M10 hash-spill substrate audit.** Confirm
  `crates/toron/src/server/spill.rs` (seeded at M10; reused
  as ADR 003 `EncryptedSpillPartitionStore`) supports:
  - Random-access page read by `(SpillId, PageOffset)`. If
    not, document `read_page_at` extension surface for
    Phase 5.
  - Per-page nonce derivation supports point-lookup (nonce
    from `SpillId + page_index`).
  - Cleanup composes with task termination (ADR 003
    partition-store cleanup hooks).

- **P0.4: Scheduler audit.** Catalog every call site of
  `StagePlan::upstream_stage` in
  `crates/toron/src/distributed/scheduler.rs` and
  `crates/toron/src/distributed/task_state.rs`. The rename
  is mechanical: `Some(s) → vec![s]`, `None → vec![]`. No
  silent semantic change.

- **P0.5: `Expr::Column` refactor scope.** Enumerate every
  callsite of `Expr::Column(String)` across
  `crates/toron/src/sql_planner.rs`,
  `crates/toron/src/rune_engine.rs`,
  `crates/toron/src/binder.rs`, test fixtures, and bench
  harnesses. Refactor to `Expr::Column(ColumnRef)` is
  structural; one-commit if callsite count manageable,
  split per Codex's iter-2 guidance if too large.

- **P0.6: Broadcast probe scan-share rule.** Pin the
  deterministic rule for distributing probe-side input
  slices across consumer workers under `Broadcast`. Default:
  `xxh3_64(source_tablet_id) mod worker_count`. Worker
  `w_i` reads tablets whose hash maps to `i`. Required for
  cross-replica determinism — assignment must be
  reproducible across runs.

- **P0.7: Fixture matrix identification.**
  - 1M × 100K in-process `INNER JOIN` baseline.
  - 10M × 1M distributed `INNER JOIN` at 2 / 4 / 8 workers
    (`HashPartitioned`).
  - 10M × 10M distributed `INNER JOIN` at 4 / 8 workers
    (memory-pressure fixture; executor evidence row).
  - 4-cell NULL-key matrix (`Inner`): build-null,
    probe-null, both-null, neither-null.
  - Multi-column key: 10M × 1M on `(region, customer_id)`.
  - Duplicate-output-name fixture (both tables have `id`
    column; bound IR carries both; downstream projects by
    ordinal).
  - Hash-partitioned left-producer SIGKILL replay.
  - Hash-partitioned right-producer SIGKILL replay.
  - Join-worker SIGKILL retry.
  - Broadcast under-cap correctness.
  - Broadcast over-cap terminal failure.
  - Broadcast build-publish SIGKILL replay.
  - Broadcast probe scan-share determinism (two runs).
  - Build-side spill engagement (forced memory cap).
  - Build-side spill exhaustion (forced spill cap;
    terminal).
  - Cross-replica determinism (digest pair per strategy).
  - Binder rejection: non-equi `ON`, key-type mismatch,
    float key, computed key, ambiguous unqualified
    column, unknown qualifier.
  - Skew observation (90%-skew fixture; per-partition
    row counts).
  - ADR 003 single-exchange GROUP BY regression check.

- **P0.8: Architecture guard literal text.** Predicates
  for G16, G17, G18 ready for
  `crates/toron/tests/architecture_test.rs`:
  - **G16.** Grep `crates/toron/src/exec/**` +
    `crates/toron/src/distributed/**` for identifiers
    matching `SortMergeJoin`, `NestedLoopJoin`,
    `CrossJoin`, `SemiJoin`, `AntiJoin`, `OuterJoin`,
    `LeftJoin`, `RightJoin`, `FullJoin`. None appear in
    production paths (test fixtures excepted). Permitted:
    `Join`, `HashPartitioned`, `Broadcast`.
  - **G17.** `ExecutionPlan` has exactly one join-shaped
    variant named `Join`. `JoinStrategy` has exactly two
    variants: `HashPartitioned`, `Broadcast`. Compile-
    time exhaustiveness + grep guard against other
    variant names (`HashJoin`, `BroadcastJoin`).
  - **G18.** `StagePlan::upstream_stage: Option<StageId>`
    does not appear as a field in
    `crates/toron/src/distributed/**` — only
    `upstream_stages: Vec<StageId>`. Scheduler dispatch
    reads `.iter().all(...)` over the vec.

- **P0.9: Soft-gate threshold calibration.**
  - Default `partition_count` for hash-partitioned join =
    cluster worker count (configurable).
  - `memory_per_task_bytes` (ADR 003 default 64 MiB,
    unchanged).
  - `spill_bytes_reserved_per_query` (ADR 003 default
    512 MiB, unchanged).
  - Hash-spill page size = 64 KiB.
  - Hash-spill cache size = 8 MiB per join task.
  - `broadcast_build_bytes_per_task` = 64 MiB.
  - `broadcast_replicated_bytes_per_query` = 1 GiB.
  - Throughput target: ≥ 1.5× speedup per worker doubling
    on 10M × 1M `INNER JOIN`.

- **P0.10: Bench-row schema extension.** ADR 003 base
  columns plus:
  - `strategy: HashPartitioned | Broadcast`
  - `build_rows`, `probe_rows`, `output_rows`
  - `left_shuffle_bytes`, `right_shuffle_bytes` (hash-
    partitioned only)
  - `broadcast_replicated_bytes` (broadcast only)
  - task-total `pulled_shuffle_bytes`, `published_shuffle_bytes`,
    `matched_rows`, `emitted_rows`, `emitted_batches`
  - `task_wall_ms`, `shuffle_pull_decode_ms`, `build_ms`,
    `probe_ms`, `spill_ms`, `output_publish_ms`,
    `task_setup_ms`, `batch_encode_ms`, `batch_send_ms`,
    `batch_decode_ms`, `dispatch_wait_ms`,
    `telemetry_lock_ms`, `framework_overhead_ms`
  - `spill_bytes`, `retry_count`, `stale_rejection_count`,
    `cancellation_cleanup_count`
  - skew partition counts remain observation-only for V1

- **P0.11: Runtime telemetry stacked into ADR 004.** The
  four ADR 003 watch counters (pulled shuffle bytes, retry
  count, stale rejection count, cancellation cleanup count)
  plus executor/join phase spans land with ADR 004 because
  the promotion work is still staged. The bench-row schema
  (P0.10) now emits the attribution columns ADR 005 needs.

### Phase 1 — Qualifier-aware logical IR; `JoinKind`; `JoinStrategy`

Lands first because every later phase depends.

Files:
- `crates/toron/src/ir.rs` — `ColumnRef`, `TableRef`,
  `JoinKind`, `JoinStrategy`, `LogicalPlan::Join`; rewrite
  `Expr::Column` variant.
- `crates/toron/src/ir/execution.rs` —
  `ExecutionPlan::Join`.
- `crates/toron/src/distributed/task.rs` — `ScanShare`
  struct; change `TaskInputBinding::Scan` from fieldless
  to `Scan { share: Option<ScanShare> }`. Every callsite
  that constructed `TaskInputBinding::Scan` rewrites to
  `TaskInputBinding::Scan { share: None }`; match arms
  update accordingly.
- `crates/toron/src/sql_planner.rs` — emit qualified
  `ColumnRef`.
- `crates/toron/src/rune_engine.rs` — emit unqualified
  `ColumnRef`; no join lowering in V1.
- `crates/toron/src/binder.rs` — single-input path
  refactored to use `ColumnRef`; unchanged in behavior.
- Test fixtures and benches that build plans manually.

Work:
- `TableRef(String)`, `ColumnRef { qualifier:
  Option<TableRef>, name: ColumnName }`.
- `JoinKind { Inner }`, `JoinStrategy { HashPartitioned,
  Broadcast }`.
- `LogicalPlan::Join`, `ExecutionPlan::Join`.
- `ScanShare { worker_index: u32, worker_count: u32 }` in
  `src/distributed/task.rs`; `TaskInputBinding::Scan
  { share: Option<ScanShare> }` (was fieldless).
- Rewrite `Expr::Column(String) → Expr::Column(ColumnRef)`
  at every callsite per P0.5.
- Rewrite every `TaskInputBinding::Scan` construction site
  to `TaskInputBinding::Scan { share: None }`. Match arms
  update.
- Extend `ExecutionPlan::schema()` for `Join`.
- Single-input binder path unchanged.
- Architecture guard G17.

Tests:
- `column_ref_single_input_resolves_unqualified`.
- `column_ref_single_input_resolves_qualified_against_match`.
- `column_ref_single_input_rejects_qualified_against_wrong`.
- `join_kind_single_variant`.
- `join_strategy_two_variants`.
- `scan_share_round_trip` — `ScanShare { worker_index: 2,
  worker_count: 4 }` serializes / deserializes
  byte-identical.
- `task_input_binding_scan_share_none_matches_legacy_source_stage`
  — existing source-stage `Scan { share: None }` behavior
  unchanged.
- `architecture_test::g17_passes`.

### Phase 2 — Join binder rules

Files:
- `crates/toron/src/binder.rs` — `bind_join` + multi-scope
  resolver.
- `crates/toron/src/error.rs` — new `BindError` variants.

Work:
- New `BindError` variants:
  - `AmbiguousColumnRef { name, found_in: Vec<TableRef> }`
  - `UnknownTableRef { qualifier }`
  - `UnknownColumn { name }`
  - `JoinPredicateNotEqui`
  - `JoinKeyTypeMismatch { left_type, right_type }`
  - `JoinKeyTypeUnsupported { type_name }` (floats etc.)
  - `JoinComputedKeyUnsupported`
  - `JoinPartitionCountMismatch { left, right }`
  - `JoinKeyExchangeMismatch`
  - `BroadcastBuildSidePlacement` — **structural** check
    (not stats-driven intent inference): under
    `JoinStrategy::Broadcast`, the left subtree must
    terminate in the single-partition publish shape
    (`Exchange { partition_count: 1 }` or
    `BroadcastPublish` per P0.2). Missing or wrong-count
    left publish shape rejects. Any right-side publish shape,
    including both-sides-published, rejects as ambiguous.
    The binder cannot know what the caller "meant" to
    put on the left without statistics; the check
    enforces only the caller-discipline structural
    contract (build = left).
  - `BroadcastCapExceededAtBind { dimension, estimated, cap }`
- Implement `bind_join`:
  1. Bind left + right sub-plans recursively.
  2. Collect table-ref scopes for left and right from
     `LogicalPlan::ScanNamed` leaves + SQL aliases.
  3. For each `(left_expr, right_expr)` in `on`:
     - Both must be `Expr::Column(_)`; else
       `JoinComputedKeyUnsupported`.
     - Resolve left expr's `ColumnRef` against left scope
       first; if it resolves against right instead, auto-
       swap the pair.
     - Resolve right expr's `ColumnRef` against the other
       scope.
     - Ambiguous unqualified → `AmbiguousColumnRef`.
     - Unknown qualifier → `UnknownTableRef`.
     - Type mismatch → `JoinKeyTypeMismatch`.
     - Float type → `JoinKeyTypeUnsupported`.
  4. Construct `output_schema` = left fields ++ right
     fields. Nullability preserved (no nullable
     adjustment for `Inner`). Duplicate names permitted.
  5. For `HashPartitioned`: assert left and right inputs
     terminate in `Exchange` with matching
     `partition_count` and `partition_key`. Mismatch →
     typed error.
  6. For `Broadcast`: assert left (build) input terminates
     in `Exchange { partition_count: 1 }` (or
     `BroadcastPublish` per P0.2). Any plan whose left
     input does not terminate in that single-partition
     publish shape rejects with `BroadcastBuildSidePlacement`.
     Right (probe) does not need `Exchange`; any right-side
     publish shape, including both-sides-published, rejects
     with `BroadcastBuildSidePlacement`.
  7. If broadcast input sizes statically known (scan-stats
     hint) and known-over-cap, reject with
     `BroadcastCapExceededAtBind`.

Tests:
- `bind_join_inner_round_trip`.
- `bind_join_resolves_qualified_keys`.
- `bind_join_resolves_unqualified_unambiguous`.
- `bind_join_rejects_unqualified_ambiguous`.
- `bind_join_rejects_unknown_qualifier`.
- `bind_join_rejects_non_equi`.
- `bind_join_rejects_key_type_mismatch`.
- `bind_join_rejects_float_key`.
- `bind_join_rejects_computed_key`.
- `bind_join_rejects_partition_count_mismatch` (hash-
  partitioned).
- `bind_join_auto_swaps_pair_order`.
- `bind_join_permits_duplicate_output_column_names`.
- `bind_broadcast_join_left_build_succeeds`.
- `bind_broadcast_join_rejects_right_build_placement`.
- `bind_broadcast_join_rejects_static_over_cap`.
- `bind_join_deterministic_under_fingerprint`.

### Phase 3 — Multi-upstream `StagePlan`; scheduler barrier

Files:
- `crates/toron/src/distributed/fragment.rs` —
  `StagePlan::upstream_stages: Vec<StageId>`.
- `crates/toron/src/distributed/scheduler.rs` — barrier
  iterates vec.
- `crates/toron/src/distributed/task_state.rs` —
  acceptance index multi-upstream traversal.
- `crates/toron/tests/architecture_test.rs` — G18.

Work:
- Mechanical rename per P0.4. Every callsite updates:
  `Some(s) → vec![s]`, `None → vec![]`.
- Scheduler barrier check: `stage_plan.upstream_stages.iter().all(|s|
  every_task_in_stage_s_is_live_accepted(s))`.
- Cancellation cascade re-evaluates barrier on upstream
  `LiveAccepted → Invalidated → Open` transitions.
- G18 architecture test.

Tests:
- `stage_plan_supports_two_upstreams`.
- `scheduler_blocks_join_dispatch_until_both_upstreams_accepted`.
- `scheduler_handles_one_upstream_invalidation_independently`.
- `existing_groupby_single_upstream_unchanged` (regression
  gate).
- `architecture_test::g18_passes`.

### Phase 4 — Fragmenter rewrite (both strategies)

Files:
- `crates/toron/src/distributed/fragment.rs` (rewrite).

Work:
- Replace "single exchange supported" check with recursive
  walk:
  - `Scan` / `Filter` / `Project` / `Aggregate` fold into
    current stage; no boundary.
  - `Exchange` cuts a stage boundary.
  - `Join { strategy: HashPartitioned }` cuts a consumer
    stage with two upstream stages.
  - `Join { strategy: Broadcast }` cuts a consumer stage
    with one upstream (the build-publish stage); the
    probe input becomes per-worker scan shares within the
    consumer stage's tasks.
- Reject `Join { strategy: HashPartitioned }` without
  `Exchange` on both inputs:
  `FragmentError::JoinInputNotExchanged`.
- Reject `Join { strategy: Broadcast }` without
  `Exchange { partition_count: 1 }` (or `BroadcastPublish`
  per P0.2) on left:
  `FragmentError::BroadcastBuildNotPublished`.
- Stage-id allocation: bottom-up walk, topological order.
- `ExchangeId` allocation: one per `Exchange`, stable
  across fragmenter calls on the same bound plan.
- Build join task `TaskSpec`:
  - `HashPartitioned`: two `ShuffleConsumer` entries.
  - `Broadcast`: one `Scan` + one `ShuffleConsumer`.
- Hash-partitioned worker routing discriminates the two
  shuffle inputs by `producer_stage_id` matching the bound
  `Join` operator's left/right subtree. Broadcast routing is
  by binding variant and is covered in Phase 7.
- Broadcast probe scan-share assignment per P0.6: consumer
  task `w_i` reads tablets whose
  `xxh3_64(tablet_id) mod worker_count == i`.

Tests:
- `fragment_inner_join_hash_partitioned_emits_three_stages`.
- `fragment_inner_join_broadcast_emits_two_stages`.
- `fragment_hash_join_stage_has_two_upstreams`.
- `fragment_broadcast_join_stage_has_one_upstream`.
- `fragment_hash_join_task_has_two_shuffle_consumers_distinct_exchanges`.
- `fragment_broadcast_join_task_has_scan_plus_shuffle_consumer`.
- `fragment_rejects_hash_join_without_pre_shuffle`.
- `fragment_rejects_broadcast_without_left_publish`.
- `fragment_broadcast_probe_scan_share_deterministic`.
- `fragment_exchange_ids_stable_across_repeated_fragmentation`.
- `fragment_groupby_single_exchange_unchanged`.

### Phase 5 — Local hash-join operator + build-side spill

Files:
- `crates/toron/src/exec/hash_join.rs` (new) —
  `HashJoinOperator`, build + probe + spill.
- `crates/toron/src/distributed/hash_spill.rs` (new) —
  `HashSpillStore` trait + `EncryptedHashSpillStore`.
- `crates/toron/src/server/spill.rs` (extend per P0.3 if
  random-access API needs to land).
- `crates/toron/src/distributed/budget.rs` (verify) —
  `SpillBytes` dimension already exists; no new variant
  for spill.
- `crates/toron/src/exec.rs` — wire `Join` into operator
  dispatch.

Work:
- `HashKey` newtype computed via `xxh3_64` over canonical-
  encoded key values, consistent with ADR 003 §5 partition
  assignment.
- `HashJoinOperator`:
  - Build phase pulls left (build) input; insert non-null-
    key rows.
  - Probe phase pulls right (probe) input; lookup + emit.
  - NULL-key build rows dropped; NULL-key probe rows
    produce no matches.
  - Multi-column keys: pairwise comparison per declared
    pair order; all components must be non-NULL and equal.
  - Output: left cols ++ right cols.
  - Build memory accounting per row insertion;
    reservation against `memory_per_task_bytes`.
- `HashSpillStore` trait: `write_bucket(bucket_id, rows)`,
  `read_bucket(bucket_id) → Vec<Row>`, `cleanup()`.
- `EncryptedHashSpillStore`:
  - One spill file per join task; `SpillId` allocated at
    task start.
  - 64 KiB pages (P0.9).
  - AES-256-GCM-SIV via M10; per-page nonce =
    `SpillId + page_index`.
  - Index: `HashMap<HashBucketId, PageRange>`.
  - LRU page cache (default 8 MiB per task; P0.9).
  - **Disk bytes** reserve against
    `QueryBudget::spill_bytes_reserved_per_query`.
  - **In-memory bytes** — bucket → page-range index, LRU
    decrypted-page cache, spill-writer buffers in flight,
    per-bucket scratch — reserve against
    `QueryBudget::memory_per_task_bytes`. The cache is
    part of the task memory budget, not additional to
    it. Spilling lets the table grow beyond what fits in
    memory by shifting persistent state to disk while
    keeping resident bookkeeping bounded; it does not
    let the task escape its memory cap.
- Operator spill engagement:
  - On insert overrun: promote (flush in-mem buckets to
    encrypted pages; switch to spill writes). **One-way
    per task attempt.**
  - Probe lookup: in-memory first (pre-promotion buckets);
    fall back to spill (with LRU cache).
- Cleanup on task termination.

Tests (all in-process):
- `inner_join_basic_match`.
- `inner_join_one_to_many`.
- `inner_join_many_to_many`.
- `inner_join_no_matches`.
- 4-cell NULL-key matrix per P0.7 — every cell digest-
  equal to reference row-level join.
- `inner_join_multi_column_key`.
- `inner_join_duplicate_output_column_names_permitted`.
- `hash_spill_round_trip`.
- `hash_spill_encrypted_pages_round_trip`.
- `hash_spill_lru_cache_hit_path`.
- `hash_join_with_spill_engages_at_memory_cap` —
  digest-equal to in-memory baseline.
- `hash_join_spill_exhaustion_fails_terminal`.
- `hash_join_oversized_single_row_fails_memory_typed` —
  single serialized build row cannot fit in the spill
  writer page-buffer memory budget.
- `hash_spill_cache_and_index_charged_against_task_memory`
  — Phase 5 memory-accounting test: configure a fixture
  where the spilled bucket count is large enough that the
  bucket → page-range index plus the LRU page cache plus
  spill-writer buffers would exceed
  `memory_per_task_bytes` if naively unbounded. Verify
  the operator either (a) shrinks the cache / evicts to
  stay under the cap and completes correctly, or (b)
  fails terminally with
  `BudgetExhausted { dimension: Memory }`. Verify it
  does **not** silently exceed the cap.
- `hash_spill_cleanup_on_termination`.
- `hash_join_output_digest_stable_after_normalization`.

### Phase 6 — Distributed hash-partitioned integration

Files:
- `crates/toron/src/server/worker.rs` (extend) — join
  task dispatch.
- `crates/toron/src/coordinator.rs` (extend) — fragmenter
  call.

Work:
- Worker receives `TaskSpec` for a join task with two
  `ShuffleConsumer` inputs.
- Worker identifies build vs probe by matching
  `TaskSpec.inputs[i].producer_stage_id` against the bound
  `Join` operator's left / right subtree terminal
  `Exchange` stages. Input order in `inputs` is not load-
  bearing.
- Build phase pulls all left-side `ProducerPartitionRef`s
  via `PullPartition`; inserts into `HashJoinOperator`.
- Probe phase pulls right-side; probes; emits `TaskBatch`.
- Pull-side validation: `ExchangeId`, `PartitionId`,
  producer `StageId`, `AttemptId` all checked. Mismatch →
  `PartitionMissing { reason: AttemptStale }` per ADR 003.

Tests:
- `distributed_inner_join_two_workers_no_failure`.
- `distributed_inner_join_four_workers_no_failure`.
- `distributed_inner_join_eight_workers_no_failure`
  (scaling soft gate).
- `distributed_join_multi_column_key`.
- `distributed_join_output_digest_equals_in_process_baseline`.
- `distributed_join_with_per_worker_spill_engagement`.
- `distributed_join_input_order_independence` (swapping
  `inputs` order does not affect output).

### Phase 7 — Bounded broadcast strategy

Files:
- `crates/toron/src/distributed/fragment.rs` (extend) —
  broadcast 2-stage DAG path (lands in Phase 4; Phase 7
  exercises end-to-end).
- `crates/toron/src/distributed/budget.rs` —
  `BudgetDimension::BroadcastReplication` variant;
  `QueryBudget::broadcast_build_bytes_per_task` and
  `broadcast_replicated_bytes_per_query` fields.
- `crates/toron/src/planner.rs` (extend) — broadcast
  admissibility helper.
- `crates/toron/src/coordinator.rs` (extend) — dispatch-
  time cap check.
- `crates/toron/src/exec/hash_join.rs` (extend) —
  broadcast probe path (per-worker probe scan; no shuffle
  on probe).

Work:
- Add `QueryBudget` fields per P0.9 defaults.
- Add `BudgetDimension::BroadcastReplication` variant.
- Fragmenter (Phase 4 already produces 2-stage broadcast
  DAG) emits consumer tasks with
  `TaskInputBinding::Scan { share: Some(ScanShare
  { worker_index: i, worker_count: n }) }` for the probe
  input and `TaskInputBinding::ShuffleConsumer` for the
  broadcast build manifest. Each consumer task gets a
  distinct `worker_index` in `[0, worker_count)`. The
  scan-share rule per P0.6: worker `w_i` scans tablets
  satisfying `xxh3_64(tablet_id) mod worker_count ==
  worker_index`.
- Worker discriminates broadcast inputs by **binding
  variant**: `Scan` is probe, `ShuffleConsumer` is build.
  No `producer_stage_id` matching for the scan side
  (the scan has no upstream stage).
- Coordinator dispatch-time cap check: compare actual build
  manifest `size_bytes` against the broadcast-specific byte
  caps. Over-cap on those caps →
  `BudgetExhausted { dimension: BroadcastReplication }`.
  The inherited `memory_per_task_bytes` cap remains
  `BudgetExhausted { dimension: Memory }`. Terminal; no
  silent strategy switch.
- Broadcast producer-loss handler in scheduler: build
  manifest invalidated; rebuild; all probe consumers
  redispatch.

Tests:
- `broadcast_join_under_cap_correctness` (digest equals
  hash-partitioned baseline).
- `broadcast_join_over_cap_terminal_failure`.
- `broadcast_build_producer_loss_replay` (digest byte-
  identical).
- `broadcast_payload_not_inlined_in_start_task`
  (architecture-style test).
- `broadcast_probe_scan_share_deterministic_across_retries`.
- `broadcast_probe_scan_share_deterministic_across_runs`.
- `broadcast_task_carries_exactly_one_scan_and_one_shuffle_consumer`
  — architectural-style fixture: every broadcast consumer
  `TaskSpec.inputs` contains exactly one
  `Scan { share: Some(_) }` and exactly one
  `ShuffleConsumer`. Ambiguity would break binding-
  variant input routing.
- `broadcast_probe_scan_share_partition_covers_all_tablets_disjointly`
  — for the V1 default share rule, verify across the
  full assigned worker set that every probe tablet is
  scanned by exactly one worker (no gaps, no
  duplication).

### Phase 8 — Failure modes + cross-replica determinism

Files:
- `crates/toron/tests/distributed_join_failure_test.rs`
  (new).
- `crates/toron/tests/distributed_join_determinism_test.rs`
  (new).

Work:
- **Hash-partitioned left producer SIGKILL** fixture: 4-
  worker `INNER JOIN`; SIGKILL build-stage producer host
  after manifest accepted; rebuild; downstream join tasks
  redispatch; final digest byte-identical to no-failure
  baseline.
- **Hash-partitioned right producer SIGKILL** fixture:
  symmetric.
- **Join-worker SIGKILL** fixture: retry on different
  worker without invalidating upstream; final digest
  byte-identical.
- **Broadcast build-publish SIGKILL** (Phase 7).
- **Cross-replica determinism** fixture: two independent
  multi-worker runs of the same join query (one with
  SIGKILL); both produce byte-identical normalized digests
  for **each** strategy. Reuses
  `canonical_normalize(batches, SortPolicy::FullRowLex)`.

Tests: each cross-references hard gates from ADR §"Hard
gates."

### Phase 9 — Architecture guards G16-G18

Files:
- `crates/toron/tests/architecture_test.rs` (extend).

Work:
- G16: grep across `crates/toron/src/exec/**` and
  `crates/toron/src/distributed/**` for banned join-
  algorithm identifiers; test rejects appearance.
- G17: compile-time + grep `ExecutionPlan` has only `Join`
  as join-shaped variant; `JoinStrategy` has exactly two
  variants.
- G18: grep `StagePlan::upstream_stage: Option` does not
  reappear; scheduler reads `Vec<StageId>` semantics.
- **Broadcast input-shape assertion** (per ADR §5 and §6):
  every `TaskSpec` emitted by the fragmenter for a
  `Join { strategy: Broadcast }` stage carries exactly
  one `TaskInputBinding::Scan { share: Some(_) }` and
  exactly one `TaskInputBinding::ShuffleConsumer`. Any
  other shape (two scans, two shuffle consumers, scan
  without share, scan with share-but-no-broadcast,
  mixed-strategy task) is a fragmenter bug. Enforced as
  both a fragmenter unit-test that snapshots `inputs` for
  the broadcast fixture and an architecture-test guard over
  the broadcast fragmenter path.

### Phase 10 — Bench matrix and promotion

Records all promotion gates in
`plans/impl/004_distributed_inner_equi_join_bench_results.md`.
That artifact contains quick local smoke rows, the promotion-
scale matrix, and the recorded soft-gate scaling miss.

Files:
- `crates/toron/benches/distributed_inner_equi_join.rs`
  (new).

Work:
- Bench flag: `--distributed-inner-equi-join-only`.
- Required rows per P0.7.
- Per-row metrics per P0.10.

Verification commands:

```text
# Local quick verification
cargo fmt
cargo check --workspace
cargo test -p toron
cargo test -p toron --test architecture_test
cargo test -q exec::hash_join
cargo test -q distributed::hash_spill
cargo test -q distributed::budget
cargo test -q distributed::fragment
cargo test -q binder::join
cargo test -p toron --test distributed_join_failure_test
cargo test -p toron --test distributed_join_determinism_test

# Pre-promotion full verification
cargo test --workspace
cargo bench --bench distributed_inner_equi_join --no-run
cargo bench --bench distributed_inner_equi_join -- --quick \
    --distributed-inner-equi-join-only
cargo bench --bench distributed_inner_equi_join -- \
    --distributed-inner-equi-join-only --strategy=hash \
    --workers=2,4,8 --build-rows=10000000 \
    --probe-rows=1000000
cargo bench --bench distributed_inner_equi_join -- \
    --distributed-inner-equi-join-only --strategy=hash \
    --workers=8 --build-rows=10000000 \
    --probe-rows=10000000
cargo bench --bench distributed_inner_equi_join -- \
    --distributed-inner-equi-join-only --strategy=hash,broadcast \
    --workers=8 --build-rows=1000000 \
    --probe-rows=10000000
cargo bench --bench distributed_inner_equi_join -- \
    --distributed-inner-equi-join-only --strategy=hash \
    --workers=4 \
    --build-rows=10000000 --probe-rows=1000000 \
    --composite-key
cargo bench --bench distributed_inner_equi_join -- \
    --distributed-inner-equi-join-only --kill-test
git diff --check
```

Suggested commit slices:

1. `chore: adr 004 prep — roadmap honesty + fragmenter
   audit notes`
2. `plan: adr 004 toron distributed inner equi-join`
3. `feat: adr 004 phase 1 — qualifier-aware ColumnRef +
   Join IR`
4. `feat: adr 004 phase 2 — join binder rules`
5. `feat: adr 004 phase 3 — multi-upstream StagePlan +
   scheduler barrier`
6. `feat: adr 004 phase 4 — fragmenter rewrite for both
   strategies`
7. `feat: adr 004 phase 5 — hash join operator + build-
   side encrypted spill`
8. `feat: adr 004 phase 6 — distributed hash-partitioned
   integration`
9. `feat: adr 004 phase 7 — bounded broadcast strategy`
10. `feat: adr 004 phase 8 — failure modes + cross-replica
    determinism`
11. `feat: adr 004 phase 9 — architecture guards G16-G18`
12. `feat: adr 004 phase 10 — telemetry, bench matrix +
    promotion`
    (updates `bench_results.md` with the full promotion
    matrix; marks ADR 004 shipped on roadmap)

Roadmap update: `plans/wip/roadmap.md` records ADR 004
promotion; forward sequence updated with ADR 005 candidates
evidence-driven from this bench matrix.

If hard gates pass, promote. If a hard gate fails, the
council decides among three explicit paths (ADR 003 / ADR
070 precedent):

1. **Revise V1.** Narrow scope (e.g., defer broadcast to
   ADR 004.x; defer multi-column to V1.x).
2. **Scope down.** Ship hash-partitioned + IR + binder +
   scheduler substrate; defer broadcast and spill to ADR
   004.x.
3. **Shelve.** Keep ADR 003 substrate unchanged; document
   the gap; reopen when fixture matrix tells where to look.

## Files touched

Implementation:

- `crates/toron/src/ir.rs` — `ColumnRef`, `TableRef`,
  `JoinKind`, `JoinStrategy`, `LogicalPlan::Join`; rewrite
  `Expr::Column(String) → Expr::Column(ColumnRef)`.
- `crates/toron/src/ir/execution.rs` —
  `ExecutionPlan::Join`.
- `crates/toron/src/distributed/task.rs` — `ScanShare`
  struct; `TaskInputBinding::Scan { share:
  Option<ScanShare> }` variant change.
- `crates/toron/src/binder.rs` — `bind_join` + multi-scope
  resolver; pair auto-swap.
- `crates/toron/src/error.rs` — new `BindError` variants.
- `crates/toron/src/sql_planner.rs` — emit qualified
  `ColumnRef`; lower `JOIN ... ON` with aliases; default
  `JoinStrategy::HashPartitioned`.
- `crates/toron/src/rune_engine.rs` — emit unqualified
  `ColumnRef`; no join lowering in V1.
- `crates/toron/src/distributed/fragment.rs` — multi-
  exchange fragmenter; `StagePlan::upstream_stages`
  rename; broadcast 2-stage DAG.
- `crates/toron/src/distributed/scheduler.rs` — multi-
  upstream barrier; broadcast cap dispatch check;
  cancellation cascade.
- `crates/toron/src/distributed/task_state.rs` —
  acceptance index multi-upstream.
- `crates/toron/src/distributed/budget.rs` —
  `BudgetDimension::BroadcastReplication` variant; new
  `QueryBudget` fields.
- `crates/toron/src/distributed/hash_spill.rs` (new) —
  `HashSpillStore` + `EncryptedHashSpillStore`.
- `crates/toron/src/exec/hash_join.rs` (new) —
  `HashJoinOperator` with build / probe / spill /
  broadcast probe paths.
- `crates/toron/src/exec.rs` — wire join dispatch.
- `crates/toron/src/server/worker.rs` (extend) — join
  task dispatch.
- `crates/toron/src/coordinator.rs` (extend) — fragmenter
  call; broadcast cap check.
- `crates/toron/src/planner.rs` (extend) — emit explicit
  `Exchange` above each `Join` input; broadcast
  admissibility helper.
- `crates/toron/src/server/spill.rs` (extend if P0.3 audit
  requires).
- `crates/toron/tests/architecture_test.rs` (extend) —
  G16, G17, G18.
- `crates/toron/tests/distributed_join_failure_test.rs`
  (new).
- `crates/toron/tests/distributed_join_determinism_test.rs`
  (new).
- `crates/toron/tests/hash_join_null_semantics_test.rs`
  (new) — 4-cell NULL × `Inner` matrix.
- `crates/toron/benches/distributed_inner_equi_join.rs`
  (new).
- `crates/toron/src/lib.rs` — wire up new modules.

Retired:

- (No ADR 003 surface retires here. The `StagePlan` rename
  preserves single-upstream semantics via `vec![s]`.)

Planning / evidence:

- `plans/adr/004_distributed_inner_equi_join.md`
  (this ADR pair).
- `plans/impl/004_distributed_inner_equi_join.md`
  (this file).
- `plans/impl/004_distributed_inner_equi_join_bench_results.md`
  (quick smoke evidence, full promotion matrix, and soft-gate
  scaling miss).

Roadmap:

- `plans/wip/roadmap.md` — record ADR 004 promotion;
  reframe ADR 005 candidates evidence-driven from join
  bench matrix.

Out of scope (V1.x or later) — per ADR §"Deferred":

- Outer / semi / anti / cross joins.
- Non-equi joins / theta-joins.
- Sort-merge join.
- Multi-way join planning.
- Cost-based build / probe swap.
- Cost-based strategy selection.
- Bloom / runtime filter pushdown.
- Skew mitigation.
- `USING` clause / comma-join.
- Floating-point join keys.
- SQL join hints.
- Intra-task build / probe pipelining.
- Hash-spill demotion to memory.
- SQL-surface auto-rename of duplicate output names.
- Third or further `JoinStrategy` variants.

## Risks and mitigations

- **R1: Multi-upstream barrier subtle bugs.**
  *Mitigation:* Phase 3 regression test for ADR 003 GROUP
  BY single-upstream; new tests for two-upstream join and
  empty-vec source stage.

- **R2: `Expr::Column` refactor ripple.** Every plan-
  building site changes in one commit (or two per P0.5).
  *Mitigation:* P0.5 enumerates callsites before Phase 1;
  refactor split if count is high.

- **R3: Broadcast cap underestimate at planner time.**
  Planner may see only scan-stats hint; actual manifest
  size differs.
  *Mitigation:* dispatch-time validation against actual
  manifest size is the load-bearing check; over-cap fails
  typed terminal.

- **R4: Build / probe matching ambiguity.** Worker matches
  by `producer_stage_id`, not `inputs[0]`.
  *Mitigation:* Phase 6 test
  `distributed_join_input_order_independence`.

- **R5: Hash-spill random-access API not in M10
  substrate.** If M10 page API is streaming-only.
  *Mitigation:* P0.3 audit; extend `SpillManager` with
  `read_page_at(spill_id, page_offset)` in Phase 5 if
  needed.

- **R6: Hash-table peak memory hard to predict.** Build-
  side cardinality + Arrow row size + hash-bucket
  overhead.
  *Mitigation:* spill is safety net; P0.9 calibration;
  bench records peak.

- **R7: Hash-spill page size wrong.** 64 KiB may be too
  large for narrow rows (page underfilled) or too small
  for wide rows / wide schemas (many pages per bucket).
  *Mitigation:* P0.9 calibration; bench records page
  utilization; tune via configuration.

- **R8: Cross-replica determinism noise.** Hash-bucket
  iteration order is non-deterministic.
  *Mitigation:* ADR 003 §13 full-row sort handles it.
  Broadcast probe scan-share is also deterministic (P0.6).

- **R9: Stage-id allocation non-deterministic.**
  Fragmenter walks the IR; a `HashMap` iteration anywhere
  could vary order.
  *Mitigation:* fragmenter uses sequential allocator on
  bottom-up walk; topological order pinned.

- **R10: ADR 003 GROUP BY regression on `StagePlan`
  rename.**
  *Mitigation:* Phase 3 regression test; per-phase re-run
  of ADR 003 hard-gate suite before merge.

- **R11: SQL planner refactor scope.** Supporting `JOIN`
  syntax + qualified column refs touches
  `sql_planner.rs` significantly.
  *Mitigation:* P0.5 enumerates callsites; Phase 1 lands
  the refactor; Phase 2 lands `JOIN` syntax lowering.

- **R12: Large-build joins always spill.** Default
  `memory_per_task_bytes` (64 MiB) is tight relative to
  typical analytical builds.
  *Mitigation:* acceptable for V1 — spill correctness is
  the contract; bench evidence shows engagement rate;
  V1.x can raise memory cap when evidence guides.

- **R13: Duplicate output names break downstream
  consumers.** Some downstream code path may assume name
  uniqueness.
  *Mitigation:* Phase 5 test verifies `Project` by ordinal;
  SQL planner enforces naming discipline (top-level
  ambiguous reference fails typed at SQL planner, not
  binder).

- **R14: Broadcast under-cap fixture stability.** Build
  size right at cap edge could flap.
  *Mitigation:* fixture uses build sizes well under cap
  (e.g., 1 MiB build with 64 MiB cap); over-cap fixture
  uses sizes well over.

- **R15: Spill + broadcast budget contention.** Shared
  spill cap with partition store.
  *Mitigation:* default `spill_bytes_reserved_per_query`
  calibrated to cover both. Per-dimension caps deferred
  to V1.x.

- **R16: Broadcast probe scan-share churn across
  deployments.** Worker count changes between runs shift
  the modulo assignment.
  *Mitigation:* within a single query run, worker count is
  fixed; assignment stable within the query. Across
  independent runs the canonical normalization handles
  any per-task output reordering.

- **R17: Hash-spill cleanup leak.** Failed or cancelled
  task could leak spill files.
  *Mitigation:* Phase 5 cleanup test verifies removal
  under all task-termination paths (complete, failed,
  cancelled).

## Definition of done

- All 11 Phase 0 pins recorded in the bench-results
  artifact.
- Phase 0 P0.11 runtime telemetry is stacked into ADR 004
  promotion and emits bench attribution columns.
- `LogicalPlan::Join` + `ExecutionPlan::Join` with
  `JoinKind { Inner }` and `JoinStrategy
  { HashPartitioned, Broadcast }`.
- `Expr::Column(String) → Expr::Column(ColumnRef)`
  refactor lands; single-input plans unchanged in
  behavior.
- Binder rejects with typed errors: non-equi, key-type
  mismatch, float key, computed key, ambiguous unqualified,
  unknown qualifier, partition-count mismatch, key-
  exchange mismatch, broadcast-build-side-placement,
  broadcast-cap-at-bind.
- Binder **permits duplicate output column names**.
- `StagePlan::upstream_stage` renamed to
  `upstream_stages: Vec<StageId>` mechanically; no compat
  shim.
- Fragmenter emits 3-stage hash-partitioned DAG and
  2-stage broadcast DAG.
- `HashJoinOperator` implements `Inner` equi-join with
  multi-column keys, NULL-drop on both sides, left=build,
  build-side memory reservation, spill engagement on
  memory cap, spill-cap exhaustion fails terminal.
- `EncryptedHashSpillStore` lands with bucket → page
  index, AES-256-GCM-SIV pages via M10, LRU cache,
  cleanup on termination. **All spill-resident in-memory
  state (bucket index, LRU cache, spill-writer buffers,
  scratch) reserves against
  `QueryBudget::memory_per_task_bytes`** — cache is part
  of task memory, not additional to it; cache + index
  pressure cannot silently exceed the memory cap.
- `TaskInputBinding::Scan { share: Option<ScanShare> }`
  variant landed (replacing fieldless `Scan`); existing
  source-stage callsites updated to `Scan { share: None }`
  with unchanged behavior.
- Distributed 4-worker hash-partitioned `INNER JOIN` at
  10M × 1M produces output byte-identical to in-process
  baseline after canonical normalization.
- Multi-column key `INNER JOIN` correct at 4 workers.
- Broadcast under-cap correct; over-cap fails terminal.
- Broadcast build worker loss replays with byte-identical
  digest.
- All three SIGKILL fixtures (left producer, right
  producer, join worker) produce byte-identical replay
  digests for each strategy.
- Cross-replica determinism: two independent runs (one
  with SIGKILL) produce byte-identical normalized digests
  per strategy.
- Build-side spill engagement: byte-identical to in-memory
  baseline.
- Build-side spill exhaustion: typed terminal failure.
- Broadcast probe scan-share assignment deterministic
  across runs.
- Skew observation soft gate: per-partition row counts
  recorded for a 90%-skew fixture.
- Architecture guards G16, G17, G18 land in
  `tests/architecture_test.rs` and pass.
- All 21 hard promotion gates pass; soft gates met or
  explicitly justified.
- ADR 002 + ADR 003 hard-gate suites stay green (per-phase
  regression rule).
- Bench artifact at
  `plans/impl/004_distributed_inner_equi_join_bench_results.md`
  records the full fixture matrix.
- Roadmap updated to record ADR 004 promotion; ADR 005
  candidates reframed by join bench evidence.
