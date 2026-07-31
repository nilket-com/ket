# Impl Plan — ADR 123: Multi-Upstream Live-Input Dispatch for Streamable-Both Consumers

**Migration for** `plans/adr/123_multi_upstream_live_input_dispatch.md`. **Scope:** Toron interior
— distributed scheduler (`scheduler.rs`) + worker (`server/mod.rs` one new dispatch arm).
**Non-scope:** `exec.rs`; IR; fragmenter; Wendelon; PBC; partition store; SQL grammar;
`streamable_unary_plan` widening; output-side joins-as-streamable-producers; A2 mixed
streamable+sealed; executor reorder; bounded-channel partition store.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Per-task readiness | `scheduler.rs:637-680 task_ready_for_dispatch` |
| Bail condition | `scheduler.rs:646` |
| Per-input walk (preserved bit-for-bit) | `scheduler.rs:651-680` |
| Dispatch-loop predicate | `scheduler.rs:626 stage_requires_stage_barrier`; call site `:244` |
| Barrier preserved | `scheduler.rs:705 stage_barrier_open` |
| `is_streamable_exchange_producer` | `task.rs:204` |
| `is_streamable_shuffle_consumer` | `task.rs:212` |
| `streamable_unary_plan` (STAYS) | `task.rs:225` excludes `ExecutionPlan::Join` |
| Worker dispatch tree | `server/mod.rs:210-275` |
| `has_live_shuffle_input` | `server/mod.rs:380` |
| `LiveInputStorageScan` (reused unchanged) | `server/mod.rs:3182` |
| `live_scan_overrides` (reused unchanged) | `server/mod.rs:3287` |
| `drive_streamable_plan` Join error arm (STAYS) | `server/mod.rs:3118` |
| `execute_bound` Join handling (left-then-right; preserved) | `exec.rs:790-815` |
| `fragment_hash_join_plan` | `distributed/fragment.rs:357` |
| 119 guard to delete + replace | `tests/architecture/distributed_execution/scheduler_dispatch.rs:266` |
| 122 customer | `crates/toron/tests/rune_relation_handles_test.rs` |

## Phase 0 — Write the failing gates first

In `crates/toron/tests/distributed_execution_test.rs` (or equivalent):

1. `multi_upstream_streamable_both_consumer_dispatches_before_either_upstream_accepts`
2. `multi_upstream_streamable_both_worker_uses_live_input_storage_scan`
3. `multi_upstream_streamable_both_consumer_starts_before_sealed_manifests`
4. `multi_upstream_streamable_both_all_hashpartitioned_kinds_execute_correctly` (parametric 8)
5. `multi_upstream_non_streamable_one_side_keeps_barrier`
6. `multi_upstream_non_streamable_both_sides_keeps_barrier`
7. `multi_upstream_multi_producer_fan_in_keeps_barrier`
8. `single_upstream_streamable_path_unchanged`
9. `execution_plan_join_still_rejected_by_drive_streamable_plan`

Gates 1-4 must fail on `main`; gates 5-9 must pass on `main`.

## Phase 1 — Scheduler: `is_streamable_producer_stage` predicate (D2, G7)

```rust
fn is_streamable_producer_stage(stage_id: StageId, stages: &[StagePlan]) -> bool {
    stages.iter()
        .find(|stage| stage.stage_id == stage_id)
        .is_some_and(|stage| {
            stage.tasks.len() == 1 && is_streamable_exchange_producer(&stage.tasks[0].spec)
        })
}
```

G7: exactly one streamable producer task per upstream stage; no multi-producer fan-in in V1.

## Phase 2 — Scheduler: relax `task_ready_for_dispatch` + `stage_requires_stage_barrier` (D1, G1)

Replace bail at `:646`:

```rust
let upstreams_all_streamable_producers = stage.upstream_stages.iter()
    .all(|us| is_streamable_producer_stage(*us, stages));
let live_eligible = (stage.upstream_stages.len() == 1
    && is_streamable_shuffle_consumer(&task.spec))
    || upstreams_all_streamable_producers;
if !live_eligible {
    return self.shuffle_inputs_fully_sealed(&task.spec, stages);
}
// per-input walk preserved verbatim
```

Mirror in `stage_requires_stage_barrier:626`.

## Phase 3 — Worker: new dispatch arm (D4, G5)

Insert after `is_streamable_exchange_producer` at `server/mod.rs:210-275`:

```rust
} else if self.has_live_shuffle_input(spec) {
    if is_streamable_shuffle_consumer(spec) {
        self.execute_live_shuffle_consumer(spec, task_context, morsel).await?
    } else {
        let storage = Arc::new(LiveInputStorageScan {
            base: self.storage.clone(),
            partition_store: self.partition_store.clone(),
            live_overrides: live_scan_overrides(spec),
        });
        let executor = Executor::with_context(storage, task_context);
        (executor.execute_bound(&spec.plan, morsel).await?, None)
    }
}
```

Other 4 dispatch branches untouched.

## Phase 4 — Behavior tests (Phase 0 gates land green)

- Gate 2: assert `LiveInputStorageScan` used (telemetry / task-spec state inspection).
- Gate 3: consumer task begins consuming before `accepted_manifests` contains either upstream.
- Gate 4: parametric loop over 8 `JoinKind` variants.

## Phase 5 — Architecture guards (G1-G8)

- **G1** `adr123_task_ready_for_dispatch_relaxes_for_streamable_both`.
- **G2** `adr123_streamable_unary_plan_still_excludes_join`.
- **G3** `adr123_drive_streamable_plan_still_errors_on_join`.
- **G4** `adr123_multi_upstream_barrier_holds_for_non_streamable_both` — **delete** old
  `adr119_multi_upstream_barrier_untouched`; add new.
- **G5** `adr123_worker_bridges_live_refs_to_non_streamable_consumer` — predicate-text + paired
  `LiveInputStorageScan` + `execute_bound` symbols co-located.
- **G6** `adr123_executor_pbc_wendelon_ir_fragmenter_unchanged`.
- **G7** `adr123_streamable_producer_stage_predicate_is_single_task`.
- **G8** `adr123_v1_covers_all_eight_hashpartitioned_join_kinds` — parametric variant list.

## Phase 6 — Verification

```sh
cargo fmt
cargo test -p toron distributed::scheduler::tests
cargo test -p toron --test architecture_test adr119 adr120 adr122 adr123
cargo test -p toron --test join_kind_test
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test wendelon_storage_execution_test
cargo test -p toron
cargo test -p wendelon
cargo check -p toron --all-targets
git diff --check
```

## Acceptance

All 9 behavior gates green; `join_kind_test.rs` passes exactly; ADR 122 tests pass exactly;
G1-G8 hold; 115-122 regressions hold; full Toron + Wendelon suites + `--all-targets` green.
Migration: `plans/adr/123_multi_upstream_live_input_dispatch.md`.
