# ADR 123: Multi-Upstream Live-Input Dispatch for Streamable-Both Consumers

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — converged on Codex's
iter-2 framing after three grounding-corrections-during-iteration in this ADR cycle alone (worker
bridge needed; all-kinds not Inner-only; drop the over-promising probe-side-scan-pauses gate).
**Category:** Toron interior — distributed scheduler + worker dispatch arm.
**Predecessors:** 117 (multi-upstream acceptance barrier — to relax for streamable-both),
119 (single-upstream per-task readiness + `LiveInputStorageScan` + cancellation chain — to extend),
120 (storage cancellation chain composes here), 122 (Rune-authored joins — the customer this ADR
finally pays off).
**Migration:** `plans/impl/0123_multi_upstream_live_input_dispatch.md`.

## Scale boundary

ADR 123 delivers **input-side live dispatch** for multi-upstream consumer stages when all upstream
stages are single streamable producer stages, covering all 8 HashPartitioned join kinds. Two-layer change:
**scheduler** relaxes `task_ready_for_dispatch` to allow live refs when
`is_streamable_producer_stage` holds for every upstream; **worker** adds one inserted dispatch arm
so a non-streamable consumer (Join) with live refs uses `LiveInputStorageScan` for inputs +
`execute_bound` for the plan. Non-scope: `exec.rs` executor logic (untouched; `:790`'s
left-then-right Join collection preserved); IR; Wendelon; PBC; fragmenter; partition store
(`append_live` Vec-buffered behavior preserved); broadcast strategy selection; mixed
streamable+sealed multi-upstream (A2); output-side joins-as-streamable-producers; SQL grammar;
**executor reorder for asymmetric build-then-probe** (named follow-on); **bounded-channel
partition store** (named follow-on).

## Doctrine — what V1 honestly delivers; what it explicitly defers

> Input-side live dispatch + worker consumption before sealed manifests. **No** claim about
> probe-side scan pause or natural backpressure — those require executor reorder + partition store
> redesign, both deliberately-named follow-ons.

The cut's load-bearing observables:
1. **Scheduler dispatches the consumer before either upstream `LiveAccepted`** (timestamp ordering).
2. **Worker uses `LiveInputStorageScan` for inputs** (consumption path honesty —
   [[feedback_cut_must_deliver_the_name]] composed across layers).
3. **Consumer begins consuming before either upstream manifest reaches `accepted_manifests`**
   (the end-to-end honest observable).
4. **All 8 HashPartitioned join kinds execute correctly** through the new dispatch arm.

What V1 does NOT claim: probe-side Wendelon scan pauses during build; natural backpressure flows
producer-side through the partition store. `exec.rs:790-815`'s Join arm collects
`left_batches`/`right_batches` via two sequential `execute_bound` calls (both materialize fully
before any join logic). `append_live` is Vec-buffered behind shuffle budget, not a bounded
backpressured channel. The asymmetric-build executor reorder and bounded-channel partition store
are explicitly **named follow-ons** in §"What to do next" — not part of 123.

## Decisions (promotion-locked)

**D1 — Scheduler: relax `task_ready_for_dispatch` for the streamable-both case.** Refactor
`scheduler.rs:637-680`'s outer bail. The per-input walk at `:651-680` is preserved bit-for-bit:

```rust
let upstreams_all_streamable_producers = stage.upstream_stages.iter()
    .all(|us| is_streamable_producer_stage(*us, stages));
let live_eligible = (stage.upstream_stages.len() == 1
    && is_streamable_shuffle_consumer(&task.spec))
    || upstreams_all_streamable_producers;
if !live_eligible {
    return self.shuffle_inputs_fully_sealed(&task.spec, stages);
}
// existing per-input walk
```

Mirror change in `stage_requires_stage_barrier:626`.

**D2 — A1 lock: all upstream stages must be single streamable producer stages.** New predicate
`is_streamable_producer_stage` requires exactly one task in the upstream stage and that task must
satisfy `is_streamable_exchange_producer` (G7). Mixed streamable+sealed and multi-producer fan-in
are deferred follow-ons.

**D3 — Output-side restriction preserved unchanged.** `task.rs::streamable_unary_plan` still
rejects `ExecutionPlan::Join` (G2). `server/mod.rs::drive_streamable_plan` still errors on Join in
the live stream path (G3). Output-side joins-as-streamable-producers is a deliberately-named
follow-on requiring executor restructuring.

**D4 — Worker: new dispatch arm for live refs + non-streamable consumer.** In
`server/mod.rs:210-275`, insert one branch after `is_streamable_exchange_producer`, bifurcating
`has_live_shuffle_input(spec)`:

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

Other 4 dispatch branches untouched. Reuses `LiveInputStorageScan:3182` + `live_scan_overrides:3287`
unchanged. Pinned by G5.

**D5 — V1 covers all 8 HashPartitioned join kinds.** `execute_bound` runs all variants via the
same buffered path at `exec.rs:790`'s single `ExecutionPlan::Join` entry — no per-kind constraint.
V1: Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, RightSemi, LeftAnti, RightAnti. Pinned by
the parametric behavior gate (G8).

**D6 — Retarget `adr119_multi_upstream_barrier_untouched`.** Delete the 119 guard + add G4
asserting barrier preserved + path-into-barrier requires non-streamable-both. Cleaner git diff;
honest about the invariant change.

**D7 — Executor and partition store unchanged.** No `exec.rs` change (the all-kinds claim relies
on the existing left-then-right collection behavior). No partition store change. `append_live`'s
Vec-buffered behavior is the V1 baseline.

**D8 — Scope boundary.** No `exec.rs` change. No IR. No Wendelon. No PBC. No
`distributed/fragment.rs`. No partition store. No SQL grammar. No 117
`reject_sort_barrier_inside_join_inputs` change. No 120 cursor surface change. The change surface
is precisely: 2 scheduler functions + 1 worker dispatch arm + 1 deleted guard + 8 new/retargeted
guards.

## Behavior gates

- **`multi_upstream_streamable_both_consumer_dispatches_before_either_upstream_accepts`** —
  scheduler timestamp ordering.
- **`multi_upstream_streamable_both_worker_uses_live_input_storage_scan`** — worker-bridge
  honesty.
- **`multi_upstream_streamable_both_consumer_starts_before_sealed_manifests`** — end-to-end
  observable that the consumer task begins consuming live partitions before either upstream
  manifest reaches `accepted_manifests`.
- **`multi_upstream_streamable_both_all_hashpartitioned_kinds_execute_correctly`** — parametric
  over all 8 `JoinKind` variants.
- `multi_upstream_non_streamable_one_side_keeps_barrier` (D2 boundary regression).
- `multi_upstream_non_streamable_both_sides_keeps_barrier` (D2 boundary regression).
- `multi_upstream_multi_producer_fan_in_keeps_barrier` (D2 boundary regression).
- `single_upstream_streamable_path_unchanged` (119 regression).
- `execution_plan_join_still_rejected_by_drive_streamable_plan` (G3 regression).

Regressions: 115/116/117/118/119/120/121/122 suites green; `join_kind_test.rs` green; ADR 122 Rune
join tests green.

## Architecture guards

- **`adr123_task_ready_for_dispatch_relaxes_for_streamable_both`** (G1) — body contains
  `is_streamable_producer_stage` + the live-eligible disjunction.
- **`adr123_streamable_unary_plan_still_excludes_join`** (G2) — `task.rs::streamable_unary_plan`
  rejection arm still contains `ExecutionPlan::Join { .. }`.
- **`adr123_drive_streamable_plan_still_errors_on_join`** (G3) — `server/mod.rs::drive_streamable_plan`
  error arm still contains the verbatim Join rejection.
- **`adr123_multi_upstream_barrier_holds_for_non_streamable_both`** (G4) — replaces
  `adr119_multi_upstream_barrier_untouched`. Asserts `stage_barrier_open` body unchanged + path
  into it requires non-streamable-both.
- **`adr123_worker_bridges_live_refs_to_non_streamable_consumer`** (G5) — `server/mod.rs` contains
  the new `has_live_shuffle_input(spec)`-branching shape with `LiveInputStorageScan` +
  `execute_bound` paired in the new else-branch.
- **`adr123_executor_pbc_wendelon_ir_fragmenter_unchanged`** (G6) — `exec.rs`, `ir.rs`, `pbc.rs`,
  `distributed/fragment.rs`, Wendelon source contain no ADR 123 markers.
- **`adr123_streamable_producer_stage_predicate_is_single_task`** (G7) — predicate body requires
  `stage.tasks.len() == 1` and applies `is_streamable_exchange_producer` to that single task.
- **`adr123_v1_covers_all_eight_hashpartitioned_join_kinds`** (G8) — parametric test source
  iterates all 8 `JoinKind` variants.
- **Recipe:** `cargo test -p toron` + `cargo test -p wendelon` + `cargo test -p toron --test
  architecture_test adr119 adr120 adr122 adr123` + `cargo check -p toron --all-targets`.

## Council history

iter-0: 3-of-3 on topic A (multi-upstream live pipelining). iter-1: Claude conflated input-side
and output-side pipelining axes; Codex grounded `streamable_unary_plan` + `drive_streamable_plan`
both exclude `Join`; Claude conceded; V1 = input-side only. iter-2: Claude framed cut as
scheduler-only; Codex grounded `LiveInputStorageScan` double-gated by
`is_streamable_shuffle_consumer`; Claude conceded; V1 adds worker dispatch arm (D4). Codex also
conceded his iter-0's Inner-only narrowing — V1 covers all 8 kinds (D5). iter-3: Claude
uncritically locked Gemini's "natural backpressure for free" framing as gate 2; Codex grounded
`exec.rs:790` collects left-then-right to EOF + `append_live` is Vec-buffered behind shuffle
budget. **Claude conceded gate 2**; iter-3 drops it and reframes doctrine to honest live-input-
dispatch + worker-consumption claims only. Three grounding-corrections in one ADR cycle,
ninth-overall in the 119→123 arc. The discipline:
[[feedback_ground_the_premise_before_sketching_scope]] applied recursively at every layer,
including peer claims — Gemini's elegant framing was wrong on two axes; Codex's grep caught it.
The mechanical check: after every "the existing system handles this for free" claim, grep the
relevant code.

## What to do next

- **Executor reorder for asymmetric build-then-probe** — build = smaller side first, probe =
  stream second. Would enable probe-side scan pause via storage cancellation + 120 cursor drop.
- **Bounded-channel partition store** — replace `append_live`'s Vec-buffered shuffle-budget shape
  with a bounded channel that backpressures the producer. Pairs with executor reorder.
- **Output-side joins-as-streamable-producers** — widen `streamable_unary_plan` + restructure
  executor.
- **Mixed streamable+sealed multi-upstream (A2)** — deferred permissive case from D2.
- **Multi-producer fan-in live dispatch** — deferred until the live-ref materialization/readiness
  rules can handle multiple producer tasks per upstream stage.
- **Broadcast join strategy selection** — `JoinStrategy::Broadcast` constructor + cost-based or
  hint-based selection.
- **Streamable `Sort`/`Aggregate` coverage** (120 V2).
- **SQL joins around derived tables / GROUP BY over joins** (122 deferred).
- **Qualified wildcards** (121 deferred).
- **Logical-type lifecycle** (`DROP`/`ALTER LOGICAL TYPE`) — tier pivot.

## Promotion criteria

All 9 behavior gates green; `join_kind_test.rs` regression passes exactly; ADR 122 Rune join
tests pass exactly; G1-G8 hold; 115-122 regressions hold; full `cargo test -p toron` + `cargo
test -p wendelon` + `cargo test -p toron --test architecture_test adr119 adr120 adr122 adr123` +
`cargo check -p toron --all-targets` green.
Migration: `plans/impl/0123_multi_upstream_live_input_dispatch.md`.
