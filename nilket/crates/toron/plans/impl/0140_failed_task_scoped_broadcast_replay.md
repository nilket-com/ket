# Impl Plan — ADR 140: Failed-Task-Scoped Broadcast Replay

**Migration for** `plans/adr/0140_failed_task_scoped_broadcast_replay.md`.
**Scope:** `ir.rs` / `ir/execution.rs` (`JoinReplayId` + reduced replay eligibility on
`ExecutionPlan::Join`) · `binder.rs` (id from the bind-time `join_index`; eligibility stamp;
`force_hash_join_ids` policy; `choose_join_strategy` membership) · `distributed/fragment.rs`
(preserve replay metadata through rewrites) · `coordinator.rs` (per-task replayable-id collection
+ targeted replay) · tests/guards.
**Non-scope:** per-dim-within-task precision (A1 — worker failure-hint `ShuffleConsumer
{ broadcast_join_id }` + per-build cap validation + `replay_hint` wire change); stats-backed
optimizer facts (next ADR); `ShuffleBytes` trigger; mutation replay; `BroadcastReplication`
retryability; 138 fact-split / metering change.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| deterministic id seed | bind-time `join_index = trace.join_decisions.len()` `binder.rs:1384` (move the computation before `choose_join_strategy`; bind-order remains stable across rebind) |
| optimizer receipt target | `JoinTraceTarget { join_index }` (reuse as `JoinReplayId`) |
| execution join (carry metadata) | `ExecutionPlan::Join { left, right, strategy, .. }` `ir/execution.rs` — add `replay_id: JoinReplayId`, `replay_eligibility: BroadcastReplayEligibility` |
| substrate fence | existing guards keep `JoinStrategyIntent` out of `ExecutionPlan`; carry only reduced replay eligibility, not full authoring intent |
| policy flag (replace) | `PlannerConfig::force_hash_joins: bool` `binder.rs:67` → `force_hash_join_ids: BTreeSet<JoinReplayId>` |
| force-hash seam | `choose_join_strategy` `binder.rs:4066` (Auto/SqlHint demotion arms `:4069/4075`); SQL exchange-install `strip_root_exchange` |
| 139 replay-class veto (replace) | `plan_broadcast_replay_class` / `scan_logical_plan_for_broadcast_replay` / `BroadcastReplayScan` (coordinator.rs) |
| 139 trigger + task walk (extend) | `broadcast_cap_replay_trigger` `coordinator.rs:1140`; `task_key_contains_broadcast_join` `:1157`; `execution_plan_contains_broadcast_join` `:1166` |
| star builds co-located → demote set | `fragment_broadcast_join_plan` puts all `build_inputs` on each consumer task (`fragment.rs:520-560`) |
| fragment rewrites to preserve id | hash-join replacement (`JoinStrategy::HashPartitioned` arm `:350`), broadcast-star rewrite (`rewrite_broadcast_star_probe`) |

## Phases
0. **Failing gates first:** `mixed_sql_and_rune_broadcast_plan_replays_sql_preserves_rune` and
   `broadcast_in_other_stage_stays_broadcast_when_failed_task_replays` **fail on `main`** (139's
   global veto / global force-hash). `broadcast_star_over_cap_replays_all_task_local_dims_and_succeeds`
   green before + after (never worse than 139). 139 regressions green throughout.
1. **`JoinReplayId(u32)` + eligibility (`ir.rs` / `ir/execution.rs`):** add `replay_id` and
   `replay_eligibility: BroadcastReplayEligibility` (`Replayable` / `Terminal`) to
   `ExecutionPlan::Join`. Do not put `JoinStrategyIntent` in execution IR; the coordinator needs
   the reduced replay decision, not the authoring surface.
   **`cargo check -p toron --all-targets`** — IR-field churn across benches/tests
   ([[feedback_verify_all_targets_on_new_ir_field]]).
2. **Assign the id (`binder.rs`):** compute `join_index = trace.join_decisions.len()` before
   `choose_join_strategy`, wrap it as the `JoinReplayId`, pass it into the chooser, then push the
   receipt after the decision with the same value. Stamp the id onto the bound
   `ExecutionPlan::Join`, stamp eligibility from `strategy_intent` (`Auto`/SQL broadcast pin →
   `Replayable`; Rune/Direct pin → `Terminal`), and keep the optimizer receipt target using the
   same value. Deterministic across rebind (guard).
3. **Targeted policy (`binder.rs`):** `force_hash_joins: bool` → `force_hash_join_ids:
   BTreeSet<JoinReplayId>`. `choose_join_strategy` receives the join's `replay_id`; demote a
   broadcast to hash iff `replay_id ∈ force_hash_join_ids` **and** the intent is source-eligible
   (`Auto` / `Pinned{Broadcast, SqlHint}`). `Rune`/`Direct` never demoted even if listed. Keep
   139's `strip_root_exchange` + hash-exchange install for a demoted SQL-pinned broadcast.
4. **Per-join eligibility (`coordinator.rs`):** drop the global `has_terminal_pinned_broadcast`
   veto in `plan_broadcast_replay_class`/`BroadcastReplayScan`; after binding, eligibility comes
   from the `ExecutionPlan::Join` metadata in the failed task, not a whole-logical-plan veto.
5. **Trigger collects the failed task's replayable id *set* (`coordinator.rs`):** replace
   `execution_plan_contains_broadcast_join` (bool) with
   `collect_replayable_broadcast_join_ids(task_plan) -> BTreeSet<JoinReplayId>` (ids of
   `ExecutionPlan::Join { strategy: Broadcast, replay_eligibility: Replayable, .. }` in the failed
   task). On `BudgetExhausted(BroadcastReplication)` for task T, `!retried`, `!batch_sent`:
   `ids = collect(T.plan)`; **empty → terminal**; **non-empty → `force_hash_join_ids = ids`;
   namespace bump; retry once**. *Not* single-or-terminal.
6. **Preserve replay metadata through fragmentation (`fragment.rs`):** hash-join replacement +
   broadcast-star rewrite carry `replay_id` and `replay_eligibility` unchanged, so the failed
   task's `ExecutionPlan::Join` ids equal the bound ids the coordinator forces and the coordinator
   can still distinguish replayable from terminal pins. Guard.
7. **Guards + behavior tests** (the ten `adr140_*` + gate list); re-run 129–139.
8. **Verify:** `cargo test -p toron` · `--test architecture_test -- adr140` (+ adr129..139) ·
   `cargo check -p toron --all-targets` · `git diff --check`.

## Replay contract (the written spec)
```
attempt 0: bind(force_hash_join_ids={}) → fragment(ns=0) → dispatch
  Err(e): if e == BudgetExhausted(BroadcastReplication) on task T, !retried, !batch_sent:
            ids = collect_replayable_broadcast_join_ids(T.plan)   // Broadcast + Replayable metadata in T
            if ids.is_empty(): terminal                            // all Rune/Direct → fail loud
            else: force_hash_join_ids = ids; ns += 1; retry        // attempt 1
          else: existing 139 branch / terminal
attempt 1: bind(force_hash_join_ids=ids) → only those joins hash (+ install hash exchanges);
           other broadcasts (healthy ones in other tasks, Rune/Direct) unchanged → dispatch
  Ok → digest == hand-pinned mix ; Err → terminal (one-shot)
```
Invariants: `JoinReplayId` = trace `join_index`, deterministic across rebind; ids and reduced
eligibility survive fragmentation to the task; replay only before first output; never worse than
139 (single-task star demotes its whole replayable set → succeeds as hash).

## Risk register
- **Star regression from "single-or-terminal"** (the corrected trap): collect and force the
  *set*; `adr140_failed_task_collects_replayable_id_set_not_single` + `broadcast_star_..._succeeds`.
  Reverting to single-id must fail the star gate.
- **Non-deterministic id across rebind** (replay targets wrong join): seed from bind-order
  `join_index`; `join_replay_id_deterministic_across_rebind`.
- **Id lost in a fragment rewrite** (coordinator forces an id the task no longer carries):
  `adr140_fragment_rewrites_preserve_join_replay_metadata`.
- **Eligibility lost after binding** (coordinator cannot distinguish SQL/Auto from Rune/Direct in
  the failed task): reduced `BroadcastReplayEligibility` on `ExecutionPlan::Join`, preserved by
  fragment rewrites; no `JoinStrategyIntent` leak into execution IR.
- **Rune/Direct demoted** (breaks 139 absolute-control): `choose_join_strategy` checks source even
  when id ∈ set; coordinator never collects terminal ids; `..._stays_terminal` gates.
- **IR-field churn miss:** `--all-targets` in phase 1.
- **139 fence regression:** re-run the full 139 guard/gate set; `adr140_preserves_139_fences`.
- **Scope creep into A1:** no `ShuffleConsumer` build-id tagging, no per-build cap validation, no
  failure-wire change; `adr140_no_failure_wire_or_shufflebytes_scope_change`.

## Acceptance
A mixed SQL+Rune plan replays the SQL join and preserves the Rune broadcast (fails on `main`); a
healthy broadcast in another stage stays broadcast; a single-task star demotes its replayable set
and succeeds (never worse than 139); an all-Rune/Direct failed task is terminal; the
`JoinReplayId` is the deterministic trace `join_index` and survives fragmentation with reduced
replay eligibility; the replay force-hash targets a join-id set (not a global bool, not
single-or-terminal); every 139 fence green un-regressed; 138 split untouched; ten `adr140_*` +
129–139 guards green.
