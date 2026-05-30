# Impl Plan — ADR 139: Source-Aware Broadcast-Cap Hash Replay

**Migration for** `plans/adr/139_source_aware_broadcast_cap_hash_replay.md`.
**Scope:** `ir.rs` (source enum) · `sql_planner.rs` / `rune_engine.rs` (source at lowering) ·
`binder.rs` (resolution + force-hash replay mode) · `distributed/fragment.rs` (id namespace) ·
`coordinator.rs` (replay trap + failure classification) · retarget the SQL hint terminal test ·
tests/guards.
**Non-scope:** targeted mixed-source per-join replay; generic `ShuffleBytes` recovery; mutation
replay; making `BroadcastReplication` retryable; ADR 138 fact-model / `size_bytes()` metering change.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| intent (add source) | `enum JoinStrategyIntent { Auto, Pinned(JoinStrategy) }` `ir.rs:621` → `Pinned { strategy, source }` |
| SQL hint → `SqlHint` | `/*+ BROADCAST */` `sql_planner.rs:6371/6553` |
| Rune `broadcast_join` → `Rune` | `rune_engine.rs:239` (`join_with_strategy(.., Broadcast)`) |
| shared join helper | `sql_planner::build_join_plan` is used by SQL and Rune; add source plumbing here or wrappers will collapse provenance again |
| direct/programmatic → `Direct` (default) | `lib.rs:1585` + binder/sql test pins (~28 sites) |
| binder resolution | `choose_join_strategy` `binder.rs:4055` |
| ShuffleBytes per-task total (excluded) | `shuffle_bytes_published_per_task` `budget.rs:80`; `shuffle_used` `:1783/1813`; broadcast build single-partition `fragment.rs:589` |
| trigger dim | `BudgetDimension::BroadcastReplication` `budget.rs:63`; `retryable()` `:70` (leave unchanged) |
| classify before mapping | `SchedulerError::TaskFailed { key, kind }` before `scheduler_error` drops `TaskKey` |
| read retry loop | `run_streaming_read_driver` `coordinator.rs:836` (desc-evolution `!batch_sent` `:883`); non-streaming `:670/680` |
| fragment ids (NOT AttemptId) | `FragmentPlanConfig`/`FragmentIdAllocator` `fragment.rs` |
| digest harness | `distributed/determinism.rs` |

## Phases
0. **Failing gates first:** `estimated_broadcast_over_cap_replays_as_hash_and_matches_digest`
   (Rune+SQL) + `sql_broadcast_hint_over_cap_replays_as_hash` fail on `main`.
   `rune_authored_broadcast_over_cap_stays_terminal` + `direct_pinned_broadcast_over_cap_stays_terminal`
   green before + after.
1. **IR source enum (`ir.rs`):** `Pinned { strategy: JoinStrategy, source: JoinStrategyPinSource }`;
   `enum JoinStrategyPinSource { Direct, SqlHint, Rune }`. Update ~28 construction sites
   (`source: Direct` default; SQL site `SqlHint`; Rune `broadcast_join` `Rune`) + all matches for
   exhaustiveness. Because SQL and Rune both use `sql_planner::build_join_plan`, add an explicit
   pin-source parameter or source-specific wrappers there; otherwise the new provenance collapses
   back to `Direct`. **`cargo check -p toron --all-targets`** ([[feedback_verify_all_targets_on_new_ir_field]]).
2. **Binder resolution + force-hash (`binder.rs`):** `Pinned { strategy, .. } => strategy`
   (PinnedByAuthor, unchanged). Add `force_hash_joins: bool` to `PlannerConfig` (default false) +
   `HashByDefaultReason::HashAfterBroadcastCap`. Force-hash demotes **only `Auto` and
   `Pinned { Broadcast, SqlHint }`** → hash, installing hash exchanges on both sides; all other
   intents bind normally. (A replayable plan therefore binds broadcast-free under force-hash.)
3. **Replay classifier:** `plan_broadcast_replay_class(plan: &LogicalPlan)`: `Replayable` iff the
   logical plan has at least one replay-permitted join (`Auto` or `Pinned { Broadcast, SqlHint }`)
   **and no** `Rune`/`Direct` pinned broadcast; otherwise (any terminal pin, or no replay-permitted
   candidate) → not replayable. Do this before provenance is lost in `ExecutionPlan`; the staged
   broadcast-cap failure separately proves that a replay-permitted candidate actually bound to
   broadcast and hit the cap.
4. **Fragment id namespace (`fragment.rs`):** `id_namespace: u32` (default 0) on `FragmentPlanConfig`;
   seed stage/exchange id bases disjointly from it — **not** `AttemptId`. Guard.
5. **Coordinator trap (`coordinator.rs`):** classify `TaskFailed { key, kind }` vs `stages` **before**
   `scheduler_error`. `broadcast_cap_replay_trigger`: `kind == BudgetExhausted(BroadcastReplication)`
   on a broadcast-consumer task **and** `plan_class == Replayable` **and** `!retried_broadcast_replay`
   **and** `!batch_sent`. On trigger: `config.force_hash_joins = true; id_namespace += 1; continue`.
   Apply at the `:836` loop and the `:670/680` non-streaming site. Reads only; assert mutation paths
   never set the flag.
6. **Retarget + preserve tests:** `sql_broadcast_hint_respects_cap_terminally` →
   `sql_broadcast_hint_over_cap_replays_as_hash` (rows + digest); **preserve** the Rune terminal
   behavior as `rune_authored_broadcast_over_cap_stays_terminal`; add
   `direct_pinned_broadcast_over_cap_stays_terminal`. Name the SQL retarget in the ADR (discipline #4).
7. **Guards + behavior tests:** the `adr139_*` guards + the gate list; re-run 129–138.
8. **Verify:** `cargo test -p toron` · `--test architecture_test -- adr139` (+ adr129..138) ·
   `cargo check -p toron --all-targets` · `git diff --check`.

## Replay contract (the written spec)
```
attempt 0: bind(force_hash:false) → fragment(ns=0) → dispatch
  Err(e): if kind(e)==BudgetExhausted(BroadcastReplication) on a broadcast-consumer task
            and plan_class==Replayable and !retried and !batch_sent:
              force_hash_joins=true; ns=1; retry            // attempt 1
          else (HasTerminalPin / ShuffleBytes / generic): existing branch or terminal
attempt 1: bind(force_hash:true) → demote Auto + Pinned{Broadcast,SqlHint} → hash (+exchanges);
           fragment(ns=1) → dispatch
  Ok → rows byte-identical to a forced-hash plan ; Err → terminal (one-shot)
```
Invariants: a `Replayable` plan binds broadcast-free under force-hash; external `QueryId`
preserved; stage/exchange ids disjoint across attempts (not `AttemptId`); replay only before
first output.

## Risk register
- **Direct/Rune pin silently degraded** (breaks the steer): force-hash demotes only `Auto` +
  `SqlHint`; `plan_with_any_terminal_pinned_broadcast_does_not_replay` + `rune_/direct_..._stays_terminal`.
- **ShuffleBytes false trigger** (Fork 2; hash replay wouldn't relieve it): `BroadcastReplication`-only;
  `shuffle_bytes_exhaustion_does_not_trigger_broadcast_replay`.
- **IR exhaustiveness / churn miss:** `--all-targets` in phase 1.
- **`AttemptId` namespace collision** with scheduler attempt-id rewriting: dedicated id base;
  `adr139_attempt_id_not_used_as_replay_namespace`.
- **Stale broadcast wrapper in a demoted SQL pin:** install outer hash exchanges;
  `fallback_demoted_sql_broadcast_installs_hash_exchanges` + `..._has_no_broadcast_join`.
- **PartitionAddress collision / duplicated output / loop / digest drift / retryable:** namespace
  offset, `!batch_sent` gate, one-shot flag, determinism harness, leave `retryable()` unchanged.

## Acceptance
Rune+SQL auto broadcast over a tiny cap returns the forced-hash digest; 136-hard replays; **SQL
hint degrades (retargeted), Rune + Direct stay terminal (preserved/added)**; a plan with any
terminal pinned broadcast does not replay; trigger is `BroadcastReplication`-only; replay is
one-shot, broadcast-free, `!batch_sent`-gated, fresh non-`AttemptId` namespace; demoted SQL installs
hash exchanges; mutation paths don't replay; `size_bytes()` + 138 split unchanged; the `adr139_*` +
129–138 guards green.
