# ADR 139: Source-Aware Broadcast-Cap Hash Replay

**Status:** Promoted. **Genuine consensus on the one-shot replay mechanism (Claude, Codex,
Gemini); source-aware D4 policy is operator + Codex + Claude** (Gemini's latest 139 draft
predates the operator's Rune/SQL steer — surfaced transparently, not papered). ADR 138 made
broadcast estimates honest but left an availability residual: a build whose *actual* encoded
bytes exceed the runtime cap fails the query where a hash join would have completed. 139
converts that miss into a **one-shot, coordinator-owned, whole-query hash replay** — and
decides *who* gets degraded by **how the broadcast was authored**.

**Category:** Toron interior — distributed-runtime recovery (coordinator-owned) + a
broadcast-authority distinction in the join IR.
**Predecessors:** 138 (the residual this retires), 125 (Rune `broadcast_join` — now
*absolute*), 128 (SQL `/*+ BROADCAST */` — now *advisory under runtime caps*), 129/132
(strategy/chooser), the existing descriptor-evolution `!batch_sent` retry (the mirrored pattern).
**Migration:** `plans/impl/139_source_aware_broadcast_cap_hash_replay.md`.

## Doctrine — the cap is resource truth; the authoring surface decides who absorbs it
> The runtime caps a broadcast build against the **encoded Arrow IPC payload byte length**
> (`PartitionPayload::size_bytes()`), meters *actual* bytes, and rejects with a typed
> `BudgetExhausted(BroadcastReplication)` — **never wrong rows**. So an over-cap broadcast is a
> resource-admission miss, not a query meaning error. 139 recovers it by replaying the whole
> read once with broadcast forced to hash — *but who is degraded depends on who authored the
> broadcast.* A planner-inferred broadcast (138 estimate, 136 hard) and a **SQL** `/*+ BROADCAST */`
> hint are preferences under resource caps → they degrade and return rows (protect the naive
> user). A **Rune** `broadcast_join` and a **direct/programmatic** pin are explicit commands →
> they stay fail-loud-terminal (absolute control). Invalid hints still fail at planning on every
> surface; only a *valid* degrade-eligible broadcast that hits a runtime cap degrades.

This synthesizes two disciplines on the surfaces where each belongs: the fail-loud-for-authored-intent
rule (128/[[logical_identity_receipt_doctrine]]) is kept for Rune (the prescriptive
power-user surface) and Direct (programmatic IR), while the pragmatic degrade goes to SQL (the
compatibility surface). [[rune_is_primary_interface]].

## Why a single cut delivers the name (grounded — the tractability that flipped the framing)
The fallback is **not** a multi-ADR re-fragmentation arc. The coordinator already runs a
`!batch_sent`-gated **whole-query retry** for descriptor evolution (`run_streaming_read_driver`,
`coordinator.rs:836/883`) and holds the logical plan + `PlannerConfig`. 139 is the same loop
with a force-hash policy on the second attempt — broadcast and hash are disjoint fragment
topologies (`fragment.rs:350/353`), so recovery is a whole-query *replay*, not an in-flight
subtree splice. (Claude argued an intractable arc in iter-0; Codex grounded the existing retry;
conceded.)

## The broadcast-authority distinction (the IR addition)
`JoinStrategyIntent::Pinned(JoinStrategy)` erases whether a broadcast pin came from SQL, Rune,
or direct IR — all three lower to `Pinned(Broadcast)`. 139 adds source provenance:
```rust
enum JoinStrategyIntent { Auto, Pinned { strategy: JoinStrategy, source: JoinStrategyPinSource } }
enum JoinStrategyPinSource { Direct, SqlHint, Rune }
```
`Direct` = programmatic/manual/test pins (default); `SqlHint` = `/*+ BROADCAST(t) */`; `Rune` =
`broadcast_join`. Normal binding is identical for all three — they differ **only** in replay
eligibility. (Chosen over a Rune-only `RequiredBroadcast` variant because that left
direct/programmatic pins advisory, silently degrading an explicit command — Codex's correction.)

## Decisions
**D1 — Replay eligibility by source.** Replayable: `Auto` broadcasts (138 estimate / 136 hard)
and `Pinned { Broadcast, SqlHint }`. Terminal: `Pinned { Broadcast, Rune }` and
`Pinned { Broadcast, Direct }`. **If the logical plan contains *any* terminal pinned broadcast,
the coordinator does not replay** — keeping the replay attempt provably broadcast-free without
preserving an absolute pin or demoting it. Mixed-source per-join replay (recover an auto/SQL
broadcast while a healthy Rune pin coexists) is the named follow-on; honest cost: a mixed plan
forgoes SQL protection until then.

**D2 — Trigger: `BroadcastReplication` only.** Before output, `BudgetExhausted(BroadcastReplication)`
from a failed task whose staged `ExecutionPlan` contains `JoinStrategy::Broadcast`, and the plan
has no terminal pinned broadcast. Classify `SchedulerError::TaskFailed { key, kind }` against the
`StagePlan`s **before** `scheduler_error` drops the `TaskKey`. **`ShuffleBytes` is excluded**
(grounded): `shuffle_bytes_published_per_task` (`budget.rs:80`) is a per-task *total*, and a
broadcast build is single-partition (`fragment.rs:589`), so the hash version of that
single-producer side publishes N buckets summing to the *same total* against the *same per-task*
budget — hash replay does not reliably relieve `ShuffleBytes`. `BroadcastReplication` is the
broadcast-specific signal (replication is exactly what hash eliminates).

**D3 — Coordinator-owned, one-shot, query-scoped, force-hash.** The scheduler stays same-plan
terminal; `BroadcastReplication` stays non-retryable. Replay mode demotes `Auto` + `SqlHint`
broadcasts to hash, **installing hash exchanges on both sides** even for an originally-pinned SQL
broadcast shape (changing the strategy enum alone is insufficient — the fragmenter reads the
distribution wrappers). The replay attempt contains **no** `JoinStrategy::Broadcast`. One-shot; a
replay failure is terminal.

**D4 — Same external `QueryId`, fresh stage/exchange namespace.** The replay preserves the
external `QueryId` (cancellation/audit/telemetry continuity) and fragments under checked nonzero
stage/exchange bases so attempt-0 published partitions cannot collide
(`PartitionAddress` includes `exchange_id`/`producer_stage_id`; `InMemoryPartitionStore::publish`
rejects duplicates). **Not `AttemptId`** — the scheduler rewrites attempt ids from pending task
state (Codex's catch).

**D5 — Output/side-effect fence.** Distributed reads only; streaming gated on `batch_sent == false`
(mirroring the descriptor-evolution retry); mutation/source-write paths out.

**D6 — No metering change; 138 hard/estimate fact split untouched.** `PartitionPayload::size_bytes()`
stays the cap unit.

## Policy summary (the operator's steer, realized)
| broadcast origin | over-cap behavior |
|---|---|
| `Auto` (138 estimate / 136 hard) | **replay → hash** (138 residual retired) |
| SQL `/*+ BROADCAST */` (`SqlHint`) | **replay → hash** (protect the naive user) |
| Rune `broadcast_join` (`Rune`) | **terminal** (absolute control — fail-loud) |
| direct/programmatic (`Direct`) | **terminal** (explicit command — must-broadcast) |

## Behavior gates (Rune-first per [[rune_is_primary_interface]])
- `estimated_broadcast_over_cap_replays_as_hash_and_matches_digest` — **headline (Rune+SQL auto)**:
  a 138 estimate-driven broadcast over a tiny cap returns **byte-identical normalized output** to a
  forced-hash plan (digest equality — the V1 correctness bar).
- `hard_bound_broadcast_over_cap_replays_as_hash_and_matches_digest` — a 136 hard fact degrades too.
- `sql_broadcast_hint_over_cap_replays_as_hash` — **retargets** `sql_broadcast_hint_respects_cap_terminally`.
- `rune_authored_broadcast_over_cap_stays_terminal` — Rune absolute control preserved fail-loud.
- `direct_pinned_broadcast_over_cap_stays_terminal`.
- `plan_with_any_terminal_pinned_broadcast_does_not_replay`.
- `sql_and_rune_broadcast_pins_have_distinct_sources` — IR grounding.
- `fallback_demoted_sql_broadcast_installs_hash_exchanges`.
- `fallback_attempt_contains_no_broadcast_joins`; `broadcast_replay_is_one_shot`;
  `streaming_replay_requires_no_batch_sent`; `replay_uses_fresh_fragment_namespace`.
- `shuffle_bytes_exhaustion_does_not_trigger_broadcast_replay`.
- `non_broadcast_budget_exhaustion_does_not_replay`; `mutation_path_does_not_replay_after_side_effects`.
- Regressions: 129–138 green; 138 fact split untouched; 125/128 planning unchanged.

## Architecture guards
- `adr139_join_strategy_intent_carries_pin_source`; `adr139_sql_hint_pin_is_replayable`;
  `adr139_rune_broadcast_pin_is_terminal`; `adr139_direct_broadcast_pin_is_terminal`.
- `adr139_broadcast_replication_not_marked_retryable`;
  `adr139_replay_trigger_classifies_scheduler_error_before_query_error_mapping`.
- `adr139_replay_owned_by_coordinator_not_scheduler`;
  `adr139_replay_mode_demotes_auto_and_sql_broadcasts_only`;
  `adr139_replay_mode_installs_hash_exchanges_for_demoted_pins`;
  `adr139_replay_attempt_has_no_broadcast_join`; `adr139_replay_is_one_shot`.
- `adr139_shuffle_bytes_not_a_replay_trigger`;
  `adr139_fragment_namespace_offsets_stage_and_exchange_ids`;
  `adr139_attempt_id_not_used_as_replay_namespace`.
- `adr139_partition_payload_size_unit_unchanged`; `adr139_138_estimate_channel_untouched`.

## Non-goals
Targeted mixed-source per-join replay; generic `ShuffleBytes` recovery (wants multi-producer
fan-out); a Rune advisory-broadcast variant; making `BroadcastReplication` retryable; mutation
replay; ADR 138 threshold/fact changes; `size_bytes()` metering change.

## Council history
iter-0: Claude dissented on tractability (claimed a multi-ADR arc); Codex grounded the
coordinator's existing `!batch_sent` whole-query retry; conceded. iter-1: converged on one-shot
replay. iter-2 (dual): provisional Fork A (all broadcasts replay) + build-`ShuffleBytes` trigger.
**Operator steered D4 — Rune absolute / SQL advisory.** iter-3: Claude added a Rune-only
`RequiredBroadcast` variant; **Codex grounded two corrections** — direct/programmatic pins must
stay terminal (the variant degraded them) → a `source` enum; and `ShuffleBytes` is per-task-total
so hash replay can't relieve it → `BroadcastReplication`-only. iter-4: Claude **verified both
against the code and conceded** (third grounded concession this arc — iter-0 tractability,
iter-4 Direct-pins + ShuffleBytes — [[feedback_concession_cycle_ground_not_headcount]]). Locked:
source-aware enum, `BroadcastReplication`-only trigger, no-replay-if-any-terminal-pin, no-`AttemptId`
namespace.

## Follow-ons
Targeted mixed-source per-join replay (join identity bind→fragment→failure — eliminates the
mixed-plan SQL-protection gap); generic `ShuffleBytes` recovery via multi-producer fan-out; a Rune
advisory-broadcast variant if power users later want opt-in degrade; then length-carrying logical
-type refinement (Utf8/Binary estimate → hard bound) and Take/LIMIT single-base lineage as the next
optimizer-IQ cuts.

## Promotion criteria
Source provenance flows through `JoinStrategyIntent`; SQL hint over-cap replays while Rune + Direct
stay terminal; a plan with any terminal pinned broadcast does not replay; the trigger is
`BroadcastReplication`-only (ShuffleBytes excluded, guarded); replay is coordinator-owned, one-shot,
broadcast-free, `!batch_sent`-gated, fresh non-`AttemptId` namespace; demoted SQL broadcasts install
hash exchanges; the estimate-driven case returns the forced-hash digest; mutation paths don't replay;
`size_bytes()` + 138 split unchanged; the `adr139_*` guards + 129–138 guards green; `cargo test -p
toron`, `--test architecture_test -- adr139`, `cargo check -p toron --all-targets`, `git diff --check`
all green.
