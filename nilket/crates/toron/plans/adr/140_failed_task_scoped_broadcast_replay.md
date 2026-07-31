# ADR 140: Failed-Task-Scoped Broadcast Replay

**Status:** Promoted. **Genuine consensus on A2 (Claude + Gemini + operator's "small,
Toron-local" steer).** Codex iter-2 advocated the stronger join-*identified* variant (a worker
failure-hint so a single-task star demotes only the over-cap dim); that is surfaced here as the
**named A1 follow-on**, not folded — it touches the worker cap validator and the failure wire
payload, which the operator scoped out of this cut ("small, Toron-local; directly completes the
139 recovery cut"). ADR 139 made an over-cap broadcast an availability event recoverable by a
whole-query hash replay; its force-hash was **global**. 140 scopes the force-hash by **join
identity, to the failed task** — removing 139's global terminal-pin veto and its cross-task
bluntness, with every 139 fence intact.

**Category:** Toron interior — distributed-runtime recovery refinement (coordinator + reduced
join replay metadata on the join IR).
**Predecessors:** 139 (the whole-query replay this scopes; every fence kept), 135
(broadcast-star — why builds co-locate in one task), 138 (the estimate residual), 134 (the
optimizer trace `join_index` reused as the id), 132 (the chooser).
**Migration:** `plans/impl/140_failed_task_scoped_broadcast_replay.md`.

## Doctrine — preserve the replay, scope the force-hash to the failed task by join id
> 139's second attempt set `PlannerConfig::force_hash_joins` and demoted **every** replayable
> broadcast in the query; a Rune/Direct pin **anywhere** vetoed replay entirely. That was the
> right minimal recovery, but blunt. 140 keeps the identical whole-query replay substrate and
> gives each join a stable id, so the second attempt force-hashes only the **replayable
> broadcast joins in the failed task** — healthy broadcasts in other tasks/stages stay
> broadcast, and a SQL/Auto broadcast can replay even when a Rune pin lives elsewhere. Per-dim
> precision *inside* one task is the next refinement (A1), not this one.

## Why per-dim-in-task precision is out of this cut (grounded)
A broadcast-star co-locates **all** dim builds in one consumer task — `fragment_broadcast_join_plan`
puts every build as a `ShuffleConsumer` input on each consumer task. And the worker's broadcast-cap
failure is coarse: it sums all build inputs and emits `BudgetExhausted { dimension, requested,
available }` with **no join/build id**. So the coordinator can identify the failed *task* and the
replayable broadcast joins in it, but **not which individual build crossed the cap**. Exact
single-dim replay therefore needs a worker failure-hint + per-build cap validation (A1, a
wire/payload change) — explicitly the follow-on. Consequently A2 demotes the failed task's whole
replayable set: for a single-task star that equals 139's behavior (succeeds as hash), **never
worse**, and strictly better on the veto + cross-task axes.

## Decisions
**D1 — Stable, deterministic `JoinReplayId`.** Reuse the existing bind-time join receipt index
(`join_index = trace.join_decisions.len()`, `binder.rs:1384` — minted in deterministic bind
order: left/right subtrees, then the join) as `JoinReplayId(u32)`; carry it on
`ExecutionPlan::Join` and the optimizer receipt target. Rebinding the same logical plan produces
the same ids, so the replay targets the same joins. The binder computes this id before
`choose_join_strategy` needs it, then pushes the decision receipt afterward with the same value.
No new counter.

**D2 — Targeted force-hash policy.** Replace `PlannerConfig::force_hash_joins: bool` with
`force_hash_join_ids: BTreeSet<JoinReplayId>`. `choose_join_strategy` receives the join's id and
demotes a broadcast to hash iff `id ∈ force_hash_join_ids` **and** the intent is source-eligible
(`Auto` / `Pinned{Broadcast, SqlHint}`). `Rune`/`Direct` are never demoted even if listed
(belt-and-suspenders on the 139 source policy).

**D3 — Reduced per-join replay eligibility replaces the global veto.** Drop 139's global
`has_terminal_pinned_broadcast` veto. The binder also stamps each `ExecutionPlan::Join` with a
small replay-eligibility tag derived from `JoinStrategyIntent`: `Auto` and SQL-pinned broadcast
are replayable; Rune/Direct pins are terminal. This is deliberately **not** the full
`JoinStrategyIntent` (keeps the existing execution/substrate fence), but it is the metadata the
coordinator needs after a task failure. Eligibility is now per-join by id, so a terminal
Rune/Direct pin no longer disables replay for unrelated replayable joins.

**D4 — Trigger collects the failed task's replayable broadcast id *set*.** On
`BudgetExhausted(BroadcastReplication)` for a task, walk that task's staged `ExecutionPlan` and
collect ids from `ExecutionPlan::Join { strategy: Broadcast, replay_eligibility: Replayable, .. }`.
**Empty set → terminal** (the task's broadcasts are all Rune/Direct → absolute, fail loud).
**Non-empty → replay with `force_hash_join_ids` = that set.** This is the *set*, **not**
"single-broadcast-or-terminal" — the latter regresses the co-located star (>1 broadcast →
terminal where 139 succeeds).

**D5 — Preserve every 139 fence.** One-shot; `!batch_sent`; reads only (mutation paths out); same
external `QueryId`; fresh stage/exchange namespace (not `AttemptId`);
`BudgetExhausted(BroadcastReplication)` only (**no `ShuffleBytes`** — 139's grounded exclusion
stands); `BroadcastReplication` non-retryable; coordinator-owned (no scheduler topology mutation).
Digest equality vs a hand-pinned plan.

**D6 — No worker/wire/failure-taxonomy change; no 138 fact-split or metering change.** A2 reads
only the static staged plan + the existing coarse failure. Per-build failure signaling is A1
(follow-on). `PartitionPayload::size_bytes()` and 138's hard/estimate split are untouched.

## Behavior gates (Rune-first per [[rune_is_primary_interface]])
- `mixed_sql_and_rune_broadcast_plan_replays_sql_preserves_rune` — **headline**: a SQL/Auto
  broadcast over cap coexists with a Rune broadcast; on replay the SQL join hashes and the Rune
  join stays broadcast; **fails on `main`** (139's global veto makes it terminal). Digest matches
  the hand-pinned mix. (Rune + SQL.)
- `broadcast_in_other_stage_stays_broadcast_when_failed_task_replays` — cross-task isolation: a
  healthy broadcast in a different stage is untouched; fails on `main` (global force-hash demotes
  it).
- `broadcast_star_over_cap_replays_all_task_local_dims_and_succeeds` — anti-regression: a 135
  single-task star demotes its whole replayable set and **succeeds** (never worse than 139);
  digest matches the all-hash plan.
- `failed_task_with_only_rune_or_direct_broadcasts_is_terminal` — empty replayable set → fail loud.
- `join_replay_id_deterministic_across_rebind` — the id is stable so the replay targets the same
  joins.
- Regressions: all 139 gates green (SQL over-cap replays, Rune/Direct terminal, one-shot,
  `!batch_sent`, namespace, `BroadcastReplication`-only, mutation fence); 138 split untouched.

## Architecture guards
- `adr140_execution_join_carries_stable_replay_id_and_replay_eligibility`;
  `adr140_join_replay_id_is_trace_join_index_deterministic`.
- `adr140_optimizer_receipt_target_uses_join_replay_id`.
- `adr140_fragment_rewrites_preserve_join_replay_metadata`.
- `adr140_replay_force_hash_targets_join_id_set_not_global_bool`.
- `adr140_terminal_pins_do_not_globally_veto_replay` (per-join execution metadata, no global
  logical-plan veto).
- `adr140_failed_task_collects_replayable_id_set_not_single` (the anti-regression — set, not
  single-or-terminal).
- `adr140_preserves_139_fences`; `adr140_no_failure_wire_or_shufflebytes_scope_change`;
  `adr140_138_fact_split_untouched`.

## Non-goals
Per-dim precision *within* a single failed task (→ A1 worker build-id failure-hint + per-build cap
validation — follow-on); stats-backed optimizer facts (next ADR); length-carrying refinement;
`Take`/`LIMIT` lineage; generic `ShuffleBytes` recovery; making `BroadcastReplication` retryable;
in-flight sub-DAG splice (this stays whole-query *rebind*, just targeted by id).

## Council history
iter-0: split (Gemini + Claude → length-refinement B; Codex → targeted replay A). iter-1: Claude
conceded to A on grounding (139's force-hash is global → degrades the broadcast-star; B can't be
honest Toron-only without storage enforcement). iter-2: Gemini + Claude converged on **A2
(failed-task-scoped, Toron-local)**; Claude **corrected the "single-or-terminal" trigger** — it
regresses the co-located star — to **demote the failed task's replayable set**. Codex iter-2
advocated the stronger **A1** (worker failure-hint for exact single-dim replay) and offered the
A2 fallback; the **operator's "small, Toron-local; completes the 139 cut" steer decided A2**, with
A1 named as the follow-on. Operator reframe captured: **Wendelon is not absolute-facts-only — it
has KLL quantiles + maintained HLL/bloom/small-set stats**, which are legitimate *labeled-approximate*
optimizer inputs (estimate/selectivity channels), not hard `byte_upper_bound` receipts —
reshaping the next-candidate list. [[feedback_concession_cycle_ground_not_headcount]],
[[feedback_cut_must_deliver_the_name]] (A2's honest scope named; per-dim-in-star is A1),
[[feedback_human_steer_catches_the_wrong_cut]].

## Follow-ons
**A1 — exact join-identified replay** (Codex's iter-2 hint): tag broadcast build inputs with the
`JoinReplayId` (`ShuffleConsumer { broadcast_join_id }`), validate tagged builds per-input, and
emit `BudgetExhausted(BroadcastReplication, replay_hint = BroadcastJoin(id))` so the coordinator
demotes exactly the over-cap dim within a single-task star (a worker cap-validation + failure-wire
change). **Stats-backed optimizer facts (the operator-confirmed next ADR, selectivity-first):** a
136-W-style cross-ring pair exposes Wendelon's **maintained HLL cardinality** (and later
size/length quantiles — a new producer extension, since those are not maintained today) as
**labeled-approximate** facts Toron consumes for selectivity (and sharper 138 estimates) — never
as a hard `byte_upper_bound`. This attacks the *root* (bad estimates → over-caps) and is the real
CBO substrate (selectivity propagation → join reordering). Then: generic `ShuffleBytes` fan-out
recovery; `Take`/`LIMIT` single-base lineage.

## Promotion criteria
A mixed SQL+Rune plan replays the SQL join and preserves the Rune broadcast (fails on `main`); a
healthy broadcast in another stage stays broadcast; a single-task star demotes its replayable set
and succeeds (never worse than 139); an all-Rune/Direct failed task is terminal; the
`JoinReplayId` (the trace `join_index`) is deterministic across rebind and survives fragmentation
with its replay-eligibility tag; the replay force-hash targets a join-id *set*, not a global bool,
and not single-or-terminal; every 139 fence (one-shot, `!batch_sent`, namespace,
`BroadcastReplication`-only, mutation fence, Rune/Direct terminal) green un-regressed; 138 split
untouched; the ten `adr140_*` guards + 129–139 guards green; `cargo test -p toron`,
`--test architecture_test -- adr140`, `cargo check -p toron --all-targets`, `git diff --check`
all green.
