# ADR 129: Optimizer Fact Spine for Tiny Broadcast Inference

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** on the
optimizer-spine direction and final fenced rule, verified against the actual
iter-1/iter-2 peer drafts. The first deliberate step of a cost-based-optimizer
arc: build a conservative optimizer **fact spine** and spend it on one fenced
rule (tiny-left broadcast inference). Three grounding corrections shaped the
final form and are surfaced in §Council history, not papered: facts enter at the
**binder**, not the catalog-free `SqlPlanner` (Gemini's impl reverted this —
rejected by grounding);
**no hash hint exists** so the precedence gate can't assume one (Codex); and
Claude's iter-0 Fork-B "unify exchange insertion" lean was withdrawn after
applying the ossification discriminator honestly.
**Category:** Toron interior — optimizer substrate + one conservative join-strategy
rule. No SQL grammar / executor / fragmenter / scheduler / worker-protocol /
Wendelon-stats-protocol change.
**Predecessors:** 125 (strategy-aware `build_join_plan`, `JoinStrategy::Broadcast`),
128 (SQL `/*+ BROADCAST */` hints → pinned authoring), 121 (`plan_qualifiers`
qualifier resolution), 116 (bounded-`Take` row facts), 095/097/098
(empty-relation contradiction folding). The roadmap's deferred "cost-based
strategy selection."
**Migration:** `plans/impl/129_optimizer_fact_spine.md`.

## Doctrine — build the CBO spine, spend it on one tiny rule

> Toron is pivoting toward a real cost-based optimizer. The disciplined first step
> is a **minimum reusable fact spine** plus one fenced consumer, not a cost-model
> monolith. Grounded: `SqlPlanner` is a unit struct (`sql_planner.rs:1468`) —
> catalog-free; `PlannerConfig` + `&dyn Catalog` are both already threaded into
> `bind()` (`binder.rs:56,62`) — so the binder is the one place holding *facts*
> and *thresholds*. The spine: conservative table facts entering at catalog
> resolution, a logical `JoinStrategyIntent` separating authoring intent from
> concrete execution strategy, and a **single** fact-driven decision helper that
> returns a strategy *and a reason*. Its only consumer now is tiny-left broadcast —
> `Broadcast` only when the build side is a single base relation with a *hard byte
> upper bound* below a conservative cap-aware threshold; unknown facts → hash,
> always.
> Inference is do-no-harm; explicit hints stay fail-loud. Execution still receives
> only concrete plans. The spine is the deliverable; the rule proves it. Future
> consumers (right-side build selection, zero-worker placement, selectivity) reuse
> the spine without reopening this contract.

The star-schema target — tiny dimension tables joined repeatedly to a huge fact
table — is the motivating case. This ADR fires only when the tiny dimension is the
**syntactic left/build** input; the common `fact JOIN dim` shape (dim on the
right) stays hash here. Right-side build selection is the named next CBO
milestone, not a regression.

## Decisions

**D1 — Hard optimizer fact types.** `TableOptimizerFacts { byte_upper_bound:
Option<ByteCount>, row_upper_bound: Option<RowCount> }` on catalog-resolved
tables; `PlanOptimizerFacts { origin: PlanOrigin, byte_upper_bound, row_upper_bound
}` on bound subplans. Facts are **hard upper bounds or unknown** — no `confidence`,
no soft estimates in this cut (deferred to when the optimizer *weighs* costs). The
broadcast rule uses bytes only; rows are carried because `Take`, empty relations,
and future zero-worker placement reuse the same spine.

**D2 — Facts enter through binding, default unknown.** `TableBinding`
(`catalog.rs:76`) gains `optimizer_facts: TableOptimizerFacts`; all existing
descriptor-derived constructors default to unknown. Test catalogs seed hard
bounds. **No `LogicalPlan::ScanNamed.stats` field and no catalog access in
`SqlPlanner`** — facts ride the binding, consumed in the binder; this keeps the
planner catalog-free (grounding correction, see §Council history).

**D3 — Logical strategy intent.** `LogicalPlan::Join` carries
`JoinStrategyIntent::{Auto, Pinned(JoinStrategy)}`; `ExecutionPlan::Join` keeps the
concrete `JoinStrategy`. SQL `/*+ BROADCAST(alias) */` + Rune `.broadcast_join` →
`Pinned(Broadcast)`; Rune default joins + EXISTS desugar → `Pinned(HashPartitioned)`;
unhinted top-level SQL explicit joins → `Auto`. **`Auto` must not survive binding**
or appear in execution/fragmentation/scheduling.

**D4 — Binder-only auto resolution via a single helper (the CBO seam + spine
honesty).** `bind_join` resolves `Auto` through one
`choose_join_strategy(intent, kind, left_facts, right_facts, config) ->
JoinStrategyDecision`. `bind_join` carries **no fact-driven**
`JoinStrategy::Broadcast` selection outside that helper — exchange shaping may
match the returned concrete strategy, but a second CBO rule is a new arm in the
helper, not a new branch in the binder. This is the structural proof it is a
spine, not a buried `if tiny` (mirrors how 125/128 kept `build_join_plan` the
single strategy-aware authoring helper).

**D5 — Fork A exchange placement, named debt.** `Pinned` joins keep the existing
125/128 planner-shaped exchange (`build_join_plan`). `Auto` joins are bare logical
joins until binding; the binder inserts the concrete exchange shape after choosing
strategy. Chosen over unifying exchange insertion (Fork B) because the problem is
genuinely smaller — pinned joins need no exchange re-homing for this rule
(ossification discriminator applied with grounding). The split is real debt; the
guard `adr129_exchange_insertion_split_is_temporary` names it, and the event that
earns unifying it is a future CBO rule that must reshape a *pinned* plan.

**D6 — Tiny-left broadcast rule, deliberately narrow.** `Auto` → `Broadcast` only
when ALL: kind is `Inner`; no pinned hint applied; left is a single base relation
(alias wrapper allowed); left has a hard `byte_upper_bound`; config threshold is
set; the effective threshold is capped by a static conservative broadcast-cap
ceiling (for example 25% of default `broadcast_build_bytes_per_task`) until
per-query budget is available in binding; and the bound is below that effective
threshold. Anything else → `HashPartitioned`. Right/probe is never swapped;
right-side small tables stay hash in this ADR.

**D7 — Do-no-harm inference vs fail-loud hints.** Unknown / stale / soft facts,
composite left, non-inner, missing config, or over-threshold bounds all produce
hash. An inferred path must never introduce a failure a hash plan wouldn't have.
`Pinned` broadcast keeps ADR 128's fail-loud behavior (over-cap / wrong-side /
non-INNER). A pinned author choice is **never downgraded** by inference.

**D8 — Carry safe row bounds, schedule on nothing.** `PlanOptimizerFacts`
propagation sets `row_upper_bound = Some(0)` for `EmptyRelation`
(`ir/execution.rs:192`) and `min(child, limit)` for `Take` (`ir.rs:686`; `Some(0)`
for `LIMIT 0`) — folding the two existing proto-CBO islands into one spine.
**Fence:** ADR 129 changes no scheduling — non-shuffle fragmentation still emits one
task (`fragment.rs:600`), zero-partition exchange is still rejected (`:59`). The
guard `adr129_row_facts_carried_but_not_scheduled_on` asserts no
fragmenter/scheduler consumer reads `row_upper_bound`, keeping "carry for the
future" provably inert rather than an unverifiable feature claim.

**D9 — Reason receipts.** `JoinStrategyDecision` carries `PinnedByAuthor`,
`BroadcastInferred { build_bytes, threshold }`, or `HashByDefault { reason }`.
Tests assert reasons now, before a user-facing `EXPLAIN` exists — the debuggable
seam a real optimizer needs.

**D10 — Dark-launch config.** `PlannerConfig.auto_broadcast_max_build_bytes:
Option<ByteCount>` (binder-owned, not SQL config) defaults to **`None`**, so the
foundation lands **inert** in production until (a) a storage-ring producer ADR
populates real Wendelon facts AND (b) the knob is set — otherwise inference would
fire on `MockCatalog`-seeded facts in prod-shaped paths. Tests opt in with an
explicit small threshold.
The configured threshold is the opt-in safety gate; ADR 129 does not thread
tenant/query budget into `bind()`, so actual per-query cap awareness is a named
follow-on rather than an implicit requirement.

**D11 — Scope boundary.** No right-side broadcast / operand swap, no join
reordering, no non-INNER widening, no selectivity model, no Wendelon table-summary
producer, no `EXPLAIN` surface, no executor/fragmenter/scheduler/wire change, no
zero-worker dispatch. `Auto` confined to ir + planner + binder (grounded: 53
`JoinStrategy::` uses / 8 files; the intent type touches only those three).

## Behavior gates
- `sql_unhinted_tiny_left_join_infers_broadcast` — seeded hard byte bound + config
  set → concrete broadcast after binding. **Fails on `main`** (no spine; unhinted →
  concrete hash).
- `sql_unhinted_unknown_size_left_join_stays_hash`,
  `sql_unhinted_large_left_join_stays_hash`,
  `sql_tiny_right_join_does_not_swap_or_broadcast`.
- `sql_broadcast_hint_still_pinned_and_fail_loud`,
  `pinned_author_choice_not_downgraded_by_inference` (the precedence gate — written
  as pinned-not-downgraded, **not** as a non-existent hash hint; Codex correction),
  `rune_default_join_remains_pinned_hash`.
- `auto_intent_never_survives_binding` (execution plans concrete-only).
- `inferred_broadcast_carries_reason_receipt`.
- `take_or_empty_relation_seeds_row_bound_without_dispatch_change` (D8 — row fact
  carried AND task/partition count unchanged).
- `second_cbo_rule_extends_the_decision_helper` — fixture proving a new rule is a
  new arm in `choose_join_strategy`, not a branch in `bind_join` (spine honesty).
- Existing ADR 125/128 broadcast digest + cap gates remain green.

## Architecture guards
- `adr129_table_binding_carries_optimizer_facts`
- `adr129_logical_join_has_strategy_intent_execution_join_concrete`
- `adr129_auto_resolves_only_in_binder`
- `adr129_strategy_decision_is_single_helper` (`bind_join` has no
  fact-driven `JoinStrategy::Broadcast` selection outside `choose_join_strategy`)
  — the spine gate.
- `adr129_tiny_broadcast_requires_hard_byte_upper_bound`
- `adr129_exchange_insertion_split_is_temporary` (Fork A debt, named)
- `adr129_row_facts_carried_but_not_scheduled_on` (D8 fence)
- `adr129_no_executor_fragmenter_scheduler_wendelon_change`
- Retargeted 122/125/128 join guards (SQL loop threads `JoinStrategyIntent`, not
  concrete strategy; `Auto` logical-only and binder-resolved; pinned broadcast
  still builds left-exchange-one / bare-right).

## Council history
- **iter-0:** Operator seeded the CBO-foundation framing ("eat the elephant 64
  bytes at a time"). Gemini proposed `PlanEstimate{rows,bytes,confidence}` in
  `SqlPlanner`. Codex grounded the placement correction: `SqlPlanner` is
  catalog-free, the binder owns catalog+config, joins are strategy-shaped pre-bind
  — so the seam is binding-time, not planner. Claude reframed as a *fact spine*
  with a single-helper honesty gate and leaned (wrongly) toward Fork B unifying
  exchange insertion.
- **iter-1:** Claude conceded Fork B → Fork A (the ossification discriminator,
  applied with grounding, cleared A: pinned joins need no re-homing); dropped
  `confidence` for hard bounds (3-of-3); reconciled Gemini's impl — which reverted
  stats lookup into the catalog-free planner with a `ScanNamed.stats` field — back
  to the binder seam by grounding. Codex folded in the Take/EmptyRelation row-fact
  thread (two existing proto-CBO islands, grounded at `ir.rs:686` /
  `ir/execution.rs:192`); Claude fenced it with the no-scheduling guard.
- **iter-2:** Codex caught that Gemini's `sql_broadcast_hint_overrides_inference`
  gate assumes a **hash hint** that ADR 128 never added (only `/*+ BROADCAST */`
  exists) — replaced with `pinned_author_choice_not_downgraded_by_inference`.
  Genuine 3-of-3 on the final spine and fenced rule; no load-bearing divergence
  remains.

Three grounding corrections this arc (planner-vs-binder placement; hard-bounds-vs-
confidence; no-hash-hint), each caught by reading the actual code before promotion
([[feedback_concession_cycle_ground_not_headcount]],
[[feedback_small_cut_bias_structure_ossification]]).

## What to do next
Right-side build selection + output-order preservation (the real `fact JOIN dim`
star-schema win); unify exchange insertion when a pinned-reshaping rule earns it
(pays the Fork-A debt); Wendelon-backed live table-size facts (storage-ring
producer ADR — flips D10 to default-on); selectivity propagation through
filters/projections; fold ADR 116 bounded-take + ADR 095 empty-relation shortcuts
into the general fact model; zero-worker / coordinator-local placement; multi-way
join ordering; optimizer `EXPLAIN` over the reason receipts; soft/weighted
estimates (where `confidence` returns).

## Promotion criteria
Tiny-left joins with seeded hard byte facts + explicit config bind to concrete
broadcast (headline gate fails on `main` beforehand); missing / large /
right-side-small joins bind to hash; hints + Rune pinned authoring preserve
125/128 behavior; row facts carried without any dispatch change; `Auto` never
reaches execution; the spine gate (`adr129_strategy_decision_is_single_helper`)
holds; retargeted 122/125/128 guards hold; `cargo test -p toron` + `cargo test -p
toron --test architecture_test adr122 adr125 adr128 adr129` + `cargo check -p toron
--all-targets` + `git diff --check` green.
