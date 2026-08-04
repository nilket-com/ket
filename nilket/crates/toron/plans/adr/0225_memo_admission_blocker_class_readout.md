# Toron ADR 225: Memo Admission Blocker-Class Readout

**Status:** Promoted. **Migration:** `plans/impl/0225_memo_admission_blocker_class_readout.md`.
**Category:** Toron optimizer instrumentation — a read-only refinement of
`information_schema.optimizer_memo_admission`. No memo engine, no bounded candidate
contract, no actuation/selection, no planner policy consumer, no new durable
key/counter/flush, no new `SystemTableKind`, no `optimizer_decision_conflicts` (ADR 220)
schema change, no receipt-gate manifest change, no executor / fragmenter / wire / daemon /
Wendelon / storage surface. Still renders **zero `admissible` rows**
(`bounded_candidate_count = 0` for every row).

## Why this cut exists

ADR 224 made memo admission per-group honest about *recency*: a row is `blocked_shape` only
when its own `(decision_kind, provenance)` conflict is lifetime-conflicted, recent-conflicted,
and healthy. But the forward step — "one bounded candidate contract" — assumed the blocked
side is a single thing memo could fix. It is not.

The conflict's blocked side collapses three operationally different blockers:

```text
Reordered                    -> reordered
KeptThinMargin + Declined(*) -> blocked_or_near_miss   <- collapses margin / hard / capability
KeptOptimal                  -> neutral
```

A blocked row could be blocked because the margin policy rejected a *generated* candidate
(→ ADR 218 margin tuning, **not** memo), because a *hard* limit means no candidate exists
or can be costed (→ statistics / operators, **not** memo), or because the recognizer
*refused a shape a bounded generator could enumerate* (→ the one place memo earns its keep).
Marking any conflicted-recent-healthy row `admissible` — or even naming a "bounded
candidate contract" for it — before that distinction is legible would ship the name of a
contract while the behavior is a margin restatement or an impossible one. ADR 225 makes the
blocker class explicit so the *next* ADR names a contract only where memo is genuinely the
answer. It builds no contract and reaches no `admissible` row.

## Grounding (verified)

- The conflict accumulator already carries the raw split:
  `DecisionConflictReport` has `kept_thin_margin_count`, per-reason `decline_counts`,
  `top_decline_reason` / `top_decline_count`, and the aggregate
  `blocked_or_near_miss_count = kept_thin_margin + Σ declined`
  (`join_order.rs:911`, top-decline ≈`:932`). 225 classifies existing fields — **no new
  evidence is recorded**.
- `conflict_strength = reordered.min(blocked_or_near_miss)` (join_order.rs) is the ADR 220
  pressure score and must stay byte-identical — the aggregate is preserved, not replaced.
- The 7 decline reasons (`OptimizerDecisionDeclineReason`, decision_log.rs) partition by
  what actually owns the fix:
  - **hard:** `NonInnerEquiJoin`, `MissingRelationLabel`, `DuplicateDimension`,
    `MissingEstimate` — no candidate exists, or no cost estimate to choose with; memo
    cannot manufacture either.
  - **capability:** `MultipleProbeRelations`, `MixedStrategyStar`, `ProbeNotScanLinear` —
    the strict recognizer refused a shape a bounded generator might enumerate.
- `StarOrderActuationResult` is `NotCandidate | Actuated | Declined` — it has **no
  `KeptThinMargin`**. So every `flat_star/*` and `star_candidate/*` conflict is
  decline-limited and **never `margin_limited`**; only the graph family can be
  `margin_limited`. A star row reading `margin_limited` would be a bug.
- `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE = 20` (join_order.rs:8) is the readiness floor.
- The memo-admission table has 11 columns today (explain.rs); 225 appends 6 → 17. The
  table family count (`SystemTableKind`) is unchanged.

## Decisions (promotion-locked)

**D1 — Additive blocker split on the report.** Add `margin_blocked_count`
(= `kept_thin_margin_count`), `hard_decline_count`, and `capability_decline_count` to
`DecisionConflictReport`, summed from existing per-reason decline counts. **Preserve**
`blocked_or_near_miss_count` and `conflict_strength` exactly (ADR 220 contract). Pure
consumer-side projection — no new outcome variant, no new runtime evidence.

**D2 — Three operational blocker classes, floor-gated.** For each row, with the floor = 20:

```text
margin_ready     = margin_blocked_count     >= floor
hard_ready       = hard_decline_count       >= floor
capability_ready = capability_decline_count >= floor

not lifetime-conflicted              -> not_conflicted
exactly one of the three ready       -> that class (margin_limited | hard_limited | capability_limited)
more than one ready, OR none ready
  but blocked_or_near_miss >= floor  -> mixed_blocker
```

Floor-gating each class (not just the aggregate) is load-bearing: it prevents a row that is
*also* margin- or hard-limited at floor from overclaiming `capability_limited`.

**D3 — Capability is not a contract.** `capability_limited` means a bounded generator
*might* enumerate the refused shape — it is a pointer to the next ADR, not a contract.
`bounded_candidate_count` stays `0`; no row becomes `admissible`. The first contract is
named in a later ADR, only after a `capability_limited` row is shown in production *and* its
decline reason is grounded as memo-fixable.

**D4 — Status precedence unchanged (ADR 224).** `cold` / `stale_evidence` /
`degraded_evidence` still short-circuit before blocker interpretation. Blocker class refines
only `reason` / `next_cut` for `blocked_shape` rows; a stale or degraded row renders its
measured blocker counts but keeps its 224-governed status and `next_cut`. A stale capability
decline must not read ready.

**D5 — Honest `reason` / `next_cut` for `blocked_shape`.**

```text
margin_limited     -> reason margin_limited_blocker     · next_cut margin_policy_tuning
hard_limited       -> reason hard_limited_blocker       · next_cut hard_blocker_resolution
capability_limited -> reason capability_limited_blocker · next_cut bounded_candidate_contract_investigation
mixed_blocker      -> reason mixed_blocker_class        · next_cut blocker_class_detail
```

`investigation`, never `generation` — the decline case says "where to look," not "memo will
fix it."

**D6 — Read-only; no engine; ADR 220 table untouched.** No resolver / recognizer /
actuation / margin-selector / fragmenter / executor / daemon / Wendelon / storage reads the
blocker class; no memo arena, candidate graph, generic search, Cascades surface, candidate
memoization, `src/optimizer.rs`, or planner consumer. No new durable key / counter / flush.
`information_schema.optimizer_decision_conflicts` (220) and its schema are unchanged.

## Schema (additive to the 224 memo-admission table; 220 untouched)

```text
… existing 11 columns (ADR 224) …
blocker_class Utf8 · margin_blocked_count Int64 · hard_decline_count Int64 ·
capability_decline_count Int64 · top_decline_reason Utf8 · top_decline_count Int64
```

`top_decline_reason` renders the snake-case decline label (e.g. `multiple_probe_relations`,
`missing_estimate`) for rows with declines, empty string otherwise (non-null finite schema,
mirroring 223). The first 11 column positions are unchanged.

## Honesty gates (discriminating)

- `memo_admission_margin_limited_conflict_points_to_margin_tuning` — graph `Reordered≥20`
  + `KeptThinMargin≥20`, no declines → `blocked_shape`, `blocker_class = margin_limited`,
  `next_cut = margin_policy_tuning`, `bounded_candidate_count = 0`.
- `memo_admission_hard_limited_conflict_points_to_hard_blocker_resolution` — `Reordered≥20`
  + `Declined(MissingEstimate)≥20` → `blocker_class = hard_limited`,
  `next_cut = hard_blocker_resolution`, `top_decline_reason = missing_estimate`.
- `memo_admission_capability_limited_conflict_points_to_contract_investigation` —
  `Reordered≥20` + `Declined(MultipleProbeRelations)≥20` → `blocker_class =
  capability_limited`, `next_cut = bounded_candidate_contract_investigation`.
- `memo_admission_mixed_blocker_does_not_overclaim_capability` — margin and capability both
  ≥ floor → `mixed_blocker`, not `capability_limited`.
- `memo_admission_star_family_is_never_margin_limited` — a `flat_star/native` conflict is
  decline-classed (star has no thin-margin path). **Fails if a star row reads
  `margin_limited`.**
- `memo_admission_blocker_class_does_not_bypass_stale_recency` — capability lifetime
  conflict outside current+previous windows → `stale_evidence`, not `blocked_shape`.
- `memo_admission_blocker_class_does_not_bypass_degraded_health` — capability recent
  conflict under non-`Ok` evidence → `degraded_evidence`.
- `memo_admission_blocker_columns_are_additive_to_existing_schema` — first 11 columns
  unchanged, six appended.
- `memo_admission_still_renders_zero_admissible`.
- `optimizer_decision_conflicts_220_schema_unchanged`.
- `rune_queries_optimizer_memo_admission_with_blocker_class`.

## Architecture guards (promotion-locked)

- `adr225_memo_admission_adds_blocker_class_columns_after_existing_schema`
- `adr225_conflict_report_preserves_blocked_or_near_miss_aggregate`
- `adr225_blocker_class_reuses_existing_decision_rows_not_new_evidence`
- `adr225_blocker_class_splits_margin_hard_and_capability`
- `adr225_star_family_conflicts_are_never_margin_limited`
- `adr225_capability_class_does_not_create_admissible_without_contract`
- `adr225_decline_next_cut_is_investigation_not_generation`
- `adr225_blocker_class_does_not_change_status_precedence`
- `adr225_no_generic_memo_engine_or_cascades_surface`
- `adr225_no_policy_consumer_or_actuation_reader`
- `adr225_no_new_durable_key_counter_flush_or_wendelon_surface`
- `adr225_no_new_system_table_kind_receipt_gate_or_220_schema_change`

**Reconciled prior guards (named):**
- ADR 223/224 memo-admission schema guards → allow the six appended explanatory columns;
  the first 11 positions and their semantics unchanged.
- ADR 224 `no_admissible_rows_without_bounded_candidate_contract` and zero-admissible →
  **unchanged** (still zero admissible, `bounded_candidate_count = 0`).
- ADR 220 conflict-model guards → **unchanged**; `blocked_or_near_miss_count` and
  `conflict_strength` preserved, the detail table untouched (blocker class is consumer-side).
- ADR 221 / 197 no-memo fences → **unchanged** (no engine; the pre-existing
  `optimizer_memo_admission` readout name remains the only allowance).
- ADR 217 five-row cockpit / ADR 216 140-220 / ADR 222 window-key count / ADR 102 finite
  table family → **unchanged**.

## Consensus (transparent)

3-of-3. The arc: Claude iter-0 grounded that a naive "{reorder, keep}, pick cheaper"
contract reduces to a 218 margin policy (redundant) and proposed splitting the blocked side
by decline class; Codex iter-0 proposed a flat-star two-slot contract; Gemini iter-0 a
generic split. Claude iter-1 named the three-class taxonomy (margin / hard / capability) but
over-reached by making `admissible` reachable for the capability class. Codex and Gemini
iter-1/2 **held that `admissible` must stay unreachable** until a real bounded generator is
named — Claude **conceded** on grounding (a capability decline is not yet a contract;
`MissingEstimate`→stats, `MixedStrategyStar`→placement, `ProbeNotScanLinear`→unsupported
shape). Gemini **conceded** its iter-1 ADR 220 table rewrite to the additive-on-
memo-admission resolution (Codex/Claude: `blocked_or_near_miss_count` is the load-bearing
ADR 220 aggregate). Claude contributed the star-has-no-thin-margin guard and the
`investigation`-not-`generation` naming; Codex contributed the floor-gated
exactly-one-ready classifier that prevents capability overclaim. No residual divergence.

## Forward arc

```text
224 group-window recency
-> 225 memo-admission blocker-class readout (this; zero admissible)
-> 226 one bounded candidate contract for a named capability-limited group/reason
       (likely MultipleProbeRelations), only once a capability_limited row is shown and the
       reason is grounded memo-fixable — lifts bounded_candidate_count >= 2, makes the first
       admissible row reachable, retires the no-memo fence for that one shape
-> receipt-armed memo actuation -> wider candidate families
```

## Deferred (named)

Any bounded candidate contract / `admissible` row / memo actuation / selection; generic
Cascades search; memo arena; candidate memoization; per-reason decline detail on the 220
table; hard-blocker resolution (statistics substrate for `MissingEstimate`, operator
support for `NonInnerEquiJoin`); placement-authorization path for `MixedStrategyStar`;
window-key GC; SQL parenthesized join factors; mixed-chain pushdown; deployment elasticity
signal.
