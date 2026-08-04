# ADR 211: Safe-Side Broadcast Authoring

**Status:** Promoted. **Migration:** `plans/impl/0211_safe_side_broadcast_authoring.md`.
**Category:** Toron optimizer policy and SQL/Rune authoring surface. No executor, fragmenter, storage,
wire, runtime-filter, recognizer, or telemetry widening.

## Why This Cut Exists

ADR 209 made preserved-probe broadcast executable and optimizer-selectable for non-inner joins, but kept
author pins deliberately inner-only. ADR 210 then made null-rejected outer edges reachable in natural
mixed-chain SQL/Rune shapes. The next missing step is authoring: users can now write queries whose safe
non-inner broadcast shape is known, reachable, and executable, but SQL hints and Rune authoring still reject
the request before the ADR 209 placement matrix is consulted.

The desired contract is narrower than "allow non-inner broadcast hints": **an author may pin broadcast only
for the build side that ADR 209 already proves safe for the effective join kind.** Preserved sides remain
rejected, `FullOuter` remains rejected unless demoted before binding, and true outer joins remain recognizer
fences.

## Grounding

- `JoinStrategyIntent::Pinned` currently carries only `{ strategy, source }` in `ir.rs`; it loses which side
  the author intended to build.
- SQL `/*+ BROADCAST(alias) */` already names an alias, but `join_strategy_intent_for_hint` rejects targets
  on the right side and rejects `kind != Inner` during SQL lowering.
- `build_join_plan` currently physicalizes broadcast pins by placing a single-partition `Exchange` on the
  left input and rejects non-inner broadcast. That makes safe right-build authoring impossible unless the
  binder owns pinned broadcast physicalization.
- `bind_scoped_join` still has a blanket `Pinned Broadcast && kind != Inner` rejection before strategy
  selection.
- `broadcast_physical_placement` already encodes the safe matrix:
  `Inner => SmallerBuild`, `Right* => NaturalLeftBuild`, `Left* => SwappedRightBuild`,
  `FullOuter => Never`.
- Rune's current `broadcast_join` constructs an inner join. The existing non-inner constructors emit `Auto`
  intent, so Rune needs a post-join broadcast pin annotation rather than more join constructors.

## Decisions

**D1 - Pinned broadcast carries build-side intent.** `JoinStrategyIntent::Pinned` grows a build-side field
for broadcast pins. The field uses the same left/right vocabulary as the binder's broadcast decision path;
move that side enum to an IR-visible location rather than creating a second hint-only type. Hash pins carry
no build side.

**D2 - The binder owns pinned broadcast physicalization.** Frontends stop pre-shaping broadcast pins with
single-partition exchanges. SQL, Rune, and direct logical plans emit a logical join with a pinned
strategy/build side; `bind_scoped_join` applies the same exchange/swap/restore machinery used by auto
broadcast after validating the pin. This avoids encoding side choice by tree shape and removes the old
`force_hash_join_ids` strip-root-exchange special case.

**D3 - Frontends resolve side; the binder judges kind.** `/*+ BROADCAST(alias) */` resolves the target
alias against the immediate left/right join inputs and emits that side in the pinned intent. SQL lowering
does not reject by syntactic join kind, because ADR 209/210 demotion has not run yet. The binder admits only
the ADR 209 safe side for the effective kind: inner joins accept either side, left-preserving joins accept
right-build, right-preserving joins accept left-build, and true `FullOuter` rejects. A target inside a
composite side remains rejected; this is authoring, not general side extraction.

**D4 - Unsafe pins reject loudly.** A preserved-side pin, ambiguous alias, composite-side alias, or
`FullOuter` pin without demotion returns a descriptive error. Silent degradation to auto is forbidden:
prescriptive hints that cannot be honored must fail rather than rot.

**D5 - Validity is judged against the effective post-normalization kind.** SQL/Rune lowering may attach a
pin to a syntactic outer join, but the authoritative validation point is binding after ADR 209/210
normalization. A `FullOuter` that demotes to `Inner` may honor a pinned side that is valid for `Inner`; a
true `FullOuter` still rejects.

**D6 - Rune broadcast authoring is a post-join pin.** Keep existing `broadcast_join` as the inner
left-build shorthand. Add a Rune method that annotates the just-built join with `Pinned Broadcast` and an
explicit build side, for example `df.left_join(dim, on).broadcast_build(Right)`. The same binder gate
validates the annotation for every join kind. No Rune-only placement policy and no constructor explosion.

**D7 - No new execution or recognition semantics.** Fragmenter safe-kind checks, runtime-filter eligibility,
join-order recognizers, and executor joins are unchanged. A true outer join is still not an inner shape for
recognizers; this cut only lets authors request broadcast where the already-bound physical shape is safe.

## Honesty Gate

`sql_left_outer_broadcast_hint_on_null_supplied_side_binds_preserved_probe_broadcast`: a query such as

```sql
SELECT /*+ BROADCAST(c) */ *
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
```

binds as broadcast using the ADR 209 swapped-right-build physical path, records `PinnedByAuthor`, and
returns the same rows as the hash plan. This fails on main because the SQL hint resolver rejects a target on
the right side and the frontend/binder both reject non-inner broadcast pins.

## Load-Bearing Discriminators

- `sql_left_outer_broadcast_hint_on_preserved_side_rejects_loudly` - fails under silent degradation.
- `sql_right_outer_broadcast_hint_on_null_supplied_side_binds_preserved_probe_broadcast`.
- `sql_full_outer_broadcast_hint_rejects_without_demotion`.
- `sql_full_outer_broadcast_hint_is_honored_after_both_sides_null_rejecting_demote`.
- `sql_inner_broadcast_hint_can_target_either_immediate_side`.
- `direct_pinned_non_inner_wrong_side_rejects` and `direct_pinned_non_inner_safe_side_binds`.
- `rune_post_join_broadcast_pin_matches_sql_hint` and `rune_preserved_side_broadcast_rejects_like_sql` -
  parity pins both halves: admission and the loud reject.
- `safe_side_hinted_outer_join_distributed_result_matches_hash`.
- `force_hash_join_ids_still_demotes_sql_hint_without_frontend_exchange_strip`.
- `hinted_safe_side_outer_join_explain_reports_pinned_by_author`.

## Architecture Guards

- `adr211_pinned_broadcast_intent_carries_build_side`
- `adr211_frontends_do_not_preshape_pinned_broadcast`
- `adr211_sql_hint_resolves_immediate_safe_side`
- `adr211_binder_validates_author_pins_through_209_placement_table`
- `adr211_preserved_side_and_full_outer_pins_reject_loudly`
- `adr211_rune_post_join_pin_shares_sql_admission_path`
- `adr211_no_fragmenter_executor_wire_runtime_filter_or_recognizer_widening`

**Reconciled prior guards:** ADR 209's author-pinned non-inner rejection inverts to table-validated
admission: safe side accepted, preserved side rejected. SQL planner guards for right-side and non-inner
broadcast hints retarget to the new safe-side rules. Existing inner broadcast hint behavior remains green.
All ADR 210 propagation guards must pass unchanged.

## Rejected Alternatives

**SQL-only widening** leaves Rune and direct logical plans with a different contract and keeps side choice
implicit. **Planner-side swapping** smuggles author intent through tree shape and duplicates binder
placement logic. **Kind-specific Rune broadcast constructors** multiply surface area for a policy that is an
annotation on an existing join. **Silent fallback to auto** makes hints untrustworthy. **General
composite-side extraction** is broader than authoring and belongs with later mixed-chain pushdown work.
**Runtime-filter or recognizer widening for true outer joins** is out of scope and unsafe here.

## Consensus (transparent)

3-of-3 on the spine across two iterations. The convergence trail: Codex contributed the load-bearing
design center - `Pinned` must carry the build side or side semantics smuggle through tree shape - plus the
frontend pre-shaping grounding that became D2 (the left single-partition `Exchange` at lowering and the
`force_demoted_sql_broadcast`/`strip_root_exchange` special case it retires). Claude contributed two
structural corrections: kind-validity must be judged at the binder gate because hints resolve at lowering,
*before* the 209/210 pre-bind demotion normalizer (making D5's effective-kind rule structural rather than
special-cased), and the Rune surface must be a post-join annotation because `broadcast_join` is a
constructor with no kind to lift - a self-correction of his own iter-0 lean. Gemini merged the D-set and
carried the loud-reject and effective-kind decisions into convergence. The one scope add - inner hints
targeting either side (D3) - was flagged by Claude, left optional by Codex, and folded on the grounding
that the left-only restriction was v1 scaffolding, not a safety rule. All anchors in this document,
including `PinnedByAuthor` (which already exists in the decision-reason vocabulary), were verified against
the code at promotion review.

## Forward Arc

`209 preserved-probe broadcast -> 210 path-scoped propagation -> 211 safe-side broadcast authoring (this)
-> OR-aware null rejection -> identity-remapped Alias/Project motion -> general mixed-chain pushdown ->
evidence-driven outer ordering`. The outer-join surface is complete after this cut - executable (008) ->
optimizable (209) -> reachable (210) -> authorable (211); everything beyond is condition-gated, so the next
cycle rotates naturally.

## Deferred

OR-aware null rejection; Alias/Project motion; composite-side authoring; general mixed-chain pushdown;
FullOuter broadcast without demotion; named Rune broadcast constructors if the annotation proves
insufficient; runtime filters for true non-inner joins; evidence-driven outer-join ordering.
