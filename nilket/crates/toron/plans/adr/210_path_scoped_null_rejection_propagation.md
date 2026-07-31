# ADR 210: Path-Scoped Null-Rejection Propagation

**Status:** Promoted. **Migration:** `plans/impl/210_path_scoped_null_rejection_propagation.md`.
**Category:** Toron optimizer logical normalization — a reach cut on ADR 209. No Wendelon, executor,
fragmenter, storage, wire, runtime-filter, strategy, authoring-hint, or recognizer widening.

## Why this cut exists

ADR 209 made null-rejected outer joins useful to the optimizer — but only when the rejecting filter sits
**directly** above the demotable join. Natural SQL puts it higher:

```sql
SELECT o.id, c.id, p.id, s.id
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
INNER JOIN products p ON o.product_id = p.id
INNER JOIN stores  s ON o.store_id   = s.id
WHERE c.tier > 0;
```

The WHERE lands above the **top** (inner) join; 209's direct `Filter(Join)` match never sees the nested
outer edge, and the 209 durable-lane gate had to hand-author the demotable subtree to prove the arc. This
cut makes the arc reachable for the shapes people actually write: **carry a 209-proven null-rejecting
conjunct down an inner spine to the first outer edge it can demote — and nowhere else.** Not outer-join
reordering, not a pushdown engine: a conjunct that can't reach a demotable edge through motion-safe nodes
stays exactly where it was. (Authoring hints were the competing candidate; reach won because hints only
amplify what's reachable.)

## Grounding (verified against the code)

- 209 is single-hop by construction: the Filter arm demotes only when its (normalized) input is directly a
  `LogicalPlan::Join` (outer_join_demotion.rs Filter arm). SQL strands the mixed-chain WHERE above the top
  join (`all_inner_join_chain` sql_planner.rs:3422 declines non-inner chains) — the headline genuinely
  fails on main.
- **Scope-transparency ≠ motion-transparency.** The 209 scope helper
  (`logical_scope_for_outer_demotion`) looks through `Filter | Exchange | Sort | Take` and remaps `Alias`
  — sound for **column visibility**. Predicate **motion** is a strictly smaller relation: `Take` selects
  rows (LIMIT-then-filter ≠ filter-then-LIMIT), and — the second catch of this cycle —
  `LogicalPlan::Sort` carries `limit: Option<usize>` + `offset: usize` (ir.rs:965-970), so a **top-N or
  offset sort is a row-selection boundary exactly like `Take`**; only a pure sort preserves the multiset.
- `LogicalPlan::Exchange` is frontend-emitted at logical altitude (rune_engine.rs:371,
  sql_planner.rs:3292/:3518). Rune chaining and hand-authored logical plans can place it on the descent
  path; it is row-preserving and motion-safe.
- The 209 classifier (`null_rejecting_sides`, exhaustive default-deny) and demotion table
  (`demoted_join_kind`) are reused byte-for-byte — nothing here touches them.

## Decisions (promotion-locked)

**D1 — Reuse 209's classifier and demotion table, unchanged.** A conjunct proves null rejection with the
existing allowlist and demotes with the existing table. No OR fold, no new allow-arm, no path-special
cases. (OR-aware classification is the named classifier follow-on — folding it here would break this
decision's reuse-only contract, which *is* the cut's safety story.)

**D2 — One pre-bind router subsumes depth zero.** The 209 pass stays the shared SQL/Rune pre-bind
normalization; its Filter arm becomes a conjunct router where depth-0 (209's direct case) is one route
outcome and path-scoped descent is another. One mechanism, no second pass, no post-strategy rewrite.

**D3 — Minimal motion set: inner joins, `Exchange`, and pure `Sort` only.** A conjunct descends through an
`Inner` join into the single side that wholly owns its column references (ownership routing, not demotion
proof), through `Exchange`, and through `Sort` **only when `limit.is_none() && offset == 0`**. It stops —
silently, staying residual — at `Take`, limited/offset `Sort`, `Alias`, `Project`, `Aggregate`, scans,
mutation/source statements, and every other shape (explicit match arms; a new `LogicalPlan` variant must
force a decision).

**D4 — The first non-inner edge is the only possible target.** Descent reaching a non-inner join either
demotes it (D1) or stops. Conjuncts never cross through an outer/semi/anti join to search deeper; semi/anti
have no null-supplied side and are stops.

**D5 — Move only on demotion success.** A conjunct leaves the original Filter only if it reached a target,
proved the demotion, and was installed by 209's demotion-owned relocation. Cross-side, ambiguous,
unsupported, boundary-stopped, or non-demoting conjuncts stay residual in place. A missed propagation is a
performance miss; a duplicated or dropped predicate is a correctness bug — the A/B net proves neutrality.

**D6 — The existing demotion receipt grows path detail.** `OuterJoinDemotionReceipt` gains a propagation
path of side-steps (`left.left`; empty = depth-0, rendering exactly as today). One cockpit row kind — the
demotion outcome plus where its proof came from. No new system table, durable key, or telemetry surface.

**D7 — Recognizers remain actual-inner only.** Untouched; they see the edge only because the normalization
made it inner. True outer joins remain fences.

**D8 — Not general mixed-chain pushdown.** Non-demoting predicates do not move — not for facts, estimates,
or broadcast eligibility. Safe-side broadcast authoring, OR-aware rejection, identity-remapped
`Alias`/`Project` motion, and general pushdown are named follow-ons.

## Honesty gate

`sql_mixed_chain_null_rejection_demotes_nested_left_join_and_reaches_durable_verdict`: the natural mixed
star above, against the existing durable star fixture — the nested edge demotes, the query actuates as an
inner star, and a fresh evidence handle reads the durable flat-star verdict. **Fails on `main`** (the
predicate is stranded above the top inner join).

**Load-bearing discriminators:**
- `filter_does_not_move_below_take_limit` — the motion-vs-scope pin (fails under a transparent-`Take`
  implementation — the corrected draft bug).
- `filter_does_not_move_below_sort_limit_or_offset` — top-N/offset sort is a row-selection boundary
  (fails under unconditional-Sort transparency — the second corrected draft bug).
- `pure_sort_allows_path_scoped_demote_with_results_match` — pure sort is transparent and result-neutral.
- `predicate_does_not_cross_outer_boundary_to_reach_deeper_edge` — the D4 fence.
- `cross_side_conjunct_stays_residual_above_original_filter` — no accidental broad pushdown.
- `is_null_and_negated_in_remain_non_demoting_inside_mixed_chain` — D1's reuse, proven from the new path.
- `mixed_chain_demoted_results_match_undemoted` — the A/B net over descent shapes (through one and two
  inner joins, through `Exchange`, through pure `Sort`, split-across-siblings ⇒ residual).
- `rune_mixed_chain_null_rejection_matches_sql` — the parity gate, day one.
- `depth_zero_behavior_matches_adr209` — the refactor-safety pin: the full 209 suite passes through the
  unified router with zero test edits.
- `propagation_receipt_reports_descent_path` (D6) and
  `recognizers_still_decline_true_outer_join_after_non_demoting_filter` (D7).

## Architecture guards (promotion-locked)

- `adr210_descent_reuses_209_classifier_and_demotion_table`
- `adr210_filter_router_subsumes_depth_zero_demotion`
- `adr210_motion_set_is_inner_exchange_and_pure_sort_only`
- `adr210_topn_sort_and_take_are_motion_boundaries`
- `adr210_conjuncts_move_only_on_demotion_success`
- `adr210_propagation_extends_existing_outer_join_demotion_receipt`
- `adr210_no_recognizer_pushdown_strategy_executor_or_wire_widening`

**Reconciled prior guards:** ADR 143's pushdown fence gains this named propagation exception beside 209's
demotion exception — same fence, exceptions enumerated, the old non-inner-chain counterexample still
rejected. **Every `adr209_*` guard must pass unchanged** — if one needs weakening, the design is wrong;
surface it, don't retarget quietly.

## Rejected alternatives

**Safe-side broadcast hints first** — they amplify only reachable shapes; reach first (the iter-0
menu's recommendation, re-sequenced by this argument). **OR-aware rejection now** — changes the classifier;
its own cut. **All-Sort transparency** — wrong when `limit`/`offset` select rows. **The scope helper as
the motion helper** — different contracts (the organizing distinction of this ADR). **`Alias`/`Project`
motion now** — needs qualifier/identity remapping the headline doesn't require. **Recognizer outer
semantics** — order-sensitive and receipts-ungated.

## Consensus (transparent)

3-of-3 on the frame and D-set. The convergence trail is a clean correction chain: Codex proposed
reach-before-authoring (Claude conceded the iter-0 authoring recommendation); Claude contributed the
scope-vs-motion distinction and caught `Take` in Gemini's motion set (results-changing; Gemini adopted
with credit); **Codex then caught the same bug class in Claude's set** — unconditional `Sort`
transparency, falsified by the IR's `limit`/`offset` fields — refined to pure-Sort-only (D3). Gemini's
final draft predates that refinement and carries unconditional `Sort`; the promoted D3 is the grounded
2-of-3, surfaced here per council discipline. Gemini's OR-fold was withdrawn by Gemini himself after the
reuse-only argument. Claude resolved his own carried open (`Exchange` is frontend-emitted at logical
altitude — stays in the motion set).

## Forward arc

`209 demotion + preserved-probe broadcast → 210 path-scoped propagation (this) → safe-side SQL/Rune
broadcast authoring (reach now amplifiable) → OR-aware null rejection → identity-remapped Alias/Project
motion → general mixed-chain pushdown (named demanding shape required) → evidence-driven outer ordering
(only if durable receipts show demotion still leaves wins)`.

## Deferred (named)

Safe-side broadcast authoring (next); OR-aware null rejection; `Alias`/`Project` motion-transparency
(qualifier/identity remap); general mixed-chain pushdown; declared-nullability schema preservation;
FullOuter broadcast via global reduction; evidence-driven outer-join ordering; memo/Cascades.
