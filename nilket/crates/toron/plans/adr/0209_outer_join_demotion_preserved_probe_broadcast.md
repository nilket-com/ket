# ADR 209: Null-Rejected Outer-Join Demotion and Preserved-Probe Broadcast

**Status:** Promoted. **Migration:** `plans/impl/0209_outer_join_demotion_preserved_probe_broadcast.md`.
**Category:** Toron optimizer / local-pair strategy cut. No Wendelon, wire, executor join-semantics,
runtime-filter, memo, or join-order recognizer widening. The arc payoff cut: with 207 (durable evidence),
208 (shape-scoped advisory), and Wendelon 076 (replicated counters) all live, a defensive LEFT JOIN can now
demote, push, broadcast, actuate, and land a measured verdict in durable replicated cluster evidence.

## Why this cut exists

ADR 008 made outer joins executable; the 180–208 arc made **inner** joins participate — filter facts shrink
inputs, broadcast avoids shuffles, recognizers actuate and accrue verdicts. Outer joins still sit outside
that arc, fenced four ways: SQL conjunct pushdown requires an all-inner chain; auto broadcast hashes every
non-inner kind; broadcast authoring/validation/fragmenting reject `kind != Inner`; and the shape
recognizers (correctly — outer joins don't commute) decline non-inner edges. The unlock is not teaching
recognizers outer semantics. It is the ordinary SQL identity: **when a post-join predicate rejects the
null-filled side of an outer join, that join is equivalent to a stronger kind.**

```sql
SELECT * FROM fact f LEFT JOIN dim d ON f.dim_id = d.id WHERE d.region = 'north'
```

has no surviving null-extended rows — it *is* an inner join wearing a defensive costume. Demote it, push
the demoting predicate to `dim`, and everything already built applies: filtered facts, preserved-probe
broadcast, star recognition, durable verdicts. The second fold widens broadcast itself: broadcast was never
unsound for outer/semi/anti joins per se — it is unsound only when the **preserved/emitted side is the
replicated build** (each partition would emit its own copy of the unmatched rows). Placement-aware
admission fixes that without touching execution.

## Grounding (verified against the code; council-triple-checked)

- Outer/semi/anti joins are first-class below the optimizer: all eight `JoinKind`s (ir.rs:870); SQL + Rune
  author them (`left_join()` rune_engine.rs:284; `EXISTS` lowers to `LeftSemi`/`LeftAnti`,
  sql_planner.rs:3686-3691); hash execution covers every kind incl. semi/anti
  (exec.rs:2642/:2676/:2989/:3014/:3038). **No null-rejection analysis exists anywhere** (grep-clean), and
  no existing pass fits: `optimize_for_required` (binder.rs:2819) is post-strategy physical projection;
  predicate pushdown (pushdown.rs) never traverses joins.
- The four fences: `all_inner_join_chain` (sql_planner.rs:3422 — any non-inner join strands the whole
  WHERE above the chain); `choose_auto_join_strategy` early-hashes via `HashByDefaultReason::NonInnerJoin`
  (join.rs:457) before any size consideration; `validate_strategy_shape` (join.rs:915) + the sql_planner
  hint sites reject non-inner broadcast; `FragmentError::BroadcastNonInner` (fragment.rs:84, :518, :621).
- **The bind walk is bottom-up** — the Join arm (binder.rs:1127) binds inputs and chooses strategy before
  the parent Filter arm (:857) sees the predicate. An in-walk rewrite is structurally too late; the
  demotion must be a pre-bind normalization. (This grounding resolved the placement divergence — see
  Consensus.)
- **The mirrored-kind trap:** `JoinPhysicalPlacement::SwappedRightBuild` swaps plans/keys via
  `std::mem::swap` and keeps `kind` (binder.rs:2119-2123) — symmetric for inner, wrong for any sided kind.
- `runtime_filter_join_eligible` (fragment.rs:960) is already correctly inner-only — untouched.

## Decisions (promotion-locked)

**D1 — Pre-bind `Filter(Join)` normalizer, frontend-neutral.** A narrow logical normalizer runs at bind
entry, before the bottom-up walk: it considers only `Filter { input: Join, predicate }`, splits top-level
`AND`, and may rewrite. SQL and Rune get identical behavior (both arrive as `LogicalPlan` — Rune-primary
holds, pinned by gate). The ADR 143 SQL pushdown path is untouched for all-inner chains.

**D2 — Conservative null-rejection classifier (default-deny allowlist).** Recognized as null-rejecting for
one side: `side.col IS NOT NULL`; comparison of `side.col` to a **non-null literal** (`=`, `<>`, `<`,
`<=`, `>`, `>=`); `LIKE` on `side.col` with a literal pattern; positive `IN` over non-null literals;
AND-conjuncts of rejecting terms. Everything else does not demote: `IS NULL` (the opposite!), `OR`, `NOT`,
`CASE`/`COALESCE`, arithmetic, casts, cross-side or unresolvable expressions, nullable literals, negated
`LIKE`/`IN`, subqueries, and every unsupported `Expr` shape. A miss keeps today's plan — never an error.
The risk asymmetry dictates the posture: a missed demotion costs performance; a wrong demotion changes
query results. (OR-both-branches rejection is sound in principle and explicitly deferred — see Consensus.)

**D3 — Strongest equivalent kind; effective-kind schema.** `LeftOuter` + right-rejected → `Inner`;
`RightOuter` + left-rejected → `Inner`; `FullOuter` degrades stepwise (left-only → `LeftOuter`; right-only
→ `RightOuter`; both → `Inner`). Semi/anti never demote (no null-filled side). The visible schema follows
the **effective demoted kind** — nullability reflects the truthful post-filter state (the strict,
Arrow-compatible direction), and downstream facts/NDV reasoning sees honest non-null columns.
Declared-nullability preservation is the named deferred alternative if a wire-compat need ever surfaces.

**D4 — Push only demotion-owned predicates.** The side-local conjuncts that proved the demotion move onto
their owning join input — so scan filters, NDV/range facts, and auto broadcast see the smaller input.
Everything else stays as a residual `Filter` above the rewritten join. **Not** general mixed-chain
path-aware pushdown (named follow-on).

**D5 — Preserved-probe broadcast placement (auto-only), one table.** Broadcast admits a non-inner join iff
the preserved/emitted side is the non-replicated probe: `RightOuter`/`RightSemi`/`RightAnti` → natural
left-build; `LeftOuter`/`LeftSemi`/`LeftAnti` → swapped right-build with **mirrored physical kind** (D6);
`FullOuter` → never (hash, unless demoted first); `Inner` → existing smaller-side choice. Unsafe
placements hash. The table lives in one helper, not scattered kind checks. Semi/anti enter only here —
the soundness rule is identical and `EXISTS` makes them reachable today. Swapped `LeftSemi`/`LeftAnti`
produce the declared emitted/probe side already; they must not route through a two-sided order-restore
projection.

**D6 — Effective physical kind, separate from declared syntactic kind.** A swapped sided broadcast
executes the mirrored kind (`Left*` → `Right*`) and restores declared output order where the output has
both sides. One-sided semi/anti paths assert schema equality instead. Never a sided kind with silently
swapped inputs (the binder.rs:2121 trap, fixed at the source).

**D7 — Author-pinned non-inner broadcast remains rejected.** SQL hints, Rune `broadcast_join`, and direct
pins stay inner-only this cut; the structural verifier and fragmenter accept the safe physical shapes
because **auto** planning produces them. Safe-side hint widening is a named follow-on with its own
125/128 authoring semantics. "Direct" here means `JoinStrategyPinSource::Direct` too — the widened shape
verifier must not become a backdoor authoring surface.

**D8 — Fragmenter retarget, not executor rewrite.** The broadcast fragmenter accepts the safe physical
one-sided shapes (same single-published-build + scan-linear-probe requirements); recursive broadcast-star
children remain inner-only; `execute_hash_join` is untouched.

**D9 — Runtime filters and recognizers stay inner-only.** Both fences untouched. A demoted join qualifies
for both *because it is inner* before they ever see it.

**D10 — Receipt the demotion.** EXPLAIN render shows `left_outer → inner via null-rejecting filter
(d.region = 'north')`. Diagnostic only — no new counters, durable keys, wire fields, or strategy variants.

## Honesty gate

`left_outer_null_rejecting_right_filter_binds_inner_and_broadcasts_filtered_dim`: the defensive-LEFT-JOIN
query above, with facts making the filtered dim selective and broadcast-eligible — identical results to
the unsimplified plan, bound as inner, filter pushed to `dim`, broadcast chosen. **Fails on `main` three
independent ways** (non-inner chain strands the WHERE; auto hashes via `NonInnerJoin`; recognizers
decline).

**Load-bearing discriminators:** `is_null_on_null_supplied_side_does_not_demote` (the dangerous-side pin —
fails under any "predicate touches null side ⇒ demote" rule);
`left_outer_null_rejected_results_match_unsimplified` + the `right_outer`/`full_outer` A/B variants
(results-neutrality proven, not claimed);
`full_outer_one_side_null_rejection_demotes_stepwise` + `full_outer_both_sides_demotes_to_inner`;
`left_outer_right_build_auto_broadcast_executes_as_physical_right_outer` (fails under swap-without-mirror —
wrong preserved side, asserted on results);
`left_semi_and_left_anti_auto_broadcast_build_right_no_duplicates` (also pins one-sided output schema/no
two-sided restore; + an `EXISTS`-shaped fixture);
`direct_non_inner_broadcast_pin_stays_rejected`;
`left_outer_left_build_auto_broadcast_hashes_as_unsafe` +
`full_outer_without_demote_stays_hash_non_inner` (the placement matrix from the reject side);
`fragmenter_accepts_safe_non_inner_broadcast_and_rejects_preserved_build`;
`non_inner_broadcast_distributed_result_matches_hash_for_safe_kinds` (distributed A/B per safe kind);
`runtime_filter_stays_inner_only_for_actual_outer_join`;
`rune_filter_after_left_join_gets_same_demotion_as_sql` (the D1 altitude proof — fails under SQL-only
placement); `explain_reports_null_rejected_outer_join_demotion`;
`demoted_join_verdict_reaches_durable_lane` (the arc gate: a demoted star actuates and its verdict lands
in the 208-tree-scoped, 076-replicated evidence lane).

## Architecture guards (promotion-locked)

- `adr209_null_rejection_classifier_is_default_deny_allowlist`
- `adr209_outer_demotion_runs_before_join_strategy_choice`
- `adr209_demotion_is_frontend_neutral_prebind_normalization`
- `adr209_demoted_predicates_push_to_owning_side_only`
- `adr209_auto_broadcast_uses_preserved_probe_matrix`
- `adr209_swapped_sided_broadcast_uses_mirrored_physical_kind`
- `adr209_author_pinned_non_inner_broadcast_remains_rejected`
- `adr209_fragmenter_accepts_only_safe_non_inner_broadcast_shapes`
- `adr209_runtime_filters_remain_actual_inner_only`
- `adr209_join_order_recognizers_remain_actual_inner_only`
- `adr209_explain_receipts_report_outer_demotion`
- `adr209_no_wendelon_wire_executor_semantics_or_strategy_variant`

**Reconciled prior guards (named retargets, discipline #4):** ADR 129's auto non-inner hash guard → the
preserved-probe matrix + FullOuter fallback. ADR 135's fragmenter `BroadcastNonInner` guard → safe
physical one-sided shapes with preserved probe. ADR 143's all-inner pushdown guard → keeps the fence and
its counterexample, adds the explicit null-rejected-demotion exception. ADR 125/128 authoring guards →
stay pinned (auto-only scope). All join-order non-inner declines: untouched.

## Rejected alternatives

**SQL-only rewrite** — Rune produces the same `Filter(Join)` shape; this is semantic normalization, not
SQL-emits-the-Rune-shape (the 143 situation), so it belongs at the frontend-neutral boundary.
**In-walk binder rewrite** — structurally too late: the walk is bottom-up and the Join binds (strategy
chosen) before the parent Filter is visited. **Demotion in `optimize_for_required`** — that pass is
post-strategy physical projection over `ExecutionPlan`; demotion there affects neither strategy nor
recognition. **Teach recognizers outer semantics** — broader, order-sensitive; demotion keeps them honest.
**Allow all non-inner broadcast** — duplicates preserved build rows per partition. **Right*-only broadcast
(no mirror)** — misses the safe swapped `Left*` placements. **Exclude semi/anti from placement** — same
soundness rule, already reachable via `EXISTS`; excluding is false economy. **Runtime filters for actual
non-inner joins** — unsafe for preserved rows.

## Consensus (transparent)

3-of-3 on the two folds, the conservative posture, recognizers-stay-inner, and the receipt. Convergence
trail: Codex iter-1 contributed the mirrored-kind trap, the pushdown boundary ("demotion alone doesn't
shrink the build side"), and semi/anti placement reachability (Claude conceded his semi/anti deferral for
the placement fold); Claude iter-1 contributed the pre-bind altitude with the bottom-up structural
argument and conceded exhaustive-match to Codex's default-deny; Codex iter-2 sharpened the classifier
(non-null literals, negated `LIKE`/`IN` excluded) and flagged the schema question, settled here as
effective-kind schema (D3). **Two Gemini divergences resolved by grounding, not headcount:** his
`optimize_for_required` placement is structurally too late (post-strategy physical projection — the same
grounding that eliminated it at iter-0); his OR-both-branches classifier arm is sound in principle but
excluded from v1 (2-of-3 conservatism; named deferral). The operator supplied the promoted name — demotion
and preserved-probe are sharper words than the drafts'.

## Forward arc

`207 durable evidence → 208 scoped advisory → 076 replicated counters → 209 outer demotion +
preserved-probe broadcast (this) → safe-side SQL hint widening → OR-aware / wider null-rejection →
general mixed-chain pushdown → evidence-driven outer-join ordering (recognizer outer semantics — only if
durable receipts show demotion leaving wins on the table)`.

## Deferred (named)

Safe-side SQL hint widening (and Rune named methods for non-inner broadcast, if ever wanted); OR-aware
null rejection; declared-nullability schema preservation; general mixed-chain pushdown; actual outer-join
reordering; runtime filters for true non-inner joins; FullOuter broadcast via global unmatched-row
reduction; memo/Cascades.
