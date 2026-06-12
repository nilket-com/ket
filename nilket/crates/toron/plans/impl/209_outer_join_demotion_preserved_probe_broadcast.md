# Impl Plan — ADR 209: Null-Rejected Outer-Join Demotion and Preserved-Probe Broadcast

Pairs with `plans/adr/209_outer_join_demotion_preserved_probe_broadcast.md`. Two mechanisms, both
Toron-side: a pre-bind logical normalization (classifier + demotion + demotion-owned conjunct relocation)
and a placement-table retarget of the broadcast fences (with the mirrored-physical-kind fix at the swap
site). Execution semantics untouched — every kind already runs. Phased so each step lands green and the
headline flips on the final fence retarget.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Pre-bind home | bind entry (binder.rs — shared helper feeding both `bind` and `bind_with_explain_trace`); the walk is bottom-up: Join arm :1127 chooses strategy before Filter arm :857 sees the predicate |
| Logical shapes / side schemas | `LogicalPlan::Filter` / `LogicalPlan::Join` (ir.rs); side attribution via the existing `join_schema` derivation (binder/join.rs; `pub use` binder.rs:77) |
| Classifier raw material | `Expr::IsNotNull`/`IsNull` (binder.rs:2315/:3625); no existing null-rejection analysis (grep-clean) |
| SQL pushdown gate (untouched) | `all_inner_join_chain` sql_planner.rs:3422, conjunct split :3438-:3482 — the pre-bind pass makes demoted chains all-inner before this matters for facts |
| Auto-strategy fence | `choose_auto_join_strategy` join.rs:447; `HashByDefaultReason::NonInnerJoin` early-return :457 |
| Pinned/shape fences | `build_join_plan` / `join_strategy_intent_for_hint` reject SQL/Rune non-inner pins; `JoinStrategyPinSource::Direct` reaches the binder; `validate_strategy_shape` join.rs:915 is currently source-blind (reject string :944-946) |
| Frontend hint fences (stay) | sql_planner.rs build_join_plan / join_strategy_intent_for_hint sites (~:5090/:7455/:8019; test :8106) |
| Swap site (D6 fix) | binder.rs:2119-2123 `JoinPhysicalPlacement::SwappedRightBuild` — swaps plans/keys, keeps `kind`; existing two-sided declared-order restore is `restore_declared_join_order_project` at binder.rs:2159 / binder/join.rs:631 |
| Fragmenter fence | `FragmentError::BroadcastNonInner` fragment.rs:84, enforced :518/:621; test :1823 |
| Runtime-filter fence (untouched) | `runtime_filter_join_eligible` fragment.rs:960 (used :399) |
| Semi/anti reachability | `EXISTS` → LeftSemi/LeftAnti sql_planner.rs:3686-3691; exec arms exec.rs:2676/:2989/:3014/:3038 |
| Receipt home | `OptimizerExplainRenderTrace` (binder/explain.rs), 174/175 cockpit pattern; conjunct text via the 175 bound-program pretty-printer |
| Guards to retarget | adr129 (auto fence), adr135 (fragmenter), adr143 (all-inner pushdown), adr125/128 (authoring — assert still pinned); locate by `adrNNN_` grep in tests/architecture/ |

## Phase 0 — Discriminating tests first (red before green)

1. Headline `left_outer_null_rejecting_right_filter_binds_inner_and_broadcasts_filtered_dim` — observe
   it fail on `main` for all three reasons (stranded WHERE, NonInnerJoin hash, recognizer decline).
2. `is_null_on_null_supplied_side_does_not_demote` — written to assert non-demotion explicitly; must pass
   before AND after.
3. `left_outer_right_build_auto_broadcast_executes_as_physical_right_outer` — asserted on **results**
   (swap-without-mirror produces the wrong preserved side, not a plan-shape diff).
4. The A/B harness `*_null_rejected_results_match_unsimplified`, parameterized over the v1 classifier set
   × {LeftOuter, RightOuter, FullOuter} — row equality, not schema-flag equality (D3 effective-kind).
5. `rune_filter_after_left_join_gets_same_demotion_as_sql` — the altitude proof.

## Phase 1 — Null-rejection classifier (pure)

`fn null_rejecting_sides(conjunct, left_schema, right_schema) -> NullRejectedSides` beside the join
helpers: explicit allow-arms (IS NOT NULL; strict comparisons vs **non-null literals**; `LIKE` literal
pattern; positive `IN` over non-null literals; AND unions rejecting child sides), an explicit
`IS NULL ⇒ empty` arm with the why-comment, default-deny fall-through. Side attribution against the two
input logical schemas; unresolvable/mixed-side/qualifier-ambiguous atomic expressions ⇒ empty.
**Verify-first:** whether `LIKE`/`IN` shapes are classifiable at pre-bind altitude — if only post-lowering,
drop them from v1 (strict comparisons + IS NOT NULL still deliver the headline) and name the narrowing in
the evidence doc. Unit net: every allow-arm per side; both-side AND union; every deny case incl. nullable
literal and negated `LIKE`/`IN`.

## Phase 2 — Pre-bind demotion normalization (binder.rs bind entry)

`fn demote_null_rejected_outer_joins(plan) -> (plan, Vec<DemotionReceipt>)`: walk; at each
`Filter { input: Join, .. }` split top-level AND, classify (Phase 1), apply the D3 table (FullOuter
stepwise), relocate demotion-owned side-local conjuncts below the join (D4), and rewrite the logical join
kind to the effective kind. `LogicalPlan::Join` carries no output schema; the existing binder
`join_schema`/scope computation must observe the rewritten kind and produce D3 nullability. One pass, no
fixpoint — relocation moves conjuncts to a side, never onto another join's filter (pin with a unit test).
Wire into the shared bind-entry helper so SQL and Rune hit it identically; thread receipts to the render
trace (Phase 5). Semi/anti: classifier never fires (no null-supplying side) — assert, don't special-case.

## Phase 3 — Preserved-probe placement + mirrored kind (binder/join.rs + binder.rs)

- `fn broadcast_physical_placement(kind) -> BroadcastPlacementRule` — the single D5 table.
- `choose_auto_join_strategy` (:447): replace the `:457` early-hash with the table (FullOuter and
  no-safe-placement still hash via the existing reason).
- Preserve the D7 authoring split explicitly: reject `JoinStrategyIntent::Pinned { strategy: Broadcast,
  .. }` with `kind != Inner` before/alongside shape validation (including `JoinStrategyPinSource::Direct`).
  Then retarget `validate_strategy_shape` (:915) to accept safe **auto-produced physical** shapes. If the
  helper remains source-blind, pass an `allow_non_inner_broadcast` flag or the pin source rather than
  widening all callers accidentally.
- Swap site (binder.rs:2119): derive `physical_kind` from `(declared_kind, decision.placement)`. On
  `SwappedRightBuild`, mirror `LeftOuter`→`RightOuter`, `LeftSemi`→`RightSemi`, and
  `LeftAnti`→`RightAnti`; the `ExecutionPlan::Join` stores `physical_kind`, while declared scope/facts
  stay based on the pre-swap logical kind. Reuse the existing two-sided
  `restore_declared_join_order_project` for swapped `Inner` and swapped `LeftOuter`; for swapped
  `LeftSemi`/`LeftAnti`, do **not** call the two-sided restore helper — assert the physical output schema
  already equals the declared emitted/probe schema.
- Frontend hint fences: unchanged (D7), and direct pinned logical plans stay covered by the binder gate.
- `scripts/cargo-cgroup check -p toron --all-targets` after any widely-constructed struct grows.

## Phase 4 — Fragmenter retarget (distributed/fragment.rs)

`BroadcastNonInner` (:518/:621) admits the safe **physical** kinds on the broadcast path (Inner,
RightOuter, RightSemi, RightAnti post-mirror); everything else rejects with the same error. Single
published build + scan-linear probe requirements unchanged; recursive broadcast-star children stay
inner-only; test :1823 retargets.

## Phase 5 — Receipt (binder/explain.rs)

`DemotionReceipt { from_kind, to_kind, conjunct_text }` rendered per demotion on EXPLAIN. Reuse the 175
pretty-printer. No information_schema change, no postcard preimage, no goldens.

## Phase 6 — Tests + guards

Behavior: the Phase-0 set plus `full_outer_*` stepwise pair, semi/anti broadcast no-duplicates (+ the
`EXISTS`-shaped variant and a one-sided-schema assertion),
`direct_non_inner_broadcast_pin_stays_rejected`,
`left_outer_left_build_auto_broadcast_hashes_as_unsafe`,
`full_outer_without_demote_stays_hash_non_inner`,
`fragmenter_accepts_safe_non_inner_broadcast_and_rejects_preserved_build`,
`non_inner_broadcast_distributed_result_matches_hash_for_safe_kinds` (one distributed A/B per safe kind),
`runtime_filter_stays_inner_only_for_actual_outer_join`,
`explain_reports_null_rejected_outer_join_demotion`,
`demoted_join_verdict_reaches_durable_lane` (LiveWendelonFixture-class — flake discipline: rerun isolated
failures 3-5×; binary-level result canonical).

Guards: the twelve `adr209_*` from the ADR; the four named retargets (129 → placement matrix, 135 → safe
physical shapes, 143 → fence + demotion exception, 125/128 → assert still pinned). The author-pinned guard
must inspect both frontend fences and the binder's `JoinStrategyPinSource::Direct` path. Run the **full
architecture binary** — and check `adr176_no_runtime_or_wire_surface_change`'s later-ADR allowlist for any
newly-touched file (sql_planner.rs, fragment.rs, exec-adjacent tests are already listed; verify, don't
assume — the 207 F2 lesson).

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib null_reject
scripts/cargo-cgroup test  -p toron --lib demot
scripts/cargo-cgroup test  -p toron --lib broadcast
scripts/cargo-cgroup test  -p toron --test hash_join_kinds_test
scripts/cargo-cgroup test  -p toron --test architecture_test adr209
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — non-optional
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Hygiene (the bitten-before list)

- No m-word in src/tests (adr043 substring scan — Toron-side applies); margins "loosen/tighten",
  sequences "strictly increasing".
- `DemotionReceipt` is in-memory EXPLAIN state — zero golden re-pins expected; a shifted golden is a
  finding, not a re-pin.
- Plain names, no V1 suffixes; all cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first items)

1. `LIKE`/`IN` classifiability at pre-bind altitude (Phase 1 narrowing rule).
2. The cleanest source-aware hook for keeping `JoinStrategyPinSource::Direct` non-inner broadcast rejected
   while allowing auto-produced safe physical shapes.
3. Whether `validate_strategy_shape`'s reject string is asserted anywhere beyond sql_planner.rs:8106
   (grep before changing the message).
