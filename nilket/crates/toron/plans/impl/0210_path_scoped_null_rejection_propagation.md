# Impl Plan — ADR 210: Path-Scoped Null-Rejection Propagation

Pairs with `plans/adr/0210_path_scoped_null_rejection_propagation.md`. One file carries the mechanism:
`binder/outer_join_demotion.rs` grows a descent router that subsumes 209's direct case as depth-0, plus the
receipt field and its explain.rs rendering. No binder-walk, strategy, fragmenter, executor, or frontend
changes.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Single-hop limit to lift | outer_join_demotion.rs Filter arm — `let LogicalPlan::Join {..} = input else { return Filter{..} }` |
| Scope helper (resolution only; reused) | `logical_scope_for_outer_demotion` — Scan/ScanNamed terminal, Alias remap, `Filter\|Exchange\|Sort\|Take` pass-through, Join composes, Project/Aggregate → None |
| Classifier + table (untouched, D1) | `null_rejecting_sides` :440 (exhaustive default-deny); `demoted_join_kind`; `demotion_owned_sides`; the left_push/right_push/residual relocation |
| Receipt struct (grows, D6) | `OuterJoinDemotionReceipt { from_kind, to_kind, conjunct_text }` |
| Receipt rendering | binder/explain.rs `outer_join_demotions` rows (209 D9 surface) |
| Pure-sort rule (D3) | `LogicalPlan::Sort { order_by, limit: Option<usize>, offset: usize }` ir.rs:965-970 — motion-safe iff `limit.is_none() && offset == 0`; `Take { limit }` ir.rs:971 always a boundary |
| Exchange at logical altitude | emitted by Rune sort and SQL sort/aggregate paths (rune_engine.rs:371, sql_planner.rs:3292/:3518); row-preserving and motion-safe |
| Headline strand-on-main | left-deep SQL lowering + `all_inner_join_chain` sql_planner.rs:3422 |
| Durable star fixture (headline reuse) | tests/wendelon_storage_execution_test.rs — the 209 `demoted_join_verdict_reaches_durable_lane` family (:755) |
| Guards home | tests/architecture/outer_join_completion.rs; 143 fence guard in tests/architecture/optimizer.rs |

## Phase 0 — Discriminating tests first (red before green)

1. Headline `sql_mixed_chain_null_rejection_demotes_nested_left_join_and_reaches_durable_verdict` —
   observe the strand on `main` (no receipt; nested edge stays outer; AST/hash plan).
2. `filter_does_not_move_below_take_limit` — fixture where motion below LIMIT changes surviving rows;
   assert residual placement AND result equality with the unoptimized plan.
3. `filter_does_not_move_below_sort_limit_or_offset` — the pure-Sort pin: top-N and offset variants both
   stop; `pure_sort_allows_path_scoped_demote_with_results_match` as the allow-side twin.
4. `depth_zero_behavior_matches_adr209` — the full existing 209 demotion suite green through the unified
   router with zero test edits, before any descent case lands.
5. `predicate_does_not_cross_outer_boundary_to_reach_deeper_edge` — stacked outer joins; the deeper
   target's conjunct stays residual.
6. `mixed_chain_demoted_results_match_undemoted` A/B — descent shapes: one inner hop, two inner hops,
   through `Exchange`, through pure `Sort`, split-across-siblings ⇒ residual.

## Phase 1 — Unify (behavior-neutral refactor)

Extract the 209 Filter-arm body into a conjunct router (`route_conjuncts(...) -> { demotions, pushes,
residual, receipts }`) with the current logic as the depth-0 route. Gate: Phase 0.4 green. No descent yet.

## Phase 2 — The descent (D2/D3/D4/D5)

Per conjunct, `descend(plan, conjunct, path) -> Descent` with **explicit arms, no wildcard**:
- `Join { kind: Inner, .. }` → resolve column ownership against side scopes (existing helper plus a small
  expression-side walker over column refs using `column_side` ambiguity rules; this is not a new
  null-rejection classifier); wholly-one-side → recurse with the side-step appended; split/ambiguous →
  `Stop`.
- `Join { kind: non-inner, .. }` → classify via `null_rejecting_sides` against this join's scopes;
  qualifying → `Demote { path }` (apply `demoted_join_kind` + the 209 relocation at the edge; rebuild the
  spine above); else `Stop` (semi/anti always: no null-supplied side).
- `Exchange { .. }` → recurse. `Sort { limit: None, offset: 0, .. }` → recurse;
  `Sort { .. }` (limited/offset) → `Stop`.
- `Take`, `Alias`, `Project`, `Aggregate`, scans, mutation/source statements, and every other variant →
  `Stop`.
Move-only-on-success: `Stop` leaves the conjunct in the original residual. Termination: strictly downward,
single pass, no fixpoint; the rebuilt spine is not re-entered (pin with a unit test).

## Phase 3 — Receipt (D6)

`OuterJoinDemotionReceipt` gains `propagation_path: Vec<JoinSideStep>` (empty = depth-0, renders as
today); explain.rs appends `(propagated: left.left)` style detail for non-empty paths. Pick the side-step
vocabulary by what renders legibly in the existing EXPLAIN row width.
`scripts/cargo-cgroup check -p toron --all-targets` after the struct change (the 116 lesson).

## Phase 4 — Tests + guards

Phase-0 set plus `cross_side_conjunct_stays_residual_above_original_filter`,
`is_null_and_negated_in_remain_non_demoting_inside_mixed_chain`,
`rune_mixed_chain_null_rejection_matches_sql` (the 209-F1 loop pattern: one fixture, both frontends),
`recognizers_still_decline_true_outer_join_after_non_demoting_filter`,
`propagation_receipt_reports_descent_path`, a FullOuter-stepwise-via-propagation variant, and a
multi-conjunct fixture (two conjuncts demoting two different edges in one pass — prefer sharing the
durable star fixture's catalog).

Guards: the seven `adr210_*` from the ADR in tests/architecture/outer_join_completion.rs. Retarget: the
143 fence guard in tests/architecture/optimizer.rs gains the propagation exception (beside 209's demotion
exception — exceptions enumerated, counterexample still rejected). **All `adr209_*` guards pass
unchanged** — a needed weakening is a design finding, not a retarget. Check the `adr176` later-ADR
allowlist if any newly-touched file falls outside binder-family + architecture + the existing entries.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib outer_join
scripts/cargo-cgroup test  -p toron --lib demot
scripts/cargo-cgroup test  -p toron --lib propagat
scripts/cargo-cgroup test  -p toron --test wendelon_storage_execution_test mixed_chain
scripts/cargo-cgroup test  -p toron --test architecture_test adr210
scripts/cargo-cgroup test  -p toron --test architecture_test adr209
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — non-optional
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Hygiene (the bitten-before list)

- No m-word in src/tests (adr043 substring scan); sequences "strictly increasing".
- The receipt field is in-memory EXPLAIN state — zero goldens expected; a shift is a finding.
- `--all-targets` after the receipt struct grows; plain names, no V1 suffixes; cargo via
  `scripts/cargo-cgroup`.
- The durable-lane headline is LiveWendelonFixture-class — rerun an isolated failure 3-5× before
  reporting; the binary-level result is canonical.

## Open for the implementing session (verify-first)

1. Whether `Sort` appears between Filter and the spine in real frontend output (it does at scan level;
   if it cannot occur on the descent path, keep the arm anyway — the pin tests construct it directly).
2. The receipt path vocabulary (side-steps vs depth) — choose at the explain.rs render site.
3. Whether the two-edge multi-conjunct fixture shares the durable star catalog cleanly.
