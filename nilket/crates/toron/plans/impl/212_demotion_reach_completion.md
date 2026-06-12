# Impl Plan — ADR 212: Demotion Reach Completion

Pairs with `plans/adr/212_demotion_reach_completion.md`. The semantic change lives in
`binder/outer_join_demotion.rs`; receipt rendering may also touch `binder/explain.rs` if the path-step
representation changes. Everything else should be tests and guards. Two independent mechanisms sit behind
one purpose, each with its own falsification family; phase the OR/proof-ownership half green before the
wrapper half begins.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| OR deny site (to classify) | `null_rejecting_sides` `Expr::Or` in the deny group, outer_join_demotion.rs:695 |
| Ownership walker (already Or-aware; untouched) | `conjunct_column_sides` Or arm :794 |
| Rejection sides struct (gains `intersect`) | `NullRejectedSides { left, right }` + `union` |
| Router wrapper arms (to add) | `route_filter_conjuncts` — `Alias`/`Project` currently in the explicit stop group |
| Alias arity/empty-columns precedent | `alias_schema_for_outer_demotion` :858 (arity :865; empty list = qualifier-only) |
| Project scope (intentionally None) | `logical_scope_for_outer_demotion` — Project arm returns None; the descent arm builds its own passthrough map |
| Project IR | `Project { input, exprs, output_names }` ir.rs:957 — passthrough = `exprs[i]` is `Expr::Column` |
| Proof-vs-ownership split (must be explicit) | non-inner relocation currently records proof sides; ADR 212 must also check `conjunct_column_sides` before pushing |
| Receipt path (extends) | `propagation_path: Vec<JoinSideStep>` (210) + explain.rs rendering |
| Guard home | tests/architecture/outer_join_completion.rs — **check the line ceiling** (211 flagged split placement) |

## Phase 0 — Red tests first

1. `or_both_branches_null_rejecting_demotes_left_join` — red on main.
2. `or_one_branch_is_null_does_not_demote` + `or_branches_reject_different_sides_do_not_demote` — written
   to assert non-demotion explicitly; green before AND after.
3. `alias_wrapped_left_join_filter_reaches_demotion` — red on main. **Verify-first:** grep
   `lower_select`'s derived-table lowering to fix the fixture's exact shape (`Alias(Project(..))` vs
   `Alias(..)` over the subquery plan).
4. `project_passthrough_filter_reaches_demotion` — red on main; `computed_project_column_stops_motion` —
   green before and after.
5. `or_cross_side_conjunct_demotes_without_moving` — red until relocation checks ownership separately from
   the OR proof side.
6. `alias_project_residuals_preserve_original_filter_shape` — a conjunct descends through the alias and
   stops at a `Take` below; the residual re-emits the ORIGINAL alias-scope spelling.
7. `remapped_conjunct_results_match_unwrapped_query` — the A/B harness extension over both wrappers.

## Phase 1 — OR intersection (independent; shippable alone)

- `NullRejectedSides::intersect(self, other)` beside `union`.
- Move `Expr::Or(left, right)` from the deny group (:695) to a classify arm:
  `null_rejecting_sides(left).intersect(null_rejecting_sides(right))`. Deny group otherwise
  byte-unchanged.
- Unit net: the three OR cases per side; nested `Or(And(..), ..)` (intersection of a union);
  `or_cross_side_conjunct_demotes_without_moving` (D2's pin — requires the Phase 2 ownership check before
  the OR branch can be considered complete).
- Gate: Phase-0.1 flips green; 0.2 stays green; 209/210/211 behavioral suites stay unweakened. Source
  guard retargets are handled in Phase 6.

## Phase 2 — Proof sides and ownership sides split

- In the non-inner join arm, record both:
  - proof sides from `null_rejecting_sides(&routed, ...)`, used only to decide `demoted_join_kind`;
  - ownership sides from `conjunct_column_sides(&routed, ...)`, used only to decide whether the conjunct can
    be installed below one input.
- Relocation is allowed only when the demotion-owned side is true in the proof sides **and** ownership is
  exactly that same side with `ambiguous == false`. Otherwise the original conjunct remains residual.
- Receipt proof text still includes the original proof conjunct, even when the conjunct is not relocated.
- This phase is mandatory before declaring Phase 1 green: OR intersection can produce a single proof side
  for a conjunct that still references both sides.

## Phase 3 — Alias descent arm

- Router `Alias { input, alias, columns }` arm: positional inverse map (reuse the :858 arity discipline;
  empty columns = qualifier-only). Per conjunct: rewrite every `ColumnRef` (qualifier → input scope, name
  `columns[i]` → input field `i`); **stop the conjunct if any rewritten reference does not resolve
  uniquely** in the input scope (D3).
- **Pair tracking (D5):** wrapper arms route `(original, routed)` pairs; relocation installs `routed`;
  residuals returning from below re-emit `original`. No inverse-remap function.
- Wrapper preserved around the routed input; receipt path gains the alias step.
- Unit net: bijection both directions; empty-columns; ambiguity stop; collision-below stop (derived table
  over a join with duplicate names); pair-tracked residual restoration.

## Phase 4 — Project descent arm

- Router `Project { input, exprs, output_names }` arm: per-output passthrough map (only `Expr::Column`
  entries; duplicate/missing output names poison the map). Conjunct moves iff every referenced output
  maps; rewrite through the map; same pair tracking.
- Computed reference ⇒ stop (explicit, tested). The descent arm constructs the input-side resolution
  itself (the scope helper's Project-None is intentional); **if that construction grows beyond the
  passthrough case, stop and surface** — the swap-trigger discipline.
- Receipt step added.

## Phase 5 — Receipts

`JoinSideStep` (or a small wrapper-step extension — render-site legibility decides, the 210 pattern)
gains alias/project steps; explain.rs rendering extends.
`scripts/cargo-cgroup check -p toron --all-targets` after the enum grows.

## Phase 6 — Guards

The eight `adr212_*` from the ADR. Retargets:

- `adr209_null_rejection_classifier_is_default_deny_allowlist` no longer pins `Expr::Or(_, _)` as denied;
  it pins the intersection arm and OR discriminator names.
- `adr210_motion_set_is_inner_exchange_and_pure_sort_only` allowlist gains the two constrained wrapper arms
  while `Take`, limited/offset `Sort`, and `Aggregate` remain explicit stops.
- `adr210_propagation_extends_existing_outer_join_demotion_receipt` accepts the richer path-step rendering
  if `JoinSideStep` is generalized.

Zero changes to all other prior guards or the design is wrong. Split `outer_join_completion.rs` to a
companion module if the line ceiling fires (the 211 note).

## Named falsifications (for the review)

1. Intersection → union: `or_one_branch_is_null_does_not_demote` must fail.
2. Unconditional Project transparency: `computed_project_column_stops_motion` must fail.
3. Dropped pair tracking (hoist the routed form): `alias_project_residuals_preserve_original_filter_shape`
   must fail.
4. Relocation by proof side instead of ownership side: `or_cross_side_conjunct_demotes_without_moving`
   must fail.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib or_
scripts/cargo-cgroup test  -p toron --lib alias
scripts/cargo-cgroup test  -p toron --lib project
scripts/cargo-cgroup test  -p toron --lib demot
scripts/cargo-cgroup test  -p toron --test architecture_test adr212
scripts/cargo-cgroup test  -p toron --test architecture_test adr209
scripts/cargo-cgroup test  -p toron --test architecture_test adr210
scripts/cargo-cgroup test  -p toron --test architecture_test adr211
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — non-optional
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Hygiene (the bitten-before list)

- No m-word in src/tests (adr043 substring scan).
- Receipts/EXPLAIN are in-memory only — zero goldens expected; a shift is a finding.
- `--all-targets` after any enum/struct growth (the receipt step enum is pattern-matched in tests).
- Plain names, no V1 suffixes; all cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. The derived-table lowering shape (Phase 0.3 — fixes the headline fixture).
2. The wrapper-step representation in the receipt path (Phase 4 — render legibility decides).
3. The guard-file line ceiling (Phase 5 — split if the 211 note fires).
