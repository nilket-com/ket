# Impl Plan — Toron ADR 221: Safe-Side Outer Ordering Surface Completion

Pairs with `plans/adr/0221_safe_side_outer_ordering_surface_completion.md`. Extends
219's `try_bind_preserved_outer_star_actuation` recognizer to two more provably-safe
shapes; **no durable/wire change, no memo, no no-memo-fence touch**. The payoff is
widening a proven receipt-armed actuator; the risk surface is the snowflake opacity
proof, gated by executing null-miss tests.

Touch surface:
- `binder/join_order.rs` — generalize `extract_left_outer_star_join_candidate`
  (right-outer mirror + opaque left-outer sub-tree dimensions); introduce a
  preserved-outer dimension descriptor that carries the exported root label/key for
  multi-source sub-trees; extend the accept/decline fences; reuse the actuation +
  schema restore + shared gate.
- `binder/tests.rs` — behavior net (executing null-miss tests for the new shapes).
- `tests/architecture/optimizer_rotation.rs` — `adr221_*` guards + the 219 retarget.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| 219 recognizer (base-leaf, left-only) to generalize | `extract_left_outer_star_join_candidate` (join_order.rs:1358); `left_outer_star_base_leaf_label` (:1397) |
| Current 219 base-leaf assumptions that must be retired only for opaque sub-trees | dimension binding uses `scope.unique_source_label()` (:3310/:3394) and `bound_left_outer_star_dimension` rejects `composite_build` (:4088) |
| 219 actuator + restore + preflight to reuse | `try_bind_preserved_outer_star_actuation` (:3271); `preflight_left_outer_star_order_strategy` / `bound_left_outer_star_dimension` (:3322/:3358/:4083) |
| Gate + cost signal (unchanged) | `preserved_outer_ordering` gate; 214 counterfactual as cost-only |
| Strict first-match dispatch (extend subset, not add path) | binder.rs:1255–1299 |
| 219 guard to retarget | `adr219_outer_actuation_is_left_outer_star_only` (optimizer_rotation.rs:214) |
| No-memo fences (must stay unchanged) | `optimizer_rotation.rs:449`; `join_order.rs:146/159/1461/1474`; `adr197_..._not_generic_memo` |

## Phase 0 — Red tests first (the executing proofs are the headline)

1. `safe_side_left_outer_snowflake_preserves_rows_and_schema` — execute `A LEFT JOIN (B LEFT JOIN C) …` with null misses + a multi-match; multiset + schema == declared-order. Red on main.
2. `safe_side_right_outer_star_preserves_rows_and_schema` — mirror.
3. `within_subtree_reorder_stays_fenced`.
4. `existing_left_outer_base_leaf_stars_bind_byte_identical` — the generalized extractor doesn't perturb 219's stars.

## Phase 1 — Recognizer generalization (join_order.rs)

Generalize candidate extraction to accept: (i) **right-outer star** (preserved anchor
on the right — transpose 219's side checks and canonicalize to a preserved-anchor
probe + dimension list); (ii) a top-level dimension that is an **opaque left-outer
sub-tree** (root-keyed equi-join to the anchor; internals not inspected for reorder).
Keep 219's base-leaf path as the simple case (assert byte-identical). For the opaque
sub-tree path, do **not** rely on `unique_source_label()` for the whole dimension
plan; carry the exported root label/key from recognition into binding, and allow a
composite dimension only when that root-key opacity proof succeeds. **Decline fences
(extend 219's):** full-outer; right/left mixed chains; right-outer opaque snowflake;
within-sub-tree reorder; a predicate referencing a non-root sub-tree-internal column
from the anchor level or spanning two dimensions; nullable-residual. Default to
decline on any uncertainty (safety bias).

## Phase 2 — Actuation + restore (join_order.rs)

Reuse `try_bind_preserved_outer_star_actuation`: rank top-level dimensions (sub-trees
opaque) with the existing cost signal; build a canonical preserved-anchor outer
sequence (left-outer physical form is acceptable for right-outer stars if declared
schema/column order is restored); preflight declared + selected as outer joins;
restore declared schema/column order. Same opt-in + gate + single snapshot.

## Phase 3 — Behavior net (tests.rs)

The discriminators from the ADR — both executing null-miss equivalence tests
(snowflake + right-outer), the within-sub-tree fence, full-outer / cross-into-sub-tree
fences, inert-until-actionable, manual byte-identity, the 219-stars-unchanged pin,
inner-targets-not-used continuity, Rune parity.

## Phase 4 — Guards + the 219 retarget

Add the `adr221_*` guards. **Retarget `adr219_outer_actuation_is_left_outer_star_only`**
to the extended subset (left/right star + opaque-sub-tree snowflake), keeping the
within-sub-tree / full-outer / cross-dimension fences. **Assert the no-memo fences
unchanged** (`adr221_no_memo_fences_unchanged`) and 216/217/218 unchanged.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib safe_side
scripts/cargo-cgroup test  -p toron --lib preserved_outer
scripts/cargo-cgroup test  -p toron --test wendelon_storage_execution_test preserved_outer  # executes null-miss rows
scripts/cargo-cgroup test  -p toron --test architecture_test adr221
scripts/cargo-cgroup test  -p toron --test architecture_test adr219
scripts/cargo-cgroup test  -p toron --test architecture_test adr197   # no-memo fence still holds
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — the 219 retarget participates
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Permute a sub-tree's internal order: `within_subtree_reorder_stays_fenced` + the executing snowflake test fail.
2. Accept a full-outer or cross-into-sub-tree-predicate shape: the fenced-shapes test fails.
3. Perturb existing left-outer base-leaf stars: `existing_left_outer_base_leaf_stars_bind_byte_identical` fails.
4. Reorder under `Manual`/cold: byte-identity fails.
5. Introduce `memo`/`Cascades`/`src/optimizer.rs`: `adr221_no_memo_fences_unchanged` + the existing no-memo guards fail.
6. Reuse the 214 inner-equi targets as the live transform: the inner-targets-not-used guard fails.
7. Add a durable key / change 140/220: the 216 guard fails.

## Hygiene

- No m-word; no version-suffix idiom; `Cascades`/`memo` stay out of source (deferred).
- No goldens (planner-internal reorder) — proof is the executing null-miss tests.
- `--all-targets` after any candidate-struct shape change.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. The exact sub-tree opacity check (root-keyed equi-join to anchor; no anchor-level predicate references a non-root sub-tree-internal column) — default decline if uncertain.
2. Replacing the current `unique_source_label()` / `composite_build` base-leaf assumptions with an exported-root descriptor for opaque sub-trees, without weakening the base-leaf path.
3. Right-outer schema-restoration symmetry (219's restore may assume left-anchor row shape).
4. That the generalized extractor cleanly subsumes 219's base-leaf path (byte-identical on the 219 fixtures).
