# Impl Plan - Toron ADR 219: Receipt-Armed Safe-Side Outer Ordering

Pairs with `plans/adr/0219_receipt_armed_safe_side_outer_ordering.md`. Reuses the
214/217 gate + cost evidence and the 218 self-arming + single-snapshot shape;
**builds a new, correctness-critical left-outer-star transform**. No
durable/wire surface; `PreservedOuterOrder` stays verdict-only.

Touch surface:
- `binder/join_order.rs` - typed `PreservedOuterOrderingGate`; `EffectiveJoinOrderPolicy`
  bundle; `recognize_left_outer_star_subset`; `try_bind_preserved_outer_star_actuation`
  beside the star helpers.
- `binder.rs` - resolve the policy bundle once at construction; thread it through
  normal/cloned/no-op binders; call the actuation at the star-binding point.
- `binder/tests.rs` - behavior net (the executing null-miss test is the headline).
- `tests/architecture/optimizer_rotation.rs`, `tests/architecture/optimizer_accrual.rs`,
  and any existing join-order architecture guards that pin the resolver/inner-star
  seams - `adr219_*` guards + 191/208/213/214/215/216/217/218 retargets.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| 214 receipt (evidence only; inner-equi) | `preserved_outer_order_receipt` (binder.rs:409) - `inner_equi = true`; **do not actuate `edge_targets`** |
| 217 gate + advisory recommended step (D7) | `preserved_outer_ordering_gate` (join_order.rs:646) -> `margin_advisory_report_for_scope(PreservedOuterOrder).advisory` (recommendation + recommended_step) |
| Inner-only fences (must NOT relax) | `NonInnerEquiJoin` at join_order.rs:1241/1338/1741 |
| Inner-star actuation to mirror (schema/order restore) | `bind_star_order_sequence` (:3954), `try_bind_graph_actuation` (:3047) |
| 218 single-snapshot resolver to extend | `effective_join_order_soft_margin_steps` (reads one `receipt_gate_snapshot()`) |
| 216 matrix guard (must stay green, unchanged) | `adr216_decision_matrix_is_140_keys_and_evidence_matrix_220` - `!recording_kinds.contains("PreservedOuterOrder")` (optimizer_accrual.rs:295) + `decision_keys.len()==140` (:303) |

## Phase 0 - Red tests first (in this order)

1. `safe_side_left_outer_star_preserves_rows_and_schema` - execute a left-outer star with null-supplied misses; rows + schema equal the declared-order plan. **The headline red; red against any unsafe transform.**
2. `selected_order_uses_left_outer_preflight_not_inner_receipt_targets`.
3. `adr219_join_order_policy_uses_one_receipt_snapshot` (the bundle).
4. `preserved_outer_policy_uses_gate_recommended_step` (D7).
5. inert / manual / fenced / Rune.

## Phase 1 - Policy bundle + typed gate result (join_order.rs)

- `PreservedOuterOrderingGate { status, advisory }` (advisory = the
  `MarginAdvisory` carrying recommendation and `recommended_step`); refactor
  `preserved_outer_ordering_gate` to return it; the 217 row builder becomes a wrapper.
- `EffectiveJoinOrderPolicy { tree_step, graph_steps, preserved_outer_gate }`;
  extend the bind-entry resolver so `ApplyAdvisory` derives all of it from **one**
  `receipt_gate_snapshot()` (D6), while `Manual` constructs the base policy with
  **zero** receipt-gate snapshots. No second policy runtime-evidence read; the
  information-schema report path still reads evidence when the table itself is queried.

## Phase 2 - Left-outer-star recognition + decline fences (join_order.rs)

`recognize_left_outer_star_subset(children) -> Option<LeftOuterStarCandidate>`:
accept **only** the D2 subset (one common left anchor; all other edges `LeftOuter`
base-dimension leaves; equi `A.k_i=D_i.k_i` only; no cross-dimension or
nullable-residual predicate). Return the dimension edge list. **Decline** (return `None`)
for full/right/mixed outer, snowflake/bridge/galaxy, composite-build, residual
predicate - caller keeps declared order.

## Phase 3 - Actuation (join_order.rs, near the star helpers)

`try_bind_preserved_outer_star_actuation`:
- consume the bundle's `preserved_outer_gate` (do **not** re-snapshot, do **not**
  read info-schema);
- recognize the subset; if `status == Actionable` AND reorder win > `advisory.recommended_step` margin (D7):
  rank dimensions with the same estimate inputs/ranking machinery used for the 214
  counterfactual cost signal, but do **not** call `preserved_outer_order_receipt`,
  consume `edge_targets`, or flip the live edges to inner; build a real
  `JoinKind::LeftOuter` sequence, preflight declared + selected as outer joins,
  restore declared output schema/column order (mirror inner-star restore);
- otherwise return Decline; caller binds today's preserved order unchanged.
- **No decision accrual** for this actuator: no `accrue_outer_order_decision`, and
  no reuse of `accrue_star_order_receipt` / `accrue_star_order_decline` under
  `FlatStar`/`Snowflake*` decision kinds (D8 / 216).

## Phase 4 - Bind wiring (binder.rs)

Resolve `EffectiveJoinOrderPolicy` once at `Binder` construction; thread through
`new`, clone, and `without_decision_recording`. Call the actuation at the
star-binding point; fall through to declared-order on Decline.

## Phase 5 - Behavior net (tests.rs)

The nine discriminators from the ADR, headlined by the executing null-miss
result-equivalence test and the inner-targets-not-used test; inert/armed/fenced
trio; manual byte-identity; shared-gate-result; recommended-step; Rune/SQL parity.

## Phase 6 - Guards + retargets

Add the `adr219_*` guards, including a guard that the outer actuator does not call
the inner `accrue_star_order_*` helpers. Retarget
`adr217_readout_is_read_only_no_policy_consumers` -> exactly two consumers
(graph_margin_divergence + preserved_outer_ordering); the 217 preserved-outer gate
guard -> typed result. Retarget existing `optimizer_accrual.rs` guards that
`first_block` the old policy resolver or assert binder fields by name, plus the
ADR218 rotation guards that expect `graph_margin_divergence` to be the only resolver
consumer. **Assert `adr216_decision_matrix_is_140_keys_and_evidence_matrix_220`
unchanged** (load-bearing). Narrow ADR 214 no-actuation wording to this policy only;
keep 214 opportunity-class/replay-id/matrix and 218 graph guards unchanged.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib preserved_outer
scripts/cargo-cgroup test  -p toron --lib safe_side
scripts/cargo-cgroup test  -p toron --test wendelon_storage_execution_test preserved_outer   # executes null-miss rows
scripts/cargo-cgroup test  -p toron --test architecture_test adr219
scripts/cargo-cgroup test  -p toron --test architecture_test adr217
scripts/cargo-cgroup test  -p toron --test architecture_test adr218
scripts/cargo-cgroup test  -p toron --test architecture_test adr191
scripts/cargo-cgroup test  -p toron --test architecture_test adr216
scripts/cargo-cgroup test  -p toron --test architecture_test adr214
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary; retargets participate
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Actuate the 214 inner-equi `edge_targets`: the executing null-miss test drops rows and fails.
2. Accept a full/right/mixed outer or cross-dimension/residual-predicate shape: the fenced-shapes test fails.
3. Record `PreservedOuterOrder` in the decision lane: `adr216_decision_matrix_is_140_keys_and_evidence_matrix_220` fails.
4. Reuse `accrue_star_order_*` from the outer actuator: `adr219_outer_actuation_does_not_record_inner_decisions` fails.
5. Keep the base margin after a Loosen recommendation: `preserved_outer_policy_uses_gate_recommended_step` fails (armed-yet-inert).
6. Take a second evidence snapshot in policy: `adr219_join_order_policy_uses_one_receipt_snapshot` fails.
7. Recompute the gate in the actuator: the shared-gate-result guard fails.
8. Relax the inner-star recognizer to admit outer edges: the inner-fence + left-outer-star-only guards fail.
9. `Manual`/cold ledger reorder or snapshot read: the byte-identity guard fails.

## Hygiene

- No m-word; no version-suffix/product-stage idiom; no council-internal labels in code/promoted text.
- No goldens (planner-internal reorder; no wire/encoding/durable change) - proof is the **executing** null-miss result-equivalence test + the behavior net.
- `--all-targets` after the policy-bundle/shared-struct shape changes.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Whether the inner star candidate collector can be reused with a left-outer filter or needs a parallel collector (the inner path may assume `JoinKind::Inner` deep down).
2. The exact output-order restore seam for an outer reorder (inner-star restore may assume inner row shape).
3. A left-outer star with a predicate on a dimension column (not just the equi key) must **decline** (it can null-reject, so it is unsafe); default to decline if uncertain.
4. Whether to keep compatibility wrappers for `effective_join_order_soft_margin_steps`
   or retarget every existing architecture guard to `EffectiveJoinOrderPolicy`; do not
   rely on the new `adr219_*` filter alone.
