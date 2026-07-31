# Impl Plan — Toron ADR 218: Receipt-Armed Graph-Kind Margin Application

Pairs with `plans/adr/218_receipt_armed_graph_kind_margin_application.md`.
Consumes the 213/215/217 path; **no new durable key/counter/table/`SystemTableKind`**,
no new config mode. The behavioral payoff is one line at the actuation seam.

Touch surface:
- `binder/join_order.rs` — typed `GraphMarginDivergenceGate` result; typed
  `EffectiveGraphJoinOrderSoftMarginSteps` bundle + `step_for`; arming in the
  `ApplyAdvisory` graph resolver; the `try_bind_graph_actuation` seam.
- `binder.rs` — carry the graph-step bundle through `Binder::new` / clone /
  `without_decision_recording` no-op binders.
- `binder/tests.rs` — behavior net.
- `tests/architecture/optimizer_accrual.rs` — `adr218_*` guards + 217/215 retargets,
  including the 217 static-manifest/readiness guards touched by promoting the
  graph row builder into a shared typed gate result.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Application mode (opt-in) | `JoinOrderSoftMarginApplication::{Manual, ApplyAdvisory}`; `effective_join_order_soft_margin_steps` (join_order.rs:305) |
| 217 gate pieces to promote | `graph_margin_divergence_gate` (:465), `graph_kind_gate_signals` (:505), `GraphKindGateSignal` (:429); floor `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE=20` (:7) |
| Single-graph-step resolver (today) | `effective_graph_join_order_soft_margin_step` (:332) |
| **Actuation seam (the behavioral line)** | `try_bind_graph_actuation` (:2953); `let margin = self.graph_join_order_soft_margin_step.margin();` (**:3017**) |
| Kind selector at the seam | `decision_kind()` (:2842) |
| Binder field + threading | `graph_join_order_soft_margin_step` (binder.rs:457; :483/499/514 clone/no-op) |

## Phase 0 — Red tests first (the seam red is the most valuable)

1. `graph_kind_margin_reaches_graph_actuation_decision` — plan/binder-level: with ≥2 ready divergent kinds, the committed graph actuation for a specific kind reflects that kind's step, not the family step. **Red on main, and red against a resolver-only impl.**
2. `graph_actuator_and_receipt_table_share_gate_result` — readout + actuator agree on actionable + ready steps (D3).
3. `per_kind_graph_margin_inert_until_gate_actionable` — cold-ledger identity pin.

## Phase 1 — Shared typed gate result (join_order.rs)

- `pub(crate) struct GraphMarginDivergenceGate { status: OptimizerReceiptGateStatus, ready: Vec<GraphKindGateSignal> }` (optionally `sample_count`/`reason`).
- Refactor `graph_margin_divergence_gate` to return it; the 217 `optimizer_receipt_gates` row builder becomes a thin wrapper deriving its row from this object (no second threshold). One predicate, one ready set (D3).

## Phase 2 — Typed step bundle + armed resolver (join_order.rs)

- `EffectiveGraphJoinOrderSoftMarginSteps { family, galaxy, bridge, bridge_chain }` + `step_for(kind: OptimizerDecisionKind) -> SoftMarginLadderStep` (debug-assert/`unreachable!` for non-graph kinds). `EffectiveJoinOrderSoftMarginSteps.graph` becomes this bundle.
- `effective_join_order_soft_margin_steps` (`ApplyAdvisory` arm): from the **one** snapshot, resolve tree + graph family as today, then the `GraphMarginDivergenceGate`. If `status != Actionable`, every kind field = family (215-equivalent). If `Actionable`, each ready kind = its `recommended_step`, non-ready kinds = family (D5). `Manual` arm = configured base for all (D1/D2). Single-snapshot discipline (D7).

## Phase 3 — Binder threading (binder.rs)

- `Binder` carries `graph_join_order_soft_margin_steps: EffectiveGraphJoinOrderSoftMarginSteps` (replacing the scalar); thread through `Binder::new`, clone, and `without_decision_recording` (binder.rs:457/483/499/514).

## Phase 4 — The actuation seam (join_order.rs:3017)

- Replace `let margin = self.graph_join_order_soft_margin_step.margin();` with
  `let margin = self.graph_join_order_soft_margin_steps.step_for(shape.decision_kind()).margin();`.
  This is the one behavioral line (D6). Apply smaller render/analyze retargets **only** where a test proves they are needed (don't pre-thread the recognizers if the seam carries it).

## Phase 5 — Behavior net (tests.rs)

The seven discriminators from the ADR — critically the plan-level seam test
(Phase 0 #1), the shared-result pin, and the inert/armed/fallback trio. Plus
`manual_ignores_graph_kind_divergence`, tree byte-identity, Rune/SQL parity.

## Phase 6 — Guards + retargets (optimizer_accrual.rs)

Add the seven `adr218_*` guards. Retarget
`adr217_receipt_gates_are_explicit_five_row_system_table` so the static manifest
still pins five rows while allowing the graph row to wrap
`GraphMarginDivergenceGate`; retarget
`adr217_graph_gate_uses_ready_kind_divergence_not_family_rollup` to inspect the
shared typed gate result rather than a row-builder-only function. Retarget
`adr217_readout_is_read_only_no_policy_consumers` to allow exactly the
`graph_margin_divergence` consumer (row still reported); keep
`adr217_windowing_and_counter_handoff_are_substrate_blocked` unchanged; assert
213/215 readout + tree/family-application guards unchanged. Do the 217 retarget
**last**, once the new consumer is visible.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib graph_kind
scripts/cargo-cgroup test  -p toron --lib join_order
scripts/cargo-cgroup test  -p toron --lib binder
scripts/cargo-cgroup test  -p toron --test architecture_test adr218
scripts/cargo-cgroup test  -p toron --test architecture_test adr217
scripts/cargo-cgroup test  -p toron --test architecture_test adr215
scripts/cargo-cgroup test  -p toron --test architecture_test adr213
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — retargets participate
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Resolver returns distinct fields but the seam still reads the family scalar: `graph_kind_margin_reaches_graph_actuation_decision` fails.
2. Actuator recomputes the ready set instead of consuming the shared gate result: `graph_actuator_and_receipt_table_share_gate_result` fails.
3. Remove the arming check (always per-kind): `per_kind_graph_margin_inert_until_gate_actionable` fails.
4. Route a graph-kind step into tree margins, or let `Manual` read it: the tree/Manual byte-identity guards fail.
5. Add any durable key/counter/table/`SystemTableKind`: `adr218_no_new_durable_*` fails.
6. Make another receipt gate a consumer, or mark windowing/counter-handoff actionable: the 217 retarget/substrate guards fail.

## Hygiene

- No m-word; no version-suffix/product-stage idiom; no council-internal labels in code/promoted text.
- No goldens (planner-internal margin selection; no wire/encoding/durable change) — proof is the behavior net + byte-identity + the plan-level seam test.
- `--all-targets` after the `EffectiveJoinOrderSoftMarginSteps` shape change (widely-constructed struct → an E0063 can hide in a bench/example).
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. The exact `shape` type at join_order.rs:3017 and that `decision_kind()` returns one of the three graph kinds there (it should, since it's the graph actuation path).
2. Whether the 217 row builder can wrap `GraphMarginDivergenceGate` with no behavior change to the readout (it must — 217 stays reported).
3. Whether any clone/no-op binder path resets the bundle to family-only (must thread the real bundle, or the seam silently de-arms).
