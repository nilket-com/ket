# Impl Plan — ADR 215: Graph-Family Margin Advisory Application

Pairs with `plans/adr/215_graph_family_margin_advisory_application.md`. Touch surface: `binder/join_order.rs` (the graph
resolver + report rendering), `binder.rs` (construction-site wiring), tests, guards. No new
config, keys, tables, or modes; no Wendelon; the 213/214 evidence surfaces open only where the
named retargets say.

## Anchors (re-ground line numbers at implementation)

| Concern | Anchor |
|---|---|
| Tree resolver (the twin to mirror) | `effective_tree_join_order_soft_margin_step` join_order.rs (Manual → base; ApplyAdvisory → tree-scoped recommendation) |
| Graph base fn (becomes the resolver) | `graph_join_order_soft_margin_step(config)` join_order.rs — currently returns `config.join_order_soft_margin_step` unconditionally (208-D2) |
| Binder construction (both steps resolved) | binder.rs `Binder::new` — `tree_join_order_soft_margin_step` + `graph_join_order_soft_margin_step` fields (the 208 split; both fields should come from one shared bind-entry snapshot) |
| Graph thin-margin gate (unchanged reader) | `self.graph_join_order_soft_margin_step.margin()` (the 208/213 read site) |
| Scope filter (reused) | `MarginAdvisoryScope::GraphFamily` + `margin_advisory_input` (213); the 214 three-family partition already excludes `PreservedOuterOrder` |
| Floor + hysteresis (the inertness source) | `recommend_soft_margin` + `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE` (the steer's structural guarantee — no new code) |
| Report rendering (D5 retarget) | the 213 `margin_advisory_report_rows` graph knob row — `application_mode`/`applied_margin` hardcoded manual → row-effective |
| Guards to invert/retarget | `adr213_graph_readout_does_not_drive_graph_gate`; the 213 `graph_readout_is_read_only_no_plan_change` behavioral gate; 208's binder-construction string guard; 213's rendering assertion |

## Phase 0 — Behavioral gates first

1. `graph_evidence_changes_near_margin_graph_decision_while_tree_evidence_does_not` — **red on
   main** (the graph gate ignores evidence today). Fixture: the 213 falsification's
   forbidden-wiring scenario, now the desired behavior — seed graph `MissedWin` past the floor
   under `ApplyAdvisory`; a near-margin galaxy actuation flips; identical seeds as tree kinds
   flip nothing.
2. `apply_advisory_with_insufficient_graph_evidence_binds_byte_identical` — **continuity, green
   before implementation and still required after**:
   below-floor seeding and within-hysteresis seeding each bind byte-identical to Manual. Written
   before the resolver exists; must pass before and after.
3. `manual_mode_byte_identical_for_both_families` — continuity, both families in one fixture.
4. `tree_and_graph_evidence_do_not_cross_steer` — both directions, one fixture.
5. `preserved_outer_evidence_steers_neither_family` — seed `preserved_outer_order` rows past the
   floor; neither knob moves.

## Phase 1 — The resolver (join_order.rs)

Add a bind-entry helper that resolves both family steps from one snapshot, e.g.
`effective_join_order_soft_margin_steps(config) -> (tree, graph)`. Manual returns the configured
base for both. ApplyAdvisory reads `config.runtime_evidence.verdict_snapshot()` once, derives the
tree step from `MarginAdvisoryScope::TreeFamily`, and derives the graph step from
`MarginAdvisoryScope::GraphFamily`.

Keep narrow single-family wrappers only where tests/report helpers already need them, but
`Binder::new` must call the shared helper so one bind cannot use tree evidence from one snapshot
and graph evidence from another. The 191 resolve-once comment travels (resolved at bind entry,
never in low-level helpers, never mutating evidence). Rename the graph base fn to
`effective_graph_join_order_soft_margin_step` if a single-family test helper is kept; do not
leave an unconditional-base graph helper with the old name.

## Phase 2 — Wiring (binder.rs)

Construction stores the resolved graph step in the existing field; the thin-margin gate's read
site is untouched (it already reads the field — the 208 split pays off: this cut changes what
the field *holds*, not who reads it). `--all-targets` after the rename.

## Phase 3 — Report rendering (join_order.rs / explain rendering)

The graph knob row renders row-effectively: `application_mode` from config, `applied_margin` =
the graph row's recommendation under `ApplyAdvisory` and the configured base under `Manual`.
Kind-detail rows unchanged (still rendered read-only — pinned). The 213 report's single-snapshot
discipline holds: use the row's already-computed advisory from the report snapshot, not a
bind-entry resolver that would take a second evidence read.

## Phase 4 — Retargets + guards

Behavioral retargets: the 213 `graph_readout_is_read_only_no_plan_change` gate splits into the
Phase-0 gates 2+3 (Manual byte-identity + insufficient-evidence byte-identity — together they
carry the old gate's protection under the new contract). Guard retargets: 213's
`adr213_graph_readout_does_not_drive_graph_gate` inverts to pin the floor-gated consumption;
208's construction-string guard updates for the shared bind-entry resolver; 213's hardcoded-manual
rendering assertion → row-effective. New guards: the `adr215_*` set from the ADR, including the
one-shared-snapshot pin. All other 208/213/214 guards pass unchanged or the design is wrong.

## Named falsifications (for the review)

1. Resolver consumes the unfiltered snapshot (drop the GraphFamily scope):
   `tree_and_graph_evidence_do_not_cross_steer` must fail.
2. Bypass the floor (apply the recommendation below MIN_SAMPLE):
   `apply_advisory_with_insufficient_graph_evidence_binds_byte_identical` must fail — the
   steer's contract is exactly what this edit violates.
3. Wire the kind-detail rows (per-kind application):
   `adr215_kind_detail_rows_remain_non_actionable` + the 213 detail-row gate must fail.
4. Include `PreservedOuterOrder` in the graph scope:
   `preserved_outer_evidence_steers_neither_family` must fail.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib graph
scripts/cargo-cgroup test  -p toron --lib advisory
scripts/cargo-cgroup test  -p toron --lib apply_advisory
scripts/cargo-cgroup test  -p toron --test architecture_test adr215
scripts/cargo-cgroup test  -p toron --test architecture_test adr213
scripts/cargo-cgroup test  -p toron --test architecture_test adr208
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — non-optional
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Hygiene (the bitten-before list)

- No deprecated product-stage or version-suffix terminology; **no council-internal labels in
  promoted text** (gate conditions are written as their referents — the lesson of this very
  cycle).
- No goldens (config-level policy; plans byte-identical under Manual by gate).
- `--all-targets` after the resolver rename; plain names; cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Whether the near-margin fixture can reuse the 213 falsification scenario's catalog verbatim
   (prefer reuse — that scenario was already built to flip on exactly this wiring).
2. Whether any test besides the construction site calls the old
   `graph_join_order_soft_margin_step` name (grep before the rename).
