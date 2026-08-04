# Impl Plan — ADR 206: Cross-Shape Joint Actuation

Pairs with `plans/adr/0206_cross_shape_joint_actuation.md`. Verdict-log-only (plans + EXPLAIN tree receipts
byte-identical). The new mechanism is the reversed-evidence construction with reversed-plan targets; the
boundary fix is a gate generalization. All anchors verified.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Stale XOR gate | `measured_verdicts_from_summaries` (explain.rs:549) — `applied_galaxy_root \|\| applied_bridge_root`, chain omitted |
| Applied-root fields | `applied_galaxy_root` / `applied_bridge_root` / `applied_bridge_chain_root` (explain.rs) → add `applied_graph_root()` accessor |
| Chain verdict arms (suppressed when actuated) | `measured_verdicts_from_summaries` chain arm + the EXPLAIN ANALYZE row in `bridge_analyze_rows` — gated on `applied_bridge_chain_root.is_none()` (205) |
| Reversed plan edges | `render_trace.join_order_edges` after the re-bind (the reversed targets) |
| Evidence to reverse | `BridgeChainEvidence` / `BridgeLink` (`fact`/`dim`, `b_in/out_signature`, `in/out_edge_rows`, `in/out_target`) |
| Verdict classifier (reused) | `measured_graph_order_verdict` + `bridge_chain_measured_costs` |
| Actuator restore site | `try_bind_graph_actuation` (join_order.rs) — where 205 carries the scratch chain |

## Phase 1 — D1: partition-complete gate

`explain.rs`: add `OptimizerExplainRenderTrace::applied_graph_root(&self) -> Option<&str>` returning the first
of `applied_galaxy_root` / `applied_bridge_root` / `applied_bridge_chain_root` that is set; replace the
`graph_actuated` expression with `self.applied_graph_root().is_some()`. Exhaustive by construction — a new
shape adds one arm here, not a forgotten OR term.

## Phase 2 — D2: reversed evidence + actuated chain verdict

`join_order.rs`: `reverse_chain_evidence(chain, reversed_edges) -> Option<BridgeChainEvidence>` — reverse the
hop order, swap each hop's `fact/dim` + `in/out` signature + `in/out_edge_rows`, and resolve `in/out_target`
by matching the hop's `(probe, dimension)` relation labels against the reversed plan's `join_order_edges`.
In `try_bind_graph_actuation`, when `shape == Chain` actuated, set the **reversed** evidence (not the scratch
AST evidence 205 carried) via `set_observed_bridge_chain`, so the receipt renders the reversed path AND the
verdict keys to reversed measured costs.

`explain.rs`: in the chain verdict arms (`measured_verdicts_from_summaries` + `bridge_analyze_rows`), emit the
verdict for an actuated chain too — the evidence now carries reversed targets, so the 205 suppression
(`applied_bridge_chain_root.is_none()`) is removed for the chain *verdict* (the bridge-verdict and orphan-tree
suppressions stay). Kept (observe) chains unchanged.

## Phase 3 — tests + guards

`binder/tests.rs` (reuse the 204/205 `three_hop_bridge_chain_catalog` + selective variant):

| test | locks | fails under |
|---|---|---|
| `actuated_chain_verdict_vindicates_applied_root` | the actuated chain is judged (applied order) | `main` (205 suppresses it) |
| `actuated_chain_emits_no_orphan_tree_verdict` | the partition-complete gate (D1) | the galaxy/bridge-only gate |
| `actuated_chain_verdict_uses_reversed_plan_targets` | reversed targets resolve to executed joins + the verdict reads the reversed declared cost | a broken `reverse_chain_evidence` (unresolved → Indeterminate) |
| kept-chain regression (existing `chain_verdict_*`) | non-actuated chains unchanged | any drift |
| galaxy/bridge actuated-verdict suites (existing) | the gate generalization reproduces old behavior | a regressed `applied_graph_root` |

`tests/architecture/join_order.rs`:
- `adr206_graph_actuated_is_partition_complete`: the gate uses `applied_graph_root()` (not a galaxy/bridge-
  only OR); the accessor reads all three applied-root fields.
- `adr206_actuated_chain_verdict_uses_reversed_evidence`: `reverse_chain_evidence` exists and resolves targets
  from the reversed edges; the actuated path sets reversed (not scratch-AST) evidence.

## Verification gate (one filter per command)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib chain
scripts/cargo-cgroup test  -p toron --lib galaxy        # + snowflake/bridge/star (verdict behavior intact)
scripts/cargo-cgroup test  -p toron --test architecture_test adr206
scripts/cargo-cgroup test  -p toron --test architecture_test adr205
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check ; git diff --check
```

## Risks

- **Reversed target re-matching** — resolving `in/out_target` against the reversed edges by label must be
  exact (a mis-match keys the verdict to the wrong join); pinned by `actuated_chain_verdict_uses_reversed_
  plan_targets` (every reversed target must be an executed replay id, and exploding the reversed declared-end
  must fault the reversal). **Finding:** a pure AST-vs-reversed *outcome* discriminator is not behaviorally
  achievable — the AST and reversed declared ends use the same plan indices `{0,1}`, and the executed plan is
  the reversed one, so both keyings read the same measured summaries (they differ only in the declared-root
  *label*). So the test locks target-resolution soundness + reversed-cost response, not AST-outcome-divergence.
- **Gate generalization regressions** — `applied_graph_root()` must reproduce the galaxy/bridge suppression
  exactly for the existing cases; pinned by the unchanged galaxy/bridge actuated-verdict suites.
- **Receipt vs verdict evidence** — D2 keeps the AST evidence on `render_trace` for the receipt (the
  `chain[F->…->D]` path label is unchanged, confirmed by 205's reroot test still green) and builds the
  reversed evidence ONLY inside the verdict arms, so the verdict keys correctly while the receipt is intact.
