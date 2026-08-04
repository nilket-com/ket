# ADR 186: Recursive Hash-Snowflake Branch Actuation with Edge-Owned Star Receipts

**Status:** Promoted. **Migration:** `plans/impl/0186_recursive_hash_snowflake_branch_actuation.md`.

185 reordered a uniform-hash snowflake's **root**, building branch composites in declared
order. 186 finishes the safe hash-snowflake actuation story: a **bushy branch** — a branch
that is itself a local flat star with at least two sub-dimensions — can reorder recursively,
restore its local declared output, and then appear to its parent as the same composite branch
185 already knows how to join.

The enabling design is **edge-owned star receipts**. Recursive actuation can produce multiple
local star decisions in one plan: root `orders` may reorder `{stores, customers}` while branch
`customers` reorders `{segments, regions}`. The current render trace has one star slot and one
snowflake slot:

```rust
join_order: Option<StarOrderOutcome>,
snowflake_order: Option<SnowflakeOrderEvidence>,
```

That shape can lose branch evidence, clobber root evidence, or represent a plan as both an
acted star tree and an observe-only snowflake. 186 replaces it with an exclusive render state:

```rust
enum JoinOrderRenderState {
    None,
    StarReceipts(Vec<StarOrderRenderReceipt>),
    ObservedSnowflake(SnowflakeOrderEvidence),
}
```

A plan is either a set of acted/recognized star levels or an observe-only snowflake. It cannot
be both.

## Grounding

- **185 created the branch seam.** `try_bind_star_order_actuation` already partitions a
  left-deep candidate and builds branch composites through `bind_declared_star_branch_dimension`.
  That helper is the deferred switch point.
- **Only bushy branches can reorder.** A chain branch like `customers -> regions` has one child
  and no local ordering choice. A branch like `customers -> {regions, segments}` is a local
  flat star and can spend the 181/185 actuator.
- **Restores compose by plan nesting.** `restore_declared_star_order_project` restores one
  local star level by segment label and width. A reordered branch restores itself before its
  parent sees it, so the parent receives one stable composite segment. No special nested
  restore helper is needed.
- **The existing star tail is the right actuator.** `preflight_star_order_strategy`,
  `bind_star_order_sequence`, `restore_declared_star_order_project`,
  `star_output_segments_for_order`, and `recognized_star_order_outcome` already implement the
  local decision. 186 factors and reuses them rather than adding a snowflake-specific reorder.
- **Synthetic branch-plan recursion is avoidable.** One draft proposed synthesizing a branch
  `LogicalPlan::Join` and sending it back through `bind_plan_with_trace` so the normal join arm
  re-enters `try_bind_star_order_actuation`. That reuses code, but it also re-enters unrelated
  binder flow and makes scratch-vs-real receipt ownership fragile. A private local-star helper
  over already-bound inputs is the more controlled reuse.
- **Runtime verdicts need edge ownership.** `optimizer_analyze_rows` currently spends all
  global `join_order_edges` against one `StarOrderEvidence`. Multiple local stars need each
  receipt to own the `JoinTraceTarget`s it bound, so branch verdicts cannot spend root replay
  IDs and root verdicts cannot spend branch replay IDs.
- **Broadcast remains the execution cliff.** `rewrite_broadcast_star_probe` still assumes a
  scan-like published build path. 186 stays binder/explain-only and hash-only.

## Decisions

**D1 - Make join-order render state exclusive.** Replace independent star/snowflake optional
fields with `JoinOrderRenderState::{None, StarReceipts, ObservedSnowflake}`. A root-declined
snowflake can replace a declined-only star candidate with `ObservedSnowflake`, but a recognized
star-receipt tree and observe-only snowflake evidence cannot coexist.

**D2 - Make star receipts plural and edge-owned.** Add:

```rust
struct StarOrderRenderReceipt {
    probe_label: String,
    outcome: StarOrderOutcome,
    edge_targets: Vec<JoinTraceTarget>,
}
```

`probe_label` gives stable cockpit labels such as `star[probe=o]` and `star[probe=c]`.
`edge_targets` is the load-bearing runtime/verdict ownership.

**D3 - Extract a private local-star actuator.** Factor the 181/185 actuation tail into a helper
that accepts an already-bound probe, already-built local dimensions, a probe label, and
trace/render sinks. The helper computes evidence, preflights declared and selected orders,
requires uniform `HashPartitioned` for snowflake-tree actuation, binds the selected order, and
restores local declared output when reordered. It returns the restored plan plus an edge-owned
receipt. It does not accept arbitrary logical plans.

**D4 - Build an acyclic snowflake tree by relation-label parentage.** Replace 185's
single-level `member_indices` partition with a tree over the existing left-deep candidate.
Fact-sourced dimensions are root children; dimension-sourced dimensions attach under their
parent dimension. Duplicate labels, shared parents, cycles, unresolved labels, non-inner/non-equi
joins, and non-tree shapes decline. This is still single-fact snowflake recognition, not galaxy
or bridge recognition.

**D5 - Recurse bottom-up; restores compose.** Build child branches first. A local level with
fewer than two children binds declared and emits no local receipt. A bushy level with at least
two children calls the local-star actuator. If it reorders, it immediately restores to its
local declared shape. The parent receives that restored branch as one composite dimension. No
`restore_declared_snowflake_order_project`.

**D6 - Root commit gates recursive actuation.** Recursive branch decisions are first planned in
scratch. If the root does not produce a recognized hash star receipt, discard branch receipts
and return `Ok(None)` so the existing declared bind plus 182-184 observe-only snowflake path
runs. If the root is recognized hash, rebind for real and commit the root and branch star
receipts. A branch-local decline inside a committed root keeps that branch declared but does
not force the root to decline.

**D7 - Keep snowflake actuation hash-only.** Every acted snowflake level must classify as
uniform `HashPartitioned` under the existing dual-preflight. Broadcast or mixed branch levels
bind declared locally. Broadcast snowflake execution remains deferred.

**D8 - Render and analyze per receipt.** `EXPLAIN` renders one existing `join_order` row per
star receipt. `EXPLAIN ANALYZE` iterates recognized receipts, filters `join_order_edges` by the
receipt's `edge_targets`, emits runtime rows for those edges only, and emits one existing
5-outcome star verdict per recognized local level. Declined receipts may appear in `EXPLAIN`;
analyze remains recognized-only as today.

**D9 - Preserve result shape and row semantics.** Every local restore returns its branch to the
local declared schema and relation scope before parent binding. Final schema, column order, and
row multiset match declared-order execution.

**D10 - Binder/explain only.** No executor, `fragment.rs`, `rewrite_broadcast_star_probe`, PBC,
storage, Wendelon, catalog, cost model, optimizer shell, memo, Cascades, or broad
`PlanProperties` contract.

## Headline Gates

- `hash_snowflake_branch_emits_edge_owned_star_receipt` - a snowflake with
  `orders -> {customers -> {regions, segments}, stores}` reorders both the bushy `customers`
  branch and the root, emitting two edge-owned `StarReceipts` with disjoint `edge_targets`
  (branch `[0,1]` bound bottom-up, root `[2,3]`) and applied orders `[g,r]` / `[s,c]` — the
  edge-ownership and bottom-up recursion proof.
- `hash_snowflake_reorder_is_results_identical` - the recursively actuated bushy snowflake
  (`non_optimal_snowflake_order_project_sql`) returns the declared schema/column order and the
  same row multiset as the declared-order baseline, executed via `execute_hash_join` — the
  composed-restore correctness hinge.
- `root_decline_keeps_observed_snowflake_state` - a mixed/broadcast root discards the bushy
  `customers` branch receipts with the root and falls back to the 182-184 `ObservedSnowflake`
  path (no committed `StarReceipts`).

## Supporting Gates

- `deep_snowflake_actuates_root_and_keeps_unary_branches_receiptless` - a chain snowflake
  (one-child branches) actuates only the root; unary branches emit no local receipt (185
  behavior preserved).
- the 177-181 `star_order` suite stays green: a flat star remains one `StarReceipts` entry with
  byte-identical target/outcome/detail.
- the 185 `hash_snowflake` suite stays green: one-level root actuation stays one receipt with no
  observed-snowflake rows.
- the 182-184 `snowflake` observational suite stays green when the root declines.
- `rune_hash_snowflake_root_reorders_same_as_sql` - Rune shares the binder/explain receipt path
  (recursive-specific Rune coverage deferred; the shared path is exercised by the SQL gates).

## Architecture Guards

The four shipped guards consolidate the planned set:

- `adr186_join_order_render_state_is_exclusive` - the render trace carries one
  `JoinOrderRenderState` (`None` / `StarReceipts` / `ObservedSnowflake`); star receipts and
  observed-snowflake evidence cannot coexist (subsumes the impossible-state proof).
- `adr186_recursive_branch_actuation_is_bottom_up_and_edge_owned` - branches actuate bottom-up
  and each `StarOrderRenderReceipt` is plural and owns its `Vec<JoinTraceTarget>`; runtime/verdict
  rendering spends only receipt-owned edges, not all global edges (subsumes
  star-receipts-are-plural-and-edge-owned + analyze-spends-only-owned-edges).
- `adr186_root_hash_gate_commits_or_discards_branch_receipts` - a recognized hash root commits
  root + branch receipts; a declining root discards branch receipts and returns to the
  observe-only path (D6 atomicity).
- `adr186_branch_helpers_do_not_rebind_synthesized_logical_joins` - recursive branch building
  uses private local-star helpers over already-bound inputs (reusing the 181 strategy preflight,
  sequence binder, and declared-order restore — no nested-restore helper), not synthesized
  `LogicalPlan::Join`s through `bind_plan_with_trace` (subsumes local-actuator-reuses-181,
  restores-compose-without-nested-helper, and private-helpers-not-synthesized-binder).

D10's binder/explain-only scope (no executor / `fragment.rs` / `rewrite_broadcast_star_probe` /
cost-model / Wendelon / optimizer-shell change) is enforced by the cut touching only
`binder.rs`, `binder/explain.rs`, `binder/join_order.rs`, and tests — verifiable in the staged
diff — rather than a dedicated `adr186_` no-change guard.

## Rejected Alternatives

**Single receipt slot.** Rejected because it loses branch evidence or clobbers root evidence.

**Independent `star_order_receipts` plus `snowflake_order` options.** Better than the single
slot, but still representationally permits the invalid state this ADR exists to avoid. Use an
exclusive render-state enum.

**Synthetic branch `LogicalPlan` recursion through `bind_plan_with_trace`.** Rejected for the
implementation plan. It reuses the top-level join arm, but it also re-enters unrelated binder
flow and makes scratch-vs-real receipt ownership fragile. Extract the local actuator and call
it directly over bound inputs.

**Bespoke nested restore.** Rejected. Local restores compose by plan nesting.

**Branch-only actuation when the root declines.** Rejected. It creates a hybrid cockpit state.
Root recognition is the activation boundary.

**A new recursive snowflake verdict.** Rejected. Acted levels are star decisions and already
use the 179/180 five-outcome verdict. The 184 snowflake verdict remains observe-only.

**Broadcast snowflake actuation.** Rejected until the fragmenter can publish composite builds
safely.

## Non-Goals

- no broadcast-snowflake execution relaxation;
- no mixed-strategy cost model;
- no galaxy, bridge, many-to-many, or cyclic recognition;
- no always-on or persisted verdict accrual;
- no executor, fragment, storage, PBC, wire, catalog, Wendelon, or cost-model change;
- no optimizer shell, memo, Cascades, or broad `PlanProperties`.

## Consensus Trail

The council converged on recursive bushy-branch actuation plus plural edge-owned star receipts.
Claude's strongest contribution was the restore insight: no special nested restore is needed,
because each local `restore_declared_star_order_project` composes by plan nesting. Gemini and
Codex converged on extracting the local-star helper over already-bound inputs. Codex promoted
the stricter render-state form, aligned with the operator's "make illogical states
unrepresentable" steer: a plan cannot carry both acted star receipts and observe-only snowflake
evidence.

## Forward Arc

`182 evidence -> 183 runtime -> 184 verdict -> 185 root hash-snowflake actuation -> 186
recursive hash-snowflake branch actuation + exclusive edge-owned receipts -> workload verdict
accrual -> broadcast-snowflake execution cliff -> galaxy/multi-fact -> bridge/many-to-many ->
cyclic caps -> memo only when receipts show decisions fighting`.

## Deferred (named)

Session/workload verdict accrual; broadcast-snowflake execution cliff
(`rewrite_broadcast_star_probe` with composite published builds); mixed-strategy snowflake
ordering; galaxy / bridge / cyclic; always-on telemetry; memo.
