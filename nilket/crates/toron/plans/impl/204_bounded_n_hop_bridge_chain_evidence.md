# Impl Plan — ADR 204: Bounded N-Hop Bridge Chain Evidence

Pairs with `plans/adr/204_bounded_n_hop_bridge_chain_evidence.md`. **Observe-only**: execution plans +
EXPLAIN tree receipts byte-identical to `main`. The cut generalizes the 203 recognizer + compounded verdict
from exactly-2 to bounded-N; the recommendation and the telemetry union are already N-hop-ready. All anchors
verified against the tree.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Recognizer to generalize | `analyze_bridge_chain` (join_order.rs:1422) — `links.len() != 2` → bounded connected walk |
| Per-junction links source | `analyze_bridge` already returns **k links** for a k-hop chain (one per junction) |
| Recommendation (already N-hop) | `bridge_chain_order_recommendation` (join_order.rs:1561) — reads only `hops.first()/last()`; untouched |
| Cost model to generalize | `bridge_chain_measured_costs` (explain.rs:1014) — peak over **one** middle → **all** middles |
| End-vector invariant | `bridge_chain_measured_costs` returns exactly **two** labelled costs for any N → `measured_graph_order_verdict` stays a 2-way argmin |
| Path label to generalize | `bridge_chain_order_target_label` (explain.rs:1568) — drops interior bridges → full path |
| Verdict seam (unchanged shape) | `measured_verdicts_from_summaries` chain arm + `measured_graph_order_verdict` (explain.rs) |
| Telemetry union (already N-hop) | `star_order_runtime_replay_ids` (binder.rs) unions `chain.hops.iter().flat_map(...)` — untouched |
| Guard that must travel | `adr203_bridge_chain_recognizer_is_bounded_two_hop` asserts `links.len() != 2` → retarget to 204 |

## Phase 1 — D1: bounded N-hop recognizer

`join_order.rs`, `analyze_bridge_chain`:
- `const MAX_BRIDGE_CHAIN_HOPS: usize = 4;` — the **supported breadth** for this cut (validated breadth =
  one hop past the reachable 3-hop fixture). It does not make the walk finite; the walk is already
  structurally finite because it consumes distinct relations and declines on repeats/disconnects.
- Replace `if links.len() != 2` with `if links.len() < 2 || links.len() > MAX_BRIDGE_CHAIN_HOPS { None }`.
- Head: the unique link whose `fact` is in no link's `bridge` set (else `None`).
- Walk: index links by `fact` (collision → branching → `None`); from the head follow
  `by_fact[current.bridge]`, asserting `next.fact == current.bridge && current.dim == next.bridge`, until no
  successor. Require `order.len() == links.len()` (consumed all → single connected chain; a disconnected
  second component declines) and `k+2` distinct relations (no cycle). `hops = order.map(|i| links[i].clone())`.

The 2-hop path is the `k=2` instance — the same `hops` vector as 203, so D2/D3/telemetry stay byte-identical
for k=2.

## Phase 2 — D2/D3: full-path label + compounded-over-all-middles verdict

`explain.rs`:
- `bridge_chain_order_target_label`: build `chain[{head.fact}(->{hop.bridge})*->{tail.dim}]` over all hops
  (currently hardcodes head/tail only).
- `bridge_chain_measured_costs`: the inter-bridge middles are every **non-tail** hop's `outgoing_target` /
  `outgoing_edge_rows`. Declared (ran) peak = `max` over `measured(head.incoming_target)` ∪
  `measured(hop.outgoing_target)` for each non-tail hop. Counterfactual (un-run) peak = `max` over
  `estimated(tail.outgoing_edge_rows)` ∪ `estimated(hop.outgoing_edge_rows)` for each non-tail hop.
  `peak_all` is `None` if any term is `None` (conservative — a chain can't be judged without all its
  middles). Still returns **exactly two** `(label, cost)` entries → the verdict stays a 2-way argmin. `k=2`
  reduces to the exact 203 expression (byte-identical).

## Phase 3 — D4: reachable fixture + tests

`binder/tests.rs`:
- `three_hop_bridge_chain_catalog()` — 5 relations `orders→order_lines→line_promotions→promo_tiers→
  promotions`, cardinalities monotonic-increasing along `F→B1→B2→B3` (e.g. `1_000 / 5_000 / 20_000 /
  80_000`, dim `promotions` small) so each junction passes the inverse-F1 gate (`bridge_rows >= fact_rows`)
  and `analyze_bridge` yields 3 stitching links.
- Tests:

| test | locks | fails under |
|---|---|---|
| `three_hop_chain_is_recognized` (EXPLAIN) | bounded walk accepts k=3, full-path label | 203 recognizer (`len()!=2` → no row) |
| `three_hop_chain_interior_middle_explosion_is_missed_win` (ANALYZE/sampled) | peak folds **all** middles | 2-hop cost model (misses `B2⋈B3`) |
| `branching_bridge_chain_is_declined` | the walk rejects a fork | a links-count recognizer |
| `chain_plus_unrelated_bridge_declines_chain` | single-connected-chain rule (disconnected declines) | a links-count recognizer |
| `over_cap_chain_is_declined` | the claim cap bounds the shape | an uncapped walk |
| `two_hop_chain_row_and_verdict_unchanged` | k=2 byte-identical | any drift in the 203 instance |
| 194–203 EXPLAIN + execution suites byte-identical | observational purity (D5) | any plan/receipt drift |

The interior-explosion test targets the *interior* middle via `chain.hops[1].outgoing_target.join_index`
(the `B2⋈B3` replay), entries small — proving the peak folds middles beyond the head's. Revert-verify it
fails two ways (recognizer capped at 2 → no row; cost model head-only → `Vindicated`).

## Phase 4 — guards (retarget 203 → 204)

`tests/architecture/join_order.rs`:
- **Retarget** `adr203_bridge_chain_recognizer_is_bounded_two_hop` → `adr204_bridge_chain_recognizer_is_
  bounded_n_hop`: assert **structural termination** (distinct-relation walk, branching/cycle/disconnected
  declines via `order.len() == links.len()`) **and** the `MAX_BRIDGE_CHAIN_HOPS` claim cap as *separate*
  facts; drop the `links.len() != 2` assertion. (Continuous refactoring travels with the surface this cut
  owns.)
- New `adr204_chain_verdict_folds_all_middles`: `bridge_chain_measured_costs` iterates the non-tail hops
  (not just `head.outgoing_target`), names the `max`/peak combinator, and returns a 2-entry end vector
  (the N-invariant).
- `adr203_bridge_chain_is_observe_only` / `_runs_at_bind_time` / `_verdict_reuses_graph_machinery` stay
  (still true) — re-verify they pass unchanged.

## Verification gate (one filter per command)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib bridge_chain
scripts/cargo-cgroup test  -p toron --lib chain
scripts/cargo-cgroup test  -p toron --lib bridge        # single-bridge byte-identical
scripts/cargo-cgroup test  -p toron --lib galaxy        # + snowflake (no drift)
scripts/cargo-cgroup test  -p toron --test architecture_test adr204
scripts/cargo-cgroup test  -p toron --test architecture_test adr203
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check ; git diff --check
```

## Risks

- **Breadth-for-breadth** — N-hop evidence only earns its keep if real N-hop chains are reachable; the
  reachable ≥3-hop fixture (not an injection) is the non-negotiable guard against measuring a shape that
  never arises.
- **Recognizer over-claim** — a branch / cycle / disconnected / over-cap shape must decline, not be
  mis-stitched; pinned by the four decline discriminators + the bounded-N guard.
- **Cost-model regression at k=2** — the generalization must leave the 203 2-hop row + verdict byte-
  identical; pinned by `two_hop_chain_row_and_verdict_unchanged`.
- **Peak under-counts cumulative drift** — a small interior middle that blows up but stays below the largest
  middle's estimate is missed; named in Deferred (sum-compounding), surfaced rather than silently bounded.
