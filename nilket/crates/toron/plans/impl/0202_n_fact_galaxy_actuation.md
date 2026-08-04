# Impl Plan — ADR 202: N-Fact Galaxy Actuation

Pairs with `plans/adr/0202_n_fact_galaxy_actuation.md`. The new capability is exactly the n-fact re-root;
recognition, recommendation, the gated seam, and 200/201 accrual are reused. Sequencing mirrors 198/199:
**Phase 0 (the distributed fragmentation gate) ships and passes before any actuation code.**

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Re-root to generalize (2-fact only today) | `reroot_galaxy_join_tree` (join_order.rs:584) — exact `Join(Join(F1,D),F2)` leaf match; flips on-pairs `(l,r)→(r,l)` on position change |
| Recognition — ALREADY n-fact | `analyze_galaxy` (conformed dim shared by `incident.len() >= 2`, captures all `incident_relations`) |
| Recommendation — ALREADY n-fact | `galaxy_order_recommendation` (loops `incident_relations`, argmin over n facts → `pivot` + `preferred_root`) |
| Gated actuation seam | `try_bind_graph_actuation` (join_order.rs) — calls the re-root, binds via the snowflake staging path |
| Restoration / margin / preflight / accrual | `restore_declared_star_order_project` (join.rs), `join_order_soft_margin_step`, `preflight_star_order_strategy`, `GraphActuationResult` + `accrue_graph_order_decision` (binder.rs) — all reused |
| Phase-0 distributed precedent (extend to 3-fact) | `galaxy_exec_catalog` / `galaxy_exec_storage` / `galaxy_reroot_query_digest_matches_ast_order_distributed` (lib.rs:~2777/2849/2892) |

## Phase 0 — Distributed fragmentation gate (DO FIRST; the cut's life-or-death)

Prove a hand-written re-rooted **3-fact** galaxy executes distributed and digest-matches the AST baseline.
If it fails, the n-fact re-root is a fragmenter cut, not a binder reorder → re-scope to observe-only.

1. Extend `galaxy_exec_catalog` / `galaxy_exec_storage` with a **third fact** joining the conformed
   `product` (e.g. `shipments`, the smallest fact so it is the cost-optimal root): `sales`(100k) /
   `product`(100, dim) / `returns`(5k) / `shipments`(1k), all on `…product_id = product.id`, FK-complete.
2. `three_fact_galaxy_reroot_digest_matches_ast_order_distributed` (sibling of the 198 test):
   - AST: `SELECT sales.id, product.id, returns.id, shipments.id FROM sales JOIN product … JOIN returns …
     JOIN shipments …` (left-deep, FROM root `sales`).
   - Hand-written re-rooted (shipments-first): `FROM shipments JOIN product … JOIN sales … JOIN returns …`
     — same projection / column order (digest sorts rows; columns must align).
   - Assert `canonical_arrow_ipc_digest(&results, SortPolicy::FullRowLex)` equal. **PASS gates Phase 1.**

## Phase 1 — D1: generalize the re-root to n facts

`join_order.rs`, re-signature `reroot_galaxy_join_tree(plan: &LogicalPlan, pivot: &str, preferred_root:
&str) -> Option<LogicalPlan>`:
- **Collect** the left-deep inner-join chain. Walk from the outer Join down to the innermost
  `Join(F1, D)`; confirm the inner-right leaf label == `pivot`. For each fact extract its **canonical
  `(fact_keys, dim_keys)`** from that node's `on`:
  - innermost `Join(F1, D)` (F1 left, D right) → `(on.left, on.right)`;
  - each outer `Join(composite, Fi)` (Fi right, D in left composite) → `(on.right, on.left)`.
  Reject (`None`) if any node is non-inner / non-leaf, or a fact's `on` does not reference `pivot` (not a
  clean conformed galaxy) → decline.
- **Rebuild** left-deep with `preferred_root = Fk` as the base driver:
  `Join(Join(…Join(Join(Fk, D), Fj1)…, Fjm)`, `{Fj}` = the remaining facts in original order. Orient each
  `on` by position: base `Join(Fk, D)` → `(Fk.fact_keys, Fk.dim_keys)`; each added `Join(composite, Fj)`
  → `(Fj.dim_keys, Fj.fact_keys)` (D in the left composite, Fj on the right).
- **The 2-fact case is the `n=2` instance.** Checkpoint: the existing 2-fact galaxy lib suite +
  `galaxy_reroot_query_digest…` stay **byte-identical** after the generalization (run before Phase 2).

`try_bind_graph_actuation`: pass `recommendation.pivot` + `recommendation.preferred_root` to the re-root
(today it calls `reroot_galaxy_join_tree(plan)` with no args). The recommendation already names both for
any n.

## Phase 2 — D3: actuate through the existing seam (reuse only)

No new code beyond the Phase-1 call-site wiring: the existing `try_bind_graph_actuation` binds the deeper
re-rooted snowflake through the staging path, applies the soft-margin gate, declines on a selected-
preflight cliff, restores the AST schema (178/186), terminates recursion (the re-rooted galaxy's own
recommendation is `KeptAlreadyOptimal`), and accrues the `GalaxyRoot` decision (200) + sampled verdict
(201). Verify each carries to n-fact via the tests; expect **zero** new lines in the accrual/gate paths.

## Phase 3 — guards + tests

Guards (`tests/architecture/join_order.rs`):
- **Retarget** `adr198_galaxy_reroot_is_bounded_two_fact` → `adr202_galaxy_reroot_is_bounded_n_fact`: the
  re-root handles a left-deep conformed-galaxy chain of any n (asserts the pivot-collect + rebuild shape;
  still `!join_order.contains("Cascades")`, no general permutation/search; rejects non-conformed).
- Add `adr202_n_fact_reroot_reuses_gated_seam`: `try_bind_graph_actuation` calls `reroot_galaxy_join_tree`
  with the recommendation's pivot/preferred_root; no new decision/verdict kind.
- Add `adr202_galaxy_actuation_is_binder_only`: no `reroot_galaxy_join_tree` / galaxy-actuation symbols
  in fragment/exec/storage/pbc/wire/wendelon.

Tests (`binder/tests.rs` + `lib.rs`):

| test | locks | fails under |
|---|---|---|
| `three_fact_galaxy_reroot_digest_matches_ast_order_distributed` (Phase 0) | re-root correct + fragmentable | a fragmenter cliff |
| `three_fact_galaxy_reroots_to_cheapest_root_and_executes_distributed` (headline) | actuation fires + digest-safe | `main` (declines today) |
| `three_fact_galaxy_already_optimal_kept` | cost decides, not a constant | a constant re-root |
| `n_fact_galaxy_reroot_preserves_output_schema` | 178/186 restoration on the deeper tree | dropped restoration |
| `three_fact_galaxy_accrues_galaxy_root_reorder` | 200/201 accrual carries to n-fact | accrual fenced to 2-fact |
| 2-fact galaxy suite + `galaxy_reroot_query_digest…` byte-identical | D1 neutrality on n=2 | a leaky generalization |

## Verification gate (one filter per command)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib three_fact_galaxy_reroot_digest_matches_ast_order_distributed
scripts/cargo-cgroup test  -p toron --lib galaxy        # 2-fact byte-identical + n-fact
scripts/cargo-cgroup test  -p toron --lib snowflake     # staging unaffected
scripts/cargo-cgroup test  -p toron --test architecture_test adr202
scripts/cargo-cgroup test  -p toron --test architecture_test adr198   # reroot guard retarget green
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check ; git diff --check
```

## Risks

- **Phase 0 is life-or-death.** If the 3-fact re-root does not fragment + digest-match, n-fact actuation
  is a fragmenter ADR — re-scope to observe-only-confirm (today's decline) + a decline guard, honesty
  gate `n_fact_galaxy_declines_on_fragmenter_cliff`.
- **On-orientation bug (D1).** The canonical `(fact_keys, dim_keys)` extraction + position-based flip is
  the one subtle piece — pinned by the schema-preservation + digest tests and the 2-fact byte-identical
  regression.
- **Bushy / non-conformed shapes.** Only left-deep chains where every fact joins the single conformed
  `pivot` re-root; anything else returns `None` → decline (pinned by `adr202_galaxy_reroot_is_bounded_n_fact`).
