# Impl Plan — ADR 194: Role-Neutral Join Graph Evidence Substrate

Pairs with `plans/adr/194_role_neutral_join_graph_evidence_substrate.md`.

Capture the role-neutral join graph at the bind site, derive the existing star/snowflake roles from
it byte-identically, and add an observe-only galaxy evidence row that reads the role-neutral endpoints
to see a conformed dimension the directed trace erases. Substrate + recognizer + render; no actuation,
fragmenter, runtime verdict, or executor change. **Phase 0 (the reachability/negative-result proof)
goes first** — it pins why the cut is the substrate, not a recognizer over the old stream.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| The single edge capture site (throws away role-neutral info) | `binder.rs:1910-1932` — `probe = relation_label_for_join_keys(left)`, `dimension = …(right)`; `left_resolved_keys`/`right_resolved_keys` and per-source `join_order_sources` facts are in scope but not preserved on the edge |
| Edge struct to augment | `JoinOrderEdgeEvidence` `join_order.rs:241` |
| Relation label fn (reused per side) | `relation_label_for_join_keys` `join_order.rs:618` |
| Tree recognizers (must stay byte-identical, sourced from derived roles) | `analyze` `join_order.rs:781`; `analyze_snowflake` `:858`; single-parent tree via `dimension_parent` `:884`, `roots.len() != 1` `:905` |
| Observe-only post-bind hook | `binder.rs:225-248` |
| Render trace struct (+ sibling galaxy field) | `OptimizerExplainRenderTrace` `explain.rs:~84`; `join_order_edges` `:86` |
| Snowflake EXPLAIN row (template) | `optimizer_explain_snowflake_order_row` `explain.rs:942` |
| Synthetic guard to make truthful | `galaxy_or_shared_dimension_declines_snowflake` `tests.rs:9150` |
| Cartesian/synthetic-stream boundary (Phase 0 facts) | CROSS JOIN + comma join rejected at parse; `ON 1=1` → `JoinComputedKeyUnsupported`; conformed two-fact SQL is expressible but records as a single-root directed snowflake trace (probed empirically) |

## Phase 0 — Reachability / negative-result proof (do first; fold B in)

Rewrite `galaxy_or_shared_dimension_declines_snowflake` into a truthful boundary test (rename
`role_neutral_substrate_recognizes_reachable_galaxy_not_synthetic`):
- Assert the binder **cannot emit** the synthetic multi-root stream from real SQL: cross join and comma
  join return parse errors; `INNER JOIN … ON 1=1` returns `JoinComputedKeyUnsupported`.
- Assert a conformed two-fact query binds to a **single-root** snowflake-shaped trace on pre-194 code
  (one root, the second fact recorded as a dimension of the shared table).
- Keep the assertion that `analyze_snowflake` declines the hand-built multi-root edge set.

This documents the blocker before any substrate code, exactly the 193 discipline (let the probe fix
the scope). The conformed two-fact fixture lands here and is reused as the headline.

## Phase 1 — Two-fact fixture (new; reachable)

Add a catalog with two fact tables sharing a conformed dimension: `sales(id, product_id, …)` and
`returns(id, product_id, …)` both FK to `product(id, …)`, seeded with byte/row/NDV facts. SQL:
`SELECT … FROM sales s JOIN product p ON s.product_id = p.id JOIN returns r ON r.product_id = p.id`.
Confirm (Phase 0) it binds to a single-root snowflake today — that is the shape 194 makes observable.

## Phase 2 — Role-neutral edge capture (substrate; binder)

Augment `JoinOrderEdgeEvidence` (`join_order.rs:241`) with side-neutral endpoints, all populated from
values already in scope at `binder.rs:1910`:
- `left_key_source_label: Option<String>`, `right_key_source_label: Option<String>` (or shorter
  local names if the meaning stays explicit): the relation label that owns the resolved join key on
  each input side;
- `left_key_signature: Vec<String>`, `right_key_signature: Vec<String>`: resolved join-key column
  identities per side, from `left_resolved_keys` / `right_resolved_keys`;
- optional endpoint facts summary (rows/key NDV) copied from `left_plan.join_order_sources` /
  `right_plan.join_order_sources` by those key-source labels.

Do **not** use `left_plan.facts` / `right_plan.facts` for endpoint facts. On a composite input those
facts describe the current plan side, not the relation that owns the join key. Make `probe` /
`dimension` **derived** (`probe == left_key_source_label`, `dimension == right_key_source_label`) —
keep the fields (or accessor methods) so every existing reader is byte-identical, but the durable
record is the role-neutral endpoint set. No second traversal; no execution-plan read.

## Phase 3 — `analyze_galaxy` recognizer (binder)

`analyze_galaxy(&[JoinOrderEdgeEvidence], &[DecisionReceipt]) -> Option<GalaxyOrderEvidence>`:
- Build an **undirected** adjacency keyed by `(relation_label, key_signature)` endpoints.
- A relation that is an endpoint of ≥2 edges on the **same key signature** is a **conformed dimension
  candidate**; the other endpoints of those edges are its incident relation candidates.
- Recognize iff ≥1 conformed dimension candidate links ≥2 distinct incident relations (else `None` —
  a plain star/snowflake).
- Decline on non-inner/non-equi, missing labels/estimates, decision/edge target mismatch, or
  unbounded/cyclic shapes.
- Return `GalaxyOrderEvidence { conformed: Vec<ConformedDimension { label, key_signature,
  incident_relations: Vec<String> }>, edges: Vec<role-neutral edge summary>, component_count }`.
  Separate type from `SnowflakeOrderEvidence`.

No change to `bind_local_star_actuation`, `order_cost`, `rank_star_order`, or any decline reason.

## Phase 4 — Sibling render (explain)

- Add `OptimizerExplainRenderTrace.galaxy_order: Option<GalaxyOrderEvidence>` (sibling field — **not** a
  `JoinOrderRenderState` variant, so the `has_recognized_star_order` / `star_order_receipts` /
  EXPLAIN-ANALYZE-dispatch arms are untouched).
- In the post-bind hook (`binder.rs:225-248`), after the existing star/snowflake observation, also run
  `analyze_galaxy` and populate `galaxy_order` if `Some`. It is additive: the snowflake/star receipt
  still renders.
- EXPLAIN appends a `join_order_galaxy` row: `target=galaxy[conformed=p; relations=[sales,returns]]`,
  detail = conformed dimension(s) + key signature, incident relations, component count,
  role-neutral edge list with per-edge strategy/placement/estimate. Reuse the snowflake detail
  builders where shapes match.
- EXPLAIN ANALYZE may repeat the same plan row; no runtime/verdict rows (deferred).

## Phase 5 — Tests

- `conformed_two_fact_query_emits_galaxy_evidence` — headline; fails on `main`; `join_order_galaxy`
  row names `product` shared by `{sales, returns}`; plan + results byte-identical to baseline.
- `snowflake_and_star_evidence_byte_identical` — derived roles reproduce pre-194 receipts across the
  existing star/snowflake fixtures (the byte-identity regression).
- `galaxy_recognizer_declines_single_fact` — a plain star/snowflake yields `galaxy_order = None`.
- `galaxy_recognizer_declines_non_equi_or_missing_estimate`.
- `role_neutral_substrate_recognizes_reachable_galaxy_not_synthetic` — the Phase 0 boundary test.
- `galaxy_edge_list_carries_role_neutral_endpoints` — each row carries left/right key-source labels
  + key signatures.
- `rune_conformed_two_fact_matches_sql` — Rune parity (primary interface), if Rune can express the
  shared-dimension join (it joins one dataframe spine; verify the conformed query is expressible and
  produces the same galaxy row — if not, record the Rune gap explicitly, do not fake it).

## Phase 6 — Architecture guards

- `adr194_edge_substrate_is_role_neutral` — `JoinOrderEdgeEvidence` carries left/right key-source
  labels + key signatures; `probe`/`dimension` are derived from those key-source labels.
- `adr194_galaxy_is_separate_type_from_snowflake` — `GalaxyOrderEvidence` is not an alias/extension of
  `SnowflakeOrderEvidence`; galaxy lives in a sibling render field, not a `join_order` state variant.
- `adr194_galaxy_is_observe_only` — `analyze_galaxy` / galaxy render never reach
  `bind_local_star_actuation` / `StarReceipts` / any actuator.
- `adr194_tree_roles_derived_not_rewritten` — `analyze` / `analyze_snowflake` consume the derived
  `probe`/`dimension` views; no parallel ranking impl.
- `adr194_no_fragmenter_executor_or_runtime_galaxy` — `distributed/`, `exec.rs` untouched; no
  `join_order_galaxy_runtime` / `_verdict`.
- `adr194_galaxy_substrate_is_binder_only` — no storage/Wendelon/PBC/wire/grammar surface.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib conformed_two_fact_query_emits_galaxy_evidence
scripts/cargo-cgroup test -p toron --lib galaxy
scripts/cargo-cgroup test -p toron --lib snowflake
scripts/cargo-cgroup test -p toron --lib star_order
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --test architecture_test adr194
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

Use one test filter per command.

## Risks

- **Byte-identity regression (the big one).** Deriving `probe`/`dimension` from the new substrate must
  reproduce the exact pre-194 labels — any drift silently changes star/snowflake receipts. Pin with
  `snowflake_and_star_evidence_byte_identical` and the full 177–193 suite green.
- **Over-claiming conformance.** `analyze_galaxy` must key conformance on the **same key signature**,
  not just a shared relation label, or an incidental self-referential join reads as a galaxy. The
  recognizer reports trace-level role-neutral endpoints, never catalog fact/dimension semantics
  (Codex's humility).
- **Recognizer poaching snowflakes.** Single-fact shapes must yield `None`; pin with
  `galaxy_recognizer_declines_single_fact`.
- **Rune gap.** If Rune cannot express the shared-dimension join, record it as a named follow-on; do
  not weaken the SQL headline to hide it.

## Deferred

Galaxy runtime rows; galaxy measured verdicts; galaxy actuation / global graph ordering; bridge /
many-to-many evidence; catalog fact/dimension classification; heterogeneous-stage fragmenter;
branch-local mixed; strategy-aware cost model; outer joins; memo/Cascades; durable optimizer evidence.
