# ADR 182: Snowflake-Path Join Order Evidence

**Status:** Promoted. **Migration:** `plans/impl/182_snowflake_path_join_order_evidence.md`.

ADR 181 closed the **star** envelope: order observed (177), actuated (178), measured (179),
judged (180), extended to any uniform strategy (181) — all on a **single-level,
single-probe, left-deep** star. ADR 182 takes the first deliberate step *beyond the star
topology*: it makes a **snowflake**'s join order **visible**.

A snowflake is a star whose dimension hangs off **another dimension** instead of the fact —
`regions` joined to `customers`, `customers` joined to `orders`. 182 recognizes that nesting
and emits **nested** declared-vs-cost-preferred order evidence, level by level,
**observationally**. It is the 177-analog one topology level up: **observe before act**.
Nothing is reordered; the bound `ExecutionPlan` is byte-identical. The snowflake reorder is a
later rung, *justified by the evidence this one produces*.

> **Grounding corrections that shaped the cut (kept for the record).** Three early
> assumptions were wrong and are fixed here:
> 1. **The SQL snowflake is left-deep, not nested-syntax.** `lower_table_factor`
>    (`sql_planner.rs`) supports `TableFactor::Table`/`Derived`, **not** `NestedJoin`, so
>    `orders ⋈ (customers ⋈ regions)` is not expressible. The reachable snowflake is the
>    **left-deep graph** `FROM orders o JOIN customers c ON o.customer_id=c.id JOIN regions r
>    ON c.region_id=r.id` — syntactically left-deep, but a snowflake *graph* because
>    `regions` keys to `customers`. The recognizer recovers topology from **join-key
>    sources**, so it is syntax-independent.
> 2. **Snowflakes do not leave `join_order` unset.** `extract_star_join_candidate` accepts
>    the left-deep snowflake as a flat-star candidate (every right is a scan), then
>    `try_bind_star_order_actuation` declines during dimension binding — `regions`'s key
>    `customers.region_id` does not resolve against the fact probe scope (binder.rs:1771),
>    firing `record_star_order_decline(MissingRelationLabel)`, which **sets `join_order`**.
>    Evidence is computed from the always-populated edge stream and **supersedes** that
>    misleading flat decline.
> 3. **Drop the scan-linear gate for evidence.** `broadcast_probe_is_scan_linear` is false
>    for any hash-join output, so the 177 fence would observe only **broadcast** snowflakes
>    (the execution cliff) and exclude **hash** snowflakes (the binder-only-actuatable case
>    we most want to see). 182 records per-edge strategy as *evidence*, not a decline gate.

## Grounding

- **The `probe != fact` decline IS the snowflake signal.** During actuation, each dimension
  resolves its join key against the fact probe scope; a branch dimension's key resolves to
  *another dimension* and fails (`MissingRelationLabel`, binder.rs:1771). That failure is
  exactly "this dimension hangs off a dimension, not the fact" — the structural snowflake.
- **The nesting is already in the edge stream — no new fields, no re-binding.** After
  actuation declines, the Join arm falls through to ordinary `bind_join` (binder.rs:1004),
  which pushes a `JoinOrderEdgeEvidence` per join. `relation_label_for_join_keys`
  (`binder/join_order.rs:308`) labels each edge's probe/dimension by the join key's **source
  relation**. For the left-deep snowflake the edges are `[(probe=orders, dim=customers),
  (probe=customers, dim=regions)]`. The tree reconstructs by chaining `outer.dimension ==
  inner.probe`; the **fact root** is the probe label that never appears as a dimension.
- **Edges carry their decision index.** Each `JoinOrderEdgeEvidence.target.join_index` is the
  `JoinReplayId`/index into `trace.join_decisions`, so each edge's chosen strategy/placement
  is read from the existing `DecisionReceipt`s — no new capture.
- **Snowflakes already execute** as ordinary nested joins (hash, or broadcast per dim size);
  they are never routed to the broadcast-star fragmenter (the flat-star actuation declines
  them). Observing order requires no execution change. *(Whether a* broadcast *snowflake
  survives `rewrite_broadcast_star_probe` at fragmentation is the actuation rung's question —
  recorded, not answered.)*
- **Per-level ranking already exists.** Each level (root star, each sub-star) is a flat star,
  so per-level declared-vs-preferred evidence reuses the ranking logic inside
  `recognized_star_order_outcome` — no second cost model, no new estimator. It does **not**
  reuse `StarOrderEvidence` directly: after ADR 181 that struct carries flat-star actuation
  semantics (`strategy`, `applied_order`, verdict inputs) that would be false for an
  observe-only snowflake.
- **179/180 key off `join_order`.** `star_order_runtime_replay_ids` measures only a
  `Recognized` `join_order`. Emitting snowflake evidence in a **separate** field, and clearing
  `join_order` when a snowflake is recognized, keeps the star path unchanged and defers
  snowflake runtime/verdict cleanly.
- **The arc principle, applied to itself.** 182 emits the evidence that orders the next rung
  and records the fact that rung needs: each edge's **strategy/placement** (hash vs
  broadcast). Hash-heavy snowflakes are binder-only-actuatable later (181 machinery);
  broadcast edges identify the execution cliff. 182 observes; it does not act.

## Decisions

**D1 - Recognize a strict single-root snowflake from the existing edge stream.** Add a
read-only `analyze_snowflake(&edges, &decisions)` that builds the join tree by chaining
`outer.dimension == inner.probe`. A snowflake = one **fact root** (a probe label never
appearing as a dimension) whose dimensions may themselves be a probe of a sub-star,
recursively, with **at least one branch**. Require per edge: `inner_equi`, both labels
present, no duplicate/shared dimension, single root, acyclic. Anything ambiguous → return
`None` (the existing flat path keeps its decline; 182 never weakens a decline, and never
emits its own decline row). **The scan-linear fence is not a decline gate here** (D5).

**D2 - Nested per-level order evidence reuses the flat ranking, not flat actuation evidence.**
Factor the strategy-free ranking portion of `recognized_star_order_outcome` into a helper
(`StarOrderRanking` / `rank_star_order`), then produce recursive
`SnowflakeOrderEvidence { ranking, edge_strategies, branches }`. Each `ranking` is computed
for that level's direct dimensions (a branch dimension participates in its parent level as
one dimension, scored by its own edge estimate). Per-level declared positions are
**level-local** (the relative order of that level's edges), not the global
`declared_position`. No new estimator or cost model.

**D3 - Observational invariant (the headline).** 182 changes no plan, strategy, build side,
disposition, or execution. The bound `ExecutionPlan` is byte-identical with and without the
evidence; the snowflake executes in declared order. Pinned by the full lib/arch suites
staying green plus a guard that `analyze_snowflake` is read-only and never consulted by
`bind_join`/strategy/`extract_star_join_candidate`/runtime.

**D4 - Snowflake evidence supersedes the flat decline; the star path is pristine.** Add
`render_trace.snowflake_order: Option<SnowflakeOrderEvidence>` and a `join_order_snowflake`
cockpit kind. After binding, compute `snowflake_order = analyze_snowflake(...)`; when it is
`Some`, **clear `join_order`** (drop the actuation's misleading `MissingRelationLabel` flat
decline) so the cockpit tells the real story. A flat star is untouched: `analyze_snowflake`
returns `None` (no branch), and the existing `join_order`/`analyze` path runs exactly as
before. There is **no snowflake decline row** — an unrecognized near-snowflake falls to the
flat path, which keeps its richer decline.

**D5 - Record per-edge strategy as evidence, do not gate on it.** Each level carries the
actual chosen strategy/placement for each direct edge, read by correlating that edge to
`trace.join_decisions` via `target.join_index` (the same `DecisionReceipt`s 181 reads). This
keeps mixed levels representable (`customers=hash`, `stores=broadcast`) and distinguishes a
mostly **hash snowflake** (binder-only-actuatable next) from a **broadcast snowflake**
(execution cliff). Strategy is rendered evidence only: no uniform-strategy gate, no fake
`StarOrderStrategy`, no reorder. A missing/mismatched decision receipt makes the recognizer
return `None`; it is a trace integrity failure, not a new snowflake decline kind.

**D6 - EXPLAIN-gated and cheap.** Snowflake evidence is computed only when the EXPLAIN render
trace is present (normal bind pays nothing), exactly as 177.

**D7 - No snowflake runtime or verdict this cut.** `star_order_runtime_replay_ids` keys off
`join_order`, which is cleared for a recognized snowflake → EXPLAIN ANALYZE on a snowflake
shows the **nested plan evidence and no `join_order_runtime` or `join_order_verdict` row**.
Snowflake runtime evidence (179-analog) and verdict (180-analog) are the named next rungs.

**D8 - Frontend-shared; no shell, no actuation, no executor.** SQL `EXPLAIN`/`EXPLAIN ANALYZE`
and Rune `explain()`/`analyze()` share the path. No `optimizer.rs`, memo, Cascades, or broad
`PlanProperties`. No `extract_star_join_candidate` change, no reorder, no
`fragment.rs`/`rewrite_broadcast_star_probe` change, no cost model, no new optimizer fact, no
Wendelon/storage/PBC/wire change.

## Headline Gates

- `snowflake_order_evidence_reports_nested_declared_vs_preferred` - the left-deep snowflake
  `orders → customers → regions` (plus a leaf dim on `orders`) written in a deliberately
  non-optimal order yields nested evidence: the root star's declared/preferred order **and**
  the `customers` branch's declared/preferred order, per-dimension estimates shown.
- `snowflake_order_evidence_is_observational` - the bound `ExecutionPlan` is byte-identical
  with and without the evidence; execution in declared order (uses a **hash** snowflake so it
  also executes for a results check).
- `snowflake_recognized_supersedes_flat_missing_relation_decline` - the same query that emits
  `declined_missing_relation_label` on `main` instead emits a `join_order_snowflake` row and
  leaves the flat `join_order` unset. (Fails on `main` and under the star-only recognizer —
  the discriminating gate.)

## Supporting Gates

- `snowflake_branch_links_outer_dimension_to_inner_probe` - the branch is reconstructed by
  `outer.dimension == inner.probe` chaining over the existing edge list (no new edge field).
- `flat_star_emits_no_snowflake_row` - a flat star still emits the flat `join_order` row and
  no snowflake row (`analyze_snowflake` returns `None` with no branch).
- `galaxy_or_shared_dimension_declines_snowflake` - a shared dimension / second fact root /
  cycle yields no snowflake row (strict single-root tree); the prior flat decline stands.
- `snowflake_evidence_records_per_edge_strategy_without_gating` - a hash edge reports
  `strategy=hash_partitioned`; a small-dim edge reports `broadcast`; a mixed level still
  emits snowflake evidence instead of declining.
- `snowflake_order_evidence_is_explain_gated` - a normal (non-EXPLAIN) bind emits nothing.
- `snowflake_explain_analyze_shows_plan_evidence_without_runtime_or_verdict` - EXPLAIN
  ANALYZE on a snowflake shows the nested plan evidence and emits no `join_order_runtime` or
  `join_order_verdict` row.
- `rune_explain_shows_snowflake_order_evidence` - Rune parity.

## Architecture Guards

- `adr182_snowflake_recognition_is_observational` - `analyze_snowflake` is read-only;
  `bind_join`/strategy/`extract_star_join_candidate`/runtime do not consult `snowflake_order`;
  no reorder.
- `adr182_snowflake_reconstructs_from_existing_edges` - no new `JoinOrderEdgeEvidence` field;
  recognition is dimension/probe label chaining.
- `adr182_snowflake_uses_strategy_free_ranking_not_fake_strategy` - snowflake evidence uses
  the factored ranking helper, not `StarOrderEvidence`; no `StarOrderStrategy::Broadcast` or
  other placeholder strategy is injected for a snowflake level.
- `adr182_snowflake_records_strategy_as_evidence_not_gate` - `analyze_snowflake` reads
  `DecisionReceipt`s by `target.join_index` into per-edge strategy evidence; it does not call
  `uniform_star_order_strategy` or decline mixed-strategy snowflake levels.
- `adr182_snowflake_supersedes_decline_without_gating_runtime` - recognized snowflake clears
  `join_order`; `star_order_runtime_replay_ids` still reads `join_order` only.
- `adr182_no_executor_fragment_costmodel_or_wendelon_change` - no executor, `fragment.rs`,
  `rewrite_broadcast_star_probe`, cost model, storage, PBC, wire, or Wendelon change.
- `adr182_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.
- `adr182_cockpit_is_frontend_shared` - SQL and Rune render through the same path.

## Rejected Alternatives

**Gate snowflake evidence on `join_order.is_none()`.** Wrong: the SQL-reachable left-deep
snowflake gets a flat `MissingRelationLabel` decline from the actuation attempt, so the
fallback never runs. Evidence is computed from the always-present edge stream and supersedes
that decline.

**Require the scan-linear probe fence (177-style).** Wrong for evidence: it would observe
only broadcast snowflakes (the cliff) and hide hash snowflakes (the binder-only-actuatable
target). 182 records strategy as evidence instead.

**A `SnowflakeOrderOutcome::Declined` row.** Dropped: a snowflake decline row would suppress
the flat path's richer decline. An unrecognized near-snowflake returns `None` and falls to the
flat path.

**Recurse the planning-time actuator for evidence.** It binds legs and risks drifting toward
actuation. The flat edge stream already carries per-edge estimates from ordinary binding, so
the recognizer reconstructs the tree post-bind with no re-binding — the same observe-only
shape as 177.

**Fold in snowflake runtime/verdict.** A genuine mechanism boundary (the same one that
separated 177 from 179/180). 182 delivers the complete plan-evidence topic; runtime is the
named next rung.

## Non-Goals

- no snowflake reorder/actuation (binder-only hash-snowflake actuation is a named next rung);
- no broadcast-snowflake execution relaxation (`rewrite_broadcast_star_probe` — the cliff);
- no snowflake runtime evidence or measured verdict (179/180-analog, deferred);
- no galaxy (multi-fact), bridge/many-to-many, or cyclic recognition;
- no cost model, no new optimizer fact, no second estimator;
- no executor, fragment, storage, PBC, wire, catalog, or Wendelon change;
- no optimizer shell, memo, Cascades, or broad `PlanProperties` contract.

## Consensus Trail

The council took the topology step over deepening within the star (mixed-strategy cost) or
banking robustness (verdict accrual): climb to the next schema shape, but **observe before
act**. Grounding sharpened the cut materially: the SQL snowflake is the *left-deep graph*
(recovered from key-source chaining, not nested syntax), it currently surfaces as a misleading
`MissingRelationLabel` decline that 182 supersedes, and the scan-linear fence is recorded as
strategy *evidence* rather than a decline gate so the **hash** snowflakes — the ones the next
rung can actuate binder-only — are the ones we see. Observational, additive, star path
byte-identical.

## Forward Arc

`182 snowflake plan-order evidence -> snowflake runtime evidence (179-analog) -> snowflake
measured verdict (180-analog) -> hash-snowflake order actuation (binder-only, 181 preflight
machinery) -> broadcast-snowflake execution cliff (rewrite_broadcast_star_probe scan-build ->
published sub-plan build) -> galaxy/multi-fact -> bridge/many-to-many -> cyclic caps -> memo
only when receipts show decisions fighting`.

After 182 the snowflake's order is *visible*, per level, with the strategy that tells the next
rung which snowflakes it can cheaply reorder — evidence, not faith.

## Deferred (named)

Snowflake runtime evidence + measured verdict (179/180-analog); hash-snowflake order
actuation; the broadcast-snowflake execution cliff (`rewrite_broadcast_star_probe`); galaxy /
bridge / cyclic recognition; always-on snowflake telemetry; a strategy-aware cost model for
mixed/snowflake ordering.
