# ADR 177: Star-Path Join Order Evidence

**Status:** Promoted. **Migration:** `plans/impl/0177_star_path_join_order_evidence.md`.

The first rung of the optimizer arc. Toron takes join *order* from the query and spends
its join cardinality estimate only on build-side selection. ADR 177 does **not** reorder
anything — it makes the join-order *decision observable*: for a recognized left-deep
inner-join star, compute the order the cost model *would* choose and emit it as a receipt
alongside the declared order. It is **purely observational** — the bound plan and
execution are byte-identical — applying the 174/175 "see the decision before acting on
it" discipline to join order. The reorder (ADR 178) is a later rung, *justified by the
evidence this one produces*.

### Why evidence first (the arc principle, applied to itself)

The arc's rule is "emit evidence that orders the next step." Join reorder is a behavior
change on a path (`fragment.rs:80` left-deep broadcast star) we want to keep trustworthy.
So before flipping behavior, we make the decision legible and watch it: how often does
declared order differ from cost-preferred? Does the estimate that drives it hold up? 177
answers those from real `EXPLAIN`s; 178 acts on the answer — the same observe-then-act
shape that made 174 (receipts) → 175 (readability) safe.

## Grounding

- **Order is declared, estimate already computed.** `bind_join` (`binder.rs`) binds the
  nested `LogicalPlan::Join` tree in declared order and computes `join_estimated_row_count`
  via `join_output_ndv_containment_estimate` / `multi_key_join_cardinality`
  (`binder/selectivity.rs`) — spent on build-side only. 177 spends the *same* estimate on
  the order *evidence*; it computes no new fact.
- **The executor path already has a scan-linear probe fence.**
  `broadcast_probe_is_scan_linear` (`binder/join.rs`) is not a complete star recognizer;
  it is the current probe-shape check used by broadcast planning. ADR 177 should reuse
  that check as an eligibility fence where it applies, while deriving relation identity
  for grouping from the bound scopes / scan labels. The invariant is "do not call
  something a scored star if the existing probe-shape helper would reject its spine,"
  not a brittle one-to-one equivalence claim.
- **The receipt home exists** (174/175: `OptimizerTrace`, render sidecar,
  `optimizer_explain_*` in `binder/explain.rs`, the cockpit). A join-order evidence row
  joins the join-strategy and scan/filter rows already there.
- **No execution change.** 177 emits evidence only; it never reorders, so `fragment.rs:80`
  is untouched.

## Decisions

**D1 - Recognize a strict left-deep inner-join star with the existing probe fence.** A
star = one probe relation joined to N dimensions by inner equi-joins, left-deep. The
scorer may add the minimal relation-label helpers it needs, but it must also respect the
current scan-linear probe helper (`broadcast_probe_is_scan_linear`) instead of inventing
a looser executable-star definition. Anything else (outer join, non-equi key, non-star
spine) **declines** with a recorded reason. Recognition only drives the receipt; no plan
is built differently.

**D2 - Emit per-join evidence at the real estimate (fidelity).** When
`render_trace.as_mut()` is present, `bind_join` pushes a cheap per-edge entry —
`{probe, dimension, declared_position, estimated_rows, estimate_source, inner_equi}` —
carrying the *same* `join_estimated_row_count` the binder already computed. The evidence
reports the optimizer's actual number, never a re-derivation. Normal bind has
`render_trace = None` and pushes nothing (hot path unchanged).

**D3 - Compute the cost-preferred order without applying it.** In a dedicated step, group
the per-edge entries sharing a bound probe label into a star chain and require the current
probe-shape fence to accept the spine. Compute the greedy cost-preferred order
(dimensions most-selective-first) with a simple intermediate-size cost proxy. Produce
`declared_order`, `cost_preferred_order`, `orders_agree`, and both costs. **Nothing is
reordered** — this is a computation.

**D4 - Emit a join-order evidence receipt.** Into the cockpit (174/175 path), a
`join_order` evidence row: star recognized (or decline reason), probe + dimensions,
per-dimension estimated rows + estimate source, `declared_order`, `cost_preferred_order`,
`orders_agree`, declared/preferred cost. The cost is labeled the *model's* preference, not
truth.

**D5 - Observational invariant (the headline).** 177 changes no plan, disposition, PBC
program, estimate, join strategy, build side, or execution. The bound `ExecutionPlan` is
byte-identical with and without the evidence. Pinned by the full lib/arch suites staying
green and a guard that the evidence step is read-only and never feeds planning.

**D6 - EXPLAIN-gated and cheap.** Evidence is captured only when the EXPLAIN render
sidecar is present: a normal query pays nothing; `EXPLAIN`/`explain()` computes it.
(Always-on order-evidence telemetry is a deferred follow-on.)

**D7 - The optimizer layer's earned beginning.** Star grouping + cost compare live in a
dedicated `binder/join_order.rs`, pulled into existence by the evidence consumer — not an
empty shell, and not a new decision smuggled into `bind_join`'s path.

**D8 - Frontend-shared, binder/system-table only.** SQL `EXPLAIN` and Rune `explain()`
both surface the evidence via `LogicalPlan::Explain` → the cockpit. No executor, PBC,
storage, Wendelon, fact-producer, or wire change.

## Headline Gates

- `star_order_evidence_reports_declared_vs_preferred` - a 3-dimension inner star written
  in a deliberately non-optimal order yields an evidence row with `orders_agree = false`
  and `cost_preferred_order` ≠ `declared_order`, the per-dimension estimates shown.
- `star_order_evidence_is_observational` - the bound `ExecutionPlan` is byte-identical with
  and without the evidence; execution still uses declared order.
- `non_star_join_declines_with_reason` - an outer join / non-star shape records
  `star_recognized = false` + a decline reason, no evidence order.

## Supporting Gates

- `star_order_evidence_agrees_when_declared_is_optimal` - an already-optimal star →
  `orders_agree = true`, `preferred == declared`.
- `star_order_evidence_estimates_match_join_estimates` - per-dimension rows equal the
  `join_estimated_row_count` the binder computes (fidelity; no second estimator).
- `star_recognition_respects_scan_linear_probe_fence` - a candidate star whose probe spine
  fails `broadcast_probe_is_scan_linear` declines rather than emitting scored evidence.
- `star_order_evidence_is_explain_gated` - a normal (non-EXPLAIN) bind pushes no per-edge
  entries (hot path unchanged).
- `rune_explain_shows_star_order_evidence` - Rune `explain()` parity.

## Architecture Guards

- `adr177_star_order_evidence_is_observational` - the evidence step is read-only over the
  bound plan; `bind_join`/strategy/dispatch do not consult it; no reorder.
- `adr177_join_order_evidence_lives_in_optimizer_step` - grouping + cost compare live in
  `binder/join_order.rs`, not inline in `bind_join`'s decision path.
- `adr177_evidence_reuses_existing_estimate` - per-edge `estimated_rows` is the value
  `bind_join` already computes; no new fact producer / second estimator.
- `adr177_star_recognition_respects_scan_linear_probe_fence` - recognition uses
  `broadcast_probe_is_scan_linear` as a strict eligibility fence, not a looser star
  definition.
- `adr177_evidence_is_explain_gated` - per-edge entries pushed only when
  `OptimizerExplainRenderTrace` is present.
- `adr177_no_optimizer_shell_or_planproperties_contract` - no top-level `optimizer.rs`,
  `struct Optimizer`, broad `PlanProperties`, Cascades, or memo search is introduced.
- `adr177_no_execution_or_fragment_change` - no executor/PBC/`fragment.rs`/storage change.
- `adr177_cockpit_is_frontend_shared` - rendered via `optimizer_explain_plan`.

## Non-Goals

- **no reorder / no plan change** (that is ADR 178, justified by this evidence);
- no outer joins, no non-star/general join graphs, no snowflake chains;
- no new optimizer fact, producer, or second estimator;
- no top-level optimizer shell or broad `PlanProperties` contract;
- no `fragment.rs:80` / execution / topology change;
- no always-on evidence telemetry (EXPLAIN-gated this cut);
- no property contract beyond local per-edge rows/keys consumed by the compare.

## Consensus Trail

The council opened the optimizer arc but sharpened the first cut from "cost-based ordering"
to **order evidence**: observe the decision before acting, exactly as 174→175 did for the
read path. 177 spends the cardinality estimate the binder already computes on a legible
declared-vs-preferred join-order receipt — observational, EXPLAIN-gated, in a dedicated
optimizer step, with star recognition respecting the executor's existing
`broadcast_probe_is_scan_linear` probe fence. It delivers a real capability (a power-user
sees whether their star is ordered optimally) while generating the evidence that
justifies and shapes the reorder in ADR 178.

## Forward Arc

`177 star-path join order evidence -> 178 act on it (reorder when evidence shows
declared≠preferred and the estimate holds) -> snowflake/chain order -> bounded enumeration
-> outer-join order safety -> relax left-deep (fragment.rs:80) -> bridge/m:n -> cyclic caps
-> memo (only when receipts show decisions fighting)`.

After 177 the join-order decision is *visible* on the safe star path; 178 makes it *act*,
on evidence, not faith.

## Deferred (named)

The reorder itself (178); always-on order-evidence telemetry; outer-join + snowflake +
general-graph evidence; bounded enumeration; a fuller `PlanProperties` contract; the
`fragment.rs:80` execution relaxation.
