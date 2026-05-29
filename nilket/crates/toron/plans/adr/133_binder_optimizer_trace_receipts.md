# ADR 133: Binder Optimizer Trace Receipts

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini); operator-selected
topic.** Makes the CBO decisions of 129–132 **durable and inspectable** by
collecting a crate-private `OptimizerTrace` during binding — one `DecisionReceipt`
per join-strategy decision — without yet adding a user-facing surface. Binder-local;
no public statement, no `ExecutionPlan` field, no wire. The durable diagnostic
object a future public `EXPLAIN` (ADR 134) renders from.

**Category:** Toron interior — optimizer observability (binder-local).
**Predecessors:** 129–132 (strategy rules whose `decision.reason` is discarded after
binding — `binder.rs:1163-1180` keep only `strategy`/`placement`).
**Follow-on:** ADR 134 — public `EXPLAIN <query>` rendered *from* this trace,
likely through the existing `SystemTable` result machinery (grounded feasible:
system-table rows already materialize to `RecordBatch` at `exec.rs:667`; the exact
`SystemTableKind`/schema is an ADR 134 decision). Deferred here so this cut locks
the trace semantics first.
**Migration:** `plans/impl/133_binder_optimizer_trace_receipts.md`.

## Context
129–132 built a real strategy seam: `choose_join_strategy` →
`JoinStrategyDecision { strategy, placement, reason }`. Grounded: `bind_join`
consumes `strategy`/`placement` to build `ExecutionPlan::Join` and **discards
`reason`** — so *why* a join chose its strategy is testable only by direct helper
calls or by inferring from plan shape. Before extending the optimizer (multi-way,
selectivity) or flipping inference default-on, the decisions must be durable and
inspectable. This ADR makes them so; the public `EXPLAIN` surface that renders them
is the immediate follow-on (134), deliberately separated so the trace semantics are
locked in tests before a user surface is designed.

## Doctrine — decisions must leave a trail
> An optimizer that cannot explain its choices cannot be tuned or trusted. Every
> join-strategy decision during binding emits a structured, machine-readable
> receipt. The trace is the *source of truth* — automated tests assert CBO behavior
> against it directly (no plan-shape inference), and a future public `EXPLAIN`
> renders from it. It is a diagnostic object, not a new strategy rule and not a
> public interface.

## Decisions

**D1 — `OptimizerTrace` / `DecisionReceipt` (`pub(crate)`).**
```rust
pub(crate) struct JoinTraceTarget { pub(crate) join_index: usize }
pub(crate) struct DecisionReceipt {
    pub(crate) target: JoinTraceTarget,
    pub(crate) strategy: JoinStrategy,
    pub(crate) placement: JoinPhysicalPlacement,
    pub(crate) reason: JoinStrategyDecisionReason,
}
pub(crate) struct OptimizerTrace { pub(crate) join_decisions: Vec<DecisionReceipt> }
```
Identity is a **deterministic `join_index`** (bind-order, self-join-safe). Human
labels / qualifiers are deferred to the public-EXPLAIN ADR (Codex tightening) — the
index is the stable test key now.

**D2 — Visibility: `pub(crate)`, required.** `JoinStrategyDecisionReason` +
`HashByDefaultReason` + `BroadcastBuildSide` + `JoinPhysicalPlacement` are
**module-private today** (grounded: bare `enum` at `binder.rs:193/199/205/219`).
Bump to **`pub(crate)`** so tests (and later the EXPLAIN render) can read them. NOT
`pub` — they stay crate-internal, never a public API or wire type.

**D3 — `pub(crate) bind_with_trace` is the single producer; always-collect.** Add
`pub(crate) bind_with_trace(...) -> Result<(ExecutionPlan, OptimizerTrace), …>`.
The binder **always** accumulates the trace during binding (a `Vec` of ≤ #joins
entries — negligible vs the plan); the existing `bind` becomes a thin wrapper that
calls `bind_with_trace` and drops the trace. Thread a `&mut OptimizerTrace` through
the private recursive binding path (Codex tightening) — **no `Option`/`RefCell`**
state on `Binder`.

**D4 — Records every join decision.** A receipt is emitted for `PinnedByAuthor`
hints, every `HashByDefault` reason (incl. `ProbeNotScanLinear`,
`MissingBuildByteUpperBound`, …), AND inferred broadcasts — the trace must show
*why a join stayed hash*, the most common operator question.

**D5 — Side-effect-free.** The produced post-projection `ExecutionPlan` is
structurally identical whether bound via `bind` or `bind_with_trace`; trace
collection never influences the plan.

**D6 — Binder-local; off `ExecutionPlan` and wire.** `OptimizerTrace` and the reason
types are `pub(crate)`, NOT added to `ExecutionPlan` and NOT serialized on the wire.
Preserves the 129–132 no-wire / no-substrate guards; no protocol commitment while
the optimizer internals still move.

## Behavior gates
- `trace_records_broadcast_inference_with_rejected_magnitude` — both-eligible
  right-dominance join → receipt with `BroadcastInferred { build_side: Right, …,
  rejected_side_bytes: Some(_) }`.
- `trace_records_hash_reason_for_non_scan_linear_probe` — nested-join probe →
  receipt with `HashByDefault { reason: ProbeNotScanLinear }`.
- `trace_records_pinned_strategy_intent` — `/*+ BROADCAST */` → receipt with
  `PinnedByAuthor`.
- `trace_indexes_each_join_deterministically` — a 2-join plan yields 2 receipts
  with stable `join_index` ordering (self-join-safe).
- `bind_remains_structurally_identical_with_and_without_trace` — `bind` and
  `bind_with_trace` return the same post-projection plan shape (D5). No canonical
  plan digest exists today, so the gate should use a focused structural comparison
  or stable debug representation rather than inventing a digest substrate.

## Architecture guards
- `adr133_optimizer_trace_is_binder_local` — trace types + recording only in `binder.rs`.
- `adr133_trace_types_are_crate_private_not_public` — `OptimizerTrace` /
  `DecisionReceipt` / the reason enums are `pub(crate)`, never bare `pub`.
- `adr133_no_trace_on_execution_plan_or_wire` — trace/reason types absent from
  `ir/execution.rs`, `wire.rs`, `sql_wire.rs`, `distributed/`.
- `adr133_bind_is_wrapper_over_bind_with_trace` — single trace producer; `bind` delegates.
- `adr133_no_executor_fragmenter_scheduler_or_wendelon_change`.
- 129–132 guards stay green.

## Non-goals
No public `EXPLAIN` SQL statement or result rows (→ ADR 134, renders from this
trace). No trace on `ExecutionPlan` or wire. No human qualifier labels yet (index
only). No row/operator-level cost or cardinality metrics. No new strategy logic —
pure observability over 129–132.

## Council history
iter-0: Claude named the dark/invisible CBO; Codex picked the trace substrate;
Gemini picked multi-way. iter-1: Gemini conceded multi-way → trace; Claude led
public EXPLAIN then conceded → trace-first, contributing `pub(crate)` (reason enums
are module-private — required), always-collect over `Option<RefCell>`, deterministic
target. iter-2: Codex grounded that public EXPLAIN *is* feasible through existing
row-result machinery — Claude briefly reversed toward it, but the council +
operator selected **trace receipts first** (EXPLAIN as the immediate follow-on);
Gemini adopted Claude's iter-1 refinements; Codex added the `join_index`-only
identity and `&mut`-not-`RefCell` threading. Genuine 3-of-3 on trace-first. The
EXPLAIN-feasibility grounding isn't wasted — it de-risks ADR 134
([[feedback_ground_the_premise_before_sketching_scope]]).

## Follow-ons
**ADR 134** — public `EXPLAIN <query>`: lower the (currently `unsupported`)
`Statement::Explain`, bind via `bind_with_trace`, and render trace rows through the
existing SQL result path, likely by adding an EXPLAIN-shaped system table plan. No
new wire message should be needed. Then Wendelon-backed live facts (cross-ring) →
conservative default-on (EXPLAIN validates it) → recursive fragmentation /
multi-way → selectivity.

## Promotion criteria
`bind_with_trace` emits a deterministic receipt per join (broadcast/hash/pinned)
carrying the full 129–132 reason incl. `rejected_side_bytes`; `bind` and
`bind_with_trace` produce structurally identical plans; trace + reason types
`pub(crate)`, off `ExecutionPlan`/wire; the five `adr133_*` guards + 129–132 guards
green; `cargo test -p toron`, `--test architecture_test -- adr133`, `cargo check -p
toron --all-targets`, `git diff --check` all green.
