# ADR 248 — Multiple Window Specifications Per Query

> Depends on **ADR 247 (stacked exchange boundaries)**. Distributed execution of multiple
> specs requires the seed fragmenter to fragment a stacked-exchange plan; ADR 247 provides that
> substrate. This ADR is the lowering + local-execution layer that produces the stacked plan.

## Status

Proposed.

## Context

The window substrate (ADR 241) and the function families built on it — ranking (241),
aggregate (242), the three frame units (243 ROWS / 244 RANGE / 245 GROUPS), and value
lookups (246) — all share one structural restriction: **a query may carry only one OVER
specification.** `lower_select_window` (`sql_planner.rs:3800`) funnels every window call in
a SELECT into a single `Window`-over-`Exchange` node, and the moment two calls disagree on
PARTITION BY / ORDER BY it rejects:

```
sql_planner.rs:3834  "multiple distinct OVER specifications in one query are not supported"
```

This blocks the most common real analytic shape — several windows over *different*
partitionings in one query:

```sql
SELECT customer,
       RANK()       OVER (ORDER BY sales)                       AS global_rank,
       ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales)   AS region_seq
FROM orders
```

`global_rank` is partitioned globally; `region_seq` is partitioned by region. They are two
distinct specs, so today the query rejects. Without multiple specs, window support is a toy:
every dashboard query that ranks globally *and* sequences within a group needs two specs.

### Grounding — what stacks today, and what ADR 247 unblocks

The `Window` node is handled recursively at the binder, executor, and *local* fragmenter:

- the binder binds a Window node's input recursively, then wraps it
  (`binder.rs:1300`), and the output schema is *input fields + one column per function*
  (row-preserving, N→N — pinned by `adr241_window_is_row_preserving_not_an_aggregate`);
- `execute_window` (`exec.rs:3625`) processes one Window node and recurses into its input;
- the single-node executor runs a stacked chain `Window → Exchange → Window → Exchange → Scan`
  directly.

So a stacked plan binds and executes **single-node** with no change. The blocker was
**distributed**: the seed fragmenter (`fragment.rs`) accepted only one exchange boundary and
rejected a two-spec plan with `MultipleExchangeBoundaries` — which is exactly what the original
single-spec reject deferred (`sql_planner.rs` doc-comment: *"the seed fragmenter allows one
exchange boundary"*). **ADR 247 lifts that limit** (stacked exchange boundaries). This ADR is
therefore two complementary pieces: the lowering that emits the stacked plan (here) on top of
ADR 247's fragmenter substrate. The earlier framing of this as "lowering only, the fragmenter
already handles it" was wrong — it conflated the local fragmenter with the distributed seed
fragmenter.

## Decision

`lower_select_window` **groups window calls by their OVER spec and lowers one
`Window`-over-`Exchange` node per distinct spec, stacked.** The single-spec reject is removed.

```
Projection (user column order, by internal name)
└─ Window(spec_N, funcs_N)
   └─ Exchange(key = spec_N.partition)
      └─ ...
         └─ Window(spec_1, funcs_1)
            └─ Exchange(key = spec_1.partition)
               └─ <input plan>
```

### The grouping key is (PARTITION BY, ORDER BY) — not the frame

Two window calls share a Window node exactly when they share **partition and order** — the
expensive work the node embodies (the `Exchange` re-partition and the partition sort). The
**frame is not part of the grouping key**: it travels per-call on
`WindowFunctionCall.frame` and is a cheap per-row slice already resolved per-call since
ADR 246. So:

- `SUM(x) OVER (ORDER BY a ROWS 1 PRECEDING)` and `SUM(x) OVER (ORDER BY a ROWS 2 PRECEDING)`
  → **one** Window node (same partition+order, two per-call frames);
- `SUM(x) OVER (PARTITION BY a)` and `SUM(x) OVER (PARTITION BY b)`
  → **two** Window nodes (distinct partitionings, two exchanges).

This reuses the existing `window_specs_match` (`sql_planner.rs:7290`) — which already compares
partition+order structurally — now as the **grouping predicate** rather than a reject gate.

### Stack order and naming are deterministic

- Distinct specs are kept in **first-seen order** across the projection; the first-seen spec
  is lowered innermost (closest to the input). Order between independent specs is a plan-
  stability choice only — each Window is row-preserving and schema-additive, so the final row
  set and schema are identical regardless of stack order; first-seen-innermost makes the plan
  deterministic.
- Each window call is assigned a **globally unique** internal output name in projection
  (left-to-right) order, so columns from different specs never collide in the final schema.
  The rewritten projection references those names in the user's SELECT order, so output column
  order is preserved (the `Projection` above the stack handles ordering, exactly as today).

### Why this is correct with no new IR or executor surface

Each Window node preserves all input columns and appends its own; `Exchange` is schema- and
row-preserving. So spec_1's columns survive up through spec_2's Window+Exchange, and the
topmost Window's output schema carries every input column plus every window column. The binder
and executor already recurse over this shape (grounding above), and the distributed
fragmentation is provided by ADR 247 — the cut adds **no** new logical/physical node, no binder
arm, and no executor case.

## Consequences

- Real multi-partition analytic queries plan and execute (single-node immediately; distributed
  once ADR 247 lands).
- Each distinct spec is its own exchange boundary; an N-spec query fragments into N+1 stages
  via ADR 247's stacked-exchange fragmentation — no window-specific distributed mechanism.
- A query with many distinct specs builds a deep Window stack (one re-partition + sort per
  spec). That is the inherent cost of distinct partitionings; sharing the sort across specs is
  an optimizer concern, explicitly **not** in this cut.

## Discriminators (each fails under the rejected single-spec alternative)

- **`multiple_specs_plan_to_distinct_window_nodes`** — `SELECT SUM(x) OVER (PARTITION BY a),
  SUM(x) OVER (PARTITION BY b) FROM t` plans with `count_window_nodes == 2`. On `main` this
  *rejects*, so the test cannot pass without the cut.
- **`multiple_specs_compute_independent_values`** — two specs with different partitioning over
  one fixture, e.g. `SUM(score) OVER (PARTITION BY grp)` (per-group sum) next to
  `SUM(score) OVER ()` (grand total): the two columns are correct only under their own
  partitioning and must differ. A single-spec funnel produces a wrong column for one of them.
  (Locks correctness, not just node count — and needs no new fixture.)
- **`same_spec_calls_share_one_window_node`** — two calls with one shared spec →
  `count_window_nodes == 1` (guards against over-stacking N nodes for N calls).
- **`same_partition_order_different_frames_share_one_node`** — same partition+order, two
  different per-call ROWS frames → `count_window_nodes == 1` (locks the grouping key =
  partition+order, not frame).
- **`stacked_window_output_order_preserved`** — a multi-spec query keeps user SELECT column
  order and input row order through the re-partitioning stack (each spec's `Exchange` reorders
  rows internally; the top `Projection` restores user order). Guards against the stack leaking
  re-partition order into the result.
- **`multiple_specs_distributed_gate_is_value_discriminating`** — a global `OVER ()` total next
  to a per-set_key spec over the distributed fixture: the total is correct only if the
  empty-spec exchange gathers every row across both workers, alongside a distinct per-key
  exchange (236/238 cross-producer cardinal). This gate **requires ADR 247** — on the
  one-exchange seed fragmenter it fails with `MultipleExchangeBoundaries`.

## Architecture guards

- `adr248_lowering_stacks_one_window_per_distinct_spec` — `lower_select_window` groups by spec
  and builds a `Window`-over-`Exchange` per group; the single-spec reject string is gone.
- `adr248_spec_grouping_key_is_partition_and_order_not_frame` — `window_specs_match` is the
  grouping predicate; the frame is not consulted in grouping.
- `adr248_multiple_specs_add_no_new_node_or_executor_surface` — no new logical/physical Window
  variant and no executor case (the stack rides the existing recursive handling + ADR 247's
  fragmentation).
- `adr248_multiple_specs_distributed_gate_is_value_discriminating` — the distributed gate uses
  the 236/238 cross-producer cardinal discipline.
- Reconcile **`adr241_single_over_spec_per_query_multiple_specs_deferred`** → the single-spec
  fact is lifted; retarget it to assert calls sharing a spec collapse to one node while distinct
  specs stack (the "deferred" claim is now false).

## Deferred

- **NTILE(n)** — a distinct mechanism (computed bucket, not a partition/order grouping). ADR 249.
- **Named windows** (`WINDOW w AS (...)`) — frontend desugar; ADR 249-or-later.
- **IGNORE / RESPECT NULLS** — value-function null-skipping mechanism.
- **EXCLUDE** frame clause — sqlparser 0.62 carries no field; external blocker.
- **Shared sort/exchange across specs** — optimizer concern; out of scope.

## Forward arc

247 stacked exchange boundaries → 248 multiple OVER specs (this) → 249 NTILE (+ named windows) →
window arc complete (IGNORE NULLS and EXCLUDE deferred on mechanism / external grounds).
