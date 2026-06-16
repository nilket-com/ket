# Implementation Plan — ADR 248: Multiple Window Specifications Per Query

> Depends on **ADR 247 (stacked exchange boundaries)** for distributed execution. The lowering
> here is single-node-complete on its own; the distributed gate requires 247.

## Summary

Lift the one-spec-per-query restriction in `lower_select_window`. Group window calls by their
(PARTITION BY, ORDER BY) spec and lower one `Window`-over-`Exchange` node per distinct spec,
stacked first-seen-innermost. Remove the `sql_planner.rs:3834` reject. No binder or executor
change (they recurse over stacked Window nodes); distributed fragmentation comes from ADR 247.

## Verify-first probe (already run; recorded here)

The original claim "the substrate already stacks (incl. the fragmenter)" was **half right** and
this probe is what corrected it:

1. Hand-built / lowered stacked plan **binds** (output schema = input fields + both specs'
   window columns) — confirmed.
2. **Single-node execution** computes each window column under its own partitioning — confirmed
   (the 47 `window_test` gates, incl. `multiple_specs_compute_independent_values`).
3. **Distributed `fragment_plan`** rejects two exchange boundaries with
   `MultipleExchangeBoundaries` — the seed fragmenter's one-exchange limit. This is the gap
   ADR 247 closes; do not implement the distributed gate here until 247 lands.

## Phase 1 — Lowering: group by spec, stack the nodes

Rewrite `lower_select_window` (`sql_planner.rs:3800`):

- Walk the projection once. For each item, `extract_window_call`. Assign each window call a
  **globally unique** internal output name from a running counter
  (`window_internal_output_name(window_index)`, `window_index += 1`) in left-to-right order.
- Bucket each call into a **spec group** keyed on (partition_by, order_by): find an existing
  group whose spec satisfies `window_specs_match(group.partition, group.order, call.partition,
  call.order)`; if found, push the call's `WindowFunctionCall` there; else open a new group
  (preserving first-seen order). The frame is **not** consulted (it rides the call).
- Non-window items pass through unchanged; reuse the existing
  `window-inside-compound-expression` reject.
- Build the stack by folding the groups, first-seen-innermost. Forward iteration achieves
  this: the first iteration wraps the input directly (first-seen spec adjacent to the input);
  later specs wrap outward. (Traced: `.rev()` would put first-seen *outermost* — the opposite —
  and stack order does not affect column values anyway, only determinism. The landed
  implementation uses forward iteration to match the prose.)
  ```
  let mut node = input_plan;
  for group in groups {  // first-seen group innermost (forward iteration)
      node = LogicalPlan::Exchange {
          input: Box::new(node),
          partition_count: if group.partition_by.is_empty() { 1 }
                           else { DEFAULT_AGGREGATE_PARTITION_COUNT },
          key: group.partition_by.clone(),
      };
      node = LogicalPlan::Window {
          input: Box::new(node),
          partition_by: group.partition_by,
          order_by: group.order_by,
          functions: group.functions,
      };
  }
  ```
- Return `(node, rewritten)` where `rewritten` references each internal name in user SELECT
  order (unchanged from today — the `Projection` above the stack restores column order).
- **Delete** the `"multiple distinct OVER specifications in one query are not supported"`
  reject and the single-`spec` accumulator.

Keep the empty-PARTITION-BY → `partition_count = 1` gather rule per group (matches today's
single-spec behavior and `adr241_over_spec_lowers_to_window_over_partition_exchange`).

## Phase 2 — Confirm no downstream change (bind/exec only)

Run the real lowering through bind + single-node execute on the headline query. Expect green
with no edits to `binder.rs` or `exec.rs`. Distributed execution depends on ADR 247; the
distributed gate (below) is held until 247 lands. If `--all-targets` surfaces an exhaustiveness
break, it would only be from a new IR variant — and this cut adds none, so none is expected.

## Phase 3 — Tests, guards, verification, seal

### Behavioral gates (`tests/window_test.rs`)

- `multiple_specs_plan_to_distinct_window_nodes` — `SELECT SUM(score) OVER (PARTITION BY grp),
  SUM(score) OVER () FROM wins` plans with `count_window_nodes == 2` (rejects on `main`).
- `multiple_specs_compute_independent_values` — same query, asserting the per-group column
  (`PARTITION BY grp` → 40 for group a) differs from the grand-total column (`OVER ()` → 50);
  each is correct only under its own partitioning. Reuses `wins`, no new fixture.
- `same_spec_calls_share_one_window_node` — `count_window_nodes == 1` for two calls, one spec.
- `same_partition_order_different_frames_share_one_node` — two ROWS frames, same partition+order
  → `count_window_nodes == 1` (locks grouping key = partition+order, not frame).
- `stacked_window_output_order_preserved` — a multi-spec query returns rows in input order and
  columns in user SELECT order despite the per-spec `Exchange` re-partition (folded from Codex
  iter-1).
- Reconcile `deferred_window_forms_stay_rejected` (`window_test.rs:168` has the multiple-distinct-
  OVER case) — drop that case (it now plans); keep named-window / IGNORE NULLS / NTILE rejects.

Reuse harness: `run_window`, `count_window_nodes` (`window_test.rs:1029`),
`first_window_functions`, `group_values`, the `wins` fixture (grp a: scores 10,10,20; grp b:
5,5). No new fixture needed — `PARTITION BY grp` vs `OVER ()` are two distinct specs.

### Distributed gate (`tests/distributed_process_test.rs`) — requires ADR 247

- `multiple_specs_distributed_gate_is_value_discriminating` — over `set_left`, a grand-total
  spec (`SUM(set_left.id) OVER ()`, read as col1 = 10) next to a per-set_key spec
  (`ROW_NUMBER() OVER (PARTITION BY set_left.set_key ORDER BY set_left.id)`, the unread col2 that
  forces the second distinct spec): the `OVER ()` total is 10 only if the empty-spec exchange
  gathers every row across both workers. Assert `assert_cross_producer_cardinal_is_value_discriminating`.
  Fresh seed. **This gate fails with `MultipleExchangeBoundaries` until ADR 247 lands** — add it
  only once 247 is in.

### Architecture guards (`tests/architecture/window_functions.rs`)

Add:
- `adr248_lowering_stacks_one_window_per_distinct_spec`
- `adr248_spec_grouping_key_is_partition_and_order_not_frame`
- `adr248_multiple_specs_add_no_new_node_or_executor_surface`
- `adr248_multiple_specs_distributed_gate_is_value_discriminating`

Reconcile:
- `adr241_single_over_spec_per_query_multiple_specs_deferred` — rename + retarget: assert
  `window_specs_match` is used as the grouping predicate and the single-spec reject string is
  gone, while same-spec calls still collapse to one node. Update the module-doc line that says
  "the single-`OVER`-spec bound".

### Verification gate (route every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test window_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test multiple_specs
scripts/cargo-cgroup test  -p toron --test architecture_test adr247
scripts/cargo-cgroup test  -p toron --test architecture_test        # FULL — variant/count guards
scripts/cargo-cgroup test  -p toron --test distributed_process_test # FULL
scripts/cargo-cgroup test  -p toron --lib                           # serde/postcard (no IR shape change expected)
cargo fmt -p toron -- --check
git diff --check
```

The `--lib` run is a backstop: this cut adds no IR field/variant, so postcard goldens should be
untouched — confirm rather than assume.

### Seal

Commit exactly the implementation files as `impl: adr 248 multiple window specifications`, with
the `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.

## Risk notes

- **Deep stacks**: many distinct specs → many exchange+sort stages. Inherent to distinct
  partitionings; sharing sorts is an optimizer follow-on, out of scope.
- **Internal-name collision**: the global counter (not a per-group index) is load-bearing —
  per-group indices would collide across specs in the final schema. The
  `multiple_specs_compute_independent_values` gate would catch a collision (a column would read
  the wrong values).
- **Stack-order nondeterminism**: first-seen-innermost is the deterministic rule; a HashMap-
  ordered grouping would make plans unstable. Use an insertion-ordered group list.
