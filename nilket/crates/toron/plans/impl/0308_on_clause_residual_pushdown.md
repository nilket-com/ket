# ADR 308 — Implementation plan: ON-clause residual pushdown (single-input residuals)

Pairs with `plans/adr/0308_on_clause_residual_pushdown.md`. Engine cut (`crates/toron`: lowering + catalog-aware
binder normalization), reusing the logical side-classification machinery in `binder/outer_join_demotion.rs`.
**No `ExecutionPlan::Join` / exec / fragmenter change.** Plus a shared `o_comment` fixture add in `toron-tpch`.
Anchors are current-tree.

## Phase 0 — confirm the anchors
- `lower_join_on` (sql_planner:6100) rejects non-equality conjuncts at `:6110`. `LogicalPlan::Join { …, on:
  Vec<(Expr,Expr)>, … }` (ir.rs:1165) — key-only.
- Reuse the catalog-aware logical side classifier from `binder/outer_join_demotion.rs`
  (`logical_scope_for_outer_demotion`, `conjunct_column_sides`, `push_logical_filter_conjuncts`) rather than
  `binder/pushdown.rs`, which is post-bind scan/PBC pushdown over `BoundExprProgram`.
- Confirm how a residual's input-side is determined (which `Expr` columns resolve to left vs right), including
  the no-column/constant case — the pushability classifier needs it.

## D1 — lowering: collect the residual (`sql_planner.rs`)
- `lower_join_on` returns `(Vec<(Expr, Expr)>, Option<Expr>)` — equi-keys + the conjunction of non-equality ON
  residuals (instead of rejecting at `:6110`). `lower_join_operator` threads the residual to a transient logical
  `on_filter` on the join (logical-only; not on `ExecutionPlan::Join`).

## D2 — binder: classify + push (or reject) (`binder/outer_join_demotion.rs` + join planning)
For a join with an `on_filter`:
- determine the residual's referenced input(s) (left / right / both) from its column refs;
- **push** when single-input and pushable for the kind:
  - `Inner` + single-side → `Filter(side)` (reuse the side-local WHERE pushdown);
  - `LeftOuter` + right-(null-supplying)-only → rewrite to `Join(left, Filter(right, residual), keys, LeftOuter)`;
  - `RightOuter` + left-(null-supplying)-only → symmetric on the left input;
- **reject precisely** otherwise (`Inner` is always pushable single-side; OUTER preserved-side-only, any
  cross-side, FULL OUTER, no-column/constant residual, or residual with no equality key) → a clear
  `unsupported(...)` naming the theta-residual gap (the future ADR). Keep the message narrow so targeted tests
  can assert the boundary.
- The rewrite produces an ordinary `Filter` below the join → the existing exec + fragmenter handle it unchanged
  (distributed works for free).

## D3 — fixture: add `orders.o_comment` (`toron-tpch/src/lib.rs`)
Append `o_comment` (Utf8, non-null) to `orders_schema` + populate from `OrderGenerator.o_comment` in
`generate_orders`. Then run the FULL `-p toron-tpch` suite + both matrices + any orders-schema/golden assertion to
prove the existing orders-backed rows are unmoved (q03/q04/q05/q10/q12/q22; q18 is not implemented yet).

## Tests
- **Engine** (binder/exec test home):
  - `LEFT OUTER JOIN r ON l.k=r.k AND r.pred` → rewrites to `Filter(r)` below the join; executes with **correct
    null-extension** — a left row whose only key-matching right row **fails** `r.pred` emits **one null-extended
    row** (not dropped); pin the count (`COUNT(r.col)=0` for it).
  - `INNER JOIN r ON l.k=r.k AND r.pred` (or `l.pred`) pushes the residual into that side; same result as the
    equivalent WHERE.
  - **rejects precisely:** `LEFT OUTER … ON l.k=r.k AND l.pred` (preserved-side); `… AND l.x < r.y` (cross-side);
    a FULL OUTER residual; and `JOIN ... ON l.x < r.y` with no equality key. (The theta boundary.)
  - same-shape with no residual unchanged. Discriminating: revert D1/D2 → rejects at sql_planner:6110.
- **Architecture:** any guard pinning the `lower_join_on` reject string / join-shape reconciled; full
  `architecture_test`.
- **Gauge** (`toron-tpch`): verify-first re-measure q13 (with `o_comment`).

## Phase 3 — verify-first re-measure q13
```
target/debug/toron-tpch run --query q13 --mode dev   --sf 0.01 --allow-blocked
target/debug/toron-tpch run --query q13 --mode strict --sf 0.01 --allow-blocked
```
- **Success = q13 moves OFF `unsupported_non_equi_join_on@plan`** to its next wall (the count-of-counts
  `GROUP BY <agg-alias>`, or further), measured + classified honestly. `pass@execute` not required. Update the q13
  harness row to the new measured shape; if the next wall lacks a precise classifier, record the raw error (names
  ADR 309).

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) — FULL toron + the fixture
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q13 --mode dev --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q13 --mode strict --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/0308_on_clause_residual_pushdown_results.md`: the pushability classification (push vs
reject matrix); the null-extension equivalence + its test; q13's new measured wall (off
`unsupported_non_equi_join_on` → what's next, naming ADR 309); the `o_comment` add + same-suite no-regression
(15/15 boards unchanged).

## Risk / blast radius
- **Pushability classifier** — the load-bearing logic; pin push (null-extension preserved) AND reject
  (preserved-side / cross-side / FULL OUTER). Over-push = wrong outer results; over-reject = q13 stays red.
- **Shared `o_comment` add** — full `-p toron-tpch` + goldens.
- **No `ExecutionPlan::Join` change** — the logical rewrite is a `Filter` below the join; exec + fragmenter
  unchanged (distributed for free). Confirm no residual leaks onto the bound join node.
- **Don't smuggle** the theta-residual (cross-side/preserved-side) — reject + defer to a future ADR.

## Seal
`impl: adr 308 on clause residual pushdown`. Do not commit wip drafts.
