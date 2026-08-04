# ADR 311 — Implementation plan: key comma joins through NOT IN anti-joins

Pairs with `plans/adr/0311_key_comma_joins_through_not_in_anti_joins.md`. Engine cut in `crates/toron`, centered on
`binder/outer_join_demotion.rs::recognize_cross_equi_joins`.

## Phase 0 — confirm the shape

- Reproduce or inspect q16's logical plan after ADR 310:
  - `FROM partsupp, part` starts as a keyless `Cross`.
  - `ps_suppkey NOT IN (...)` lowers to the ADR 258 null-guard `LeftAnti` plus membership `LeftAnti`.
  - The remaining `p_partkey = ps_partkey` filter sits above that anti-join stack, so the cross-equi recognizer
    never sees it at the `Cross`.
- Confirm this is not `LogicalPlan::DeferredExists`; that variant is the ADR 279 unqualified correlated
  EXISTS/NOT EXISTS handoff, not q16's uncorrelated `NOT IN` lowering.

## D1 — add a semi/anti filter push-through helper

In `recognize_cross_equi_joins`, after recursively recognizing the filter input, add a case for:

```rust
LogicalPlan::Join {
    kind: JoinKind::LeftSemi | JoinKind::LeftAnti,
    ..
} => self.push_left_semi_anti_output_filter(input, predicate).await
```

The helper should:

- destructure the semi/anti join;
- split the filter predicate into conjuncts;
- derive relation scopes for the join's left and right children with `logical_scope_for_outer_demotion`;
- classify each conjunct with `conjunct_column_sides`;
- push only unambiguous left-only conjuncts into the left child;
- keep ambiguous, right-only, and cross-side conjuncts above the semi/anti join;
- call `recognize_cross_equi_joins` on the pushed left child so an underlying `Filter` over `Cross` can lift the
  equi key;
- rebuild the same `LeftSemi`/`LeftAnti` join with unchanged right child, join keys, `on_filter`,
  `null_safe_keys`, and `strategy_intent`;
- rewrap any kept conjuncts with `wrap_residual_filter`.

Keep this as a separate async helper, not inline in the recursive match arm, following `lift_cross_equi` and the
ADR 282 helper.

## D2 — tests

Add focused `crates/toron` tests:

- **q16-shaped plan keying:** `SELECT ... FROM a, b WHERE a.k = b.k AND a.x NOT IN (SELECT y FROM c WHERE ...)`
  binds/plans with the `a ⋈ b` edge as keyed `Inner`, not keyless `Cross`, under the anti-join stack.
- **Result preservation:** the q16-shaped query returns the same rows as an equivalent explicit keyed join form.
- **No over-push:** a predicate that is ambiguous or not left-only relative to the semi/anti join is not pushed
  into the left child; it stays as a residual filter or follows the existing error path.
- **Family boundary:** cover `LeftAnti` for q16. Add `LeftSemi` coverage if the helper is shared for both, since
  the algebra is the same for left-output-only filters.

Update or add `toron-tpch` coverage:

- Replace the tiny-scale-only q16 reach test with a keyed-plan assertion where practical, or add a dedicated
  helper that inspects the q16 plan before execution.
- Keep the tiny-scale digest check as a value-preservation guard.
- Run q16 at default `sf=0.01` live and record time/RSS before/after in the results doc.

## D3 — results doc

Add `crates/toron/plans/impl/0311_key_comma_joins_through_not_in_anti_joins_results.md` with:

- the corrected mechanism: `NOT IN` anti-join stack, not `DeferredExists`;
- proof that `p_partkey = ps_partkey` keys the `partsupp ⋈ part` edge;
- q16 default-scale before/after RSS and wall time;
- unchanged q16 digest;
- the updated reach board: q16 is now genuinely keyed reach, while correctness remains unchecked.

## Seal gate

Run after `fmt`:

```bash
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q16 --mode dev --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- run --query q16 --mode strict --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

The live q16 runs are part of this gate because ADR 310's tiny-scale test masked the cartesian. The plan-shape
assertion is still the durable regression guard; the live run confirms default-scale tractability.

## Risk / blast radius

- **Wrong pushdown:** only left-output-only predicates may move through a semi/anti join.
- **False green:** keep the keyed-plan assertion; do not rely on tiny-scale execution alone.
- **Shared rewrite:** full `-p toron --no-fail-fast` is required because the recognizer is global.
- **Stack growth:** keep recursion in a helper future.

## Seal

`impl: adr 311 key comma joins through not in anti joins`. Do not commit wip drafts.
