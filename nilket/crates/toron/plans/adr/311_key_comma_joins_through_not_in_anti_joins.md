# ADR 311 — key comma joins through NOT IN anti-joins

## Status
Proposed.

## Context

ADR 310 correctly unblocked q16's uncorrelated `NOT IN` inner-residual path, but the review measured q16 riding a
materialized cartesian. At `sf=0.01`, `partsupp` (8000 rows) × `part` (2000 rows) produces about 16M intermediate
rows and roughly 8 GB RSS. The tiny `sf=0.0001` test hid that O(n^2) shape, so ADR 310's q16 row is only
"executes via cartesian", not a true keyed reach cell.

The mechanism is not `LogicalPlan::DeferredExists`. q16's uncorrelated `NOT IN` lowers through the ADR 258 path:
a null-`x` filter, a null-guard `LeftAnti`, and a membership `LeftAnti`. The remaining outer WHERE equality
`p_partkey = ps_partkey` sits above those anti-joins. `recognize_cross_equi_joins` recurses into the anti-join
children, but it never pushes that left-output-only filter through the anti-join recombine, so the underlying
`partsupp, part` `Cross` never sees the equality and cannot be keyed.

This is the ADR 282 lesson in the semi/anti-join path: a left-preserving recombine can strand a comma-join
equality above the place where the cross-equi recognizer works. ADR 311 adds the missing push-through for
left-output-only filters over `LeftSemi`/`LeftAnti` joins. q16's motivating case is the stacked `LeftAnti` pair
from `NOT IN`.

## Decision

### D1 — Push left-only filters through semi/anti joins

In `binder/outer_join_demotion.rs`, extend `recognize_cross_equi_joins` for `Filter` over a `Join` with
`JoinKind::LeftSemi` or `JoinKind::LeftAnti`:

- Split the filter predicate into conjuncts.
- Use the existing catalog-backed scope machinery (`logical_scope_for_outer_demotion` +
  `conjunct_column_sides`) to identify conjuncts that reference only the join's left/output side.
- Push only those conjuncts into the join's left child, then recurse with `recognize_cross_equi_joins` so an
  underlying comma-join `Cross` can lift its equality into keyed `Inner`.
- Keep any ambiguous, right-side, or cross-side conjunct above the semi/anti join. In normal bound output scope
  those right-side references should not be legal, but keeping rather than guessing preserves the existing error
  path.
- Rebuild the original semi/anti join unchanged except for its rewritten left child.

Keep the helper as a separate future, like `lift_cross_equi` and the ADR 282 deferred-scalar helper, so the
recursive dispatch's poll frame does not grow in the main match arm.

### D2 — Preserve NOT IN semantics

Do not rewrite the `NOT IN` anti-join keys, null guard, subquery filter, or null semantics. The only legal change
is moving a filter that references the anti-join output rows into that same left input before the anti join runs:

`Filter_left(LeftAnti(left, right)) == LeftAnti(Filter_left(left), right)`

The q16 answer digest must stay unchanged. Only the plan shape and memory profile should change.

### D3 — Make the anti-mask durable

A tiny-scale `pass@execute` is not enough for comma-join-over-subquery shapes. This cut needs a deterministic
keyed-plan assertion: q16's `partsupp ⋈ part` edge is an equi-`Inner` join after recognition, not a keyless
`Cross` hidden under the `NOT IN` anti-joins. The live default-scale run remains useful evidence, but the
plan-shape assertion is the regression gate.

## Acceptance

- q16's `partsupp, part` edge is keyed by `p_partkey = ps_partkey` under the `NOT IN` anti-join stack.
- q16's result digest is unchanged at the existing tiny test scale and at default scale.
- The default `sf=0.01` q16 run no longer shows the ADR 310 cartesian profile; it should drop from the measured
  multi-GB/O(n^2) behavior to keyed-join memory and time.
- A focused engine test proves a filter over `LeftAnti` can push left-only equality into the left child and key an
  underlying comma join; reverting the helper leaves the cross unkeyed.
- A negative/guard test keeps ambiguous or right-side conjuncts above the semi/anti join rather than pushing them.
- Full `-p toron --no-fail-fast`, `--all-targets`, `architecture_test`, and `toron-tpch` remain green.

## Risks

- **Semantics:** pushing a predicate that depends on the anti-join right side or on anti-join match state would be
  wrong. Push only left-only conjuncts proven by relation scopes.
- **Masking:** timing-only tests can miss this at small scale. Keep the keyed-plan assertion.
- **Shared recognizer path:** `recognize_cross_equi_joins` runs for every query; limit the new rewrite to
  `Filter` over `LeftSemi`/`LeftAnti` and rely on the full suite.
- **Stack:** keep the recursive work in a separate helper future.

## Forward arc

After this cut, q16 can honestly count as the keyed reach-17 cell. q16 correctness is then a natural rail catchup,
but the recorded fixture fact still matters: `%Customer%Complaints%` matches no supplier at `sf=0.01`, so the
correctness oracle needs a larger scale or synthetic coverage to exercise the anti-join exclusion branch.

q11/q15 remain likely-green coverage; q19/q21 remain the heavier reachability frontier. The keyed-plan assertion
habit should ride future comma-join-over-subquery reach probes so cartesian passes cannot bank fake greens.

## Why toron-home

This is a `crates/toron` binder rewrite in `recognize_cross_equi_joins`. q16's harness row flips from
executes-via-cartesian to keyed execution without changing the SQL, fixtures, executor, or fragmenter.

## Council provenance

Operator-directed after the ADR 310 code review approved the `NOT IN` filtering fix but found q16's reach row was
a cartesian. The first draft framed the issue as `DeferredExists`; review against current lowering corrected the
target to the ADR 258 `LeftAnti` pair produced by uncorrelated `NOT IN`.
