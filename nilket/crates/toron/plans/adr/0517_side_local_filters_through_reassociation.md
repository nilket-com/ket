# ADR 517 - Preserve side-local filters through comma-join reassociation

ADR 516's attribution ranked one shared premise for q08 and q09: the part-suffix join probes all
47,989,007 lineitem rows at multiplicity exactly 1.000 and materializes the full-width output
(5.0 / 6.4 GB of Arrow) that the immediately following `suffix_filter` then cuts. The defect
producing that shape is already pinned, and it is narrower than any new executor path: during
cross-to-inner reassociation, the binder **classifies** `p_type`/`p_name` as side-local
(`left_only`/`right_only` at the conjunct classifier) and then deliberately appends them back to
the root residual (`reassociated_residual.extend(...)`, binder/outer_join_demotion.rs:928 area).
They consequently ride the runtime as `post_filter` and execute as `suffix_filter` predicates
after full join materialization (exec/join/streaming.rs:12635 area).

The cut is a bounded completion of ADR 401's existing side-local rule (outer_join_demotion.rs:1229
extends it to non-Cross shapes): after reassociation, route safe, single-owner predicates to
their source branch instead of the residual. This is not a late-materialization executor path,
not suffix-lookup optimization, and not join reordering.

## The transformation and its proof

The standard bag-semantics identity:

```text
sigma_p(L join R) = L join sigma_p(R)    when p references only R's columns
```

SQL NULL behavior is preserved (a false-or-unknown `p` eliminates the same rows on either side),
duplicates and multiplicity are preserved (the filter is row-local on R either way), and the
spine's equi-keys, strategy intents, exchanges, and output schema are unchanged. The predicate is
**moved once, never cloned**.

Evaluation-order hazards get a declining gate, all three conditions required:

- single-branch ownership (the existing exclusive `conjunct_column_sides` classification);
- `is_deterministic()`;
- the existing `guard_filter_pushdown_safe` (the same fence outer-join demotion trusts).

Both motivating predicates pass all three. Volatile, ambiguous, mixed-side, strict-cast,
arithmetic, and other potentially erroring expressions remain residual, byte-for-byte as today.

For q08/q09 the effect is that part is filtered before its suffix runtime builds:
q08's part build 1,600,000 → **10,801** rows; q09's 1,600,000 → **87,125**.

## The economics, stated before measurement

The directly priced removable work from the ADR 516 receipts is modest and per-cell:

| cell | expansion + payload take + filter | directly removable |
|---|---|---:|
| q08 | 618.7 + 1,549.0 + ~303 ms | ~2.47 s |
| q09 | 626.0 + 1,670.3 + ~522 ms | ~2.82 s |

Both sit **below** the inherited 3,554 ms floor on the directly priced component alone. A smaller
build may also reduce preparation and lookup cost, but ADR 516's discipline forbids assuming it.
Phase 0 therefore carries real authority: if neither shape twin clears the floor, ADR 517 closes
red for the cost of the probe, and the receipt says exactly which component fell short.

## Decision

1. Complete the ADR 401 rule at the reassociation site: route `left_only`/`right_only` conjuncts
   passing the three-condition gate onto their source branch as a branch `Filter`; everything
   else stays residual. One code path, no knob asymmetry between shapes, no change to keys,
   intents, exchanges, or schemas.
2. Typed, latched control: `PlannerConfig::side_local_filter_routing`, compiled default off at
   the seal; gauge env `TORON_TPCH_SIDE_LOCAL_FILTER_ROUTING`, loud contract, row fields with
   provenance. Plans change, so cohorts run under the plan-distinct gate.
3. Measurement is **per cell**: separate q08 and q09 cohorts against the 3,554 ms floor; the two
   cells are never summed to manufacture materiality. Activation per ADR 472's rule requires at
   least one `activate` verdict and no engaged-cell regression; the receipt reports both cells
   either way.

## Acceptance

1. **Phase 0 shape twin, before mechanism code**: run the equivalent queries with explicitly
   prefiltered part branches (the SQL twin of the intended bound shape), proving the intended
   plan shape binds and pricing the attainable wall-time per cell. No summing across cells. If
   neither twin clears the floor, close red here.
2. **Structural battery**: left-cross and right-cross reassociation both route; the predicate
   appears exactly once in the bound plan (single filter receipt, no clone); join metadata
   (keys, intents, exchange keys/counts, output schema) identical to today's except the branch
   filter; explicit declines for volatile, ambiguous, mixed-side, strict-cast, and arithmetic
   predicates — plan byte-identical.
3. **Semantic battery**: NULL-bearing filter columns, duplicate source rows, duplicate join
   matches, always-false and unknown predicates, and error-fence fixtures (a predicate that
   would error on rows the join would have eliminated must stay residual via the gate).
4. **Census evidence**: engaged set determined empirically across q01-q22 and m00-m10; q08/q09
   must show the filter below the part build, suffix-1 emissions collapsing 47,989,007 →
   **323,097** (q08) and **2,610,723** (q09), identical digests everywhere; every
   fingerprint-changed cell runs its strict correctness rail in both arms; q19 sentinel.
5. **Paired authority**: plan-distinct cohorts per cell at the fixed floor, predeclared
   estimator, no post-hoc extension; census-wide correctness and regression rails before any
   default flip.

## Guardrails

- The three-condition gate fails closed; unrouted predicates are byte-identical residuals.
- No executor changes: the suffix runtime, probe driver, and ADR 516 instrument are untouched.
- ADR 511-515 surfaces are untouched; the branch filter feeds the existing ADR 511/512 placement
  and admission machinery with better-bounded inputs, and any resulting route-fact changes are
  census-adjudicated, not assumed.

## Risks

- The economics may close red at Phase 0 or at the cohorts; both are honest outcomes the plan
  prices in advance.
- Routing filters below reassociation changes optimizer-fact flow (`filtered_without_estimate`
  marks appear on the branch); other cells may engage — the census enumerates them.
- The moved filter interacts with scan pushdown (it may become a PBC scan predicate); that is
  the existing pushdown pass's decision and stays out of this ADR's scope beyond census
  observation.

## Forward

- If activated, the remaining q08/q09 envelope re-ranks (base probe, repartition, the generic
  partsupp gap from ADR 516's row 3).
- The generic-key instrumentation gap and the scalar-prefetch decline remain named follow-ons.
