# ADR 519 - Preaggregated counts over unique-key left outer joins

q13's grounding at the sealed `ec095a23` coordinate found the fused left-outer
probe/materialization path — not the 5 MB spill — owning the cell: `left_outer_grouped` runs
8.90 s of the 8.9-9.4 s total and holds all 5,131,808 spill bytes, but spill preload/read/decode
costs ~15 ms with every page pinned — the spill is a representation fact, not an I/O cost. The
build is 0.25 s and the grouped fold 2.55 s; the remaining ~5.77 s is probe/materialization
volume: 11,869,667 qualifying orders enter the fused path and 12,269,685 rows are materialized
for grouping (including 400,018 unmatched customers). ADRs 434-437 already establish that
another generic grouping-layout optimization is a poor bet; the volume itself is the target.

A supported SQL twin proved the plan-shape economics at the exact coordinate: preaggregate
qualifying orders by `o_custkey` (799,982 count rows), left-join those to the 1,200,000 unique
customer keys, and coalesce missing counts to zero. The twin ran 8,937.96 → 3,335.83 ms
(**+5,602.13 ms**, clearing the 3,554 ms floor), spill 5,131,808 → 0, RSS ~650 → ~344 MiB,
strict correctness green; join work collapsed to 76 ms and the surviving orders aggregation
(3.18 s) became the envelope. The remedy is Toron-specific: DuckDB runs the original
join-then-group shape in ~1.42 s.

One twin finding is itself a gate: the twin returned correct rows and a matching schema
fingerprint but a **different Arrow IPC digest** — physical representation/nullability details.
The optimizer rewrite must therefore retain the original aggregate's field contract exactly,
not copy the twin literally; census digest equality is a hard requirement on the rewrite that
the twin was exempt from.

## The transformation and its proof

For a grouped count over a left outer join whose group key is the preserved side's unique join
key:

```text
Aggregate[g = k_L; COUNT(col_R)](L left_outer_join_{k_L = k_R} R)
  = Project[k_L, COALESCE(cnt, 0)](L left_outer_join_{k_L = k_R} Aggregate[k_R; cnt = COUNT(col_R)](R))
```

Uniqueness of `k_L` makes each output group exactly one preserved row's match set: its matched
`R` rows (counted identically on either side, `COUNT(col_R)` skipping nulls identically), or
the single null-extended row, whose `COUNT` is 0 — the coalesced miss. Bag semantics hold
because no other preserved row shares the group; without uniqueness the identity fails, which
is why the key proof is a gate, not a heuristic.

## Decision

1. A bounded plan-shape rewrite behind a declining gate, all conditions required:
   - LEFT OUTER with plain-equality ON, no residual and no null-safe keys;
   - grouping exactly by the preserved-side join key (no extra group keys);
   - the preserved key proven unique (primary-key/uniqueness evidence, fail-closed);
   - exactly `COUNT(<non-null-counted right column>)` as the aggregate;
   - existing deterministic right-side filtering left in place below the preaggregate;
   - the original aggregate's output metadata and nullability preserved exactly — the
     rewrite emits the original field contract, and the census digest rail enforces it.
2. Typed, latched control: `PlannerConfig::outer_count_preaggregation`, compiled default off at
   the seal; gauge env `TORON_TPCH_OUTER_COUNT_PREAGGREGATION`, loud "0"/"1" contract, row
   fields with provenance. Plans change; cohorts run plan-distinct.
3. Measurement per cell against the inherited 3,554 ms floor; activation per ADR 472's rule.

## Acceptance

1. **Structural battery**: the q13 shape engages (preaggregate below, outer join above,
   coalesce projection with the original output schema); declines pinned for every gate —
   inner/right/full join kinds, a residual ON conjunct, null-safe keys, grouping by a non-key
   or by extra keys, an unproven/non-unique preserved key, `COUNT(*)`-style or left-column or
   non-COUNT aggregates, and a nondeterministic right-side filter; knob-off byte-identity.
2. **Semantic battery** (both arms identical results): unmatched preserved rows coalescing to
   zero, duplicate right-side matches, NULLs in the counted column (skipped identically),
   NULL join keys on both sides, an empty right side, and an all-filtered right side; the
   engaged fixture asserts output schema and nullability identity between arms.
3. **Census**: q01-q22 + m00-m10 both arms, digests identical everywhere — including the
   rewritten q13, which the twin could not satisfy; fingerprint-changed cells run strict
   correctness in both arms; q19 sentinel. Required q13 evidence: `left_outer_grouped`'s
   8.90 s stage absent from the on-arm profile, spill 5,131,808 → 0, the preaggregate
   (~3.2 s envelope) and the collapsed join (~76 ms class) in its place.
4. **Paired authority**: q13 cohort, 20 runs / 10 adjacent pairs, plan-distinct gate, fixed
   floor, Hodges-Lehmann + 95% Wilcoxon from the persisted journal; regression-check cohorts
   for any other engaged cell; both-way verdict reporting.

## Guardrails

- Every gate fails closed; the knob-off arm is byte-identical.
- No executor changes: the rewrite composes existing Aggregate/Join/Project nodes; the
  preaggregate feeds the same placement and admission machinery as any grouped input.
- ADR 511-518 surfaces untouched; interaction with their knobs is census-adjudicated.

## Risks

- The economics rest on the twin; the optimizer-built shape may price differently (the twin's
  3.18 s orders aggregation becomes the new envelope either way) — the cohort arbitrates.
- Uniqueness evidence for the preserved key must come from catalog/primary-key facts; if that
  proof is unavailable at bind time for the official shape, the rewrite declines and the ADR
  closes red on engagement — an honest outcome the probes surface before measurement.
- Digest identity under the rewrite is a stronger requirement than the twin met; if the
  composed plan cannot reproduce the exact field contract, the census digest rail catches it
  before any cohort runs.

## Forward

- Post-519, q13's residual is the ~3.2 s orders preaggregation — a grouped-fold cell in the
  ADR 507/508 family, and possibly a third tenant for the `uncovered_generic_fold` recognizer
  gap (now q18 + q20) if it lands on that route.
- SUM/AVG/MIN/MAX variants of the identity stay out of scope until a cell demands them.
- The board successor set after q13: q09 (6.5 s gap), q18 (5.8 s), q05 (5.6 s, ungrounded);
  q21 stays parked.
