# ADR 148: IN-List Equality Selectivity from Existing NDV Facts

**Status:** Promoted (plan). **Council-selected topic.** ADR 141 taught Toron how
to estimate `col = literal` from labeled NDV facts, ADR 142 made Wendelon produce
those facts, and ADR 146 made the required base row count live in production. The
remaining gap is predicate shape: `col IN (literal, ...)` is an equality union, but
today it ignores the same NDV fact and falls back to the blunt default. ADR 148
extends the equality-NDV consumer to positive single-column IN lists, **estimate
channel only**, with no Wendelon producer or wire change.

**Placement:** Toron-sequence optimizer consumer extension. This spends existing
cross-ring facts but is Toron-only: no Wendelon schema, producer, client, or wire
surface changes.
**Predecessors:** 141 (labeled NDV equality selectivity), 142 (Wendelon NDV facts),
146 (live hard row upper bound), 138/145 (estimated byte size derived from row
estimates), 147 (estimate-channel discipline for facts-derived selectivity).
**Migration:** `plans/impl/0148_in_list_equality_selectivity_from_ndv.md`.

## Doctrine — complete the common equality predicate shape before adding producers

> The next useful optimizer step is not a richer Wendelon sketch garden yet. It is
> spending the facts already live. A positive IN list is a bounded union of equality
> predicates over one column. Its literal count is exact, the NDV fact is existing,
> and the estimate remains replay-protected through the existing estimated-byte
> broadcast path.

IN-list selectivity is deliberately conservative for broadcast. It sums one
`ceil(base_rows / NDV)` equality estimate per **distinct non-null literal**, then
caps at `base_rows`. Distinct equality classes on one column are **disjoint** (a row
has one `col` value), so the sum is the exact union of the *present* values; the
conservative over-count comes from per-value `ceil` rounding and from literals that
are **rare or absent** in the column (each still contributes one
`ceil(base_rows / NDV)`). Over-counting suppresses some broadcasts (safe); the
estimate must never under-count by assuming independence or inventing correlation.

## Why not hard bytes or sketch producers in this cut

**Hard materialized bytes** are a real follow-on, but the hard part is the proof.
The runtime admission unit is encoded Arrow IPC payload bytes, not schema width.
Promoting Toron's current estimate into a Wendelon hard fact requires a measured
framing/alignment/validity-buffer upper-bound ledger and a tight eligible-type
fence. ADR 148 avoids relabeling an estimate as hard.

**New sketches** (KLL, Count-Min/heavy hitter, Theta, tensor/correlation sketches)
should be evidence-gated by a specific optimizer miss and a specific consumer. IN
lists need no new producer and finish an existing consumer family. Sketches remain
named follow-ons, not dark facts added speculatively.

**Temporal range** remains valuable but is engine-breadth: Toron has some Arrow
date/timestamp plumbing, but no temporal `ScalarValue`/literal/comparison consumer
path for optimizer facts. That is not this small predicate-shape cut.

## Decision

**D1 — Recognize positive single-column IN lists in the equality-NDV helper.**
Extend `filter_equality_ndv_estimated_row_count` through
`predicate_node_equality_ndv_estimated_row_count` so it recognizes:

```text
Column IN (literal_1, literal_2, ...)
```

over `PlanOrigin::SingleBaseRelation`. The IN expression side must be a direct
`BoundExprNode::Column`; every counted list item must be a bound literal. `NOT IN`
is out of scope and fails open for this estimate.

**D2 — Estimate as a capped union of equality estimates.** Let:

- `base_rows` = hard `row_upper_bound` from ADR 146.
- `ndv` = `DistinctCountFact::point_estimate()` for the column.
- `k` = number of distinct non-null literal values in the list.

Then:

```text
rows_per_value = ceil(base_rows / max(1, ndv))
estimate = min(base_rows, saturating(k * rows_per_value))
```

If `base_rows == 0`, the estimate is zero. If `k == 0`, the IN-list path fails
open rather than creating an empty-relation rewrite.

**D3 — Literal distinctness is typed and canonical.** Add a small local
`InListLiteralKey` helper instead of depending on `ScalarValue` being hashable.
Supported keys:

- Boolean
- Int32
- Int64
- Utf8
- finite Float32/Float64, with `-0.0` normalized to `+0.0` and keyed by native
  bits after normalization

Null literals are ignored because they add no true matches under SQL IN semantics.
Any unsupported literal shape or non-finite float literal fails open for the **whole**
IN-list estimate — do *not* skip the offending member: skipping a `+inf` literal
(which can match rows) would under-count `k`, under-estimating rows and
**over-broadcasting**. A NaN member matches nothing and could be skipped safely, but
fail-whole is the simpler uniform rule and NaN/inf in an IN list is vanishingly rare.

**D4 — Existing AND composition stays minimum-only.** Multiple equality-derived
estimates continue to combine by `min`, not product. For example:

```text
col = literal AND col2 IN (...)
```

takes the tightest available single-predicate estimate. ADR 148 adds no
independence or correlation model.

**D5 — Estimate channel only.** The IN-list path writes only
`estimated_row_count`. It never writes `row_upper_bound`, `byte_upper_bound`, or
`EmptyRelation`, even for empty or all-null lists.

**D6 — Scope fences.** No Wendelon changes; no `NOT IN`; no tuple/multi-column IN;
no subquery IN; no expression-side IN such as `lower(col) IN (...)`; no OR-to-IN
canonicalization; no hard zero folds; no hard byte facts; no temporal scalar
support; no KLL/Count-Min/Theta/tensor producer; no join selectivity or reordering.

## Headline gate

`in_list_filtered_dim_with_wendelon_ndv_infers_broadcast_by_estimate` — a dimension
table with live `row_upper_bound` and NDV for `region`; `WHERE region IN ('north',
'south')` derives an estimated row count near `2 * ceil(rows / ndv)`. The 138/145
byte-estimate path makes the filtered build broadcast where the unfiltered dim
hashes. **Fails on main** because IN-list selectivity is not consumed.

## Supporting gates

`in_list_ndv_estimates_distinct_non_null_literals`;
`in_list_ndv_deduplicates_duplicate_literals`;
`in_list_ndv_ignores_null_literals`;
`in_list_ndv_all_null_list_fails_open`;
`in_list_non_finite_float_literal_fails_open` (D3 — fail the whole estimate, never
skip a member, since skipping a `+inf` member would under-count `k`);
`in_list_k_exceeds_ndv_caps_at_base_rows` (D2 cap — more distinct literals than
distinct values estimates all rows);
`not_in_does_not_use_ndv_selectivity`;
`in_list_without_ndv_or_row_bound_fails_open`;
`in_list_and_equality_keep_tightest_estimate`;
`in_list_does_not_write_hard_bounds_or_empty_relation`;
`equality_ndv_existing_behavior_unchanged`.

## Architecture guards

Add `adr148_in_list_ndv_selectivity_is_consumer_only_and_estimate_channel`:

- the equality-NDV helper recognizes `BoundExprNode::InList`;
- the IN-list path requires `PlanOrigin::SingleBaseRelation`;
- the IN-list path requires `row_upper_bound`;
- the helper has a local `InListLiteralKey` and normalizes finite float zero;
- the helper writes `estimated_row_count`, not hard bounds;
- no Wendelon optimizer-fact/schema/client file changes are required;
- `NOT IN`, tuple/subquery IN, hard byte facts, hard-zero elision, KLL,
  Count-Min, Theta, tensor, and `EmptyRelation` stay out of the ADR 148 helper.

Keep ADR 141, 142, 146, and 147 guards intact.

## Non-goals (named follow-ons)

Hard materialized-byte facts with an encoded-IPC upper-bound proof; hard zero-row
folds after snapshot/facts freshness exists; temporal scalar breadth; decimal
range; KLL/quantile range precision after measured min/max-uniformity misses;
Count-Min/heavy-hitter equality refinement after measured skew misses; Theta
join-key intersection; multi-column/correlated selectivity; join reordering.

## Forward arc

`141 equality NDV → 142 Wendelon NDV producer → 146 live hard row bound → 147 float
range → **148 IN-list equality** → hard-byte proof / temporal+decimal breadth →
KLL or heavy-hitter sketches after measured misses → Theta join-key intersection →
join selectivity → join reordering`.

## Consensus trail

Claude and Codex converged on IN-list selectivity as the smallest live cut after
147; Gemini's hard-byte lead was grounded as a valid but proof-heavy follow-on.
The operator/Council selected ADR 148 as IN-list equality selectivity from existing
NDV facts. The retained refinements are: no Wendelon producer, no hard channel,
distinct non-null literal count, fail-open unsupported literals, AND=min, and
explicit fences against sketch-producer creep.

