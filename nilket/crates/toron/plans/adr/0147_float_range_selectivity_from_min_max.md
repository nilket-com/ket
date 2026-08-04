# ADR 147: Float Range Selectivity from Maintained Min/Max

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini).** ADR 144 added
labeled range selectivity from Wendelon's maintained min/max, but Toron's consumer
decoded only `Int32`/`Int64`. Float columns are **dark**: `arrow_stats_type_tag`
returns no tag for `Float32`/`Float64` (`stats.rs:382`) so the CSB builder emits
`ARROW_STATS_TAG_OPAQUE_UNSUPPORTED` (counts only); `range_fact_tag_supported`
excludes float; Toron's range helper is integer-shaped (`RangeAtom.literal_value:
i64`, integer `RangeInterval`, `decode_range_i64`). So `WHERE score >= 0.8` gets the
blunt default → float-range dimensions hash. ADR 147 closes it: a finite-float
Wendelon producer + a continuous Toron consumer, **estimate-channel only**.

**Placement:** Toron-sequence cross-ring optimizer-fact extension (142/144/146
precedent — Wendelon producer + Toron consumer).
**Predecessors:** 144 (the range substrate this extends), 146 (row-hard /
estimate-honesty / no elision), 138/145 (byte estimates that spend row estimates),
142 (facts sidecar + full-contributor coverage).
**Migration:** `plans/impl/0147_float_range_selectivity_from_min_max.md`.

## Doctrine — complete the range story for the last native scalar family
> Toron already executes `f32`/`f64` end-to-end (`ScalarValue::{Float32,Float64}`,
> float exec/PBC/runtime-filter paths), so this is a **pure optimizer-facts cut, no
> engine-breadth** — and the smallest-correctness-surface continuation of 144:
> estimate-channel only, no hard bounds, no `EmptyRelation`. Float means `f32`/`f64`,
> **not** Decimal (Toron has no Decimal scalar) — currency/price columns stay
> fail-open (a named engine-breadth follow-on).

## Why not hard-zero folds (the considered-and-deferred alternative)
The council weighed hard zero-row folds (the ADR 146 elision follow-on) for three
iters and **grounded it out of this cycle**: `EmptyRelation` is correctness-sharp
(stale-low → dropped rows, not replay-protected), and the freshness proof does not
exist *and would be inert if built*. Production SQL reads use unbounded/latest
`HlcBound(0)` (`daemon.rs:1274`) with no `AS OF`/bounded-snapshot path; a correct
freshness contract (no elision on latest reads) would therefore fire for **zero**
queries. Hard-zero is doubly-deferred — it needs both a cross-ring
facts/snapshot-validity receipt *and* bounded-snapshot reads. Float is the live,
right-sized cut.

## Decision

**D1 — Schema-owned float range tags.** Add `ARROW_STATS_TAG_FLOAT32`/`FLOAT64`,
owned in Wendelon's schema optimizer-facts module, re-exported through
`wendelon-client`, added to valid stats tags + `range_fact_tag_supported`. **No
`TableOptimizerFacts` struct change** — tag-only; old consumers that don't decode
the tags fail open (the 144 wire-compat fallback covers it).

**D2 — Finite-only producer; it is a min/max *computation* arm, not just a tag.**
min/max is computed per-type in `collect_column_stats_in_one_pass`
(`min_max_for_array` is its test wrapper), which has no float arm — so D1's tag
alone does nothing. Add a float arm that downcasts `Float32Array`/`Float64Array`,
folds min/max over non-null values via `partial_cmp`, and:
- **suppresses the column's range payload if any value is NaN or ±∞** (counts-only /
  no-envelope stats for that column, no opt-in payload built from only the finite
  subset, never error the block/write path) — because `partial_cmp` is undefined on
  NaN and an ∞ envelope would mis-summarize the finite population;
- **normalizes `-0.0`→`+0.0`** before encoding (so stats order matches SQL equality);
- encodes via a **new schema-owned order-preserving float canonical encoder** beside
  144's signed/unsigned helpers at the native `f32`/`f64` width (sign-bit flip for
  ≥0, all-bits for <0); decode rejects malformed lengths and non-finite results. All
  142/144/146 coverage fences kept; old opaque-tag float blocks fail open by tag
  mismatch.

**D3 — Typed Toron range domain; continuous intervals carry inclusivity.** Refactor
144's integer-only helper into a typed domain:
- **`Integral`** — 144 **verbatim** (`+1`/`-1` discrete successor, i128 bounds).
- **`Real`** — `f64` bounds with an explicit **inclusivity** flag
  (`FloatBound { value, inclusive }`). Floats have **no discrete successor**, so
  `Gt 0.8` is *exclusive* 0.8 (not `0.8+ε`) — the integer `+1` trick cannot be
  reused. `Gt`→exclusive, `Gte`→inclusive (lower); `Lt`→exclusive, `Lte`→inclusive
  (upper); intersect = tightest value, `inclusive = lhs && rhs` at equal values.
- A float fact may consume finite `Int32`/`Int64`/`Float32`/`Float64` literals
  (`f64`-representable); NaN/null/non-representable literals fail open.

**D4 — Continuous selectivity, inclusivity does the endpoint work.** For non-null
`N`, envelope `[min,max]`, clipped interval `[low,high]`:
- `min == max` → all-or-zero (evaluate the single value), no div-by-zero.
- `frac = (high − low)/(max − min)`; **non-empty** overlap →
  `max(1, ceil(N·frac))` (the tiny-overlap floor — a real overlap never rounds to 0).
- **Endpoint-only** (`score >= max` → `low=high=max`, width 0 but the endpoint is
  *included*) → **≥ 1 row**; `score > max` (exclusive) or a disjoint interval → **0**.
  The inclusive flag is the *only* thing distinguishing `>= max` (1) from `> max`
  (0) — load-bearing, not droppable.
- Integer behavior is byte-for-byte preserved (pinned by a regression guard).

**D5 — Channel honesty.** Float range writes only `estimated_row_count` — never
`row_upper_bound`, `byte_upper_bound`, or `EmptyRelation`, even for a disjoint
envelope.

**D6 — Scope fences.** No Decimal, temporal range consumer, UTF-8/Binary/Bool
range, KLL/quantile, hard byte facts, hard-zero folds, scan elision, or join
reordering.

## Headline gate
`float_range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate` — a
`FLOAT64` dim with finite maintained min/max; `WHERE score >= 0.8` derives a smaller
`estimated_row_count`; the 138/145 byte-estimate path makes the filtered build
broadcast where `main` hashes (float facts fail open). **Fails on `main`.**

## Supporting gates
`table_optimizer_facts_emits_float_range_for_finite_values`;
`table_optimizer_facts_suppresses_float_range_with_nan_or_infinity`;
`float_stats_normalizes_negative_zero`;
`float_canonical_encode_round_trips_and_orders` (negatives < positives, −0=+0);
`float_range_continuous_midpoint_estimates_half_non_null_rows`;
`float_range_endpoint_inclusive_ge_max_estimates_one_row_but_gt_max_zero` (the D4 edge);
`float_range_constant_column_min_equals_max_no_div_by_zero`;
`float_range_outside_envelope_estimates_zero_without_hard_bound`;
`float_range_does_not_write_hard_bounds_or_empty_relation`;
`int_literal_against_float_fact_consumed_as_f64`;
`integer_range_facts_keep_discrete_interpolation` (refactor regression);
`utf8_range_facts_still_fail_open`.

## Architecture guards
Retarget 144's `range_utf8_and_float_facts_fail_open` → "UTF-8 fail-open; **finite
float facts estimate**"; extend the scalar-tag guard (float tags schema-owned +
client-re-exported + encode/decode helpers); **144 estimate-channel + 146
no-elision guards untouched**; add a guard that no KLL/hard-bound/`EmptyRelation`/
compressed-byte path appears in the float range helper.

## Non-goals (named follow-ons)
**Hard zero-row folds** — deferred until *both* a query-snapshot/facts-validity
receipt (Gemini's `provenance_hlc`) *and* bounded-snapshot reads exist (today
`HlcBound(0)`=latest, no `AS OF`). **Decimal range** (needs a Decimal scalar —
engine-breadth); temporal range consumer (engine-breadth arc); 136-W-byte hard
materialized bound (marginal post-146 + measured ledger); KLL/quantile (evidence-
gated); join selectivity/reordering.

## Forward arc
`… → 146 hard row → **147 float range** → (hard-zero folds once snapshot reads
exist · 136-W-byte · decimal/temporal) → KLL/quantile → Theta-sketch join-key
intersection → join selectivity → reordering`.

## Consensus trail
Iters 0–2 split hard-zero vs float, with a Claude↔Codex cross-over at iter-1.
Resolved on grounding, not headcount: Codex's stale-cache counterexample +
Claude's read-bound grounding (`HlcBound(0)`=latest, no epoch↔HLC linkage) showed
hard-zero's elision proof is absent *and inert if built* — Claude retracted (iter-2)
and proved the inertness (iter-3). All three converged on float by iter-3.
Claude's iter-4 sharpenings folded in: the producer **min/max computation arm**
(NaN/∞ suppress, −0 normalize — D2) and the consumer **float inclusivity** (D3/D4's
`>= max` vs `> max` endpoint correctness). No remaining divergence.
