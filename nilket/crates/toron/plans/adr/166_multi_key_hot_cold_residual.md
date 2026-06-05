# ADR 166: Multi-Key Hot+Cold Residual From Group Top-K + Group NDV

**Status:** Promoted. **The cheap completion that closes the 164+165 composition (operator chose to bank
it before the CONVERT pivot).** A **consumer-only, Toron-local** cut — no producer, no Wendelon change, no
wire change. It spends facts already produced (164 group top-k + 165 group NDV) to turn 164's
`max(prior, exact_hot_tuple)` *floor* into the full **hot+cold decomposition** `exact_hot_tuple +
cold_residual`. The cold residual is *now valid* because 165 supplies a **real composite tuple-NDV** — 164
deferred this precisely because subtracting hot tuples from the `Π max(ndv)` independence *proxy* is fake
precision; 165's effective measured `ndv(a,b)` denominator makes `composite_ndv - |H|` a real cold-tuple
count.

## The estimate — 162's decomposition, finally valid for tuples

At `L ⋈ R on (a,b)` with **both** group facts present:

```
estimate = max( post_165 ,  exact_hot_tuple + cold_residual )
  exact_hot_tuple = Σ_{t∈H} freq_L(t)·freq_R(t)                      (164, EXACT)
  cold_residual   = range_fold( |L_cold|·|R_cold| / cold_ndv )       (165 composite-NDV, cold-only)
     |L_cold| = |L| − Σ freq_L(t)   |R_cold| = |R| − Σ freq_R(t)     (hot rows removed)
     cold_ndv = effective_composite_ndv − |H|                        (real cold-tuple count — 165-unlocked)
```

Identical in shape to 162 (exact hot intersection + cold residual, no double-count, raising-only
`max`-guard) — only now the *cold denominator is a measured composite tuple-NDV*, not the proxy 164 was
stuck with. **No double-count:** the `|H|` hot tuples and their rows are removed from the cold residual and
counted exactly in `exact_hot_tuple`. **Why it beats the `max` floor:** when a join has *both* a co-hot
whale *and* heavy cold traffic, `max(prior, exact_hot_tuple)` takes the larger of the two; the sum captures
both — the genuine improvement.

## Composition / darkness — a strict refinement of post-165

166 changes the estimate **only when both group facts are present**:
- **both top-k + NDV** → hot+cold sum (the new path);
- **only top-k** → 164's `max(refined, exact_hot_tuple)` floor (unchanged);
- **only NDV** → 165's capped `refined` (unchanged);
- **neither** → 163 base (unchanged).

Absent-either-fact ⇒ **byte-identical to the current post-165 behavior** (the composition-darkness
invariant). `max(post_165, …)` keeps it **raising-only** — never below the current behavior, even though
the cold half inherits 165's HLL approximation. Estimate-channel, 139/140-backstopped.

## Decisions

**D1 — Consumer-only refinement in `multi_key_join_cardinality`.** No new producer, no Wendelon change, no
wire change. Modify the 164 group-top-k floor section: when the 165 group-NDV fact is *also* present,
compute the cold residual and sum instead of flooring.

**D2 — The hot+cold sum (reuse 164 + 165); no double-cap [iter-2].** `hot_tuple_join_contribution` (164)
returns `{ rows, count, left_rows, right_rows }`; `effective_composite_ndv` is the denominator 165 already
spent: `min(Π max(ndv), max(group_ndv_L, group_ndv_R))`. Then `cold_rows_{L,R} =
rows_{L,R}.saturating_sub(hot.{left,right}_rows)`; `cold_ndv =
effective_composite_ndv.saturating_sub(hot.count)` (**no second cap** — `cold_ndv` is a real cold-tuple
count derived from the already-capped composite, not a proxy to re-bound). If cold rows remain but
`cold_ndv == 0`, decline to the post-165 answer rather than fabricating a one-key cold tail. Otherwise
`cold_residual = range_fold(ndv_containment_join_rows_from_denominator(cold_rows_L, cold_rows_R,
cold_ndv))`, `candidate = hot.rows + cold_residual` (checked `u128`), and return
`Some(max(post_165, candidate))`.

**D3 — Requires both facts; else current behavior (byte-identical).** Gate on both
`clean_base_column_group_top_k_for_join_input` (164) and `clean_base_column_group_ndv_for_join_input` (165)
matching the join group. Missing either / `H = ∅` / invalid → the existing 164-or-165-or-base path,
unchanged.

**D4 — No double-count; raising-only; disjoint-safe by ordering [iter-2].** Hot rows/tuples removed from
the cold residual (`saturating_sub`); `cold_rows == 0` → cold_residual `0`; cold rows with
`cold_ndv == 0` → post-165 decline; `max(post_165, candidate)` floor. The existing
`if refined.get() == 0 { return Some(refined) }` disjoint short-circuit
**precedes** this block — so a disjoint join returns `0` before hot+cold is reached (and no shared hot
tuple can exist in disjoint ranges anyway), giving disjoint-dominates-stale-hot-tuple safety for free.
Checked `u128`; overflow → fall back to the post-165 answer.

**D5 — Cold residual re-applies 161 range-overlap; hot tuples are exact (no range-fold).** Cold keys are
still range-constrained → range-fold the cold containment (reuse `range_refined_containment_join_rows`).
Hot tuples are specific in-data values (always in-overlap) → counted exactly, no range scaling.

**D6 — Estimate-channel, approximate (cold inherits 165's HLL composite-NDV).** Hot half exact (164),
cold half approximate (165), the `max(post_165, …)` floor keeps the whole monotone ≥ current behavior.
Sets `estimated_row_count`/`estimated_byte_size` only; `adr136` markers unchanged.

## Headline gate

`live_column_group_hot_cold_recovers_multi_key_skew_and_correlation_without_seeding`: `orders ⋈ payments`
on `(customer_id, region_id)` with **both** a co-hot whale tuple **and** heavy cold traffic, using the
existing storage-fact-to-descriptor path for column-group top-k and column-group NDV (no Toron-seeded
facts, no new producer). Calibrate so `refined_165` *and* `exact_hot_tuple` each land **just under** the
broadcast threshold (current `max` → broadcast → over-cap → replay) while `exact_hot_tuple +
cold_residual` lands **above** → chooser **hash, `replayed == false`**. A strategy **flip** (proving the
`max` was dropping the cold mass), not just a number. Paired seeded arithmetic gates cover
`only_one_group_fact_present_is_byte_identical_to_post_165` (composition-darkness — top-k-only → 164 floor;
NDV-only → 165 cap; exact equality).

## Supporting gates

- `hot_plus_cold_no_double_counts_hot_tuples` (total = exact_hot + cold; hot rows/keys removed from cold);
- `cold_ndv_uses_effective_composite_not_independence_proxy_or_raw_overcount` (the 165-unlock —
  `cold_ndv = effective_composite − |H|`, not `Π max(ndv) − |H|` and not raw HLL overcount);
- `hot_plus_cold_never_lowers_post_165` (raising-only `max` floor);
- `cold_residual_zero_when_all_tuples_hot` (cold rows `0` → cold `0`, estimate = exact_hot);
- `cold_ndv_zero_with_cold_rows_declines_to_post_165` (no clamp-to-one fabricated tail);
- `disjoint_join_short_circuits_before_hot_plus_cold` (the ordering / disjoint-dominance, iter-2 Close 2);
- `cold_residual_applies_range_overlap`; `hot_plus_cold_fails_open_without_either_group_fact`;
- guards: `adr166_hot_plus_cold_is_estimate_channel_only`, `adr166_requires_both_group_facts`,
  `adr166_no_new_producer_or_wire` (consumer-only — no Wendelon/wire diff),
  `adr166_hot_plus_cold_never_lowers_prior`; retarget ADR164's exact-floor guard to pin top-k-only
  behavior; `adr136` markers unchanged; 160–165 `join_cardinality` regressions green.

## Consensus trail

iter-0: I'd leaned the CONVERT breadth pivot; the operator steered to bank this cheap completion first
(close the 164+165 composition before pivoting). iter-1: the dual — consumer-only hot+cold sum, the
165-unlock (real composite tuple-NDV makes the cold subtraction valid), composition-darkness, raising-only.
iter-2 closed three forks on grounding: no double-cap (`cold_ndv` from the already-capped composite); the
disjoint short-circuit already precedes hot+cold (disjoint-dominance for free); headline calibrated as a
strategy flip (both halves sub-threshold, sum above).

## Forward arc

`164 skew + 165 correlation + **166 hot+cold sum (composition complete)** → [join CBO substantially done]`.
The remaining join residual is **Theta cold-key value-set overlap** (residual 3, measure-first — the last,
lowest-value residual). **Agreed next topic: dimensioned CONVERT (009)** — the breadth pivot off the join
axis (dimension on the nominal-identity spine 087–092, bind-time dimensional safety). Rune optimizer
cockpit (010) is the other standing breadth candidate.

## Deferred (named)

Theta/MinHash cold-key value-set overlap (residual 3, measure-first — the last, lowest-value join
residual); N-column groups beyond 2 + wider scalar lanes; **dimensioned CONVERT (009)** as the agreed next
topic; the Rune optimizer cockpit (010); multi-join reordering (the eventual join-CBO consumer of all these
estimates).
