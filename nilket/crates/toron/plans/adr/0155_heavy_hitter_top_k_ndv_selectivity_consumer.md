# ADR 155: Heavy-Hitter Top-K NDV Selectivity Consumer

**Status:** Promoted. **Genuine 3-of-3 (no residual divergence).** A **Toron-local
consumer cut, built dark** (the 141 pattern): Toron learns to spend exact per-column
top-k value frequencies for equality/positive-`IN` selectivity, fixture-tested now, inert
in production until a Wendelon producer (ADR 156) lands. It spends the evidence ADR 154
produced — 154 measured the uniform NDV miss as real and harmful (hot key estimated
`ceil(10_000/100)=100`, chose `BroadcastEstimated`, then 139/140-replayed where the real
filtered dim had 5000 rows). 155 *fixes* that miss when the fact is present, without
widening the ring boundary.

## Placement (the load-bearing call, grounded)

**155 = Toron consumer; 156 = Wendelon producer.** This follows the 141→142 split, not
the 144/147 unify, because **Wendelon has no frequency stat to loop over**: `small_set`
is distinct values only (`storage/stats.rs:129`), HLL is cardinality, min/max/null counts
carry no per-value frequencies. A real producer needs a **new maintained heavy-hitter
sketch** (Space-Saving/Misra-Gries) + ingest/compaction + coverage — a separate
Wendelon-home cut (discipline #5: don't fold cross-ring producer+consumer). 155 pins the
consumer contract before the producer exists, exactly as 141 pinned the NDV consumer
before 142 activated it.

## Decisions

**D1 — Toron consumer, built dark.** Map and spend `column_heavy_hitters`;
`descriptor_optimizer_facts_from_storage` (`catalog.rs:163`) emits empty, so production
behavior is unchanged until 156. Fixture-tested by seeding the catalog (as 141 seeded
`column_ndv`).

**D2 — `ScalarLiteralKey`, promoted from 148's `InListLiteralKey`.** Promote the existing
binder-private key plus its literal-classification helper to a shared module, used by both
positive-`IN` dedup and heavy-hitter lookup — behavior-neutral (148 tests pin
canonicalization). The helper must preserve the current distinction between `Key`, `Null`,
and `Unsupported`: nulls are ignored for positive `IN` and decline equality estimates;
unsupported literals keep the pre-existing non-heavy-hitter behavior rather than being
silently dropped. Supported families = the existing variants: `Boolean`, `Int32`, `Int64`,
`Date32`, `Timestamp(TimeUnit, ticks)`, finite `Float32`/`Float64` (`-0.0`→`0.0`), full
`Utf8`. Non-finite float / interval / calendar-interval / binary / `LargeUtf8` /
unit-mismatch / unsupported → `Unsupported` (fail-open). The key variant *is* the type tag,
so no separate `arrow_type_tag` consumer-side. **No wire `Vec<u8>` key in 155** — that's
156's producer encoding.

**D3 — Carrier mirrors `column_ndv` exactly.** `ColumnHeavyHitterFact { entries:
BTreeMap<ScalarLiteralKey, u64> }`; `TableOptimizerFacts.column_heavy_hitters:
BTreeMap<ColumnOrdinal, ColumnHeavyHitterFact>` (`catalog.rs:119`); `DescriptorOptimizerFacts`
`#[serde(default)] column_heavy_hitters: Vec<DescriptorColumnHeavyHitterFact>` (`:131`);
`for_descriptor` mapping (`:157`); **`Scan.column_heavy_hitters: Vec<(ColumnOrdinal,
ColumnHeavyHitterFact)>`** (matching `column_ndv`'s Scan shape, `ir/execution.rs:239`);
`column_heavy_hitters_for_input` (mirror `column_ndv_for_input:4142`). **Validation:** ignore
a fact whose entry `frequency == 0` or `Σ frequency > base_rows`.

**D4 — Projection ordinal remap.** `optimize_for_required` (`binder.rs:6059`) already remaps
the Scan's `column_ndv`/`column_range` ordinals under projection pushdown (`ordinal_remap:6098`,
rewrite `:6104–6133`); `column_heavy_hitters` **must be remapped identically there** — else
post-pushdown ordinals go stale and the fact is mis-spent or lost.

**D5 — Consumer formula (inject at `equality_ndv_estimated_row_count:3998` /
`in_list_ndv_estimated_row_count:4041`).** For a valid fact with `B=base_rows`, `D=ndv`,
all-entry frequency sum `T (≤B)`, entry count `K`, and
`tail = ceil((B−T)/max(D−K,1))`:
- equality tracked → `min(frequency, B)`; this tracked spend needs only the valid fact;
- equality untracked (NDV + tail usable) → `tail`, capped at `(B−T)`/`B`;
- positive `IN` → sum only the frequencies of matched tracked literals (`M`), then add
  `min(B−T, untracked_distinct × tail)`, returning `min(B, M + added_tail)`;
- absent/invalid fact, unsupported key, null, `NOT IN`, `OR`, range, join, multi-column,
  expression → **unchanged pre-155 behavior** (fail-open). A present fact with uncomputable
  tail still allows tracked equality and all-tracked `IN`; mixed tracked/untracked `IN`
  falls back to the existing uniform/decline path.

**D6 — Estimate-channel + risk fences.** Frequency writes only `facts.estimated_row_count`;
never `row_upper_bound`/`byte_upper_bound`/`EmptyRelation`; hard-byte still beats
estimated-byte; replay keys only on `BroadcastReplication`. Spending `frequency` → larger
estimate → **hash**: stale-high → unnecessary hash (perf, never wrong), stale-low →
over-broadcast → 139/140 replay (recovered). An estimate-quality improvement, not a
correctness gate.

**D7 — Retarget 154's guards.** `adr154_uniform_ndv_math_stays_current_behavior` → allow the
155 wrapper but require the uniform fallback to route through `estimated_rows_for_ndv`.
`adr154_no_live_heavy_hitter_or_count_min_producer` → "consumer owned by 155; **Wendelon
producer + Count-Min absent in both rings**; descriptor sync emits empty." No public EXPLAIN
surface.

## Headline gate (closes 154's (b) loop, dark)

`hot_key_filter_with_heavy_hitter_fact_estimates_frequency_and_hashes`: 154's skew dim
(`row_upper_bound=10_000`, `NDV=100`), seeded `column_heavy_hitters={ region:{'hot'→5000} }`,
`orders ⋈ Filter(customers, region=='hot')` under 154's tight broadcast cap. Without the
fact → 154's `BroadcastEstimated` → replay; **with** → estimate 5000 → **not**
`BroadcastEstimated` → hash up front → **`replayed == false`**, digest matches forced-hash.
Fails on `main`. Closes the exact decision-boundary miss 154 measured; dark until 156.

## Supporting gates

- `hot_key_equality_uses_heavy_hitter_frequency`;
- `hot_key_in_list_sums_tracked_frequencies_and_uniform_tail`;
- `untracked_literal_uses_uniform_tail_not_global_uniform`;
- `missing_heavy_hitter_fact_preserves_141_148_uniform_ndv_behavior`;
- `invalid_heavy_hitter_fact_zero_freq_or_over_base_is_ignored`;
- `non_finite_or_unsupported_literal_fails_open_to_existing_behavior`;
- `heavy_hitter_estimate_stays_estimate_channel_not_hard_bound`;
- `heavy_hitter_fact_projection_remaps_with_scan_projection` (D4);
- `stale_low_heavy_hitter_frequency_still_replays_as_hash`;
- architecture: `adr155_heavy_hitter_is_estimate_only_consumer`;
  `adr155_no_wendelon_heavy_hitter_producer_or_count_min`;
  `adr155_descriptor_sync_emits_empty_heavy_hitter_facts` (Codex); the D7 retargets.

## Consensus trail

Genuine 3-of-3, no residual divergence. **Placement** (155 consumer / 156 producer) was
Claude's grounding — Wendelon has no frequency stat, so the producer is a new maintained
sketch (141/142 pattern + discipline #5) — adopted by Codex and Gemini and confirmed by the
operator's topic name. **`ScalarLiteralKey`** (promote 148's key; no wire `Vec<u8>` in 155)
converged 3-of-3 by iter-3 (Gemini moved off its `Vec<u8>`). **Projection remap** (Codex's
catch, grounded against `optimize_for_required:6098`) and **seeded-fact validation** folded
3-of-3. The uniform-tail consumer formula was identical across all drafts throughout.

## Forward arc

`154 skew evidence → 155 Toron heavy-hitter consumer (exact top-k, dark) → 156 Wendelon
heavy-hitter sketch producer (activation) → equality/IN selectivity live → KLL/quantile range
evidence+consumer → Theta join-key intersection → join selectivity → reordering` · *(parallel:
CONVERT/units)*.
