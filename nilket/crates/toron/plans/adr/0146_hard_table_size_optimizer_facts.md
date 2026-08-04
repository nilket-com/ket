# ADR 146: Hard Table-Size Optimizer Facts (Live Hard Row Upper Bounds)

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini).** Production
catalogs leave the hard table-size channel **dark**: Toron already has
`DescriptorOptimizerFacts::row_upper_bound` + `materialized_byte_upper_bound`, but
`descriptor_optimizer_facts_from_storage` imports NDV/range facts while hard-coding
both hard fields to `None`. ADR 146 lights the **row** half — Wendelon emits an
exact hard `row_upper_bound` (sum of stats-block `row_count` under the
all-contributors coverage fence); Toron imports it into the consumer ADR 136
already built. The **materialized-byte half stays deferred** (row-hard /
byte-estimated split).

**Placement:** Toron-sequence cross-ring activation (142/144 precedent — Wendelon
producer field + Toron import; the Toron consumer fields already exist).
**Predecessors:** 136 (hard table-size consumer), 138 (row-source byte estimation
+ the encoded-IPC-not-provable grounding), 141 (equality-NDV estimate — needs
`base_rows`), 142 (facts sidecar + coverage fence), 144 (range facts + wire
compat), 145 (projected build-side byte estimation). [[adr_118_cost_bound_scope_projected_path]].
**Migration:** `plans/impl/0146_hard_table_size_optimizer_facts.md`.

## Why now — lights the hard channel *and* un-darkens 141/142 in production
Two production wins, both from the same missing piece (a live hard row count):
1. **Unfiltered small dimensions broadcast.** `fact JOIN small_dim` (no filter)
   hashes today — no row source to anchor a byte estimate. A hard `row_upper_bound`
   → 138 derives a build byte estimate from exact rows → broadcast.
2. **141/142 equality-NDV selectivity finally fires live.**
   `filter_equality_ndv_estimated_row_count` (`binder.rs:3791`) is gated
   `let base_rows = facts.row_upper_bound?;` — it returns `None` without a hard row
   bound, which is `None` live. So 141's equality estimate has been **inert on real
   tables** (firing only in seeded fixtures); 146 supplies the missing `base_rows`.

## Decision

**D1 — Explicit universal hard row fact.** Wendelon `TableOptimizerFacts` gains
`#[serde(default)] pub row_upper_bound: Option<u64>` — a Wendelon storage fact (not
a Toron newtype; Toron wraps to `RowCount` at import). Legacy/missing payloads
decode `None` = unknown, never zero. As in ADR 144, postcard compatibility must be
explicit: add a fallback decode for the current range-capable payload that lacks
`row_upper_bound`, then keep the older NDV-only legacy fallback; do not assume
`#[serde(default)]` alone makes mixed payload shapes decode. (Conceded the iter-0
from-144-counts shortcut: that's opportunistic — numeric/temporal columns only; the
explicit table fact is universal and reuses the built consumer.)

**D2 — Produce universally under the coverage fence.** `table_optimizer_facts`
(`manager.rs:5060`) sums matching `ParsedColumnStatsBlock.header.row_count` after
the existing all-contributors-or-absent coverage proof. **Restructure so row
production runs independent of NDV/range candidate presence** — today the function
early-returns `None` when both candidate sets are empty (`:5067`), so a stat-less
(string-only) table never reaches coverage; the row fact must be emitted for *every*
covered table. Overflow saturates to `u64::MAX` (a valid upper bound) or suppresses;
never wraps (pin the choice with a test).

**D3 — Unknown stays unknown.** Any uncovered/active-tail local rows, missing or
drifted tiered contributor, or identity/generation/fingerprint mismatch suppresses
the fact. After the full fence passes with no matching contributor, the table is
proven empty → may emit `row_upper_bound = 0`.

**D4 — Row-hard / byte-estimated split.** No Wendelon `materialized_byte_upper_bound`
field, no positive hard byte fact; the Toron storage import keeps
`materialized_byte_upper_bound = None`. **Grounded reason it's deferred, not just
harder:** ADR 138 established Toron *cannot prove* the encoded-IPC byte unit the
runtime meters — fixed-width byte size is an *estimate*, so a hard byte producer
needs a proven encoded-size ledger 138 declined (the 136-W-byte follow-on, never a
mapping from compressed `size_bytes`). The hard row bound still lights the path:
138's `derive_estimated_byte_size` prefers `row_upper_bound` (`estimate_row_source`,
`binder.rs:318`), anchoring the byte estimate to exact rows (145 sharpened the
projected case).

**D5 — Truthful zero is sizing-only; no new elision.** A fully-covered empty table
emits `row_upper_bound = 0`; `for_descriptor` already normalizes row-0 → byte-0.
**Grounded:** a `row_upper_bound = 0` *fact* does **not** create `EmptyRelation`
today — elision arises only from a static-false predicate (`AlwaysReject`), and the
144 test at `binder.rs:9820` pins that `estimated_row_count = 0` keeps a normal plan.
So row-0 is sizing-only and **replay-protected** (stale-non-empty → over-broadcast →
139/140 replay, never wrong rows / never a dropped scan). ADR 146 adds **no**
Scan/filter → `EmptyRelation` rewrite; the hard-zero *fold* is a deferred ADR.

**D6 — Hard-bound doctrine (post-139/140 risk model).** `row_upper_bound` is a hard
upper bound and must never understate live rows (identity + `producer_epoch` +
coverage gating keep it so). The consumer-side risk is bounded: a stale-low bound
(zero or non-zero) → smaller estimate → over-broadcast → replay → byte-identical.
The only *un*-replay-protected consumer (scan elision) is excluded by D5. So the
producer emits liberally under the fence.

**D7 — Toron import + guard retargets (discipline #4).**
`descriptor_optimizer_facts_from_storage` maps
`facts.row_upper_bound.map(RowCount::new)` (drop the hard-coded `None`);
`materialized_byte_upper_bound` stays `None`; identity gating still precedes import;
no Toron stats cache / freshness oracle. **Retarget the ADR 142 hard-bound guard:**
142 pinned that NDV facts don't synthesize hard bounds; 146 inverts the live-mapper
row invariant. The guard now asserts: NDV/range-only facts still don't synthesize
`row_upper_bound`; only the explicit Wendelon field maps into Toron hard rows; the
mapper no longer hard-codes `None`; materialized-byte stays absent.

## Headline gate
`wendelon_live_row_upper_bound_enables_unseeded_small_dimension_broadcast` — a live
storage-backed descriptor (real CSB stats, **no** `seed_optimizer_facts`) yields a
hard `row_upper_bound`; an unfiltered small dim broadcasts. **Fails on `main`** (the
payload has no row field; the mapper maps live row bounds to `None`).

## Supporting gates
- `table_optimizer_facts_emits_row_bound_for_stat_less_table` (D2 universality)
- `table_optimizer_facts_sums_matching_block_header_row_counts`
- `table_optimizer_facts_suppresses_row_bound_with_uncovered_local_rows` /
  `..._on_missing_tiered_stats` (fence)
- `table_optimizer_facts_known_empty_emits_zero_row_bound`
- `legacy_facts_decode_row_bound_absent_not_zero` (wire compat)
- `storage_optimizer_facts_map_hard_row_bound` (import)
- `hard_row_bound_anchors_138_byte_estimate`
- `live_ndv_equality_estimate_uses_imported_row_upper_bound` (un-darkens 141)
- `row_zero_fact_does_not_elide_scan` (D5)
- `materialized_byte_bound_stays_none_byte_estimated_from_rows` (D4)

## Architecture guards
`adr146_hard_row_bound_is_wendelon_sidecar_field`,
`adr146_table_schema_descriptor_not_mutated_for_size_facts`,
`adr146_row_production_independent_of_ndv_range_candidates`,
`adr146_row_fact_requires_full_contributor_coverage`,
`adr146_live_mapper_imports_row_upper_bound`,
`adr146_ndv_range_do_not_synthesize_hard_bounds` (the 142 retarget),
`adr146_no_materialized_byte_producer_yet`,
`adr146_no_compressed_bytes_as_row_or_byte_bound`,
`adr146_no_scan_elision_from_row_count_fact`.

## Non-goals (named follow-ons)
Wendelon materialized-byte field / positive hard byte producer (136-W-byte — the
proven encoded-size ledger); compressed-byte substitution; **hard zero-row filter
fold / scan-to-`EmptyRelation` rewrite** (Gemini's earlier D4 — needs the
elision-freshness doctrine this cut sets up); exact-membership payload for NDV
small sets; KLL/quantile; float range; temporal types; join selectivity/reordering;
new facts RPC / descriptor mutation / Toron stats cache / background refresh.

## Forward arc
`… → 145 column pruning → **146 hard row bound (lights the hard channel; un-darkens
141 equality)** → (136-W-byte hard materialized bound · hard-zero folds) →
KLL/quantile → Theta-sketch join-key intersection → join selectivity → reordering`.

## Consensus trail
3-of-3 on Live Hard Row Upper Bounds. Claude's iter-0 row-count observation
refined the cut off full 136-W; iter-1 conceded the opportunistic from-144-counts
shortcut to the explicit universal field (Codex). The **row=0 divergence** —
Gemini's iter-1 "elide on hard 0" vs Codex's "no new elision" — resolved on
Claude's grounding that a row-0 *fact* does not create `EmptyRelation` today
(`binder.rs:9820`); Gemini reversed to "no new elision" (iter-2). The byte half is
deferred on 138's encoded-IPC-not-provable grounding. No remaining divergence.
