# Toron Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) — git holds the
archaeology, this file looks forward.

**Update cadence.** Rewritten at planning sessions, not implementation
commits. The roadmap lags the code on purpose — check `git log` and the
relevant impl plan for actual status. Per-ADR decisions live in
`plans/adr/NNN_*.md` + each ADR's §"What to do next"; this file holds only
the forward view + the active line.

## Status

ADRs 001–163 promoted (implemented unless a `feat` is still pending — check
`git log`). Per-ADR detail in `plans/adr/NNN_*.md`.

## V1 target

"First Query with Integrity" plus "Distributed Execution Correctness":
correct analytical results against a Wendelon-native cluster with pushdown
maximized, identity flow real, stream-shaped storage boundary, multi-process
coordinator/worker mechanics surviving a worker SIGKILL with byte-identical
normalized output, and the first two-input relational operator working across
hash-partitioned and bounded broadcast strategies. Multi-tenant governance,
executor model upgrade, sort-merge, output-side join streaming, and full
end-to-end natural backpressure remain post-V1.

## Prioritization philosophy

Toron is the compute ring of Nilket; Wendelon is the storage ring. We control
both. The boundary exists for scaling dynamics and resource isolation, not
because storage and compute are independent products.

- **Toron's reason for existence is smart-storage pushdown.** Every cut
  polishes a system doing its primary job along its primary axis.
- **Evidence drives scope.** The most informative artifact for the next ADR
  is the previous ADR's bench results or a code-evidence silent-drop /
  inert-feature defect surfaced during selection.
- **Minimize strategic rework.** Two narrow ADR shapes otherwise equivalent
  — prefer the one preserving plausible long-term seams.
- **Ring-product placement** (`plans/conventions/ring-product-placement.md`):
  Interior / shared substrate stays in this Toron sequence; Exterior
  ring-specific products return home to their own ring.
- **Complete cut, not granular slice.** Fold adjacent scope; defer only at
  genuine mechanism/topic boundaries.

## Forward sequence

### ADR 163 promoted; ADR 164 candidates

ADR 163 promoted **Multi-Key Equi-Join Cardinality** — fills a **verified total gap**: composite-key joins
(`L.a=R.a AND L.b=R.b`) got *no* estimate (the seam matched a single-element slice, binder.rs:1479, else
`composite_unknown`). Generalize to all key pairs under **attribute independence**: `|L⋈R| ≈ |L|·|R| / Π_k
max(ndv(L.k), ndv(R.k))` — the textbook multi-column estimate, the `k≥2` generalization of 160's
single-key containment. **Range-overlap composes conjunctively** (a row matches on *all* keys, each needing
its value in that key's overlap) → fractions multiply, and **any single disjoint key proves the whole join
empty** (more keys = more chances to prove emptiness). Reuses every 160/161 helper *per key* — only new
code is the product denominator + the per-key range fold. **Single-key byte-identical to 162** (factor
`ndv_containment_join_rows_from_denominator`; `_from_points` delegates with `denom = max`). **iter-2
verified the fold is correct:** disjoint-`0` propagates because 161's floor `if refined==0 && base>0 {1}`
only bumps a *positive* base that rounded to 0 (a sliver), never reviving a disjoint-proven-empty join;
`_from_denominator`'s `((num−1)/denom)+1` keeps `base≥1` for any non-empty input regardless of how large
`Π max(ndv)` grows — so only a *truly disjoint* key yields `0`. **The new risk (named):** independence
over-states `denom` on *correlated* keys (`ndv(a,b) ≈ ndv(a)`, not the product) → **under-estimate** (→
broadcast → replay) — the multi-key analog of 160's containment assumption; ships the independent baseline,
estimate-channel/139-140-backstopped, correlation deferred to the residual evidence. **Heavy-hitter (162)
stays single-key** — multi-key skew lives in the hot *tuple* `(a,b)`, which *cannot* be reconstructed from
156's per-column marginals (the correlation problem). Clean `Scan⋈Scan`/Inner/all-keys-need-clean-base-NDV,
estimate-channel, `adr136` markers unchanged, no execution-side change. **Headline:** `order_items ⋈
line_prices` on `(order_id, line_no)` — `composite_unknown` on `main` → real product-denominator estimate
that flips the strategy; paired disjoint-on-any-key → `0`.

**ADR 164 lead candidates:** **Wendelon composite/tuple heavy-hitter top-k producer + multi-key-skew
consumer** (the operator's call: multi-key skew is the **first join-arc need that requires *new* Wendelon
production** — the cheap reuses 160–163 are exhausted. A Space-Saving sketch keyed on the multi-column tuple
`(a,b,…)`, extending **156** per-column → composite, same exact-or-decline merge; **Toron-sequence cross-ring
producer, the 156/159 pattern** — Wendelon owns the maintained composite sketch, Toron consumes it in a
multi-key-skew refinement, the 162-analog for tuples, with brand-clean Wendelon names such as
`ColumnGroupTopKSpec`, `ColumnGroupTopKFact`, and `ColumnGroupTopKEntry` (no new `*V1` suffixes). A
*proven build pattern* — the concrete lead beyond the cheap reuses, more so than the vague "Theta");
**measured residual join-miss evidence** (the 154/157
gate over the *complete* estimator — containment+range+skew+multi-key independence — to scope the composite
producer + whether correlation/Theta matter; likely the honest precursor to the producer); **dimensioned
`CONVERT` / linear units** (parallel breadth, brainstorm 009, operator-energy); **Rune optimizer cockpit**
(brainstorm 010 — the fact spine now carries NDV / min-max / top-k / range-distribution / join-cardinality /
overlap-fraction / join-skew / multi-key gauges). **Forward arc:** `163 multi-key containment → measured
residual miss → composite/tuple top-k producer (156/159 pattern) for multi-key skew + Theta only for the
cold-key overlap residual → multi-join reordering` · *(parallel: CONVERT/units; Rune cockpit)*. The
cheap-reuse join arc (160→161→162→163) is **complete**; 164 is where new Wendelon production re-enters, or
where the residual evidence shows it isn't needed.

### ADR 162 promoted; ADR 163 candidates (HISTORICAL)

ADR 162 promoted **Heavy-Hitter Join-Skew Refinement** — takes the **skew** half 161 left: at `L.x = R.y`,
recover the *exact* contribution of keys hot on both sides from 156's already-produced top-k, leave the
cold keys to 160/161 containment. **Toron-home** (consumer reuse of 156's facts; no new Wendelon
production — provenance test). **Decomposition (no double-count):** `candidate = exact_hot_both +
residual_containment` where `exact_hot_both = Σ_{v∈H} freq_L(v)·freq_R(v)` over `H = hot_L ∩ hot_R` (exact,
since 156 emits only exactly-counted keys present in both maps; absent keys are untracked, not zero), and
the residual runs 160/161 containment on the cold keys with the hot-both rows/keys **removed**
(`|L|−Σfreq_L`, `ndv_L−|H|`, saturating). The whale case: `customer 7`
× 50k orders × 50k payments = 2.5B rows that uniform containment spreads across `max(ndv)` and badly
under-counts (154's hot-key miss across two tables) — the intersection recovers it to the row.
**Direction-honest (distinct from 161's pure tightening):** 162 can move the estimate *up* — co-hot
correlation produces more rows than uniform assumes; that's the safe direction (higher → hash; the
*dangerous* error is the under-estimate → broadcast → replay, which 162 fixes; over-correction → missed
broadcast, never incorrect). **Pure superset of 161:** `H = ∅` (anti-correlated / no HH facts) ⇒
`exact_hot_both = 0`, residual unreduced ⇒ **byte-identical to 161**; with co-hot facts, returned estimate
is `max(prior_160_161, exact_hot_both + residual)` so 162 never lowers 160/161; disjoint ranges ⇒ no
shared key ⇒ estimate `0` (a contradictory shared HH sidecar is ignored by the zero-overlap prior guard,
preserving 161's disjoint win). **Residual re-applies 161 range-overlap** by sequencing the two existing
helpers (`ndv_containment_join_rows:4203` →
`range_refined_containment_join_rows:4266`) on the reduced inputs — no new `containment_then_range` helper.
**Full hot+cold sum, not the hot-only `max(containment, hot_both)` floor** (that floor under-counts the
cold residual, re-introducing the under-estimate); the final `max(prior_160_161, sum)` is only the baseline
guard. Estimate-channel, 139/140-backstopped, clean `Scan⋈Scan` / Inner / single-key (160/161 disciplines),
`adr136` markers unchanged. **Headline:** `orders ⋈ payments` on `customer_id` with a shared whale →
estimate ≫ uniform → **hash, no replay** (160/161 would broadcast → over-cap → replay). Anti-correlated
hot keys (hot-one-side) stay in the residual; the one-sided `freq_L·R_threshold` bound is deferred.

**ADR 163 lead candidates:** **measured *residual* join-miss evidence** (the 154/157 pattern — now that
160/161/162 exhaust the cheap reuses, measure what the three still miss on a join: the honest gate before
any new sketch; likely small); **Theta/MinHash key-overlap producer** (the 156/159 new-sketch pattern,
Toron-sequence cross-ring — gated on the evidence; possibly closed out entirely if the residual is
negligible); **multi-key equi-joins** (product-of-per-key-selectivities — the deferred 160/161/162
generalization, a cheap completion); **dimensioned `CONVERT` / linear units** (parallel breadth, brainstorm
009, operator-energy); **Rune optimizer cockpit** (brainstorm 010 — the fact spine now carries NDV /
min-max / top-k / range-distribution / join-cardinality / overlap-fraction / **join-skew** gauges).
**Forward arc:** `162 heavy-hitter skew → measured residual miss → Theta only for the residual → multi-join
reordering` · *(parallel: CONVERT/units; Rune cockpit)*. Evidence-gate 163 on the 162 feat; the cheap-reuse
arc (160→161→162) is now complete.

### ADR 161 promoted; ADR 162 candidates (HISTORICAL)

ADR 161 promoted **Join-Key Range-Overlap Refinement for NDV Containment** — a **pure tightening** of
160's containment estimate, reusing the live min/max `RangeFact`s (158/159) on the join keys. **Reframed
from the roadmap's "evidence → Theta":** containment's weakness splits into *disjoint* keys (a cheap
reuse fix needing no evidence — min/max *prove* emptiness under the optimizer fact contract) and
*skew-correlated* keys (the half that warrants evidence + heavy-hitter/Theta). 161 takes the
disjoint/partial-overlap half. **Load-bearing
invariant:** only keys in the overlap `[max(min_L,min_R), min(max_L,max_R)]` can join; outside keys are
phantom matches containment over-counted — so the refinement only *removes* phantoms: **`refined ≤
containment`, always** (monotone; never inflates, never more wrong than 160; under-counts still
139/140-backstopped). Disjoint key ranges → estimate `0`; partial → scaled; contained/identical →
unchanged. **Fraction `f = overlap_w / min(width_L, width_R)`** — the *derived* safe
coarsening of the exact two-sided participation model (`E' = |L|oL·|R|oR / max(ndv_L·oL, ndv_R·oR)`): exact
at disjoint/contained/symmetric-partial, safe-direction (under-tightening) error only on asymmetric-partial;
**`min` not `max`/union** (max would wrongly penalize the contained `L⊂R` case). **Integral type lane**
(Int32/Int64/Date32/Timestamp via one `i128` path, `+1` inclusive width — every common join key), with the
consumer validating each range tag against the join key type because descriptor range import is ordinal-only;
**Real fails open at the helper** (SQL float join keys are already rejected before estimation).
**Estimate-channel only** — disjoint → estimate `0`, **not** a hard `EmptyRelation` prune (that crosses into the correctness
channel with its own replay grounding — deferred, D7). Fires only on clean `Scan ⋈ Scan`, Inner, single-key
(160 disciplines); no execution-side change (sharpens the estimate 160's consumer wire already reads).
**Headline:** disjoint `region` ranges (`[1,5]` vs `[100,200]`) → refined `estimated_row_count == 0` where
160 estimated `|L|·|R|/max(ndv) > 0`; paired partial-overlap-scales-down. **Payoff gate:** a 2-join plan
spends that zero child estimate through 160's existing composite `BroadcastEstimated` wire without replay,
plus a stale-range pair proving tight-budget replay still matches forced hash.

**ADR 162 lead candidates:** **heavy-hitter-join skew refinement** (the skew half — intersect per-side
top-k on the join keys, reuse 155/156; the direct sequel, catches the hot-key join miss range-overlap
can't); **measured residual join-miss evidence** (the 154/157 pattern — warranted now for what survives
161 + heavy-hitter, to gate any new sketch); **Theta/MinHash key-overlap producer** (the 156/159 new-sketch
pattern — gated on the evidence; likely a small residual after the two cheap reuses); **dimensioned
`CONVERT` / linear units** (parallel breadth, brainstorm 009, operator-energy); **Rune optimizer cockpit**
(brainstorm 010 — the fact spine now carries join cardinality + overlap-fraction gauges). **Forward arc:**
`161 range-overlap tightening → heavy-hitter-join skew → measured residual miss → Theta only for the
residual → multi-join reordering` · *(parallel: CONVERT/units; Rune cockpit)*. Evidence-gate 162 on the
161 feat.

### ADR 160 promoted; ADR 161 candidates (HISTORICAL)

ADR 160 promoted **Join-Output Cardinality from NDV Containment** — opens the **join-cardinality arc**,
reusing existing NDV (the 144 pattern, *not* a from-scratch Theta sketch — the roadmap's "Theta" lead was
reframed on grounding: join cardinality is greenfield (`bind_join` emits `composite_unknown()`) and NDV
facts already exist). A **complete Toron-local cut**, two halves: (1) a join-output `estimated_row_count`
from System-R **containment** `|L⋈R| ≈ |L|·|R| / max(ndv(L.x), ndv(R.y))` (the join analog of 144's
NDV-uniform filter estimate, same NDV facts), fired only on clean `Scan ⋈ Scan` (`SingleBaseRelation`
inputs — base NDV is stale over a `Filter`; fail-open otherwise), single equi-key first, checked/floored-≥1/
capped-at-cross-product; (2) the **consumer wire** — relax `build_byte_candidate` to size a parent join's
build side from a **composite estimated** input. **Why complete not dark:** the estimate would be inert
(the chooser's `build_byte_candidate` rejected `origin != SingleBaseRelation`), so a dark estimate is a
fact nobody reads (the too-granular shape); and relaxing the gate is **replay-safe** — the over-cap→hash
broadcast replay is **origin-agnostic** (verified: trigger = `BudgetExhausted{BroadcastReplication}` byte
budget; eligibility = `JoinStrategyIntent` Auto=Replayable; recovery = force-hash; none consult
`PlanOrigin`). So 160 opens the **estimate-channel** path for composite build sides only
(`BroadcastEstimated`, replayable); the **hard/terminal** path (`BroadcastInferred`) stays
`SingleBaseRelation`-only (a derived cardinality is never a hard bound). Estimate-channel only,
139/140-backstopped, `adr136` markers gain no join entry. **Headline:** `(orders ⋈ customers) ⋈ regions`
— the inner join's containment estimate sizes the outer join's build side → broadcasts without replay
(fails on `main`: inner = `composite_unknown` → no broadcast); **paired** with a disjoint-key over-estimate
that over-broadcasts and recovers byte-identically as hash (the D6 safety proof). **Containment's known
weakness** (disjoint / skew-correlated keys) is named as the future measured-miss → Theta/MinHash target;
160 is the baseline that makes the miss measurable.

**ADR 161 lead candidates:** **measured disjoint/skew-key join-miss evidence** (the 154/157 evidence
pattern, now that 160 gives a baseline to measure against — the direct sequel that grounds whether a
key-overlap sketch is worth it); **Theta/MinHash join-key intersection producer** (the 156/159 new-sketch
pattern — gated on the evidence above; do not build before the miss is shown); **dimensioned `CONVERT` /
linear units** (the parallel breadth track, brainstorm 009, operator-energy); **Rune optimizer cockpit**
(brainstorm 010 — the fact spine now carries NDV / min-max / top-k / range-distribution + `rank_error_ppm`
/ **join cardinality** receipts; Rune-primary). **Forward arc:** `160 join cardinality (live for 2-join
strategy) → measured disjoint/skew-key miss evidence → Theta/MinHash producer → multi-join reordering` ·
*(parallel: CONVERT/units; Rune cockpit, brainstorm 010)*. Evidence-gate 161 on the 160 feat. *(Hygiene
carry retired — adr077/adr031 went green at the 159 review; the full architecture binary is clean.)*

### ADR 159 promoted; ADR 160 candidates (HISTORICAL)

ADR 159 promoted **Range-Distribution Optimizer Fact Producer + Activation** — builds the **live**
producer to 158's bucket/CDF contract and **flips the dark consumer on**: a maintained **KLL** sketch
per opt-in column, merged across tiered slabs, queried into equal-mass buckets at the wire. **Single
cross-ring Toron ADR** — placement resolved Toron-home on the operator's **provenance principle**
("would Wendelon build a maintained quantile sketch if Toron didn't exist? No → Toron-home"), overruling
my own producer/consumer-split lean and Codex's full-relocate (the principle, not just the precedent,
now lives in the placement convention). **F1 = KLL, forced by the merge model** (self-verified): slabs
seal *independently*, so no slab knows the global domain → every boundary-carrying histogram
(equi-depth, equi-width) is unmergeable across the tiered `ndv_fact_for_column`→`HllRegisters::merge`
path → only **boundary-free KLL** survives; equal-mass buckets are derived *post-merge* from the global
CDF. KLL is Wendelon-private aggregate-state (like HLL registers); only buckets cross
`TABLE_OPTIMIZER_FACTS_GET`. The same insight explains **why 159 is approximate where 156 was exact**:
mergeability and exactness trade off — heavy-hitter keeps exactness by *declining* on incomplete;
a full distribution **buys mergeability with rank-error** (`rank_error_ppm` = the price, honestly
carried). **Coverage = all-contributors-or-decline** (the NDV/HLL pattern, no complete-flag) → partial
coverage = absence → **158's proven dark min/max-uniform fallback fires**. Approximation can still miss,
so the safety boundary is `rank_error_ppm` labeling, estimate-channel-only spending, and replay-backed
recovery. **Opt-in policy**
`ColumnRangeDistributionSpec { column_name, k: KllK }` on `ColumnStatsPolicyV1` (brand-clean, no new
`*V1`; reuse existing `KllK` with `AGG_KLL_DEFAULT_K = 200` and bounds `64..=2048`;
`rank_error_ppm` derived conservatively from the sketch/k); lowest-priority in
`apply_opt_in_stats_budget`. **Fixed N=64 equal-mass buckets** on the wire (decoupled from `k`; honesty
cap `N·rank_error ≲ 1`). Estimate-channel only (`adr136` hard-field
markers gain **no** range entry). Every producer seam mirrors an HLL sibling (build `:1227`, encode/decode
`:1718`/`:2284`, merge `manager.rs:695`, coverage `:5216`, policy `table.rs:1332`, budget `:1061`),
so the cut is large but templated; Phase 4 is the one-line dark→live flip
(`descriptor_optimizer_facts_from_storage` stops emitting empty). **Headline:**
`produced_range_distribution_fact_makes_clustered_filter_hash_without_seeding` (mirrors 156's
`lib.rs:1293`) — 157's clustered fixture ingested through the real producer across **≥2 sealed slabs**
(`force_seal_active_for_test`) → estimates ~9_000 from live merged buckets → hashes, `replayed == false`;
fails on `main`. Closes the **157→158→159 range-selectivity arc — range selectivity is now LIVE.**

**ADR 160 lead candidates:** **Theta join-key intersection** (the join-cardinality arc — now unblocked
by live single-column selectivity; heaviest, needs a set-sketch substrate for key-overlap estimation;
the natural sequel to "range selectivity live"); **dimensioned `CONVERT` / linear units** (the parallel
breadth track, de-risked, operator-energy, brainstorm 009); **Rune optimizer cockpit** (brainstorm 010 —
the fact spine now carries NDV / min-max / top-k / **range-distribution + `rank_error_ppm`** receipts;
the first concrete gauge surface, Rune-primary); **KLL `k` auto-tuning** (feedback loop from observed
selectivity-miss → adjust per-column `k`; small, evidence-driven). **Council hygiene (carry):** the
standalone architecture cleanup restored the full binary; keep the full `architecture_test` binary in the
review checklist so targeted ADR runs do not mask cross-guard drift. **Forward arc:** `159 range
selectivity live → Theta join-key intersection → join
selectivity → reordering` · *(parallel: CONVERT/units; Rune cockpit, brainstorm 010)*. Evidence-gate 160
on the 159 feat.

### ADR 158 promoted; ADR 159 candidates (HISTORICAL)

ADR 158 promoted **Range-Distribution Selectivity Consumer** — spends ADR 157's measured
range-skew evidence: a **Toron-only consumer, built dark** (the 141/155 pattern), refining
in-envelope range selectivity *when a distribution fact is present*, fixture-tested now, inert in
production until the Wendelon producer (159). **Consumer-first split** (3-of-3; grounded — Wendelon's
`KllSketch` is aggregate-state-not-CSB-payload, so the producer is a new maintained sketch, the
155/156 situation, *not* the 144 reuse). **Contract = bucket/CDF, sketch-agnostic** (Claude+Codex
grounded over Gemini's `Kll{bytes}` enum — a `Kll`-bytes payload would couple Toron's optimizer-fact
to Wendelon's private aggregate-state; the schema-owned-contract discipline): `ColumnRangeDistributionFact
{ arrow_type_tag, non_null_count, rank_error_ppm, buckets: Vec<{lower, upper, count}> }`. KLL is a
159 producer candidate/internal algorithm if chosen (queried into buckets before the wire); equi-depth
is `rank_error_ppm`-honest buckets. **Consumer refines in-envelope only:** envelope-prune via
`RangeFact` first (min/max owns zero-out, out-of-range → 0), then Σ covered-bucket counts +
partial-overlap interpolation; **fail-open
to 144/147 uniform** when absent/malformed/type-mismatch. **Approximate, not exact** (`rank_error_ppm`
rides the fact — distinct from 156's exact-or-decline); estimate-channel only, 139/140-backstopped,
never hard bound/`EmptyRelation`. Carrier mirrors `column_range` + **projection remap** in
`optimize_for_required` (the 155 catch). **No new range-distribution `*V1` type names.** **Why dark
(the load-bearing property):** absent fact → exact 144/147 min/max-uniform estimate and chooser path;
the proof is
`absent_range_distribution_falls_back_to_min_max_uniform` asserting *exact* equality (not "no crash")
— without it, an empty-fact-handling bug (`Some(0)`/floor/div-by-zero) would silently regress every
range filter fleet-wide → over-broadcast→replay tax, on a feature delivering nothing until 159. Two
safety layers: provable-darkness (protects production) + estimate-channel-only (protects correctness).
Genuine 3-of-3 on the dark-consumer pattern; the bucket contract grounded 2-of-3. Headline gate:
`range_distribution_fact_makes_clustered_range_filter_hash_without_replay` (157's clustered fixture +
seeded bucket fact → ~9_000 not 101 → hashes, `replayed == false`; fails on `main`).

**ADR 159 lead candidates:** **Wendelon range-distribution producer + activation** (the direct sequel
— a maintained KLL/equi-depth CSB payload built to 158's bucket contract: opt-in per-column policy,
ingest/compaction merge, the `TABLE_OPTIMIZER_FACTS_GET` extension, all-contributors coverage like
142/156, descriptor sync mapping; **grounds the KLL-vs-equi-depth call** where merge/rank-error/
serialization are real; **brand-clean names even when extending `ColumnStatsPolicyV1`**; Toron-sequence
cross-ring producer like 156 — makes 158's dark consumer *live*); **dimensioned `CONVERT` / linear
units** (the parallel breadth track, de-risked, operator-energy); **Rune optimizer cockpit** (brainstorm
010 — the thin Rune-only observability API the fact spine now feeds: NDV / min-max / top-k / range-
distribution receipts + confidence labels; a deliberate breadth cut); **Theta join-key intersection**
(the join-cardinality arc — heaviest, needs a set-sketch substrate). **Council hygiene (carry):** `main`
still has 2 pre-existing red architecture guards (`adr077` `optimizer.rs` line-ceiling; `adr031`
BoundExprNode count) — a standalone cleanup + **add the full `architecture_test` binary to the review
checklist**. **Forward arc:** `158 range-distribution consumer (dark) → 159 Wendelon producer (live) →
range selectivity live → Theta join-key intersection → join selectivity → reordering` · *(parallel:
CONVERT/units; Rune cockpit, brainstorm 010)*. Evidence-gate 159 on the 158 feat.

### ADR 157 promoted; ADR 158 candidates (HISTORICAL)

ADR 157 promoted **Measured Range-Skew Miss Evidence** — applies ADR 154's evidence
discipline to *range* selectivity, the next estimate-quality frontier after the 154→155→156
equality/IN heavy-hitter arc went live. **Toron-local evidence cut, no behavior change**:
proves whether 144/147's min/max-uniform range estimate materially misses, and names the
future quantile fact contract without building it. **The precise locus:** min/max is *not*
wrong — it stays correct for envelope pruning (out-of-range→0); the miss is **in-envelope
uniformity** (`estimated_rows_for_integral_range` = `non_null_count × overlap/domain`), exactly
as 154 measured the NDV-uniform assumption not NDV. **Deterministic fixture (3-of-3, RNG-free,
direct `LogicalPlan`):** `amount: Int64`, seeded `RangeFact(0, 10_000, non_null_count=10_000)`,
actual 9_000 rows clustered in `[0,100]` with tail rows outside the filter stretching the
envelope → `amount BETWEEN 0 AND 100` estimates **101** vs actual **9_000** (~89×
under-count). **The 154 three-state split:** (a) raw miss; (b)
decision-boundary → `BroadcastEstimated` → `BudgetExhausted(BroadcastReplication)` → 139/140
replay; (c) wrong-but-absorbed (under default budget, no replay) — so the cut is
decision-resolving either way. **Forward deliverable (inert):** `ColumnRangeDistributionFact`
(KLL/equi-depth quantile, refines *in-envelope* interpolation, min/max stays the envelope fact)
— a **distribution/quantile, NOT a top-k** (a different consumer contract than 155/156's
`HeavyHitterFact`); `KllSketch` exists today only as aggregate-state. Estimate-channel only;
no Wendelon/producer/public-EXPLAIN change; guards in a fresh `range_skew.rs` (not the
over-ceiling `optimizer.rs`). Genuine 3-of-3, no residual divergence. **Brainstorm 010
(SQL-auto / Rune-cockpit over one fact spine)** folded 3-of-3 as forward doctrine: 157 produces
a *gauge reading* that belongs in the future Rune optimizer cockpit, not SQL `EXPLAIN` — which
is *why* it keeps that out of the public surface. Headline gates:
`range_skew_filter_records_min_max_uniform_miss` + `range_skew_miss_triggers_broadcast_replay`.

**ADR 158 lead candidates:** **KLL / range-distribution producer + consumer** (the direct sequel
**if 157's (b) gate fires** — a Wendelon quantile stats payload built to 157's D5 contract +
a Toron consumer that refines in-envelope range selectivity; the 156-pattern cross-ring
producer-activation, Toron-sequence; exact KLL vs equi-depth buckets is the iter-1 call);
**reprioritize off the spine if 157 shows wrong-but-absorbed** (the range miss is real but within
139/140 headroom → low-value producer; pivot); **dimensioned `CONVERT` / linear units** (the
parallel breadth track, de-risked, operator-energy); **Theta join-key intersection** (the
join-cardinality arc — heaviest, needs a set-sketch substrate); **the Rune optimizer cockpit**
(brainstorm 010 — a thin Rune-only observability API the fact spine feeds; a deliberate breadth
cut alongside CONVERT); **Count-Min** (error-bounded frequencies, own consumer contract).
**Council hygiene (carry):** `main` still has 2 pre-existing red architecture guards (`adr077`
`optimizer.rs` line-ceiling; `adr031` BoundExprNode count) — a standalone cleanup + **add the
full `architecture_test` binary to the review checklist**. **Forward arc:** `157 range-skew
evidence → (if (b)) 158 KLL range-distribution producer+consumer → Theta join-key intersection →
join selectivity → reordering` · *(parallel: Linear-Dimension CONVERT; Rune optimizer cockpit,
brainstorm 010)*. Evidence-gate 158 on the 157 feat's measured outcome.

### ADR 156 promoted; ADR 157 candidates (HISTORICAL)

ADR 156 promoted **Heavy-Hitter Top-K Optimizer Fact Producer** — makes ADR 155's dark
heavy-hitter consumer **live**, closing `154 measured miss → 155 consumer → 156 producer`
into a production fix. A **Toron-sequence cross-ring producer + activation** (the
142/144/147 pattern — *not* Wendelon-relocated; corrects the prior roadmap note). Wendelon
adds an **opt-in per-column** heavy-hitter stats payload (`ColumnStatsPolicyV1.heavy_hitters`,
default-empty, budgeted `max_entries`/`max_bytes`), built **at write/compaction** (recomputed
from survivors, never request-time scan), behind a named CSB flag (`COLUMN_STAT_HAS_HEAVY_HITTERS
= 1<<9`); aggregates fully-covered payloads into `TABLE_OPTIMIZER_FACTS_GET`; Toron only
**maps** them into 155's carrier (replace the dark `Vec::new()`), binder/chooser/replay
unchanged. **Load-bearing correctness contract (Codex's exact-or-decline merge, 3-of-3):** 155
spends `frequency` as *exact*, so a bounded Space-Saving/Misra-Gries sketch is a *candidate
selector only* — the CSB payload stores **exact** counts for emitted keys + a `complete` flag
that is true only when all distinct local keys fit `max_entries`/`max_bytes`;
**absence from an incomplete local top-k is unknown, not zero**, so a table-level key is exact
only if every contributor proves it (has it, or `complete` proves absence), else that key is
omitted. After exact totals are known, policy `max_entries`/`max_bytes` caps are applied
deterministically; the column fact is omitted only if no entries survive. This keeps 155's
`Σ frequency`/uniform-tail honest across tiered/merged tables; undercounting-as-exact would
re-create the 154 over-broadcast. Estimate-channel only; 142 all-contributors coverage fence;
`ScalarLiteralKey` never crosses the wire (only `(arrow_type_tag, value_key: Vec<u8>)`);
mixed-version postcard compatibility uses an explicit without-heavy-hitters decoder, not
`#[serde(default)]` alone; Count-Min deferred (no error-bound consumer). Genuine 3-of-3
(placement Toron-sequence, the merge proof, opt-in/CSB-flag, wire boundary). Headline gate:
`produced_heavy_hitter_fact_makes_hot_key_filter_hash_without_seeding` (Wendelon-produced
frequency → hot key hashes, `replayed == false`, no test seeding; fails on `main`).

**ADR 157 lead candidates:** **equality/IN heavy-hitter selectivity now live** — the
154→155→156 arc is *complete in production*; the spine's next estimate-quality frontier is
**KLL/quantile range-skew evidence** (the 154 *measurement* pattern for ranges: do the 144/147
min/max-uniform *range* estimates miss on non-uniform spread? gate the eventual KLL producer);
**dimensioned `CONVERT`/units arc** (the parallel breadth track, de-risked across fixed+calendar
deltas, linear-units-first — now well-positioned since the equality-skew spine is live);
**Count-Min / error-bounded frequencies** (extend 155/156 to approximate hot keys with an
exposed no-under-count bound — its own consumer-contract ADR); **Theta join-key intersection**
(the join-cardinality arc — heaviest, needs a set-sketch substrate); **`SET TIME ZONE`/REPL**,
**byte-estimate tighten**. **Council hygiene (carry):** `main` has 2 pre-existing red
architecture guards (`adr077` `optimizer.rs` line-ceiling; `adr031` BoundExprNode count) — a
small standalone cleanup + **add the full `architecture_test` binary to the review checklist**
(targeted `--test architecture_test adrNNN` masked them). **Forward arc:** `156 producer (live)
→ KLL range-skew evidence → (range heavy-hitter / Count-Min) → Theta join-key intersection →
join selectivity → reordering` · *(parallel: CONVERT/units)*. Evidence-gate the next pick on
the 156 feat.

### ADR 155 promoted; ADR 156 candidates (HISTORICAL)

ADR 155 promoted **Heavy-Hitter Top-K NDV Selectivity Consumer** — spends ADR 154's
measured skew evidence: Toron learns to consume exact per-column top-k value frequencies
for equality/positive-`IN` selectivity, **built dark** (the 141→142 pattern), fixture-
tested now, inert in production until the Wendelon producer (156). **Placement was the
load-bearing call** (Claude's grounding, adopted 3-of-3 + operator-named "Consumer"):
Wendelon has **no frequency stat** (`small_set` = distinct-values-only `stats.rs:129`,
HLL = cardinality), so the producer is a **new maintained Space-Saving/Misra-Gries
sketch** — a separate Wendelon-home cut (discipline #5), *not* a cheap 144-style
candidate-loop. **Consumer:** reuses 148's key promoted to a shared `ScalarLiteralKey`
while preserving the current Key/Null/Unsupported literal classification (no wire
`Vec<u8>` in 155 — that's 156); carrier `column_heavy_hitters` mirrors
`column_ndv` (`TableOptimizerFacts` BTreeMap + `DescriptorOptimizerFacts` serde-default
Vec + `Scan` Vec + `for_descriptor` + dark `descriptor_optimizer_facts_from_storage`),
**remapped in `optimize_for_required` like `column_ndv`/`column_range`** under projection
pushdown. **Formula** (inject at `equality_ndv_estimated_row_count`/`in_list_…`): tracked
equality → `min(freq, B)`, untracked → uniform tail `ceil((B−T)/max(D−K,1))` where `T`
is the sum of all fact entries, `IN` → matched tracked sum `M` plus
`min(B−T, miss×tail)`, capped at `B`; absent/invalid/unsupported → existing 141/148
behavior; all-tracked `IN` may spend `M` without a tail, but mixed tracked/untracked `IN`
falls back if the tail is unavailable.
Seeded-fact validation (drop `freq==0` or `T>B`). **Estimate-channel only** with the
risk-direction honesty: spending `frequency` → larger estimate → hash; stale-high →
unnecessary hash (perf, never wrong), stale-low → over-broadcast → 139/140 replay
(recovered). Genuine 3-of-3, no residual divergence. Headline gate closes 154's (b) loop:
`hot_key_filter_with_heavy_hitter_fact_estimates_frequency_and_hashes` (seed `{region:
{'hot'→5000}}` → estimate 5000 not 100 → hash up front → `replayed == false`; fails on `main`).

**ADR 156 lead candidates:** **Wendelon heavy-hitter sketch producer + activation**
(the direct sequel — a *new maintained* Space-Saving/Misra-Gries top-k stat at
ingest/compaction, the wire `value_key: Vec<u8>` ↔ `ScalarLiteralKey` encoding, the
`TABLE_OPTIMIZER_FACTS_GET` extension, all-contributors coverage like 142; **Wendelon-home**,
relocates to the Wendelon sequence — this makes 155's consumer *live* and turns the dark
headline into a production fix); **Count-Min** (only with an exposed no-under-count error
bound — its own ADR); **KLL/quantile range-skew evidence** (the range analogue of 154 —
own measured-miss cut); **dimensioned `CONVERT`/units** (the parallel breadth track —
point/delta proven across fixed+calendar); **`SET TIME ZONE`/REPL**, **byte-estimate
tighten** (`binder.rs:70`). **Forward arc:** `155 consumer (dark) → 156 Wendelon producer
(activation) → live equality/IN skew correction → KLL range-skew → Theta join-key
intersection → join selectivity → reordering` · *(parallel: CONVERT/units)*. **156 is
Wendelon-home** — drafted in the Wendelon council / relocates at promotion per
ring-product-placement.

### ADR 154 promoted; ADR 155 candidates (HISTORICAL)

ADR 154 promoted **Measured Skew Miss Evidence** — the disciplined return to the CBO
spine after the temporal arc: a **Toron-local evidence cut** resolving the ~13-ADR
standing gate (*"measured-skew is gated on a measured miss we don't have"*). It **changes
no behavior** — uniform NDV selectivity (141/148), the chooser, 139/140 replay, and
Wendelon are untouched. It turns the theoretical hot-key gap into a **checked-in
deterministic test artifact** + a **precise next-fact contract**. **Two separated
measurements (the honesty core):** **(a) raw miss** — a deterministic skewed dim
(seeded `column_ndv = D`, hot literal at `H ≫ ceil(N/D)` rows, direct `LogicalPlan` so the
filter lands on the build side, *not* SQL `WHERE`) where `facts.estimated_row_count ==
ceil(N/D)` while actual hot rows ≫ that; **(b) decision-boundary crossing** — sized to
cross the broadcast cap → `BroadcastEstimated` → `BudgetExhausted(BroadcastReplication)`
→ 139/140 replay. The miss **matters only at (b)**: 139/140 give ~64× headroom, so a
**wrong-but-absorbed** gate (miss under the cap completes without replay) makes the cut
**decision-resolving** rather than assuming harm. **Forward deliverable:** an inert,
not-wired `HeavyHitterFact { column_ordinal, entries: Vec<{value_key, frequency}>,
exactness, coverage }` contract — `value_key` uses ADR148-compatible canonical scalar-key
semantics, tracked hot literal → `frequency`, untracked → uniform tail `(base_rows − Σ
frequency)/(D − k)`, all-contributors coverage (142), estimate-only — so ADR 155's
producer builds to spec. **Observational via existing `facts.estimated_row_count`;
no public EXPLAIN expansion** (`optimizer_explain_schema` unchanged), no Wendelon change,
estimate/replay fences intact. Genuine 3-of-3 (Codex: not-benchmark-only; Claude: the
two-claim split + wrong-but-absorbed; Codex+Gemini: the Top-K contract); two Gemini
fixture divergences resolved (direct `LogicalPlan` over SQL `WHERE`; wrong-but-absorbed
included). Headline gates: `skewed_hot_key_filter_records_uniform_ndv_miss` +
`uniform_ndv_hot_key_miss_triggers_broadcast_replay`.

**ADR 155 lead candidates:** **heavy-hitter / top-k fact producer + consumer** (the
direct sequel **if 154's (b) gate fires** — a cross-ring Wendelon producer of per-column
top-k (value, frequency) built to 154's D7 contract + a Toron consumer that spends
`frequency` for tracked hot literals; exact top-k first, Count-Min only with exposed
error bounds; equality/`IN` only, all-contributors coverage like 142); **reprioritize off
the spine if 154 shows wrong-but-absorbed** (the miss is real but within 139/140 headroom
→ the producer is low-value; pivot to CONVERT/units or another axis with evidence in
hand); **dimensioned `CONVERT` / units arc** (brainstorm 009 — point/delta proven across
fixed + calendar deltas; the parallel breadth track, linear-units-first); **KLL/quantile
range skew** (the range analogue — its own measured-miss gate); **`SET TIME ZONE` / REPL
client**; **tighten the byte estimate** (`binder.rs:70`). **Forward arc:** `154 skew
evidence → (if (b)) 155 heavy-hitter producer + selectivity correction → KLL/quantile
range evidence → Theta join-key intersection → join selectivity → reordering` ·
*(parallel: CONVERT/units)*. **155's pick is gated on the 154 feat's measured outcome** —
(b)-fires → producer; absorbed → reprioritize.

### ADR 153 promoted; ADR 154 candidates (HISTORICAL)

ADR 153 promoted **Calendar Month-Year Interval Arithmetic** — adds the Time
dimension's **calendar** delta (152 built the fixed delta; this closes the temporal
arc), the missing piece for MoM/YoY analytics. `ScalarValue::CalendarMonthInterval(
Option<i32>)` = signed **months**, physical Arrow **`Interval(YearMonth)`** (disjoint
from 152's `Duration`; `MonthDayNano` deferred for the later mixed-calendar cut).
**Month/year only** — `INTERVAL '1 day'` **stays 152's fixed 86 400 s** (one SQL
spelling, one meaning); week/quarter/compound reject. Algebra: `Timestamp ±
CalendarMonthInterval → Timestamp`, `cal + ts → ts`, `Date32 ± cal → Date32` (zone-free
civil), `cal ± cal → cal`; reject `cal − ts`, `ts + ts`, **`cal ± fixed Interval`** (no
category coercion), comparison/scaling. `ts − ts` stays 152's fixed `Duration`; calendar
intervals are **literal-construct-only**. **Load-bearing runtime-frame decision (grounded
Claude+Codex):** calendar timestamp math is zone-dependent but the executor is
**session-stateless**, so the effective zone is **resolved at bind** (explicit `AT TIME
ZONE` else session) and **carried to exec as Arrow tz metadata** via 151's inserted
`AtTimeZone` lens; **exec reads timestamp array timezone metadata** as the civil frame
and never asks for session state (over Gemini's under-specified "feed the timezone to
exec," which would repeat 152's session-stateless violation). **End-of-month clamping is
chrono machinery** (checked month add/subtract on local naive values; Jan 31 + 1 month
→ Feb 28/29; `checked_*` errors on overflow); DST reuses 151's `from_local_datetime`.
**Literal calendar arithmetic folds at bind** (effective zone known) → flat `Timestamp`
literal spending 150's range facts → YoY/MoM filters broadcast
(stays on the pushdown spine). No new IR node (reuses `Binary` + `AtTimeZone` + 152
seams); PBC declines `Interval(YearMonth)`; Rune `months(n)`/`years(n)` beside 152's
`days(n)`; Toron-only, no Arrow calendar kernel. 3-of-3 on topic/scope/carrier; the
runtime-frame mechanism promoted from the identical Claude+Codex grounding. Headline gate:
`relative_calendar_month_filter_spends_timestamp_range_fact` (fails on `main`).

**ADR 154 lead candidates — the temporal arc is now closed; a real arc-boundary pick:**
**measured-skew heavy-hitter / Count-Min** (the first estimate-*quality* spine cut —
breaks the uniform assumption all of 141→153 rests on; still gated on a *measured* miss +
a Wendelon producer, so the honest form may be an **evidence-generation cut**: a
bench/trace harness that produces the miss and names the fact shape); **dimensioned
`CONVERT` / units arc** (brainstorm 009 — point/delta now proven on Time across fixed +
calendar; a deliberate type-system pivot, linear-units-first); **temporal calendar tail**
(civil day/week/quarter syntax + `MonthDayNano` mixing + calendar comparison — small
completions, fold-able, low priority now that month/year ships); **`SET TIME ZONE` / REPL
client** (deferred persistent-session surface); **tighten the byte estimate**
(`binder.rs:70` 16 KiB — fold into the next estimate-surface cut). **Forward arc:** `153
calendar interval (closes temporal) → (measured-miss → skew/quantile spine) · (CONVERT/
units pivot) → Theta join-key intersection → join selectivity → reordering`. Four cycles
of temporal (149–153) are done; ADR 154 is a genuine spine-vs-breadth pivot — evidence-gate
the next pick on the 153 feat.

### ADR 152 promoted; ADR 153 candidates (HISTORICAL)

ADR 152 promoted **Fixed-Duration Interval Delta Arithmetic** — adds the Time
dimension's **delta** scalar (151's named deferral; brainstorm-009's first
point/delta instance). `ScalarValue::Interval(Option<i64>)` = signed elapsed
**nanoseconds**, physical carrier **Arrow `Duration(Nanosecond)`** (not Arrow
`Interval`, the calendar type — the fixed/calendar split maps onto Arrow's own
`Duration`/`Interval` split). **Fixed-duration only** (ns/us/ms/s/min/hour/`day`=
86 400 s explicitly fixed-not-civil); week/month/quarter/year + compound ranges
reject. Point/delta algebra bind-typed: `Timestamp ± Interval → Timestamp`,
`Interval + Timestamp → Timestamp` (add-only), `Timestamp − Timestamp → Interval`
(matching-unit, mixed rejects), `Interval ± Interval → Interval`, interval
ordering; `Timestamp + Timestamp`/`Date32 ± Interval`/`Interval × ÷` reject.
**No new IR node** — reuses `Add`/`Sub` + `Binary` via a dedicated **no-cast
temporal branch** (the generic binder's `cast_if_needed` would corrupt
`ts + interval` to `ts + ts`). **Precision is preserve-and-reject** (checked-
divisible ns→unit; non-representable errors, never silent-round — 150's discipline)
and all arithmetic is checked (overflow errors, never wraps). **Literal±literal
folds at bind** (new, contained — no const-fold pass existed) → a flat `Timestamp`
literal that 150's `range_scalar_for_fact` spends, so `event_time > TIMESTAMPTZ '…'
- INTERVAL '7 days'` broadcasts: **Interval is on the pushdown spine, not sugar.**
Rune-first constructors; PBC declines; manual checked tick math (no Arrow
duration/interval kernel — zone-free, needs none of 151's session machinery).
3-of-3 on topic/algebra/scope; two Gemini impl-level divergences resolved on
grounding (physical carrier = `Duration` not `Interval`; precision = reject not
truncate). Headline gate: `relative_interval_filter_spends_timestamp_range_fact`
(fails on `main`). Known bound: i64-ns caps a single interval at ±~292 y
(`ts(ms)−ts(ms)` of distant instants errors, never wrong; `i128` is the follow-on).

**ADR 153 lead candidates:** **calendar intervals** (`month`/`year`/civil-day —
zone-dependent, *reuses* 151's `from_local_datetime` civil path; the direct
temporal follow-on, now with the Arrow `Interval` carrier reserved); **`Date32 ±
Interval` / civil-date arithmetic** (pairs with calendar intervals — Date32 is
civil); **measured-skew heavy-hitter / Count-Min** (the first estimate-*quality*
spine cut — still gated on a *measured* miss + a Wendelon producer; evidence
first); **dimensioned `CONVERT` / units arc** (brainstorm 009 — now with point/delta
proven on Time; a deliberate type-system pivot, not a spine cut); **`SET TIME ZONE`
/ REPL client** (deferred persistent-session surface); **tighten the byte estimate**
(`binder.rs:70` 16 KiB constant — fold into the next estimate-surface cut).
**Forward arc:** `152 Interval → (calendar intervals · Date32 arithmetic ·
CONVERT/units arc) · (measured-miss → skew/quantile) → Theta join-key intersection
→ join selectivity → reordering`. Evidence-gate the next pick on the 152 feat.

### ADR 151 promoted; ADR 152 candidates (HISTORICAL)

ADR 151 promoted **Session-Zoned Temporal Operations** — makes the 149/150
`Date32`/`Timestamp` scalars *computable*, not just filterable: SQL `EXTRACT`/
`date_part`/`date_trunc`/`AT TIME ZONE` + first-class Rune helpers, **Toron-only**
(no Wendelon producer, fact schema, PBC opcode, persistent session, or
`SET TIME ZONE`). Toron has **no scalar-function framework**, so these are
hand-wired `Expr`/`BoundExprNode` variants following the `TimeBucket` precedent
(typed field enums; the single-input node already threaded through every
structural site). **Load-bearing decisions:** **bind-time zone resolution** — the
executor is **session-stateless** (`evaluate_bound_program_without_storage` has no
`PlannerConfig`), so the effective zone is resolved at bind (where the ctx carries
the resolved `session_timezone: Tz`, `binder.rs:506`) and **baked into the bound
node** (grounded 2-of-3 over Gemini's exec-time injection, which couldn't read a
session zone the executor lacks and was type-directed); **structural (not type-directed)
zone detection** — a plain UTC column and `AT TIME ZONE 'UTC'` are both
`Timestamp(_, Some("UTC"))`, so bind inspects the explicit `Expr::AtTimeZone`
wrapper, never the `DataType` tag, else the plain column wrongly extracts in UTC
instead of the session zone; `date_trunc` is a **calendar helper reusing 150's
`chrono-tz`/DST machinery verbatim** (`utc_datetime_from_ticks` → `with_timezone`
→ truncate civil → `from_local_datetime` [gap→error, fold→earlier] →
`ticks_from_utc_datetime`), **not** `TimeBucket` floor-division (which mis-buckets
DST-day/month/year), returning a UTC instant at bucket-start; `AT TIME ZONE`
annotates a UTC instant (same ticks, never naive `Timestamp(_, None)`); 150's
result-restamp retargeted to preserve explicit non-UTC zones (`daemon.rs:1473`);
`EXTRACT`/`date_part`→`Int32`; `TIME_BUCKET` stays fenced-experimental; optimizer/
PBC unchanged (declines all three; 150's `range_scalar_for_fact` UTC-column fence
intact). Genuine 3-of-3 on topic/scope/semantics; the resolution *mechanism* was a
2-of-3 resolved on the session-stateless-executor grounding (Gemini's ADR prose
correct, impl mechanism grounded-against). Headline gate:
`extract_hour_from_timestamp_at_time_zone_uses_named_zone` (session NY,
`EXTRACT(HOUR FROM event_time AT TIME ZONE 'America/Chicago')` → Chicago hour;
fails on `main`).

**ADR 152 lead candidates:** **interval arithmetic** (the deferred temporal
completion — `ts ± INTERVAL`, needs a new `Interval` scalar + typed add; the
natural "finish temporal" follow-on, a genuine mechanism boundary 151 stopped at);
**measured-skew heavy-hitter / Count-Min** (the first estimate-*quality* spine cut
— equality/IN under-estimate hot keys under the uniform assumption all of
141/148/149/150/151 rests on; **still gated on a *measured* miss** + a new Wendelon
producer, so evidence first); **`week`/`quarter` + Date32 `date_trunc`** (small
temporal completion — separate week-start/ISO + quarter calendar policy; fold-able,
not its own ADR); **`SET TIME ZONE` / REPL client** (the deferred persistent-session
surface — net-new client/protocol, the honest home for `SET`; no `SET` handler, no
REPL today); **tighten the byte estimate** (`binder.rs:70` 16 KiB framing constant
— tiny, fold into the next estimate-surface cut). **Forward arc:** `151 temporal
ops → (intervals · measured-miss → skew/quantile estimate quality) → Theta
join-key intersection → join selectivity → reordering`. Evidence-gate the next
pick on the 151 feat.

### ADR 150 promoted; ADR 151 candidates (HISTORICAL)

ADR 150 promoted **Timezone-Aware Timestamp Selectivity + Per-Request Session
Timezone** — makes `Timestamp` a first-class **UTC-instant** Toron scalar so event-time
filters (the #1 analytical predicate alongside dates) reach the min/max facts Wendelon
**already produces** for all three units (`ARROW_STATS_TAG_TIMESTAMP_{MS,US,NS}`,
`storage/stats.rs:400–406`); the producer + 144's integral range math are done, so the engine
half mirrors 149's Date32 scalar-foundation cut. **Folded in (operator's "one big cut"):** a
**per-request session timezone** so the #1 bare-string filter interprets in the user's zone.
**Toron-only, estimate-channel only, no Wendelon change.** The operator's doctrine — "a date
without a timezone is a fundamental type error" — sharpened to its precise target: the error is
the **naive instant** (`Timestamp(_, None)`); a civil **date** (`Date32`, 149) is a *different
category* (a calendar coordinate, legitimately zone-free), so **149 is not reopened** (the
two-category model). **Decisions:** `ScalarValue::Timestamp { value: Option<i64>, unit }`
(UTC-normalized, ms/us/ns, naive never constructed); **per-request session timezone**
(`session_timezone` on `ExecuteRequest` + `PlannerConfig`, plumbed via the **coordinator
clone-and-stamp override** the 139/140 replay path already uses, `coordinator.rs:655`; resolved
once via `chrono-tz`, **never ambient at bind**); **three literal classes** (explicit-naive-typed
→ reject; explicit-offset/`TIMESTAMPTZ` → normalize UTC; **bare string target-gated → interpret
in the session zone** via 149's `coerce_literal_value` hook — bounded-lenient, the resolved
strict-vs-lenient divergence, no UTC-guess because the zone is *declared*); **DST policy**
(gap→bind error, fold→earlier instant, invalid zone→bind error); **fact-spending via the column
`DataType`** (the tz-agnostic tag is insufficient — naive/non-UTC/unit-mismatch/Date32/Date64/
raw-int fail open); reuse 144/141/148 estimate-only; Arrow `Timestamp*Array` exec + **session-zone
output rendering** (tz-restamp + arrow `chrono-tz` feature); **PBC declines** (no untyped-int
opcode); **client per-invocation** (`toron sql --timezone <IANA>` + `iana-time-zone`
auto-detect — **no `SET`/REPL**, a deferred client-surface ADR); Date32 civil / Date64 dark.
**Deps:** `chrono-tz` + `iana-time-zone` direct Toron deps + arrow `chrono-tz` feature.
Operator-approved Claude/Codex convergence; **Gemini's unified-instant model rejected** (Date32-as-
session-midnight-instant reopens 149 and makes a civil date mean a different instant per
session). Headline gate:
`timestamp_session_zone_range_filtered_dim_infers_broadcast_by_estimate` (CT-session bare-string
event-time filter → `BroadcastEstimated`; fails on `main`).

**ADR 151 lead candidates:** **persistent-session / `SET TIME ZONE` client ADR** (the deferred
client surface — a REPL/driver session so the session zone persists across statements, the
honest home for `SET`; net-new client-surface, not a temporal helper); **`AT TIME ZONE` +
temporal arithmetic** (`date_trunc`/`EXTRACT`/intervals — runtime temporal *operations* on the
now-landed instant scalar; sqlparser already exposes `Expr::AtTimeZone`); **Date64/decimal
breadth** (decimal needs a new `Decimal` scalar + cross-ring producer — heavier than temporal
was; Date64 still has no coherent role); **measured-skew heavy-hitter / Count-Min** (the first
estimate-*quality* cut — equality/IN under-estimate hot keys under the uniform assumption all of
141/148/149/150 rests on; needs a new Wendelon producer + a *measured* miss to gate);
**KLL/quantile range precision** (gate on a measured min/max-uniformity miss); **tighten the byte
estimate** (the 149-pivot leftover C — replace the fat 16 KiB framing constant; tiny, fold into
adjacent work); **136-W/hard-zero/snapshot** (the deferred hard-channel + elision items — a
distributed-snapshot-isolation cut). **Forward arc:** `… → 149 Date32 → 150 Timestamp +
session-tz → (SET/REPL client · AT TIME ZONE/arithmetic · Date64/decimal) → measured-skew
sketches → Theta join-key intersection → join selectivity → reordering`. Evidence-gate the next
pick on the 150 feat.

### ADR 149 promoted; ADR 150 candidates (HISTORICAL)

ADR 149 promoted **Temporal Date32 Selectivity Breadth** — makes `Date32` a
first-class Toron scalar so date filters (the #1 analytical predicate) can reach the
optimizer facts Wendelon **already produces** (Date32 min/max, signed-4-byte day
numbers, `ARROW_STATS_TAG_DATE32`, `stats.rs:1774`). The producer + 144's discrete
integral range math are done, so the cut is the **scalar foundation** (literal,
comparison, exec, display) with range/equality/IN selectivity falling out for free.
**Toron-only, estimate-channel only, no Wendelon change.** **Pivoted from hard
materialized bytes** (rejected: Wendelon per-contributor framing isn't a hard bound
in Toron's runtime IPC unit — encoded framing scales with the runtime `live.chunks`
count, not the contributor count — and `BroadcastInferred` does **not** retire replay,
which fires on actual `PartitionPayload::size_bytes` over-cap by *source*, not by the
reason label; so hard-byte was a relabeled estimate for an EXPLAIN string — Claude's
two groundings, endorsed by Codex + operator; a real hard-byte ADR needs a measured
encoded-size ledger or a runtime batch-count contract). **Decisions:**
`ScalarValue::Date32(Option<i32>)`; **both literal forms (bounded-lenient)** — explicit
`DATE 'YYYY-MM-DD'` (`SqlExpr::TypedString`, the sqlparser-0.62 AST) + a **bare
`Utf8`-literal→`Date32` coercion** against a Date32 column (the resolved strict-vs-lenient
divergence — grounded that it *extends the existing* `coerce_literal_value` hook
`binder.rs:7687`, literal-only + target-gated, reusing the strict `parse_date_ymd`;
Codex conceded iter-5 — without it the #1 bare-string date filter wouldn't bind);
**strict int↔date** (Date32 facts spend only Date32 literals; a raw i32 day number
can't); Arrow-`Date32Array` execution + ISO display; direct DDL carrier
(`Physical(DataType::Date32)`, **no `LogicalTypePhysical` bump** — deferred);
**PBC pushdown preserves Date32 type or declines** (never a raw-int opcode); the
trivial consumer decodes `ARROW_STATS_TAG_DATE32` as `RangeScalar::Integral` and
reuses 144/141/148 estimate-only. Genuine 3-of-3 (coercion resolved on grounding;
`LogicalTypePhysical` deferral on the direct DDL carrier). Headline gate:
`date32_range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate`
(fails on `main`).

**ADR 150 lead candidates:** **Date64/Timestamp selectivity** (the immediate
same-pattern follow-on — i64-backed, reuse 149's machinery; Timestamp adds time-unit
+ timezone semantics worth its own cut); **decimal range/scalar breadth** (currency
columns — needs a `Decimal` scalar, engine-breadth); **measured-skew heavy-hitter /
Count-Min equality refinement** (equality + IN-list under-estimate hot keys today —
`base_rows/NDV` is wrong for skew, replay-recovered — gate on a *measured* miss, export
a consumer-shaped `column_value_frequency`); **KLL/quantile range precision** (gate on
a measured min/max-uniformity miss); **tighten the byte estimate** (the 149-pivot
leftover (C) — replace the fat 16 KiB framing constant in `estimated_byte_size`; tiny,
estimate-channel, replay-covered — fold into adjacent work, not its own ADR); **hard
materialized bytes done right** (a measured encoded-size ledger or single-batch encode
contract — deferred); **136-W/hard-zero/snapshot** (the deferred hard-channel +
elision items). **Forward arc:** `… → 149 Date32 → Date64/Timestamp → decimal →
measured-skew sketches → Theta join-key intersection → join selectivity → reordering`.
Evidence-gate the next pick on the 149 feat.

### ADR 148 promoted; ADR 149 candidates (HISTORICAL)

ADR 148 promoted **IN-List Equality Selectivity from Existing NDV Facts** — the
**predicate-shape** analogue of 147's type-shape completion: `141 equality NDV → 142
producer → 146 live base_rows → 148 IN-list spends the same fact`. A **Toron-only
consumer cut** (no Wendelon producer/schema/wire/fact-struct change, no type-system,
no snapshot infra), **estimate-channel only**. `col IN (v₁…vₖ)` was ignoring the live
NDV fact and falling back to the blunt default → IN-filtered dims hash. 148 extends
`predicate_node_equality_ndv_estimated_row_count` (`binder.rs`) with a positive
single-column `BoundExprNode::InList` arm over `SingleBaseRelation`: estimate
`min(base_rows, k·ceil(base_rows/NDV))` where `k` = distinct non-null literals,
`base_rows` = 146's live `row_upper_bound`. **Disjoint-union math:** distinct equality
classes on one column are disjoint (a row has one value), so the sum is the exact
union of present values; the conservative over-count is from per-value `ceil` + rare/
absent literals (suppresses some broadcasts — safe); it must never under-count by
assuming independence. **Load-bearing literal handling (D3):** dedup via a typed
`InListLiteralKey` (normalize `-0.0`→`+0.0`, key by bits — no reliance on
`ScalarValue` hashability); null members ignored; **any non-finite float literal
fails open the *whole* estimate — never skip a member, since skipping a `+inf`
(which can match rows) would under-count `k` → over-broadcast**. `NOT IN`/tuple/
subquery/expression-side IN out of scope. AND keeps 141's `min` (no correlation
model); estimate-channel only (never `row_upper_bound`/`byte_upper_bound`/
`EmptyRelation`). Converged 3-of-3 (Claude + Codex led IN-list from iter-0; Gemini's
hard-byte lead grounded as a proof-heavy follow-on — 138's encoded-IPC unit isn't the
schema width, so a hard byte fact needs a measured framing/buffer ledger, not a
relabeled estimate). Headline gate:
`in_list_filtered_dim_with_wendelon_ndv_infers_broadcast_by_estimate`
(`WHERE region IN ('north','south')`, ndv=100, base=10_000 → ~200 rows → broadcast;
fails on `main`).

**ADR 149 lead candidates:** **136-W-byte hard materialized-byte producer** (the
deferred byte half — *the proof is the feature*: a measured encoded-IPC framing/
alignment/validity-buffer upper-bound ledger for an eligible fixed-width type fence,
never compressed `size_bytes`; marginal-but-real after 146's row-anchored estimate —
it earns `BroadcastInferred`/no-replay for the slice where the conservative estimate
is just over threshold but the hard bound is under); **read-snapshot isolation +
`provenance_hlc`** (the prerequisite that unblocks **hard-zero folds** — production
reads are `HlcBound(0)`=latest with no `AS OF`, so elision is inert until bounded
snapshots + a facts-validity receipt exist; a distributed-consistency cut, not a
planner cut); **temporal types (Date32 start)** (unblocks 144's dark date/timestamp
producer + the #1 analytical range filter; full engine-breadth — `ScalarValue`/
literals/Arrow kernels/exec/display); **decimal range** (needs a `Decimal` scalar —
engine-breadth); **KLL/quantile** (skew-aware range; evidence-gate on a measured
min/max-uniformity miss); **Count-Min/heavy-hitter** (equality refinement after
measured hot-key skew — the IN-list/equality estimate under-estimates hot keys today,
replay-recovered); 140 A1; multi-column/correlated. **Forward arc:** `… → 147 float
range → 148 IN-list → hard-byte proof / snapshot+hard-zero / temporal+decimal breadth
→ KLL or heavy-hitter after measured misses → Theta join-key intersection → join
selectivity → reordering`. Evidence-gate the next pick on the 148 feat.

### ADR 147 promoted; ADR 148 candidates (HISTORICAL)

ADR 147 promoted **Float Range Selectivity from Maintained Min/Max** — completes
144's range story for Toron's last **native** scalar family (`f32`/`f64`); 144's
consumer decoded only `Int32`/`Int64`, so float columns were dark
(`arrow_stats_type_tag` returns `None` → CSB emits `OPAQUE_UNSUPPORTED`;
`range_fact_tag_supported` excludes float; the Toron helper is integer-shaped). A
**pure optimizer cut, no engine-breadth** (Toron already executes `f32`/`f64`),
**estimate-channel only** (no hard bounds, no `EmptyRelation`). Cross-ring like 144:
Wendelon adds schema-owned `ARROW_STATS_TAG_FLOAT32/64` (client-re-exported; tag-only,
no struct change) + a **finite-float min/max producer**, Toron consumes via a
**typed range domain**. **Two load-bearing impl realities (Claude's iter-4):** (1)
the producer is a min/max *computation arm* in `collect_column_stats_in_one_pass`,
not just a tag — downcast `Float32Array`/`Float64Array`, fold via `partial_cmp`,
**suppress the column on any NaN/±∞** (undefined order / mis-summarized envelope),
**normalize `-0.0`→`+0.0`**, encode order-preserving big-endian (sign-bit flip ≥0 /
all-bits <0);
(2) continuous intervals **can't reuse 144's integer `+1` successor** — `score > 0.8`
is *exclusive* 0.8, so the `Real` interval carries an explicit **inclusivity** flag,
which is the only thing distinguishing `>= max` (1 row) from `> max` (0 rows). The
`Integral` arm stays 144 **verbatim** (regression-guarded). Selectivity: `min==max`
all-or-zero, continuous `(high−low)/(max−min)`, non-empty overlap floors at ≥1 row,
outside-envelope → estimate 0 (never hard). Float ≠ Decimal (no Decimal scalar →
currency stays fail-open). **Hard-zero folds grounded out of this cycle** (the
3-iter debate): elision is correctness-sharp and its freshness proof is both absent
*and inert if built* — production reads are `HlcBound(0)`=latest (`daemon.rs:1274`),
no `AS OF`/bounded-snapshot path, so a correct contract would fire for zero queries
(Codex's stale-cache counterexample + Claude's read-bound grounding; a Claude↔Codex
iter-1 cross-over resolved on grounding, Claude retracted + proved the inertness).
Retargets 144's `range_utf8_and_float_facts_fail_open`; keeps 144 estimate-channel +
146 no-elision guards intact. Genuine 3-of-3. Headline gate:
`float_range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate`
(fails on `main`).

**ADR 148 lead candidates:** **hard zero-row folds** (the deferred 146/147
elision — needs *both* a cross-ring facts/snapshot-validity receipt, e.g. Gemini's
`provenance_hlc`, *and* bounded-snapshot reads, since `HlcBound(0)`=latest makes it
inert today — a distributed-snapshot-isolation cut, not a planner cut); **136-W-byte
hard materialized-byte producer** (the deferred byte half — marginal post-146's
row-anchored estimate + needs a *measured* encoded-size ledger, never compressed
`size_bytes`); **decimal range** (needs a `Decimal` scalar — engine-breadth);
**temporal types (Date32 start)** (unblocks 144's dark date/timestamp producer; the
#1 analytical range filter; engine-breadth — `ScalarValue` lacks temporal);
**KLL/quantile range precision** (skew-aware; new maintained producer; evidence-gate
on a measured min/max-uniformity miss); 140 A1; multi-column/correlated selectivity.
**Forward arc:** `… → 146 hard row → 147 float range → (hard-zero once snapshot reads
exist · 136-W-byte · decimal/temporal) → KLL/quantile → Theta-sketch join-key
intersection → join selectivity → reordering`. Evidence-gate the next pick on the
147 feat.

### ADR 146 promoted; ADR 147 candidates (HISTORICAL)

ADR 146 promoted **Hard Table-Size Optimizer Facts (Live Hard Row Upper Bounds)** —
**lights the dark hard channel**, the through-line gap across 141→145: production
catalogs mapped `row_upper_bound`/`byte_upper_bound` to `None`
(`catalog.rs::descriptor_optimizer_facts_from_storage`), so the whole CBO ran on
estimates + 139/140 replay. 146 activates the **row** half: Wendelon emits an exact
hard `row_upper_bound` (sum of `ColumnStatsBlockHeader::row_count` under the existing
142/144 all-contributors coverage fence) on the facts blob; Toron imports it into the
consumer ADR 136 already built (drop the hard-coded `None`). **Two production wins
from the one missing piece:** (a) unfiltered small dims **broadcast** (no row source
to anchor a byte estimate before — `fact JOIN small_dim` hashed); (b) **141/142
equality-NDV finally fires live** — `filter_equality_ndv_estimated_row_count`
(`binder.rs:3791`) is gated `base_rows = facts.row_upper_bound?`, so it was **inert
on real tables** (seeded fixtures only); 146 supplies `base_rows`. **Row-hard /
byte-estimated split:** no Wendelon materialized-byte field — ADR 138 established
Toron can't *prove* the encoded-IPC byte unit (fixed-width byte is an estimate), so a
hard byte producer is the deferred 136-W-byte follow-on (never compressed
`size_bytes`); 138 anchors the byte *estimate* to the hard rows (145 sharpened it).
**Producer restructure:** `table_optimizer_facts` early-returned `None` with no
NDV/range candidates (`manager.rs:5067`) — row production is made
candidate-independent so stat-less (string-only) tables get a hard row count too.
**Truthful zero, no new elision:** a covered empty table emits `row_upper_bound = 0`
(sizing-only; `for_descriptor` normalizes row-0→byte-0) — grounded that a row-0
*fact* does **not** create `EmptyRelation` today (`binder.rs:9820`; elision is
static-false-predicate-only), so it's replay-protected; the hard-zero *fold*
(scan→`EmptyRelation` rewrite) is the deferred correctness-sensitive follow-on
(resolved Gemini's iter-1 elide-on-0 → no-new-elision on Claude's grounding).
**Post-139/140 hard-bound risk model:** a stale-low row bound (zero or non-zero) →
over-broadcast → replay → byte-identical; the only un-replay-protected consumer
(elision) is excluded. Retargets the ADR 142 hard-bound guard (NDV/range still don't
synthesize hard bounds; only the explicit Wendelon field maps). Cross-ring activation
in the Toron sequence (142/144 precedent). Genuine 3-of-3 (Live Hard Row Upper Bounds;
row=0/no-elision resolved on Claude's grounding; byte half deferred on 138's
encoded-IPC-not-provable grounding). Headline gate:
`wendelon_live_row_upper_bound_enables_unseeded_small_dimension_broadcast` (live,
unseeded; fails on `main`).

**ADR 147 lead candidates:** **136-W-byte — hard materialized-byte producer** (the
deferred byte half: a *proven* materialized/encoded build-footprint upper bound — the
hard part 138 declined; never compressed `size_bytes`; would give replay-independent
hard-byte broadcast); **hard zero-row folds** (the deferred elision: promote a
complete-small-set membership-absence / 144 out-of-range / row-0 to `EmptyRelation`
with an airtight elision-freshness doctrine — fold ADR 095; Gemini's narrowed lead);
**KLL/quantile range precision** (skew-aware 144 follow-on; new maintained producer;
evidence-gate on a measured min/max-uniformity miss); **float range selectivity** (the
one excluded type Toron has — producer float-tag + IEEE order-preserving encode +
continuous consumer); **temporal types (Date32 start)** (unblocks 144's date/timestamp
range consumer; Toron `ScalarValue` lacks temporal; multi-ADR); 140 A1;
multi-column/correlated selectivity. **Forward arc:** `… → 146 hard row bound →
(136-W-byte hard materialized bound · hard-zero folds) → KLL/quantile → Theta-sketch
join-key intersection → join selectivity → reordering` (Theta still gated on both a
set-sketch substrate and a join-reordering consumer, neither of which exists).
Evidence-gate the next pick on the 146 feat.

### ADR 145 promoted; ADR 146 candidates (HISTORICAL)

ADR 145 promoted **SQL Derived Tables in Joins (+ Projected Build-Side Byte
Estimation)** — completes the SQL filtered-build authoring surface 143/144 opened,
and unlocks **build-side column pruning**. SQL rejected a derived table as a join
factor (`sql_planner.rs:2778, 2792`), so a user couldn't author the projected
filtered-dim shape Rune builds directly — `Join(fact, Alias(Project(Filter(Scan))))`
— that prunes the build to the join's needed columns. **The load-bearing council
correction: this is NOT a frontend-only cut.** All three iter-1 drafts (Claude
included) asserted the column-pruning win falls out of lifting the reject; Claude's
iter-2 checked the byte path and showed it does not — `estimated_byte_size` is
derived at the **Filter** on the *full* schema (`binder.rs:614`), and a pure-subset
`Project` preserves that full-width estimate **unchanged** (`preserve_through_project`,
`binder.rs:235`), and the chooser reads the stored fact, not the projected schema. So
the cut is **two coupled halves**: **D1 (frontend)** admit v1 derived tables as join
factors in all-inner chains (reuse `validate_derived_select_shape`; no new IR); **D2
(binder)** re-derive `estimated_byte_size` at a pure-subset project from the projected
`output_schema` (`derive_estimated_byte_size` is `SingleBaseRelation`-gated,
`min`-combining, schema-width → a **monotonic estimate shrink**, estimate-channel
only, never the hard `byte_upper_bound`). D2 is a **latent over-estimate fix that
travels with the ADR** — every pure-subset project today (Rune `.select()` included)
carries inflated full-width bytes. **Honesty gate** (`sql_join_projected_derived_dim_broadcasts_where_full_column_hashes`):
the projected derived dim broadcasts where the full-column 143 form hashes under
identical 144 facts — **fails on `main`, and fails if either half is reverted**. The
headline works via **fact propagation** (the selective predicate lives inside the
derived table → inner Filter reads scan facts → D2 carries the reduced byte estimate
up through Project + Alias); no through-`Alias` fact lookup added; 143 pushdown +
all-inner fence preserved. Reconciles the guards D2 reopens (143 no-longer-blanket-
derived-reject; 137 still owns conservative hard byte; 138 still estimate-only).
Genuine 3-of-3 (D1 from iter-1; D2 — Claude's iter-2 byte-path catch — carried by
the merged final plan). No chooser, executor, storage, or Wendelon change beyond
the named binder estimate refinement.

**ADR 146 lead candidates:** **136-W hard table-size producer** (light the dark
hard channel — live `row_upper_bound`/`byte_upper_bound` are `None`,
`catalog.rs::descriptor_optimizer_facts_from_storage`; a Wendelon producer of hard
table-size bounds the 129/136 spine already consumes); **hard zero-row folds /
hard-from-exact** (complete-small-set membership absence + 144 out-of-range →
*hard* 0-row → prune/short-circuit, fold ADR 095; needs the freshness doctrine,
narrowed per the 145 cycle to provable-empty since Exact NDV gives no hard
non-empty bound); **KLL/quantile range precision** (skew-aware 144 follow-on; a new
maintained producer, evidence-gate on a measured min/max-uniformity miss);
**float range selectivity** (the one excluded type Toron *has* — producer float-tag
+ order-preserving IEEE encode + continuous consumer); **temporal types in Toron**
(the big unlock behind 144's blocked date/timestamp range consumer — Toron's
`ScalarValue` is `{Bool,Int32,Int64,Float32,Float64,Utf8}`; a multi-ADR feature,
could start with `Date32`); 140 A1; multi-column/correlated selectivity. **Forward
arc:** `... → 145 column pruning → (hard channel: 136-W / hard-from-exact) →
KLL/quantile → Theta-sketch join-key intersection → join selectivity → join
reordering` (Theta stays gated on both a set-sketch substrate *and* a
join-reordering consumer, neither of which exists). Evidence-gate the next pick on
the 145 feat.

### ADR 144 promoted; ADR 145 candidates (HISTORICAL)

ADR 144 promoted **Labeled Range Selectivity from Maintained Min/Max** — the second
stats-backed selectivity class (the first since equality NDV), attacking range
predicates (`< <= > >=`, `BETWEEN`) that got a blunt default (the 141 guard
`range_filter_does_not_use_ndv_selectivity` pinned it). Wendelon already maintains
**always-on** per-column min/max + null/non-null counts
(`build_always_on_column_stats_block`, `stats.rs:638`) — *always-on*, unlike the
*opt-in* HLL/small-set NDV the 141→143 path rides, so range selectivity **fires
broadly day-one**. v1 is **min/max** (KLL/quantile skew-precision is the named
follow-on, `sketch.rs` is query-aggregate-only). A cross-ring cut mirroring 142:
Wendelon adds `column_range: Vec<ColumnRangeFact>` (additive on the 142
`TABLE_OPTIMIZER_FACTS_GET` blob); the range scalar tag/decode contract is
schema-owned and re-exported through `wendelon-client` (no Toron import of Wendelon
storage stats), and mixed-version postcard compatibility must be explicit rather
than assumed from `serde(default)`. Toron maps the fact and the binder derives a
**labeled approximate** uniform selectivity into the **estimate channel only**
(`estimated_row_count`, never `row_upper_bound`/`byte_upper_bound`, never elides a
scan), feeding 138's byte estimate → broadcast. **The load-bearing grounding:** live
facts map `row_upper_bound` to **`None`** (`catalog.rs::descriptor_optimizer_facts_from_storage`),
so the fact **carries its own `non_null_count`** and the estimate derives from it —
estimating from `row_upper_bound` would have shipped **inert** (the 141/142
build-but-dark lesson; Codex's iter-2 catch, verified, over Claude's iter-1
`base_rows`). **Discrete-domain interpolation** `ceil(non_null_count · overlap /
(max−min+1))`; **`BETWEEN` is a real band** (intersect same-column bounds, tightest
[low,high], contradictory → 0; over a stacked-`min`); **estimate from min/max — not
NDV** (D8 retargets the range guard; NDV stays equality-only); **full-contributor
coverage or absent** (reuse 142's fence — a partial/truncated merge narrows the
range → under-estimate → over-broadcast, the aggressive direction); **141
no-independence** (single tightest estimate across equality + range, no product).
**v1 consumer decodes `Int32`/`Int64` only** (Toron's literal surface);
unsigned/date/timestamp ride the sidecar fail-open; float/decimal/utf8/binary/bool
fail open. No default-policy flip, no hard bound, no scan elision. Genuine 3-of-3
(min/max-v1 from iter-1; `non_null_count` inertness fix + band + discrete interp
converged Codex+Gemini, conceded by Claude on verified grounding). Headline gate:
`range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate` (fails on
`main`).

**ADR 145 lead candidates:** **KLL/quantile precision** (skew-aware range
selectivity where min/max uniformity is too coarse — a *new maintained* producer,
evidence-gated on 144's feat showing uniformity-miss cases); **SQL
derived-tables-in-joins** (the Toron-local frontend completion 143/144 deferred —
lift the `FROM fact JOIN (SELECT … WHERE …) d` reject, machinery exists at
`lower_derived_table:3624`, spine survives `Alias`, `binder.rs:560`); **date/timestamp
+ unsigned range consumer** (decode the tags 144's producer already emits — a small
consumer-only extension); **hard-from-exact** (freshness/coverage doctrine letting a
fresh complete `Exact`/full-coverage min/max write a hard bound — would let 141/144
provable-empty become hard); **stats default-policy flip** (widen NDV coverage past
opt-in); **Theta-sketch join-key intersection** (the join-cardinality arc; heaviest);
140 A1; multi-column/correlated selectivity. **Forward arc:** `NDV equality (141) →
142 producer → 143 SQL → 144 min/max range → KLL/quantile → Theta-sketch join-key
intersection → join selectivity → join reordering`. Evidence-gate the next pick on
the 144 feat.

### ADR 143 promoted; ADR 144 candidates (HISTORICAL)

ADR 143 promoted **SQL Inner-Join Side-Local Filter Pushdown** — the
secondary-frontend completion of 141's headline, worth doing now that 142 made the
NDV facts live. SQL lowered `WHERE` *above* the join (`Filter(Join(...))`,
`sql_planner.rs:2782–2802`), so the chooser (`binder.rs:1420`, bottom-up, sees only
children) was handed the **unfiltered** dimension → 138 over-estimates → hash, where
Rune (`Join(..., Filter(dim))` by construction, `rune_engine.rs:302`) broadcasts.
143 splits the top-level `WHERE` conjuncts and pushes each **side-local** one onto
its **tightest single base input** during `lower_select`, reusing
`split_top_level_and` + `collect_scope_refs` (+ a new `ScopeRefs.inner` flag) +
`plan_qualifiers` (recurses through `Filter`), emitting existing
`LogicalPlan::Filter` nodes — **no binder/chooser/executor/replay/Wendelon change**.
**Placement resolved an iter-1 cross-over to the SQL frontend on grounding** (a
binder LP rewrite would reimplement the `SqlExpr` classifiers on logical `Expr`, and
its frontend-agnostic generality is illusory — Rune builds the filtered shape
directly, so SQL is the sole `Filter(Join)` producer; a binder phase is the right
refactor only when a second such frontend lands). **The load-bearing correction: the
all-inner-chain fence** — a *per-join* push (push at an inner boundary, skip at an
outer one) and an *upfront* push (wrap the owning factor before the chain is built)
are **both unsound** when an outer join sits *above* a pushed input
(`a INNER JOIN b RIGHT OUTER JOIN c WHERE b.x=1` → ∅ correctly, `{id, NULL}` if `b`
is pre-filtered, because `b` is on the RIGHT join's null-producing side); so pushdown
activates **only when every FROM join lowers to `Inner`**, else the full `WHERE`
stays above (byte-identical to `main`). **Tightest single input incl. the leftmost
base** — broadcast-relevant, not probe-only (the chooser evaluates a left build
candidate, `binder.rs:4268`, so a dim authored left broadcasts only when its leftmost
filter is pushed). Pushdown is **decoupled from NDV estimability** (push every
supported side-local predicate; non-estimable ones still earn runtime scan
selectivity); **residual preservation through the unchanged `lower_where`** (EXISTS
verbatim), pinned by a pushed-vs-above-join digest-equivalence gate. Retargets
`adr141_no_sql_join_filter_pushdown_in_this_cut` → six `adr143_*` guards. Genuine
3-of-3: topic + placement converged by iter-2; the fence correction (Claude, against
a per-join/upfront framing) adopted by **both** peers from a data-grounded
counterexample by iter-3; Codex's leftmost-is-broadcast-relevant refinement folded.
Headline gate: `sql_equality_filtered_dim_with_wendelon_ndv_infers_broadcast_by_estimate`
(fails on `main`).

**ADR 144 lead candidates:** **quantile/range selectivity** — the *second* stats
producer (inequality selectivity via Wendelon KLL + 138 byte hardening; a new
maintained-producer extension; the named arc-next, now gated on 143's feat
evidence); **SQL derived-table-in-join** (lift the `FROM fact JOIN (SELECT … WHERE …)
d` reject, `sql_planner.rs:2778, 2784` — the other filtered-build-side authoring,
143's nearest neighbor); **mixed-chain path-aware pushdown** (143's named follow-on —
push a conjunct iff its target base is on the preserved side of every join above it,
widening past the all-inner fence); **stats default-policy flip** (a storage-cost
product call to widen NDV coverage past opt-in — would make 142/143 fire on more
tables); **hard-from-exact** (a freshness/coverage doctrine letting a fresh complete
small-set `Exact` write a hard bound); 140's A1 (join-identified replay);
multi-column stats. **Forward arc (named, not next):** `NDV equality selectivity
(141) → 142 producer → 143 SQL spends it → quantile/range → Theta-sketch join-key
intersection → join selectivity → join reordering` (HLL is single-column NDV, *not*
intersections — Theta-sketches are the likely family for join-key intersection
facts). Evidence-gate the next pick on the 143 feat.

### ADR 142 promoted; ADR 143 candidates (HISTORICAL)

ADR 142 promoted **Wendelon NDV Optimizer Fact Producer** — the **cross-ring activation cut** that
makes the built-but-dark 129→141 CBO consumer live for covered opt-in tables. 141 landed the Toron NDV consumer,
but live descriptor sync emitted no facts (`column_ndv: Vec::new()`); 142 has **Wendelon produce**
table-level per-column NDV from its already-maintained **opt-in HLL/small-set** stats and **Toron
fetch** it during descriptor sync into `DescriptorOptimizerFacts.column_ndv`. **Wendelon-owned fact
type** (`TableOptimizerFacts { …identity…, column_ndv: Vec<ColumnNdvFact> }`,
`NdvFact = Exact{count} | Approx{estimate, rel_err_ppm}`; no Toron import — Toron maps it). **A
separate, fail-open facts RPC** (`TABLE_OPTIMIZER_FACTS_GET` ~`0x46/0x47`, *not* folded into
descriptor get/list, which stay length-checked descriptor-only): fetch failure / unsupported / stale
identity / no coverage all degrade to descriptor-only sync — the endpoint is **advisory, never a
planning availability dependency**, and Toron caches no Wendelon stats. **Full live-contributor
coverage or absent (D3):** a table-level NDV requires every visible contributor — **active tail +
sealed L1 + tiered runs** — to carry compatible stats; otherwise the column is unknown, and the
producer **does not scan active data at sync time** to manufacture a fact (sparse rollout is the
right trade). **Merge raw HLL registers** (`HllRegisters::merge`, estimate from merged), not
per-contributor `DistinctCount` scalars; complete small-set union → `Exact`; quantize
`std_err`→`rel_err_ppm` at the boundary (141 chose integer error to keep facts `Eq`). **No
default-policy flip, no `TableSchemaDescriptor` mutation, no hard-bound write; the 141 consumer is
unchanged** — NDV stays labeled-approximate in the estimate channel. **Risk vocabulary corrected:**
the aggressive (over-broadcast) risk is an *under-estimated row count from an inflated NDV*
(HLL inflation / skew / stale / hot literal), bounded by 139/140 replay — a partial-subset merge
*under*-counts NDV → *over*-estimates rows → conservative; so all-contributors-or-absent is a
**fact-honesty** fence, not over-broadcast prevention (corrects the iter-1/Gemini framing). Genuine
3-of-3 on Codex's final iter-2; Gemini's reversed coverage-fence rationale surfaced + corrected.
Headline gate: `rune_equality_filtered_dim_broadcasts_over_wendelon_ndv_no_seed` (fails on `main`).

**ADR 143 lead candidates:** **SQL join-filter pushdown** (side-local `WHERE` to the build / joins
around filtered derived tables — unlocks the SQL form of 141's headline, the secondary frontend; now
worth doing since 142 makes facts real); **stats default-policy decision** (a storage-cost product
call to widen NDV coverage beyond opt-in columns — would make 142 fire on more tables);
**quantile/range selectivity** (the *second* stats producer — inequality selectivity + 138 byte
hardening; KLL is a query aggregate, a new maintained-producer extension); **hard-from-exact** (a
stats-freshness/coverage doctrine letting a fresh complete small-set `Exact` write a hard bound);
140's A1 (exact join-identified replay); multi-column stats. **Forward arc (named, not next):** HLL
is for single-column NDV, *not* intersections — **Theta-sketches** are the likely Wendelon family
for join-key intersection facts → `NDV equality selectivity (141) → 142 producer → quantile/range →
Theta-sketch join-key intersection → join selectivity → join reordering`. Evidence-gate the next
pick on the 142 feat.

### ADR 141 promoted; ADR 142 candidates (HISTORICAL)

ADR 141 promoted **Labeled NDV Equality Selectivity Estimates** — the first stats-backed CBO cut,
attacking the *root* of the 138/139/140 arc (blunt estimates → over-cap broadcasts) instead of the
recovery symptom. Toron consumes a labeled **approximate** per-column distinct-count (NDV)
optimizer fact — `DistinctCountFact { Exact(u64), Approx { estimate, rel_err_ppm } }` (integer error
so it derives `Eq`) — riding the descriptor-adjacent fact sidecar (136 pattern: serialized
`DescriptorOptimizerFacts` gets `column_ndv` entries, normalized into a `TableOptimizerFacts`
`BTreeMap`, and then carried onto `ExecutionPlan::Scan` beside `primary_key_columns`). A `Filter`
proving `col = <literal>` over a single-base scan derives
`estimated_row_count = 0` for a hard zero-row base, else `max(1, ceil(base_rows / max(1, NDV(col))))`
into a **new estimate-only channel**
(`PlanOptimizerFacts.estimated_row_count`, never the hard `row_upper_bound` — even for `Exact`;
hard-from-exact needs a freshness/coverage doctrine, deferred), which feeds 138's
`estimated_byte_size` — so a common star-schema **build-side filtered dimension**
(`customers.filter(region == "north")` in Rune/direct IR; 137 leaves it at full dim size, 138 then
estimates too big) gets a realistic filtered estimate and broadcasts. **The runtime stays honest:**
approximate ≠ hard (guarded); equality-only (ranges need quantiles — the
*second* stats cut); top-level `AND` takes the **single most-selective factor** and stacked filters
take `min` (no independence product); a skewed-key under-estimate that over-caps is recovered by
139/140 replay (the net that licenses spending an approximate estimate). **Two grounded premises:**
Wendelon's HLL/small-set distinct-counts are **opt-in persisted CSB stats, not always-on**
(`ColumnStatsPolicyV1` defaults empty) → 141 is a 136-style **consumer-now / producer-named** pair
(141-T inert until 141-W, fixture-testable now); and SQL lowers `WHERE` **after** the join
(`sql_planner.rs:3693`) so the dim build side is unfiltered → the headline is **Rune + direct-IR**,
SQL join-filter pushdown a named follow-on ([[rune_is_primary_interface]]). Genuine 3-of-3 (no
divergence); Codex's `rel_err_ppm`-keeps-`Eq` and stacked-filter-`min` refinements folded. Headline
gate: `rune_equality_filtered_dim_with_ndv_infers_broadcast_by_estimate` (fails on `main`).

**ADR 142 lead candidates:** **141-W** — the Wendelon NDV producer (expose table-identity-keyed
per-column NDV adjacent to descriptor sync, merging persisted CSB HLL across row-groups, preferring
a complete small-set as `Exact`; decide the opt-in-vs-default-stats-policy question) — the
cross-ring half that makes 141 fire in production; **SQL join-filter pushdown** (side-local `WHERE`
to the build / joins around filtered derived tables — unlocks the SQL headline 141 deferred);
**quantile/range selectivity** (the *second* stats cut — inequality selectivity + 138 byte-estimate
hardening; KLL is a query aggregate, not maintained, so a new Wendelon producer extension);
hard-from-exact (a stats-freshness/coverage doctrine letting a fresh exact small-set write a hard
bound); 140's A1 (exact join-identified replay); multi-column stats. **Forward arc (named, not
next):** **Theta-sketch join-key intersection** — HLL gives union/cardinality but *not* intersection
(inclusion-exclusion compounds error), so join cardinality (`|fact ⋈ dim|`) needs Theta-sketches;
a future Wendelon producer maintains per-join-key Theta-sketches → labeled-approximate join
selectivity → the line **NDV equality selectivity (141) → quantile/range selectivity →
Theta-sketch join-key intersection → join selectivity → join reordering**. Evidence-gate the next
pick on the 141 feat.

### ADR 140 promoted; ADR 141 candidates (HISTORICAL)

ADR 140 promoted **Failed-Task-Scoped Broadcast Replay** — sharpens ADR 139's blunt recovery.
139 made an over-cap broadcast an availability event recoverable by a one-shot whole-query hash
replay, but its `force_hash_joins` was **global**: one over-cap build demoted *every* replayable
broadcast in the query, and a Rune/Direct pin *anywhere* vetoed replay entirely — degrading the
common ADR 135 broadcast-star (lose dim1/dim3 to recover dim2). 140 gives each join a stable
**`JoinReplayId`** (the existing deterministic bind-time `join_index`, carried on
`ExecutionPlan::Join` with a reduced replay-eligibility tag, preserved through fragmentation),
replaces `force_hash_joins: bool` with `force_hash_join_ids: BTreeSet<JoinReplayId>`, and on a
`BudgetExhausted(BroadcastReplication)` failure collects the **replayable broadcast id *set* in
the failed task** from that execution metadata and replays forcing only that set to hash — healthy
broadcasts in other tasks/stages stay broadcast, and a SQL/Auto broadcast replays even when a Rune
pin lives elsewhere (the named 139 mixed-source gap). Source eligibility per 139 (Auto/SqlHint
replayable; Rune/Direct terminal) is now keyed by id, replacing 139's global terminal-pin veto,
without carrying full `JoinStrategyIntent` into execution IR. **The corrected trap:** *not*
"single-broadcast-or-terminal"
(both Gemini's and Claude's iter-1 floated it) — a star co-locates all dim builds in one consumer
task (`fragment_broadcast_join_plan`), so >1 broadcast → terminal where 139 *succeeds*; the policy
is **demote the set**, never worse than 139. **Honest scope:** A2 demotes the failed task's whole
replayable set — a single-task star still demotes all its dims (= 139 for the pure star), because
the worker's coarse failure carries no per-build id; per-dim-in-star precision is the **A1
follow-on** (a worker failure-hint + per-build cap validation — a wire/payload change the operator
scoped out: "small, Toron-local"). Every 139 fence preserved (one-shot, `!batch_sent`, fresh
non-`AttemptId` namespace, `BroadcastReplication`-only, mutation paths out, coordinator-owned, 138
split untouched). Consensus: A2 (Claude + Gemini + operator's "small, Toron-local" steer); Codex's
stronger join-*identified* A1 surfaced transparently as the follow-on, not folded. Headline gate:
`mixed_sql_and_rune_broadcast_plan_replays_sql_preserves_rune` (fails on `main`).

**ADR 141 lead candidates:** **stats-backed optimizer facts (operator-confirmed next direction,
selectivity-first).** Wendelon is *not* absolute-facts-only — it has **KLL approximate quantiles**
(a query aggregate) and **maintained per-column row-group stats (HLL cardinality, bloom,
small-set)**. A 136-W-style cross-ring pair exposes these as **labeled-approximate** optimizer
facts Toron consumes in the estimate/selectivity channels — *never* as hard `byte_upper_bound`
receipts (channel honesty: approximate ≠ hard). **Lead with selectivity** — HLL cardinality is
already maintained per-column, so equality/filter/join selectivity is the best-grounded first
stats ADR (the real CBO substrate Gemini has wanted since 137, → eventual join reordering);
per-column byte/length quantiles are a *second* cut (a new producer extension — not maintained
today). This attacks the *root* of the 138/139/140 arc (bad estimates → over-caps) rather than
the recovery symptom. Also queued: **ADR 140's A1** (exact join-identified replay via a worker
failure-hint — `ShuffleConsumer { broadcast_join_id }` + per-build cap validation, for single-task
star per-dim precision); generic `ShuffleBytes` fan-out recovery; `Take`/`LIMIT` single-base
lineage. Evidence-gate the next pick on the 140 feat.

### ADR 139 promoted; ADR 140 candidates (HISTORICAL)

ADR 139 promoted **Source-Aware Broadcast-Cap Hash Replay** — the keystone that retires ADR
138's named availability residual (an estimated broadcast whose *actual* encoded bytes exceed
the runtime cap fails the query where hash would have completed). The runtime caps a broadcast
build against actual encoded Arrow IPC payload bytes (`PartitionPayload::size_bytes`), failing
with a typed `BudgetExhausted(BroadcastReplication)` — never wrong rows — so an over-cap
broadcast is a **resource-admission miss, not a query meaning error**. 139 converts it into a
**one-shot, coordinator-owned, whole-query hash replay**: the coordinator already runs a
`!batch_sent`-gated whole-query retry for descriptor evolution (`coordinator.rs:836`), so the
replay is the same loop with a force-hash policy on the second attempt — no in-flight subtree
re-fragmentation (broadcast and hash are disjoint fragment topologies). **Who degrades depends
on how the broadcast was authored** (the operator's Rune/SQL steer): `JoinStrategyIntent::Pinned`
gains a `source: {Direct, SqlHint, Rune}` provenance (it previously erased authoring). **Auto**
(138 estimate / 136 hard) and **SQL `/*+ BROADCAST */`** broadcasts **replay → hash** (protect
the naive user); **Rune `broadcast_join`** and **direct/programmatic** pins **stay terminal**
(absolute control / explicit command — fail-loud). This synthesizes the fail-loud-for-authored-intent
discipline (kept on Rune + Direct, the prescriptive/programmatic surfaces) with the
pragmatic degrade (SQL, the compatibility surface) — [[rune_is_primary_interface]]. A plan with
*any* terminal pinned broadcast does not replay (keeps the replay attempt provably
broadcast-free; mixed-source per-join replay is the follow-on). Trigger is **`BroadcastReplication`
only** — `ShuffleBytes` is excluded because `shuffle_bytes_published_per_task` is a per-task
*total* and a single-producer broadcast build's hash version publishes the same total against the
same budget (hash replay can't relieve it; replication is what hash eliminates). Replay preserves
the external `QueryId` under a fresh stage/exchange id namespace (not `AttemptId` — the scheduler
rewrites attempt ids) so attempt-0 published partitions can't collide; one-shot; reads only
(`!batch_sent`-gated; mutation paths out); demoted SQL broadcasts install hash exchanges; the
estimate-driven case returns the **forced-hash digest** (byte-identical — the V1 bar).
Consensus: one-shot replay mechanism 3-of-3; source-aware D4 policy operator + Codex + Claude
(after three grounded concessions — iter-0 tractability, iter-4 Direct-pins + ShuffleBytes).
Headline gate: `estimated_broadcast_over_cap_replays_as_hash_and_matches_digest` (Rune+SQL).

**ADR 140 lead candidates:** **targeted mixed-source per-join replay** (join identity
bind→fragment→failure — closes 139's mixed-plan SQL-protection gap and removes the
whole-query-replay bluntness); **length-carrying logical-type refinement** (promotes
`Utf8`/`Binary` from a 138 *estimate* to a *hard* `byte_upper_bound` — shrinks the estimate
surface; likely a Toron+Wendelon pair); **`Take`/`LIMIT` single-base lineage** (129-retarget so
row-capped dims broadcast); generic `ShuffleBytes` recovery via multi-producer fan-out; the
**136-W** Wendelon hard-facts producer (Wendelon-home); remote-live multi-producer fan-in;
multi-column / histogram selectivity; join reordering. Evidence-gate the next pick on the 139 feat.

### ADR 138 promoted; ADR 139 candidates (HISTORICAL)

ADR 138 promoted **Estimate-Tolerant Per-Row Materialized-Size Model** — the cut where the
council, on an operator steer, traded the spine's "hard upper bound only" purism for an
honest estimate. 137 left a row-bounded relation with `row_upper_bound` set but
`byte_upper_bound` untouched, so a *string-bearing tiny dimension* still hashed: the chooser
is byte-only and nothing converts rows to bytes, and there is no hard length signal for
`Utf8`/`Binary` (refinements are value-only; the descriptor carries no per-column width). The
grounded reframe: the runtime caps a broadcast build against **actual encoded Arrow IPC
payload bytes** (`PartitionPayload::size_bytes`), failing with a typed `BudgetExhausted` —
never wrong rows — and already tolerates an imprecise planner (`cap/4` = 16 MiB ceiling vs a
1 MiB default threshold vs 64 MiB runtime cap → ~64× headroom *by design*). So undercount is
an **availability** concern, not a correctness one, and a conservative over-count estimate
beats refusing to broadcast every string dimension forever. **D1:** a new
`estimated_byte_size` channel beside the **untouched** hard `byte_upper_bound`; **138 never
writes the hard field** — Toron computes *estimates*, only the storage ring (136/136-W) issues
hard byte facts (the cross-ring [[logical_identity_receipt_doctrine]] cut). Fixed-width is an
exact-*ish* estimate, string/binary a looser one — **both** in `estimated_byte_size`, after
Codex grounded that Toron cannot prove the encoded-IPC byte unit the runtime actually meters.
**D2:** over-count biased, validated `estimated_byte_size ≥ encode_record_batch(..).len()` at
`R ∈ {1,8,1000}` (the real unit, framing included); any nested/dict column ⇒ estimate `None`.
**D3:** chooser prefers hard (`broadcast_inferred`), falls back to estimate
(`broadcast_estimated`) only when hard is absent — and reads byte-typed fields only, so 137's
chooser-byte-only guard stays green. **D4:** distinct `BroadcastEstimated` receipt → EXPLAIN
labels estimated vs hard (134). **D5:** binder-local; derive at `from_table` (descriptor rows)
+ post-137-PK-cap, carried through 137's row-local helpers; default threshold **unchanged**.
**`Take` dropped (grounded):** `take` returns `Composite`, and the chooser only reads build
bytes for `SingleBaseRelation`, so `LIMIT n` cannot broadcast without a 129-retargeting
lineage decision — a named follow-on, not an accidental side effect (corrected an earlier
"fold 116" framing). **Named residual:** until ADR 139, a gross (>headroom) under-estimate of a
genuinely-large input fails the query where hash would have succeeded (availability, not wrong
rows). Genuine 3-of-3 + operator on B′ after two iter-3 promotion blockers were ground shut
(Take lineage; encoded-IPC byte unit → fixed-width demoted to the estimate channel). Headline
gate (Rune+SQL): `string_bearing_tiny_dim_infers_broadcast_by_estimate` from a hard row source
(fails on `main`); seven `adr138_*` guards + 129–137 regressions incl. the two 137 honesty
guards un-retargeted.

**ADR 139 lead candidates:** **runtime broadcast-build → hash fallback** (the keystone the
138 reframe named — catch the build-side `BudgetExhausted` and degrade *that join* to hash
instead of failing the query; retires 138's availability residual *and* independently hardens
every existing hard-bound broadcast against a mis-set cap; distributed-runtime ADR —
fragmenter/server/scheduler) — strong continuity, my lean; a **length-carrying logical-type
refinement** (would promote `Utf8`/`Binary` width from estimate to a *hard* bound); **`Take`/
`LIMIT` single-base lineage** (129-retarget so limited dims broadcast); the **136-W** Wendelon
hard-facts producer (Wendelon-home); remote-live multi-producer fan-in; multi-column /
histogram selectivity; join reordering. Evidence-gate the next pick on the 138 feat.

### ADR 137 promoted; ADR 138 candidates (HISTORICAL)

ADR 137 promoted **Fact-Spine Preservation Through Row-Local Volume-Non-Increasing
Nodes** — the natural IQ step now that 136 made base facts real in production. 136 let
`byte_upper_bound`/`row_upper_bound` flow, but the binder threw them away the moment a
relation was wrapped: `Filter` (`binder.rs:380`) and `Project` (`:396`) each replaced
the child facts with `composite_with_row(...)` (`PlanOrigin::Composite`, byte `None`),
so the join chooser's single-base + byte-bound requirement (`:3950/3953`, `:3889/3897`)
rejected the build → every build-side `Filter`/`Project` shape silently demoted a tiny
dimension to hash. Yet a filter only *removes* rows and a column-subset project only
*drops* columns — neither grows the relation, so the hard upper bounds stay valid and
the origin stays single-base. **D1:** `Filter` preserves the full single-base spine
(origin+byte+row).
**D2:** `Project` preserves byte facts **only** for a pure distinct-column
subset/permutation; an expression/cast/literal/dup project keeps row + single-base
lineage but **drops byte** (it may grow bytes); aggregating/expanding stays `Composite`.
**D3:** single-column PK equality (`primary_key_columns.len()==1`, that column, non-null
literal) caps `row_upper_bound=1` but **never shrinks byte** — the chooser is byte-only,
so a PK-filtered large table **caps rows yet still hashes**; gated honestly as *caps
rows, still hashes* (the [[feedback_cut_must_deliver_the_name]] discipline — no name the
mechanism can't deliver). **D4:** one guarded place — `preserve_volume_non_increasing` /
`preserve_through_project` / `with_pk_row_cap` on `PlanOptimizerFacts`, so the invariant
can't silently rot. **D5:** binder-local — no new fact unit, width model, or
substrate/wire/fragmenter/scheduler change; EXPLAIN (134) is the validation surface.
**`Sort` considered and dropped (grounded):** it is the third `composite_with_row` site
(`:479`) and volume-non-increasing, so folding it looked free — but every surface
`ORDER BY` lowers as `Sort { input: Exchange{partition_count:1} }`
(`apply_order_by_limit_tail`), and the one-partition `Exchange` already resets origin to
`Composite` *below* the Sort, so Sort preservation is **inert** for SQL/Rune; revisit
only if a direct-IR un-exchanged `Sort` becomes reachable (Codex's caught caveat,
confirmed against `sql_planner.rs`). Genuine 3-of-3 on B′ (Gemini withdrew the
overclaimed `pk_filtered_large_table_infers_broadcast` gate, conceding the byte-only
grounding). Headline binder gate: `filtered_tiny_dim_infers_broadcast` (fails on
`main`); five `adr137_*` guards + 129–136 regressions.

**ADR 138 lead candidates:** **hard per-row materialized-width model** (the direct
138 follow-on — converts a row cap into a `byte_upper_bound` so a PK-filtered/known-small
row from a large table actually broadcasts; bounded-width schemas: fixed Arrow types
always, `Utf8`/`Binary` via a refinement max-length, else keep input byte bound) — the
natural completion of 137's honestly-deferred half; the **136-W** Wendelon hard-facts
producer (the other half of the 136 pair, Wendelon-home); remote-live multi-producer
fan-in (complete 135's live path for pinned-Rune broadcast); richer cost units;
multi-column / range / histogram selectivity; join reordering. Evidence-gate the next
pick on the 137 feat.

### ADR 136 promoted; ADR 137 candidates (HISTORICAL)

ADR 136 promoted **Wendelon-Backed Hard Table-Size Facts (Toron consumer + default
flip)** — the "data data data" cut that makes the 129–135 CBO spine fire on real
production tables instead of test-only `seed_optimizer_facts`. Today
`from_descriptor_with_logical_types` ends at `optimizer_facts:
TableOptimizerFacts::default()` (`catalog.rs:204`) → every Wendelon-built catalog is
blind → hash. 136 lets hard table-size facts ride a **descriptor-adjacent sidecar**
(keyed by `(tenant, table_object_id, schema_generation, physical_fingerprint,
manifest_epoch)`; `TableSchemaDescriptor` untouched) into `TableOptimizerFacts` via
facts-aware constructor/registry/bootstrap/sync paths; descriptor-only paths keep
defaulting unknown. **Cross-ring pair:** this council promotes **136-T** (the Toron
consumer + flip); the **136-W** producer (table-identity-keyed hard-facts ledger) is
named as a precise contract and handed to Wendelon's sequence — Wendelon owns its
producer; Toron holds no stats cache. Hard bounds only: unknown/absent/mismatched →
hash; producer-stale facts arrive as absent/unknown. Overcount suppresses broadcast
(safe); **undercount is a correctness bug**. Two load-bearing safety calls: the byte
fact is a
**materialized** build-footprint upper bound (named `materialized_byte_upper_bound`),
never compressed storage `size_bytes` (which would undercount decompressed Arrow
size → wrong broadcast) — Claude's correction; and descriptor refresh must update
`optimizer_facts` **even when descriptor identity is unchanged** (facts track
`manifest_epoch`, a tiny dim grows without a schema change) — Codex's correction.
`PlannerConfig::default()` flips to `Some(1 MiB)` (clamped by the 16 MiB cap
ceiling) — **inert until 136-W ships** (no facts → do-no-harm hash), so 136-T lands
first and is fixture-testable now. Known-empty normalizes to both `row_upper_bound`
0 and materialized bytes 0, so it broadcasts below cap while staying distinct from
unknown (folds ADR 095). EXPLAIN (134) is the validation surface.
Quorum: operator + Claude + Codex; Gemini held selectivity (→ 137 lead). Headline
gate: `star_query_infers_broadcast_without_seed_optimizer_facts` (SQL + Rune).

**ADR 137 lead candidates:** **fact-spine selectivity propagation through filters**
(Gemini — row-local nodes preserve upper bounds; PK-equality `col = literal` →
`row_upper_bound` 1, but broadcast still needs a separately proved hard byte cap
under the current byte-first chooser; `fact ⋈ (tiny_dim WHERE …)` infers broadcast)
— the natural IQ step now that base facts flow; the **136-W** Wendelon producer ADR
(the other half of the pair); remote-live
multi-producer fan-in (complete 135's live path for pinned-Rune broadcast); richer
cost units; join reordering. Evidence-gate the next pick on the 136 feat.

### ADR 129 promoted (HISTORICAL)

ADR 129 promoted **Optimizer Fact Spine for Tiny Broadcast Inference** — the
roadmap pivot's first step toward a cost-based optimizer. Builds a conservative
**fact spine** (hard `byte`/`row` upper bounds on `TableBinding`, propagated to
bound subplans) and spends it on one fenced rule: unhinted INNER joins whose
**syntactic-left** input is a single base relation with a hard byte bound below an
effective dark-launched threshold infer `JoinStrategy::Broadcast`; everything
else stays hash. Seam is **binding-time** (grounded: `SqlPlanner` is a
catalog-free unit struct; the binder owns catalog + `PlannerConfig`) via a single
`choose_join_strategy(...) -> {strategy, reason}` helper (the spine-honesty gate —
a 2nd CBO rule is a new arm, not a new branch). `LogicalPlan::Join` gains
`JoinStrategyIntent::{Auto, Pinned}`; `ExecutionPlan::Join` stays concrete; `Auto`
never survives binding. Fork A exchange placement (pinned stays planner-shaped,
auto shaped in binder) as **named temporary debt**. Row facts from `Take`/`LIMIT
0`/`EmptyRelation` folded into the spine but **carried, not scheduled on** (no
fragmenter/dispatch change). Config dark-launched `None`; the effective threshold
is capped by a static conservative broadcast-cap ceiling until actual
`QueryBudget` reaches binding, so production inference stays inert until a real
Wendelon-facts producer ADR plus knob enablement. Genuine 3-of-3 after three
grounding corrections (planner→binder placement; hard-bounds over `confidence`;
no-hash-hint precedence gate — ADR 128 only added `BROADCAST`). Retargets
122/125/128 join guards; do-no-harm inference, fail-loud hints unchanged.

**ADR 130 lead candidates** (the CBO arc now has a spine to extend): right-side
build selection + output-order preservation (the real `fact JOIN dim`
star-schema win — dim on the right); Wendelon-backed live table-size facts
(storage-ring producer ADR — flips 129's config to default-on); unify exchange
insertion (pays the Fork-A debt once a pinned-reshaping rule earns it);
selectivity propagation; fold ADR 116 take / ADR 095 empty-relation into the
general fact model; zero-worker / coordinator-local placement; optimizer
`EXPLAIN` over the reason receipts. Evidence-gate the pick on the 129 feat.

### ADR 128 promoted (HISTORICAL)

ADR 128 promoted **SQL broadcast join hint authoring** — closes the broadcast
authoring story (125 did Rune; this does SQL). `SELECT /*+ BROADCAST(alias) */
… INNER JOIN …` lowers to the existing `JoinStrategy::Broadcast` via a strict
raw-text recognizer over `OptimizerHint.text`; build-side resolution through
121's `plan_qualifiers`; no-silent-fallback reject of malformed / right-side /
non-INNER / ambiguous / composite-left / unused hints; EXISTS stays hash. **No
dialect/parser/substrate change** — `GenericDialect` already captures comment
hints (`generic.rs:288`); the cut replaces `lower_select`'s blanket hint reject
(`:2560`) only. Behavioral 3-of-3 with the promoted mechanics incorporating
three sqlparser-layer grounding corrections (wrong crate version 0.59→0.62;
hints are raw text not parsed `Expr`; `GenericDialect` already captures →
Claude's `ToronDialect` was unnecessary and conceded iter-2). Retargets
`adr125_no_sql_hint_authoring_yet` →
`adr128_sql_broadcast_hint_authoring_is_narrow`; adds a
`generic_dialect_remains` guard memorializing the no-dialect-change correction.

**ADR 129 lead candidates** (no strong continuity pull — broadcast surface now
whole, live-shuffle memory chain has one remaining link): chunk-granular sealed
manifest (the last live-shuffle memory link — true 1-copy, consumer-`pull`-API
break); executor build/probe reorder → streamable joins (the big distributed
arc); non-INNER broadcast widening (per-kind binder/fragmenter proofs);
frontend (qualified `t.*`, joins over derived tables). Evidence-gate the pick
on whatever the 128 feat surfaces.

### ADR 127 promoted (HISTORICAL)

ADR 127 promoted **Live-chunk release after seal** — the second link of the
live-shuffle memory chain, collapsing the 2-copy steady state ADR 126 left.
Grounded 3-of-3 (Claude final pass, Codex iter-1, Gemini iter-1) on: a
lock-coupled `active_readers` count on `LivePartition` (no count for a
fallback-only `pull_live`); a stream-owned reader guard with dual duty (decrement
once on both stream-end paths **and** preserve 124's unsealed-abnormal-drop
cancellation); idempotent `release_live_chunks_if_idle` (drop `live.chunks` +
the 124 append-side `live.reservations`, never the 126 sealed reservation)
triggered at seal-if-idle and last-reader-end; and the **correctness spine** —
`pull_live` falls back to the sealed payload from `inner.partitions` when
`sealed && chunks` empty (bypassing pacing, yielding once for index 0, erroring
on missing-sealed rather than silent EOF). This also closes a latent
silent-empty-read bug at `partition_store.rs:733`. **Topic was an operator
steer to B over the peers' iter-0 lean to C (SQL broadcast hints)** —
conditioned on the fallback, exactly the B-blocker Codex named; surfaced not
papered. Named for the common-case reclaim (one resident copy after
release/drain), NOT constant memory (worst case — reader mid-stream at seal —
is inherent; literal-constant is the deferred chunk-granular manifest). No
scheduler/consumer/wire change. 15th grounding-correction arc note: the final
promotion corrected the shorthand "3-of-3" wording against the actual peer
files — convergence was substantive, not a same-number iteration claim.

### ADR 126 promoted (HISTORICAL)

ADR 126 promoted **Live-shuffle seal correction** — the first link of ADR
125's deferred "live shuffle memory chain." Genuine 3-of-3 (Fork A) after
Codex's iter-0 *proved* (throwaway Arrow IPC test) a latent multi-chunk
data-loss bug: `seal_live`'s chunk fallback byte-concatenated N framed IPC
streams, and the single `StreamReader` stops at the first EOS → only the
first chunk's rows survive (invisible because the one covering test used
synthetic non-IPC bytes). The cut makes `live.chunks` the **single source**
seal derives from via streaming Arrow IPC reassembly (decode→write→drop per
chunk); drops `partition_batches` decoded retention + the live producer's
redundant `publish_task_output` + `TaskBatch` transmission (general publish
path preserved for non-live producers); carries `schema: Option<SchemaRef>`
on `LivePartition` so empty partitions seal as schema-bearing IPC; reserves
the sealed payload atomically (no partial publish on budget exhaustion);
manifest-derived telemetry; **retargets the one 124 guard**
(`adr124_seal_live_materializes_from_chunks` → reassembly invariant). Named
"correction," NOT "single-pass/constant-memory" — encode count stays 2,
resident copies stay 2 (chunks stay resident for in-flight `pull_live` +
retry-from-zero). 14th grounding-correction in the 119→126 arc (Claude's
"3→1 copy" headline + "no 124 guard inverts" both corrected; Gemini conceded
Fork B→A on grounded schema-availability). Substrate behavior change; no new
IR/opcode/wire type.

**Live shuffle memory chain — remaining link** (post-127):
chunk-granular sealed manifest (true constant-memory / single-copy:
multi-entry-per-partition manifest OR streaming sealed assembly — the only
path to "1 copy" without retaining one sealed partition payload in the store).

**Distributed execution arc:** executor build/probe reorder (pairs with
124 for end-to-end natural backpressure); output-side
joins-as-streamable-producers (widen `streamable_unary_plan` for `Join` +
restructure executor); multi-producer fan-in + remote live transport
(Codex's three-substrate framing); configurable `LIVE_BACKLOG_DEPTH` via
`QueryBudget`.

**Broadcast continuation:** non-INNER broadcast widening (binder + fragmenter
per-kind correctness proofs); cost-based strategy selection (needs cardinality
estimates not in IR today).

**Identity tier:** logical-type lifecycle (`DROP`/`ALTER LOGICAL TYPE` —
cross-ring DROP + referential integrity, bounded 090 re-open);
declared-additive types (`SUM(Money) → Money` via logical-type property);
PK/FK-derived identity (blocked on FK support at `sql_planner.rs:663`).

**Frontend:** qualified wildcards (`t.*` + mixed — planner↔binder
restructuring or `Wildcard` IR variant); qualified DML RETURNING; DML
target aliases / qualified assignment targets; SQL joins around derived
tables / GROUP BY over joins; Rune sort/limit; SIMILAR TO / RLIKE / REGEXP.

**Distributed (other):** streamable Sort/Aggregate coverage (120 V2 —
top-k bounded buffers + partial+merge); row-group-lazy-from-disk scan
(118 follow-on — per disk page laziness).

**Optimizer / catalog:** general project-fusion beyond 105's
adjacent-identity case (evidence-gated); further `information_schema`
relations (constraints / indexes / PK-FK / views); Wendelon `*V1`/`*v1`
de-version sweep (Wendelon-home chore).

**Long-horizon:** subquery/`UNNEST`/row-value `IN`; `CHECK`/`DEFAULT`;
`ALTER`/composite-PK/`UNIQUE`; pre-op intent audit; fragmentable bulk DML;
JWT/OIDC/SPIFFE; gRPC/Arrow Flight/Postgres frontends.
