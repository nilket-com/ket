# ADR 83 - Quantile sketch substrate correctness

**Status:** Partially implemented 2026-08-11. The aggregate quantile core is
implemented and in use by Toron ADR 546, which is no longer blocked. The CSB
storage-migration half — v2 payload, mixed-fleet fence, rebuild driver, and v2's
own confidence scope — remains open, so this record is not sealed.

## Evidence that forced the decision

`KllSketch` does not compute quantiles. It advertises a 0.5% rank bound and
returns answers wrong by up to 55 rank points on ordered input.

Measured against the real type, k=200, n=1000, values 0..999 inserted as
canonical big-endian `Int32`, compaction seed **42** (the seed Toron's legacy
`ApproxQuantile` arm passes at `exec.rs:668`):

| arrival order | retained | p50 (true 500) | p25 (true 250) | p75 (true 750) |
| --- | --- | --- | --- | --- |
| ascending | 197 | **899** | **800** | 950 |
| descending | 197 | **100** | | |
| shuffled | 197 | 522 | | |
| merge of two ascending halves | 199 | 499 | | |

The live CSB path does not use seed 42. It uses
`COLUMN_STATS_KLL_COMPACTION_SEED = 0x5A17_1590` (`storage/stats.rs:50`). The
same corpus replayed through the current compactor with the CSB seed returns
ascending p50 = 899 and descending p50 = 100 unchanged, so the finding is a
property of the compactor rather than of one seed choice.

`rank_error_ppm()` reported 5,000 (0.5%) in every row above.

The ascending/descending pair is the diagnosis. The same multiset returns 899 or
100 depending only on arrival order. Shuffled returns 522 and merge-of-halves
returns 499, which is why this survived: on unordered data it looks correct, and
merging partially cancels the bias.

Merge direction is independently answer-changing today. A Stage 0 probe split
the population `0..1233` unevenly: sketch `a` received ascending `0..900`, sketch
`b` received descending `901..1233`, and both began with seed 42. Reversing only
the receiver produced:

| merge | retained | reported p50 | serialized state |
| --- | ---: | ---: | --- |
| `a.merge(b)` | 50 | **894** | differs |
| `b.merge(a)` | 50 | **893** | differs |

The exact median is approximately 617. The one-rank difference is the
load-bearing observation: merge direction changes the answer, not merely an
irrelevant byte representation. The mechanism is that `merge` keeps the
receiver's evolved `compaction_seed`, while unequal fragment histories have
advanced that seed differently. Every production caller of `merge` therefore
inherits receiver-order dependence, including aggregate-partial and CSB block
merges.

The mechanism is a missing weight. `KllSketch` holds one flat
`values: Vec<Vec<u8>>`. `compact_if_needed` sorts, keeps alternating indices, and
halves again until the buffer fits `k`, but survivors carry no weight and
`total_item_count` is tracked separately. Items compacted early were halved more
often than late arrivals, so each retained item stands for a different and
unrecorded number of stream items. `kll_rank_to_retained_index` then maps rank to
position assuming a uniform equal-weight subsample. Under ordered arrival that
assumption is false in a systematically biased direction.

`rank_error_ppm()` is `1_000_000 / k`: a function of the parameter alone,
reflecting no property of the retained sample and derived from no theorem.

Neither persisted representation carries weights today:

- `AggregateState::Kll` has `levels: Vec<Vec<Vec<u8>>>` and the encoder walks
  arbitrarily many levels — but `KllSketch::into_state` always writes
  `levels: vec![self.values]`, and `merge_kll_states`
  (`storage/aggregate.rs:650`) does `levels.into_iter().flatten().collect()`.
  The level capability is unreachable in both directions.
- `ColumnStatsKllPayload` (`storage/stats.rs:164`) is flat `retained_values`
  with no level structure at all. This is what the live CSB path persists.

The CSB path reaches query planning: persisted payload → `stats_facts.rs` →
`ColumnRangeDistributionFact` → Toron `catalog.rs` → binder
`column_range_distributions` → `range_distribution_integral_estimate` and
`range_distribution_real_estimate`. Those are selectivity estimates, and
selectivity drives join ordering. Per-block sketches are built in storage order,
so any column whose blocks are populated in sorted order reproduces the
ascending row above. Whether production datasets are ordered in practice is a
Stage 0 measurement, not an assumption of this record.

## Decisions

### 1. Weighted levels are the representation

The sketch holds levels, not one buffer. A compaction at level *h* consumes
items of weight 2^h and produces items of weight 2^(h+1) at level *h+1*. Rank
queries accumulate weights rather than mapping position by ratio. Every retained
item's contribution to the stream is recorded rather than inferred.

### 2. The algorithm is the fully mergeable KLL variant

CSB facts merge across blocks, so mergeability is a hard eligibility filter, not
a property to optimise afterwards. The KLL paper (Karnin, Lang, Liberty,
`arxiv.org/abs/1603.05346`) offers two constructions and they cannot both be
had:

- Special handling of the top log log(1/δ) compactors gives
  O((1/ε)·log²log(1/δ)) single-quantile space, and *"This sketch is fully
  mergeable."*
- Replacing those top compactors with a GK sketch reduces space to
  O((1/ε)·log log(1/δ)), but *"this prevents the sketch from being mergeable
  because the GK sketch is not known to have this property."*

**This record selects the fully mergeable construction.** The previous draft
quoted the O((1/ε)·log log(1/δ)) bound, which is the non-mergeable one; that
selection was incoherent with CSB's merge requirement and is withdrawn.

The governing guarantee is the **all-quantiles** bound, since the substrate
answers arbitrary quantiles rather than one fixed rank. From the paper's
Table 1, the mergeable KLL row is O((1/ε)·log²log(1/δε)) for all quantiles,
against O((1/ε)·log²log(1/δ)) for a single quantile.

The two failure probabilities are named separately, because conflating them
constrains the configuration wrongly:

```text
δ_all     the advertised failure probability for the whole sketch —
          the number an operator reads
δ_single  the per-query failure probability the construction is sized for,
          with  δ_single ≤ ε · δ_all
```

The union bound runs over the O(1/ε) distinct quantiles, which is where the
ε factor enters; substituting δ_single into the single-quantile bound yields the
O((1/ε)·log²log(1/(ε·δ_all))) all-quantiles term. The paper's *"Setting
δ = Ω(ε)"* remark is the constant-success special case of this relation, not a
constraint on every shipped configuration. An earlier draft of this record read
it as the latter, which would have forbidden exactly the high-confidence
configurations where δ_all ≪ ε — the ones an operator is most likely to want.
Configurations are sized from the relation above, not from that special case.

The paper's merge procedure is adopted as specified: same-height compactors are
concatenated, every level then holding more than k_h items is compacted, and k_h
is recomputed from the new maximal height H derived from the combined stream
lengths. The sampler merges by having the lower-height sampler feed its item at
its weight into the higher-height sampler, with all compactors at height ≤ h in
the lower sketch feeding into it as well.

**Equal sampler heights need a tie-break, and it is pinned here.** The paper's
rule distinguishes the two sketches by sampler height, which says nothing when
the heights match — and at equal height the two sketches may retain different
sampler items, so an unbroken tie makes `a.merge(b)` and `b.merge(a)` retain
different items while agreeing on seed, identity and counters. Byte-identity
would fail on sampler state alone. When sampler heights are equal, the two input
sketches are ordered by the lexicographic key `(identity, seed)` over their
fixed 32-byte values; the smaller is the donor and the larger the receiver, and
the sampler merge then proceeds as above. The key is complete because identity
is unique per sketch and seed breaks any residual tie, so donor and receiver are
determined before any state is touched and do not depend on call order.

The selection is made here, not deferred. ADRs 546 and 547 depend on it and
cannot be written against an open question; a record that both selects and keeps
selecting licenses implementation before the choice exists. MRL — deterministic
*and* mergeable at O((1/ε)·log²(εn)) — is therefore **rejected** below rather
than held open. If Stage 0 finds the randomized confidence model unacceptable,
the answer is to redraft this record around MRL and revise 546 and 547 with it,
not to leave both algorithms live and let whichever is implemented first win.

### 3. The aggregate bound uses an explicit conservative derivation

`rank_error_ppm() = 1_000_000 / k` is removed. The fully mergeable KLL result in
Theorem 4 states an asymptotic bound with an unspecified constant `c₀`; quoting
that theorem cannot produce a defensible shipped parts-per-million value. This
record instead derives a larger executable bound from Theorem 3's explicit
inequalities.

For the aggregate surface, pin:

```text
c                         = 2/3
top equal-capacity levels = s = 4
k                         = next_even(ceil(8 / ε))
δ_execution               = 2^-10
maximum sketch states     = 2^10 = 1,024 per query execution
δ_all per sketch          = δ_execution / 1,024 = 2^-20
δ_single                  ≤ ε · δ_all
```

Toron ADR 546 enforces the 1,024-state ceiling independently of bytes and
divides it across terminal tasks so their allowances sum to at most 1,024. A
per-task ceiling would multiply with worker count and invalidate the execution-
wide confidence claim.

The derivation allocates half the rank budget to the deterministic top levels
and half to the sampler and lower compactors. With the lower-system error
parameter set to `ε/4`, Theorem 3 gives the conservative single-rank failure
bound

```text
δ_single,actual ≤ 4 · exp(-(ε² · k² · 4^s) / 768).
```

The top levels contribute at most `s·n/k ≤ εn/2`. For `s=4` and
`k≥8/ε`, the displayed probability is at most `4·exp(-64/3)`, below
`2.2·10^-9`. Across the candidate ε range `[0.005, 0.02]`, that is no larger
than `ε·2^-20`; the all-quantiles union bound is therefore within `2^-20` per
sketch, and the declared 1,024-state admission keeps the whole execution within
`2^-10`. Choosing ε outside that range requires re-running the derivation rather
than extrapolating it.

The retained-item bound is `7k+1`: at most `4k` across the top equal-capacity
levels, at most `3k` across the geometrically decreasing lower levels for
`c=2/3`, and one sampler item. **This record selects ε=1%, hence `k=800` and
at most 5,601 retained items per sketch.** The rejected alternatives and their
price are retained for comparison:

| ε | k | retained-item bound |
| ---: | ---: | ---: |
| 0.5% | 1,600 | 11,201 |
| 1% | 800 | 5,601 |
| 2% | 400 | 2,801 |

At eight bytes per canonical value, the selected retained payload is at most
44,808 bytes (43.8 KiB) per sketch and 43.8 MiB across the 1,024-state execution
cap. That is about 68% of Toron's default 64 MiB task budget before level,
sampler, hash-table, key and batch overhead, leaving roughly 20 MiB of payload
headroom. Four-byte Int32 and Float32 values use 22,404 bytes (21.9 KiB) per
sketch and 21.9 MiB at the cap. Accounting is therefore width-aware rather than
charging every accepted type as eight bytes.

The 1,024 count ceiling and 64 MiB default are coupled operationally even though
they are enforced independently: increasing the count ceiling requires
re-deriving the execution confidence and demonstrating that the default budget
still admits the intended workload. Byte headroom never authorizes state 1,025,
and count headroom never authorizes a state whose bytes do not fit.

ε=0.5% is rejected for this cut because its 87.5 MiB retained payload cannot fit
the default task budget even before non-sketch state. ε=2% fits cheaply but gives
quartile edges ±2 rank points on the 25-point q1-to-median separation, visible
as box-edge jitter in ADR 547's motivating use case. The selected 1% is the
middle contract, not an inherited default.

No lone rank-error number ships. Aggregate execution carries ε together with
the execution-wide confidence and state-cap contract. CSB v2 must pin its own
confidence scope before it restores range-distribution facts; it cannot inherit
an execution-scoped δ. Until v2, no CSB rank-error value is reported at all.

### 4. Randomness has two surfaces and both are specified

The theorem's guarantee holds over compaction randomness drawn **independently
of the data**. A global constant seed (42, or `0x5A17_1590`) is fixed and
knowable before the data exists, so the theorem does not cover structured input
— which is the input that failed above. Persisting a seed fixes this only where
a sketch is durable. It is not sufficient on its own: if the same PRNG stream is
reused, or if two sketches that will later be merged draw correlated streams,
the independence the analysis needs is lost even though every seed was drawn
before its data.

Two surfaces therefore need separate rules.

**Durable CSB sketches.** A seed is drawn at creation from the platform CSPRNG
(`aws_lc_rs::rand::fill`, already used in `crypto.rs`) and persisted with the
sketch. Replay from persisted state is byte-identical.

**Per-execution aggregate sketches.** `APPROX_QUANTILE` sketches are constructed
fresh on every execution (`exec.rs:668`) and never recovered from durable state,
so persistence does not reach them. This record decides: **one seed is issued by
the coordinator per query execution and propagated to every fragment and partial
sketch in that execution.** Consequences, stated rather than left implicit:

- results are stable within one execution, including across its fragments;
- results may differ between executions of the same query, within the rank
  bound. This is the honest contract under a randomized algorithm and replaces
  ADR 546's fixed-seed byte-identical promise;
- the seed is data-independent because it is drawn before the query reads
  anything.

The alternative — a stable secret-derived seed giving cross-execution
byte-identity — is available only with an explicit adversarial-input model
stating who can observe the secret and what happens when input correlates with
it. This record does not adopt it, and does not permit adopting it silently.

**The stream itself is pinned**, since a seed alone does not specify a PRNG.

The seed is 256-bit, replacing the `u32 compaction_seed` field, which is too
narrow to key a persistent random stream. `SketchIdentity` is likewise **fixed
32 bytes, assigned at creation and never absent** — not an optional field, a
variable-width name, or something derived lazily at first compaction. It is
drawn at creation alongside the seed for a fresh sketch, and produced by the
merge rule below for a merged one, so every sketch has one from the moment it
exists. Fixed width is what lets it be ordered and concatenated unambiguously in
the rules that follow. The PRF is `blake3::keyed_hash`, already a workspace
dependency in both crates. A compaction's coin is

```text
coin(level, index) = keyed_hash(
    key = seed,
    msg = DOMAIN_TAG_COMPACT             // 8-byte constant
       || len(sketch_identity) as u32 BE || sketch_identity
       || level as u32 BE
       || index as u64 BE
)                                        // low bit selects the retained parity
```

Every field is fixed-width or length-prefixed, so no two distinct inputs share
an encoding.

A coin depends on the seed, the identity, and the per-level compaction index, so
**all three** must merge deterministically or `a.merge(b)` and `b.merge(a)` will
still diverge. Merging sketches `a` and `b` produces:

```text
merged_seed     = keyed_hash(key = DOMAIN_KEY_MERGE,
                             msg = min(seed_a, seed_b) || max(seed_a, seed_b))

merged_identity = keyed_hash(key = DOMAIN_KEY_IDENTITY,
                             msg = min(id_a, id_b) || max(id_a, id_b))

counter[h]      = max(counter_a[h], counter_b[h])   for every level h
```

each ordering on the 32-byte values lexicographically, so every rule is
commutative in its pair by construction. That is what stops the result depending
on which operand happened to be the receiver. The counter reconciles by `max`
rather than resetting: the merged identity already opens a fresh coin domain,
and keeping each level's counter strictly increasing across a merge chain means
no coordinate inside one identity is ever drawn twice.

None of this is claimed associative across differing merge trees, consistent
with Acceptance 3 scoping reproducibility to an identical merge sequence.
Today's `merge` ignores `compaction_seed` entirely while `compact_if_needed`
rotates it, so none of these properties hold.

**Serialized state carries all of it** — seed, identity, per-level compaction
counters, the levels themselves, and sampler state (its retained item, that
item's weight, and its height). A sketch that round-trips without its counters
or sampler is not the same sketch, and Acceptance 4 round-trips every field.

`sketch_identity` is load-bearing rather than decorative: without it, seed plus
coordinates alone would give blocks *i* and *j* identical coins at the same level
and index, and two independently built sketches that later merge would compact
in lockstep.

**The security assumption is stated, because it is what bridges this
construction to the theorem.** The paper's analysis assumes compaction choices
are independent uniform random bits. A PRF supplies *computationally*
domain-separated pseudorandom streams, not information-theoretic independence.
This record therefore assumes `blake3::keyed_hash` is a secure PRF and that the
party choosing the input is computationally bounded and does not observe the
seed; under that assumption the sketch's error distribution is
indistinguishable from the analysed one. Where the seed is observable, or the
input may be chosen adversarially with knowledge of it, the theorem's guarantee
is not inherited. This is an assumption the record adopts, not a proof it
supplies.

### 5. The CSB payload is versioned, with a mixed-fleet rule

`COLUMN_STATS_BLOCK_FORMAT_VERSION` (`storage/stats.rs:38`) is a single global
version and readers reject every other value; `ColumnStatsKllPayload` has no
independent envelope. The migration is therefore a block-format decision:

- **CSB version 2 with a dual reader.** Version 1 blocks remain readable.
- **Valid version 1 statistics are preserved.** Min, max, null counts, bloom
  filters and HLL registers are unaffected by this defect and are not discarded.
  Only the legacy range-distribution payload is dropped.
- **A version 1 block yields no range-distribution fact.** It is not
  reinterpreted as equal-weight items. A flat payload records no weights, and
  reading one as equal-weight invents a measurement that was never taken and
  reproduces the bias this record removes.
- **Rebuilding is lazy, through the existing block rewrite path** where one
  exists; Stage 0 determines whether it does, and if not this record adds an
  explicit driver rather than leaving rebuild implicit.
- **Mixed-fleet rule.** A dual-reading new node does not help an old node, which
  rejects version 2 outright. Writing version 2 is therefore gated behind either
  a writer-admission fence that establishes every reader in the deployment
  understands version 2, or an offline upgrade. Which one is chosen is settled
  before implementation; what is forbidden is emitting version 2 into a fleet
  that has not been established as able to read it.

Absence of a fact is a supported state — the binder already handles a column
with no distribution. A wrong fact is not.

**Landing order is aggregate core first, CSB v2 second.** When the weighted
core lands, every version 1 range-distribution payload becomes unreadable as a
quantile fact and new version 1 blocks stop emitting that payload. Min, max,
null counts, bloom filters, HLL registers and every other valid version 1 fact
remain available. Until the v2 writer, fleet fence and rebuild driver land,
there are no range-distribution facts anywhere in the fleet — neither recovered
legacy facts nor newly written ones.

That interval is deliberate safe degradation, not a migration accident. It can
change join choices because the binder falls back from distribution-aware
selectivity, and timing baselines taken before, during and after it are not
directly comparable. Stage 0 records representative plan shapes and timings
with the fact class present versus absent before the core is enabled.

### 6. Legacy aggregate state is validated, not assumed

A weighted reader of `AggregateState::Kll` must verify
`Σ(level_len × 2^level) == total_item_count`.

- **Uncompacted legacy state** — one level whose length equals
  `total_item_count` — is exact, carries no implied weights, and is safely
  recoverable as level zero.
- **Compacted legacy state** fails the identity and is refused. Its weights were
  discarded at compaction time and cannot be recovered.

`merge_kll_states`' flattening is removed as part of this decision; a merge that
flattens levels destroys exactly the weights the identity checks.

### 7. Memory is a stated contract, tested through insert and merge

The sketch pins a maximum retained-item count and a maximum byte size as
functions of the selected configuration, charges real allocations rather than
item counts alone, and proves both through sustained insertion *and* repeated
merging. Merge is where an unbounded implementation grows while every insert
path looks correct.

Retained canonical values use contiguous fixed-width storage per level, not
`Vec<Vec<u8>>`. The supported quantile families are four- or eight-byte values;
the selected ε=1% payload is at most 43.8 KiB for eight-byte values and 21.9 KiB
for four-byte values, before level, sampler and seed metadata. A
`Vec<Vec<u8>>` representation would add an outer vector object and a separate
allocation per retained item, turning thousands of group states into hundreds
of megabytes or gigabytes before useful data. The byte bound charges actual
capacities for every level and the sampler, not only logical item counts.

Toron ADR 546 currently promises "at most k values plus fixed metadata". That is
false under either candidate: mergeable KLL retains a capacity schedule of
lower-level buffers plus a sampler, and MRL grows with n. ADR 546 is revised to
consume this record's accounting before this dependency pair is committed.

### 8. Order-insensitivity is asserted as a rank bound, not as equality

Acceptance requires every arrival ordering to return a value whose true rank in
the exact population is within the declared bound of the requested rank. It does
**not** require different orderings to return identical values. Equality would
pin one compaction schedule and over-specify the algorithm.

## Stage 0

1. **Choose ε with its price visible — discharged.** Decision 3 selects 1% after
   recording the 0.5%, 1% and 2% per-state costs. The implementation pins
   `k=800`, retained items ≤5,601, and width-aware byte accounting; no hidden
   configuration may substitute another ε.
2. **The ordered multi-block fact measurement.** Build a
   `ColumnRangeDistributionFact` over an ordered column spanning several CSB
   blocks, through the real persist-and-merge path, and compare its buckets to
   the true distribution. The single-sketch error is measured above; end-to-end
   error after cross-block merging is not, and merge-of-halves returning 499
   shows merging can mask it. Use a named ordered fixture, and separately record
   whether any measured production dataset is stored in sorted order — the
   earlier draft asserted that timestamp columns are ascending in storage, which
   is not a source-level invariant and is withdrawn.
   Before suppressing v1 range facts, also record representative binder plan
   shapes and timings with the fact present versus deliberately absent. This is
   a comparability receipt for the no-facts interval, not a requirement that the
   biased fact produce a better plan.
3. **Merge order dependence today — discharged red.** The measured `894` versus
   `893` p50 result in §Evidence confirms that receiver order changes both state
   and answer. That exact uneven-history fixture becomes a red-first control for
   the commutative pair-merge rules in decisions 2 and 4.
4. **The rebuild inventory and the fleet fence.** Enumerate where version 1
   payloads exist, what source data can rebuild each, whether a block rewrite
   path already exists, and which mixed-fleet mechanism decision 5 will use.

## Acceptance

1. **Rank bound against the exact population.** Ascending, descending, shuffled,
   all-identical, and two-valued inputs each return values whose true rank is
   within εn of the requested rank, for p1, p25, p50, p75, p99. Today ascending
   p25 misses by 55 rank points.
2. **Every merge order is bounded against truth.** Each merge order's result is
   within εn of the exact population answer. Orders are **not** compared to each
   other: two results each within εn of truth may legitimately differ by 2εn, so
   a pairwise comparison would assert a bound the algorithm never promised.
   Correctness applies independently to every merge tree, not only a preferred
   one.
3. **Reproducibility is scoped to identical seed and identical merge sequence.**
   Replaying the same inputs with the same seed and the same merge sequence
   produces byte-identical state. This record does **not** claim a system-wide
   canonical merge order: no component today establishes one, and asserting it
   in acceptance would not create it. Any future byte-identity claim across
   differing merge sequences requires first defining that order and proving
   every producer supplies it.
4. **Full state survives serialization.** A multi-level sketch round-trips
   through `AggregateState::Kll` and versioned `ColumnStatsKllPayload` with
   every field of decision 4 intact — seed, identity, per-level compaction
   counters, levels with their weights, and sampler item, weight and height. A
   round-trip collapsing to one level fails, and so does one that drops
   counters or sampler state, since the restored sketch would then draw
   different coins than the original.
   **Merge is commutative on a pair:** `a.merge(b)` and `b.merge(a)` produce
   identical serialized state, which holds only if seed, identity *and*
   counters all reconcile commutatively. The corpus includes a pair whose
   **samplers are at equal height with different retained items** — the case
   the tie-break in decision 2 exists for, and the one that fails on sampler
   state alone while every other field already agrees.
5. **Legacy state is validated.** Uncompacted legacy aggregate state recovers as
   level zero; compacted legacy state is refused; a version 1 CSB block produces
   no range-distribution fact and does not reach the equal-weight path.
6. **No unsupported or unscoped bound is reported.** Aggregate execution exposes
   the selected ε only together with `δ_execution=2^-10` and the 1,024-state
   ceiling from decision 3. Version 1 CSB facts expose no rank error because they
   expose no range-distribution fact; CSB v2 does not report one until its own
   confidence scope is pinned. The Acceptance 1 corpus is a **falsification
   battery**: it can refute the derived bound but cannot establish it, and
   passing it is not treated as proof of the guarantee.
7. **Streams are domain-separated.** Two sketches built independently with
   distinct identities produce **different** compaction coin sequences at the
   same level and index, and a control fails if `sketch_identity` is dropped
   from the PRF input — at which point the sequences become identical. This
   tests domain separation, which is checkable; it does not test independence,
   which no finite control can establish and which decision 4 records as a PRF
   assumption rather than a result. Within one query execution every fragment
   uses the coordinator-issued seed, and the merge-seed formula is asserted
   commutative on a pair.
8. **Memory bounds hold** through sustained insertion and repeated merging,
   charged against real allocation. Retained values occupy contiguous
   fixed-width level buffers; a guard rejects per-item heap allocation.
9. **The Stage 0 ordered multi-block fact** is within the declared bound.
10. **The no-facts interval is explicit.** With the weighted aggregate core and
    v1 writer active, new and recovered v1 blocks produce no range-distribution
    fact while every unaffected statistic survives. Binder controls prove
    absence falls back safely, and the plan/timing receipt from Stage 0 records
    the expected shape change rather than treating it as a regression.

## Guards and controls

- A guard rejects reintroduction of a flat single-buffer quantile representation
  in the CSB payload, and reintroduction of level flattening in merge.
- A guard rejects per-item heap ownership in retained quantile levels; canonical
  values are stored in contiguous fixed-width buffers.
- A guard rejects an **unsupported** rank-error formula — one not traceable to
  the selected algorithm's theorem and configuration. Deriving a bound from `k`
  together with fixed contract parameters is legitimate; `1_000_000 / k` is not,
  because it derives from no theorem.
- A guard rejects a constant compaction seed reaching the sketch constructor.
- The Acceptance 1 corpus rides this record as a named regression test, not as a
  one-time probe.

## Rejected and deferred

- **The GK-topped O((1/ε)·log log(1/δ)) KLL variant.** Ineligible: the paper
  states it is not mergeable, and CSB merges across blocks.
- **Deterministic KLL.** Withdrawn: the paper proves a randomized versus
  deterministic separation, so this is not a variant of the same algorithm.
- **MRL.** Rejected as the selection, not held open. It is deterministic *and*
  mergeable at O((1/ε)·log²(εn)) for both single and all quantiles, which is a
  real alternative — it trades δ away for space growing with n, and would make
  ADR 546's bounded-memory framing false rather than merely inexact. It is
  rejected because a substrate whose footprint grows with the stream is the
  wrong default for per-block column statistics that exist to be cheap. If
  Stage 0 finds the randomized confidence model unacceptable, this record is
  redrafted around MRL and 546/547 revised with it; it is not resolved by
  leaving both live.
- **Reinterpret legacy flat payloads as equal-weight samples.** Rejected in
  decision 5. It manufactures unrecorded weights and preserves the bias.
- **Raise `k` to reduce the error.** Rejected. `k` controls sample size, not
  weight bookkeeping. No `k` fixes an ascending median of 899.
- **Keep a global constant compaction seed.** Rejected in decision 4: it voids
  the theorem's independence precondition.
- **Write CSB v2 as part of the aggregate-core landing.** Rejected as the
  sequence. It would shorten the no-facts interval by letting new blocks carry
  corrected distributions immediately, but pulls the mixed-fleet writer fence
  and rebuild mechanism onto the correctness repair's critical path. The fence
  is the riskiest migration component; safe absence is preferred to delaying
  the aggregate repair or preserving known-wrong v1 facts.
- **Cross-execution byte-identical quantiles.** Rejected as a default in
  decision 4; available only with an explicit adversarial-input model.
- **Repair this inside Toron ADR 546.** Rejected on placement. The sketch, the
  CSB payload, and the statistics facts are storage-ring concerns that exist
  whether or not Toron exposes `APPROX_QUANTILE`.
- **Correct `LinearRegress` here.** Deferred; it remains experimental.

## Consequences

Toron ADR 546 becomes implementable once this lands, and inherits a stated
guarantee it can quote rather than one it must invent. ADR 546 has been revised
to withdraw its fixed seed, 5,000 ppm figure and at-most-k memory promise, and to
enforce the execution-wide state cap this record's confidence requires. ADR 547
follows 546 and inherits the same change from deterministic quartiles to values
stable within an execution.

Range-distribution facts change. If Stage 0 finds material end-to-end error,
existing plan choices on ordered columns were made on biased selectivity, and
plan-shape changes after this repair are corrections rather than regressions.
Timing baselines taken before the repair may not be comparable, and attribution
work in flight over ordered columns should be re-grounded after it.

CSB gains a second format version, a dual reader, and a fleet fence — the first
such split in this block format, setting the precedent for the next one.
