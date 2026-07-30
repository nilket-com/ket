# ADR 518 - Reachable keys through membership-filtered chains

The post-517 board re-rank put q20 second by absolute gap (8,740 ms toron vs 617 ms one-thread
DuckDB, 14.2x — the worst ratio on the board). The two-stage grounding ordered for this cycle
found one dominant owner and one exactly-localized decline.

**The envelope (stage 1).** The correlated scalar aggregate — `SUM(l_quantity)` grouped by
`(l_partkey, l_suppkey)` — owns 5.81 s of q20's 9.55 s instrumented span (61%): stream_pull
0.90 s, fold 2.86 s (nearly all `row_lookup_update`), finalize 1.71 s, zero spill, full
reconciliation. It folds all 47,989,007 lineitem rows into 6,396,683 groups. The reachable set
is tiny: 17,203 forest parts → 68,812 partsupp pairs → **517,084** reachable lineitem rows. The
attainable reduction is 92.8x on fold input and 93x on finalized groups; the directly priced
removable component (fold + finalize scaled by the ratio) is ≈ 4.5 s — **above** the 3,554 ms
floor on its own, unlike ADR 517's economics.

**The decline (stage 2).** Four bind-level probes localize it exactly (a 2x2: plain-`Filter`
vs membership-semi outer x single vs composite correlation). Composite keys already engage when
the outer chain is plain-filtered — ADR 512's rule handles multi-key correlation today. Both
semi-filtered shapes decline, at two independent gates:

1. `trace_key_to_filtered_base` (binder/scalar_reduction.rs) recognizes equality partners only
   at `Inner` joins and ends any trace at other join kinds; a membership `LeftSemi` neither
   queues its build-side partner nor counts as filter evidence, so `ps_partkey` never reaches
   the deterministically filtered part scan sitting one edge away.
2. `derive_reachable_key_source` requires **every** correlation key to trace to one common
   filtered source. `ps_suppkey` has no filtered source anywhere in q20, so even with gate 1
   opened the composite pair declines.

## Decision

Two bounded relaxations, both instances of ADR 512's existing superset theorem — the semi build
must only be a superset of the keys actually probed; row-removal above the source never breaks
that:

1. **Semi-partner traversal.** At a membership semi in the trace, the surviving probe-side key
   domain is bounded by the build-side equality partner (a row survives only by matching), so
   the partner is queued exactly as an `Inner` equality partner is today, and the semi is
   skipped as a row-removing node like `Filter`/`Distinct`. Orientation-general (left or right
   membership), same plain-column equality requirement as the `Inner` arm.
2. **Traceable-subset derivation.** When a nonempty subset of correlation keys traces to one
   common filtered source and the remaining keys trace nowhere, derive the semi on that subset.
   A semi on fewer keys removes fewer rows — a strictly weaker, still-sound superset reduction.
   Keys tracing to **different** filtered sources still decline (no multi-source composition in
   this cut); zero traceable keys still decline; every other gate is untouched.

The reachable-key source this yields for q20 is the **filtered part scan** (17,203 rows at
sf=8) — the same cloned-tiny-scan acquisition class ADR 512 accepted for q17. The alternative
source (the semi-filtered partsupp chain itself, which would carry both keys) is **rejected**:
it duplicates the partsupp membership join — ADR 513's duplicated-acquisition red flag — and at
q20's foreign-key-complete data the partkey-only reduction already reaches the same 517,084
rows, so the second key buys nothing for the cost of a cloned join.

Typed, latched control: `PlannerConfig::semi_filtered_reachable_keys`, compiled default off at
the seal; gauge env `TORON_TPCH_SEMI_FILTERED_REACHABLE_KEYS`, loud "0"/"1" contract, row fields
with provenance. Plans change; cohorts run plan-distinct.

## Acceptance

1. **Structural battery**: the four grounding probes ride the impl, the two semi shapes flipped
   to engagement (single-key and composite both insert the reduction semi keyed on the traced
   subset); declines pinned for keys tracing to different filtered sources, zero traceable
   keys, and a non-equality membership; the derived source is the filtered dimension scan, not
   a join clone (asserted on the bound plan).
2. **Semantic battery** (both arms identical results, the 512/515/517 fixture family): NULL
   keys on both sides, duplicate outer rows and duplicate matches, empty membership, and a
   subset-reduction case where the untraced key would have removed additional rows — proving
   the weaker semi is a pure superset.
3. **Census**: q01-q22 + m00-m10 both arms, digests identical, fingerprint-changed cells run
   strict correctness in both arms; q19 sentinel. Required q20 evidence from the ADR 516
   instrument vocabulary: aggregate fold input 47,989,007 → ~517,084, finalized groups
   6,396,683 → ≤68,812.
4. **Paired authority**: q20 cohort, 20 runs / 10 adjacent pairs, plan-distinct gate, fixed
   3,554 ms floor, Hodges-Lehmann + 95% Wilcoxon from the persisted journal; activation per ADR
   472's rule (at least one activate verdict, no engaged-cell regression); every engaged cell
   the census surfaces gets at least a regression-check cohort.

## Guardrails

- Both relaxations fail closed to today's residual behavior; the knob-off arm is
  byte-identical.
- No executor changes; the semi the rule inserts is the same LeftSemi the placement and
  admission machinery already handles (ADR 511/512 surfaces untouched).
- The trace still terminates at 64 steps; the semi arm adds no new recursion class.

## Risks

- ADR 515's membership-first commute may relocate the membership semi before the reduction
  runs; the grounding probes bind with the full default stack (515 on) and still see the semi
  adjacent, but the census arbitrates every other shape.
- Other cells with membership-filtered outers may engage (q02's shape family); the census
  enumerates them and each gets its regression check.
- The subset semi is weaker than the composite ideal; if a future cell's untraced key carries
  the selectivity, the reduction under-delivers — an economics risk the per-cell cohort prices,
  not a correctness risk.

## Forward

- Multi-source composition (keys tracing to different filtered bases → intersecting semis)
  stays out of scope until a cell demands it.
- q13 grounding is next on the board (7,122 ms gap, unknown ownership, one of two spilling
  cells); q21 stays parked absent a narrower mechanism than cache tuning.
- The q20 recombine join (3.18 s envelope) shrinks as a consequence of the reduced group count;
  it is named, not priced.
