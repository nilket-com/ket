# ADR 511 - Smaller-build placement: swap toward a shrink-only bound

ADRs 509 and 510 closed the q17 spill-mechanism branch red at the fixed 3,554 ms floor: the build
envelope's cost is intrinsic index-construction CPU that relocation cannot recover. This cycle
re-grounded the premise one level up and found the envelope should not exist at all.

q17's `join0` is the comma join `lineitem x part`. The committed sf=8 plan builds on **full
lineitem** — 47,989,007 page-key rows, 622,544,968 spilled bytes, a 19,595.5 ms build envelope at
79.4% of the handler — while the probe side is `part` filtered by `p_brand`/`p_container` to about
0.1% of 1.6 M rows. The clean census row records the pre-admission build estimate as
5,758,680,840 B (`local_join_admissions: declined(estimate_exceeds_budget)`) with
`hash_smaller_build: true`: placement was active and still kept the natural left build.

A bind-receipt probe pair isolated the cause to one gate. Binding the exact q17 SQL against a
catalog with seeded byte bounds (lineitem 5,758,680,840 B, part 240,000,000 B):

| arm | join0 placement | reason |
|---|---|---|
| with the part filter | `Natural` | `HashByDefault` — comparison declined |
| without the part filter | `SwappedRightBuild` | `HashSmallerBuild { 240 MB vs 5.76 GB }` |

The filter that shrinks `part` is what forfeits building on it. The gate is ADR 472's unknown-size
guard (`binder/join.rs:1194`): the brand/container equality produces no NDV estimate, the part side
is marked `filtered_without_estimate` (`binder.rs:1167`), and the guard declines the smaller-build
comparison when **either** side carries that mark.

## The asymmetry

ADR 472 was right that an unestimated filter makes a byte bound stale — but the staleness has a
direction. Under the existing estimate model, `byte_upper_bound` is a true upper bound and `Filter`
facts are volume-non-increasing, so an unestimated filter moves a side's true size only **at or
below** its bound. The comparator swaps toward the right side only when
`right_bytes * 2 <= left_bytes`:

- a stale **right** bound overstates the proposed build side. If the overstated bound still clears
  the dominance test, the true size clears it by more. The swap is safe, and the missing estimate
  can only have delayed it.
- a stale **left** bound overstates the side being rejected as "too big to build". Its true size may
  be tiny, so a swap decided against it can be wrong. Declining remains correct.

The guard's symmetric `either side` is therefore over-conservative in exactly the direction q17
needs: it protects against a stale bound on the side the comparison must trust as larger, but it
also fires when the staleness is strictly favorable.

This claim is **safe under the existing estimate model, not universally safe**: it leans on bounds
being true bounds and on filters never increasing volume. Catalog-bound trust and memory admission
keep their normal guards — admission continues to price the raw bound (ADR 471's rule), so a
swapped build side passes the same admission estimator it would have as a probe side.

## Decision

1. **Narrow ADR 472's decision 2** from "either side" to the trusted-larger side:
   `hash_join_decision_with_placement` declines the swap only when the **left** input is
   `filtered_without_estimate`. A filtered-without-estimate right side compares on its raw bound
   (ADR 471's row-estimate scaling cannot apply without an estimate, and is not needed — an
   estimate would only shrink the build side further). Left-filtered and both-filtered shapes keep
   declining.
2. **Typed, latched control.** `PlannerConfig` gains `filtered_build_side_swap: bool`, compiled
   default **off** at the implementation seal. The gauge maps
   `TORON_TPCH_FILTERED_BUILD_SIDE_SWAP` through the same loud contract as
   `TORON_TPCH_HASH_SMALLER_BUILD` ("0"/"1" explicit, absence = compiled default), and every
   persisted row carries the value and provenance.
3. **Measure, then decide the default.** One paired q17 cohort at the immutable ADR 508 sf=8
   coordinate under the ADR 469 plan-distinct gate (the arms bind different plans by design), fixed
   3,554 ms floor. A census diff enumerates every q01-q22 and m00-m10 cell whose plan fingerprint
   changes between arms; every changed cell runs its supported strict correctness rail in both
   arms. The receipt flips the compiled default **on** iff the paired verdict is `activate` and no
   changed cell prices a material regression — ADR 472's own rule, applied verbatim.

## Acceptance

- Unit trio on `choose_join_strategy`: right-filtered + swap-worthy bounds → `SwappedRightBuild`
  (fails without the relaxation); left-filtered → `Natural` (the existing `adr472_*` pin,
  unchanged); both-filtered → `Natural`.
- Bind-level discriminating pair re-landed from this cycle's grounding probe: the q17 shape swaps
  join0 under the candidate arm; the mirrored shape with the unestimated filter on the **left**
  keeps `Natural` under both arms.
- Census diff enumerated with plan fingerprints per arm; every changed cell correctness-green in
  both arms. q19 — ADR 472's residual fixture — is a named sentinel either way.
- Engagement evidence: the candidate q17 bind receipt records `SwappedRightBuild` for join0, and
  the candidate run commits no `partitioned_streaming_scan_build` spill observation at replay 0.
- Paired verdict against the fixed floor, predeclared estimator, no post-hoc extension.

## Guardrails

- **No new estimators.** Selectivity coverage stays deferred exactly as ADR 472 left it; its future
  arrival still strictly widens the priced set.
- The left-filtered decline is load-bearing and keeps its pinned test; this ADR retargets ADR 472's
  prose ("either side" → "the trusted-larger side"), not its safety intent.
- The ADR 465 armed-evidence rule applies to the census and prices; ADR 469's plan-distinct gate is
  used as-is, unchanged.
- Production placement behavior is unchanged until the receipt commit flips the default.

## Risks

- Cells beyond q17 with a filtered-without-estimate right side will newly swap. By construction the
  chosen build side's raw bound is at most half the rejected side's, so the exposure is
  probe-lifecycle shape, restore-project overhead, and admission-path differences, not a larger
  build. The census diff and per-cell correctness rails bound this empirically before any default
  flip.
- The bind-time swap changes plan fingerprints on engaged cells; downstream tooling that pinned
  fingerprints must re-pin from the receipt's census, in the same cut.

## Forward

- **Aggregate-input reduction by reachable keys** is the second, larger q17 remedy and the next
  council topic: no pass today prunes a decorrelated aggregate's input by join reachability — q17's
  per-partkey AVG folds all 47,989,007 lineitem rows (10,584.6 ms, 91.3% grouped fold) before the
  recombine. The insertion point is between aggregate construction (`binder.rs:2858` deferred /
  `sql_planner.rs:6088` qualified) and the recombine (`binder.rs:2894`); the census harness already
  hand-writes the reduction (`q17_census_partkey_predicate`), proving the semantics.
- Selectivity coverage (decimal range tag, string NDV) remains queued behind it, unchanged from
  ADR 472's forward arc.
- The distributed runtime-filter machinery (inner-only, probe-scan-targeted) is a possible physical
  route to the same reduction if it ever generalizes past `Inner` and off the distributed path.
