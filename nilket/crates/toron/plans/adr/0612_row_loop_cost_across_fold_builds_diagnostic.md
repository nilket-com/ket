# ADR 612 - the grouped row loop's cost across fold builds: a diagnostic

**Status:** PROPOSED. Successor to ADR 611. **A diagnostic record: no
token, no pair, no cut.**

ADR 611 found that q13's row-at-a-time fold costs about 1.1 s more in
`row_lookup_update` in one build than in another, running the same
source path:

| build | q13 shape | `row_lookup_update` µs |
|---|---|---|
| ADR 610 control (`8d73e650`): `RowAtATime`, two-phase code dead | row-at-a-time | 1,532,647 |
| ADR 611 control (`1dddc7fe`): `TwoPhase`, `AllTyped` | two-phase | 1,671,386 |
| ADR 611 treatment (`18372d0c`): `TwoPhase`, `AtLeastTwoTyped` | row-at-a-time | 2,674,129 |

These are single diagnostic runs each. The ADR 611 treatment also ran
about 13 MB higher in peak RSS. This record finds out where that time
and memory go, before any further cut. Its timing is diagnostic, never
paired latency.


## Context

**The candidates**, none established:
1. **Codegen.** Inlining or layout of the row loop changes when the
   two-phase branch is live in the same function. For example, the
   lookup (`entry_or_init`, `find_bucket_in`), its closures, or the
   per-row state match may no longer be inlined, or the hot loop's
   layout may shift.
2. **Per-batch work.** The template and eligibility work in the treatment
   build: `init_bound_states_with_seed` and kernel selection, once per
   batch (1,465 batches for q13's dominant fold). This looks too small
   by count to explain 1.1 s.
3. **Allocation or code paths.** Changed allocation or code paths, which
   would also bear on the RSS difference.

ADR 610 read its own q13 regression as a second scattered pass. That
reading is reopened as another candidate, since ADR 610's two arms also
differ in which branch was dead.


## Decision

*The builds.* A symbol-rich `profiling` build (release plus
`debug = 1`, unstripped, as in ADR 508) of each of the three trees
above, on bigfoot. Each build is identified by its tree, its
executable's SHA-256, and its `.text` size. That the three profiling
builds preserve their release builds' relative behaviour on q13 is
**checked, not assumed**. The ordering check uses each build's five-run
median `row_lookup_update` and its range. It is **established** only
when the medians keep the table's ordering and the five-run ranges of
adjacent builds do not overlap. Otherwise it is unestablished, and the
cross-build reading is limited to what the separated builds show.

*The runs, on strix, all diagnostic.* Per build, q13 at the ADR 608
coordinate:
- **Each run is gated.** Its digest and plan fingerprint must equal ADR
  608's sealed q13 cell, and the route must be `scan_grouped` for the
  dominant fold. The kernel receipt must show the expected shape: ADR
  610 control and ADR 611 treatment run row-at-a-time (zero two-phase
  batches), and ADR 611 control runs two-phase. A run that fails is
  excluded and named, so that a symbol comparison cannot hide a plan or
  path change.
- **CPU:** five runs under `perf record -F 999 -e cpu-clock`, each with
  node attribution and the kernel receipt. Samples are summed by period
  and divided by the attributed `row_lookup_update`, as in ADR 508, per
  leaf symbol and per source line. The shortfall is reported, not
  absorbed.
- **Memory:** one run under `perf record -e page-faults`. It reports the
  fault distribution by leaf symbol, and each run's labelled peak RSS.
  Page-fault samples show where faults landed in that run, not what
  caused a peak-RSS difference. No heap allocation or peak composition
  is inferred from them.

*Static evidence.* Per build, the following for the symbols the fold
inlines into (the async caller, `fold_bound_grouped_aggregate_batch_*`,
and the lookup functions):
- whether the lookup functions exist as separate out-of-line symbols
  (`nm`);
- their sizes;
- the call instructions inside the hot loop's function (`objdump -d`).

These are codegen **clues**, not proof that a lookup was or was not
inlined into the hot loop. That claim requires disassembly with
source-address (DWARF line) evidence: the lookup's source lines appear
inside the hot loop's instruction range, or a `call` to its out-of-line
symbol appears there. Cases without that evidence are named as
unresolved.

*The report.*
- A per-build table of the leading symbols by period share of
  `row_lookup_update`, with their ms per run.
- The out-of-line or inlined status of the lookup path, where the
  disassembly with source addresses shows it, with unresolved cases
  named. The code sizes.
- The page-fault distribution beside each run's labelled peak RSS.
- A reading of which candidate the evidence supports, stated as
  evidence for or against each candidate, **not** as an established
  cause. What remains unexplained is stated.

*What follows.* If a candidate is supported, the next record is a
one-token cut against it, planned in its own record and measured by a
pair. If none is, the record says so, and the fold is left as ADR 610
and ADR 611 closed it.


## Acceptance

- Three profiling builds, with identities and `.text` sizes. Each
  build's q13 runs match ADR 608's digest and plan fingerprint, take
  the `scan_grouped` route, and show the expected receipt shape.
- Per build:
  - five `perf` runs with node attribution and receipts;
  - the period-weighted table, with its shortfall;
  - one page-fault run;
  - the static evidence.
- The ordering check (each profiling build's `row_lookup_update`
  ordering matches the release builds'), or the statement that it
  fails, which then bounds what the record can say.
- Plan commit, then one implementation commit carrying the results
  (and the classifier, if one is added) and the closure.


## Guardrails

- No token, no production code change, and no retain or revert
  decision. Nothing here is paired latency.
- No causal claim beyond the evidence. A symbol's share is where
  sampled CPU time landed, not a decomposition of the 1.1 s.
- Strix runs are single-process diagnostics, one at a time, with the
  host quiet. No measurement window is claimed.


## Risks

- Debug info should not change codegen, but it could shift layout. The
  ordering check guards the inference. If it fails, the record reports
  the profiling builds' own behaviour and says it may not represent the
  release builds.
- Inlined code attributes to its caller's symbol. Source-line
  attribution is needed to split the fold body, and the lines are valid
  only per build identity.
- The page-fault distribution sees where faults landed, not the peak's
  composition or its cause.
