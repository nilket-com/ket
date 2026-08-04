# ADR 347 - finish sf=1: q12/q18 retained-input (regular-inner streamed-spill build)

## Status
Proposed.

## Context

Step 1 of the continue-the-scale-arc sequence: **finish sf=1 green as the minimum qualification gate** (operator +
Codex), clearing the runway for the perf rail and the sf=10 atlas. This cut takes **q12/q18** - the **retained-input**
sub-family (`ADR 336 retained-spill guard`: q12 ~1.14 GB, q18 ~1.42 GB - the spilled build retains its full input
batches). q04/q20 (the spill-writer **index**, data-proportional) is the separate ADR 348, where the **sf=10-durable
"bound the index, not the cap"** discipline applies.

### The load-bearing distinction from 345 (the regular-inner seam)
345 streamed/dropped a **semi/anti existence** build: it only checks **membership**, so the existence-side columns are
never emitted and the input can be dropped after indexing. A **regular inner** build (q12/q18) **emits its columns**
(materialization), so the build **rows** must be **spilled and retrieved on a probe match** - not merely dropped. So
the question is whether 336/345's `SpilledJoinBuild` (which stores build rows in pages + an index) **supports
build-column retrieval on match** for regular-inner materialization, or whether the inner materialization path depends
on the **retained in-memory build batch**.

## Decision - stream the regular-inner build into spill, retrieve columns on match (gated by Phase 0)

For q12/q18's matched spilled-inner-build shape: consume the build's input batches **one at a time**, write/index each
into `SpilledJoinBuild` (rows + key index), **drop each input batch** - never `concat_or_empty` the full build input.
Consume the probe side in bounded chunks when Phase 0 shows the concat'd probe side contributes to the retained
footprint; do not clear the build retention by leaving a full retained probe batch behind. On a probe match,
**retrieve the build row's columns from the spill page** for materialization (preserved output order). Bounded resident
= the 336-budgeted spill state + one build input batch + one probe chunk.

### Phase 0 - the regular-inner streamed-spill seam (the go/no-go premise)
1. **Prove q12/q18's exact retained terms** and that they are **regular inner spilled-join inputs** (the 336 retained-
   spill guard fires on left bytes + right bytes + build-map bytes, ~1.14/1.42 GB). Split the table by side:
   build-side input bytes, probe-side input bytes, build-map bytes, and total. This prevents "stream the build but
   keep the full probe" from being mistaken for a durable fix.
2. **The fork:**
   - **Streamable (incremental):** the regular inner build can stream scan batches into spill + drop them, and
     `SpilledJoinBuild` retrieves build columns on a probe match for materialization. -> proceed.
   - **Dependent (name before implement):** the inner probe/materialization **depends on retained left/right batches**
     (e.g. materialization reads the in-memory build batch directly). -> **name that dependency in the results doc
     before implementing**; the cut becomes "add spill-retrieval for inner materialization," not a bare 345 reuse.
3. No implementation until the seam is grounded (streamable vs dependent).

## Acceptance (the operator's go/no-go)
- **q12/q18 sf=1: green** (`pass@execute` + `pass@compare`), via the streamed-spilled regular-inner build; **OR a named
  next wall** (332 retention: producer + bytes). No silent OOM.
- **The fix streams the build input into spill without `concat_or_empty` retaining the full build input**, and streams
  the probe side in bounded chunks when it is part of the measured retained footprint (the 345 gate-1 no-concat proof,
  extended to regular-inner inputs).
- **No cap-bump as a substitute for bounding retained input** - the wall is cleared by streaming the input, not by
  raising a budget (the sf=10-durability discipline).
- **Correctness:** q12/q18 `pass@compare` at sf=0.01 (their oracles - the regular-inner build emits correct columns +
  order from spill retrieval); if a query greens at sf=1, also run its sf=1 correctness comparison. Full
  `correctness-matrix` remains 22/22.
- **Green cells + q07/q14/q19/q21/q17 unchanged**; de-branded (shape-named); bounded; no cost-model change.

## Guardrails
- **Regular-inner seam grounded first** - streamable vs retained-batch-dependent; if dependent, named before
  implementation (no blind 345 reuse).
- **No-concat proof (345 gate 1)** - the build input is streamed/indexed/dropped, never `concat_or_empty`'d; any
  measured-large probe side is chunked rather than retained whole; tests prove both sides that Phase 0 marks
  contributing.
- **Column retrieval correct** - the spilled inner build retrieves the right build-row columns on a match, preserved
  output order (sf=0.01 parity is the gate).
- **No cap-bump** - bound the retained input by streaming, never raise a budget to mask it.
- **No silent wall / green cells unchanged / bounded** - the standard arc guards.

## Risks
- **Materialization dependency on retained batch** - if the inner build's columns are read from the in-memory batch
  (not spill), the cut needs a spill-retrieval path; Phase 0 names it rather than assuming 345 transfers.
- **Column retrieval correctness** - a spilled inner build must return the exact build columns for matched rows (not
  just membership); the sf=0.01 oracle gates it.
- **Cap-bump temptation** - the wall must be bounded by streaming, not a raised budget (the sf=10 crack).
- **q04/q20 not here** - their data-proportional index is ADR 348 ("bound the index"); not folded.

## Forward arc
347 (q12/q18) -> 348 (q04/q20 index, bound-not-cap) **completes sf=1 (9/9 memory cells green, durably)** -> step 2:
the perf/regression rail (wall-clock, RSS, spill bytes, digest, phase, plan fingerprint; release/cgroup) -> step 3:
the sf=10 atlas -> step 4: Rune-dataframe TPC-H on the same oracles + rail. Each a Toron-side ADR; no central roadmap.

## Why toron-home
Engine cut in `crates/toron` (exec.rs regular-inner spilled build + column retrieval; the 336/345 reuse); driven via
`toron-tpch` reach + correctness at sf=1 / sf=0.01. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. Step 1 (finish sf=1) split: 347 = q12/q18 retained-input; 348 = q04/q20 index (sf=10-durable
bound-the-index). Operator's go/no-go for 347: Phase 0 proves q12/q18's exact retained terms + whether they are regular
inner spilled-join inputs; the fix streams the build input into spill **without `concat_or_empty` retaining the full
build input**, and chunks any measured-large probe input rather than retaining it whole; acceptance is green-or-named
wall (producer/bytes); matrix 22/22; **no cap-bump as a substitute for bounding retained input.** The load-bearing
Phase 0 check is the **regular-inner streamed-spill seam** (vs the 345 semi/anti existence build): a regular inner build
emits columns, so it must spill rows + retrieve on match, not just drop - streamable -> incremental;
retained-batch-dependent -> named before implementation.
