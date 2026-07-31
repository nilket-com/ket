# ADR 409 - Adaptive derived-side build for over-cap streamed-inner stages (q03 sf=10)

## Context

408 proved q03 sf=10's six-minute wall: the partitioned streamed-inner scan-build path re-reads the projected
`lineitem` source once per active partition group - **~30.8 full passes, 1.85B rows, 82 GB in 180 s**. The same
partial-progress artifact showed that execution had barely advanced into the other side (`orders` had emitted only one
1,024-row batch), so it is not a derived-side size proof; the derived `customer x orders` size must be measured in Phase
0. 408 named the fork this ADR executes: **adaptive derived-side build if it measurably fits, else the one-pass
partition reservoir.**

**Grounded root cause (code, not conjecture):** the reorientation is **structural, never size-based**.
`extract_reoriented_inner_chain` (`exec/join/streaming.rs:357`) makes whichever side *continues the join chain* (the
derived side) the **probe** - so it can stream morsel-wise down the chain - and the leaf/scan side the **build**
(`:390-413`). Size is never consulted. At sf=1 that choice is right (the 3.24M-row lineitem build fits in memory, on
405's primitive table). At sf=10 the same choice puts a ~32M-row build over the ~1 GiB cap, and the over-cap fallback is
the 403 `PartitionedScan` machinery, whose per-group re-scan is the 82 GB loop.

The hypothesis is that the derived side is small enough: customer filtered to 300,276 rows (BUILDING), and the matching
orders subset should be materially smaller than the 32M-row `lineitem` build. This is deliberately a hypothesis, not a
planner estimate. Phase 0 measures the derived side directly. **If the executor flipped the over-cap stage to build on
the measured derived side and probe the scan side in a single streaming pass, q03 sf=10 would read lineitem once
(~2.6 GB) instead of 30.8 passes (82 GB) - with no new spill mechanism.**

Two structural facts make the flip tractable:
- the reoriented chain is **inner-only** (`JoinKind::Inner` required, `:375-380`) - no outer/anti
  `matched_build_rows` semantics to preserve across a flip;
- `probe_columns_first` (`:424`) already models both column orientations - the output-column restore machinery exists.

## Decision

### Phase 0 - measure and confirm the seam (before code)
1. **Derived-side size, measured.** Run the c x o subplan at sf=10 in isolation (bounded scope) and record actual
   rows/bytes and `estimated_build_hash_bytes`. The 408 progress hooks make this cheap. This is a **measured** size, not
   a planner cardinality - if it does not fit under the build cap, fork B.
2. **Seam confirmation.** The over-cap detection already exists: `reoriented_inner_stage_scan_build_decision` (403/405)
   computes scan-output and hash estimates before choosing `PartitionedScan`. Confirm the flip can hang exactly there:
   when the scan-side build is over-cap, the alternative is "materialize the derived side under the cap, measure, and
   if it fits build on it." Record what materializing the (currently streaming) probe side entails at this seam, and
   whether the existing `try_visit_streaming_inner_join_scan_probe_chunks` /
   `visit_streaming_in_memory_inner_scan_probe_chunks` machinery can be reused by swapping build/probe roles, or whether
   the reoriented runtime needs a narrow new adaptive-build variant.
3. **Order/consumer audit.** Flipping a stage changes that stage's emission order (new probe = scan order, not derived
   order). Confirm what consumes the chain output for the family (q03/q05/q10/q12 - aggregates/sorts above), and that
   no order-bearing contract breaks. If an order contract binds, that is a stop-line finding, not a hack site.

### Phase 1 - fork A (preferred): adaptive derived-side build
When a reoriented stage's **scan-side build is over-cap** (the currently-pathological path - today it re-scans):
- **materialize the derived side under the memory cap** (cap-guarded; if materialization exceeds the cap, abort the
  flip and fall back to today's partitioned path - never a correctness change);
- **measure actual bytes** (`estimated_build_hash_bytes` on the materialized batches - the same guard family as today);
- **if it fits:** build on the derived side (405 primitive table; inner-only, `probe_columns_first` restores column
  order) and stream the scan side through as a **single-pass probe**, preferably via the existing in-memory-build /
  streaming-scan-probe helper surface;
- **if it does not fit:** today's partitioned scan-build path runs unchanged.
Fenced: fires **only** on the over-cap reoriented path; below-cap stages (all of sf=1) are byte-unchanged.

### Fork B (fallback, named): one-pass partition reservoir
If Phase 0 shows the derived side does not fit (now or commonly for the family), or the flip cannot preserve consumer
contracts: scan the build source **once**, route partition rows to a spill reservoir, and load groups from the
reservoir instead of re-reading Parquet per group. That is the bigger streaming-completion mechanism and becomes its
own ADR (or 409's cut if A is impossible) - not smuggled in beside A.

### Stop-lines
- **No q03-shaped branch** - the flip keys on measured sizes and plan shape only.
- **No cap bump** - completing by raising memory/time budgets is out of bounds.
- **Order contract breaks** -> seal the design finding; pivot to B.
- **No reusable scan-probe seam** -> seal the design finding or add a narrow runtime variant; do not route through a
  broad executor rewrite inside 409.

## Acceptance
- **Phase 0 recorded:** the structural-reorientation grounding, measured c x o size at sf=10, seam confirmation, and
  the order/consumer audit - before code.
- **Primary: q03 sf=10 moves off time/no-row** under the 398 scope (4 GB, 360 s): persisted correctness `pass@compare`,
  **or advances to a named + reproduced next wall.** Expected mechanism proof: the 408 progress hook shows **one**
  lineitem pass (~2.6 GB), not ~31 (82 GB).
- **Measured-bytes gate:** the flip decision uses materialized/measured bytes only - never planner cardinality.
- **sf=1 byte-unchanged:** below-cap stages take the existing path; q03 sf=1 four-way within band, fingerprint
  `666e0ac028036f70`. The won ground is a control.
- **Discriminating tests:** over-cap scan-build + fitting derived side -> flipped (derived build, single-pass scan
  probe); over-cap + non-fitting derived side -> today's partitioned path; below-cap -> unchanged. Inner-chain
  correctness (join output equality across orientations), column-order restore, null routing.
- **Controls:** q08/q19/q04 sf=10 green (q08 exercises the same 403 family - it must keep its win); q05/q10/q12
  re-probed cheaply and movement recorded (generalization signal, not banked acceptance).
- **Full engine gate:** `--no-fail-fast` both crates + architecture_test after fmt; de-branded; no committed
  `.perf.data`.
- **Scope honesty:** fixes the over-cap streamed-inner family where the derived side fits; **B remains named** for the
  cases it does not; not a claim that all >360 s cells complete.

## Guardrails
- **Measured, never estimated** - the flip keys on actual materialized bytes; planner cardinality is a named
  stop-line (the 401 optimizer-evidence gap stands).
- **Fenced to the pathological path** - only over-cap reoriented stages; sf=1 and all below-cap behavior byte-identical.
- **Cap-guarded materialization** - the derived-side materialization runs under the same memory budget; overflow aborts
  the flip, falls back to today's path.
- **Inner-only chain** - the flip lives inside the inner-only reoriented machinery; no outer/anti bitmap surface.
- **B is not smuggled in** - reservoir work is its own cut.
- **Won ground is a control** - q03 sf=1 four-way + q08/q19/q04.

## Risks
- **Derived side does not fit** (here or for siblings) -> fork B named; the flip's fence already handles it at runtime
  (falls back to today's path).
- **Materialization inverts the chain's dataflow** (probe side currently streams) -> Phase 0 seam confirmation; the
  materialization is cap-guarded and only on the already-pathological path, where today's alternative is 31 re-scans.
- **Emission-order change breaks a consumer** -> Phase 0 audit; stop-line to B if a contract binds.
- **Column order across the flip** -> `probe_columns_first` + existing restore machinery; locked by tests.
- **q08 regression** (same 403 family) -> control; the flip must not fire where the scan-side build is the *right*
  choice (q08's fixed routing keeps its streamed path when the derived side does not fit).
- **Doesn't complete even with the flip** (a next wall appears) -> reach-board honesty: name + reproduce it.

## Forward Arc
A landing makes q03 sf=10 the first heavy time-cell to complete, likely pulls q05/q10/q12 with it (same signature), and
leaves fork B (reservoir) as the named mechanism for derived-sides-that-don't-fit - the remaining brick of
streaming-completion, informed by real evidence. The article chapter writes itself: *the 373 ms query that read 82 GB -
and then read 2.6 GB.* Follow-ons: B, q08 execution speed, decode residual.

## Grounding evidence (file:line)
Structural reorientation (size never consulted): `exec/join/streaming.rs:357` (`extract_reoriented_inner_chain`),
`:390-413` (chain side -> probe, leaf side -> build), `:375-380` (inner-only, no match_filter, non-null-safe,
hash-partitioned), `:424` (`probe_columns_first`). Over-cap decision seam: `reoriented_inner_stage_scan_build_decision`
(403/405; scan-output + hash-bytes estimates -> `PartitionedScan`). 408 evidence: 30.8 lineitem passes / 1.85B rows /
82 GB in 180 s; customer output 300,276 rows; orders 1,024 rows read; SQL == Rune `666e0ac028036f70` at sf=1 and sf=10;
100% CPU, ~1.54 GiB RSS (`plans/impl/408_q03_sf10_time_cell_diagnostic_results.md`). sf=1 in-memory build: 3,241,776
  rows on the 405 primitive table (404/405 results), demonstrating that a larger build can fit at sf=1 but not proving
  the sf=10 derived side fits. Progress hooks: `TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH`,
  `TORON_JOIN_BUILD_PROFILE_PATH` (408).
