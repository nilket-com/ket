# ADR 418 - q18 inner-chain-under-semi composition: awaitable chain sink into the projected LeftSemi fold

## Context

Post-417 board: 15/22 green. q18 is the last cell failing at `scan output accumulator` (512 bytes
over the 1 GiB cap - a red herring, same doctrine as 417: at sf=15 this scan is far past any cap;
cap-nudges are rejected). Its persisted sf=10 plan is

```text
Aggregate <- Exchange <- LeftSemi replay 2(
  Exchange <- Inner replay 1(
    Exchange <- Inner replay 0(customer, orders),
    Exchange <- Scan lineitem[l_quantity,l_orderkey]),   # estimated 68,489,092,352 bytes
  grouped lineitem membership)
```

`try_streaming_projected_left_semi_grouped_aggregate` (`exec/join/streaming.rs:6681`) recognizes
the outer topology, then materializes the whole preserved side via `execute_bound(left)` (`:6738`)
to feed its estimate-then-spill gate (`:6741`). That recursion consumes the terminal lineitem scan
through the vanilla materialized Scan arm and raises the accumulator at 1,073,742,336 /
1,073,741,824 before the recognizer's own spill machinery below it ever runs.

Both composition endpoints already exist. `extract_reoriented_inner_chain` (`:360`) walks exactly
this preserved shape - Join(1) over a Join(0) base, with bare/filtered/fully-pushed continuations
admitted since ADR 417 (`:615-655`) - and `ReorientedInnerJoinBuild::PartitionedScan` (`:66`)
already streams/spills a stage whose build input is a huge scan, which is precisely where q18's
lineitem sits. On the other end, the projected-LeftSemi fold already consumes a spilled projected
build (`SpilledInnerJoinBuild` written at `:6753-6771`, folded by
`fold_projected_streamed_left_semi_grouped_aggregate` at `:6796`) without caring who wrote the
pages. What is missing is only the glue - and the glue has a real seam problem, pinned during
council grounding:

- **The two existing gates are not interchangeable.** The measured stage classifier
  `reoriented_inner_stage_scan_build_decision` (`:4359`) *materializes the scan output first*
  (`materialize_streaming_scan_output_for_cap`, `:4372-4374`) - calling it as the outer gate would
  recreate the exact wall this ADR removes. The terminal metadata shortcut
  `reoriented_terminal_scan_build_estimated_over_cap` (`:4399`) is non-consuming: it reads
  `estimated_byte_size` off the Scan node and fires only past 8x the materialization cap.
- **The completed chain has no reusable exit today.** The chain's output is emitted through
  synchronous `FnMut(RecordBatch)` closures embedded in the grouped consumers (fold calls at
  `:7307/:7797/:7975`); the base visitor (`:9235`) produces only replay-0 output. Meanwhile the
  spill writer `write_projected_batch_chunks` is async. A synchronous shared seam cannot await the
  writer; a writer-private copy of the chain driver would duplicate the adaptive, scan-probe,
  generic-base, and reservoir orchestration. The seam must be made explicitly awaitable.

## Decision

### Phase 0 - persisted re-probe (before code)

Re-reproduce the q18 wall at current HEAD through the exact `perf-run-one --mode strict --sf 10`
path; re-capture the plan fingerprint (`d8b60e3a03bfaf79` as of 417), the three preserved-side
`estimated_byte_size` values, and prove the terminal shortcut fires for the replay-1 stage **on
the artifact**, not by assumption. Record the membership side's plan shape and its candidate
recognizer - it has never executed at sf=10 (the preserved side blows first), so its wall status
is genuinely unknown. Record all before code.

### The cut - one bounded composition

**1. Chain-preserved alias, in place.** When the preserved exchange input of the projected grouped
LeftSemi contains the exact q18 chain - `extract_reoriented_inner_chain` yields one replay-1 stage
over its replay-0 base - the recognizer takes the new arm instead of `execute_bound(left)`. Longer
reoriented chains remain outside this alias; they are not needed for q18 and would widen both the
intermediate-buffering proof and the semantic fence. Every existing fence stays: outer
`LeftSemi` + `match_filter: None`, exchange/group order rule, projection benefit, the admitted
stage `Inner` + `HashPartitioned` + non-null-safe keys, and the replay-0 base shape already pinned
by the extractor.

**2. Gate: non-consuming hazard fence, then unconditional spill.** After the shape fence, the
alias fires only when the terminal stage passes the existing metadata shortcut
(`estimated_byte_size` > 8x materialization cap - q18's 68.5 GB clears the 8 GiB threshold by
~8.5x). Then the completed projected preserved stream **spills unconditionally**: no second
materialize-to-estimate gate exists on this arm, by design, because obtaining the old estimate
requires the materialization being refused. The measured (consuming) classifier is banned from
the outer gate. Inherited fences are kept and documented, not relaxed: the shortcut declines under
a morsel and under sub-64-MiB task budgets (the persisted path is morsel-free at 64 MiB; q07/q09
already ride this shortcut at `:7022`). If metadata is absent or below threshold, the alias
returns `None` and today's vanilla path runs - **never** a silent default-to-spill for all
chain-preserved semis.

**3. The awaitable chain seam: a private two-arm sink.** One completed-chain driver is extracted
from the grouped consumers, generic over a chain-local sink enum (illustrative shape):

```text
ReorientedChainOutputSink
  GroupedFold    { existing group/aggregate state and expressions }
  ProjectedSpill { spilled build, retained columns, key plan, remapped semi keys }
```

`GroupedFold::emit` calls the existing synchronous fold and returns ready - the existing
reoriented grouped-chain consumer keeps its behavior through this arm. `ProjectedSpill::emit`
awaits `write_projected_batch_chunks`. Both arms are internal to `streaming.rs`: no public trait,
IR variant, channel, pull stream, or executor-wide callback contract. The enum (rather than a
generic async closure) keeps the two admitted consumers visible and prevents the seam from growing
into a generic executor API.

The await must reach every route to completed output: the ordinary suffix helper, scan-probe
replay-0 route, generic-base route, partitioned-scan reservoir route, and adaptive terminal flip.
The terminal stage may not use the current collect-then-return `Vec<RecordBatch>` API: join fanout
can make even one input probe batch produce arbitrarily many chunks. It emits and drains the sink
once per materialized `CROSS_JOIN_OUTPUT_CHUNK_ROWS` chunk before producing the next. The exact
q18 `ProjectedSpill` alias has only that one suffix stage, so it never needs an intermediate-stage
output vector. Pre-existing temporary vectors between nonterminal stages may remain for the
`GroupedFold` behavior-preservation arm; ADR 418 neither widens them nor claims they are globally
bounded. No vector accumulates **completed q18 chain output** across chunks or base/scan batches.

**4. Commit point and cleanup.** The alias performs all shape, projection, key-plan, schema,
metadata, and sink-capability checks **before any scan executes**; any failure returns `None` and
vanilla runs. Once the first chain scan or spill write starts, the arm is committed: it returns
`Some(output)` or `Err` after cleanup - never a late `None` that would trigger a silent vanilla
re-execution (and a second full customer/orders/lineitem scan). Internal adaptive-to-partitioned
fallback stays allowed inside the committed arm, subject to the scan-count locks. The spill build
and prepared runtimes are cleaned on every error and internal decline; in particular, a
membership-side wall discovered after preserved pages were written cleans the spill before
propagating (stale spill scratch has broken later runs before).

**5. Projection and exchange accounting.** The sink receives completed chain batches in the
replay-1 logical schema; the projected-spill arm uses the outer recognizer's `retained_columns`,
remapped semi keys, projected schema, and `JoinKeyPlan` exactly as the existing materialized-left
loop does. Chain-stage projection is seeded backward from that same retained set so a required
outer field cannot be pruned before the sink. Because extraction begins beneath the preserved-side
`Exchange`, eliding that exchange must be an explicit grounded decision (the spill is indexed by
the semi key and the fold applies its own output repartition) or the driver replays it once; the
parity fixture uses a multi-partition exchange so the decision cannot pass vacuously.

### Stop-line

If the implementation spike shows the awaitable seam requires an executor-wide callback/pull
surface rather than a private `streaming.rs` extraction, ADR 418 seals as evidence for the same
future execution-model C-spec evidence set as q02 - no partial generic rewrite lands.

## Acceptance

- **Per-cell primary (no rounding):** q18 either lands persisted strict sf=10 pass@compare, or
  advances to a **named, byte-reproduced next wall** (the membership aggregate is the expected
  candidate) with the scan-output producer gone. Both are wins; no scope widening after the
  preserved wall moves.
- **Eligibility-discriminating locks from the start** (the 417 review lesson, folded in rather
  than retrofitted):
  - the primary lock invokes the recognizer directly and requires `Some(output)` on an exact
    one-stage replay-1-under-LeftSemi fixture - spill telemetry alone is NOT discriminating here
    (the chain runtime or membership path can spill while the outer composition misses);
  - materialized-vs-streamed parity with **mixed-type retained columns and distinct values in
    every retained field** (an all-Int64 fixture can be digest-correct with wrong ordinals),
    duplicate membership keys, unmatched keys, duplicate preserved rows (LeftSemi emits each
    preserved row once), and a multi-partition preserved exchange;
  - a wall fixture that forces the **preserved chain itself** past the materialization cap (not
    merely some inner build): vanilla materialization fails with the accumulator error, the
    direct recognizer succeeds;
  - negative fences proving a longer reoriented chain remains unadmitted and missing/below-threshold
    terminal metadata returns `None` **before any scan telemetry changes** - the stop-line is
    proven, not just the result variant;
  - scan-count locks counting customer, orders, preserved lineitem, and membership lineitem
    separately - internal fallback cannot introduce a rescan loop;
  - a late-error cleanup fixture: membership failure after preserved pages were written leaks no
    spill scratch.
- **ScanRequest parity:** every scan through `bound_scan_request`;
  `bound_scan_request_carries_every_bound_scan_field` remains the all-observable-field lock.
- **Grouped-consumer neutrality:** the existing reoriented grouped-chain consumer is
  behavior-identical through the `GroupedFold` arm - its lib locks stay green, and the
  spill-bearing controls (q03 q07 q09 q10 q12 q14 q16 q19 q20, **plus q13** from 417) re-prove
  pass@compare with the forensic bands.
- **No cap/multiplier/scope change; full engine gate** (`--no-fail-fast` both crates +
  architecture after fmt); de-branded production source; no committed `.perf.data`.

## Guardrails

- **One bounded composition** - no pull interface, no scan spill without a pulling consumer; the
  C-spec stop-line fires rather than half-landing a generic surface.
- **The consuming classifier stays off the outer gate** - only the non-consuming metadata
  shortcut gates the alias.
- **Metadata-absent honesty** - decline to vanilla; redesign the gate in a follow-on if Phase 0
  finds the shortcut cannot fire.
- **Commit-point discipline** - all eligibility pre-I/O; no late `None`/vanilla rescan.
- **Bounded seam** - the sink drains per chunk; no cross-batch accumulation of completed output.
- **Spill scratch is cleaned on every exit** - error paths included.
- **Won ground is a control** - 417's greens re-proven, not assumed.

## Risks

- **The membership wall is probably real** (a ~15M-group lineitem aggregate that has never run at
  sf=10) -> reach honesty: advancing q18 to a named membership wall is an accepted primary
  outcome, and the preserved-side machinery landed here is required either way.
- **The extraction is the risk center** - four orchestration routes plus the shared suffix exit,
  with the adaptive arm sharpest -> `GroupedFold` behavior preservation is pinned by the grouped
  lib locks + controls; the seam refactor stays private to `streaming.rs`.
- **`estimated_byte_size` is full-row metadata, not projected bytes** -> the 8x margin is the
  existing, deliberately conservative answer (`:4419-4425`); q18 fires regardless; a
  projected-bytes fact is a named non-goal.
- **Exchange elision subtlety** -> explicit grounded decision + multi-partition parity fixture.
- **Fingerprint drift since 417** -> Phase 0 re-probes before code.

## Forward Arc

q18 green (or advanced past the preserved wall) closes the scan-output accumulator family opened
by 415 and largely resolved by 417. Remaining threads: q21+q22 look like a candidate shared
residency-policy family (semi/anti existence-set and spill-writer resident state); q02
join-output (1.738 GB, no component split) is the nearest driver for the execution-model C-spec;
q05 needs a diagnosis-first cut (kernel OOM, no graceful producer); q11 is the oracle
reconciliation correctness cut. The q10 compact-index speed win stays parallel.

## Grounding evidence (file:line)

Recognizer + miss: `exec/join/streaming.rs:6681` (`try_streaming_projected_left_semi_grouped_aggregate`),
`:6738` (`execute_bound(left)`), `:6741` (materialize-to-estimate gate), `:6753-6771` (projected
spill write), `:6796` (fold endpoint). Chain machinery: `:360` (`extract_reoriented_inner_chain`),
`:615-655` (continuation, Exchange unwrap + bare/filtered/fully-pushed aliases per ADR 417), `:66`
(`ReorientedInnerJoinBuild::PartitionedScan`), `:32` (`ReorientedStageScanBuildDecision`). Gates:
`:4359` (measured classifier; consuming at `:4372-4374`), `:4399` (non-consuming terminal
shortcut; morsel/64-MiB fences `:4404`, 8x rationale `:4419-4429`), `:7022` (q07/q09 shortcut
rider). Chain exits to unify: `:9235` (base visitor), fold closures `:7307/:7797/:7975`, adaptive
lower visitor `:7471/:7529`. Wall bytes: q18 1,073,742,336 / 1,073,741,824
(`plans/impl/415_sf10_full_board_live_sweep_results.md`, reproduced in 417). Artifact byte
estimates: customer 1,999,516,544 / orders 12,315,016,448 / lineitem 68,489,092,352 (Phase 0
re-verifies). 417 classification: `plans/impl/417_scan_output_accumulator_family_results.md`
(q18 row + follow-on naming).
