# ADR 410 - q10 reoriented build accumulator completion (post-409 fast wall)

## Context

409 completed q03 sf=10 (time/no-row -> ~19 s pass, one lineitem pass) and its sibling sweep converted the remaining
heavy cells from opaque six-minute timeouts into **measured, named walls**:

| cell | post-409 state |
|---|---|
| q10 | **fails in 7.6 s**: `memory_budget_exhausted: reoriented join build accumulator`, requested 1,776,981,960 / available 1,073,741,824 |
| q12 | fails in 9.5 s: `streamed inner join build spill resident state`, 1,073,809,552 / 1,073,741,824 (**67,728 bytes over**) |
| q05 | still time/no-row: 44 lineitem scans / 127.2 GB - the deep re-scan loop survives the terminal-stage flip |

410 is **q10-led** - iteration economics plus mechanism clarity: q10 is the fast probe (7.6 s) into the next completion
seam, and its wall is the same accumulator class q08 hit before 403. **q12 is assessed in Phase 0 and folded in only if
it is proven a small accounting/eviction correction** (it is a *different* producer - the spill resident-state check -
and must not be smuggled in otherwise). **q05 is deferred to 411**: its loop is reservoir-sized and expensive to
iterate; 410's findings on the accumulator/derived-build seam are exactly the evidence 411 needs.

**Grounded seam:** q10's wall fires at the 403 materialized-fallback sites
(`exec/join/streaming.rs:8770`/`:8786`, in `prepare_reoriented_inner_join_runtime_from_build_batches`): the build is
**executed into batches, concatenated, and then walled** on the hash estimate. Three facts shape the likely fix:
1. at the wall, the build batches **already exist in memory** (~1.78 GB requested - the executor materialized them
   before failing);
2. the **spill machinery lives in the same module** (`SpilledInnerJoinBuild`, the writer behind q19's working ~51 MB
   spill and 403's `Spilled` variant);
3. 403 explicitly named "intermediate/derived join-build spilling" as the deferred (B) case, and 409's flip only covers
   terminal stages whose *other* side fits.

So the likely bounded completion is: **route an over-cap materialized (derived) build into the existing spill writer
instead of the in-memory accumulator** - a 403-style routing fix reusing proven machinery, not a new mechanism. Phase 0
verifies this instead of assuming it.

The code already gives this cut a narrow seam: `SpilledInnerJoinBuild::write_batch_chunks` accepts `RecordBatch`
input, and `ReorientedInnerJoinBuild::Spilled` is already probed by `probe_reoriented_inner_join_streaming`. The
load-bearing question is therefore not "can a spill writer write batches?" but **can the over-cap `build_batches` be
written page-by-page before `concat_or_empty` creates the giant build batch**, with the existing spilled probe path
preserving schema/key/projection semantics. A concat-first spill is rejected: it preserves the resident spike that
caused the wall.

## Decision

### Phase 0 - classify both walls (fast; everything fails in seconds)
1. **q10's failing stage:** probe with the 408/409 hooks (`TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH`,
   `TORON_JOIN_BUILD_PROFILE_PATH`) + join telemetry. Identify which join/replay walls, whether its build is derived or
   scan-shaped, terminal or non-terminal, and why 409's flip did not absorb it (non-terminal? other side unfittable?
   metadata below the 8x shortcut?). Record requested-vs-cap and what the batches contain.
2. **Spill-writer reuse check:** confirm the narrow plumbing for already-materialized batches: write each batch through
   `SpilledInnerJoinBuild::write_batch_chunks` before `concat_or_empty`, derive the same `JoinKeyPlan` from the build
   schema/probe schema, and reuse the existing `ReorientedInnerJoinBuild::Spilled` probe arm. Record any schema,
   projection, or `probe_columns_first` gap. Existing writer + existing probe arm + no concat-first resident spike =
   bounded fix; new probe machinery or reservoir-shaped requirements = stop-line evidence.
3. **q12's 66 KB overshoot:** read the resident-state check (`exec/join/spill.rs:940`) + its eviction policy. Classify:
   accounting error (bytes counted that eviction cannot shed), eviction-ordering (check fires before eviction runs), or
   a genuine budget shortfall. **Fold q12 into 410 only if this is a small accounting/eviction correction;** otherwise
   name it out as its own cut.

Record all classifications before any fix.

### The cut - bounded completion, or the stop-line
- **Bounded fix (preferred):** generic routing at the named seam - an over-cap materialized build takes the existing
  spill path (or an equally bounded, fenced correction Phase 0 names) instead of walling. No q10/table branch; the
  fence is size/shape-based; below-cap behavior byte-unchanged. If q12's edge is Phase-0-trivial, its accounting/
  eviction correction rides along, separately tested.
- **Stop-line (explicit, per the operator):** **if q10's real fix is the one-pass partition reservoir, do not
  half-build it in 410.** Seal 410 as diagnostic/fork evidence and make **411 the reservoir cut** (which then also
  carries q05). Partial reservoir machinery smuggled into a routing ADR is rejected.

## Acceptance
- **Phase 0 recorded:** q10's failing stage classification (derived/scan, terminal/non-terminal, why-not-409),
  spill-writer reuse verdict, q12's overshoot classification - before code.
- **Primary: q10 sf=10 moves off the accumulator wall** under the 398 scope: persisted correctness `pass@compare`, **or
  a named + reproduced next wall** (it was time/no-row pre-409 - a next wall behind this one is a possible honest
  outcome). Mechanism proof via the progress/telemetry hooks (the spilled build exists; no accumulator wall).
- **q12 (only if folded):** same reach-board honesty - pass@compare or named + reproduced next wall behind the 66 KB
  edge; its correction separately tested (accounting/eviction), never blended into q10's routing change.
- **Won ground unregressed:** q03 sf=10 still passes (~19 s band, one lineitem pass); q03 sf=1 four-way within band
  (fingerprint `666e0ac028036f70`, spill 0); q04/q08/q19 sf=10 green - **q08 especially** (same accumulator seam fixed
  by 403; the new routing must not disturb its streamed path), q19's working spill byte-sane.
- **No concat-first spill:** q10's over-cap build batches are written to spill pages before a single giant build batch is
  materialized; the fix must remove the accumulator resident spike, not merely move the error label.
- **Measured discipline:** routing keys on measured/actual bytes and plan shape; no planner cardinality; **no cap bump**
  (the 66 KB edge is not fixed by raising the budget).
- **Full engine gate:** `--no-fail-fast` both crates + architecture_test after fmt; de-branded; no committed
  `.perf.data`.
- **Scope honesty:** q05 explicitly deferred to 411; if the stop-line fires, 410's diagnostic + fork evidence is the
  accepted outcome and 411 is the reservoir.

## Guardrails
- **Reuse the proven spill machinery** - no second spill path; if the writer cannot be reused boundedly, that is
  stop-line evidence, not license to build new machinery here.
- **No concat-first spill** - do not concatenate the over-cap build just to feed a spill writer; write batch chunks
  directly or stop.
- **q12 fold-in bar** - a small accounting/eviction correction only; a different mechanism goes to its own cut.
- **Stop-line over half-reservoir** - reservoir-shaped work seals as diagnostic; 411 owns it.
- **Fenced + generic** - size/shape-keyed routing; below-cap and scan-shaped paths byte-unchanged; no qNN branch.
- **Won ground is a control** - q03 (both scales), q04/q08/q19.

## Risks
- **The fix is reservoir-shaped** -> stop-line; 410 seals as evidence, 411 executes (operator-mandated).
- **Spilled derived build needs probe-side machinery the scan-fed spill lacks** -> Phase 0 step 2 surfaces it; bounded
  only if the gap is narrow.
- **A next wall behind q10's accumulator** (it was time/no-row pre-409) -> reach-board honesty: name + reproduce.
- **q12's edge hides a mechanism** -> fold-in bar keeps it out; named as its own cut.
- **q08/q19 regression** (shared seam/machinery) -> controls + full gate.
- **Spill volume explosion** (a 1.78 GB derived build spills fine, but the probe side re-reads could reintroduce
  re-scan pathology) -> the progress hooks quantify total I/O; acceptance requires the mechanism proof, not just a pass.

## Forward Arc
410 landing q10 (+q12 if trivial) leaves **q05 as the only remaining >360 s cell**, and 411 - the one-pass partition
reservoir / non-terminal flip extension, now scoped by 408/409/410 evidence - closes the heavy-cell board. The article
arc gets its third act: opaque timeouts -> named walls -> completions. Follow-ons after 411: q03's remaining
orders/customer re-scans, q08 execution speed, the decode residual.

## Grounding evidence (file:line)
q10 wall sites: `exec/join/streaming.rs:8770`/`:8786`
(`budget_exhausted(Memory, "reoriented join build accumulator", ...)` in
`prepare_reoriented_inner_join_runtime_from_build_batches` - build batches materialized before the wall; the current
code then `concat_or_empty`s them before the accumulator check). q12 wall:
`exec/join/spill.rs:940` (`streamed inner join build spill resident state`; also `:291` writer resident state,
exec.rs:4160 producer test). Spill machinery: `SpilledInnerJoinBuild::write_batch_chunks` (`exec/join/spill.rs`),
`ReorientedInnerJoinBuild::Spilled` probe arm (`exec/join/streaming.rs`), q19 working spill
(~51 MB, 398/405/409 controls). Sibling numbers: `plans/impl/409_adaptive_derived_side_build_results.md` (q10 7.633 s /
1,776,981,960 / 1,073,741,824, peak 1,894,288 KiB; q12 9.466 s / 1,073,809,552 / 1,073,741,824, peak 2,156,392 KiB; q05
44 scans / 127.2 GB). 403 (B) derived-build spilling named:
`plans/impl/403_q08_reoriented_build_accumulator_streaming_results.md`. 409 flip terminal-only + 8x shortcut:
`plans/impl/409_adaptive_derived_side_build_results.md`. Hooks: `TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH`,
`TORON_JOIN_BUILD_PROFILE_PATH` (408).
