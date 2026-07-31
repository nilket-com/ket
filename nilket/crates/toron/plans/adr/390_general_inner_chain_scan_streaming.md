# ADR 390 - general inner-chain stage-build scan streaming (q03/q05/q10)

## Status
Proposed.

## Context

388/389 proved q03/q05/q10 are the same structural problem at **deeper scan edges** in the reoriented-inner-chain. But
**the all-stage loop already exists**: `extract_reoriented_inner_chain` yields `(probe, stages)` and the per-stage
memory-aware gate `reoriented_inner_stage_scan_build_exceeds_scan_output_cap` (streaming.rs:4352) already loops. So **390
is NOT "add the loop"** - it is **diagnose why the existing all-stage loop declines q03/q05/q10, then widen only that
gate**.

The likely decline point: the per-stage gate requires both `decompose_streaming_scan_probe_input(stage.build)` (4357)
**and** the **global** base recognizer `reoriented_inner_chain_grouped_base_scan_probe_mode` (757) classifying the base
as FilteredJoin0/BareJoin0 (else None -> the whole streamed path is gated off). **Expected diagnosis: q03/q05/q10 are
blocked by the base-mode/global recognizer, not by absent per-stage machinery.** Scope: the q03/q05/q10 inner-join-chain
family only. Success = **all eligible stage build-scan producers gone or residual named** - not "all scans streamed".

## Decision

### Phase 0 - diagnose the decline (per-query producer table, before code)
For q03/q05/q10, per chain stage:

| query | stage | base_scan_probe_mode (FilteredJoin0/BareJoin0/None) | stage.build decomposes? | scan-output estimate vs cap | base traversal / probe API | runtime today (InMemory/Spilled/PartitionedScan) | reason not streamed |
| --- | --- | --- | --- | --- | --- | --- | --- |

**reason not streamed** is one of: **base-mode gate** (global recognizer None/declined), **undecomposable scan edge**
(`decompose_streaming_scan_probe_input` returns None), **estimate under cap** (memory-aware gate chose materialized),
**consumer mismatch** (the stage consumer isn't the streamed shape). The base traversal / probe API column is
load-bearing: if the only reachable traversal is the fallback `try_visit_streaming_reoriented_inner_chain_base` path,
which calls the synchronous `probe_reoriented_inner_join`, then a non-in-memory stage runtime cannot be consumed there
and must be classified as consumer mismatch unless 390 also wires that traversal to `probe_reoriented_inner_join_streaming`.
This table names exactly what to widen.

### The cut - widen the declining gate, shape-fenced
- **If blocked by the base-mode/global recognizer (expected):** **relax the base-mode dependency for stage-build
  streaming when the stage itself is eligible** - i.e. a stage whose `stage.build` decomposes and whose scan-output
  estimate exceeds the cap may stream **even if the global base mode is not FilteredJoin0/BareJoin0**. Still **shape-
  fenced to the reoriented-inner-chain** (not a general scan-streaming recognizer).
- **Do not strand streamed runtimes behind a synchronous consumer.** The widened path must route covered non-in-memory
  stage runtimes through a traversal that calls `probe_reoriented_inner_join_streaming`; otherwise the edge is a named
  consumer-mismatch residual. This avoids a false implementation that prepares streamed stage builds and then falls back
  to the generic materialized path.
- **If an undecomposable edge or consumer mismatch:** name it residual; cover only edges the chain mechanism handles.
- **Reuse the 388 memory-aware gate per edge** - each edge streams only when memory-bound; sf=1 stays materialized.
- Topology-named; no qNN.

### Scope correction - stage BUILD scans, not the probe by default
**Stage build scans are the target.** Base/probe streaming has **separate semantics** and currently materializes one
side in places. The probe scan is **enumerated in Phase 0** but **implemented only if the table proves it is the
remaining scan-output producer AND the existing base streaming path covers it cleanly** - otherwise named residual.

## Acceptance
- **Phase 0 producer table recorded** with the decline reason per stage (base-mode / undecomposable / under-cap /
  consumer-mismatch).
- **All eligible stage build-scan producers gone OR residual named** for q03/q05/q10 - the repeated scan-output cycle
  broken for the chain-stage build edges the cut covers; classify each cell live (green / advanced to named downstream /
  residual).
- **No stranded streamed runtime:** every covered non-in-memory stage runtime is consumed via
  `probe_reoriented_inner_join_streaming`; if the only available traversal uses synchronous probing, it is named residual
  instead of silently falling back.
- **Persisted sf=10 correctness for any pass row.**
- **sf=1 no-regression control** - the memory-aware gate keeps small-scale materialized; a sf=1 control proves no
  per-edge streamed-path regression.
- **Small-scale strict/dev parity** per widened edge (column order, residual/filter, multi-stage join + group);
  ScanRequest parity (no dropped fields).
- **Probe scan only if Phase 0 proves it the remaining producer + cleanly covered**; else named residual.
- **No cap bump; `--no-fail-fast` + `toron-tpch --no-fail-fast`**; de-branded; prior greens + q08/q12/q19 unchanged.

## Guardrails
- **Diagnose the decline, then widen only that gate** - not "add the loop" (it exists); the Phase-0 table names what to
  widen.
- **Relax the base-mode dependency for eligible stages, shape-fenced** to the reoriented-inner-chain; no general
  recognizer.
- **Stage build scans are the target; probe only if proven + cleanly covered** (separate semantics).
- **Memory-aware gate per edge; deterministic order; ScanRequest parity; no cap bump; full engine gates; no qNN.**
- **Success = eligible stage-build producers gone or residual named**, not all scans streamed.

## Risks
- **Relaxing base-mode over-matches** - keep it shape-fenced (stage eligibility: decomposes + estimate over cap +
  reoriented-inner-chain context); a fence test confirms non-chain shapes stay materialized.
- **Some edge undecomposable / consumer-mismatch** - named residual, not forced.
- **Probe streaming pulled in prematurely** - gated behind the Phase-0 proof.
- **Small-scale perf regression** - memory-aware gate + sf=1 control per edge.
- **Wall-move to a downstream producer** - successful; rerank names it.

## Forward Arc
390 breaks the q03/q05/q10 stage-build scan-output cycle, then **rerank**: if they land on time, a cleaner time/perf
board; if a non-chain edge or downstream producer remains, that named residual is the real next target. Then the
perf/Rune arc the foundation-proof has earned.

## Grounding evidence (file:line)
loop exists: `extract_reoriented_inner_chain` streaming.rs:337, per-stage gate
`reoriented_inner_stage_scan_build_exceeds_scan_output_cap` 4352 (`decompose_streaming_scan_probe_input(stage.build)`
4357). global base gate: `reoriented_inner_chain_grouped_base_scan_probe_mode` 757 (FilteredJoin0/BareJoin0/None). 388:
the matched-stage streaming + memory-aware gate. q03/q05/q10 DuckDB 269/207/781ms.
