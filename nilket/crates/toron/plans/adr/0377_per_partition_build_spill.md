# ADR 377 - per-partition build spill (eliminate the whole-build re-scan, q07 canary)

## Context

The waste-removal before parallelism. After 374/375/376 peeled q07's accounting + encode + bucket churn, the **dominant
residual is the whole-build-per-partition re-scan**: `probe_partitioned_streamed_inner_scan_build` (streaming.rs:6135)
loops `for partition in 0..partition_count` (6163) and **re-scans the build source once per partition** -
**O(build x partitions)**. 376 removed the per-pass bucket re-encode; the re-scan of the build *data* remains.
Parallelism would spread that waste over cores; per-partition spill writes remove it. Diagnostic-first; q07 canary;
**parallelism stays next** after the cleaned profile.

## Decision

### Phase 0 - map BOTH sides (before code)
1. **Build side:** confirm the whole-build re-scan per partition (the 6135/6163 path), not bucket churn (the q07
   post-376 profile).
2. **Probe side:** confirm whether it is **held once**, **streamed once**, or **also re-scanned per partition**.
   - If the probe is **also re-scanned**, 377 must **either route probe partitions too** (same one-pass spill) **or
     narrow its claim to build-only and name the expected probe residual** - not silently leave a probe re-scan.
3. **Partition count `N`:** bound it. Per-partition spill files are fine **only if** resident stays bounded to the
   active partition + small writer buffers; **open-file/file-handle count must be named and bounded**. If `N` can reach
   high values, use **lazy writers or a small writer pool** (not "N active writers forever") unless Phase 0 proves `N`
   is small enough.
4. **Spill-byte peak:** the old path writes/probes/cleans one partition at a time; a one-pass router may have **all
   partition spill bytes present at once**. Phase 0 must measure/predict the peak spill reservation. If all-partition
   spill exceeds the unchanged per-query spill budget, 377 must either use a bounded partition-group strategy or report
   a named spill-volume wall - not silently raise or bypass the spill budget.
5. **Routing surface:** record exactly where routing occurs in the old path: after scan filtering and any exchange
   repartitioning, before retained-column projection. The router must use the same `partition_build_keys` over the same
   row stream; `write_build_keys` / projected ordinals are for spill materialization, not bucket routing.

### The cut
- **One build pass** -> route each row by its (376-precomputed) bucket into its partition's spill (per-partition files,
  or one file with per-partition extents), at the same post-filter/post-exchange point where the old selector ran.
  **Read only the active partition once** during processing -> O(build) total instead of O(build x partitions).
- **One-partition-resident memory bound preserved** - exactly one partition's index/spill resident; only file handles +
  small write buffers added, bounded per the Phase-0 `N` decision.
- **Spill budget preserved** - total/peak spill reservation remains under the existing budget, uses a bounded
  partition-group strategy, or is reported as the next named wall. No cap bump.
- Probe handled per Phase 0 (routed too, or build-only with named residual).
- **No broad spill abstraction** unless Phase 0 proves a narrow helper would be worse. Scope: the streamed inner
  scan-build partitioned path; not parallelism, q17, q21. Topology-named; no qNN in core.

## Acceptance
- **Correctness - row order preserved:** the per-partition route order within each partition **matches the old
  select-per-partition scan order** (stable routing), so join output is order-identical.
- **Correctness - routing/key parity:** routing uses the same post-filter/post-exchange rows and `partition_build_keys`
  as the old selector; retained-column projection / `write_build_keys` must not change bucket assignment.
- **Correctness - join output parity:** materialized/re-scan vs per-partition-spill compared for the **q07-shaped inner
  chain** (multi-batch, multi-partition, null/residual keys); **q07 stays `pass@compare`** (strict small-scale +
  persisted sf=10 if reachable).
- **Profile gate:** re-profile q07 (373 method) - the **whole-build re-scan / bucket residual materially drops**
  (O(partitions) -> O(1) build passes), and the **next hot stack is named**. Green incidental.
- **Memory bound preserved:** one-partition-resident; **file-handle count named and bounded** (lazy/pool if `N` large);
  no cap bump.
- **Spill budget preserved:** unchanged spill-byte budget; if all-partition spill raises peak reservation beyond budget,
  it is either bounded by design or named as a wall.
- **Coverage (measured, not promised):** q07 canary; q03/q05/q10/q12 are **measured** for family impact - reported as
  coverage, not a promised green.
- **No parallelism; no cap bump; no broad spill abstraction (unless proven better); `--no-fail-fast` required;**
  de-branded; prior greens unchanged.

## Guardrails
- **Map both sides** - build re-scan confirmed; probe held/streamed/re-scanned named; if probe re-scanned, route it or
  narrow + name the residual (no silent probe re-scan).
- **Row order is a correctness gate** - stable per-partition route order == old scan order; join output order-identical.
- **Bounded resident + bounded file handles** - one partition resident; lazy/pool writers if `N` large; no cap bump.
- **Spill-byte peak is a gate** - one-pass partition files must not turn a memory perf cut into a silent spill-budget
  bypass or cap bump.
- **Routing happens at the same surface** - post-filter/post-exchange, pre-projection, same key ordinals.
- **Parity vs re-scan** is the correctness gate (q07-shaped inner chain).
- **Coverage measured, not promised; no parallelism; no broad abstraction unless proven; --no-fail-fast; no qNN.**

## Risks
- **Probe re-scan left silent** - Phase 0 maps it; route or narrow+name, never silent.
- **File-handle blow-up at high `N`** - named + bounded (lazy/pool); proven-small or pooled.
- **Spill-byte budget regression** - all partition files resident on disk may exceed the prior one-partition peak;
  measure it and either bound by design or report the named wall.
- **Wrong key surface** - using projected write ordinals for routing would move rows to wrong partitions; routing/key
  parity gates it.
- **Row-order drift** - the per-partition route must preserve scan order; the order-parity test gates it.
- **Broad-abstraction creep** - kept narrow unless Phase 0 proves a shared helper is better.
- **Regression** - `--no-fail-fast` required.

## Forward arc
377 (per-partition build spill, re-scan removed) -> **reassess parallelism** from the cleaned q07 profile: with the
re-scan waste gone, the residual is real spill/read/write/Arrow work and **parallelism (the deferred one-thread
systemic, 373) is the next lever**. q17 (grouped fold + spill-read), q19 (spill-read I/O), q21 (712-byte resident-state)
remain queued; q03/q05/q10/q12 may green or advance per 377's measured impact.
