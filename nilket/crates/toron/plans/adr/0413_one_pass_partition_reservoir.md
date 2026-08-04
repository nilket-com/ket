# ADR 413 - One-pass partition reservoir (q05 primary, q12 co-driver)

## Context

The sf=10 board after 412: q03/q04/q08/q10/q19 green; two walls remain, and they point at **the same mechanism**:

- **q05** - the last time/re-scan pathology: ~44 lineitem scans / 127.2 GB in the bounded window (409 sibling probe).
- **q12** - the spilled build's **resident row index alone** exceeds the retained cap
  (`resident_index_bytes=1,074,377,480 / 1,073,741,824`, 412's component payload, reproduced).

**The grounded seam - the re-scan loop is one hoist away from a reservoir.** In
`try_load_resident_partitioned_streamed_inner_scan_build` (`exec/join/streaming.rs:9410`), the group loop
**re-executes the full source scan once per group** (`:9443`: `for group in 0..group_count { ...
write_partitioned_streaming_scan_inner_build_batches(..., scan_input, ...) }`), creating spill writers only for the
active group's partition range and discarding out-of-range rows - scans x groups = the 44-pass/127 GB loop. The
one-pass reservoir is architecturally a **hoist**: create `SpilledInnerJoinBuild` writers for **all** partitions up
front, scan the source **once**, route every row to its partition's writer, then iterate groups loading from the
already-written reservoir. Everything downstream - `LoadedSpilledInnerBuild`, per-group residency accounting, the
active-group probe (`partition_streaming_join_batch_active_group_with_buckets`) - is the machinery **q08 already
completes through**, unchanged. This is why the stop-line is unlikely to fire: per-group probing with correct output
semantics exists in production; 413 changes the *build-fill source* (re-scan -> reservoir read), not the probe/output
contract.

**q12 as co-driver:** a reservoir/active-group design shards the row index - only the active group's index is
resident (~total/groups), which is exactly the mechanism q12's ~1 GiB index needs. Phase 0 confirms q12's route and
per-group index arithmetic; if one partition's index alone exceeds the cap, that is a **named skew floor**, not a
silent miss.

**New resident cost to account:** the one-pass write holds all partitions' writer scratch simultaneously
(vs one group's today) - order of `partition_count x page scratch` (64 KiB pages); small, but it must be measured and
guarded, not assumed (412's writer-scratch lesson).

## Decision

### Phase 0 - measure both drivers + confirm the seam (before code)
1. **q05's exact re-scan stage:** probe with the 408/412 hooks (scan progress + component payload). Identify the
   join/stage in the group loop, its `partition_count`/`group_count`, per-pass scan volume, and confirm the loop at
   `:9443` is the producer of the 44 passes. Record why it is non-terminal/unfittable for the 409 flip.
2. **q12's partitioning/index arithmetic:** confirm q12's failing route (its `site=write` index growth vs the
   partitioned-group path); measure/estimate **per-active-group index bytes** under the reservoir (total index /
   groups, plus skew check). If the max partition's index alone exceeds the cap -> the skew floor is named up front.
3. **Reservoir residency math:** writer-scratch bytes x `partition_count` for the one-pass write, against the
   retained cap; the reservoir must fit its own write phase under the existing guards (412's per-site payload makes
   this measurable).

### The cut - hoist the scan, reuse everything else
- **One-pass reservoir** at the `try_load_resident_partitioned_streamed_inner_scan_build` family (and the non-resident
  sibling path if Phase 0 shows q05/q12 route there): writers for all partitions; one source pass routing rows by
  partition key; per-group `LoadedSpilledInnerBuild` loads from the reservoir instead of re-scanning; active-group
  probe unchanged. Reuse `SpilledInnerJoinBuild` pages/writers and the existing residency guards - **no second spill
  subsystem**.
- **Index sharding falls out:** each group's index is built at group load (existing behavior) - only the active
  group's index resident. q12's route joins the sharded path per Phase 0's confirmation.
- Generic: size/shape-keyed at the existing seam; no qNN/table branch; no cap/multiplier/scope change.

### Stop-line (operator-mandated)
If preserving **probe/output order** requires a broader execution-model rewrite - i.e. the reservoir cannot reuse the
existing active-group probe semantics and would need to reorder output beyond what q08's path already does - **seal
413 as the reservoir design/spec with proof**; do not half-build an unsafe reorder. (Grounding says this is unlikely:
the probe contract is untouched by the hoist - but the plan proves it, not assumes it.)

## Acceptance
- **Phase 0 recorded:** q05 stage identification (+ why-not-409), q12 route + per-group index arithmetic (skew floor
  named if present), reservoir write-phase residency math - before code.
- **q05 primary:** lineitem/orders/customer scan counts collapse from repeated full scans to **one reservoir pass plus
  bounded group reads** (progress-hook proof: scan starts ~1 per source), and q05 sf=10 completes
  (`pass@compare`) **or advances to a named + reproduced next wall**.
- **q12 co-driver:** moves off `resident_index_bytes > cap` (pass@compare or named + reproduced next wall), **or** the
  skew floor is named with per-partition evidence (one partition's index alone exceeds the cap).
- **Mechanism proof, not just timing:** the 408-family hooks show the scan-count collapse and the per-group resident
  index staying under cap (412's component payload).
- **Controls:** q03/q04/q08/q10/q19 sf=10 pass@compare - **q08 especially** (it lives on this exact path today and
  must not regress); q19 spill byte-sane; q03 sf=1 four-way in band (fp `666e0ac028036f70`, spill 0).
- **No cap/multiplier/scope bump**; correctness via persisted sf=10 pass rows for any greened cell.
- **Full engine gate:** `--no-fail-fast` both crates + architecture_test after fmt; de-branded; no committed
  `.perf.data`.
- **Scope honesty:** the reservoir bounds the *known* sf=10 memory/time pathologies by design; any residual (skew
  floor, next wall) is named; execution speed of completed cells stays a separate thread.

## Guardrails
- **Hoist, don't rebuild** - reuse `SpilledInnerJoinBuild` writers/pages, `LoadedSpilledInnerBuild`, group residency
  guards, and the active-group probe; a second spill subsystem is rejected.
- **Probe/output contract untouched** - the reservoir changes where group builds come from, not how groups probe or
  emit; if that cannot hold, the stop-line fires.
- **Write-phase residency measured** - all-partition writer scratch is guarded under the existing caps (412 payload),
  not assumed small.
- **Skew floor named, not silently missed** - per-partition index arithmetic in Phase 0 and in the results doc.
- **No cap bump; won ground is a control** - q08 (same path) and q19 (spill sentinel) are the sharpest tripwires.

## Risks
- **Skew floor** (one partition's index/build alone over cap) -> named residual with evidence; a deeper
  repartition/recursive split is a future mechanism, not smuggled in.
- **Write-phase scratch x partitions over cap** -> Phase 0 math first; page-size/flush policy within existing guards
  if needed; else named.
- **q08 regression** (shares the path) -> control + the existing tests; the hoist must leave the fits-resident and
  small-build routes byte-unchanged where they already work.
- **A next wall behind q05's loop** (it was time/no-row; walls behind walls) -> reach-board honesty.
- **Order contract surprise** -> stop-line: design/spec with proof, per the operator.

## Forward Arc
413 landing changes the story from "two last walls" to **"all known sf=10 memory/time pathologies bounded by
design"** - the atlas cells from 398 either green or named-with-evidence (skew floor). The named backlog then is
execution speed (q08/q10/q05 completions are slow), external sort, decode residual, and the article's arc closes:
opaque timeouts -> named walls -> completions -> a bounded-by-design engine. 412's component payload and 408's
progress hooks remain the permanent diagnosability dividend.

## Grounding evidence (file:line)
Re-scan loop: `exec/join/streaming.rs:9410` (`try_load_resident_partitioned_streamed_inner_scan_build`), `:9443`
(`for group in 0..group_count` -> per-group `write_partitioned_streaming_scan_inner_build_batches(..., scan_input,
...)` = full source scan per group), `:9433` (resident-floor gate). Existing per-group machinery:
`LoadedSpilledInnerBuild` (`:1175`), active-group probe (`:289`, used at `:4113`), q08 completes through this path
(403/409/410 controls). q05: 44 scans / 127.2 GB (409 results). q12: `resident_index_bytes=1,074,377,480 /
1,073,741,824`, `site=write`, reproduced (412 results). Writer/page machinery: `SpilledInnerJoinBuild`,
`JOIN_SPILL_PAGE_SIZE = 64 KiB` (`spill.rs:4`). Hooks: scan progress (408), component payload (412).
