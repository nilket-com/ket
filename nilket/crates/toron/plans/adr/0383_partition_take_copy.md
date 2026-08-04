# ADR 383 - q07 partition/take/copy (378 G-pass re-work, diagnostic-first)

## Context

ADR 381 named q07's dominant frames: `__memmove` (16.50%) + `partition_bucket_with_scratch` +
`partition_bucket_if_non_null_with_scratch` (12.0%) + `arrow_select::take::take_bytes` (5.83%) - **partition/take/copy**
(~34%). This is the **378 G-pass re-work**, not 376's within-group recompute (the scan path already precomputes buckets).
Two code-grounded wastes:
- **All-P take then filter:** `probe_partitioned_streamed_inner_scan_build` (streaming.rs:3696/3703) **takes all P
  partitions per batch** via `partition_streaming_join_batch_with_buckets`, then `continue`s past partitions outside the
  active group `[partition_start, partition_end)` (3706). So it does **G x P** takes when it needs **P**.
- **G x bucket precompute:** the per-row bucket precompute runs once per group pass (the build re-scanned per group).

Reduce the reducible re-work **without re-introducing the 378 spill peak**. q07 is the canary, but the active-group
helper is shared with the partitioned streamed-inner family, so q03/q05/q10 are explicit controls. No cap bump.

## Decision

### Phase 0 (diagnostic-first) - measure the re-work
Record for q07 at sf=10: **G** (group count), **P** (partition count), **rows scanned per group**,
**bucket-computation count**, **partition-take count** - before, and projected after each lever. Plus, for the bucket
cache: **rows x bucket_repr_bytes** (the cache's resident cost) and a **scan-order-stability** check across the G passes.

### Lever 1 (first, clean, spill-peak-safe) - active-group-only partition selection
Take/build **only the partitions in `[partition_start, partition_end)`**, not all P then filter. A variant of
`partition_streaming_join_batch_with_buckets` that skips rows whose bucket is outside the active group **before** the
`take`. This cuts partition-take from **G x P -> P** total, directly attacking `take_bytes`/`__memmove`, and **cannot
re-introduce the spill peak** (it takes strictly fewer partitions). No scan-order/cache dependency.

### Lever 2 (second, resident-gated) - bucket-array cache across G passes
Only if Phase 0 proves the scan order is **stable across the G passes**: compute each row's bucket once, reuse across
the G build-re-scans -> bucket precompute **G x rows -> rows**. **Do not call it "small" until measured:** budget the
cache as **partition-bucket-cache resident** (`rows x bucket_repr_bytes`), require a **compact representation**, and if
it does not fit safely use a disk/sidecar design **or name the stop-line**. Holding the build to skip the re-scan is the
**stop-line** (re-introduces the 378 peak) - named, not shipped.

### The cut
Lever 1 first (active-group-only take). Lever 2 only if resident-safe + scan-order-stable; else named. Join-output take
(join.rs:907-1034) is inherent materialization - not the target. q07 parity; q03/q05/q10 controls if the shared helper
moves; topology-named; no qNN; no cap bump.

## Acceptance
- **Phase 0 counts recorded** - G, P, rows scanned/group, bucket-computation count, partition-take count, **before and
  after** each shipped lever.
- **Lever 1 shipped** (active-group-only take), with partition-take count dropping ~G x; `take_bytes`/`__memmove` share
  drops materially (re-profile, 381 method).
- **Lever 2 only if measured resident-safe** - `rows x bucket_repr_bytes` budgeted as partition-bucket-cache resident,
  compact repr, **scan-order-stability proven** before cross-pass reuse; else named (sidecar or stop-line).
- **378 no-peak-regression guard:** `spill_bytes` / `writer_peak` / resident-guard **unchanged or better**; **no cap
  bump** (including the bucket cache as a named resident term, not a cap raise).
- **Correctness parity:** q07 strict `pass@compare` (sf=0.01/sf=1) + persisted sf=10 if it advances/greens; partition
  assignment + ordered output identical.
- **Shared-helper controls:** if the implementation changes the shared partitioned streamed-inner helper, q03/q05/q10
  correctness stays green at the oracle scale and their sf=10 wall class is recorded as unchanged, improved, or named.
- **`--no-fail-fast` required**; de-branded; prior greens unchanged.

## Guardrails
- **Active-group-only take is lever 1** - clean, spill-peak-safe (takes fewer); no scan-order dependency.
- **Bucket cache is measured, not "small"** - budget `rows x bucket_repr_bytes`, compact repr, scan-order proof; sidecar
  or stop-line if not resident-safe.
- **Never re-introduce the 378 peak** - holding the build is the stop-line; the cache is a named resident term, not a
  cap bump.
- **Join-output take is inherent** - not the target; name the floor if it dominates after levers 1/2.
- **Diagnostic-first counts; correctness parity; shared-helper controls; --no-fail-fast; de-branded; no qNN.**

## Risks
- **Bucket cache resident blows budget at sf=10** -> measured `rows x bucket_repr_bytes`; compact/sidecar/stop-line.
- **Scan order not stable across passes** -> no cross-pass bucket reuse; lever 1 still stands.
- **Re-introducing the 378 peak** -> lever 1 is strictly-fewer-takes; cache is named resident, not held build.
- **Residual is inherent join-output take** -> name the floor.
- **Shared-helper regression** -> q03/q05/q10 controls plus `--no-fail-fast`.

## Forward arc
383 (q07: active-group-only take + measured bucket cache, or named floor) -> rest of the time cluster (q17/q19/q21) ->
parallelism engagement-policy follow-up (381 side table) -> memory-tail cells. With q03 near its floor (382) and q07's
G-pass re-work reduced here, the partitioned spill/join family's perf is characterized.

## Grounding evidence (file:line)
381: q07 `__memmove` 16.50% / `partition_bucket_*` 12.0% / `take_bytes` 5.83%. all-P-take-then-filter:
`probe_partitioned_streamed_inner_scan_build` streaming.rs:3696 (`precompute_join_partition_buckets` all P) / 3703
(`partition_streaming_join_batch_with_buckets` all P) / 3706 (filter `[partition_start, partition_end)`). bucket
precompute: join.rs:2209. join-output take (inherent): join.rs:907-1034. 378 G-pass re-scan = the spill-peak bound.
q07 DuckDB=210ms.
