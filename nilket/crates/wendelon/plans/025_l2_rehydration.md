# Plan: L2 Rehydration Policy

## Summary
This plan adds a controlled L2 -> L1 rehydration path for hot slabs. A slab that has already been tiered to L2 remains authoritative in the Control Ring, but the local `SlabManager` may materialize a sealed L1 copy again when repeated reads prove the slab is hot enough to justify local residency.

## Motivation
Today a tiered slab is always served from L2 once its L1 file has been unlinked. That is correct but inefficient for repeated reads of the same slab because each lookup pays full remote fetch latency and bandwidth. Workstream D.8 closes that gap without weakening the existing tiering safety rules.

## Goals
- Define an explicit hot-slab trigger instead of promoting on a single ad hoc read.
- Rehydrate by downloading the L2 blob, verifying it, and restoring a sealed local slab file.
- Prevent immediate re-demotion so a burst of reads does not cause slab thrash.
- Keep reads correct while a slab is racing between L2 service and local rehydration.

## Non-Goals
- New transport providers.
- Query spill control.
- Backup or export semantics.
- Changing Control Ring tier metadata during local rehydration. L2 remains the authoritative remote pointer.

## Implementation
### Trigger
- Track per-slab reads that were actually served from L2.
- Promote only after a configurable threshold of consecutive L2-served reads, not after the first miss.
- Reset the counter once the slab is successfully rehydrated into L1.

### Mechanism
- Extend `SlabManagerConfig` with a rehydration policy:
  - `promote_after_l2_reads`
  - `min_local_residency`
- Add an L2 fetch helper that returns shared bytes so the threshold-crossing read can reuse the already-downloaded blob for rehydration instead of issuing a second fetch.
- When the threshold is reached:
  1. Verify the fetched blob against the Control Ring payload hash.
  2. Write it to a temporary local file.
  3. Atomically rename it into the canonical `slab_<id>.wnd` path.
  4. Open it as a sealed `LocalSlabStore` and publish it in a dedicated rehydrated-local map.
- Rehydrated slabs stay outside the ordinary `sealed` set so tombstone compaction and L1 -> L2 demotion logic do not accidentally treat a local cache copy as a new authoritative slab.

### Thrashing Prevention
- Each rehydrated slab receives a `keep_local_until = now + min_local_residency` deadline.
- The background sweeper may reclaim rehydrated local copies only after that deadline expires.
- Until then, the slab remains eligible for local reads even though the Control Ring still says the authoritative cold copy lives in L2.

### Read Safety
- Reads resolve in this order:
  1. active L1 slab
  2. sealed L1 slab
  3. rehydrated local slab
  4. L2 pointer
- A slab is published into the rehydrated-local map only after the full blob is written, renamed, and successfully opened, so no read can observe a half-written local file.
- Reads that race rehydration continue to use the already-authoritative L2 pointer and remain correct.
- If rehydration fails, the read still returns the bytes fetched from L2 and the slab simply remains remote.

## Risks and Mitigations
- **Risk:** repeated concurrent hot reads trigger duplicate downloads.
  - **Mitigation:** keep a per-slab in-flight rehydration guard so only one local install runs at a time.
- **Risk:** a rehydrated slab is reclaimed immediately and starts bouncing between tiers.
  - **Mitigation:** enforce `min_local_residency` in the sweeper before any local cache eviction.
- **Risk:** local cache bytes diverge from the L2 object.
  - **Mitigation:** verify the L2 blob hash before serving or installing it.

## Timeline
- Land the policy and local implementation in Workstream D.8.
- Keep richer heuristics such as adaptive thresholds or byte-budgeted hot-cache eviction for later follow-up.

## References
- `plans/014_lazy_tiering_sweeper.md`
- `plans/adr/006_tiered_storage_lazy_migration.md`
- `plans/gap/codex_execution_workstream_D_20260415.md`
- `src/storage/manager.rs`
- `src/storage/sweeper.rs`
- `src/storage/l2.rs`
