# Plan: WAL Recovery and Bootstrap Contract

## Summary
This plan defines the recovery contract for `WendelonLogStore::open()` and the
bootstrap rule for switching from today's in-memory path to durable startup.

The recommended decision is:

- `WendelonLogStore::in_memory()` remains a test-only harness.
- Live node bootstrap must use durable `open()` whenever durable artifacts are
  present.
- Recovery is WAL-first: open vote state, load snapshot if present, replay the
  WAL tail after the snapshot boundary, rebuild the in-memory cache, and
  truncate a torn tail back to the last good record.
- Tail corruption is recoverable by truncation; interior corruption is a hard
  startup failure.

## Motivation
The current code already contains most of the durable machinery:

- `src/consensus/raft_storage.rs` defines a WAL-backed `open()`.
- the same module documents checksummed records, checkpoints, and snapshot
  support.
- `src/node.rs` still boots live nodes with `WendelonLogStore::in_memory()`
  because replay is not wired.

That leaves Wendelon in an unstable middle ground: durable structures exist,
but the live bootstrap contract is still effectively ephemeral. This plan
removes that ambiguity.

## Goals
- Define exactly when a node does fresh bootstrap versus durable recovery.
- Define the replay order for vote, snapshot, WAL tail, and checkpoints.
- Define the corruption policy for torn tails versus interior damage.
- Keep the execution wave scoped to recovery correctness, not full compaction
  or backup semantics.

## Non-Goals
- Rolling-upgrade compatibility.
- Backup/export format.
- Snapshot compression or transport chunking.
- Reworking the higher-level cluster join flow beyond what bootstrap needs.

## Must Settle
1. **Fresh bootstrap vs recovery**
   A node recovers if durable artifacts exist. Fresh bootstrap is only valid
   when no WAL, vote, or snapshot artifacts are present, or when an explicit
   operator wipe/rebootstrap action has occurred.

   Mixed artifact cases are treated explicitly:
   - vote only: recover vote state, start with empty log/state machine
   - WAL only: recover from WAL with empty vote/snapshot defaults
   - snapshot only: load snapshot and proceed without WAL replay
   - WAL + vote, no snapshot: recover vote, then replay WAL from the beginning
   - any artifact set that fails structural validation: hard-fail startup with a
     clear operator-visible error rather than silently rebootstrap

2. **Replay order**
   The recovery order is:
   - load vote state
   - load snapshot if present
   - replay the WAL stream after the snapshot boundary, honoring embedded
     checkpoint records for `commit_index` / `e_safe_index`
   - rebuild the in-memory entry cache and state-machine image

3. **Corruption policy**
   - truncated or torn tail at EOF: recover by truncating to the last verified
     record
   - checksum mismatch in the middle of the log: fail startup loudly

4. **Checkpoint contract**
   Checkpoint records remain part of the WAL format for `commit_index` and
   `e_safe_index`. Recovery must clamp `e_safe_index <= commit_index`.

5. **Live bootstrap rule**
   `node.rs` must stop using `in_memory()` for live boot. The in-memory store
   remains available only for tests and intentionally ephemeral harnesses.

## Implementation
1. **Finalize artifact layout**
   Keep the current WAL + vote separation. Snapshot loading is supported if a
   snapshot exists; otherwise replay starts from an empty state machine.

2. **Implement replay**
   Teach `WendelonLogStore::open()` to scan verified records in order, rebuild
   `entries`, `last_log_id`, `last_purged`, and any checkpointed monotonic
   counters.

3. **Wire recovery into live bootstrap**
   Replace the current `in_memory()` live path in `src/node.rs` with durable
   open-and-recover behavior.

4. **Reserve strict failure modes**
   Tail truncation is allowed; interior corruption is not.

5. **Test restart semantics**
   Add recovery tests that exercise normal restart, torn tail, checkpointed
   restart, and snapshot-plus-tail replay once snapshot artifacts are present.

## Risks and Mitigations
- **Risk: scope creep into full snapshot lifecycle**
  - **Mitigation:** keep M1 focused on loading existing snapshots if present,
    not on redesigning compaction or snapshot distribution.

- **Risk: legacy dev directories with incompatible artifacts**
  - **Mitigation:** fail fast with a clear operator-visible error rather than
    silently recovering into an undefined state. Fresh rebootstrap remains an
    explicit operator action.

- **Risk: bootstrap ambiguity during later multi-node work**
  - **Mitigation:** make the local rule artifact-based now. Workstream E can
    layer cluster-join logic on top of a clear local recovery contract.

## Timeline
- M0: approve this contract.
- Workstream A.2: implement replay and live durable bootstrap.
- Workstream E: extend the same contract into config-driven multi-node join.

## References
- `plans/gap/codex_execution_20260415.md`
- `plans/gap/codex_execution_workstream_A_20260415.md`
- `plans/consensus/raft-core.md`
- `src/consensus/raft_storage.rs`
- `src/node.rs`
