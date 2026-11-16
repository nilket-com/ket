# ADR 0109 - A recovery rebuild takes the current published snapshot

## Status
Accepted for implementation, 2026-11-16 (DC-257). Direction ruled by Codex 01a1e66b, after the REDs of Codex 01a1e65d. Not delivered.

## Context
- **A replacement's handoff judges its start authority on recovered state.** `ControlOnlyNode::hand_over` flushes its checkpoint, then rebuilds with `log_store.recovered_state_machine()` and runs `replacement_start_authority` on the result (control_only_node.rs:323-339).
- **The rebuild used the snapshot loaded at open.**
  - `recovered_state_machine` passed `inner.recovered_snapshot`, the retained entries and the recovery checkpoint to `recover_sm_inner`.
  - `recovered_snapshot` is written only at construction and at open. A snapshot installed, or built and then purged below, later in the same lifetime is recorded by `SnapshotPublication` and the state machine, never there.
- **The replay accepted a missing base.** With no snapshot, `recover_sm_inner` started from a default state and applied every retained entry up to the checkpoint. It never checked that the first one followed a boundary, or that the replay reached the checkpoint. A tail above a runtime snapshot was applied onto an empty base: the right applied index with the prefix missing. Under ParentDecisions the decision state is then empty, so the handoff refused "the instance was never granted".
- **Reproduced** (reviews/dc257-snapshot-rebuild-20261116/RED-01.md):
  - three deterministic REDs: a same-lifetime install; a same-lifetime build plus covered purge; and the replacement's real start authority after an install, refused with that exact error;
  - a no-snapshot control passes;
  - every reopen recovers correctly.
- **Not attributed:** DST pilot 2's own failure; its stores did not survive.
- **What open already guarantees, which the rebuild did not:**
  - `check_retained_log_contiguous`: the first retained entry is the boundary's successor; with no snapshot it is index 0 or 1, unless the store began in the legacy single-file WAL (`legacy_start`, any index); and there are no interior gaps;
  - `resolve_recovered_committed`: the checkpoint's commit index is the boundary or a retained entry.

## Decision

### D1. The base is the current publication, read coherently
`recovered_state_machine` runs four steps. It never holds one lock while taking another:
1. **Under `inner`, released:** what open loaded (`recovered_snapshot`'s full `Option<LogId>`), and the artifact path and file I/O.
2. **Under the publication lock (a leaf), released:** the publication must be **healthy**. Its failure fence refuses even when the cached base is current.
   - **NoArtifact:** valid only if open loaded none.
   - **`Artifact(id)` equal to the cached base** (`Artifact(None)`, an empty-state artifact, is distinct from NoArtifact; the full `LogId`, including index 0, is compared): the cached base is used.
   - **Otherwise:** the current artifact is opened and read under the same lock, as every reader of `current` is (the backup export precedent), with open's validated loader `load_snapshot_artifact`. Its body must hold exactly `id`.
3. **Under `inner`, released:** the retained entries, the checkpoint, the cached base and the open-time `legacy_start`.
4. **Under the publication lock, released:** the publication must still be healthy and **equal** to step 2's. Any change refuses (`Interrupted`), with nothing rebuilt; there is no retry loop.

Both publication origins (received install and local build) publish the same `current` artifact, so both are covered.

### D2. Replay only a contiguous suffix through the checkpoint
This is `recover_sm_inner`, which every recovery shares:
- **The covered prefix is skipped:** entries at or below the snapshot boundary are already in it. A runtime log may still retain them; they are neither replayed nor refused.
- **Above the boundary:**
  - the first replayed entry is the boundary's successor;
  - with no snapshot, open's rule applies: index 0 or 1, or any index for a legacy start, carried from open in `LogStoreInner::legacy_start` and not inferred;
  - each next entry follows the last;
  - replay must reach the checkpoint's commit index whenever that is above the base.
- **Unchanged:** nothing past the checkpoint is applied. With no checkpoint, only the snapshot counts. A checkpoint behind the snapshot is refused.
- **A gap, or a tail short of the checkpoint, refuses** (`InvalidData`, "recovery replay: …"). It is never a state.
- **Successors use checked arithmetic.** A terminal index (`u64::MAX`) needs none. An entry supplied after it refuses rather than wrapping or re-entering the genesis exemption.

### D3. Callers (census)

| caller | when | what it supplies |
|---|---|---|
| node startup (node.rs:2196) | right after open | open-checked inputs; the rebuild equals open's |
| ControlOnly start (control_only_node.rs:104) | right after open | same as node startup |
| `hand_over` (control_only_node.rs:325) | same lifetime | the case this ADR corrects |
| exported-control replay (raft_storage.rs:9976, backup) | — | runs `check_retained_log_contiguous(…, legacy = true)` and `resolve_recovered_committed` itself, and passes `legacy_start = true` |
| direct test constructions (decision_mode_tests.rs; a `from_recovery_with_metrics` test) | — | contiguous entries from 1, or a snapshot plus its successors; they pass `legacy_start = false` |

## Rationale
- **Rejected alternatives:**
  - Trusting the live state machine in `hand_over`, relaxing `replacement_start_authority`, or faking the grant: all would judge authority on something other than durable state.
  - Updating `recovered_snapshot` from the publish path: it needs `inner`'s write lock under a held publication, against the documented order.
- **Why read-twice:** comparing the loaded artifact with one earlier sample is not enough, because publication can move or fail after it. Reading twice makes the captured inputs belong to one healthy publication.
- **Why fail closed:** the replay checks turn a silently wrong state into a refusal, as open already does.

## Consequences
- **A rebuild racing a publication refuses rather than waits.** The caller sees `Interrupted`.
  - `hand_over(self)` does not retry: the daemon (cli.rs) runs it once, prints "handover failed", and exits 1 (`activation_exit`). The consumed ControlOnly handle is gone.
  - The refusal comes before `activate_identity`, so the page stays ControlOnly. But the preparation, Raft and the servers were already stopped and closed.
  - Any later start depends on the existing supervisor or operator policy; no retry mechanism is added.
- **When the cached base is not current,** a rebuild reads the artifact under the publication lock, briefly blocking publishers. This happens only on that path.
- **A store whose retained tail does not reach its checkpoint** now refuses a same-lifetime rebuild where it used to produce a partial state. Open already refused such a store.

## Validation (focused; Codex 01a1e66b)
- **The four DC-257 tests turn GREEN,** with both reopen controls.
- **New controls:**
  - a publication moved between the two observations (a deterministic test hold) refuses, and the next rebuild takes the new base;
  - a failed publication refuses, with a usable cached base and when set during the hold;
  - replay: the covered entry is not re-applied; a boundary gap, an interior gap and a tail short of the checkpoint refuse; snapshot-only recovery works with or without a boundary checkpoint; nothing applies without a checkpoint or past it; a checkpoint behind refuses; no-snapshot genesis versus legacy start; an empty-state artifact as the base.
- **Existing families, once:** `consensus::raft_storage` (publication, install, build, purge, recovery, checkpoint, legacy, zero-coordinate), the replacement start authority, the in-process replacement, activation and ControlOnly cluster tests, and the ownership and backup families.

## Not decided here
- DST pilot 2's attribution: it stays unestablished.
- The later owning-lane and real-daemon validation, and delivery scope: chosen on the patch's review.
