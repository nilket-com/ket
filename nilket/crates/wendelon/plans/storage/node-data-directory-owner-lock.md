# F-OWN-1: Wendelon Node Data Directory Owner Lock

## Status

Approved implementation plan. Production changes and validation remain pending.

## Summary

Give one Wendelon node exclusive write ownership of one canonical data directory. A second process or bootstrap that claims the same directory must fail immediately with a typed `DataDirectoryBusy { path }` error. Ownership must begin before the first file-backed initialization or restore write and remain held until every storage worker that can write has terminated.

## Motivation

Wendelon can be started more than once against the same local data path. Concurrent owners can corrupt or inconsistently mutate slabs, WAL, keys, audit state, and counter witnesses. A process lock at the common startup boundary makes the single-writer assumption explicit and prevents a competing node from proceeding.

Worker cancellation is part of this ownership boundary. Storage workers share the owner token; requesting task cancellation does not prove that the task has terminated. Reopening the same directory before all workers have terminated can create an intermittent busy result or overlap writers. Every restart path must therefore await worker termination before reacquiring ownership.

## Goals

- Acquire ownership after read-only configuration validation and before any key material, slab, WAL, audit, data-server, counter-witness, or other file-backed initialization.
- Acquire ownership before restore helpers write, then transfer the same token into common node bootstrap without releasing and reacquiring it.
- Hold ownership while any storage worker can write, including during cancellation and shutdown.
- Fail fast on a competing owner and fail closed when the filesystem does not provide the required lock semantics.
- Preserve the lock sentinel inode across reset and node handoff.
- Keep read-only inspection tools independent of the exclusive owner lock.
- Make all production and test restart/reopen paths await termination of every worker before reopening the same directory.

## Non-goals

- No lock is added to read-only dump, audit, or scan operations.
- No stale-owner recovery, PID-file protocol, blocking wait, retry loop, polling loop, or sleep is introduced.
- This plan does not claim that advisory locking protects against programs that ignore the protocol.
- Generic NFS safety is not claimed; NFS use requires a verified cross-client `flock` configuration.
- This plan does not change storage formats or storage algorithms.

## Ownership contract

Let `D` be the canonical data directory, `L(D)` its persistent `LOCK` inode, and `Own(p, D)` mean process `p` holds the exclusive lock on that inode. `Write(p, D)` means `p` can mutate any Wendelon-owned byte below `D`, including key material, slab/LSM files, control WAL/vote/snapshot data, audit output, finalization/outcome stores, and counter witnesses.

1. **Exclusion:** two distinct live owners cannot both hold `Own(*, D)`.
2. **Write authority:** `Write(p, D)` implies `Own(p, D)`. Ownership precedes the first write and lasts through termination of the last writer.
3. **Stable pathname identity:** aliases to the canonical directory contend on the same sentinel inode. Normal operation and reset never unlink or replace that inode.
4. **Read compatibility:** read-only operations can overlap an owner if their own consistency requirements are met and they open only read-only descriptors.
5. **Handoff:** a new owner can acquire only after all prior writers have terminated and the prior shared owner token has been dropped. Startup failure releases ownership; partial restore data remains for explicit recovery.

## Lock-file behavior

- Use `<canonical_data_dir>/LOCK` as a persistent sentinel, not a PID record.
- Only an authorized entry point may create the parent directory. Open the sentinel without truncation using `O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW` and mode `0600`; use `fstat` to require a regular file. Refuse symlinks, non-regular files, and unsafe directory paths.
- Acquire `flock(LOCK_EX | LOCK_NB)`. Map `EWOULDBLOCK`/`EAGAIN` to `DataDirectoryBusy { path }`. Do not wait, spin, inspect stale PIDs, or remove the sentinel on failure or drop.
- Keep the descriptor close-on-exec. Do not use `fcntl` record locks; independent opens in one process must conflict as documented by the existing L-1 test.
- Fail closed, with the path and underlying error, when locking is unsupported (`ENOSYS`, `ENOTSUP`/`EOPNOTSUPP`, or `ENOLCK`). Deploy on NFS only after verifying that the configured mount and server propagate `flock` across clients.

## Entry points and lifecycle

### Common startup and restore

Normal startup acquires ownership after read-only configuration validation and before common bootstrap begins file-backed initialization. Backup restore and replacement restore acquire before their helpers create or modify the target directory, then pass the same owner token into common bootstrap. They must not release it and reopen/re-lock between restore and node startup.

The owner is a shared token reachable from each storage root and every owned background writer. Dropping the node's direct reference cannot release the OS lock while a worker still has write capability. Graceful shutdown retains ownership through the abort-and-join sequence.

### Shutdown and restart ordering

Every production or test path that stops a node and then reopens the same data directory must await termination of every worker first. Prefer `NodeHandles::shutdown`, which aborts and joins its owned workers. If a path must abort workers individually, it must await every corresponding `JoinHandle` before dropping/reopening the node. A bare `abort(); drop(node);` followed by immediate reopen is prohibited.

Convert the bare abort-and-drop reopen cases identified in the census, including `node.rs` around lines 3589–3590 and the restart cases in `tests/property_suite.rs`, to the awaited shutdown form. Audit the other restart cases in the census for the same ordering. Add a deterministic synchronization-based test that proves:

- the lock remains held while an aborted worker has not yet terminated;
- if several workers share ownership, termination of only some workers does not release the lock; and
- a competing acquisition succeeds only after every worker has terminated and the final shared token is dropped.

The test must use explicit synchronization and observable task completion, not sleeps or retry loops. No retry, polling, or sleep loop may be added anywhere to hide `DataDirectoryBusy`.

### Reset and read-only tools

`nilket local --reset` retains its existing profile lock, then acquires the Wendelon owner on the existing `<profile>/data/wendelon/LOCK` inode. If a node owns the directory, reset refuses without changing generated bytes. On success, reset clears generated data while preserving the held sentinel inode; it must not remove the directory containing the lock. Reset releases the owner before starting the replacement child node. `local init` remains a profile/config operation and does not acquire this owner.

WAL checkpointing, snapshot install, and live snapshot/backup export stay within the node's owner interval. Read-only tools, including `dump_slab_record`, `audit verify`, and the planned C-S6 scan, take no exclusive owner lock.

## Required validation

### Contract coverage

1. Same-process independent opens and a second process both fail fast when another node owns the directory; use independent `flock` opens.
2. A new owner can acquire after orderly shutdown and startup failure. Cancellation cannot release ownership while a storage worker remains alive.
3. Backup restore and replacement restore reject a held directory before changing target bytes and pass the same owner token into startup.
4. `nilket local --reset` refuses while a standalone Wendelon owns the path, succeeds after release, and preserves the sentinel inode; existing profile-lock behavior remains intact.
5. `dump_slab_record`, `audit verify`, and the read-only scan path work without the exclusive lock.
6. Unsupported-lock errors fail closed, and deployment documentation states the NFS qualification.
7. All restart/reopen tests await every worker. The added cancellation test proves the lock stays held until all shared-token holders have terminated. No retry or sleep-based busy handling is present.

### Concurrency stress

Follow `ci/README.md`'s **Test concurrency** rule. A parallel-only failure is a defect candidate, never a flake. Run the rest of the Wendelon suite as background load while stressing each converted restart test in the foreground with `cargo nextest run -p wendelon --lib -E 'test(<name>)' --stress-count 30` or the corresponding `--test <target>` for integration tests. Include the converted `node.rs` unit tests and both `tests/property_suite.rs` restart tests. Also run the in-process `cargo test -p wendelon --lib -- --test-threads=12` workload ten times with the thread count explicit. Record both loaded runs and any failure rate; do not classify a failure as a flake or add serialization/retries/sleeps to make it pass.

### Acceptance gates

1. Commit this reviewed plan and pass Wendelon's `architecture_test` and `mutation_visibility_architecture_test` before proposing or performing the fast-forward.
2. Implement and submit the source package for review.
3. Pass LOCAL changed-area validation, then DEV on the same paths.
4. Pass the full 18-step storage ladder. The full ladder is required because bootstrap is exercised by Wendelon multinode, simulator, and property steps, and by Toron distributed and embedded-node steps. Changed-area evidence alone is insufficient for this startup-precondition change.
5. Retain the complete raw evidence and pass an independent external audit.
6. Send a final evidence package for review. No publication or runtime enablement is implied by this plan.

## Risks and mitigations

- **Premature release during cancellation:** keep a shared token in all writers, await all workers on shutdown/restart, and test the held-lock interval deterministically.
- **Filesystem lock mismatch:** fail closed for unsupported locking and qualify NFS deployments by verified cross-client behavior.
- **Sentinel replacement:** preserve the lock inode on reset and never unlink it as part of ordinary cleanup.
- **Unintended blocking or retry behavior:** use nonblocking `flock` and return a typed busy error immediately; prohibit retry and sleep loops.
- **Startup regressions outside the changed area:** require the full 18-step ladder and independent audit.

## References

- `ci/README.md`, **Test concurrency**.
- `WendelonNode::bootstrap_with_transport_stack` and restore wrappers in `src/node.rs`.
- `NodeHandles::shutdown` in `src/node.rs`.
- Existing `flock` precedent in `src/storage/mutation_visibility/root2/durable/local_store/adapter.rs` and the L-1 independent-open test in `src/storage/mutation_visibility/root2/provisioning/tests/owner_log.rs`.
