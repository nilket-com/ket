# ADR 0131 - A rehydration installer owns its slab paths until its copy is registered or removed

## Status
Proposed (plan for review; DC-306, DC-307). Base: delivered 190a5cf45. DC-308 (a registered copy outlives a clean close) is a separate successor, not decided here.

## Context
- **What 190a5cf45 does.** A hot tiered slab is promoted to a local copy inside `read_from_l2`:
  - `note_l2_read(slab)` admits one attempt (`rehydrating`; lease-scoped while fetching, DC-305).
  - The read fetches the object. `begin_rehydration_install` converts the admission to `Installing`, and `install_rehydrated_local_copy` then runs `unlink(tmp)`, `write_all(tmp)`, `open`, `fsync`, `link_no_replace(tmp, path)`, `unlink(tmp)` and `fsync_dir`, and opens the store with `LocalSlabStore::open`, which creates the file if it is missing and recovers before returning.
  - `tmp` (`slab_{id}.wnd.rehydrate.tmp`) and `path` (`slab_{id}.wnd`) are fixed per slab.
- **The production backend** (`IoUringFileOps::with_isolated_blocking`) runs every name operation as a job on four metadata workers, and fsync on io_uring. An enqueued job runs to completion even if its awaiting task is dropped.
- **DC-307 (source-confirmed).** Publication, including an exact same-pointer replay (tiered_io.rs ~1866), removes the `Installing` entry by id without waiting. Later reads can then admit a second installer on the same names while the first one's jobs are queued or running.
  - The no-replace link protects an existing destination. It does not establish ownership of the source inode or of later cleanup.
  - No serving outcome has been observed. The integrity impact is undetermined.
- **DC-306 (source-identified).** A read cancelled after the handoff leaves `Installing` held until a publication, retirement or quarantine.
- **Admission today.** `write_permit()` is refused once the owner leaves `Active`, and every file operation takes its own permit in the submitting task (file_ops.rs `run_owned` and the blocking path; io_uring.rs submissions).
  - `begin_close` alone fences admission; `close` performs the drain.
  - An operation holding one outer permit would delay close, but every later operation it issues would be refused.
- **Released files.** `unlink_released_slab_file` removes its id from `released_slab_files` *before* awaiting the unlink, and unlinks by id alone. Its own comment says that is sound only while no file can appear at a released path before the unlink.
- **Open-time evidence.** `classify_discovered_slab_files` quarantines discovered **final** `slab_{id}.wnd` files of tiered or retired slabs, and its report is keyed by slab id. Temporary names are counted only as id-allocation evidence (`slab_ids_named_by_file`). They are not classified.

## Decision

### D1. One installer per slab, owned by the manager; admission on the resolved pointer
- **Admission takes the resolved pointer.** `note_l2_read(&pointer)` admits an attempt only if, under the state lock, `state.tiered[pointer.slab_id] == pointer`. A read that resolved `L2(old)` before a publication or retirement never admits, whatever the counts. Today admission takes only the slab id.
- **Capacity is acquired at admission, before any fetch** (D5). If none is free, nothing is admitted.
- **The record.** `installers: std::sync::Mutex<HashMap<SlabId, InstallRecord>>` (never held across an await), in a new module, `storage/manager/rehydration_install.rs`. The record holds:
  - `attempt` and `pointer`;
  - `phase` (D3);
  - `candidate: Option<FileIdentity>` and `final_name_owned: bool`;
  - `outcome: watch::Sender<InstallState>`.
  - `InstallState` is `Pending`, or the terminal `Settled(InstallOutcome)`.
- **The handoff.** It runs in the reader's task, under the state lock, with **no await** between its steps:
  1. confirm the exact-token admission (the DC-305 check);
  2. take an **admitted operation** from the owner (D2); if it is refused, the attempt ends, its capacity token is dropped, no retry floor is set, and the row is returned;
  3. insert the record with `Pending`, and subscribe the reader's receiver **before** spawning;
  4. `tokio::spawn` the installer, moving into it the plaintext, the admitted operation and the attempt's capacity token (D5).
- **Ownership.** The installer task alone records outcomes and finishes the disposition.
  - **The terminal value is published on the watch before the record is removed.** A `watch` retains its last value, so a reader that first waits after settlement still observes `Settled(..)`. A reader that disappears changes nothing.
  - The reader awaits `Settled(..)`, so the threshold read's timing is unchanged. Dropping the waiter leaves ownership with the installer.
- **After the reader leaves.**
  - If the installer succeeds and its attempt is still current, later reads use the registered copy.
  - If it fails or its attempt is revoked, the installer applies DC-304's retry floor itself, and only if it still owns the admission.
- **Exclusion.** While a record exists for a slab:
  - `note_l2_read` admits no attempt;
  - GC eligibility refuses `WouldBlock` (a new clause beside `pin_count`);
  - `unlink_released_slab_file` and `drop_rehydrated_slab` refuse for that slab.
- **Publication, retirement and quarantine** never wait for the record and never remove it. They revoke the **right to register** by removing the `rehydrating` admission, as today.

### D2. An admitted operation: one close contract for an operation that issues many file writes
- **`DataDirectoryOwner::admitted_operation() -> AdmittedOperation`:**
  - It is refused unless the owner is `Active`, so `begin_close` alone fences new admitted operations, and with them new handoffs.
  - It counts in `outstanding`, as `WritePermit` does, so the drain in `close` waits for it.
  - Dropped without `complete()`, it revokes this run's clean-shutdown record, as `LifecyclePermit` does.
- **The scope.** The installer body runs inside `ADMITTED_OPERATION.scope(op_handle, …)` (a task-local).
  - `DataDirectoryOwner::write_permit` is the single choke point every backend reaches, always in the **submitting** task.
  - If the current task's scope holds an admitted operation for **the same owner** (`same_owner`), it returns a **child** `WritePermit`, even while `Closing`. The child is minted in the submitting task.
  - The child **moves with the actual I/O**, into the blocking job or the ring submission, exactly as an ordinary permit does today. It is counted in `outstanding` independently, so it **remains counted even if the parent unwinds or is dropped first**.
  - Minting needs no wait. It only checks that the parent is still outstanding, and that holds while the scope runs, because the scope owns the parent.
- **Everything else stays fenced in `Closing`:**
  - unrelated callers, including other tasks of the same owner outside a scope;
  - other owners;
  - new top-level admissions: `write_permit`, `lifecycle_permit`, `admitted_operation`.
- **Every owner-close entry:**
  - `begin_close` (from `close`, or from a dropped startup barrier) fences admission;
  - `close` drains, waiting for every installer's admitted operation and every child permit, including cleanup;
  - **then** joins the workers, so the executors run until the drain completes;
  - **then** writes `CLEAN_SHUTDOWN`.
  - **Timeout:** `close` returns `ShutdownIncomplete` with the installer still in scope. Admission stays fenced, and a repeated close resumes the drain. A stuck installer leads to the existing hard-kill and recovery path.
- **The future DC-308 sweep** must be ordered after the fence and after this drain of installers. A sweep before `owner.close` could miss a late registration.

### D3. Identity, names and disposition
- **Creation, before any byte is written.**
  - The installer opens `tmp` with `create_new` and `write`, and immediately records the handle's `fstat` identity: `candidate = Some(id)` and `phase = Created`.
  - **If the identity lookup fails,** the installer still owns an inode it cannot name by identity. It closes the handle, settles `Failed(IdentityUnknown)` (publishing it, then removing the record; no unlink is attempted without an identity), and reports the slab's **tmp name** as inhibited (below). It never deletes by name alone.
- **The full write keeps `write_all` semantics.** `write_plain_at` performs one `write_at` and returns the count. The installer loops `write_plain_at(handle, offset, &blob[offset..])`:
  - it advances by the returned count;
  - it treats `0` as `ErrorKind::WriteZero`;
  - it requires `offset == blob.len()` before continuing.
  - Each step is a separate submission, carrying its own child permit (D2).
  - The owned `Vec` (the plaintext) stays in the installer until the write completes, and is dropped right after (D5). Then `fsync(handle)` (io_uring, existing) and `phase = Written`.
- **The link.** `link_no_replace(tmp, path)`.
  - **On success, immediately record `final_name_owned = true`**, before any further fallible step. From then on, the final name is the installer's responsibility.
  - Then `stat(path)`. A matching identity confirms the link. A different identity or an error goes to the disposition with the final name still recorded as owned, and the final name is examined by identity there.
  - `EEXIST`: the final name holds someone else's file. The installer is not responsible for it and must not touch it. Only `tmp` is disposed of.
- **Removing `tmp`.** `stat(tmp)`; on an identity match, unlink it. `phase = Linked { tmp_present: false }`. Then `fsync_dir`.
- **Registration.**
  - `LocalSlabStore::open_recovered(path, .., create: false)` opens the existing file without creating a missing one; recovery runs on the verified bytes. The opened handle's identity must equal the candidate.
  - Then, under the state lock, the right to register holds only if the record names this attempt, `rehydrating` holds `Installing(attempt)`, and `tiered[slab] == pointer`.
  - In one critical section: insert into `rehydrated` (with the cached identity, D4) and end the admission.
  - Then publish `Settled(Registered)`, remove the record, and `complete()` the admitted operation.
- **Disposition on failure, revocation or supersession.** For each name the record owns (`tmp` while present; `path` if `final_name_owned`):
  - `stat` the name. On an identity match, unlink it. On a mismatch or a missing name, leave it.
  - Then `fsync_dir`.
  - Publish the terminal outcome (`Removed`, or a failure from D5) **before** removing the record and releasing the exclusion.
- **Intermediate errors** (open, write, fsync, link, stat, unlink, dir sync, store open, identity mismatch) all go to the disposition with whatever names the record owns at that moment. Nothing settles without a published outcome.
- **tmp and final names are separate in evidence and disposition.**
  - **A tmp name this process did not create** (`create_new` → `EEXIST`, for example a previous process's leftover, or a tmp left by an `IdentityUnknown` settlement):
    - It establishes nothing about the final file.
    - The installer never touches it, inhibits **only promotion of that slab** (no further admissions, the reason recorded as `RehydrationTempPresent` with the tmp's identity if `stat` succeeds), and settles `NoAuthority(tmp)`.
    - This is a **separate report** (`rehydration_inhibited`, keyed by slab). It is not an entry in the final-file quarantine, so any distinct final-file evidence (a quarantined `slab_{id}.wnd`) is preserved unchanged beside it.
    - GC, publication and reads are not inhibited. GC never considers tmp names.
  - **A final name** found occupied at link (`EEXIST`) is someone else's file: the installer never claims it. If it is not already in the quarantine report, it is left exactly as found, and the existing paths decide.
  - **Removal durability.** A removed tmp whose directory sync failed may reappear after a crash. It is then a tmp leftover (handled as above at the next attempt). **The existing open-time quarantine does not classify tmp names, so it does not catch this**, and the plan does not rely on it. A removed final name whose sync failed may reappear as a final file, which open-time quarantine does classify.

### D4. Released files stay visible until their unlink and disposition finish
- **The structure.** `released_slab_files` becomes `std::sync::Mutex<HashMap<SlabId, ReleasedFile { identity, phase }>>`, with phase `Owed` or `Unlinking`.
  - `release_slab_file(slab, identity)` takes the identity the released backing **cached at its open** (`LocalSlabStore` records `opened_file_identity` once), so no syscall runs under the state lock.
- **A cancellation-safe owner.** The unlink of a released file is performed by a manager-owned **release settler** task, holding an admitted operation (D2), as the installer does.
  - `unlink_released_slab_file` moves `Owed` to `Unlinking`, spawns the settler (no await between), and awaits the settler's watch.
  - The settler `stat`s the path. On an identity match it unlinks and fsyncs the directory, then removes the entry. On a mismatch or a missing name, it removes the entry with a warning, because it has no authority over that file.
  - On an unlink failure, it returns the entry to `Owed` (retry owed, as today), with its identity.
  - The entry therefore leaves `Unlinking` only through the settler, never through a waiter's drop. A waiter dropping does not release the exclusion early, and the entry never strands without an owner.
- **Admission refuses** while an entry exists in **either** phase, so the exclusion is continuous through the actual unlink.

### D5. Capacity follows the actual work; bounds; retry ownership
- **One capacity token per attempt.**
  - `promotion_slots: Arc<Semaphore>` with `MAX_CONCURRENT_PROMOTIONS = 2` permits (an internal constant; no operator setting).
  - `note_l2_read` takes an `OwnedSemaphorePermit` with `try_acquire_owned`, never waiting, **before admitting**. No permit means no admission, and the counts keep accumulating.
  - The permit travels with the attempt: the read's `AdmittedRehydration` holds it through the fetch; it moves into the installer at the handoff; it is dropped at settlement, or when the attempt is abandoned (fetch error, cancellation, refused handoff).
  - It is **independent of pointer invalidation**. A publication that erases a `Fetching` entry does not free the slot while the whole-object fetch continues. A dead `Fetching` entry on a cold slab holds no slot, because its permit dropped with its lease.
- **The supported bound is a count, not a byte figure.**
  - At most **2 promotion attempts** in flight per manager, across fetch, decode, handoff, write and disposition.
  - Each attempt may hold, at once, its fetched object buffer and its plaintext (decryption overlaps them), plus the read's own record bytes.
  - So promotion holds at most 2 × (object + plaintext) beyond the foreground reads.
  - **No numeric byte bound is claimed.** `max_slab_bytes` is a rotation target, and `target_for_append` lets the first frame on an empty slab exceed it, so there is no enforced maximum object size.
- **Queued work.** Each installer has at most one job queued at a time, so at most two installation jobs sit in the metadata queue.
- **Records.** At most 2 installer records. Released-file entries number at most one per slab (an existing structure, now with identity and phase). Inhibition entries number at most one per slab. Cleanup records hold no slab bytes.
- **Retry ownership.** The installer (or the release settler) alone retries, while holding its admitted operation:
  - 3 attempts with a 100 ms → 400 ms backoff, each a fresh `stat` plus unlink or dir sync.
  - The exclusion ends when it settles, never before its outcome is published.
- **A persistent unlink error on a known candidate.**
  - The installer settles `CleanupFailed { name, identity }` (published first). The slab's promotion is inhibited (`rehydration_inhibited`, reason `RehydrationCleanupFailed`, separate from the final-file quarantine). Then the record is removed.
  - If the stuck name is the final `slab_{id}.wnd`, GC refuses that slab (`WouldBlock`) while the inhibition names a final file. That is a bounded, reported per-slab state reached only by an I/O failure, and it is cleared by an operator action, or by the next open's classification of the final file.
  - A stuck tmp name does not affect GC.

### D6. Effects on delivered guarantees
- **DC-304 and DC-305:** unchanged. A returned fetch error or a cancelled fetch drops the capacity token, and the next eligible read retries.
- **Single admission while a fetch is live:** unchanged.
- **Threshold-read timing:** unchanged.
- **DC-306:** resolved. A cancelled reader leaves the installer to settle.
  - If the installer succeeds and is current, the copy is registered and later reads are local.
  - Otherwise, the first eligible read after settlement is admitted.
  - No retry starts while the earlier installer's I/O is unfinished.
- **DC-307:** resolved. The record's exclusion outlives publication, and registration requires the admission, the pointer and the identity.
- **No-replace** stays as defence in depth.

## Rationale
- **Why a manager-owned task, not the read.** Only an owner that no reader can cancel can make "every submitted operation has finished" equal "the owner observed its result". The read keeps its timing by waiting on a notification.
- **Why a same-owner task-local admitted operation.** It keeps the existing backends and their single permit choke point. Production and test backends get the same completion contract: the installer awaits the same `AsyncFileOps` calls in sequence, and every submission carries its own counted child permit. Close ordering becomes a property of the owner.
- **Why identity at creation and an explicit final-name responsibility.** Every unlink is then conditional on an inode the installer created, and every name it may have to clean is recorded before the step that could fail.

## Consequences (the negatives)
- **A new owner primitive** that mints child permits during `Closing`. It must stay narrow (same owner, inside a scope, while the parent is outstanding) and be audited by an architecture census of its call sites (the installer and the release settler only).
- **The handle write** replaces `std::fs::write`. Test backends that implement only `write_all` on this path must implement `write_plain_at`.
- **Two new reports** (`rehydration_inhibited` reasons) and a per-slab GC refusal after a persistent failure to unlink a final name.
- **The promotion cap of 2** can delay a third hot slab's promotion. Its reads keep being served from L2.
- **manager.rs is at its 4650-line cap.** The new state lives in `rehydration_install.rs`, and the root needs a net-zero edit or a carve.

## Algebra
- **Keys and identities:**
  - an admission is `(slab, attempt)`, admitted only for `tiered[slab] == pointer`;
  - an installer record is keyed by `slab`, with identity `(slab, attempt, candidate (dev, ino), final_name_owned)`;
  - a released file is keyed by `slab`, with identity `(slab, (dev, ino))`;
  - an inhibition is keyed by `slab`, with identity `(slab, name kind, (dev, ino)?)`.
  - Keying by slab is deliberate: one installer and one release per slab. Every destructive step compares the full identity, never the key alone.
- **Order:**
  - capacity token ≤ admission < handoff < created (identity) < written (full count) < linked (final name owned) < registered or removed < outcome published < record removed < admitted operation completed and capacity token dropped;
  - `begin_close` fence < drain completed < workers joined < `CLEAN_SHUTDOWN`.
- **No narrower key authorises a deletion.**

## Validation (ordered; each stage needs its own Claude+Codex GO)
1. **Benign state-level controls**, one installer at a time and never two writers. A test backend gates the installer's handle write. RED on the parent where applicable, then GREEN.
   - **C1, same-pointer replay:** with the installer held, the replay returns; reads past the threshold admit nothing and serve L2. On release, the candidate is removed by identity and not registered. The next eligible read admits and serves locally.
   - **C2:** a replacement.
   - **C3 (DC-306):** the threshold reader is aborted after the handoff while the installer succeeds; the copy is registered and a subsequent read is local. Variant: the installer fails, and the first eligible read after settlement admits.
   - **C4:** a read that resolved an old pointer admits nothing.
   - **C5, close with a held installer:**
     - `close` waits;
     - an **unrelated caller's** write in the same owner during that drain is refused (negative control), and a new admitted operation is refused;
     - on release, close completes with `CLEAN_SHUTDOWN`;
     - a close timeout returns `ShutdownIncomplete`, and a repeated close completes after the release.
   - **C6:** GC, the released-file unlink and the copy drop refuse while a record exists. A released file whose identity differs is left alone. A waiter dropped during a release leaves the entry `Unlinking` under the settler until it completes.
   - **C7:** a short write (a test backend returns fewer bytes) completes by looping; a zero-progress write fails with `WriteZero` and disposes.
   - **C8:** an injected unlink failure (a test backend error) → retries → `CleanupFailed`, published, with the inhibition reported.
   - **C9:** a leftover tmp → `RehydrationTempPresent`; never unlinked; final-file evidence unchanged.
   - **C10:** the capacity token is held across a publication that erases `Fetching`, and a third attempt is refused until it drops.
   - **Plus the delivered set:** healthy, returned-Err, cancelled fetch, the stale tests, quarantine, reopen.
2. **The focused lane**, stressed ×5; the architecture tests, including the census of the new primitive; `cargo fmt --check`.
3. **`fix-gate --full --test-support`** and the real three-daemon `parent_decisions_processes` test on the exact tree.
4. **Delivery** as one plan commit plus one impl commit, after review.

## Open Questions
- **DC-308:** disposal of registered copies at clean close, ordered after the D2 fence and drain. A separate successor ADR.
- Whether a tmp leftover from a previous process should be removed at open under an identity rule, instead of inhibiting promotion. Deferred; this ADR never deletes a name without authority.
