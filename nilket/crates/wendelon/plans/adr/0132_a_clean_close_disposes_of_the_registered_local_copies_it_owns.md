# ADR 0132 - A clean close disposes of the registered local copies it owns

## Status
Proposed, r4 (plan for review; DC-308). Stage-1 implementation cleared by Codex 01a221fb on the folded decisions. Base: delivered 4d9357484 (ADR 0131). Text only: no product or test change is made by this plan.

## Context
- **DC-308, observed by C5a on 190a5cf45** (fixture 0a7b9cfc7, `DC307-C5A-RESULT.md`):
  - A normally promoted, current-generation, registered rehydrated copy survives the ordinary clean close (the same inode, with `CLEAN_SHUTDOWN` written).
  - The next open's `classify_discovered_slab_files` (slab_file_quarantine.rs) quarantines it as `TierPointer`, and promotion of that slab is skipped from then on.
  - Rows stay correct from L2. The open-time quarantine of a file found at reopen is intentional, and is kept.
- **`DataDirectoryOwner::close` today** (ownership.rs ~742):
  1. `begin_close` fences admission;
  2. **then `close_gate` is acquired outside the timeout**;
  3. `timeout(bound, …)` covers the drain (`outstanding == 0`, `startup_barriers == 0`), the in-order worker joins, the `CLEAN_SHUTDOWN` write (if `may_record_clean_shutdown`) and the lock release.
  - A timeout or error gives `ShutdownIncomplete`, with admission fenced. A repeat resumes the joins.
- **The release path that disposal must reuse** (ADR 0131 D4, slab_file_quarantine.rs ~308–560):
  - `claim_released_slab_file` takes **`owner.admitted_operation()?`**, which is refused once `Closing` begins, even inside an admitted scope, which only mints child **write permits**.
  - It then spawns `settle_released_slab_file` (no scope is inherited by the spawned task).
  - Per try, `unlink_if_released_file` runs `path_identity` and then a separate `unlink`; an identity mismatch (`ReleasedPath::Other`) ends the release with a warning and `Ok`.
  - After an unlink, `fsync_parent_dir` runs per entry with up to 3 tries; a `sync_only` claim runs only that sync.
- **Close entries.**
  - **The node:** `Node::shutdown` (node.rs ~3740–3800) quiesces requests and stops the transports, then calls `_directory_owner.close(...)`. The `SlabManager` lives until the node is dropped after that call.
  - **Direct owners:** the startup-failure close (node/startup.rs ~466), repair (manager.rs ~2137, no copies), the test-support readers (node.rs ~53, ~226), and tests. Here the manager may be dropped **before** `owner.close`.
- **The gaps r1 left** (Codex 01a221f6):
  1. no close-time path can run the release body;
  2. a dropped manager silently lost the disposal obligation;
  3. a successful settlement was not preserved across a failed or cancelled join;
  4. the bounds did not match the helper.
- **The counterexamples r2 left** (Codex 01a221f8):
  1. **A strong cycle.** Owner → ledger → `Arc<dyn AsyncFileOps>` → owner: `BlockingFileOps.owner` and `IoUringFileOps.io` (an `IoRingHandle` whose owner) are `Option<DataDirectoryOwner>`, which holds `Arc<OwnerInner>`. An active owner dropped without close would then keep its lock and workers alive.
  2. **Evidence without a release.** `quarantine_bootstrap_collision_locked` (tier_authority.rs ~3272–3292) records a `TierPointer` quarantine and removes `state.rehydrated` **without** `release_slab_file`, deliberately keeping that inode. A stale ledger registration would then have authorized unlinking that evidence.
- **Every site that changes `rehydrated` or produces released debt** (4d9357484):
  - **Inserted:** only through `ManagerState::install_rehydrated` (backing_install.rs ~35), called from ADR 0131's `register_candidate` (rehydration_install.rs ~834).
  - **Removed, with a release:**
    - publication (tiered_io.rs ~1905–1913, `sealed.or(copy)`);
    - retirement (~2291–2299 and ~2451–2459);
    - eviction, `drop_rehydrated_slab` (~2663);
    - GC, `gc_unlink_owned_slab_file` (slab_file_quarantine.rs ~244).
  - **Removed without a release:** the bootstrap collision quarantine (tier_authority.rs ~3289).
  - **Released debt from sealed backings:** publication, both retirements, compaction (compaction.rs ~1583) and GC.

## Decision

### D1. One disposal ledger owned with the directory, independent of the manager's lifetime, with no cycle
- **The ledger.** `LocalFileLedger` (new, `storage/manager/local_file_ledger.rs`) owns the in-memory record of the **copy** files this process must dispose of:
  - the **registered copies**: `slab -> (dev, ino)`, kept in step with `rehydrated` by D5;
  - the **released files**: today's `ReleasedSlabFiles`, moved into the ledger, with identity plus `Owed | Unlinking | SyncOwed`, and now also an **origin**, `Copy | Backing` (D5).
  - It also holds the data directory path. It holds **no backend at all**: the close settle receives a transient one from `close` (below).
  - It holds **no** `DataDirectoryOwner`, **no** owner-bound backend, **no** `IoRingHandle` and **no** `Arc<SlabManager>`; only a `Weak<SlabManager>`, for notification.
- **The ownership graph** (→ is strong, ⇢ is weak):
  - `OwnerInner` → `settlers: Vec<Arc<dyn CloseSettler>>` → `LocalFileLedger` → {tables, `PathBuf`}; `LocalFileLedger` ⇢ `SlabManager`.
  - `SlabManager` → `Arc<LocalFileLedger>`, → `DataDirectoryOwner` (`_directory_owner`), → its owner-bound `file_ops`.
  - **No path leads from the ledger back to `OwnerInner`.**
  - **Active last drop is preserved:** an owner whose last clone and manager are dropped without close frees `OwnerInner`, its lock and its workers as today, and the ledger with it. The copies are then left as an unclean exit leaves them, for the next open's quarantine.
  - **The intentional failed-close pin is preserved:** `closing_pin` keeps `OwnerInner`, and with it the ledger, alive after a failed close, so a repeat can still settle.
- **Close-time I/O with the manager gone, charged to the owner (decision of Codex 01a221fb).**
  - `close` creates a **transient** `BlockingFileOps::with_owner(self.clone())` for the same directory owner and passes it to `settler.settle(op, io)`. It is **never stored in the registered ledger**, and it lives only as long as the settle call and its release tasks.
  - The release tasks run inside their child admitted scope (D2), so every mutation takes a child `WritePermit` in the submitting task.
  - The delivered `unlink` and `fsync_dir` use `run_blocking_write` → `spawn_blocking_io_owned`, which **moves the `WritePermit` into the blocking closure itself**. So a queued or running mutation stays counted in `outstanding` even if its async waiter disappears. Awaiting an unbound `spawn_blocking` handle would not give that property.
  - Drain 2 therefore cannot pass, the marker cannot be written and the lock cannot be released before every disposal mutation has actually finished.
  - The transient backend does hold an owner clone, but only for its own short lifetime. The registered ledger holds none, so the graph stays acyclic and K12 (active drop) and the failed-close pin are unaffected.
- **A dropped manager no longer loses the obligation.** Its registered copies and owed copy releases stay in the ledger, which the owner keeps alive until close.
- **A manager that is alive at close** is told through the ledger's `Weak<SlabManager>` (a notification only, not the obligation) to move its copies out of `rehydrated` under its state lock, so new resolutions go to L2. The ledger then disposes of them.
- **Both cases are covered:**
  - **Direct-owner, manager dropped first:** nothing can read, and the ledger disposes of the recorded copies directly.
  - **Normal node shutdown, manager alive:** the copies are first unpublished from `rehydrated`, then disposed of.

### D2. A post-drain settle phase in the directory owner, with an operation the ledger can actually use
- **`DataDirectoryOwner::register_close_settler(Arc<dyn CloseSettler>)`:** it is refused once `Closing` has begun, and the census pins it to `LocalFileLedger`.
- **`AdmittedOperation::child_operation(&self) -> AdmittedOperation`** (new, ownership.rs): a counted operation for the **same owner**, minted only while the parent is outstanding (as a child `WritePermit` is), and permitted in `Closing`.
  - New **ordinary** admissions (`write_permit` outside a scope, `lifecycle_permit`, `admitted_operation`) stay fenced.
- **The release claim takes its admission from the caller:** `claim_released_slab_file(.., admission: ReleaseAdmission)`, where `ReleaseAdmission` is either:
  - `Owner`: `owner.admitted_operation()`, as at runtime today; or
  - `Child(&AdmittedOperation)`: `parent.child_operation()`, at close.
- **The spawned settler task** receives that operation and runs the **unchanged** settle body inside `op.run(…)`. The body is parameterized only by its backend:
  - **at runtime**, the manager's owner-bound backend, whose I/O takes child write permits (ADR 0131 D2);
  - **at close**, the transient owner-bound `BlockingFileOps::with_owner` created by `close` (D1), whose mutations carry their child `WritePermit` into the blocking job.
- **Preserved unchanged:** the single claim (the entry moves to `Unlinking` under the files mutex), the identity check, the error results, `SyncOwed`, and joining an in-flight owner.
- **Permit and claim ownership:**
  - the **close** future owns nothing that I/O needs;
  - the **settle task** owns the settle operation;
  - **each release task** owns its child operation, and awaits each I/O job to completion. At runtime, each job also owns its child `WritePermit`.
  - A close timeout or a cancelled close caller leaves all of them running; the owner's drain waits for them.
  - A claim leaves `Unlinking` only through its release task (ADR 0131 D4).
- **The new `close` sequence.** The deadline is `deadline = now + bound`, computed **on entry**.
  1. `begin_close`.
  2. `close_gate` acquired **within the deadline** (`timeout_at`). Overlapping callers share one bound each, and none waits on the gate unbounded.
  3. **Drain 1** (`outstanding == 0 && startup_barriers == 0`): installers and their children settle, including a late registration.
  4. **Settle, unless already recorded as `Settled`:** for each settler, `close` mints one admitted operation (only here) and **spawns** `settler.settle(op, io)`, with `io` the transient `BlockingFileOps::with_owner` of D1, then awaits its result.
  5. **Drain 2:** every settle and release operation and child permit has finished.
  6. Record **`settled = true`** in the owner state.
  7. **Joins**, in order.
  8. **`CLEAN_SHUTDOWN`**, if `may_record_clean_shutdown` (unchanged) **and** `settled`.
  9. Release.
- **Retry and cancellation:**
  - **A settler error:** `ShutdownIncomplete`, with no join and no marker. The repeat drains, then runs the settle phase again (settlement is idempotent).
  - **Cancelled or timed out during settle:** the tasks keep their operations. The repeat drains (waiting for them), then settles again.
  - **Cancelled, or a join error, after `settled`:** the repeat skips the settle phase and **resumes the joins** (the existing rule: "cancellation never treats a live join as done"). No new obligation can arise after `settled`: handoffs are fenced, installers have drained, and the manager adds no copies once unpublished. Executors already stopped are therefore never needed again.
- **Preserved:** the startup-barrier check and the existing clean-marker eligibility.

### D3. The ledger's settle
1. **No installer may remain.** A remaining ADR 0131 installer record is an invariant breach and returns `Err`.
2. **If the manager is alive** (`Weak::upgrade`), it moves every `rehydrated` copy into the ledger's released files under its state lock (the `drop_rehydrated_slab` step, origin `Copy`). Otherwise, the ledger moves its own registered copies to released (origin `Copy`).
3. **For each released entry of origin `Copy`, one at a time:** the delivered release body (`claim_released_slab_file` with `Some(identity)` and `ReleaseAdmission::Child(&op)`, then the unchanged settle body), run with the transient owner-bound backend passed by `close`, then await its result. An in-flight owner is joined.
4. **`Ok` iff the ledger holds no registered copy and no released entry of origin `Copy`.**
- **Never touched:**
  - entries of origin `Backing` (sealed slabs released by publication, retirement, compaction or GC), which keep their existing handling (D5);
  - quarantined files (`quarantined_slab_files`);
  - ADR 0131 inhibited names;
  - active and sealed slabs.
- **Managed-path exclusion across the separate `stat` and `unlink`.** While an entry is `Unlinking`:
  - its slab admits no promotion (ADR 0131 D1);
  - GC joins the owner (ADR 0131 D4);
  - the allocator never reuses an id;
  - one manager per directory holds the owner lock.
  - So no managed actor can install a file at that path between the two calls.
  - An unmanaged external writer is outside this ADR, as today.
- **A mismatched file** (`ReleasedPath::Other`) ends its release `Ok` with a warning, as delivered. **No count is promised.** The next open's quarantine report names the file.

### D4. Bounds (matching the delivered helper; no batching)
- **Per released entry:**
  - up to **3 tries of (`path_identity` + `unlink`)**; the identity check repeats each try;
  - then, after an unlink, **up to 3 `fsync_parent_dir`** (per entry, not batched);
  - a `SyncOwed` entry: up to 3 syncs only.
- **Total at close:** with `R` registered copies and `D` owed releases, at most `3(R+D)` stat-and-unlink pairs and `3(R+D)` directory syncs. Entries are processed **one at a time**, so there is one release task and one queued metadata job at a time.
- **Memory:**
  - The identity payload is 24 B per entry (an 8 B `SlabId` plus a 16 B `(dev, ino)`).
  - Auxiliary storage per ledger entry: its hash-map slot and the `ReleasedFile` phase. A claimed entry also has one `watch` channel (one `Arc` allocation) and one spawned task while it is in flight, and at most one entry is in flight at close.
  - No slab bytes are held.
- **`R`** is the copies resident at close, set by the existing residency and reclaim policy; it is not capped here (Open Questions). `D` counts only owed releases of origin `Copy`.

### D5. Registration lifecycle: disposal authority follows the file, and is revoked when the file becomes evidence
- **One helper owns every `rehydrated` change.** `rehydrated` is changed only through two `ManagerState` helpers, under the manager's state lock, and each updates the ledger's registration **in the same critical section** (the ledger's tables sit behind a std mutex, never held across an await).
  - `install_rehydrated(slab, copy)`: inserts the copy and registers `(slab, identity)`.
  - `take_rehydrated(slab, disposition)`: removes the copy and its registration, and returns the copy.
- **Two dispositions:**
  - **`Release`** (publication, both retirements, eviction, GC): the caller's existing `release_slab_file(slab, identity)` records the release with origin `Copy`. The sealed half of those `sealed.or(copy)` sites records origin `Backing`.
  - **`Evidence`** (the bootstrap collision, tier_authority.rs ~3289): the copy and its registration are removed **with no release**. The quarantine insert (`insert_bootstrap`, `TierPointer`) happens in the same critical section.
- **Existing debt at the collision: unlink authority is separated from durability** (decision of Codex 01a221fb). The collision decides, under the state lock and the ledger mutex, **before any irreversible effect**:
  - **`Unlinking`** of the same `(slab, identity)` (a settler in flight): it returns `WouldBlock` first, recording nothing. On retry it **re-reads the actual state**: the release may have finished (no file, nothing to quarantine); it may have failed back to `Owed` (handled below); or it may have reached `SyncOwed` (handled below).
  - **`Owed`** of the exact same `(slab, identity)`: the unlink authority is **withdrawn**, because the file is retained as evidence. The entry is removed, with a warning.
  - **`SyncOwed`**: the unlink already happened, so the inode is no longer at the path and the entry grants no unlink authority over any current occupant. It is **kept until its directory sync succeeds**, never withdrawn by the collision.
    - If a collision encounters it, the "copy" at the path is a different inode, so the collision quarantines the actual occupant by its own identity and leaves the sync-only debt untouched.
    - From the reachable transitions, a `SyncOwed` copy release leaves `rehydrated` without that slab, so the collision normally cannot meet it. It is still preserved if encountered.
  - **Any other claim** (other identities, and origin `Backing`) keeps its identity, origin and phase unchanged.
  - So once a file is quarantine evidence, no outstanding claim holds unlink authority over its inode, and every durability debt is preserved.
- **Census:** an architecture test pins `state.rehydrated.insert` and `state.rehydrated.remove` to these two helpers, so no future site can change `rehydrated` without updating the ledger.
- **The scope of the close obligation, narrowed explicitly:**
  - The close settle owns **copy** files only: the registered copies and the releases of origin `Copy`.
  - Releases of origin `Backing` (sealed slab files from publication, retirement, compaction, GC) keep their **existing** handling: the runtime settler, and at close, nothing new, so whatever the next open does today.
  - `settled = true` therefore asserts only that no copy file remains owed. No copy obligation can arise after it: handoffs are fenced, installers have drained, and the live manager's copies were taken under the lock with the `Release` disposition. A collision **after** the settle finds no copy to take, because the settle already took every copy out of `rehydrated` under the lock. That collision touches only what it finds: a sealed slab, or no local backing.

## Rationale
- **The obligation belongs with the directory owner**, not the manager. The files outlive the manager's lifetime in direct-owner closes, and the owner already governs clean completion.
- **Reusing the delivered release body**, with its admission supplied by the caller, keeps one protocol for runtime and close-time cleanup: the same single claim, the same identity rule, the same `SyncOwed`.

## Consequences (the negatives)
- **Two new owner-level primitives** (`register_close_settler`, `child_operation`) and a settle phase inside `close`. They are confined: only `close` mints settle operations; only an outstanding operation mints a child; both are census-pinned.
- **The released-files table moves** from the manager into the ledger, and gains an origin tag.
- **`rehydrated` is changed only through two census-pinned helpers.**
- **A bootstrap collision can now return `WouldBlock`** while a copy release of the same identity is in flight.
- **The close settle uses a transient owner-bound backend** created by `close`. It is not stored, and its mutations carry child permits into their blocking jobs.
- **Owed sealed-backing releases are left at close as today.** That debt is not addressed by this ADR.
- **A close can now fail** on a persistent disposal error. Previously the copy was silently left to be quarantined at reopen.
- **Close time grows** by the bounds above.

## Algebra
- **Keys and identities:**
  - a registered copy and a released file are keyed by `slab`, with identity `(slab, (dev, ino))`; a released file also carries its origin, `Copy | Backing`;
  - a settler by registration order;
  - an operation by `(owner, parent)`.
- **Order:** fence < gate (within the deadline) < drain 1 < settle (unpublish → release → one-at-a-time identity-checked unlink → sync) < drain 2 < `settled` recorded < joins < marker < release.
- **No narrower key authorizes a deletion.**

## Validation (ordered; each stage needs its own Claude+Codex GO)
1. **Controls,** benign and state-level, one actor per name. The parent 4d9357484 is RED where applicable.
   - **K1 (C5a before/after, the actual owned-manager path):** a normally promoted copy, then `owner.close`: the file is gone by identity and `CLEAN_SHUTDOWN` is written. Reopen: no quarantine, correct rows from L2, promotion at the normal threshold, a later read local. The parent is RED.
     - **K1b:** the same with the release task held inside its unlink, and then inside its sync, across a timed-out close; the repeat completes.
   - **K2, a dropped manager** (through the transient owner-bound backend):
     - (a) with a registered copy: drop the manager, then `owner.close` → disposed, marker, reopen clean;
     - (b) with an owed `Owed` and a `SyncOwed` release → settled;
     - (c) with no work → `Ok`.
   - **K3, a late installer:** held at its write when close begins → drain 1 waits → it registers → it is disposed of → the marker is written.
   - **K4, a mismatched file:** the path is replaced by another inode → left, with a warning → close `Ok` → reopen quarantines it; rows correct.
   - **K5, failed cleanup then retry:** a persistent unlink fault → `ShutdownIncomplete`, no join, no marker → the fault clears → the repeat settles → the marker is written.
   - **K6, a join failure or cancellation after `settled`:** the repeat skips the settle phase and resumes the joins; the marker follows.
   - **K7, overlapping closes:** two callers with short bounds; neither waits beyond its deadline on the gate; one disposal.
   - **K8, evidence through the real transition:**
     - a normally registered copy → the real bootstrap collision quarantine (`quarantine_bootstrap_collision_locked`) → drop the manager → `owner.close` → the **same inode** survives (`dev`, `ino`) → reopen: the quarantine report names it, and its evidence is intact.
     - **Naturally reachable:** the full sequence above.
     - **Constructed debt controls** (state injected through test seams, labelled as constructed):
       - an exact `Owed` debt at the collision is withdrawn and the file kept;
       - an `Unlinking` debt gives `WouldBlock` with nothing recorded; the retry re-reads the state (settled, `Owed`, or `SyncOwed`) and acts on it;
       - a `SyncOwed` debt is kept through the collision;
       - a `Backing` claim of another identity is unchanged.
     - **K8b, failed sync then retry with a surviving replacement:** a copy release unlinks, but its sync fails (`SyncOwed`); a replacement file at the path (another inode) is quarantined by its own identity; the sync-only debt is kept; a retried sync discharges it; the replacement survives with its identity; no debt is left over.
   - **K14, a held blocking job:** a disposal unlink held inside the actual blocking closure (a test seam in the job), with the close caller timed out or cancelled. Order: the job completes → then drain 2 → then the marker → then the owner lock is released. Never the lock before the job.
     - Inhibited tmp names are untouched.
   - **K12, active drop and reacquire with an owner-bound backend:** an owned manager over the production backend (`IoUringFileOps::with_isolated_blocking`, owner-bound) with a registered copy → drop the manager and every owner clone **without** close → `DataDirectoryOwner::acquire_existing` succeeds promptly (the lock was released, so no cycle) → the copy file remains, as an unclean exit leaves it.
   - **K13, backing debt unchanged:** an owed `Backing` release (a sealed slab released by publication) at close is not settled by the close settle, and keeps today's behaviour.
   - **K9, a direct-owner held read:** a read held inside its local read while the settle phase unlinks its copy, then released before the joins, returns the correct row. Reads after the joins are not relied on.
   - **K10, the node path:** `Node::shutdown` with a registered copy → reopen clean, with promotion.
   - **K11, fencing:** during the settle phase, a new `write_permit` outside a scope, an `admitted_operation` and a `lifecycle_permit` are refused (negative control).
2. **The focused lane** stressed ×5; the architecture tests (including the census of `register_close_settler` and `child_operation`); fmt.
3. **`fix-gate --full --test-support`** and the real three-daemon test on the exact tree.
4. **Delivery** as one plan commit plus one impl commit, after review.

## Open Questions
- **A cap on `R` for small nodes,** if the measured residency policy does not already bound it. Deferred to the small-node review.
