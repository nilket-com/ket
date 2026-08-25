# Maintenance rewrite publication: staged design (pre-implementation)

Design for the follow-up fix to `945b94e1` (respect slab pins during
maintenance reclaim). Committed as a plan ahead of implementation so the
reviewed design survives intact; implementation is deliberately held.
Ceremony remains `fix:` — the mechanism is new, but it exists to restore
two existing invariants that maintenance rewrites currently violate:
**a reader is never failed by transient maintenance state**, and **a
Live manifest run always names a backed, reachable slab**.

## The two defects this closes

1. **Reader regression (live, observed):** `945b94e1` marks a slab
   delete-pending for the entire rewrite; every read pins its slab
   (`pin_lsm_read_location`) and pin acquisition refuses delete-pending
   — so a reader of live rows co-resident with dropped debris receives
   a typed terminal failure ("slab N is delete-pending for LSM GC")
   during any maintenance rewrite of that slab. One live occurrence on
   the evidence deployment; the window is structural.
2. **Manifest breach (live, latent):** every nonempty sealed slab is a
   Live L0 manifest run (`record_l0_compaction_run` — called from every
   sealing path, including drop-reclaim's own seal step), and
   `manager/compaction.rs` never transitions runs. Maintenance removal
   therefore leaves a **Live manifest run naming a removed slab** — the
   strongest available account of how the LSM picker planned the absent
   slabs in the 2026-08-24 incident.

## Design summary

Two phases. **Publish** never gates and never fails a reader; **dispose**
is owned by the existing GC sweep once a durable Retired disposition
exists. A dedicated, idempotent maintenance-publication operation — not
the compaction or GC commit shapes, which cannot express it (normal
compaction commits enforce visible-digest invariance structurally, and a
maintenance rewrite changes visible rows by design; GC commits express
only Retired→DeletePending and DeletePending→removed).

### Ordering (with-survivors branch)

- **D0 — staged build.** The replacement is written under
  `maintenance-staged-<old_id>-<replacement_id>.wnd`: never matched by
  slab discovery, never drained, never shadowing a row, carrying its
  publication identity in its name. (Grounding: `open` scans the dir
  for final `slab_<hex>.wnd` files and drains them into the live index
  **before** loading redb, treating a failed or missing redb as
  nonauthoritative and rebuilding from those files — so a final-path
  file is authoritative by location, nothing may occupy that location
  before its publication is durable, and absence of redb rows proves
  nothing about disposability.)
- **D2 — the atomic publication commit.** One `PersistentIndexUpdate`
  carrying `put_records` (replacement locations), `delete_records`
  (old locations), and `put_lsm_runs` = [old →
  `Retired { successors: [replacement] }`, replacement → Live].
  **G1 (confirmed):** `PersistentIndexUpdate::apply` performs record
  and LSM-run changes in one redb write transaction.
- **D1′ — install.** Rename staged → final; fsync the directory. Only a
  committed publication ever produces a final-path file.
- **D3 — the volatile flip.** Sealed-map insert, index rewire,
  `relocated_reads` population. The old slab **stays in the sealed
  map** for in-flight readers; no delete-pending mark exists anywhere
  in D0–D3.

Phase D thereafter is the untouched GC sweep: Retired → delete-pending
on the emptiness predicate (`record_rows_for_slab(..).is_empty()`, true
after the flip by construction) → deletion under rewrite claim, pin
drain, and `can_retire_local_slab`.

### Publication validation (replaces, not waives, invariance)

Preconditions: the old run is Live; the replacement summary derives from
the relocation set; the idempotency key is
`(old_slab_id, replacement_slab_id)`; re-apply against the post state is
a no-op success. The maintenance pass declares its reclaimed row set,
and publication validates that the survivors are exactly the old visible
rows minus the declared reclaim — an undeclared visible-row change is
refused exactly as compaction refuses digest drift.

### No-survivors branch

The pin-gate stands at the first irreversible mutation — **before**
secondary-index reconciliation (a refused gate defers the whole pass
with zero partial state; a fully-dead slab's rows sit behind the
admission tombstone, so no legitimate reader exists there to refuse).
Then: reconciliation proof → **D2′** (one update deleting the slab's
records and transitioning old → `Retired { successors: [] }`) →
in-memory removal and inline unlink after D2′. Neither branch can leave
a Live run naming an absent slab.

### D1′ failure without a crash

A rename/fsync error after D2 leaves the publication durably committed
with staged bytes present inside a live process. That state enters the
**same S1 reconciler immediately, in-process, under a per-publication
lock** — one function with two callers (recovery pre-discovery, and the
live error path) so the crash and no-crash stories cannot drift. It
never returns as an ordinary failed pass awaiting restart: the world is
already committed, and "failed" would be a lie the next tick acts on.

The reconciler retries a rename/fsync failure only to a bounded policy.
If it cannot install S1 within that policy, it takes the explicit
**restart-required fail-stop**: the publication gate remains closed, no
planner can observe the D2-without-D3 world, and the local manager stops
serving so its supervisor restarts it. Startup S1 reconciliation then
performs the only permitted recovery before discovery. The implementation
must not release the gate and continue normally, nor turn exhaustion into
an ordinary maintenance-pass error.

### Live-process publication exclusion

The existing manifest write lock is synchronous and is deliberately
dropped before async mirror writes; it must not be held across an
await. The exclusion is an **explicit async maintenance-publication
gate** (`tokio::sync::RwLock`): planners acquire/read-check it before
selecting a plan; a publication holds it in write mode across the
D2→D3 handoff; the short synchronous manifest lock is taken only around
the in-memory manifest mutation inside D3. **Lock order, defined and
tested:** slab-rewrite claim → publication gate → persistent-index
writer → short manifest lock; the picker takes gate(read) → short
manifest lock; the GC sweep takes claim → persistent-index writer and
never the gate. No path acquires the claim while holding the gate.
Readers are unaffected (index and pins, not the manifest).

## Recovery: the staged-publication state machine

A staged-publication reconciliation pass runs **before normal slab
discovery and before the first slab-ID allocation** (staged files are
invisible to discovery by design, so a rebuilt allocator would
otherwise re-issue a staged replacement's ID and collide at a future
rename).

- **S0 — staged, pre-publication** (crash before D2): reap the staged
  file only. Its safety derives from the staged protocol's ordering,
  not from an absent redb row: D0 neither changes nor removes the old
  backing, and D2 is atomic. With healthy redb, the lack of a matching
  committed publication classifies this staged file as pre-D2; it does
  not prove that any rows are absent or disposable. In a declared
  redb-rebuild, D2 is absent from the reconstructed world, so the staged
  file is likewise uninstalled by construction while the old backing
  remains. Reverting is consistent end to end: rows a lost redb
  resurrects from the old backing sit behind the raft-durable admission
  tombstone, and the raft-durable reclaim debt re-drives the pass.
- **S1 — committed, uninstalled** (crash between D2 and D1′): detected
  by the unambiguous triple — Live replacement run + absent final file
  + present staged file — and rolled forward (rename completed) before
  discovery runs.
- **S2 — committed final** (crash after D1′): a normal slab; the old
  backing, if present, is a Retired reference-empty run — the sweep's
  diet.
- **Final-path files are never reaped by inference.** The only reap is
  S0's, keyed to the staged name and the recovery mode's explicit
  semantics.

Crash matrix: after D0 → S0 reaped, world unchanged; mid-D2 → redb
atomicity yields pre (S0) or post (S1) only; after D2 before D1′ → S1
rolled forward, with both the replacement mapping and the old run's
Retired disposition present before any cleanup resumes; after D1′
before D3 → S2, restart rebuilds the flipped world, sweep collects the
old backing; after D2′ before unlink → Retired empty run with a file,
sweep collects; redb lost pre-D1′ → publication gone from the rebuilt
world, staged file reaped, debt re-drives; redb lost post-D1′ → the
final file itself carries the rows through the rebuild, which is why
the rename crosses the commit.

## Controls

1. **Reader-through-rewrite** completes (regression control; no
   delete-pending mark exists in D0–D3), timing-free via barrier seams.
2. Disposal defers to reader pins; the GC sweep collects after release.
3. **Crash arms at every cut** (barrier seams + restart): S0 reaped —
   with the negative arm that a slab with records, a run, or active
   status is never reaped; mid-D2 pre/post-only; S1 rolled forward
   before discovery, both dispositions present before cleanup; S2 with
   sweep collection; D2′-to-unlink; and the redb-nonauthoritative arm
   (staged file through a declared rebuild is reaped and the re-driven
   pass completes).
4. Publication validation refuses an undeclared visible-row change
   (negative arm included).
5. Idempotent re-apply is a no-op (same key, post state).
6. **Live-run invariant:** after any maintenance pass, every Live run's
   slab exists and is reachable.
7. **In-process D1′-failure control:** inject a rename failure after
   D2; the pass reconciles in-process under the per-publication lock;
   the replacement is usable, the old run Retired, and — through the
   exclusion window — no reader completes against a half-published
   world and no LSM tick plans the old run.
   A persistent injected failure exhausts the bounded policy, enters
   restart-required fail-stop without releasing the publication gate,
   and a simulated restart rolls S1 forward before discovery.
8. **Allocation-ordering assertion:** S0 cleanup precedes the first
   slab-ID allocation of a recovery; a planted staged file's
   replacement ID is never re-issued.
9. A staged file is never drained by discovery and can never shadow a
   row or become the active slab.
10. Lock-order control against the persistent-index writer and the
    slab-rewrite claims.
11. The full `945b94e1` contract suite stays green unchanged.

## Implementation gates (first steps in code)

- G1 re-verified at the apply site (single redb write transaction; if
  ever violated, the design is invalid).
- The staged-name namespace is disjoint from every existing cleanup
  glob (`compaction-temp-*`, retention/`.table_drop.tmp` reapers).
- `finish_compaction_with_replacement` is restructured from its current
  volatile-then-durable order into D2 → D1′ → D3; the old slab's map
  removal moves to the sweep.
