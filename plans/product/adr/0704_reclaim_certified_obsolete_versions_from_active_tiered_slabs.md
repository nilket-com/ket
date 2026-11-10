# ADR 0704: reclaim certified-obsolete versions from active tiered slabs

Status: proposed, 2026-11-09 (Claude, Codex). Implementation lands with this plan. Direction: Codex 01a1c1f5. The design was cleared over draft r1 and addenda 1–3 (01a1c1fe, 01a1c219, 01a1c21c, 01a1c21d). Review record: `reviews/tiered-certified-cleanup/`.

## Why
- **The gap:** in ParentDecisions mode, certified-obsolete versions are reclaimed from sealed local slabs (ADR 0699 S5b) and local LSM runs (ADR 0703). Tiered slabs stay contained (`manager.rs:3866`, `:4044`; 0703 excludes tiered inputs). So an obsolete version with a copy in an active tiered slab is never removed there, and its debt never retires: `retire_debt` counts every indexed copy, tiered ones included.
- **The defect found while tracing (on main, both modes):** recovery delivers an Applied tier op's pointer without checking that it is still the ring's current generation. With A then B (B replacing A) both Applied, B installed and handed off, and A's intent not handed off, recovery reinstalls A over B. The RED `an_older_applied_tier_op_never_reinstalls_over_a_newer_generation` reproduces this after a restart and in process. Rows a newer generation dropped (a Today retention prune too) would then serve again. A certified rewrite that drops rows makes it fatal, so the shared fix is part of this ADR.
- **Boundary (Codex 01a1c1f5):**
  - removal from the **active** tiered representation only, through one existing owner-qualified rewrite or retirement;
  - **no physical L2 deletes** (ADR 0696): old objects are retained, and their bytes reported separately;
  - no follower forwarding, topology change, new authority protocol or new long-lived worker.

## Premises (from source, main 2a96a0654)
- **The rewrite shape exists:** `prepare_tiered_table_retention_rewrite` builds one `CompactionRecord` (with `reclaim`) per indexed record of the published object, writes the survivors to a fresh immutable object, or returns a retirement. `rewrite_one_tiered_slab_for_retention` claims, uploads, reads back, records the candidate, proposes `Tier{replaces}` or `Retire{expected}`, delivers, and retains the old object.
- **The ring** enforces `replaces`/`expected` against its current generation, and a retired slab id is permanently `removed`.
- **Delivery** re-derives rows from the new object and replaces the slab's rows in the pointer's own index transaction.
- **Leader-only, owner-scoped proposals; no forwarding.**
- **No committed object is ever deleted.** Reopen loads current pointers only.
- **`TierSnapshotLease`** is an owned read guard over the canonical state: ring apply waits while it is held. A recorded outcome carries `Applied { index }`.

## Decision
**A leader reclaims certified-obsolete versions from one of its own active tiered slabs through the existing retention-rewrite shape, under one certified selection bound to the slab's exact source generation. The ring records the rewrite's certified provenance, which every publication path reads under a held canonical lease, and every tier publication installs only the ring's current generation. No L2 object is deleted.**

### 1. The job and the decision
- **The claim** takes the slab-rewrite exclusive claim and the published pointer (the **source generation**). It reads and validates the object and builds its records as the retention rewrite does, each with 0703's `FrameTable` stamp and its durable attribution.
- **The selection** is 0703's `decide_members`, unchanged (S5b's rule per copy; conflicts, malformed frames and migrated-range markers stay). It is read from one `version_facts` read and `content_evidence`, and bound to `active_gid`, the source generation and a job fingerprint (owner, incarnation, slab, source generation, the slab's durable rows digest).
- **The rewrite** marks `reclaim` from the selection only (never from retention or table-drop rules, never under containment), keeps `keep_conflicting_versions`, and requires that the reclaimed records be exactly the selection's occurrence multiset.
  - **Survivors** → a fresh `certified/` object and `Tier{replaces: source}`.
  - **None** → `Retire{expected: source}`.
  - **An empty selection** proposes nothing.

### 2. Provenance on the ring (the classification every path reads)
- **Tier:** `TieredSlab` (command and committed state) carries a trailing `certified: Option<CertifiedProvenance { op: OpKey, source: TierGeneration, postimage_rows_digest }>`. It is not part of `TierGeneration`.
- **Retire:** `RetireOp` carries `certified: Option<CertifiedRetirement { op: OpKey, slab: SlabRef, source: TierGeneration }>`. On apply it is stored only in that op's **retained outcome entry**, beside its digest and outcome, and pruned with the entry when the owner's acknowledged prefix retires it. This is bounded by the existing retained window: no lifetime history.
- **Apply validates** that `op` is the envelope's key, `source` is `replaces`/`expected`, and the owner, incarnation and slab agree. Allocation builds the provenance itself from the allocated key and the selection's source (a caller cannot supply it, and the ordinary allocation refuses a command that carries it), then fixes the command digest, with the op intent and certificate written in the same transaction.
- **Snapshots** serialize the payload with the entry, atomically.
- **In mode,** a Retire must be certified (admission and apply refuse others); Today is unchanged.
- **Format:** in place (Raft envelope, WAL and snapshot formats, mode marker). Old state refuses, typed (pre-release).

### 3. The local certificate and the op discriminator
- `OpIntent` gains `certificate: None | Required`. `tier_certified_selections_v1[op_seq]` holds:
  - `job_fingerprint`, `active_gid`;
  - `source_generation`, `source_rows_digest`;
  - `postimage_rows_digest`, computed before submission with the delivery's own derivation over the uploaded, read-back object; empty for a Retire;
  - `drop_digest`, `drop_rows_digest`, `drop_count` (0703's `canonical_drop`, its table from the debt record);
  - `op_binding` (the op digest).
- **Written** in the op's allocation transaction under `active_gid == selection.active_gid`. A moved generation writes nothing, and the candidate goes through the existing unsubmitted-candidate path.
- **Cleared** only in that op's hand-off transaction, by exact binding.
- **Orphans** fence certified tiered cleanup at open: a certificate without its `Required` intent, a `Required` intent without its certificate, a binding to another op, or an unreadable record. The store opens and reads are unaffected.

### 4. One publication rule for every path
**The paths:** the live observer, deferral replay, the authority reconcile, the local sweep fallback and recovery delivery install through `apply_tier_slab_under_publication_lock_prepared`; retirements go through `retire_tiered_slab_under_publication_lock`, and a snapshot install's retirement marks are classified by the same retirement rule under the observer's post-install lease (a fault fences and leaves the mark owed; the install completes). A certified generation may be staged or registered as pending verification; the verifier then publishes it only by the same rule, under the held canonical lease (the bound certificate, the exact source rows, the postimage and the exact drop).

**Each path follows the same sequence:**
1. **Take authority and the locks** in one order: `TierSnapshotLease` (the manager's bound canonical source) → slab publication lock → cleanup claim → index publication gate. The authority verifier publishes under the same held authority (the retry's or the bound one), proves the pending generation is the committed one before classifying it (absent or different is obsolete or unknown, never Ordinary), and never publishes through a path that skips the rule. In a marked store, verification without canonical authority waits, retryably.
2. **Classify before any mutation** by the table below, comparing immutable `TierGeneration`: a Superseded or Unknown publication stages, defers or replaces nothing that belongs to another state.
3. **Prepare on demand, outside authority** (object fetch, validation, the row-refresh plan): a path that has no prepared delivery releases the lease and locks, prepares, then re-takes them and classifies again; a failed preparation is settled under fresh authority.
4. **Publish** (and for recovery, hand off) while holding them. A certified generation that is staged or pending publishes only through the verifier, by the same rule under held authority.

Object I/O never happens under the lease. No path re-enters the reconcile while holding locks it reacquires. The no-source fallback (isolated unit stores only) refuses certified work: an intent marked `Required` or a command carrying provenance.

**Proof of supersession of a Tier X:** the lease's state records X `Applied { index: i }` for its owner's current incarnation (recovery), or X is the observer's event already applied at index i ≤ the state's last applied index. X's apply set the pointer to X, and apply is deterministic. So a different committed generation, or no pointer **and** the slab in `removed`, was written after i.

**A Retire X is in force** iff there is no committed pointer **and** the slab is in `removed`.

| Case | Evidence (under the lease) | Observer / replay / reconcile | Recovery (an abandoned op X) |
|---|---|---|---|
| **ordinary, current** | provenance None; committed = X (or the retirement in force) | publish | publish, then hand off |
| **Required, cert present, source exact** | provenance Some; cert binds op; local pointer = source, rows = `source_rows_digest` | the deleted rows (a consuming match) = the cert's drop and the inserted rows hash to the postimage → publish; else defer, typed | the same, then hand off |
| **Required, cert missing** | provenance Some; no matching cert | only an already-published postimage completes (no-op); otherwise defer, typed, and install nothing | only an already-published postimage hands off; otherwise unresolved and fenced |
| **already published** (also after a legitimate hand-off) | Tier: local generation = X, local rows hash = the ring's `postimage_rows_digest` (ordinary: local = X). Retire: no local pointer, no rows, the retirement in force | no-op | hand off; no certificate and no debt read |
| **superseded (proved)** | the proof above | skip, install nothing; the newer state's path converges | release the locks, run the reconcile (installs Y, itself by this table), re-take the locks and lease, require the exact current local postimage of Y, then hand off X. X never runs Y's completion, deferral or fence helpers |
| **unknown** | no lease, owner not current, outcome absent, or an unreadable record | defer, typed; nothing installed | unresolved and fenced; nothing installed or handed off |

**A retirement the ring removed, for a slab the store still holds:**
- **Required authority** comes only from an **Applied** certified-retirement entry for the exact current owner/incarnation/slab in the retained outcomes (bounded scan). Rejected or conflicting entries are no match, and several inconsistent matches refuse.
- **No such entry:**
  - a local empty postimage → a duplicate no-op;
  - a non-empty local preimage in a marked store → fenced (absence of provenance is never deletion authority);
  - Today → unchanged.
- **Ordering:** local retirement → hand-off → acknowledgement → pruning.

### 5. Debt and retained objects
- After a delivery deletes the dropped rows, `retire_debt` retires a version's debt once no **indexed** copy remains, as it already defines it (local copies and any other tiered copy count).
- Retained historical objects were never indexed. They are unreachable through reads (superseded locations, retired ids, reopen loading current pointers only, and now the current-generation-only publication rule).
- They are reported separately (count and bytes, test-only). No object-store capacity is claimed.

### 6. Admission and lifecycle
- **Placement:** a certified step in the existing per-node leader-gated compaction driver, beside 0703's certified LSM step (it stops on a follower), in mode only, with the decision delegated to the data server, as 0703 does. Objects are written under `certified/<owner>/`.
- **Admission:** `may_clean()`.
- **Permits:** a lifecycle permit over the step and any spawned work. Shutdown and join ownership is unchanged.
- **Still contained:** the ordinary tiered retention and table-drop rewrites, LSM with tiered inputs, and tiered slabs held by a follower's store until its node leads.

## Acceptance (fixed map)
1. **The workflow** (three nodes, controlled clock, LocalFs object store, extending 0703's three-node control):
   - obsolete and protected versions; the leader's slabs tiered and their local copies reclaimed; a generation activated;
   - a certified **partial** rewrite, and an **all-obsolete** retirement;
   - exact reads before, after, and after a same-store restart; the exact survivor set; debt retired where no indexed copy is left and kept otherwise; retained objects (count and bytes) reported and still present.
2. **Crash cuts** (the tier-op reopen harness):
   - after upload before proposal;
   - after proposal before a decision (Cancel → rejected candidate, old view);
   - after acceptance before delivery (published by the table);
   - after delivery before hand-off, for a partial drop and for a Retire: completes as already published, including after acknowledgement and pruning.
3. **Object store unavailable:** at the claim (refused) and at delivery (fenced; reads refuse). Missing evidence never becomes deletion.
4. **The shared fix:**
   - the RED (restart and in-process);
   - an observer event for X while the state holds Y: skipped;
   - a stale Retire never retires a slab still pointed at;
   - unknown authority: nothing installed or handed off.
5. **Certificates and provenance:**
   - a certificate deleted before the observer delivers its op: defers, installs nothing;
   - a tampered certificate fences;
   - a generation that moves between decision and allocation writes nothing;
   - apply refuses provenance naming another op, source or slab.

## Not established
- Physical object-store reclamation (ADR 0696).
- Follower-held tiered slabs until their node leads.
- Follower run merging.
- The all-member barrier.
- §8.
- **DC-195, the tiered layout collision** (liveness; fails closed): a replacement object restarts offsets at 0, so a survivor written in the same parent write as a pruned row (shared HLC, equal length) can take that row's exact location, and the layout refuses the rewrite (`StaleLocation{LayoutCollision}`) before upload or proposal, keeping the old view. Such a slab is never cleaned. Reproduced for both the certified arm and the ordinary retention rewrite on this candidate; that the ordinary path predates this ADR on main rests on its unchanged source. Both reproducers assert today's refusal. A separate ADR changes the layout model; until then this ADR makes no unrestricted tiered-cleanup claim. The workflow's survivor is written in its own parent write.
- **DC-196:** one unexplained `CertifiedEvidence` fence under load in the after-acceptance crash cut (first 2d run, fault not captured), not reproduced since; registered, with its diagnostic armed.
