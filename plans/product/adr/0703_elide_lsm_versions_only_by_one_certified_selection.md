# ADR 0703: elide LSM versions only by one certified selection

Status: proposed, 2026-11-09 (Claude, Codex). Implementation lands with this plan. The design was cleared by Codex (01a1c0fd) over draft r1 and addendum 1. Obligation: ADR 0699 S5b, Codex constraint C (`reviews/dc54-index-continuity-r1/l2/s5b/CODEX-1.md`), left open by the S5b landing ruling (CODEX-3, CODEX-4). Review record: `reviews/lsm-certified-elision/`.

## Why
In ParentDecisions mode the only path that may drop a stored version is S5b's certified cleanup of one sealed local slab. LSM compaction stays contained: `pick_lsm_compaction_plan` returns None when `versions_contained()` (`lsm_driver.rs:2263`). So versions that S5b certified as obsolete are reclaimed only from sealed local slabs. LSM runs keep every version they hold, and their debt never retires.

Constraint C sets the bar:
- **One selection per job:** one immutable captured selection for every phase of an LSM job, keyed by logical version identity;
- **Winners are never dropped:** the ordinary retention and tombstone pass never runs first and discards a certified winner;
- **Non-mode behaviour is preserved;**
- **Recovery is exact:** it verifies the exact prepared outputs or selection, or abandons with the inputs retained, and never blesses missing rows because the generation advanced;
- **The guard goes last:** the mode's picker guard stays until the whole path, recovery included, uses the rule.

## Premises (from source, base 999f96fec)
- **Today, production elision drops nothing.** Both production pickers set `retention_floor_snapshot` to `zero_hlc()` (`lsm.rs:913`, `:964`), nothing raises it, and mode refuses any MVCC retention policy but `Disabled` (`node.rs:545`).
- **The floor is read at six sites:**
  - output planning (`lsm_worker.rs:242`);
  - the two sizing passes (`lsm_driver.rs:3519`, `:3554`);
  - the two verification passes (`:2865`, `:2878`);
  - the bridge's own "no Drop at or above the floor" check (`:2805`).
- **The bridge (`lsm_single_row_bridge_digest_summary`) proves consistency, not safety.** It compares applications of the rule with each other, with the written bytes and with the visible domain. In mode, safety comes from the certified facts.
- **Recovery never recomputes elision.** It accepts outputs by identity:
  - the intent's row digests (`lsm_rows_digest`);
  - its file (len, blake3) pairs and run summaries;
  - the commit digest.

  `publish_applied_install` pairs output rows to input rows by (tenant, key, hlc) with a non-consuming `find`. Record rows carry no table id.
- **One redb holds both sides.** The install intent and the op row are written in one write transaction (`allocate_ops`). That redb also holds `base_meta_v1`, `cleanup_debt_v1` and `slab_records_v1`.
- **LSM plans may include tiered inputs.** Tiered storage stays contained in mode.
- **Job ids are deterministic** (epoch·4 + level + 1).
- **Only the cluster leader compacts.** LSM ticks run only on the Raft leader, on its own store, in the `LsmCompactionService` task (`LeaderGatedLsmCompactionDriver`).

## Decision
**In mode, an LSM job drops a version iff one certified selection, captured once for that job and bound to its exact inputs, selects it. A mode job's plan carries no retention floor, so the floor rule cannot run on it.**

### 1. The plan carries a typed elision policy
`CompactionPlan.retention_floor_snapshot` becomes `elision: PlanElision`:
- `RetentionFloor(HybridTimestamp)`: today's rule, unchanged;
- `Certified(Arc<CertifiedSelection>)`.

`apply_elision(records, &PlanElision)` dispatches:
- `RetentionFloor` runs today's rules byte for byte;
- `Certified` marks a record `Drop { reason: Certified }` iff the selection names its (tenant, key_identity, hlc, kind), and every other record `Survivor`.

All six sites read `plan.elision`. For `Certified`, the bridge's floor check becomes: the Drop decisions are exactly the input records the selection names. `CompactionPlan` no longer derives `Deserialize`.

### 2. The selection
`CertifiedSelection` is built only by `CertifiedSelection::decide(job, facts, evidence)`.
- **The job:** `LsmCertifiedJob` is the claimed plan with its pins and claims, plus its lifecycle-resolved entries. `job.versions()` gives the sorted, distinct `VersionRef`s.
- **The facts:** `CoherentFacts` read for exactly those `VersionRef`s in one index transaction. Facts read for another set are refused.
- **The evidence:** `ContentEvidence` over every stored copy, as S5b builds it.
- **Membership M** holds distinct (tenant, table, key_identity, hlc, kind) entries. A version is a member iff every copy of it in the job satisfies all of the following (S5b's local rule, applied per copy):
  - its facts carry debt under exactly one table, not ambiguous; that table is the member's;
  - its frame is well formed, and a table its frame stamps, and a table its durable row attributes, if any, are that table. An untyped copy takes the debt's table: ParentDecisions writes its data rows untyped (no stamped table, no durable attribution, so an `AmbiguousTable` lifecycle), and S5b reclaims them on the same terms. Untyped means only a frame that is not multi-row, or carries no table column. A multi-row frame whose batch or table column does not decode is malformed, never untyped, and its version is kept;
  - no owner names it;
  - no active base entry of that table is at its HLC;
  - its content evidence matches the debt and the kind: every stored copy has the kind, and every row copy has the debt's content digest under that table's descriptor.

  So M has at most one member per (tenant, key, hlc), with the kind of every copy. A conflicting pair (two kinds or two contents at one (tenant, key, hlc)) has no evidence, and every copy of it is kept. `MigratedRangeTombstone` markers are never members.
- **No tombstone-coverage override.** The bridge's group override (a tombstone next to an ambiguous or mixed lifecycle keeps its whole group) exists because the floor rule infers obsolescence from tombstone coverage. A certified drop is proved by its own debt and content evidence, so the override is the floor rule's alone: `decide` does not apply it and the verifier applies it only under `RetentionFloor`. Applied to mode, where every deleted key holds an untyped row and a typed tombstone, it would keep every deleted row forever. (The design applied it at capture; found in implementation, see the review record.)
- **Occurrences D** are physical, with multiplicity: every input record row (`PersistentRecordRow`, its exact durable encoding) whose (tenant, key_identity, hlc, is_tombstone) is a member. A member is all-or-nothing inside the job. Copies outside the job are untouched.
- **The canonical form:** one function, `canonical_drop(rows, table_of) -> (drop_digest, drop_rows_digest, drop_count)` in `storage::lsm`, with one deterministic encoding and sort, used by the bridge and by recovery:
  - `drop_digest` is blake3 over the sorted (tenant, table, key_identity, hlc, kind, count) of each member present among the inputs;
  - `drop_rows_digest` is `lsm_rows_digest(D)`;
  - `drop_count` is |D|;
  - an occurrence's table is the table its durable debt record names (at capture, the member's; in recovery, read from `cleanup_debt_v1` for exactly the residual versions, one unambiguous record each), never a catalog. The residual rows are copies, and debt retires only once no copy is left, so their records stand through recovery. A durable row attribution, where present, must agree. A residual version without such a record refuses.
- **Job binding:** the selection carries `active_gid` and `job_fingerprint` = blake3(job_id, manifest_epoch_pre, sorted input slab ids, the claimed inputs' `lsm_input_rows_digest`).
  - `finish_certified_lsm` refuses a selection whose fingerprint is not the job's.
  - Install refuses if the intent's `local_input_rows_digest` differs from the selection's.

### 3. Admission: leader-only, in the existing LSM service
- **The ordinary picker still returns None in mode**, and tiered inputs stay contained.
- **The admission path:** in mode, `LeaderGatedLsmCompactionDriver` holds the data server's certified elider.
  - Its tick checks `is_leader()`, the explicit admission check, then delegates to `certified_lsm_step`.
  - That step admits a job only if `may_clean()` holds.
- **The three stages:**
  - **claim:** `claim_lsm_for_certified_elision` picks with an `eligible` predicate that refuses any plan naming a slab the manifest holds as tiered. It takes the existing pins and claims, revalidates, refuses counter inputs, and resolves the entries;
  - **decide:** the driver reads the facts and evidence and decides;
  - **finish:** `finish_certified_lsm` runs the existing prepare → worker → write → bridge → control-ring install path with `PlanElision::Certified`.
- **What refuses:**
  - the certified tick refuses outside mode, and while an orphan certificate fences the store;
  - the ordinary tick never builds a `Certified` plan, and in a contained store the ordinary runner refuses any uncertified plan (closing the gap where only the picker was guarded);
  - the local, ring-less install path refuses a `Certified` plan.
- **An empty selection** is a valid lossless merge.
- **The maintenance loop is not involved:** the step never runs there, so the clock probe never waits on an LSM job.
- **Lifecycle permits:**
  - the step holds a lifecycle permit for the whole job;
  - each task the certified install path spawns takes its own permit before spawning, moves it in, and completes it at its end. A permit that cannot be taken refuses through the existing failure path;
  - an aborted step drops its permit uncompleted, which revokes the clean record;
  - Today's LSM path is unchanged.
- **Already-accepted work** follows §5 across leadership changes.

### 4. Install binds the certificate atomically
In the `allocate_ops` transaction that writes the op row and the intent, a certified job also:
- **checks:** it requires `base_meta_v1.active_gid == selection.active_gid`. On a mismatch, nothing is written, the job abandons through the existing pre-rename failure path (temp outputs removed, inputs retained), and the next step decides again;
- **records:** it writes `lsm_certified_selections_v1[job_id]` = {job_fingerprint, active_gid, drop_digest, drop_rows_digest, drop_count, intent_binding}. `intent_binding` is a blake3 over the job id, the commit digest and the intent's input and output row digests. It is stable across the intent's phase transition, whose bytes change, and identifies exactly one install.

**Clearing and retries:**
- Every transaction that clears the intent clears its certificate, both by exact match, on every path, completed-control recovery included.
- Allocation is never retried against an existing certificate. `allocate_ops` refuses an existing intent for the job before any certificate is written, and a certificate already present for the job refuses too. Nothing is overwritten, and recovery, not allocation, owns retries.
- An orphan certificate (no intent with its job id) found at open fences certified LSM on that store: no certified job is admitted and the job id is not reused. It is reported, and only an operator action clears it.
- A mode intent without its certificate is fenced.

`LsmInstallIntent` does not change.

### 5. Recovery follows the recorded outcome and certificate; it never re-decides
- **Certified jobs use only Control intents.** A Local intent with a certificate is fenced. No Prepared-phase rollback applies, and no branch reads the gid.
- **`recover_abandoned_install` keeps its protocol:**
  - `ViewOutcome::None` → `cancel_op` with the original ticket → read again;
  - authoritative Rejected with no trace → `undo_abandoned_install`, clearing the certificate with the intent;
  - Rejected with a trace, Conflict, or None after the Cancel → fenced;
  - Applied → `finish_applied_install`, which first requires the matching certificate (`intent_digest` and `job_fingerprint` equal to this intent's) before any publication or clear, even when the output already looks exact.
- **On the publish path,** a certified intent's pairing is a consuming multiset match:
  1. index the input rows by (tenant, key_identity, hlc, is_tombstone), with counts;
  2. let each output occurrence consume one; an output with nothing left refuses;
  3. require that no residual identity appears among the outputs;
  4. require `canonical_drop(residual, input-frame tables)` to equal the certificate's triple, refusing any residual whose frame yields no table.

  The existing output row digest check is kept exactly, and nothing is deduplicated. Today intents keep the existing pairing.
- **At open,** the durable mode obligation is read first: the store's ParentDecisions marker in its data directory (`decision_mode::store_is_marked`). An unreadable marker refuses the open; unknown is never taken for Today. A marked store is contained from its open, before any reconciliation.
  - A control intent without ops (`reconcile_completed_control_lsm_intent`) needs a certificate that binds it exactly, and in a marked store a compaction install (one that retires inputs) without one stays fenced. This happens before its exact-postimage check and clear.
  - A Local intent stays fenced if it has a certificate, and in a marked store always: a marked store installs only certified control compactions.
  - Intents with linked ops stay fenced for the deferred op recovery, which applies the same rule.
  - GC marks and removals retire no inputs and carry no certificate; their recovery is unchanged.
  - An unmarked (Today) store's legacy recovery is unchanged.
- **Permits:** recovery's spawned undo and finish of a certified install each take a lifecycle permit where they are spawned.

### 6. Generation safety (constraint D, from existing checks)
gid equality at the intent is a freshness check between capture and install. Safety rests on existing checks:
- **Immutable claimed inputs:** pins, claims, `revalidate_lsm_plan_after_claims`, and the intent's input digests and file identities.
- **Owner-before-append with exact-version refusal:** `begin_parent_intent` refuses `ForeignVersion` while an unowned version has stored copies (`parent_intents.rs:241`, over `slab_records_v1`).
  - The job's input rows stay there until the install completes, so a selected version cannot gain an owner between capture and install.
  - Afterwards, copies outside the job keep refusing.
  - A version re-owned once no copy remains is a new admission, kept by the ownership rule.
- **No new base winner from unowned debt:** `build_candidate` (`certification.rs:449`) takes, per key, the newest of the active base entry and the closed children's proved manifests. A debt version with copies is neither.

So an Applied job is never abandoned because the gid advanced.

### 7. Compatibility
- **The store marker moves to `parent-decisions-v7`, and v6 is retired.**
  - A pre-0703 binary refuses a v7 store in both modes (`read_marker`, "of another version").
  - This binary refuses v6 stores, which need a fresh replacement by the operator. Nothing resets or migrates automatically.
- **WAL, snapshot and frame formats and the capability bits are unchanged.** The certificate is local. This is not a claim of rolling-upgrade support.
- **Today stores** carry no marker and never write certificate records. The certificate table itself is initialized on every store, as the intents table is.

### 8. Debt retirement and non-mode behaviour
- **Debt:** once an install removes an input's key-index rows, `debt_sweep_step` retires a certified version's debt when no copy, owner or base remains. A version still held outside the job keeps its debt.
- **Today:** Today plans carry `RetentionFloor` with their current floor, and the Today bridge, install and recovery are unchanged.

## Controls
**The acceptance test, `a_certified_lsm_merge_drops_exactly_the_certified_versions`,** runs on a three-node mode cluster with a controlled clock, reusing the S5b workflow fixture.
1. **Setup:**
   - overwrite a fixed key set across generations G1 → G3, so obsolete versions sit in several L0 runs of the leader's store;
   - include one key per keep class: base, owned, unlisted, ambiguous table, content mismatch, a tombstone-and-mixed-lifecycle group, a conflicting pair inside the job, and a version with a copy outside the job;
   - include one selected version with identical copies in two input runs.
2. **Run** one `certified_lsm_step`.
3. **Assert:**
   - the outputs are exactly the inputs minus D (the duplicate dropped twice);
   - every keep class survives;
   - reads are exact before and after;
   - the visible digest is equal;
   - the selection is non-empty;
   - `debt_sweep_step` retires exactly the versions with no copy left.
4. **Report** distinct versions and physical bytes separately.

**Crash and generation controls** (`ControlLsmInstallCut`, `reopen_install_at`). Each case runs a certified job with mixed rows, reopens the same store, and requires inputs retained or the exact recorded postimage, with exact reads:
- before rename;
- after rename before acceptance;
- after acceptance before publication;
- after publication before the intent clear.

Plus:
- **an activation between decide and install:** no intent, abandoned, inputs retained;
- **an activation after the intent is durable, then a crash at each later boundary:** the recorded outcome and certificate decide, never a recomputed selection;
- **an Applied-unpublished reopen with a tampered drop set:** fenced;
- **a mode intent without its certificate, and an orphan certificate:** fenced;
- **the ordinary tick in mode:** None;
- **a frame's table stamp:** typed, untyped (not multi-row), and a multi-row frame with a wrong-typed table column read as malformed, never untyped;
- **open in a marked store:** a completed certified install whose intent names no operation and whose certificate is gone stays fenced, as Control and as Local; in an unmarked store the same Control intent reconciles as before;
- **`apply_elision` with `Certified`:** exactly the selected records drop, under every lifecycle.

**Today's controls** (elision, `dc123_*`, `dc124_*`, partial survivors, crash cuts, recovery) pass, with only mechanical call-site edits.

## Not established
- Capacity or physical-byte bounds: the acceptance test is an algorithm control.
- Follower reclamation through LSM, since only the leader compacts. It is registered.
- Tiered reclamation, which stays contained.
- Rolling-upgrade support.
- Any change to `survivor_policy`, equal-HLC conflict resolution, or the S5b local rule.
