# ADR 0107 - A recovered replacement takes part in certification

## Status
Accepted for implementation, 2026-11-16 (member replacement, the gate-3 remainder split from ADR 0106 by Codex 01a1e1e4). Design GO, Codex 01a1e2d3, with binding clarifications folded in below. Not delivered.

## Context
- **The barrier.** After ADR 0105 promotes J, every target names (N, J), and `commit_generation` needs (N, J)'s own report (generations.rs:250-266). J never reports: it is ControlOnly and runs no data server, so `certification_step` never runs for it (control_only_node.rs:1-10). No generation has committed since promotion, so no covered decision record is reclaimed while a replaced slot exists. Retained claim bytes are capped at 256 MiB at admission (decisions.rs:65): a blocked certification ends in refused admission, not unlimited growth.
- **The plan check.** Even run, `certify_pending` returns nothing for J: it requires `child.plan.contains(&local)` (certification.rs:301-305), and every child written before promotion names (N, I).
- **Order.** The recovery driver compares the current certificate with the historical record before anything else, and stops with `RecoveryUnsupported` when it moved (replacement_recovery.rs:253-262). The first commit J itself enables would stop it.
- **What exists.**
  - ADR 0106 gave J a validated base at G, every owed child Complete through K, delta repair after K, and `RecoveryProved`.
  - `in_recorded_ancestry` (decisions.rs:1653) and `repair_child_in` with `RepairContext` work on the restricted store.
  - `ForwardedDecision::Report` is admitted when `reporter.node == sender` (decision_proposals.rs:496-501), and `record_report` checks the signature against the bound (N, J) key and the current incarnation (generations.rs:193-231).
- **Physical proof today.** `build_candidate` proves the stored rows of winners from children, but not of winners inherited from the active base (`source = None`); it relies on that base's validation at this start. Equal report roots cross-check inventory commitments; they do not prove rows exist.
- **Timing.** A target covers only bodies whose retry keys the floor retired: E = `DECISION_EXPIRY_MS`, 3,600,000 + 300,000 + 60,000 ms, about 66 min after issuance (decisions.rs:41). In-process tests jump the floor; a real daemon cannot, and this ADR adds no daemon override.
- **Trust model (unchanged):** trusted nodes, crash and stale-state faults.

## Decision

### C1. When J takes part
Every step, J takes part only if all of these hold:
- ADR 0106 D1: J is its slot's current, promoted, bound replacement;
- the applied `recovered` record for its token reconciles exactly with the statement saved in J's store;
- this process has validated J's stored data at J's active certificate (C4).

It is not required that the historical recorded G equal the current G. Before the record, J reports nothing, so 0106's premise that G is fixed until the record stands. A certificate that moved while no record exists stays the fault stop `RecoveryStale`.

### C2. Plan membership by ancestry
In the closed set, a child counts for the local store (N, X) iff its plan names N at an incarnation in X's recorded ancestry (`in_recorded_ancestry`, ADR 0106 D4). This is copy eligibility: J must still hold, prove and sign for its own copy; no credit is inherited. A store that was never replaced has only itself as ancestry, so survivors see no change; the same rule serves J after gate 4. A child whose plan names no incarnation of N is still "this store holds no copy" (unchanged).

### C3. One store-level certification core
The lifecycle moves from `WendelonDataServer` methods into one core over (slab, state machine, signer, repair context, retention policy, progress state):
1. activate the committed candidate;
2. build;
3. stage or restage durably;
4. sign a report only from the durable staged metadata.

- **The data server** delegates, with its test seams and its maintenance report path unchanged.
- **J's recovery task** calls the core on the restricted store under the store owner's lifecycle permit, and forwards the signed report through its existing bounded forward (`DECISION_RPC_DEADLINE`).
- **Replay rules unchanged.** The report is skipped while applied state holds it; a lost or stopped answer never means "not recorded"; only the exact staged candidate is restaged; no index mutex or ownership permit is held across the network wait.
- **Answers to J's report never stop recovery.** A stale or superseded target, a refusal or an unknown outcome is reconciled on a later step. Only loss of D1 authority (checked every step) or a store fault stops it.

### C4. Live ordering and progress state
**Progress state.** It belongs to the store and to this process: the generation built this run (`built_gid`), validated, and behind the boundary. It is created once per recovery task, never per step, and reset only by the existing rules: activating a candidate not built this run clears validation.

**Each step after authority and exact reconciliation:**
1. **Activate.** If the current certificate moved past J's active base, activate J's staged candidate for it first (the core's `activate_committed`), before any repair or validation at the old G.
   - **Invariant:** a commit needs J's report, so J has durably staged that certificate, and J cannot report a later target until its active base is that target's recorded predecessor. So J's active base is either the current certificate, or that certificate's recorded predecessor with the certificate staged. The link is the recorded predecessor, not gid + 1: abandoned target gids may be skipped.
   - **Any other state** (missing or mismatched staging) sets J behind the boundary and stops recovery. It never resets or recopies an established base.
2. **Validate** if this process has not validated the active base:
   - repair the owed children through the record's K, then check them, with every owed child Complete and its rows verified;
   - stream the active base in fixed pages (1024 entries, 256 accumulators) against the CURRENT certificate's chunks, or the empty sums at genesis, with every entry stored (ADR 0106's fixed-page validation, unchanged).
   - Children covered by a commit were erased; the base now carries them, and no erased intent is required.
3. **Delta:** repair owed children after K, as in ADR 0106, by decision position over retained records. A pass covers what its read selected; it never waits for later writes.
4. **Certify,** only when the delta pass found nothing missing: the core builds, stages and reports the pending target.

This order holds both while running and after a reopen. The record is never re-sampled or rewritten.

### C5. Physical proof at J's report
J proves every winner of a new candidate from its own stored data before staging: child winners as today, and **inherited winners too** (`entry_is_stored`, the same check as base validation), once per new candidate. Damage present before that proof point blocks J's report, and with it the commit and any erasure. Damage occurring after a truthful report is not claimed detected, and this is not general corruption repair. The data server keeps its current proof (it relies on its start validation); that is unchanged. The whole-candidate build holds the candidate's entries in memory, as the data server's build already does; no streaming rewrite is made here.

### C6. What J still does not do
J stays ControlOnly and NotServing. It runs no lease, coordination, election, serving, tier upload or reclaim. Gate 4 owns serving activation, the role change, the index rebuilds and the receipts. Old instance I is not current: its reports are refused by the existing incarnation check.

### C7. Formats
No format change: the report, the record and the store metadata are unchanged. Any changed persisted or wire interpretation found during implementation is listed in the source map and bumped in place (pre-release).

## Control map (in-process, controlled clock)
1. **Live commit:** after J's record, a target covering children naming (N, I) (before the loss) and (N, J) (after promotion) commits only after J's own report; the reports agree; covered bodies are erased; J activates and keeps running. **Mutant:** the exact-plan check restored, so no J report and no commit.
2. **No early report:** a target pending while J is promoted but not recorded gets no (N, J) report and no commit.
3. **Physical proof:** a nonzero base. After J's start validation, one inherited base row on J is damaged, then a target is recorded. J reports nothing, nothing commits, nothing is erased. **Mutant:** without the inherited reprove, J reports and the generation commits.
4. **Saved report, lost answer:** J's report is applied but its answer is lost or stopped; a later step skips or retries the exact report; the commit and activation follow.
5. **Restart after commit:** J stopped after a commit (both before and after its activation). It reopens, activates from its staging if needed, validates against the current certificate (not the recorded G), continues, and the reader oracle returns exactly the acknowledged rows.
6. **Two generations:** a second target commits after J activated the first.
7. **Behind the boundary:** a committed certificate with no matching staging on J stops recovery, without a reset.
8. **Stale report:** a report for a superseded target is refused, and recovery continues.
9. **Survivors unchanged:** the existing certification tests pass.
10. **Daemon (after source clearance):** ONE owning run, option (b) (Codex 01a1e2d3):
    - it waits for a committed certificate that COVERS the selected pre-loss and post-promotion children;
    - then it checks J's matching activation and exact physical rows, restarts J once, and writes and proves one bounded new row, with the historical record unchanged;
    - the expiry reference, an absolute deadline, bounded periodic observation, process ownership, cleanup and seals are declared before launch. The deadline is not renewed, and there is no automatic repeat.

## Consequences
- After a replacement records its recovery, generations commit again, so covered decision records are reclaimed. This does not show that total memory is bounded, that retained state stops growing under arbitrary load, or that §8 passes (§8 remains FAILED and unwaived).
- A replacement that cannot prove its candidate (a damaged row, a needed child with no source) still blocks certification, and now says why in its task's log.
- J's inventory is cross-checked against the survivors' at each commit, before erasure.

## Not decided here
Gate 4 (serving activation, role change, election, lease/HLC bindings, index rebuild and receipts); DC-247 (a replacement's stop leaves no clean-shutdown record; nothing here marks ownership recovery complete); reclaim of I's objects; repeated replacement and source availability beyond ADR 0106 D4; a streaming candidate build.

## As built
- **Core** (`network/data_server/certification.rs`):
  - `StoreCertification {slab, state_machine, progress, build}` with `activate_committed` and `prepare_report` (build, stage or restage, sign from the staged metadata; `None` when there is nothing to report);
  - `CertificationProgress {built_gid, validated, behind}` and `CandidateBuild {repair, retention, now_ms, reprove_inherited}`;
  - a free `under_permit`.
- **The data server** delegates:
  - its progress is its existing flags (the ones its reads and cleanup gate on);
  - `reprove_inherited: false`;
  - its test seams are kept (`CertificationStep`, `CertificationBeforeReport`, `ReaderBeforeRepair` through `CandidateBuild::peer`, `hold_activation`);
  - it reports through `maintenance.report` as before.
- **Shared proofs.** `reader::local_child_in` and `reader::proved_location_in` are free functions; the data server's methods delegate. `proved_version` was removed (only the build used it). `repair::repair_context()` builds the repair context once for both repair and certification.
- **C2.** One ancestry rule in `prepare_report` for every store (`in_recorded_ancestry`). For a store never replaced it equals the old exact-membership check.
- **Driver** (`node/replacement_recovery.rs`):
  - `RecoveryDriver.progress`, created once per preparation task, and `.retention`, the configured policy;
  - `after_record` runs C4's order: activate, then `RecoveryBehind` unless active at the current certificate, then validate once per process at the current certificate through the record's K, the delta, and `prepare_report` with `reprove_inherited: true`;
  - **Codex 01a1e309 R1:** after activation, the certificate is read again, so activation, the active-base comparison and validation use one coherent certificate. A view read before a concurrent commit used to turn that commit's correct activation into a permanent `RecoveryBehind` stop. A certificate that moved after activation's own read is now `Pending`, activated on the next step; missing or mismatched staging still sets `behind` and stops. The control `a_generation_committed_during_a_recovery_step_is_activated_and_recovery_continues` (one-shot seam `PAUSE_AFTER_VIEW`) was RED on 45fd4dbc8 and is GREEN with the fix;
  - `Progress::Report`: the preparation forwards it and never stops on its answer;
  - `RecoveryUnsupported` is removed.
- **Status.** `ReplacementReportWire.certified` carries the current certificate (gid, cut). `replace status` prints `; certified gid C through T` after the recorded recovery only. The admin answer's extension gains this field in place (pre-release), as ADR 0106's `recovered` did; there are no other format changes.
- **Test support.**
  - `RecoveredRowsForTest.active_gid` and `.base_rows`. Every base row entry is now verified, including one a Complete child also holds.
  - Seams `HOLD_ACTIVATION` (by store directory) and `LOSE_REPORT_ANSWERS` (by data directory).
- **Architecture census:** `after_record` is a lifecycle-permit site owned by the spawned preparation task (`SpawnedTask`).
- **Controls:**
  - `a_recovered_replacement_reports_and_its_generations_commit` covers controls 1, 2, 4, 5 (before and after activation), 6 and the reader oracle;
  - `a_replacement_with_a_damaged_inherited_row_does_not_report` covers 3;
  - `a_replacement_without_its_committed_staging_stops_without_a_reset` covers 7;
  - existing certification tests cover 9;
  - control 8 (a superseded target's report) is not separately exercised: the answer is never classified (`Progress::Report`), and control 4 covers a lost answer;
  - control 10 is the extended daemon workflow (its certification phase awaits a certificate whose cut reaches every selected receipt's decision index).
- **Mutants**, each RED on its control:
  - M1, the exact plan check: no commit;
  - M2, no inherited reproof: the damaged replacement reports;
  - M3, never report (main's behaviour): no commit;
  - M4, the old `RecoveryUnsupported` order: the first live commit is never activated.
- **Fixture finding.** The controlled clock's floor jumps move the ring clock about `E` ahead, so a range taken over after the loss begins with a tenure floor that far ahead, and a claim there is refused `CommitFromFuture`. The tests choose rows in ranges whose floor is current (`write_current`). In production the floor is the stamp minus `E`, which keeps the ring clock at physical time.
- **Limits.**
  - J runs no certified cleanup (ADR 0699 S5b): what cleanup would remove stays on J's store until gate 4.
  - The candidate build holds the candidate's entries in memory, as the data server's does.
  - After an unclean start, activating a candidate not built in this run requires base recovery, as for the data server; DC-247 is unchanged.
