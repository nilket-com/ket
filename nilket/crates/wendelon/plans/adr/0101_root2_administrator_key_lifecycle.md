# ADR 0101 - Root2 administrator key lifecycle: recovery role, key epochs, control freeze and committed retirement

## Status

Accepted; code complete (2026-10-17).
- Profile 3, key epochs and freeze: d9f3c8b5 (C-S4a), 722a951b (C-S4b1), 524db8a2 (C-S4b2).
- Committed retirement: 6388e2f6 (C-S3a); owner binding and terminal export: c059c3fb (C-S3b).
- C-S6, the compromise-audit tool and runbooks including M2: planned (contract accepted 2026-10-18; implementation pending). See the amendment below.
- Open, and required before real data: C-S6 implementation, M2 handover (C-S5), and key custody.

The operator chose the recovery-role model (M1 option a) on 2026-10-15, and decided the same day that Unfreeze requires two roles.

Everything here is dormant: no proposer, serving or runtime path enables it. Real customer data stays blocked until key custody (below) and the M2 handover runbook exist.

## Context

Before this ADR, a Root2 root was bound for life to one admin key and at most one governance key (`AdminManifest2`; manifest profiles 1 and 2). That gave no routine rotation, no single-key recovery, and no way to stop a compromised root.

Allowing each of the two roles to rotate the other unilaterally fails: a compromised admin key can rotate governance first and then use its own replacement to rotate admin, so recovery becomes a race. Retirement existed only as a provisioning-local owner-directory event. Replicas holding an older bound tuple never observed it.

## Decision

1. **Profile 3 manifest** (241 B): admin, governance and recovery roles.
   - The three ids and three keys are pairwise distinct, and every key passes `check_enrollable_admin_key2`.
   - Fixed masks: admin 3, governance 0x1FF, recovery 0x1F. The recovery bits are Freeze, Unfreeze, RotateAdmin, RotateGovernance and RetireRoot.
   - The profile is explicit (tag plus exact length), never inferred.
   - Root state is classified fail-closed by control_len ∈ {0 (legacy non-manifest), 137, 189, 241}; any other value is refused.
2. **Key epochs.** Each role's key is a sequence: epoch 0 is the manifest key, and successors are recorded in state with their activation coordinate. Rules:
   - every signed form is verified against the key active in the **pre-entry** state;
   - legacy signed forms implicitly claim epoch 0 and are never tried under a successor;
   - every successor key is unique across all roles and epochs in the root (so a rotation envelope is single-use);
   - at most 64 successors per role.
3. **Authority table.**

   | Command | Authorized by |
   |---|---|
   | RotateAdmin, RotateGovernance | recovery |
   | RotateRecovery | joint admin + governance |
   | Freeze | recovery |
   | Unfreeze | recovery + (admin or governance) |
   | RetireRoot | recovery, or joint admin + governance |

   Admin and governance never rotate each other.
4. **Control freeze.** While frozen, admission statements and the nine governance commands are refused with consumed `Denied(23)`. This is checked before authorization, on both reducer paths.
   - Rotations, Unfreeze and RetireRoot stay allowed.
   - Reserve, Finalize and Cover are unchanged.
   - Freeze and Unfreeze bind the coordinate of the previous transition, so a replayed envelope is refused (`Denied(24)`).
   - This is a control freeze, not a stop-all-writes switch.
5. **Committed retirement.**
   - Retirement is a terminal state recorded at its committed coordinate. The state keeps the first-retire coordinate, entry fingerprint and command digest.
   - After the coordinate, every other Root2 command is a consumed `Denied(25)` after the structural and binding checks, with no authorization evaluated.
   - `install_admin2` refuses a retired state; replay confirms it with `confirm_terminal2`.
   - Owner-directory retirement of a profile-3 root must consume the committed observation.
6. **Formats.** The joint role proof is the 224 B `VISROL03` sidecar (it fits the existing 296 B cap). State versions:
   - V4 is the legacy format;
   - V5 adds a role extension, used only when it is non-empty or frozen, and stays for life once frozen;
   - V6 is used only for a retired root.

   Older readers refuse newer versions.

## Rationale

- A third, offline role turns single-role compromise from a race into recovery.
- Pre-entry verification and globally unique keys give deterministic replay and single-use rotation without extra state.
- Placing freeze before authorization makes containment independent of the compromised key.
- Retirement belongs in the replicated log, so every replica learns it at the same coordinate.

## Consequences

- **A compromised recovery key can take the root**: it rotates admin and governance. Recovery-key custody is therefore critical, and losing it means M2 (succession to a new root).
- Containment is forward-only: entries accepted before detection are not undone. A compromise-audit tool (C-S6) lists them for review.
- A replica that has not yet replayed a coordinate has not observed it (bounded lag), so the serving and proposer work must account for it.
- Profiles 1 and 2 keep provisioning-local retirement only; real data requires profile 3.
- Mixed old/new binaries on one cluster are unsupported.

## Open questions

- C-S3b: the final exported record of a retired root (TerminalRecord2) and owner-binding byte formats.
- C-S6: planned; see the amendment below.
- C-S5: M2 handover.
- Key custody: per-role key files with owner-only permissions, an offline recovery key, and encrypted backups. Required before real data.

## Amendment — C-S6 compromise audit and runbooks (2026-10-18)

Contract r2 plus Addendum A, accepted 2026-10-18. Plan only; implementation pending.

1. **Audit tool** (`wendelon audit root2`): a deterministic, offline, read-only fold over the node's retained Control Raft log for one root.
   - The scanner opens segments `O_RDONLY` and never repairs or truncates. A malformed, checksum-failing or gapped record in the examined range is invalid; a torn tail past the stop is reported, not an error.
   - The fold shares the production reducer and verifiers; it never reimplements authorization, never calls `MarkStore2::advance`, and never constructs a writing `WendelonLogStore`.
   - It reads raw `Root2Carrier1` requests, never a production Root2 state cache, the volatile log cache or the `VISJRN02` journal.
2. **Root key** is the full bound `AdminContext2`, not `RootIdentity2`. The operator supplies the trusted profile-3 manifest out of band; the tool binds it to the root's install entry in the log. A missing install entry, failed binding or pin mismatch is invalid, with no partial binding.
3. **Attribution.** Each row carries `(root key, LogId, EntryIdentity2 fingerprint)` and two independent views:
   - `effect`: the reducer's verbatim output (applied, or consumed `Denied(n)`);
   - one record per signer slot: `{claimed_role, claimed_epoch, crypto_valid, active_at_entry}`. `crypto_valid` is checked against that epoch's key in the replayed key history (keys are never discarded after rotation), with the production signature primitive and preimage; an epoch absent from the history is `crypto_valid = false`, `unknown_epoch`. `active_at_entry` is the reducer's pre-entry view. No sidecar, an unparseable sidecar or a form naming no role gives one `unattributable` record.

   Neither `effect` nor `active_at_entry` is used to infer `crypto_valid`. The audit verifies where the reducer did not (frozen or retired roots).
4. **Match classes.** `suspect_use`: some record is `crypto_valid` by a suspect `(role, epoch)`, regardless of `active_at_entry` and `effect`. `suspect_claim_unverified`: some record claims a suspect identity with `crypto_valid = false`; it is never counted as use. Suspects are `--role R --epoch E` (repeatable) or `--key <file>`, which must resolve to exactly one `(role, epoch)` in the root.
5. **Containment, per suspect identity**, computed from the replayed history independently of first use: `superseded_at` (the rotation that replaced the epoch, or null), `retired_at` (shared, or null) and `freeze_intervals` (`[freeze, unfreeze or null]` pairs, shared). Freeze is partial and reversible, so it is reported as intervals, never as one boundary. Each `suspect_use` row carries per-identity `after_superseded`, `after_retired` and `in_freeze_interval`; a row after supersession or retirement whose effect is not a denial gets `finding: true`.
6. **Coverage is explicit.** The final record names the snapshot / last-purged boundary, the first and last examined LogId, the local committed/applied stop, and counts of uncommitted entries and torn tail past the stop. A missing prefix or gap between the install entry and the stop is partial. The stop is local, not a quorum-currentness claim.
7. **Exit codes** (typed; text is never status): 0 complete; 1 invalid history or binding; 2 I/O, usage or trust input; 3 partial coverage; 4 unresolved or ambiguous suspect. A `complete` coverage record is never printed with 1, 2 or 4.
8. **Runbooks** in `plans/ops/root2_compromise.md`: admin key compromised; governance key compromised; both; recovery key compromised (a race, otherwise M2); recovery key lost; M2 succession to a new root, ending at "new root provisioned; old root retired and audited" (C-S5 owns the handover).
9. **Retention.** A full-coverage audit of a real-data root requires `ControlRetention::Full`. Under `Compacting`, once compaction passes the install entry the audit is partial (exit 3); a snapshot does not restore deleted signer evidence. Making `Full` mandatory belongs to key custody and runtime enablement.

Out of scope: undoing entries, C-S5, key custody, runtime enablement, automatic detection and quorum-currentness proofs.
