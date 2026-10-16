# ADR 0101 - Root2 administrator key lifecycle: recovery role, key epochs, control freeze and committed retirement

## Status

Accepted and partially implemented (2026-10-16).
- Profile 3, key epochs and freeze are on main: d9f3c8b5 (C-S4a), 722a951b (C-S4b1), and 524db8a2 (C-S4b2, freeze).
- Retirement (C-S3a/b) is selected and in progress.

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
- C-S6: the compromise-audit tool and runbooks, including M2.
- C-S5: M2 handover.
- Key custody: per-role key files with owner-only permissions, an offline recovery key, and encrypted backups. Required before real data.
