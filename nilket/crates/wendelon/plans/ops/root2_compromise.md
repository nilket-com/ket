# Root2 compromise response runbooks (ADR 0101 C-S6)

One procedure per compromise case of a profile-3 Root2 root: admin, governance, both,
recovery compromised, recovery lost, and M2 succession to a new root. Each section gives
the detection signal, the containment commands and their authority, the audit invocation,
how to review its rows, and the exit criteria.

Status: Root2 is dormant. No proposer or serving path exists, so the containment
commands below have no submission tool yet: each step names the exact Root2 command, its
proof and the reducer outcome a reviewed submission must observe. The audit tool is real
and is the verification step of every section. Key custody is out of scope here; these
steps assume the operator holds the uncompromised role keys.

## Preface

### Retention precondition

A root holding real data runs every replica with `ControlRetention::Full`
(`[storage] control_retention = "full"`). Under the default `Compacting`, once a snapshot
and purge pass the root's install entry the audit can only report **partial** coverage
(exit 3): a snapshot carries current state, not the deleted signer evidence. Making `Full`
mandatory for real-data roots belongs to key custody and runtime enablement.

### Where to run the audit

The audit stops at the node's **local** committed position (the recovered checkpoint),
which is not a quorum-currentness claim. Run it on a replica whose `stop_position.index`
is at or past the index of the last containment entry you submitted; if it is not, wait
for that replica to apply further, or pick another replica, and run again. Stored entries
past the stop are counted in `uncommitted_past_stop` and never audited.

The audit is offline and read-only. It opens every Control artifact read-only, never
repairs or truncates a torn tail, writes nothing and takes no data-directory owner lock.
Run it against a stopped node's directory, or a filesystem snapshot or copy taken while
the node is stopped. A run beside a live node is best-effort: re-run any exit 1 or 3 from
a live directory on a quiesced copy before acting on it.

### The audit command

```text
wendelon audit root2 <data-dir> --trusted-manifest <manifest-file> \
    (--role <admin|governance|recovery> --epoch <n>)... | --key <key-file> \
    [--suspect-only]
```

- `<data-dir>`: the node's data directory (the one holding `control.wal*`).
- `<manifest-file>`: the root's 241-byte profile-3 manifest, from the operator's own
  out-of-band copy, never from the node. The tool binds it to the root's install entry in
  the log; any mismatch is exit 1.
- Suspects: one `--role R --epoch E` pair per suspected signer identity, or one `--key`
  file holding the suspected Ed25519 public key as 64 hex digits. A key must resolve to
  exactly one role and epoch of this root, or the audit exits 4.
- `--suspect-only` prints only the `suspect_use` and `suspect_claim_unverified` rows,
  plus the final record.

Output: one compact JSON object per line. Each `entry` row is one Root2 entry, in log
order, with `root` (the full bound administrator context), `log_id`, `fingerprint`,
`command`, `effect` (the reducer's verbatim result), `signatures` (one record per signer
slot: `claimed_role`, `claimed_epoch`, `form` (the proof format: `VISROL03`, `VISADG01`,
`VISADS02` or `VISADS03`), `slot`, `crypto_valid`, `active_at_entry`, and a `reason` when
not valid; or one `unattributable` record), `match`, `positions` and `finding`. The last
line is the `coverage` record: `snapshot_boundary`, `first_examined`, `last_examined`,
`stop_position`, `stop_scope` (always `local_committed_not_quorum_current`),
`uncommitted_past_stop`, `torn_tail_past_stop` (bytes), one `containment` object per
suspect identity (`activated_at`, `superseded_at`, `retired_at`, `freeze_intervals`) and
`status`.

| Exit | Meaning | Next step |
|---|---|---|
| 0 | complete coverage; audit done | review the rows |
| 1 | invalid history or binding (checksum, gap, malformed record; manifest not this root's), or a Root2 entry the production verifier refuses (the diagnostic names it) | stop; preserve the directory; investigate the node or the manifest copy |
| 2 | I/O, usage or trust-input error | fix the invocation or the input files |
| 3 | partial coverage (compacted prefix, or no local commit position); only the coverage record is printed, no rows | audit a `Full` replica; see the retention precondition |
| 4 | unresolved or ambiguous suspect key | check the key file against the root's history |

Text is never status: act on the exit code and the record's `status`.

### Reviewing rows

- **First, check every `containment` object's `activated_at` is non-null.** A null
  `activated_at` means this root never held that role epoch: a wrong `--epoch` matches
  nothing and silently gives no `suspect_use` rows. Correct the epoch and re-run.
- `suspect_use`: a suspect identity produced a valid signature on this entry, whatever
  the reducer did. Every such row before the identity's containment is a pre-detection
  action to review; containment does not undo it (forward-only). Rows whose `effect` is
  `applied` changed the root and must be reviewed for follow-up (for example a granted
  permission that must be revoked by the new key).
- `suspect_claim_unverified`: a sidecar claims a suspect identity but does not verify
  (`reason`: `signature`, `unknown_epoch`, `binding_mismatch`, `unsigned`,
  `no_signing_domain`). It is a forged or garbled attempt, never counted as use.
- `positions`: for each suspect identity used in the row, whether the row is after that
  identity's `superseded_at`, after the root's `retired_at`, or inside a freeze interval
  (strictly after a Freeze, through its Unfreeze).
- `finding: true`: a suspect use after supersession or retirement that the reducer did
  **not** deny. ADR 0101 says this cannot happen; treat it as a defect and escalate.

### Shared exit criteria

A section is done when the audit run on a replica past the last containment entry:

1. exits 0 with `status` `complete`, with every suspect identity's `activated_at` non-null;
2. shows each contained identity's `superseded_at` (or the root's `retired_at`) at the
   containment entry you submitted;
3. has no row with `finding: true`;
4. has every `suspect_use` row after containment with `effect.outcome` `denied`;
5. has had every pre-containment `suspect_use` row with an `applied` effect reviewed.

## 1. Admin key compromised

- **Detection signal:** an Admission permission statement, or a joint-signed command,
  the administrator did not issue; a `suspect_use` row for `admin` in a routine audit.
- **Containment** (ADR 0101 authority table):
  1. `Freeze(previous transition)`, recovery `VISROL03` proof; expect `Registered`.
     Admission statements are then a consumed `Denied(23)`.
  2. `RotateAdmin(new admin key)`, recovery proof; expect `Registered`. The old admin
     epoch is superseded at this entry.
  3. `Unfreeze(freeze coordinate)`, recovery + governance proof (not the compromised
     admin); expect `Registered`.
- **Audit:** `wendelon audit root2 <data-dir> --trusted-manifest <manifest> --role admin
  --epoch <compromised epoch> --suspect-only`; expect exit 0.
- **Review:** every pre-containment `suspect_use` row; revoke any permission the attacker
  granted with a new-key statement.
- **Exit:** the shared criteria for `admin` at the compromised epoch.

## 2. Governance key compromised

- **Detection signal:** a governance command (enrolment, revocation, grant, cut,
  resolve, issuer change) the governance holder did not issue.
- **Containment:**
  1. `Freeze`, recovery proof; expect `Registered` (the nine governance commands are then
     `Denied(23)`).
  2. `RotateGovernance(new key)`, recovery proof; expect `Registered`.
  3. `Unfreeze`, recovery + admin proof; expect `Registered`.
- **Audit:** `... --role governance --epoch <compromised epoch>`; expect exit 0.
- **Review:** pre-containment governance changes; reverse each with the new key where
  the protocol allows.
- **Exit:** the shared criteria for `governance`.

## 3. Admin and governance both compromised

Admin + governance jointly authorize `RotateRecovery` and `RetireRoot`, and Freeze does
not block rotations, so this is a race for the recovery role.

- **Detection signal:** suspect activity under both roles.
- **Containment:** `Freeze`; then `RotateAdmin(new)` and `RotateGovernance(new)`, each by
  recovery; then `Unfreeze`, recovery + the new admin. Each expects `Registered`.
- **Audit:** `... --role admin --epoch <e> --role governance --epoch <g>`; expect exit 0
  with two containment objects and distinct `superseded_at`.
- **Race lost:** a `suspect_use` row whose command is `rotate_recovery` or `retire_root`
  with an `applied` effect means the attacker moved first: go to section 6 (M2).
- **Exit:** the shared criteria for both identities.

## 4. Recovery key compromised

A compromised recovery key can rotate admin and governance, freeze and retire. Recovery
is a race against the attacker; otherwise M2.

- **Detection signal:** a Freeze, rotation or RetireRoot nobody intended.
- **Containment:** `RotateRecovery(new key)` at once, admin + governance proof; expect
  `Registered` (allowed while frozen). If the attacker froze the root, then `Unfreeze`
  with the new recovery key + admin.
- **Audit:** `... --role recovery --epoch <compromised epoch>`; expect exit 0.
- **Race lost:** a `suspect_use` row for `rotate_admin`, `rotate_governance` or `retire_root`
  (which recovery alone authorizes) with an `applied` effect (and `superseded_at` null
  for recovery) means the attacker moved first: go to section 6 (M2).
- **Exit:** the shared criteria for `recovery`.

## 5. Recovery key lost

Not a compromise, but the root can no longer freeze, unfreeze or rotate admin or
governance.

- **Containment:** `RotateRecovery(new key)`, admin + governance proof; expect
  `Registered`.
- **Audit:** `... --key <file with the lost recovery public key>`; expect exit 0, the key
  resolved to recovery at its epoch, and `superseded_at` at the rotation. Any
  `suspect_use` row means the key was not merely lost: handle as section 4.
- **Exit:** the shared criteria for `recovery`.

## 6. M2: succession to a new root

When a role cannot be recovered (section 3 or 4 lost, or recovery lost and admin or
governance compromised), the root is retired and replaced.

- **Containment:** `RetireRoot`, recovery proof or joint admin + governance proof;
  expect `Registered`. Every later Root2 command of the root is a consumed `Denied(25)`,
  with no authorization evaluated.
- **Audit:** `... --role admin --epoch <e> --role governance --epoch <g> --role recovery
  --epoch <r>` (every suspect identity); expect exit 0 and `retired_at` at the RetireRoot
  entry. A suspect signature after retirement is still verified and shown, with
  `after_retired: true` and `Denied(25)`.
- **Exit:** "new root provisioned; old root retired and audited": the old root meets the
  shared criteria with `retired_at` set, and a new profile-3 root is provisioned under
  new keys. The data handover is C-S5 and is not covered here.

## Runbook acceptance

Each section's containment order and audit invocation is run once against a WAL fixture
built from real carrier entries, as a scripted dry run with test keys only:
`root2::tests::compromise::runbook` (`section1_admin_key_compromised` through
`section6_m2_old_root_retired_and_audited`). Recorded outcomes: every section exits 0
with `status` `complete` and no finding; sections 1-5 show `superseded_at` at the
rotation and every later suspect use denied; section 4's lost race shows an applied
suspect `rotate_admin` with recovery never superseded; section 6 shows `retired_at` and
`Denied(25)` after it.
