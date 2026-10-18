# Impl — Wendelon ADR 0101: Root2 administrator key lifecycle

Pairs with `plans/adr/0101_root2_administrator_key_lifecycle.md`. The
work lands as a chain of slices. Each slice is one signed commit through
the full gate pipeline. Everything stays dormant: no slice adds a
proposer, serving or runtime path.

`R` = `src/storage/mutation_visibility/root2`.

## Landed

| Slice | Commit | Content |
|---|---|---|
| C-S4a | d9f3c8b5 | Profile 3 manifest (241 B); recovery role; owner directory v2 (512 B events); snapshot v3; fail-closed admission by profile |
| C-S4b1 | 722a951b | Key epochs; recovery-authorized RotateAdmin/RotateGovernance; joint RotateRecovery; `VISROL03` 224 B joint proof; state V5 role extension |
| C-S4b2 | 524db8a2 | Freeze/Unfreeze (kinds 18/19); `Denied(23)` before authorization on both reducer paths; `Denied(24)` stale/repeat binding; production AVL `Map::remove` |

## C-S3a — committed retirement (next)

- `RetireRoot` (kind 20), a unit command. It is authorized by recovery alone or by joint admin + governance. Structural refusals happen before install; authorization failures are a consumed `Denied(8)`.
- `R/retirement.rs`: `is_retired`, `retire(root, at, seed, fingerprint, digest)` and `consume()`, which diverts every post-coordinate Root2 command to a consumed `Denied(25)` on both reducer paths, after the structural and binding checks and before authorization.
- State V6: the V5 extension plus an 84 B retired record (Log2, entry fingerprint, command digest). It is written only for a retired root. A repeat RetireRoot after the coordinate is a consumed `Denied(25)`; the first retirement record is preserved.
- `admin_install.rs`: `InstallError2::Retired`. Replay confirms the terminal state with `confirm_terminal2`.
- Decoder reason ranges extend to 1..=25. There is an exhaustive-site census for the new kind and the new denial.
- `R/retirement_shape.rs` is a parse-only guard: no retire path outside `retirement.rs`.
- Tests live under `root2::tests::retirement`. They cover authorization (both forms, refusals), idempotence, every command kind after the coordinate, V6 codec goldens and the old-reader refusal, install refusal and replay determinism.

## C-S3b — owner binding and terminal record

- An owner-directory `Retire` for a profile-3 root must consume the committed retirement observation (identity plus coordinate), recorded in the inventory.
- Export publishes a `TerminalRecord2` (identity, coordinate, last state digest), which is the M2 cut input.
- A mismatch between the owner identity and the committed identity is a typed HOLD.
- Profiles 1/2 keep provisioning-local retirement unchanged.

## Later

- **C-S6:** planned; see the C-S6 plan section below.
- **C-S5:** M2 handover to a successor root.
- **Key custody:** per-role key files with owner-only permissions, an offline recovery key, and encrypted backups.

Real customer data needs all of C-S3a/b, C-S6, custody and the M2 runbook.

## Gates (every slice)

contract (with Algebra) → census delta → unsigned source → pre-seal
`cargo check --lib --tests` + fmt → recorded guard preflight → fresh
local → review → sign → SOURCE verify → probe → dev → 18-step ladder →
external audit → FINAL → joint FF. A rebase past other-lane commits with
zero overlap gets one confirming ladder.

## C-S6 plan — compromise audit tool and runbooks

### Status

Plan only; no source is authorized by this section. Contract r2 plus
Addendum A, accepted 2026-10-18 (Addendum A replaces r2's attribution,
match-class and containment text). Census: source b7280e6a.

### Tool form and placement

- `wendelon audit root2 …`, under the existing `audit` surface, next to
  `audit verify`. Inputs: the node data directory, the trusted profile-3
  manifest (out of band, like `--trusted-audit-roots`), and the suspect
  set: `--role R --epoch E` (repeatable) or `--key <file>`;
  `--suspect-only` prints only `suspect_use` and
  `suspect_claim_unverified` rows plus the final record.
- CLI code in a new `cli/root2_audit.rs` (`cli.rs` is 1,514 lines).
- The Root2 reader, fold and output are new modules, each under the
  750-line Root2 production-module cap. `command.rs` (703) must not grow
  past the cap.
- Tests in a new focused test module, not `R/replay/tests.rs` (747).
- No new persisted key space; nothing is stored.

### Read-only path

1. **Scanner.** Opens `control.wal` (legacy) or `control.wal.<start>.seg`
   `O_RDONLY`; validates header, fixed record, checksum and contiguity;
   never repairs or truncates. A malformed, checksum-failing or gapped
   record inside the examined range is exit 1; a torn tail past the stop
   is reported, not an error. The volatile `BTreeMap` cache and the
   `VISJRN02` journal are not read.
2. **Binding.** Root key = the full bound `AdminContext2`
   `(generation, cluster, installed_at, bootstrap_fingerprint,
   bootstrap_command, genesis, initial_epoch, manifest_pin)`, not
   `RootIdentity2`. The trusted manifest is bound to the root's install
   entry in the log; a missing install entry, failed binding or pin
   mismatch is exit 1, with no partial binding.
3. **Fold.** Applies the existing reducer and verifiers
   (`role_command::verify_for`, the governance verifiers) to each
   pre-entry state over raw `Root2Carrier1` requests. It does not
   reimplement authorization, never calls `MarkStore2::advance`, and never
   constructs a writing `WendelonLogStore`. Open for Codex to propose:
   make `acquire_committed_prefix2` generic over a read-only reader, or a
   sibling read-only acquisition; either way the reducer calls are shared.
4. **Key history.** Per role, epoch 0 is the manifest key and each
   successor is recorded with its activation LogId; keys are never
   discarded after rotation.
5. **Stop.** The local committed/applied position. Stored entries past
   it are counted, never audited. `HistoryCheck::Complete` is local
   contiguity, not quorum currentness.

### Output schema

One compact JSON object per line (serde, fixed struct field order,
default `serde_json` escaping), rows in ascending LogId index. Laws:
L1 determinism (same log bytes, manifest and suspect set give
byte-identical output); L2 every examined entry of the root is reported,
and every entry where a suspect key produces a valid signature is
`suspect_use`; L3 `suspect_use` requires `crypto_valid` by a suspect
identity; L4 explicit coverage; L5 no currentness claim.

- **Entry row:** `{kind:"entry", root, log_id, fingerprint, command,
  effect, signatures:[…], match}`.
  - `effect`: the reducer's verbatim output (applied, or consumed
    `Denied(n)`).
  - `signatures`: one record per signer slot,
    `{claimed_role, claimed_epoch, crypto_valid, active_at_entry}`.
    Claims come from the proof form: `VISROL03` explicit; `VISADG01`
    governance epoch 0; legacy admin forms admin epoch 0; `VISADS03`
    explicit admin epoch. `crypto_valid` is checked against the history's
    key for `(claimed_role, claimed_epoch)` with `auth/signature.rs` and
    `auth/preimage.rs`; an epoch absent from the history is
    `crypto_valid = false`, `reason: unknown_epoch`. `active_at_entry` is
    true when the claimed epoch is active in the pre-entry state. No
    sidecar, an unparseable sidecar or a form naming no role gives one
    `{unattributable: reason}` record. Neither `effect` nor
    `active_at_entry` infers `crypto_valid`.
  - `match`: `suspect_use` (some record `crypto_valid` by a suspect
    `(role, epoch)`, regardless of `active_at_entry` and `effect`),
    `suspect_claim_unverified` (some record claims a suspect with
    `crypto_valid = false`; never counted as use), or unmarked.
  - Each `suspect_use` row carries per-identity flags
    `{identity, after_superseded, after_retired, in_freeze_interval}`.
    After supersession or retirement with a non-denial effect, the row
    gets `finding: true` (ADR 0101 says it is impossible).
- **Final record:** `{kind:"coverage", snapshot_boundary, first_examined,
  last_examined, stop_position, uncommitted_past_stop,
  torn_tail_past_stop, containment, status}`. `status` names the same
  outcome as the exit code. If the install entry or any entry between it
  and the stop is absent, the result is partial; a snapshot does not
  restore deleted signer evidence, and an unread range is never reported
  as empty.

### Containment objects

One per suspect `(role, epoch)`, computed from the replayed history
independently of first use:

- `superseded_at`: LogId of the first committed successor activation for
  that role with a higher epoch; `null` if never superseded.
- `retired_at`: LogId of the committed RetireRoot; shared; `null` if not
  retired.
- `freeze_intervals`: `[freeze_log_id, unfreeze_log_id or null]` pairs
  over the examined range; shared and repeated per object. Freeze is
  partial and reversible, so it is intervals, never one boundary.

### Exit codes

Typed; text is never status.

| Code | Meaning |
|---|---|
| 0 | complete coverage, audit done (any number of rows) |
| 1 | invalid history or binding: checksum, gap or malformed record in range; install or manifest mismatch |
| 2 | I/O, usage or trust-input error (same as `audit verify`) |
| 3 | partial coverage (audit done over the available range only) |
| 4 | unresolved or ambiguous suspect (`--key` absent from the root or resolving to more than one identity) |

With 1, 2 or 4 a diagnostic goes to stderr; rows already written stay on
stdout, and no `complete` coverage record is printed.

### Acceptance

Fixtures reuse the signed-command builders and expected reducer results
of `R/tests/{profile_three_reserve, role_freeze, role_epochs/*,
retirement/*}.rs` and `R/replay/tests/{carrier, completion}.rs`. A new
controlled WAL fixture writes real Control-WAL records to a temp
directory; the CLI reads them through the read-only path. Containment
assertions in A3 and A9 use the Addendum A objects and flags.

- **A1.** Single-role suspect (admin epoch 0): exactly the expected
  `suspect_use` rows in LogId order; applied and denied effects both
  appear; exit 0.
- **A2.** A joint proof with one suspect signer is `suspect_use`; the
  honest co-signer's valid record is shown but is not the suspect.
- **A3.** A suspect whose role was rotated: the rotation is the
  identity's containment.
- **A4.** `--key` not in the root, and an ambiguous key: exit 4, no
  `complete` coverage record.
- **A5.** Missing prefix (install entry compacted): exit 3, boundary in
  the final record, no rows for the missing range.
- **A6.** Determinism: two runs give byte-identical stdout.
- **A7.** Read-only: the data directory is byte-identical (every file's
  content and mtime) before and after, including over a torn tail.
- **A8.** Retired root: rows up to and past retirement are listed; a
  suspect signature after retirement is valid with effect `Denied(25)`.
- **A9.** Frozen root with suspect use during the freeze: effect
  `Denied(23)`, valid signature, inside a freeze interval.
- **A10.** A forged sidecar claiming a suspect role and epoch is
  `suspect_claim_unverified`, never `suspect_use`.
- **A11.** A checksum-corrupted record in range: exit 1. A torn tail past
  the stop: exit 0 with `torn_tail_past_stop` reported.
- **A12.** Uncommitted stored entries past the stop are counted in
  `uncommitted_past_stop` and appear in no row.
- **A13.** Stale key after rotation: suspect admin epoch 0, RotateAdmin
  to epoch 1, then an entry signed with the epoch-0 key: `suspect_use`,
  `crypto_valid = true`, `active_at_entry = false`, the reducer's denial,
  `after_superseded = true`, no finding.
- **A14.** Containment before first use: RotateAdmin before any suspect
  use, then a stale-key attempt; `superseded_at` is the earlier rotation.
- **A15.** Multiple suspects (admin epoch 0, governance epoch 0) with
  different rotation points: two containment objects with distinct
  `superseded_at`; row flags per identity.
- **A16.** Freeze, Unfreeze, Freeze: `freeze_intervals` is
  `[[f1, u1], [f2, null]]`; a use between `u1` and `f2` has
  `in_freeze_interval = false`; one after `f2` has `true` and
  `Denied(23)`.
- **A17.** A `VISROL03` claim of a nonexistent epoch: `crypto_valid =
  false`, `reason: unknown_epoch`; `suspect_claim_unverified` if the
  claim names a suspect.

### Runbook outline

New `plans/ops/root2_compromise.md`, linked from `plans/ops/README.md`
and from ADR 0101 open questions. Preface: the retention precondition (a
real-data root runs `ControlRetention::Full`; under `Compacting` the audit
is partial, exit 3, once compaction passes the install entry), and run
the tool on a replica whose applied position is past the containing
entry. Each section gives the detection signal, the containment command
and its authority (ADR 0101 table), the audit invocation, review of
`suspect_use` and `suspect_claim_unverified` rows, and exit criteria;
every step names its exact command, expected outcome code and
verification.

1. Admin key compromised.
2. Governance key compromised.
3. Admin and governance both compromised.
4. Recovery key compromised: a race against the attacker, otherwise M2.
5. Recovery key lost.
6. M2, succession to a new root; stops at "new root provisioned; old
   root retired and audited". C-S5 owns the handover.

Runbook acceptance: each section's commands run once against the fixture
as a scripted dry run, outcomes recorded; no real keys.

Out of scope: undoing entries, C-S5, key custody, runtime enablement,
automatic detection, quorum-currentness proofs.

### Commits and gate order

- `plan: adr 0101 compromise audit and runbooks` (this section plus the
  ADR amendment); Wendelon arch and MV tests pass before FF.
- `impl: adr 0101 compromise audit tool`: tool and tests.
- `docs: root2 compromise runbooks`.

plan commit → unsigned source → review → sign → LOCAL → DEV → full
18-step ladder → external audit → FINAL → FF.
