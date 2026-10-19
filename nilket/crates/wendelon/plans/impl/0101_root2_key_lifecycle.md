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
    key for `(claimed_role, claimed_epoch)`, using each form's production
    preimage function and `role_command::slot_signed`; `auth/signature.rs`
    and `auth/preimage.rs` cover only admission and holder signatures.
    `crypto_valid` also requires the proof to bind this root and this
    entry (context digest and command digest), so a genuine signature
    replayed onto another entry is `binding_mismatch`, never suspect use;
    an epoch absent from the history is `crypto_valid = false`, `reason: unknown_epoch`. `active_at_entry` is
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
- Signature records also carry `form`, `slot` and `reason`. Every row
  carries `positions` and `finding`, which are empty or false when the row
  is not suspect use.
- **Final record:** `{kind:"coverage", snapshot_boundary, first_examined,
  last_examined, stop_position, stop_scope, uncommitted_past_stop,
  torn_tail_past_stop, containment, status}`. `stop_scope` states that
  the stop is local and not quorum-current (L5). `status` names the same
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
  partial and reversible, so it is intervals, never one boundary. An
  interval covers (Freeze, Unfreeze]: the entries after the Freeze through
  the Unfreeze entry, whose pre-entry state is still frozen.
- `activated_at`: LogId at which that epoch became active (`null` if the
  epoch never existed), so a nonexistent epoch is not mistaken for "never
  superseded".

### Exit codes

Typed; text is never status.

| Code | Meaning |
|---|---|
| 0 | complete coverage, audit done (any number of rows) |
| 1 | invalid history or binding: checksum, gap or malformed record in range; install or manifest mismatch; or a Root2 entry the production verifier refuses |
| 2 | I/O, usage or trust-input error (same as `audit verify`) |
| 3 | partial coverage: the coverage record only, with no rows, because there is no pre-entry state without genesis and the install entry. A log with no local committed position is also partial. |
| 4 | unresolved or ambiguous suspect (`--key` absent from the root or resolving to more than one identity) |

With 1, 2 or 4 a diagnostic goes to stderr and nothing is printed on
stdout. Rows are rendered only after the whole fold, because the position
flags depend on the full history. A Root2 entry that the production
verifier refuses outright stops the fold with exit 1, and the diagnostic
names its LogId.

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

## C-S6b plan: Root2 context export

### Status

Plan only. This section authorizes no source changes.
- Contract: `/home/me/work/.agent-bridge/wendelon-adr0101-c-s6b-context-export-contract-r2/CONTRACT.md` (r2, accepted).
- It amends C-S6 and is the first slice of key custody (contract r4).

### Behavior

`nilket wendelon audit root2 <data-dir> --trusted-manifest <file> --export-context <out> --node-id <n>` writes a versioned file (`WRCTXEXP`, version 1). It holds:
- the bound `AdminContext2` bytes, through one `context_bytes` encoder that `context_digest` also uses;
- the trusted manifest;
- every role-key rotation's committed `Command2` and VISROL03 sidecar, with its activation `LogId20`;
- the freeze, transition and retirement state, the activation LogIds, the stop, coverage and `node_id`, all typed `AuditDerived` (as is the completeness of the successor list);
- an integrity digest.

The rules:
- **Complete fold only.** The export is written only after a complete fold (exit 0).
- **Path checks first.** They run before the fold. The parent fd is walked by `(dev, ino)`, and a target inside the data dir is refused. A name with the tool's temp prefix is refused, and the data dir's identity is re-checked just before publication.
- **Publication.** Exclusive 0600 create through the parent fd, fsync, `RENAME_NOREPLACE`, then a directory fsync, all before any row is printed.
- **Failures.** Any failure leaves no file and prints nothing. A stdout failure after publication withdraws the file. Withdrawal is `unlinkat(parent_fd, name)` only after an `fstatat` shows the same `(dev, ino)` that was published, followed by a directory fsync. It exits 2. If that identity check fails, the file is left in place and the diagnostic names it.
- **Empty suspect set.** Allowed with `--export-context`, which amends C-S6's "never an empty result".

The export is evidence, not trust. The custody tool re-verifies the rotation chain against the trusted manifest. The context is authenticated only when at least one successor exists. Unverifiable fields are usable only to refuse, failing safe.

### Acceptance

- **T-X1:** every exit path, injected write and sync failures, and stdout failure after publication.
- **T-X2:** the chain re-verifies with the production verifier, with one refusal per replay check.
- **T-CTX:** golden context bytes, plus per-field sensitivity.
- **T-X4:** the output is deterministic.
- **T-X5:** the data dir stays read-only.
- **T-PATH:** aliasing, symlinks and temp-prefix names are refused, and a swapped data dir is caught.
- **T-FMT:** the format is byte-exact, and every malformed input is refused.
- **T-CLI:** the empty-suspect usage works.

An architecture guard pins the export as pure, run after the fold and published before the rows, along with the publish path's exact syscall discipline.

Non-goal: detecting a bind mount of a data-dir subdirectory.

### Commits and gate order

- `plan: adr 0101 root2 context export` (this section). Arch and MV checks pass before the FF.
- `impl: adr 0101 root2 context export`.
- `docs: root2 context export runbook`.

Gate order: independent source review → ladder → log audit → FF, after C-S6 has landed.

## Key custody slice 3 plan: operator proof facade

### Status

Plan only. This section authorizes no source changes.
- Contract: key custody r4 (`/home/me/work/.agent-bridge/wendelon-adr0101-key-custody-contract-r4/CONTRACT.md`), §2 K7 and §3 item 2.
- Context reader: C-S6b r2 §5.
- Slices 1 (C-S6) and 2 (C-S6b) are on main.

### Behavior

This slice is library only. It adds no binary and no signing.
- **One shared verifier core.** `role_command::verify_for`'s slot loop becomes `verify_core(context_digest, active, …)`, and `verify_for` becomes a wrapper over it. The production verifier still calls `verify_strict` exactly once, through `slot_signed`.
- **One role view.** The four checkers (`verify_core`, `check_epoch_statement3`, `check_admin_statement2` and `check_governance_command2`) read a `RoleView3 { context, profile, admin_key, namespace, governance: Option<GovernanceRole2>, active: Option<[(u64, Digest2); 3]> }`. Each checker's body is unchanged apart from how it reads these fields: the profile gate, verification under the manifest `admin_key` for VISADS02, and the epoch-0 refusals stay as they are today.
  - Production obtains it from `InstalledAdmin2::view()`, which copies those fields one-for-one.
  - The offline facade obtains it from `RoleView3::offline(context, manifest, verified_history)`.
  - A `RoleView3` cannot produce a `VerifiedCommand2`, and `verification.rs` obtains views only through `install_admin2`.
- **One role requirement.** `role_requirement(command, slot0_role)` is extracted from `verification.rs`, and both the verifier and the facade use it.
- **The facade, `root2/operator_proof.rs`.** It is the only new public module, and its signatures use byte arrays only. It provides:
  - unsigned layouts and preimages for every form (VISROL03 by slot and form, VISADS03, VISADS02, VISADG01);
  - signature splicing;
  - `self_check` through the production checkers. For the governance family, `self_check` with a VISROL03 [2,0] sidecar includes the governance action-bit check (`R/governance_verification.rs:83-84`), exactly as `verify_governance2` applies it;
  - production VISROL03 and VISADS03 encoders, which replace the duplicated test helpers;
  - the production context-export reader `verify_context_export(bytes, trusted_manifest)`. It runs C-S6b r2 §5 steps 1-4 in order: parse, digest, version, coverage and caps; manifest byte-equality and pin; the full replay (each sub-check listed there); and the return of the history, active epochs, `context_authenticated` (`count ≥ 1`) and the `AuditDerived` fields. `RoleView3::offline` is built only from this reader's `VerifiedContext`, never from caller-supplied history;
  - validation of a two-host VISROL03 merge. It refuses unless both inputs are byte-equal outside 86..150 and 160..224, each fills exactly one slot, and each slot verifies via `slot_signed` under the history key for its claimed (role, epoch). It then runs `self_check`. A second-slot signer's layout is rebuilt from (command, context) and compared with the partial.
- **No signing code.** No non-test Root2 module, including the facade, contains signing code.

### Acceptance

- **T-VIEW:** every profile 1/2/3 vector gives identical results through `InstalledAdmin2` and `view()`. Control: altering `profile`, `admin_key`, `active[0]`, `namespace` and `governance` in turn each changes at least one result.
- **T-K7:** the full cell matrix in contract §5, single and two-slot (merged) VISROL03, the governance family with both sidecars, VISADS02, VISADS03, plus the refused cells. Each accepted cell passes `self_check`, the production verifier and the reducer. Controls: a flipped preimage byte, the wrong role alternative, and an altered command digest are each refused. The action-bit check is exercised on a test-only `RoleView3` whose governance mask lacks the action's bit (built under `cfg(test)` only, never through the public facade), and `self_check` refuses. An architecture pin asserts that the facade's governance VISROL03 path calls the same action-bit predicate as `verify_governance2`.
- **Merge:** refusals for mismatched commands or contexts, a slot overwritten, an invalid first slot, and a layout differing from the rebuilt one.
- **Context reader:** the C-S6b T-X2 refusals, now through the production reader. `count = 0` verifies with `context_authenticated = false`.
- **Architecture pins:** the auth, admin_statement and role-visibility pins, plus the signing-token bans, are amended equally strict. New pins: root2's public surface is exactly the existing re-exports plus `operator_proof`; views come only through `install_admin2`; a view can never become a `VerifiedCommand2`. Every new pin is probed.

### Commits and gate order

- `plan: adr 0101 key custody operator proof facade` (this section). The arch and MV checks pass before FF.
- `impl: adr 0101 key custody operator proof facade`.

Gate order: independent source review → ladder → log audit → FF.
