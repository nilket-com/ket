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
  - Amended by the profile-3-only plan (K7 amendment below): from its sub-slice d, `RoleView3 { context, admin_key, namespace, governance: GovernanceRole2, active: [(u64, Digest2); 3] }`, five fields with no `Option` and no `profile`; the profile gate goes with profiles 1/2.
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
  - Amended by the profile-3-only plan: from its sub-slice c2, T-VIEW runs profile-3 vectors only; the `profile` alteration is deleted, `with_governance(None)` becomes another governance key, and each remaining alteration still changes a result.
- **T-K7:** the full cell matrix in contract §5, single and two-slot (merged) VISROL03, the governance family with both sidecars, VISADS02, VISADS03, plus the refused cells. Each accepted cell passes `self_check`, the production verifier and the reducer. Controls: a flipped preimage byte, the wrong role alternative, and an altered command digest are each refused. The action-bit check is exercised on a test-only `RoleView3` whose governance mask lacks the action's bit (built under `cfg(test)` only, never through the public facade), and `self_check` refuses. An architecture pin asserts that the facade's governance VISROL03 path calls the same action-bit predicate as `verify_governance2`.
- **Merge:** refusals for mismatched commands or contexts, a slot overwritten, an invalid first slot, and a layout differing from the rebuilt one.
- **Context reader:** the C-S6b T-X2 refusals, now through the production reader. `count = 0` verifies with `context_authenticated = false`.
- **Architecture pins:** the auth, admin_statement and role-visibility pins, plus the signing-token bans, are amended equally strict. New pins: root2's public surface is exactly the existing re-exports plus `operator_proof`; views come only through `install_admin2`; a view can never become a `VerifiedCommand2`. Every new pin is probed.

### Commits and gate order

- `plan: adr 0101 key custody operator proof facade` (this section). The arch and MV checks pass before FF.
- `impl: adr 0101 key custody operator proof facade`.

Gate order: independent source review → ladder → log audit → FF.

## ADR 0100 Root2 profile-3-only plan

### Status

Plan only. This section authorizes no source changes beyond its own test-only corpus.
- Contract: `/home/me/work/.agent-bridge/wendelon-root2-profile3-only-contract-r5/CONTRACT.md` (r5, accepted). It is slice P3 of the Root → Root2 cutover, before live activation. Census: `de4c86ee2`.
- It amends the K7 text above (see "K7 amendment").

### Decisions (operator and coordinator, 2026-10-19)

- A pre-release reset with no migration. Every root is profile 3 and bootstraps with a 241-byte manifest committed by its own pin (`control_len` 241).
- Removed: profiles 1 (137 B) and 2 (189 B); class-0 roots (`control_len` 0); owner directory v1 (`DirectoryFormat2::One`, VISIEV02, VISREP03); plain Retire; snapshot v2 and v1 with entries; the frozen old-reader copies; VISADC01 (D-C1, the facade refusal stays pinned).
- Kept: state V4 (an unrotated root), V5 and V6; VISADS02/03, VISADG01, VISROL03; directory v2; snapshot v1 (empty only, D-S1), v3 and v4; VISTRM03; the bootstrap record profile word 1.
- An unmarked owner directory is refused even when empty, as `Unformatted` (D-O1).
- The S1 goldens keep their names. Their contents are replaced in b3: committed to the new `M3_S1` manifest, with the parent lineage renumbered by +1 from index 4 for an admitting permission (D-G1). The S1c oracle compares through a permission-stripping projection (D-G2).
- `ManifestProfile2` is deleted, `AnyAdminManifest2` collapses, and the profile-only `Option` fields of `InstalledAdmin2` and `RoleView3` become non-optional (D-T1, d).
- D-G3: the published test-only seeds are in `R/tests/statement_seeds.rs`: administrator `[0xe1; 32]` (public `25fc32c4…cb78`), governance `[0xe2; 32]` (`c91cb3ce…3e9f`), recovery `[0xe3; 32]` (`4dd9b649…f226`). They sign only for `M3_S1`, `M3_KAT` and `M3_KAT_FOREIGN`. VISADS02 over `M3_PRIMARY` keeps the seed `[11; 32]`.
- D-K1 (cross-root uniqueness of all three role keys) is a follow-up contract after d.

### Laws

- **P1, one admin form per codec.** Each codec accepts exactly its one form and refuses every other with the contract's typed error at the contract's step (§3). The bootstrap record word stays 1.
- **P2, lineage unchanged.** V4 → V5 → V6 behave as today; `control_len` other than 241 is `ControlClass`.
- **P3, no profile-3 behaviour change.** Every kept golden is byte-identical in input and outcome, except the C-S6 message below and `errors[5]` (`ProfileLength` → `UnsupportedProfile`). After a, profile-3 suites never read S1-family bytes.
- **P4, no loosened guard.** Each architecture pin is narrowed with a probe or deleted under §13.1. The 241 gates stay as tested invariants.
- **P5, no lost refusal.** Every refusal branch of a kept codec that only profile-1/2, class-0 or v1-directory fixtures cover is ported to profile 3 or directory v2 first, with the same typed error at the same step (the v1-twin rule; R-PIN for the renumbered S1 tests).

### Removed-forms corpus

`R/tests/removed_forms.json` holds the exact bytes of N1-N15 (28 rows), each with its SHA-256, source (vector file and key, or a derivation for N7 and the N14 lengths 137/189) and today's outcome. `R/tests/removed_forms.rs` asserts every outcome holds today and that the file digest is:

`3244bfa6faa088ec63f8433c3faff0b1a8b31747d61fbd5f8dc3bab0884b9f89`

The bytes were copied from the existing vectors with `python3 -I`, outside the repository. In c1 (N4-N12) and c2 (N1-N3, N13-N15) each row flips to its §12.2 typed refusal against an accepted twin, and the digest is re-asserted.

Today the v2 event codec is reachable from R/tests only through `OwnerDir2`, so N9 and N10 are asserted through a v2 directory fold and export.

### Sub-slices

Each is one commit by Joel, through review → ladder → audit → FF.

| # | Commit | Content |
|---|---|---|
| a | `impl: adr 0100 root2 profile three fixtures` | neutral `profile_three_fixture` module (`M3_PRIMARY`, `M3_OTHER`, `M3_SIBLING`, `M3_S1`, `M3_KAT`, `M3_KAT_FOREIGN`), manifest base inline, S1 reads frozen; no behaviour change |
| b1 | `impl: adr 0100 root2 profile three admin coverage` | admin, root and verification ports; the KAT port; the bootstrap length conjunct; T-INV |
| b2 | `impl: adr 0100 root2 profile three provisioning coverage` | provisioning ports on directory v2; N16 |
| b3 | `impl: adr 0100 root2 s1 goldens governed` | S1 twins, T-FAMILY, R-PIN, D-G2 |
| c1 | `impl: adr 0100 root2 profile three provisioning only` | provisioning removals; corpus N4-N12 flip |
| c2 | `impl: adr 0100 root2 profile three only` | manifest and class-0 removals; corpus N1-N3, N13-N15 flip; completion gate green |
| d | `impl: adr 0100 root2 profile three types` | `Option` collapse and the K7 pin to five fields |

Recorded R0 assay boards are not comparable across b3: registry usage grows by one permission per parent-lineage root, and encoded digests change.

### Completion gate (green at the end of c2)

From `nilket/crates/wendelon`, contract §16.1, with `removed_forms.{rs,json}` excluded:
- G1 (removed identifiers, `"a"`/`"b"` and old fixture names), G2 (137/189 literals other than manifest-layout offsets), G3 (length 0 and class 0) and G4 (v1 directory and plain Retire) print nothing.
- G1-A prints only the tombstone list of `manifest_codec_census_is_exact_and_dormant`.
- G5 (`VISIEV02`) prints exactly: the marked + VISIEV02 → `ForeignFormat` check in `provisioning/owner_log/directory.rs`, the new architecture pin and its probe, and the T-O1 literal in `provisioning/tests/profile_three.rs`.
- G6 is `R/tests/state_blob_gate.rs` (c2): every V4/V5/V6 blob in every R JSON file except the corpus has `control_len` 241. Today's census is 29 blobs in 8 files: `goldens.json` 9 (0), `origin_vectors.json` 1 (0), `gate_vectors.json` 1 (137), `root_profile_vectors.json` 1 (137), `role_epochs/vectors.json` 2, `role_freeze/vectors.json` 3, `retirement/vectors.json` 4, `retirement/owner_binding/vectors.json` 8 (241). c2 records the counts after the `p3_` replacements.

Plus the full root2 suite (also at 8 threads; a parallel-only failure is a defect candidate), the wendelon lib, and the architecture and MV tests.

### K7 amendment

The key custody slice 3 "One role view" and T-VIEW bullets above are amended in place: from d, `RoleView3` has five non-optional fields and no `profile`; from c2, T-VIEW is profile 3 only. The architecture pin moves to the same five fields in d, with probes for a re-inserted `Option`, a sixth field and the pin text. Key custody contract r4 (`/home/me/work/.agent-bridge/wendelon-adr0101-key-custody-contract-r4/CONTRACT.md`) disagreed with itself: four fields at line 156 and six at line 161. Five resolves it.

### C-S6 message

From c2, a 137- or 189-byte trusted manifest is refused with "the trusted manifest does not decode as an administrator manifest" (`compromise/binding.rs:63`), not "…is not a profile-3 manifest" (`:65`). The exit code (2) is unchanged. No runbook quotes the old text.

### Commits and gate order

- `plan: adr 0100 root2 profile three only` (this section, the K7 amendment, the corpus and its test, the D-G3 seeds). The crate architecture and MV tests pass before FF.
- Then a, b1, b2, b3, c1, c2, d, in that order.

## Key custody slice 4 plan: key-guard custody, terminal and memory helpers

### Status

Plan only. This section authorizes no source changes.
- Contract: key custody r4 (`/home/me/work/.agent-bridge/wendelon-adr0101-key-custody-contract-r4/CONTRACT.md`), §2 K1(a), K2, K3 (passphrase, process hardening, memory), K4 (host check) and §7 item 4.
- Slice 3 (the operator proof facade) is on main.

### Placement

KG is `nilket/crates/key-guard`. It holds every syscall the custody tool needs, because `wendelon-key` forbids unsafe code (K1(a)).

The following stay out of KG and belong to slice 5 (`wendelon-key`):
- key generation;
- the K3 envelope (AES-256-GCM, scrypt, the identity digests, golden vectors);
- passphrase policy (NFC, at least 16 characters);
- the `custody.log` JSON hash chain.

They need non-FIPS aws-lc-rs, ed25519 and the Root2 context. KG is linked into the FIPS daemon through `tls-pki`, and a node links no role-signing path (§2 Owner).

KG is form- and role-agnostic. It never parses key or command bytes, so it adds nothing that limits new proof forms, such as D-21's `RoleForm3::Containment`.

The new modules are Linux-only (`renameat2`, `prctl`). On other platforms the tool refuses (K2, "Unix only").

### Algebra

- **Identity.**
  - A custody directory is the `(st_dev, st_ino)` of the handle that was checked, never its path.
  - A custody file is one single-component name resolved through that directory handle. After opening, it is the opened handle.
  - KG never interprets file contents. Key identity (the K3 Pending and Bound digests) is slice 5.
- **Owner.** The effective uid:
  - the directory is mode 0700 and owned by the euid;
  - every ancestor, reached by `openat(fd, "..", O_PATH|O_DIRECTORY)` and judged on its fstat (so an execute-only ancestor such as a 0711 `/home` is not refused for lack of read permission), is owned by root or the euid and is neither group- nor other-writable;
  - files are mode 0600, owned by the euid, with `nlink == 1`.
- **Keys.**
  - The mutation key is an exclusive `flock` on the directory's open file description. Every mutating function exists only on the lock guard, so an unlocked mutation cannot be written. A handle refuses a second lock while its guard is held, because `flock` on the same open file description would succeed and either guard would unlock both.
  - A name is published at most once: create-new only, through `RENAME_NOREPLACE`, and never overwritten.
- **Laws.**
  - **G1, refuse and never repair.** Any failed check refuses with a typed reason. No mode, owner or link is changed on an existing object.
  - **G2, whole or nothing.** Creation runs `O_CREAT|O_EXCL|O_NOFOLLOW` 0600 temp, then write, fsync, `renameat2(RENAME_NOREPLACE)` and a directory fsync. EINVAL refuses with no plain-rename fallback, and every failure removes the temp file.
  - **G3, reads are bounded.** `openat O_RDONLY|O_NOFOLLOW|O_NONBLOCK|O_CLOEXEC`, then an fstat check of a regular file with an admitted exact size. `custody.log` is exempt from the size rule only, and is opened `O_APPEND`. A FIFO never blocks.
  - **G4, secrets are inert.** A `SecretBuf` has a fixed capacity and never reallocates. It is zeroed (with `zeroize`) before it is freed, and munlocked. Its `Debug` shows only the length, capacity and lock state. No KG error carries secret bytes.
  - **G5, mlock is best effort and reported.** Per K3, a failed mlock is not fatal. `SecretBuf::is_locked` reports the result so the tool can warn. The daemon's existing mlock and munlock move into KG unchanged in behaviour.
  - **G6, passphrase entry.** The passphrase is read from `/dev/tty` only, which must be a terminal. Echo, canonical mode and tty signals are off during the read. After `tcsetattr`, the settings are read back, and entry is refused unless ECHO, ICANON and ISIG are all off. `max` is capped at `MAX_SECRET_LEN`. The saved termios is restored when the guard drops, on return, on error and on unwind. The INTR, QUIT and SUSP characters abort the read as typed `Interrupted` after the restore. A fatal signal sent by another process is not caught (stated limit; recovery is `stty sane`).
  - **G7, hardening.** `PR_SET_DUMPABLE=0` and `RLIMIT_CORE=0`, both read back.
  - **G8, host probe.** `getifaddrs` lists the up, non-loopback interfaces, for K4's refusal in slice 5.

### Tests (each with a negative control)

| Test | Checks | Negative control |
|---|---|---|
| T-K2 | each defect is refused with its typed reason: symlink, mode 0644, setuid, wrong uid (on recorded facts), nlink 2, non-0700 directory, group-writable ancestor, symlinked directory, existing target | the same fixture with that one defect fixed is accepted |
| T-K2 walk | the production ancestor walk agrees with a path-based oracle up to `/` | a fixture-anchored walk with a group-writable ancestor is refused |
| T-K2 fifo | a FIFO under a custody name is refused without blocking | a regular file is accepted |
| T-K2 size | a size outside the admitted set is refused | the admitted size is accepted |
| T-K2 lock | a second lock on the same directory, through another handle or the same one, is `Busy` | after the guard drops, the lock succeeds |
| T-K2 search-only | an ancestor with search but not read permission is admitted | the same ancestor made group-writable is refused |
| T-TTY read-back and cap | settings with any of ECHO, ICANON or ISIG on are not quiet; a line over `MAX_SECRET_LEN` is `TooLong` under any larger `max` | engaged settings read back quiet; a line of exactly `MAX_SECRET_LEN` is read whole |
| T-K2 move/destroy | moving onto an existing name is refused, and a destroyed name is gone | moving onto a fresh name succeeds |
| T-K3z | after the wipe, the drop probe sees every byte zero | before the wipe, the same probe sees the bytes non-zero |
| T-SECRET | `Debug` and every error string, including for an overlong passphrase, contain no secret bytes | the probe finds the bytes in a plain formatted string |
| T-TTY | over a pty, the read returns the line with no echo, and termios is identical afterwards on success, error, `Interrupted` and panic | with echo on, the pty master sees the bytes |
| T-TTY non-tty | a regular file or a pipe is refused as not a terminal | a pty is accepted |
| T-HARD | in its own test process, dumpable and the core limit read back as 0 | before hardening, the process is dumpable and the hard core limit is non-zero |
| T-IF | the classifier admits exactly the up, non-loopback interfaces | loopback and down interfaces are excluded |

There are two KG source pins. Both enumerate every `.rs` file under `custody/`, not a fixed list, and both are probed:
- every `unsafe` lives in `custody/sys.rs`, and each carries a `SAFETY` comment;
- the walk's stop type is built only as the `/` constant and, with an anchor, only inside the private `#[cfg(test)] fn at`, and no non-test `pub` item names it.

### Test fixtures

All fixtures live under KG's own self-cleaning `TestDir`, in `TMPDIR`.

Fixture ancestors on a dev host can be group-writable: `/home/me/work` is 0775 under umask 002. A `cfg(test)`-only anchor therefore ends the ancestor walk at the fixture root, and nothing in the public API can stop the walk early. Slice 5 inherits this constraint and decides its own fixture route.

### Slice 5 obligations

These are review findings that KG cannot settle alone. Slice 5 must meet each one, or state it as a limit:
- **Lock scope.** The tool takes only the top-level custody directory's lock. `move_file` into a subdirectory relies on the parent's lock, so slice 5 either requires holding the target's lock too or enforces that the target is a child of the locked directory.
- **Panic and the terminal.** Release builds use `panic = "abort"`, so unwinding never runs the termios guard. Slice 5 installs a panic hook that restores the terminal, or states the gap as a limit.
- **Leftover temp files.** A crash between the temp create and the rename leaves a `.wrolekey.tmp.` file, which no custody name can open. Slice 5 reports any it finds.

### Dependencies

- `zeroize` 1.8 is added to key-guard. It is already in `Cargo.lock`, and the daemon already links it through wendelon.
- `wendelon` gains a direct path dependency on `key-guard`, which it already links through `tls-pki`.
- No crate is new to the workspace.

### Commits and gate order

- `plan: adr 0101 key custody guard functions and secret handling` (this section). The arch and MV checks pass before FF.
- `impl: adr 0101 key custody guard functions and secret handling`.

Gate order: independent source review → ladder → log audit → FF.

## Key custody slice 5a plan: guard amendments for the key tool

### Status

Plan only. This section authorizes no source changes.
- Contract: key custody r4 (`/home/me/work/.agent-bridge/wendelon-adr0101-key-custody-contract-r4/CONTRACT.md`), §2 K2 and K3, and the coordinator rulings in `IMPL-NOTES.md` next to it (slice 5 split; D-a).
- Slice 4 (KG) is on the branch. This slice settles the three "Slice 5 obligations" recorded in the slice 4 section, and adds the test route D-a chose.
- Slice 5 is split into 5a (this section), 5b (the `wendelon-key` crate without signing), 5c (`build`, `sign`, `sign-statement`, `merge`) and 5d (containment, D-21). Each has its own plan, impl, review, ladder, audit and FF.

### Behavior

Key-guard only. No wendelon source changes.
- **Directory entries.** `CustodyDir::names()` lists the directory through its handle (`openat(fd, ".")`, `fdopendir`, `readdir`), never by path. It opens no entry, so a FIFO cannot block it. `CustodyLock::leftover_temps()` returns the entries that carry the temp prefix `.wrolekey.tmp.`. It exists only on the lock, because under the lock no other custody operation can be between its temp create and its rename. The tool reports leftovers; nothing removes them (G1).
- **Lock scope.** `CustodyLock::move_file` refuses with `Refusal::OutsideLock` unless the target directory is the locked directory itself or a direct child of it. A child is decided by identity: `openat(to, "..")` must have the locked directory's `(dev, ino)`. A grandchild or an unrelated custody directory is refused, so one lock covers every move.
- **Terminal restore on panic.** Release builds use `panic = "abort"`, so no destructor runs on a panic. While an `EchoOff` guard is engaged, its saved settings and a duplicate of the terminal handle are registered in a process-wide list, and the guard's drop removes them. `install_terminal_restore_hook()` installs a panic hook that restores every registered terminal and then calls the previous hook. The panic hook runs before the abort. The hook uses `try_lock`, so a panic while the list is locked skips the restore instead of deadlocking. A fatal signal from another process is still not caught (the existing stated limit).
- **Fixture anchor (D-a).** A key-guard cargo feature `fixture-anchor` gates `open_custody_dir_anchored(path, anchor)`. It is the production open, except that the ancestor walk also ends at the anchor directory's `(dev, ino)`. Fixtures of a downstream crate under a group-writable tree (`/home/me/work` is 0775) need it. It is not in `default`. Only `wendelon-key`'s `[dev-dependencies]` may enable it. Resolver 3 does not unify dev-dependency features into a non-test build, so no shipped binary links the anchored open.

### Algebra

- **Identity.** A directory is its `(dev, ino)` from the checked handle (unchanged). An entry is one name read through that handle. A move target is admitted by the identity of its `..`, never by a path.
- **Owner.** The effective uid (unchanged). The terminal-restore list is owned by the process, and its entries by the guard that registered them.
- **Keys.** The mutation key is still the top-level directory's lock. The lock-scope law makes it the only key: every move stays within the locked directory or one level below it.
- **Laws.**
  - **G9, listing is inert.** Listing opens no entry and changes nothing.
  - **G10, leftovers are reported under the lock and never removed.**
  - **G11, one lock covers a move.** The target is the locked directory or its direct child.
    - Limit: a holder of only a child's lock can race a move into that child made under the parent's lock. This is harmless: both sides publish only through create-new names (`RENAME_NOREPLACE`, `O_EXCL`), so the loser gets `Exists` and nothing is overwritten. The tool always takes the parent's lock first.
  - **G12, a panic restores the terminal** when the hook is installed and the list is not locked. Otherwise the existing stated limit applies.
  - **G13, the anchor is not shipped.** The anchored open exists only under `cfg(test)` or the `fixture-anchor` feature. No normal or build dependency edge enables the feature, no package feature forwards it, and only `wendelon-key`'s dev edge may enable it.
- **Visibility.** Entry names and the restore list carry no secret bytes. A name is not a secret.

### Tests (each with a negative control)

| Test | Checks | Negative control |
|---|---|---|
| T-ENT | `names()` lists files, subdirectories, a FIFO (without blocking) and a non-UTF-8 name, and excludes `.` and `..` | an empty directory lists nothing |
| T-LEFT | a planted `.wrolekey.tmp.` file is reported by `leftover_temps()` and still present afterwards | after only `create_file`, nothing is reported |
| T-SCOPE | a move into a grandchild, a sibling custody directory, and the parent of the locked directory are refused as `OutsideLock`, and the file stays | a move into the locked directory itself and into a direct child succeeds |
| T-PANIC | with the hook installed, a guard forgotten (simulating abort) and a panic on another thread restores the pty's settings | with no restore, a forgotten guard leaves echo off |
| T-PANIC list | the drop of a guard removes its registration; the abort branch restores every registered terminal, whichever thread engaged it | while engaged, the registration is present; the unwinding branch leaves another thread's terminal quiet |
| T-ANCHOR | the anchored open admits a fixture whose group-writable ancestor lies above the anchor | the production open refuses the same fixture with `AncestorWritable` |
| T-FEATURE | over `cargo metadata --no-deps --offline`: no workspace package enables `key-guard/fixture-anchor` on a normal or build edge, no package feature forwards it (also through a renamed dependency), `key-guard`'s `default` does not hold it, and only `wendelon-key` may enable it on a dev edge | synthetic metadata with a normal edge, a forwarding feature, a feature forwarded through a renamed dependency, a default entry, and a dev edge from another package each fail; a `wendelon-key` dev edge passes |

The source pins are amended equally strict, and each new rule is probed:
- The anchor pin admits the stop's anchor constructor only under the exact attribute `#[cfg(any(test, feature = "fixture-anchor"))]`, and outside test files only the anchored open calls it.
- The anchored open and its re-export carry the same exact attribute.
- Every `unsafe` stays in `sys.rs` with a `SAFETY` comment.

### Dependencies

- `serde_json` 1 becomes a key-guard dev-dependency for T-FEATURE. It is already in `Cargo.lock`, and dev-dependencies do not enter the daemon graph.
- There is no new normal dependency, and no crate is new to the workspace.

### Commits and gate order

- `plan: adr 0101 key custody guard amendments for the key tool` (this section). The arch and MV checks pass before FF.
- `impl: adr 0101 key custody guard amendments for the key tool`.

Gate order: independent source review → ladder → log audit → FF.

## Key custody slice 5b plan: the wendelon-key tool without signing

### Status

Plan only. This section authorizes no source changes.
- Contract: key custody r4 (`/home/me/work/.agent-bridge/wendelon-adr0101-key-custody-contract-r4/CONTRACT.md`) §2 (identity, K1(a), K2-K6), §3 items 1 (the key-file subcommands), 3 and 4, §5; the coordinator rulings in `IMPL-NOTES.md` (split, D-a, D-b).
- Slice 5a (guard amendments) is on the branch. 5c adds `build`, `sign`, `sign-statement` and `merge`; 5d adds containment.

### Behavior

A new package `nilket/crates/wendelon-key`: a library holding all logic and a thin `wendelon-key` binary. Both are `#![forbid(unsafe_code)]`; every system call goes through key-guard. It depends on `wendelon` with `default-features = false, features = ["non-fips"]`. Nothing depends on it, and the FIPS daemon build (`--bin wendelon --features fips`) does not contain it.

Subcommands. Every one takes `--dir <custody dir>`, opened with `open_custody_dir`, and holds its lock for the whole operation:
- `generate --role R --intended-epoch E --out NAME [--context F --trusted-manifest M]`. The seed is 32 bytes from `aws_lc_rs::rand::fill`; a deterministic seed exists only under `cfg(test)`. The passphrase is asked twice. The public key must pass the facade's new-key check: enrollable, and with a context, not already in the history (`Denied(21)`) and the role not at 64 successors (`Denied(22)`). Writes a Pending file and prints the 64-hex public key and its fingerprint.
- `bind --key NAME --context F [--context F2] --trusted-manifest M --out NAME2 [--epoch E]`. Decrypts the Pending file. Refuses unless:
  - the verified context's `history[role][epoch] == pk`, where the epoch is the intended one unless `--epoch` names the one actually received;
  - the Pending context, if it has one, equals the verified context;
  - with no successors (genesis), a second export has byte-identical context bytes and a different `node_id`.

  Re-encrypts under the same passphrase with a fresh salt and nonce, writes the Bound file `NAME2`, and moves the Pending original into `pending/`.
- `show-public --key NAME` decrypts and prints exactly one 64-hex line, the key derived from the seed, only if it equals the header key. `show-public --unauthenticated` decrypts nothing and lists every key file in the directory, `pending/` and `retired/`, each line labelled `UNAUTHENTICATED`.
- `backup --key NAME [--pending] --to DIR2 --out NAME2` decrypts under the primary passphrase. `--pending` reads a Pending original from `pending/`, under the same lock. It writes a second envelope with `backup_of = id` under a backup passphrase, asked twice, which must differ from the primary.
- `restore --from DIR2 --backup NAME2 --out NAME` decrypts under the backup passphrase and re-encrypts under a new primary passphrase, asked twice, with a fresh salt and nonce.
- `restore-verify --key NAME --from DIR2 --backup NAME2 --context F --trusted-manifest M` decrypts the backup in memory only. It requires the backup's identity to equal the Bound file's identity and the key to be the verified context's key for that (role, epoch). It writes no key file.
- `retire-epoch --key NAME --context F --trusted-manifest M` refuses unless the file is Bound, its context equals the verified one, and the verified context shows a later epoch for its role. The operator types the number of backups the log records for this identity. Then the file moves to `retired/`.
- `destroy --key NAME [--retired] --context F --trusted-manifest M (--context F2 | --successor NAME3)` decrypts the file. It refuses unless the epoch is retired in the verified context and there is second-source evidence:
  - a second export from a different node showing the same context and the same later epoch; or
  - the successor's Bound file decrypting on this host to the history key of a later epoch.

  The operator then types the fingerprint, and the file is overwritten and unlinked (hygiene, not secure erasure).
- `destroy --pending --key NAME --context F --trusted-manifest M --evidence accepted-proof|crypto-valid-row --evidence-digest D` decrypts the Pending original in `pending/`. It refuses unless the Pending header's context, when it carries one, equals the verified context, and the verified context shows the key's epoch active. The operator types the fingerprint; the evidence is logged.
  - Limit: an orphan Pending file, one that can never bind (its rotation was denied or another landed first), has no destroy route yet, because `destroy --pending` requires its epoch active. The slice-7 runbook carries this as an item (until then: keep it, encrypted, and record it at log review).
Common rules:
- **Process and terminal.** Startup installs the terminal-restore hook, then `harden_process`.
- **Passphrases.** Passphrases and typed confirmations are read only through key-guard's `Terminal`. A passphrase is UTF-8, NFC-normalised into a `SecretBuf`, and at least 16 characters.
- **Host checks (K4).** They run on every listed operation for a recovery-role file, and on every file `destroy` decrypts (the target, a `--successor` file and a Pending original): a data directory (`<XDG data home>/nilket/local` or `$HOME/.wendelon` exists) or any up, non-loopback interface refuses, unless `--override-host-checks` is given, which is logged.
- **Swap.** A non-empty `/proc/swaps` prints a warning and is logged.
- **Leftovers.** Temp files left in the custody directory are reported on stderr and logged, never removed.
- **Lock scope.** Moves go only from the custody directory into `pending/` or `retired/`, its direct children. Mutations inside a child also hold that child's lock.
- **Exit codes.** 0 on success, 2 on any refusal.

### File format (K3), version 1, 371 bytes

| Offset | Field |
|---|---|
| 0..8 | `WROLEKEY` |
| 8..10 | version u16 LE = 1 |
| 10 | state: 1 Pending, 2 Bound |
| 11 | provider: 1 file |
| 12 | KDF id: 1 scrypt |
| 13 / 14 / 15 | log_n (17..=20), r (8), p (1..=4) |
| 16..48 | salt |
| 48..60 | nonce |
| 60 | role (1 admin, 2 governance, 3 recovery) |
| 61..69 | epoch u64 LE (Pending: intended epoch) |
| 69 | context present (0/1; Bound: 1) |
| 70..258 | context, or 188 zero bytes |
| 258..290 | public key |
| 290 | `backup_of` present (0/1) |
| 291..323 | `backup_of`, or zeros |
| 323..371 | AES-256-GCM over the 32-byte seed: ciphertext 32, tag 16 |

- The AAD is bytes 0..323 exactly as read.
- The key is scrypt(NFC passphrase, salt, log_n, 8, p) → 32 bytes.
- New files use log_n 17, p 1 (128 MiB).
- A header outside the bounds, a non-zero fill, a presence byte other than 0/1, an unknown state, provider, KDF or version, or any length other than 371 refuses before derivation. The size check is key-guard's exact-length admission.
- After decryption, the key derived from the seed must equal the header key.

Identity digests, as the contract defines them (role u8, epoch u64 LE, key 32 B):
- Bound: SHA-256(`WROLEKEY-ID\0` ‖ context ‖ role ‖ epoch ‖ key).
- Pending: SHA-256(`WROLEKEY-PENDING\0` ‖ role ‖ intended epoch ‖ has-context ‖ context or zeros ‖ key).

A backup carries its primary's identity in `backup_of` and has the same identity itself. The display fingerprint is the first 16 hex digits of SHA-256(key); it is never an identity.

### Operation log (`custody.log`)

- **Format.** One JSON line per operation, through key-guard's `open_log`. Fields: `prev`, `time`, `host`, `operation`, `identity`, `command_digest` (null in 5b), `form_slot` (null in 5b), `outputs` (name and SHA-256 of each written file), `backup_of`, `overrides`, `leftovers`, `swap` and `outcome`.
- **Chain.** `prev` is the SHA-256 of the previous line's bytes, and zeros for the first line.
- **Coverage.**
  - Every operation that reaches the locked directory appends exactly one line: successes, refusals and wrong passphrases. This includes a failed host probe and a busy child-directory lock: both happen after the lock is held.
  - A wrong-passphrase line records the file's identity read from its header, labelled `unauthenticated_identity`, since decryption never authenticated it.
  - An operation refused before the directory is admitted has no log to write to (stated limit).
  - Before appending, the whole chain is verified; a broken chain refuses the operation.
  - The head digest is printed on stderr after every append, for the runbook's review record.
  - The chain cannot detect a truncated tail (stated limit).

### Algebra

- **Identity.**
  - A key file is its K3 identity digest: Pending or Bound, from the authenticated header after decryption, never from its name.
  - A custody directory is its `(dev, ino)` (key-guard).
  - A log line is its position in the SHA-256 chain.
  - A backup is the pair (its own identity, which equals the primary's, and `backup_of`).
- **Owner.** The operator's effective uid. The tool is the only writer; no node reads or links it.
- **Keys.**
  - Mutations hold the top-level directory's lock for the whole operation, plus a child's lock when mutating inside it.
  - A file name is published once (create-new).
  - Pending → Bound is keyed by the verified history's `(role, epoch) → key`.
- **Laws.**
  - **KL1, secrets.** The seed and passphrases live in `SecretBuf`s. The dalek `SigningKey`, the scrypt working set and the AES key schedule are the contract's stated limits.
  - **KL2, decrypt then authenticate.** No header field enables a key use (bind, backup, restore, show-public, destroy) before decryption succeeds and the derived key equals the header key. An unauthenticated header may only cause a refusal, a listing (`--unauthenticated`, which says so on every line), the comparison `restore-verify` makes against an authenticated backup, or the `retire-epoch` move, which only restricts a key.
  - **KL3, bind needs history.** A Bound file exists only for a `(context, role, epoch, key)` the re-verified chain holds; genesis needs two node-distinct, byte-identical exports.
  - **KL4, one line per operation** that reaches the locked directory, chained.
  - **KL5, exact format.** One length per version; the AAD is the on-disk header bytes.
  - **KL6, closed process.** No unsafe code, no network, no passphrase source but the terminal.
  - **KL7, no reimplementation.** Every acceptance decision about keys and contexts comes from the facade (`verify_context_export`, `VerifiedContext`, the new-key check).
- **Visibility.** stdout carries public keys, fingerprints and listings only. Passphrases, seeds and derived keys never appear in output, `Debug`, errors or the log.

### Facade amendment

`operator_proof::check_new_role_key(context: Option<&VerifiedContext>, role: u8, key: &[u8; 32]) -> Result<(), ProofRefusal>` runs `check_enrollable_admin_key2`. With a context, it also runs the rotation's own reuse and successor-limit rules: the key is anywhere in the history, or `SUCCESSORS3` successors already exist for the role. It adds `ProofRefusal::{NotEnrollable, KeyReused, SuccessorLimit}`. The facade public-items pin is amended to the new export list, equally strict. The facade still contains no signing code.
- **Follow-up (refactor).** `check_new_role_key` restates the `Denied(21)` and `Denied(22)` predicates of `role_epochs.rs` (reuse anywhere in the history; the per-role successor limit) over the facade's verified history. A later refactor shares one predicate between the two.

### Tests (each with a negative control)

| Test | Checks | Negative control |
|---|---|---|
| T-FMT | golden v1 vectors (deterministic seed, salt and nonce, `cfg(test)` only) decode and decrypt to the pinned key | a version+1 file, and each malformed field, refused before derivation |
| T-K3 | flipping each of the 323 header bytes, one at a time, fails; each valid alternative of a field (state 2→1, role 3→1, p 1→2) fails decryption, so the associated data covers it; log_n 16 and 21, r 9 and p 0 and 5 refused before derivation; trailing and truncated bytes refused; wrong passphrase refused; derived-key mismatch refused | the unmodified file decrypts |
| T-PASS | the argument parser has no passphrase flag (`--passphrase` is an unknown flag); a set `WENDELON_KEY_PASSPHRASE` has no effect; the binary without a controlling terminal (`setsid`) is refused as not a terminal before anything else; no non-test source reads the environment (except the host probe's `HOME` and `XDG_DATA_HOME`) or stdin; the tool's `startup` leaves its own test process non-dumpable with a zero core limit | the same binary under a pty (`script`) passes the terminal check and reaches the custody directory; the test process is dumpable before `startup`; a short (15-character) or mismatched pair is refused, and a 16-character pair in two normal forms is accepted |
| T-GEN-BIND | genesis bind with one export refused; two exports with the same `node_id` refused; Pending kept in `pending/` after bind; Pending backup and restore work; `destroy --pending` without a context showing the epoch active refused | genesis bind with two byte-identical exports from different nodes succeeds |
| T-BIND | Pending → Bound for a successor | mismatched epoch, and a history without the key, refused |
| T-CTX | partial coverage and a tampered chain refused (facade reader); header context ≠ verified context refused | a verified chain is accepted |
| T-HOST / T-K4 | each recovery-role operation refuses with a data directory present, and with a non-loopback interface up, including `destroy` of a recovery key (with a `--successor` file) and `destroy --pending` of a recovery Pending original; the override flag is logged | the same operation on a clean host probe succeeds |
| T-K5 | backup → restore → show-public round trip; restore-verify accepts | a backup for another epoch or context, a wrong backup passphrase, and a backup passphrase equal to the primary are refused |
| T-K6 | retire succeeds with a verified later epoch; destroy succeeds with each second-source form and the typed fingerprint | retire without a successor, and with a forged (unsigned) successor, refused; destroy without second-source evidence, or with the wrong fingerprint, refused, and the file is intact |
| T-LOG | every operation, success and refusal, appends one line and the chain verifies | an edited middle line breaks the chain, and the next operation refuses |
| T-SECRET | the passphrase, the seed bytes and the seed hex appear in no stdout, stderr, log, error `Display` or `Debug` across a full run | the probe finds them in a plainly formatted string |
| T-LEFT | a planted temp file is reported and logged, and kept | a clean directory reports none |
| T-K1a | over `cargo metadata --no-deps`, no package but `wendelon-key` depends on `wendelon-key`; the FIPS build lines in `ci/check_fips_symbol_sweep.sh` and `ci/generate_regulated_release_evidence.sh` name only `--bin wendelon`; `src/cli.rs` has no `key` branch | a synthetic metadata fixture where `nilket` depends on `wendelon-key` fails; a probe `--bin wendelon-key` line fails |
| T-FIPS (D-b) | `cargo tree` of `-p wendelon --no-default-features --features fips`: no `wendelon-key`, no aws-lc-rs `non-fips` feature, no wendelon `non-fips` feature | the default `-p wendelon` graph does show wendelon's `non-fips`, so the matcher sees real output |
| T-NET | no `net`, `socket`, `reqwest`, `quinn`, `tokio` or `opendal` token in `wendelon-key` sources; `#![forbid(unsafe_code)]` heads the library and the binary | a probe token in a copy of a source fails |
| T-ANCHOR-USE | `open_custody_dir_anchored` appears only in `cfg(test)` modules of the tool | a probe call in a non-test module fails |

Fixtures:
- Every fixture lives in a `TMPDIR` directory admitted through key-guard's `fixture-anchor` open, which only `wendelon-key`'s `[dev-dependencies]` enable (5a pin tightened to that).
- Exports are built by the tests' own encoder of the C-S6b format over the published `M3_S1` manifest (the D-G3 test seeds). Successor proofs come from the facade's `prepare` and `splice`, with signatures from the test seeds.
- Passphrases, confirmations, the host probe, randomness and the clock are injected through one `cfg(test)` seam; production wires key-guard's `Terminal`, `non_loopback_interfaces_up`, `aws_lc_rs::rand` and the system clock.

### Dependencies

`wendelon-key`:
- **Normal dependencies.** `wendelon` (path, non-FIPS), `key-guard` (path), `aws-lc-rs`, `scrypt` 0.11, `ed25519-dalek` 2, `serde` and `serde_json`, `thiserror` 2, `unicode-normalization` 0.1, `zeroize`. SHA-256 comes from aws-lc-rs.
- **Dev-dependency.** `key-guard` with `fixture-anchor`.
- **Lock file.** All are already in `nilket/Cargo.lock`, and no crate is new to it.
- **Profile change.** `[profile.dev.package.scrypt]` and `[profile.dev.package.salsa20]` get `opt-level = 3` in the workspace manifest, so the 128 MiB KDF runs in test builds at release speed. Code generation for other packages is unchanged.

### Commits and gate order

- `plan: adr 0101 key custody wendelon-key tool without signing` (this section). The arch and MV checks pass before FF.
- `impl: adr 0101 key custody wendelon-key tool without signing`.

Gates:
- the new crate's tests at default threads and at 8;
- key-guard;
- the wendelon lib;
- both wendelon arch tests;
- `check --workspace`;
- `fmt --check`;
- the D-b graph checks.

Order: independent source review → ladder → log audit → FF.

## Key custody slice 5c plan: wendelon-key signing

### Status

Plan only. This section authorizes no source changes.
- Contract: key custody r4 (`/home/me/work/.agent-bridge/wendelon-adr0101-key-custody-contract-r4/CONTRACT.md`) §3 item 1 (`build`, `sign`, `sign-statement`, `merge`), K3 (the `sign` refusals), K4 (what-you-sign), K5 (the round trip through `sign`), K7 (no reimplementation, operator choices), §5 T-SIGN, T-K7, T-WYS, T-MERGE.
- Slices 5a and 5b are on main (`c66fcaeb9`). 5d (containment, D-21) follows.

### Behavior

Four subcommands. Every one runs in a locked, logged session on `--dir` (5b). The facade is unchanged: every layout, preimage, splice, merge and self-check is `operator_proof`'s, and the tool adds only the Ed25519 signature over the facade's preimage.
- **`build --dir D --context F --trusted-manifest M --out NAME <kind> [args]`.** It opens no key. Kinds:
  - `rotate-admin|rotate-governance|rotate-recovery --key HEX64`;
  - `freeze` and `unfreeze`, whose binding is the context's `transition` (`freeze_binding`);
  - `retire-root`.

  It writes the canonical `Command2::encode` bytes as `NAME.command` and prints the full command digest and its what-you-sign code. `build statement --action grant|revoke --permission-key ID32HEX:REVISION [--sidecar visads02|visads03]` writes nothing and prints the statement's code (below).
  - The nine governance kinds are not built here: their field sets belong to the requesting node's own tooling (stated limit). `sign` accepts their canonical bytes from any producer.
- **`sign --dir D --key NAME --context F --trusted-manifest M --command FILE [--partial FILE] [--with ROLE] [--sidecar visadg01|visrol03] [--override-host-checks] --out NAME2`.**
  - **Gates before any output.** Host checks run, then the passphrase is read and the file decrypted. Then come the K3 refusals, in order:
    - the file is Bound;
    - its context is the verified context byte for byte;
    - the history holds its key at its role and epoch.

    The facade's `prepare` then refuses the rest (typed `ProofRefusal`): a stale or retired epoch, a role the requirement does not name, a retired root, a frozen root with a governance command, a wrong `--with` or `--sidecar`, and the non-role-signed kinds.
  - **Display and confirmation.** The tool prints the kind and its decoded fields, the signer's role, epoch and form and slot, the Freeze/Unfreeze binding, and a TERMINAL warning for RetireRoot. It withholds the command digest and a rotation's new key. The operator types the 32-hex what-you-sign code. A mismatch aborts with nothing written.
  - **After confirmation.** The tool prints the full command digest, the new key and its fingerprint (rotations), and the SHA-256 of the exact preimage. It then signs that preimage and splices the signature.
  - **Checks before writing.**
    - A complete proof must pass `self_check`.
    - A first joint partial must be accepted by the facade as the partner's `--partial`: `prepare` for the partner's active key verifies the signed slot with the production check.
    - A second signer's `--partial` is checked by `prepare` against the layout rebuilt from (command, context).
  - **Output.** `NAME2.command` (the exact command bytes signed over) and `NAME2.sidecar`, create-new 0600 in `D`. Their SHA-256 digests are printed and logged with the command digest and the form and slot.
- **`sign-statement --dir D --key NAME --context F --trusted-manifest M --action grant|revoke --permission-key ID32HEX:REVISION [--sidecar visads02|visads03] --out NAME2`.** The same gates, then `prepare_statement`: VISADS03 by default, VISADS02 only at administrator epoch 0 and frozen roots refused. The display shows the action, authority and namespace. It writes `NAME2.command`, the full `Command2` with the signature embedded, after `self_check`.
- **`merge --dir D --context F --trusted-manifest M --command FILE --partial A --partial B --out NAME`.** It opens no key file and calls the facade's `merge`, which refuses any byte difference outside 86..150 and 160..224, a slot filled twice or not at all, a layout other than the one rebuilt from (command, context), and a slot that does not verify under its claimed (role, epoch). The facade's `self_check` runs before `NAME.sidecar` is written.

**What-you-sign code.** K4's two sentences ("it then prints … for rotations, the new key's hex" and "never prints the command digest or the new key before confirmation") are read together: the decoded display is in two parts, and the key and digest are shown only after the code matches. The code is 32 hex digits (128 bits; the K4 amendment in `IMPL-NOTES.md`, "Slice 5c review rulings": 64 bits would let an attacker who alters the command file in transit search for another command with the same prefix):
- for a rotation, the first 32 hex digits of the new key, which the operator reads from the new key's authenticated `show-public` over a second channel;
- for every other command, the first 32 hex digits of the command digest, from `build` on the requesting host, or from the command's producer;
- for a statement, the first 32 hex digits of SHA-256 of the facade's unsigned statement preimage, from `build statement`.

### Algebra

- **Identity.**
  - A command is its canonical bytes under the context's (generation, cluster). Its digest is `Command2::digest`.
  - A proof output is (command bytes, sidecar or statement bytes) for one signer (role, epoch, slot, form).
  - The signed object is exactly the facade's preimage for that layout.
- **Owner.** The operator signs. A host that merges holds no key.
- **Keys.** One signature per (proof layout, slot). The slot comes from the key's role; the only operator choices are `--with` and `--sidecar`. A merge combines the two slots of one layout.
- **Laws.**
  - **SL1, what-you-sign.** The bytes displayed, the bytes the code is checked against, the preimage signed, and the command bytes written all come from the one command buffer read from `--command`. A wrong code writes nothing.
  - **SL2, no output without the production check.** No sidecar or statement is written unless the facade has checked it: `self_check` for a complete proof, partner acceptance for a first partial.
  - **SL3, the facade decides.** Every refusal about a layout, form, slot, epoch, freeze or retirement is the facade's typed `ProofRefusal`. The tool adds only the 5b file checks and the confirmation.
  - **SL4, outputs are bearer authority.** They are written create-new 0600 into the custody directory and logged by digest, never by content.
- **Visibility.** stdout and the log carry digests, public keys, decoded command fields and file names. Seeds, passphrases and the 64-byte signature never appear on stdout or in the log; the signature is only in the written file.

### Recorded limits and amendments (coordinator, from the 5a/5b second opinion)

- **Signing authority.** In 5c and 5d, signing requires an authenticated Bound header (decrypted, derived key equal to the header key), its context equal to the verified context, the history holding its key at its role and epoch, and that epoch active. Whether a file is in or out of `retired/` never defines authority: a file moved back out of `retired/` is still refused by the active-epoch rule, and a file never retired is refused once its epoch is stale.
- **KL4 is completed-invocation logging, not crash-atomic.** A line is appended after the mutation it records. After a crash, or a failed log append or fsync, an operation may have published, moved or destroyed a file with no final line. Recovery is by inspection (the directory listing, `show-public --unauthenticated`, the log head), never a blind retry. Bind's create-then-move has the same rule: a Bound file whose Pending original is still beside it, not in `pending/`, means the move did not happen. The original stays where it is, encrypted, and the operator records it at log review; a repeated `bind` refuses, because its output name exists. Slice 7's runbook carries both as items.
- **What `restore-verify` proves.** It proves the backup decrypts under its passphrase, carries the Bound file's identity, and holds the verified history's key. It does not decrypt the primary, so it does not prove the primary's ciphertext is usable. The command's output line and slice 7's runbook say so.
- **G11 as built** (fix `custody move takes the destination lock …`). `move_file` takes the destination directory's `CustodyLock` as a capability, either the locked directory itself or a locked direct child, so both directories a move mutates are held. Locks never wait, so the parent-then-child order cannot deadlock, and a busy child refuses. `bind` and `retire-epoch` take the child's lock first, before checking that the destination name is free and before publishing anything, so the name check runs under the lock that covers the move. A busy `pending/` refuses `bind`, and a busy `retired/` refuses `retire-epoch`, each with nothing changed (tested, each with a control). The 5a limit "a child-only lock can race a parent-locked move" no longer applies.
- **Audit provenance as built.** `retire-epoch` logs the header's identity as `unauthenticated_identity`, since it decrypts nothing. `restore-verify` logs the Bound file's claimed identity as `unauthenticated_identity` and sets `identity` only after the authenticated backup matches it.

### Tests (each with a negative control)

| Test | Checks | Negative control |
|---|---|---|
| T-SIGN | each refusal, with nothing written: Pending file; header context ≠ verified context; key not in history; header epoch ≠ history epoch; stale (retired) epoch; a role the requirement does not name; retired root; frozen root with a statement and with a governance command | each paired with a run that differs only in that condition and signs |
| T-K7 (tool) | through `sign` and `merge` in the tool: single recovery slot RotateAdmin, RotateGovernance, Freeze, RetireRoot; two-host merged RetireRoot [1,2], RotateRecovery [1,2], Unfreeze [1,3] and [2,3], each also via a second signer over the first's partial; the governance kinds with VISADG01 (governance epoch 0) and VISROL03 [2,0] (epochs 0 and 1); VISADS02 (admin epoch 0) and VISADS03 (epochs 0 and 1). Each output passes the facade's `self_check` independently of the tool | the refused cells: Bootstrap, Cover, VISADC01, VISADG01 after governance rotated, VISADS02 after admin rotated, the wrong `--with`, each with nothing written |

Reserve and Finalize carry authority-key claims whose fixtures belong to wendelon; their refusal (`NotRoleSigned`, the same branch as Bootstrap and Cover) is covered at the facade by slice 3's T-K7, and the tool passes the facade's refusal through unchanged.
| T-WYS | a wrong code writes nothing, and the log records the refusal; a code that matches only the first 16 hex digits is refused; the signature verifies over the facade preimage rebuilt from the written command and the context; the displayed digest and preimage hash equal those recomputed from the written files; the written command bytes equal the input bytes | the correct code writes; the same signature does not verify over another command's preimage, and another command displays another digest |
| T-MERGE | a two-host merge passes `self_check` | mismatched commands, partials of another root's context, a slot filled by both partials, an invalid first slot, and a single byte changed outside the slot ranges are each refused; a difference only inside the slot ranges (the normal case) merges |
| T-K5 (5c leg) | backup → restore → sign round trip: the restored key signs and its proof passes `self_check` | a restored file under the wrong passphrase does not sign |
| T-HOST (5c rows) | `sign` and `sign-statement` with a recovery or any key on a dirty host: recovery refuses, override logged | a clean host signs |

### Dependencies

None new. The facade's public surface is unchanged, so its pin is not amended.

### Commits and gate order

- `plan: adr 0101 key custody wendelon-key signing` (this section). The arch and MV checks pass before FF.
- `impl: adr 0101 key custody wendelon-key signing`. It touches no `plans/**` file.

Gates: as 5b. Order: independent source review → ladder → log audit → FF.
