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
- State V6: the V5 extension plus an 84 B retired record (Log2, entry fingerprint, command digest). It is written only for a retired root. A repeat retire is `Registered(first)`.
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

- **C-S6:** the compromise-audit tool (lists entries accepted under a role key between a given coordinate and detection) and the runbooks, including M2.
- **C-S5:** M2 handover to a successor root.
- **Key custody:** per-role key files with owner-only permissions, an offline recovery key, and encrypted backups.

Real customer data needs all of C-S3a/b, C-S6, custody and the M2 runbook.

## Gates (every slice)

contract (with Algebra) → census delta → unsigned source → pre-seal
`cargo check --lib --tests` + fmt → recorded guard preflight → fresh
local → review → sign → SOURCE verify → probe → dev → 18-step ladder →
external audit → FINAL → joint FF. A rebase past other-lane commits with
zero overlap gets one confirming ladder.
