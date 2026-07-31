# Brainstorm 018: FROST-Signed Raft / WAL Authority

## Status

Open as a placeholder split out of brainstorm 015. **Not on
the active queue.** No ADR drafting from this brainstorm until
the trigger conditions below fire.

## Premise

Brainstorm 015 (algebraic ring topology) bundled five separable
threads. Four were closed or split during the 2026-04-29 council
session (see the status header in
`plans/brainstorms/015_algebraic_ring_topology_prime_group.md`).
The fifth — **FROST-signed Raft / WAL entries** — was split here
because it is an *authority / durability* concern, not a
topology one.

The 015 closure verdict (opaque `NodeId` + membership-bound
signing key) means FROST consensus, if and when it lands, would
operate over signing keys referenced through membership state,
not over curve-derived `NodeId`s directly. That decoupling is
why this brainstorm can sit independently from topology.

## What this brainstorm is about

Whether Wendelon's Raft consensus log entries (and / or other
durability-log records: durability-replay frames, snapshot
boundaries, control-ring mutation receipts) should carry a
single constant-size threshold signature produced by FROST over
the quorum's signing keys, instead of (or in addition to) the
current per-node signature posture.

The candidate operational claims:

- **Constant-size WAL signature.** A 5-of-7 quorum signature is
  one Schnorr-shaped 32-byte point + 32-byte scalar, regardless
  of quorum size. Reduces WAL footprint and audit-verification
  cost.
- **Audit verification.** Anyone with the cluster's group
  public key can verify a Raft log range without knowing
  individual node keys.
- **Replay protection.** A signed entry binds (term, index,
  payload digest) algebraically; replay across epochs is
  detectable through the group key rotation surface.

## What this brainstorm is *not* about

Per `no-premature-backward-compat.md`, this brainstorm does
**not** pre-pin:

- Whether `NodeId` is curve-derived. (Closed Outcome B; do not
  reopen here.)
- Membership-state shape, including how individual node signing
  public keys are registered, rotated, and bound to `NodeId`.
- Exact FROST variant (FROST1 / FROST2 / specialized).
- Whether the signature lives on every Raft entry, on snapshot
  boundaries only, or on durability-log frames.
- Replay-window / epoch-rotation surface.
- DKG ceremony, threshold parameters, key-refresh cadence.

These are the design surfaces a future authority / durability
ADR would commit. This brainstorm names the question.

## Why this is its own brainstorm

Three reasons.

1. **It composes cleanly with the lifecycle pass without
   blocking it.** ADRs 046 / 047 / 048 do not need authority
   signatures. Anti-entropy (the marquee future consumer of
   ADR 046's substrate) does not need FROST either — point
   comparison is the verification primitive there.
2. **It is one of several authority-layer choices.** Per-entry
   FROST is one option; per-snapshot signing, log-segment
   batched signing, or Merkle-rooted log segments with a single
   FROST root signature are others. The shape decision needs
   its own analysis pass, not a topology side-effect.
3. **The Wendelon ADR chain has no current Byzantine-peer
   threat surface.** Replicas are treated as cooperating peers
   with bounded clock drift (ADR 012). Signing the WAL is an
   audit / cryptographic-durability move, not a Byzantine-
   tolerance move. Audit value is the lens this brainstorm
   should evaluate against.

## Trigger conditions (when this opens for real)

This brainstorm should not open for active drafting until at
least one of the following is true:

- **A compliance / audit requirement names the WAL.** A SOX /
  HIPAA / SOC-2-like control or a customer audit demand for
  cryptographic log-integrity proof at quorum granularity.
- **A first authority-layer ADR is drafted that needs to sign
  cluster-state mutations.** For example, signed admission
  tokens for cross-tenant operations, or cryptographically-
  proven schema-change provenance.
- **An anti-entropy reconciliation ADR opens** and chooses to
  sign the reconciled-set proof rather than rely on bare
  `StateDigest` comparison. (Likely not — `StateDigest`
  comparison is sufficient for honest-replica reconciliation
  — but if the threat model widens, this becomes load-bearing.)
- **Threat-model brainstorm 019 widens the assumption from
  honest replicas to partially-Byzantine peers.** That moves
  WAL signing from audit-value to correctness-load-bearing.
- **Backup / restore needs a verifiable quorum attestation**
  over snapshot or log material — restoring from cold storage
  with cryptographic proof that the snapshot was committed by
  a quorum, not just written by a single leader.
- **A regulator or deployment profile** requires proof that a
  log range was acknowledged by a quorum, not merely written
  by the current leader.

Until one of these fires, the brainstorm sits as a placeholder.
The 015 closure note's rework budget covers the case where this
brainstorm reopens.

## Open questions for the eventual session

To be answered when this brainstorm opens for real:

1. What is the threat model? (Honest-but-curious replicas? Audit
   demand? Partially Byzantine? Disk tampering? Compromised
   object store?) The threat model picks the answer.
2. **Why is the existing Ed25519 authority insufficient?** The
   current system already has `LeaseAuthority` and
   `TransferAuthority` keyed by numeric `NodeId.id`. Any
   threshold-signature ceremony has to explain why those are
   inadequate or how it composes with them. Do not introduce a
   parallel authority layer without this analysis.
3. Where does the signature live? Per Raft entry? Per log
   segment? Per snapshot? Merkle root over a batch? Audit
   hash-chain checkpoint?
4. Who is the verifier? Online recovery code, an offline audit
   tool, a backup-restore reader, or anti-entropy?
5. How does the group key rotate without a flag day?
6. How does FROST interact with R₀ / Rₙ multi-Raft architecture
   (brainstorm 016)? Per-ring group key, or shared?
7. What is the verification surface? Audit replay only, or live
   on the consensus apply path?
8. What is the cost? FROST signing is non-trivial CPU; can it be
   amortized across a snapshot or batch?

## Candidate outcomes (menu for the eventual session)

When this brainstorm opens for real, the council should choose
among (or recombine) these starting positions rather than
inventing options from scratch.

1. **Close as unnecessary** under the current crash-fault trust
   model. Existing Ed25519 lease / transfer authority covers
   the verifier surface that exists today.
2. **Add single-leader Ed25519 signatures to audit roots only.**
   Smallest-surface authority improvement; no DKG, no threshold.
3. **Add threshold signatures to snapshot or WAL checkpoints,
   not every entry.** Amortizes ceremony cost across batches.
4. **Add threshold signatures to committed log ranges.** Live
   cryptographic durability proof per range; higher cost.
5. **Defer until anti-entropy or backup / restore has a
   concrete verifier** that names exactly what it must verify
   and why bare `StateDigest` comparison is insufficient.
6. **Merkle-rooted or hash-chain log segments with one
   threshold signature per segment.** The log forms a
   Merkle / hash chain over its entries; a single threshold
   signature covers the segment root. Amortizes ceremony
   cost without losing per-entry tamper detection (verifier
   walks the chain to a signed root).

### Preferred starting bias

Defer implementation. If this reopens, two implementation
postures are load-bearing:

1. **Anti-pattern: per-entry FROST on the Raft hot path.** The
   FROST signing ceremony cost is non-trivial; placing it on
   every committed entry would inject coordination latency
   into the consensus apply path. Bias toward
   batched / checkpoint signing (snapshot roots, log-segment
   Merkle roots, audit hash-chain checkpoints).
2. **Out-of-band verification.** Design the verifier for
   offline audit tools, backup / restore readers, or
   anti-entropy reconciliation — not for live replication
   hot-path verification. The verifier surface determines
   where the signature lives, and an offline-first verifier
   bias keeps the live consensus path uncoupled from
   threshold-signature ceremony cost.

Reason: the system already has Ed25519 lease and transfer
authorities keyed by numeric `NodeId.id`; the next authority
ADR should explain why those are insufficient before
introducing a threshold-signature ceremony.

## References

- Brainstorm 015 (`plans/brainstorms/015_algebraic_ring_topology_prime_group.md`)
  — original bundling and topology-gate closure status header.
- Brainstorm 016 (`plans/brainstorms/016_r0_control_ring.md`)
  — multi-Raft architecture this would have to compose with.
- ADR 012 — bounded clock drift; current cluster threat-model
  baseline.
- ADR 025 — audit event model (potential consumer of signed
  audit roots).
- Existing `LeaseAuthority` / `TransferAuthority` Ed25519
  surfaces — the prior authority art any new threshold-
  signature ADR has to compose with or supersede.
- Raft fixed records and snapshot headers (`src/schema/core.rs`)
  — the durable surfaces a signature would have to live on.
- `plans/conventions/no-premature-backward-compat.md` — the
  rule this brainstorm honors by refusing to pre-pin shape.
