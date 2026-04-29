# Brainstorm 019: Sybil Resistance / Admission-Control Threat Model

## Status

Open as a placeholder split out of brainstorm 015. **Not on
the active queue.** No ADR drafting from this brainstorm until
the trigger conditions below fire.

## Premise

Brainstorm 015 (algebraic ring topology) included §3
"Sybil-Resistant Placement via Verifiable Generation" as a
motivation for cryptographic `NodeId`. The 2026-04-29 council
closed 015 with Outcome B (opaque `NodeId` + membership-bound
signing key) on the grounds that Sybil resistance is *admission
policy*, not topology shape — the verifiable-generation proof
works equally well over an opaque `NodeId` bound to hardware-
derived signing keys in epoch membership state.

This brainstorm exists to hold the *real* Sybil-resistance
question for when it becomes load-bearing: under what threat
model does Wendelon need to resist a malicious operator
spinning up many node identities, and what admission-control
mechanism enforces it?

## What this brainstorm is about

The admission-control surface that decides whether a candidate
node may join the cluster (or a specific R₀ / Rₙ ring), under a
threat model that treats node-identity proliferation as a real
attack vector.

### Threat-model axes

Before any mechanism choice, the eventual session must locate
Wendelon along these axes:

**Membership environment.**
- Fully operator-controlled private cluster.
- Hosted multi-tenant cluster with internal node admission.
- Federated or semi-open cluster where external parties may
  contribute nodes.
- Test / dev clusters with no Sybil concern.

**Attacker control.**
- Can request arbitrary numeric `NodeId`.
- Can request many nodes.
- Can choose advertised capacity.
- Can choose network location.
- Can acquire valid certificates.
- Can compromise one admitted node but not the admission
  authority.

**Target.**
- Own a key range.
- Eclipse a tenant or table.
- Bias VNode placement.
- Capture write quorum.
- Capture read quorum.
- Bias anti-entropy reconciliation.

**Authority.**
- Operator config.
- Cluster CA.
- Hardware-rooted identity.
- Cloud instance identity.
- Admission quorum.
- External attestation service.

### Candidate enforcement mechanisms (not yet committed)

- **Operator-assigned opaque `NodeId` only.** The substrate-
  level minimum: nodes never self-select identity.
- **Cluster-operator allowlist.** Explicit operator-managed list
  of authorized node signing keys. Sybil resistance is "the
  operator does not list forgeries."
- **Admission service assigns `NodeId`** rather than the node;
  binds capacity claims to operator approval or telemetry-
  backed limits.
- **Verifiable VNode generation from epoch seed.** Ring
  assignment deterministically derived from
  `(opaque_node_id, declared_capacity, epoch_seed)`. Candidate
  cannot pick adjacent-to-target placement. (This is the 015
  §3 proposal, recast as admission-time enforcement rather
  than `NodeId` shape.)
- **Hardware / cloud attestation gates admission.** TPM / SEV /
  enclave / cloud-instance identity as the Sybil bound.
- **Certificate issuance binds role, node id, and cluster.**
  Composes with mTLS surface already in place.
- **Placement weights capped until trust earned over time.**
  Reduces value of a successful Sybil at admission.
- **Hybrid.** Operator allowlist for the substrate plus
  attested hardware identity plus capped placement weights for
  cryptographic / operational checks.

## What this brainstorm is *not* about

- **`NodeId` shape.** Closed under Outcome B. Do not reopen
  here.
- **Transport authentication.** mTLS / cluster CA already
  handles this; transport identity is rotatable and short-
  lived per 015 §"Rejected Ideas."
- **Per-tenant / per-user authentication.** That is a different
  layer (tenant catalog, capability tokens) and has its own
  surface.
- **FROST-signed Raft / WAL.** Authority / durability concern;
  see brainstorm 018.
- **Anti-entropy custody proof.** Distinct surface; reopens
  with the first ADR that needs to verify a signature against
  a partition holder.

## Why this is its own brainstorm

Sybil resistance is a *threat-model-driven* decision. The
Wendelon ADR chain currently treats replicas as cooperating
peers with bounded clock drift (ADR 012). Without a documented
threat model that justifies treating node-identity proliferation
as an attack, any specific Sybil-resistance mechanism is
premature shape.

This brainstorm therefore opens **only after** a documented
threat model exists that names the attacker, the asset, and the
attack vector. Without that document, any policy choice here is
guessing.

## Trigger conditions (when this opens for real)

- **A documented threat model.** Either a `plans/governance/`
  document or a dedicated threat-model brainstorm naming the
  attacker, asset, and attack surface for node-identity
  proliferation.
- **A multi-tenant operator scenario** where cluster operators
  cannot fully trust each other (e.g., shared infrastructure,
  cross-organization deployments).
- **An external compliance demand** for hardware-attested node
  identity (FIPS / FedRAMP / regulated-environment customer).
- **A first ADR that needs admission control to make a
  correctness argument.** For example, a custody-proof ADR
  that needs to ensure a malicious node cannot inject itself
  as a proxy custodian.
- **Capacity or placement fraud becomes an observed
  operational concern.** A single-operator deployment can
  still hit this if a misconfigured or compromised host
  inflates its capacity claim or chooses adjacent VNodes
  to a high-value partition.
- **R₀ / Rₙ membership split introduces different trust
  boundaries** for control-ring voters and data-ring workers.
  Control-ring voters vote on cluster state and may warrant
  stricter admission than data-ring workers; the asymmetry
  becomes load-bearing once the rings carry meaningfully
  different authority.

Until one of these fires, the brainstorm sits as a placeholder.

## What this brainstorm should produce when it opens

1. A **threat model** in prose: who attacks, what they want,
   what surfaces they have access to, and what Wendelon
   currently does and does not promise.
2. A **policy choice** among the candidate mechanisms above
   (or a new one), driven by the threat model — not by
   aesthetic preference.
3. A **non-goals list** explicitly excluding Byzantine
   tolerance, transport rotation, tenant-level authentication,
   etc., unless those are part of the chosen mechanism.
4. A **trigger for promotion to ADR**: when the chosen
   mechanism has a concrete enforcement point in the code
   (admission server, R₀ control-ring mutation, etc.).

## Open questions for the eventual session

- Does Wendelon's positioning (data warehouse, typically
  single-operator per cluster) actually need Sybil resistance
  beyond an operator allowlist?
- Is the relevant attack identity selection, capacity
  inflation, or certificate issuance? The answer drives the
  mechanism.
- Is hardware-attested identity worth the operational cost
  given the deployment model?
- Should VNode placement use a cluster epoch seed even with
  opaque `NodeId`? (This is recoverable Sybil resistance even
  when no other mechanism is committed.)
- How does admission control compose with the R₀ control-ring
  brainstorm (016)? Is admission an R₀ mutation?
- **Does the answer differ for control-ring voters and
  data-ring workers?** R₀ membership and Rₙ membership may
  warrant different policies — control-ring voters likely
  need stricter admission since they vote on cluster state.
- What does node *removal* look like — is admission revocable,
  and if so, how does that interact with replicated state?

## Candidate outcomes (menu for the eventual session)

When this brainstorm opens for real, the council should choose
among (or recombine) these starting positions.

1. **Close as not relevant** to the launch trust model.
2. **Document operator-assigned `NodeId` plus certificate
   issuance** as sufficient — formalize the existing posture.
3. **Add an admission-control ADR** for node join authorization
   (likely as an R₀ mutation).
4. **Add verifiable VNode generation** from opaque `NodeId`,
   capacity, and epoch seed.
5. **Add hardware / cloud attestation** as a deployment-profile
   option (off by default, on for regulated environments).

### Preferred starting bias

Do not move this ahead of the current roadmap. The launch model
appears operator-controlled, and existing certificate issuance
already binds numeric node identity. Reopen when Wendelon has
an open-membership or externally-contributed-node threat
model.

If defense must elevate before such a model exists, bias toward
**verifiable VNode generation from a cluster epoch seed** as
the lowest-cost first step. Rationale: it requires no new
hardware dependencies (no TPM / SEV / cloud-instance-identity
integration), composes with the existing operator-assigned
opaque `NodeId`, and removes the most directly attackable
surface (a malicious operator picking VNodes adjacent to a
target). Hardware-attestation pathways stay reserved for
deployment profiles where regulation or contract mandates
them.

## References

- Brainstorm 015 (`plans/brainstorms/015_algebraic_ring_topology_prime_group.md`)
  — original bundling and topology-gate closure status header.
- Brainstorm 016 (`plans/brainstorms/016_r0_control_ring.md`)
  — admission likely composes with R₀.
- ADR 012 — current bounded-trust baseline.
- `plans/governance/` — destination for the threat-model
  document this brainstorm requires before opening for real.
