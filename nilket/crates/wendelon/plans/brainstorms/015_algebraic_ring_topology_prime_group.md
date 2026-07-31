# Brainstorm 015: An Algebraic Ring Topology over a Prime Group (Ristretto255)

## Status: Closed (2026-04-29) — Outcome B

Topology gate closed with the verdict that durable / protocol-
visible `NodeId` does **not** need to be a cryptographic
commitment. Wendelon keeps `NodeId` opaque — a compact handle
already pervasive in `HybridTimestamp`, Raft entries, slab
metadata, peer manifests, object-store keys, TLS SANs, metrics
labels, and routing — and binds node signing identity through
epoch membership state at the point a real verifier exists.

**Terminal topology decision, not a soft deferral.** Wendelon's
routing layer should remain a standard numeric locality / hash-
ring design. Future authority or custody ADRs may bind signing
keys to opaque `NodeId`, but they should not treat curve-derived
topology as the expected destination.

**Also not a "permanent closure" with no re-open path.** Future
evidence can re-open the gate, but the future ADR must pay the
rework budget below and record the roadmap change explicitly.
The closure is durable, not eternal.

This was decided in the AI Swarm Council session of 2026-04-29,
after the lifecycle validation pass (ADRs 046 / 047 / 048)
provided concrete evidence about where the algebraic substrate
actually pays out.

### Why Outcome B

The session compared brainstorms 014 (data identity) and 015
(node-identity / topology) with the benefit of three shipped
ADRs.

- **014 paid out at the data-identity layer.** ADR 046 extracted
  commutative state identity (`StateDigest`) without replacing
  row `Identity`; ADRs 047 and 048 validated visible-state
  add / subtract semantics under tombstones and retention. Each
  operational claim in 014 (O(1) reconciliation, commutative
  accumulation, instant logical-state reflection) maps to an
  ADR clause that is now under test on real storage state.

- **015's strongest operational claims do not require curve-
  derived `NodeId`.** Going section by section:
  - §1 prime-group routing aesthetics — theoretical, not
    operationally observed; standard hash rings work.
  - §2 collision-free VNodes — collision-space delta from `u64`
    to Ristretto is operationally meaningless at any realistic
    VNode count.
  - §3 Sybil-resistant placement — the verifiable-generation
    proof works equally well over an opaque `NodeId` bound to
    a hardware-derived signing key in epoch membership state.
    The proof is over the binding, not over `NodeId`'s bytes.
  - §4 proof of custody + FROST consensus — both operate over
    signing keys. Whether the `NodeId` labeling those keys is
    curve-derived or opaque is irrelevant to FROST's threshold
    math or to a custody signature over `(partition_id, epoch,
    NodeId)`.
  - §5 violent failure resilience — standard distributed-systems
    failover; nothing here requires curve-derived `NodeId`.

- **Sunk-cost surface argues against shape change.** `NodeId`
  is already a compact numeric handle tied into HLC, Raft,
  storage indexes, wire metadata, TLS SANs, and routing.
  Cryptographic authority can bind to that handle through
  membership / config when a real verifier exists. Concrete
  inventory below.

- **The "elegance" lives in the data-identity layer.** Brainstorm
  014's framing ("mathematical supremacy") was right about
  *something* — that something turned out to be the digest
  substrate, not topology. The same elegance does not transfer
  to node identity, where the operational payout is absent.

### Concrete `NodeId` surface inventory

The codebase already treats `NodeId` as compact numeric
infrastructure. A curve-derived shape would touch every site
below; none of them justify the rework absent a verifier:

- `HybridTimestamp` is fixed at 16 bytes and carries
  `node_id: u32` as a tie-breaker.
- `NodeId` is an 8-byte POD shape: `id: u32` plus a reserved
  `u32`.
- OpenRaft is configured with `NodeId = u32`.
- Raft epoch / log fixed records carry `NodeId` in fixed-size
  durable structures.
- Storage indexes encode HLC node ids as 4-byte fields.
- Multi-row storage encodes HLC as 16 bytes with the node id in
  the final 4 bytes.
- TLS node identity uses numeric SANs of the form
  `node-<id>.<cluster_domain>`.
- Lease and transfer signing already trust verifying keys keyed
  by numeric `NodeId.id` (existing Ed25519 `LeaseAuthority` /
  `TransferAuthority` surfaces).
- Ring routing hashes `NodeId.id` plus VNode index; no curve
  shape is required for stable placement.

(Inventory contributed during the council session and adopted
into this closure note.)

### Rework budget cited by future ADRs

Any future ADR that needs cryptographic node identity (custody
proofs, anti-entropy reconciliation, FROST-signed Raft, signed
admission tokens, etc.) must:

- Specify the exact signed statement, the verifier, the replay
  window, and the digest / partition / epoch binding before
  pinning any shape. Candidate signed-statement shape for a
  custody proof, suggested by the council:
  ```text
  (epoch, node_id, partition_id, digest, proof_context)
  ```
  signed by a public key authorized through membership state.
- Treat the current opaque `NodeId` as the durable handle and
  bind to a node signing public key through epoch membership
  state. Do not retroactively reshape `NodeId`.
- Cite this closure note's rationale when re-opening the
  question, including the concrete `NodeId` surface inventory
  above.
- Explain why the existing Ed25519 `LeaseAuthority` /
  `TransferAuthority` surfaces are insufficient for the
  proposed verifier, or how the new authority composes with
  them. Do not introduce a parallel authority layer without
  this analysis.
- Keep authority / signing concerns separate from topology
  (locality / routing) concerns, per the project's Identity-
  vs-Locality philosophy. Authority signing belongs to its own
  ADR; do not cross those layers unless the future ADR
  explicitly re-opens this closure.

Re-open this topology gate only if a future ADR can show that
the opaque-handle model forces more cumulative rework than a
`NodeId`-shape migration would — measured against the concrete
surface inventory above. **Speculative threat models, aesthetic
preferences, and the desire to reuse curve machinery are not
sufficient grounds to re-open the gate.** Do not back-edit this
closure.

### What this closure does not pre-pin

Per `plans/conventions/no-premature-backward-compat.md`, this
closure does **not** reserve byte tags, allocate enum slots,
fix membership-state shape, define custody-proof format, or
specify epoch boundaries. It also does not change HRFT frames,
the Raft WAL format, HLC layout, TLS SAN format, or any node
capability bit. Each of those decisions belongs to the ADR
that first consumes it.

### Council convergence path

The 2026-04-29 council session reached this Outcome B verdict
through one round of divergence and two rounds of iteration:

- **Initial divergence.** Gemini's first draft framed the
  closure as **ADR 049** with **permanent** closure ("no
  rework budget needs to be cited"). Codex and Claude framed
  it as a brainstorm closure with a cited rework budget.
- **Convergence.** After iterating on the council drafts,
  Gemini conceded the framing on three grounds (recorded in
  Gemini's council draft for the 014 / 015 disposition; see
  git history):
  1. **Slot allocation.** The ADR 049 slot is earmarked for
     content-hash identity (roadmap #5). Topology-gate closure
     consumes no source-code shape and earns no ADR. ADRs
     commit architecture; brainstorm closures record which
     design space the council elected not to consume.
  2. **Evidence discipline.** "Permanently closed" overcommits
     beyond the available evidence. The lifecycle ADRs decide
     what *currently* makes operational sense; future evidence
     (Byzantine threat surface, anti-entropy reconciliation
     cost, customer-driven attestation requirement) may
     justify a re-open. The rework budget is the disciplined
     channel for that.
  3. **Re-open path.** A rework-budget closure forces any
     future re-opener to do real work: cite the budget,
     justify reshape costs, and re-open the gate explicitly.
     "Permanently closed" creates a litigated re-open where
     some future agent argues "the previous council was
     wrong" — exactly the pattern this discipline is meant
     to avoid.

The final verdict (Outcome B, brainstorm closure, rework
budget, splits to 018 / 019) is unanimous across Codex,
Claude, and Gemini.

If a future council does re-open the gate with new evidence,
that re-open is recorded in the roadmap and supersedes this
note; the closure note itself is not back-edited.

### Splits

The 015 brainstorm bundled five separable threads. Three remain
deferred under this closure; two split to their own brainstorms.

- **NodeId shape** — closed under Outcome B (this note).
- **VNode generation** — operational; reopens with the
  topology-implementation ADR if Outcome A ever returns.
- **Sybil-resistant admission** — split to brainstorm 019
  (threat-model / admission-control). Not topology shape.
- **Partition-custody proof** — deferred; reopens with the
  first ADR that needs to verify a signature against a
  partition holder (likely anti-entropy or a secondary-index
  reconciliation ADR).
- **FROST-signed Raft / WAL** — split to brainstorm 018
  (authority / durability). Authority layer, not topology.

### References

- ADRs 046 / 047 / 048 (lifecycle validation pass — evidence
  that 014's substrate pays out).
- Brainstorm 014 (`014_algebraic_record_identity_ecmh.md`),
  marked harvested into ADR 046.
- Brainstorm 018 (`018_frost_wal_authority.md`) — FROST-WAL
  split.
- Brainstorm 019 (`019_sybil_resistance_threat_model.md`) —
  Sybil-resistance split.
- `plans/wip/roadmap.md` — forward sequence; topology gate
  marked closed; tip advances to roadmap #5 (content-hash
  identity).
- `plans/conventions/no-premature-backward-compat.md` — the
  rule this closure honors by refusing to pre-pin downstream
  shapes.
- `plans/philosophy.md` — Identity-vs-Locality framing.

## The Premise
Currently, Wendelon's **Unified Hash Ring** (described in ADR 002) uses an 8-byte numeric `u32` (plus a reserved 32-bit slot) as its `NodeId`. It acts purely as a positional identifier for routing and deterministic placement.

This brainstorm explores an evolution of that topology: moving away from a simple integer space and instead casting the ring as an **Algebraic Space over a Prime Group**, specifically using the points of **Ristretto255** (built over Curve25519). 

This is not a cryptography feature; this is a pursuit of **mathematical supremacy** for the topology itself.

A standard $2^{256}$ integer ring is not prime. By Lagrange's Theorem, a group of order $2^{256}$ has many non-trivial subgroups, meaning mathematical operations on the ring can get "trapped" in smaller cycles. Even raw Curve25519 has a cofactor of 8, meaning it is not a pure prime-order group. By contrast, Ristretto255 mathematically abstracts the cofactor away, providing a flawless prime-order group. In a prime group, **every non-identity element is a generator**. Every node inherently has the mathematical power to span the entire topological space, making the ring structurally immune to subgroup confinement and mathematically flawless for distributed algorithms.

*(Note: This Topological Identity is explicitly divorced from the Transport Identity (mTLS). Transport security remains standard and rotatable.)*

## 1. Lagrange's Theorem & The Power of the Generator
If the topology is built on a prime group, the structure of the ring changes from a simple array to a cyclic group where every node is a generator.
*   **The Limitation of $2^{256}$:** A standard hash ring operates in a space that is highly composite. Mathematical routing or hashing algorithms applied iteratively can fall into small subgroups, creating localized routing loops or uneven data clustering.
*   **The Advantage:** Because the Curve25519 subgroup order is prime, Lagrange's Theorem dictates that no smaller subgroups can exist (other than the identity). Any node, applying a uniform scalar multiplication, will deterministically cycle through the *entire* universe of the ring without ever getting stuck. The algebraic foundation guarantees perfect, mathematically uniform dispersion.

## 2. Infinite, Collision-Free Virtual Nodes (VNodes)
Expanding the ring into this algebraic space provides effectively infinite resolution. 
We can allocate granular weights (e.g., 512 VNodes for a heavy server, 64 for a light one) by deterministically generating points on the curve. Because the space is vast and bounded by prime-field algebra, the risk of coordinates colliding (the birthday paradox) is mathematically neutralized, ensuring perfectly even data distribution.

## 3. Sybil-Resistant Placement via Verifiable Generation
In standard distributed hash tables, a malicious actor might attempt an "eclipse attack" by picking IDs adjacent to a target node.
*   **Mechanism:** A node's location becomes a deterministic, cryptographically proven point generated on the curve from its underlying hardware identity and a cluster-wide epoch seed.
*   **The Advantage:** A node cannot arbitrarily "pick" its location. It must mathematically prove to the cluster: *"Based on my root identity, these are the exact algebraic coordinates I am forced to occupy."* The topology becomes un-gameable.

## 4. Proof of Partition Custody & Algebraic Signatures
Because the Node ID is an element of the prime group, the responding node exclusively holds the corresponding scalar.
*   **Proof of Custody:** Clients can challenge a node algebraically to prove it holds the scalar for that location on the ring, strictly guaranteeing that the node serving the data is the rightful topological owner.
*   **Constant-Size Threshold Consensus (FROST):** Curve points can be added together algebraically ($Point_A + Point_B = Point_C$). A Raft quorum can use their Topological points to collaboratively produce a **single, constant-size signature**. This drastically shrinks the Write-Ahead Log (WAL) footprint and accelerates audit verification.

## 5. Resilience to Violent Node Failure
By explicitly rejecting any requirement to transfer the Topological Private Key during a node replacement, the architecture is inherently resilient to violent hardware failures (where the key is permanently lost):

*   **FROST Consensus is Threshold-Based:** If a Raft group of 5 nodes loses a member to a hard crash, the remaining 4 nodes still easily meet the threshold (e.g., 3-of-5) required to produce the single, constant-size valid signature. The dead node's lost key is not a single point of failure for consensus.
*   **Standard VNode Replacement:** When a new replacement node is racked, it possesses a completely new hardware identity and mathematically generates a *new* set of VNode coordinates. The cluster treats this as a standard topology change (just like Cassandra or Dynamo) and replicates data to the new coordinates without ever needing the dead node's key.
*   **Proof of Custody Failover:** If a client challenges a dead node to prove partition ownership, the request times out. The client simply routes to one of the surviving replicas for that partition, which successfully proves custody. The system relies entirely on standard quorum math and data rebalancing to heal, using the prime group purely to make the structure of that healing flawless.

## Rejected Ideas & Trade-offs
*   **Rejected: Zero-Discovery mTLS:** We initially considered using the curve point as the actual transport encryption key. This was rejected because modern zero-trust postures require short-lived, routinely rotated mTLS keys. If Topology = Transport, rotating a key would force the node to change its algebraic location, triggering a massive cluster rebalance.
*   **Rejected: Topological Handoffs for Recovery:** We considered allowing a failing node to pass its scalar to a replacement node to avoid a Raft topology change. This was rejected because it fails the "violent crash" test, where the key is lost and recovery deadlocks.
*   **Trade-off: Routing via Numerical "Distance":** We cannot route based on the *algebraic distance* between two curve points (due to the Discrete Logarithm Problem). We must treat the 32-byte public point as a massive numerical integer and route based on numerical distance, preserving standard DHT routing mechanics while retaining the underlying algebraic superpowers.
