# Brainstorm 015: An Algebraic Ring Topology over a Prime Group (Ristretto255)

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
