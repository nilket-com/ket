# Capability vs feature naming

In Wendelon code — types, fields, methods, identifiers — use **capability**
for declared and negotiated node abilities. Reserve **feature** for casual
prose: marketing copy, blog posts, informal documentation.

## Rule

- Code: `NodeCapability`, `NodeCapabilitySet`, `advertised_capabilities()`,
  `capability_bits`, `peer_capabilities`, `CapabilityNegotiation`.
- Casual prose: "Wendelon's stats-pruning feature lets the planner skip
  slabs without reading data bytes" — fine in a README intro.
- Don't mix within a single file or section. Pick the register and
  stay there.

## Why

"Capability" is the precise term for a declared, negotiated, gated
ability. "Feature" is a fuzzier marketing word that mixes
"thing-the-product-does" with "ability-the-node-advertises." When code
uses both interchangeably, the resulting type names ("`NodeFeature`"
holding negotiated capability bits) blur the distinction the spec text
("capability negotiation") relies on.

Inconsistent vocabulary compounds: every new field, method, or doc
section that picks the wrong word adds another correction the next
collaborator has to make.

## How to apply

- New types and fields: prefer "capability" if the thing being named
  is a declared/negotiated ability. If it's a user-visible product
  surface ("the partial-aggregate feature"), "feature" is fine in the
  doc but the underlying type still uses "capability" if it's a bit on
  a negotiated set.
- The intra-cluster surface uses `NodeCapability`,
  `NodeCapabilitySet`, `advertised_capabilities()`,
  `PeerCapabilities.capabilities`, and `capability_bits` on the
  serde wire structs (`ClusterPeerWire`, `EpochPeer`,
  `JoinClusterRequestWire`). New code adding capability bits or
  serializing capability state should reuse this vocabulary.
