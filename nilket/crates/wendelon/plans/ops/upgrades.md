# Upgrade Strategy

Wendelon must support zero-downtime rolling upgrades for the entire cluster.

## 1. Rolling Upgrades
- Nodes are restarted one by one with the new binary.
- Because of the `FrameHeader.version` and backward-compatible `#[repr(C)]` structs, old and new nodes can communicate seamlessly.

## 2. Feature Gates
- New architectural features (e.g., a new hashing algorithm, a new WPBC opcode) are hidden behind Raft Feature Flags.
- **Activation:** The flag is only flipped to `true` by an operator once *all* nodes in the cluster have reported to the Control Ring that they are running a binary version that supports the feature.

## 3. Storage Migrations
- If a catastrophic change is required to the local NVMe slab layout, Wendelon uses the "Lazy Migration" protocol.
- The upgraded node simply treats its local disk as a "legacy" node, running the Background Sweeper to stream the old slab format into the new slab format locally, sealing and uploading the new format transparently.