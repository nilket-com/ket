# Protocol & Versioning

This document outlines the strategy for protocol negotiation, versioning, and ensuring forward/backward compatibility within the Wendelon ecosystem.

## 1. Wire Framing Versioning
The `FrameHeader` includes a strict `version: u8` field.
- **V1:** The initial release.
- **Negotiation:** If a node receives a frame with an unsupported version, it immediately closes the QUIC stream with a `PROTOCOL_VIOLATION` error code.
- **Client Upgrades:** Clients should attempt connections with their highest supported version. If rejected, they may fall back if the SDK supports it.

## 2. Bytemuck Struct Compatibility
Because `#[repr(C)]` structs are zero-copy, they are brittle to change.
- **Append-Only Fields:** Minor versions may safely utilize existing `_pad` bytes.
- **Major Version Bumps:** Changing the size or meaning of existing fields requires a new `MsgType` discriminant or a new `FrameHeader.version`.
- **Dual-Support:** During rolling upgrades, nodes must be compiled to understand both V_current and V_next struct layouts.

## 3. Quorum Upgrades
The Control Ring dictates the active protocol version for the cluster.
- An epoch transition can include a `min_protocol_version` flag.
- Nodes operating below this version will be fenced out of the `ring(E+1)` until they are upgraded.