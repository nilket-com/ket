# Moved from plans/protocol.md

(This file was copied into plans/network/protocol.md during a planned refactor.)

# Protocol & Storage Engine

This document details HLC reconciliation, per-range lease lifecycle, and the Data Ring storage engine behavior.

Key points:
- HLC merge rule and structure: see plans/schema/schemas.md (HybridTimestamp)
- Per-range lease acquisition via ControlRing (AcquireLeaseRequestMeta/Response)
- Storage engine design: io_uring, append-only slab per range, lightweight in-memory index mapping keyhash -> slab offset
- Garbage collection via E_safe watermark and slab compaction

