# Moved from plans/write_flow.md

(This file was copied into plans/network/write_flow.md during a planned refactor.)

# Write Flow — Proxying, Finalization & E_safe

This document captures sequence diagrams for write proxying, leader handling, idempotency cache lookup, quorum ack, and FinalizationMarker emission. See plans/consensus/raft-core.md for WAL and snapshot details.

Highlights:
- Proxying semantics: followers forward with proxy_hop incremented; proxy_hop max=2
- HLC merge: leader_hlc = max(local_hlc, client_hlc) + 1
- Idempotency: leader maintains DashMap cache keyed by ClientWriteId
- Finalization: emitted by leader after quorum; appended as small Raft entry (FinalizationMarker) to drive E_safe

