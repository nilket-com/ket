# Error Model & Retry Semantics

This document unifies the error taxonomy across Wendelon. Errors are primarily encoded in the `status` byte of the `WriteResponseMeta` and `ReadResponseMeta` frames.

## 1. Discriminants (`u8`)

| Code | Name | Description | Client Action |
| :--- | :--- | :--- | :--- |
| `0x00` | `OK` | Operation succeeded. | Proceed. |
| `0x01` | `EPOCH_STALE` | The client's `expected_epoch` is older than the server's committed epoch. | The response includes the new `epoch`. The client must refresh its cached `ring(E)` and retry the request at the new topological owners. |
| `0x02` | `NOT_LEADER` | The node does not hold the lease (Data Ring) or is not the Raft Leader (Control Ring). | The response populates `leader_hint`. The client must seamlessly retry the request against the hinted `NodeId`. |
| `0x03` | `DUPLICATE` | The `ClientWriteId` was found in the idempotency cache. | Treat as `OK`. The original `WriteResponseMeta` is echoed back bit-for-bit. |
| `0x04` | `PROXY_LIMIT` | The request was proxied too many times (`proxy_hop > 2`). | Failsafe against infinite routing loops. Client should sleep, refresh the epoch/ring, and retry. |
| `0x05` | `LEASE_EXPIRED` | The primary's lease for this range expired before it could finalize the write. | Client retries. The primary will attempt to renew the lease, or a new primary will be elected. |
| `0x06` | `E_SAFE_BEHIND` | A `LINEARIZABLE` read requested a `min_hlc` that is newer than the node's local `E_safe` watermark. | Client can wait and retry, or direct the read to the leaseholding primary. |
| `0x07` | `QUOTA_EXCEEDED`| The tenant's Namespace has exceeded its IOPS or storage quota. | Client halts. Manual operator intervention required. |
| `0xFF` | `INTERNAL_ERROR`| Unrecoverable local error (e.g., `io_uring` disk failure). | Client logs error and surfaces to user. |

## 2. Wire Encoding
Errors are strictly embedded in the 96-byte `WriteResponseMeta` or 56-byte `ReadResponseMeta`. There are no dynamic-length string error messages on the hot path. If a complex Control Ring RPC requires a human-readable string (e.g., an invalid schema definition), it is delivered in the optional Arrow IPC payload.