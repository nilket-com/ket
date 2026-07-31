# Moved from plans/rpc_traits.md

(This file was copied into plans/network/rpc_traits.md during a planned refactor.)

# RPC Traits — DataRing & ControlRing (async traits)

This document defines the Rust async_trait-based interfaces that replace gRPC service defs. Use these traits as the single source of truth for server implementations.

## DataRing trait

- write(meta: WriteRequestMeta, payload: RecordBatch) -> (WriteResponseMeta, Option<FinalizationMarker>)
- read(meta: ReadRequestMeta, key: RecordBatch) -> (ReadResponseMeta, Option<RecordBatch>)
- stream_finalization(start_log_index: u64, epoch: u64) -> mpsc::Receiver<FinalizationMarker>
- bulk_write(requests: mpsc::Receiver<(WriteRequestMeta, RecordBatch)>) -> mpsc::Receiver<WriteResponseMeta>

Notes:
- RecordBatch schemas: plans/schema/schemas.md (write_payload_schema, read_key_schema, read_value_schema)
- Implementations: LeaderDataRing, FollowerProxy

## ControlRing trait

- transfer_leadership(req: TransferLeadershipRequestMeta) -> (TransferLeadershipResponseMeta, Option<TransferToken>)
- acquire_lease(req: AcquireLeaseRequestMeta) -> (AcquireLeaseResponseMeta, Option<LeaseDescriptor>)
- advance_epoch(config: RecordBatch) -> (RaftEpochEntryFixed, RecordBatch)
- get_cluster_status() -> (ClusterStatusResponseMeta, Option<LeaseDescriptor>, RecordBatch)

Notes:
- ControlRing operations are rate-limited and run on a separate control port with mTLS.
- RecordBatch schemas for epoch configs: plans/schema/schemas.md (epoch_entry_var_schema)

