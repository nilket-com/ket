# Moved from plans/idempotency_cache.md

(This file was copied into plans/consensus/idempotency_cache.md during a planned refactor.)

# Idempotency Cache — Leader-side Exactly-Once Cache

This document describes the leader-side idempotency cache design, testing scenarios, and WAL-backed checkpointing strategies. See plans/schema/schemas.md for the ClientWriteId and WriteResponseMeta bytemuck layouts.

## Design

- Key: (client_uuid: [u8;16], sequence_no: u64)
- Value: WriteResponseMeta (bytemuck 96-byte struct)
- In-memory structure: DashMap<( [u8;16], u64 ), CachedEntry>
- CachedEntry: includes WriteResponseMeta, insertion_hlc, persisted_wal_offset (when checkpointed)
- Eviction: TTL-based + LRU under memory pressure + piggy-backed ack window
- Persistence: WAL-backed checkpoint writes a contiguous bytemuck slice of (ClientWriteId, WriteResponseMeta) with a leading u64 count. Snapshot includes this checkpoint.

## Testing scenarios

(Scenarios copied from original spec: duplicate rejection, cache survive restart, leadership transfer rebuild, TTL expiry under load, split-brain guard, LRU eviction, piggy-backed ack window, snapshot install recovery.)

## Implementation notes

- Use a background tokio task for periodic checkpointing and TTL eviction.
- Serialize checkpoint entries as bytemuck slices to avoid per-entry serde costs.
- On leader startup, rebuild cache by scanning latest WAL tail and the most recent snapshot checkpoint. Default scan window = max(10_000 entries, 300s).

## Metrics

- idempotency_cache_size (gauge)
- idempotency_evictions_total (counter)
- idempotency_checkpoints_written_total (counter)
- idempotency_checkpoint_last_offset (gauge)

