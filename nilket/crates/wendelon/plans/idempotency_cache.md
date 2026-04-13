10. Idempotency Cache — Testing Scenarios
Scenario 1: Basic Duplicate Rejection

Code


Copy
Setup:  Leader stable, cache warm.
Action: Client sends WriteRequest(write_id={uuid=A, seq=1}) twice.
Expect: First → status=OK, log_index=N.
        Second → status=DUPLICATE, identical log_index, commit_hlc.
Verify: Only one Raft log entry exists.
        WriteResponseMeta bytes are identical (bytemuck bit-exact;
        memcmp the 96-byte struct directly).
Scenario 2: Cache Survives Leader Restart (WAL-backed)

Code


Copy
Setup:  Leader commits write_id={A, seq=5}, caches response.
Action: Leader process crashes, restarts.
        Client retries write_id={A, seq=5}.
Expect: If cache checkpoint is in WAL → DUPLICATE, bit-exact original.
        If cache is memory-only → OK (re-committed); Raft dedup at
        state-machine level prevents data divergence.
Verify: Read-back of key yields identical value regardless of path.
        No silent data corruption.
Cache impl note: WAL-backed cache writes one 128-byte record
        (ClientWriteId ++ WriteResponseMeta) per committed entry.
        Recovery scans the tail on startup.
Scenario 3: Cache Rebuild After Leadership Transfer

Code


Copy
Setup:  Leader-L1 commits write_id={A, seq=10}.
Action: TransferLeadership → L2. Client retries seq=10 at L2.
Expect: L2 rebuilds cache by scanning recent Raft log entries
        (last N entries or last T seconds, whichever is larger).
        Finds seq=10 → returns DUPLICATE.
        If outside scan window → OK (harmless re-commit).
Verify: Scan window default = max(10_000 entries, 300s).
        Rebuild happens during epoch transition, before L2
        opens the data-ring QUIC endpoint.
Scenario 4: TTL Expiry Under Load

Code


Copy
Setup:  idempotency_ttl_ms=5000. High write throughput.
Action: Client sends write_id={A, seq=1}. Wait 6s. Retry.
Expect: Cache entry expired. Re-applied as new write. Status=OK.
Verify: State machine tolerates replayed writes (last-writer-wins
        or CAS at application layer). No assertion panics.
        Eviction runs on a background tokio task, not inline.
Scenario 5: Split-Brain Guard (Concurrent Same write_id)

Code


Copy
Setup:  Network partition → two nodes think they're leader.
Action: Client sends write_id={A, seq=7} to both.
Expect: Only the true leader (valid lease/quorum) commits.
        Stale leader returns LEASE_EXPIRED or NOT_LEADER.
        After partition heals, exactly one log entry for seq=7.
Verify: Raft's single-leader invariant prevents dual commits.
        Idempotency cache is a performance optimization, not
        the correctness mechanism. Assert single entry with
        `cargo test -- --test-threads=1` for determinism.
Scenario 6: Memory Pressure / LRU Eviction

Code


Copy
Setup:  Cache capacity = 100K entries (100K × 128B = ~12.8 MB).
Action: 200K unique writes arrive.
Expect: LRU eviction activates. Evicted entries return OK on retry.
        No OOM. DashMap shard count tuned to core count.
Verify: Prometheus gauge `idempotency_cache_size` exported.
        Alert at 80% capacity. Eviction counter monotonic.
        `MADV_SEQUENTIAL` hint on cache memory for prefetch.
Scenario 7: Piggy-backed Ack Window

Code


Copy
Setup:  Client sends seq=1, gets OK. Sends seq=2.
Action: On seq=2 commit, check cache for seq=1.
Expect: Entries with seq < (client's latest seq - ack_window)
        eligible for early eviction. Default ack_window = 5.
Verify: No premature eviction if client has sequence gaps.
        Test with seq=1, seq=5 (gap), seq=6: seq=1 is NOT
        evicted because 5-1=4 ≤ ack_window.
        Then seq=7: now 7-1=6 > 5 → seq=1 evicted.
Scenario 8: Recovery After Raft Snapshot Install

Code


Copy
Setup:  Follower falls far behind. Leader sends snapshot.
Action: Follower becomes leader. Client retries a write_id whose
        log entry was truncated by the snapshot.
Expect: Snapshot includes serialized cache checkpoint:
        Vec<(ClientWriteId, WriteResponseMeta)> — bytemuck
        slice cast, ~128 bytes per entry.
        If write_id in checkpoint → DUPLICATE.
        If predates checkpoint → OK (harmless re-commit).
Verify: Snapshot size growth bounded:
        100K entries × 128B = 12.8 MB cache section.
        Checkpoint is appended as a known byte range at the
        end of the snapshot, preceded by a u64 entry count.
Key Design Decisions
bytemuck gives true zero-cost metadata — bytemuck::cast_ref::<[u8; 80], WriteRequestMeta>() is a pointer cast, not a parse. No branches, no allocations, no validation beyond size check. On the write hot path this saves ~50ns per message vs. protobuf, and it compounds at 100K+ writes/sec.

Arrow IPC for variable-length data is the natural fit — keys and values aren't fixed-size, so they can't be bytemuck. Arrow gives us zero-copy access to the buffers via arrow::ipc::reader::StreamReader, built-in LZ4/ZSTD compression toggled per-batch, and a free path to bulk writes (N-row batches) without a second framing protocol.

QUIC replaces gRPC's HTTP/2 multiplexing — each RPC is a stream, no head-of-line blocking. 0-RTT is safe because writes are idempotent via ClientWriteId. quinn is mature, async-native, and gives us connection migration for free (useful when clients follow leader_hint redirects).

The FrameHeader is the protocol's universal envelope — 24 bytes, bytemuck-castable, self-describing via msg_type + meta_len + arrow_len + trailing_len. Any combination of bytemuck metadata, Arrow payload, and trailing struct is expressible. No negotiation, no schema registry, no version hell.

Trailing structs solve the "response with optional attachment" problem — a WriteResponse with finalization is just has_finalization=1 + 88 bytes of FinalizationMarker appended. No nested message, no allocation, no optional-field bitmask. The receiver casts the trailing bytes directly.

