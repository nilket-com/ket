# Moved from plans/implementation_checklist.md

(This file was copied into plans/consensus/implementation_checklist.md during a planned refactor.)

# Prioritized Implementation Checklist

Priority	Item	Depends On	Risk	Notes
P0	HybridTimestamp — Ord, merge, Pod/Zeroable impls, NTP drift guard	—	Med	Foundation; every other struct embeds it. Fuzz the Ord impl with proptest.
P0	FrameHeader codec — read_frame / write_frame over AsyncRead/AsyncWrite	—	Med	Must handle partial reads, validate magic, enforce max_frame_size. This is the TCP/QUIC adaptation layer.
P0	ClientWriteId generation (client SDK)	—	Low	UUIDv4 + AtomicU64 counter. Trivial but correctness-critical.
P0	Arrow schema registry — write_payload_schema(), read_key_schema(), etc.	—	Low	Shared between client and server; must be identical on both sides.
P0	Idempotency cache (leader-side)	ClientWriteId	High	DashMap<(uuid,seq), WriteResponseMeta> with TTL eviction. See plans/consensus/idempotency_cache.md.
P1	DataRing::write impl over QUIC	FrameHeader, WriteRequestMeta, Arrow schemas, idem cache	High	Core hot path. Include proxy_hop enforcement (reject if >2).
P1	DataRing::read impl with E_safe check	ReadRequestMeta, LeaseDescriptor	Med	Three consistency levels; lease check on LINEARIZABLE path.
P1	E_safe tracking + monotonic advancement	FinalizationMarker	High	Must never regress on restart; persist to WAL alongside commit_index.
P1	FinalizationMarker emission on quorum ack	Raft commit callback	Med	Exactly-once per committed entry; bytemuck cast to trailing frame bytes.
P1	quinn QUIC endpoint setup	FrameHeader	Med	Separate endpoints for data (:4001) and control (:4002). 0-RTT enabled.
P2	DataRing::stream_finalization	FinalizationMarker, QUIC streams	Med	Backpressure via bounded mpsc; drop slowest after 10s.
P2	TransferToken creation + Ed25519 verification	ed25519-dalek	Med	Atomic with epoch bump; verify canonical encoding of signed region.
P2	LeaseDescriptor grant/renewal	Heartbeat protocol, HLC	Med	Lease clock is HLC-based, not wall-clock. Immediate invalidation on epoch bump.
P2	ControlRing::transfer_leadership	TransferToken, RaftEpochEntry	Med	Drain in-flight writes before step-down.
P2	RaftEpochEntryFixed + Arrow var payload	epoch_entry_var_schema	Med	Hash-chain (prev_epoch_hash) computed at commit time.
P3	DataRing::bulk_write bidi-streaming	DataRing::write	Low	Multi-row Arrow batches; can reuse unary path initially.
P3	TCP+TLS fallback transport	FrameHeader	Low	Length-prefixed framing; stream_id mux. For environments where QUIC is blocked.
P3	ControlRing::get_cluster_status	All state	Low	Observability endpoint; non-critical path.


