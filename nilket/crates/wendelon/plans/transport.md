4. Wire Framing Protocol (QUIC-native, TCP fallback)
Code


Copy
┌─────────────────────────────────────────────────────────────────┐
│                     FRAME LAYOUT (on wire)                      │
├──────────────┬──────────────┬───────────────┬───────────────────┤
│  FrameHeader │  Metadata    │  Arrow IPC    │  Trailing         │
│  (24 bytes)  │  (bytemuck)  │  payload      │  bytemuck struct  │
│  bytemuck    │  fixed-size  │  (optional)   │  (optional)       │
├──────────────┴──────────────┴───────────────┴───────────────────┤
│                                                                 │
│  FrameHeader:                                                   │
│    magic:          u32  = 0x48524654 ("HRFT")                   │
│    version:        u8   = 1                                     │
│    msg_type:       u8   (see MsgType enum)                      │
│    flags:          u16  (compression, trailing-struct, etc.)    │
│    stream_id:      u32  (for multiplexed QUIC streams)          │
│    meta_len:       u32  (length of Metadata section)            │
│    arrow_len:      u32  (length of Arrow IPC section; 0=none)   │
│    trailing_len:   u32  (length of trailing struct; 0=none)     │
│                                                                 │
│  Flags (bitfield):                                              │
│    bit 0: arrow_compressed  (1 = Arrow body uses LZ4 frame)    │
│    bit 1: has_trailing      (1 = trailing bytemuck struct)      │
│    bits 2-15: reserved                                          │
└─────────────────────────────────────────────────────────────────┘
rust


Copy
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct FrameHeader {
    /// Magic bytes: 0x48524654 = "HRFT"
    pub magic: u32,
    /// Protocol version; currently 1
    pub version: u8,
    /// Message type discriminant (see MsgType)
    pub msg_type: u8,
    /// Bitfield: bit0=arrow_compressed, bit1=has_trailing
    pub flags: u16,
    /// QUIC stream ID or TCP-mux logical stream
    pub stream_id: u32,
    /// Byte length of the metadata section (bytemuck struct)
    pub meta_len: u32,
    /// Byte length of the Arrow IPC section (0 = no Arrow payload)
    pub arrow_len: u32,
    /// Byte length of trailing bytemuck struct (0 = none)
    pub trailing_len: u32,
}
// static_assert: size_of::<FrameHeader>() == 24

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct MsgType(pub u8);

impl MsgType {
    // data ring
    pub const WRITE_REQ:       Self = Self(0x01);
    pub const WRITE_RESP:      Self = Self(0x02);
    pub const READ_REQ:        Self = Self(0x03);
    pub const READ_RESP:       Self = Self(0x04);
    pub const BULK_WRITE_REQ:  Self = Self(0x05);  // Arrow has multiple rows
    pub const BULK_WRITE_RESP: Self = Self(0x06);
    pub const STREAM_FIN_REQ:  Self = Self(0x07);  // StreamFinalization subscribe
    pub const STREAM_FIN_EVT:  Self = Self(0x08);  // FinalizationMarker push

    // control ring
    pub const TRANSFER_REQ:    Self = Self(0x10);
    pub const TRANSFER_RESP:   Self = Self(0x11);
    pub const LEASE_REQ:       Self = Self(0x12);
    pub const LEASE_RESP:      Self = Self(0x13);
    pub const EPOCH_ADV_REQ:   Self = Self(0x14);
    pub const EPOCH_ADV_RESP:  Self = Self(0x15);
    pub const STATUS_REQ:      Self = Self(0x16);
    pub const STATUS_RESP:     Self = Self(0x17);
}
Frame examples on the wire:

RPC	meta section	Arrow section	trailing section
Write request	WriteRequestMeta (80B)	1-row batch: key+value	none
Write response (with finalization)	WriteResponseMeta (96B)	none	FinalizationMarker (88B)
Write response (no finalization)	WriteResponseMeta (96B)	none	none
Read request	ReadRequestMeta (40B)	1-row batch: key	none
Read response	ReadResponseMeta (56B)	1-row batch: value	none
BulkWrite request	WriteRequestMeta (80B)	N-row batch: keys+values	none
StreamFinalization event	none (meta_len=0)	none	FinalizationMarker (88B)
TransferLeadership response	TransferLeadershipResponseMeta	none	TransferToken (128B)


6. Transport Layer — QUIC via quinn
Code


Copy
┌──────────────────────────────────────────────────────────────────┐
│                       TRANSPORT ARCHITECTURE                     │
│                                                                  │
│   Client                           Server                        │
│   ┌──────────┐                     ┌──────────────────────┐      │
│   │ quinn    │ ◄── QUIC 0-RTT ──► │ quinn endpoint       │      │
│   │ endpoint │                     │ :4001 (data ring)   │      │
│   └──────────┘                     │ :4002 (control ring)│      │
│                                    └──────────────────────┘      │
│                                                                  │
│   QUIC Stream Mapping:                                           │
│   ─────────────────────────────────────────                      │
│   • Unary RPC (Write, Read):                                     │
│       Client opens a bidi stream, sends 1 frame, receives 1      │
│       frame, stream closes. Stream-per-request, no head-of-line. │
│                                                                  │
│   • StreamFinalization:                                           │
│       Client opens a bidi stream, sends 1 subscribe frame.       │
│       Server pushes STREAM_FIN_EVT frames indefinitely on the    │
│       same stream. Client closes to unsubscribe.                 │
│                                                                  │
│   • BulkWrite:                                                   │
│       Client opens a bidi stream. Sends N frames (BULK_WRITE_REQ)│
│       Server sends N frames (BULK_WRITE_RESP) interleaved.       │
│       Client half-closes when done; server half-closes after      │
│       last response.                                             │
│                                                                  │
│   TCP+TLS Fallback:                                              │
│       Same frame format over a TLS 1.3 TCP connection.           │
│       Multiplexing via stream_id field in FrameHeader.           │
│       Length-prefix each frame: [u32 total_frame_len][frame...]   │
│                                                                  │
│   Connection Parameters:                                         │
│       max_idle_timeout:     30s                                   │
│       keep_alive_interval:  10s                                   │
│       max_frame_size:       5 MiB (data ring)                   │
│       max_frame_size:       64 KiB (control ring)               │
│       max_concurrent_bidi:  256 (data), 16 (control)             │
│       0-RTT:                enabled (replay-safe; writes are      │
│                             idempotent via ClientWriteId)         │
└──────────────────────────────────────────────────────────────────┘
7. Wire Size Analysis
Message	Exact Size	Notes
FrameHeader	24 B	Every frame; constant overhead
HybridTimestamp	16 B	No varints — fixed, cache-line friendly
NodeId	8 B	Address resolved out-of-band
ClientWriteId	32 B	UUID + seq + ttl, padded
TransferToken	128 B	56B signed payload + 64B signature + 8B pad
FinalizationMarker	88 B	Could pack to 80B; chose clarity
LeaseDescriptor	128 B	Two HLCs + signature
RaftEpochEntryFixed	216 B	Plus Arrow payload for voter/learner lists
WriteRequestMeta	80 B	Plus Arrow payload for key+value
WriteResponseMeta	96 B	Optionally + 88B trailing FinalizationMarker
ReadRequestMeta	40 B	Plus Arrow payload for key
ReadResponseMeta	56 B	Plus Arrow payload for value


Comparison vs. protobuf wire encoding:

Metric	protobuf (varint)	bytemuck (fixed)	Winner
Encode/decode CPU	~50 ns (small msg)	0 ns (cast)	bytemuck
Allocations per parse	1-3 (arena helps)	0	bytemuck
HybridTimestamp wire size	12-16 B (varies)	16 B (fixed)	protobuf by ~2B
WriteRequestMeta wire size	~60-70 B	80 B	protobuf by ~15B
Predictability / branch-free parsing	Poor	Perfect	bytemuck
Alignment-safe SIMD comparison	No	Yes	bytemuck


The ~15% size increase per header is trivially dwarfed by the Arrow payload (key+value can be up to 4 MiB). Zero-copy casting eliminates serialization entirely on the hot path.

