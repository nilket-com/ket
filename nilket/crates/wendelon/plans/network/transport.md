# Moved from plans/transport.md

(This file was copied into plans/network/transport.md during a planned refactor.)

# Transport — QUIC-native framing & TCP fallback

This document specifies the wire framing protocol (HRFT FrameHeader), QUIC stream mapping, TCP+TLS fallback, and wire-size analysis. See plans/schema/schemas.md for the FrameHeader and bytemuck struct layouts.

## Frame layout

FRAME LAYOUT (on wire):
[ FrameHeader (bytemuck 24 bytes) ][ Metadata (bytemuck struct) ][ Arrow IPC payload (optional) ][ Trailing bytemuck struct (optional) ]

FrameHeader fields:
- magic: u32 = 0x48524654 ("HRFT")
- version: u8 = 1
- msg_type: u8
- flags: u16 (bit0=arrow_compressed, bit1=has_trailing)
- stream_id: u32
- meta_len: u32
- arrow_len: u32
- trailing_len: u32

## QUIC mapping
- Data ring: :4001
- Control ring: :4002 (mTLS required)
- Stream-per-RPC: unary requests use one bidi stream per RPC; StreamFinalization uses long-lived stream with server pushes; BulkWrite uses interleaved frames on a single bidi stream.
- 0-RTT enabled; writes are idempotent via ClientWriteId.

## TCP+TLS fallback
- Same frame format over TLS 1.3/TCP
- Length-prefixed frames: [u32 total_frame_len][frame bytes]
- stream_id field used to multiplex logical streams

## Wire Size Analysis
- FrameHeader: 24 B
- HybridTimestamp: 16 B
- ClientWriteId: 32 B
- TransferToken: 128 B
- FinalizationMarker: 88 B
- LeaseDescriptor: 128 B
- RaftEpochEntryFixed: 216 B
- WriteRequestMeta: 80 B
- WriteResponseMeta: 96 B (+ optional 88B FinalizationMarker)

Comparison vs protobuf: bytemuck fixed-size headers trade ~15% size increase for 0 ns decode time and no allocations.

