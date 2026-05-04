# ADR 053: Wire-Side Arrow IPC Body Compression

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/053_wire_arrow_ipc_body_compression.md`

## Status

Accepted

## Context

Wendelon's custom transport (ADR 004, ADR 011) operates over
QUIC with a strict zero-copy posture. Each HRFT frame carries
a fixed-size `FrameHeader` followed by three sections:

```text
[FrameHeader] [meta bytes] [Arrow IPC bytes?] [trailing bytes?]
```

`FrameHeader` and hot metadata structs are
`#[repr(C)] + bytemuck::Pod`; non-Pod metadata uses postcard
(ADR 010). The Arrow section is an Arrow IPC stream. ADR 043
added storage-resident Arrow IPC body compression for slab
records, using Arrow IPC's native `BodyCompression` field with
`LZ4_FRAME`, but explicitly left network writers on default
options:

> "Wire-side IPC encoders … also remain on default options;
> ADR 043 is storage-resident only." (ADR 043 §5)

Roadmap #9 closes that gap. The roadmap's stated rationale is
forward-compatibility with CDC (#10): "Lands before CDC so
streaming surfaces inherit transport-side compression instead
of inventing a stream-local contract." Settling the wire-side
codec now buys CDC a smaller surface to design over.

Three facts shape the decision:

- **Storage pass-through is already compressed.** A
  `QUERY_RESP` or `READ_RESP` returning Arrow IPC bytes from
  a `Lz4Frame`-policy slab carries per-buffer-LZ4 body
  buffers on the wire by construction. The framing layer
  treats those bytes as opaque; the receiver's
  `StreamReader` decompresses them transparently because the
  `ipc_compression` Cargo feature is on in all production
  builds (ADR 043 §8). Any wire-side scheme that *also*
  compresses the section as a whole would either
  double-compress these bytes for no density gain at extra
  CPU cost, or require per-frame detection-and-skip logic.
  Per-buffer compression *inside* the IPC FlatBuffer composes
  trivially: storage pass-through bytes are already in the
  target shape.
- **Freshly-encoded wire payloads are not compressed.**
  Several wire-side Arrow IPC writer sites synthesise
  payloads from in-memory state rather than passing storage
  bytes through. Direct `StreamWriter::try_new(...)` call
  sites:
  `encode_batch` in `src/client/core.rs:1685` (client write
  request encoding — `TABLE_DELETE_REQ` row-key-id frames
  and similar) and `encode_migration_records` in
  `src/network/data_server.rs:10060` (peer-only migration
  RPCs). Indirect call sites that go through the storage
  helper layer with a hardcoded uncompressed policy:
  `src/network/data_server.rs:3233` (TABLE_QUERY_RESP fresh
  response synthesis) and `src/network/data_server.rs:4387`
  (READ_RESP fresh response synthesis), both routed through
  `src/storage/multirow.rs::encode_batch` which calls
  `encode_record_batch_bytes(batch, RecordBodyCompressionPolicy::Uncompressed)`.
  Server-synthesised query responses constructed
  batch-by-batch from in-memory state and future CDC events
  share the same shape; ADR 053's audit must catch both
  direct and indirect call patterns.
- **Arrow IPC's per-buffer compression already handles the
  small-payload edge case.** Arrow's per-buffer compressor
  falls back to uncompressed bytes with a `-1` length-prefix
  sentinel when LZ4 would expand the buffer (high-entropy or
  sub-frame-overhead inputs). The receiver decodes both
  shapes uniformly. A framing-layer envelope around the
  entire Arrow section does not get this property for free
  and would have to re-implement it via an external
  threshold.

The constraints from ADR 004 (zero-copy POD metadata, Arrow
IPC payloads), ADR 010 (tiered serialization), ADR 011
(QUIC-first; no auto-adaptive transport claim), ADR 043 (the
storage-side codec choice ADR 053 inherits), and
`plans/conventions/no-premature-backward-compat.md` all hold.

A small loose end exists on the wire envelope.
`FrameHeader.flags` documents bit 0 as `arrow_compressed` and
bit 1 as `has_trailing` (`src/schema/core.rs:504`). Neither
bit is read or written anywhere. Bit 1 is redundant with the
existing `trailing_len > 0` signal; bit 0 was reserved for
exactly the wire-side compression capability this ADR ships,
but a flag is the wrong shape because Arrow IPC already
self-describes. Per `no-premature-backward-compat.md` the
right move is to delete both reservations rather than wire
either bit, and to land a `FRAME_FLAG_KNOWN_MASK` validation
so unknown bits reject loudly instead of being silently
ignored.

## Identity-vs-Locality framing at the wire axis

Per `plans/philosophy.md`, every record has an Identity (what
it IS) and a Locality (where it LIVES). Wire-frame compression
touches neither: it changes the byte representation of an
Arrow IPC payload between processes, not the row identity
(`__row_key_id__`), not the routing locality, and not the
descriptor binding fingerprints. The bytes decode back into
the same Arrow `RecordBatch` on the receiver. Identity is
fingerprinted at the storage and catalog layers; locality is
computed from identity at admission. Compression is a
representation-density choice on the wire, orthogonal to
both.

The Arrow IPC FlatBuffer is the single source of truth for
what bytes carry — the same property ADR 043 §4 made
load-bearing on the storage axis. ADR 053 inherits the
property: a frame whose `arrow_payload` section was encoded
with `BodyCompression { codec = LZ4_FRAME, method = BUFFER }`
self-describes; receivers dispatch by reading the IPC
metadata, not by reading an HRFT envelope flag. A second
consequence is that **forwarding paths are
flag-stripping-free**: nodes that proxy or store-and-forward
Arrow payloads (multi-row read fan-out, future CDC catch-up
replay, any store-and-forward shape) ship bytes verbatim. The
HRFT layer never modifies a compression flag because there is
none.

## Non-goals (V1)

- **No new HRFT frame family or message type.** Existing
  message types (`READ_RESP`, `QUERY_RESP`, `TABLE_WRITE_REQ`,
  `TABLE_DELETE_REQ`, `MIGRATE_REQ` / `RESP`,
  `BULK_WRITE_REQ`, etc.) carry compressed body buffers
  inside their existing `arrow_payload` section.
- **No HRFT envelope `format_version` bump.**
  `HRFT_ENVELOPE_VERSION` stays at 1.
- **No new `FrameHeader.flags` bit for compression.**
  Compression is signalled inside the Arrow IPC FlatBuffer's
  `BodyCompression` field. ADR 053 *deletes* the pre-existing
  bit-0 (`arrow_compressed`) and bit-1 (`has_trailing`)
  reservations on `FrameHeader.flags` (neither was ever read
  or written) and lands a `FRAME_FLAG_KNOWN_MASK` validation
  so unknown bits reject loudly.
- **No new node capability bit.** Mixed-cluster handling is
  automatic via the IPC FlatBuffer's self-description and the
  `ipc_compression` Cargo feature being on in all production
  build profiles (ADR 043 §8). ADR 053 elevates the feature
  pin to a mechanical CI guard.
- **No transport-tier compression.** Application-layer Arrow
  IPC compression is the right placement; QUIC stream-frame
  compression and TLS-record compression are out of scope
  (TLS-record compression is dead by RFC 8446).
- **No compression of HRFT meta or trailing sections.** Two
  reasons compose. *Density*: meta and trailing are small
  (postcard-encoded RPC metadata, fixed-size POD structs,
  finalization markers, transfer tokens), typically O(100)
  bytes; LZ4 framing overhead dominates. *Compression-oracle
  surface*: meta and trailing carry operator and authorization
  material; the Arrow section is tenant-controlled. Mixing the
  two into a single compression context would create a CRIME /
  BREACH-class side-channel where chosen Arrow input could
  leak length-signal about secret control fields.
  Restricting compression to the Arrow section keeps the
  control-bytes context disjoint and side-channel-free.
- **No compression of the descriptor schema IPC**
  (`encode_arrow_schema_ipc` in `src/schema/table.rs`). ADR
  043 §5 excluded this on the storage axis for boot-time
  independence; ADR 053 keeps the exclusion on the wire.
- **No zstd, snappy, or any second codec in V1.** LZ4_FRAME
  only, matching ADR 043. No reserved enum slots; a future
  codec lands its own ADR when measured workload pulls.
- **No `CompressionLevel` newtype.** LZ4_FRAME exposes no
  useful tuning knob. Add the newtype if and when a zstd
  ADR opens.
- **No per-frame, per-tenant, per-table, or per-RPC
  compression policy.** The wire-side codec is a constant.
- **No cluster-level kill-switch in V1.** The ADR pins the
  codec; if a build hits a regression, an amendment opens.
- **No new `WireBodyCompressionPolicy` enum** parallel to
  `RecordBodyCompressionPolicy`. The codec enum names a
  codec set, not a placement axis; the same enum is used at
  two call sites with different policy resolution.
- **No new Cargo feature or dependency.** `ipc_compression`
  is already in the Arrow feature list (ADR 043 §8); ADR 053
  pins it on by architectural guard but adds nothing.
- **No `ObjectStore` trait change.** L2 traffic is bounded by
  ADR 020 envelope encryption.
- **No PBC / AGG protocol change.** Both consume decoded
  `RecordBatch` values; whether the IPC bytes were compressed
  is invisible to PBC opcodes and AGG accumulators.
- **No reserved enum slots, compatibility shims, or `V1` /
  `V2` Rust type suffixes** for any new symbol.
- **No project-name prefix on new symbols.**
- **No CRIME-class compression-side-channel mitigation in
  V1.** The meta / trailing exclusion above keeps tenant-
  controlled Arrow bytes and operator-controlled control
  bytes in disjoint compression contexts. mTLS-protected
  QUIC further bounds the threat model. The active mitigation
  budget for length-padding / randomised dispersal is zero;
  if a future client surface changes the threat model, an
  amendment opens.

## Decision

### 1. Use Arrow IPC `BodyCompression` for fresh wire encoders

Every wire-side Arrow IPC encoder that serialises an
in-memory `RecordBatch` for transport routes through one of
two helpers in `src/network/ipc.rs`:

```rust
pub const WIRE_BODY_COMPRESSION: RecordBodyCompressionPolicy =
    RecordBodyCompressionPolicy::Lz4Frame;

/// Options-level helper. For callers that drive the
/// `StreamWriter` themselves (e.g., custom error-conversion
/// style, multi-batch streams, future CDC).
pub fn wire_ipc_write_options() -> std::io::Result<IpcWriteOptions> {
    crate::storage::ipc::ipc_write_options_for_policy(WIRE_BODY_COMPRESSION)
}

/// Function-level helper. For callers that just need
/// "encode this batch for the wire" — drops in directly
/// where today's code calls
/// `multirow::encode_batch(batch)` or assembles a
/// one-shot encoder with default options.
pub fn wire_encode_batch(batch: &RecordBatch) -> std::io::Result<Vec<u8>> {
    crate::storage::ipc::encode_record_batch_bytes(batch, WIRE_BODY_COMPRESSION)
}
```

Both helpers reuse the `RecordBodyCompressionPolicy` enum
and the ADR 043 builder / encoder; ADR 053 introduces no
parallel "wire" enum and no parallel encoding logic. Arrow
IPC writes per-buffer `BodyCompression` metadata into the
IPC FlatBuffer; that metadata is the single source of truth,
and HRFT does not duplicate it in a header flag.

Direct `StreamWriter::try_new(...)` call sites flip to
`StreamWriter::try_new_with_options(..., wire_ipc_write_options())`.
Indirect call sites that go through a one-shot
`encode_batch`-style wrapper (today: `multirow::encode_batch`)
flip to `wire_encode_batch` directly.

### 2. Compress only freshly encoded wire IPC

V1 fresh-wire writer sites that flip:

- **`src/client/core.rs::encode_batch`** — client encoding
  for `TABLE_DELETE_REQ` row-key-id frames and any future
  client-emitted Arrow payloads. Direct
  `StreamWriter::try_new(...)` site; flips to
  `StreamWriter::try_new_with_options(..., wire_ipc_write_options())`.
- **`src/network/data_server.rs::encode_migration_records`**
  — peer-only migration RPCs. Direct
  `StreamWriter::try_new(...)` site; flips to
  `StreamWriter::try_new_with_options(..., wire_ipc_write_options())`.
- **`src/network/data_server.rs:3233`** — TABLE_QUERY_RESP
  fresh response synthesis. Indirect site; today calls
  `multirow::encode_batch(&concatenated)` which hardcodes
  `RecordBodyCompressionPolicy::Uncompressed`. Flips to
  `wire_encode_batch(&concatenated)`.
- **`src/network/data_server.rs:4387`** — READ_RESP fresh
  response synthesis. Indirect site; today calls
  `multirow::encode_batch(&batch)` for the optional per-key
  value batch. Flips to `wire_encode_batch(&batch)`.

After the flip, `src/storage/multirow.rs::encode_batch` has
no production wire-path callers — its remaining callers are
all in `tests/` modules (classification: `test-or-bench`).
The architectural test pins this so a future production
caller cannot regress through the uncompressed shim. A
non-wire production caller that emerges later picks an
explicit `encode_batch_with_policy` site or routes through
`wire_encode_batch` deliberately.

Storage pass-through paths bypass both helpers. A path that
already has Arrow IPC *bytes* from a slab record forwards
them verbatim — pre-compressed (under ADR 043) or
uncompressed, self-described in either case. Re-encoding
pass-through bytes on the transport path would burn CPU for
no density gain and create a second policy owner. Note that
the four sites above all hold *decoded `RecordBatch`*
values, not bytes — the query planner / read path has
already decoded, applied PBC filtering, and is producing a
new IPC stream from in-memory state. That is a fresh-encode,
not a pass-through.

The descriptor schema IPC encoder
(`src/schema/table.rs::encode_arrow_schema_ipc`) is on the
**explicit exclusion list** and stays on default options for
ADR 043 §5 boot-time independence.

Future wire-side fresh encoders (CDC roadmap #10, follow-up
streaming surfaces) reuse `wire_ipc_write_options()` or
`wire_encode_batch()` by default. New uncompressed wire-side
encoders must justify the exception with the same boot-time-
independence shape ADR 043 §5 set the precedent for, and add
an architectural guard.

### 3. Leave HRFT framing clear and unchanged

`FrameHeader` size (24 bytes) is unchanged. `HRFT_ENVELOPE_VERSION`
stays at 1. ADR 053 does **not** add:

- a compression flag bit (`FRAME_FLAG_ARROW_LZ4`,
  `FRAME_FLAG_ACCEPTS_ARROW_LZ4`, or any other);
- a new HRFT message family;
- a new `MsgType` constant;
- a negotiated node capability;
- an envelope version bump.

The Arrow section is still "Arrow IPC bytes"; the difference
is that record batch buffers inside those IPC bytes may carry
Arrow's standard `BodyCompression` field.

### 4. `FrameHeader.flags` cleanup and known-mask validation

The pre-existing comment on `src/schema/core.rs:504`
documents bit 0 as `arrow_compressed` and bit 1 as
`has_trailing`. Both reservations are deleted; neither bit is
read or written anywhere in the codebase, and `trailing_len`
is the trailing-section authority.

ADR 053 also lands flag-honesty hardening:

```rust
pub const FRAME_FLAG_KNOWN_MASK: u16 = FRAME_FLAG_STREAM_END;
```

`read_frame_header` validates that
`header.flags & !FRAME_FLAG_KNOWN_MASK == 0`. Bit 2 is the
only allocated bit in V1; frames with other bits set reject
with `ErrorKind::InvalidData`. Future flag-introducing ADRs
extend both `FRAME_FLAG_*` constants and
`FRAME_FLAG_KNOWN_MASK` together.

The known-mask validation is **not a compression signal**.
Per-buffer Arrow IPC self-description handles compression
dispatch entirely. The validation's value is independent: it
turns silent flag-drift into an audible framing error, costing
one comparison per frame. The cleanup and the validation land
in the same ADR because doing the cleanup without the
validation leaves the surface accurately documented but
unenforced — a worse end-state than enforcing what the
documentation already says.

### 5. Receivers rely on Arrow readers

No HRFT read-path decompression is added. Arrow IPC
`StreamReader` sees the per-buffer `BodyCompression` metadata
and decompresses transparently — the same property ADR 043
§4 made load-bearing on the storage axis. This is
**load-bearing in two directions**:

- **Mixed-shape `arrow_payload` is first-class.** Frames
  whose payloads came from storage (compressed under ADR 043
  if the table opts in) and frames whose payloads were freshly
  encoded (always compressed under ADR 053) coexist on the
  wire and decode through the same path.
- **Rolling deploys are uneventful.** No protocol-version
  bump, no capability bit to flip, no negotiation window. A
  pre-ADR-053 node's wire frames stay uncompressed; receivers
  decompress when present and pass through when absent.

If a deployment must support binaries whose Arrow build
cannot read `LZ4_FRAME` IPC body compression, that is a
release compatibility constraint outside ADR 053's protocol
surface. The implementation pins `ipc_compression` on
mechanically (architectural guard) so a build that disables
it fails CI rather than silently producing wire frames with
no decompressing peer.

Decode failures remain Arrow IPC read errors at the existing
handler call sites; ADR 053 does not introduce HRFT-layer
LZ4 code.

### 6. Section caps and encryption order unchanged

The existing `MAX_SECTION_BYTES = 5 MiB` cap on each HRFT
section applies to **encoded (wire) bytes** —
post-compression. A peer sending a frame whose `arrow_len`
exceeds the cap is rejected before any payload is read, as
today. Compression shrinks bytes, not the cap.

Decoded-payload bound is the responsibility of Arrow's
`StreamReader`, which enforces its own per-buffer bounds
during decode. ADR 053 does not introduce a separate
decoded-byte budget.

Compression happens inside Arrow IPC encoding before HRFT
framing, before QUIC mTLS encryption. The effective order:

```text
RecordBatch encode → LZ4 body compression → HRFT framing →
QUIC mTLS encryption → wire
```

Compress-then-encrypt is the correct order both for density
(compression ratios collapse on high-entropy ciphertext) and
confidentiality. The reverse order is a known anti-pattern.
ADR 053 preserves the order by construction; the order was
already true for storage pass-through, and ADR 053 simply
adds the LZ4 step to fresh-encode paths.

### 7. No negotiation in V1

ADR 053 is a coordinated wire behaviour change, not an
optional per-peer feature. New writers emit Arrow IPC streams
with `BodyCompression` on fresh wire-encode paths. Readers
that use the project's Arrow IPC reader support both
compressed and uncompressed streams.

Because the representation is self-described inside Arrow
IPC, the wire contract does not need a node capability bit.
Adding a capability bit would create two sources of truth
(topology state and Arrow IPC metadata); the metadata is
authoritative.

Mixed rollout expectations are therefore simple:

- New-writer to new-reader traffic uses compressed Arrow IPC
  buffers.
- Old-writer to new-reader traffic remains readable because
  the IPC metadata has no compression field.
- Pass-through storage bytes remain whatever ADR 043
  produced and are already self-described.

### 8. Observability

Wire-side encode CPU time, encoded byte counts, batch counts,
ratios, and decode-error counts mirror ADR 043's metrics with
two changes: a new prefix to keep storage and wire axes
distinct, and a bounded per-site `path` label.

```text
network_compression_batches_total{codec, path, result}
network_compression_encoded_bytes_total{codec, path}
network_compression_encode_micros{codec, path}
network_compression_ratio{codec, path}
network_compression_decode_error_total{codec}
```

**Codec label values** (matches ADR 043): `uncompressed`,
`lz4_frame`. The metric prefix is `network_*` to keep the
wire-axis cost story separately readable from
`storage_compression_*`.

**Path label values** (bounded enumeration; architectural
test pins the set):

```text
client_batch       — src/client/core.rs::encode_batch
migration_records  — src/network/data_server.rs::encode_migration_records
query_response     — src/network/data_server.rs:3233 (TABLE_QUERY_RESP)
read_response      — src/network/data_server.rs:4387 (READ_RESP)
```

CDC and any other future fresh-encoder register a new `path`
value at the same time the encoder lands. No
tenant / table / peer / schema labels — the path enumeration
is the only per-site dimension.

**Result label values** (on `batches_total` only):
`compressed`, `error`. The metric increments only at
fresh-encode call sites (the helper); pass-through bytes do
not pass through the helper and are therefore not counted by
this metric family at all (see the receiver-side decode
counter for cross-axis observability).

**Decode-error counter is unlabelled beyond `codec`.**
Diagnostic reason text comes from `arrow::error::ArrowError`
which is unbounded in cardinality; emitting it as a label
would blow up Prometheus cardinality. The metric reports rate
only; per-error context lives in logs / spans at the
`StreamReader` consumer call site.

**Plaintext-byte counters are deliberately omitted from the
V1 production hot path.** ADR 043 §3 accepts a double-encode
cost (encode-once-uncompressed for `max_record_bytes`
enforcement, encode-compressed for the actual write) — but
the wire path has no enforcement axis to amortise the cost
against. The plaintext-byte ratio is observable through bench
fixtures at full precision; in production, operators read
encoded bytes plus batch counts plus the bench-derived ratio.
`network_compression_ratio` populates opportunistically when
a ratio sample is available without double-encoding (debug
builds, bench-instrumented call paths); production sees a
sparse histogram.

**Decode-latency histogram is omitted in V1.** Decoding is
owned by Arrow `StreamReader`; ADR 053 introduces no
HRFT-layer LZ4 code.
`network_compression_decode_error_total` is the sole
receiver-side counter, populated from instrumentation at the
existing wire-side `StreamReader::try_new(...)` consumer call
sites. Decoding observes the union of storage-pass-through
bytes and freshly-compressed wire bytes; the two cannot be
told apart at decode time (same Arrow IPC FlatBuffer shape)
and a single counter is the right resolution.

No per-frame audit event. Audit events would be per-RPC and
overwhelm the audit chain. A single deploy-time event marking
ADR 053 activation is enough.

### 9. Correctness domains are unchanged

Wire compression changes physical bytes only. Every logical
correctness invariant Wendelon ships continues to operate
over decoded Arrow values or the existing storage byte
domain, never over transient wire bytes. The list is explicit
so future ADRs that add new logical surfaces know where the
line is:

- **Admission decisions** — table-write admission,
  content-hash collapse (ADR 049), PK-delete supersession
  (ADR 050), secondary-index emission (ADR 052) all run after
  the receiver has decoded the Arrow batch.
- **Idempotency keys** — client-write idempotency keys
  compute over decoded payload bytes, not wire bytes.
- **Digest domains** — every `StateDigest` element preimage
  (ADR 046 substrate; ADR 047 / 048 / 049 / 050 / 052
  consumers) lives in the storage domain, not the wire domain.
- **Row / batch ordering** — Arrow batch row order survives
  per-buffer LZ4 round-trips losslessly. Multi-row record
  packing (ADR 042) sees the same order on the receiver as
  the sender produced.
- **HLC behavior** — hybrid-logical-clock timestamps live in
  row metadata; compression does not perturb them.
- **Raft / witness semantics** — Raft replicates control-ring
  state via postcard-encoded metadata, not Arrow payloads.
- **Storage descriptor fingerprints** —
  `LogicalSchemaFingerprint` /
  `PhysicalDescriptorFingerprint` (ADR 051) compute over
  descriptor bytes in the storage / catalog domain, not wire
  frames.
- **Object-store keys** — L2 paths are storage-domain.
- **CDC logical records** (roadmap #10, future) — CDC
  cursors and event ordering bind to row-axis facts
  (HLC, `__row_key_id__`), not wire bytes.
- **Secondary-index six-step revalidation** (ADR 052 §6) —
  every step runs over decoded rows.

Any hash, signature, or digest over logical records continues
to operate over decoded Arrow values or the existing storage
byte domain, not over transient wire bytes.

### 10. Composition with existing ADRs

- **ADR 004 / 010**: `bytemuck` POD metadata and Arrow IPC
  payloads are both unchanged at the framing layer. The
  `arrow_payload` section bytes are now self-describing as
  LZ4 inside the IPC FlatBuffer; the framing layer treats
  them as opaque the same way it always has.
- **ADR 011**: QUIC-first transport unchanged. ADR 053 does
  not introduce a transport-tier compression claim and does
  not couple to RDMA / TCP fallback.
- **ADR 020**: L2 envelope encryption unchanged. The wire
  axis is mTLS-encrypted on QUIC; the L2 axis is
  envelope-encrypted on storage. ADR 053 touches neither.
- **ADR 037**: tenant table schemas — descriptor surface
  unchanged. No new descriptor field. The wire-side codec is
  constant per build, not per-table.
- **ADR 042**: analytical multi-row records — wire-side
  encoding of multi-row Arrow IPC inherits per-buffer LZ4;
  the inner row-shape and synthetic columns are unchanged.
- **ADR 043**: per-table storage compression unchanged.
  Storage pass-through paths skip the wire helper. The two
  axes share the codec enum and the IPC FlatBuffer
  self-description, but their policy resolution is separate
  (per-table for storage; pinned for wire).
- **ADR 052**: secondary indexes — index-lookup responses
  carry storage-pass-through Arrow payloads (already
  compressed under ADR 043 if the table policy is `Lz4Frame`);
  no change.

## Rationale

**Why now.** Roadmap #9. ADR 043 closed the storage axis and
explicitly deferred the wire axis (ADR 043 §"Deferred to
follow-up ADRs" #7). CDC (roadmap #10) is next and benefits
from a settled wire-side codec contract.

**Why LZ4_FRAME.** Same reasoning as ADR 043 §Rationale.
Decompress speed (≈3–5 GB/s/core) dominates ratio on the read
path; wire frames are at least as latency-sensitive as
storage reads. The codec is already in the dependency graph
(`ipc_compression` Cargo feature). Sharing the codec with
storage means no second decode path, no second benchmark
surface, no second corruption-test regime.

**Why per-buffer (the Arrow IPC `BodyCompression` shape)
rather than a framing-layer LZ4 envelope.** Three properties
make per-buffer the load-bearing choice:

1. **Storage pass-through composes only with per-buffer.**
   Bytes coming from a `Lz4Frame` slab record are already
   per-buffer LZ4 inside the IPC FlatBuffer. A framing-layer
   envelope would double-compress them or require per-frame
   "is this already compressed?" detection logic to skip
   them. Per-buffer LZ4 inside the FlatBuffer composes
   trivially: pass-through bytes are already in the target
   shape.
2. **Small-payload fallback is automatic.** Arrow's
   per-buffer compressor falls back to uncompressed bytes
   with a `-1` length sentinel when LZ4 would expand the
   buffer (high-entropy or sub-frame-overhead inputs). A
   framing-layer envelope around the entire section does not
   get this for free and would have to re-implement it via an
   external threshold knob.
3. **Forwarding is flag-stripping-free.** A framing-layer
   flag would have to be stripped, normalised, and re-decided
   on every forwarding hop; the per-buffer design has no flag
   and bytes flow opaquely.

ADR 043 §4 rejected the same framing-layer shape on the
storage axis (`SlabRecordHeader` flag) for the same reasons;
ADR 053 inherits the architectural posture.

The framing-layer design's one genuine win — compressing
Arrow IPC FlatBuffer header bytes (schema descriptors,
dictionary batches), which per-buffer compression leaves
uncompressed — is too small to overturn the three above.
FlatBuffer headers are tens of bytes per stream; the density
of buffer bytes dominates by orders of magnitude.

**Why no HRFT flag bit.** A flag duplicates information
already stored in the Arrow IPC FlatBuffer. Duplicated codec
state invites disagreement bugs: HRFT could say compressed
while Arrow metadata says uncompressed, or the reverse. ADR
053 chooses one authority.

**Why the known-mask validation lands here.** The known mask
is not a compression signal — it is a framing-honesty guard.
It lands in this ADR because the bit-0 / bit-1 cleanup makes
the known-mask surface clean by construction (only
`FRAME_FLAG_STREAM_END` is allocated). Doing the cleanup
without the validation leaves the surface accurately
documented but unenforced — a worse end-state than enforcing
what the documentation already says.

**Why no node capability bit.** Build-time Cargo feature plus
IPC self-description is the correct shape for the current
peer set: every Wendelon node and the only client crate
(`crates/wendelon-client/`) build against the same `arrow`
crate with `ipc_compression` on. The capability-bit design
would be the right shape *if and when* a non-Rust SDK without
LZ4 support emerges, or if a deployment ever intentionally
builds without `ipc_compression`. Per ADR 011's truth-matched
discipline and `no-premature-backward-compat.md`, V1 declines
to ship a negotiation surface whose only consumer is
hypothetical. An architectural guard pins `ipc_compression`
on so a build that disables it fails CI rather than silently
degrading.

**Why mandatory for fresh wire encoders, no V1 knob.** A
threshold, config knob, or per-peer negotiation matrix would
increase the protocol surface before there is evidence the
extra choice is useful. Arrow IPC buffer compression is
narrow, local, and easy to audit. Per
`no-premature-backward-compat.md`, the right discipline is
to pin the codec and amend if benchmarks reveal a problem.

**Why descriptor schema IPC stays uncompressed.** Descriptor
schema IPC is small, boot-time critical, and intentionally
readable without depending on storage body-compression
policy.

**Why storage pass-through is not re-encoded.** Pass-through
bytes are already in a durable physical representation
selected by storage policy. Re-encoding them on the transport
path burns CPU, risks changing byte accounting, and creates
a second policy owner.

**Why keep meta and trailing bytes uncompressed.** Two reasons
compose. *Density*: those sections are small (typically
O(100) bytes of high-entropy postcard / POD bytes); LZ4
framing overhead dominates. *Compression-oracle surface*:
meta and trailing carry operator and authorisation material;
the Arrow section is tenant-controlled. Restricting
compression to the Arrow section keeps the contexts disjoint
and removes a CRIME / BREACH-class side-channel.

**Why `{codec, path}` bounded labels on metrics.** The path
label tells operators which fresh-encoder produced the
bytes. The bounded enumeration (architectural-test pinned)
prevents cardinality blow-up and prevents the future leak of
tenant / table / peer / schema labels into a metric family
that isn't built for that fan-out.

**Why no production plaintext-byte counter in V1.** ADR 043
§3 accepted the double-encode cost because it amortises
against `max_record_bytes` enforcement. The wire path has no
enforcement axis to amortise against. Bench fixtures expose
ratios at full precision; production reads encoded bytes
plus the bench-derived ratio. A future ADR can lift the
omission if operators surface a need.

**Why no decode-latency histogram in V1.** Decoding is owned
by Arrow `StreamReader`; ADR 053 introduces no HRFT-layer
LZ4 code, so decode-latency observation would require new
instrumentation at every reader call site. The
decode-error counter is enough V1 receiver-side surface.

**Why this is its own ADR rather than an ADR 043
amendment.** Wire compression is a discrete capability with
its own observability surface, threat-model analysis, helper
site, and forward-compat story (CDC).

**Why this is its own ADR rather than waiting for the CDC
ADR.** CDC will be a multi-iteration design with a wide
surface (cursor shape, event ordering, replay semantics).
Bundling the wire-side codec choice into CDC would slow
both.

## Consequences

- **Encode CPU on every fresh-encode wire-side path.**
  LZ4_FRAME encodes at ≈500 MB/s/core. Of the four flipped
  call sites, two (`client_batch`, `migration_records`) are
  small-payload paths with negligible CPU impact; two
  (`query_response`, `read_response`) can carry larger
  server-synthesized batches and will see the same encode
  share ADR 043 storage-side writers see (typically 10–20%
  of a write-bound core). Future high-throughput wire-side
  encoders (CDC streaming) inherit the same profile.
- **Decode CPU on every wire-side reader path.** The
  `ipc_compression` Cargo feature is on; receivers already
  decompress storage-pass-through bytes. ADR 053 shifts more
  bytes into the compressed shape but does not introduce a
  new code path.
- **Wire bandwidth shrinks on Arrow payloads.** Compressible
  analytical batches see ~2× density.
- **Mixed-shape `arrow_payload` is first-class.** Frames
  whose payloads came from storage and frames whose payloads
  were freshly encoded coexist on the wire and decode through
  the same path.
- **Rolling deploys are uneventful.** No protocol-version
  bump; no capability bit to flip; no negotiation window.
- **`FrameHeader.flags` bit-0 / bit-1 reservations are
  freed.** A future ADR allocates a fresh bit.
- **`FRAME_FLAG_KNOWN_MASK` validation hardens the framing
  layer.** Receivers reject frames with unknown flag bits.
  Mostly invisible in steady state; surfaces flag-drift bugs
  early during rollouts of future flag-introducing ADRs.
- **CRIME-class concerns are documented and dismissed at the
  V1 threat model.** The meta / trailing exclusion keeps
  control bytes and tenant bytes in disjoint compression
  contexts; the V1 length-padding mitigation budget is zero.
- **Observability gains a `network_compression_*` metric
  family.** Operators reading wire-side cost dashboards see
  a parallel surface to ADR 043's `storage_compression_*`.
- **External SDKs in non-Rust languages would need an LZ4
  decoder** if and when Wendelon ships them. Today the only
  client crate is `crates/wendelon-client/` (Rust); ADR 053
  takes no SDK contract change in V1.
- **No storage shape change, no descriptor field, no
  `format_version` bump, no new HRFT frame family, no new
  node capability bit, no Cargo feature or dependency
  change.**

## Rejected alternatives

- **Framing-layer LZ4 envelope around the Arrow section
  with `FRAME_FLAG_ARROW_LZ4` bit and
  `[decoded_len_le: u32][lz4_block]` shape.** Rejected. The
  framing-layer envelope competes with per-buffer Arrow
  `BodyCompression` on three axes (storage pass-through
  composition; small / high-entropy buffer fallback;
  forwarding-path simplicity) and loses on each. ADR 043 §4
  rejected the same shape on the storage axis for the same
  reasons.
- **`FrameHeader.flags` bit-0 wired as the dispatch
  signal.** Rejected as a sub-case of the framing-layer
  envelope alternative.
- **Per-RPC negotiated compression** (request carries an
  `accept_compressed` flag; response opts in only on matched
  bit). Rejected for V1 with a real trade-off: composes well
  with the framing-layer envelope and is the right shape *if
  and when* a non-LZ4 SDK lands. The cost of carrying it now
  (per-frame branch, runbook complexity, two-state matrix
  between every pair of peers) is real and recurring; the
  benefit is hypothetical. A future ADR introducing the
  non-LZ4 SDK adds the negotiation surface at the same time.
- **`NodeCapability::FrameArrowLz4Compression` bit.**
  Rejected for the same reason as the per-RPC accept flag.
  V1 adds an architectural guard pinning `ipc_compression`
  on instead.
- **Cluster-level kill-switch.** Rejected per
  `no-premature-backward-compat.md`. If LZ4_FRAME proves
  wrong on a workload, a follow-up ADR opens.
- **Per-table wire compression policy** (read the table's
  `RecordBodyCompressionPolicy` and apply it on the wire
  too). Rejected. Wire-axis cost is dominated by frame size,
  not table-shape.
- **zstd as the V1 codec.** Rejected. Decompression speed
  cost is too high for the latency-sensitive wire path.
- **snappy.** Rejected. LZ4_FRAME dominates snappy on both
  axes; same reasoning as ADR 043.
- **Compress meta and trailing sections.** Rejected for
  density (small payloads) and CRIME-class
  compression-oracle surface.
- **Compress the descriptor schema IPC.** Rejected. Boot-time
  independence per ADR 043 §5.
- **Transport-tier compression** (QUIC stream-frame
  compression). Rejected. No standardised path.
- **TLS-record compression.** Rejected by RFC 8446.
- **Encrypt-then-compress.** Rejected — anti-pattern.
- **Reserve `Zstd` and `Snappy` enum slots.** Already
  rejected by ADR 043.
- **`WireBodyCompressionPolicy` parallel newtype.** Rejected.
  The codec enum names codecs, not placements.
- **Bump `HRFT_ENVELOPE_VERSION` to 2.** Rejected. Receivers
  decompress transparently; nothing for the envelope version
  to gate.
- **New HRFT message types `ARROW_COMPRESSED_*` parallel to
  existing types.** Rejected. Doubles the message-type enum
  without buying separation.
- **Apply LZ4 over the entire HRFT frame** (header + meta +
  arrow + trailing) at the framing layer. Rejected.
  Compresses the bytemuck POD header (high-entropy under
  varint encoding — poor ratio), bypasses IPC FlatBuffer
  self-description, and breaks the existing zero-copy
  bytemuck cast on `FrameHeader`.

## Resolved questions

V1 commitments — each can be revisited in a follow-up ADR
without on-disk surface change.

1. Wire-side fresh-encode Arrow IPC: per-buffer LZ4_FRAME,
   pinned codec.
2. Codec enum reused: `RecordBodyCompressionPolicy::Lz4Frame`.
3. Receivers decode transparently via `StreamReader` with
   `ipc_compression` Cargo feature on (already enforced by
   ADR 043 §8; ADR 053 elevates to architectural guard).
4. Storage pass-through paths skip the wire helper.
5. No new HRFT frame family, message type, envelope version
   bump, or node capability bit.
6. `FrameHeader.flags` bit-0 (`arrow_compressed`) and bit-1
   (`has_trailing`) doc reservations removed.
7. `FRAME_FLAG_KNOWN_MASK` validation in `read_frame_header`;
   unknown flag bits reject with `ErrorKind::InvalidData`.
8. Wire-side encoder helpers at
   `src/network/ipc.rs::wire_ipc_write_options` (options-
   level) and `src/network/ipc.rs::wire_encode_batch`
   (function-level), both pinned via `WIRE_BODY_COMPRESSION`.
9. Four wire-side fresh-encoder call sites flip:
   `src/client/core.rs::encode_batch`,
   `src/network/data_server.rs::encode_migration_records`,
   `src/network/data_server.rs:3233` (TABLE_QUERY_RESP fresh
   response synthesis, today routed through
   `multirow::encode_batch`),
   `src/network/data_server.rs:4387` (READ_RESP fresh
   response synthesis, same shim).
   `src/schema/table.rs::encode_arrow_schema_ipc` stays
   uncompressed (architectural guard).
   `src/storage/multirow.rs::encode_batch` reduces to
   test-only after the flip; architectural guard pins zero
   production wire-path callers.
10. Compress-then-encrypt order preserved by construction.
11. `MAX_SECTION_BYTES = 5 MiB` cap unchanged; applied on
    encoded (wire) bytes. Decoded-payload bound owned by
    `StreamReader`.
12. Observability metrics under `network_compression_*` prefix
    with bounded `{codec, path}` labels.
    `codec ∈ {uncompressed, lz4_frame}`;
    `path ∈ {client_batch, migration_records,
    query_response, read_response}` initially, pinned by
    architectural test.
    `batches_total` carries `result ∈ {compressed, error}`.
    `decode_error_total` carries `{codec}` only — diagnostic
    text lives in logs, not labels (Arrow's error surface is
    unbounded). Plaintext-byte counter omitted from V1
    production hot path. Decode latency histogram deferred.
    Pass-through bytes are *not* counted by the
    `network_compression_*` family — they never invoke the
    fresh-encode helper.
13. CRIME-class concerns documented and dismissed; meta /
    trailing exclusion is framed as side-channel-context-
    disjointness, not just payload-density.
14. Architectural guard pins `ipc_compression` Cargo feature
    on so a build that disables it fails CI.

## Open questions

V1 picks defaults; each can be revisited without wire surface
change.

1. **Helper colocation.** `src/network/ipc.rs` (new) is the
   V1 home. If a third axis emerges, the shared
   `ipc_write_options_for_policy` builder may migrate to a
   neutral `src/ipc/options.rs`.
2. **Whether the architecture writer audit lives in one
   broad test or in smaller tests next to network IPC
   helpers.** Implementation-side decision.
3. **Codec evolution path.** When and if a future ADR adds a
   second wire-side codec (zstd for cold-CDC replay, for
   example), the codec choice becomes per-stream rather than
   build-pinned. The IPC FlatBuffer already supports the
   dispatch; a future ADR adds the policy plumbing.
4. **CDC path-label registration.** When CDC lands (roadmap
   #10), it registers a new `path` label value. The exact
   name is the CDC ADR's call (`cdc_event`, `cdc_replay`,
   etc.). ADR 053 does not pre-reserve a value; the label
   set is ADR-extensible via the architectural-test
   enumeration.
5. **Whether to lift the V1 plaintext-byte omission.** If
   operators surface a need for live ratio accounting, a
   follow-up ADR opts into the double-encode shape (mirrors
   ADR 043 §3) without changing metric names.

## Forward-compatibility analysis

**Roadmap #10 CDC.** CDC emitters are wire-side fresh
encoders by construction (events flow from in-memory state,
not storage pass-through). They reuse
`wire_ipc_write_options()` and inherit per-buffer LZ4 by
default. CDC registers a new `path` label value. No
CDC-specific codec decision required.

**Roadmap #11 LSM-style leveled compaction.** Storage axis
only; ADR 053 does not change the LSM compaction surface.
Wire-side fan-out of compaction outputs (e.g., range-handoff
RPCs) inherits ADR 053's wire encoder.

**Roadmap #12 vector embeddings.** ANN index sidecars follow
the storage axis (ADR 043 / future). Wire-side ANN responses
(k-NN result Arrow batches) inherit ADR 053.

**Roadmap #13 encoding / fetch / distributed compute
refinements.** LCE V2 / V3 (homerolled SIMD encodings, COCD)
target the storage axis. Wire-side density is amenable to
LCE encodings inside Arrow buffers as a future option, but
ADR 053 does not pre-allocate the surface.

**Future zstd codec for capacity-tier CDC replay.** A
follow-up ADR adds a new `RecordBodyCompressionPolicy`
variant when measured workloads pull. The wire helper becomes
per-stream-policy; the IPC FlatBuffer dispatch is unchanged.

**Future per-buffer page directory** (Brainstorm 011 V2
sub-piece). ADR 053's per-buffer LZ4 substrate makes the
page directory work straightforwardly on the wire as it does
on storage.

**Future column-projection on the wire.** A future ADR that
lets receivers fetch only projected columns over the wire
composes with per-buffer LZ4: each column buffer
decompresses independently, so the receiver can skip the
buffers it doesn't project without fetching them.

**Future external multi-language SDKs.** When Wendelon ships
a non-Rust SDK, the SDK either bundles an LZ4 decoder
(preferred — keeps the wire contract uniform) or a future
SDK ADR introduces a per-RPC accept flag plus
`NodeCapability::WireBodyLz4` for the negotiation case.
ADR 053 does not pre-reserve the surface.

**Future wire-side encryption changes.** Compress-then-encrypt
is invariant under any TLS / QUIC version change because
compression sits above framing sits above QUIC.

## Deferred to follow-up ADRs

1. zstd codec for the wire axis (cold-CDC replay,
   capacity-tier streaming).
2. Per-stream / per-RPC compression policy negotiation.
3. Cluster-level wire-compression kill-switch.
4. External multi-language SDK compression contract.
5. Compression of HRFT meta or trailing sections.
6. Per-buffer page directories on the wire (Brainstorm 011
   V2).
7. Column-projection on the wire (composes with per-buffer
   LZ4 substrate).
8. CRIME-class side-channel mitigation (length padding,
   randomised dispersal) if a future client surface raises
   the threat model.
9. `FrameHeader.flags` bit reorganisation if a future ADR
   needs new flag bits and the freed bits 0–1 become useful.
10. Compute-over-compressed-data on wire-flowing batches
    (Brainstorm 011 V3 mirror).
11. Wire-side decode-latency histogram if operationally
    pulled.
12. Live plaintext-byte / ratio accounting in production (if
    operators surface the need; lift the V1 omission via the
    same metric names, switching only the population
    call-site).

## References

- **ADR 004** — zero-copy custom transport; `bytemuck` POD
  metadata; Arrow IPC payloads.
- **ADR 010** — tiered serialization strategy.
- **ADR 011** — QUIC-first transport; no auto-adaptive
  multi-transport claim.
- **ADR 020** — L2 envelope encryption and DEK lifecycle;
  compress-then-encrypt order; AEAD invariance under ADR
  053.
- **ADR 037** — tenant table schemas; descriptor surface
  unchanged.
- **ADR 042** — analytical multi-row records; multi-row
  Arrow IPCs flow through wire helper.
- **ADR 043** — Arrow IPC body compression on storage; ADR
  053 mirrors the choice on the wire and inherits the codec,
  the IPC FlatBuffer dispatch, the `ipc_compression` Cargo
  feature contract, and the compress-then-encrypt
  invariant.
- **ADR 052** — secondary indexes; index-lookup responses
  flow through wire helper for fresh-encode results,
  pass-through for storage results.
- `plans/conventions/newtype-units.md`.
- `plans/conventions/no-version-suffixes.md`.
- `plans/conventions/no-premature-backward-compat.md` —
  load-bearing on bit-0 / bit-1 cleanup, on the no-V1-knob
  decision, and on the deferred negotiation surface.
- `plans/conventions/no-project-name-in-protocols.md` —
  metric prefixes and helper names stay brand-clean.
- `plans/conventions/capability-vs-feature-naming.md` — no
  new node capability bit.
- `plans/philosophy.md` — Identity-vs-Locality framing
  applied to the wire axis.

## Promotion checklist

ADR 053 promotes when every item below is settled in code or
test, with the council on record.

- [ ] Council agrees ADR 053 is per-buffer Arrow IPC
      `BodyCompression` on fresh wire encoders, not a
      framing-layer LZ4 envelope.
- [ ] `WIRE_BODY_COMPRESSION = RecordBodyCompressionPolicy::Lz4Frame`
      pinned in `src/network/ipc.rs` and verified by
      positive architectural guard.
- [ ] `wire_ipc_write_options()` is the single helper used
      by every fresh-wire Arrow IPC writer, with
      `wire_encode_batch()` as its function-level companion
      for callers that previously routed through
      `multirow::encode_batch`.
- [ ] Fresh-wire fresh-encoder call sites enumerated in
      code — all four flipped, no missed sites in the audit
      grep (which covers both direct
      `StreamWriter::try_new[_with_options]` and indirect
      `encode_record_batch_bytes` / `encode_record_batch` /
      `multirow::encode_batch`):
      - direct `StreamWriter` sites:
        - `src/client/core.rs::encode_batch`
        - `src/network/data_server.rs::encode_migration_records`
      - indirect sites (currently call
        `multirow::encode_batch`):
        - `src/network/data_server.rs:3233`
          (`TABLE_QUERY_RESP`)
        - `src/network/data_server.rs:4387` (`READ_RESP`)
- [ ] After flip, `multirow::encode_batch` has zero
      production wire-path callers (architectural guard).
- [ ] Explicit exclusions documented and architecturally
      guarded: `src/schema/table.rs::encode_arrow_schema_ipc`
      stays uncompressed; storage pass-through bypasses the
      helper.
- [ ] `FrameHeader.flags` bit-0 / bit-1 doc reservations
      removed; `FRAME_FLAG_KNOWN_MASK` validation in place.
- [ ] `ipc_compression` Cargo feature pinned on by
      architectural guard.
- [ ] No new HRFT message family, message type, envelope
      `format_version` bump, `FRAME_FLAG_*` constant for
      compression, node capability bit, Cargo feature, or
      `WireBodyCompressionPolicy` / `WireCompressionLevel`
      newtype.
- [ ] `network_compression_*` metrics with bounded
      `{codec, path}` labels;
      `path ∈ {client_batch, migration_records,
      query_response, read_response}` pinned by
      architectural test.
      `result ∈ {compressed, error}` on `batches_total`.
      `decode_error_total` carries `{codec}` only. No
      production plaintext-byte counter; no decode-latency
      histogram.
- [ ] Bench matrix populated:
      `plans/impl/053_wire_arrow_ipc_body_compression_bench_results.md`
      shows encode CPU, decode CPU, payload-size ratio, and
      end-to-end RPC latency at small-edge (1 KiB), small
      (4 KiB), medium (1 MiB), large (10 MiB) shapes.
- [ ] Bench acceptance: indexed encoded bytes ≤ 60% of
      uncompressed on analytical-shaped batches; small-frame
      RPC latency does not regress > 5%; storage pass-through
      paths show zero encode CPU under the wire helper.
- [ ] Runbook lands at
      `plans/ops/runbooks/wire_arrow_ipc_body_compression.md`.
- [ ] `plans/wip/roadmap.md` rewritten to reflect ADR 053
      promoted to `plans/adr/`; companion impl plan moved to
      `plans/impl/`.
