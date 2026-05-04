# Implementation Plan: ADR 053 — Wire-Side Arrow IPC Body Compression

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/053_wire_arrow_ipc_body_compression.md`

Inherits codec choice and IPC FlatBuffer dispatch:
`plans/adr/043_arrow_ipc_body_compression.md`.
Inherits transport surface:
`plans/adr/004_zero_copy_transport.md`,
`plans/adr/011_auto_adaptive_transport.md`.
Inherits serialization tiering:
`plans/adr/010_serialization_tiers.md`.
Inherits encryption invariant:
`plans/adr/020_l2_envelope_encryption_and_dek_lifecycle.md`.

## Goal

Flip every wire-side fresh-encode Arrow IPC writer from
default `IpcWriteOptions` to per-buffer LZ4_FRAME, mirroring
ADR 043's storage-side choice. Receivers decompress
transparently via existing `StreamReader` plus the on-by-
default `ipc_compression` Cargo feature. Storage pass-
through Arrow payloads flow unchanged. No new HRFT frame
family, no new envelope `format_version`, no new node
capability bit, no per-table or per-RPC negotiation. Free
the dead `arrow_compressed` (bit 0) and `has_trailing` (bit
1) reservations on `FrameHeader.flags` and land
`FRAME_FLAG_KNOWN_MASK` validation as flag-honesty
hardening alongside the cleanup.

The implementation lets wire-side compression drive:

- two helpers (`wire_ipc_write_options() -> IpcWriteOptions`
  for direct `StreamWriter` callers, and
  `wire_encode_batch(&RecordBatch) -> Vec<u8>` for one-shot
  callers) plus a `WIRE_BODY_COMPRESSION` constant pinned to
  `RecordBodyCompressionPolicy::Lz4Frame`;
- four flipped wire-side fresh-encoder call sites:
  - `src/client/core.rs::encode_batch` (direct);
  - `src/network/data_server.rs::encode_migration_records`
    (direct);
  - `src/network/data_server.rs:3233` TABLE_QUERY_RESP
    fresh response synthesis (today routes through
    `multirow::encode_batch` with hardcoded
    `Uncompressed` policy);
  - `src/network/data_server.rs:4387` READ_RESP fresh
    response synthesis (same shim);
- one **explicitly excluded** writer call site
  (`src/schema/table.rs::encode_arrow_schema_ipc`) guarded
  by an architectural test;
- a four-category writer-audit taxonomy (`fresh-wire` /
  `descriptor-schema` / `storage` / `test-or-bench`) that
  classifies every Arrow IPC `StreamWriter` site;
- `network_compression_*` metric family with bounded
  `{codec, path}` labels; `result ∈ {compressed, error}` on
  `batches_total`; `decode_error_total` carries `{codec}`
  only (Arrow's error surface is unbounded, so reason text
  goes to logs not labels);
- `FrameHeader.flags` bit-0 / bit-1 doc cleanup plus
  `FRAME_FLAG_KNOWN_MASK` validation in `read_frame_header`;
- an architectural guard pinning the `ipc_compression` Cargo
  feature on so a build that disables it fails CI;
- bench matrix that distinguishes encode CPU, decode CPU,
  and end-to-end RPC latency from payload-size reduction.

## Deliverables

- `WIRE_BODY_COMPRESSION` constant,
  `wire_ipc_write_options()` helper, and
  `wire_encode_batch()` helper in `src/network/ipc.rs`
  (new).
- Flipped `encode_batch` in `src/client/core.rs`.
- Flipped `encode_migration_records` in
  `src/network/data_server.rs`.
- Flipped fresh response synthesis at
  `src/network/data_server.rs:3233` (TABLE_QUERY_RESP) and
  `src/network/data_server.rs:4387` (READ_RESP) — both
  switch from `multirow::encode_batch` to
  `wire_encode_batch`.
- `src/storage/multirow.rs::encode_batch` reduced to a
  test-only utility (no production wire-path callers
  remain); architectural test pins this so a future
  production caller cannot regress.
- `FrameHeader.flags` doc-comment cleanup in
  `src/schema/core.rs`; `FRAME_FLAG_KNOWN_MASK` constant
  added; `read_frame_header` validates against the mask.
- `network_compression_*` metric family in
  `src/metrics/registry.rs` with bounded labels.
- Architectural guards: every wire-side
  `StreamWriter::try_new(...)` site routes through
  `wire_ipc_write_options()` *or* is on the explicit
  exclusion list; descriptor schema IPC stays uncompressed;
  no new flag bit is allocated for compression; no envelope
  `format_version` bump; no new HRFT frame family / message
  type; no new node capability bit; `ipc_compression` Cargo
  feature pinned on; no `WireBodyCompressionPolicy` /
  `WireCompressionLevel` newtype; no project-name prefix on
  new symbols.
- Golden vectors covering:
  - wire-side fresh-encode round-trip with LZ4 on;
  - storage pass-through round-trip with bytes untouched;
  - mixed pre-/post-deploy frame round-trip (uncompressed
    sender, compressed receiver and vice versa);
  - migration RPC round-trip;
  - client write Arrow round-trip;
  - descriptor schema IPC stays uncompressed (positive
    guard);
  - forwarding-path invariant (proxied bytes flow
    verbatim);
  - frames with bit 0, bit 1, and bit 0 + stream-end
    rejected by known-mask validation.
- Bench: wire-side encode / decode CPU, payload-size ratio,
  end-to-end RPC latency at small-edge (1 KiB), small
  (4 KiB), medium (1 MiB), large (10 MiB) payload sizes.
- Operator metrics, runbook at
  `plans/ops/runbooks/wire_arrow_ipc_body_compression.md`.

## Non-goals

- No new HRFT frame family or message type.
- No new HRFT envelope `format_version` bump.
- No new `FrameHeader.flags` bit allocated for compression
  (the pre-existing bit-0 reservation is *removed*, not
  wired).
- No new node capability bit.
- No per-frame, per-tenant, per-table, or per-RPC
  compression negotiation.
- No cluster-level kill-switch.
- No second wire-side codec (zstd / snappy) in V1.
- No `CompressionLevel` newtype.
- No `WireBodyCompressionPolicy` enum (reuse
  `RecordBodyCompressionPolicy`).
- No compression of meta / trailing sections.
- No compression of the descriptor schema IPC.
- No new Cargo feature or dependency. `ipc_compression` is
  already on per ADR 043 §8; ADR 053 pins it but adds
  nothing.
- No `ObjectStore` trait change.
- No PBC / AGG protocol change.
- No reserved enum slots, compatibility shims, or `V1` /
  `V2` Rust type suffixes.
- No CRIME-class side-channel mitigation in V1.
- No external multi-language SDK contract change.
- No production plaintext-byte counter in V1.
- No decode-latency histogram in V1.

## Phase 0 — Contract freeze, fixtures, architectural guards

Lock the helper signature, the constant, the emission rule,
the writer-audit taxonomy, the metric family shape, the
section-byte cap discipline, the `FRAME_FLAG_KNOWN_MASK`
shape, and the architectural guards before touching
encoder call sites or metrics.

Files:

- `src/network/ipc.rs` (new — wire-side encoder helper)
- `src/network/mod.rs` (re-export)
- `src/schema/core.rs` (constants, doc cleanup)
- `tests/fixtures/wire_arrow_ipc_body_compression/` (new)
- `tests/architecture_test.rs`

Work:

1. Freeze the helper signatures:

   ```rust
   pub const WIRE_BODY_COMPRESSION: RecordBodyCompressionPolicy =
       RecordBodyCompressionPolicy::Lz4Frame;

   /// Options-level helper for direct `StreamWriter` callers.
   pub fn wire_ipc_write_options() -> std::io::Result<IpcWriteOptions> {
       crate::storage::ipc::ipc_write_options_for_policy(WIRE_BODY_COMPRESSION)
   }

   /// Function-level helper for one-shot encoders that
   /// previously called `multirow::encode_batch` or rolled
   /// their own `StreamWriter` dance with default options.
   pub fn wire_encode_batch(batch: &RecordBatch) -> std::io::Result<Vec<u8>> {
       crate::storage::ipc::encode_record_batch_bytes(batch, WIRE_BODY_COMPRESSION)
   }
   ```

   Both helpers sit in `src/network/ipc.rs`. The codec enum
   is reused from `src/schema` (the same enum ADR 043
   uses); the option builder
   `ipc_write_options_for_policy` and the encode wrapper
   `encode_record_batch_bytes` are reused from
   `src/storage/ipc.rs`. ADR 053 does *not* move those
   storage helpers to a neutral home in V1 — that refactor
   is opportunistic and lands when / if a third axis
   emerges.

2. Freeze the **emission rule**:

   - Every wire-side fresh-encode Arrow IPC writer routes
     through `wire_ipc_write_options()` and
     `StreamWriter::try_new_with_options(...)`.
   - Storage pass-through paths bypass the helper. The
     bytes are already self-describing under ADR 043.
   - Receivers always use `StreamReader::try_new(...,
     None)` with the `ipc_compression` Cargo feature on
     (already enforced by ADR 043 §8 build profile; this
     ADR adds a mechanical CI guard).

3. Freeze the **writer audit taxonomy**. Every Arrow IPC
   write site in the repo classifies into exactly one
   category. "Write site" includes both direct
   `StreamWriter::try_new(...)` instantiations and indirect
   shim helpers that wrap them with a fixed policy
   (`multirow::encode_batch`, `encode_record_batch_bytes`,
   `encode_record_batch`).

   - **`fresh-wire`** — encodes an in-memory `RecordBatch`
     into bytes destined for the `arrow_payload` section of
     an HRFT frame. Must call `wire_ipc_write_options()`
     (direct callers) or `wire_encode_batch()` (one-shot
     callers). V1 known sites:
     `src/client/core.rs::encode_batch`,
     `src/network/data_server.rs::encode_migration_records`,
     `src/network/data_server.rs:3233` (TABLE_QUERY_RESP),
     `src/network/data_server.rs:4387` (READ_RESP).
   - **`descriptor-schema`** — encodes Arrow schema metadata
     embedded inside descriptors. Must use default
     (uncompressed) options for ADR 043 §5 boot-time
     independence. V1 known site:
     `src/schema/table.rs::encode_arrow_schema_ipc`.
   - **`storage`** — encodes Arrow batches destined for
     slab records. Governed by ADR 043's per-table policy
     via `src/storage/ipc.rs::ipc_write_options_for_policy`.
     ADR 053 does not change these. Includes
     `multirow::encode_multirow_from_survivors_with_policy`,
     `multirow::pack_multirow_source_rows_with_policy`,
     and similar `_with_policy` siblings.
   - **`test-or-bench`** — encodes batches inside test
     fixtures or benchmarks. Mirrors a production
     classification or documents intentional default-options
     use. After Phase 1, `multirow::encode_batch` (the
     hardcoded-`Uncompressed` shim) lands here — its
     production wire-path callers all flip to
     `wire_encode_batch`; only test callers remain.

   The audit grep covers direct *and* indirect sites:

   ```sh
   rg -n 'StreamWriter::try_new\b|StreamWriter::try_new_with_options' src tests benches
   rg -n '\bencode_record_batch_bytes\b|\bencode_record_batch\b|multirow::encode_batch\b' src tests benches
   ```

   Phase 1 classifies each match. The architectural test in
   Phase 6 turns the classification into a permanent
   invariant: every match falls into one of the four
   categories, every `fresh-wire` site uses the helper, every
   `descriptor-schema` site stays on default options, no
   site is unclassified.

4. Freeze the **explicit exclusion list** as the literal
   value of the `descriptor-schema` and known
   `test-or-bench-default` categories. The list lives in
   `src/network/ipc.rs` documentation and is read by the
   Phase 6 architectural test.

5. Freeze the **metric family** with bounded labels:

   ```text
   network_compression_batches_total{codec, path, result}
   network_compression_encoded_bytes_total{codec, path}
   network_compression_encode_micros{codec, path}
   network_compression_ratio{codec, path}
   network_compression_decode_error_total{codec}
   ```

   - **`codec`**: `uncompressed`, `lz4_frame`. Matches
     ADR 043.
   - **`path`** (bounded enumeration; architectural test
     pins the set): `client_batch`, `migration_records`,
     `query_response`, `read_response`. A future ADR adding
     a fresh-encoder registers a new value at the same
     time.
   - **`result`** (on `batches_total` only):
     `compressed`, `error`. The metric increments only at
     fresh-encode call sites (the helper); pass-through
     bytes do not pass through the helper and are
     therefore not counted by this family at all.
   - **`decode_error_total` is unlabelled beyond `codec`**
     — diagnostic reason text comes from
     `arrow::error::ArrowError`, which is unbounded;
     emitting it as a label would blow up Prometheus
     cardinality. Per-error context lives in logs / spans
     at the `StreamReader` consumer call site.
   - Prefix `network_*` distinguishes from ADR 043's
     `storage_compression_*`.

   No `tenant`, `table`, `peer`, `schema`, or `node`
   labels on this family.

   **Plaintext-byte counter is omitted from V1 production
   hot path.** ADR 053 §8 declines the double-encode cost
   ADR 043 §3 accepts, because the wire path has no
   enforcement axis to amortise the cost against.
   Plaintext-byte ratio is observable through bench
   fixtures at full precision; in production,
   `network_compression_ratio` populates opportunistically
   when a ratio sample is available without
   double-encoding (debug builds, bench-instrumented call
   paths).

   **Decode-latency histogram is omitted in V1.** Decoding
   is owned by Arrow `StreamReader`; ADR 053 introduces no
   HRFT-layer LZ4 code.

   **Receiver-side decode error counter** is the sole V1
   receiver-side metric, populated from instrumentation at
   the existing wire-side `StreamReader::try_new(...)`
   consumer call sites in `src/network/data_server.rs`,
   `src/network/control_server.rs`, `src/client/core.rs`.

6. Freeze the **`FRAME_FLAG_KNOWN_MASK` validation** at the
   framing layer:

   ```rust
   pub const FRAME_FLAG_KNOWN_MASK: u16 = FRAME_FLAG_STREAM_END;
   ```

   Bit 2 is the only allocated flag in V1. Bits 0 and 1
   (the freed `arrow_compressed` / `has_trailing`
   reservations) are *not* in the known mask.
   `read_frame_header` rejects frames whose
   `flags & !FRAME_FLAG_KNOWN_MASK != 0` with
   `ErrorKind::InvalidData`. A future flag-introducing ADR
   extends both the `FRAME_FLAG_*` constants and
   `FRAME_FLAG_KNOWN_MASK` together.

7. Freeze the **section-byte cap discipline**:

   - `MAX_SECTION_BYTES = 5 MiB` (existing) applies to
     **encoded (wire) bytes**, i.e. post-compression.
   - Decoded-payload bound is owned by Arrow
     `StreamReader`, not by this ADR.
   - The compression helper does not change either bound.

8. Architectural guards (TODO entries; populated by Phase
   6):

   - `wire_ipc_write_options` is the only wire-side
     `IpcWriteOptions` constructor used by
     `StreamWriter::try_new_with_options(...)` outside the
     storage layer.
   - Wire-side `StreamWriter::try_new(...)` (default
     options) appears only in the documented exclusion list.
   - `WIRE_BODY_COMPRESSION ==
     RecordBodyCompressionPolicy::Lz4Frame` (positive
     guard).
   - `HRFT_ENVELOPE_VERSION` unchanged from `main`.
   - `FrameHeader.flags` doc comment does not contain
     `arrow_compressed` or `has_trailing`. Construct
     forbidden literals via `concat!()` per the ADR 050 /
     051 / 052 architecture-test convention:
     `concat!("arrow_", "compressed")`,
     `concat!("has_", "trailing")`.
   - `FRAME_FLAG_KNOWN_MASK == FRAME_FLAG_STREAM_END`
     (positive guard).
   - `read_frame_header` references both `flags` and
     `FRAME_FLAG_KNOWN_MASK` in the same scope.
   - No new `MsgType` constant added with `_COMPRESSED_`
     infix. Construct forbidden needle via
     `concat!("MsgType::", "ARROW_COMPRESSED_")`.
   - No new `FRAME_FLAG_*` constant added for compression.
     Construct forbidden needle via
     `concat!("FRAME_FLAG_", "COMPRESS")`.
   - No `WireBodyCompressionPolicy` /
     `WireCompressionLevel` newtype.
   - No project-name prefix on new symbols.
   - No `RecordBodyCompressionPolicyV[0-9]` /
     `WireCompressionV[0-9]` suffix.
   - No new node capability bit. `NodeCapability::ALL.len()`
     unchanged from `main`.
   - **`ipc_compression` Cargo feature pinned on**
     (positive guard). The architecture test parses
     `Cargo.toml` and asserts `ipc_compression` is in the
     `arrow` feature list. ADR 043 §8 already requires
     this; ADR 053 elevates it to a mechanical guard.
   - No `Cargo.toml` change to feature flags or
     dependencies.
   - Path-label enumeration is bounded.

Behaviour fixtures under
`tests/fixtures/wire_arrow_ipc_body_compression/`:

- Wire-side fresh-encode round-trip: `RecordBatch` →
  `wire_ipc_write_options()` → `StreamWriter` → bytes →
  `StreamReader` → identical `RecordBatch`; emitted bytes
  carry `BodyCompression { codec = LZ4_FRAME }`.
- Storage pass-through round-trip: pre-compressed IPC
  bytes from `src/storage/ipc.rs::encode_record_batch` →
  splice into `arrow_payload` → receiver decode →
  identical `RecordBatch`. Verify the wire helper *did
  not* run on this path.
- Mixed pre-/post-deploy: a sender that encodes
  uncompressed (default options) frames a payload; a
  receiver post-ADR-053 decodes correctly. Reverse
  direction also passes.
- Migration RPC round-trip: `encode_migration_records`
  produces an IPC stream whose FlatBuffer carries
  `BodyCompression`; `decode_migration_records`
  round-trips identically.
- Client-write round-trip: `client::encode_batch` produces
  a compressed IPC; server-side decode of
  `TABLE_DELETE_REQ` reads the same row-key-id list.
- Descriptor schema IPC stays uncompressed (positive
  guard): `encode_arrow_schema_ipc(schema)` produces bytes
  whose `BodyCompression` is absent.
- Forwarding-path invariant: a node proxies an
  `arrow_payload` between two peers; bytes flow
  verbatim; no flag-stripping; no re-encode.
- `FRAME_FLAG_KNOWN_MASK` rejection: a frame with
  `flags = 0x0001` rejects; `flags = 0x0002` rejects;
  `flags = 0x0003` rejects; `flags = 0x0004`
  (`FRAME_FLAG_STREAM_END`) succeeds; `flags = 0x0005`
  (stream-end + bit 0) rejects.

Exit:

- Contract pinned; helper signature stable; fixtures fail
  with `unimplemented!()` until later phases.
- Architectural guards in place.

## Phase 1 — Wire-side encoder flips

Files:

- `src/network/ipc.rs`
- `src/network/data_server.rs`
- `src/client/core.rs`
- `tests/wire_arrow_ipc_body_compression_round_trip.rs` (new)

Work:

1. Implement `wire_ipc_write_options()` and
   `wire_encode_batch()` per Phase 0 §1.

2. Flip `src/client/core.rs::encode_batch` (direct
   `StreamWriter` site):

   ```rust
   fn encode_batch(batch: &RecordBatch) -> std::io::Result<Vec<u8>> {
       let options = crate::network::ipc::wire_ipc_write_options()?;
       let mut buf = Vec::new();
       {
           let mut writer = StreamWriter::try_new_with_options(
               &mut buf, &batch.schema(), options,
           )
           .map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow stream init: {e}")))?;
           writer.write(batch).map_err(...)?;
           writer.finish().map_err(...)?;
       }
       Ok(buf)
   }
   ```

3. Flip `src/network/data_server.rs::encode_migration_records`
   identically.

4. Flip the indirect call sites at
   `src/network/data_server.rs:3233` and
   `src/network/data_server.rs:4387` from
   `multirow::encode_batch` to `wire_encode_batch`:

   ```rust
   // line 3233 (TABLE_QUERY_RESP fresh response synthesis):
   let arrow_bytes = crate::network::ipc::wire_encode_batch(&concatenated)?;

   // line 4387 (READ_RESP fresh response synthesis):
   let arrow_payload = value_batch.map(|batch| {
       let bytes = crate::network::ipc::wire_encode_batch(&batch)?;
       let mut buf = self.pool.acquire();
       buf.as_mut_vec().extend_from_slice(&bytes);
       Ok::<_, std::io::Error>(buf)
   });
   ```

   Update the `data_server.rs` import block to reach into
   `network::ipc` rather than `storage::multirow` for these
   two call sites.

5. Run the expanded writer audit grep (Phase 0 §3) and
   confirm every match classifies. Any new `fresh-wire` site
   the survey missed flips at this phase. The grep covers
   direct `StreamWriter` instantiations *and* indirect shim
   helpers; confirm `multirow::encode_batch` has zero
   production wire-path callers after the flip.

6. Tests:

   - Migration RPC round-trip (sender encodes via wire
     helper; receiver via `decode_migration_records`):
     identical records, FlatBuffer carries
     `BodyCompression`.
   - Client-write round-trip via `TABLE_DELETE_REQ`:
     identical row_key_ids, FlatBuffer carries
     `BodyCompression`.
   - **TABLE_QUERY_RESP round-trip**: a fresh response
     synthesised at line 3233 carries
     `BodyCompression { codec = LZ4_FRAME }`; client decodes
     identical batch.
   - **READ_RESP round-trip**: a fresh value-batch response
     synthesised at line 4387 carries `BodyCompression`;
     client decodes identical batch.
   - Mixed pre-/post-deploy decode succeeds in both
     directions.
   - Storage pass-through: a `READ_RESP` whose
     `arrow_payload` came from a slab record (not from
     in-memory synthesis) does *not* re-encode; the wire
     helper is not on the path.
   - Descriptor schema IPC continues to produce
     uncompressed bytes; positive architectural guard.

Exit:

- Four wire-side fresh-encoder sites flipped. All wire-side
  fresh-encode Arrow IPC bytes carry
  `BodyCompression { codec = LZ4_FRAME, method = BUFFER }`.
  Receivers decode both shapes uniformly.
  `multirow::encode_batch` has only test-or-bench callers.

## Phase 2 — Receiver-side validation and pass-through

Files:

- `src/network/data_server.rs`
- `src/network/control_server.rs`
- `src/client/core.rs`
- `tests/wire_arrow_ipc_body_compression_receiver.rs` (new)

Work:

1. Verify every wire-side `StreamReader::try_new(..., None)`
   site already runs with the `ipc_compression` Cargo
   feature on. ADR 043 §8 enforces the build-profile
   assertion; ADR 053 verifies it has not regressed.

2. Add the `network_compression_decode_error_total{codec}`
   counter at every wire-side `StreamReader` consumer.
   Increment on `arrow::error::ArrowError` decode failures
   whose root cause is compression-related (heuristic:
   error message names the codec or LZ4 keyword). The
   error's diagnostic detail goes to logs / spans, not to
   metric labels — Arrow's error surface is unbounded and
   would blow up Prometheus cardinality. Steady-state
   expected count: zero.

3. Add a pass-through test where storage-owned IPC bytes
   travel over the network path and the outbound bytes
   are identical to the input bytes (no re-encode).

4. Add a descriptor schema IPC test proving its output
   does not carry Arrow IPC body compression.

5. Tests:

   - Receiver decodes a fresh-encoded LZ4 frame correctly
     across every wire-side reader: `data_server` query
     response decode, `control_server` schema fetch decode,
     client write ack decode, peer migration RPC decode.
   - Receiver decodes a pass-through-compressed `READ_RESP`
     correctly.
   - Receiver decodes a pre-ADR-053 uncompressed frame
     correctly.
   - Tampered LZ4 buffer fails decode with the
     `decode_error_total` counter incremented; the consumer
     receives an `InvalidData` error from Arrow.
   - Forwarding path: bytes flow verbatim; no flag
     manipulation; no re-encode.

Exit:

- Receivers handle both shapes; decode-error counter is
  wired and exercised; pass-through and forwarding
  invariants hold.

## Phase 3 — `FrameHeader.flags` cleanup and `FRAME_FLAG_KNOWN_MASK`

Files:

- `src/schema/core.rs`
- `src/network/core.rs`
- `tests/architecture_test.rs`
- `tests/wire_known_mask.rs` (new)

Work:

1. Add `FRAME_FLAG_KNOWN_MASK` constant beside
   `FRAME_FLAG_STREAM_END`:

   ```rust
   /// All currently-allocated flag bits. `read_frame_header`
   /// rejects frames whose flags include any bit outside
   /// this mask. New flag bits land alongside the ADR that
   /// introduces them and extend this mask in the same
   /// commit.
   pub const FRAME_FLAG_KNOWN_MASK: u16 = FRAME_FLAG_STREAM_END;
   ```

2. Update the doc comment on `FrameHeader.flags`:

   Before:
   ```rust
   /// Bitfield: bit0=arrow_compressed, bit1=has_trailing
   pub flags: u16,
   ```

   After:
   ```rust
   /// Bitfield. Allocated bits are listed in
   /// `FRAME_FLAG_KNOWN_MASK`; today only
   /// `FRAME_FLAG_STREAM_END` (bit 2) is allocated. Bits
   /// outside the known mask reject at framing-layer
   /// decode.
   pub flags: u16,
   ```

3. Wire the validation into
   `src/network/core.rs::read_frame_header`:

   ```rust
   if header.flags & !FRAME_FLAG_KNOWN_MASK != 0 {
       return Err(Error::new(
           ErrorKind::InvalidData,
           format!(
               "HRFT frame carries unknown flag bits 0x{:04x}",
               header.flags & !FRAME_FLAG_KNOWN_MASK,
           ),
       ));
   }
   ```

   Beside the existing `validate_frame_version` and
   `check_section_len` calls; identical error category.

4. Verify no code reads / writes bit 0 or bit 1:

   ```sh
   rg -n 'flags\s*&\s*1\s*[<<]?\s*0\b|flags\s*&\s*1\b|flags\s*&\s*2\b|flags\s*&\s*0x[01]\b' src tests
   ```

   Empty.

5. Tests:

   - Architecture test: `FrameHeader.flags` doc comment
     does not mention `arrow_compressed` or
     `has_trailing` (Phase 0 §8 guard).
   - Architecture test: `FRAME_FLAG_KNOWN_MASK ==
     FRAME_FLAG_STREAM_END` (positive guard).
   - Wire test: `flags = 0x0001` rejects;
     `flags = 0x0002` rejects; `flags = 0x0003` rejects;
     `flags = 0x0004` (`FRAME_FLAG_STREAM_END`) succeeds;
     `flags = 0x0005` (stream-end + bit 0) rejects.
   - Existing `FRAME_FLAG_STREAM_END` paths regress-test
     unchanged.

Exit:

- Dead reservations on `FrameHeader.flags` are removed.
- `FRAME_FLAG_KNOWN_MASK` validation hardens
  `read_frame_header`. The flags surface honestly
  documents only allocated bits, and the wire enforces
  the documentation.

## Phase 4 — Metrics, audit, runbook

Files:

- `src/metrics/registry.rs`
- `src/network/ipc.rs`
- `tests/metrics_smoke.rs`
- `plans/ops/observability.md`
- `plans/ops/runbooks/wire_arrow_ipc_body_compression.md` (new)
- `plans/ops/runbooks/README.md`

Work:

1. Implement the `network_compression_*` metric family per
   Phase 0 §5. Labels are bounded as defined; the
   architectural test in Phase 6 pins the path-label
   enumeration.

2. The wire encoder helper records, per fresh-encode:

   - `network_compression_batches_total{codec, path, result}`
     — counter; one per encoded batch with
     `result = compressed` (steady state),
     `result = error` on encode failure.
   - `network_compression_encoded_bytes_total{codec, path}`
     — post-LZ4 IPC body byte count.
   - `network_compression_encode_micros{codec, path}` —
     encode-time histogram.
   - `network_compression_ratio{codec, path}` — histogram;
     populated only when an opportunistic ratio sample is
     available without double-encoding (debug builds,
     bench-instrumented paths). Production typically sees a
     sparse histogram.

   No production
   `network_compression_plaintext_bytes_total` counter
   (Phase 0 §5 disposition). Bench fixtures explicitly
   produce plaintext-byte numbers at full precision.

3. Receivers record at every wire-side
   `StreamReader::try_new(..., None)` consumer in
   `src/network/data_server.rs`,
   `src/network/control_server.rs`, `src/client/core.rs`:

   - `network_compression_decode_error_total{codec}` —
     counter; expected zero in steady state. Diagnostic
     error context lives in logs / spans, not metric
     labels.

   No decode-latency histogram in V1 (Phase 0 §5).

   The helper detects codec by reading the IPC FlatBuffer
   metadata (reuse
   `record_batch_compression_policy_from_ipc` from
   `src/storage/ipc.rs`).

4. **No per-frame audit event.** Audit events would be
   per-RPC and immediately overwhelm the audit chain. A
   single deploy-time audit event marking the ADR 053
   activation is enough.

5. Runbook
   (`plans/ops/runbooks/wire_arrow_ipc_body_compression.md`):

   - Codec is `lz4_frame`; pinned per build.
   - When LZ4 helps vs hurts (small frame edge case;
     high-entropy payloads; Arrow's `-1` sentinel handles
     these automatically).
   - Storage pass-through vs fresh-encode boundary: the
     `network_compression_*` family counts only
     fresh-encode bytes (the helper invocations).
     Pass-through bytes are accounted for on the storage
     axis under `storage_compression_*` (their original
     seal / read encode), and surface again here only
     through the receiver-side decode-error counter if a
     decompression problem occurs. To estimate the wire-
     side mix between pass-through and fresh-encode bytes,
     operators correlate fresh-encode counts (this family,
     by `path` label) against the existing read /
     query / replicate RPC counters; this ADR does not add
     a single metric that splits the two on the wire.
   - Decode-error response: typically wire corruption or
     mid-flight truncation; existing TLS / QUIC checksums
     should already catch the underlying issue.
   - Rolling-deploy semantics: no protocol-version bump,
     no negotiation, no special order. A node deployed
     post-ADR-053 ships compressed wire-side fresh
     encodes; pre-ADR-053 nodes still decode them via the
     on-by-default `ipc_compression` Cargo feature.
   - How to interpret `FRAME_FLAG_KNOWN_MASK` rejections
     on `FrameHeader.flags`.
   - Composition with ADR 043 storage compression
     (storage pass-through is double-counted by neither
     metric family — `storage_compression_*` covers the
     storage seal / read; `network_compression_*` covers
     wire fresh-encode).
   - Identity-vs-Locality framing on the wire axis.

6. Tests:

   - Metrics smoke: every `network_compression_*` metric
     has a registry entry with the documented labels.
   - One round-trip increments the expected counters and
     histograms.
   - Path-label enumeration is pinned to `client_batch`
     and `migration_records` in V1.
   - No high-cardinality label can be populated from
     table, tenant, peer, or schema values.

Exit:

- Operators can observe wire-side compression cost; the
  runbook documents codec, decision tree, and
  decode-error response.

## Phase 5 — Bench

Files:

- `benches/wire_arrow_ipc_body_compression.rs` (new)
- `Cargo.toml` (`[[bench]]` entry)
- `plans/impl/053_wire_arrow_ipc_body_compression_bench_results.md`
  (new, populated when the bench runs)

Bench matrix:

1. **Wire-side encode CPU** at small-edge (1 KiB), small
   (4 KiB), medium (1 MiB), and large (10 MiB) Arrow
   batches with low / high cardinality. The 1 KiB / 4 KiB
   pair brackets the LZ4 frame-overhead crossover; Arrow's
   per-buffer `-1` length-sentinel handles the
   pathological case automatically, but the bench confirms
   the empirical CPU cost. Compare default options vs
   `wire_ipc_write_options()`.

2. **Wire-side decode CPU** at the same payload sizes.
   Confirm decompress speed dominates encode speed
   (≈3–5 GB/s/core decompress vs ≈500 MB/s/core encode,
   per ADR 043).

3. **Payload size ratio** at the same payload sizes.
   Confirm 1.5× to 3× density on representative analytical
   batches; flag any regression on small high-entropy
   frames.

4. **End-to-end RPC latency**:

   - `TABLE_DELETE_REQ` (small payload).
   - `TABLE_QUERY_RESP` synthesised in-memory (medium
     payload).
   - `MIGRATE_RESP` (medium-large payload).
   - Storage pass-through `READ_RESP` (medium-large
     payload — should show no encode CPU because
     pass-through bypasses the helper).

5. **Crossover analysis**: at what payload size does
   per-frame compression overhead exceed wire-bandwidth
   savings? V1 pins the codec; the bench answers whether
   the small-frame edge case is operationally relevant.

6. **Workload mixes**: narrow integer batches; wide
   string batches; high-cardinality strings; high-entropy
   binary; mixed nullability batches.

7. **Bench partitioning** (mandatory): isolate **encode
   cost** from **wire transmission cost** from **decode
   cost** so a slow path has a named source.

Acceptance starting points:

- Decode CPU is at most ~10–20% of encode CPU on
  representative payloads.
- Wire bandwidth on analytical-shaped Arrow batches
  shrinks to ≤ 60% of uncompressed, matching ADR 043's
  storage-side density.
- Small-frame (< 4 KiB) end-to-end latency does not
  regress by more than 5% under the pinned codec.
- Storage pass-through paths show zero encode CPU under
  the wire helper (positive guard that pass-through
  skips the helper).
- Per-frame compression overhead is below an
  operator-readable threshold (e.g., < 50 µs per typical
  RPC).

Bench results in
`plans/impl/053_wire_arrow_ipc_body_compression_bench_results.md`.
Quick smoke acceptable for code review; full
small-edge / small / medium / large matrix with
end-to-end latency is the ADR 053 promotion-to-shipped
gate.

## Phase 6 — Architectural guards, lockdown

Files:

- `tests/architecture_test.rs`

**Convention.** Forbidden-literal checks construct needles
via `concat!(...)` per the ADR 050 / 051 / 052 architecture-
test convention.

Guards to add:

- ☐ `wire_ipc_write_options` exists in `src/network/ipc.rs`
  and is the only wire-side `IpcWriteOptions` constructor
  used by `StreamWriter::try_new_with_options(...)` outside
  the storage layer.
- ☐ Every wire-side `StreamWriter::try_new(...)` site
  uses the helper, except for the documented exclusion
  list (which today contains exactly
  `encode_arrow_schema_ipc`).
- ☐ Storage-pass-through paths (`READ_RESP` / `QUERY_RESP`
  / `BULK_WRITE_RESP` etc. that splice storage IPC bytes
  into `arrow_payload`) do not invoke the wire helper.
- ☐ `WIRE_BODY_COMPRESSION ==
  RecordBodyCompressionPolicy::Lz4Frame` (positive guard).
- ☐ `HRFT_ENVELOPE_VERSION` unchanged from `main`.
- ☐ No new `MsgType` constant. Construct forbidden needle
  via `concat!("MsgType::", "ARROW_COMPRESSED_")`.
- ☐ No new `FRAME_FLAG_*` constant for compression.
  Construct forbidden needle via
  `concat!("FRAME_FLAG_", "COMPRESS")`.
- ☐ `FrameHeader.flags` doc comment does not mention
  `arrow_compressed` or `has_trailing`.
- ☐ `FRAME_FLAG_KNOWN_MASK == FRAME_FLAG_STREAM_END`
  (positive guard).
- ☐ `read_frame_header` references both `flags` and
  `FRAME_FLAG_KNOWN_MASK` in the same scope.
- ☐ No new node capability bit. `NodeCapability::ALL.len()`
  unchanged from `main`.
- ☐ No `WireBodyCompressionPolicy` /
  `WireCompressionLevel` newtype.
- ☐ No project-name prefix on new symbols.
- ☐ No `RecordBodyCompressionPolicyV[0-9]` /
  `WireCompression[A-Z][a-z]+V[0-9]` suffix.
- ☐ Receiver path always uses
  `StreamReader::try_new(..., None)` with the
  `ipc_compression` feature on. Positive guard:
  `Cargo.toml` has `arrow = { ..., features = [..,
  "ipc_compression"] }` and the architecture test reads
  the feature list.
- ☐ Compress-then-encrypt order preserved (call-graph
  topology-enforced).
- ☐ Build-profile assertion from ADR 043 §8 still
  enforces `ipc_compression` on (positive guard, no
  regression).
- ☐ Path-label enumeration is bounded. The architectural
  test enumerates the allowed `path` label values
  (`client_batch`, `migration_records`,
  `query_response`, `read_response` in V1) and asserts
  every metric registration with a `path` label uses one
  of those values.
- ☐ `result` label on `network_compression_batches_total`
  is bounded to `{compressed, error}`. Construct
  forbidden needle via
  `concat!("uncompressed_", "passthrough")` and assert it
  does not appear in `src/`.
- ☐ `network_compression_decode_error_total` carries
  `{codec}` only — no `reason` label. Construct forbidden
  needle via
  `concat!("decode_error_total{", "codec, reason}")` and
  assert it does not appear in `src/`.
- ☐ `multirow::encode_batch` has zero production
  wire-path callers (positive guard: every reference
  outside `tests/` either inlines through
  `wire_encode_batch` or has been removed).
- ☐ No `network_compression_plaintext_bytes_total` metric
  exists in production code.
- ☐ No `network_compression_decode_micros` histogram in
  V1.
- ☐ Writer audit taxonomy: every Arrow IPC `StreamWriter`
  site in production code is classified into exactly one
  of `fresh-wire`, `descriptor-schema`, `storage`,
  `test-or-bench`. The architectural test reads the
  classification list and cross-checks against the grep
  audit.

Exit:

- Architecture is mechanically protected; future wire-side
  encoders cannot drift back to default options without
  explicit exclusion.

## Verification gate

Required before declaring ADR 053 shipped:

```sh
cargo test --lib network::ipc
cargo test --lib client::core
cargo test wire_arrow_ipc_body_compression --quiet
cargo test wire_arrow_ipc_body_compression_round_trip --quiet
cargo test wire_arrow_ipc_body_compression_receiver --quiet
cargo test wire_known_mask --quiet
cargo test --test architecture_test --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench wire_arrow_ipc_body_compression --no-run
cargo bench --bench wire_arrow_ipc_body_compression -- --quick
git diff --check
cargo fmt --check
```

Grep guards:

- Wire-side `StreamWriter::try_new` (default options) sites
  appear only in the documented exclusion list:

  ```sh
  rg -n 'StreamWriter::try_new\b' src
  ```

  Output: only `src/schema/table.rs::encode_arrow_schema_ipc`
  and any storage-internal sites the architectural test has
  already classified. No `src/network/`, no `src/client/`
  output.

- Wire-side `StreamWriter::try_new_with_options` sites
  route through the wire helper:

  ```sh
  rg -n 'StreamWriter::try_new_with_options' src
  ```

  Output: storage path callers (already covered by ADR
  043), `src/client/core.rs::encode_batch`,
  `src/network/data_server.rs::encode_migration_records`,
  any future wire-side fresh-encoder.

- Helper exists:

  ```sh
  rg -n 'fn wire_ipc_write_options' src
  ```

  Present in `src/network/ipc.rs`.

- Constant matches:

  ```sh
  rg -n 'WIRE_BODY_COMPRESSION' src
  ```

  Present; equals `RecordBodyCompressionPolicy::Lz4Frame`.

- No new HRFT envelope `format_version` bump:

  ```sh
  rg -n 'HRFT_ENVELOPE_VERSION' src
  ```

  Value unchanged from `main`.

- No new node capability bit:

  ```sh
  rg -n 'enum NodeCapability' src
  ```

  Variant count unchanged from `main`.

- Bit-0 / bit-1 doc reservations removed:

  ```sh
  rg -n 'arrow_compressed|has_trailing' src tests
  ```

  Empty.

- `FRAME_FLAG_KNOWN_MASK` exists and is referenced by
  `read_frame_header`:

  ```sh
  rg -n 'FRAME_FLAG_KNOWN_MASK' src tests
  ```

  Present in both schema constants and framing read path.

- `ipc_compression` Cargo feature pinned on:

  ```sh
  rg -n 'ipc_compression' Cargo.toml
  ```

  Present in `arrow` feature list.

- No `MsgType::ARROW_COMPRESSED_*` or
  `FRAME_FLAG_COMPRESS*`:

  ```sh
  rg -n 'MsgType::ARROW_COMPRESSED|FRAME_FLAG_COMPRESS' src tests
  ```

  Empty.

- No `WireBodyCompressionPolicy` or
  `WireCompressionLevel` types:

  ```sh
  rg -n 'WireBodyCompressionPolicy|WireCompressionLevel' src tests
  ```

  Empty.

- No project-name prefix on new wire-compression symbols:

  ```sh
  rg -n '\bWendelonWireCompression|\bwendelon_wire_compression_' src tests
  ```

  Empty.

- No version suffixes on ADR 053 Rust types:

  ```sh
  rg -n 'WireCompression[A-Za-z]*V[0-9]|RecordBodyCompressionPolicyV[0-9]' src tests
  ```

  Empty.

- Receiver decoders always pass `None` for `projection`:

  ```sh
  rg -n 'StreamReader::try_new\b' src
  ```

  Every site passes `None` (or, in deliberate exceptions,
  is documented).

- Storage axis metric prefix and wire axis metric prefix
  are distinct:

  ```sh
  rg -n 'storage_compression_|network_compression_' src tests
  ```

  Both prefixes present and exercised.

- Path label enumeration is bounded:

  ```sh
  rg -n 'path\s*=\s*"[a-z_]+"' src/network/ipc.rs src/network/data_server.rs src/client/core.rs
  ```

  Output: only `client_batch`, `migration_records`,
  `query_response`, and `read_response` in V1.

- Result label on `batches_total` is bounded:

  ```sh
  rg -n 'uncompressed_passthrough' src
  ```

  Empty.

- `decode_error_total` carries `{codec}` only:

  ```sh
  rg -n 'decode_error_total\{codec, reason' src tests
  ```

  Empty. The metric registration uses `{codec}` alone.

- `multirow::encode_batch` has no production wire-path
  callers:

  ```sh
  rg -n 'multirow::encode_batch\b' src
  ```

  Empty in `src/`. Remaining callers are in `tests/`.

- No `network_compression_plaintext_bytes_total` in
  production:

  ```sh
  rg -n 'network_compression_plaintext_bytes_total' src
  ```

  Empty in `src/`.

- No `network_compression_decode_micros` histogram in
  V1:

  ```sh
  rg -n 'network_compression_decode_micros' src
  ```

  Empty.

Convention guards:

- No `#[serde(default)]` shim on new types (there are no
  new types).
- No reserved enum slots.
- No envelope `format_version` bump.
- No new HRFT frame family.
- No new node capability bit.
- No `ObjectStore` trait break.
- No on-disk shape change (storage axis untouched).
- Codec disposition (Phase 0 §1), exclusion list (Phase 0
  §4), emission rule (Phase 0 §2), known-mask (Phase 0
  §6), and metric labels (Phase 0 §5) are settled in code.

## File-by-file expected edits

`src/network/ipc.rs` (new)
- `WIRE_BODY_COMPRESSION` constant.
- `wire_ipc_write_options()` helper.
- Documentation listing the exclusion list with rationale
  and citing ADR 053 §2.

`src/network/mod.rs`
- Re-export the new module.

`src/network/data_server.rs`
- `encode_migration_records` flips to
  `wire_ipc_write_options()` +
  `StreamWriter::try_new_with_options`.
- TABLE_QUERY_RESP fresh response synthesis at line 3233:
  swap `multirow::encode_batch(&concatenated)` for
  `wire_encode_batch(&concatenated)` (import path
  updated).
- READ_RESP fresh response synthesis at line 4387: swap
  `multirow::encode_batch(&batch)` for
  `wire_encode_batch(&batch)` (import path updated).

`src/client/core.rs`
- `encode_batch` flips to `wire_ipc_write_options()` +
  `StreamWriter::try_new_with_options`.

`src/schema/core.rs`
- `FrameHeader.flags` doc comment cleaned up.
- `FRAME_FLAG_KNOWN_MASK` constant added (initial value:
  `FRAME_FLAG_STREAM_END` only).

`src/network/core.rs`
- `read_frame_header` adds the
  `flags & !FRAME_FLAG_KNOWN_MASK` validation alongside
  the existing magic / version / section-length checks.

`src/storage/ipc.rs`
- `ipc_write_options_for_policy` and
  `encode_record_batch_bytes` are callable from
  `src/network/ipc.rs`. No code change expected; only
  visibility audit (`pub` already correct per ADR 043
  helper export).

`src/storage/multirow.rs`
- `encode_batch` (the hardcoded-`Uncompressed` shim at
  line 715) loses its production wire-path callers. Two
  options for the implementation phase: (a) leave the
  function in place classified as `test-or-bench` (only
  test callers remain); (b) inline its body at the test
  call sites and delete the function. Either is
  acceptable; the architectural test pins zero
  production wire-path callers. The `_with_policy`
  siblings on storage encoders
  (`encode_multirow_from_survivors_with_policy`,
  `pack_multirow_source_rows_with_policy`) are
  unchanged — they belong to the `storage` taxonomy
  category governed by ADR 043.

`src/metrics/registry.rs`
- New `network_compression_*` metric family with bounded
  `{codec, path}` labels;
  `result ∈ {compressed, error}` on `batches_total`;
  `decode_error_total` carries `{codec}` only.

`tests/architecture_test.rs`
- ADR 053 guards.

`tests/wire_arrow_ipc_body_compression_round_trip.rs` (new)
- Encoder fixtures.

`tests/wire_arrow_ipc_body_compression_receiver.rs` (new)
- Decoder fixtures.

`tests/wire_known_mask.rs` (new)
- Known-mask validation fixtures.

`benches/wire_arrow_ipc_body_compression.rs` (new)
- Bench harness.

`Cargo.toml`
- `[[bench]]` entry for `wire_arrow_ipc_body_compression`.
  No feature-flag changes; no dependency additions.

`plans/ops/observability.md`
- Document new metrics.

`plans/ops/runbooks/wire_arrow_ipc_body_compression.md`
(new)
- Runbook.

`plans/ops/runbooks/README.md`
- Add wire-compression runbook to the index.

## V1 design summary

Engine surface decisions (within Wendelon's scope):

- Wire-side fresh-encode Arrow IPC body buffers compress
  per-buffer with LZ4_FRAME; the codec is pinned by
  `WIRE_BODY_COMPRESSION = RecordBodyCompressionPolicy::Lz4Frame`.
- One helper site
  (`src/network/ipc.rs::wire_ipc_write_options`) produces
  `IpcWriteOptions` for every wire-side fresh-encoder; the
  storage helper `ipc_write_options_for_policy` is reused.
- Four wire-side fresh-encoder call sites flip in V1:
  two direct `StreamWriter` sites
  (`src/client/core.rs::encode_batch`,
  `src/network/data_server.rs::encode_migration_records`)
  flip to `wire_ipc_write_options`; two indirect sites that
  currently call `multirow::encode_batch`
  (`src/network/data_server.rs:3233` for `TABLE_QUERY_RESP`
  and `src/network/data_server.rs:4387` for `READ_RESP`)
  flip to `wire_encode_batch`. One site is explicitly
  excluded (`src/schema/table.rs::encode_arrow_schema_ipc` —
  boot-time independence per ADR 043 §5). After flip,
  `multirow::encode_batch` retains zero production wire-path
  callers; an architectural guard pins this.
- Storage pass-through paths bypass the wire helper;
  pre-compressed IPC bytes flow verbatim.
- Receivers decode both shapes via
  `StreamReader::try_new(..., None)` with
  `ipc_compression` Cargo feature on (architectural guard
  pins the feature).
- No HRFT envelope `format_version` bump, no new HRFT
  frame family, no new `MsgType` / `FRAME_FLAG_*`
  constant, no new node capability bit, no new descriptor
  field, no Cargo feature or dependency change.
- `FrameHeader.flags` bit-0 (`arrow_compressed`) and
  bit-1 (`has_trailing`) doc comments are removed; bits
  themselves are unchanged at the byte layout.
- `FRAME_FLAG_KNOWN_MASK` introduced;
  `read_frame_header` rejects frames with unknown flag
  bits set (`ErrorKind::InvalidData`).
- `network_compression_*` metrics with bounded
  `{codec, path}` labels.
  `codec ∈ {uncompressed, lz4_frame}`;
  `path ∈ {client_batch, migration_records,
  query_response, read_response}` initially, pinned by
  architectural test.
  `batches_total` carries `result ∈ {compressed, error}`.
  `decode_error_total` carries `{codec}` only —
  diagnostic detail goes to logs, not labels (Arrow's
  error surface is unbounded). Pass-through bytes do not
  flow through the helper and are *not* counted by this
  family. No production plaintext-byte counter (avoids
  ADR 043 §3 double-encode cost where there is no
  enforcement axis to amortise it). Decode-latency
  histogram deferred to a follow-up ADR.
- Compress-then-encrypt order preserved by construction.
- Rolling-deploy is uneventful: receivers decompress
  when present, pass through when absent; no
  negotiation, no protocol-version bump.
- No per-frame, per-tenant, per-table, or per-RPC
  policy variation; no cluster kill-switch in V1.
- CRIME-class concerns documented and dismissed at the
  V1 threat model; the meta / trailing exclusion is
  framed as side-channel-context-disjointness, not just
  payload-density.
- `MAX_SECTION_BYTES = 5 MiB` cap unchanged; applied on
  encoded (wire) bytes. Decoded-payload bound owned by
  Arrow `StreamReader` (predates this ADR).

Out of Wendelon's scope (client-tool / SDK ADR territory):
multi-language SDK compression contract (a future ADR
when non-Rust SDKs ship); cluster-wide compression
operator surface (an amendment if benchmarks or
operations surface a need).

## Deferred to follow-up ADRs

Mirrors ADR 053 *Deferred* section:

1. zstd codec for the wire axis (cold-CDC replay,
   capacity-tier streaming).
2. Per-stream / per-RPC compression policy negotiation.
3. Cluster-level wire-compression kill-switch.
4. External multi-language SDK compression contract.
5. Compression of HRFT meta or trailing sections.
6. Per-buffer page directories on the wire (Brainstorm
   011 V2).
7. Column-projection on the wire (composes with per-
   buffer LZ4 substrate).
8. CRIME-class side-channel mitigation (length padding,
   randomised dispersal).
9. `FrameHeader.flags` bit reorganisation if a future
   ADR needs new flag bits and the freed bits 0–1 become
   useful.
10. Compute-over-compressed-data on wire-flowing batches.
11. Wire-side decode-latency histogram if operationally
    pulled.
12. Live plaintext-byte / ratio accounting in production
    (lift the V1 omission via the same metric names,
    switching only the population call-site).
