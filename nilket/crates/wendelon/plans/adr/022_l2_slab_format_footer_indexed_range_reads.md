# ADR 022: L2 Slab Format and Footer-Indexed Range Reads

## Status
Proposed

## Context
ADR 006 committed Wendelon to an L0 → L1 → L2 tiered hierarchy with append-only slabs. It did not specify the slab's on-disk/on-object layout, the record-addressing strategy for range reads, or the `ObjectStore` trait shape that the L2 code path consumes. Without those, the L2 read path either downloads a whole slab to serve a 128-byte point read (operationally broken), or it leans on sidecar manifests or provider-side metadata (consistency puzzles on S3-class stores). The layout must also compose with ADR 020's per-slab DEK and 64 KiB chunked AEAD, because the slab is simultaneously the unit of encryption and the unit of object-store placement — which means the slab's trailer carries the crypto metadata that ADR 020 §6 already references but did not freeze.

Forces in play:
- **One slab = one object** is the simplest durable model; sidecar manifests and multi-object slab splits multiply consistency surface and invalidate list-join orphan-GC (ADR 024).
- **Range-GET cost dominates tail latency on L2.** A cold read needs at most two round trips (one to locate the record, one to fetch it) and must never download the whole slab.
- **ADR 020's per-slab DEK scope is load-bearing.** The slab's footer is where the wrapped DEK lives; freezing the footer freezes ADR 020's §6 encryption header.
- **ADR 014** already names `rustfs` as the test substrate for the object-store path. The new trait shape must be the canonical surface that `rustfs` implements, not a parallel interface.
- **Breaking the `ObjectStore` trait repeatedly** is how call sites acquire migration scar tissue. The trait is small and its consumers are few; one coordinated break lands the full capability surface at once.

Derived from `plans/gap/claude_answers_20260417.md` §4.

## Decision

1. **One slab, one object, one addressable byte layout.** A slab is a single, contiguous, content-addressed byte sequence persisted identically on L1 (NVMe) and on L2 (object store). No sidecar manifests, no provider-metadata indexes, no multi-object splits. L1 writes the slab; the L2 uploader streams the same bytes as one object; the L2 reader reads back the same bytes via range-GETs.

2. **Frozen slab layout.** The slab is a byte sequence in the following order. All multi-byte integers are big-endian; all sizes in bytes:

   ```
   offset                                   size    field
   ---------------------------------------  ------  --------------------------------------------------
   0                                         8      magic_head             = b"WENDSLAB"
   8                                         SH     slab_header            (fixed-size, see §3)
   8 + SH                                    P      encrypted_chunk_stream (m × ADR 020 64 KiB chunk)
   8 + SH + P                                I      offset_index           (variable-length, see §4)
   8 + SH + P + I                            SF     slab_footer            (fixed-size, see §5)
   8 + SH + P + I + SF                       4      footer_length          = SF (u32 big-endian)
   8 + SH + P + I + SF + 4                   4      index_length           = I  (u32 big-endian)
   8 + SH + P + I + SF + 8                   8      magic_tail             = b"BALSDNEW"
   ```

   `P` is already 4 KiB-aligned by ADR 020 §5 (encryption above `AlignedBuf`, padded to 4 KiB). `SH` (64 bytes) and `SF` (256 bytes) are fixed; the 16-byte tail block (`footer_length || index_length || magic_tail`) is the fixed anchor a reader fetches first. The offset-index and the slab-footer are **not** encrypted under the slab DEK — they carry the authenticators and identifiers that a reader needs *before* any chunk is decrypted. Their integrity, together with the integrity of every other non-ciphertext byte, is protected by an HMAC in the footer (§5) keyed by the Tenant KEK (not the slab DEK, which would be circular since the slab DEK itself is carried wrapped inside the footer). The encrypted chunks are separately authenticated by per-chunk AEAD (ADR 020 §2).

3. **`slab_header` (SH = 64 bytes, plaintext, fixed-layout).** Readable before decrypt. Carries identifiers that are redundant with the footer so that a partial slab can still be classified during forensics:

   ```
   offset  size  field
   ------  ----  --------------------------------------------------
     0       4   slab_format_version    (u32; starts at 1)
     4       1   cipher_id              (u8; matches ADR 020 §2 AAD cipher_id)
     5       3   reserved               (must be 0x000000)
     8      16   slab_id                (UUID, network byte order)
    24      16   tenant_id              (UUID, network byte order; matches ADR 020 AAD)
    40       8   created_hlc            (HybridTimestamp, u64 big-endian)
    48       4   chunk_size_bytes       (u32; ADR 020 pins this at 65536 today)
    52       4   chunk_count            (u32; number of encrypted chunks in the stream)
    56       8   magic_head_crc         (CRC32C of the first 56 bytes, for fast triage)
   ------  ----
   total   64
   ```

   The fields here also appear in the footer (§5). Duplication is deliberate: the header is cheap-to-read (part of the first multipart part, first byte of any prefix-GET) and lets L1 file-scan tooling classify a slab without seeking to the tail.

4. **`offset_index` (I = variable, plaintext, fixed-layout entries).** A densely packed array of record-locator entries, ordered by `record_ordinal`. Each entry is fixed-width so the index is navigable by ordinal arithmetic; the variable cost is `record_count × 32 B`. One entry per logical record:

   ```
   offset  size  field
   ------  ----  --------------------------------------------------
     0      16   key_hash               (blake3(key)[0..16], NOT the unified ring hash)
    16       8   byte_offset            (u64; byte offset into the plaintext chunk stream, pre-encryption)
    24       4   byte_length            (u32; plaintext byte length of the record)
    28       4   flags                  (u32; bit 0: tombstone, bit 1: compressed, bits 2-31 reserved)
   ------  ----
   total   32
   ```

   **The offset is into the *plaintext* stream, not the ciphertext.** A reader computes the chunk range needed as `[byte_offset / chunk_size, (byte_offset + byte_length - 1) / chunk_size]`, issues a range-GET for those chunks' ciphertext (each chunk is `chunk_size + 16 B tag` in the ciphertext stream), decrypts them with the slab DEK (ADR 020 §2), and slices the plaintext. A record may span up to two chunks in the common case; a very large record may span more, and the reader's range-GET size scales accordingly. The index is **not** a hash index in the unified-ring sense — it is a blake3-derived locator used only for intra-slab lookup. Collisions inside a slab are resolved by linear scan across the matching entries (the reader already has the chunks decrypted).

5. **`slab_footer` (SF = 256 bytes, plaintext, fixed-layout).** The footer is the single authoritative metadata record. It duplicates the header for integrity-check redundancy and carries the ADR 020 §6 encryption metadata bit-exactly:

   ```
   offset  size  field
   ------  ----  --------------------------------------------------
     0       4   slab_format_version    (u32; must equal header field)
     4       1   cipher_id              (u8; must equal header field)
     5       3   reserved               (must be 0x000000)
     8      16   slab_id                (UUID; must equal header field)
    24      16   tenant_id              (UUID; must equal header field)
    40       8   created_hlc            (u64 BE; must equal header field)
    48       4   chunk_size_bytes       (u32; must equal header field)
    52       4   chunk_count            (u32; must equal header field)
    56       8   record_count           (u64 BE)
    64      32   tenant_kek_id          (opaque bytes; right-padded with 0x00 if shorter)
    96      96   wrapped_slab_dek       (opaque bytes; right-padded with 0x00 if shorter)
   192      32   footer_hmac_sha256     (HMAC-SHA-256; coverage defined below)
   224      32   reserved               (must be 0x00 × 32)
   ------  ----
   total  256
   ```

   **HMAC coverage (authenticates the full non-ciphertext surface of the slab).** The HMAC input is the byte string formed by concatenating, in order:
   - `magic_head` (8 B)
   - `slab_header` (64 B)
   - `offset_index` (I B, the full index as written)
   - `slab_footer[0..192]` (the 192 bytes of the footer that precede the HMAC field itself)
   - `slab_footer[224..256]` (the 32 trailing reserved bytes)
   - `footer_length` (4 B)
   - `index_length` (4 B)
   - `magic_tail` (8 B)

   The key is the Tenant KEK identified by `tenant_kek_id`. The HMAC intentionally **omits** the encrypted ciphertext chunks (separately authenticated by per-chunk AEAD with AAD binding `tenant_id || slab_id || chunk_index` per ADR 020 §2, so chunk-level tampering is caught without the slab-level HMAC having to re-authenticate them) and the HMAC field itself (excluded to avoid circularity). Everything else — including the offset index, the tail anchor, and the `footer_length` / `index_length` fields that tell the reader where to find the footer — is under the HMAC, so:

   - A tampered offset-index entry (the attack mode where an attacker redirects a record lookup to a different byte range within the same slab) fails HMAC verification before the reader performs any chunk fetch.
   - Tampered `footer_length` or `index_length` fields that would otherwise redirect the reader to parse alternative bytes as the footer are caught by HMAC verification, because verification fails when the recomputed input does not match.
   - A tampered `slab_header` field is caught by HMAC verification; separately, readers MUST cross-check the fields duplicated between header and footer and reject on mismatch (defense in depth).

   **HMAC verification on read.** The reader fetches the tail window (which covers the footer, the tail anchor, and — for common-case slabs — the offset index), optionally fetches the offset index separately if it falls outside the tail window, reads `tenant_kek_id` from the footer, resolves the Tenant KEK via ADR 020 §3 (the Tenant KEK is selected by `tenant_kek_id`, not by the slab's claimed `tenant_id` — the reader's request-context `tenant_id` must match; see ADR 021 §2 for the dispatcher-level cert-vs-target pairing), then recomputes the HMAC over the byte ranges listed above and compares to `footer_hmac_sha256` in constant time. Verification precedes DEK unwrap. The `slab_header` bytes are available to the reader either from a multipart-part zero fetch (when the slab was written via multipart and part 1 covers the header) or from a prefix range-GET; in the common steady-state read path, the reader keeps a header cache keyed by `slab_id` and avoids the extra fetch. A reader that has not cached the header may skip verifying the header bytes against the HMAC *only* when the header's duplicated fields have been cross-checked against the footer's — in which case the header's influence on the read is neutralized (readers do not make read-resolution decisions from the header; it is forensic).

   `wrapped_slab_dek` is sized to 96 bytes to accommodate AES-GCM-wrapped 256-bit DEKs plus nonce/tag overhead plus future growth; shorter wraps right-pad with zeros and the length is implicit in `cipher_id`'s wrap scheme. `tenant_kek_id` identifies which Tenant KEK version was used (addresses rotation); padding likewise.

6. **Tail-first read protocol.** The L2 reader fetches records in a bounded two-RTT protocol:

   - **RTT 1.** `get_range(slab, start = size - 64 KiB, end = size)`. 64 KiB is the common provider minimum-range-size and covers the 16-byte tail anchor, the 256-byte footer, and most offset indexes (for slabs with ≤ 2032 records; a 64 MiB slab at 16 KiB mean record size has exactly 4096 records and an offset index of 128 KiB). The reader parses `magic_tail`, reads `footer_length` and `index_length` from the tail anchor, and reconstructs the byte range of the footer and offset index. If the tail window already includes both (the common case for small/medium slabs), no second metadata fetch is needed. If the offset index falls before the 64 KiB window, a second range-GET fetches exactly `[size - 16 - SF - I, size - 16 - SF]`.
   - **RTT 2.** With `byte_offset` and `byte_length` from the offset index, the reader issues `get_range` for the ciphertext chunks that cover the plaintext range, decrypts them per ADR 020 §2, and slices out the record.

   Metadata-only reads (listings, count-only queries) terminate after RTT 1. Point reads complete in two RTTs for cold reads and one RTT amortized under the chunk/record cache.

7. **Writer protocol.** The slab writer emits bytes in the order of §2 — `magic_head`, then header (held in RAM with chunk_count = 0 initially, patched once all chunks are written), then encrypted chunks as ADR 020 §2 produces them, then the offset index (built in RAM as records are appended; this is bounded because `slab_max_bytes ≤ 2^35 B` from ADR 020 §2 limits record cardinality), then the footer (HMAC computed once all preceding fields are fixed), then the 16-byte tail anchor. The header's `chunk_count` is patched in the in-RAM buffer before the slab is flushed; L1 writes the entire slab in one io_uring issue sequence to preserve O_DIRECT alignment. For L2, the same byte stream is uploaded via ADR 023's lifecycle.

8. **Canonical `ObjectStore` trait (single coordinated break).** The new trait replaces the existing shape. Implementations: production S3, GCS, Azure Blob adapters; the `rustfs` test backend from ADR 014; the mocked `ObjectStore` from ADR 028. All call sites migrate in the same change. The trait is sized to cover every operation Wave 4 ADRs consume — object I/O (ADR 022 §6), multipart upload (ADR 023 §3), multipart recovery (ADR 023 §4), and orphan reclaim (ADR 024 Tracks A and D):

   ```rust
   #[async_trait]
   pub trait ObjectStore: Send + Sync + 'static {
       // --- Object I/O ---
       async fn head(&self, key: &ObjectKey) -> Result<ObjectMetadata, ObjectStoreError>;
       async fn get(&self, key: &ObjectKey) -> Result<BytesStream, ObjectStoreError>;
       async fn get_range(
           &self,
           key: &ObjectKey,
           range: Range<u64>, // half-open [start, end)
       ) -> Result<BytesStream, ObjectStoreError>;
       async fn put(
           &self,
           key: &ObjectKey,
           body: BytesStream,
           expected: PutCondition, // IfNotExists | IfMatch(etag) | Unconditional
       ) -> Result<ObjectMetadata, ObjectStoreError>;
       async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectStoreError>;
       async fn copy(
           &self,
           src: &ObjectKey,
           dst: &ObjectKey,
           expected: PutCondition,
       ) -> Result<ObjectMetadata, ObjectStoreError>;

       // --- Object listing (paged) ---
       async fn list(
           &self,
           prefix: &ObjectPrefix,
           continuation: Option<ContinuationToken>,
       ) -> Result<ListPage, ObjectStoreError>;

       // --- Multipart: create, resume, list, abort ---
       // New multipart upload. Returns a handle whose upload_id is provider-assigned.
       async fn put_multipart(
           &self,
           key: &ObjectKey,
       ) -> Result<Box<dyn MultipartUpload>, ObjectStoreError>;
       // Re-hydrate a handle for an upload previously created (possibly by another
       // process or before a crash). Required by ADR 023 §4 recovery.
       async fn resume_multipart(
           &self,
           key: &ObjectKey,
           upload_id: &UploadId,
       ) -> Result<Box<dyn MultipartUpload>, ObjectStoreError>;
       // Enumerate in-progress multipart uploads under a prefix. Used by ADR 023's
       // disk-loss fallback and ADR 024 Track A orphan-multipart sweep.
       async fn list_multipart_uploads(
           &self,
           prefix: &ObjectPrefix,
           continuation: Option<ContinuationToken>,
       ) -> Result<ListMultipartUploadsPage, ObjectStoreError>;
       // Enumerate parts already accepted for an in-progress multipart upload.
       // Used by ADR 023 §4 to reconcile the local ledger against provider state.
       async fn list_parts(
           &self,
           key: &ObjectKey,
           upload_id: &UploadId,
           continuation: Option<ContinuationToken>,
       ) -> Result<ListPartsPage, ObjectStoreError>;
       // Abort by (key, upload_id) without needing a live handle. Used by ADR 024
       // Track A after cross-referencing against the `tiered_slabs` state machine.
       async fn abort_multipart(
           &self,
           key: &ObjectKey,
           upload_id: &UploadId,
       ) -> Result<(), ObjectStoreError>;
   }

   #[async_trait]
   pub trait MultipartUpload: Send + Sync {
       fn upload_id(&self) -> &UploadId;
       fn key(&self) -> &ObjectKey;
       async fn upload_part(
           &mut self,
           part_number: PartNumber,
           body: BytesStream,
       ) -> Result<PartEtag, ObjectStoreError>;
       async fn complete(self: Box<Self>, parts: Vec<(PartNumber, PartEtag)>)
           -> Result<ObjectMetadata, ObjectStoreError>;
       // Abort via the handle (equivalent to calling `ObjectStore::abort_multipart`
       // with this handle's key and upload_id). Present for callers that already hold
       // a live handle and do not need the free-function form.
       async fn abort(self: Box<Self>) -> Result<(), ObjectStoreError>;
   }

   pub struct ListMultipartUploadsPage {
       pub uploads: Vec<InProgressUpload>,
       pub continuation: Option<ContinuationToken>,
   }

   pub struct InProgressUpload {
       pub key: ObjectKey,
       pub upload_id: UploadId,
       pub initiated_at: SystemTime,
   }

   pub struct ListPartsPage {
       pub parts: Vec<AcceptedPart>,
       pub continuation: Option<ContinuationToken>,
   }

   pub struct AcceptedPart {
       pub part_number: PartNumber,
       pub size_bytes: u64,
       pub etag: PartEtag,
       pub last_modified: SystemTime,
   }
   ```

   Surface rationale. The trait covers three distinct use-case clusters:

   - *Object I/O:* `head`, `get`, `get_range`, `put`, `delete`, `copy`. `get_range` covers §6's tail protocol. `head` covers orphan-suspect triage without body transfer. `copy` covers intra-bucket migration (cross-tenant moves, archival staging) without re-upload. `PutCondition` carries If-None-Match / If-Match semantics so single-PUT writes can be idempotent under retry. `BytesStream` is `impl Stream<Item = Result<Bytes>>` — no whole-buffer API at the trait level, composing with ADR 009's zero-copy posture.
   - *Object listing:* `list` with pagination covers ADR 024 Track D's full-bucket orphan-GC scan.
   - *Multipart:* `put_multipart` / `resume_multipart` / `list_multipart_uploads` / `list_parts` / `abort_multipart`. This cluster is the one Wave 4's first review flagged as incomplete. `put_multipart` creates new uploads; `resume_multipart` re-hydrates a handle for an upload we did not create in the current process (after crash recovery or leader transition); `list_multipart_uploads` and `list_parts` support ADR 023's `ListMultipartUploads`-based recovery fallback and ADR 024 Track A's orphan-multipart scan; `abort_multipart` is the top-level abort-by-id form that Track A calls after its state-machine join.

## Rationale

- **Parquet/ORC have already solved this problem.** The self-describing tail-footer layout is the empirically validated answer for object-store-backed immutable columnar-style files. Wendelon's slabs are not Parquet, but the pattern — magic-head, payload, offset index, footer, tail anchor, magic-tail — is transferable because the forcing function (one object, range-GET, no list-side consistency) is identical.
- **Plaintext offsets in the index simplify the decrypt math.** Encrypting the index would require unwrapping the DEK before finding the record, which would invert the intended "locate, then decrypt the minimum number of chunks" flow. The index itself reveals only record count, record size distribution, and key-hash distribution — all coarse-grained signals that do not leak record content. A reader who can obtain the slab (ciphertext) and unwrap the tenant KEK also obtains these signals; protecting them further is defense-in-depth that does not block a realistic adversary.
- **Footer HMAC keyed by Tenant KEK is the right scope.** The footer carries the wrapped DEK, so keying its integrity check with the DEK would be circular. Keying with the Tenant KEK ensures the footer cannot be tampered with without tenant-level key access.
- **HMAC covers every plaintext byte that influences a read, nothing more.** Including the ciphertext chunks in the HMAC would double-authenticate bytes already covered by per-chunk AEAD — an expensive operation because the reader would have to stream the full chunk range even for single-record fetches. Excluding the offset index, tail anchor, or header would let an attacker redirect a read to a different byte range within the same slab without tripping either the AEAD (which binds chunk_index, not byte_offset) or the HMAC (which didn't cover the mapping from byte_offset to chunk). The in-between scope — HMAC over index + footer + tail anchor + header; AEAD over chunks — is the minimum that closes the redirection attack without re-authenticating bytes that are already authenticated.
- **Rotation is addressed by `tenant_kek_id` versioning, not by rewriting slab objects.** Object stores do not support partial-object updates, so "rewrite just the footer on rotation" is not a primitive that exists. ADR 020 §3 addresses rotation by keeping the historical Tenant KEK versions available for unwrap: new slabs are wrapped under the latest version; old slabs continue to point at their historical `tenant_kek_id` and continue to be readable. HMAC verification uses the KEK identified by `tenant_kek_id`, which remains the correct key for old slabs after rotation.
- **Fixed-size 256-byte footer beats a length-prefixed variable footer.** Parsing a variable-length footer requires two tail fetches in the worst case (fetch length, fetch body); fixed-size collapses that to one. 256 bytes is enough for the current field set plus ~60 bytes of forward growth headroom.
- **Fixed 32-byte offset-index entries beat a varint-packed index.** Varint would cut the index by roughly 40% on small records but forces sequential scan to navigate — no ordinal arithmetic. 32 bytes per record at 16 KiB mean record size is 0.2% overhead on the slab, which is an acceptable cost to keep the index randomly addressable.
- **One `ObjectStore` break covers the full surface that L2 needs.** The previous trait was shaped for single-PUT uploads and whole-object GETs; the new trait shape covers what ADRs 020, 022, 023, and 024 all consume. Splitting the break into per-feature migrations costs more churn across call sites than the single coordinated redesign.
- **`rustfs` (ADR 014) is a durable test dependency, not a speculative one.** Keeping it as the canonical test backend means the trait shape is validated against a real S3-semantics implementation rather than only against mocks (ADR 028's deterministic mock is orthogonal — one is for semantics, the other for fault injection).
- **The 64 KiB tail window is sized for common-case slab geometry.** At the ADR-020 default `slab_max_bytes = 64 MiB` with 16 KiB mean records, the offset index is 128 KiB — slightly larger than the tail window, so a second 128 KiB range-GET finishes the metadata phase. At `slab_max_bytes = 256 MiB`, two metadata RTTs are expected. Operators tuning larger slabs pay the expected cost; the default is sized for the common path.

## Consequences (The "Negatives")

- **Two metadata RTTs on large slabs.** When the offset index does not fit in the 64 KiB tail window, a cold point read is three RTTs (tail anchor + index + record data) instead of two. Mitigated by caching the offset index on the reader side (bounded memory per slab; indexed by slab_id) and by sizing `slab_max_bytes` so the common case stays within one metadata RTT.
- **Footer layout change is a hard break.** A slab written under `slab_format_version = N` cannot be read by code that only understands version `N-1` once the layout changes. Mitigated by: `slab_format_version` being a first-class field; readers must reject unknown versions loudly rather than silently misparse; migration of stored slabs across format changes is the migration sweeper's job (ADR 020 §7).
- **No sidecar manifest means no cheap per-slab update path.** If an operator wants to mark a slab as "rewritten to fix a corruption," the only mechanism is to write a new slab and update the `tiered_slabs` state machine record; the old slab is GC'd by ADR 024. Accepted because the alternative — a mutable sidecar — introduces S3 consistency puzzles that cost more than they save.
- **`ObjectStore` trait break touches every call site at once.** The coordinated migration is real engineering cost (estimate: all storage, tiering, and test code paths). Mitigated by the trait being small, by `rustfs` (ADR 014) providing semantic validation during the migration, and by the fact that partial migrations would have to either wrap the new surface in the old shape (doubling code paths) or ship a half-migrated codebase (structural debt).
- **Index-level tombstones grow the offset index even after compaction.** An entry with `flags.tombstone = 1` consumes 32 bytes until the E_safe compactor rewrites the slab. Mitigated by E_safe compaction being the intended GC path for dead-key accumulation (`plans/ops/storage_tiering.md` §4).
- **Cross-slab record lookups are out of scope.** The offset index locates records *within* a slab. The unified ring (ADR 002) routes `(tenant_id, key) → slab_id`; once the slab is located, the offset index does the intra-slab half. Readers who issue range-GETs without a prior `slab_id` (wildcards, scans) pay the cost of listing tenants' slabs and are structurally slower — this is intentional, not a defect.
- **Rejected alternatives explicitly:**
  - *Sidecar manifest (one manifest object per slab, separately fetched)* — rejected for consistency puzzles on eventually-consistent stores and doubled object count (N slabs → 2N objects, doubled LIST cost, doubled orphan-GC surface).
  - *S3 object user-metadata as offset index* — rejected for the 2 KiB user-metadata size cap (too small for real index sizes) and for the fact that user-metadata is not atomically updatable without re-uploading the object.
  - *Multi-object slab splits (slab-N.data + slab-N.index)* — rejected for atomicity surface: a reader can observe the index before the data on an eventually-consistent store and return stale or missing data. Single-object slabs are atomic by object-store semantics.
  - *Variable-length footer with length prefix at the head of the footer* — rejected for the two-fetch parse path; fixed-size wins on tail-GET economics.
  - *Encrypting the offset index* — rejected because it would force DEK unwrap before record location, inverting the tail-read flow without a realistic adversarial benefit.
  - *Splitting the `ObjectStore` trait break across multiple PRs* — rejected for structurally doubled call-site migration cost and for leaving the codebase half-migrated between releases.

## Open Questions

All items below must be closed before this ADR moves from Proposed to Accepted.

- **Tail-window size tuning.** 64 KiB is chosen to match common provider minimum-range-read sizes (S3 does not charge for sub-part-size range-GETs on standard tier but does incur request cost). A measurement against AWS S3, GCS, and Azure Blob should confirm that 64 KiB is the right window; some providers may see better aggregate cost at 128 KiB. *Resolves via:* a benchmark against the three production providers and `rustfs`, measuring RTT + transferred bytes + request cost for tail windows in {16, 32, 64, 128, 256} KiB at representative slab geometries.
- **Bloom filter in the footer.** A small (~8 KiB) bloom filter over the 16-byte key hashes would let a reader reject a slab without issuing the index range-GET, useful for scan-heavy workloads. Adding it now widens the footer; adding it later is a `slab_format_version` bump. *Resolves via:* a measurement pass against representative read workloads (scan-heavy vs point-heavy) to decide whether the bloom's false-positive economics justify the footer-size increase.
- **Multi-record range-GET coalescing.** When a single L2 read fetches N records from the same slab, the reader should coalesce adjacent chunk ranges into a single range-GET. The coalescing policy (always coalesce, or only when records are within M chunks of each other) is a tunable. *Resolves via:* an implementation-time benchmark; the ADR commits to the coalescing capability, not the specific threshold.
- **`chunk_size_bytes` variable-per-slab or pinned.** ADR 020 pins chunk size at 64 KiB today. The header field is 4 bytes so a future ADR could vary chunk size per slab. Whether pilot code emits only 65536 (and asserts on read) or already supports read-time chunk-size dispatch is an implementation decision. *Resolves via:* implementation-time decision; the ADR commits to the field being present, not to runtime chunk-size diversity.

## References
- ADR 002: Unified Hash Ring Topology (routes `(tenant_id, key) → slab_id`; this ADR handles the intra-slab half)
- ADR 006: Tiered Storage and Lazy Migration (extended — this ADR freezes the slab format)
- ADR 009: io_uring Threading Model (`AlignedBuf`; the slab is written in one aligned issue sequence)
- ADR 014: rustfs Test Backend (implements the canonical `ObjectStore` trait defined here)
- ADR 018: Dual-CA mTLS Identity and OID-Bound Authorization (`tenant_id` source for the header/footer field)
- ADR 020: L2 Envelope Encryption and DEK Lifecycle (freezes the ADR 020 §6 footer-format open question)
- ADR 021: Tenant-Aware Architecture from Day One (`tenant_id` flows through the slab header/footer/AAD consistently)
- ADR 023: L2 Upload Lifecycle (uses this slab layout as the multipart upload unit)
- ADR 024: L2 Orphan Cleanup (paged LIST joins against the `tiered_slabs` state recording these slabs)
- ADR 028: Deterministic Simulator (mocked `ObjectStore` implements the trait defined here)
- `plans/ops/storage_tiering.md` (rewritten to reference this ADR's frozen layout)
- `plans/gap/claude_answers_20260417.md` §4
