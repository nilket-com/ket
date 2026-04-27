# ADR 043: Arrow IPC Body Compression (LZ4)

Author: AI Swarm Council (Claude / Codex / Gemini)

## Status

Accepted

## Context

ADR 042 froze the storage-resident shape of analytical
writes as `[SlabRecordHeader : 72B] [Arrow IPC bytes]
[zero pad → 4K]`, where the Arrow IPC payload is
*uncompressed*. That choice was deliberate: 042 was a
layout ADR, not a density ADR. The IPC payload remains the
largest single contributor to slab bytes for analytical
workloads — typically 8–16 bytes/value of in-memory Arrow
representation, materialized verbatim onto NVMe and into L2.

Bandwidth and resident memory are the dominant variable
cost as analytical scale grows. The shipped stack already
attacks the leverage axes from above (locality narrowing,
stats pruning, AGG folding, multi-row packing); the
remaining floor is bytes-on-the-wire and bytes-in-cache
during the admitted-slab read.

Arrow IPC supports per-buffer body compression natively.
The codebase already enables the feature flag (`arrow = {
version = "58.1.0", features = ["ipc_compression"] }` in
`Cargo.toml`). Turning it on at the writer side is a small
surface change with disproportionate cost-axis return:

- LZ4_FRAME compresses Arrow buffer bytes at NVMe-write
  speed (≈500 MB/s/core encode) and decompresses at
  memory-bandwidth speed (≈3–5 GB/s/core), versus zstd's
  denser-but-slower trade.
- Arrow IPC's per-buffer compression layout means each
  column buffer is independently decompressable, leaving
  room for a future cold-tier column-fetch ADR without
  rewriting the format.
- The reader-side change is mostly metadata plumbing: the
  `StreamReader` already understands compressed buffers
  when the `ipc_compression` feature is on.

Brainstorm 011 (the "Vortex" playbook) originally bundled
this as the V1.5 step before homerolled lightweight
columnar encodings. Promoting it out lets the cost-axis
win land on its own schedule and lets V2/V3 LCE/COCD
design proceed as a measured next step, not a speculative
one.

## Non-goals (V1)

- **No homerolled lightweight columnar encodings (LCE).**
  FastLanes bit-packing, ALP for floats, FSST for strings,
  encoding cascades — all Brainstorm 011 V2 territory.
- **No compute-over-compressed-data (COCD).** PBC continues
  to see decompressed Arrow batches. Brainstorm 011 V3.
- **No page-directory point-read optimization.** A point
  read on a multi-row table still decompresses the whole
  enclosing IPC payload (bounded by `max_record_bytes` on
  the *uncompressed* axis — see §3).
- **No L2 columnar Range-GET / page-fetch directory.**
  Column-aware Range-GETs from L2 — the cold-tier story
  raised by the council — is a separate high-priority
  follow-up ADR. ADR 043's per-buffer LZ4 is the
  *substrate* for it (each column buffer decompresses
  independently from its containing 64 KiB AEAD chunk) but
  not the implementation.
- **No provider-native object-store filtering** such as S3
  Select. ADR 020 envelope encryption already precludes it:
  the storage provider sees only ciphertext.
- **No Parquet / SSE cold-tier fork.** Switching L2 to
  Parquet plus provider-side encryption is a storage
  architecture change, not a compression policy. It would
  trade ADR 020's zero-trust model and ADR 042's
  rehydration invariant for ecosystem pushdown — an
  ADR-grade decision on its own merits, not a substitute
  for ADR 043.
- **No encryption chunk-layout or AEAD range-authentication
  redesign.** ADR 020's per-slab DEK + 64 KiB chunked AEAD
  discipline is unchanged. Future column-fetch work
  composes with that discipline at chunk granularity, not
  by rewriting it.
- **No zstd, snappy, or other codecs.** LZ4_FRAME only in
  V1. No reserved enum slots; future codecs land in their
  own ADR when measured workload pulls.
- **No compression of network wire frames.** ADR 011
  governs transport; 043 is a storage-resident-payload
  ADR. Wire-side IPC frames stay on default options.
- **No compression of the slab-footer CSB (ADR 039).** Stats
  live at fixed offsets, are already AEAD-sealed (ADR 020),
  and need O(1) random access during admission.
- **No compression of the descriptor schema IPC**
  (`encode_arrow_schema_ipc` in `src/schema/table.rs`).
  Descriptor decode must not depend on the
  `ipc_compression` Cargo feature being on at boot.
- **No compression of the `SlabRecordHeader` (72B).**
  Header is the torn-write and checksum boundary; it stays
  in fixed-shape cleartext per ADR 037 / 042.
- **No new public HRFT frame family.**
- **No new node capability bit.** A node either runs the
  storage engine with `ipc_compression` support or it does
  not. The storage bytes are self-describing via the Arrow
  IPC FlatBuffer's `BodyCompression` field.
- **No `SingleRowLayoutDescriptor` refactor.** Compression
  policy lives at the top level of `TableSchemaDescriptor`,
  parallel to `record_layout_policy`.
- **No project-name-prefixed enum names, descriptor fields,
  metric segments, or reject codes** (per
  `plans/conventions/no-project-name-in-protocols.md`).
- **No `V1` / `V2` suffixes** on Rust types (per
  `plans/conventions/no-version-suffixes.md`).
- **No envelope-format-version bump.** Per-buffer
  compression is signalled inside the Arrow IPC FlatBuffer
  metadata; the envelope shape is unchanged.
- **No compatibility shim for pre-ADR-043 readers.** No
  deployed external storage format contract to preserve.

## Decision

### 1. Compression policy is a per-table descriptor field

Add to `TableSchemaDescriptor`:

```rust
enum RecordBodyCompressionPolicy {
    Uncompressed,
    Lz4Frame,
}
```

The descriptor field is:

```rust
record_body_compression_policy: RecordBodyCompressionPolicy
```

Naming aligns with `RecordLayoutPolicy` — both are
per-table storage-policy fields on the same descriptor, and
the symmetric name reads correctly at every call site. The
enum exists (rather than a `bool`) so future codecs add a
variant rather than reshape a field, but no slots are
reserved for them.

V1 admits exactly two states. The default is *layout-driven*:

- `RecordLayoutPolicy::MultiRow` ⇒ default
  `RecordBodyCompressionPolicy::Lz4Frame`. Multi-row records
  carry enough rows per IPC for LZ4 framing overhead to
  amortize.
- `RecordLayoutPolicy::SingleRow` ⇒ default
  `RecordBodyCompressionPolicy::Uncompressed`. Single-row
  IPCs are too small for compression to help reliably.

Operators override per table. The policy is set at table
creation; changing it on an existing table requires no
migration sweep — mixed-compression slabs are readable
side-by-side (§4) and natural compaction re-emits the new
shape (§7).

The persisted descriptor stores an explicit value (no
implicit "derive from layout at decode time" — that would
silently flip if the layout policy changed). Construction
helpers resolve the layout-driven default; serialized state
is always explicit.

The field is included in descriptor fingerprint
computation, matching the existing discipline for
`record_layout_policy`.

### 2. Slab record physical shape unchanged

```text
[SlabRecordHeader : 72B] [ Arrow IPC bytes : frame_len ] [ zero pad → 4K ]
```

`SlabRecordHeader.length` continues to name the IPC frame's
byte length on disk: compressed length under `Lz4Frame`,
raw IPC length under `Uncompressed`. The torn-write /
checksum boundary discipline is unchanged: the record
checksum / payload hash is computed over the *stored* bytes
(compressed when applicable), preserving corruption
detection at the actual disk boundary.

The frame bytes themselves are valid Arrow IPC stream
bytes; whether buffers inside are compressed is signalled
by the stock `BodyCompression { codec = LZ4_FRAME, method =
BUFFER }` field in the IPC FlatBuffer header.

The envelope is *not* version-bumped. Adding a parallel
storage-envelope version creates a second source of truth
with no new information — and a place where envelope and
IPC can disagree.

### 3. Two byte axes: enforcement vs. accounting

ADR 042 §10 caps multi-row point-read amplification at
`max_record_bytes` (default 1 MiB, hard ceiling 8 MiB).
Compression must not weaken that bound. Two distinct byte
quantities matter and they are policed against different
rules:

- **`max_record_bytes` is enforced against the
  *uncompressed* IPC body.** The cap exists to bound
  point-read decompressed payload, decode-time memory
  peaks, and the cost of `take(&[record_row_index])`.
  Compression shrinks bytes-on-disk; it does not shrink
  bytes-in-RAM after decode. Letting compression admit
  more rows per record would silently raise ADR 042's
  amplification ceiling and convert the descriptor's
  point-read trade-off into a workload-shape lottery.
- **Stored-byte / quota accounting uses the *compressed*
  IPC body** (post-compression, post-encryption, plus
  padding). Tenant quotas count what is actually written
  to storage, matching the existing multi-tenancy rule
  that quotas count stored ciphertext bytes per logical
  replica.

The packer measures both axes. V1 may encode once
uncompressed to enforce `max_record_bytes`, then encode
compressed for the actual write when the policy says
`Lz4Frame`. The double-encode cost is amortized at the
per-record level (not per-row) and shows up in
`storage_compression_encode_micros`; if it becomes
operationally significant, a later implementation pass can
adopt a streaming-estimate path without changing the ADR
contract.

### 4. Reader side: Arrow IPC metadata is the format authority

`StreamReader::try_new` decompresses transparently when the
`ipc_compression` Cargo feature is enabled (already on). No
descriptor lookup is required at read time — the IPC
FlatBuffer header carries the `BodyCompression` metadata,
and the reader handles both compressed and uncompressed
records in the same pass.

This is **load-bearing** in two directions:

- *Mixed-compression slabs are first-class.* Physical
  records outlive descriptor policy edits. A node carrying
  pre-043 sealed slabs reads them without policy knowledge
  or migration step. New writes after the 043 deploy are
  compressed per descriptor. A descriptor edit flipping
  `Lz4Frame ↔ Uncompressed` does not invalidate any
  existing records.
- *Descriptor and disk cannot disagree.* Descriptor-only
  dispatch would create a place where the descriptor says
  "this table is compressed" and a specific record says
  otherwise. The Arrow IPC FlatBuffer is the single source
  of truth for what a given record actually contains; the
  descriptor names the *write-time* policy.

The read path on the storage side dispatches as before:

1. Slab admission (ADR 038 locality + ADR 039 CSB
   pruning).
2. Decode the Arrow IPC frame (decompresses internally
   when present).
3. Strip ADR 042 synthetic columns.
4. Run PBC on the resulting batch.
5. Fold survivors into AGG accumulators (ADR 040).

Steps 2 and beyond are textually unchanged. The following
paths all decode through the same centralized helper and
therefore treat both shapes uniformly: point reads, range
scans / PBC, AGG folding, CSB stats recomputation during
seal / rewrite, retention rewrite, tombstone-aware
reclaim, supersession-metadata backfill, explicit layout
recluster, crash recovery.

### 5. Writer side

All storage-resident IPC writers route through one helper:

```rust
fn ipc_write_options_for(
    descriptor: &TableSchemaDescriptor,
) -> IpcWriteOptions;
```

For `RecordBodyCompressionPolicy::Uncompressed`, the helper
returns default Arrow IPC write options. For
`RecordBodyCompressionPolicy::Lz4Frame`, it returns options
with `CompressionType::LZ4_FRAME`.

Storage writers use this helper for multi-row pack output,
single-row table records, retention rewrites,
tombstone-aware reclaim, and layout recluster output. Two
storage-internal encoders are explicit exclusions and use
default IPC options only:

- **`encode_arrow_schema_ipc`** (descriptor schema IPC) —
  descriptor decode must not depend on the
  `ipc_compression` feature being on at boot.
- **Slab-footer CSB encoder** — fixed-offset random access
  and ADR 020 envelope sealing are the relevant invariants.

Both exclusions are guarded in tests so a future "compress
everything" refactor cannot pull them under record-body
compression. Wire-side IPC encoders
(`src/network/data_server.rs`,
`src/network/control_server.rs`, `src/client/core.rs`,
`crates/wendelon-client/`) also remain on default options;
ADR 043 is storage-resident only.

Compression happens *before* AEAD encryption. The order is:

```text
Arrow encode → LZ4 body compression → slab payload checksum →
envelope encryption → disk / L2 object storage
```

Compress-then-encrypt is the correct order both for density
(compression ratios collapse on high-entropy ciphertext)
and confidentiality. The reverse order is a known
anti-pattern.

### 6. Stats writer composes unchanged

ADR 039's CSB is computed over **rows**, not bytes. The
stats writer iterates rows from the decoded Arrow batch,
folds them into per-column
`min/max/null/non-null/KLL/sketch` builders, and writes the
resulting CSB into the slab footer.

Compression is orthogonal to that loop. The stats writer
reads batches via the same `decode_batch` helper as PBC;
whether the buffers were compressed on disk is invisible to
the stats fold. The slab-footer CSB itself stays
uncompressed at fixed offsets and stays AEAD-sealed per
ADR 039 / 020. The footer's existing
`stats_format_version: u16` is not bumped; stats content is
unchanged.

### 7. Compaction re-emits per current descriptor

Single coherent compaction (ADR 042 §13) is the natural
place where mixed-compression slabs converge. Each rewrite
the sweeper emits — retention, tombstone-aware reclaim,
layout migration — encodes the new IPC per the *current*
descriptor, applying LZ4_FRAME when the descriptor says so.
The pointer-swap primitive is unchanged; the bytes it
commits to are denser.

No dedicated "compression migration" sweep is added. A
re-encoding upgrade joins the existing pointer-swap rather
than running on its own pass; a dedicated pass would add
write amplification without changing logical correctness.
Operators who want the density gain *before* natural
compaction reaches a slab can trigger a recompact via the
existing layout-recluster sweep (ADR 042 Phase 7) — the
infrastructure already does the work.

The convergence gauge (§9)
`storage_compression_uncompressed_resident_bytes` decays
toward zero on tables that have moved to `Lz4Frame` as
natural compaction re-emits.

### 8. Recovery is descriptor-free

Recovery from raw slab bytes (ADR 042 §3) decodes the Arrow
IPC to rebuild the redb key-index and locality-index. The
decoder sees compressed or uncompressed buffers
transparently; no descriptor is needed during recovery.

ADR 042's "descriptor-free recovery" guarantee is
preserved: synthetic columns carry per-row identity, HLC,
and routing locality; LZ4 just changes the byte
representation of those columns inside the IPC frame.

Recovery requires the `ipc_compression` Cargo feature to be
on. The feature is on in all production build profiles; a
build-profile assertion pins the feature on.

### 9. Observability

Most metrics carry a `{codec}` label so a future codec ADR
adds label values rather than reshaping the metric catalog.
Tenant-axis labeling is left to the metric registry's
existing per-tenant conventions and is not made part of
this ADR's contract. Metric segment names stay brand-clean
per `plans/conventions/no-project-name-in-protocols.md`.

```text
storage_compression_records_written_total{codec}     // counter
storage_compression_plaintext_bytes_total{codec}     // counter
storage_compression_encoded_bytes_total{codec}       // counter
storage_compression_encode_micros{codec}             // histogram
storage_compression_decode_micros{codec}             // histogram
storage_compression_decompressed_bytes_total{codec}  // counter
storage_compression_decode_error_total{codec}        // counter
storage_compression_l2_record_body_bytes_fetched_total{codec} // counter
storage_compression_ratio{codec}                     // histogram
storage_compression_uncompressed_resident_bytes      // gauge (unlabeled)
```

The `uncompressed_resident_bytes` gauge is unlabeled by
design: it measures *currently resident* uncompressed IPC
bytes across sealed and tiered slabs **whose current
descriptor selects `Lz4Frame`** — i.e. records that should
be compressed under today's policy but haven't been
re-emitted yet. The codec dimension is omitted because the
metric's filter (descriptor-current = `Lz4Frame`) makes
"uncompressed codec" the only meaningful value. A gauge
(not a counter) is the right shape because the question is
"what is still resident now?", not "how many uncompressed
bytes were ever written?". The gauge decays as natural
compaction reaches each slab.

An implementation may need to inspect sealed and tiered
slab bodies to refresh this gauge. That refresh is an
explicit maintenance / compaction diagnostic and must not
run from a Prometheus scrape path; doing so would turn the
gauge into a periodic cold-tier full scan.

The L2 fetched-bytes counter tracks admitted whole
record-body bytes after the L2 range read has produced a
record body and the IPC codec has been detected from Arrow
metadata. It exists to verify ADR 043's cold-tier claim:
LZ4 reduces L2 bytes only by shrinking admitted record
bodies, not by adding provider-side filtering or
column-level Range-GET pushdown.

Core stabilization signals:

- `plaintext_bytes_total / encoded_bytes_total` per codec
  — the density story.
- `decode_micros` per codec on admitted slabs — the CPU
  story.
- `uncompressed_resident_bytes` gauge — the convergence
  story.
- `decode_error_total` — corruption / version drift signal;
  expected zero in steady state.

### 10. New newtype

Per `plans/conventions/newtype-units.md`:

```rust
struct CompressionRatio(f32); // encoded / plaintext, finite and >= 0;
                              // values > 1 indicate compression overhead
                              // exceeded savings on high-entropy inputs.
```

`CompressionRatio` wraps the value recorded in
`storage_compression_ratio{codec}` and any
in-process density checks. The newtype rejects NaN and
infinity but does *not* cap at 1: high-entropy inputs
(encrypted blobs, already-compressed payloads, dense
binary) can produce encoded bytes larger than plaintext
bytes once LZ4 framing overhead applies. Capping at 1
would hide exactly the bad-compression cases the metric
exists to surface. `RecordBodyCompressionPolicy` is an
enum, not a newtype. No `CompressionLevel` newtype lands
in this ADR because LZ4_FRAME does not expose a useful V1
tuning knob.

### 11. PBC and AGG protocols unchanged

Neither PBC (ADR 035 / 036) nor AGG (ADR 040) sees
compression. Both consume decoded `RecordBatch` values; the
storage path strips compression before handing batches off.
The predicate language, opcode catalog, accumulator
catalog, and visibility-metadata discipline
(`tombstone_metadata`, `supersession_metadata`) are
textually unchanged.

The CSB short-circuit (ADR 040 §5 + ADR 042 §11) continues
to operate on per-slab visibility metadata, which is itself
uncompressed in the slab footer (§6 above).

### 12. L2 cold-tier fetch pruning scope

ADR 043 does not change the L2 unit of body fetch. The
current cold read path remains:

1. Use ADR 038 locality and ADR 039 CSB stats to prune
   whole slabs without fetching record bodies.
2. Use the ADR 022 unencrypted footer / offset index to
   locate admitted records.
3. Range-GET the stored bytes for each admitted record body
   (chunk-aligned per ADR 020's 64 KiB chunked AEAD).
4. Decrypt, decode Arrow IPC, decompress LZ4 buffers when
   present.
5. Run PBC and AGG over decoded batches.

The compression win is step 3: admitted record bodies are
~50% smaller on disk and over the wire. The pruning win
remains step 1: rejected slabs do not fetch record bodies
at all. ADR 043 does **not** let the storage node fetch
only the predicate column, only the projected columns, or
only the rows that survive a predicate inside a multi-row
record.

That limitation is intentional scope control. True
cold-tier sub-record fetch pruning requires a separate
format and protocol decision: an authenticated directory
from record/column/page to L2 byte ranges, chunk-aligned
AEAD range reads, planner / PBC column projection, and
probably row-group or mini-page granularity for selective
predicates. Per-buffer LZ4 (this ADR's choice over
record-level compression) is useful substrate for that
future work because each column buffer compresses
independently inside the IPC body, but it is not
sufficient on its own.

Provider-native filtering (S3 Select) is also outside
ADR 043: ADR 020 envelope encryption already precludes it.
Moving L2 to Parquet plus provider-side encryption would
be a storage-architecture fork with different trust,
rewrite, recovery, and execution-engine tradeoffs — an
ADR-grade decision on its own merits, not a substitute
for ADR 043.

### 13. Migration from pre-043 sealed slabs

No migration sweep. The reader handles both shapes;
compaction naturally re-emits in the descriptor-current
shape. Existing tiered slabs stay readable indefinitely
in their original shape.

This is the same "graceful coexistence under coherent
compaction" pattern ADR 042 used for pre-042 sealed slabs
and their `Untracked → tracked` supersession-metadata
transition.

The convergence gauge (§9) lets operators decide whether
to trigger an explicit recompact or wait for natural
retention turnover.

### 14. Capability and protocol surface

No new negotiated node capability is introduced. This is
an internal storage encoding selected by table descriptor
and self-described by Arrow IPC metadata, not a new
Toron-visible wire protocol.

No new reject-code family is introduced. Descriptor
validation errors use the existing table-schema update
path. Decode failures surface as storage corruption /
invalid-data errors on the same paths that currently
handle malformed Arrow IPC.

## Rationale

**Why LZ4 first.** LZ4 body compression is already part of
Arrow IPC, already in the dependency graph, and preserves
the Arrow batch boundary that PBC and AGG execute over. It
attacks the dominant bandwidth and storage-density cost
without designing a new format. Decompress speed
(≈3–5 GB/s/core) matters more than ratio on the scan-heavy
read path; zstd becomes interesting if and when L2
capacity cost dominates L1 read-bandwidth cost — that is a
measured signal, not a speculative one.

**Why not jump to LCE.** Brainstorm 011 V2/V3 is real
work, but its value depends on evidence that LZ4 leaves
meaningful headroom. Custom encodings would need page
directories, cascade selection, SIMD decode paths,
metadata validation, corruption tests, and PBC awareness.
LZ4 gives a measurable baseline first.

**Why per-table descriptor field rather than cluster-wide
default.** Same reason `RecordLayoutPolicy` is per-table.
Workloads vary; compression that helps an analytical fact
table can hurt a small-payload event log.

**Why default-by-layout
(MultiRow → Lz4Frame, SingleRow → Uncompressed).**
Compression overhead amortizes over IPC frame size;
LZ4_FRAME's header is small (≈10 bytes per buffer block)
but non-trivial against a single-row Arrow IPC that may
itself only carry a few hundred bytes of payload. Multi-row
records pack thousands of rows per IPC and compression
overhead disappears into the noise.

**Why the persisted descriptor stores an explicit value.**
Implicit derivation would create a place where flipping
`RecordLayoutPolicy` silently flips
`RecordBodyCompressionPolicy` without an audit trail.

**Why no envelope version bump.** The `BodyCompression`
field lives in the Arrow IPC FlatBuffer schema metadata.
Arrow's reader handles both shapes natively. Adding a
parallel storage-envelope version creates a second source
of truth with no new information.

**Why readers trust Arrow IPC metadata, not the
descriptor.** Physical records can outlive descriptor
policy edits. Descriptor-only dispatch would make
mixed-shape tables fragile during the natural-convergence
window after a policy edit. Arrow IPC already records
compression per batch.

**Why `max_record_bytes` is enforced against uncompressed
bytes.** ADR 042's amplification cap is a *decode-memory*
and *decompressed-payload* bound, not an on-disk bound.
Compression shrinks the disk side; it does not shrink the
in-RAM side after decode. Enforcing against compressed
bytes would convert a stable point-read trade-off into a
workload-shape lottery. Quota accounting uses compressed
bytes because that is what is actually written; the two
axes serve different invariants.

**Why compress-then-encrypt.** AEAD on compressed bytes is
the correct order for density and confidentiality. The
reverse order is a known anti-pattern.

**Why centralize the writer-options helper.** Three storage
writer call sites today plus two storage-internal sites
that must *not* compress (`encode_arrow_schema_ipc`,
slab-footer CSB encoder). One helper, three
architectural-test guards, no drift.

**Why no compression migration sweep.** Same logic as ADR
042 §13's "single coherent compaction" rule. The
pointer-swap primitive already runs on retention,
tombstone reclaim, and layout migration; tacking
compression policy onto the existing re-emit is free.

**Why no per-buffer page directory in V1.** Brainstorm 011
V2 covers it as part of a coherent LCE design. The
page-directory piece is genuinely valuable but genuinely
complex; bundling it into 043 stretches a small ADR into a
multi-week design surface.

**Why no new node capability bit.** Self-describing
storage bytes plus a build-time Cargo feature are the
right shape. Negotiating compression support across nodes
would imply a mixed-cluster compatibility contract this
project does not offer at the storage layer.

**Why keep descriptor fingerprinting coupled in V1.** The
current descriptor fingerprint already includes physical
storage policy such as `record_layout_policy`. Adding one
more physical policy keeps integrity simple and cache
invalidation conservative.

**Why not jump to L2 columnar Range-GET in this ADR.** The
council's L2 pushdown concern is real: fetching a full
admitted record from object storage to discard most
columns or rows is not Parquet-class pushdown. But fixing
that requires an authenticated record-to-buffer offset
directory, chunk-aligned encrypted range-fetch protocol
changes, and column projection in the PBC executor — a
coordinated change across ADRs 020, 022, 035, and 042.
ADR 043 deliberately chooses per-buffer LZ4 (over
record-level zstd) so the later directory ADR can target
independent Arrow buffers without a re-encode, but the
directory, AEAD range semantics, and PBC projection work
belongs in a dedicated ADR.

**Why not switch ADR 043 to Parquet plus provider-side
encryption.** A Parquet cold-tier fork could improve
ecosystem pushdown (S3 Select), but it changes more than
compression: L1-to-L2 tiering becomes transcoding, the
storage engine carries two physical formats, recovery no
longer rehydrates the same slab record shape, and
provider-side encryption abandons ADR 020's zero-trust
posture. That may be a valid future architecture
direction, but it is not a small replacement for LZ4 body
compression. ADR 043 stays narrow.

**Why this is its own ADR rather than an ADR 042
amendment.** Compression is a discrete capability with its
own descriptor field, observability surface, per-table
policy discipline, and forward-compat story. An amendment
would either bury it as a configuration knob or expand 042
retroactively.

**Why this is its own ADR rather than waiting for the LCE
ADR.** LCE V2/V3 is multi-quarter design. LZ4
BodyCompression is a few-day implementation surface that
captures most of the bandwidth/memory headroom Brainstorm
011 identified.

## Consequences (the negatives)

- **Decode CPU moves onto admitted reads.** Tenants with
  poor stats pruning may see latency regressions if they
  enable LZ4 on the wrong table. The per-table opt-out and
  `storage_compression_decode_micros` metric are the relief
  valves.
- **Encode cost on every write.** LZ4_FRAME encodes at
  ≈500 MB/s/core. Write throughput on multi-row tables
  drops by the encode share — typically 10–20% on a
  write-bound node.
- **Encode cost includes measurement cost.** §3 requires
  encoding once uncompressed (or estimating) for
  `max_record_bytes` enforcement and once compressed for
  the actual write. The double-encode is record-level.
- **Pre-043 sealed slabs stay uncompressed indefinitely.**
  Until natural compaction or operator-triggered recluster
  re-emits them, the on-disk and L2 footprint is unchanged.
- **Compressed slabs cannot be inspected with raw byte
  tools.** An operator dumping raw IPC bytes from a slab
  record now sees Arrow-compressed buffers. Decode requires
  an Arrow reader.
- **Decompressed memory peaks unchanged.** ADR 042's
  "memory spike on multi-row decode" caveat remains: the
  in-resident decoded batch is still bounded by
  `max_record_bytes` on the uncompressed axis.
- **Stored-byte / quota accounting changes.** Quotas and
  charged bytes shrink for compressible records.
- **Compression hides wide-row pressure in stored-size
  metrics.** Operators reading only `encoded_bytes_total`
  understate decode-memory risk.
- **Golden-byte fixtures are more fragile.** LZ4 output
  bytes may change across `arrow` or `lz4_flex` upgrades
  even when decoded batches are byte-identical. Golden
  tests must pin *semantic round-trip* and *FlatBuffer-
  metadata presence*, not raw payload bytes.
- **Compression-ratio histogram is workload-shape
  dependent.** Different schemas show different numbers.
- **Mixed-compression slab footprint during rollout.** A
  tenant that disables `Lz4Frame` after enabling it leaves
  compressed slabs in place that need natural re-emit to
  flip back.
- **Descriptor fingerprint churn on physical-only edits.**
  Including `record_body_compression_policy` in the
  fingerprint means a policy flip changes the fingerprint
  and may invalidate fingerprint-keyed caches even though
  the logical schema is unchanged.
- **One more knob in the descriptor surface.** Toron and
  any other descriptor-aware control plane learn a new
  field.
- **L2 cold-tier fetch granularity is unchanged.** ADR 043
  reduces the *bytes per fetched record* by ~50%, but it
  does not change the *unit of fetch*: today's L2 read
  path fetches whole records via ADR 022's footer-indexed
  Range-GETs, then decodes and filters node-side. For an
  analytical query touching 5 of 50 columns, ADR 043 still
  moves all 50 columns' compressed bytes across the
  S3-to-node link. The full column-level fetch-pruning
  story is a separate follow-up ADR.

## Rejected alternatives

- **Outer-record compression around the whole slab
  payload.** Breaks per-record random access — the slab
  footer index points at byte offsets within the slab, and
  offset-addressable records are load-bearing for ADR 037
  / 042. Also duplicates Arrow's compression layer.
- **Compress at the storage block layer (ZFS-style
  transparent compression).** Outsources policy to the
  filesystem, which ADR 030 does not assume. Loses
  per-table policy. Loses portability of L2 bytes.
- **Use zstd as the V1 codec.** Decompression speed cost
  is too high for the scan-heavy read path. Future ADR if
  capacity-axis cost overtakes bandwidth-axis cost.
- **Use snappy.** LZ4_FRAME dominates snappy on both axes.
- **Cluster-wide forced compression.** Penalizes the
  small-IPC corner and removes operator recourse.
- **Compression policy as a `bool` instead of an enum.**
  Forces a redesign at the Rust type level the moment a
  second codec ships.
- **Reserve `Zstd` and `Snappy` enum slots for future
  use.** Convention violation per
  `plans/conventions/no-premature-backward-compat.md`.
- **Bump envelope `format_version` to 2.** The Arrow IPC
  FlatBuffer's `BodyCompression` field already carries the
  signal.
- **Encrypt-then-compress.** Cryptographically broken;
  standard anti-pattern.
- **Per-table compression-level tunable.** LZ4_FRAME has
  effectively no useful level knob. Add the newtype if and
  when zstd ADR opens.
- **Compress the slab-footer CSB.** Stats need O(1)
  random-access decryption during admission; compression
  defeats fixed-offset access.
- **Compress the descriptor schema IPC.** Descriptor
  decode must work without the `ipc_compression` Cargo
  feature for boot-time independence.
- **Compression flag in `SlabRecordHeader`.** Arrow IPC
  metadata is already the format authority. A second flag
  creates mismatch states without helping recovery.
- **Compression dispatch from descriptor at read time.**
  Physical records can outlive descriptor edits;
  descriptor-only dispatch fragilizes mixed-shape tables.
- **Bundle a page directory for O(1) point-read
  decompression.** Brainstorm 011 V2 territory. Coupling
  it to LZ4 adoption delays both.
- **Bundle homerolled SIMD encodings (FastLanes, ALP,
  FSST).** Brainstorm 011 V2 / V3. Multi-quarter design
  surface.
- **Per-column compression policy.** Arrow IPC's
  `BodyCompression` is per-buffer; in principle this is
  expressible. In practice, per-column policy is a
  policy-surface explosion and the V2 LCE story does
  column-aware encoding cascades better.
- **Auto-tune compression on / off based on per-record
  measured ratio.** Adds writer-side branching and a
  feedback loop. The descriptor opt-in already lets
  operators set policy from observed metrics.
- **Compress wire-frame IPCs.** Different ADR. Transport
  policy lives in ADR 011 / 004; storage policy lives
  here.
- **Refactor `SingleRow` into
  `SingleRow(SingleRowLayoutDescriptor)` to nest a
  compression field.** Heavier refactor than 043 needs.
- **New negotiated node capability bit for compression
  support.** No mixed-cluster compatibility contract at
  the storage layer.
- **Enforce `max_record_bytes` against compressed bytes.**
  Converts a stable point-read decode-memory bound into a
  workload-shape lottery.
- **Counter sum-rate as the convergence metric.**
  Operationally noisier than a gauge; the question "what
  is still resident now?" is gauge-shaped, not
  counter-shaped.
- **Compression-only sweeper pass.** Doubles write-amp on
  slabs natural compaction would have reached anyway.
- **Bundle L2 columnar Range-GET pushdown into ADR 043.**
  This is the right problem but the wrong change envelope.
  It needs new authenticated offset metadata, chunk-aligned
  encrypted range reads, column projection in the
  decode/PBC path, and a row or page granularity story for
  selective predicates. Per-buffer LZ4 makes that future
  easier; it does not implement it.
- **Switch L2 to Parquet plus provider-side encryption in
  this ADR.** That is a storage architecture fork, not a
  compression policy. It trades the current
  rehydration/zero-trust model for ecosystem pushdown and
  must be evaluated directly against ADR 020, ADR 022,
  ADR 039, ADR 042, and the PBC execution model — its own
  ADR.
- **Claim S3 Select or provider-native filtering parity.**
  ADR 043 writes encrypted custom slab objects, not
  provider-queryable Parquet/CSV/JSON. Any provider-native
  filtering path is a separate cold-tier ADR and inherits
  ADR 020's tradeoff space.

## Forward-compatibility analysis

ADR 043 keeps Brainstorm 011's V2/V3 path open and the LSM
forward-compat analysis from ADR 042 untouched.

**L2 columnar fetch (sovereign pushdown).** ADR 043's
per-buffer LZ4 encoding is necessary infrastructure for a
future ADR that implements column-aware Range-GETs against
encrypted L2 objects — the cold-tier story the council
surfaced after the first aggregation attempt. Each Arrow
IPC buffer (≈one per column, sometimes more) compresses
independently under per-buffer LZ4_FRAME, so the future ADR
can fetch and decompress only the buffers a query projects
without reading the whole record. Design surface for that
future ADR (sketch, not committed):

- Extend the ADR 022 unencrypted slab footer to carry a
  per-record column-buffer offset index, or define a
  two-phase fetch protocol that pulls the head 64 KiB
  chunk first to read the IPC FlatBuffer metadata, then
  issues a targeted chunk-aligned Range-GET for the chunks
  containing the target column buffers. Either path stays
  inside ADR 020's per-slab DEK + 64 KiB chunked AEAD
  discipline; granularity remains chunk-aligned, not
  byte-aligned.
- Wire column projection into the PBC executor (ADR 035)
  so decoded batches materialize only the projected
  columns.
- Compose with ADR 042 multi-row records (column buffers
  are per-batch; `record_row_index` projection still
  works) and ADR 043 per-buffer LZ4 (each column buffer
  decompresses independently from its containing chunk).

ADR 043 leaves this entire surface to its own ADR. Per-
buffer LZ4 (not record-level zstd) is the load-bearing
codec choice — buffer independence is the property the
future ADR depends on.

**LCE V2** can add a new `RecordBodyCompressionPolicy`
variant when a custom encoding format is pulled by
measurements. The writer and reader dispatch points
introduced here become the natural place for that
extension.

**LCE V3 / compute-over-compressed-data** remains a PBC
executor change. LZ4 buffers are not predicate-friendly,
so this ADR does not complicate the future opcode design.

**Page-directory point reads** can build on the per-buffer
Arrow layout later. ADR 043 still decompresses whole IPC
frames for point reads and does not pre-allocate directory
fields.

**Capacity-tier ZSTD** can be a future descriptor variant
with a new compression-level newtype if measured L2
capacity cost dominates L1 read bandwidth. This ADR
deliberately does not reserve the variant.

**LSM-style compaction** composes unchanged. Future merge
runs emit records using the target descriptor's
compression policy.

**Logical vs. physical descriptor fingerprints** can split
later if enough physical-only descriptor fields
accumulate. ADR 043 does not adopt the split.

## Deferred to follow-up ADRs

1. **L2 columnar fetch (sovereign pushdown).** Column-aware
   Range-GETs from L2 that preserve ADR 020 zero-trust
   while avoiding the "fetch all 50 columns to filter to
   the 5 the query projects" cost. Composes with ADR 022
   footer extension, ADR 020 chunked AEAD, ADR 042
   multi-row records, ADR 035 column projection in the
   PBC executor, and ADR 043's per-buffer LZ4.
   **High-priority** post-043 pickup.
2. **LCE V2: homerolled SIMD encodings.** Brainstorm 011
   V2. Pulled when post-043 metrics show the bandwidth /
   memory headroom remaining is worth the multi-quarter
   design surface.
3. **LCE V3: compute-over-compressed-data.** Brainstorm
   011 V3. Pulled with or after V2.
4. **Per-buffer page directory for O(1) point-read
   decompression.** Brainstorm 011 V2 sub-piece; may land
   independently if point-read amplification metrics
   surface a real cost.
5. **zstd codec for capacity-tier L2.** When and if
   measured L2 capacity cost dominates L1 bandwidth cost
   on real workloads.
6. **Per-column compression policy.** When the LCE V2 ADR
   opens, designed coherently with column-aware encoding
   cascades.
7. **Wire-frame compression** (control-server schema
   fetch, data-server raw-batch responses). Separate
   transport ADR when measured.
8. **Logical / physical descriptor fingerprint split.**
   Pulled when descriptor accumulates enough physical-only
   fields to justify the split.
9. **Compression-only operator command.** The existing
   layout-recluster path (ADR 042 Phase 7) already covers
   the capability; a dedicated command is operator sugar.

## References

- ADR 011: auto-adaptive transport — bounds the wire-side
  compression decision space outside this ADR.
- ADR 020: L2 envelope encryption and DEK lifecycle —
  compress-then-encrypt order; AEAD wraps the IPC frame
  opaquely; per-slab DEK + 64 KiB chunked AEAD discipline
  is the future-column-fetch ADR's chunk granularity.
- ADR 022: L2 slab format, footer-indexed range reads —
  slab-footer CSB stays uncompressed at fixed offsets;
  future column-fetch ADR extends the unencrypted footer.
- ADR 030: deployment profiles and support envelope — no
  filesystem-level compression assumption.
- ADR 035 / 036: PBC predicate protocol — sees decompressed
  batches; opcode catalog unchanged. Future column-fetch
  ADR adds column-projection wiring.
- ADR 037: tenant table schemas — descriptor surface gains
  `record_body_compression_policy` field.
- ADR 038: column-aware locality V1 — slab admission
  unchanged; decompression happens after admission.
- ADR 039: slab-footer column statistics V1 — stats fold
  over decompressed rows; CSB itself stays uncompressed.
- ADR 040: aggregate pushdown — protocol unchanged; folding
  pays per-record decompression.
- ADR 041: project-name decoupling — names follow brand-
  clean discipline.
- ADR 042: analytical multi-row slab fragments — `Lz4Frame`
  is the layout-driven default for `MultiRow`; coherent
  compaction (ADR 042 §13) is the convergence primitive;
  `max_record_bytes` enforcement axis (§3) extends ADR 042
  §10.
- Brainstorm 011: lightweight columnar encodings (Vortex
  playbook) — LCE V2/V3 compose by replacing the codec at
  the same descriptor field.
- `plans/conventions/newtype-units.md` — `CompressionRatio`.
- `plans/conventions/no-premature-backward-compat.md` — no
  reserved enum slots; no envelope version bump; no
  `#[serde(default)]` shim for the new descriptor field.
- `plans/conventions/no-version-suffixes.md` — types are
  `RecordBodyCompressionPolicy`, no V1 suffix.
- `plans/conventions/no-project-name-in-protocols.md` —
  enum, descriptor field, metric segments stay brand-clean;
  codec label values (`uncompressed`, `lz4_frame`) are
  functional.
