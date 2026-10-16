# Huge-file scaling: 80–200 GiB documents

## Objective

Baryon must open, navigate, edit, and save 80 GiB SQL dumps and 200 GiB CSV files without
loading the whole document into memory, overflowing document coordinates, or allocating one
object per line. Time to the first usable viewport must depend on the viewport and a small amount
of metadata, not on total file size.

This plan addresses six independent limits in the current design:

1. document-wide byte and line quantities are still stored as `u32` in several layers;
2. ingestion creates one permanent ECS leaf per 64 KiB while the registry has a fixed one-million
   node arena;
3. an exact line count requires inspecting every byte unless a reusable index already exists;
4. the production coordinate-lookup paths walk a flat sibling chain and may synchronously inflate
   every preceding leaf;
5. several feature paths still reserve or materialize memory proportional to total lines or bytes;
6. clean physical leaves loaded for old viewports remain resident without a byte budget or eviction.

The status bar line count is a physical document-line count (`\n` count plus Baryon's final-line
semantics), not a CSV logical-record count. Quoted CSV fields may contain physical newlines.

## Current baseline

The sparse projection already has several properties worth preserving:

- absolute document bytes use the `u64`-backed `DocByte` type;
- physical leaves refer to backing-file regions through `SvpPointer` rather than owning the full
  document;
- viewport DMA and mutations operate on bounded leaves;
- unloaded leaves can contribute aggregate entries to the layout sum tree rather than one entry per
  line, but construction still walks the registry and currently reserves capacity by total lines;
- the status bar now reports `counting lines` until the root metric is exact, then displays the
  aggregate count in O(1);
- the background metric scanner uses 16 disjoint lanes, so it reads each 64 KiB chunk once rather
  than running 16 overlapping scans.

The current viewport residency is sparse only initially: hydrated physical leaves accumulate in
`virtual_data` and are not evicted as navigation moves elsewhere.

The first-viewport objective is not yet met. Measured on 2026-10-15 (release, sparse fixtures,
`crates/libaryon/plans/impl/startup-latency.md` section 10), time to the first usable viewport is
linear in cold extents: about 1 ms per GiB, 79 ms at 64 GiB and 208 ms at 200 GiB.

Those properties are necessary but not sufficient for the target sizes.

## Scale findings

### F1 — root byte metrics truncate above 4 GiB

`SpanMetrics.byte_length` is `u32`, and ingestion currently initializes the root with
`file_size as u32`. A leaf-local byte length can safely remain `u32`, but a document or subtree
aggregate cannot. Exact 80 GiB and 200 GiB sizes are multiples of 4 GiB and can truncate all the
way to zero.

This affects more than display. Any operation using the root metric for EOF, range clamping,
selection, saving, or byte lookup can observe the wrong document extent.

### F2 — global line coordinates and metrics are `u32`

`SpanMetrics.newlines`, `DocLine`, `DocLineCount`, and sum-tree line contributions are `u32`.
The limit is 4,294,967,295 lines. A 200 GiB document crosses it whenever its average physical
line is roughly 50 bytes or shorter; a valid `x\n` file can contain over 100 billion lines.

Line deltas and intermediate arithmetic must be widened with the stored values. Widening only the
status projection to `u64` does not fix the underlying overflow.

### F3 — the fixed arena is too small

With one leaf per 64 KiB:

| File size | Physical leaves required |
|---|---:|
| 80 GiB | 1,310,720 |
| 200 GiB | 3,276,800 |
| Current arena | about 1,000,000 total nodes |

The arena is shared by roots, semantic grafts, edits, undo state, and every open document. Raising
the arena to four million nodes would consume hundreds of megabytes at startup and still leave a
fixed ceiling with little mutation or multi-tab headroom.

### F4 — exact line count is an I/O operation

Without prior metadata, an exact newline count requires inspecting every byte. A bounded-memory
scan is safe for RAM but still consumes 200 GiB of device bandwidth and may compete with viewport
reads or pollute the page cache. No in-memory data structure can remove this first-scan cost.

### F5 — production coordinate lookup is linear and can perform synchronous I/O

Ingestion appends every physical leaf directly under the document root. The root therefore owns a
flat `first_child` / `next_sibling` chain rather than a balanced hierarchy with prefix summaries.

The production line lookup path does not use the test-only `RootChildLineIndex`:

- `find_node_at_line_col` passes no root-child index and begins at the root;
- the traversal visits preceding leaves in document order;
- an unknown leaf metric calls `ensure_metrics_inflated`, which performs a synchronous backing-file
  read;
- `doc_byte_for_node_offset` independently sums all preceding siblings at each ancestor.

Consequently, a fresh `goto line` near EOF can synchronously read most of the file on the engine
thread, and cursor-to-byte projection can be O(leaves). Coarse extents reduce the constant but do
not repair the asymptotic behavior; repeated splitting grows the flat fan-out again.

The balanced layout `SumTree` does not currently solve this production projection path. It is
rebuilt from the flat registry, and the UAST lookup APIs above do not consume the test-only root
index. Coordinate lookup needs an authoritative production index or a genuinely balanced document
extent tree.

### F6 — hidden O(lines) and O(bytes) feature paths remain

The layout sum-tree builder emits aggregate entries for unloaded leaves, but first calls
`Vec::with_capacity(total_newlines + 1)`. The reservation alone is O(lines) and can attempt an
enormous allocation before aggregation saves any memory.

Several other paths can materialize a complete document or line collection:

- `read_document_snapshot` reserves `get_total_bytes(root) as usize` and streams the whole document
  into a `Vec`;
- whole-document `read_loaded_slice` callers include diff and some edit/navigation operations;
- search/substitute and shadow-generation paths take complete byte snapshots;
- structural semantic analysis and Markdown preview may request full snapshots behind size or
  feature-specific gates;
- text minimap construction recounts newlines with a narrowing cast, while byte-fallback minimap
  construction linearly walks every leaf;
- overview/line-index builders contain capacities derived directly from document line counts.

Some of these paths are intentionally limited to small documents, but the limits are not a single
enforced huge-file policy. Every user-visible feature must be classified as bounded/windowed,
streaming, indexed, explicitly size-gated with a clear status, or unsupported. No feature may
silently allocate by total document bytes or lines after a huge file is open.

### F7 — hot leaf residency grows with bytes visited

Viewport DMA stores each completed physical leaf in `virtual_data`. In the ordinary source-backed
read path, old clean viewport leaves are not evicted when they leave the viewport. Their bytes are
normally cleared only when save rebinds the document to a backing generation or when mutation takes
ownership of an edited leaf.

A read-only traversal across a huge file therefore accumulates every visited leaf in RAM. This
violates the objective even if initial opening is sparse. It also feeds back into the layout index:
loaded leaves emit per-line `LineMetric` entries, so retained residency can make index memory grow
toward O(visited lines).

Clean physical leaves remain reconstructible from their `SvpPointer` and validated backing-file
generation, so demotion can drop their resident bytes while preserving wide aggregate metrics and
index summaries. Dirty/virtual-only content is not reconstructible and must remain pinned or be
spilled to an owned shadow store before its memory can be reclaimed.

## Representation rules

Global and subtree quantities must be wide; bounded node-local quantities should stay compact.

```rust
// Bounded by physical/hot leaf size.
NodeByteOffset(u32)
NodeByteLen(u32)
LeafNewlineCount(u32)

// May span the entire document.
DocByte(u64)
ByteLen(u64)
DocLine(u64)
DocLineCount(u64)
DocumentNewlineCount(u64)
ByteDelta(i64)
LineDelta(i64)
```

`SvpPointer.byte_length` may remain `u32`: one physical extent must stay well below 4 GiB.
Conversions from document-wide to node-local coordinates must be checked at the boundary after a
specific leaf has been selected. There must be no lossy `as u32` conversion of document sizes,
line positions, or subtree summaries.

Two implementation shapes are viable:

1. **Transitional:** widen both fields of `SpanMetrics` to `u64`. This is mechanically simpler but
   adds per-node memory and blurs local metrics with subtree summaries.
2. **Preferred:** keep compact `LeafMetrics` and introduce a 64-bit `SubtreeSummary` or
   `DocumentMetrics`. Branch/root aggregation and the sum tree use the wide summary; leaf offset
   APIs retain their current `u32` domain.

The preferred split makes invalid cross-domain arithmetic harder and avoids paying for wide local
fields where they are unnecessary.

## Node-scaling options

### Option A — larger fixed physical leaves

Increasing initial ingestion leaves is the smallest tactical change:

| Leaf size | Leaves for 80 GiB | Leaves for 200 GiB |
|---|---:|---:|
| 64 KiB | 1,310,720 | 3,276,800 |
| 256 KiB | 327,680 | 819,200 |
| 512 KiB | 163,840 | 409,600 |
| 1 MiB | 81,920 | 204,800 |

512 KiB or 1 MiB admits one 200 GiB document within the current arena. This is a bridge, not the
final architecture. Larger leaves increase viewport hydration latency, resolver buffers, hot-leaf
memory, and the amount copied when a physical leaf is promoted for a small edit.

The ingestion extent size, resolver read size, and mutation rechunk size should become separate
constants before benchmarking. Changing the existing 64 KiB constant globally would couple three
different policies.

This separation requires a real resolver contract change. Today an oversized `SvpPointer` is
clamped to the fixed DMA buffer size; only a debug assertion reports the mismatch, so release builds
can silently short-read. Before larger extents are permitted:

- oversized whole-node requests must return a hard error in every build;
- requests must carry an offset and length within an extent;
- fixed-size transfer buffers must be independent of extent size;
- residency must represent covered subranges rather than treating one partial completion as a
  fully loaded `Option<Vec<u8>>` leaf;
- newline scan completion and viewport hydration must define their coverage separately;
- mutation rechunking must use its own bounded hot-leaf policy.

Simply raising the shared DMA constant would allocate larger buffers for every in-flight lane and
promote an entire large extent for each touched viewport/edit. It is acceptable only as a measured,
temporary experiment, not the Phase 4 design.

### Option B — dynamically paged ECS storage

Replace each fixed boxed component slice with pages allocated as node IDs grow. A page size such as
16,384 or 65,536 nodes allows direct mapping:

```text
page = node_id / PAGE_SIZE
slot = node_id % PAGE_SIZE
```

This preserves structure-of-arrays locality and stable `NodeId`s without reallocating or moving
existing component storage. Small sessions allocate only a few pages; large sessions grow on
demand. Allocation of a new page may use a narrow lock, while reads and writes to existing pages
retain the current direct-indexing model.

Paged storage removes the startup-memory penalty and fixed one-million-node failure, but a 200 GiB
file still creates 3.3 million nodes at 64 KiB. It should be paired with coarse cold extents.

### Option C — coarse cold extents that split on demand

Represent untouched backing-file regions with coarse physical extents, perhaps 4–16 MiB. When a
viewport or edit needs a smaller region, split only that extent into hot leaves around the target.
Unvisited regions remain coarse.

At 8 MiB per cold extent, a 200 GiB file needs only 25,600 initial nodes. Node growth then follows
the amount of the file visited or modified, not total file size.

Required behavior:

- a viewport request can hydrate a bounded subrange of a coarse extent;
- an edit splits at its byte boundaries and promotes only the affected hot region;
- siblings that remain unmodified continue to reference the original backing file;
- save streams physical and virtual extents in document order without materializing them;
- subtree byte and line summaries remain exact across splits, merges, undo, and redo;
- cold extents may coalesce again when safe, though coalescing is not required for the first cut.

This is the preferred steady-state representation. Paged ECS storage makes its mutation-driven
node growth safe.

## Production coordinate index

Every document needs an incrementally maintained prefix index over its extents. It may be a
productionized balanced `SumTree`, a B+ tree over document extents, or real balanced interior UAST
nodes, but it must support:

- select extent by absolute byte in O(log extents);
- resolve an extent/node plus local offset to absolute byte in O(log extents) or better;
- select extent by known document line in O(log extents);
- splice summaries after split, insert, delete, undo, and redo without rebuilding from the first
  leaf;
- represent line summaries as known or unknown without reading the extent;
- produce fixed-size minimap bands from summaries without walking every leaf per render.

Byte prefixes are known from file metadata at ingestion and can be indexed immediately. Line
prefixes become known from a validated persistent index or as background scan blocks complete.
Until the required line prefix is known, an exact line lookup must return a pending/unavailable
result and schedule asynchronous indexing; it must not synchronously read preceding leaves.

Promoting the existing `RootChildLineIndex` is a reasonable bridge only if it becomes production
code, includes byte prefixes, and is maintained across mutations. A flat vector with binary search
can provide O(log n) selection, but O(n) splice costs and full rebuilds make it transitional. The
steady-state extent tree should store wide byte/line summaries in balanced interior nodes.

Interactive lookup has a strict contract:

- coordinate queries never perform backing-store I/O directly;
- misses return explicit state to the engine, which queues bounded high-priority I/O and redraws
  after completion;
- an unknown line index is distinguishable from “line does not exist”;
- byte and line lookup never begin a traversal from byte/line zero merely because the target is
  cold.

During Phase 2 the existing physical leaves are still at most 64 KiB, so a pending lookup can queue
the current bounded whole-node resolver request. It does not depend on Phase 4 subrange residency;
Phase 4 is required before cold extents become larger than the transfer size.

## Resident data budget

Track resident source bytes explicitly and enforce a configurable session or per-document budget
with hysteresis. A clock or LRU policy is sufficient if accounting and pinning are correct.

Each resident range/leaf needs enough state to answer:

- how many bytes it contributes to the resident budget;
- whether it is visible, referenced by an in-flight operation, or otherwise pinned;
- whether it is clean and reconstructible from the current backing generation;
- whether it is dirty/virtual-only and requires shadow persistence before eviction;
- when it was last used for eviction ordering.

When the high-water mark is crossed, demote least-recently-used clean unpinned physical data until
the low-water mark is restored. Demotion drops resident bytes only; it must not discard newline
metrics, coordinate-index summaries, fold state, or the physical pointer. Reload after demotion
returns through the normal asynchronous resolver path.

Viewport, cursor, active edit, pending DMA, and any operation holding a byte slice must pin the
relevant coverage. Dirty leaves stay pinned in the first implementation. A later shadow-backed
resident cache may spill and evict dirty content, but correctness must not depend on that
optimization.

Layout-index shape must not depend permanently on residency. Evicting a leaf must allow its
per-line hot layout details to collapse back to the persistent aggregate summary, otherwise byte
eviction alone still leaves O(visited lines) metadata behind.

## Persistent sparse line index

The first exact line count needs one full scan. Persist its result so subsequent opens do not pay
that cost again. This index is not merely a status-bar optimization: it supplies cold-extent line
prefixes to the production coordinate index, allowing exact line lookup without inflating leaves.

For each fixed-size index block—1 MiB is a reasonable starting point—store its newline count:

```rust
struct BlockMetric {
    newline_count: u32,
    ends_with_newline: bool,
}
```

The terminal-newline bit is required for coordinate correctness: when a target line equals a
block's cumulative newline boundary, the line belongs to the next block only if the preceding
block actually ends in `\n`. A newline count alone cannot distinguish that case from a line that
starts after the block's last newline and continues across the extent boundary.

Add periodic cumulative `u64` checkpoints or a compact prefix tree. A 200 GiB file contains
204,800 one-MiB blocks, so raw four-byte newline counts require less than 1 MiB. Even with headers,
wide checkpoints, hashes, and checksums, the index should remain only a few megabytes.

The index supports:

- instant total line count after a validated reopen;
- binary-search `goto line` followed by scanning at most one index block;
- byte-to-line and line-to-byte lookup;
- correct minimap scaling before document leaves are hydrated;
- scan progress measured in indexed bytes rather than ECS nodes.

### Identity and invalidation

An index must be bound to the exact file generation. Store, where available:

- device and inode/file ID;
- file size;
- high-resolution modification time;
- index format version and block size;
- optional hashes of the first, last, and sampled interior blocks.

A mismatched identity invalidates the index. A clean append-only change may extend a validated
index in a later optimization; the first version should prefer a full rebuild over a potentially
wrong count.

### Edits

The persistent index describes the immutable backing-file generation. In-session virtual edits
contribute overlay deltas through the document tree. Only blocks touched by a saved rewrite need
new metrics in the resulting generation. A save must never leave stale metrics paired with new
bytes: it either publishes the new file and index together, or it discards the candidate index and
lets the next open rebuild from a mismatched identity.

Without a valid persistent index, the background scanner incrementally publishes block summaries
to the in-memory coordinate index. A requested line whose required prefix has not been indexed yet
remains pending; it never forces synchronous prefix inflation on the engine thread.

### Save durability

Crash atomicity and a bounded rewrite cannot both hold at these sizes. A temporary file plus an
atomic rename never destroys the original, but it copies the entire document — 200 GiB to change
one byte near EOF. Writing the tail in place is bounded, but it overwrites the user's only copy.

A bounded save must be a deliberate, documented capability, never something a document silently
falls into because of its leaf layout. So the trade is the user's to make, and it is refused by
default:

- **`:set savedurability atomic`** (the default) rewrites every save through a temporary file and an
  atomic rename, whatever the document costs. Nothing about a document's size or shape can put a
  torn tail on the user's file.
- **`:set savedurability bounded`** additionally permits the in-place suffix rewrite, and only then
  under both of the conditions below.

When the user has authorised it, the bounded path still applies a threshold:

- **Below `MIN_IN_PLACE_SAVE_BYTES`** (currently the 256 MiB whole-document limit) the save stays
  atomic anyway. At this size the copy is cheap and atomicity is effectively free, so it must not be
  traded away for nothing. This is the common case: an ordinary edited file becomes a single trailing
  virtual leaf, which is exactly the shape the bounded path would otherwise match.
- **At or above it**, a same-file edit confined to one trailing hot extent, behind an unchanged
  physical prefix, may rewrite only its suffix in place. This path is *not* crash-atomic. It
  retains the old bounded suffix and rolls back an in-process write failure, but a power loss
  mid-write leaves a partially rewritten tail. That is an accepted trade, because the alternative
  is rewriting the whole document.

Any other edit shape — a length-changing edit in the middle, a shifted physical suffix, a save to a
different path — falls back to the streaming rewrite regardless of size, because moving a physical
suffix requires copying it or filesystem-specific copy-on-write support.

Because the same setting can produce either method depending on the document, a save reports which
one it took. `save_document` returns a `SaveMethod`, and a bounded save says so on the status line:
consent to the risk in general is not knowledge that a particular file now carries it.

## Scanner scheduling and resource policy

The scanner must remain lower priority than interactive viewport I/O.

- Use bounded reusable buffers; never retain scanned file contents.
- Keep scan lanes disjoint and account for each block exactly once. Advance them through an
  explicit immutable-ingestion-range cursor rather than node-ID or sibling arithmetic, so edit-time
  splits and disconnects cannot strand a lane.
- Pause or throttle low-priority work while high-priority viewport requests are queued. A request
  priority is only real if something acts on it: a single FIFO hands a backend whatever arrived
  first, so a run of queued scan reads delays the viewport and the priority is never observed. The
  request queue must drain viewport work ahead of scan work. Reducing scan depth is not a
  substitute — that fixes priority inversion by destroying scan throughput.
- Cancel scans when their document closes or backing generation changes.
- Emit coarse progress notifications rather than one redraw per block.
- Avoid blocking the engine thread on scan completion.
- Consider sequential access hints and cache-dropping hints where supported, but keep correctness
  independent of platform-specific advice.
- Surface read failure as an unknown/incomplete count; never publish a partial count as exact.

On rotational storage, the scheduler should favor sequential block order. On NVMe, a modest number
of adjacent in-flight reads can improve throughput. Concurrency must be configurable or selected by
backend rather than baked into document semantics.

## SSD-safe synthetic testing

A 200 GiB test must not mean writing 200 GiB of zeros. Separate logical address-space coverage from
scanner throughput and from real-device performance. The default developer test suite must never
perform a full 200 GiB scan or save unless the developer explicitly opts in.

### Tier 1 — generated backing store, every normal CI run

Introduce a narrow backing-store interface below ingestion and the resolver:

```rust
trait BackingStore: Send + Sync {
    fn len(&self) -> u64;
    fn read_exact_at(&self, offset: u64, dst: &mut [u8]) -> io::Result<()>;
    fn identity(&self) -> BackingIdentity;
}
```

The production implementation wraps a file. A test-only `GeneratedBackingStore` exposes a logical
200 GiB length and fills only the requested buffer from a deterministic repeating pattern. It must
not allocate the logical document or touch the filesystem. Useful patterns include:

- all zero bytes for pure byte-address tests;
- fixed-width `x\n` rows for line counts well above `u32::MAX`;
- block-number text at deterministic offsets to catch truncation and wrong-extent reads;
- distinct markers at byte zero, both sides of 4 GiB boundaries, the midpoint, and EOF;
- invalid UTF-8 and newlines crossing read/extent boundaries.

Most huge-address tests should inject exact document metrics or a synthetic prebuilt block index.
They then exercise open, first viewport, random byte jumps, line lookup, split, edit, undo/redo, and
EOF clamping without scanning 200 GiB. The test fails if total generated bytes read exceeds a small
budget, such as 16 MiB, proving that the operation stayed sparse.

An eviction stress variant should traverse generated viewports across the entire logical 200 GiB
address space while retaining only a small rolling working set. A strided or sampled traversal that
exceeds the residency budget many times over is sufficient; a full sequential sweep of every 64 KiB
leaf is not required. It asserts peak resident bytes stay within the configured high-water mark,
clean leaves are re-readable after demotion, dirty/pinned leaves are not discarded, and hot per-line
layout metadata collapses with evicted bytes. This variant enforces the residency budget rather than
the sparse-operation read budget above, so it is exempt from the small read-byte limit.

This tier should cover both the normal and forced blocking resolver backends. It is fast enough for
`ci/check.sh` and developer `cargo test` runs.

### Tier 2 — verified sparse filesystem fixture, Linux CI

Create a regular file with `File::set_len(200 * GiB)` or `truncate -s 200G`. Extending a file this
way creates a hole on common Linux filesystems: it changes the logical size without allocating or
writing the intervening data blocks. Write only a few small marker islands at selected offsets with
`write_at`/`pwrite`.

Before Baryon opens the fixture, verify both facts from `stat`/`MetadataExt`:

1. logical length is exactly the requested size;
2. allocated blocks remain below a strict ceiling, initially 16 MiB.

If the filesystem does not support sparse files, the dedicated huge test must skip with a clear
message or fail before opening the fixture. It must never fall back to materializing zeros.

This test exercises the real file metadata, path registration, file descriptor, seek/read-at, and
near-EOF code. It must use an injected valid line index or a test option that disables automatic
metric scanning. It should read the marker islands and holes near the beginning, 4 GiB boundaries,
middle, and end, then make a bounded edit near EOF.

Do not test full save to another regular file in this tier: streaming a sparse source into a normal
destination can materialize 200 GiB. Save behavior at this scale should use a generated/discarding
writer test, or a separately implemented sparse-preserving writer with its own allocation checks.

### Tier 3 — scanner and index correctness at reduced scale

The newline scanner's correctness does not depend on the fixture being 200 GiB. Exercise its full
read path on a deterministic fixture sized to complete comfortably in CI—tens or hundreds of MiB,
not hundreds of GiB—and verify:

- every block is scanned exactly once;
- disjoint scan lanes cover all blocks without gaps or overlap;
- count and index output match an analytical expected value;
- cancellation, read failure, stale generation, and priority preemption behave correctly;
- a warm index eliminates the scan;
- the bytes-read counter equals the fixture's logical length.

Property tests can vary block size, short final block, newline placement, and completion order. This
provides stronger scanner coverage than repeatedly pushing 200 GiB of zeros through `memchr`.

### Tier 4 — explicit nightly/benchmark jobs

An optional private-runner job may perform a full logical 200 GiB sparse scan to catch elapsed-time,
counter-width, cancellation-latency, and long-running scheduling defects. Gate it behind an explicit
environment variable such as `BARYON_CI_HUGE=1` and keep its Rust tests ignored by default.

Run real allocated-data throughput benchmarks only on disposable or ephemeral storage provisioned
for that purpose. They are performance experiments, not ordinary correctness CI, and should report
bytes written/read and device type in their artifacts.

The repository-owned entrypoints should be:

```text
ci/check.sh                 normal generated-store and reduced scanner tests
ci/huge-file-check.sh       verified sparse integration tests
BARYON_CI_HUGE=1 ...        opt-in full logical scan/nightly checks
```

`ci/huge-file-check.sh` must install a cleanup trap, verify sparsity before launching Baryon, apply
a timeout, and print logical versus allocated bytes. It should refuse to run a destructive variant
when the opt-in variable is absent.

### Developer-machine safety rules

- Never create a huge fixture with `dd if=/dev/zero`, a zero-filled `Vec`, ordinary `cp`, or
  `fallocate` without hole-preserving flags.
- Never assume `/dev/shm` or `memfd` prevents SSD traffic; tmpfs pages may be swapped.
- Never run a whole-file checksum, parser, semantic pass, save, or line scan in the default sparse
  addressability test.
- Verify allocated blocks after writing marker islands and after the test edit.
- Place all fixtures under a unique temporary directory and remove them with a cleanup guard/trap.
- Default tests must enforce a small read-byte and write-byte budget through instrumentation.
- Full-scale scans and saves require a separate explicit opt-in and should target ephemeral CI
  storage, not a developer database disk.

## Delivery plan

### Phase 0 — guardrails and observability

1. Replace lossy size casts at ingestion boundaries with checked conversions or wide values.
2. Return a user-visible load error instead of panicking when node reservation fails.
3. Add counters for allocated nodes, cold/hot extents, scanned/indexed bytes, pending scan blocks,
   resident bytes, sum-tree rebuilds, and high-priority I/O latency.
4. Add synthetic tests that expose the current 4 GiB and line-count overflow without allocating
   giant buffers.
5. Remove the `total_newlines + 1` sum-tree reservation immediately; reserve/build by emitted
   aggregate entries or extents so partial metric inflation cannot trigger O(lines) allocation.
6. Coalesce or throttle render-path sum-tree rebuilds while background metric completions change
   `total_lines`; one leaf completion must not cause another full-leaf walk and allocation.

### Phase 1 — 64-bit document metrics and coordinates

1. Introduce `u64`-backed `DocLine`, `DocLineCount`, and document newline totals.
2. Widen sum-tree line contributions and query results.
3. Separate leaf-local metrics from wide subtree/document summaries, or widen `SpanMetrics` as a
   transitional cut.
4. Change edit deltas and aggregate updates to signed 64-bit arithmetic.
5. Audit every `as u32` from metadata, document bytes, subtree bytes, line positions, and counts,
   including minimap line counting.
6. Make `get_total_bytes` read a genuinely wide source rather than widening an already-truncated
   `u32` value.
7. Remove misleading dead-code allowances from load-bearing metrics so narrowing mistakes remain
   visible to compiler/lint review.
8. Preserve `u32` for `NodeByteOffset`, terminal dimensions, and other genuinely local domains.

### Phase 2 — production coordinate index and asynchronous lookup

1. Introduce an authoritative balanced prefix index over extents with wide byte summaries and
   known/unknown line summaries.
2. Populate byte prefixes at ingestion without reading file contents.
3. Promote the useful root-child test index behavior into production as a bridge, or route the
   projection APIs directly through the balanced extent index.
4. Make byte-to-extent, line-to-extent, and node-local-to-document-byte lookup O(log extents).
5. Incrementally splice summaries after split, insert, delete, undo, and redo.
6. Remove synchronous `ensure_metrics_inflated` calls from interactive coordinate lookup. Return
   pending state and queue the existing bounded (at most 64 KiB in this phase) whole-node I/O
   through the resolver instead.
7. Route cursor projection and byte-fallback minimap lookup through indexed summaries rather than
   repeated sibling walks.

### Phase 3 — reusable sparse line index

1. Define a versioned, checksummed sidecar format.
2. Build it during the existing background metric scan.
3. Validate and load it before scheduling a scan.
4. Publish completed block summaries incrementally into the production coordinate index.
5. Use it for status count and exact cold line/byte lookup without leaf inflation.
6. Make lookup beyond the indexed prefix explicitly pending rather than synchronously scanning.
7. Add cancellation, progress, invalidation, and atomic save/index publication.

### Phase 4 — resolver subranges and larger ingestion extents

1. Separate cold ingestion extent size, resolver transfer size, and mutation hot-leaf size.
2. Replace the release-mode short-read clamp with a hard checked error for invalid whole-node
   requests.
3. Add offset-plus-length subrange requests within a physical extent.
4. Track partial residency/coverage without marking a partially read extent fully loaded.
5. Keep scanner coverage independent from viewport residency.
6. Add resident-byte accounting, pinning, and budgeted clean physical-leaf demotion before larger
   extents are enabled.
7. Benchmark 256 KiB, 512 KiB, and 1 MiB initial extents.
8. Select the smallest size that opens 200 GiB with adequate mutation headroom and acceptable
   viewport latency.

### Phase 5 — bounded feature paths

1. Inventory every complete-document snapshot and document-derived `Vec::with_capacity`.
2. Give search, substitute, diff, Markdown preview, semantic analysis, folding, and minimap an
   explicit streaming, windowed, indexed, or size-gated implementation.
3. Require size gates to compare wide quantities before conversion to `usize`.
4. Make byte-fallback minimap generation O(fixed bands × log extents), not O(all leaves) per
   rebuild.
5. Add a shared huge-file capability policy so unsupported operations fail with a concise status
   instead of allocating or blocking unexpectedly.

### Phase 6 — dynamically paged registry

1. Page each structure-of-arrays component consistently by `NodeId`.
2. Allocate pages on demand and preserve stable IDs.
3. Make capacity exhaustion a recoverable error.
4. Verify concurrent resolver writes and engine reads across page boundaries.
5. Stress multiple huge tabs plus edit-generated nodes.

### Phase 7 — adaptive cold extents

1. Ingest the file as coarse physical extents.
2. Split extents for viewport hydration and edits using bounded subrange reads.
3. Update the production coordinate index incrementally for every split and splice.
4. Update saving, folding, undo, and redo for mixed extent sizes.
5. Ensure a one-byte edit does not copy or retain an entire multi-megabyte cold extent.
6. Ensure every split hot range participates in the same resident-byte budget and can collapse its
   hot layout details after clean demotion.

### Phase 8 — policy and tuning

1. Tune extent, index-block, buffer, and concurrency sizes on NVMe, SSD, HDD, Linux, macOS, and
   Windows.
2. Decide whether first-open exact counting is automatic, delayed until idle, configurable, or
   explicitly requested.
3. Add cache and I/O-priority hints where they materially help.
4. Document expected first-scan and warm-index behavior.

## Verification

### Correctness tests

- A synthetic document summary above 4 GiB retains its exact byte length.
- Byte lookup, EOF clamping, selection, save, undo, and redo work across the 4 GiB boundary.
- More than `u32::MAX` physical lines can be represented, navigated, and displayed.
- Line totals remain correct when newlines are inserted or deleted in hot leaves.
- Byte and line indexes remain correct across extent split, insertion, deletion, undo, and redo.
- Interactive line/byte lookup records zero direct backing-store reads; cold misses return pending
  state and complete through the resolver.
- An oversized whole-node resolver request fails in debug and release rather than returning a
  truncated buffer.
- Partial extent hydration never marks uncovered bytes resident or their line metrics known.
- Clean physical leaf demotion preserves metrics and reloads the same bytes asynchronously.
- Dirty, visible, in-flight, and explicitly pinned coverage is not evicted.
- Eviction can collapse hot per-line layout details without changing coordinate results.
- Physical extents split at the beginning, middle, and end without losing or duplicating bytes.
- Mixed physical and virtual extents stream-save byte-for-byte in order.
- A document below `MIN_IN_PLACE_SAVE_BYTES` always saves through the temporary file and rename,
  even when its leaf layout would match the bounded in-place path.
- An EOF-only edit to a huge document rewrites only its bounded tail and rereads no unchanged
  prefix bytes.
- A failed in-place suffix write restores the original suffix and length.
- Sum-tree and minimap construction allocate by extents/fixed bands, not total physical lines.
- Size-gated features reject a synthetic 200 GiB document before converting its size to `usize` or
  allocating a document-sized buffer.
- A partial or failed scan never appears exact.
- A valid sidecar is accepted; size, timestamp, identity, hash, checksum, and version mismatches are
  rejected.
- Closing or reloading a document cancels its old-generation scan.

### Scale tests

- Open generated 80 GiB and 200 GiB backing stores without integer truncation, arena panic, or
  allocation proportional to file bytes or line count, while enforcing a small read-byte budget.
- Open a verified sparse 200 GiB Linux fixture with tiny marker islands and confirm its allocated
  blocks stay below the safety ceiling.
- Render the first viewport before the background scan completes.
- Jump near EOF in O(log extents) without inflating or synchronously reading preceding physical
  extents.
- Edit near the beginning, middle, and end of a 200 GiB fixture; exercise save through a bounded
  generated/discarding sink unless the test explicitly provisions disposable output storage.
- Open multiple large documents while retaining explicit node and memory headroom.
- Reopen an indexed 200 GiB fixture and obtain an exact line count without rescanning it.
- Traverse generated viewports across the full logical 200 GiB range and keep peak resident bytes
  within the configured budget rather than proportional to bytes visited.

Generated and sparse files validate addressability and memory behavior but do not substitute for
throughput benchmarks. Scanner performance must also be measured against real allocated data on
representative disposable storage. Full-scale allocated-data tests are never part of the default
developer test gate.

### Performance invariants

- Resident source bytes are bounded by a configured budget plus explicitly pinned/dirty data, not
  by cumulative bytes visited.
- First-viewport memory is O(registry pages + hot viewport data), not O(file bytes) or O(lines).
- Initial node count is O(cold extents), not O(64 KiB chunks) in the final representation.
- Byte lookup, known-line lookup, and node-to-document projection are O(log extents) or better.
- Interactive coordinate lookup performs no synchronous backing-store I/O.
- Index mutation is bounded by tree height plus the number of changed extents, not total siblings.
- Sum-tree construction and minimap fallback memory are O(extents) and O(fixed bands), never
  O(document lines).
- No feature allocates O(document bytes) without an explicit, wide-value size gate.
- Exact first-open indexing performs at most one full-file pass.
- Warm-index open does not perform a full-file pass.
- A small edit copies only bounded hot data.
- High-priority viewport latency remains bounded while indexing is active, and stays bounded as
  scan depth grows: viewport I/O is never handed to a backend behind queued scan reads.
- A save below the in-place threshold is crash-atomic; above it, a bounded EOF save reads and
  writes O(hot tail), not O(document).
- Background metric progress causes bounded/coalesced index maintenance, not one full rebuild per
  completed leaf.

## Recommended decision

Do not solve the target by only raising the fixed arena size. Adopt 64-bit document metrics first,
then install a production balanced coordinate index whose interactive lookup path never performs
synchronous I/O. Persist a sparse per-block newline index so cold line lookup works without leaf
inflation and the unavoidable first scan becomes a one-time cost for an unchanged file generation.
After the resolver supports partial extent residency, use larger initial extents as a measured
bridge, move the registry to dynamic pages, and finally adopt coarse cold extents that split on
demand. Enforce a resident-byte budget with clean-leaf demotion before navigation can accumulate
unbounded hot data.

This sequence restores correctness before optimization, provides an earlier path to opening
200 GiB files, and ends with resource use proportional to the regions Baryon actually touches.
