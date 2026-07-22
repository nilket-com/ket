# Specification: Byte and Text Semantics

Issue-inventory item 4 of `plans/rune-scripting.md` (§3.2 and the bounded-read model of §2.8). This
spec defines the **engine-level** contract for reading and mutating document bytes: what a range and a
line mean, where UTF-8 validation happens, what a caller observes at an invalid boundary or against
cold storage, and how work, output, and failure are bounded and reported.

It is deliberately **Rune-independent**, exercised through the existing engine API, and is the contract
`plans/rune-buffer-api.md` (item 9) thinly expresses. Object identity is settled and inherited:
`BufferId` is the durable buffer identity, `ViewId` scopes cursors/views, `StateId` is a content
version, roots/`NodeId`s stay internal (`plans/object-identity.md`, `buffer-store.md`, `caret-anchors.md`).

**Scope: these contracts apply to `TextBuffer`.** Synthetic buffers (`FileBrowser`, `MarkdownPreview`,
`session.rs:194`) have separate narrower capabilities, out of scope.

## 0. Prerequisites — prior engine stages, not incidental implementation

Reads are bytes-first today, but **edits and undo are not**, and the edit path **hides blocking I/O**.
The following are explicit prior stages this contract depends on; each is independently testable:

1. **`TextDelta` becomes byte-authoritative.** It stores `String` today (`undo.rs:5`). Migrate to
   `Vec<u8>`/`Arc<[u8]>` **throughout undo, replay, registers, search, and view fan-out**, so
   `replace_bytes` (§9) round-trips arbitrary bytes.
2. **A byte-exact splice/delete primitive.** `commit_delete` delegates to `delete_backwards`, which
   deletes **UTF-8-width groups** by scanning continuation bytes (`mutation.rs:459`, test
   `:928`) — an exact byte range would **overshoot its start and trip an invariant assertion**.
   `replace_bytes` needs a primitive that deletes exactly `[start, end)`. `commit_insert` must accept
   **bytes**, not `&str` (`edit.rs:200`).
3. **Mutation preflight hydrates before commit.** The edit path reads cold backing **synchronously**
   (`read_slice_sync_for_edit`, `edit.rs:205`) and physical insert/delete resolves leaves
   synchronously (`split_node_pvp`/`delete_backwards`, `mutation.rs:341,459`). The commit must begin
   only when every byte and metric the splice touches — **including the insertion leaf and the UTF-8
   boundary windows** — is resident and exact; cold touched leaves hydrate first (§9).
4. **A persistent, generation-checked cold-read outcome cache** (§5), so a failed read is *observed*
   on retry rather than silently resubmitted.

## 1. Ground truth this builds on

- **Bytes primary, lines derived.** Read primitive `read_loaded_slice(root, DocByte, DocByte)`
  (`uast/projection.rs:414`); length `get_total_bytes(root) -> u64` (`registry.rs:1116`). `DocByte`,
  `ByteLen`, `DocLine`, `DocLineCount` are **all `u64`** — there is no `u32` line ceiling.
- **`(line, col)` today is a display column** (`line_col_from_doc_byte_sparse`, `cursor.rs:243`):
  `DocDisplayCol` is **`u32`, tab-expanded** (`support.rs:44`) and **overflows on a huge line.** §2.2
  replaces the script coordinate with a `u64` line-relative byte offset.
- **Content is device-backed.** Leaves carry a `content_generation` (`bump_content_generation`,
  `mutation.rs:341`); cold reads need SVP I/O that must not block the engine thread. **Cold I/O
  failure emits no refresh today** (`svp/resolver/blocking.rs:84`, `resolver.rs:421`) and queue
  admission can fail synchronously (`resolver.rs:288`).
- **The whole-document cap** `MAX_WHOLE_DOCUMENT_SNAPSHOT_BYTES = 256 MiB` declines before allocating
  (`io.rs:11,443`), with an address-space guard (`io.rs:457`).
- **Text mutation seam:** `mutate_text_buffer(_, acting_view_id: ViewId, _)` (`session.rs:2718`) —
  buffer resolved from an **acting view**; `mutate_buffer` is synthetic-only.
- **`StateId`** is a per-buffer, monotonically-allocated **content-version token, not reused across
  reloads** (`caret-anchors.md` §3.3); it is not an identity — `current_state_id` moves back on undo
  and forward on redo (revisits existing tokens).

## 2. Coordinate contract

### 2.1 Half-open byte ranges

`[start, end)` over `DocByte`; `start == end` is empty (a valid insertion point). `total_bytes` is the
one length; `byte == total_bytes` is the append position. **Reads** are total: endpoints clamp to
`[0, total_bytes]`, `start > end` reads empty. **Mutations are strict (§9), not clamped.**

### 2.2 Lines are bytes-first

- **Line number:** zero-based `DocLine(u64)`. Line `L` = `[line_start(L), line_start(L+1))`; its
  terminating newline belongs to `L`; the last line has none.
- **Line-relative position ("column"):** **byte offset within the line**, `byte - line_start(L)`, a
  `u64`. This is the coordinate reads/edits use; it cannot overflow.
- **Display column** (`DocDisplayCol`, tab-expanded, `u32`, fallible/bounded) is **separate render
  metadata**, never the text coordinate a script addresses bytes with.
- **Line count** = `newline_count + 1`: empty document = **1**; a trailing newline yields an empty
  final line that **is** counted.
- **Line lookup is strict, not clamped** (unlike byte reads, §2.1). A `DocLine` outside `0..line_count`
  is **`LineOutOfRange`** — never silently clamped to the first/last line. Bytes clamp because a
  byte offset past EOF still names the unambiguous append position; a line number past the end names no
  line, so clamping would fabricate a location.

## 3. Reads are bounded and bytes-first

- **No implicit whole-document read**; every content read needs an explicit range or `max_bytes` (§7).
- **A single read/chunk is capped at `MAX_READ_BYTES` (§10)**; over-cap is a **`ReadError` (rejected,
  not truncated)** — the caller chunks (§8). Preflighted before reading.
- **Reads return raw bytes**, **byte-exact** (including a split UTF-8 sequence at either edge); no
  silent widening or repair.

## 4. Text versus bytes, on read

- Conversion is explicit, never implicit:
  - **strict** — `Err` on invalid UTF-8, reporting the **absolute `DocByte`** of the first bad byte.
  - **lossy** — U+FFFD substitution. A read is already ≤ `MAX_READ_BYTES`, and U+FFFD expands each bad
    byte ≤ 3×, so **lossy output is ≤ 3 × `MAX_READ_BYTES` structurally** — no separate reject cap; the
    ceiling follows from the input cap.
- A bytes-first file reads fully as bytes and simply fails strict decode.

## 5. Readiness and failure — one model with liveness

```
Result<Readiness<T>, ReadError>
Readiness<T> = Ready(T) | NotReady { waiting_on }
waiting_on   = LineIndexBuilding | ColdExtent { range } | LineNewlineHydration { line }
ReadError    = TooLarge | InvalidUtf8 { absolute_offset } | QueueFull | ScanLimitExceeded
             | ColdReadFailed | StaleVersion
             | NotResident | InvalidChunkSize | LineOutOfRange | WorkLimitExceeded
```

- **Liveness invariant:** `NotReady` is returned **only when the resolving work is admitted or already
  in flight**, and **completion *or* failure always wakes** the engine via `InternalRefresh`
  (`host.rs:72`) — requiring the prerequisite that cold-I/O failure emits a refresh.
- **Hydration granularity is whole-leaf (decision, not left open).** A cold API read hydrates the
  **complete physical leaf/leaves** its range touches; initial leaves are bounded at 1 MiB, and a
  hydrated leaf becomes visible through the existing `virtual_data` read path. This is what makes
  `(NodeId, content_generation)` a *complete-request* key. Baryon also has **partial-range residency**
  (`request_dma_range`), where one subrange failing must not poison unrelated bytes in the same node
  generation — that model would require the key to carry `(extent_offset, request_length)` and
  `read_loaded_slice` to consume partial residency. **This API chooses whole-leaf**; the two are not
  interchangeable and the partial path is out of scope here.
- **Persistent, generation-checked cold-read outcome cache (prerequisite 4).** An admitted whole-leaf
  read stores its outcome keyed by `(NodeId, content_generation)`: **admitted ⇒ eventual success or a
  *recorded* failure**; either wakes the engine; a retry after the wake observes `Ready` or
  `ColdReadFailed` and **never re-submits the same generation in a loop**. The entry is cleared when the
  leaf's `content_generation` changes or an explicit retry is requested.
- **Conditions no work can resolve are errors, not `NotReady`:** `QueueFull` (admission,
  `resolver.rs:288`), `ColdReadFailed`, and **`ScanLimitExceeded`** for a **resident** in-leaf scan
  past `MAX_LINE_SCAN_BYTES` (hydration cannot make a resident 10 GiB scan cheaper — retrying would
  loop; the caller uses byte-offset addressing instead).
- **Topology work is bounded, because native host work cannot be interrupted.** `MAX_READ_BYTES` does
  not bound traversal: a 1 MiB range can cross a pathological number of tiny leaves or structural
  nodes. A **traversal/fragment budget** (`MAX_OPERATION_NODES`, §10) bounds node/fragment visits for
  reads, line resolution, snapshot preflight, and mutation preflight; exceeding it is
  **`WorkLimitExceeded`** (mutations: an `EditError` of the same class). **Hydration admission also
  bounds the touched-leaf/request count** (`MAX_HYDRATION_LEAVES`, §10), so one operation cannot admit
  an unbounded fan of DMA requests.
- **Line-byte resolution is metrics-first.** `line_start(L)` is found from per-leaf **newline metrics**
  plus **bounded hydration of the one leaf containing the target newline** — not a whole-line display
  scan (those are gone with §2.2). `MAX_LINE_SCAN_BYTES` bounds only the **unavoidable in-leaf scan**
  to locate a newline within that leaf; a cold such leaf defers as `NotReady { LineNewlineHydration }`.
  Any display-column metadata has its **own** separate cap and is never on the text-coordinate path.

## 6. Sparse and cold reads

Reads serve resident/virtual bytes synchronously. A range hitting a **cold** extent returns
`NotReady { ColdExtent }` **iff** hydration is admitted (else `QueueFull`), resolving on a later refresh
to `Ready` or `ColdReadFailed` via the outcome cache (§5) — never blocking, never fabricating bytes.
Small/fresh buffers are fully resident → immediate `Ready`.

## 7. Snapshot preflight

- `snapshot(max_bytes)` **fails before reading** if `total_bytes > max_bytes` (`TooLarge`, both sizes)
  or `max_bytes > 256 MiB`; address space guarded via `usize::try_from` (`io.rs:443,457`).
- **Resident-only, whole-document residency preflight.** Before allocating the output, verify the
  **entire** document is resident **under the same residency guard used to collect it** — otherwise a
  single cold leaf near EOF triggers a 256 MiB allocation only to then fail. A non-resident document
  returns **`NotResident`** immediately, with no output allocation. Streaming a snapshot through a
  bounded background job is the future lift (`rune-scripting.md` item 7), named and deferred.

## 8. Chunking and paging — with a version contract

- **Chunk cursor is explicit:** `{ range: [start, end), position: DocByte, state: StateId,
  max_chunk_len: ByteLen }`. `read_chunk(cursor) -> Result<Readiness<{ bytes, next: DocByte }>, ReadError>`
  reads at most `min(max_chunk_len, MAX_READ_BYTES)` from `position`, advancing `next`.
- **Version contract:** if the buffer's current content state ≠ the cursor's captured `StateId`, return
  **`StaleVersion`** — a mutation between pages is a clean error, never skipped/duplicated bytes.
  (Equality means the content **is** the captured state; undo/redo may revisit that token, which is
  safe because the bytes are then identical.)
- **`max_chunk_len == 0`:** on a **non-empty remaining range** → `InvalidChunkSize` (a no-progress
  cursor invites infinite loops); on an **already-exhausted/empty range** → `Ready(empty, done)`.
- **Lazy line iteration** carries the same captured-`StateId` contract (`StaleVersion` **when the
  current content state differs** from the captured `StateId` — an undo/redo back to that exact token is
  safe, the bytes are identical), yields a bounded per-line view (capped prefix + truncation marker, or a chunk
  cursor for over-cap lines), pages lazily, and surfaces `NotReady`/`ReadError` per step.

## 9. Mutation behavior

- **One seam, explicit acting view.** All text mutations go through `mutate_text_buffer` with an
  **explicit acting `ViewId`** (`session.rs:2718`): a script edit is view-scoped, the acting view
  supplies the undo caret snapshot, siblings rebase by affinity. (A buffer-neutral path with its own
  undo-cursor policy is deferred, §12.)
- **Preflight returns `Result<Readiness<MutationOutcome>, EditError>`.** The preflight is **mutation-free
  and may be `NotReady`:** cold touched leaves — **the insertion leaf and the UTF-8 boundary windows
  included** — must hydrate first (admitted → wake, per §5). **The infallible commit begins only when
  every byte and metric the splice needs is resident and exact**, so the commit never blocks and bytes
  and `StateId` advance together or neither moves.
- **Two operations:**
  - **`replace_bytes(range, bytes)`** — exact byte endpoints, **arbitrary** replacement bytes, **never
    clamps**, via the byte-exact primitive (prerequisite 2). The byte-authoritative primitive.
  - **`replace_text(range, text)`** — replacement must be valid UTF-8 **and** both endpoints must be
    char boundaries.
  - Insert (`range` empty) / delete (empty replacement) are the degenerate cases.
- **`replace_text` boundary validity is *local*, not whole-document** (validating a huge/cold document
  as UTF-8 is unbounded and undefined on a byte-authoritative file). The context window is
  **`[max(0, endpoint − 3), min(total_bytes, endpoint + 3))`** — at most the 3 preceding bytes **and 3
  bytes beginning at the endpoint.** The trailing bytes are required: a scalar can *span* the endpoint
  (e.g. `E2 | 82 AC` with the split before `82`), and only the following continuation byte(s) prove it
  is a well-formed multi-byte scalar the endpoint falls inside. An endpoint is a boundary iff it does
  not fall inside such a scalar; this **equals `str::is_char_boundary` on a valid document** while
  staying defined around invalid bytes. A cold endpoint window is `NotReady` (admitted) or an
  `EditError`. An off-boundary endpoint is **`EditError::NotUtf8Boundary`** — the range is never
  silently widened.
- **Mutations return `EditError`, not `ReadError`.** `EditError = NotUtf8Boundary | RangeInvalid |
  TooLarge | BatchTooLarge | NotUtf8Text | WorkLimitExceeded | QueueFull | ColdReadFailed`. **Strict
  range rules:** `start > end`, or either endpoint `> total_bytes`, is **`RangeInvalid`**; append at
  exactly `total_bytes` is valid.
- **Caps, per-splice and aggregate:** a single splice is bounded by `MAX_EDIT_BYTES`; a **batch** is
  additionally bounded by `MAX_BATCH_DELTAS` and cumulative `MAX_BATCH_BYTES` (deleted + inserted) — per-
  splice caps alone do not bound a 1 024-delta native call. All preflighted before mutating.
- **Atomicity is per commit, not per undo group:** one splice = atomic; one preflighted multi-delta
  batch = atomic; **multiple separate calls coalesced into one undo entry = non-transactional** (undo
  grouping coalesces history, it is **not** a rollback — a later call's failure does not revert earlier
  committed calls). Each committed call preserves invariants (`rune-scripting.md` §3.12).
- **Bytes-first, affinity.** `replace_bytes` does not force UTF-8 or alter other bytes
  (`object-identity.md` §6.8). Caret insertion ambiguity resolves by affinity; deltas satisfy the
  descending, non-overlapping contract `transform_cursor_byte` needs (`caret-anchors.md` §6).

## 10. Constants (proposed; calibrate against the corrected op shapes)

| Limit | Value | Basis |
|---|---|---|
| `MAX_READ_BYTES` (single read/chunk) | 1 MiB | one synchronous output alloc; resolver transfer size |
| `MAX_EDIT_BYTES` (single splice) | 1 MiB | symmetric write cap, preflighted |
| `MAX_BATCH_DELTAS` / `MAX_BATCH_BYTES` | 1 024 / 16 MiB | bound a many-delta batch beyond per-splice caps |
| `MAX_LINE_SCAN_BYTES` (unavoidable in-leaf newline scan) | 1 MiB | resident overflow → `ScanLimitExceeded` |
| `MAX_OPERATION_NODES` (node/fragment visits per op) | 65 536 | bound traversal a byte cap can't → `WorkLimitExceeded` |
| `MAX_HYDRATION_LEAVES` (touched leaves admitted per op) | 256 | bound the DMA-request fan of one operation |
| lossy decode output | ≤ 3 × `MAX_READ_BYTES` (structural) | U+FFFD ≤3× expansion; no separate reject |
| `snapshot` ceiling | 256 MiB (`io.rs:11`) | reuse the whole-doc decline |
| per-line iterator cap | caller-supplied, ≤ `MAX_READ_BYTES` | a line can be tens of GiB |

The 1 MiB figures are directionally sensible (resolver transfer size) but stay **proposed** until the
corrected `replace_bytes`/`replace_text`/`read_chunk` shapes are benchmarked.

## 11. Testing (through the existing engine API, no Rune)

- Ranges/coords: empty range, EOF/append, over-large `end` clamps, `start > end` empty; over-`MAX_READ_BYTES`
  → `TooLarge` before alloc; line count of empty (1) and trailing-newline docs; line-relative offset is
  `u64` on a synthesized huge line; a resident over-cap in-leaf newline scan → `ScanLimitExceeded`; a
  `DocLine` `== line_count` and `> line_count` → `LineOutOfRange` (**no** clamp to the last line).
- Work bounds: a range/preflight crossing more than `MAX_OPERATION_NODES` tiny leaves → `WorkLimitExceeded`
  (read) / same-class `EditError` (mutation); an operation touching more than `MAX_HYDRATION_LEAVES` cold
  leaves is rejected rather than admitting an unbounded DMA fan.
- Decode: non-UTF-8 fixture reads as bytes; strict fails at an **absolute `DocByte`**; lossy output ≤ 3×;
  a code-point-splitting read is byte-exact.
- Readiness/failure: still-building index → `NotReady`→refresh→`Ready`; an admitted cold read whose I/O
  fails produces a refresh and a **recorded** `ColdReadFailed` on retry (no resubmission loop); a full
  queue → `QueueFull`.
- Snapshot: `total_bytes > max_bytes` / `max_bytes > 256 MiB` → `TooLarge` before alloc (reuse the 200 GiB
  test, `io.rs:864`); a document with **one cold leaf near EOF** → `NotResident` with **no** 256 MiB
  allocation.
- Version/chunk: `read_chunk`/line iterator → `StaleVersion` when a mutation lands between pages;
  `max_chunk_len == 0` → `InvalidChunkSize` on a non-empty range, `Ready(empty, done)` when exhausted;
  explicit cursor fields round-trip.
- Mutation: `replace_bytes` round-trips arbitrary non-UTF-8 bytes through undo (post-prerequisites);
  `replace_bytes` deletes an **exact** range without `delete_backwards` overshoot; `replace_text`
  rejects an off-boundary endpoint (`NotUtf8Boundary`) instead of widening, and its boundary test equals
  `is_char_boundary` on a valid doc; **every interior boundary of a 2/3/4-byte scalar** rejects (an
  endpoint before each continuation byte, incl. the `E2 | 82 AC` split that needs the *trailing* window
  byte to prove it is inside a scalar), while truncated/malformed lead bytes near the endpoint do not
  spuriously reject a real boundary; a cold touched leaf makes preflight `NotReady`, and commit runs only
  once resident; `MAX_EDIT_BYTES`/`MAX_BATCH_*` reject before mutating; a preflight-failing batch leaves
  bytes and `StateId` unmoved; three coalesced calls do **not** roll back when the third fails; a mutation
  rebases a second view's carets by affinity.

## 12. Non-goals / deferred

- Rune handles, stale-handle resolution, focused-buffer conveniences, optimistic `StateId` checks, the
  host-call quota — item 9.
- Async delivery of `NotReady`/cold content to a synchronous script consumer — item 9, pending the
  event/job substrate (items 6–7).
- A **buffer-neutral** mutation path and its undo-cursor policy — deferred; the default requires an
  acting `ViewId` (§9).
- **Streaming** snapshots/cold reads via a bounded background job (item 7) — synchronous forms return
  `NotResident`/`NotReady`.
- Multi-caret visual selection, fold storage, `MAX_CURSORS` (`caret-anchors.md` §13).

## 13. Implementation order

Each step is independently testable and lands before the next depends on it:

1. **Byte-authoritative `TextDelta`** (prerequisite 1) — undo/replay/registers/search/fan-out on bytes.
2. **Exact byte splice/delete primitive** (prerequisite 2) — deletes `[start, end)` without
   `delete_backwards` UTF-8-width overshoot; `commit_insert` takes bytes.
3. **Hydration + outcome tracking** (prerequisites 3–4, §5–6) — whole-leaf hydration, the
   `(NodeId, content_generation)` outcome cache, and the cold-I/O-failure refresh.
4. **Bounded read / coordinate layer** (§2–4, §7–8) — bytes-first lines, `Readiness`/`ReadError`,
   `MAX_READ_BYTES`/work budgets, snapshot preflight, chunk/line version contract.
5. **Non-blocking mutation layer** (§9) — `replace_bytes`/`replace_text`, local boundary window,
   mutation preflight over the hydration layer, `EditError`, batch caps.
6. **Benchmark / constants gate** (§10) — calibrate the proposed limits against the corrected
   `replace_bytes`/`replace_text`/`read_chunk` shapes before finalizing.
