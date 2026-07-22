# Specification: Rune Buffer/Cursor API (`baryon::*`)

**The buffer/cursor slice of umbrella issue-inventory item 9** of `plans/rune-scripting.md`. Item 9 also
names panes, options, keymaps, commands, and autocmds; **those are out of scope here** and item 9 is not
closed by this plan — this defines only reading and mutating buffer content and cursors from a script.

It is a **thin expression** of two already-settled engine contracts and adds no new core semantics:

- **Object identity** — `plans/object-identity.md`, `buffer-store.md`, `caret-anchors.md`: `BufferId`
  durable/never-reused, close+reopen ⇒ new id (stale handle errors, never re-resolves by path), `ViewId`
  scopes cursors/views, `StateId` is a content version (not identity), `CanonicalPath` dedups only,
  roots/`NodeId`s internal.
- **Byte and text semantics** — `plans/byte-text-semantics.md`: half-open `DocByte` ranges, bytes-first
  lines, `Readiness<T>`/`ReadError`/`EditError`, bounded reads/work, snapshot preflight, chunk/line
  version contract, `replace_bytes`/`replace_text` with hydrating preflight and infallible commit.

**The API translates; it does not decide.** Engine-level questions (what a boundary is, how cold storage
resolves, what a range means) **defer** to the contracts above. This spec answers only Rune-level
questions: the host ABI, how `NotReady` reaches a synchronous script, batch shape, the concrete value
surface, host quotas, and render effects.

Runs inside the VM lifecycle of `plans/rune-vm-lifecycle.md` (committed): `EditorHost` (`#[derive(Any)]`,
`name=Editor`) owns the `EditorSession`; `HostServices`/`HostBudget` bound host work (`facade.rs:20,66`).

## 1. Host ABI — the guarded editor is an explicit argument

Handles carry **only ids**; the lifecycle deliberately provides **no ambient session pointer**
(`facade.rs`). Therefore **every stateful associated function takes `&mut EditorHost` explicitly**; pure
token operations (equality, debug) may omit it.

```rune
let buf  = baryon::focused_buffer(editor)?;      // free fn, editor arg
let data = buf.read_bytes(editor, start, end)?;  // method, editor arg
let ver  = view.replace_bytes(editor, start, end, bytes)?;
let step = chunks.next(editor)?;                 // cursors too
buf == buf2                                       // pure token op, no editor
```

- **Registry/resolver reach.** Reads and hydration need the content registry and SVP resolver, which
  `EditorHost` does not currently hold (`facade.rs:61` — `session`, `services`, `staging` only). **Give
  the facade private `Arc<UastRegistry>` / `Arc<SvpResolver>` fields**, cloned from the same engine-owned
  handles used by `RuntimeContext`. They are **derived authority, not parallel state**, and are never
  exposed as Rune values. They deliberately do **not** live inside the mutex-protected
  `SharedHostServices`: reads must not hold the output/budget lock across registry traversal or resolver
  admission, and the compiler/I/O shims do not need editor data-plane authority.
- **ABI test (blocker gate):** prove a `#[rune::function]` associated native fn can accept
  `(&Buffer, &mut EditorHost, ...)` and resolve the buffer's content through the facade authority — this
  is the load-bearing assumption of the whole API and is validated before any surface is built.

## 2. Handles and the complete value surface

All registered as `#[derive(Any)]` opaque Rune values unless noted:

| Rune value | Wraps / is | Notes |
|---|---|---|
| `Buffer` | `BufferId` | identity token; `Eq` (id equality), `Debug`, `Clone` (copies the id) |
| `View` | `ViewId` (resolves its current `BufferId`) | as above |
| `Version` | `StateId` | `Eq`, `Debug`, `Clone`; **no ordering**; `undo`/`redo` may revisit a value |
| `Caret` | `{ byte: u64, affinity: Affinity }` | value struct, read-only |
| `Affinity` | enum (`before`/`after`, `caret-anchors.md`) | |
| `OnInvalid` | enum `Strict \| Lossy` | text-decode policy |
| `Edit` | `Edit::bytes(start,end,bytes)` / `Edit::text(start,end,text)` | declarative; payload stays Rune-allocator-backed (§5) |
| `ChunkCursor` | `{ BufferId, range, position, version, max_chunk_len }` | `next(editor)` — §4 |
| `LineCursor` | `{ BufferId, position, version, per_line_cap }` | `next(editor)` — §4 |
| `LinePage` | `{ line, start, end, bytes, truncated }` | one bounded line-iterator result; newline is part of the range |
| `BufferError` | typed error | `.kind() -> BufferErrorKind` — §3 |
| `BufferErrorKind` | high-level error enum | may carry a `NotReadyKind`, `ReadErrorKind`, or `EditErrorKind` |
| `NotReadyKind` / `ReadErrorKind` / `EditErrorKind` | bounded reason enums | no error owns an unbounded message/payload |
| `Bytes` | **Rune's existing `std::bytes::Bytes`** | not a new type |

- **Ranges are `start: u64, end: u64` argument pairs**, not another handle type — half-open `DocByte`.
- **`Buffer::version(editor) -> Result<Version, BufferError>`** gives optimistic edits a source version.
- **Equality:** `Buffer`/`View` compare by id; `Version` by content-version equality only. Cloning an
  opaque value copies its id/token — it never deep-copies engine state.

### Acquiring and crossing handles

This slice exposes only live **text** buffers/views:

```
baryon::focused_buffer(editor) -> Result<Buffer, BufferError>
baryon::focused_view(editor)   -> Result<View, BufferError>
view.buffer(editor)            -> Result<Buffer, BufferError>
buffer.views(editor)           -> Result<Vec<View>, BufferError>
```

- A focused synthetic buffer/view returns `NotTextBuffer` / `NotTextView`; it is not represented by a
  fake text handle. A `View` resolves its **current** buffer at each call — retargeting a live view does
  not mint a new `ViewId` or leave a captured `BufferId` inside the token.
- `Buffer::views` returns live attached text views in deterministic layout traversal order and is capped
  by `MAX_HANDLE_RESULTS`; overflow is `ResultLimitExceeded`, never an unbounded materialization.
- Opening/closing, all-buffer enumeration, and path lookup remain out of scope (§9). These four functions
  are enough to acquire the focused identities and cross the existing buffer↔view relationship.

## 3. One error ABI; explicit retry, not auto-recall

Both reads and edits can be `NotReady`, so signatures do **not** return bare `Bytes`/`Version`. The whole
API returns **`Result<T, BufferError>`**, one type:

```
BufferError.kind() -> BufferErrorKind
BufferErrorKind = NotReady(NotReadyKind) // admitted/in-flight; retry on a later turn
                | StaleBuffer | StaleView
                | NotTextBuffer | NotTextView
                | StaleVersion
                | ResultLimitExceeded
                | Read(ReadErrorKind) // TooLarge, InvalidUtf8, QueueFull, ColdReadFailed, NotResident,
                                      //   ScanLimitExceeded, InvalidChunkSize, LineOutOfRange,
                                      //   WorkLimitExceeded, …
                | Edit(EditErrorKind) // NotUtf8Boundary, RangeInvalid, BatchTooLarge, NotUtf8Text, …
```

- **Bounded numeric details remain inspectable** (e.g. the absolute `DocByte` of a strict-decode failure,
  a `TooLarge` size) via accessors returning small scalars — never unbounded blobs.
- The exact native return ABI is **`VmResult<Result<T, BufferError>>`**: expected failures occupy the
  inner Rune `Result::Err`; **broken invariants** and **hard host-quota exhaustion** (§6) use the outer
  `VmResult::panic` and unwind the script. Host functions remain total under `panic = "abort"` — no
  user-controlled path or invariant report uses Rust `panic!`/`assert!`.
- **`NotReady` is "return and explicitly retry on a later callback," not auto-recall.** `InternalRefresh`
  today only requests a render (`runtime.rs:2157`, `InternalEvent::Refresh => true`); it does **not**
  invoke Rune. Until the event bus (items 6–7) exists, a script that gets `NotReady` must return and be
  re-entered by a later user/event callback; **tests manually reinvoke**. The API never spins, sleeps, or
  awaits — that would freeze the single engine thread.

## 4. Reads

Thin wrappers over byte-text-semantics §3–4, §7–8; all bounds/errors are the engine's.

- **`Buffer::read_bytes(editor, start, end) -> Result<Bytes, BufferError>`**,
  **`Buffer::read_text(editor, start, end, OnInvalid) -> Result<String, BufferError>`** — one read,
  capped at `MAX_READ_BYTES`; over-cap is `Read(TooLarge)` (caller chunks). Reads clamp to `[0,
  total_bytes]`; mutations don't.
- **`Buffer::total_bytes(editor) -> Result<u64, BufferError>`**,
  **`Buffer::line_count(editor) -> Result<u64, BufferError>`**,
  **`Buffer::line_range(editor, line) -> Result<(u64,u64), _>`** (`Read(LineOutOfRange)` outside
  `0..line_count`, strict — never clamped).
- **`Buffer::read_chunks(editor, start, end, max_chunk_len) -> Result<ChunkCursor, _>`** and
  **`Buffer::lines(editor, per_line_cap) -> Result<LineCursor, _>`** return **explicit cursor values**.
  Each **stores its `BufferId`** and advances via an explicit mutable receiver:
  **`ChunkCursor::next(editor) -> Result<Option<Bytes>, BufferError>`** and
  **`LineCursor::next(editor) -> Result<Option<LinePage>, BufferError>`**.
  - They **do not** implement Rune's ordinary `NEXT`/`for` protocol — iteration needs the guarded
    `editor` argument, which that protocol cannot supply. Scripts loop with an explicit `while let`.
  - **Position advances only on `Ready`.** `NotReady`/`StaleVersion` leave the cursor unmoved so a retry
    resumes exactly. `StaleVersion` when content ≠ captured `Version`; `InvalidChunkSize` on a zero step
    over a non-empty remainder; `Ready(None)` when exhausted. A `LinePage` carries the full line range,
    a prefix of at most `per_line_cap` bytes, and `truncated`; callers page the full `[start,end)` with
    `read_chunks`. The terminating newline belongs to the page's range, matching byte-text-semantics §2.2.
- **`Buffer::snapshot(editor, max_bytes) -> Result<Bytes, _>`** — preflighted: `Read(TooLarge)` if
  `total_bytes > max_bytes` or the engine's 256 MiB cap; host-quota/allocation admission keeps its distinct
  outer failure (§6). `Read(NotResident)` (no allocation) if the whole document is not resident. Streaming
  is deferred (item 7).

## 5. Cursors and declarative mutations

Cursor **reads** are `View` methods scoped by `ViewId` (byte-text-semantics §9 acting view):
**`View::carets(editor) -> Result<Vec<Caret>, BufferError>`**,
**`View::primary_caret(editor) -> Result<Caret, BufferError>`**. The returned list is **bounded** by the
existing `MAX_CURSORS` (§6).

**Cursor mutation is out of scope in this cut.** "Motion helpers through existing commands" is not
implementable for an arbitrary `ViewId` without focus/runtime policy this slice does not define; **carets
are read-only here** and cursor motion is explicitly deferred (§7). Edits still move carets — via the
engine's own affinity rebase (`transform_cursor_byte`, `caret-anchors.md` §6), which the API does not
reimplement.

**Mutations are declarative — no host function calls Rune.** `edit_batch(closure)` is rejected: invoking a
Rune closure from inside a native host fn would re-enter the non-reentrant VM while holding the guarded
editor borrow. Instead the script builds a `Vec<Edit>` in **Rune-owned memory**, then makes **one host
call** that validates and commits:

```rune
let edits = [Edit::bytes(s0, e0, bytes), Edit::text(s1, e1, text)];
let ver   = view.edit(editor, edits)?;                  // unconditional
let ver2  = view.edit_if(editor, expected, edits2)?;    // optimistic
```

- **`View::replace_bytes(editor, start, end, bytes)`** / **`replace_bytes_if(editor, expected,
  (start, end), bytes)`** — exact endpoints, arbitrary bytes, never clamps. The optimistic convenience
  takes its range as a plain tuple because Rune's test-build native-function bridge supports five total
  arguments (including the receiver); this keeps the real-VM ABI testable without inventing a range
  handle.
- **`View::replace_text(editor, start, end, text)`** / **`replace_text_if(editor, expected,
  (start, end), text)`** — replacement valid UTF-8; both endpoints char
  boundaries by the engine's **local** window (`E2 | 82 AC` case, byte-text-semantics §9). Insert = empty
  range; delete = empty replacement. These are the single-`Edit` convenience over `View::edit`.
- **`View::edit(editor, edits)`** / **`View::edit_if(editor, expected, edits)`** — one host call validates
  the whole batch (per-splice and aggregate caps `MAX_BATCH_DELTAS`/`MAX_BATCH_BYTES`, overlap/descending
  order, UTF-8 boundaries); the `_if` form first requires current content == `expected`, else
  `StaleVersion`. Rune/native functions are fixed-arity, so this is **two names**, not an omitted optional
  argument. Both commit **atomically** and return the **new `Version`**; success means bytes **and**
  `Version` advanced together (infallible commit once resident).
- **Payload/accounting contract.** `Edit` owns `rune::runtime::Bytes` or Rune's allocator-backed `String`;
  its constructors consume those values and never copy them into an untracked Rust `Vec`/`String`.
  `View::edit*` accepts the Rune runtime vector, checks its length and every payload byte count before
  cloning/converting anything, then borrows payload slices while building the bounded engine batch. A
  `Vec<Edit>` therefore remains covered by Rune's allocation/retained-memory limits as well as the engine
  batch caps.
- **Optimistic concurrency** uses the `_if` forms. **Separate top-level calls are not transactional** even
  when undo-coalesced — the API surfaces no
  rollback the engine doesn't provide (byte-text-semantics §9).
- A cold touched leaf (insertion leaf / boundary window) makes the call `NotReady` (preflight admitted the
  hydration); the script retries on a later turn (§3).

## 6. Host quotas and render effects — exact integration

Engine caps bound a *single* operation; they do not bound a script issuing a million operations, nor do
they cause a caught successful edit to render. Both are Rune-level and bound here.

**Quota — extend `HostBudget` from one counter to three** (`facade.rs:20`, currently `remaining: usize`):
separate **call**, **read-byte**, and **edit-byte** counters, **reset on every VM entry** — startup
evaluation, sourced-file evaluation, **and** callbacks — not just callback turns. Host functions must:

- **Charge the call before native work** (like today's `charge()`). Pure O(1) token protocols and `Edit`
  constructors have no editor/services argument and are instead bounded by Rune instructions/allocation;
  every stateful/materializing Baryon entrypoint and every I/O shim charges the call counter.
- **Reserve a conservative byte upper bound before native work.** Reads reserve their exact byte output
  or the ≤3× lossy bound; edits reserve the declared deleted+inserted volume. An expected error or
  `NotReady` releases the byte reservation (the call charge remains); success finalizes it to the actual
  materialized/read or committed/edit byte count. An RAII reservation guard makes outer `VmError` paths
  release safely.
- **Preflight cumulative output quota and remaining Rune allocation before allocating engine output**, so
  a loop of individually-sub-`MAX_READ_BYTES` reads still trips the aggregate before allocating.
- **Count lossy-decoded output, not only input bytes** (U+FFFD expands ≤3×; the produced `String` is what
  consumes quota and Rune memory).
- **Construct returned `Bytes`, `String`, `LinePage`, and vectors inside the host as Rune-allocator-backed
  values**, after admission and before returning, rather than returning a Rust collection for an implicit
  second conversion. Admission includes the payload's conservative allocation upper bound plus bounded
  wrapper overhead.
- **Keep failure channels distinct:** an engine per-operation cap violation is
  `BufferError::Read(TooLarge)`; insufficient host read/edit quota is an outer `VmResult::panic`; Rune
  allocation refusal is an outer VM allocation error. The effective admission ceiling is the minimum of
  all three, but it does not relabel quota/allocation exhaustion as `TooLarge`.
- **Bound materialized handle lists** such as `Buffer::views` / `View::carets` (a cap + typed error).
- Exhausting a hard quota is a **VM panic** (§3), not a `Result::Err` — a script cannot catch its way past
  the aggregate bound. Handles/ids are cheap and uncounted; **materialized bytes are counted**.

**Render effects.** A successful edit or (future) cursor change must repaint even when the Rune entry
returns no status. `EditorHost` owns **`HostEffects { needs_render }`**. `RuneHost` resets it immediately
before, and takes it immediately after, **every** top-level VM entry (startup `main`, sourced `main`, and
callback), including error exits so effects cannot leak into the next invocation. Callback effects merge
into `DynamicActionOutcome`; sourced-file effects merge into the engine's compile-result render outcome;
startup effects feed the initial-render report. Thus a caught, status-less successful edit still renders,
and source/startup calls do not rely accidentally on a status message to repaint.

Initial values are **candidates**, calibrated with byte-text-semantics §10 at the same benchmark gate
before stage 5 becomes the compatibility contract:

| Limit | Candidate | Basis |
|---|---:|---|
| host calls / VM entry | 100,000 | preserves `DEFAULT_HOST_CALL_QUOTA` |
| materialized read bytes / VM entry | 32 MiB | leaves headroom inside the 64 MiB callback allocation cap |
| committed edit bytes / VM entry | 16 MiB | equals the engine's candidate `MAX_BATCH_BYTES` |
| `MAX_HANDLE_RESULTS` | 4,096 | bounds cross-view materialization independently of byte quota |

## 7. Testing (Rune-level, over the engine contract's own tests)

- **ABI (gate):** an associated native fn accepts `(&Buffer, &mut EditorHost, ...)` and resolves content
  through the facade registry/resolver authority (§1); cursor `next` also proves simultaneous
  `(&mut ChunkCursor, &mut EditorHost)` guarded arguments.
- Handle identity: a `Buffer` for a closed buffer → `StaleBuffer`, and **does not** resolve a same-path
  reopen (new `BufferId`); a closed view → `StaleView`; `Version` equality is revisited by undo/redo and
  offers no ordering. Focused synthetic content is rejected without manufacturing a text handle; a live
  retargeted `View` resolves its new current buffer; `Buffer::views` is deterministic and bounded.
- Error ABI: reads and edits both surface `NotReady` through the **same** `BufferError`/`BufferErrorKind`;
  a `NotReady` returns and, after a **manual** reinvoke, a re-run sees `Ready`; `ColdReadFailed` surfaces
  as an error, not a loop.
- Reads/cursors: `read_text` `Strict` errors at the absolute `DocByte`, `Lossy` substitutes and its output
  is quota-counted; over-`MAX_READ_BYTES` → `TooLarge`; `ChunkCursor`/`LineCursor` `next(editor)` advance
  **only on `Ready`**, return `StaleVersion` across an intervening mutation and `InvalidChunkSize` on a
  zero step; `snapshot` → `NotResident`/`TooLarge` without allocating; `carets` is read-only and bounded.
- Mutations: `replace_bytes` round-trips non-UTF-8; `replace_text` rejects an off-boundary endpoint
  (`Edit(NotUtf8Boundary)`); `edit_if(..., expected, ...)` commits on a matching `Version`, `StaleVersion`
  otherwise; a declarative `Vec<Edit>` batch is atomic while three separate calls do **not** roll back; a
  cold touched leaf yields `NotReady` then commits after reinvoke; the returned `Version` differs from the
  prior; `edit` and `edit_if` have separately fixed arities; retained `Edit` payloads remain charged to
  Rune allocation accounting; a successful caught edit still produces `HostEffects { needs_render }`.
- Quotas: a loop of sub-cap reads trips the aggregate read quota (VM panic) before allocating; lossy output
  counts toward it; `Buffer::views`/`View::carets` are length-bounded; quota resets on startup and sourced
  evaluation, not only callbacks. Effects are taken on callback success/error, sourced evaluation, and
  startup so no entry leaks a render bit into the next one.

## 8. Implementation order

**Prerequisite:** the engine stages in `byte-text-semantics.md` §13 land first. The ABI may be falsified
with a throwaway prototype earlier, but no production Rune read/edit surface lands on the legacy string
delta or blocking sparse-I/O paths. Then each step below is independently testable:

1. **ABI / value-type prototype** — register `Buffer`/`View`/`Version`/`Edit`/… ; prove the
   `(&Buffer, &mut EditorHost, ...)` associated-fn ABI (§1 gate) on a trivial read.
2. **Facade authority + error type** — direct shared registry/resolver handles on the facade;
   `BufferError` and all reason enums; focused/cross-handle acquisition.
3. **Reads and cursor reads** — `read_bytes`/`read_text`/line metrics/`ChunkCursor`/`LineCursor`/`LinePage`,
   read-only `carets` and bounded `Buffer::views`.
4. **Declarative mutations** — allocator-backed `Edit`, `View::edit*`, `replace_*`, optimistic `_if` forms.
5. **Quotas and render effects** — three-counter `HostBudget`, per-entry reset, output preflight,
   `HostEffects { needs_render }` into `DynamicActionOutcome`.
6. **Integration gate** — end-to-end through a real VM; calibrate quota constants with byte-text-semantics §10.

## 9. Non-goals / deferred

- The rest of item 9: panes, options, keymaps, commands, autocmds.
- Opening/closing buffers, path resolution, buffer lifecycle mutation — §2 only operates on already-held
  buffers.
- **Cursor mutation / motion** — read-only carets here (§5).
- `await`-style suspension on cold content — needs the event/job substrate (items 6–7); §3 is
  return-and-explicitly-retry.
- Streaming snapshots/cold reads via a background job — item 7.
- Display-column / render-metadata coordinates — never the byte-addressing path (byte-text-semantics §2.2).
- Multi-caret visual selection and fold storage — `caret-anchors.md` §13. The existing `MAX_CURSORS`
  remains the hard bound for `View::carets`; this API does not invent another cursor limit.
