# ADR 0060: libaryon document checkpoints and restoration

Status: accepted 2026-08-08. This record makes PRODUCT-0059's shared session
store consume the editor state libaryon actually owns: durable document
bindings, exact ordinary-document bytes, file/scratch provenance, dirty state,
and enough deterministic fallback presentation to inspect a restored document
set. It does not persist the accepted pane topology, host-panel placement,
cursor state, undo history, or any Polariton object. Those remain the next
owner extension. Implementation evidence is recorded in
`../impl/0060_libaryon_document_checkpoints_and_restoration_evidence.md`.

## Context

PRODUCT-0059 deliberately stops one operation before persistence becomes
visible. It can mint a `SessionId` and `DocumentId`, hold a lifetime exclusive
lock, and atomically publish an owner-partitioned generation, but its base
manifest names documents without storing their content. No engine startup path
consumes the store, and no runtime `BufferId` is bound to a restored
`DocumentId`.

The next authority is unambiguous. `EngineHandle` owns the engine thread;
`EditorSession.buffers` owns live buffers; a text document's authoritative bytes
are reachable from its UAST root. Polariton's `CodeEditor.document_texts` and
`Viewport::document_text` are bounded or lossy observations, not persistence
sources. In particular, `QueryDocumentText` converts arbitrary bytes with
`String::from_utf8_lossy`, while `read_document_snapshot` already streams a text
root into an exact `Vec<u8>` and enforces the established 256 MiB whole-document
ceiling. The checkpoint must read from the root, never from either frontend
observation.

Not every engine buffer is a durable document. Stage 0 found four distinct
classes behind the current `BufferContent` and side indexes:

| live object | authority | v1 checkpoint disposition |
|---|---|---|
| ordinary `BufferRole::Document` text | author/editor | persist |
| output-panel text | Rune/job producer | recompute; exclude |
| Git snapshot partner | immutable repository object | reacquire; exclude |
| file browser, Markdown preview, UAST inspector, host panel | derived or host-owned | recompute or later owner; exclude |

Testing only `BufferContent::Text` would therefore persist derived output and a
Git revision as though the user authored both. The durable predicate has to be
positive and centralized: an ordinary text document, not an output panel, not
a snapshot partner, and not an encrypted plaintext buffer.

Encrypted documents are the security boundary. `LoadEncryptedFile` places
decrypted plaintext in an ordinary `TextBuffer` and records
`encrypted_backing`. Copying that root into an unencrypted session owner would
silently defeat the feature's at-rest guarantee. Restoring a locked placeholder
would require a new buffer state and save refusal so an empty placeholder could
not overwrite the ciphertext. No such state exists. V1 therefore refuses the
entire checkpoint while any encrypted plaintext document is live; the previous
committed generation remains authoritative.

Two accepted runtime representations also need strengthening before they can
cross restart.

First, PRODUCT-0059 stores exact Unix path bytes in `DocumentIdentity`, but
`TextBuffer.file_path` is an `Option<String>` used for both I/O and display. A
lossy reconstruction would preserve the manifest while opening or saving a
different path. Restoration is the first consumer that makes that latent split
observable. The engine's file authority must become `PathBuf`; lossy strings
remain licensed only for labels and status prose. They are not language-service
identity: two distinct non-UTF-8 paths can have the same lossy display.

A second path defect appears only across processes. A relative path is exact
bytes but not a durable location when the restoring process has a different
working directory. Conversely, persisting `CanonicalPath` would resolve a
symlink and silently change the access path the author opened. The durable
attribute therefore needs an absolute **lexical** access path: resolve it
against the current directory when binding or checkpointing, retain every
remaining component and exact `OsStr` byte, and do not resolve symlinks.
Lexically removing `..` would itself be wrong across a symlink boundary. Runtime
canonical paths remain the separate deduplication guard.

Second, `TextBuffer.active_language_id` records a resolved provider but not why
it was selected. File loading derives it from the extension; `:set language`
authors an override; `:set language plaintext` authors the absence of a
provider. Recomputing every value from the path loses explicit overrides, while
persisting every resolved value freezes automatic detection. A durable
checkpoint therefore needs the missing provenance, not a guess about it.

The exact layout remains intentionally later. libaryon owns ordinary document
panes and tabs; Polariton owns the application state that will reattach host
panels, Rune views, and categorical continuation. That later generation must
restore the whole editor-wide topology together. Yet restoring documents into
an unreachable `BufferStore` would provide no inspectable consumer and no
falsifiable end-to-end oracle. This record uses a deterministic one-pane tab
fallback when no application layout has been restored. It is a bootstrap view,
not durable layout truth.

Finally, PRODUCT-0059 gate 17 currently guards read-only capability closure by
inspecting source text. The Rust type system does prevent the misuse today, and
one `compile_fail` doctest proves `commit` does not type-check on
`SavedSession<ReadOnly>`. But a compile-fail doctest proves only that its snippet
failed, not why: a renamed method, broken import, or typo passes just as green.
Libaryon has a library target, so PRODUCT-0049's binary-only rationale does not
apply. The reason for trybuild here is banked diagnostic causality. Passing
writable twins through the identical imports close the broken-fixture vacuity
path.

## Decision

### 1. Capability closure becomes a compiler-observed contract first

Before the writable surface grows, libaryon adds `trybuild` as a dev-dependency
and replaces PRODUCT-0059's source-text proxy with UI fixtures covering all six
existing writable-only operations:

```text
mint_document
update_document_path
remove_document
set_owner_payload
commit
cleanup_unreferenced
```

Each operation receives one compile-fail fixture accepting a
`&mut SavedSession<ReadOnly>` and one passing twin accepting a
`&mut SavedSession<Writable>`. Banked stderr must identify that operation's
method unavailability on the read-only typestate, not an unresolved import,
missing crate, malformed argument, or unrelated type error. Each pair uses the
same `libaryon::core` import path, method spelling, and type-correct arguments.
The passing twin need not construct a session or perform I/O; compilation is
the oracle. Separate pairs keep one compiler error limit or diagnostic change
from masking another operation.

The existing compile-fail doctest may remain as local API documentation, but it
does not discharge the gate. The architecture test also guards that `trybuild`
is dev-only. The source-text split over an exact impl-header spelling is
removed rather than retained as a second, weaker authority.

### 2. A saved engine is explicit and exclusively opened

Libaryon adds a separately named saved-engine entrypoint equivalent to:

```text
EngineHandle::spawn_saved(
    EngineConfig,
    SavedSession<Writable>,
) -> Result<EngineHandle, SessionRestoreError>
```

The caller resolves the platform store, selects a `SessionId`, and obtains the
exclusive handle before invoking the engine. A read-only saved session cannot
be passed because an editor can create, close, rename, or edit a document as
soon as it starts. The engine owns the `SavedSession<Writable>` and its lock for
the engine lifetime; no frontend receives a second writer or raw root.

`EngineConfig::initial_file` and saved restoration are two authorities for the
initial document set and refuse when combined. Viewport size, settings, and the
explicit Rune init entrypoint remain compatible. The init script runs only
after the restored document set has committed to a complete in-memory engine,
so its actions observe the same documents the first viewport will project.

Restoration validates and constructs the complete ordinary-document set before
publishing an `EngineHandle` or first viewport. Unsupported document-owner
schema, identity mismatch, capacity refusal, malformed bytes,
document-construction failure, or duplicate path identity each returns an error
and drops the exclusive lock. It never starts an empty engine or reduces a
structural failure to later status prose. An unavailable language provider is
not a structural failure; §6 preserves that authored selection while declining
its derived semantics.

`EngineHandle::spawn` remains the ephemeral default. Baryon and Polariton do
not discover, create, continue, or checkpoint a saved session in this cut.
Recent sessions, command-line syntax, startup prompts, and “last session”
policy remain the surface record's work.

### 3. Libaryon owns one strict document payload

The engine stores its content in owner `libaryon_documents`, schema version 1.
It does not widen the PRODUCT-0059 root manifest or create independently
published per-document files. The payload is one byte-exact member of the same
generation as the base document identities and every unknown sibling owner.

The v1 payload is a bounded binary envelope:

```text
fixed magic
u64 little-endian manifest byte length
strict UTF-8 JSON manifest
raw document byte extents concatenated in ascending DocumentId order
```

The strict manifest contains, for every durable document:

```text
DocumentCheckpoint {
    document_id
    byte_len
    dirty
    virtual_path?
    language_selection
    file_stamp?
}

LanguageSelection = Auto | ExplicitPlaintext | Explicit(canonical provider-id string)
FileStamp = { byte_len, modified_unix_seconds?, modified_subsec_nanos? }
```

The file path is not duplicated: it remains the exact-byte `PathBuf` attribute
of the base `DocumentIdentity`. Content extents are not JSON arrays, Unicode
strings, or base64. The decoder uses checked arithmetic, rejects duplicate or
out-of-order IDs, rejects trailing or missing bytes, and requires the payload's
ID set to equal the base document set exactly. An empty base may omit the owner
payload for compatibility with PRODUCT-0059's initial empty generation. A
nonempty base without the payload refuses because identity alone cannot restore
scratch content.

Unknown document-owner schema refuses engine restoration without rewriting it.
The generic session store remains able to open and preserve that owner as
opaque bytes. Root schema and owner schema retain the independent versioning
PRODUCT-0059 established.

### 4. Checkpoint capture is exact, bounded, and all-or-nothing

A session-bound `EngineHandle` exposes a synchronous ordered checkpoint
request. It enters the same engine queue as edits, so it observes every command
accepted before it and none accepted after it. Capture and identity
reconciliation happen on the engine thread while its `EditorSession` is the
sole document authority.

The request:

1. enumerates every positive durable-document candidate;
2. refuses if any candidate carries `encrypted_backing`;
3. reads exact bytes from each UAST root through the same snapshot/streaming
   seam used by save, never through a viewport or frontend cache;
4. stages the complete strict owner payload and base identity set in memory;
5. updates the owned `SavedSession<Writable>` only after every document staged;
6. calls its atomic `commit`; and
7. replies success only after `CURRENT` publication completes.

The v1 aggregate raw document-content budget is the existing
`MAX_WHOLE_DOCUMENT_SNAPSHOT_BYTES` (256 MiB), not 256 MiB per open document.
The envelope adds checked bounded metadata overhead. Exceeding the aggregate,
encountering an incompletely loaded pathless root, or failing allocation
refuses the checkpoint before the `SavedSession` snapshot is mutated. The
previous generation and every live buffer remain unchanged. A real session
that needs a larger budget is the trigger for a streaming/multi-file owner
extension; silently omitting one document is never the fallback.

Checkpointing does not mark a document saved, clear its file-dirty bit, create
an undo boundary, move its cursor, or change its view. “Saved in the workspace
session” and “written to its file” are distinct facts.

Commit errors retain PRODUCT-0059's publication boundary. Failure before
`CURRENT` replacement leaves the previous document generation authoritative.
Failure after replacement exposes the complete new generation and makes the
engine's saved-session handle publication-uncertain; later checkpoint requests
refuse until the session is dropped and reopened. The engine may keep its live
documents available so the caller can report or copy them, but it may not retry
through the poisoned handle or describe the preceding generation as current.

That severity is deliberate rather than an assumption that observation is
impossible. While the engine retains its exclusive lock it could re-read
`CURRENT`, validate the referenced generation, and learn which complete
snapshot is visible. But PRODUCT-0059 makes a **fresh opened handle** the
boundary that re-establishes publication truth after a post-pointer failure;
adding an in-place reconciliation operation here would supersede that recovery
contract and grow the writable capability surface inside its first consumer.
This record instead keeps the lock and live bytes available, refuses further
publication, and reports that later edits cannot be checkpointed through this
engine. Lock continuity outranks automatic recovery: releasing and reacquiring
could admit another process between them. The startup/close surface may later
design an explicit controlled teardown/reopen flow, including its contention
and unsaved-edit prompts; it may not disguise that flow as an ordinary retry.

There is no per-keystroke generation and no implicit commit on `Drop`,
`EngineHandle::shutdown`, or `EditorCommand::Quit` in this record. The caller
must request a checkpoint and receive its result before deciding to close. This
keeps failure observable and preserves PRODUCT-0059's explicit-commit rule;
the later startup/close surface decides prompting, debouncing, and automatic
checkpoint cadence rather than burying those policies in the substrate.

### 5. Durable identity follows ordinary document lifecycle

A session-bound engine maintains a private bijection between every durable
ordinary `BufferId` and one `DocumentId`:

```text
DocumentId -> BufferId
BufferId   -> DocumentId
```

Restoration allocates fresh `BufferId`s and installs the saved IDs. A newly
created ordinary scratch or file document mints a `DocumentId` before it enters
the live layout. Creating a second view of one buffer does not mint. Closing one
of several views does not remove. Releasing the last view removes the document
from the next checkpoint but never makes its ID reusable. Save-as, rename,
path detach, and `ReplaceDocument` preserve the ID while updating its path
attribute. A derived buffer never mints an ID.

The active `PaneViewport` and every `DocTabSummary` gain
`document_id: Option<DocumentId>`. It is `Some` for an ordinary document in a
saved engine and `None` for ephemeral or derived buffers. `BufferId` remains
the runtime routing identity; `DocumentId` is the durable producer key the next
Polariton owner consumes. No `PaneId` or `ViewId` enters the document payload.

Lifecycle mutation is staged in the opened `SavedSession` immediately but is
durable only after the next successful checkpoint. A failed or abandoned
checkpoint cannot publish a half-updated document set. This preserves the
single generation's equality between base identities and content.

### 6. File authority becomes byte-exact; presentation may remain lossy

`TextBuffer`'s authoritative file path becomes `Option<PathBuf>`, and internal
open, reload, save, save-as, file-stamp, canonical-path, and Git-path operations
consume `Path`/`OsStr` without converting through UTF-8. A restored non-UTF-8
path therefore rebinds to the same file and saves back to that exact byte
sequence.

Before a live file path enters `DocumentIdentity`, the engine records its
absolute lexical access path. It resolves a relative authoring against the
current working directory but retains its remaining components; it neither
collapses `..` across a possible symlink nor replaces the spelling with
`CanonicalPath`. Restoring under a different working directory therefore
reaches the original location, while a symlink remains the access path the
author chose. Canonical resolution still prevents two live buffers from
claiming aliases of the same target.

Existing public command arguments may remain strings because their current
grammar can author only UTF-8. `PaneViewport.file_name`, status messages, and
other presentation may use an explicitly lossy value; none of those strings may
be reused for filesystem I/O, canonical identity, the session manifest, or
language-service document identity. Until the provider API accepts an exact
path type, path-dependent semantic work for a non-UTF-8 file is unavailable
rather than being invoked under a colliding lossy name. Editing, lexical
display, checkpoint, restore, and save remain available. The architecture test
pins the exact/lossy boundary at the I/O, restore, and provider seams.

The engine also replaces bare `active_language_id` authority with runtime
selection provenance equivalent to:

```text
Auto
ExplicitPlaintext
Explicit(canonical provider-id string)
```

File load selects `Auto`; explicit language commands select an explicit case;
ordinary save-as keeps today's behavior of re-detecting from the new path and
therefore returns to `Auto`; an embedder replacement records the language it
authored. `active_language_id` becomes the resolved output. The checkpoint
stores selection, and restoration resolves it against the current registry. If
an explicitly named provider is no longer registered, the engine preserves the
canonical string, restores the document as plaintext with a startup warning,
and leaves `active_language_id` absent. A later checkpoint retains the authored
selection rather than silently rewriting it to plaintext; reintroducing the
provider can make it resolvable again. Derived syntax trees, highlights,
diagnostics, minimap state, and semantic requests are recomputed.

### 7. Restore preserves content truth without inventing undo history

Every payload byte extent is rebuilt through libaryon's real document creation
and registry-accounting path. Restoration does not deserialize `NodeId`s,
trees, devices, spans, or provider outputs. Arbitrary non-UTF-8 document bytes
round-trip exactly.

Scratch and virtual documents restore the checkpoint bytes directly. A
file-backed document uses the checkpoint's dirty bit to choose authority:

- **dirty at checkpoint:** restore the checkpoint bytes and retain dirty state;
  if the current file stamp differs or the path is unavailable, retain those
  bytes and include the conflict in an aggregate startup warning;
- **clean at checkpoint and current path readable:** load the current file,
  because the file—not a redundant clean snapshot—is the continuing authored
  authority; when its stamp differs from the checkpoint, include that external
  replacement in the aggregate startup warning even though the disk bytes win;
- **clean at checkpoint and path missing or unreadable:** restore the
  checkpoint bytes, mark the document dirty so the fallback cannot masquerade
  as current disk truth, retain the path, and report the recovery.

The stored clean bytes are therefore both an equality oracle and a recovery
fallback, not a license to overwrite a file that changed while the application
was closed. Dirty restoration follows the live editor's existing external-file
policy: it preserves the user's unsaved text and warns rather than silently
reloading it. Clean restoration still follows current disk, but it also names
the divergence: continuing a session that showed X and now shows Y is not the
same interaction as opening that file afresh.

Undo/redo history is not persisted. A restored dirty document receives an
explicit dirty baseline with empty history: it remains dirty, `undo` reports no
older transaction, and neither operation can manufacture a clean state.
Restored clean content receives a clean empty ledger. Cursor, selection, folds,
scroll, search, jump history, navigation anchors, parked generations, shadow
history, and semantic state reset and recompute. The record preserves content
and file-save responsibility, not the process's editing timeline.

### 8. A deterministic fallback makes restoration inspectable, not exact

Until an application owner supplies the accepted editor-wide layout, a restored
document set is materialized as one root pane containing one view per document,
ordered by ascending `DocumentId`, with the lowest ID active. Each document
appears exactly once. Runtime `PaneId`, `ViewId`, cursor, and tab position are
fresh.

If the saved session contains no documents, the engine creates one ordinary
empty scratch document, mints its `DocumentId`, and presents the existing
single empty pane. The ID remains uncommitted until an explicit checkpoint.

This fallback is intentionally weaker than layout persistence:

- it collapses multiple views of one document to one;
- it does not restore split direction or ratios;
- it does not restore active tabs or focus;
- it excludes output, browser, preview, inspector, and host-panel panes; and
- it emits no `PanelRestored` event.

The next Polariton owner generation replaces the fallback when it has a valid
layout payload from the same committed generation. It may not merge half of a
saved layout with the fallback. Document bytes and bindings remain libaryon's
authority either way.

## Implementation sequence

1. Replace PRODUCT-0059 gate 17's text proxy with libaryon trybuild
   compile-fail fixtures and writable twins. Land this green before adding any
   persistence mutator or engine entrypoint.
2. Introduce exact internal `PathBuf` authority and language-selection
   provenance as byte-identical runtime refactors. Existing ephemeral load,
   save, reload, save-as, highlighting, and rendered labels remain the oracle.
3. Add the strict `libaryon_documents` codec with synthetic identities and
   arbitrary bytes. Prove schema, ID-set, extent, budget, and encrypted-content
   refusals without involving the engine.
4. Add the session-bound engine identity maps and `DocumentId` projection.
   Prove lifecycle updates through scratch, load, second view, close, save-as,
   and replacement before disk restoration.
5. Add synchronous ordered checkpoint capture and atomic publication. Fault
   tests reuse PRODUCT-0059's commit points rather than adding an independent
   document commit mechanism.
6. Add pre-start restoration and the deterministic one-pane fallback. Drive
   dirty/clean external-file cases, non-UTF-8 paths and bytes, and derived-buffer
   exclusions end to end.
7. Update the persistence ledger: document restoration is landed; the next
   dependency is the Polariton layout/view/color owner extension.

Steps 2 and 3 must remain independently green. A path/provenance refactor that
changes existing pixels or file targets cannot be hidden beneath newly restored
state. The checkpoint may not land without the restore oracle, and restoration
may not land without checkpoint round-trip from the engine authority.

## Acceptance gates

1. Libaryon's trybuild suite compiles six passing writable twins and rejects six
   read-only twins, one pair per PRODUCT-0059 mutator, with banked
   method-unavailable diagnostics. Each pair uses the same public imports,
   method spelling, and type-correct arguments. Deliberately breaking an import
   makes its passing twin fail. `trybuild` appears only under libaryon's
   dev-dependencies, and the source-text impl-block proxy is removed.
2. `spawn_saved` accepts an exclusively opened `SavedSession<Writable>` and
   refuses `SavedSession<ReadOnly>` at compile time. Combining it with
   `EngineConfig::initial_file` refuses before an engine thread or viewport
   exists. Existing `EngineHandle::spawn` remains byte-for-byte ephemeral and
   creates no session path.
3. A strict codec round-trips two documents containing NUL, invalid UTF-8, and
   quote/backslash-shaped bytes. The manifest is strict; bad magic, truncated
   manifest, duplicate/out-of-order IDs, checked-length overflow, missing or
   trailing content, unknown schema, and an ID set unequal to the base each
   refuse without mutating the opened session.
4. A synthetic PRODUCT-0059 empty generation without a document owner restores
   as the valid empty predecessor and receives one uncommitted scratch document.
   A nonempty base without the owner refuses. An unknown future document-owner
   schema can still be opened and copied byte-for-byte by the generic store but
   cannot start an engine.
5. One session opens a scratch document, a file, a second view of that file, an
   output panel, a Git snapshot partner, a file browser, a Markdown preview, a
   UAST inspector, and a host panel. The checkpoint contains exactly two
   document IDs and two content extents: scratch and ordinary file. The second
   view does not duplicate content; every derived/host object is absent.
6. The same lifecycle fixture proves bijection: scratch and file receive unique
   durable IDs; the second view reuses; closing one view retains; closing the
   last removes; save-as and `ReplaceDocument` preserve the ID while updating
   or clearing its exact path. A later document never reuses the removed ID.
   No serialized byte contains `BufferId`, `PaneId`, or `ViewId`.
7. Every active ordinary saved-session pane and doc-tab summary projects the
   expected `Some(DocumentId)` while ephemeral and derived panes project
   `None`. Reopen assigns different `BufferId`s and `ViewId`s but reproduces
   the exact `DocumentId -> BufferId` meaning independent of allocation order.
8. Checkpoint ordering is behavioral. Queue edit A, checkpoint, and edit B;
   reopen contains A but not B while the live engine contains both. The reply
   arrives only after `CURRENT` names the complete generation. A forced
   pre-pointer failure returns an error and preserves the previous generation.
   A forced post-pointer failure returns publication uncertainty, exposes the
   complete new document generation after reopen, and makes every later
   checkpoint on the old engine refuse while its exclusive lock and live bytes
   remain held. The error states that later edits are not session-checkpointable
   through that engine. No in-place `CURRENT` observation, lock release, or
   automatic reopen weakens PRODUCT-0059's fresh-handle boundary. Neither error
   class changes any live document's dirty state, cursor, undo ledger, or bytes.
   A synthetic unknown sibling owner survives the successful document
   checkpoint byte-for-byte, proving the engine updates its owner rather than
   replacing the owner set.
9. The codec/capture seam accepts a budget parameter in tests. A small synthetic
   budget succeeds immediately below its boundary and refuses one byte above,
   while a direct constant assertion pins the production budget to 256 MiB.
   Refusal occurs before mutating the saved snapshot or staging a generation.
   Allocation failure and an incompletely loaded pathless root have the same
   atomic refusal. No document is omitted to make the checkpoint fit, and the
   gate does not make every suite run allocate a quarter gigabyte merely to
   prove arithmetic.
10. With one encrypted plaintext buffer among ordinary documents, checkpoint
    refuses with a diagnostic naming the security boundary. Searching every
    committed generation finds none of the known plaintext. Closing the
    encrypted document permits the remaining ordinary set to commit; no empty
    placeholder or encrypted path is fabricated.
11. A dirty scratch containing arbitrary non-UTF-8 bytes checkpoints and
    restores byte-for-byte with the same `DocumentId` and dirty state. Its undo
    history is empty; invoking undo does not clean it or alter bytes. Search,
    folds, selections, navigation, semantics, and UAST node identities are not
    serialized and begin from their documented reset/recompute state.
12. File restoration covers a four-cell matrix. Unchanged clean and dirty files
    reproduce the intended bytes and cleanliness. A changed clean file loads
    current disk bytes and reports the external replacement in the aggregate
    startup warning. A changed dirty file keeps checkpoint bytes and reports
    conflict. A missing clean file falls back to checkpoint bytes, becomes
    dirty, retains its path, and reports recovery. Every cell that diverges from
    checkpoint content is surfaced; session checkpointing never marks a file
    clean.
13. A file whose Unix name contains invalid UTF-8 is loaded, checkpointed,
    restored, edited, and saved. The exact original `OsStr` bytes name the file
    at every I/O boundary; no sibling at the lossy display spelling is opened or
    created. Labels may be lossy but are never fed back into path or provider
    identity. Two files with distinct byte names but the same lossy display do
    not alias; path-dependent semantic work declines for both. A relative path
    checkpointed under directory A restores the same absolute lexical target
    after the process working directory changes to B, while a symlink access
    path remains a symlink spelling rather than its canonical target.
14. Automatic file language, explicitly selected language, and explicitly
    selected plaintext each survive checkpoint with distinct
    `LanguageSelection` values. Renaming or save-as follows today's detection
    rule. An explicit provider absent from the current registry restores the
    bytes, preserves its canonical selection string, resolves to plaintext, and
    reports the unavailable provider; a second checkpoint does not rewrite the
    selection. No syntax tree, highlight, or diagnostic is serialized.
15. Three restored documents whose IDs are intentionally nonconsecutive appear
    once each in one pane's tab strip in ascending ID order, with the lowest ID
    active. All content is inspectable. No split, ratio, duplicate view, cursor,
    active-tab history, host panel, group state, or `PanelRestored` occurrence
    is claimed. This gate is explicitly a fallback oracle, not layout parity.
16. Explicit checkpoint and restore change no default startup, shutdown, Rune,
    SQL/Toron, plot/table, SVG, screenshot, or rendering baseline. Baryon and
    Polariton gain no session selector or implicit last-session behavior. The
    full workspace suite passes with no ignored persistence gate.

## Stop condition

> This record may replace PRODUCT-0059's capability proxy with trybuild; add
> exact internal path and language-selection authority; add libaryon's strict,
> bounded document owner; bind `DocumentId` to ordinary buffers; expose the
> binding in projections; add explicit checkpoint and saved-engine entrypoints;
> and install the deterministic one-pane fallback. If implementation requires
> persisting exact pane topology, a host panel, Polariton layout/color state,
> cursor/selection, undo history, a semantic artifact, an encrypted plaintext,
> a runtime ID, or a Toron database, work stops and returns to its owner.

> If exact checkpointing within the v1 aggregate budget cannot use the current
> single owner payload without an unbounded allocation, work stops for a
> streaming owner-store record. If a secret document cannot be excluded without
> silently losing the rest of the session, work stops for an explicit locked
> document state. Boundedness and at-rest secrecy outrank a successful demo.

> If restoring a non-UTF-8 path requires converting it through a display string
> for filesystem I/O, work stops. Exact path identity outranks retaining the
> current `String` field shape.

## Supersession

This record consumes and narrows PRODUCT-0059 §6. The base manifest continues
to own identity and exact paths; the new owner supplies content and restoration.
PRODUCT-0059's ordinary-startup and explicit-commit rules remain. Its gate 17
source-text enforcement is replaced by compiler-observed, banked trybuild
evidence; the production typestate itself is unchanged.

It refines PRODUCT-0059 §7's phrase “libaryon document content and restoration”
by separating content fallback from exact layout. Documents and bindings land
here. The one-pane tab strip exists only when no later layout owner is present.
The Polariton extension still owns ordinary pane/tab topology together with
host-panel placement, Rune-view linkage, and categorical continuation.

It does not supersede `ShadowSession`. That mechanism remains a per-file
large-edit recovery journal with generation history. The session checkpoint
stores one current logical document image and no undo chain. It neither imports
nor deletes `.baryon` state.

It preserves existing file-authority behavior while making it restart-aware:
clean file-backed content follows current disk; dirty content follows the
editor and reports divergence. The new `LanguageSelection` records provenance
that the runtime previously collapsed but does not change existing selection or
save-as outcomes.

## Rejected

- **Persist `Viewport::document_text` or Polariton's cache:** both are
  observations; the former is lossy for arbitrary bytes and the latter belongs
  to a non-owner.
- **Persist every `TextBuffer`:** captures output panels and Git snapshots as
  authored documents and makes recomputable state durable by accident.
- **Write one generation per keystroke:** rewrites an aggregate owner payload on
  the hot path, turns typing latency into filesystem latency, and still needs a
  failure/prompt policy.
- **Implicitly checkpoint on `Drop`:** cannot report failure, conflates crash
  teardown with graceful continuation, and contradicts PRODUCT-0059's explicit
  publication rule.
- **Clear publication uncertainty by re-reading `CURRENT` in place:** may be a
  viable later recovery mechanism, but supersedes PRODUCT-0059's fresh-handle
  boundary and leaves the surface question of how unsaved edits and lock
  reacquisition interact unanswered. This record keeps the lock and refuses
  further publication rather than smuggling in that lifecycle.
- **Put document bytes in JSON:** inflates arbitrary binary data, invites UTF-8
  confusion, and makes a parser representation part of the content oracle.
- **One owner payload file per document without a store record:** changes
  PRODUCT-0059's exact owner-file set and atomic preservation rules. Streaming
  or multi-file owners require their own format decision.
- **Give each document an independent generation:** permits identity, content,
  and later layout to observe different moments after a crash.
- **Persist only dirty or scratch content:** makes a missing clean file
  unrecoverable and prevents an exact end-to-end checkpoint oracle.
- **Always prefer checkpoint bytes over a changed clean file:** turns redundant
  clean state into a stale second authority and risks overwriting work produced
  outside the closed application.
- **Always prefer disk over dirty checkpoint bytes:** discards the unsaved work
  session persistence exists to retain.
- **Persist decrypted encrypted buffers:** silently replaces encrypted-at-rest
  storage with plaintext-at-rest storage.
- **Restore an empty encrypted placeholder:** creates a document that can be
  mistaken for real plaintext and saved back destructively without a new locked
  state.
- **Persist the resolved language ID without provenance:** freezes automatic
  detection and cannot distinguish an authored plaintext override from no
  provider having been detected.
- **Recompute every language from path:** loses explicit language and plaintext
  choices on restart.
- **Refuse the whole document set when one explicit provider is unavailable:**
  makes derived analysis availability outrank recoverable authored text. Keep
  the unresolved selection and decline only its semantics.
- **Convert an exact path to UTF-8 for restoration:** preserves the manifest but
  can open or overwrite a different filesystem object.
- **Persist a relative path as the durable location:** gives process working
  directory durable meaning and can reopen a different file after restart.
- **Persist the canonical target instead of the lexical access path:** erases
  symlink spelling and changes rename/save behavior even though path is an
  attribute rather than document identity.
- **Feed a lossy non-UTF-8 path to a language provider:** lets distinct files
  alias under one provider identity. Declining path-dependent semantics is
  narrower and honest until that API accepts exact paths.
- **Serialize UAST nodes or semantic outputs:** persists allocator-local and
  provider-derived state whose invalidation rules are stricter than document
  content.
- **Persist undo as part of “document restoration”:** turns one current image
  into a history/migration record and couples recovery to internal delta and
  parked-generation formats.
- **Leave restored buffers unattached:** technically rebuilds content but gives
  no inspectable user behavior and no end-to-end correspondence oracle.
- **Call the fallback tab strip layout persistence:** it intentionally loses
  split, ratio, focus, active-tab, duplicate-view, and host-panel state.
- **Restore exact ordinary layout here:** splits one atomic editor-wide decision
  across two owners before Polariton can name its panels and continuation state.
- **Let `initial_file` override or append to a saved session:** gives two startup
  authorities an order-dependent interaction without a product request.
- **Auto-open the last session:** couples document mechanics to discoverability,
  consent, contention UX, and close policy before either head has designed them.

## Consequences

- The first explicit checkpoint/reopen can show the same scratch and unsaved
  file content under the same durable document identities, even though runtime
  buffer/view numbers and exact layout are new.
- The checkpoint is a consistent editor transaction and a complete store
  generation, not a collection of independently autosaved buffers.
- File writes and workspace checkpoints remain distinct. A dirty document stays
  dirty after the workspace safely records it.
- A clean file changed outside the closed application still follows disk, but
  the continuing session reports that its visible content changed rather than
  presenting the replacement as continuity.
- Exact Unix path bytes finally reach the engine consumer that PRODUCT-0059
  prepared them for; display strings lose their accidental authority over I/O.
- Language choice gains provenance because persistence makes the old collapsed
  state ambiguous. The resolved provider remains derived.
- Secret plaintext cannot leak into the generic session directory. The cost is
  an explicit v1 refusal until a real locked-document restoration state exists.
- The 256 MiB aggregate cap is intentionally conservative. It makes memory and
  atomicity honest now and supplies a concrete trigger for streaming owner
  payloads instead of assuming them prematurely.
- The fallback makes restoration observable without weakening the fixed order.
  The next record still owns exact pane/tab topology, host panels, Rune view
  linkage, and color continuation as one Polariton generation.
- The persistence arc advances from identity/exclusion/publication to actual
  content. Automatic startup and close remain deliberately absent; the later
  surface can build them atop a checkpoint operation whose failure is already
  explicit. That surface also inherits the rare but severe post-publication
  state: continued editing is possible, continued checkpointing is not, until
  it deliberately tears down and reopens the saved engine.
