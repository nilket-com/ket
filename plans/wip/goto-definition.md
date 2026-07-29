# Rust Go to Definition (`gd`, `:definition`)

Status: planning converged 2026-07-28. Not implemented.

## Purpose

Add provider-owned code navigation without pretending that UAST node identity
is symbol identity. The first slice is Rust-only because `RustProvider` already
owns a live `ra_ap_ide::AnalysisHost`; the engine-facing contract remains
provider-neutral so unsupported languages fail honestly and later providers can
opt in without adding Rust branches to the editor runtime.

Canonical entry points:

- Normal-mode `gd`.
- `:definition`, with `:def` as a discoverable alias.

Both dispatch the same `EditorCommand::GotoDefinition`.

## Scope

This stage provides:

- same-document Rust definition jumps;
- cross-file Rust workspace definition jumps;
- exact live-buffer analysis rather than disk-only analysis;
- asynchronous provider work;
- stale-result rejection;
- deterministic selection when rust-analyzer returns several targets; and
- safe reuse of existing buffers and views.

This stage does **not** provide a target picker, declarations, type
definitions, references, hover, cross-language definitions, or jump-back
history. A stamped `Ctrl-o` jumplist is stage 6.1 and requires a separate
anchor-rebasing/reject-stale design. Bounds-clamping a stale byte is not an
acceptable substitute because it can land on unrelated text.

## Architectural boundary

Go to definition is not a UAST query. UAST provider kinds describe stored
syntax; rust-analyzer owns name resolution, macro expansion, crates, imports,
and workspace symbol identity.

Provider work must also not run on the engine thread. `RustProvider` protects
its workspace states and `AnalysisHost`s with one mutex, and workspace
initialization can run Cargo metadata and load a source graph. Calling it from
command dispatch could freeze input and rendering.

The existing semantic reactor is the execution boundary:

```
gd / :definition
  -> stamped GotoDefinitionRequest with exact source bytes
  -> semantic-reactor worker
  -> LanguageProvider::goto_definition (Rust opts in)
  -> InternalEvent::NavigationResolved
  -> engine stamp validation, buffer/view resolution, cursor jump
```

The reactor input becomes a private work enum:

```
ReactorRequest::Analyze(SemanticRequest)
ReactorRequest::GotoDefinition(GotoDefinitionRequest)
```

Analysis continues to return through the existing `SemanticResponse` channel.
Navigation does **not** enter that channel or the graft application path. The
reactor already owns an `InternalSender`; it posts a boxed
`InternalEvent::NavigationResolved` onto the engine's ordered request stream,
following the existing internal-event pattern. No second response channel is
needed.

## Provider-neutral contract

Add an optional `LanguageProvider::goto_definition` method whose default
result is `Unsupported`. The input is valid UTF-8 source, its provider-facing
path, and a UTF-8 byte offset. The output is owned and contains no
rust-analyzer types:

```
DefinitionTarget {
    location: SameDocument | File(PathBuf),
    focus_range: Option<(u64, u64)>,
    full_range: (u64, u64),
    name: String,
}

DefinitionResult =
    Unsupported
  | NoDefinition
  | Targets(Vec<DefinitionTarget>)
  | Failed(String)
```

The worker rejects non-UTF-8 input before invoking the provider. The existing
`MAX_SEMANTIC_BYTES` 8 MiB gate applies to navigation as well: a request at or
above the limit is refused without cloning or analyzing the whole document.

`GotoDefinitionRequest` carries:

```
(RequestId, BufferId, root NodeId, StateId, ViewId, cursor DocByte,
 language id, provider-facing path, exact source bytes)
```

The session retains the latest pending navigation request. A newer invocation
supersedes an older one.

## Rust provider

`RustProvider::goto_definition` reuses the workspace/single-file resolution
path used by `semantic_reactor`:

1. Resolve or initialize the workspace state under the provider mutex.
2. Apply the request's exact source text to its `FileId`.
3. Call `Analysis::goto_definition` at the byte offset.
4. Convert rust-analyzer `NavigationTarget`s into provider-neutral targets.

The semantic and navigation paths should share a helper for resolving the
current `WorkspaceState` and `FileId`; phase 6 must not create subtly different
workspace caches.

`WorkspaceState::file_ids: HashMap<VfsPath, FileId>` is sufficient for reverse
translation:

- A target with the requesting `FileId` becomes `SameDocument`.
- A different real `VfsPath` becomes its absolute filesystem path.
- A different virtual path cannot be opened and is omitted with an explicit
  unavailable-target outcome if no usable target remains.

Single-file fallback hosts use virtual `/baryon/...` paths, so they support
same-document jumps only. Cross-file navigation outside a real workspace
reports that limitation rather than trying to open the synthetic VFS path.

`TextSize`/`TextRange` offsets are UTF-8 bytes and map directly to `DocByte`.
Use `focus_range` for cursor placement when present; otherwise use the start of
`full_range`.

## Deterministic target choice

Exact duplicate targets are deduplicated. Remaining targets are sorted by a
total key:

1. same-document before cross-file;
2. canonical target path;
3. focus-range start (or full-range start when focus is absent);
4. full-range start;
5. focus-range end;
6. full-range end; and
7. symbol name.

The engine opens the first target and reports the total when more than one was
returned, for example `Jumped to definition (3 definitions)`. A picker can
replace this policy later without changing the provider or worker boundary.

## Freshness and response validation

Navigation responses echo the full request stamp. On
`InternalEvent::NavigationResolved`, the engine requires:

- the response `RequestId` is still the session's latest pending navigation;
- the initiating `ViewId` still exists and still shows the stamped `BufferId`;
- the source buffer still has the stamped root; and
- its current `StateId` still matches.

Failure of any check rejects the response. It must not clamp offsets, retarget
another view, or apply a result to newer content. A closed initiating view gets
a concise discarded-result status when there is still a frontend to show it.

Cursor movement alone does not stale a request: the command intentionally
refers to the byte captured when `gd` was pressed. A later `gd` supersedes it
through `RequestId`.

## Target application and dirty-buffer policy

Path identity uses the existing `CanonicalPath::resolve` and
`BufferStore::buffer_for_path` index. Stored display paths are not rewritten,
and phase 6 does not add a parallel path-identity mechanism.

Application rules:

1. **Same document:** move the initiating view's primary cursor to the target
   byte and focus its pane. This is safe regardless of dirty state.
2. **Target already visible:** focus the existing target pane/view, activate
   its tab, and move that view's cursor. The initiating dirty buffer remains
   attached and untouched.
3. **Target buffer exists but has no usable view:** treat it like a load/retarget
   operation below; do not assume buffer existence alone preserves the source.
4. **Target requires loading or retargeting:** refuse when the initiating
   source buffer is dirty. There is no bang variant for `gd`.
5. **Clean source:** resolve or load through the ordinary BufferStore path,
   retarget the exact initiating view (not whichever tab is active when the
   response arrives), focus it, then set the cursor after validating the
   target range against the loaded document.

The dirty refusal is deliberately stricter than blindly calling the current
`:e PATH` plumbing: retargeting the final view of a dirty buffer releases that
buffer and can discard its only workspace reference.

Target ranges are validated after resolving the destination buffer. Invalid or
out-of-bounds ranges are rejected, never clamped.

## User-visible outcomes

Every terminal outcome produces a concise status:

- provider does not support definitions;
- no definition at cursor;
- document is too large or not valid UTF-8;
- only a virtual cross-file target was returned;
- source changed or initiating view closed;
- dirty source prevents a cross-file retarget;
- target could not be loaded or its range was invalid; or
- jump succeeded, including the target count when greater than one.

Starting a request may show `Finding definition…`; completion replaces it.

## Tests

### Provider

- Same-file local definition resolves to `SameDocument`.
- Workspace import resolves to the canonical file path and focus range.
- Exact live request bytes override stale disk/host content.
- Single-file fallback refuses virtual cross-file targets honestly.
- `ProviderKindId` and UAST topology are not consulted.
- Multiple targets deduplicate and sort by the documented total order.

### Reactor and engine

- Navigation posts `InternalEvent::NavigationResolved` and never appears as a
  `SemanticResponse`.
- Ordinary semantic analysis and structural graft responses remain unchanged.
- Default provider method returns `Unsupported`.
- A newer navigation request supersedes an older response.
- StateId or root changes reject a response.
- Closing or retargeting the initiating view rejects a response.
- Same-file navigation moves only the initiating view.
- An already-visible cross-file target is focused without losing a dirty
  source.
- A dirty source refuses a cross-file load/retarget.
- A clean cross-file target loads/reuses the canonical buffer and lands on the
  exact focus byte.
- Multi-target selection is deterministic and reports the count.
- The 8 MiB and invalid-UTF-8 gates reject before provider analysis.

### Input and end to end

- `gd` arms and clears the existing `g` prefix correctly.
- `:definition` and `:def` dispatch `EditorCommand::GotoDefinition`.
- Builtin command/key catalogs are regenerated.
- A production engine with a real two-file Rust fixture navigates across the
  workspace and publishes the destination viewport.

## Implementation sequence

1. Add provider-neutral navigation types, the optional provider method, and
   Rust conversion/ranking tests.
2. Widen the semantic worker input and post navigation completion as its own
   internal event.
3. Add stamped pending state and engine-side validation/application.
4. Add `EditorCommand::GotoDefinition`, `gd`, `:definition`, help/catalog
   updates, and end-to-end tests.
5. Run full workspace tests and strict production Clippy for libaryon, Baryon,
   and Polariton.

## Deferred stage 6.1

`Ctrl-o` jump-back history will have its own plan. A future entry will at
minimum need buffer/root/content identity and a meaning-preserving strategy for
edits. Until that contract exists, phase 6 records no hidden one-off history
and makes no stale-position clamping promise.
