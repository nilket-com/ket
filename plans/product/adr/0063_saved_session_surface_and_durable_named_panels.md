# ADR 0063: saved-session surface and durable named panels

Status: proposed 2026-08-09. This record makes the saved-session substrate from
PRODUCT-0059 through PRODUCT-0062 reachable from Polariton. It adds explicit
command-line creation and selection, a visible session identity, explicit and
close-time checkpointing, and an honest failure escape. Durable named Rune
panels are its first live exhibit. It adds no implicit last-session policy,
periodic autosave, session registry, dashboard persistence, or startup code
execution.

## Context

Four records have built a complete but unreachable persistence chain:

```text
PRODUCT-0059  SessionId, DocumentId, exclusion, owner-partitioned generations
PRODUCT-0060  exact document checkpoint and restoration
PRODUCT-0061  head-neutral logical layout and coupled application checkpoint
PRODUCT-0062  Rune-view meanings and categorical colour continuation
```

`CodeEditor::open_saved_with_init` and `CodeEditor::checkpoint_session` are
production functions, but their only callers are tests. Polariton's actual
startup (`gui/state.rs:409`) always calls `open_initial_file_with_init` or
`new_with_init`, both ephemeral. Its actual close path (`state.rs:1813`) drains
the local Toron workspace when present and then closes the window; `:q` reaches
that same path through `take_quit_request`. No production action selects,
creates, checkpoints, or reopens a saved session.

The absence is deliberate up to this point. PRODUCT-0059 left discovery,
command-line spelling, contention UX, and window/session lifecycle to the first
surface. PRODUCT-0060 removed implicit checkpointing from `Drop` and assigned
close policy to that surface. PRODUCT-0061 made a coupled checkpoint occasionally
refusable when the frontend witness is stale, and required any automatic or
debounced consumer to decide bounded retry, coalescing, and starvation rather
than adding an unbounded loop.

The store supplies creation and exact-ID opening, but no discovery authority:

```text
SessionStore::platform_default()
SessionStore::create()                  -> SavedSession<Writable>
SessionStore::open_writable(SessionId)  -> SavedSession<Writable>
```

There is no list, alias, friendly name, recent-session database, or last-session
pointer. `SessionId` already has the only canonical spelling: 32 lowercase
hexadecimal digits. A first surface can therefore be useful without inventing a
second durable index.

This record also closes the oldest persistence deferral in the product arc.
PRODUCT-0010 §5 specified the durable named-view key as:

```text
(workspace identity, durable document identity, view name) -> HostPanelId
```

The substrate has now realized every component. The saved session supplies
`SessionId`; every ordinary and scratch document supplies `DocumentId`; and
PRODUCT-0062 persists `(DocumentId, LocalViewIdentity) -> HostPanelId` inside
that session's `polariton_session` owner. The remaining work is not another
identity mechanism. It is letting a user enter the continuing session in which
the identity is observable.

Three premises from PRODUCT-0010 §5 have changed and must be reconciled openly:

- scratch documents are durable now. PRODUCT-0060 mints them a `DocumentId`, so
  their named views survive a continuing saved session rather than remaining
  process-local;
- rename and `:w <path>` retarget the same document and retain its `DocumentId`,
  so panel identity follows the document without a binding migration;
- a genuinely separate document receives a fresh `DocumentId`, so it cannot
  inherit another document's panel even when its path or source bytes match.

Finally, close ordering has a subtle additional boundary. The local-workspace
stop is asynchronous. If the session checkpoint succeeded and Polariton then
accepted an edit **or published an already-running task's completion** while
Toron drained, the window would close with visible state absent from the
generation it had just called safe. Successful checkpoint and eventual close
must therefore be one message-frozen transition, with the drain's own completion
admitted as the event that finishes it.

## Decision

### 1. Startup chooses one of three explicit authorities

Polariton parses startup into a closed sum:

```text
Ephemeral { initial_file: Option<String> }
CreateSaved
OpenSaved(SessionId)
```

The command-line spelling is:

```text
polariton                         # existing ephemeral scratch
polariton path/to/file.rn         # existing ephemeral initial file
polariton --session new           # create one saved session
polariton --session <32-hex-id>   # continue that exact saved session
```

`--session` occurs at most once and requires exactly one value. `new` is a
reserved selector, not a friendly session name. Every other value must parse as
the canonical `SessionId`; uppercase, zero, short, malformed, missing, and
duplicate values refuse before the application window opens.

A saved-session selector and a positional initial file are mutually exclusive.
PRODUCT-0060 already rejected letting an initial file override or append to a
restored session, because that gives startup two document authorities. `--`
retains its current meaning for dash-prefixed ephemeral paths, and unknown flags
retain their current future-option posture. Existing invocations therefore keep
their accepted meaning; the new option only turns a previously ignored flag
plus its value into an explicit authority.

Production supplies `SessionStore::platform_default` to one startup resolver;
tests supply a temporary store to that same resolver rather than overriding the
user's platform directory or duplicating launch logic. `CreateSaved` calls
`create` once; `OpenSaved` calls `open_writable` once. The resulting writable
handle is passed exactly once into `CodeEditor::open_saved_with_init`, which
registers PRODUCT-0062's coupled owner before the engine thread starts. An open
or restore failure exits nonzero with the exact session identity and cause. It
never opens an empty window and never falls back to an ephemeral or read-only
session.

The surface is available in the default `toron` build. A build without that
feature remains compilable; selecting `--session` there refuses explicitly
rather than silently starting an ephemeral editor.

### 2. Identity is visible; discovery remains absent

A saved window title is exactly:

```text
Polariton — session <SessionId>
```

The existing ephemeral title remains exactly `Polariton`. A saved window gains
a `Session` menu containing `Checkpoint` and `Copy session ID`; an ephemeral
window has no session menu. Copying yields the canonical 32-character spelling
accepted by `--session`.

The full identity is deliberately visible rather than abbreviated. It is the
only reopening token in v1, and an abbreviation would require a collision and
lookup policy the store does not have.

This record adds no friendly session name, picker, directory scan, recent list,
or last-session pointer. Starting Polariton with no selector remains ephemeral
even when saved sessions exist. A later selector can build an index as its own
owner and must decide deletion, aliases, ordering, privacy, and contention; none
of those decisions are smuggled into the first usable path.

### 3. One checkpoint controller owns manual and close-time publication

`Session -> Checkpoint` publishes one PRODUCT-0061 coupled generation using the
current projected `SharedIdentityCensus` and PRODUCT-0062 runtime colour state.
Success reports `Session checkpointed`; failure reports the complete cause and
leaves the editor open and usable. An ephemeral editor has no checkpoint action,
and no generic libaryon `checkpoint()` is called for the coupled engine.

An ordinary close request on a saved session — custom title-bar close or `:q` —
uses the same operation before any teardown begins. A successful checkpoint is
the permission to continue into the existing local-workspace drain and window
close. An ephemeral close retains today's path and writes no saved-session
state.

There is no timed, debounced, edit-count, focus-loss, or background checkpoint.
There is also no dirty-bit optimization at the session level: a saved close
publishes one complete generation even when the user believes nothing changed.
Documents, logical layout, panel meanings, and colour allocation have different
mutation sources, and guessing cleanliness from one of them would create a
partial authority.

Checkpointing remains synchronous and ordered as PRODUCT-0060 defined it. The
operation is explicit and rare; moving it to a second frontend-owned snapshot
thread would weaken the FIFO boundary it exists to provide.

### 4. Retry is user-bounded, and close coalescing is trivial

Each manual checkpoint or close intent submits **exactly one** coupled
checkpoint request. Polariton adds no automatic retry. If queued engine work
makes the application witness stale, the request refuses without mutation, the
window remains open, and the status says that the workspace changed before the
checkpoint reached the FIFO head and the user may checkpoint or close again.
The next user action rebuilds from the latest complete projected census.

This is the bounded-retry decision PRODUCT-0061 required:

```text
automatic retries per intent  0
maximum in-flight requests    1
coalescing                    unnecessary; update handles one synchronous
                              request to completion before the next intent
starvation                    impossible; there is no retry loop
```

A repeated click or repeated `:q` is a new deliberate attempt after the prior
result is visible, not a queued retry. Continuous editing may make repeated
user attempts stale, but cannot trap the application in an automatic loop or
publish an application payload from a different moment.

If a later record adds periodic or debounced checkpointing, this decision does
not license it. That record must supply its own bounded retry, coalescing, and
starvation policy under continuous mutation.

### 5. Close is message-frozen after it begins, and failure never masquerades
as success

When a saved close request begins, Polariton enters an explicit close state and
stops accepting editor mutations, runs, pane/layout mutations, connection
changes, and further close/checkpoint intents. This is not merely an input gate:
it also governs completions from tasks initiated before close. The checkpoint
call is synchronous within the close update, so no application message
interleaves with its result. There are two outcomes:

- **checkpoint refuses:** cancel the close state, restore ordinary input, keep
  the session lock and all live state, resume ordinary handling of queued task
  completions, show the full error, and expose `Session -> Close without
  checkpoint` for that failed close;
- **checkpoint succeeds:** keep the message freeze, clear the discard escape,
  drain the local Toron workspace if present, and close the window. No ordinary
  input or asynchronous completion is allowed to publish state between
  committed generation and process-final `AppState::drop`.

On the successful branch, pending runs that have not yet dispatched are
abandoned. Work already dispatched to Toron, SQLite, or an external adapter is
not claimed cancellable: it may finish, and a database side effect may already
have occurred. Its completion message is consumed without installing a result,
minting a panel or group, changing a `DataFrame`, updating colour allocation, or
changing ordinary status. Persistence never promised to checkpoint or roll back
database state; it does promise not to show a post-checkpoint result and then
silently discard it with the window.

The same rule covers plot/export, connection, scratch-open, local-workspace
startup, and other task completions already in flight. The one state-transition
exception is `Message::LocalWorkspaceStopped`, which may update shutdown status
and emit the final window-close task. Without that explicit exception, the
asynchronous drain path at `state.rs:2084` would deadlock under the freeze. When
no local workspace exists, the successful checkpoint emits the close task
directly and needs no completion message.

`Close without checkpoint` is not an ordinary always-visible quit command. It
is armed only by a failed close-time checkpoint, and its label names the loss.
Invoking it skips session publication, re-enters the same message-frozen
local-workspace drain, and closes. It releases the lifetime lock by dropping the
editor; it does not delete, repair, or advance the saved session. Whatever
complete generation `CURRENT` names remains the next open's authority; after an
ordinary pre-publication refusal, that is the predecessor.

A failed manual `Session -> Checkpoint` reports failure but does not arm the
discard action, because no close was requested. A later ordinary close makes a
fresh attempt. A post-`CURRENT` publication-uncertain failure follows the same
surface: the window stays open, later ordinary checkpoints continue to refuse
through the poisoned handle as PRODUCT-0060 requires, and the user must copy
needed work or explicitly close without checkpoint. Polariton does not disguise
a teardown/reopen flow as recovery.

Process crash, `SIGKILL`, panic, power loss, or an external kill remains outside
the close controller. `Drop` publishes nothing. The kernel releases the lock and
the next open observes the last generation whose `CURRENT` publication
completed.

### 6. Contention is refusal, never an authority switch

Opening a session already held by another writer refuses before a Polariton
window opens. The diagnostic names the requested `SessionId`, says that another
process holds it, and offers only truthful remedies: retry after that owner
exits, or explicitly create/open a different session.

Polariton does not silently:

- open the session read-only;
- fall back to ephemeral startup;
- create another session;
- delete or replace a lock file;
- wait with a timeout or attempt takeover.

Those would each change the authority selected by the command line. PRODUCT-0059
defines contention as an ordinary explicit refusal and the first surface keeps
that meaning.

### 7. Durable named panels are the live acceptance exhibit

No new Rune syntax or panel allocator is added. The accepted spelling remains:

```rune
relation.views([
    view("bars", figure(bar("region", "revenue", "product"))),
    view("data", table().show_types()),
])
```

Within one selected saved session, completing the producer resolves the named
view through PRODUCT-0062's restored relation:

```text
(DocumentId, LocalViewIdentity::Named("bars")) -> HostPanelId
```

The panel occupies the logical placement restored by PRODUCT-0061, and its
categorical assignments resume the PRODUCT-0062 colour allocation. Reopening
never executes the producer: the restored panel is an honest unresolved
placeholder until the author runs the buffer. On that completion it resolves
in place, with the exact prior `HostPanelId`, group, placement, and palette
positions.

The durable workspace component in PRODUCT-0010 §5 is now `SessionId`. Because
the application owner is already scoped to one session generation, the stored
relation need not repeat it inside every key; semantically the complete key is:

```text
(SessionId, DocumentId, LocalViewIdentity::Named(name)) -> HostPanelId
```

This record supersedes PRODUCT-0010 §5 only where its pre-persistence premises
changed:

- a scratch document in a saved session has a durable `DocumentId`, so its named
  panels survive restart;
- rename and current `:w <path>` behavior update the same document's path
  attribute, so the binding stays without migration;
- a separately opened or created document receives a new `DocumentId`, so the
  same name is a distinct key and cannot claim the original panel.

Anonymous views also restore according to PRODUCT-0062's accepted
`Anonymous(ordinal)` meaning, but this record does not strengthen their
identity. Inserting a view before them still shifts ordinals and may move panel
and colour continuation. Named views are the durable address when source-order
stability is required.

### 8. Persistence restores intent and placement, not execution

Opening or creating a saved session does not connect a database, execute SQL or
Rune, rebuild a relation, or restore the in-memory SQLite `:memory:` database.
The existing scratch database is a fresh process resource. Restored documents
and placeholders tell the truth about what was authored and where its output
belonged; running code is a separate user action.

The nested dashboard remains outside the saved payload exactly as PRODUCT-0061
decided. Its `PlotKernel`s and `DataFrame` are derived state, and persisting its
pane grid without them would restore empty shapes while implying recovered
content. This surface does not use the dashboard as a proxy for session
persistence.

One Polariton process opens one writable saved-session authority. Multiple
windows, a read-only viewer, session switching inside a live window, export,
copy, delete, and rename are later surfaces with distinct ownership decisions.

## Implementation sequence

1. Replace `initial_file_from_args` with a pure startup-target parser and bank
   the existing positional/`--` behavior before adding `--session`.
2. Resolve `CreateSaved`/`OpenSaved` through `SessionStore::platform_default`
   before ordinary GUI startup, then hand the non-clone writable handle exactly
   once into `CodeEditor::open_saved_with_init`. Keep no-default-feature builds
   green and make their saved-session refusal explicit.
3. Carry `Option<SessionId>` as application presentation state; make the title
   dynamic and add the saved-only `Session` menu with exact copy behavior.
4. Add one checkpoint controller and the explicit checkpoint action. Preserve
   the complete typed/store error until the UI boundary rather than reducing
   every cause before close policy can classify it.
5. Refactor the existing close branch into checkpoint, local-workspace drain,
   and window-close phases. Bank the current ephemeral close behavior before the
   saved branch; add the failed-close-only discard action.
6. Drive the full production startup and close path through a temporary store in
   tests, then add the named Rune-view continuation exhibit and evidence.

No step may refresh plot, table, SVG, renderer, or ordinary editor baselines.

## Acceptance gates

1. **Startup parsing is a pure, exhaustive matrix.** Existing no-argument,
   positional-file, unknown-flag, and `--` fixtures retain their exact results.
   `--session new` yields `CreateSaved`; a canonical nonzero ID yields
   `OpenSaved`; missing, duplicate, malformed, uppercase, zero, and
   session-plus-positional forms refuse with specific diagnostics. Arguments
   after `--` are paths, never session selectors.
2. **The default remains ephemeral.** Supply the production startup resolver a
   temporary store containing saved sessions; ordinary no-selector startup
   opens a fresh ephemeral scratch, takes no session lock, creates no generation,
   exposes no Session menu, and retains title `Polariton`. The existing
   initial-file startup is unchanged.
3. **Create and open traverse production entrypoints.** `--session new` mints
   exactly one ID and starts `open_saved_with_init`; close and reopen via
   `--session <that-id>` restore the same `SessionId`, documents, logical
   layout, and application owner. No test-only constructor stands in for the
   main/GUI routing under test.
4. **Identity is a round-trippable UI fact.** A saved title contains the exact
   canonical ID, `Copy session ID` writes those exact 32 bytes, and feeding them
   to the parser selects the same session. The ephemeral title and menu remain
   unchanged.
5. **Contention preserves the selected authority.** While one process holds a
   saved session writable, a second production startup for the same ID refuses
   before opening a window and names contention. It neither creates a session
   nor opens ephemeral/read-only state. After the first process exits, the same
   command succeeds.
6. **Manual checkpoint is one coupled generation.** From a saved window, mutate
   a document, split and move its panel, and establish Rune colour meanings;
   `Session -> Checkpoint` publishes all four facts in one generation and
   reports success. A fault at every PRODUCT-0061 publication seam leaves the
   complete predecessor or complete successor, never a mixed owner set.
7. **Close checkpoints before teardown.** A saved close with dirty scratch,
   changed layout, and changed colour state publishes successfully before the
   local workspace owner is taken or the window-close task is emitted. Reopen
   observes every change. The same title-bar and `:q` requests use this one
   branch.
8. **A stale close is one refused attempt, not a loop.** Queue an identity
   mutation ahead of close while Polariton still holds the preceding census.
   The first close submits once, refuses stale, keeps the window and lock live,
   restores input, and reports that the user may retry. After the next projected
   census, a second user close submits once and succeeds. Counters prove zero
   hidden retries and at most one in-flight request.
9. **Successful close freezes every post-checkpoint producer.** Before close,
   leave one run waiting on its execution snapshot and hold one already-
   dispatched Rune-view run behind a completion barrier. After checkpoint
   success, also inject editor, run-initiation, pane, connection, plot/export,
   scratch-open, local-workspace-startup, and repeated-close messages. Prove the
   pending run is never dispatched and the completed run is consumed without
   installing a result, minting a panel/group, changing a `DataFrame` or colour
   allocation, or updating ordinary status; the other messages likewise cannot
   mutate state or queue engine commands. An already-dispatched side-effecting
   database fixture may complete externally, pinning that the freeze discards
   presentation rather than claiming rollback. While the stop task is held,
   `LocalWorkspaceStopped` is the sole admitted state transition; releasing it
   updates shutdown status and emits the window-close task. Reopen and observe
   exactly the state committed before shutdown — no accepted-or-published state
   was silently lost.
10. **Failure is visible and discard is deliberate.** A pre-`CURRENT` store
    fault during close leaves the window usable, keeps the predecessor current,
    and exposes `Close without checkpoint`; ordinary close retries publication.
    The discard action appears only after that failed close, advances no
    generation, performs the normal Toron drain, and closes. A manual checkpoint
    failure alone does not expose it.
11. **Publication uncertainty remains terminal for the handle.** Inject a
    post-`CURRENT` fault, prove the window remains open and reports that later
    checkpoints through this engine are unavailable, and prove a subsequent
    ordinary close cannot masquerade as recovery. Explicit discard closes; a
    fresh open selects whichever complete generation `CURRENT` names under
    PRODUCT-0060's contract.
12. **Crash is not close.** Kill a process after edits but before any successful
    checkpoint. Reopening sees the preceding generation and the lock is released;
    no destructor, signal path, or `AppState::drop` published the edits.
13. **A named panel survives the real restart boundary.** Through the production
    argument parser and startup resolver, with only the store root injected as a
    temporary, select `--session new`, author and run two named Rune views,
    record their exact `HostPanelId`s, group, placements, and resolved category
    colours, close, reopen by the copied ID, and prove no Rune runs at startup.
    Re-run the producer; both views resolve in their restored panels with the
    exact IDs, group, placements, and colours.
14. **Scratch is durable; documents remain the namespace.** The gate-13 producer
    is repeated in an untitled scratch and survives restart. Renaming or
    `:w <path>` preserves its `DocumentId` and panel IDs. A genuinely separate
    document with identical bytes and view names receives a fresh `DocumentId`
    and distinct panels. The same document in a newly created `SessionId` also
    starts a fresh identity and colour lineage.
15. **Restoration never executes authored code.** A saved Rune buffer contains
    a deliberately observable database write and a named view. Reopen restores
    its bytes and unresolved placeholder without performing the write or
    constructing a kernel; only explicit Run causes both. A saved SQL buffer and
    the process's fresh `:memory:` database assert the same boundary.
16. **The no-toron and ordinary suites remain honest.** `cargo check` and tests
    pass with default features and with `--no-default-features`; the latter
    explicitly refuses `--session`. No existing editor, Rune, table, plot, SVG,
    GPU evidence, dashboard, or Baryon baseline changes. The PRODUCT-0057
    opt-in render target still builds with all features.

## Stop conditions

> If a saved `AppState` cannot receive the one non-clone writable handle without
> opening or creating the session more than once, stop. Add a one-shot GUI boot
> handoff; do not reacquire the lifetime lock or mint a replacement session in
> the initializer.

> If the existing asynchronous local-workspace drain cannot exclude both direct
> mutations and in-flight task completions after a successful checkpoint while
> still admitting `LocalWorkspaceStopped`, stop. Do not call a generation
> durable and then accept or publish state the closing window will discard; do
> not block the one message required to finish shutdown.

> If a named Rune view needs a new identity, path-derived key, or re-execution at
> startup to return to its panel, stop. PRODUCT-0062 already owns the meaning;
> this record is the reachability surface, not a second mapping.

> If the session surface requires placement, ordering, descriptors, or panel
> meanings to enter `SharedIdentityCensus`, stop. PRODUCT-0061's three-set witness
> is sufficient only while that projection stays purely observational.

> If implementation introduces periodic, debounced, or concurrent checkpoint
> requests, stop. The zero-retry synchronous policy in §4 does not decide
> coalescing or starvation for an automatic producer.

> If a useful first launch requires a friendly-name registry, session scan,
> delete operation, or implicit last-session authority, stop and draft that
> discoverability surface separately. Do not make a directory enumeration into
> an accidental product database.

## Rejected alternatives

- **Ship named-panel persistence as an isolated record first:** all its gates
  would use test-only saved constructors while the behavior remained
  unreachable. The session surface is the missing mechanism; named panels are
  its first acceptance exhibit.
- **Open the last session automatically:** changes no-argument startup from
  ephemeral to durable, needs a last-session authority and contention policy,
  and makes merely having used persistence once change future startup.
- **Generate a Rune session script:** `.rn` documents already are authored
  intent. A generated script would become a second representation and still
  could not own libaryon's logical layout or exact unsaved bytes.
- **Let `--session` coexist with an initial file:** gives startup two document
  authorities and reopens PRODUCT-0060's explicitly rejected merge question.
- **Fall back on contention:** opening ephemeral, read-only, or a new session
  after the user selected one exact ID silently switches authority.
- **Checkpoint on `Drop` or signal handlers:** cannot report failure, races
  teardown, and conflates clean close with crash recovery.
- **Checkpoint after Toron shutdown:** a checkpoint failure would strand the
  user in a half-closed application. Publication must succeed before teardown
  begins.
- **Accept edits or publish in-flight results while Toron drains:** creates
  visible state newer than the committed generation and then discards it on
  close. Already-dispatched external effects may finish, but their UI results do
  not cross the committed close boundary.
- **Retry stale witnesses automatically until success:** continuous mutation can
  starve close forever. One user intent means one request in this record.
- **Timed or debounced autosave:** grows a second checkpoint producer and needs
  coalescing, retry, rate, and failure-notification policy. Close-time safety and
  an explicit action satisfy the first surface without it.
- **Always-visible close-without-checkpoint:** makes destructive exit an ordinary
  adjacent command. It appears only as the explicit escape from a reported
  close-time failure.
- **Persist dashboard kernels or the scratch database:** both are derived
  process state. Restoration preserves authored buffers and placement and never
  claims to resurrect execution.

## Consequences

- Saved sessions become usable without changing Polariton's ordinary startup.
  The first surface is intentionally exact-ID and CLI-first: austere, complete,
  and built on the identity the store already owns.
- A normal saved-session close is a durable transaction. Failure keeps the
  application live; loss requires an explicitly named action after that failure.
- The asynchronous Toron drain becomes part of the session close state machine.
  The application rejects interaction and late task publication once its final
  generation commits, while admitting the drain-completion message that closes
  the window.
- No background durability promise is implied. Before a successful explicit or
  close-time checkpoint, a crash loses changes since the previous generation.
- Durable named panels finally satisfy PRODUCT-0010's deferred across-restart
  key. Scratch documents do better than its original premise because they now
  possess durable document identity.
- Reopening is continuation, not execution: documents, topology, semantic panel
  meanings, and colours return; databases, relations, kernels, and pixels do not.
- Session discoverability, friendly naming, autosave, multiple windows,
  read-only viewing, copy/export/delete, dashboard fate, and closed-panel
  tombstones remain separate, now with a working surface on which exhibits can
  be gathered.
