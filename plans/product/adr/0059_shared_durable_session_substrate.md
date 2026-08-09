# ADR 0059: shared durable session substrate

Status: accepted 2026-08-08. This record establishes the durable identity,
ownership, concurrency, and atomic-snapshot substrate that libaryon's heads can
share. It intentionally does not restore a document, pane, result, database, or
plot. Those consumers follow in order after the substrate can name a continuing
session and its documents without runtime IDs. Implementation evidence is
recorded in `../impl/0059_shared_durable_session_substrate_evidence.md`.

## Context

Polariton now has a mature language for authored workspace intent, but its
operational state ends with the process. Layout, scratch buffers, unsaved edits,
view-to-panel linkage, and categorical-color continuation do not survive a
restart. Durable named-panel identity has consequently remained blocked on
layout and document persistence since PRODUCT-0010 and PRODUCT-0015.

Stage 0 found that the first missing mechanism is not a Polariton serializer.
libaryon's `EngineHandle` owns documents and their text. Polariton's
`CodeEditor.document_texts` is explicitly a cache of snapshots observed from
that authority. Persisting the cache would create a second session format for
the same buffers and allow the Baryon and Polariton heads to disagree about
which document exists or which text is current.

Polariton also has no general serialization dependency, but that is not an
argument for generating Rune. The existing `.rn` buffer is already the authored
program. A generated session script would duplicate that authority and still
need new language for split ratios, scratch identity, and panel attachment.
Startup configuration remains a separate Rune-authored preference mechanism;
session operational state is data.

The runtime identifiers cannot become that data:

```text
PaneId    = monotonic u32 allocated in this process
BufferId  = monotonic u64 allocated in this process
ViewId    = monotonic u64 allocated in this process
```

Polariton's two persistence-relevant indexes are currently rooted in those
ephemeral values:

```text
rune_plot_panes:
    BufferId -> PaneId

rune_categorical_identities:
    (BufferId, LocalViewIdentity, layer ordinal) -> ColorIdentity
```

Opening the same files in a different order changes the buffer numbers, and a
restored pane receives a fresh pane number. Serializing either map verbatim
would produce keys that match nothing after restart.

`LocalViewIdentity` is different. A named view carries its authored name; an
anonymous view carries its presentation ordinal. Both reproduce when identical
source completes identically. The anonymous form is deliberately weaker:
inserting one view above another shifts later ordinals and therefore changes
their view and color identity. Persistence does not create that behavior, but
makes it visible across restart. This record names the consequence rather than
silently promising stability the current language does not have.

libaryon's `ShadowSession` is not a workspace-session store. It is a per-file
generation journal, rooted beside one original file, for large-edit recovery.
It supplies useful precedents for versioned metadata and atomic replacement,
but it neither owns the document set nor coordinates concurrent processes.
Two writers can currently choose the same next generation and converge through
last-writer-wins renames. Widening it would inherit the exact ownership and
concurrency defect this substrate exists to remove. It remains bounded and
unchanged.

The shared store must assume concurrent heads. Baryon and Polariton can address
the same engine state, and separate application processes may attempt to open
one saved session. Atomic rename prevents a torn file but cannot prevent lost
updates between two live writers. There is no honest merge algebra for unsaved
text, undo state, pane topology, or categorical-color allocation.

The platform already supplies the needed exclusion primitive. `libc` is a
workspace and libaryon dependency, and Unix `flock` releases its advisory lock
when the owning process dies, including panic and `SIGKILL`. A PID-and-timestamp
lease would instead need an unsafe stale-owner heuristic vulnerable to PID
reuse, clock assumptions, containers, and namespaces. The kernel-held lock is
both smaller and stronger, within one explicit boundary: the durable store is a
local-filesystem facility. Network filesystems whose advisory-lock semantics
cannot be trusted are unsupported.

Finally, persistence must distinguish authored inputs from derived artifacts.
Relations, `DataFrame`s, resolved plots, kernels, SVG, draw operations, and
pixels are recomputed. Categorical color allocation is the one already-visible
continuation state that later needs an owner extension: PRODUCT-0007 promises
stable assignments for the life of a session, so continuing a saved session
must preserve its allocation and tombstones. A new session begins fresh. This
record supplies the identity and partition on which that later choice can be
implemented; it does not serialize Polariton state itself.

## Decision

### 1. A saved session has one opaque, durable identity

libaryon owns `SessionId`, an opaque nonzero 128-bit value minted when a saved
session is explicitly created. Its canonical filesystem spelling is 32
lowercase hexadecimal digits. Creation uses fresh random bytes and an exclusive
directory create; the vanishingly unlikely collision retries rather than
opening or overwriting the existing session. The type exposes equality,
hashing, parsing, and display; it does not expose arithmetic or path
construction from unvalidated text.

Creating a saved session always mints a new identity. Opening an existing saved
session continues its identity. Copying or exporting a session is a separately
named operation and must decide whether it preserves or remints identity; this
record adds neither operation.

No implicit “last session” policy arrives here. The existing default remains an
ephemeral session and writes no durable-session state. Startup selection,
recent-session lists, command-line spelling, and UI belong to the surface record
that first makes the store reachable.

### 2. Every durable document has a minted session-scoped identity

libaryon owns `DocumentId`, an opaque nonzero value minted inside one
`SessionId`. Its semantic key is therefore `(SessionId, DocumentId)`, even if
the Rust value stores only the session-local component while held by an opened
session.

Identity exists independently of location:

- a scratch document receives a `DocumentId` before it has any path;
- opening a file creates a document record whose path is an attribute;
- rename, move, and `:saveas` update that attribute and never change identity;
- closing or removing a document never makes its identifier reusable within
  the continuing session;
- a different session may assign the same local number without collision,
  because `SessionId` is part of the durable key.

The initial allocator is a persisted, monotonically increasing nonzero `u64`.
Minting checks exhaustion and refuses rather than wrapping or reusing an ID.
The single-writer rule in §5 makes allocation serial without a distributed
counter. Because v1 is Unix-only, a path attribute stores the exact `OsStr`
bytes in a tagged lowercase-hex representation. It does not require UTF-8,
normalize components, resolve symlinks, or use a display string as identity.

At runtime, the owning engine maintains the association between fresh
`BufferId`s and durable `DocumentId`s. `BufferId`, `PaneId`, and `ViewId` never
enter a durable manifest. Reopening documents in a different order may produce
different runtime IDs while preserving every document identity.

This is the hard dependency for all Polariton persistence. A future extension
may replace the buffer component of its keys with `DocumentId`, but it may not
persist the current counter-rooted maps before libaryon can restore documents
and publish the new `DocumentId -> BufferId` bindings.

### 3. The store is one atomic snapshot partitioned by owner

Saved sessions live below a head-neutral platform state root:

```text
Linux/XDG:  $XDG_STATE_HOME/libaryon/sessions/<SessionId>/
            or ~/.local/state/libaryon/sessions/<SessionId>/
macOS:      ~/Library/Application Support/libaryon/state/sessions/<SessionId>/
```

The v1 durable API is Unix-only because §5 requires `flock`. An unsupported
platform refuses persistence explicitly; it does not emulate a weaker lock
while reporting the same guarantee. A remote filesystem is outside the API's
documented contract even when the operating system cannot classify it
reliably. Existing Baryon history, configuration, and per-file `.baryon` shadow
paths do not move.

All traversal below the resolved state root is relative to trusted directory
descriptors. Directories and regular files are opened without following
symlinks and validated at the boundary; `CURRENT`, `session.lock`, a generation,
or an owner name cannot redirect an operation outside the session root. Only
validated session, generation, and owner identifiers may select child paths. A
document's path attribute is inert manifest data and is never joined to the
store root. Every raw `open` or `openat` in the store includes `O_CLOEXEC` in
the atomic open flags; setting close-on-exec afterward would leave a
multithreaded fork/exec race.

Each session directory has a stable lock inode, one atomic current-generation
pointer, and immutable committed generations:

```text
session.lock
CURRENT
generations/
  0000000000000001/
    store.json
    owners/
      <owner id>.payload
```

`store.json` contains the store schema version, session identity, generation,
next document identifier, and libaryon's document identity records. Owner
payload descriptors name an owner, that owner's independent schema version, and
the exact payload byte length used to reject a missing or truncated generation;
the payload filename is derived exactly as `<owner>.payload`, never read as a
free path from the manifest. The byte length validates the container and does
not interpret the owner's schema.

An owner ID is a validated lowercase ASCII identifier matching
`[a-z][a-z0-9_-]{0,63}`. It is both the authority name and the complete payload
stem; separators, dots, Unicode, empty names, and aliases refuse. The root
schema uses strict field recognition. Adding a base field requires a root
version old readers refuse, rather than letting an old writer silently erase a
new fact it ignored.

The root schema governs the container and libaryon base. Each extension owner
governs its own payload schema. libaryon treats unknown owner payloads as opaque
bytes. A commit that updates the base or one known owner copies every untouched
owner payload byte-for-byte into the new generation. It may not parse, normalize,
drop, or rewrite another owner's state. Polariton can therefore later add
layout, view, and color continuation without moving document authority or
making Baryon depend on Polariton types.

One generation is the unit of truth. Consumers never combine `store.json` or
owner payloads from different generations. This prevents a restored document
set from racing ahead of the pane or color state that names it.

### 4. A commit publishes only a complete generation

The writer creates the next generation in a private temporary directory on the
same filesystem, writes the base and every owner payload, synchronizes their
files and containing directories, then atomically renames the complete
generation into `generations/`. Only afterward does it atomically replace and
synchronize `CURRENT`.

Readers follow `CURRENT` and validate the entire referenced generation before
returning it. A temporary, incomplete, or unreferenced generation is not a
partially valid session and may be reclaimed by a later writable open. Failure
before `CURRENT` changes leaves the preceding generation authoritative. Failure
after the pointer replacement yields the new complete generation.

Pointer replacement is the commit point even if synchronizing the session
directory subsequently fails. The opened handle advances to the new generation
immediately and becomes publication-uncertain: it may be inspected and dropped,
but cannot clean or commit again. Reopening under a fresh lock re-reads
`CURRENT` and establishes which complete generation the filesystem exposes.
This prevents a retry from treating the old generation as current and deleting
the newly published one.

The generation directory is a publication mechanism, not session history. The
snapshot referenced by `CURRENT` is the only committed truth; after publication
and directory synchronization, a later exclusively locked open may reclaim all
other complete or incomplete generations. It never removes the old current
generation before the new pointer is durable. Undo and
historical session snapshots remain separate decisions.

The store refuses rather than guesses when `CURRENT` is malformed, absent from
a non-new store, points outside the generation directory, references a missing
or incomplete generation, disagrees about session or generation identity, or
contains an unsupported root schema. It does not scan for the numerically
largest directory and call that current, because doing so could publish a
generation the interrupted writer never committed.

Atomic replacement is a crash-consistency mechanism, not a concurrency policy.
Every write still requires the lifetime lock in §5.

### 5. One OS advisory lock defines session access

Opening a saved session opens the stable `session.lock` file and acquires one
nonblocking advisory lock for the entire opened-session lifetime:

```text
Writable  flock(fd, LOCK_EX | LOCK_NB)
ReadOnly  flock(fd, LOCK_SH | LOCK_NB)
```

The lock file is opened with `O_CLOEXEC`, and the acquired descriptor retains
`FD_CLOEXEC`. A git observer, user-configured job, shell, language process, or
other child must not inherit the open file description and keep the advisory
lock alive after the owning editor dies.

For an existing session, acquiring the requested lock is the first operation
after safely opening the session directory and lock inode. No `CURRENT` byte is
read, no generation is enumerated or validated, and no cleanup is attempted
before the lock succeeds. Session creation likewise creates and exclusively
locks the permanent lock inode before publishing its first generation. Lock
contention therefore wins over every snapshot or recovery diagnostic.

Multiple read-only processes may coexist. A writable opener excludes every
other writable or read-only opener, and any existing reader excludes a writer.
Read-only sessions may inspect the committed snapshot but cannot mint a
`DocumentId`, change an attribute, reclaim generations, or commit an owner
payload.

That distinction is carried by the API rather than caller discipline. The
opened store owns its lock descriptor for its entire lifetime and has distinct
read-only and writable handle types, or an equivalent sealed access-mode
parameter. Every mutator requires the writable handle; the raw descriptor and
unlocked mutation primitives are private. Only an explicit successful commit
publishes state. Dropping the handle releases the lock and never promotes
uncommitted in-memory changes; the later consumer owns any dirty-session prompt
or autosave policy.

`session.lock` is a permanent inode for the life of the saved-session directory.
Generation cleanup never renames, truncates, or removes it. Saved-session
deletion is not in scope, because removing the directory safely requires its own
ownership and discoverability policy.

Lock contention is an ordinary explicit refusal. A caller may choose to open a
different session, remain ephemeral, or retry after the owner exits. There is
no timeout takeover, lease-file deletion, PID liveness check, or automatic
“stale” recovery. A live but hung owner remains an owner until the user
deliberately terminates it. Process death releases the kernel lock without
modifying durable metadata.

The guarantee is cooperative and local-machine scoped: every session writer
must enter through this store, and the session root must reside on a local
filesystem with reliable advisory locking and atomic same-filesystem rename.
NFS and equivalent remote mounts are outside v1. Filesystem locality is a
documented deployment precondition, not a promise that every Unix filesystem
can be classified reliably at runtime. The API refuses known unsupported
environments where it can identify them, but absence of such detection does not
license a remote root. Documentation may not call the lock distributed or
network-safe.

Within one process, multiple windows or heads share one opened-session authority
rather than independently locking and committing the same root. The precise
window/session UI lifecycle remains for the surface record.

### 6. The base manifest names documents but does not yet restore them

The v1 base is deliberately small:

```text
SessionSnapshot {
    version
    session_id
    generation
    next_document_id
    documents: [DocumentIdentity { id, path? }]
    owners: [OwnerPayload { owner, schema_version, byte_len }]
}
```

This record implements creation, opening, identity allocation, path-attribute
updates, snapshot commit/read, and runtime rebinding helpers. It does not yet
persist document text, cursor/selection, undo history, language state, or open
order. The following libaryon record owns those payloads and defines which
editor state is authored, recoverable, or derived.

That separation makes the implementation testable without pretending the
session already survives restart. `DocumentId` is production vocabulary and
the store is a production substrate, but no application startup path consumes
them by default in this cut.

### 7. Polariton extensions follow document restoration, never precede it

The persistence arc has this fixed order:

```text
session/document identity + locked atomic store       this record
    -> libaryon document content and restoration      next substrate consumer
    -> Polariton layout/view/color owner extension    product consumer
    -> durable named-panel identity                    unblocked surface
```

The Polariton extension will key document-owned state by `DocumentId`, retain
authored `LocalViewIdentity`, and replace durable `PaneId` use with a stable
layout identity decided in that record. Categorical allocation state is allowed
there as an explicit continuation exception; DataFrames, completed artifacts,
resolved plots, kernels, and rendered output remain recomputed.

`LocalViewIdentity::Anonymous(output_order)` remains positional. With identical
source, restore reproduces it. Inserting, deleting, or reordering an earlier
view shifts later anonymous identities and can reattach or recolor them as new
views. That is today's within-session meaning, not a persistence failure.
An exhibit where the behavior is intolerable triggers stable anonymous-view
identity as its own language decision; this substrate neither freezes source
ordinals nor invents hidden IDs for authored views.

SQL text restoration also does not restore Toron's `:memory:` database. A later
consumer must mark dependent artifacts unavailable or stale and may not
silently replay SQL merely to approximate the old process. Connection and
database persistence require their own authority and security decision.

## Implementation sequence

1. Introduce dependency-free `SessionId` and `DocumentId` newtypes and the
   validated canonical encodings. Bank allocation, exhaustion, path
   independence, and runtime-rebind behavior before adding disk state.
2. Add the Unix local-state locator, private session-directory creation, stable
   lock file, and lifetime `Shared`/`Exclusive` guard. Prove contention and
   crash release with child processes rather than same-process file descriptors.
3. Add the versioned base snapshot and opaque owner descriptors. Keep owner
   bytes outside the parsed base and preserve unknown owners exactly.
4. Add immutable-generation staging, synchronization, atomic publication, and
   fault injection at every boundary before and after `CURRENT` replacement.
5. Expose the substrate to libaryon's engine without selecting or creating a
   saved session during ordinary startup. Leave document content and every
   Polariton consumer absent.
6. Update the persistence-cluster ledger with the fixed dependency chain and
   name `ShadowSession` as a retained per-file journal, not the session store.

Steps 1 through 4 may land as separately green implementation commits. The ID
commit changes no filesystem. The lock commit creates state only in tests or
through an explicit substrate call. The store commit must not land without its
fault-injection and unknown-owner oracles.

## Acceptance gates

1. Two newly created sessions receive distinct, nonzero `SessionId`s whose
   canonical spelling is exactly 32 lowercase hexadecimal digits. Malformed,
   zero, uppercase, truncated, overlong, separator-bearing, and traversal-shaped
   inputs refuse before path construction. A forced mint collision retries and
   leaves the existing session byte-identical.
2. Scratch and file-backed documents both receive nonzero `DocumentId`s. IDs
   are unique and never reused within a session, including after removal;
   allocator exhaustion refuses. The same local value in two different
   sessions is distinct as a durable key.
3. Attaching a path to scratch, renaming, moving, and save-as each preserve the
   exact `DocumentId`. Reopening the committed snapshot preserves the ID and
   latest path attribute. Opening the same path in a newly created session does
   not inherit identity from that path. A valid non-UTF-8 Unix path round-trips
   byte-for-byte; JSON display-string conversion is not the path oracle.
4. A fixture commits three document identities, then rebinds them to fresh
   `BufferId`s in reverse order. Lookups by `DocumentId` recover the intended
   document independent of runtime allocation. Serialized bytes and parsed
   snapshot types contain no `BufferId`, `PaneId`, or `ViewId`.
5. A separate child process holding `LOCK_EX` makes both a second exclusive and
   a shared nonblocking open refuse. Two child processes holding `LOCK_SH`
   coexist, and each read-only handle refuses identity allocation, attribute
   mutation, generation cleanup, and commit. Commits and cleanup preserve the
   exact `session.lock` inode; no second lock file can be created beneath a live
   session. While that lock is held, a deliberately malformed `CURRENT` still
   makes each competing opener report lock contention rather than a snapshot
   error, proving the lock precedes the first pointer read.
6. The crash-release gate has two phases. First, a child acquires the exclusive
   lock, the parent observes contention, and the child is terminated with
   `SIGKILL`; a new exclusive open then succeeds without deleting a file,
   editing metadata, waiting for a lease timeout, or consulting a PID. Second,
   the lock holder spawns and execs a long-lived grandchild before it is killed.
   A new exclusive open succeeds while that grandchild remains alive, proving
   the lock descriptor was closed on exec rather than becoming a phantom
   holder. Production and tests contain no PID/timestamp stale-owner algorithm.
7. Lock tests use distinct processes and a local temporary filesystem. The
   evidence records the filesystem and platform premise. Unsupported platforms
   and known unsupported filesystems refuse the durable API rather than
   weakening the guarantee; documentation names advisory-lock and NFS limits
   and does not claim infallible runtime detection of remote mounts.
8. Fault injection before generation rename, after generation rename but before
   `CURRENT`, and while preparing the replacement pointer leaves the previous
   complete generation authoritative. Successful pointer publication exposes
   the complete new base and all owner payloads together. No reader observes a
   mixed generation. A fault after pointer rename exposes the complete new
   generation, poisons the old handle against cleanup/retry, and becomes usable
   again only after reopen. Generation-number exhaustion refuses without
   staging or altering `CURRENT`.
9. An incomplete temporary generation and a complete but unreferenced generation
   are ignored by read-only open. A later exclusively locked open may reclaim
   them without changing the committed snapshot. Recovery never selects the
   largest generation by directory scan.
10. An unknown owner payload containing arbitrary non-UTF-8 bytes survives a
    libaryon-only update byte-for-byte with the same owner and schema version.
    Updating one known owner leaves every sibling identical. Owner-schema and
    root-schema versions are tested independently; a payload whose byte length
    disagrees with its strict descriptor refuses as an incomplete generation.
11. Malformed `CURRENT`, missing referenced files, duplicate owner IDs, unsafe
    owner filenames, base/pointer generation disagreement, session-ID mismatch,
    unknown base fields, and unsupported root versions each refuse without
    fallback, overwrite, or partial state. A failed open leaves every on-disk
    byte unchanged.
12. Session directories are private to the user and newly created lock,
    pointer, base, and payload files do not grant group or world access. Session
    and owner identifiers, not caller-supplied paths, select children below the
    trusted state root. Directory-relative, no-follow opens reject symlinked
    lock, pointer, generation, and owner entries; an adversarial symlink cannot
    redirect a read, commit, cleanup, or lock outside the session root.
13. The ordinary Baryon and Polariton startup paths remain ephemeral, mint no
    saved `SessionId`, acquire no session lock, and create no durable-session
    files. Configuration paths, command history, and `.baryon` shadow journals
    retain their current behavior and bytes.
14. Polariton gains no serializer, session manifest, durable pane key, or startup
    restore path in this cut. Its current counter-rooted maps remain runtime
    state only. Architecture evidence pins that no Polariton owner payload can
    be committed before libaryon exposes restored `DocumentId -> BufferId`
    bindings.
15. Named and anonymous `LocalViewIdentity` semantics remain byte-for-byte
    unchanged. Documentation records that anonymous output insertion shifts
    later positional identities; the substrate neither claims nor tests stable
    anonymous identity across a source edit.
16. No Rune grammar, application configuration, pane layout, result lifecycle,
    database behavior, plot/table output, screenshot, SVG, or rendering baseline
    changes. The full workspace suite passes with no ignored persistence gate.
17. Every identity, path, cleanup, and commit mutator requires the exclusive
    opened-session capability in its production signature. Read-only handles
    cannot reach those operations, lock descriptors and raw root paths are not
    exposed, and no second writer beneath the session root exists outside the
    store module.

## Stop condition

> This record may add libaryon's opaque session/document IDs, local state-root
> locator, Unix advisory lock, versioned owner-partitioned store, atomic
> generation publication, runtime document rebind vocabulary, tests, and
> documentation. If implementation requires persisting document contents,
> parsing a Polariton payload, serializing a runtime ID, replaying SQL,
> changing ordinary startup, migrating `ShadowSession`, weakening exclusive
> ownership, relying on PID/timestamp stale recovery, supporting a filesystem
> without trustworthy advisory locks, or changing any product pixel, work
> stops and the requirement returns to its owning record.

> If owner payloads cannot be committed in the same atomic generation as the
> base without libaryon understanding their schemas, work stops. Atomic
> cross-owner truth outranks a superficially simpler single-file format.

## Supersession

This record supersedes no shipped persistence behavior because no workspace
session store exists. It resolves the prerequisite left open by PRODUCT-0010
§5/§9, PRODUCT-0011's `PanelRestored` deferral, PRODUCT-0015's layout boundary,
and PRODUCT-0030's session-only color continuation. Their current runtime
identity, placement, and palette behavior remain unchanged until later
consumers land.

It refines the earlier shorthand “layout persistence before durable identity.”
Durable **named-view** identity still follows observable restored layout, as
those records require. The newly separated libaryon `SessionId` and
`DocumentId` are non-visible substrate prerequisites that must precede document
and layout restoration; minting them alone neither reattaches nor makes a view
durable.

It does not supersede libaryon's `ShadowSession`, command-history store, or
Polariton's Rune configuration. Those mechanisms keep their narrower owners
and locations. The shared store borrows atomic-write and versioning discipline,
not their data models or names.

## Rejected

- **Generate a Rune file for the session:** duplicates the already-authored
  `.rn` buffers, cannot represent operational state without widening the
  language, and creates two authorities for source.
- **Persist Polariton's document-text cache:** serializes a non-owner's snapshot
  and permits Baryon and Polariton to restore different versions of one buffer.
- **Use `ShadowSession` as the workspace store:** widens a per-file recovery
  journal that has no document-set ownership or multi-process exclusion and
  inherits its last-writer-wins race.
- **Key durable documents by path:** excludes scratch buffers and silently
  breaks identity on rename, move, and save-as.
- **Serialize a path as a UTF-8 display string:** loses valid Unix filenames and
  makes a human presentation of the path replace its exact bytes.
- **Serialize `BufferId`, `PaneId`, or `ViewId`:** turns process allocation order
  into durable meaning and restores references that cannot bind.
- **Let Polariton persist before document restoration:** gives its extension no
  stable producer key and forces a later migration from counter-rooted data.
- **One application-shaped manifest:** makes libaryon parse head-specific state
  and lets one head erase fields it does not understand.
- **Independent owner commits:** permits document and pane/color generations to
  disagree after a crash. Owner schemas are independent; publication is not.
- **A PID/timestamp lease file:** needs an unsafe stale-owner verdict, is fooled
  by PID reuse and namespaces, and duplicates crash recovery the kernel lock
  already supplies.
- **Set `FD_CLOEXEC` after raw `openat`, or rely on child code to close it:**
  leaves a fork/exec race in a multithreaded editor and lets an unrelated git or
  user job become the invisible lock owner. `O_CLOEXEC` is part of the atomic
  open.
- **Automatically break a live owner's lock after a timeout:** admits two
  writers in the exact failure mode the lock must exclude. A hung owner requires
  deliberate process recovery.
- **Call atomic rename the concurrency policy:** prevents torn publication but
  not two writers from deriving and publishing incompatible next generations.
- **Scan for the newest generation after a crash:** can publish a staged
  generation whose writer never advanced `CURRENT`.
- **Pretend advisory locking is distributed:** `flock` supplies the required
  local-machine rule, not an NFS or cross-host session protocol.
- **Persist completed relations, DataFrames, kernels, or pixels:** stores derived
  output with independent invalidation and migration problems instead of
  recomputing from its owners.
- **Restore an in-memory database by replaying saved SQL:** executes code and
  side effects under an implicit recovery policy and cannot prove equivalence
  to the lost Toron state.
- **Mint hidden stable IDs for anonymous Rune views now:** changes the language's
  accepted positional identity without an exhibit and belongs after durable
  documents/layout make the problem observable.
- **Ship automatic last-session restore with the substrate:** couples format and
  concurrency decisions to startup UX before any document payload can restore.

## Consequences

- “Continuing a session” becomes a structural fact: the same `SessionId`, held
  under one lock, with preserved `DocumentId`s. “New session” remints the root
  identity and begins fresh continuation state.
- Rename and save-as stop being identity changes. Scratch documents can
  participate in later pane and color persistence without acquiring fake paths.
- Baryon and Polariton can share one atomic snapshot while retaining strict
  authority over their own schemas. Unknown head state survives a write by the
  other head.
- Crash recovery has no stale-lease heuristic. The cost is an explicit
  local-filesystem and Unix-v1 boundary, plus refusal while any incompatible
  opener remains alive. Close-on-exec keeps “alive” scoped to the session owner
  rather than any subprocess it happened to launch.
- The substrate itself restores nothing visible. That is deliberate: identity,
  atomicity, and ownership land before the first consumer can make durable
  promises atop them.
- The persistence cluster is now strictly ordered rather than merely listed.
  Document content restoration is next; only then can Polariton persist layout,
  view linkage, and categorical continuation, which in turn unblocks durable
  named panels.
- Anonymous views retain their positional identity. A source edit can therefore
  make a continuing session treat later anonymous outputs as different views;
  the known consequence supplies the trigger for a future identity record
  without misclassifying it as store corruption.
