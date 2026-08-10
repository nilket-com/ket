# ADR 0073: saved-session discovery

Status: proposed 2026-08-10. This record adds the first discovery surface for
the durable sessions PRODUCT-0059 through PRODUCT-0063 shipped:

```text
polariton --session list
```

The command lists committed sessions with their full reopening identity,
committed generation time, document count, and document paths. It does not open
a GUI, select a session, create an index, mint an alias, delete anything, or
change bare Polariton startup. A live exclusively-open session remains visible
as `active`, but its snapshot metadata stays unknown because PRODUCT-0059 makes
lock contention authoritative before the first `CURRENT` read.

## Context

The persistence chain is complete but undiscoverable. The only reopening
spelling is:

```text
polariton --session <32-lowercase-hex SessionId>
```

PRODUCT-0071 correctly removed that machinery identity from GUI chrome. The
consequence is now sharp: unless a user copied the ID somewhere outside
Polariton, the shipped session has no product surface through which it can be
found again. Listing directory names manually under
`~/.local/state/libaryon/sessions/` technically works and answers almost
nothing. A list of opaque IDs alone would be the same operation with nicer
spacing.

The cheapest distinguishing facts already live in the committed base snapshot,
not in a new registry:

- `SessionSnapshot::generation()`;
- `SessionSnapshot::documents()` and every document's optional path; and
- the immutable current generation directory's filesystem modification time.

The first two are durable authorities. The third is presentation metadata: it
orders committed generations usefully but is not session identity, checkpoint
freshness, or a dirty-state proof.

Stage 0 corrected one premise from the initial proposal. `open_read_only` is
non-mutating, but it is not non-contending: PRODUCT-0059 §5 requires
`LOCK_SH | LOCK_NB`, while a running Polariton window holds `LOCK_EX` for its
lifetime. The same record says lock contention wins before every `CURRENT`,
snapshot, or recovery diagnostic. A list cannot both use the accepted reader
and inspect a live writer's committed paths. This record keeps the lease and
shows a reduced `active` row rather than adding an unlocked reader or weakening
the lock.

One real installation currently contains a committed generation with one path:
`../uat/test16.rn`. That is enough information to answer “which session was I
working on?” without assigning a friendly name. Names become a convenience
rather than a prerequisite.

## Decision

### 1. `--session list` is a terminal CLI action

`list` becomes the second reserved non-ID value beside `new`:

```text
polariton --session new
polariton --session list
polariton --session <SessionId>
```

Listing resolves the platform-default `SessionStore`, prints the report, and
exits without loading startup Rune, constructing `AppState`, connecting to
Toron, starting libaryon's engine, or opening an iced window. It accepts no
initial file or additional positional argument. `--` retains its existing
ephemeral-path meaning and does not turn `--session list` into a GUI target.

The parser carries an explicit terminal action rather than smuggling listing
into `gui::StartupTarget`. GUI startup remains the closed three-member set
`Ephemeral | CreateSaved | OpenSaved`; no view or update branch can receive a
list request.

The command follows PRODUCT-0063's build posture. In a build without the
`toron` feature, `--session list` refuses explicitly before touching the store,
as `new` and an ID already do; this record does not use discovery to widen the
saved-session feature boundary.

### 2. Libaryon owns head-neutral enumeration

`SessionStore` gains a read-only discovery operation returning a typed report.
Polariton owns formatting, not filesystem traversal. This preserves
PRODUCT-0059's partition: libaryon owns session roots, canonical identities,
safe directory-relative opens, locks, `CURRENT`, and base snapshot validation;
the head chooses how to present those facts.

Discovery opens the existing root without creating it and enumerates entries
through the already-hardened directory-fd/listing path. An absent root is an
empty store and returns an empty report. A malformed, symlinked, wrongly owned,
or wrongly permissioned root refuses the whole command; it is not equivalent to
having no sessions.

Each canonical 32-hex directory name is parsed to `SessionId` before it can
select a child. Noncanonical root entries are returned as explicit foreign-entry
issues with escaped names. They are never treated as session IDs, traversed as
paths, or silently skipped. A canonical name whose entry is a symlink or regular
file reaches the safe `O_NOFOLLOW` session-open boundary and reports
`Unreadable`; canonical spelling never licenses the wrong filesystem type.

The existing cleanup helper's UTF-8 `list_dir` contract is not weakened. A
discovery-specific raw-name enumeration retains entry bytes so non-UTF-8 foreign
names can be reported without making writable cleanup accept them.

The operation requests no writable handle, creates no directory or lock,
reclaims no generation, rewrites no pointer, and decodes no head-owned payload.

### 3. Discovery has four honest per-session states

For each canonical session identity, libaryon attempts the existing nonblocking
shared open:

```text
Committed   shared lock acquired; complete current generation validated
Active      shared lock refused because an exclusive owner is alive
Damaged     shared lock acquired; committed store validation says Corrupt
Unreadable  shared lock acquired or directory selected; an I/O boundary failed
```

`Active` is deliberately a statement about lease ownership, not process health.
The exclusive holder may be a healthy window, a hung process, or any other live
owner. The kernel releases the lock on process death, so there is no stale-PID
heuristic and no owner payload read.

Most importantly, `Active` carries only the `SessionId`. It does not read
`CURRENT`, enumerate generations, report paths from a possibly changing
snapshot, or let a malformed pointer outrank contention. A list containing one
active session continues and classifies every other entry; it neither waits nor
fails the report.

`Damaged` includes the exact bounded `SessionStoreError::Corrupt` reason.
Malformed `CURRENT`, missing referenced generations or owner files, descriptor
disagreement, invalid root schema, duplicate identities, and unsafe names
therefore remain visible under their canonical session rather than disappearing
from the list. `Unreadable` preserves the I/O reason instead of relabelling a
permissions or transient filesystem failure as corruption.

An entire report is successful when every root entry is classified, including
`Active`, `Damaged`, `Unreadable`, and foreign entries. Those states are the
answer, not hidden command failures. Failure to enumerate or validate the store
root itself exits nonzero because the report is incomplete.

### 4. A committed row contains only base-snapshot identity facts

A `Committed` entry carries:

```text
SessionId
generation number
current generation directory modified time, if the filesystem supplies it
documents ordered by DocumentId
```

Every document contributes its durable ID and either its complete path or the
literal `<scratch>`. The count includes both path-backed and scratch documents.
On Unix, a valid UTF-8 path prints normally; a non-UTF-8 path prints an exact
hex byte spelling rather than a lossy substitute that could identify the wrong
file.

The modification time comes from the immutable generation directory selected
by the already-validated `CURRENT`, while the shared lock is held. Polariton
renders it as RFC 3339 UTC. If `modified()` is unsupported for an otherwise
valid generation, the row prints `unknown`; metadata absence does not corrupt
session truth.

Discovery does not decode `libaryon_documents`, `libaryon_layout`, or
`polariton_session` owner semantics and does not claim the session will restore
successfully through every head. The store has validated the manifest, base
snapshot, owner names, and owner byte lengths; a row is a committed session,
not a dry-run of engine startup. Adding head-specific semantic summaries would
turn a shared store census into a Polariton index and is outside this record.

### 5. Output answers selection before aesthetics

The human output is stable in shape:

```text
STATE      UPDATED_UTC           GEN  DOCS  SESSION
active     -                       -     ?  aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
committed  2026-08-09T22:26:59Z   4     1  24631c77d510daf33ae6331e1f4776ce
  document 2  /home/me/work/uat/test16.rn
damaged    -                       -     ?  bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  corrupt saved session: CURRENT must be 16 lowercase hexadecimal digits plus newline
```

The complete ID remains copyable into `polariton --session <id>`. No ordinal or
display prefix becomes an accepted selector.

Rows sort in this order:

1. active identities, ascending by `SessionId`;
2. committed identities, newest generation mtime first, then higher generation,
   then `SessionId`;
3. damaged and unreadable identities, then foreign-entry issues, each in stable
   byte order.

Unknown mtimes sort after known mtimes within the committed group. Sorting by
mtime is a presentation convenience only. A touched directory may move in the
list without changing identity, generation, or content.

No sessions prints exactly `No saved sessions.` and exits successfully. The
command has no JSON mode in this record; the first consumer is a human choosing
an ID, not an automation protocol.

### 6. Listing attachment is not save freshness

`UPDATED_UTC` means “filesystem time of the current committed generation.” It
does not mean the running window has no uncheckpointed work. An active row has
no committed time because the lock rule prevents observing which generation is
current; listing does not add a dirty bit, autosave, or a second status model.

Full document paths are printed only in response to the explicit terminal
command. Nothing enters GUI chrome or the status bar. The user already owns the
private state root and can inspect it; this is a discoverability surface, not a
new remote disclosure.

### 7. Names, deletion, and implicit selection remain absent

This record adds no alias registry, friendly-name owner, session description,
recent-session sidecar, last-session pointer, picker, unique-prefix selector,
delete operation, garbage collector, retention policy, or automatic restore.

Bare `polariton` remains an ephemeral scratch even when exactly one saved
session exists. `--session list` never changes, opens writable, or continues a
row. The user copies the complete ID into the already-shipped selector.

Sessions therefore continue to accumulate until a later explicit deletion
surface exists. That is a decided consequence, not accidental neglect. A
delete record must settle active-session refusal, recursive target validation,
recoverability, and whether owner payloads have external resources. Discovery
does not gain destructive authority merely because it reveals the need.

Friendly names become justified only if document paths and committed recency
prove insufficient in use. If they do, names belong in a committed owner or a
separately atomic index with rename/collision/deletion rules; directory names
remain the durable identity either way.

## Implementation sequence

1. Add libaryon's typed discovery report and enumerate the store root through
   directory-relative hardened helpers. Bank absence, canonical/foreign names,
   and zero mutation before adding Polariton output.
2. Add the four per-session classifications by composing the existing
   `open_read_only` result. Prove contention wins over a corrupt `CURRENT`, as
   PRODUCT-0059 already requires for direct opens.
3. Project committed snapshot metadata and generation mtime while the shared
   handle lives; add exact Unix path rendering.
4. Refactor Polariton argument resolution into GUI startup versus terminal
   action without changing any existing GUI target or error.
5. Render and sort the terminal report, then update README/session instructions
   with the discovery-to-open two-command flow.

## Acceptance gates

1. `polariton --session list` resolves to a terminal action and accepts no
   initial file or extra selector. A process gate proves it exits without
   constructing an iced application, engine, Toron client, or startup Rune.
2. A missing platform root and an existing empty root both print exactly
   `No saved sessions.` and create no filesystem entry. An unsafe root refuses
   rather than masquerading as empty.
3. Three committed sessions with asymmetric generations, mtimes, file-backed
   documents, and scratch documents render exact counts, full IDs, generation
   numbers, RFC 3339 UTC times, and DocumentId-ordered path lines. The ordering
   is newest-first with generation and ID tie controls.
4. A child process holds one writable session while another committed session
   remains closed. Listing completes immediately, prints the holder as
   `active` with unknown snapshot fields, and fully describes the closed row.
   The holder remains alive throughout, closing the vacuous release path.
5. While that writer holds the lock, the fixture corrupts `CURRENT`; listing
   must still report `active`, proving no pointer read outranks contention.
   After the holder exits, the same entry reports `damaged` with the exact
   pointer reason.
6. Independent malformed `CURRENT`, missing generation, malformed manifest,
   owner-length mismatch, and unreadable-entry fixtures each remain present in
   a report beside a valid control. No bad entry hides the valid one, no error is
   generalized to “not found,” and no largest-generation recovery occurs.
7. Canonical-ID directories, noncanonical UTF-8 and non-UTF-8 names, and both
   canonical-named symlink and regular-file controls exercise root enumeration.
   Foreign names are escaped and reported without traversal; canonical wrong-
   type entries report unreadable from `O_NOFOLLOW` rather than being followed.
8. A before/after tree ledger proves listing changes no byte, mtime, entry,
   lock inode, `CURRENT`, or generation. An unreferenced generation and temporary
   pointer remain in place, proving discovery performs no writable cleanup.
9. A store-valid session with intentionally invalid Polariton owner JSON still
   lists as committed, with a control proving normal Polariton open later
   refuses it. Discovery is base-store truth, not an unadvertised restore dry
   run.
10. UTF-8 and non-UTF-8 document paths remain distinguishable and exact in the
    rendered output; `<scratch>` cannot collide with an encoded path.
11. Existing `polariton`, positional file, `--session new`, and
    `--session <id>` parsing/startup gates remain byte-identical. A no-`toron`
    build explicitly refuses all three saved-session selectors, including
    `list`.
12. Architecture gates keep listing out of `gui::StartupTarget`, forbid
    writable store calls and owner-payload decoding on the discovery path, and
    find no alias, delete, last-session, prefix-selector, index, or autosave
    producer.
13. README documentation shows `polariton --session list`, copying one complete
    ID, and reopening it. It says active metadata is intentionally unknown and
    committed time is not dirty-state freshness.
14. Libaryon, Polariton, architecture, no-default-feature, formatting, and full
    workspace suites pass with no ignored discovery gate.

## Rejected

- **List directory names only:** `ls` with extra steps; it does not answer which
  session contains the work the user remembers.
- **Acquire a writable lock to inspect:** contends needlessly, may reclaim
  generations, and gives a read surface mutation authority.
- **Read `CURRENT` without a lock for active sessions:** supersedes
  PRODUCT-0059's ordering, races generation reclamation, and makes a prettier row
  outrank the store's concurrency contract.
- **Wait for an active writer:** turns discovery into an unbounded close watcher.
- **Treat contention as corruption or omit the row:** hides the session most
  likely to be relevant.
- **Decode Polariton owner data for labels:** makes a head-neutral store census
  depend on one head's schema and converts listing into restore validation.
- **Infer a friendly name from the first basename:** unstable under rename,
  ambiguous across common filenames, and a second implicit identity.
- **Accept list ordinals or short ID prefixes as selectors:** ordering changes
  and prefix collisions would make display position an authority.
- **Add delete beside list:** discovery is non-mutating; deletion has materially
  different safety and ownership questions.
- **Make the newest row the default startup:** reintroduces the implicit
  last-session authority PRODUCT-0059 and PRODUCT-0063 explicitly deferred.

## Consequences

- Saved sessions become findable from their contents without a second durable
  representation.
- A running session remains discoverable but opaque until its exclusive lease
  is released; lock continuity outranks metadata richness.
- An active row therefore shows `DOCS ?` and no paths. It is the least
  descriptive row because it is already open somewhere the user can inspect;
  making its count knowable would require the unlocked snapshot read this
  record rejects, not merely additional formatting work.
- Damaged sessions become more visible, not less. Discovery exposes the need for
  repair/deletion without pretending either exists.
- Full paths and full IDs remain CLI material. GUI chrome stays quiet under
  PRODUCT-0071.
- Session accumulation remains manual debt with a named trigger rather than
  being silently solved by retention.

## Stop conditions

- If listing requires a writable handle, cleanup, or a second index to answer
  the accepted row, halt; the base snapshot is supposed to be sufficient.
- If full active-session metadata can be obtained only by reading `CURRENT`
  before the shared lock succeeds, keep the reduced `active` row. Do not weaken
  PRODUCT-0059 inside its first discovery consumer.
- If document paths turn out not to live in the validated base snapshot for any
  restored document class, halt rather than decode a head-owned payload as a
  fallback.
- If session selection, deletion, aliases, last-session behavior, or a GUI
  picker becomes necessary to make the CLI list usable, stop and split that
  mechanism into its own record.
- If a root entry cannot be represented safely without traversing or lossy name
  conversion, report an escaped foreign issue; never silently skip it.
