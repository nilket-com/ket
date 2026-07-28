# Specification: Git-History Diffing for `:panediff`

Extends [diffsplit](diffsplit.md). The hybrid diff engine already compares two
UAST roots without caring where their bytes came from; this plan feeds it
snapshots from Git history so any file-backed buffer can be diffed against any
revision of itself.

## Objective

`:panediff` grows from "diff against a file on disk" into "diff against a file
on disk *or* any Git revision of the focused file", with no new commands to
learn. The long-term direction is that a document is just one node in a family
of sparse semantic trees (working copy, index, every historical revision), and
the diff engine navigates between them.

## 1. Command Surface (`input/command_registry.rs`)

- `:panediff` / `:paned` / `:pd` — argument becomes **optional**.
  - `:pd` — diff the focused file against `HEAD`.
  - `:pd <arg>` — if `<arg>` (after path expansion) names an existing file, the
    existing file-vs-file behavior is unchanged. Otherwise `<arg>` is treated
    as a Git revision of the focused file (`HEAD~3`, a branch, a tag, a SHA).
- The protocol stays `EditorCommand::DiffSplit(String)`; the empty string means
  `HEAD`. Disambiguation happens in the runtime, where session context lives.
- Failure of both interpretations produces one status message naming both:
  `Not a file or Git revision: <arg> (<git stderr>)`.

## 2. Revision Fetch (async job, `engine/git.rs` + `runtime.rs`)

Follows the `git status` panel pattern (`start_git_status_job`):

- The focused buffer must be file-backed; otherwise
  `Cannot diff an unnamed buffer against Git history`.
- Repository discovery reuses `session.git_branch_cache.repo_root_for_path`.
  The blob path is the buffer's canonical path relative to the repo root.
- Revisions are validated up front: no whitespace, no `:` (which would change
  the meaning of the `<REV>:<relpath>` spec), and no leading `-` (which would
  read as a Git option — `:pd --output=x` must never become an argument).
- Acquisition is **two-phase**, pinning identity before content:
  1. **Pin** (`GitResolveAction`): `git rev-parse <REV>^{commit}` returns the
     commit OID. This is the acquisition's **only symbolic resolution** — one
     spec, one atomic ref read — and the "unknown revision" failure surface.
     (No `--end-of-options` here — rev-parse echoes it to stdout as a
     pass-through plumbing command; the spec is option-safe by construction
     since it begins with the validated revision.)
  2. **Fetch** (`GitDiffAction`): `git cat-file --batch` fed
     `<commit_oid>:<relpath>` on stdin returns the blob OID and its content
     in one record, both derived from the immutable pinned commit. A missing
     path or a tree at that path fails here with a precise message.
  Both phases run with `cwd` = repo root and a 10s timeout.

  Why not one `rev-parse <REV>^{commit} <REV>:<relpath>`? Git resolves each
  spec with an **independent ref read** — under concurrent `branch -f` churn
  the pair measurably tears (27/400 iterations in a loop experiment returned
  commit A with blob B; the pin-then-batch sequence tore 0/400 under the
  same churn). The record parser is strict: one hex OID whose width must
  match the pinned commit's (one repository uses one object format), exact
  content-length accounting, and exactly one LF terminator — no trailing
  data, no cut-short stream.
- Git permits LF in tracked filenames, but `--batch` stdin is LF-framed; a
  path containing a newline is refused up front with a precise message
  rather than smuggling a second object spec into the stream. (`--batch -Z`
  would lift this, but requires Git ≥ 2.42; revisit if such paths matter.)
- One in-flight acquisition at a time (`session.snapshot_fetch_job`, spanning
  both phases); a second `:pd` while pending reports it.

## 3. Completion (`RunLoopJobDispatcher::complete`)

On non-zero exit: surface trimmed stderr as the status message (this is the
"unknown revision" path) and stop — no split is created on failure.

On success:
1. Verify the left pane still exists and still shows `left_buffer_id`;
   otherwise drop the result with a status message.
2. Split the left pane vertically with a fresh unnamed text buffer
   (`create_unnamed_text_view` + `split_pane_with_new_pane_order`), exactly as
   `split_target_pane` builds panes.
3. Fill it through the `ReplaceDocument` doctab mechanics: blob text,
   `language` = the left buffer's `LanguageId::as_str()`, `virtual_path` =
   `<repo_root>/.baryon-snapshots/<commit_oid>/<relpath>`. A virtual document
   (not an output panel — output panels are excluded from the semantic
   pipeline in `render.rs`) gets the full async semantic graft, so the diff
   upgrades from the line-based fallback to the hybrid AST model exactly as
   file diffs do.

   The virtual path shape is deliberate: **absolute**, so language-service
   path context does not depend on the process working directory, but **never
   the real file's path** — the Rust provider keys workspace documents by
   canonical path, and a shared path would map the live buffer and the
   snapshot onto one workspace `FileId`, letting the snapshot's old content
   poison analysis of the live document. It is keyed by the **commit OID**,
   not the symbolic revision, for the same reason at the other end:
   re-acquiring a moved ref must not alias the older snapshot in provider
   caches keyed by path. The synthetic location never exists on disk, so
   canonicalization fails and providers use their stable single-file
   fallback. True workspace-aware snapshot analysis needs a provider-level
   snapshot overlay (future work).
4. Verify materialization: `ReplaceDocument` installs the requested
   `virtual_path` only when document construction succeeded, so its absence is
   the failure signal. On failure the empty snapshot pane is rolled back
   (closed and released) and the failure status message is preserved — never
   a diff against a blank document.
5. Pair with `compute_pane_diff(left, right)`; focus stays on the new pane,
   matching the file flow.

## 4. Snapshot Semantics

Naming note: the general concept is a **snapshot partner** — a diff partner
materialized from somewhere other than the live filesystem. The diff engine
predates Git integration and stays source-agnostic; Git is one *provider* of
snapshots, so only the fetch plumbing carries Git-specific names.

- The partner is an in-memory virtual document: it has no file, so `:w`
  refuses; edits are possible but merely recompute the diff, matching the
  file-vs-file flow where both sides are editable.
- `session.snapshot_partners: HashMap<BufferId, SnapshotProvenance>` records
  where each snapshot came from. `SnapshotProvenance` is an enum
  (`GitRevision { repo_root, rel_path, revision, commit_oid, blob_oid }`
  today; the index, point-in-time file copies, or remote artifacts are future
  variants). The `revision` is the user-facing label (`HEAD~3`) and drives
  the `<basename>@<revision>` tab title; the OIDs are the immutable identity
  pinned at acquisition — the foundation for caching, deduplication, and
  timeline movement. Entries are removed in `release_buffer`.
- The viewport projection exposes the identity as
  `DocTabSummary::snapshot: Option<SnapshotIdentity>` (revision + OIDs), so
  frontends can render provenance and tests can observe that a moved ref
  never rewrites an open snapshot.
- Pairing is symmetric one-to-one: installing a new pair detaches both sides
  of any pair it replaces (`compute_pane_diff`), so re-diffing from a pane
  never leaves its abandoned partner holding a stale one-way pair. The
  abandoned pane stays open, just unpaired.
- Identity transitions: `:w PATH` on a snapshot converts it into an ordinary
  file document — the write-as commit removes its provenance entry, and the
  title renderer additionally prefers a real `file_path` over provenance so a
  stale entry can never mislabel a saved file. (The side-table shape is why
  this needs two touch points; moving provenance onto `TextBuffer` so
  replace/save/release update identity atomically is noted under future
  work.)
- Non-UTF-8 blobs are rendered lossily in this iteration (the `ReplaceDocument`
  path is `String`-shaped); binary-ish snapshots are a non-goal.
- **Size contract**: job stdout capture truncates at `MAX_JOB_CAPTURE_BYTES`
  (8 MiB); truncated snapshots are refused with a status message rather than
  diffed partially. This capture-then-flatten path deliberately does not scale
  toward Baryon's large-file strengths — a streamed, device-backed job output
  that feeds sparse ingestion is the eventual boundary, listed under future
  work.

## 5. Testing

Gate tests (no `git` binary dependency):

- Command-layer: `:pd` with no argument dispatches `DiffSplit("")`; argument
  passthrough unchanged (`prompt.rs`).
- `git.rs`: revision validation (including option-injection shapes like
  `--output=…`), rel-path computation, strict OID and `--batch` record
  parsing (mixed widths, hidden trailing junk, size mismatches all refused);
  `session.rs`: provenance titles.
- Runtime refusals via the engine harness: an unnamed buffer cannot be diffed
  against history; a file outside any repository reports it; the existing
  path-alias self-diff test proves an existing file wins disambiguation.

Ignored tests (require a `git` binary; run with `-- --ignored`):

- `:pd` against `HEAD` end-to-end: snapshot content, `name@HEAD` title (tab
  and status line), pair.
- `:pd HEAD~1` against an explicit revision.
- A language-less document (`.txt`) snapshots successfully — materialization
  success is independent of having a recognized language.
- `:w PATH` on a snapshot clears its `name@revision` identity.
- **Ref-move regression**: acquire a snapshot at a symbolic ref, `git branch
  -f` the ref elsewhere, and verify the open snapshot retains its original
  commit/blob OIDs and content.
- **Re-acquisition regression**: acquire the same symbolic ref on both sides
  of a ref move and verify the two open snapshots carry distinct identities,
  the worktree pane pairs with the new snapshot symmetrically, and the
  abandoned snapshot stays open but unpaired.
- **Stepping**: `:diffstep` walks older to the creation boundary and newer to
  the newest boundary (with their precise refusals), steps from either side
  of the pair, and follows renames in both directions — the snapshot title
  adopting the historical basename (a tab-containing one, exercising the
  `-z` protocol end to end) is the visible proof. An A→D→A history steps
  between content states, skipping the deletion; a dirty snapshot refuses
  the step. Gate-side: `:diffstep` argument parsing (directions, signed
  counts, rejects `0` and junk), the no-pair refusal, and `-z` parser units
  (tab paths, raw historical-LF paths, deletion and merge-listing skips).
  A deep step across a deletion-heavy history (11 incarnations, 10
  deletions, `:dstep -10`) proves the uncapped walk, and the mid-step
  worktree `:w PATH` race is driven deterministically — the ordered command
  queue lands the rename before the walk's completion delivery — and
  asserts cancellation with the snapshot untouched. A conflict-resolution
  merge whose content matches neither parent steps older to its first
  parent and newer back to the merge, proving the first-parent policy.
- Unknown revisions fail with git's message and create no pane.
- Oversized blobs (> 8 MiB) are refused, not partially diffed.

Untested by automation: mid-fetch pane teardown (stale-source path) and
materialization failure (needs induced registry exhaustion); both are
straight-line status+rollback code.

## 6. Timeline Stepping (`:diffstep`)

`:diffstep older|newer|±N` (alias `:dstep`) moves the snapshot partner of the
focused diff pair through the file's commit history without leaving the pair.
Negative steps are older, positive newer; `older`/`newer` mean ∓1. Either
side of the pair may issue the command; the snapshot side is identified by
its provenance entry, so file-vs-file pairs refuse with a precise message.

### Linear merge policy

All three walks use `--first-parent`: the timeline is the **first-parent
spine** of history. This is an explicit policy, not a default — Git's default
path simplification omits merge commits even when a conflict resolution
authored file content unlike either parent, which would make the actual
origin of `HEAD:<path>` unreachable by stepping (older would skip it, newer
could not return to it). Under first-parent, such a merge is a real timeline
position whose record diffs against its first parent; side-branch commits
are deliberately not stepping destinations.

### Walk direction and renames

`git log --follow --name-status --format=%H` produces the timeline: one
record per file-touching commit, whose last tab field is the **path at that
commit** (`R097 old new` → `new`). Rename traversal is directional:

- **Older** (one job): walk from the snapshot's pinned commit with the
  snapshot's own `rel_path` — that path is valid at that commit by
  construction. Position 0 is the first record (the commit that produced the
  current content, whether or not the pinned commit itself touched the
  file); the target is record `|N|`. Walking past the terminal `A` record
  reports `Already at the oldest revision for this file`. The walk is
  **uncapped**: deletion and merge records are skipped in unbounded numbers,
  so any fixed `-n` slack manufactures false oldest boundaries. The 8 MiB
  job capture is the real limit; since records are newest-first, truncation
  only cuts the older tail — a target already in hand stays valid, and only
  a miss distinguishes "history ends" from "stream cut" (`File history is
  too long to step`).
- **Newer** (two jobs): `--follow` only walks backward from its start, so
  the timeline is rooted at `HEAD` using the **worktree file's** current
  repo-relative path, which follows renames back through the snapshot's
  position. A normalize job (`git log -n1 --follow <pinned> -- <path>`)
  first maps the pinned commit to its file-touching position; the HEAD
  timeline is then searched for that position and the target is `index − N`.
  A position missing from the HEAD timeline reports that the snapshot is not
  in the current file's history; `index − N < 0` reports already-newest.
  Truncation here is also prefix-safe: a current position found inside a
  truncated capture has every newer target present, so only a *missing*
  position on a truncated stream reports `File history is too long to step`.

The walks use `git log -z`: the line-oriented protocol C-quotes paths
containing tabs, quotes, backslashes, or newlines even under
`core.quotepath=false`, which would hand escape sequences to the fetch phase
and let historical LF paths slip past the LF guard in escaped form. Under
`-z` the token stream is `hash NUL "\n"+status NUL path NUL [path2 NUL]` and
paths arrive raw. Bytes after the last NUL are an unterminated fragment
(a capture cut mid-path) and are discarded rather than surfaced as a partial
path. The parser additionally skips deletion records (`D` commits
have no blob — an A→D→A history steps between content states), records
without a status (path-limited merge listings), and non-UTF-8 paths, rather
than guessing. Stepped fetches reuse the existing `cat-file --batch` phase
verbatim with `<target_commit>:<path_at_commit>`; paths containing LF are
refused at that boundary like acquisition paths.

### Step integrity

Stepping is an in-place document swap, and each phase races user actions, so
`StepContext` pins the snapshot's commit OID **and the worktree document's
file path** at issue time, and every phase re-validates the full context:
both panes still show their buffers, the reciprocal pair still links them
(no re-pairing), the provenance entry still names the pinned commit (no
`:w PATH` conversion of the snapshot, no concurrent re-acquisition), the
worktree document still has the file identity captured at dispatch (a
`:w PATH` there would re-pair the snapshot against a renamed file using
stale repository paths), and the snapshot has no unsaved edits. A dirty
snapshot refuses the step up front — unsaved edits have nowhere to go.

### In-place re-materialization

A step replaces the snapshot pane's document (same buffer, same pane — no
new split): `ReplaceDocument` mechanics, provider identity from
`snapshot_virtual_path` with the new commit OID, provenance updated in
place, and the pair recomputed. A failed step leaves the previous snapshot
intact — there is no pane to roll back.

After stepping, the revision label becomes the target's **short commit OID**
(`main.rs@a1b2c3d4`): the original symbolic label no longer describes the
position. Full OIDs remain in provenance. A title reading
`old.txt@<short>` after stepping across a rename is the visible proof that
the path followed history.

## Future Work (out of scope here)

- Snapshot caching and deduplication keyed by blob OID (the identity now
  exists; the cache does not).
- Log picker: a `git log` panel for the focused file where selecting a commit
  opens the diff (the `git-status` panel interaction pattern).
- Index/worktree three-way: left = HEAD, middle = index, right = worktree.
- Streamed snapshot fetch: device-backed job output feeding sparse ingestion,
  lifting the 8 MiB capture contract for huge-file diffs.
- Non-Git `SnapshotProvenance` variants (point-in-time file copies, remote
  artifacts) once a second acquisition source exists.
- Move provenance onto `TextBuffer` so document replacement, save-as, and
  release update snapshot identity atomically instead of via session
  side-table touch points.
- Provider-level snapshot overlays: analyze a snapshot inside its repository
  workspace (a second `FileId` universe keyed by revision) instead of the
  single-file fallback.
