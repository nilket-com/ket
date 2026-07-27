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
- Unknown revisions fail with git's message and create no pane.
- Oversized blobs (> 8 MiB) are refused, not partially diffed.

Untested by automation: mid-fetch pane teardown (stale-source path) and
materialization failure (needs induced registry exhaustion); both are
straight-line status+rollback code.

## Future Work (out of scope here)

- Snapshot caching and deduplication keyed by blob OID (the identity now
  exists; the cache does not).
- Timeline stepping: move the diff partner older/newer through
  `git log --follow` without leaving the pair.
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
