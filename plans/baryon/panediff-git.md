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
- Spawn `git --no-optional-locks cat-file blob --end-of-options
  <REV>:<relpath>` with `cwd` = repo root and a 10s timeout. `cat-file blob`
  prints the object verbatim (no `git show` pretty-printing surprises), and
  `--end-of-options` fences the spec as a second layer against option
  injection. The `GitDiffAction` completion lease carries
  `{ left_pane_id, left_buffer_id, repo_root, revision, rel_path }`.
- One in-flight snapshot fetch at a time (`session.snapshot_fetch_job`); a
  second `:pd` while pending reports it.

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
   `<repo_root>/.baryon-snapshots/<revision>/<relpath>`. A virtual document
   (not an output panel — output panels are excluded from the semantic
   pipeline in `render.rs`) gets the full async semantic graft, so the diff
   upgrades from the line-based fallback to the hybrid AST model exactly as
   file diffs do.

   The virtual path shape is deliberate: **absolute**, so language-service
   path context does not depend on the process working directory, but **never
   the real file's path** — the Rust provider keys workspace documents by
   canonical path, and a shared path would map the live buffer and the
   snapshot onto one workspace `FileId`, letting the snapshot's old content
   poison analysis of the live document. The synthetic location never exists
   on disk, so canonicalization fails and providers use their stable
   single-file fallback. True workspace-aware snapshot analysis needs a
   provider-level snapshot overlay (future work).
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
  (`GitRevision { repo_root, rel_path, revision }` today; the index,
  point-in-time file copies, or remote artifacts are future variants). It
  drives the `<basename>@<revision>` tab title and is the seed for history
  navigation. Entries are removed in `release_buffer`.
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
  `--output=…`), rel-path computation; `session.rs`: provenance titles.
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
- Unknown revisions fail with git's message and create no pane.
- Oversized blobs (> 8 MiB) are refused, not partially diffed.

Untested by automation: mid-fetch pane teardown (stale-source path) and
materialization failure (needs induced registry exhaustion); both are
straight-line status+rollback code.

## Future Work (out of scope here)

- Resolve and store commit + blob OIDs in `SnapshotProvenance` (immutable
  identities rather than labels) before building timeline stepping; enables
  caching and dedup of identical snapshots.
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
