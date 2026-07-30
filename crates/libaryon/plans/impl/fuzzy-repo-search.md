# Modal fuzzy repo search

Scope: an engine-owned modal picker (`:fz QUERY`, alias `:rg`) that fuzzily
searches repository *content*, streams ranked results as the query is
edited, and jumps to a selected match with full navigation semantics.
Acceptance case, verbatim from the request: query `render_frame` and get
eyes on every call site across the repo. Deliverable is one `impl:` commit
reviewed against this plan.

## Architecture (headless, per house rules)

All picker state and search execution live in libaryon; Baryon renders a
snapshot. Polariton gets the same protocol payload for free later (its
surface is out of scope, per the diff-pane precedent).

- **Picker session**: `EditorSession.picker: Option<PickerState>` — query
  string (edited via the existing prompt line-editing machinery), selected
  index, generation counter, bounded ranked results, and the active search
  job handle. Modal input rides the established `InputContext` interception
  pattern (`is_picker`): printable keys edit the query, `Ctrl-n`/`Ctrl-p`
  and arrows move selection, `Enter` jumps, `Esc` closes. Everything else
  is declined while the picker is open.
- **Protocol**: `Viewport.picker: Option<PickerViewport>` (top-level, like
  `git_status_repo_root` — a modal is app-scoped, not pane-scoped): query
  echo, selection index, visible result rows
  `{path, line, col, line_text (trimmed/capped), score}`, plus honest
  totals (`shown/matched`, `searching…` flag, `>=N (truncated)`).
- **Search task**: one background task per query generation, following the
  `start_git_status_job` ownership shape without pretending the
  subprocess-only `JobRuntime` can execute an in-process scan — spawn on query
  change (debounced ~50–100 ms), stream batches through the internal event
  channel, and **cancel by supersession**: results carry their query generation;
  the engine drops batches whose generation isn't current (the house RequestId
  discipline). A superseded task also observes a cancellation flag so it stops
  walking, not just stops being heard.

## Corpus and correctness rules

- **File set**: `git ls-files -z` via the existing git shell-out precedent —
  gitignore-exact, no new walker dependency. Fallback when not in a repo:
  bounded recursive walk with the obvious skips (`.git`, `target`).
  Binary sniff (NUL in first block) and a per-file size cap
  (`MAX_SEMANTIC_BYTES` is a reasonable ceiling) exclude non-text.
- **Live-buffer preference (correctness, not nicety)**: for files open in
  the session, search the *live* bytes (`read_document_snapshot` via
  canonical-path lookup in `buffers.by_path`), not disk — jump targets must
  match what the user sees; disk offsets misjump into dirty buffers. The engine
  thread captures canonical path + bytes + `(BufferId, root, StateId)` into
  immutable task input; the worker never reaches into `EditorSession`. Closed
  files read from disk on the worker thread.
- **No index**: v1 scans per query. The repo is small; streaming +
  cancellation + caps bound the cost. A persistent index is parked until
  the dashboard-equivalent (perceived latency) says otherwise.

## Matching and ranking

Per line, case-insensitive when the query is lowercase (smartcase):

1. exact substring — top tier, earlier column breaks ties;
2. word-boundary subsequence (`render_frame` matches `render_frame(`,
   `rf` matches `render_frame` at `r`+`f` boundaries) — middle tier;
3. plain subsequence — bottom tier, gap-penalized.

Tie-breakers: shorter line, then shallower path, then path lexicographic —
deterministic, test-assertable. Caps: per-file result cap (~50) and global
cap (~1,000), both surfaced honestly in the totals line. The scorer is a
pure function with unit tests; no fuzzy-matcher dependency unless one is
already in-tree.

## Jump semantics (reuse, don't reinvent)

`Enter` on a result follows the go-to-definition flow wholesale:
already-open buffer → focus/retarget the existing view; unopened file →
`resolve_or_load_buffer` + retarget the *initiating* pane, including the
dirty-buffer refusal rule verbatim. Record the origin in the jump history
(`Ctrl-o` returns from a picker jump exactly like a `gd` jump — same
stamped anchors). For open buffers, the jump validates against the state
the result was computed from (stamp: root + state id at search time); stale →
re-run the query against the current target line. If it no longer matches,
reject the stale selection and refresh rather than clamping onto unrelated
text. Picker rows are observations, not durable identity, but the navigation
arc's never-clamp rule still applies. Disk-file jumps likewise load current
bytes and re-resolve the query on the recorded line before moving.

## Acceptance: the `render_frame` case

A fixture test (and an ignored end-to-end variant on the real repo) builds
a self-contained oracle — exhaustive scan of the corpus for the literal
query — and asserts every oracle row is present before caps (with caps, every
omission is reported honestly). Equality would be a false contract: a fuzzy
search deliberately admits valid subsequence rows beyond a literal oracle. On
this repo, `render_frame` must surface every caller and reference across all
three crates plus the plans corpus, i.e. literal-coverage parity without
depending on rg in tests. Ranking sanity is syntax-blind and honest: exact
occurrences outrank subsequence-only noise. Distinguishing code calls from
prose containing the same exact token requires a later language-aware scorer.

## Tests

1. Scorer unit tests: tier ordering, smartcase, tie-breaker determinism.
2. Oracle parity (the acceptance test above), plus truncation honesty when
   caps engage.
3. Supersession: rapid re-queries — only the final generation's results
   land; a cancelled worker stops early (observable via a walk counter).
4. Live-buffer preference: dirty open buffer — results reflect unsaved
   content and jumps land correctly; disk copy's offsets are not used.
5. Jump integration: picker jump → `Ctrl-o` returns to origin; dirty-
   buffer cross-file refusal matches `gd` behavior.
6. Modal input: keys route to the picker while open (query edit, motion,
   Esc restores the prior input context); catalog/help regenerated for
   `:fz`/`:rg`.
7. Binary/size exclusions; not-a-repo fallback walk.

## Non-goals (parked, promotion paths named)

- File-name picker mode and other sources (commands, buffers): the
  `PickerState` shape should not preclude them, but v1 ships content only.
- Regex mode (`:rg` alias notwithstanding — it is fuzzy/literal v1; regex
  is a flag away if wanted later).
- Preview pane, persistent index, rg-as-accelerator, Polariton surface.
- Multi-select / quickfix-list export (natural later pairing with an
  output panel).

## Review checklist

- [ ] Engine-owned state; Baryon renders `Viewport.picker` only — no
      frontend search logic.
- [ ] Query generations: no stale batch can land; cancelled workers stop.
- [ ] Live-open-buffer bytes searched; jump offsets provably match.
- [ ] Jump reuses gd machinery incl. dirty refusal + jump history.
- [ ] Oracle parity on `render_frame` across the whole repo corpus.
- [ ] Caps + truncation honest in the viewport payload.
- [ ] Modal input contained; Esc restores prior context cleanly.
