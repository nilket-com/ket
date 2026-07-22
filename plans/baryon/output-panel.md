# Specification: output panels and `:git` status

Two layered deliverables: a generic **output panel** primitive (a buffer-backed,
read-only, atomically-replaceable pane with producer-supplied styling), and the first
consumer, **`:git`** — an integrated, periodically refreshed git status panel replacing
the external `watch --color --differences --interval 7 git status` workflow. Panels are
core engine infrastructure; nothing here touches the Rune surface.

## 1. Panel = ordinary buffer (settled abstraction)

A panel is a normal buffer plus four properties, inheriting wrap, scrolling, selection,
rendering, and minimap behavior from the existing viewport pipeline — no parallel
`Panel → lines → spans` rendering path:

1. **Read-only and undirtyable**: a dedicated `BufferRole::OutputPanel` on the buffer
   record, enforced at the central gates — edit dispatch, save dispatch, and buffer
   navigation/cycling — with a bounded status ("Panel buffers are read-only"). One
   marker checked at three chokepoints, never per-editing-command checks. Dirty
   tracking and reload-stamp machinery skip panel buffers entirely.
2. **Named identity and reuse**: panels are keyed by a name (`"git-status"`); the panel
   registry maps name → BufferId. Lifecycle is one rule, not two: closing the pane drops
   only the view; the buffer and its registry entry persist, and the next open re-splits
   and re-reveals the **same** buffer (content stale until its refresh lands). Panel
   buffers live for the session; the name set is small and bounded by construction.
3. **Atomic content replacement**: preserves the buffer's identity (`BufferId` stable
   across refreshes — views, registry, and any queued job completions keep pointing at
   it) while replacing content, resetting render caches, and dropping all stale
   highlight spans in one engine-side operation. Scroll position is preserved when the
   new content still contains the anchor line, else clamped. No incremental editing
   surface.
4. **Producer-supplied styling**: the panel's highlight spans come from its producer,
   not the SVP syntax pipeline.

**Investigation item 1 (first task):** determine where producer styling plugs in.
`HighlightProjector` (svp/projector.rs) consumes the viewport's `highlights` +
`theme_colors`; if a panel buffer can publish synthetic highlight spans through the same
channel the SVP resolver uses (bypassing only the resolver, not the projector), the
primitive is small. If the highlight channel is structurally SVP-shaped, the adapter that
generalizes it is the core design work and gets settled here before anything else builds.
Two candidate shapes to evaluate, in preference order: (a) panel-specific token
categories alongside the existing `TokenCategory` set, so panel spans flow through the
theme exactly like syntax tokens; (b) direct theme-color spans resolved at produce time
— simpler but bypasses theme switching, requiring a re-produce on theme change. The
choice is part of this investigation's output, not pre-decided.

## 2. Placement and lifecycle

- Panels open as a horizontal split at the bottom of the current tab's pane column —
  under `:tree` when the tree is focused, under the active editor otherwise. Placement is
  policy inside the panel-open path, not part of any command's identity.
- Opening never steals focus: the requesting pane stays focused; the panel renders.
- Closing is ordinary pane close (`:q`, pane commands) — panels have no special close.
- Panel buffers are excluded from buffer cycling and the file browser, and are never
  written to disk (`:w` rejects as read-only).

## 3. `:git` command surface (settled)

One registry entry, subcommand-completed:

| Invocation | Action |
|---|---|
| `:git` | open-or-refresh the status panel (refresh, **not** toggle) |
| `:git status` | same |
| `:git diff` / `:git log` / `:git blame` | reserved; error "not yet implemented" in this cut |

## 4. The status producer — an evolution of the existing git subsystem

The engine already has a git feature: `engine/git.rs`'s `GitBranchCache` — repository
discovery from the focused path (including worktree `.git` pointer files), HEAD parsing
for the statusline branch label, and negative-result caching, with the HEAD read
currently happening on the render path. This plan **extends that subsystem into the
shared git provider** rather than adding a second git implementation:

1. Repository discovery generalizes from `GitBranchCache` into a shared git context
   (path → repo root/`.git` dir, same worktree handling, same caching).
2. The shared context has **two refresh tiers** with different costs and drivers:
   - **Cheap tier — branch probe**: the existing HEAD file read, refreshed off the
     render path on the `CheckFileReload`-style UI tick and on focus-path changes,
     independent of panel visibility. This keeps the statusline label current with
     external checkouts exactly as today's per-render read does (same data source,
     same freshness, no longer per-frame).
   - **Expensive tier — porcelain snapshot**: the authoritative structured state
     (branch, ahead/behind, file entries) from the status job, refreshed only while
     the panel is open. Zero cost when closed.
   The statusline label consumes the cheap tier, upgraded by the porcelain snapshot's
   branch/ahead-behind when one is fresher; the panel consumes the expensive tier only.
3. The panel renders the same snapshot as styled buffer content.

Producer mechanics:

- Runs `git status --porcelain=v2 --branch` via the existing `JobRuntime` under an
  engine-reserved owner (the job substrate is already bounded, cancellable, and
  drain-ordered; core consumers should use it rather than a parallel subprocess path).
  Working directory: the repo root from the shared git context.
- Parses porcelain state codes and styles natively through the theme system — staged
  entries green, unstaged red, untracked and renames per theme — with the specific
  colors themeable via the existing `ui_color` namespace (new keys under
  `colors.git.*`). **No ANSI passthrough.**
- Renders a branch/ahead-behind header line, then one styled line per entry; empty
  status renders "clean" with the branch header. Not a git repo → panel shows the error
  once, and auto-refresh stops until the next explicit `:git`.
- Output is bounded (cap lines and bytes with a truncation marker) so a pathological
  repo cannot balloon the panel buffer.

## 5. Refresh policy

- The engine loop is timerless (`rx_request.recv()` blocks indefinitely) and stays that
  way. The refresh interval is driven UI-side, following the exact `CheckFileReload`
  precedent (crates/baryon/src/ui/core.rs): the UI tick sends a `GitRefresh`-style
  command when the panel is open and the interval (default 7s, settable via
  `baryon::set(editor, "gitrefresh", seconds)` / `:set gitrefresh 7`; `0` disables) has
  elapsed. Panel closed → no commands sent, zero idle cost.
- A refresh that finds the previous status job still running is dropped, not queued
  (the job substrate's admission handles the race; one in-flight status job max).
- Phase 2, not this cut: `watch --differences`-style highlighting of changed rows. The
  producer keeps its previous parsed snapshot; rows added/changed since the last refresh
  get a transient emphasis style. Recorded so the snapshot retention lands in the
  producer's shape now (cheap) even though the highlight lands later.

## 6. Acceptance and regression tests

1. Panel primitive: open creates the buffer + split without stealing focus; reopen
   reuses it; edit/save/`:w` are rejected read-only; close via `:q` and reopen works;
   panel excluded from buffer cycling.
2. Atomic replace preserves scroll when content permits and never marks dirty.
3. Synthetic styling reaches the rendered viewport (styled spans visible in the
   viewport snapshot, not just stored).
4. `:git` end-to-end in a fixture repo: staged/unstaged/untracked entries appear with
   the mapped theme colors; clean repo shows the clean line; non-repo shows the error
   and stops auto-refresh.
5. Refresh: UI tick with panel open triggers exactly one in-flight job; interval 0
   disables; closing the panel stops refresh commands.
6. Quit latency with a hung `git` process stays within the existing bound (inherited
   from the job substrate's cancellation — pin it anyway).
7. Statusline branch label renders from the shared git context with no HEAD read on
   the render path (characterize the current label behavior first, then pin the
   equivalence after the rework — the label's user-visible output must not change),
   and an external `git checkout` in a fixture repo is reflected in the label within
   one tick **with the panel closed** — pinning the cheap-tier refresh independence.
8. Full lib suite serial in both SVP modes, clippy `--all-targets`, formatting.

## 7. Non-goals

- `git diff`/`log`/`blame` content (reserved subcommands only), staging/committing from
  the panel, hunk interaction, and FS-watching (`.git/index` inotify) — the interval
  model ships first; a watcher can replace the tick later without changing the panel.
- Rune exposure of panels. Deliberately deferred until the Rune direction is decided;
  the primitive is engine-internal in this cut.
- Multiple simultaneous panels per name, panel history, or persistence across restarts.
