# Notebook with panes — Polariton as a Baryon-centered workspace

Status: **Shipped to `main` 2026-07-20.** The host-pane seam and its first
vertical slice — run SQL into a Baryon-owned result pane — are the baseline, so
remaining work lands as independent product iterations rather than on a
long-lived branch. Sequencing steps 1–3 are complete; the notebook document
model (step 5) and Polariton's Rune runtime (step 2 of the revised order) are
the open ones. Captured from a design discussion between Joel, Codex,
and Claude. Where reviewers converged it is noted as such; ⚑ marks calls still
owned by Joel (none open).

Related: `docs/architecture.md`, `docs/iced-libaryon-migration.md`,
`baryon/plans/headless-baryon.md`, `baryon/plans/splits.md`,
`baryon/plans/markdown.md`, `baryon/plans/input-contract.md`.

## The idea

Polariton was built on the DataGrip model: a code editor is one child pane
beside a dashboard of plots and a data table (see
`polariton/doc/img/screenshot_1.png`). After the libaryon migration, a different
shape is available — Polariton as a single Baryon workspace, using Baryon's own
splits, tabs, and focus to host the data table and plots, with a Markdown/SQL
notebook as the narrative document.

Working slogan (Codex): **"a notebook document orchestrates work; Baryon panes
provide output real estate"** — explicitly *not* "Markdown with embedded plots."

## Decision log

Running record so the reasoning survives. Nothing here is committed code; ⚑
marks calls still owned by Joel.

| # | Decision | Status | Rationale |
|---|----------|--------|-----------|
| 1 | Note lives at repo root `plans/wip/`, not `baryon/plans/` | Agreed (Codex, Claude) | Intentionally cross-project; filing under Baryon would imply ownership Baryon doesn't have |
| 2 | Engine owns topology, host owns pixels | Agreed (all) | `MarkdownPreviewSnapshot` precedent; keeps polars/iced out of libaryon |
| 3 | Outputs beside, not inline | Agreed (all) | Heavyweight interactive plots want real estate; inline needs row-quantized view zones later |
| 4 | Input: engine-declines + direct pointer | Agreed (Codex, Claude) | libaryon dispatches keys by content-type context and cannot interpret host panes; drags must not round-trip |
| 5 | `Viewport` is state, never event transport | Agreed (Codex, Claude) | Host coalesces projections; events on a state channel get dropped (evidence below) |
| 6 | Lifecycle: descriptors durable, host data ephemeral | Agreed (Codex, Claude) | No DataFrame/texture/connection in libaryon or layout persistence |
| 7 | Two Rune runtimes, shared language, no shared VM | Agreed (Codex, Claude) | DataFrame / canvas / editor session have incompatible thread and lifetime rules |
| 8 | Dashboard nested as one HostPanel initially | **Ratified by Joel 2026-07-20** | Preserves dashboard-as-object (coordinated plots, export); flattening trades real semantics for uniformity |
| 9 | Notebook: explicit run + visible staleness, not reactive | Agreed (all) | 31s queries over 265M rows argue for deliberate control |
| 10 | Divider geometry is the engine's, not inferred by the host | Agreed (Codex, Claude) | Stage A copies baryon's 4-col/1-row gap math; Stage B must project divider metadata or own the hit test so engine layout changes cannot silently drift host behavior |
| 13 | Host panels must not capture keyboard input (engine-first) | Agreed (Codex, Claude) | Iced dispatches to widgets *before* subscriptions, so a capturing panel would swallow `:` and pane navigation before Baryon saw them. A panel that genuinely needs capture requires a root-level arbiter, not a local change |
| 12 | Stage B lands on the `host-pane-seam` integration branch | Agreed (Codex, Claude) | Cross-cutting, compiler-led refactor; merge when the engine cut is coherent, not when the first seam exists |
| 11 | Divider geometry ships as **projected metadata**, not a hit-test API | Agreed (Codex, Claude) | A hit-test call would put mouse-down and hover behind an async round trip — the exact interaction that must feel immediate. Metadata is state, so normal viewport coalescing is correct (consistent with #5) |

## What all three of us converged on

- **One owner per concern.** Baryon owns workspace topology: tabs, splits,
  focus, geometry, commands, persistence. Polariton owns rendering and
  interaction for specialized surfaces: DataFrame table, plot canvases, DB
  connections, menus, modals, window chrome.
- **libaryon never learns about polars, iced, or plots.** The precedent already
  exists: `MarkdownPreviewSnapshot` projects `source_text` + scroll state and
  lets the frontend render it. The engine has never rasterized anything.
- **Outputs beside, not inline.** Polariton's differentiator is heavyweight
  interactive plots (the screenshot's 265M-point adaptive raster with live
  crosshair). Those want real estate and interactivity, not notebook
  thumbnails squeezed between paragraphs.
- **Notebooks are a later, explicit document model** — stable cell ids, query
  revisions, staleness, output lifecycle — not an extension of Markdown
  preview.
- **Inline outputs last, if ever.** Row-quantized "view zones" (VS Code view
  zones / Zed block decorations, quantized to cell rows rather than pixels) are
  plausible but touch libaryon's projection and scroll invariants. Earn them
  with usage.

## The seam: host panes

Add a generic engine-owned pane whose pixels the host draws — the honest
generalization of what `MarkdownPreview` already does, reusable by any
embedder (image viewer, terminal, web view), with no data-viz concepts in
libaryon.

Baryon persists only a descriptor:

```
HostPanelRef { panel_id, kind, title }
```

Carried **engine → host**: pane rect (in cells), focus state, tab/split
membership, open/close/rebind events, unhandled key events.
Carried **host → engine**: focus requests, panel open/close requests, title
changes. The seam is bidirectional; that is worth stating up front.

### Principle: `Viewport` is state, never event transport

(Codex's framing, adopted.) The projection is a **reconciliation snapshot** —
current panel identity, geometry, focus, tab membership. Anything that must be
observed *exactly once and in order* — host input, panel close/rebind,
restoration — needs a separate ordered, non-coalescing host-event channel.

This is not theoretical; the repo already contains the evidence, twice:

- `Viewport::document_text` is a one-shot reply riding the state channel. The
  bridge coalesces projections and keeps only the latest, so the test helper
  must scan *every* viewport to catch the reply before it is overwritten.
- `Viewport::status_message` is a one-shot drained per projection, so the
  bridge has to latch it into `CodeEditor.status_message` on arrival or it
  vanishes on the next frame.

Both work, both needed special handling, and neither would survive a burst of
rapid events (typing into a focused host pane) where every keystroke matters.
If a host-event channel is built, those two fields are candidates to migrate
onto it.

## Three design decisions

### 1. Input: engine-declines, with a pointer exception (proposed, agreed)

**Correction (2026-07-20).** An earlier claim that Polariton's key ordering was
"structural — a focused widget can never swallow `:`" was **wrong**. Iced
dispatches events to the widget tree first and only then broadcasts them to
subscriptions tagged with whether a widget consumed them; Polariton forwards
only `Status::Ignored` keys, precisely so typing in app chrome does not also
edit the SQL document. A host panel that captured keyboard input would
therefore hide workspace keys from Baryon entirely.

The result table is safe because it handles only pointer input, and its
keyboard scrolling arrives the correct way — as `UnhandledInput` the engine
declined. But this is a *constraint being honoured*, not an invariant being
enforced. Decision #13 records the rule; `editor_input_for` carries it in
prose and `a_capturing_widget_would_hide_workspace_keys_from_baryon` pins it,
so the trade-off cannot be changed silently. Before a Rune console or native
text input lands, either keep panels capture-free or build a root-level focus
arbiter that forwards host-panel keys to Baryon ahead of child widgets while
excluding app-chrome forms.

Grounded in a real constraint: libaryon's keymap dispatches by *content-type
context* (`NormalContext::FileBrowser`), so the engine interprets keys
according to what kind of pane is focused. It cannot do that for a host pane —
it does not know `j` scrolls a table.

- Baryon stays authoritative for focused pane, tabs, splits, and its reserved
  global bindings (`:`, pane/tab navigation, close, `Ctrl-W`).
- Every other key, while a host pane is focused, is emitted to the host with
  its `pane_id`; Polariton dispatches to the matching widget.
- **Pointer gestures go direct to the native widget**, and the click
  *additionally* requests Baryon focus for that pane. A plot drag must never
  wait for an engine round trip.

**Name the two focus levels explicitly** (Codex) — this dissolves the nested
focus problem rather than leaving it as a hazard:

- **Workspace focus** — Baryon's focused pane. Governs splits, tabs, commands,
  global bindings. Single source of truth for "where am I."
- **Local focus** — Polariton's focused chart or table *inside* a host panel.
  Governs native interaction only, never workspace navigation.

With that vocabulary, a nested dashboard is unambiguous: Baryon holds workspace
focus on the dashboard panel; Polariton independently tracks which plot within
it has local focus. Neither system arbitrates the other's level.

### 2. Dashboard: nest initially (ratified 2026-07-20)

There are **three** layout systems today, not two: Polariton's outer pane grid,
Baryon's, and a *nested* grid inside the dashboard pane (`dashboard.panes`,
hit-tested separately for whole-dashboard export).

**Ratified:** dashboards remain first-class, nested Polariton objects for the
initial architecture. Baryon owns the dashboard's workspace pane; Polariton owns
its internal plot/table grid, local focus, composition, and export. "Detach
plot" is a future escape hatch, explicitly *not* a constraint on the probe.

This also protects the strict "nearly invisible dashboard dividers" requirement.
Baryon's workspace dividers are *structurally* cell-sized (4 columns / 1 row) so
they stay discoverable and draggable — appropriate between SQL, dashboard,
result tabs, and help, but far too chunky between dense linked plots. Nesting
keeps the two divider vocabularies separate: chunky and engine-owned at the
workspace level, pixel-thin with generous invisible hit-zones inside the
dashboard. Forcing individual plots through Baryon's cell layout would lose
that.

- **Nest** (chosen): the dashboard remains one
  `HostPanel::Dashboard` owning its internal layout. Preserves the dashboard as
  a real Polariton object — coordinated plots, shared data, whole-dashboard
  export. Cost: some panes split with Baryon, some don't.
- **Flatten**: every plot becomes its own Baryon pane. Maximum uniformity
  (`:vsplit` a chart, `gt` between them, one keymap) but discards dashboard-as-
  a-unit semantics for visual uniformity alone.

Later escape hatch if nesting chafes: a "detach plot" command promoting one
plot to a standalone host panel.

**This is a product question, not a technical one: is a dashboard a first-class
object in Polariton, or just an arrangement of panes?**

### 3. Lifecycle: descriptors durable, host data ephemeral (proposed, agreed)

No DataFrame, plot texture, or DB connection ever enters libaryon or Baryon's
layout persistence. Polariton owns backing objects:

- open / rebind a panel to a query revision;
- mark output stale when its source changes;
- dispose large DataFrames when no Baryon view references the panel;
- on session restore, render an unresolved placeholder ("Result unavailable;
  rerun query").

Needed from day one, not deferred to the notebook phase: the moment Baryon owns
topology it can persist it, and restore will hand back panel ids whose data is
gone. Cheap to design now, painful to retrofit.

## Rune scripting in Polariton

Joel: "I want to have the ability to script data viz plots in Rune where the
Rune engine is embedded in Polariton… I'm feeling like I want Rune injected into
everything I make." This changes the host-panel seam from *embed existing
widgets* into *provide programmable workspace capabilities*, so it belongs here
rather than later.

Codex's framing, agreed: **one language everywhere does not mean one Rune VM
everywhere.** Two runtimes with separate authority, sharing syntax and maybe a
small pure library, never live Rust objects or a VM:

- **libaryon Rune** — editor configuration, mappings, autocmds, editor
  commands. Engine side.
- **Polariton Rune** — database, query, DataFrame, plot, workspace scripting.
  Host side.

A DataFrame, an iced canvas, a GPU texture, and a mutable editor session have
incompatible thread and lifetime rules; crossing that boundary as Rune values
would rot fast.

Capability-oriented shape:

```rune
let frame = db.query("select …")?;
let plot = viz.scatter(frame, "trip_time", "trip_miles")?
    .color_by("dispatching_base_num");
workspace.open(plot, title: "Trip distribution")?;
```

Constraints (Codex, agreed):

- `libaryon` never depends on Polariton or its Rune API.
- Polariton's Rune runtime never receives `&mut EditorSession`.
- Cross-boundary interaction is typed host actions/events — `OpenHostPanel`,
  `RefreshPanel`, `ClosePanel`.
- Plot scripts produce declarative specs plus data handles; Polariton owns
  canvases and interaction.
- Database capability is scoped to the active workspace/connection.
- Long-running script work goes through jobs/results, never a blocking
  UI-thread Rune call.
- A shared `ket_rune_api` crate is plausible *eventually*; do not start there.

### Three things the existing code already decides for us

**1. The declarative plot spec already exists.**
`polariton/src/plot/composition.rs` defines
`PlotComposition { layers: Vec<PlotLayer> }`, `Clone + Debug + PartialEq + Eq`,
already decoupled from rendering (`create_plot(&composition, &df, w, h)`
produces the kernel). So the Rune viz API is a **builder over an existing
type**, not a new parallel model — much smaller than it sounds. The derived
`PartialEq` is also free machinery for staleness: comparing specs across a
re-run says whether the output actually changed.

**2. Polariton's Rune will hit the same `&mut` wall libaryon just did.**
Rune host objects cannot retain a borrow across the VM boundary, which is why
`editor.config()` became a *scoped deferred-command handle* (validate eagerly
at the call site, queue operations, drain and apply after the VM call returns,
expire escaped handles by epoch). `workspace.open(...)` and `db.query(...)`
face exactly that constraint against `&mut AppState`. Reuse the pattern rather
than rediscovering it — including its lesson that validation must stay eager so
a script's own `?` can observe errors. See `baryon/crates/libaryon/src/engine/
rune_host/config_api.rs` and the `baryon-rune-config` notes.

**3. Threading has an obvious home.** Polariton already runs queries off the UI
thread via tokio `Task::perform`. The Rune VM must not run on the iced UI
thread; a script that queries 265M rows would freeze rendering. libaryon's
`job_api.rs` / `DeferredRuneJob` / `RuneJobResult` is the precedent shape for
async work initiated from Rune — mirror it rather than inventing one.

### The confusion problem is real and needs a deliberate answer

Joel named it: "It gets confusing because libaryon already has Rune in it."
Two runtimes sharing one language is livable only with explicit ergonomics:

- Distinct config paths and names (`~/.config/baryon/init.rn` for the editor
  vs a Polariton-side equivalent), documented as different APIs.
- Error messages that **name the runtime that rejected the call**, so
  `editor.config()` inside a Polariton script fails with "this is the workspace
  runtime; editor configuration lives in baryon's init.rn" rather than
  "unknown function."
- The boundary is intentional, not an oversight: a notebook cell should *not*
  reach the editor VM. The documented escape hatch is sending a typed
  `EditorCommand`, not sharing a VM.
- Decide early whether Polariton's Rune gets its own `:`-style command surface
  or is notebook/config only.

## Open questions

- **Terminal Baryon's view of a host pane.** If a host panel ever appears in a
  session Baryon renders, it needs a text placeholder. Keeps the abstraction
  honest even if in practice only Polariton opens them.
- **Sub-cell remainder.** Pane rects are in cells; a plot granted 40.5 cells of
  width must decide whether to letterbox or overdraw.
- **Retirement path for Polariton's outer pane grid** (`gui/pane_grid/`) — a
  second sizable deletion after the editor, once Baryon owns topology.
- **Does Polariton's Rune get a command surface** (`:`-style) of its own, or is
  it notebook-and-config only?
- **Two `init.rn`-shaped files** — naming, discovery, and whether Polariton's
  workspace config reuses the syntax-aware `:initw` writer machinery.
- **Notebook execution semantics (phase 5).** Explicit run with *visible
  staleness*, not reactive dataflow re-runs: a 31-second query over 265M rows
  is an argument for deliberate control. This choice also determines whether
  cell outputs persist with the document or always recompute — which loops back
  to panel lifecycle.

## Risks

- **Resize feel is the sleeper risk.** Baryon's splits are cell-quantized and
  mediated by an async engine round trip. Dragging a plot-pane boundary would
  move in ~10px steps with a frame of latency versus today's pixel-smooth iced
  grid — felt immediately on exactly the panes people resize most, and not
  fixable later without un-deciding the architecture.
- **Coupling.** Polariton's layout would evolve at libaryon's pace; layout bugs
  become engine work.
- **Not a platform risk.** Correction (2026-07-20): earlier notes in
  `docs/iced-libaryon-migration.md` claimed libaryon was Linux-only. It is not.
  `io-uring` is declared under
  `[target.'cfg(target_os = "linux")'.dependencies]` and the resolver selects
  `BlockingBackend` under `cfg(not(target_os = "linux"))`. Linux gets io_uring;
  other platforms fall back to blocking I/O.

## Probe before committing

Deliberately ahead of the first useful feature, because it tests the risk most
likely to kill the approach:

1. Add a generic engine-owned `HostPanel { id, title }` view.
2. Project it with Baryon's tabs/splits/focus and pixel bounds.
3. Have Polariton render one *minimally interactive* surface in it — reacts to
   click and focus, not just a solid color.
4. Exercise `:vsplit`, tab navigation, close, resize, and mouse handoff.

**Acceptance criteria** — the probe must demonstrate all four:

1. **Focus handoff.** Clicking the host surface takes workspace focus in
   Baryon while the native widget keeps handling its own pointer input; keys
   route per the engine-declines rule with no second focus authority.
2. **No dropped host events under burst.** Hold a key or fire rapid clicks at a
   focused host pane and confirm every event arrives, in order — the direct
   test of decision #5.
3. **Close / reconcile lifecycle.** `:q` on the panel notifies the host, which
   drops its backing object; a stale or unknown panel id reconciles to a
   placeholder rather than panicking or leaking.
4. **Resize feel.** Drag a split boundary and judge whether cell-quantized,
   engine-mediated, asynchronous resizing is acceptable on a 43-inch workspace.

The first three are correctness and cheap to assert; the fourth is a judgment
call and the actual reason to do this before relocating the real dashboard.

### Sizing (measured 2026-07-20)

"An afternoon" was optimistic for the engine side. `MarkdownPreview` — the
closest analogue and the model to copy — appears **61 times** across libaryon,
and the content enums it belongs to have **43 `BufferContent::` sites** (29 of
them in `session.rs` alone) and **40 `ViewContent::` sites** spread over
`session.rs`, `render.rs`, `runtime.rs`, `document/access.rs`, and the Rune
host. A new content variant is a real engine change, not a small one.

### Therefore: run the probe in two stages

**Stage A — feel probe, no engine change (hours).** Reuse an *existing* engine
pane type as a stand-in: open a markdown-preview or output-panel pane and have
Polariton draw a native interactive widget into its projected rect instead of
text. This exercises engine-mediated splits, tabs, close, cell-quantized
geometry, and **resize feel** — acceptance criterion 4, the judgment call that
decides go/no-go — for near-zero cost and zero libaryon churn. It does *not*
test panel identity or the input-decline path.

Stage A is **built** (2026-07-20, uncommitted): `mod probe` in
`polariton/src/editor.rs`, gated by `POLARITON_HOST_PANE_PROBE=1`, plus four
marked call sites. It opens a pinned markdown preview via
`ShowMarkdownDocument`, then renders *every* projected pane at its
engine-assigned rect — probe panes get a native interactive surface, others get
a deliberately crude stub. Clicking the surface both increments a local counter
and sends `FocusPane`, exercising focus handoff.

Divider dragging is wired too, which is what makes criterion 4 a real test
rather than a window-resize proxy: a port of baryon's
`divider_under_mouse_in_rects` finds the boundary under the cursor, a grab
offset keeps the divider under the pointer, and every pointer sample while held
emits `ResizePaneBoundary` — so the async round trip is felt continuously.
Verified headlessly (the geometry, not the feel): the emitted command names the
correct before/after panes, a 10-cell drag moves the target by exactly 10, and a
press outside any gap starts no drag.

Two asymmetries to notice while judging: the drag is continuous rather than
preview-then-commit, so latency reads as the divider lagging the cursor rather
than jumping; and horizontal dividers are 1 cell (22px) tall versus 4 cells
(38px) wide for vertical ones, so they are a smaller grab target. Both are
Baryon's, not the probe's — the second argues for invisible hit-zone expansion
in Stage B.

Caveat for the feel test: the Baryon grid is sized from Polariton's *code pane*
(`sync_code_editor_viewport`), so the workspace occupies only that pane. Drag
Polariton's own splitters to maximize it before judging resize feel at scale.

### Verdict: go (2026-07-20)

Joel, after dragging real nested splits in the GUI: *"I love this. Vim splits in
a gui is god tier engineering."*

All four acceptance criteria are met. Criterion 4 — the judgment call that
could not be answered on paper — came back clearly positive: cell-quantized,
engine-mediated, asynchronous layout feels good under real nested splits and
continuous divider dragging. The ~60-site engine change is therefore earned,
and Stage B proceeds with a concrete target rather than a hypothesis.

Stage A stays disposable and should be deleted when Stage B lands: the whole
probe is `mod probe` in `polariton/src/editor.rs` behind
`POLARITON_HOST_PANE_PROBE=1`, plus call sites marked `DISPOSABLE`. Two pieces
of it are worth *keeping* by promoting into production rather than deleting:
the `prompt_line` extraction (already production, already tested), and the
divider hit-test logic — but per decision #10 that must move into the engine
rather than being copied forward.

### Stage B design: divider metadata (settled 2026-07-20)

Each viewport carries engine-authored divider records:

```
PaneDividerViewport {
    before: PaneId,
    after: PaneId,
    axis: SplitAxis,
    visual_rect: CellRect,
    hit_rect: CellRect,
}
```

libaryon computes both rectangles from its layout tree. Polariton only maps
cells to pixels, picks cursor/hover styling, and sends `ResizePaneBoundary`
during a drag. No duplicated gap constants, no adjacency inference, no round
trip before a drag can start.

**Separate `visual_rect` and `hit_rect` also fixes the asymmetry Stage A
surfaced**: horizontal dividers are 1 cell (22px) tall versus 4 cells (38px)
wide for vertical ones, so they are a much smaller grab target. With two
rectangles the engine can keep a divider looking 1 cell thin while granting it a
3-cell grab zone — the same trick the dashboard needs internally for pixel-thin
dividers, now available at the workspace level and owned by the engine, so both
frontends get it for free.

**Stage B — the real seam (a solid session).** Only if Stage A feels good: add
the `HostPanel` content variant properly and test all four criteria, including
identity, lifecycle, and burst-safe host events.

Staging this way means the expensive engine work is only undertaken after the
one question that can't be answered on paper — *does Baryon-mediated,
cell-quantized, asynchronous layout feel right for big visual panes?* — has
already been answered yes.

### Stage B merge criteria

Work continues on the `host-pane-seam` branch (`95d143f`, `283a155`,
`24fc3c5`). Merge to `main` once items 1–4 and 6 form **one coherent engine
cut** — `HostPanel` variants, projection including dividers, and the terminal
placeholder — not merely because the event seam exists. Item 7 (deleting the
Stage A probe) can follow once the real path replaces it.

### Stage B work breakdown

The two enums to extend are small; the cost is in their match sites.

```
BufferContent { Text, FileBrowser, MarkdownPreview }   // 43 sites, 29 in session.rs
ViewContent   { Text, FileBrowser, MarkdownPreview }   // 40 sites
```

1. **`HostPanel` buffer/view variants.** Add `BufferContent::HostPanel` +
   `ViewContent::HostPanel` carrying `HostPanelRef { panel_id, kind, title }`
   and view-local state (scroll offset if any). Most match sites are exhaustive
   `match`es that need a trivial arm; the compiler enumerates them.
2. **Guard rails at the boundaries.** Host panels are not text: they must be
   excluded from document operations (search, save, semantic pipeline, undo)
   the way `FileBrowser`/`MarkdownPreview` already are — follow those two as the
   template, since every such site already discriminates.
3. **Commands.** `OpenHostPanel { kind, title }`, `ClosePanel`, `RefreshPanel`,
   plus tab/split participation for free once the variants exist.
4. **Projection.** `PaneViewport` gains the host-panel descriptor; `Viewport`
   gains `dividers: Vec<PaneDividerViewport>` (decision #11).
5. **Host-event channel** (decision #5) — ✅ **first cut built 2026-07-20**,
   ahead of the variants so they land against a finished contract.
   `engine/host_event.rs` defines `HostEvent` (`UnhandledInput`,
   `PanelDisposed`, `PanelRestored`, `PanelRebound`) over `HostPanelId` /
   `HostPanelDescriptor` / `PanelRevision`, plus an **unbounded** sender —
   never a lossy bounded `try_send`, since ordering and delivery are the whole
   point. `EngineHandle::take_host_events()` exposes the receiver alongside
   `take_viewports()`, single-consumer, and `Engine::with_host_events()` keeps
   the 16-parameter `Engine::new` and its six test spawn helpers untouched.
   A departed frontend is a shutdown condition, not an error: `send` reports
   `false` and callers ignore it. Tests cover a 10,000-event burst arriving
   complete and in order, lifecycle ordering, a dropped receiver, and
   single-consumer transfer. No producer yet — that arrives with the variants.
6. **Terminal placeholder.** Baryon must render *something* for a host panel so
   the abstraction stays honest standalone.
7. **Delete Stage A**; promote `prompt_line` (already production) and retire the
   probe's divider math in favour of projected metadata.

## Sequencing

Revised to interleave Rune (Codex's revision, agreed):

1. Host-pane topology probe (above).
2. Polariton Rune runtime: a minimal `workspace.open()` plus one plot
   constructor building a `PlotComposition`, proving a script can create one
   native host panel with a lifecycle Baryon can place in tabs/splits.
3. Result-table and plot host panes, driven from both SQL and Rune.
4. Dashboard host pane, retaining nested layout and export behavior.
5. Markdown notebook document: explicit cell execution, stable cell ids, query
   revisions, visible staleness — SQL cells produce data, Rune cells turn data
   into output panels.
6. Only then consider row-quantized inline output blocks.

Ordering note: steps 1 and 2 are independent probes of the two riskiest seams
(engine-mediated layout feel; the Rune capability boundary) and could run in
either order or in parallel.
