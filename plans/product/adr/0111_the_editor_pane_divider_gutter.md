# ADR 0111: the editor pane-divider gutter

Status: proposed 2026-08-15. Polariton currently selects
`PaneDividerReservation::NONE`: editor panes abut, the rendered divider is a
hairline, and the engine exposes only its one-cell fallback hit target. The
terminal head still selects its historical four-column-by-one-row reservation.
This record makes that reservation an opt-in Polariton startup preference,
without confusing it with a facet gutter, plot padding, or a border.

This supersedes PRODUCT-0074's **Polariton default** only. PRODUCT-0074 chose
`NONE` to remove the terminal-sized gutter from every GUI session. That remains
the default. This record makes the old `4 × 1` cell reservation reachable when
an author asks for it.

## Context

`PaneDividerReservation` is Libaryon's frontend-selected, runtime-only layout
input (`libaryon/engine/protocol.rs`). Its independent `columns` and `rows`
values participate in the engine's pane layout, and `render::dividers_for`
projects each divider's visual rectangle and hit rectangle from that same
reservation. A nonzero reservation therefore changes three things together:

```text
one cell reservation
        │
        ├── pane rectangles no longer abut
        ├── divider viewport has visible cells
        └── divider hit region covers those cells
```

They are one fact, not three settings that happen to agree. Polariton's
existing `divider_candidate_rect_px` intersects the projected hit rectangle
with a proximity region: it can only narrow the engine authority, never invent
or widen it.

At zero reservation, the two axes deliberately differ. A vertical divider
publishes one preceding content column as its hit rectangle. A horizontal
divider always extends its hit rectangle one preceding row above the boundary,
even when its visual reservation is zero. Both make a divider technically
draggable, but neither is a wide visual separator. The request is for the
latter when wanted, not for a wider overlay over abutting panes.

`EngineConfig` accepts the reservation only at engine construction.
`EditorSession::set_divider_reservation` is internal and no command invokes it.
That is useful: `~/.config/polariton/init.rn` already has a real startup caller
for Polariton-owned preferences. Polariton does have a Settings dialog, but it
currently controls per-view table presentation, not startup geometry; no
settings control, keybinding, or `:set` command calls a live reservation update.
A live command would be an inert surface and would also need to decide how an
existing pane tree re-lays out under a changed reservation.

## Decision

### 1. Polariton exposes `config.pane_divider_gutter(columns, rows)` at startup

The sole user surface is Polariton's existing application configuration:

```rune
pub fn main(config) {
    config.pane_divider_gutter(4, 1)?;
}
```

Both arguments are non-negative integer **terminal cells**. `columns` reserves
space for vertical pane boundaries; `rows` does the same for horizontal
boundaries. They are independent because Libaryon's existing
`SPLIT_DIVIDER_COLS` and `SPLIT_DIVIDER_ROWS` are independent.

`PaneDividerReservation::default()` is deliberately **not** this default: it
means `TERMINAL`. Polariton's `StartupData` must instead carry an explicit
`PaneDividerReservation::NONE` fallback, and configuration loading must retain
that value when the method is absent. Threading an optional value and falling
back through `Default::default()` would silently restore the terminal gutter.

The gutter parser is deliberately distinct from the existing positive-only
`dimension()` helper used by `window_size`: zero is a valid gutter value. Missing
configuration therefore retains PRODUCT-0074's compact Polariton layout exactly.
`config.pane_divider_gutter(4, 1)` selects the same reservation the terminal
head already uses.

### 2. Cells are the unit, not Polariton pixels

The setting retains Libaryon's cell unit. Four columns mean the same visual
proportion at a small and a large editor font; a fixed pixel gutter would look
fat in the former and hairline in the latter. More importantly, cells let the
terminal and GUI heads read one reservation authority rather than translating a
Polariton pixel preference into a second layout contract.

This setting intentionally couples pane geometry to the editor's font metrics.
That is not a typography feature: the metric conversion already belongs to the
head rendering cells. This record changes only how many cells Libaryon reserves.

### 3. One reservation remains the authority for layout, paint, and hit testing

`StartupData` carries one `PaneDividerReservation` into every Polariton
`EngineConfig` construction, including fresh, file-opening, and saved-session
starts. Libaryon then remains the exclusive authority for all pane rectangles,
divider visual rectangles, and hit regions.

Polariton may continue to style the projected divider or use its existing
short-range pointer candidate, but it must not add a second gutter width,
offset, or hit rectangle. `divider_candidate_rect_px` intersects the
engine-projected hit rectangle with the visual rectangle plus a fixed proximity:
it can narrow a candidate, never widen the engine authority. The visual
division and drag target must derive from the reservation that displaced the
panes.

### 4. The preference is startup-only and is not a session setting

Changing `init.rn` affects subsequently started Polariton editors. It neither
adds an `EditorCommand` nor a Libaryon `:set` token, and it does not mutate an
already constructed pane arrangement. The existing Settings dialog remains a
per-view presentation control; it gains no startup-geometry field in this cut.

The reservation remains runtime presentation state: it is absent from saved
layouts and checkpoints. A session opened with a different Polariton preference
uses that new head's geometry while retaining the same logical pane topology.
Persisting the reservation with a session would make a frontend preference look
like portable workspace meaning.

### 5. This narrows PRODUCT-0074; it does not restore a global default

PRODUCT-0074's source-text assertion cannot survive this change: it currently
greps `polariton_engine_config` for a literal `PaneDividerReservation::NONE`,
which must disappear once the value comes from `StartupData`. The gate changes
form, rather than loosening, to these two constructed-configuration claims:

1. `(0, 0)` is Polariton's compiled default; and
2. Polariton selects only the one `PaneDividerReservation` authored through its
   startup configuration, never terminal constants or locally invented widths.

The terminal head continues to select `PaneDividerReservation::TERMINAL`. No
existing user receives the old wide gutter unless they author it. The two
`#[cfg(test)]` convenience constructors also select `NONE` explicitly rather
than inheriting `EngineConfig::default()` and exercising a terminal-only layout
by accident.

## Implementation sequence

**Stage 0 — bank the test-only transition.** The two convenience constructors
currently use `EngineConfig::default()` and have 57 Polariton test call sites.
Before changing them to explicit `NONE`, run and bank the relevant split-layout
fixtures. A failure after the transition must be read as a possible hidden
dependency on terminal-only geometry, not mechanically re-baselined: production
has never selected that reservation.

**Stage 1 — thread one explicit startup value.** Add the zero-accepting config
parser and `StartupData` fallback, then carry its one reservation through all
three production engine constructors and the two test conveniences. Replace the
PRODUCT-0074 source grep with its constructed-configuration gates.

**Stage 2 — prove the existing engine projection.** Exercise both split axes
through Libaryon's layout and `PaneDividerViewport` projection. Polariton does
not acquire divider layout arithmetic or a live control.

## Acceptance gates

1. **Explicit compact fallback.** Missing `init.rn`, a `main` that does not
   call `pane_divider_gutter`, and `pane_divider_gutter(0, 0)` each construct
   `PaneDividerReservation::NONE` and preserve PRODUCT-0074's pane geometry.
   Structural: the configuration fallback names `NONE` directly and never uses
   `PaneDividerReservation::default()`.
2. **The old reservation is reachable.**
   `config.pane_divider_gutter(4, 1)` reaches every fresh, initial-file, and
   saved-session `EngineConfig` as exactly `{ columns: 4, rows: 1 }`. The two
   test-only convenience constructors select `NONE` explicitly, so tests do not
   retain an unrelated terminal reservation.
3. **Three coupled projections, vertical.** With a vertical split and four
   configured columns, the two pane rectangles are separated by exactly four
   engine cells; the projected divider's visual rectangle is those same four
   cells; and its hit region is exactly that visual rectangle. A widened hit
   region is not an equivalent vertical implementation.
4. **Three coupled projections, horizontal.** With a horizontal split and one
   configured row, panes are separated by one visual row and the hit region is
   exactly that row plus Libaryon's pre-existing one preceding row. Vertical
   nonzero hits equal their visual reservation; horizontal hits always include
   this extra preceding row. A symmetric one-row assertion would fail the
   correct engine behavior this record preserves.
5. **Zero remains a fallback, not a hidden wide handle.** At `(0, 0)`, panes
   abut and the visual divider is empty. The vertical hit region is its one
   preceding content column; the horizontal hit region is its one preceding
   row. This distinguishes the compact default from a Polariton overlay that
   silently widens dragging.
6. **No overlay authority.** Structural: Polariton derives divider decoration
   and pointer candidacy only from `PaneDividerViewport` geometry. It may
   convert projected rectangles and pointer positions between cells and pixels,
   but never reads `reservation.columns` or `reservation.rows` to derive a
   Polariton width, offset, or replacement hit rectangle.
7. **Independent axes.** `(7, 2)` reserves seven columns at a vertical split
   and two rows at a horizontal split in the same layout. Each axis leaves the
   other unchanged.
8. **Input refuses precisely.** Negative, fractional, overflowing, and wrong
   Rune values refuse at `config.pane_divider_gutter(...)`; zero is accepted.
9. **No inert live surface.** Structural: no `EditorCommand`, Libaryon `:set`
   registration, keybinding, or Settings-dialog control is added for divider
   reservation. `init.rn` is the only new caller.
10. **Runtime-only head preference.** A checkpoint made under `(4, 1)` contains
    no divider reservation. Reopening it under `(0, 0)` preserves the logical
    pane tree but projects compact geometry; reopening under `(4, 1)` projects
    the wide geometry again.
11. **Terminal behavior remains terminal-owned.**
    `PaneDividerReservation::TERMINAL` remains `SPLIT_DIVIDER_COLS ×
    SPLIT_DIVIDER_ROWS`, and Polariton never imports those constants to infer or
    reconstruct its configured value.

## Stop conditions

> If one configured reservation cannot reach every Polariton engine
> construction unchanged, stop. A fresh editor, opened file, and restored
> session must not acquire competing defaults.

> If the implementation needs a live command, settings control, or keybinding
> to make startup configuration useful, stop. That is a separately scoped
> interaction surface and must first name its caller and its re-layout contract.

> If a saved session must encode the preference to restore its logical pane
> topology, stop. The reservation is a head-level presentation choice, not
> session meaning.

> If Polariton must paint or hit-test a divider outside the geometry projected
> by Libaryon, stop. The three-way coupling has been broken.

## Rejected

**Paint a wide Polariton-only overlay.** Attractive because it is local GUI
work. Rejected because it can widen paint while panes still abut, and its hit
region would match neither the visual divider nor the layout reservation. It is
plausible on screen and false underneath — detectable only when a user drags.

**Put it in `:set`.** Attractive because it looks editor-like. Rejected because
Libaryon's persistent settings are portable editor meaning, while divider
reservation is frontend-selected presentation state. It would also add a live
command with no consumer.

**Use a pixel preference.** Attractive because Polariton renders pixels.
Rejected because it diverges from Libaryon's cell layout authority and loses a
stable proportion across editor font sizes.

**Make `TERMINAL` Polariton's default again.** Attractive because it recreates
the old look. Rejected because PRODUCT-0074 deliberately made compact panes the
default; this record restores reachability, not a global regression.

**Treat it as `facet_gutter`, plot padding, or a border.** Attractive because
all create visible space. Rejected because those act inside a figure or plot;
this reservation belongs between editor panes and controls editor topology.

## Consequences

Polariton gains an opt-in wide visual separator and grab region that are honest
because they occupy the same cells the pane layout already excludes. The default
remains the current hairline-like compact layout.

The adjacent settings now have distinct owners: `facet_gutter(...)` separates
cells inside one plotted figure; `config.pane_divider_gutter(columns, rows)`
separates Libaryon editor panes; plot padding and borders remain inside an
individual plot. Typography may alter cell metrics, but it does not acquire a
second pane-layout authority.
