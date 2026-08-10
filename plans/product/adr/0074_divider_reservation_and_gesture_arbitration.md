# ADR 0074: head-specific divider reservation and divider gesture arbitration

Status: proposed 2026-08-10. This record repairs a shipped input defect in
divider dragging, then makes pane-divider reservation a per-head presentation
profile so a GUI head is not charged terminal-cell gutters. Baryon keeps its
current geometry byte-for-byte. It changes no logical layout, no saved payload,
and no plot output. It authorizes no dashboard removal; PRODUCT-0075 owns that
and is gated on this record's exhibit.

## Context

Retiring Polariton's nested dashboard in favour of Rune-authored host panels
requires the replacement to be spatially competitive with the thing deleted. It
is not, by a wide margin — and the dominant cause is a terminal-cell
reservation a GUI head has no use for. §5 shows that removing it closes most of
the gap but not all of it; the residue is pane chrome, and the exhibit rather
than this record decides whether it is acceptable.

libaryon reserves divider space in **terminal cells**, because it serves a TUI
head where a divider must occupy whole cells to be visible or grabbable at all:

```text
SPLIT_DIVIDER_COLS = 4      core/constants.rs:9
SPLIT_DIVIDER_ROWS = 1      core/constants.rs:11
```

Polariton multiplies those slots by its font metrics — `CELL_WIDTH = 9.6`,
`LINE_HEIGHT = 22.0` (`editor.rs:133-134`) — so a GUI split reserves **38.4px
between columns and 22px between rows**. The dashboard being retired uses a
symmetric `STRIP_THICKNESS = 7.0` (`gui/pane_grid/handle.rs:18`). The
replacement is therefore about five and a half times coarser horizontally, and
asymmetric where the dashboard is square.

Reducing the constants cannot fix this. One column is `9.6px` and one row is
`22.0px`; no integer pair of cells produces a symmetric pixel gutter. `1 × 1`
merely exchanges one asymmetry for another.

Nor can the painter be thinned alone. `split_child_rects` subtracts
`divider_total` from the available extent *before* dividing by ratios
(`engine/session.rs:3757-3759`), and `minimum_extent` adds it back (`:3520`).
Painting a 7px divider while the layout still reserves 38.4px does not tighten
anything; it leaves unallocated space between panes. The reservation and the
paint must be one number.

Zero reservation needs no new margin, because Polariton's host-panel chrome
already supplies one. Host-panel content is wrapped in a container with
`.padding(4)` and a 1px `divider_surface` border (`editor.rs:3231-3244`), so two
cell-adjacent host panels separate their plot canvases by exactly

```text
4px padding + 4px padding = 8px canvas to canvas
```

Iced's `container::layout` shrinks a child by `padding` only; `border` is a
style painted *inside* the same bounds and never enters layout. The two 1px
borders therefore live within that 8px rather than adding to it, appearing as a
2px seam of divider colour down its middle. Eight pixels, symmetric in both
orientations, against the dashboard's 7px and today's 38.4px. (The
`.padding(4)` at `gui/components.rs:174` is a different thing — the outer editor
frame, applied once around everything. The two are indistinguishable in a diff
and only one multiplies per panel.)

### The shipped defect this uncovers

The gate that motivated this record — *clicking a cell beside a boundary must
place a cursor, and only a drag may resize* — already fails today, with no
geometry change at all.

```rust
if self.pane_drag.is_none() && !self.begin_divider_drag() {
    self.click_at_pointer();                    // editor.rs:2006-2008
    self.begin_pane_drag();
}
```

`begin_divider_drag` (`:2238-2262`) converts the pointer to a cell, tests
containment against the engine-published `hit_rect`, and assigns `self.drag`
immediately. There is no pixel test and no deadband, and a successful divider
drag means `click_at_pointer` never runs. Because `PaneDividerViewport`'s
`hit_rect` is deliberately larger than its `visual_rect` — its own documentation
says a 1-row horizontal divider "is a poor pointer target at its painted size" —
the overlapped row already belongs to the pane above. Clicking text there is
consumed as a resize gesture.

This is reachable on every horizontal split in the default configuration. It
goes unreported because 22px of dead space keeps the overlap small and a
misfire reads as a mis-aimed click. Zero reservation would extend the same
defect to every vertical boundary, where the overlap is a full 9.6px column of
live text.

A deadband already exists for a neighbouring gesture: `PANE_DRAG_DEADBAND = 8.0`
(`editor.rs:105`), used for pane-header reordering at `:2318` and `:2341`, and
never applied to dividers.

There is no third contender. `drag: Option<DividerDrag>` and
`pane_drag: Option<PaneDrag>` (`editor.rs:417,420`) are the only drag states,
and visual selection is keyboard-driven, so the arbitration is between resizing
and cursor placement only.

## Decision

### 1. Gesture arbitration is repaired before any geometry moves

Divider dragging becomes a two-phase gesture:

```text
press inside an eligible region   -> pending candidate, press origin stored
movement beyond the deadband      -> promote to active resize
release while still pending       -> ordinary click at the stored origin
```

Only an active resize emits a boundary change. A press that never moves, or
wobbles below the deadband, resolves as the click it was. Pane-header dragging
retains the priority it has today: the existing `self.pane_drag.is_none()`
guard runs first and is unchanged.

This repair stands on its own. It is correct against today's 4 × 1 geometry and
is gated independently of the profile in §4, so the defect is fixed whether or
not the profile lands.

### 2. Eligibility is narrowed by pixel proximity, using its own constant

A press is an eligible divider candidate only when **both** hold:

- the pointer's cell lies inside the engine-published `hit_rect`; and
- the pointer is within `DIVIDER_GRAB_PROXIMITY_PX` of the visual boundary.

Distance to the visual boundary is defined exactly, because "near the divider"
is ambiguous once the divider has width:

```text
inside a nonzero visual_rect   distance is 0
outside a nonzero visual_rect  perpendicular distance to its nearest edge
zero-extent visual_rect        perpendicular distance to the boundary line
```

Without the first clause, a 38.4px divider's centre would sit 19.2px from either
edge and fail a 10px test, making the painted gap itself ineligible.

Two named constants are introduced, and neither is defined in terms of the
other:

```text
DIVIDER_GRAB_PROXIMITY_PX     how near the boundary a press may begin a resize
DRAG_ACTIVATION_DEADBAND_PX   how far the pointer must move to promote a gesture
```

`DIVIDER_GRAB_PROXIMITY_PX` starts at `10.0` and
`DRAG_ACTIVATION_DEADBAND_PX` at `8.0`. They answer unrelated questions, and
tuning header-drag feel must not silently widen the strip in which an ordinary
click is at risk. The accepted geometry exhibit exposed the distinction: a 2px
painted seam with an invisible 8px target still felt one pixel wide in direct
use. The final implementation consumes the full engine-published column (9.6px)
or 10px of the published row and shows the axis-appropriate resize cursor over
that exact candidate region. Layout space and the painted seam do not grow.
`PANE_DRAG_DEADBAND` is renamed to `DRAG_ACTIVATION_DEADBAND_PX` and continues
to serve pane-header promotion; no third deadband is introduced.

Under today's positive-width gaps the whole painted divider remains eligible,
and only the candidate region nearest the boundary inside the overlapped cell
is ambiguous. Under §5's zero-reservation profile the full preceding column or
10px preceding-row strip beside the boundary stays draggable. The remainder of
the overlapped cell recovers immediate cursor placement it does not have today.

### 3. The frontend narrows what the engine publishes; it never widens it

Pixel proximity may only *reject* candidates inside a published `hit_rect`. It
may never admit a press outside one, extend a region toward a neighbour, or
synthesize a boundary between panes libaryon did not report adjacent.

This keeps authority where PRODUCT-0061 put it. libaryon decides where
boundaries exist and which region may grab them; Polariton classifies pointer
intent within that region at a precision its own font metrics afford. A
frontend that widened the region would fork the two heads' notion of where panes
meet, which no saved layout could reconcile.

### 4. Divider reservation becomes a per-head presentation profile

`EngineConfig` gains a divider-reservation profile in cells, defaulting to the
current `4 × 1`. Baryon takes the default. Polariton requests `0 × 0`.

The profile is runtime presentation state. It is never persisted, never enters
`libaryon_layout`, and never reaches `LayoutNode`, whose integer `ratios` remain
the head-neutral truth PRODUCT-0061 established. Two heads opening one saved
session derive different rects from the same tree, exactly as they already do
at different viewport sizes.

### 5. Polariton reserves nothing; the gutter is existing panel chrome

With `0 × 0`, panes tile edge-to-edge in cell space and each pane's own chrome
supplies the visible separation:

Four distinct quantities must be kept apart, because only the first is what
this record sets:

```text
divider reservation            0px, both axes            set here
pane-boundary seam             2px  (two 1px borders)    existing chrome
host canvases, side by side    4 + 4                     =  8px
host canvases, stacked         4 + 22 header + 4         = 30px
```

Borders paint inside container bounds and do not shrink children, so they are
seam rather than gap. Every pane also renders a fixed `PANE_HEADER_HEIGHT`
`22.0` tab bar above its content (`editor.rs:100`), so a stacked pair separates
its canvases by the lower pane's header as well. **The reservation is symmetric;
the rendered canvas separation is not,** and no divider profile can make it so.

No new margin, inset, or spacer is added. Against today's 46.4px side-by-side
and 52px stacked, this is a large gain. Against the dashboard it is not a win:
the dashboard pairs a 7px `STRIP_THICKNESS` with an 18px
`PANE_DRAG_HANDLE_THICKNESS`, giving roughly 7 × 25. So zero reservation removes
the terminal-cell leak completely and leaves a residual difference of about
1px horizontally and 5px vertically that is attributable to pane chrome, not to
divider geometry. Whether 8 × 30 reads as competitive with 7 × 25 is what the
exhibit decides; this record does not assert that it does.

The document-pane figure is deliberately stated rather than assumed adequate.
Two text panes separated by a 2px seam is a visible change to a surface this
record does not otherwise touch, and §Implementation requires it in the exhibit
alongside the plot grid. If it reads as cramped, the remedy is document-pane
chrome, not a reservation exception — but that judgement belongs to the
exhibit, not to this text.

### 6. Every geometry consumer is parameterized together

The profile reaches every consumer of the reservation in one change:

```text
split_child_rects              engine/session.rs:3757
minimum_extent                 engine/session.rs:3520
host_panel_tile_minimum        group tiling
ensure_host_panel_group        engine/session.rs:5476   admission
reset_host_panel_group         engine/session.rs:5611   admission
visual_rect / hit_rect         divider derivation
```

The last two are easy to miss and are production, not tests: both subtract
`SPLIT_DIVIDER_COLS` while deciding whether a host-panel group *fits* an anchor.
Left stale under `0 × 0` they would reject groups that now do fit, and ordinary
tiling tests would not notice, because tiling and admission are separate
calculations that happen to agree today.

A profile that reaches the rect splitter but not the minimum-extent calculation
produces panes that do not tile, which presents as a rounding bug and is not
one.

Baryon's own mouse handling may keep referring to the terminal constants
directly. That is head-local default behaviour, not an unconverted consumer;
this record does not claim every textual occurrence of the constants
disappears.

Zero-width and zero-height `PaneRect` values are already representable and
remain valid; a zero-extent `visual_rect` denotes a boundary line rather than an
absent divider, and `hit_rect` continues to be published independently.

`hit_rect` is not mechanically derived from the reservation, because that would
make it zero-extent too and leave nothing to grab. Under a zero reservation the
engine publishes **one preceding cell** as the hit rectangle on both axes — the
column left of a vertical boundary, the row above a horizontal one — so both
orientations stay grabbable and §2's proximity test has a region to narrow.
Under the default `4 × 1` profile the published `hit_rect` is byte-identical to
today's. Other positive reservations are not claimed identical — they cannot be
— and follow the same existing rule: a vertical `hit_rect` equals its
`visual_rect`, a horizontal one adds the preceding row.

### 7. What does not change

`LayoutNode`, its ratios, `MIN_PANE_EXTENT`, saved layout payloads, the
`polariton_session` schema, document text, plot geometry, chrome colours, and
every Baryon-visible pixel. Dashboard code is untouched: this record makes its
replacement competitive, and PRODUCT-0075 decides its removal.

## Implementation sequence

1. Bank Baryon's current `4 × 1` projection byte-for-byte — rects, dividers,
   minimum extents, and rendered TUI output. This is the only thing standing
   between a presentation profile and a silent TUI layout change.
2. Repair gesture arbitration against today's geometry: the pending-candidate
   state machine, the proximity test, and the two named constants. Prove the
   shipped horizontal click-stealing defect is gone before any reservation
   changes.
3. Introduce the `EngineConfig` profile defaulting to `4 × 1`, and parameterize
   every consumer in §6 together. With the default, all output stays identical
   to step 1's bank.
4. Configure Polariton as `0 × 0`.
5. Prove panes tile the full viewport with no unallocated divider space, and
   that host-plot canvases separate by 8px side by side and 30px stacked.
6. Prove both boundaries remain grabbable and that overlapped cells still place
   cursors.
7. Capture the exhibit: a dense Rune host-panel grid beside the existing
   dashboard, **and** a split document-pane view showing the 2px seam.

Steps 1 through 3 must each leave the previous step's oracle green. Only the
accepted step-7 exhibit authorizes PRODUCT-0075.

## Acceptance gates

1. **Baryon is untouched.** With the default profile, every banked TUI rect,
   divider, minimum extent, and rendered frame is byte-identical before and
   after the profile exists. No Baryon test is refreshed.

2. **The shipped defect is closed at today's geometry.** With `4 × 1`, a press
   in the overlapped row above a horizontal divider followed by release places
   a cursor at the press point and changes no topology. The pre-repair
   behaviour — an immediate `DividerDrag` with no movement — is asserted absent.
   This gate passes before any reservation change lands.

3. **Promotion requires movement.** A press inside an eligible strip followed by
   motion beyond `DRAG_ACTIVATION_DEADBAND_PX` moves the boundary and leaves the
   cursor where it was. A sub-deadband wobble resolves as a click. Exactly one
   of the two outcomes occurs for any gesture.

4. **Proximity narrows and never widens.** A press inside a published `hit_rect`
   but beyond `DIVIDER_GRAB_PROXIMITY_PX` of the boundary places a cursor. That
   fixture must sit in **overlapped live pane content**, not inside the painted
   gap, where there is no cursor placement to succeed at and the gate would pass
   vacuously. A press anywhere inside a nonzero `visual_rect` is at distance
   zero and stays eligible. A press outside every published `hit_rect` is never
   a candidate regardless of pixel distance, and a synthetic viewport publishing
   no divider yields no candidate anywhere.

5. **The constants are independent.** Changing `DRAG_ACTIVATION_DEADBAND_PX` in
   a test alters promotion distance and leaves candidacy width unchanged, and
   the converse. Neither is defined in terms of the other, and no third
   deadband exists.

6. **Header priority is preserved.** A press on a pane header still initiates
   pane reordering and never a divider resize, including when the header
   overlaps an eligible strip.

7. **The profile parameterizes every consumer.** With `0 × 0`, the sum of pane
   extents plus dividers equals the viewport exactly in both axes, at several
   viewport sizes and split shapes. A fixture that parameterizes the rect
   splitter while leaving `minimum_extent` at the constant fails to tile and is
   shown to fail this gate.

8. **Group admission follows the profile.** A host-panel group whose minimum
   width fits an anchor under `0 × 0` but not under `4 × 1` is admitted under
   the zero profile and refused under the default, through both
   `ensure_host_panel_group` and `reset_host_panel_group`. A build that
   parameterizes tiling while leaving either admission site on the constant
   fails this gate and passes every ordinary tiling test, which is why it is
   gated separately.

9. **Rendered separation is 8px side by side and 30px stacked.** Two
   cell-adjacent host panels under `0 × 0` separate their plot canvases by 8px
   horizontally and 30px vertically, measured from rendered child geometry
   rather than computed from the constants under test. The 2px border seam lies
   inside the 8px and is asserted not to add to it; the stacked figure is
   asserted to include exactly one `PANE_HEADER_HEIGHT`. A gate deriving either
   number by summing padding and borders would pass against a wrong
   implementation, and one asserting 8px in both axes would fail a correct
   one.

10. **Both boundaries stay grabbable.** Under `0 × 0`, vertical and horizontal
   boundaries each accept a drag begun within `DIVIDER_GRAB_PROXIMITY_PX`, and
   each moves the boundary in its own axis only.

11. **Logical layout is inert.** `libaryon_layout` bytes for one workspace are
    identical under both profiles, and a session saved by one head restores in
    the other with the same topology, tabs, ratios, and focus. The profile
    appears in no payload.

12. **Zero extents are valid.** A zero-width and a zero-height `visual_rect`
    each denote a boundary, are published with a nonzero `hit_rect`, and neither
    is treated as an absent divider by projection or hit testing.

13. **The exhibit is captured.** A dense Rune host-panel grid and a split
    document-pane view are rendered under `0 × 0` and recorded alongside a
    dashboard comparison. Evidence states the measured plot-canvas gutter and
    the document-pane seam.

14. **Nothing else moves.** Plot, table, SVG, GPU-probe, persistence, session,
    and dashboard baselines pass unchanged. The full workspace suite passes with
    no ignored test newly admitted.

## Stop condition

> This record may add the pending-candidate divider gesture, the two named
> constants, the `EngineConfig` reservation profile, its parameterization of
> every geometry consumer, Polariton's `0 × 0` selection, and the named gates
> and exhibit. If implementation requires fractional or pixel-valued pane
> geometry inside libaryon, a frontend-invented boundary or widened hit region,
> a persisted profile, a change to `LayoutNode` or its ratios, a document-pane
> reservation exception, or any Baryon pixel change, work stops.

> If the repaired arbitration cannot keep header dragging, divider resizing, and
> cursor placement mutually exclusive for every press, work stops. Three
> gestures sharing one press with ambiguous resolution is worse than the defect
> being repaired.

> If the residual stacked separation proves unacceptable, work stops for a
> pane-chrome decision. It may not be answered by a negative or fractional
> reservation, because the residue is the tab bar, not the divider.

> If the document-pane seam proves unacceptable in the exhibit, work stops for a
> document-chrome decision. It may not be answered by giving document panes
> their own reservation, which would reintroduce per-pane-kind geometry inside a
> head-neutral tree.

## Supersession

This record supersedes nothing accepted. `SPLIT_DIVIDER_COLS` and
`SPLIT_DIVIDER_ROWS` become the default profile rather than fixed constants;
their Baryon-visible effect is unchanged.

It repairs an unrecorded defect in divider gesture handling. No record
authorized clicks being consumed as resizes.

It consumes PRODUCT-0061's separation of logical layout from derived geometry
and depends on it: the profile is admissible only because divider extent was
already on the derived side of that line.

It is the prerequisite for PRODUCT-0075. Retirement of the dashboard is
authorized by this record's exhibit, not by this record.

## Rejected

- **Reduce the constants to `1 × 1`:** exchanges one asymmetry for another,
  since one column is `9.6px` and one row is `22.0px`.
- **Paint a thin divider over the existing reservation:** leaves unallocated
  space between panes, because the reservation is subtracted before ratios are
  applied.
- **Give libaryon fractional or pixel pane geometry:** replaces integer cell
  arithmetic throughout the engine to solve a presentation problem one head has.
- **Add a per-panel margin for the gutter:** host-panel chrome already supplies
  4px and a border; a second inset would double it and diverge from document
  panes for no reason.
- **Let document panes keep a reservation while host panels drop it:** puts
  pane-kind-specific geometry inside a head-neutral layout tree, and the seam
  question is answerable from the exhibit instead.
- **Reuse `PANE_DRAG_DEADBAND` for candidacy:** couples two unrelated
  quantities, so tuning drag feel would silently widen the strip where clicks
  are at risk.
- **Resolve divider presses on press with an undo on promotion:** produces a
  visible cursor jump before every resize.
- **Widen the hit region in the frontend:** the engine owns adjacency; a head
  that invents it forks the two heads' geometry irreconcilably.
- **Retire the dashboard in this record:** mixes deletion with the replacement
  work that justifies it, and leaves nothing to compare the exhibit against.

## Consequences

- Presses within `DIVIDER_GRAB_PROXIMITY_PX` of a boundary become
  release-resolved, while every other click stays press-resolved. This is
  bounded and intentional; it is stated so it is not later discovered as
  "clicking near the edge feels laggy."
- The overlapped cell beside a boundary recovers cursor placement it does not
  have today. That is a repair, not a regression, and it applies at the current
  geometry as well as under the new profile.
- A GUI head stops paying terminal-cell gutters. Host-plot canvases separate by
  8px side by side and 30px stacked, against 46.4px and 52px today. The
  remaining gap to the dashboard's roughly 7 × 25 is pane chrome — a 22px tab
  bar against an 18px drag handle — not divider reservation, and no profile
  value can close it.
- Rendered separation is therefore asymmetric even though the reservation is
  symmetric. That asymmetry is inherent to per-pane headers and is stated so the
  exhibit is read against the right expectation.
- Two heads now derive visibly different pane geometry from one saved layout.
  That was already true across viewport sizes; the profile makes it true across
  heads, and PRODUCT-0061's head-neutral tree is what keeps it sound.
- Document panes gain a 2px seam where they had 38.4px. The exhibit decides
  whether that is an improvement or a debt, and this record deliberately does
  not pre-judge it.
- PRODUCT-0075 becomes pure subtraction. Once the replacement is measurably at
  least as dense, deleting the dashboard removes code without removing
  capability.
