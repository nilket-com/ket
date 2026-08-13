# ADR 0097: the faceted hover coordinate seam

Status: proposed 2026-08-13. A faceted grid builds each cell's
`CoordinateTransformer` from **absolute** widget bounds and then hands the kernel
a **plot-local** cursor, so a kernel comparing that cursor with
`transform.bounds` or geometry derived from those bounds compares values from
different spaces. Two kernels expose the mismatch — funnel
through a bounds-containment check and radial dial through geometry derived from
the bounds — and selected non-origin probes fail or resolve incorrectly. This
record makes the grid pass the absolute cursor, matching the non-faceted path,
and adds one typed hit-geometry seam that states the transformer's expectation.

It is step 12 of `plans/wip/plot-catalog-restoration.md`, sequenced after
PRODUCT-0096 so it has a validated funnel kernel as a parity subject.

## Context

### The mismatch, and that it was seen

```rust
// grid.rs:815-820
// Cell localization is explicit even though the legacy kernel seam
// below consumes the still-more-local Plot point.
let _cell_cursor = Pt::<Cell>::from_widget(cursor, cell_bounds);
let inner = grid_plot_area(cell_bounds, &resolved);
let transform = CoordinateTransformer::new(layout, inner.into_iced());
let local = Pt::<Plot>::from_widget(cursor, inner);
```

`:819` builds the transformer with absolute `inner`; `:820` localizes the cursor
to `inner`'s origin. The resulting mismatch shifts the hit region by the plot
area's absolute origin. Whether any numerical overlap survives depends on the
cell origin, size, and padding; the Stage 0 probes establish the actual failures
rather than claiming every point in every non-origin cell fails.

The comment names the seam, and `_cell_cursor` is computed and discarded. This
was observed and left, not overlooked.

### What exists, surveyed 2026-08-13

**The non-faceted path is already consistent.** `editor.rs:1528-1531` builds the
transformer from the widget's `bounds` and passes the widget-space `point` — one
space on both sides.

**All three grid transformers use absolute bounds.** Paint at `grid.rs:581`, the
second paint path at `:667`, and hover at `:819`. Only hover localizes its
cursor.

**The transformer's bounds are uniformly widget-space.** `CoordinateTransformer
{ pub layout, pub bounds: Rectangle }` (`common.rs:249-252`) is untyped, but the
grid's derivation never leaves the space: `grid_tiled_area(Rect<Widget>) ->
Rect<Widget>` (`:853`) → `grid_cell_bounds` → `grid_plot_area(Rect<Widget>) ->
Rect<Widget>` (`:881`). There is no polymorphism here, only an unstated
invariant.

**The typed vocabulary is closed and transparent.** `Pt<S>` and `Rect<S>` are
`#[repr(transparent)]` over iced types across a sealed five-space macro
(`coordinate.rs:32-58`), with `into_iced()` documented as the sanctioned unwrap
"at an iced renderer/backend emission seam."

**Exactly two kernels consume transformer bounds during hover.** Funnel
(`funnel.rs:168-170` after PRODUCT-0096) calls
`transform.bounds.contains(cursor_pos)`. Radial dial (`radial_dial.rs:84-93`)
does not call `contains`: it derives its ring centre and radii from
`transform.bounds`, then compares the raw cursor against that geometry. The
other nineteen `hover` implementations never read `transform.bounds`.

**The wide options are large.** 34 production reads of `transform.bounds`, ~83
`CoordinateTransformer::new` sites across 16 files, and 21 kernel `hover`
implementations. `editor.rs:1519-1520` takes untyped `iced::Rectangle` and
`iced::Point`.

## Decision

### 1. Absolute cursor, absolute bounds — the grid adopts the existing contract

The grid passes the **original widget-space cursor**. `editor.rs` already does
this; the grid becomes identical rather than newly specified.

**Local/local was never actually available.** Paint at `:581` and `:667` already
commits each cell's transformer to absolute bounds, so localizing hover's
transformer would give one cell two transformers with different origins —
trading a hover defect for a paint/hover divergence, the two-truths shape
PRODUCT-0084, PRODUCT-0089 §1, PRODUCT-0094, and PRODUCT-0096 §7 each removed.

### 2. The cursor is passed through, not localized and relabelled

Grid hands the **original** `Pt<Widget>` to the kernel seam through
`into_iced()`, which `coordinate.rs:90` sanctions for exactly this.

It must **not** localize to `Pt<Plot>` and then relabel or re-offset the result
back. That would produce the same number by two conversions that cancel, leaving
a reader unable to tell whether the space is correct by design or by accident —
and one later edit to either conversion silently reintroduces the defect.

### 3. One typed hit-geometry seam on the transformer

`CoordinateTransformer` gains a helper accepting `Pt<Widget>` and returning the
paired widget-space hit geometry — `(Rect<Widget>, Pt<Widget>)`, or an opaque
equivalent. Funnel uses the rectangle's typed `contains`; radial dial uses the
same rectangle and point for its existing centre/radius arithmetic. This
preserves both kernels' current hit-test behaviour while making the shared space
explicit. A Boolean `contains` helper would not serve radial dial and would force
new hit-test policy into this record.

Two narrowly named trust-boundary wrappers make the helper implementable without
genericizing the transformer:

- `Pt::<Widget>::from_plot_kernel_cursor(Point)` asserts a raw cursor from
  `PlotKernel::hover` as widget-space;
- `Rect::<Widget>::from_transformer_bounds(Rectangle)` asserts the transformer's
  raw bounds as widget-space.

The transformer method is
`widget_hit_geometry(Pt<Widget>) -> (Rect<Widget>, Pt<Widget>)`. Naming all three
surfaces is part of the contract: a generic `from_iced` would turn a narrow trust
assertion into an unrestricted relabelling door.

Both are zero-arithmetic wraps, not coordinate conversions. Their names state
the production invariant they trust. `from_plot_kernel_cursor` has exactly two
production call sites — funnel and radial dial — and
`from_transformer_bounds` has exactly one, inside `widget_hit_geometry`; gate 8
closes those sets.
The existing private `Pt::wrap` / `Rect::wrap` cannot be called from kernels or
`common.rs`, so leaving these constructors implicit would make §3 impossible to
implement.

No generics, no typed `bounds` field: the bounds are `Widget` at every production
site today, so the method states a fact rather than adding a parameter.

### 4. This does not make the mismatch unrepresentable, and the record says so

`PlotKernel::hover` still receives an untyped `iced::Cursor`. Each kernel must
therefore **assert** that its point is widget-space before wrapping it for §3's
method. The trait remains a **trust boundary**.

What §3 buys is real but bounded: the transformer's expectation becomes explicit,
and typed spaces cannot be mixed *after* the assertion. What it does not buy is
compile-time proof that the assertion is true. **The non-origin gates enforce
what the compiler cannot**, and they are the record's primary evidence rather
than a supplement to a type argument.

Claiming otherwise would be the more comfortable framing and would leave the next
reader believing a guarantee that does not exist.

### 5. No kernel-local compensation

Neither funnel nor radial dial gains an offset, a bounds adjustment, or a
space-detection branch. The defect is at the grid seam and is repaired there.

A kernel-local offset could mask today's mismatch for selected cells, but it
would duplicate the grid's coordinate responsibility and become wrong when the
seam was fixed. Both kernels would then need an un-compensation pass.

### 6. Dead typed conversions are deleted

`_cell_cursor` (`:817`) and the `Pt::<Plot>` localization (`:820`) are removed if
neither serves grid-owned behaviour after the repair.

A discarded conversion does not preserve type safety; it documents a path no
consumer uses, and reads as though a space distinction is being maintained when
nothing depends on it. If some grid-owned work does need cell-local or plot-local
points, those conversions stay **at their use sites** — not at the seam they no
longer feed.

### 7. What this record does not do

No typed cursor through `PlotKernel` — 21 implementations. No typed
`CoordinateTransformer::bounds` — 34 reads and ~83 construction sites. No typed
`editor.rs` entry. No change to any kernel's geometry, hit-test arithmetic, or
hover text. No change to paint. That wider redesign sits behind the fourth stop
condition.

## Implementation sequence

**Stage 0 — bank the red state.** For funnel and radial dial, bank one selected
probe in the first facet cell and one in a **non-origin** cell, including the
actual returned value. Do not presume the first probe is correct: top padding can
make even cell one's absolute origin non-zero. The pair identifies the defect as
positional; the bank says precisely whether each probe currently returns `None`,
the wrong stage/category, or the intended datum.

**Stage 1 — the typed hit geometry.** Add §3's two trust-boundary wrappers and
the `CoordinateTransformer` helper, with no consumer migrated.

**Stage 2 — the grid seam.** §1 and §2: pass the absolute cursor; §6's deletions.

**Stage 3 — the two kernels.** Migrate `funnel.rs:168-170` and
`radial_dial.rs:84-93` to the typed hit geometry. No geometry arithmetic changes.

## Acceptance gates

1. **Funnel hovers in a non-origin facet cell.** A two-cell faceted funnel returns
   the intended stage text for a selected position inside the **second** cell.
   The bank records the old result at that exact position.
2. **Radial dial hovers in a non-origin facet cell.** The same, and asserted
   separately: the two kernels are repaired by one grid change, and a gate
   covering only funnel would pass a fix that special-cased it.
3. **First-cell hover is correct.** Both kernels return the intended first-cell
   data at the selected Stage 0 probes. If a probe was already correct, its text
   remains byte-identical; if the bank exposed a shifted or absent hit, the gate
   records the deliberate correction. This avoids asserting parity the bank has
   not established.
4. **Hover selects the correct cell.** In a two-cell grid, a position in cell two
   returns cell two's data — not cell one's. Without this, a fix that made every
   cell hoverable against the first cell's transformer would pass gates 1 and 2.
5. **Paint and hover agree on cell geometry.** For a position inside a mark drawn
   in a non-origin cell, hover resolves to that mark. This is the underlying
   defect stated directly: two consumers of one transformer disagreed about its
   space.
6. **No kernel-local compensation exists.** Neither kernel adds an offset, bounds
   adjustment, or space-detection branch — structural, per §5, since a
   compensating kernel passes every behavioural gate above.
7. **Kernel geometry is untouched.** Every banked funnel and radial-dial figure —
   including PRODUCT-0096's post-implementation bank — is byte-identical. This
   record changes hover routing, not marks.
8. **The typed hit geometry accepts only `Pt<Widget>`, and assertions stay at
   their trust boundaries.** Extend the existing coordinate-space compile-fail
   suite so `Rect<Widget>` rejects `Pt<Cell>` and `Pt<Plot>`. An architecture
   gate asserts the production helper's `Pt<Widget>` signature and the closed
   call sites of the raw-point/raw-bounds assertion wrappers. Do not `#[path]`
   include or copy a miniature `common.rs` helper into trybuild: that would test
   the fixture rather than production.
9. **No dead typed conversions remain at the seam.** `_cell_cursor` and the
   discarded plot-local localization are gone; any surviving typed conversion has
   a consumer. Structural, per §6.
10. **The nineteen non-hit-testing kernels are untouched.** No `hover`
    implementation other than funnel's and radial dial's changes, and
    `PlotKernel`'s signature is unchanged — the bound this record accepted in §7.
11. **The non-faceted path is unchanged.** Single-plot hover through
    `editor.rs:1528` behaves byte-identically. It was already correct, and it is
    the contract §1 adopts rather than modifies.

## Stop conditions

> If repairing the seam changes any painted pixel, stop. §1 touches hover routing
> only; a paint change means the two transformers were not in fact identical, and
> that divergence is a finding to record before it is erased.

> If either kernel needs an arithmetic adjustment to hover correctly after the
> grid passes an absolute cursor, stop. §5 forbids compensation, and a kernel
> that needs one is evidence the bounds are not the space this record concluded
> they are.

> If some grid-owned behaviour turns out to depend on `_cell_cursor` or the
> plot-local cursor, stop and keep the conversion **at that use site** rather than
> at the seam. §6 removes dead conversions, not live ones, and the distinction is
> the whole point.

> If the typed hit geometry cannot be added without genericizing
> `CoordinateTransformer`, typing its `bounds` field, or changing
> `PlotKernel::hover`, stop and land **absolute/absolute as the contract without
> the typed method**. The behavioural repair does not depend on §3; the wider
> type redesign — 34 bounds reads, ~83 construction sites, 21 hover
> implementations, and an untyped `editor.rs` entry — is its own record and must
> not be entered through this one.

## Supersession

Supersedes no record. Depends on PRODUCT-0096 for a validated funnel parity
subject. Unblocks the funnel surface (PRODUCT-0098) and removes a defect radial
dial would otherwise inherit at step 14. Leaves the coordinate-vocabulary
redesign unscoped and behind the fourth stop condition.

## Rejected

**Give the hover transformer local bounds so local/local is consistent.**
Attractive because it keeps the existing `Pt<Plot>` localization and looks like
the more "typed" answer. Rejected because paint already builds the same cell's
transformer with absolute bounds (`grid.rs:581`, `:667`), so one cell would carry
two transformers with different origins — a paint/hover divergence in place of a
hover defect.

**Fix it in the two kernels by offsetting against the cell origin.** Attractive
because it touches two files instead of the grid. Rejected because it duplicates
the grid's coordinate responsibility, requires un-doing when the seam is repaired
properly, and leaves the mismatch available to trap the next kernel that consumes
transformer bounds.

**Change `PlotKernel::hover` to take a typed cursor.** Attractive because it is
the only change that would make the mismatch genuinely unrepresentable. Rejected
as scope: 21 implementations plus an untyped `editor.rs` entry, which is a
coordinate-vocabulary redesign wearing a bug fix. §4 states honestly what is
given up.

**Keep `_cell_cursor` as documentation of the space distinction.** Attractive
because deleting typed code feels like losing type safety. Rejected because a
conversion no consumer reads documents a path that does not exist — it is the
comment at `:815-816` in code form, and that comment described the defect rather
than preventing it.

**Assert the space at runtime inside the transformer.** Attractive because it
would catch a wrong-space cursor where the type system cannot. Rejected because
the coordinate values carry no runtime space tag and can be numerically
identical — for example under a zero-origin layout — so such an assertion could
only rediscover some visible mismatches, not establish the invariant.

## Consequences

Two marks resolve the intended data in faceted grids, and the grid's hover path
becomes identical to the single-plot path rather than a variant of it.

The type system is left honestly short of the guarantee. `PlotKernel::hover`
remains a trust boundary, each kernel still asserts its cursor's space, and gates
1 through 5 are what actually hold the contract. That is a weaker position than
"unrepresentable," and stating it is the difference between a reader who tests
the boundary and one who assumes it is closed.

The defect was documented in a comment before it was fixed. `grid.rs:815-816`
named the seam and `_cell_cursor` marked it, and neither prevented two kernels
from shipping failed or shifted non-origin hover — a reminder that a noted seam
is not a guarded one, and that the note outlived every reader who might have acted
on it.
