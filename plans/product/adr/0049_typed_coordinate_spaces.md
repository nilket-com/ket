# ADR 0049: typed coordinate spaces

Status: proposed 2026-08-08. This record decides one thing: coordinate values
in Polariton's plot and table geometry carry their space in their type, with
explicit one-way conversions, so that cross-space arithmetic is a compile
error instead of a screenshot. It changes **zero pixels**, adds no Rune
surface, and migrates the computation layers where the class's specimens
lived — not the iced seam, whose raw types remain the wrap/unwrap boundary.

## Context

**The class, measured.** Plot and table geometry currently passes 66 function
signatures trading raw `Point`/`Rectangle`, with ~99 inline cross-space
arithmetic sites and 12 `translate` calls — each translate a space transition
the types cannot see. The census's sharpest finding is a convention split no
one decided: **the table widget draws in window space** (absolute
`fill_quad`s from `layout.bounds()`) while **the plot widget draws in widget
space** (canvas frames with local origins). Two renderers, two conventions,
one type — the precondition for every specimen below.

**The specimens.** Three coordinate bugs shipped in one week, each invisible
to origin-anchored tests because window and widget space coincide at the
origin:

- the grid composite consumed window-space `bounds.x/y` inside widget-space
  frames, displacing every cell by the panel's position (`c4207735`);
- the composite hover compared a widget-space cursor against window-space
  cell rects, leaving hover dead in production (same commit);
- iced's own `with_clip` divergence (vertex transform versus clip rect)
  belongs to the same family one layer down — not ours to fix, but ours to
  stop being exposed to by contract ambiguity.

The z-order paste bug is deliberately **excluded**: it was ordering, not
coordinates, and an honest class census is what keeps this record bounded.
The table's hit paths audited clean — but proving that required writing
offset-parametrized tests, and an audit the compiler could have performed is
an audit that will one day be skipped.

**The house rule already covers this.** `PalettePosition` is not a `usize`;
`PointSize` is not an `f32`; `NonEmpty` is not a `Vec`. Coordinates are the
one remaining domain where the codebase trades in raw pairs across meanings,
and five distinct spaces are evidenced in code: window, widget, cell, plot
(inner), and content (scrolled) — with data space already segregated behind
`CoordinateTransformer`'s `f64` domain boundary.

## Decision

### 1. Five spaces, named from the census

```text
Window   iced events and layout.bounds(): origin = window top-left
Widget   a widget's frame: origin = widget top-left
Cell     a grid cell's region: origin = cell top-left
Plot     the inner plot area: origin = plot_area top-left
           (what CoordinateTransformer already emits)
Content  scrolled table content: origin = first cell, pre-scroll
```

The vocabulary is closed and census-derived; a sixth space is a record
amendment, not a type parameter someone adds. Data space stays where it is —
`f64` domain values behind the transformer, which becomes the typed
`Data → Plot` boundary it already is in practice.

### 2. Zero-cost phantom types with explicit, evidence-carrying conversions

```text
Pt<S>   Rect<S>       S ∈ { Window, Widget, Cell, Plot, Content }

Pt::<Widget>::from_window(p, widget_bounds: Rect<Window>)
Pt::<Cell>::from_widget(p, cell: Rect<Widget>)
Pt::<Plot>::from_widget(p, plot_area: Rect<Widget>)
Pt::<Content>::from_widget(p, data_origin, scroll)
```

Same-space arithmetic is ordinary; cross-space arithmetic does not exist —
there is no `Add` between differing markers and no accidental unwrap. Every
conversion names the transform evidence it consumes (the bounds, the area,
the scroll), so the reviewer's question "which space is this in?" is answered
by the signature and the compiler enforces the answer. The wrappers are
`repr`-identical to the raw pairs; this costs nothing at runtime.

### 3. The iced seam stays raw, and is the only place wrapping happens

iced speaks raw `Point`/`Rectangle` at `Widget::update`/`draw`, in events,
and across `PlotBackend`. Those boundaries keep their types: the first thing
an entry point does is wrap into the space the seam defines (an event cursor
is `Pt<Window>`; a normalized frame bound is `Rect<Widget>`), and unwrapping
happens only at emission into a backend or renderer call. `translate` remains
the backend's space-transition primitive; the typed layer above it decides
*which* transition, which is where every specimen actually lived.

One boundary is stated now so the first uncaught bug reads as the record
working rather than failing: **the types end where the backend's transform
stack begins.** A correctly-typed `Pt<Plot>` emitted under the wrong
accumulated `translate` still lands in the wrong place — transform state is
in no type, and specimen (a) is arguably that failure rather than an
arithmetic one. This record closes cross-space *arithmetic*; drawing-state
discipline remains the composites' contract, guarded by the parity oracles,
not by the compiler.

The two drawing conventions the census found are not unified here — they are
**named**: table draw signatures say `Rect<Window>`, plot composite
signatures say `Rect<Widget>`, and the difference becomes documentation the
compiler checks instead of folklore two bugs deep. Unifying them is a
separate record if it is ever worth its churn.

### 4. Migration scope is the computation layer where the class lives

Migrated in this cut: the grid composite's region, gutter, and hit-routing
geometry; the table's `hit_test_cell`, divider, scrollbar-thumb, and scroll
math; the plot widget's hover and plot-area paths — every function the
specimens touched or the offset tests were written to guard. Not migrated:
kernel-internal mark math (post-translate, uniformly `Plot`-space, a
follow-up if it ever earns one) and anything inside iced.

### 5. Zero pixels, and the compiler is the new gate

No behavior changes. Every existing draw oracle, parity baseline, and
origin-invariance test stays green and byte-identical. The offset and parity
tests are **not made redundant**: §3 bounds the types at the backend's
transform stack, and those tests are the load-bearing guard for exactly that
excluded territory, plus gate 6's truth-of-annotation proof. What closes
structurally is cross-space arithmetic — the next specimen of *that* class
fails at `cargo check`, not in a screenshot after midnight.

## Acceptance gates

1. The full suite is green with every rendering oracle byte-identical; no
   SVG, draw-operation, or parity baseline changes by even a byte.
2. The migrated signatures carry space types: `hit_test_cell`,
   `divider_at_cursor`, the thumb rects, grid cell/gutter geometry, and the
   composite hover path each name their spaces in their parameters.
3. Cross-space arithmetic is proven uncompilable by **`trybuild` UI tests**
   in `crates/polariton/tests/ui/` — the crate is binary-only, so rustdoc
   collects no doctests and a `compile_fail` doctest would pass vacuously
   forever, which is worse than no gate. The cases include specimen (b)'s
   exact shape — a widget-space cursor against a window-space rect — with
   the expected error text banked as evidence. Unlike `NonEmpty`, where
   unrepresentability is self-evident from the type, this claim is about
   **absent** impls, and absence is exactly what a compile-fail test proves
   and inspection does not. (`trybuild` enters as a dev-dependency; the
   manifest architecture guard scans dev-dependency tables and admits
   non-connectors.)

   The binary-only crate leaves a second vacuity path this gate closes
   explicitly: a fixture that cannot resolve its imports "fails correctly"
   for the wrong reason. Therefore: fixtures consume the **production
   definitions by path-include of the production module** — never replicas —
   which requires the space module to stay dependency-free beyond iced; a
   **passing same-space twin** compiles through the identical import path,
   so a broken include turns the suite red instead of vacuous; and the
   banked failure text names the **cross-space mismatch specifically**, not
   an unresolved import. A minimal library seam was considered and is
   rejected for this cut: exporting a library target is a crate-posture
   change a zero-pixel record should not smuggle, and the twin-guarded
   harness needs nothing from it. If the module cannot stay
   dependency-free, that choice returns as its own decision.
4. The type-level promises are gated, not prose: the `Space` marker trait is
   **sealed** with exactly five implementations, asserted by enumeration;
   wrapper fields are private with no `Deref` to the raw types; and
   `size_of`/`align_of` equal iced's raw `Point`/`Rectangle` by static
   assertion — "zero-cost" and "closed vocabulary" as checks, not claims.
   Conversions remain the only wrap points, asserted structurally: no
   constructor of `Pt`/`Rect` from raw parts exists outside the seam
   wrappers and conversion functions.
5. The existing origin-invariance and offset-parametrized tests pass
   unchanged, and remain **load-bearing**: they guard transform-state
   correctness (the territory §3 excludes from the types) and supply gate
   6's truth proof. Nothing here retires or weakens them.
6. Table draw paths are typed `Window`; plot composite paths are typed
   `Widget`. Compilation proves the annotations self-consistent, not true —
   the offset-parametrized tests of gate 5 are what prove them **true**,
   being the assertions that fail if a path's claimed space is not its
   actual one. The gate is discharged by both together, never by compiling
   alone.
7. No Rune, kernel, identity, placement, lifecycle, or timing behavior
   changes; 421+38 remains the floor.

## Stop condition

> This record may add the space markers, `Pt`/`Rect` wrappers, the named
> conversions, and the migration of the listed computation paths. If the
> migration requires changing any behavior, any iced trait bound, any
> baseline byte, or reaches into kernel-internal mark math or iced
> internals, the work stops and the requirement returns as its own record.

The expected trigger is discovering a sixth space mid-migration — a value
that is genuinely in none of the five. That is a vocabulary amendment under
this record's own census rule, not a reason to leave the value raw.

## Rejected

- **Unifying the table and plot drawing conventions while typing them:**
  bundles a behavior-preserving refactor with a behavior-risking one; the
  types make the split explicit, which removes its danger without its churn.
- **Typing kernel-internal mark math now:** post-translate kernel code is
  uniformly `Plot`-space; migrating twenty kernels for a class with zero
  specimens there is scope without evidence.
- **A runtime-checked space tag:** costs at runtime what the phantom costs
  at compile time, and turns a prevented bug back into a caught one.
- **Naming spaces after widgets ("TableSpace", "GridSpace"):** spaces are
  about origins and transforms, not owners; five origin conventions exist
  regardless of who draws in them.
- **Waiting until after the legend placement record:** that record is the
  queue's last large coordinate-touching change; written raw it becomes
  migration churn, written typed it becomes the vocabulary's first native
  consumer.

## Consequences and sequencing

- **Cross-space arithmetic** closes structurally: that class's contact
  findings become one compile error. The offset-test idiom stays
  load-bearing for transform-state correctness — the types narrow its
  responsibility; they do not inherit it.
- The legend placement/visibility record (0050, next, with the
  `Presence::Hide` repair riding along) is written in the typed vocabulary
  from its first line — the ordering chosen precisely so the queue's
  remaining coordinate work is native rather than migrated.
- The window/widget convention split is finally written down where it
  cannot drift: in signatures.
- The measured selection treatment record remains on request, orthogonal to
  this arc — color, not coordinates.
