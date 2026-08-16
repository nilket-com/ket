# ADR 0114: the bottom and left band reservation

Status: proposed 2026-08-16. PRODUCT-0106 reserved the **top** band from
resolved font sizes and left the bottom and left edges as constants —
`plot_padding_bottom = 80.0` and `plot_padding_left = 100.0`
(`common.rs:532-533`). Nothing noticed, because nothing could change the sizes
those bands hold. PRODUCT-0113 made sizes authorable and had to block four
roles on exactly this.

This record reserves both remaining bands. Unlike the top band, they cannot be
reserved from font size alone: the text that occupies them is **wide**, not
just tall, so the reservation depends on an estimate of horizontal extent. §2
states that estimate as a contract with a declared alphabet rather than as a
number, and §7 fixes the single condition under which PRODUCT-0093 §7's
measurement deferral fires.

It exposes no Rune surface. It unblocks four roles for the record after it.

## Context

### What actually occupies each band

**Bottom, `plot_padding_bottom = 80.0`,** measured down from the plot area:

| occupant | position | extent |
|---|---|---|
| x tick labels | `+x_label_offset = 10.0`, then `rotate(x_label_rotation)` (`common.rs:1902-1906`) | `x_tick_size = 18.0`, **and width once rotated** |
| x axis title | `+x_label_padding = 45.0`, `align_y: Top` (`:1524-1531`) | `x_label_size = 20.0` |

**Left, `plot_padding_left = 100.0`,** measured left from the plot area:

| occupant | position | extent |
|---|---|---|
| y tick labels | right-aligned at `plot_area.x - 10.0` (`:1866-1869`) | **width** of the longest label at `y_tick_size = 18.0` |
| y axis title | `-y_label_padding = 85.0`, rotated `-π/2` (`:1536-1550`) | `y_label_size = 20.0` horizontally, because it is rotated |

The asymmetry is the whole problem. Three of these four occupants consume space
proportional to their **font size**, which is a one-sided bound. The y tick
labels consume space proportional to their **rendered width**, and so do the x
tick labels the moment `x_label_rotation` is nonzero.

### Rotation is plumbed and unreachable

`x_label_rotation` exists (`common.rs:446`), is applied at all three x-tick draw
sites (`:1906`, `:2081`, `:2297`), and defaults to `0.0` with nothing anywhere
setting it otherwise. No Rune word, no theme write. The substrate is complete
and the language cannot say it — PRODUCT-0055's situation exactly.

It matters here because rotation is what people reach for when labels are long,
and it converts the bottom band from a font-size bound into a width-dependent
one. This record must therefore compute a band that is correct **at any
rotation**, while exposing none.

### Long labels are not a time-axis problem

Date labels are one source of long text — `AxisType::Date` renders `%Y-%m-%d`,
ten characters (`common.rs:2451-2455`). **Long category names are another, and
they need no new plumbing at all.** Any solution keyed to temporal types would
miss the more common case and would gate this work behind an unrelated question
about whether Toron preserves a date dtype end to end.

The reservation is therefore driven by estimated label extent, whatever
produced the label.

### The estimator exists, and its declared alphabet does not cover this

`estimate_legend_text_width` (`legend.rs:345-346`) is
`chars × font_size × NUMERIC_GLYPH_ADVANCE_EM`, with the advance at `0.75`
(`:286`). Its doc comment scopes it precisely (`:339-344`):

> *"Conservatively bound the fixed-precision **numeric** labels used by
> gradients. … The endpoint alphabet is limited to digits, sign, and decimal
> point."*

Every call site is a gradient endpoint. It has never been applied to
alphabetic text, and `0.75` is **not** conservative for it: `W` and `M` run
near a full em in most proportional faces. Applying it unchanged to category
names would under-reserve — text drawn past its own reservation, which is the
collision this record exists to prevent, delivered by the mechanism meant to
prevent it.

So a second estimator is needed for unbounded text. Which band gets which is
**not** decided by the edge — §5 shows the producer decides, and a temporal or
categorical axis can appear on either.

### No figure has ever exercised a long-label axis

No `uat` fixture produces long tick labels — the nine probes and `demo_sales`
are short categorical and numeric. Whatever crowding exists today has never
been on screen. This is the `demo_sales` single-quarter trap again: the
capability is reachable and the one configuration that would expose its
geometry has never been built.

### PRODUCT-0048 already computes these bands once per grid

`grid_tiled_area` (`common.rs:1217-1231`) subtracts `plot_padding_left` and
`plot_padding_bottom` **once for the whole grid**, which is PRODUCT-0048's
shared-edge gutter model and the reason facet cells stay congruent.

That is fortunate: making the bands derived preserves congruence **by
construction**, provided the derivation is grid-wide rather than per cell. A
per-cell derivation would make cells with longer labels wider than their
neighbours, which is precisely the noncongruence PRODUCT-0048 rejected as a
per-cell pixel-scale lie.

## Decision

### 1. Both bands derive from one authority, grid-wide — fed by a layout-derived input

The below-plot and left-of-plot bands are computed once, by the same authority
that already computes the top band. Every consumer — the single-plot path,
`grid_tiled_area`, and per-cell plot areas — reads that result. No caller
subtracts its own band. This is PRODUCT-0106 §4 restated for two more edges,
and PRODUCT-0048's congruence follows from it.

**But that authority cannot see labels today, and it runs too early.**
`VerticalGuideBands::resolve` takes only a fallback title, a facet label, and
settings (`common.rs:1151-1155`). And the ordering is against us: `plot_area`
is computed at `common.rs:1324`, `:1368`, and `:1660` — in each case
*immediately before* `kernel.layout(...)` at `:1325`, `:1371`, `:1661`. The
band is decided before the layout that knows what the labels will say. Left
unrepaired, §5's producer-keyed estimate is unimplementable except by
inspecting labels inside a later consumer, which is the second authority §1
exists to forbid.

So this record introduces **band requirements**: a layout-derived value naming,
per axis, the label producer and the longest label that axis can produce.

- **Single plots** derive it from their one layout, which moves
  `kernel.layout(...)` ahead of `plot_area`. The two have no dependency today —
  `layout` takes only settings — so this is an ordering change, not a
  restructuring.
- **Grids** derive it as the **maximum requirement across all cell layouts**,
  computed before `grid_tiled_area`. The maximum is what keeps the band
  figure-wide and therefore congruent; a per-cell requirement is the
  pixel-scale lie PRODUCT-0048 rejected.

**Requirements come from the domain and its producer, never from the realized
tick set.** This breaks a real circular dependency rather than expressing a
preference: how many ticks fit depends on the plot extent, the plot extent
depends on the bands, and bands keyed to realized ticks would depend back on
the extent. For `format_label`, that means sampling every formatting-branch
boundary **and the widest value available within each branch**, not merely the
domain endpoints: a domain ending at `1e6` can contain `999999.9`, whose fixed
label is wider than either scientific endpoint. Deriving those candidates from
the axis domain and its `AxisType`, or the longest category from the category
set, closes the loop — at the cost of reserving for a label a sparse tick
placement might not draw. That over-reservation is deliberate and is the
conservative direction.

### 2. The extent estimate is a three-part contract, not a number

Any estimator this record uses must state all three:

1. **The declared alphabet** it is conservative over.
2. **The budget** — the per-character advance applied within that alphabet.
3. **The behavior outside it** — what happens to a label the alphabet does not
   cover.

The existing numeric estimator already has all three, which is why it is
honest. The general one must too, and its third part cannot be "refuse": a
category value comes from data, not from authoring, so there is nothing to
refuse. Out-of-alphabet labels **may overflow their reservation**, and the
record says so plainly rather than implying a guarantee no fixed advance can
make. Unicode beyond the declared alphabet — full-width CJK, emoji, combining
marks — is exactly this case.

A flat one-em budget over an ASCII-printable alphabet is the starting point,
not a universal claim.

### 3. The bottom band is the maximum of its occupants, not their sum

Both occupants are measured from the same edge and **overlap**: ticks begin at
`x_label_offset = 10.0`, the axis title at `x_label_padding = 45.0`. They share
one band; they do not stack.

```text
bottom = max(
    x_label_offset  + rotated_tick_height,
    x_label_padding + x_label_size,
)

rotated_tick_height = estimated_width·|sin θ| + x_tick_size·|cos θ|
```

A sum would over-reserve by roughly the smaller term and would let gate 3
accept geometry that is merely larger rather than correct. This is the same
shape as PRODUCT-0106's heading band, which is `title.max(subtitle)`
(`common.rs:1180`) for the same reason.

At today's `rotation = 0` the tick term reduces to `x_tick_size`, so the
formula is correct now and stays correct when rotation becomes authorable.
Building it rotation-aware costs one term and removes the need to revisit the
authority later.

### 4. Rotation is not exposed here

No Rune word, no theme write. A record that both reserved the band and let an
author tilt labels into it would be making a geometry decision and a surface
decision in one cut, which is what PRODUCT-0064's halt exists to prevent.

The surface record that exposes rotation lands after this one, when the band it
needs already exists.

### 5. The estimator is selected by the label's producer, never by which axis it is on

An earlier draft split this left-versus-bottom. That is wrong: `y_axis_type` is
derived from the Polars dtype exactly as `x_axis_type` is, so a **temporal y
axis** is ordinary — scatter, line, bubble, fill-between and stacked-area all
carry both. Its labels would then fall outside a numeric alphabet on the very
edge the draft assumed was numeric.

Selection follows what **produced** the string:

| producer | alphabet | estimator |
|---|---|---|
| `format_label(value, axis_type)` | digits, sign, decimal point, `e`, `%`, `-`, `:`, `T`, space | formatted-label |
| category names, from data | unbounded | general budget (§2) |

Either producer can appear on either axis. The formatted-label alphabet is
bounded because `format_label` (`common.rs:2439`) enumerates its own output:
fixed-precision decimals, scientific notation, percentages, and the
`%Y-%m-%d`-family temporal forms.

**Correct the existing declaration while adopting it.** The numeric estimator
states its alphabet as "digits, sign, and decimal point" (`legend.rs:341-342`),
but `format_label(Linear)` emits `{:.1e}` above `1e6` — so `e` is already
produced and already undeclared. The mismatch is small and probably still
conservative, but it must not be carried into a new authority: the alphabet
declared here is the one `format_label` actually emits.

### 6. Per-character-class budgeting is the named pre-measurement fallback

If §2's flat budget proves too expensive, the next step is **not** measurement.
It is a per-character-class budget — ASCII bounded near one em, full-width
forms higher, emoji higher still — which stays conservative over a much wider
alphabet at proportionate rather than uniform cost.

It is more machinery than a constant, so it is not the first cut. It is named
here so that the measurement trigger cannot fire while a cheaper option remains
untried.

### 7. The measurement trigger is visible plot loss on the exhibit

PRODUCT-0093 §7's deferral fires when, and only when: a conservative budget
over a declared alphabet — **including §6's per-class refinement** — still
visibly consumes the plot area on Stage 0's exhibit.

Not on the arithmetic, and not on the suspicion that a budget might be
wasteful. On an exhibit, looked at. That is the trigger PRODUCT-0093 wrote and
PRODUCT-0106 §5 preserved, and this record is the first one positioned to fire
it honestly.

### 8. Legibility policy is out of scope

Tick thinning, format abbreviation, and label elision are **not** reservation
fallbacks. A band that cannot hold its labels is a reservation problem; a
figure whose labels are legitimately too numerous or too long to read is a
policy problem, and conflating them would let a layout record decide what an
author sees.

If Stage 0's exhibit shows a correctly reserved band eating the figure, that is
input to a legibility record — and, by §7, possibly to the measurement record.
It is not licence to thin ticks here.

### 9. Realized geometry moves, and this record says by how much

Byte-identical parity is **impossible** and claiming it would be the error
PRODUCT-0106 §6 caught in its own earlier draft. Derived bands will not
coincidentally equal `80.0` and `100.0`.

From the constants, the direction is predictable and opposite on the two axes:
the bottom band's occupants reach about `x_label_padding + x_label_size = 65`
of its `80`, so figures should **gain** plot height; the left band's
conservative tick estimate may exceed the space the current `100` leaves after
the axis title, so figures may **lose** plot width. Both are derived
expectations for Stage 0 to confirm or correct, not measurements.

| what | changes? |
|---|---|
| kernel geometry **formulas** | no |
| realized coordinates in any Cartesian figure | **yes**, by the difference between the derived band and today's constant |
| facet cell **congruence** | no — §1 keeps the derivation grid-wide |
| figures with no axis text | no — the zero case, gated |

## Implementation sequence

**Stage 0 — build the exhibit, then measure.** In order:

1. **The long-categorical fixture**, primary probe: a bar or scatter over
   category names long enough to force the overlap question at a realistic
   panel width, seeded like the existing probes. A date-axis figure is the
   secondary case and needs no new dtype work to be built as a line over an
   existing numeric-encoded column.
2. **Default-rotation capture** of that fixture. This decides whether the
   record repairs a shipped collision or only enables later work, and the
   answer belongs in the record before it is accepted.
3. **Rotated capture** at a representative angle, with rotation set directly in
   settings since no spelling exists, proving the §3 vertical extent.
4. **Parity captures** for every existing Cartesian figure, so §9's movement
   can be pinned per figure rather than asserted in general.

**Stage 1 — band requirements.** §1's layout-derived input: the producer and
longest-possible label per axis, derived from domain and `AxisType` or from the
category set. Single-plot flow moves `kernel.layout(...)` ahead of `plot_area`;
grid flow computes the maximum across cell layouts before `grid_tiled_area`. No
band consumes it yet, so this stage is parity-preserving by construction.

**Stage 2 — the estimator contract.** §2 and §5, with both estimators'
alphabets declared in code and their out-of-alphabet behavior documented at the
definition, as the numeric one already is.

**Stage 3 — the two bands.** §§1, 3, 9 — one authority, every consumer reading
it, no caller subtracting its own.

## Acceptance gates

1. **One authority, grid-wide.** Structural: the below-plot and left-of-plot
   bands are produced once, and no consumer — single plot, `grid_tiled_area`,
   or per-cell plot area — subtracts a band of its own. Per §1, and the reason
   PRODUCT-0106 §4 exists.
2. **Facet congruence survives.** Cells in a grid whose labels differ in length
   remain congruent, because the derivation is grid-wide. The fixture must have
   **unequal label lengths across cells**, or it cannot fail.
3. **The bottom band is the maximum, not the sum.** Per §3, three isolated
   cases: tick term larger, title term larger, and the two equal. Each must
   equal the larger contribution alone. A summing implementation passes any
   single-occupant fixture and any "band grew" assertion, so the
   title-term-larger case is the one that discriminates — it must **not** grow
   when the tick term grows beneath it.
4. **The bottom band tracks rotation.** At a fixed label and size, the band's
   height follows `width·|sin θ| + height·|cos θ|` across at least three
   angles, one of which is `0`. Per §3.
5. **Rotation zero reduces to the font-size term.** The `θ = 0` case equals the
   unrotated computation exactly. This is the case that keeps the formula
   honest today and would go red if the rotated term were added unconditionally.
6. **Either band tracks the longest label, not the label count.** Adding short
   labels does not widen it; lengthening one does. Both halves, since a
   count-driven implementation passes the first alone.
7. **The estimator follows the producer, on both axes.** A figure with a
   **temporal y axis** selects the formatted-label estimator on the left, and a
   figure with long category names on x selects the general budget on the
   bottom — then the mirrored pair: categorical y, temporal x. Per §5, and the
   case an edge-keyed implementation fails.
8. **The formatted-label alphabet matches what `format_label` emits.**
   Structural: the declared alphabet includes `e` (scientific notation above
   `1e6`), `%`, and the temporal separators. Per §5's correction, so the
   existing mismatch is not inherited.
9. **Each estimator declares its alphabet.** Structural, per §2: both
   estimators carry a stated alphabet, budget, and out-of-alphabet behavior at
   their definition. The numeric one already does and must keep it.
10. **Out-of-alphabet labels overflow rather than under-reserve silently.** A
   label outside the declared alphabet is rendered, the band is not secretly
   widened for it, and the behavior matches §2's stated consequence. A gate
   that demanded no overflow would demand a guarantee no fixed advance can make.
11. **No backend measurement capability is added.** Structural: `PlotBackend`
   gains no glyph or text-extent method. PRODUCT-0093 §7's deferral survives
   the record most likely to erode it.
12. **Rotation gains no surface.** Structural, per §4: no Rune word, no theme
    write, no `EditorCommand` for `x_label_rotation`.
13. **A suppressed cell applies no second local inset.** PRODUCT-0048
    suppresses axis text in interior cells while the figure keeps one shared
    outer reservation, so a cell whose local text is suppressed must inset its
    plot area by **nothing** on that edge. This replaces a synthetic "figure
    with no axis text" zero, which is not a production state: single plots
    always draw axis text (`common.rs:1519-1520` falls back to the kernel's own
    label) and grids deliberately retain the shared band for congruence. This
    gate catches the real double-reservation bug instead.
14. **Every Cartesian figure moves by its predicted amount.** Not
    byte-identical, per §9: each Stage 0 parity capture moves by the difference
    between its derived band and today's constant, to the pixel, and the data
    mapping stays proportional — a value at a known fraction of its domain
    lands at the same fraction of the new plot area.
15. **No tick thinning, elision, or format change.** Structural, per §8: label
    content and tick counts are byte-identical to Stage 0 at every band size.
    This is the gate that stops a reservation record from quietly becoming a
    legibility record when the band gets tight.
16. **Requirements precede the band, structurally.** The layout-derived
    requirement is computed before the band in both the single-plot and grid
    flows, and no consumer downstream of the band inspects a label to adjust
    geometry. Per §1, and the only way gate 7 is implementable without a second
    authority.

## Stop conditions

> If a conservative budget over a declared alphabet, **including §6's
> per-class refinement**, still visibly consumes the plot on Stage 0's exhibit,
> stop. PRODUCT-0093 §7's trigger has fired exactly as written, and the next
> record is the backend-measurement decision rather than this one.

> If the per-class refinement is reached for before the flat budget has been
> exhibited, stop. §6 is a fallback with a trigger, not a starting point, and
> adopting it early would spend machinery on a cost nobody has measured.

> If the band derivation has to become per cell to be useful, stop.
> PRODUCT-0048's congruence is the invariant, and a per-cell band is the
> pixel-scale lie that record rejected.

> If reserving the bands requires thinning ticks or shortening labels to look
> acceptable, stop and record the legibility policy separately (§8). A
> reservation that only works when the content is quietly reduced has not
> reserved anything.

> If a band requirement cannot be derived without consulting the realized tick
> set, stop. §1's loop — extent depends on band, band would depend on ticks,
> ticks depend on extent — has closed, and resolving it needs its own decision
> rather than an iteration count buried in a layout function.

> If a fourth occupant is found in either band, stop and widen the survey.
> PRODUCT-0106 found three producers in the top band by one exhibit; this
> record enumerates four across two bands by census, and a fifth would mean the
> occupants are not enumerable by the method used twice now.

## Supersession

Supersedes no record. Completes PRODUCT-0106's repair on the two edges it left
as constants. Unblocks the four roles PRODUCT-0113 §2 declined to expose, for
the surface record after this one. Leaves PRODUCT-0093 §7's deferral standing
unless §7's trigger fires, in which case it fires on evidence.

Changes no geometry **decision** of PRODUCT-0048 while feeding its gutter model
derived values instead of constants; congruence is preserved by §1 and gated by
gate 2.

## Rejected

**Keep the constants and clamp authored sizes to fit.** Attractive because it
needs no estimator and no geometry change. Rejected because it silently
overrides the author, and because the constants are already wrong for long
labels regardless of authoring — the collision exists without anyone setting a
size.

**Reserve from font size only, ignoring width.** Attractive because it is the
top band's proven formula and needs no estimate at all. Rejected because it is
correct only for unrotated horizontal text: y tick labels are width-bound
today, and x tick labels become width-bound the moment rotation exists.

**Measure realized glyph extents.** Attractive because it is exact and would
end the estimate question permanently. Rejected as premature: PRODUCT-0093 §7
deferred it with a trigger, §6 names a cheaper option that has never been
tried, and §7 fires the trigger on an exhibit rather than on the appeal of
exactness.

**Declare one em universally conservative.** Attractive because it sounds safe
and needs no alphabet. Rejected because it is false: full-width CJK, emoji, and
combining marks exceed it, so the claim would be a guarantee the code cannot
keep. An estimate with a declared alphabet is honest; an estimate claiming
universality is not.

**Solve it for time axes.** Attractive because date labels are the obvious long
ones and `AxisType::Date` already exists. Rejected because long category names
need the same band and no new plumbing, and a temporal-keyed solution would
gate this work behind whether a date dtype survives the Toron path.

**Expose rotation in this record.** Attractive because rotation is what makes
long labels usable and the band is being built for it anyway. Rejected per §4:
it would decide geometry and surface in one cut, and the surface is cheap once
the band exists.

**Thin ticks when the band gets tight.** Attractive because it keeps figures
looking clean at any label length. Rejected per §8: it decides for the author
what to display, inside a record whose only job is to reserve space for what
they already asked for.

## Consequences

The plot area stops being whatever the constants happened to leave. Every
Cartesian figure's geometry moves — the third time in this arc, after
PRODUCT-0106 and PRODUCT-0110 — and §9 states the direction rather than
discovering it during review.

Four typography roles become reservable, which is the record after this one.
Rotation becomes exposable, which is the record after that. Neither is
attempted here.

The most valuable output may be Stage 0's exhibit rather than the reservation.
No figure in this repository has ever rendered a long tick label, so the
crowding that motivated the whole track has been reasoned about and never seen.
Building that figure decides whether this record repairs something shipped or
enables something new — and it is the first exhibit positioned to fire
PRODUCT-0093 §7's measurement trigger on evidence, after three records that
carefully preserved it without ever putting it to a test.
