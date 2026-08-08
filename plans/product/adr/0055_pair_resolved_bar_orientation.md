# ADR 0055: pair-resolved bar orientation

Status: accepted 2026-08-08. This record restores horizontal-bar authoring
without adding a second bar word. Rune continues to author one `bar(x, y,
series?)` family; schema-resolved channel roles select its canonical `Bar` or
`HorizontalBar` member. Orientation may change geometry, axes, and guides; it
may not change identity.

## Context

The visual audit after PRODUCT-0054 found the next missing comparison: grouped
bars are now complete in their vertical orientation, while horizontal bars
cannot be reached from Rune at all. The apparent parity is real below the
language boundary. `PlotType::HorizontalBar` is available in the dashboard
picker, `create_bar_kernel` selects `Orientation::Horizontal`, `BarPlotKernel`
returns `CategoricalY` with a numeric x range, and shared rectangle geometry
mirrors grouped slots, `bar_width`, `category_gap`, paint, and hit along y.
Automatic kernel-field projection also maps `Bar | HorizontalBar` to the same
category/group/value contract.

There is no historical Rune spelling to restore. The only production source of
`HorizontalBar` is the dashboard's string-selected plot type. The Rune envelope
recognizes `bar`, `line`, and `scatter`, and `resolve_channels` admits authored
roles only for `Scatter | Bar | Line`. Constructing an authored
`HorizontalBar` with named channels therefore refuses before reaching the
Bar-only `kernel_fields_from_channels` projection. The projection's
`unreachable!` is not today's failure site; it is a boundary that a careless
surface widening could make reachable.

The spelling question is constrained by a second measured fact. Numeric-axis
applicability reads the resolved `Channel::X` and `Channel::Y` scales directly;
it does not inspect the eventual kernel layout. A category-first
`hbar("region", "revenue")` would consequently continue to call `region` x
and `revenue` y while drawing them on the opposite axes. Repairing every guide
consumer to compensate would make the authored channels stop telling the truth.

Rune already says that bar takes x and y strings. The present category-first,
value-second restriction makes that contract coincide with vertical
orientation, but does not erase it. The dense spelling for a horizontal bar is
therefore the same mark with the fields on their intended axes:

```rune
bar("region", "revenue", "product") // categorical x, quantitative y
bar("revenue", "region", "product") // quantitative x, categorical y
```

The accepted-program change is a pure widening, not a reinterpretation:

| resolved X | resolved Y | today | PRODUCT-0055 |
| --- | --- | --- | --- |
| non-quantitative | quantitative | `Bar` | `Bar` |
| quantitative | non-quantitative | refused at X | `HorizontalBar` |
| quantitative | quantitative | refused at X | refused as a pair |
| non-quantitative | non-quantitative | refused at Y | refused as a pair |

Exactly one quadrant changes, and it changes from refusal to acceptance. No
previously accepted Rune bar program can acquire a different orientation or
meaning under this rule. That is stronger than observing unchanged baselines:
the compatibility table proves that the accepted old set is preserved.

The remaining design pressure is identity. `ChannelBinding.composition` and
each `BoundChannel.layer: LayerIdentity` are adjacent but answer different
questions. Composition selects rendering geometry. Layer identity participates
in PRODUCT-0007 §8's color-lineage key. If both canonicalized to
`HorizontalBar`, rotating an otherwise identical series bar would start a new
lineage and could recolor every product solely because the axis carrying it
changed. That contradicts the accepted reason for lineage: colors follow
meaning, not position.

## Decision

### 1. `bar` is one authored family selected by an x/y channel pair

Rune keeps its existing two- or three-field bar spelling in every standalone
position. No `hbar`, `horizontal_bar`, or `.horizontal()` member is added.

After the named x and y fields of a Rune-authored `Bar` resolve from the
returned schema, their semantic roles select orientation:

```text
x category-role compatible, y quantitative -> Bar
x quantitative, y category-role compatible -> HorizontalBar
otherwise                                  -> attributed refusal
```

“Category-role compatible” preserves Bar's accepted compatibility boundary:
the field is non-quantitative. This record mirrors that existing rule onto y;
it does not narrow temporal acceptance, revise scale compatibility, or inspect
values. Exactly one axis must supply the quantitative value role. Two
quantitative axes and two non-quantitative axes refuse the complete sink rather
than guessing an orientation.

Once both fields resolve individually but form an invalid pair, the refusal
moves deliberately from the legacy first-incompatible channel to the pair at
the complete sink span. Both invalid quadrants use this exact reason template:

```text
bar requires exactly one quantitative value axis and one non-quantitative
category axis; x `<x-field>` resolved as <X-semantic>, y `<y-field>` resolved
as <Y-semantic>
```

Missing fields, unsupported dtypes, incompatible explicit scales, and other
failures knowable from one channel retain their existing channel attribution.
Only the two semantically valid channels whose combination cannot orient a bar
use the pair refusal. Existing tests that expected X blame for two quantitative
fields or Y blame for two non-quantitative fields are premise updates to this
named difference class, not evidence that accepted behavior changed.

The decision uses schema-derived semantic types only. PRODUCT-0007 §2 already
allows dtype-derived semantic typing and forbids value inspection and silent
substitution; both constraints remain. Named fields still resolve exactly.

### 2. Plot type becomes a channel-resolution output for the bar family

Every Rune `bar(...)` enters resolution with authored composition `Bar`. Once
both physical channels have resolved, channel resolution selects the canonical
member and writes it into `ChannelBinding.composition`:

```text
authored family Bar + vertical pair   -> resolved composition Bar
authored family Bar + horizontal pair -> resolved composition HorizontalBar
```

Domain resolution carries that composition unchanged into `ResolvedPlotSpec`,
and the factory consumes the resolved member through the existing orientation
branch. Dashboard-created horizontal bars and Rune-created horizontal bars thus
converge on one rendering representation instead of retaining `Bar` plus a
second orientation flag.

This is the first Rune mark whose concrete `PlotType` is a resolution output.
The same source text may therefore change between vertical and horizontal after
a live result's schema changes, while retaining its view identity. A schema
whose pair no longer has exactly one quantitative role refuses visibly; the
resolver never uses values or argument names to preserve the previous
orientation.

Canonical output does not create authoring authority. An
`AuthoredPlotSpec` whose composition is directly `HorizontalBar` and whose
channels are `Named` remains refused by the authored-role gate. The only Rune
constructor word is `bar`.

The phase boundary governs the existing Bar-only resolver decisions. The
authored-role allowlist continues to admit `Bar`, not `HorizontalBar`; a named
Rune Bar's x and y channels admit the two candidate semantic roles until the
pair is known;
nominal or ordinal category roles select the existing band default on either
physical axis; kernel-field projection maps category and value from the chosen
pair; and categorical capacity remains a property of the authored bar family.
Only after those family rules produce one valid pair does composition
canonicalize to the resolved member. Mechanically widening every Bar-only match
to `Bar | HorizontalBar` would cross the boundary and fails §2's negative gate.

Automatic dashboard specifications do not enter this pair-resolution rule.
Their selected `Bar` or `HorizontalBar` member remains authoritative, including
the dashboard's existing schema-order inference. This is not merely a current
implementation convenience: globally widening Bar's per-channel compatibility
would let a numeric-first automatic `Bar` rotate despite the dashboard having
selected the vertical member.

### 3. X and Y remain physical axes; kernel roles follow them

Resolved channels continue to mean the axes they name. For the vertical member,
the kernel projection reads category from X and value from Y. For the horizontal
member, it reads value from X and category from Y. The optional third field is
the same color/series channel in both orientations.

Consequently existing applicability logic requires no orientation exception:

- a horizontal bar accepts `x_bounds`, `x_tick_step`, and `x_minor_ticks` when
  their existing numeric requirements hold;
- it refuses the corresponding y settings because y resolves as the category
  axis;
- a vertical bar retains the exact inverse behavior.

Axis labels, categorical positions, numeric domains, and the kernel's
`CategoricalX`/`CategoricalY` layouts all derive from that same resolved pair.
No consumer reconstructs or swaps axis meaning later.

### 4. Layer identity retains the authored family

`BoundChannel.layer` remains `LayerIdentity { ordinal, plot_type: Bar }` for
both resolved orientations. `ChannelBinding.composition` may become
`HorizontalBar`; layer identity may not.

This is an intentional phase boundary, not two competing representations of
geometry:

```text
composition    = resolved rendering member
layer identity = authored semantic family and ordinal
```

PRODUCT-0007 §8 keys categorical continuation on resolved field, layer
identity, and category value. Since rotation keeps the color field, category
values, and authored family unchanged, every series retains its exact palette
position and resolved color. Changing the series field still starts a new
lineage, as PRODUCT-0007 requires.

Orientation may change geometry, axes, and guides; it may not change identity.

### 5. Labels name authorship; descriptions name resolved geometry

The view's stable display label remains `plot.bar` in both orientations because
`RuneViewForm::Bar` names the authored family before resolution. The resolved
composition's generated description is `Horizontal Bar Plot` for the horizontal
member because PRODUCT-0013 §6 derives description from ordered composition.

Those exact strings disagree by design. The label records what the author
wrote; the composition description records what the schema resolved and the
renderer will draw. Gates pin each string independently with assertion messages
carrying that reason. A bare inequality assertion is insufficient: any two
wrong strings would satisfy it and make the intentional divergence resemble an
unexplained defect.

### 6. Both orientations share the accepted bar behavior

The horizontal member inherits the bar mark's complete standalone register:
`.bar_width(...)` and `.category_gap(...)` apply in either order to two- and
three-field forms. Grouping, resolved palette identity, named swatch legends,
faceting, category capacity, hover prose, and the one-rectangle paint/hit rule
remain the decisions of PRODUCTS-0021, 0029, 0044, 0046, 0047, and 0053.

`bar(...)` remains unavailable inside `layers([...])`. Dashboard stacked bars,
including `HorizontalStackedBar`, are a different kernel and ordering model and
remain unauthorable from Rune.

## Stage 0 prerequisite

Before the pair rule is admitted through the production Rune completion path,
drive a three-field authored `Bar` with quantitative X, non-quantitative Y, and
a categorical series through real channel and domain resolution, then construct
the resulting kernel from its canonical `HorizontalBar` composition. Constructing
`BarPlotKernel { orientation: Horizontal }` directly does not discharge this
prerequisite because it skips the path whose exclusions this record changes.

At two substantially different plot extents, the resolved horizontal kernel
must establish all of the following before surface completion proceeds:

- exact resolved series colors reach paint and legend;
- `bar_width` and `category_gap` compose with their accepted meanings;
- each grouped cluster centroid equals `transform.categorical` on y;
- paint and hit consume the same rectangles, and a cursor in category reserve
  misses;
- category labels and reversed group order remain correct;
- horizontal hover derives its approximate value from the numeric `x_range`
  and cursor x position, with the asserted value matching that projection.

PRODUCT-0053 already tests the shared geometry by manually constructing both
orientations. This prerequisite is intentionally stronger: it proves the
horizontal branch through the new resolution route. If that route reveals a
violation of an accepted bar invariant, implementation stops and the owning
record is amended or superseded before 0055 exposes it.

## Acceptance gates

1. Legacy `plot.bar`, relation sugar, canonical `views([bar(...)])`, named-view,
   figure, two-field, and three-field positions retain one authored `Bar`
   family and their existing strict-sugar equivalence.
2. The semantic-pair matrix is behavioral: category-role X plus quantitative Y
   resolves `Bar`; quantitative X plus category-role Y resolves
   `HorizontalBar`; two quantitative or two non-quantitative fields refuse the
   complete sink at its attributed span with §1's exact pair-reason template.
   Existing single-channel failures retain their attribution, and named fields
   never substitute.
3. The same `bar("a", "b", "series")` source resolves to the appropriate
   canonical member under two schemas with opposite compatible roles. A live
   schema change rotates the retained view; an incompatible schema fails it
   visibly instead of retaining stale orientation.
4. A horizontal binding carries physical X as the quantitative value channel,
   physical Y as the category channel, and projects
   `ResolvedKernelFields::Bar { category: y, value: x, group: series }`.
   Its `ChannelBinding.composition` and completed `ResolvedPlotSpec.composition`
   both name `HorizontalBar`.
5. A negative architecture-boundary gate directly constructs an
   `AuthoredPlotSpec` composed as `HorizontalBar` with named x/y channels and
   observes refusal. The dashboard's existing automatic `HorizontalBar` path
   still resolves and renders, while a numeric-first automatic `Bar` remains
   vertical, proving that canonical output did not become a second Rune
   constructor or override dashboard selection. Source inspection does not
   discharge either half.
6. Through production Rune completion, horizontal `x_bounds`, `x_tick_step`,
   and `x_minor_ticks` apply to the numeric axis while their y counterparts
   refuse with the existing category-scale attribution. The vertical control
   preserves the inverse matrix.
7. A grouped horizontal bar driven through real channel and domain resolution
   satisfies the Stage 0 prerequisite: resolved colors, legend samples,
   `bar_width`, `category_gap`, categorical centroids, and shared paint/hit
   rectangles hold at both tested extents. Hover at a known rectangle position
   reports the value projected from `CategoricalY.x_range` and cursor x, not
   from the categorical y coordinate.
8. Rotating an otherwise identical three-field bar preserves every series'
   exact palette position and RGBA assignment. Both orientations retain
   `LayerIdentity { ordinal: 0, plot_type: Bar }`; changing only the series
   field starts a new lineage and provides the control.
9. The horizontal view pins `display_label == "plot.bar"` with the message
   “label names authored family,” and pins its resolved composition description
   as `"Horizontal Bar Plot"` with the message “composition names resolved
   geometry.” Neither a bare `assert_ne!` nor source inspection discharges the
   gate.
10. Two- and three-field horizontal bars inherit `.bar_width(...)` and
    `.category_gap(...)` directly and in either order. Cross-register,
    duplicate, invalid-value, nonliteral, wrong-arity, and `layers([...])`
    forms refuse atomically at the existing boundaries.
11. Faceted grouped horizontal bars retain panel-blind series colors, one
    figure-level named-swatch legend, congruent cells, shared category domains,
    and unchanged hover prose. Horizontal stacked bars remain unchanged and
    unreachable from Rune.
12. Every existing vertical bar image, SVG, draw-operation, hit, axis,
    identity, and dashboard baseline passes byte-identically. No baseline is
    refreshed. The semantic-pair table proves additionally that every
    previously accepted Rune bar retains its meaning; only one formerly refused
    quadrant becomes accepted. Refusal-message premises for the two still-
    invalid quadrants may refresh only to §1's named pair-attribution class.
    `../uat/test19.rn` supplies the new grouped horizontal exhibit using the same
    data and series identities as the vertical control.

## Implementation evidence

- Pair resolution is confined to a Rune-authored `Bar` with two named physical
  channels. The Stage 0 implementation initially exposed why that qualifier is
  load-bearing: widening Bar's per-channel compatibility globally would also
  reinterpret automatic dashboard specifications. Behavioral controls now
  prove that the dashboard's selected `HorizontalBar` still resolves and that a
  numeric-first automatic `Bar` remains vertical.
- The complete semantic matrix is exercised directly. Category X/value Y
  resolves `Bar`; value X/category Y resolves `HorizontalBar`; both invalid
  pairs refuse with §1's exact field-and-semantic reason. Existing one-channel
  failures retain their earlier attribution because the relaxed compatibility
  applies only while resolving a complete named Bar pair.
- Canonicalization changes `ChannelBinding.composition` and the completed
  `ResolvedPlotSpec` to `HorizontalBar`, then projects value from X, category
  from Y, and the optional group from color. Every `BoundChannel` retains
  `LayerIdentity { ordinal: 0, plot_type: Bar }`. Rotation therefore preserves
  exact series assignments, while changing the series field supplies the
  required fresh-lineage control.
- Production Rune completion reaches the horizontal member through legacy,
  relation-sugar, canonical, named-view, figure, two-field, and three-field
  spellings. Both `.bar_width(...)` and `.category_gap(...)` reach the styled
  horizontal factory in either order. The directly-authored named
  `HorizontalBar` control refuses.
- The Stage 0 renderer gate drives a grouped horizontal plot through real
  channel resolution, domain resolution, and factory construction at 100px and
  300px plot heights. Exact resolved colors reach paint and legend, grouped
  centroids equal the categorical transform, category reserves miss hit tests,
  paint and hit share rectangles, and hover derives its asserted value from
  `CategoricalY.x_range` and cursor x.
- Production completion pins the physical-axis matrix: x bounds, major step,
  and minor ticks apply to the horizontal numeric axis; corresponding y
  settings refuse as categorical. It separately pins `display_label ==
  "plot.bar"` and the resolved composition title `"Horizontal Bar Plot"`, with
  messages documenting their intentional family/geometry divergence.
- A real 2x2 faceted grouped horizontal grid retains shared categorical
  domains, panel-blind resolved colors, congruent cell layouts, and one unioned
  named-swatch legend.
- Eight PRODUCT-0055 behavior tests cover the strict widening, negative
  authoring boundary, dashboard controls, lineage, surface spellings and
  modifiers, physical axes and descriptions, native horizontal geometry, and
  faceted union. No SVG, image, draw-operation, expected-byte, or refusal
  baseline was refreshed.
- `crates/polariton/README.md` documents physical-axis ordering and horizontal
  controls. `../uat/test19.rn` renders the grouped horizontal `demo_sales`
  exhibit with Vapor colors, category clustering, and inherited gutter legend.
- `cargo test --workspace --all-targets --no-fail-fast` and `cargo check
  --workspace --all-targets` pass. The Polariton portion is 467 unit tests
  passed (five live tests ignored), 41 architecture tests, and the trybuild UI
  pair.

## Stop condition

> This record may make bar orientation a schema-resolved channel-pair decision,
> canonicalize the completed composition to `Bar` or `HorizontalBar`, map
> physical channels into the existing bar kernel roles, retain authored-family
> layer identity and display label, resite only the two invalid semantic pairs
> to §1's named complete-sink refusal, and add the named gates and UAT fixture. If
> implementation requires value inspection, a new constructor or modifier,
> changing accepted vertical pixels or temporal compatibility, changing bar
> geometry, recoloring on rotation, admitting bars inside `layers`, exposing
> stacked bars, or changing existing hover, legend, facet, or capacity policy,
> work stops and the requirement returns to its owning record.

## Rejected

- **`hbar(...)` or `horizontal_bar(...)`:** orientation is already stated by
  which physical axis carries the category role. A second constructor would
  give one bar family two authored words and require category-first arguments
  either to lie about X/Y or to duplicate the same axis information.
- **`.horizontal()`:** orientation changes channel roles, numeric-axis
  applicability, and resolved geometry. It is not a presentation value in the
  postfix bar register.
- **Keep resolved composition as `Bar` and add an orientation fact:** dashboard
  and Rune would carry two representations of the same rendering geometry. The
  existing canonical `HorizontalBar` member is the one resolved output.
- **Admit authored `HorizontalBar` channels internally:** this converts a
  renderer member into an unreviewed second authoring API and makes the
  Bar-only projection's current `unreachable!` reachable.
- **Infer orientation from field names or values:** names are not semantic
  types, and value inspection contradicts PRODUCT-0007 §2's schema boundary.
- **Key color lineage on the resolved member:** rotating an unchanged series
  field would recolor identities for a presentation-only change.
- **Unify `plot.bar` and `Horizontal Bar Plot`:** one is authored-family
  identity and the other is resolved-geometry description. Making either own
  both facts destroys the phase boundary.
- **Expose `HorizontalStackedBar` in the same cut:** stacking is different
  geometry with independent ordering, normalization, and interaction questions.

## Consequences

- Rune regains horizontal-bar capability without growing its mark vocabulary:
  scatter, bar, and line remain the three authored standalone mark words.
- Plot type is now a resolution output for one mark family. The same authored
  bar can rotate after a schema change, but only when the new schema forms the
  opposite valid pair; invalid pairs fail instead of guessing.
- The rule is a strict accepted-set widening: every formerly accepted bar keeps
  its meaning, one formerly refused quadrant becomes horizontal, and the two
  still-invalid quadrants acquire the named pair-level refusal because neither
  channel is individually at fault.
- The completed artifact has one rendering representation:
  `ResolvedPlotSpec.composition` names `HorizontalBar`, and the existing factory
  selects the horizontal kernel from it.
- Color lineage and panel labeling deliberately stay family-keyed. Rotation
  changes neither series colors nor the stable `plot.bar` label.
- The known `plot.bar` / `Horizontal Bar Plot` divergence is intentional and
  gated: the label names authored intent while the composition description
  names resolved geometry. A future consistency audit must preserve, not
  reconcile, it.
- Existing vertical figures change no pixels. The new UAT exhibit exercises
  grouped colors, category clustering, and horizontal axis applicability through
  the same Rune word.
