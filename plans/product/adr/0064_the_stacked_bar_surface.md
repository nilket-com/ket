# ADR 0064: the stacked bar surface

Status: proposed 2026-08-09. This record gives Rune the stacked bar. It adds one
modifier, `.stacked()`, which selects a resolved composition member rather than
authoring a new mark family. Orientation continues to resolve from the channel
pair, so one word and one modifier reach all four bar members. It also repairs
four substrate defects in the stacked kernel: two that the surface makes
reachable, and two that were already reachable and merely unexamined. It adds
no mark word, no channel, no percentage semantics, and no persistence schema
change.

## Context

The stacked bar is the last bar form the language cannot say. Its substrate is
complete: `StackedBarPlotKernel { prepared_data, orientation }`
(`plot/kernels/stacked_bar.rs:11`) is orientation-parameterized exactly like the
grouped kernel, the factory builds both members from
`ResolvedKernelFields::StackedBar { category, group, value }`
(`plot/factory.rs:92-105`), the automatic projection already maps
`StackedBar | HorizontalStackedBar` (`plot/spec/mod.rs:1846`), and the dashboard
picker reaches both (`gui/state.rs:3024,3032`). Only Rune cannot reach them.

That is the PRODUCT-0055 situation repeated, with one decisive difference.
Orientation could be *inferred* there because the authored x/y pair genuinely
differs between a vertical and a horizontal bar. Nothing differs here:

```text
bar(category, value, series)   grouped   (non-quantitative, quantitative, categorical)
bar(category, value, series)   stacked   (non-quantitative, quantitative, categorical)
```

Grouped and stacked have identical channel signatures. No schema fact
distinguishes them, so stacking cannot be resolved and must be spelled. This
record therefore adds a word where PRODUCT-0055 deliberately refused to — not
as a preference, but because inference is unavailable.

The spelling is a modifier rather than a mark word, and the reason is durable
data rather than elegance. `DurableLayer::from_runtime`
(`session_persistence.rs:665-673`) maps runtime plot types to PRODUCT-0062's
durable authored family:

```rust
PlotType::Scatter => AuthoredFamily::Scatter,
PlotType::Bar | PlotType::HorizontalBar => AuthoredFamily::Bar,
PlotType::Line => AuthoredFamily::Line,
other => return Err(... "plot type {other} has no durable authored family"),
```

Unknown plot types **refuse**. A `stacked_bar` mark word would require a fourth
`AuthoredFamily` variant — a schema change to a format PRODUCT-0062 shipped and
PRODUCT-0063 made reachable, carrying a version and migration decision. A
stacked *member* under the existing `Bar` family requires no change at all.

Stage 0 found four substrate defects in that kernel, of two distinct kinds.

Two are **unreachable today** — no gate can see them because nothing authorable
produces a stacked bar, so the kernel is never handed resolved colours or
authored geometry to honour:

- it reconstructs colours by ramp index — `settings.color_theme.get_color(t)`
  at `stacked_bar.rs:85` and `:122` for marks, and `get_color(group_index as f32)`
  at `:517` for the legend — instead of consuming resolved
  `CategoryColorAssignment`s;
- it hardcodes its band fraction at `* 0.8` in four places
  (`stacked_bar.rs:62,98,145,188`), ignoring `bar_width` and `category_gap`
  entirely.

Two are **already reachable** from the dashboard and simply unexamined. Any user
who sets bounds or supplies negative data meets them now:

- paint originates at the resolved lower bound (`stacked_bar.rs:66`) while hover
  accumulates from zero (`:156`), so a nonzero lower bound desynchronizes the
  rectangles from the values reported over them;
- non-positive values are treated three ways at once — included in the summed
  domain, skipped without advancing in paint (`:69`), and accumulated through in
  hover (`:169`) — so the axis, the rectangles, and the hover text describe
  three different stacks.

The distinction matters for what each teaches. The first pair is the cost of a
capability existing without a consumer: widening the surface is what makes them
findable. The second pair was findable all along and was found by reading, which
is the more sobering half.

The colour bypass is also the class that PRODUCT-0054 declared closed
at three instances. That closure was correct *as scoped*: it was bounded by the
authored-channel projection, and stacked bars were not authorable, so they held
no resolved colours to bypass and exhibited no defect. Widening the surface
creates the fourth instance. The closure claim needs re-scoping rather than
quiet extension.

Finally, stack direction. PRODUCT-0056 established that categorical
display-domain order equals visual reading order, and repaired grouped
horizontal bars whose legend ran backwards against their marks. That invariant
governs the *categorical* dimension. Stacking runs along the *value* dimension,
which already grows away from zero in both orientations without anyone finding
it inconsistent — a horizontal bar grows rightward from `x = 0` and a vertical
bar grows upward from `y = 0`. Forcing top-down agreement would require
orientation-specific legend order, which is precisely the asymmetry
PRODUCT-0056 exists to prevent.

## Decision

### 1. `.stacked()` selects a member; it does not author a family

`.stacked()` joins the bar mark's postfix modifier register beside `bar_width`
and `category_gap`. It takes no arguments. It marks the authored bar for stacked
composition; it does not name a plot type, change a channel role, or add an
encoding.

```rune
bar("region", "revenue", "product").stacked()
```

The authored composition remains the `Bar` family throughout lowering and
channel resolution. Only after PRODUCT-0055's pair rule has selected an
orientation does the presence of the modifier select the stacked member:

```text
authored Bar + vertical pair   + .stacked()  -> StackedBar
authored Bar + horizontal pair + .stacked()  -> HorizontalStackedBar
authored Bar + vertical pair                 -> Bar
authored Bar + horizontal pair               -> HorizontalBar
```

Every family rule PRODUCT-0055 §2 pins continues to run first and continues to
match on `Bar`: the authored-role allowlist (`plot/spec/mod.rs:1320`), the band
default for nominal and ordinal category roles, and the categorical capacity
property (`:2167`). Mechanically widening those matches to include the stacked
members would cross the same phase boundary PRODUCT-0055 §2 forbids.

Canonical output does not create authoring authority. An `AuthoredPlotSpec`
composed directly as `StackedBar` or `HorizontalStackedBar` with `Named`
channels remains refused by the authored-role gate. `bar(...).stacked()` is the
only Rune route, which keeps the family/member distinction structural rather
than conventional.

### 2. Orientation remains pair-resolved; four members from one word

The modifier composes with PRODUCT-0055's pair rule without qualification:

```rune
bar("region", "revenue", "product")             // Bar
bar("revenue", "region", "product")             // HorizontalBar
bar("region", "revenue", "product").stacked()   // StackedBar
bar("revenue", "region", "product").stacked()   // HorizontalStackedBar
```

Pair validity is decided exactly as PRODUCT-0055 §1 decides it, before stacking
is considered. A pair that refuses ungrouped refuses stacked, with the same
complete-sink reason naming the resolved fields and their semantic types.
`.stacked()` never rescues an invalid pair and never changes which axis carries
the category.

### 3. Layer identity remains the authored Bar family

`BoundChannel.layer` keeps `LayerIdentity { ordinal, plot_type: Bar }` for all
four members. Composition may become `StackedBar`; layer identity may not.

This is PRODUCT-0055 §4's split applied to a second geometry choice:
composition records what the renderer draws, layer identity records semantic
lineage. Because PRODUCT-0007 §8 keys categorical continuation on resolved
field, layer identity, and category value, **toggling `.stacked()` preserves
every series colour exactly**, by the same argument that makes rotation
colour-stable. A stacked bar and a grouped bar of the same series are the same
data wearing different geometry.

The durable consequence is that `session_persistence.rs` requires no change.
`DurableLayer::from_runtime` continues to see `PlotType::Bar`, the durable
family vocabulary remains `"scatter" | "bar" | "line"`, and every colour payload
written before this record stays valid.

As in PRODUCT-0055 §5, the view's stable `display_label` remains `plot.bar`
while the resolved composition description becomes `Stacked Bar Plot` or
`Horizontal Stacked Bar Plot` (`plot/core.rs:63,71`). Those strings disagree by
design: the label names authored intent, the description names resolved
geometry.

### 4. Stacked marks consume resolved colours, and 0054's closure is re-scoped

The stacked kernel gains both resolved-assignment lookups the grouped kernel
already has. A three-field mark and its legend read the resolved
`CategoryColorAssignment` for each segment's series name; a two-field mark
reads the resolved `BandFillAssignment` for its category. The legacy ramp
survives only as the no-assignment fallback for automatic dashboard plots.
Both three-field bypass sites are repaired together — a fix that corrected
marks but left `legend_content` sampling `get_color(group_index as f32)` would
reproduce PRODUCT-0047's teal-swatch-versus-yellow-fill defect exactly. The
band-fill route is equally load-bearing: without it `.stacked()` would turn a
palette-coloured two-field bar back into the legacy ramp despite changing only
geometry.

PRODUCT-0054's closure is superseded in scope, not in substance:

> Resolved-colour bypasses are closed for every currently authorable mark. The
> class is bounded by which marks are authorable, not by a count. Any future
> record that makes a new renderer reachable from Rune must prove that renderer
> consumes resolved assignments.

Stating the bound this way makes the obligation inheritable. "We closed that
class" is otherwise the sentence that makes the next reviewer skip the check.

### 5. Bar geometry modifiers apply

`bar_width` and `category_gap` apply to stacked bars with their accepted
PRODUCT-0021 and PRODUCT-0053 meanings. The hardcoded `* 0.8` at
`stacked_bar.rs:62,98,145,188` is replaced by the resolved fractions, so
`bar_width(0.8)` reproduces today's appearance exactly and any other value is
finally honoured.

The stacked geometry has one band per category rather than a cluster of slots,
so `bar_width` scales the single band and `category_gap` reserves space between
categories. Both remain order-irrelevant, refuse duplicates, and refuse
cross-register members, exactly as they do ungrouped.

### 6. Legend order equals stack order from the baseline outward

Every stack originates at **data zero**, not at the resolved lower bound. Legend
entry zero identifies the segment adjacent to that origin, and stacking proceeds
outward in resolved display-domain order:

```text
vertical    first series bottommost, stacking upward from y = 0
horizontal  first series leftmost,  stacking rightward from x = 0
```

Today the two consumers disagree: paint begins at `y_range.0`
(`stacked_bar.rs:66`) while hover accumulates from `0.0` (`:156`), so any
authored `y_bounds` whose lower bound is nonzero desynchronizes the rectangles
from the values reported over them. True zero is the correct origin because a
stacked bar's segments are cumulative *from zero* by construction; originating
at the resolved lower bound would place the top of a stack at
`lower + sum` rather than `sum`, which misstates every magnitude on the axis.

When zero lies outside the authored domain the stack clips naturally, exactly as
any other mark does. Clipping is honest: an author who cuts the value axis has
cut the picture, and the record does not compensate by moving the origin.

This is the value-axis analogue of PRODUCT-0056's categorical rule, not an
exception to it. PRODUCT-0056 governs which band sits where along the category
dimension; stacking runs along value, where both orientations already grow away
from zero. A top-down rule would make legend order depend on orientation, which
PRODUCT-0056 rejected for grouped bars and is no more defensible here.

PRODUCT-0056 §5's clause that horizontal stacked segments keep their existing
stack order is preserved and now has a stated reason rather than being an
untouched inheritance.

### 7. The two-field form is the degenerate one-segment case

PRODUCT-0046 §1 states that a register belongs to the mark, so every encoding
form of that mark admits it. `.stacked()` is therefore admissible on the
two-field bar:

```rune
bar("region", "revenue").stacked()
```

PRODUCT-0047 found that a two-field bar already travels the grouped path at
`N = 1` with `group_names` defaulting to `vec!["Value"]`. The stacked
degenerate case is the same shape: one segment spanning the whole bar. It has a
defined answer, so under the rope rule it renders rather than refuses, and it
carries no legend under PRODUCT-0044 §5's band-fill rule.

Refusing a modifier that the mark's own register admits would contradict
PRODUCT-0046 §1 directly. Rendering a bar identical to its unstacked form is
valid-but-pointless, which the arc has consistently allowed.

### 8. Inherited data semantics are stated, not repaired

`prepare_stacked_bar_data` is first-row-wins per category and series pair. This
record does not change that. Aggregation is relation-stage work: an author who
wants sums writes them in the relation, exactly as PRODUCT-0056 rejected
implicit sorting for the same reason — mark geometry may not compute over data.

Non-positive values are a different matter, because "not drawn" understates the
situation. Three consumers currently disagree about what the stack is:

```text
domain      preparation includes negatives in the summed range
rectangles  paint skips `val <= 0.0` without advancing (stacked_bar.rs:69)
hover       accumulates every value including negatives  (stacked_bar.rs:169)
```

A gate asserting only that negatives are not painted would pass while the axis,
the rectangles, and the reported values describe three different stacks. This
record therefore requires **one canonical positive-segment sequence** per
category, produced once and consumed by the domain, the rectangles, hit
testing, and hover alike. Skipping a segment must skip it everywhere or nowhere.

The mechanism is the one the grouped kernel already uses: a shared rectangle
producer, as `bar_rectangles` serves both paint and hit in `bar.rs`. That closes
this alongside the paint/hit agreement gate rather than as separate work, and it
makes the positive-only rule structural instead of three coincidentally similar
loops.

Both properties are pinned by tests so they are known rather than accidental,
and both are why percentage-of-total is a separate record: a denominator over
first-row-wins values, and a percentage of a total that ignores negatives, are
decisions this record has no exhibit for.

The value axis carries the **sum** of a category's segments. `y_bounds`,
`y_tick_step`, and `y_minor_ticks` apply to that summed axis with their existing
numeric requirements, and their x counterparts apply in the horizontal member,
per PRODUCT-0055 §3. Nothing about stacking changes which axis is numeric.

## Implementation sequence

1. Add `.stacked()` to the bar modifier register in the envelope beside
   `bar_width` (`rune_relation_envelope.rs:2593`), with the same
   at-most-once, order-irrelevant, cross-register-refusal treatment. Prove
   lowering equality and every refusal before any resolution change.
2. Repair the substrate defects **first and separately**, against the
   dashboard-reachable path that already exists: introduce the shared canonical
   segment producer with a true-zero origin, route resolved assignments into
   paint and legend, and replace the four hardcoded `0.8` fractions with the
   resolved settings. Two oracles are required together, because neither alone
   is sufficient: `bar_width(0.8)`, zero-based bounds, and no assignments must
   reproduce today's output byte-for-byte, **and** a direct kernel/factory test
   with synthetic `CategoryColorAssignment`s and `BandFillAssignment`s must
   prove both resolved branches. The byte oracle exercises only the unchanged
   fallback; without its companions the resolved paths would stay untested
   until after surface exposure.
3. Thread the modifier through resolution so composition canonicalizes to the
   stacked member after the pair rule, leaving the authored-role allowlist,
   band default, and capacity property matching on `Bar`.
4. Add the identity, persistence, correspondence, and degenerate-form gates.
5. Add `../uat/test20.rn` as the live exhibit and update the corpus and
   language-as-built.

Step 2 must be independently green. A colour or geometry error found after the
surface exists is indistinguishable from a surface defect, and the repair is to
code the dashboard already reaches.

## Acceptance gates

1. **The modifier obeys the register.** `.stacked()` is admissible on two- and
   three-field bars in every accepted standalone position, composes with
   `bar_width` and `category_gap` in either order, refuses when repeated,
   refuses with an argument, refuses on `line` and `scatter`, and refuses inside
   `layers([...])`. Refusals are atomic and span-attributed at the existing
   boundaries.

2. **Step 2's repair is byte-identical on the fallback and proven on the
   resolved branch.** Before any surface exists, the repaired stacked kernel
   under `bar_width(0.8)`, `category_gap(0.0)`, zero-based bounds, and no
   resolved assignments reproduces every existing dashboard stacked-bar image,
   SVG, and draw-operation baseline exactly; no baseline is refreshed. A
   separate direct kernel/factory tests supply synthetic
   `CategoryColorAssignment`s and `BandFillAssignment`s and prove paint and
   legend consume the applicable resolved artifact.
   The byte oracle alone does not discharge this gate — it exercises only the
   unchanged ramp fallback.

3. **Four members from one word.** The pair matrix crossed with the modifier
   yields `Bar`, `HorizontalBar`, `StackedBar`, and `HorizontalStackedBar` in
   `ChannelBinding.composition` and the completed `ResolvedPlotSpec`. An invalid
   pair refuses identically with and without `.stacked()`, naming the resolved
   fields and their semantic types.

4. **Identity does not move.** All four members carry
   `LayerIdentity { ordinal: 0, plot_type: Bar }`. Adding or removing
   `.stacked()` on an otherwise identical three-field bar preserves every
   series' exact palette position and RGBA. Changing the series field starts a
   fresh lineage and supplies the control.

5. **Persistence is untouched.** A colour-bearing stacked figure checkpoints
   and restores through PRODUCT-0062 with no change to
   `session_persistence.rs`: `DurableLayer::from_runtime` observes
   `PlotType::Bar`, the durable family remains `"bar"`, and a payload written
   from a grouped figure restores onto a stacked one with identical colours.
   A synthetic `AuthoredFamily` addition is shown to be unnecessary.

6. **Resolved colours reach both paint and legend.** A stacked bar under
   `palette("blues")` renders blues in its segments *and* in its named swatches.
   A two-field stacked bar consumes the same exact category band fills as its
   grouped twin.
   A control proves the legacy ramp path still serves an automatic dashboard
   stacked bar with no assignments. Marks-only and legend-only repairs each fail
   the gate.

7. **Geometry modifiers apply.** `bar_width` and `category_gap` change stacked
   band geometry with their accepted equations at two substantially different
   plot extents. Paint and hit consume the same rectangles, and a cursor in
   category reserve misses.

8. **Baseline-outward correspondence is joint.** With asymmetric per-series
   values, the segment adjacent to the zero baseline carries legend entry zero's
   exact colour and value, and subsequent entries proceed outward in display
   order — bottom-to-top vertically, left-to-right horizontally. Independent
   legend and segment assertions do not discharge this gate; the fixture uses
   distinct values so a reversal is detectable.

9. **The degenerate form renders.** `bar("region", "revenue").stacked()` draws
   one segment spanning each bar, carries no legend, and matches the unstacked
   two-field bar's geometry and exact resolved band-fill colour under identical
   modifiers. It does not refuse.

10. **The negative boundary holds.** Directly constructing an
    `AuthoredPlotSpec` composed as `StackedBar` or `HorizontalStackedBar` with
    named channels refuses. The dashboard's automatic stacked path still
    resolves and renders. Source inspection discharges neither half.

11. **Label and description disagree by design.** A stacked view pins
    `display_label == "plot.bar"` and its resolved composition description as
    `"Stacked Bar Plot"` (and `"Horizontal Stacked Bar Plot"` rotated), each
    with a message naming why. A bare inequality assertion does not discharge
    it.

12. **Facets and axes inherit unchanged.** Faceted stacked bars retain
    panel-blind series colours, one figure-level legend, congruent cells, and
    shared domains. `y_bounds`/`y_tick_step`/`y_minor_ticks` apply to the summed
    value axis and their category-axis counterparts refuse, with the inverse in
    the horizontal member.

13. **One canonical segment sequence; first-row-wins is inherited.** With a
    category containing a negative and a zero among positives, the summed axis
    domain, the rendered rectangles, the hit regions, and the hover text all
    describe the **same** positive-segment sequence: hover over each rectangle
    reports the value and running total its geometry implies, and the axis
    domain derives from the painted stack's top while preserving the inherited
    10% headroom (`upper == painted_top * 1.1`). Asserting only that negatives
    go unpainted does not discharge this gate. Separately, first-row-wins for a
    duplicated category/series pair is asserted with a message naming it as
    inherited and relation-stage, not a defect of this record.

14. **Stacks originate at zero under authored bounds.** With
    `y_bounds(20, 150)` over data summing below and above 20, the rectangles
    and the hover values agree, and the stack clips at the domain edge rather
    than being rebased to the lower bound. The pre-repair behaviour — paint
    from `y_range.0`, hover from `0.0` — is asserted absent. The horizontal
    member repeats it with `x_bounds`.

15. **Nothing else moves.** Every existing grouped, ungrouped, horizontal,
    faceted, line, scatter, and table baseline passes byte-identically.
    `../uat/test20.rn` exercises both stacked orientations, the degenerate
    two-field form, and colour continuity against a grouped control. The full
    workspace suite passes with no ignored gate.

## Stop condition

> This record may add the `.stacked()` modifier, canonicalize composition to the
> stacked members after pair resolution, introduce one shared canonical
> positive-segment producer originating at data zero, route resolved colours
> into stacked paint and legend, honour `bar_width` and `category_gap` in the
> stacked kernel, and add the named gates and fixture. If implementation requires a new mark
> word, a new channel, a fourth `AuthoredFamily`, a change to
> `session_persistence.rs`, a `LayerIdentity` whose plot type is not `Bar`,
> widening the authored-role allowlist or capacity property to the stacked
> members, admitting bars into `layers`, or changing any existing pixel outside
> the enumerated stacked repair, work stops and the requirement returns to its
> owning record.

> If percentage-of-total semantics are needed to make absolute stacking useful,
> work stops. A denominator over first-row-wins values, and a percentage that
> ignores negatives, are scale decisions this record has no exhibit for.

> If step 2's repair cannot reproduce existing stacked baselines byte-for-byte
> under `bar_width(0.8)` with zero-based bounds, work stops. A geometry change
> smuggled in beneath a new surface is indistinguishable from a surface defect.

> If the domain, rectangles, hit regions, and hover cannot be made to consume
> one segment sequence, work stops rather than pinning any one of them as the
> inherited behaviour. Three consumers describing three different stacks is an
> incoherence to repair, not a limitation to ratify.

## Supersession

This record supersedes PRODUCT-0055 §6's clause that dashboard stacked bars
"remain unauthorable from Rune," and only that clause. PRODUCT-0055's pair
resolution, canonical composition, physical axes, authored-family identity,
colour lineage, display label, and refusal boundaries are consumed unchanged.

It re-scopes PRODUCT-0054's closure of the resolved-colour bypass class from a
count to a boundary: closed for every currently authorable mark, with a standing
obligation on any record that makes a new renderer reachable.

It preserves PRODUCT-0056 §5's horizontal stacked segment order and supplies the
reason it was left alone — stacking runs along the value axis, which
PRODUCT-0056's categorical rule does not govern.

It does not supersede PRODUCT-0046 §1, PRODUCT-0047, PRODUCT-0053, or
PRODUCT-0044 §5. `.stacked()` is a register member under 0046's rule, the
degenerate two-field case follows 0047's `N = 1` discovery, the geometry
equations are 0053's, and the degenerate form's absent legend is 0044's.

`StackedArea` remains dashboard-only and out of scope. It shares the picker and
will ask the same question, but it is a different kernel with its own ordering
and interpolation decisions.

## Rejected

- **A `stacked_bar` mark word:** forces a fourth `AuthoredFamily` variant and a
  schema change to a shipped persistence format with real user data, and gives
  one bar family two authored words for a difference that is geometry rather
  than meaning.
- **Infer stacking from the data:** grouped and stacked have identical channel
  signatures, so no schema fact distinguishes them. Inferring from cardinality,
  field names, or value ranges would make factoring change meaning.
- **`.stacked(true)` or `.stack("absolute")`:** invites `.stack("percent")` as a
  parameter of the same modifier, which would smuggle a scale decision into a
  composition selector before its semantics are decided.
- **Let `LayerIdentity` become `StackedBar`:** changes the colour-lineage key so
  toggling the modifier recolours the figure, and makes every colour checkpoint
  containing a stacked bar refuse at `DurableLayer::from_runtime`.
- **Repair colours and geometry inside the surface commit:** makes an
  independently verifiable byte-identical repair indistinguishable from new
  behaviour, and leaves no oracle if the surface later regresses.
- **Repair marks without the legend:** reproduces PRODUCT-0047's exact defect,
  where swatches sampled the ramp while marks used resolved assignments.
- **Stack top-down to match legend reading order:** makes legend order depend on
  orientation, the asymmetry PRODUCT-0056 removed for grouped bars. Value axes
  grow from the baseline in both orientations.
- **Refuse `.stacked()` on the two-field form:** contradicts PRODUCT-0046 §1's
  rule that a register belongs to the mark, to avoid a rendering that is merely
  pointless rather than undefined.
- **Aggregate duplicate category/series rows:** makes mark geometry compute over
  data, which the relation pipeline owns. First-row-wins is inherited and now
  stated.
- **Include `StackedArea`:** a different kernel with independent ordering and
  interpolation questions, sharing only a picker entry.

## Consequences

- Rune reaches every bar member. One word and one modifier produce grouped and
  stacked in both orientations, and the authored vocabulary remains three mark
  words.
- Toggling `.stacked()` is a pure geometry change: colours, identity, legend
  content, and durable payloads are unaffected. A saved session written before
  this record restores onto a stacked figure unchanged.
- Four defects in shipped dashboard code are repaired, from two causes. The
  colour bypass and the ignored geometry modifiers were unreachable by any gate
  while no authorable mark produced a stacked bar — widening the surface is what
  made them findable. The bounds desynchronization and the non-positive
  incoherence were reachable all along by anyone who set bounds or supplied a
  negative value, and were found by reading the kernel rather than by any test
  or exhibit.
- The resolved-colour bypass class is now bounded by a rule rather than a count,
  so the next surface widening inherits the obligation instead of relying on a
  reviewer remembering it.
- Percentage-of-total remains unwritten, with its trigger stated: a denominator
  over first-row-wins values and a percentage that ignores negatives are
  decisions needing their own record.
- `StackedArea` remains the one picker entry Rune cannot say, and its record
  will face this same family-versus-member question from a stronger precedent.
