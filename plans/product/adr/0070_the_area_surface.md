# ADR 0070: the area surface

Status: proposed 2026-08-09. This record gives Rune the stacked area as a
fourth authored mark word, `area`. Unlike PRODUCT-0064 it cannot be spelled as a
modifier, and unlike every prior surface record it changes a shipped
persistence schema: the durable authored-family vocabulary grows to four and
`polariton_session` becomes version 2. It also discharges PRODUCT-0064 §4's
standing obligation by routing resolved colours into the area kernel.

**This record is blocked on a prerequisite correction.** The area preparation
currently sorts series labels alphabetically rather than retaining the resolved
display domain, which contradicts PRODUCT-0044 §2's entry order and
PRODUCT-0064 §6's baseline-outward rule in the same stroke — legend order,
stack order, and display domain cannot agree while one of them is alphabetical.
It also carries negatives through running totals, and silently sums duplicate
`(x, series)` rows via an upstream `group_by(...).agg(sum())` where the bar
family keeps the first row. PRODUCT-0069 corrects all three, as PRODUCT-0065
did for PRODUCT-0064, rather than letting them be blessed inside a combined
surface and schema change. This record consumes that corrected kernel.

## Context

`StackedAreaPlotKernel` is complete and dashboard-reachable. Its resolved
fields are already projected — `ResolvedKernelFields::StackedArea { series:
Option<String>, x: String, y: String }` (`plot/spec/mod.rs:990`), with the
automatic projection at `:1947` and factory construction at
`plot/factory.rs:201`. Its layout is `Cartesian` with an `x_axis_type`
(`kernels/stacked_area.rs:17-27`), so continuous and temporal x are both
supported. Only Rune cannot reach it.

Two facts distinguish this from the bar family. First, **there is exactly one
member**: no unstacked area kernel exists, and no horizontal area exists, so
there is nothing for a composition modifier to select and no pair to resolve.
Second, **`series` is already optional**, so the two-field form is supported by
construction rather than by a degenerate path.

### Why a mark word, and why PRODUCT-0064's argument does not transfer

PRODUCT-0064 chose `.stacked()` over `stacked_bar` specifically to avoid a
fourth `AuthoredFamily`. The tempting move here is the same one: the channel
signature of a stacked area — continuous x, quantitative y, optional
categorical series — is *identical* to PRODUCT-0054's series line, so
`line(x, y, series).stacked()` would reach the kernel with no new word and no
schema change.

That would be wrong, and PRODUCT-0064 says why in its own terms. Its §1 defines
the modifier as marking a bar "for stacked composition; it does not name a plot
type, change a channel role, or add an encoding." A stacked bar is still
rectangles. A stacked line is not a line: `StackedArea` draws filled polygons
with boundary strokes. Reusing `.stacked()` there would make one register
member mean *compose differently* on one mark and *become a different mark* on
another, which is exactly the kind of overload the register rule exists to
prevent.

`.stacked()` therefore does not enter area's register at all. It is a bar
register member, and PRODUCT-0046 §1 scopes registers to marks, so
`area(...).stacked()` refuses as an ordinary cross-register error.

### The schema consequence is unavoidable

A multi-series area carries categorical colour continuation exactly as grouped
bars and series lines do, so its `LayerIdentity` reaches PRODUCT-0062's durable
mapping. `DurableLayer::from_runtime` (`session_persistence.rs:665-673`)
refuses any plot type outside `Scatter | Bar | HorizontalBar | Line`:

```rust
other => return Err(... "plot type {other} has no durable authored family"),
```

So making area authorable *requires* a fourth family. There is no framing that
avoids it: the alternative is a colour-bearing area figure that hard-fails
every checkpoint, which PRODUCT-0063 made a user-visible close-time failure.

This is the first real consumer of the owner-schema versioning PRODUCT-0059 §3
and PRODUCT-0062 §1 built and nothing has yet exercised. The change is additive
for existing data — no stored payload contains an area family, because none
could — but a payload written by this version is unreadable by a binary that
predates it. Claiming version 1 while emitting a vocabulary version 1 readers
reject would be dishonest versioning, so the owner version bumps.

### Inherited obligations

`stacked_area.rs:53` reconstructs colours with `settings.color_theme.get_color(t)`
— the fifth instance of the resolved-colour bypass class, and precisely the
case PRODUCT-0064 §4's re-scoped closure anticipated:

> Resolved-colour bypasses are closed for every currently authorable mark. …
> Any future record that makes a new renderer reachable from Rune must prove
> that renderer consumes resolved assignments.

PRODUCT-0068 already governs the guide question. Area fills are opaque, so
gridlines are occluded beneath them and legible in exposed headroom; the
boundary stroke uses `mark_decoration_color` at `0.5px` from that record.

Full occlusion **is** reachable here, and not only through the percentage
normalization PRODUCT-0068 named. An authored `y_bounds(0, summed_max)` removes
the headroom exactly, leaving every gridline beneath the fill. This record
decides that case rather than deferring it: axis ticks and labels outside the
plot area, together with the resolved series boundaries inside it, remain
sufficient to read an area whose stack meets the axis maximum. An author who
sets bounds flush to the data has chosen that framing, and the record takes no
strata exception for it — PRODUCT-0068's stratum is reopened by a record, never
by a bounds value.

## Decision

### 1. `area` is the fourth authored mark word

```rune
area("month", "revenue")              // one filled area
area("month", "revenue", "product")   // stacked areas, one per product
```

`area` joins `scatter`, `bar`, and `line` as an authored standalone mark in
every accepted position: legacy `plot.area`, relation sugar, canonical
`views([...])`, named views, and figure bodies. It is not admitted inside
`layers([...])`, which remains the closed line/scatter surface PRODUCT-0013 §2
defined.

### 2. Channels are continuous x, quantitative y, and an optional series

```text
x       quantitative or temporal   required
y       quantitative               required
series  nominal or ordinal         optional third field
```

Only **x** inherits PRODUCT-0054's line compatibility, which admits quantitative
or temporal. Area's y is quantitative-only, because the automatic projection
resolves it through `number(1, "y")` (`plot/spec/mod.rs:1949`) and a temporal y
has no defined summation. Line permits a temporal y; area does not, and the
difference is deliberate rather than an oversight in transcription. The third
field follows PRODUCT-0047's categorical rule.

A categorical x, a temporal or non-quantitative y, and a quantitative series
each refuse with the existing span-attributed reason naming the resolved field
and its semantic type.

There is no pair resolution. Unlike bar, area has no horizontal member, so the
x/y roles are fixed and no schema fact selects a geometry. Reversing the fields
does not rotate the mark; it produces whatever the compatibility rules say
about the new pair, which for a categorical x is a refusal.

### 3. One member, no composition modifier

`area` resolves to `PlotType::StackedArea` in both the two- and three-field
forms. No modifier selects composition, because no second member exists.

`.stacked()`, `.bar_width()`, and `.category_gap()` are bar register members and
refuse on area as cross-register errors. This record adds no area-specific
register; boundary colour and width are PRODUCT-0068's resolved chrome, not
authored values.

If an unstacked or overlaid area member is ever built, it needs its own
selector. The absence of a modifier already means stacked here, so `.stacked()`
may not be retrofitted as the discriminator — a later record must choose a word
for the *new* member instead.

### 4. Layer identity is the authored `Area` family

`BoundChannel.layer` carries `LayerIdentity { ordinal, plot_type: StackedArea }`,
and the durable projection maps it to a new `AuthoredFamily::Area`.

This follows PRODUCT-0055 §4 and PRODUCT-0064 §3: layer identity records the
authored family. Area has one member, so family and member coincide today; the
distinction still matters, because a future second member must join the same
family rather than start a new lineage and recolour existing figures.

As with every other mark, the view's stable `display_label` remains `plot.area`
while the resolved composition description is `Stacked Area Plot`
(`plot/core.rs:70`).

### 5. `polariton_session` becomes schema version 2

The durable vocabulary grows:

```text
v1  "scatter" | "bar" | "line"
v2  "scatter" | "bar" | "line" | "area"
```

Writers emit version 2. Readers accept **both**: a v1 payload decodes with the
three-word vocabulary and restores unchanged, and a v2 payload decodes with
four. A v1 reader encountering v2 refuses with PRODUCT-0062's existing
unsupported-schema error, which is the honest outcome and the reason the
version moves rather than the vocabulary silently widening inside v1.

Nothing else in the payload changes. Panel and group mappings, allocator
high-water marks, palette positions, and `ColorAllocationSpace` are identical
across versions, so a session written before this record restores with every
colour intact.

This is the first schema migration in the persistence arc. It is deliberately
the smallest possible one — a version integer and one vocabulary entry — and
the record does not use the occasion to change anything else in the format.

### 6. Area consumes resolved colours

The kernel gains the resolved-assignment lookup that `line.rs:466`'s
`series_color` and the bar kernels already have. Fills and legend swatches both
read the resolved `CategoryColorAssignment` for a series; the legacy ramp
survives only as the no-assignment fallback for automatic dashboard plots.

Both consumers are repaired together. A fix that corrected fills while leaving
the legend sampling the ramp would reproduce PRODUCT-0047's defect for the
third time.

### 7. Guides, legend, and the two-field form

The three-field form earns a legend of named swatches in resolved display-domain
order, as PRODUCT-0044 §2 requires. The two-field form has one unnamed region
and carries no legend, on PRODUCT-0044 §5's reasoning that a single synthetic
entry is not a guide.

Omission is not automatic and must be repaired here. `entries_from_labels`
(`plot/legend.rs:149-161`) hardcodes `LegendProvenance::Named`, so today the
two-field form's synthetic `"All Data"` label would be marked Named and
PRODUCT-0044 §1's policy would **retain** it. The prepared/kernel contract
therefore carries provenance explicitly: `Synthetic` when the series field is
absent, `Named` when present. An optional `series` makes the geometry work by
construction; it does not make legend omission work.

Stack order follows PRODUCT-0064 §6's baseline-outward rule: series zero is
adjacent to the numeric zero baseline and stacking proceeds upward, matching
legend order. Areas originate at data zero, and where zero lies outside an
authored `y_bounds` the stack clips rather than being rebased.

`x_bounds`, `y_bounds`, tick steps, and minor ticks apply to the Cartesian axes
with their existing numeric requirements. Facets, headings, and the figure-level
legend inherit unchanged.

### 8. What this record does not add

No unstacked or overlaid area member. No horizontal area. No percentage
normalization — that remains its own scale record, and PRODUCT-0068's
full-occlusion trigger applies to it rather than here. No area entry in
`layers([...])`. No authored boundary colour or width. No interpolation or
smoothing control: the kernel's existing straight-segment geometry is
preserved exactly.

It also does not specify the area preparation's data semantics, because they are
currently incoherent with three accepted rules and are the prerequisite
correction named at the head of this record:

```text
duplicate (x, series) rows   implicitly summed  (bars keep the first)
series label order           sorted             (not resolved display domain)
non-positive values          accumulated        (folded bands, empty hover intervals)
```

The sorting is the load-bearing one. PRODUCT-0044 §2 fixes legend entry order to
the display domain and PRODUCT-0064 §6 requires stack order to match legend
order from the baseline outward; an alphabetical stack cannot satisfy both.

PRODUCT-0069 decides all three: two pinned ordering routes, first-row-wins in
place of implicit summation, and one canonical value matrix — non-finite and
non-positive samples clamped to zero — read by range, polygon, boundary, and
hover alike. This record consumes that kernel and asserts none of those semantics
itself.

## Implementation sequence

1. Repair the fifth colour bypass **first and separately**, against the
   dashboard-reachable path. Two oracles as PRODUCT-0064 established: every
   existing stacked-area baseline reproduces byte-for-byte with no resolved
   assignments, **and** a direct kernel/factory test with synthetic
   `CategoryColorAssignment`s proves fills and legend consume them. The byte
   oracle alone exercises only the unchanged fallback.
2. Add `AuthoredFamily::Area` and the v1/v2 reader split in
   `session_persistence.rs`, with round-trip and cross-version tests, before
   any surface exists. A stored v1 payload must restore unchanged.
3. Add `area` to the envelope in every accepted standalone position, with its
   arity, compatibility, and cross-register refusals.
4. Thread resolution so `area` resolves to `StackedArea` and its layer identity
   projects to the new family.
5. Add `../uat/test23.rn` as the live exhibit and update the corpus, README, and
   language-as-built.

Steps 1 and 2 must be independently green. A colour error discovered after the
surface exists is indistinguishable from a surface defect, and a schema change
landed alongside a new mark cannot be separately reverted.

## Acceptance gates

1. **The word obeys the surface.** `area` lowers identically in legacy
   `plot.area`, relation sugar, canonical views, named views, and figure bodies,
   in two- and three-field forms. It refuses inside `layers([...])`, refuses at
   wrong arity, and refuses non-literal fields except where PRODUCT-0058's
   static bindings apply — which they do, in every field position.

2. **Step 1's repair is byte-identical and separately proven.** Existing
   dashboard stacked-area images, SVG, and draw operations reproduce exactly
   with no resolved assignments; a separate direct test with synthetic
   assignments proves fills and legend both consume them. Neither oracle alone
   discharges the gate.

3. **Channel compatibility matches line plus a categorical series.** Temporal
   and quantitative x both resolve; a categorical x, a non-quantitative y, and a
   quantitative series each refuse at their attributed span naming the resolved
   field and semantic type. Reversing x and y does not rotate the mark.

4. **Cross-register refusals hold.** `.stacked()`, `.bar_width()`, and
   `.category_gap()` each refuse on `area` with the existing cross-register
   reason. No area-specific register member is introduced.

5. **Layer identity is the Area family.** Two- and three-field areas carry
   `LayerIdentity { ordinal: 0, plot_type: StackedArea }`, and the durable
   projection yields `AuthoredFamily::Area`. Changing the series field starts a
   fresh lineage; re-running identical source preserves every palette position.

6. **The schema migration is exact in both directions.** A payload stored at
   version 1 decodes, restores, and reproduces every panel mapping, allocator
   mark, and palette position unchanged. A payload written now declares version
   2 and round-trips. A synthetic version-1 reader refuses a version-2 payload
   with PRODUCT-0062's unsupported-schema error rather than silently dropping
   the area entries. A v2 payload containing no area family is still emitted at
   version 2 — the version tracks the vocabulary, not the content.

7. **The version seals the vocabulary, not merely the reader.** A valid v2
   payload containing an `"area"` family, relabelled as version 1, **refuses**
   because `area` is outside v1's three-word vocabulary. Accepting schema
   `{1, 2}` against one enum that happens to contain `Area` would let a
   v1-labelled payload carry it, making the version decorative. This gate is
   what gives v1 a meaning.

8. **A colour-bearing area survives a real checkpoint.** Through the production
   `--session` path, a three-series area figure checkpoints, the session
   reopens, and re-running the producer restores every series colour exactly.
   Before this record the same figure fails at `DurableLayer::from_runtime`; the
   gate asserts that prior failure is gone rather than merely that the new path
   works.

9. **Resolved colours reach fills and legend.** An area under `palette("blues")`
   renders blues in its regions *and* its named swatches. A control proves the
   ramp fallback still serves an automatic dashboard area. Fill-only and
   legend-only repairs each fail.

10. **Legend policy matches the family.** The three-field form produces named
   swatches in display-domain order; the two-field form produces none. Stack
   order is baseline-outward and agrees with legend order.

11. **Zero origin, clipping, and full occlusion.** With `y_bounds` whose lower
    bound is nonzero, regions and hover agree and the stack clips at the domain
    edge rather than rebasing. A control at `y_bounds(0, summed_max)` removes
    all headroom: every gridline is occluded, axis ticks and resolved
    boundaries remain, and no overlay reference line appears. Data semantics —
    duplicate-row handling, series order, and negative values — are **not**
    asserted here; they belong to the prerequisite correction.

12. **Guides obey PRODUCT-0068.** Gridlines are occluded beneath area fills and
    legible in headroom; the boundary renders in `mark_decoration_color` at
    `0.5px`. No area figure requires an overlay reference line, and the record
    takes no exception to the stratum.

13. **Facets and axes inherit.** A faceted three-series area retains
    panel-blind colours, one figure-level legend, congruent cells, and shared
    domains under PRODUCT-0065. Bounds, tick steps, and minor ticks apply to
    both Cartesian axes.

14. **Nothing else moves.** Every existing scatter, bar, line, table, facet, and
    dashboard baseline passes byte-identically. `../uat/test23.rn` exercises
    both forms against a series-line control. The full workspace suite passes
    with no ignored gate newly admitted.

## Stop condition

> This record may add the `area` mark word, resolve it to `StackedArea`, add
> `AuthoredFamily::Area` and `polariton_session` version 2 with a v1-compatible
> reader, route resolved colours into the area kernel's fills and legend, and
> add the named gates and fixture. If implementation requires an unstacked or
> horizontal area member, an area register member, a change to the payload
> beyond the version integer and one vocabulary entry, a modifier retrofitted as
> a composition selector, admitting area into `layers`, or changing any existing
> pixel outside the enumerated colour repair, work stops.

> If a stored version-1 payload cannot be read unchanged after the migration,
> work stops. A schema bump that silently strands existing sessions is worse
> than an unauthorable mark, and PRODUCT-0063 made those sessions user-visible.

> If area's guide behaviour requires reference lines above its fills, work stops
> and reopens PRODUCT-0068's stratum explicitly. This record may not take the
> unrecorded exception that record forbids.

## Supersession

This record supersedes PRODUCT-0062 §1's `polariton_session` version 1 as the
written format, while requiring it to remain readable. The three-word durable
vocabulary in PRODUCT-0062 §5 becomes four.

It discharges PRODUCT-0064 §4's standing obligation for one renderer. The
obligation itself is unchanged and still binds the next surface widening.

It does not supersede PRODUCT-0064's `.stacked()` decision. That modifier
remains bar-scoped and this record explains why the same spelling cannot be
reused across marks whose geometry differs.

PRODUCT-0068's stratum, PRODUCT-0065's shared domains, PRODUCT-0044's legend
policy, PRODUCT-0054's line compatibility, and PRODUCT-0058's field bindings are
all consumed unchanged.

## Rejected

- **`line(x, y, series).stacked()`:** reaches the kernel with no new word and no
  schema change, which is exactly its appeal — but it makes one register member
  mean "compose differently" on bars and "become a different mark" on lines.
  PRODUCT-0064 §1 defines the modifier as not changing the mark.
- **Keep the family at three and map area onto `Line`:** produces a durable
  lineage that lies about what was authored, so a later real area family would
  have to migrate existing payloads rather than extend them.
- **Widen the vocabulary inside version 1:** old readers would reject data
  claiming a version they support. The version exists to describe exactly this.
- **Bundle other format changes into the migration:** the first schema bump
  should be the smallest possible one, so a later reader can be certain what v2
  means.
- **Add `.stacked()` to area for symmetry:** implies a choice that does not
  exist, and forecloses the spelling a genuine second member would need.
- **Infer orientation as bar does:** area has no horizontal member, so there is
  no member for a pair rule to select and reversing the fields is simply a
  compatibility error.
- **Author the boundary colour or width:** PRODUCT-0068 just made both resolved
  chrome. Re-authoring them here would reopen a stratum decision two records
  old.
- **Add smoothing or interpolation:** new geometry with its own monotonicity and
  overshoot questions, and no exhibit asks for it.
- **Aggregate duplicate x/series rows:** makes mark geometry compute over data,
  which the relation pipeline owns.

## Consequences

- Rune reaches the last mark the dashboard could draw and the language could
  not. The authored vocabulary becomes four words.
- The persistence arc gets its first schema migration, and it is a version
  integer plus one string. Sessions written before this record restore with
  every colour intact.
- The resolved-colour bypass class is discharged for its fifth and final
  currently-reachable instance. The obligation PRODUCT-0064 wrote as a rule
  rather than a count is what carried it here.
- PRODUCT-0068's stratum gets its first new consumer and needs no exception:
  absolute areas leave headroom, so their gridlines remain legible where the
  canvas shows.
- Percent-of-total remains docketed and now has two records pointing at it —
  PRODUCT-0064 for its scale semantics and PRODUCT-0068 for the reference lines
  a fully-occluding chart would need.
- The dashboard picker still holds many kernels Rune cannot say — box plot,
  bubble, candlestick, fill-between, funnel, heatmap, hexbin, histogram and
  more. Area is not the last of them; it is the one whose substrate was already
  complete and whose absence blocked nothing but a word.
