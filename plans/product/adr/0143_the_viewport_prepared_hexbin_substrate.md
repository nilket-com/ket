# ADR 0143: the viewport-prepared hexbin substrate

Status: accepted 2026-08-30. Hexbin is the last catalog kernel whose absence is
not merely an authoring gap. It bins in Polariton with a hard-coded data-space
radius of `0.02`; unequal axis scales turn its cells into non-regular screen
shapes, and resize, zoom, and pan leave membership attached to an obsolete
viewport. Moving the same aggregate to Toron unchanged would preserve the
relation/mark boundary and preserve the defect: the correct lattice is known
only after Polariton resolves the final plot rectangle.

This record makes viewport-parameterized preparation a first-class contract.
Polariton owns an explicit screen-space lattice; a preparer owns validation,
point assignment, and sparse count aggregation; and the hexbin kernel consumes
only a lattice-tagged prepared artifact. The first preparer executes in-process
over the materialized snapshot Polariton already owns. A later deferred-source
preparer may execute the identical contract in Toron without changing the Rune
spelling or the rendering artifact.

It does not add that deferred-source executor, a Toron operator, or a Rune word.
`hexbin(x, y)` is PRODUCT-0144's subject. It also preserves PRODUCT-0091's
decision that hexbin receives no colour-job word: counts are non-negative and
the continuous scale is sequential.

## Context

### The current kernel has two authorities and neither is a viewport

`prepare_hexbin_data` (`plot/kernels/hexbin.rs:237`) receives a `radius: f32`,
converts the source columns to `f64`, computes axial coordinates in data space,
and aggregates them through a Polars `group_by`. Both production callers supply
the literal `0.02`: the factory (`plot/factory.rs:66-67`) and planning validation
(`plot/request.rs:176-177`).

Paint then reconstructs hexagon vertices in the same data-space units and sends
each vertex through `CoordinateTransformer::cartesian`
(`plot/kernels/hexbin.rs:42-62`). That transform has independent x and y scales.
A regular data-space hexagon therefore remains regular on screen only when
pixels-per-x-unit equals pixels-per-y-unit. A 2:1 plot rectangle over equal
numeric domains stretches every cell 2:1; changing either domain changes the
shape again.

Membership is already fixed by then. `HexbinPreparedData` retains only
`HashMap<(i32, i32), u32>`, `max_count`, and the data-space radius. It has thrown
away the observations needed to answer a different viewport. Resize, authored
bounds, and PRODUCT-0133 pane overrides can transform the old cells, but cannot
re-bin them.

Hit-testing repeats the inverse axial calculation independently
(`hexbin.rs:107-125`). Preparation uses Polars half-to-even rounding while hit
uses Rust `f64::round`; an exact boundary may therefore be assigned by one rule
and queried by another. The geometry defect is not only visual.

### The relation-side rule is real, and hexbin is its exceptional test

The restoration arc has consistently kept aggregation out of marks:

- histogram consumes authored `[bin_start, bin_end, count]` rows after Toron ADR
  545 supplied alias-first numeric bucketing;
- box consumes authored Tukey summary rows;
- heatmap refuses duplicate cells and tells the author to aggregate in the
  relation; and
- violin's live UAT authors `kernel_density(...)` and grouped
  `approx_quantile(...)`, joins them in one relation invocation, and passes the
  prepared rows to `violin(...)` (`uat/test34.rn`, `uat/test35.rn`).

Hexbin is different in one precise way. Its preparation **is an aggregation**,
but the author cannot write the correct aggregate: bin membership depends on
the final plot-area dimensions and active domains, facts that do not exist when
the Rune relation program is authored or executed. Requiring authored bins
would make one static relation correct for one accidental viewport. Calling
mark-side aggregation “rendering” would instead erase the boundary by naming it
differently.

The exception is therefore system synthesis, not renderer ownership. Polariton
synthesizes the lattice because presentation owns it; a preparer computes the
relation-shaped answer; the mark renders that answer. The exception is narrow
enough to state and gate: **only preparation whose parameters are unavailable
before presentation layout may be system-synthesized under this record.**

### Materialized now, deferred later

`PlotSource` currently has one variant, `Materialized`, carrying an owned
`ResultArtifact` (`plot/request.rs:24-34`). Its comment already reserves the
other posture: materialized is the only variant until deferred `db.table(...)`
relations land, and the enum exists so consumers distinguish snapshots from
sources that still require planning.

The deferred direction is not invented by hexbin. PRODUCT-0007 says deferred
relations join later without changing the plot contract, and deferred-ledger
DB-01 remains “will do.” But it is not implemented, and PRODUCT-0135 explicitly
leaves live-table planning outside its cancellation cut.

Making hexbin wait for that work would require a round trip for rows Polariton
already holds today. Conversely, implementing an untyped local shortcut would
give the future remote path a second geometry and aggregation authority. This
record takes the middle path: one executor-neutral request and artifact, with a
materialized in-process executor first.

### The guide participates in the viewport

The count gradient is not downstream decoration. In fallback placement,
`legend_layout` removes the measured guide width before the axis bands and plot
rectangle are resolved. Hexbin's guide endpoint is `max_count`, but `max_count`
depends on the plot rectangle. If actual guide content reserves its own binning
viewport, the dependency is circular:

```text
plot width -> lattice -> max_count -> guide width -> plot width
```

Hiding the guide for a first pass and adding it on the second merely makes the
first displayed artifact knowingly stale. An explicit source-stable reservation
is part of the preparation contract, not an incidental renderer workaround.

## Decision

### 1. One viewport-preparation contract, independent of executor

The contract has three values:

```text
HexbinPreparationRequest {
    source_token,
    generation,
    x_field,
    y_field,
    lattice,
}

HexLatticeSpec {
    x_domain,
    y_domain,
    plot_width_px,
    plot_height_px,
    radius_px,
    orientation,
    origin,
}

HexbinViewportArtifact {
    source_token,
    generation,
    lattice,                 // exact request value, not reconstructed
    bins: [(q, r, count)],   // occupied bins only
    max_count,
}
```

Names may vary in implementation; the distinctions may not.

- `source_token` identifies the exact observation publication. A prepared
  answer from an older materialized result is stale even if its fields and
  lattice happen to match.
- `generation` orders requests for one source and view. Only the newest
  generation may publish.
- `lattice` is copied intact into the artifact. Untagged `(q, r, count)` rows
  are not renderable truth because axial coordinates have no meaning without
  their grid.
- `orientation` is carried despite having one admitted value because the
  artifact is self-identifying across executors. A response prepared against a
  different orientation must refuse as a lattice mismatch, not be interpreted
  under the renderer's fixed default. The field is a tag, not an invitation to
  widen the surface.
- `bins` are a sparse relation-shaped result. Empty cells are absent rows,
  following Toron ADR 545's sparse histogram precedent.
- `max_count` derives from `bins`; it is not an independently supplied input.

The executor may be synchronous or asynchronous, local or remote. Those are
scheduling facts, not alternate preparation semantics.

### 2. Polariton owns the lattice, after final layout

The lattice is derived only after these authorities have resolved:

1. the source domains and any authored domain settings;
2. PRODUCT-0133's pane viewport override;
3. guide reservation (§8), axis-guide bands, padding, headings, and facets; and
4. the final plot-area width and height in **logical pixels**.

The plot rectangle's widget-space x/y position is excluded. Hex geometry uses
plot-local coordinates, so moving an unchanged rectangle within a window does
not invalidate membership. Width and height are included. Device pixel ratio
is excluded because the plotting contract and authored sizes are in logical
pixels.

The active x/y domains are included exactly as resolved for the transform. A
change to either domain, including zoom or pan, creates a different lattice.
Colour ramps, stroke colour, and other paint-only settings do not.

This cut admits **quantitative x and y only**. The current automatic resolver
groups temporal dtypes with numeric candidates, but the kernel then labels both
axes `Linear` and projects temporal values through `f64`, losing the exact
temporal authority PRODUCT-0128 established. Extending a remotely reproducible
lattice to `Date`, zoned `Datetime`, or `Time` requires a typed exact projection
contract. PRODUCT-0144 may not widen compatibility by treating their integer
storage as ordinary quantities; temporal hexbin needs its own follow-on.

### 3. Screen geometry is regular, pointy-top, and plot-local

`HexRadiusPx::DEFAULT` is **10 logical pixels**. It is a positive finite bounded
type; zero, non-finite values, and a radius too small to keep axial coordinates
within `i32` for the maximum admitted plot rectangle refuse. No Rune spelling
for radius is added here.

The default preserves the old `0.02` radius at its one non-distorting reference
case: a normalized domain rendered into a 500 × 500 logical-pixel plot area.
Unlike the old value, it remains ten pixels under every domain and aspect ratio.

The lattice is **pointy-top**, matching the existing center formula, with its
origin fixed at plot-local `(0, 0)`. For a radius `R` and axial cell `(q, r)`:

```text
center_x = R * sqrt(3) * (q + r / 2)
center_y = R * 3/2 * r
```

Vertices are constructed directly in plot-local pixel space at angles
`-30°, 30°, ... 270°`; they are not sent through the data transform a second
time. All six center-to-vertex distances are therefore `R`, within floating
point tolerance, under unequal domains and non-square viewports.

Cells may cross the plot edge and are clipped by the existing plot-area clip.
The center need not be inside merely because an assigned observation was.

### 4. Observation assignment is one specified operation

Every validated observation is mapped through the authoritative x/y transform
into plot-local logical pixels. An observation outside either **closed active
domain** is omitted from this viewport after validation; a value exactly on a
domain endpoint participates. Omission here is viewport selection, not data
cleaning: changing the viewport may make the same validated row participate.

Pixel coordinates are converted to fractional axial coordinates, then to the
nearest cube coordinate. Component rounding is **ties-to-even**. When two cube
components have the same largest rounding error, correction priority is
`q`, then `r`, then `s`. Preparation and hit-testing call the same shared helper;
neither may restate the arithmetic in Polars expressions or private kernel code.

`count` is a non-negative `u64`. Axial coordinates are `i32`, protected by
`HexRadiusPx` and the bounded plot rectangle. Two observations assigned to the
same `(q, r)` increment one row. No occupied bin has count zero, and:

```text
sum(bin.count) = number of validated observations inside the active domains
```

This conservation equation is the aggregate's primary oracle.

### 5. Preparation validates before viewport filtering

Required numeric-channel discipline follows PRODUCT-0090. Preparation is
fallible and has no empty-column or numeric fallback:

- a missing x or y column refuses, naming the channel and field;
- a non-quantitative or inexactly projectable column refuses;
- a temporal `Date`, `Datetime`, or `Time` column refuses explicitly, naming the
  channel and field; integer storage does not make it quantitative for this
  contract;
- a null, non-finite, or inexact x/y cell refuses, naming the channel, field,
  and 1-based row; and
- a schema-valid empty relation succeeds with zero observations.

Validation scans **all rows before applying the active-domain filter**. Zooming
away from a malformed row cannot turn an invalid source into a valid one.
Column precedence is x then y; within a column, missing before incompatible;
rows then scan in producer order, x before y, null before non-finite before
inexact. The first fault wins deterministically.

The validated materialized source is an opaque `HexbinObservations` collection
containing paired coordinates, field labels, observed ranges, and observation
count. Its fields are private. Domains and guide bounds derive from the same
validated rows, so a caller cannot inject a count or range that disagrees with
the observations.

### 6. The mark never receives raw observations

The in-process implementation is a `MaterializedHexbinPreparer` over an
`Arc<HexbinObservations>`. It implements §1 and returns
`HexbinViewportArtifact`. Its use of local Rust/Polars execution is an executor
choice, not permission for `HexbinPlotKernel::plot` to aggregate.

The kernel consumes a prepared artifact and does only four things:

- construct regular pixel-space paths from `(lattice, q, r)`;
- colour each occupied path from its count;
- publish the count guide from `max_count`; and
- hit-test through the artifact's same lattice and bin map.

Architecture gates forbid a dataframe, raw x/y arrays, `group_by`, or point
assignment arithmetic in the mark renderer. The local preparer lives at the
planning/viewport-preparation boundary, not under paint disguised as a helper.

This is the degenerate local case of the relation/mark boundary: the executor
happens to be in the same process because the complete source snapshot already
is. It does not establish “small data may be aggregated by marks.”

### 7. One artifact feeds paint, guide, and hit

The common plot path gains a viewport-preparation seam. For hexbin it:

1. resolves the source-stable guide reservation (§8);
2. resolves the final plot rectangle and `HexLatticeSpec`;
3. obtains or reuses the artifact for the exact `(source_token, lattice)` key;
4. supplies that artifact to mark paint and actual legend content; and
5. supplies the same artifact to hit-testing.

Separate reconstruction in plot and overlay caches is allowed only through the
same deterministic preparer and exact key. A bin map computed for paint and a
second map computed for hover are not two authorities merely because they
happen to compare equal in a fixture.

Hit reports `(q, r)`, `Count`, and the bin **center** projected back through the
authoritative transform. It does not label the cursor coordinate as the bin's x
and y value, which the current kernel does. Polygon-selection semantics are not
added; PRODUCT-0133's structured hit may present the bin without inventing a
two-dimensional relation predicate.

### 8. A source-stable count-guide envelope breaks the cycle

For fallback legend placement, hexbin reserves a `Count` gradient envelope from
the validated source observation count `N`, before viewport aggregation:

```text
reservation label budget = ["0", widest decimal string of digits(N) positions]
actual guide domain = (0, max_count)
```

The count guide has a deliberately closed label contract: it displays **only
the two integer endpoints**, `0` and `max_count`, as unabbreviated base-10
digits with no fractional part, grouping separator, intermediate tick label, or
unit suffix. Reservation does not format `N` and assume its particular glyphs
are widest. It budgets `digits(N)` positions, each at the widest decimal-digit
advance under the resolved legend typography, plus the same bar, gap, and
padding geometry the displayed guide uses.

Because `max_count <= N` under §4, `digits(max_count) <= digits(N)`; because
every position reserves the widest digit, every legal actual endpoint fits.
This is a structural width bound, not an assumption that formatter output or
tick selection is monotone. Introducing abbreviation, fractional or
intermediate ticks, separators, or a different count-label formatter must
replace this envelope contract in the same record; it cannot arrive as a
legend-only change.

The renderer uses the envelope only for geometry and the actual domain only for
colour and displayed endpoint content. It never displays `N` as though one bin
contained every source row.

For a non-empty source, the envelope remains reserved even when the active
viewport contains no observations. Actual guide content is then absent, as
PRODUCT-0087 requires for a degenerate `(0, 0)` domain, but the plot rectangle
does not expand and invalidate the lattice. Empty source reserves nothing.
Authored overlay and hidden legend modes retain their existing placement rules;
the envelope does not force a gutter when policy does not request one.

The common legend API therefore distinguishes **reservation content** from
**display content**. Every existing producer supplies the same value for both;
hexbin is the first producer whose honest content is viewport-prepared and whose
reservation must precede it.

### 9. Invalidation and publication are explicit even when local is immediate

A new preparation generation is minted when any lattice key member or
`source_token` changes. Completions publish only when both match the newest
request. The materialized executor completes synchronously today, making the
ordering trivial in production, but a delayed fake executor gates the rule now
so the future remote path cannot reinterpret it.

`ViewportMotion::Settled` requires an exact artifact for the current lattice.
During `Moving`, the renderer may reuse the previous exact artifact as a
degraded preview under PRODUCT-0139's discipline. The present seam freezes that
old plot-local geometry and clips it to the new plot area; it does not imply a
gesture transform that PRODUCT-0133 stages 3–4 have not supplied. A follow-on
may transform the frozen geometry with an explicit gesture transform, in which
case it may temporarily cease to be regular. Either preview remains stale and
is never called exact. Hit-testing is disabled against it: interaction must not
report membership from a lattice the screen no longer means.

The first frame with no prior artifact prepares exactly even if motion is in
progress. The first settled frame prepares exactly once for the final lattice.
Resize outside a viewport gesture is settled and therefore exact; it does not
receive a motion exemption.

Paint-only changes reuse the artifact. Source, domain, plot width/height,
radius, orientation, or origin changes do not.

### 10. Materialized durability is complete; deferred durability stays with deferred sources

Current live and pinned plots retain their materialized `ResultArtifact`.
`MaterializedHexbinPreparer` therefore remains able to produce a new lattice
after resize, zoom, or pan; pinning does not freeze the viewport and needs no new
retention policy.

A future deferred source cannot inherit that conclusion automatically. Its
record must choose whether a pin retains a replayable source, materializes the
observations, or freezes a last prepared viewport artifact. PRODUCT-0143 does
not decide the lifetime of a source variant that does not exist. It does require
that any such executor return the same tagged artifact and obey §9; changing
pin semantics cannot change hex assignment.

### 11. The deferred/Toron executor is a continuation, not a second design

When deferred `db.table(...)` sources land, a preparer may send
`HexbinPreparationRequest` to Toron and receive the relation-shaped bins plus
the exact lattice tag. Toron owns validation, assignment, and aggregation for
that execution. Polariton still owns lattice derivation and refuses a response
whose source token, generation, or lattice differs.

That continuation must supply:

- local/remote correspondence fixtures over boundary, negative, duplicate,
  null, non-finite, and empty cases;
- newest-wins scheduling and out-of-band cancellation using PRODUCT-0135's
  substrate where the adapter supports it;
- the source observation count needed by §8 before final lattice preparation;
  and
- a durability decision under §10.

No Toron code or protocol field lands in this record. Adding a remote executor
without correspondence evidence is not “moving the local implementation”; it
is creating a second binning language and triggers a halt.

### 12. What this record does not do

No Rune constructor, callable-registry entry, mark-field carrier, authored
family, layer mark, or persistence spelling. Hexbin remains unavailable through
the Rune presentation language until PRODUCT-0144.

No colour job word, diverging anchor, or categorical palette. Counts retain
`ContinuousScaleIntent::SEQUENTIAL`, and PRODUCT-0091 §8 continues to forbid a
hexbin job word even after a surface exists.

No authored radius, orientation, or lattice origin. No temporal axes (§2), log
axes, weighted counts, facets sharing one lattice, normalization by visible
total, selection polygon, density estimate, or zero-filled cells. Each changes
the preparation request or meaning and requires its own record.

## Implementation sequence

**Stage 0 — bank the defect.** Render the same normalized observations in a
square and a 2:1 plot area with the current `0.02` data radius. Bank unequal
screen side lengths, unchanged memberships after a domain change, and a boundary
case where preparation and hit rounding disagree. These are negative controls,
not parity promises.

**Stage 1 — the contract and reference math.** Add `HexLatticeSpec`, request,
tagged artifact, bounded radius, one axial assignment helper, and conservation
oracles. No kernel migration.

**Stage 2 — validated observations and the materialized preparer.** Make
preparation fallible, delete empty-column and numeric fallbacks, retain the
validated source snapshot, and implement the local executor outside paint.
Planning validates; factory construction receives the same approved snapshot or
reconstructs it deterministically under the existing validation-then-
reconstruction rule.

**Stage 3 — viewport preparation and guide reservation.** Split legend
reservation from display content, derive the lattice after final plot layout,
add exact cache keys and generations, and exercise the delayed fake executor.

**Stage 4 — the kernel.** Replace data-space paths and private hit arithmetic
with artifact-backed pixel paths and the shared helper. Remove both hard-coded
`0.02` callers and `HexbinPreparedData::radius` in data units.

**Stage 5 — motion and durability.** Add stale-preview refusal for hit, exact
settle behavior, resize/zoom/pan invalidation, and live/pinned materialized
tests.

**Stage 6 — re-bank.** Bank regular geometry and stable local results as the
substrate oracle PRODUCT-0144 consumes.

## Acceptance gates

1. **Unequal scales still produce regular hexagons.** A 2:1 plot rectangle over
   equal x/y domain spans yields six center-to-vertex distances equal to the
   resolved logical-pixel radius and six equal side lengths within tolerance.
   The pre-repair negative control does not.
2. **Radius is screen-space.** The same lattice radius under two numeric domain
   spans remains ten logical pixels; data-space extents differ instead of screen
   geometry.
3. **Resize re-bins.** One observation fixture crosses at least one bin boundary
   when only plot width changes. The artifact lattice and occupied `(q, r)` row
   change; stretching old bins does not pass.
4. **Zoom and pan re-bin.** Domain-only changes mint new generations and produce
   artifacts tagged with the new domains. A paint-only ramp change does neither.
5. **The lattice is final-layout geometry.** Changing title/axis/legend settings
   so the final plot area changes also changes the lattice; moving the same plot
   rectangle without resizing it does not.
6. **Counts are conserved.** Across negative coordinates, duplicates, boundary
   points, and sparse cells, the sum of `u64` counts equals the number of
   validated in-domain observations.
7. **Sparse means absent.** No zero-count row is emitted, and no rectangular or
   axial extent is filled synthetically.
8. **Boundary rounding is shared.** Exact and adjacent-to-exact tie fixtures
   produce the specified ties-to-even/correction result in preparation and hit.
   Architecture inspection finds one assignment implementation.
9. **The artifact is self-identifying.** Swapping a bin relation between two
   lattices or source tokens refuses; equality of the rows alone cannot admit
   it.
10. **One artifact feeds all consumers.** Paint colour, actual guide maximum,
    hit count, and hit center agree for the same bin. No consumer reconstructs
    membership.
11. **The cursor is not the bin center.** Hovering two different positions in
    one bin reports the same projected center and count, not two cursor-derived
    x/y values.
12. **Column faults refuse.** Missing, non-quantitative, and inexactly
    projectable x and y fields each refuse with channel and field attribution.
    `Date`, `Datetime`, and `Time` are three explicit refusal cases in both
    channels; their integer storage never reaches quantitative preparation.
13. **Row faults refuse before filtering.** Null, NaN, positive/negative
    infinity, and inexact numeric values each refuse with a 1-based row. A bad
    row outside the active viewport still refuses.
14. **Precedence is deterministic.** Missing/invalid columns beat row faults;
    x beats y; and within a cell null beats non-finite beats inexact.
15. **Empty input is honest.** A schema-valid empty relation yields no
    observations, no bins, `max_count == 0`, no displayed gradient, and no guide
    reservation. Missing required columns on an empty relation still refuse.
16. **The guide cannot move its lattice.** A non-empty source reserves a
    `digits(N)` widest-digit endpoint budget before binning; actual
    `(0, max_count)` content fits that envelope. A viewport with no visible
    points omits content without changing plot width.
17. **Actual colour remains honest.** Bin colours and the displayed count guide
    use `(0, max_count)`, never `(0, N)`. A fixture with `max_count < N`
    discriminates reservation from display.
18. **The count envelope survives formatter-width traps.** Two discriminating
    fixtures exercise the bound: one where `N` is a power of ten but
    `max_count == N - 1`, and one where the widest decimal digit does not occur
    in `N`. The actual endpoint fits without a second layout. Source guards pin
    exactly two integer endpoint labels and forbid abbreviation, fractional or
    intermediate ticks, grouping separators, and suffixes.
19. **Newest wins.** A delayed fake executor completes generations 2, 1, and 3
    in that order; only 2 and then 3 may publish, and 1 has no paint, guide,
    hover, cache, or status effect.
20. **Moving may degrade; settled may not.** A moving frame can reuse a tagged
    prior artifact with hit disabled. The first settled frame produces one exact
    current-lattice artifact and restores hit.
21. **A first frame is not blank by policy.** With no prior artifact, even a
    moving materialized viewport prepares an exact local result.
22. **Materialized pins remain responsive.** A pinned hexbin retains its source
    snapshot and produces a different exact artifact after resize or domain
    change without relation execution.
23. **The renderer does not aggregate.** Structural inspection finds no
    dataframe, raw coordinate arrays, grouping, or assignment formula in
    `HexbinPlotKernel` paint/hit. They consume `HexbinViewportArtifact`.
24. **One contract, one local executor.** Production has exactly one hexbin
    lattice/request/artifact vocabulary and one materialized preparer. No
    “local bin” alternate type or untagged factory shortcut exists.
25. **No premature remote surface.** No Toron protocol/operator, deferred
    `PlotSource` variant, adapter branch, or cross-process cancellation code is
    introduced.
26. **No Rune surface.** Callable registry and presentation corpus contain no
    `hexbin`; no mark field transport or `LayerMark` variant appears.
27. **No colour job word.** Registered colour-job applicability remains heatmap
    and bubble only; hexbin stays sequential count colour under PRODUCT-0091.
28. **The old authority is gone.** Production contains no `0.02` hex radius, no
    data-space hex vertex transform, and no independently rounded hit formula.
29. **Full battery.** Polariton unit, architecture, Rune compiler, GUI, UAT, and
    workspace checks pass with no ignored or tolerated failure added for
    hexbin.

## Guardrails and stop conditions

- If the final plot rectangle cannot be resolved from a source-stable guide
  envelope before binning, halt. Do not add iterative layout whose displayed
  result depends on which pass happened to stop.
- If the materialized executor cannot keep validation and viewport filtering in
  separate phases, halt. A zoom must never hide malformed source data.
- If ten logical pixels permits axial overflow under the repository's admitted
  plot bounds, tighten `HexRadiusPx`/bounds explicitly; do not saturate or wrap.
- If exact settled local preparation blocks the UI beyond the existing frame
  budget on a materialized fixture the product admits, halt and add a bounded
  worker/scheduling record. Do not quietly label a stale artifact settled.
- If a future Toron implementation cannot match the shared boundary/tie
  fixtures, its record must change the common contract and re-bank local too;
  it may not normalize the disagreement in the adapter.
- Temporal admission, weighted observations, authored radius/origin, shared
  facet lattices, and selection polygons reopen their named boundaries. None is
  an implementation convenience.

## Rejected

**Keep radius `0.02` and compensate vertices at paint.** This can make a cell
look regular but does not change which observations were aggregated together.
Visual repair without membership repair is a more convincing wrong answer.

**Normalize x and y independently once, then retain bins across viewports.** A
normalized grid is regular only for the aspect ratio used to define it. Resize
changes normalized-unit pixels, and zoom/pan change membership even when aspect
ratio is constant.

**Treat local binning as mark rendering.** Attractive because the current
kernel already has the dataframe. Rejected because aggregation then becomes a
private paint operation that cannot correspond to a remote executor, export,
or testable prepared relation. The materialized preparer is local; the mark is
not the preparer.

**Require authors to prepare `(q, r, count)`.** They do not know the final plot
rectangle or pane override. Any authored lattice would be correct only for
presentation state that is neither available nor durable in the relation
program.

**Wait for deferred relations.** This avoids writing a local executor but makes
an existing owned snapshot take an unnecessary database round trip and blocks
the last geometry repair on an unscheduled arc. The contract, not executor
location, is the durable investment.

**Implement Toron first.** Same sequencing problem, plus a premature protocol
whose semantics have not been exercised locally. Correspondence is cheaper to
prove after the request/artifact pair has a materialized reference executor.

**Return untagged bin rows.** `(q, r)` is local identity inside a particular
lattice. Without the lattice and source token, stale rows are structurally
indistinguishable from current rows.

**Let actual `max_count` size the fallback legend.** It makes bin membership
choose the width that chose bin membership. A two-pass render still displays or
caches an answer from the wrong first rectangle unless it prepares twice, and
threshold cases can oscillate. The source-count envelope is stable.

**Use the previous guide width as the next frame's answer.** This turns frame
history into layout authority: the same source and viewport can render
differently depending on how the user arrived there.

**Re-bin only at gesture settle and permit stale hit during motion.** Degraded
paint is an acknowledged preview; stale interaction asserts a false bin
membership. Hit stays disabled until exact state exists.

**Admit temporal fields because they cast to integers.** PRODUCT-0128 made exact
temporal domains authoritative precisely because their storage representation
is not an ordinary quantitative meaning. A remote-executable temporal transform
is a separate contract.

## Consequences

Hexbin becomes geometrically correct and viewport-responsive over every source
Polariton can plot today, without waiting for deferred relations and without
turning the renderer into an aggregation authority. The existing kernel's
hard-coded data-space radius, silent preparation fallbacks, and duplicate axial
math disappear together.

The plot pipeline gains its first viewport-prepared artifact and its first split
between legend reservation and displayed legend content. Both additions are
narrow but reusable: the first is the exact seam a future deferred executor
needs; the second expresses any future guide whose honest domain is known only
after plot geometry.

The local preparer is intentionally not throwaway. It is the reference executor
and correspondence oracle for Toron. When deferred sources land, the remote path
replaces where the same request executes, not what a hexbin means.

PRODUCT-0144 can now be routine in the useful sense: it still must add the Rune
constructor, field transport, projection, compatibility, diagnostics, and live
exhibit, but it need not invent geometry or decide whether aggregation belongs
inside paint. Its spelling is `hexbin(x, y)` over raw quantitative observations,
and PRODUCT-0091 already decides the absence of a colour-job modifier.
