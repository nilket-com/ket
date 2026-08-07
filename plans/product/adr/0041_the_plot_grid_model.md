# ADR 0041: the plot grid model

Status: accepted 2026-08-06. This record decides one thing: that a figure may
contain several plot areas coordinated by a per-axis domain group, and that the
accepted capability is the smallest case — one body repeated over one shared
domain. It adds **no Rune surface**; the spelling is PRODUCT-0042.

## Context

The motivating images were seaborn's `jointplot` and `pairplot`, and neither is a
facet. The family is broader, and its members coordinate differently:

| | cells determined by | body per cell | domain sharing |
|---|---|---|---|
| value facet | a field's values | one body, repeated | one domain, all cells |
| pair grid | variable pairs | differs per cell | per column (x), per row (y) |
| joint grid | main plus margins | differs per position | margin shares one axis with main |

Naming this record after facets would name the general thing after its smallest
case, which is how `y_ticks` came to mean intervals. The general thing is a grid
of plot areas with a coordination rule; a value facet is its first accepted rule.

**Three premises are already settled and are banked here rather than re-argued.**

PRODUCT-0007 §5 places `Facet` as a plot-side stage *inside* one specification —
`AuthoredPlotSpec` contains the plot-side stages — so a grid lives inside a
figure rather than containing figures. PRODUCT-0016 §1 committed to the same
branch in passing when it chose the word: `figure` "survives facets, where one
figure will contain several plots." This record honors that sentence as a
decision rather than outgrowing it as a prediction.

**A grid is one view.** One `ViewIdentity`, one `HostPanelId`, one panel, N plot
areas within it. The alternative — one view per cell — would make view identity
*data-dependent*, and PRODUCT-0010 §3 makes identity named-or-positional from
source, PRODUCT-0029 §1 keys palette positions by first sighting, and
PRODUCT-0015 §5 makes group membership the latest analyzed view set. A panel
count that comes from the data breaks all three simultaneously.

**The statistics are not plot-side.** PRODUCT-0007 §5: binning, aggregation, and
calculated fields "lower into relation operations before a specification exists."
A histogram is `bar` over a binned frame and a KDE is `line` over a density
frame, so both are spellable today once the relation supplies the verbs. What the
motivating images need from this side is marginal *placement*, which is this
record, not new marks.

## Decision

### 1. A grid is N plot areas in one figure

A figure's body resolves to a `PlotGrid` of one or more cells. An ungridded
figure lowers to a one-cell grid and renders identically to today.

### 2. Coordination is one domain group per axis per cell

```text
PlotGrid  { cells: NonEmpty<GridCell>, arrangement }
GridCell  { position, body, data, x_group: DomainGroupId, y_group: DomainGroupId }
```

`data` is the cell's resolved data slice. How a slice is produced belongs to
the coordination rule (§5), not to this representation.

Cells naming the same group on an axis resolve **one** domain for that axis,
computed once across every cell in the group.

This single integer expresses every member of the family:

```text
value facet   all cells -> x:0, y:0
pair grid     cell (i,j) -> x:j, y:i
joint grid    main and top -> x:0;  main and right -> y:0;
              each margin owns the other axis
free scales   every cell -> its own group on both axes
```

A coordination *language* was rejected in favor of one identifier per axis: it is
the least mechanism that expresses all four, and everything deferred below is a
relaxation of one clause over this unchanged representation rather than a change
of shape — PRODUCT-0013 §2's maneuver, applied to sharing instead of encoding.

### 3. The accepted capability is one group and one body

> Every cell names group `0` on both axes, and every cell carries the same body.

That is a value facet, and it is all this record accepts.

### 4. What is representable and refused

Both perimeter cases are expressible in §2's representation and refused by §3's
clause, each naming what it refused on:

- **differing domain groups** — the offending axis and the groups found;
- **differing per-cell bodies** — the offending cell.

**Free scales are not a separate promise.** They are the every-cell-its-own-group
case, so they are representable today and refused by the same clause. Relaxing
§3 admits pair grids, joint grids, and free scales together rather than three
times.

This is the record's load-bearing sentence, and the difference between value
facets being a foundation and being a ceiling.

### 5. Cell production belongs to the coordination rule

The representation receives cells; it does not manufacture them. Pair grids
produce cells from variable pairs and joint grids from main-plus-margins, so a
`PlotGrid` that carried a partition field would weld value-facet semantics into
the general shape — relaxing §3 would then change the representation rather
than one clause, which is what §2 exists to prevent.

This record therefore defines production for the accepted rule only. A value
facet partitions a named field over the stamped result: one cell per distinct
value, each cell receiving exactly the rows of its value as its `data` slice.
The rule supplies the slice; the representation only carries it. A pair grid
would hand every cell the same frame with different bodies, and a joint margin
a derived series — which is why the slice, not a partition value, is the
general field.

This does not disturb PRODUCT-0010's shared-result guarantee. `Arc::ptr_eq`
holds at sink level; a grid is one sink, and its cells hold derivatives of the
stamped result the sink already shares.

**Cell order is the display domain's order, re-derived at each resolution.**
This deliberately diverges from PRODUCT-0029 §1's sticky continuation, and the
difference is authored intent: nothing in a buffer asks for a color to change,
so recoloring on rerun would be a change nobody requested — but
`.sort([col("cat").desc()])` is an authored statement about order, and an
author who re-sorts expects the panels to follow. Both rules are stable under
filtering, which removes a cell and preserves the remaining relative order;
they differ only under re-sorting, where the display domain is right.

### 6. One shared domain resolves across all cells, not per cell

Under §3 the domain is computed once over every cell's data, not per partition.
A partition whose local range is narrower still renders against the shared
domain.

That is the property that makes panels comparable, and it is why a grid is worth
having at all. It extends PRODUCT-0013 §5's "one shared Cartesian domain" to a
second partition axis without changing its meaning.

### 7. The rendering boundary is one grid composite

The composite implements `PlotKernel`, so every existing consumer — the widget,
the SVG backend, export, panel content — is untouched: PRODUCT-0013 §3's move,
applied one level up. It owns exactly two things: realizing the arrangement it
is given as per-cell regions, and the per-cell coordinate transforms over the
one shared resolved domain. Each cell draws through the existing single-plot
machinery inside its region; no complete kernel is nested per cell.

Every cell draws its full chrome — axes, ticks, and tick labels — in this cut.
Suppressing interior labels on shared edges is a visible design choice for a
later record, not an implementation judgment call.

Hit geometry routes a point to the cell whose region contains it — arithmetic
only the composite can perform, and authorized here. What a hover *means* once
routed — ranking, wording, cross-cell preference — is interaction policy and
remains with PRODUCT-0013 §3's existing bridge.

### 8. The body becomes a template

Today a figure's body is one plot. Under a grid it is drawn **once per cell**.

This is the only place a grid changes an existing meaning, and it is stated
loudly because every figure member's documentation currently reads as though a
figure has exactly one plot. The eleven existing members remain figure-wide and
keep their meanings exactly — one skin, one palette, one bounds pair, one tick
lattice, shared across cells, which is what comparability requires.

### 9. Cell count is bounded

A grid whose cell count exceeds a declared limit refuses, attributed, naming the
partitioning field, the observed count, and the limit — the field because the
accepted rule has one, not because every grid does. No truncated grid, no
dropped cells.

The declared limit is **512 cells**, and the number is chosen to make the next
sentence true rather than false: a capacity limit bounds work that would
otherwise not terminate, and it is not a value threshold on anything an author
may look at — PRODUCT-0007 §8's and PRODUCT-0037 §6's rule, applied to a third
quantity. A limit picked for legibility — two dozen cells, say — would refuse
something that renders correctly but looks bad, the posture PRODUCT-0034 and
PRODUCT-0035 removed. A 200-cell grid is unreadable, and the author is
entitled to look at it.

### 10. Empty partitions produce no cell

Cells come from the data present. A partition with no rows after filtering does
not appear, so cell count varies with the data.

That is safe **because** of §1: identity is per view, not per cell, so a changing
cell count moves no address. It is recorded as a consequence of the identity
decision rather than as an independent rule.

An empty relation — every partition absent — yields §1's degenerate one-cell
grid, rendering exactly what an ungridded figure renders today over an empty
relation. `NonEmpty` remains true by construction, and the empty case falls
out of an existing rule rather than acquiring one of its own.

### 11. Faceting by the colored field renders

Partitioning by the same field an authored color channel or band fill names is
admitted: every cell renders in its partition's single color. That is defined,
so it renders — no refusal, no warning, no substitution, no special case, per
PRODUCT-0030 §4 and PRODUCT-0035 §1. The result is often ugly, and it is the
author's to see.

### 12. Panel labels are a bridge

A cell needs its partition value shown or the grid is unreadable. This record
authorizes the minimum to make cells identifiable and nothing more.

> Panel labelling here is authorized **only** to make cells identifiable. It
> carries no compatibility guarantee, is not an acceptance gate, and decides no
> Guide behavior. Placement, formatting, templating, and interaction with the
> legend belong to the Guide record, which replaces this bridge.

This is the ninth live bridge and the Guide cluster's fifth consumer, after
legend content, swatch sizing, band-fill legend, and legend merging. Recording it
now means the Guide record arrives with its consumers enumerated rather than
discovering one.

### 13. Grid shape is not decided here

The substrate realizes an arrangement it is given. Who chooses rows and columns —
kernel from cell count and aspect, or author-stated — is a spelling question for
PRODUCT-0042, with PRODUCT-0015 §2's split (engine owns geometry, frontend
requests) as the precedent one level in.

### 14. No Rune surface

Nothing here is reachable from a buffer. `PlotGrid` with more than one cell has
no production caller until PRODUCT-0042 — the sixth substrate cut in this arc to
ship test-only, at the same accepted cost.

### 15. What this record does not authorize

No Rune syntax. No pair or joint grid. No free scales. No per-cell bodies. No
cell production rule beyond §5's value-facet rule. No interior-chrome
suppression. No new mark, no statistical mark, no relation verb. No Guide
decision beyond §12's bridge. No grid shape policy. No change to identity, host
panel groups, placement, lifecycle, palettes, skins, chrome, bounds, ticks, or
any existing kernel's rendering.

## Acceptance gates

1. A one-cell grid renders **byte-identically** to the equivalent ungridded
   figure, measured against a baseline banked before any refactor.
2. A four-cell value-facet grid over one shared domain paints four **disjoint**
   regions with the expected geometry, **each containing exactly its own
   partition's data**, observed through rendered output — not by inspecting the
   cell vector. Correct geometry with shuffled partition assignment, and
   correct partitions in overlapping regions, must each fail this gate.
3. The shared domain is resolved once across all cells: a partition whose local
   range is narrower than the whole still renders against the whole's domain,
   asserted by comparing a cell's axis range to the global one.
4. Every cell draws an identical major and minor tick lattice, asserted by
   comparing rendered tick positions across cells. A cell deriving positions
   from its local extent is PRODUCT-0037 §4's defect reappearing one level up,
   and it produces a grid that looks plausible until two panels are compared.
5. Differing `x_group` or `y_group` values across cells are constructible and
   refuse under §3, naming the axis and the groups found.
6. Differing per-cell bodies are constructible and refuse under §3, naming the
   cell.
7. Every-cell-its-own-group — free scales — refuses with the **same** diagnostic
   as gate 5, proving one relaxation rather than three.
8. A cell count exceeding the declared 512-cell limit refuses, attributed with
   field, count, and limit; no truncated grid is produced. A large admissible
   count — hundreds of cells — renders rather than refusing, proving the limit
   is a work bound and not a legibility judgment.
9. Value-facet cells appear in the display domain's order: re-sorting the
   relation reorders the cells, and filtering removes only the filtered cell
   while preserving the remaining relative order. A control asserts colors stay
   sticky through the same edits, pinning §5's deliberate divergence from
   PRODUCT-0029.
10. An empty partition produces no cell, and the view's identity, `HostPanelId`,
    and group membership are unchanged across a filtering edit that changes the
    cell count.
11. The same category resolves to the same colour in every cell, without a new
    rule — PRODUCT-0029's lineage is keyed by field and category and is
    panel-blind by construction.
12. Partitioning by the field an authored color channel or band fill names
    renders one single-colored cell per partition, with no refusal, warning, or
    substitution.
13. Panel labelling is covered by tests marked as **bridge coverage**, recorded
    in evidence as non-gating.
14. No Rune grammar, identity, group, placement, lifecycle, palette, skin,
    chrome, bounds, tick, or kernel behavior changes, and no production path
    constructs a multi-cell grid.

## Stop condition

> This record may add the `PlotGrid` representation and group resolution, the
> §5 value-facet production rule, the grid composite with per-cell transforms
> and containing-cell hit routing, shared-domain resolution across cells, cell
> capacity, the §12 label bridge, and the one-cell parity path. If the
> implementation requires deciding what a partially shared domain means, a
> Guide decision beyond §12's bridge, a Rune surface, a grid-shape policy, a
> change to any existing kernel's rendering, or identity, placement, group, or
> lifecycle work, the work stops and the requirement returns as its own record.

The expected trigger is scale composition. The moment two cells need different
domains on one axis, §3's clause is being relaxed — and that is a record, not
an implementation convenience.

## Rejected

- **Naming this record "facets":** names the general thing after its smallest
  case. A value facet is one coordination rule; the substrate is the grid.
- **One view per cell:** makes view identity data-dependent and breaks
  PRODUCT-0010, PRODUCT-0015, and PRODUCT-0029 at once. It is the only shape that
  would let a cell be closed or dragged individually, and that capability is not
  worth a dynamic address.
- **A coordination language instead of a group identifier:** more expressive than
  any known consumer needs, unfalsifiable against the four cases in §2, and
  impossible to gate.
- **A partition field in `PlotGrid`:** welds the smallest case's semantics into
  the general representation, so relaxing §3 would change the shape rather than
  one clause — the exact mistake the naming argument warns against.
- **Sticky cell order through continuation state:** makes panels ignore an
  authored re-sort. Order is what `.sort` is *about*; color is not, which is why
  PRODUCT-0029 keeps stickiness and §5 does not.
- **One kernel per cell:** composes layout, legend, and hover N times — the
  defect PRODUCT-0013 §3 rejected, multiplied by cell count. The composite
  realizes regions; it does not nest complete kernels.
- **Accepting per-cell bodies now:** value facets do not need them; pair and joint
  grids do, and both are behind scale composition regardless.
- **Per-partition domain resolution:** destroys the comparability that is the
  entire reason to draw a grid, and would make each cell an independent plot with
  a shared border.
- **Deciding grid shape here:** spelling, per §13.
- **Making panel labels an accepted guide:** the Guide record decides guides, and
  §12's bridge exists so the grid is legible until it does.

## Consequences and sequencing

- PRODUCT-0042 is the surface: one figure member, the twelfth, plus a grid-shape
  policy. Small, because this record carries the semantics.
- **Scale composition's trigger sharpens.** Accepting a pair grid, a joint grid,
  or free scales relaxes §3, and each requires deciding what a partially shared
  domain means. That is the deferred scale-composition decision, and it is now
  reachable by three distinct requests rather than one.
- The Guide record acquires its fifth consumer and its scope becomes concrete:
  legend content, swatch sizing, band-fill legend, legend merging, and panel
  labels.
- The arc gains a ninth live bridge, and it is the first one authorized with its
  replacement already scheduled.
- Histograms and kernel densities become spellable with no plot-side record at
  all, the moment the relation supplies `bin` and `density` — which is upstream of
  this boundary and outside this record's scope.
