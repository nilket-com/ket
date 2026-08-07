# ADR 0042: the facet figure surface

Status: accepted 2026-08-06. This record decides two things, because
PRODUCT-0041 §13 assigned it both: the Rune spelling that reaches the grid
substrate's accepted rule, and the arrangement policy the head supplies. It
invents no grid semantics — coordination, production, capacity, empty
partitions, colored-field admission, and the label bridge were all decided by
PRODUCT-0041 and are inherited unchanged.

## Context

PRODUCT-0041 landed the plot grid model and shipped it test-only: value facets
are proved through fourteen gates, `PlotGrid::value_facet` (`grid.rs:221`)
partitions a named field and hands each cell its slice, and no production path
constructs a multi-cell grid. That was the agreed cost of the substrate/surface
split, paid for the sixth time. This record pays it back.

Everything the spelling needs already exists. Figure children are recognized
contextually, the literal grammar is fixed, and PRODUCT-0040 resolves bound
names for figure elements. What remains is one name, one lowering, and the one
decision the substrate deliberately refused to make: who chooses rows and
columns.

## Decision

### 1. `facet(field)` is a figure element

```rune
figure([
    title("Revenue by region"),
    palette("tab20c"),
    facet("region"),
    bar("month", "revenue"),
])
```

`facet` joins the metadata group of PRODUCT-0016 §2 as the twelfth member,
recognized only as a direct element of a proven `figure([...])` sink. Outside
proven positions the call remains ordinary Rune under PRODUCT-0008 §3.

The name is the PRODUCT-0007 §5 stage whose first accepted rule it spells.
The surface names the rule, not the model: the substrate is a grid, but what
an author writes today is a value facet, and `grid("region")` would promise
the general capability while delivering its smallest case — the inverse of the
naming mistake PRODUCT-0041 avoided.

### 2. One field, in the established literal grammar

`facet` takes exactly one string literal and appears at most once. Duplicates
refuse. A bound name resolves under PRODUCT-0040 §1's rule — this element
joins §6's coverage list, so `let by = "region"; facet(by)` resolves and the
refusal for a wrong-kind or unresolvable binding names the binding and its
span, per PRODUCT-0040 §4.

### 3. The normal form carries the field, and faceting is not a setting

The figure intent gains one structural field carrying the authored partition
field identity. No `PlotGrid`, cell, slice, arrangement, or region crosses the
envelope; the head constructs `PlotGrid::value_facet` at sink completion, per
PRODUCT-0041 §5.

Faceting deliberately does **not** enter PRODUCT-0007 §7's settings cascade.
Every cascaded property styles what is drawn; `facet` names a field in the
author's data and changes what the artifact *is* — a template drawn once per
partition. A tier that could supply it — an application theme partitioning a
user's figure, a pane override refacting it — would let presentation layers
restructure an artifact whose data they know nothing about. Facet is intent,
like the body, and only the buffer can state it.

### 4. Refusal follows the two-phase split

**At lowering**, in the envelope (PRODUCT-0038 §3's first phase): wrong arity,
a non-literal argument or failed binding resolution, a duplicate `facet`
element, `facet(...)` inside `layers([...])`, and `facet(...)` as the figure
body each refuse at their own span.

**At resolution**, destination-local: a field absent from the stamped result's
schema refuses naming the field and the element's span — the shape of
PRODUCT-0007 §2's channel refusal, applied to a partition — and a partition
count exceeding PRODUCT-0041 §9's 512-cell work bound refuses with the
substrate's attributed diagnostic at the element's span. Siblings over the
same origin complete; the failed destination retains its previous complete
grid stale.

**No field type refuses.** Partitioning by distinct values is defined for
every dtype — categorical, numeric, temporal. A float field of all-distinct
values produces one cell per row and meets the capacity bound honestly.
Refusal means there is no defined answer, and a defined-but-enormous partition
is what the work bound is for.

### 5. The head derives the arrangement; nothing is authored

For `n` cells:

```text
rows    = floor(sqrt(n))
columns = ceil(n / rows)
```

filled row-major in PRODUCT-0041 §5's display-domain cell order, the last row
partially filled when `n < rows × columns`. One cell is the degenerate 1×1,
three tile 3×1, four tile 2×2, five tile 3×2, seven tile 4×2. The rule is
stated exactly so it is testable, and it is wider-than-tall by construction
rather than by claim: `columns = ceil(n / floor(sqrt(n))) ≥ ceil(sqrt(n)) ≥
rows` for every `n`, matching the panel aspect a typical figure has to spend.
(An earlier draft used `columns = ceil(sqrt(n))`, which produces a square with
an empty slot at three cells and two empty slots at seven — contradicting the
wider-than-tall intent at exactly the commonest small counts.)

No authored shape ships in this cut. PRODUCT-0015 §2's split holds one level
in — the composite owns geometry, an author may eventually request — but a
rows/columns request is a spelling with no felt miss yet, and the set grows
when a use exists, not in advance of one (PRODUCT-0011 §1). A future record
may add an arrangement request; it inherits this policy as the default.

### 6. The body is the template, and every member composes

The faceted body may be anything a figure body may be: a bare mark, a styled
mark, or a `layers([...])` composition. Each cell draws the same body over its
slice — PRODUCT-0041 §8's template rule reaching source.

All other figure members keep their figure-wide meanings exactly: one shared
domain (bounds apply once, per PRODUCT-0041 §6), one tick lattice identical
across cells, one palette, one skin, one heading pair. Faceting by the field
an authored color channel or band fill names renders every cell in its single
color, with no refusal or warning — PRODUCT-0041 §11, now reachable from a
buffer.

The same posture covers the overlap an author is most likely to hit by
accident: faceting by a field that is also a mark's x or y field.
`facet("month")` with `bar("month", "revenue")` gives every cell exactly one
bar, because each cell's slice contains a single value of the mark's own
domain. That is defined — the shared domain still spans every partition, each
cell draws its slice against it — so it renders. It degenerates the mark's
domain rather than its coloring, which is why it is stated here beside the
color case instead of inherited silently from it.

### 7. Identity, lifecycle, and placement do not change

A faceted figure is one view: one `ViewIdentity`, one `HostPanelId`, one panel,
one position in presentation order, one tile in its producer group. Adding,
changing, or removing `facet(...)` in the same named or positional slot reuses
all of them — content, not identity, per PRODUCT-0016 §9 and PRODUCT-0030 §11.
A filtering edit that changes the cell count moves no address (PRODUCT-0041
§10), and a re-sort reorders panels inside the one panel (PRODUCT-0041 §5)
without touching group geometry.

### 8. What this record does not authorize

No pair-grid, joint-grid, or free-scale spelling. No per-cell body. No
authored arrangement. No panel-label formatting or placement — the
PRODUCT-0041 §12 bridge stands until the Guide record. No interior-chrome
suppression. No substrate, kernel, capacity, or production-rule change. No new
mark, no relation verb, no change to identity, placement, groups, lifecycle,
or any existing figure member.

## Acceptance gates

1. `figure([facet("region"), bar("month", "revenue")])` lowers to one figure
   intent carrying exactly the authored field and completes through
   `PlotGrid::value_facet` into one host panel — the motivating case, end to
   end, in the real head.
2. `let by = "region"; facet(by)` resolves identically to the inline literal;
   a wrong-kind and an unbound name each refuse at the argument's span naming
   the binding, per PRODUCT-0040 §4.
3. Each lowering refusal fires at its own span with a distinct diagnostic:
   wrong arity, non-literal argument, duplicate `facet`, `facet` inside
   `layers([...])`, and `facet` as the figure body.
4. A field absent from the resolved schema refuses destination-locally, naming
   the field and the element's span; siblings over the same origin complete,
   and the destination retains its previous complete grid stale.
5. A partition count above 512 refuses through the Rune path with the
   substrate's attributed diagnostic; a large admissible count renders.
6. The arrangement rule is exact: one, three, four, five, and seven cells
   produce 1×1, 3×1, 2×2, 3×2, and 4×2-with-one-empty-slot, filled row-major
   in display-domain order, and `columns >= rows` holds in every case.
7. Re-sorting the relation reorders the panels end to end through the Rune
   path; filtering removes only the filtered panel and preserves the remaining
   relative order, while colors stay sticky — PRODUCT-0041 gate 9's divergence,
   observed from source.
8. A `layers([...])` body facets: every cell draws the full layered
   composition over its slice in authored paint order.
9. Faceting by the authored color or band-fill field renders one
   single-colored panel per partition from source, with no refusal or warning.
10. A figure with no `facet` element lowers through the one-cell path and
    renders byte-identically to today, measured against a baseline **banked
    before the routing change lands**. This gate guards more than it reads:
    PRODUCT-0041 gate 1 proved one-cell parity for a test-only code path,
    while this record routes every unfaceted figure in the product through
    it — a padding change in the one-cell path would shift every existing
    plot in the arc. Every existing figure member, modifier, sink, identity,
    placement, and group behavior is unchanged.
11. Adding, changing, and removing `facet(...)` in the same slot reuses the
    view's identity, `HostPanelId`, group membership, tile, and labels.
12. `../uat/test14.rn` — the arc's next dogfood fixture, created with this
    record — parses, completes, and renders a faceted figure beside its
    existing siblings.

## Stop condition

> This record may add the `facet` element's recognition and refusals, binding
> resolution coverage, the one structural lowering, head-side
> `PlotGrid::value_facet` construction at completion, the §5 arrangement
> policy, and the `test14.rn` fixture. If the implementation requires changing
> the substrate's representation, production rule, capacity, or composite; a
> scale-composition decision; a Guide decision beyond the existing
> PRODUCT-0041 §12 bridge; an authored arrangement; or identity, placement,
> group, or lifecycle work, the work stops and the requirement returns as its
> own record.

The expected trigger is arrangement pressure: the first grid whose derived
shape fights its content is evidence for an authored request, and that is a
record, not a tuning constant.

## Rejected

- **`grid("region")`:** promises the general model while delivering its
  smallest case — the inverse of PRODUCT-0041's naming argument. When pair or
  joint grids arrive, each is its own rule with its own spelling over the same
  substrate.
- **`col(...)` / `row(...)`:** collides with the relation column function
  `col("x")`, the most-typed name in the language. Contextual recognition
  would disambiguate mechanically and no reader would.
- **`facet_wrap(...)`:** borrows a name whose second half describes an
  arrangement policy this cut deliberately does not author.
- **An arrangement argument (`facet("region", 3)`):** an accumulating option
  with no felt miss, and it would put geometry in the language before the
  derived default has produced evidence of a wrong guess.
- **Faceting as a mark modifier:** a per-mark facet is a per-cell body, which
  PRODUCT-0041 §3 refuses; figure level is forced, not chosen.
- **Faceting as a cascade setting:** would let an application theme or pane
  override repartition an artifact whose data it cannot know. Facet is
  structural intent; only the buffer states it.
- **Refusing high-cardinality or non-categorical fields:** a value threshold
  on something an author may look at. The 512-cell work bound is the only
  admission rule, per PRODUCT-0041 §9.

## Consequences and sequencing

- The two-record pattern closes again: PRODUCT-0041's substrate acquires its
  production caller, and the seaborn arc's grid leg reaches source.
- The figure gains its twelfth member, and the first structural one — every
  prior member styles or bounds the picture; this one multiplies it.
- Panel labels become user-visible for the first time, which makes the
  PRODUCT-0041 §12 bridge's replacement schedule concrete: the Guide record
  now has a visible consumer in production rather than five enumerated
  obligations on paper.
- Authored arrangement, pair grids, joint grids, free scales, interior-chrome
  suppression, and label formatting remain unaccepted, each behind its named
  trigger.
- `../uat/test14.rn` becomes the fixture that shows a faceted, palette-driven,
  bounded, tick-authored figure in one buffer — the arc's first artifact that
  uses a member from every register era at once.
