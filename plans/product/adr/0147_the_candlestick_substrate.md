# ADR 0147: the candlestick substrate

Status: accepted 2026-08-31. Candlestick is the last catalog kernel without
a Rune spelling, and its substrate is the least honest remaining. The
kernel samples the legacy ramp twice — bullish at `get_color(1.0)`,
bearish at `get_color(0.0)` — to encode the mark's entire meaning in
colours no record resolves. Preparation cannot fail and therefore lies: a
present x with an absent close panics with an index out of bounds, nulls
are read past the validity bitmap as garbage buffer values, no
open-high-low-close containment is checked anywhere in the tree, and a
failed cast collapses to a silently empty plot. Body width derives from
only the first two x values, so a leading weekend gap mis-sizes every
candle and a single candle renders roughly seventy times the canvas. The
hit path binary-searches unsorted data with a NaN-capable comparator and
hardcodes its x semantic as temporal. The automatic resolver contradicts
the kernel outright: it selects a *categorical* x, so a temporal OHLC
frame can never resolve while the kernel's populated path already carries
exact PRODUCT-0128 temporal domains. The kernel has zero tests.

This record repairs the substrate so PRODUCT-0148 can add
`candlestick(x, open, high, low, close)` as a routine surface: fallible
role-attributed preparation with strict ordering and containment,
**structural direction encoding** — hollow, filled, and an explicit doji
geometry, with no colour distinguishing direction on any skin — resolved
single-authority colour discharging the last CI-03-class obligation under
PRODUCT-0064, an honest width law, exact temporal x everywhere including
the empty branch, a repaired resolver, sound axis-typed hits with a
direction field, retirement of the redundant open/close ticks, and an
explicit no-legend decision. It adds no Rune word: the constructor, field
transport, and authored resolution are PRODUCT-0148's subject, and the
PRODUCT-0075 surface-absence posture stands until that record supersedes
it.

## Context

### The colour repair is inherited, and colour is the wrong carrier anyway

PRODUCT-0064's obligation binds any record that makes a new renderer
reachable, CI-03 is closed won't-do, and 0145 already named this arc as
the inheritor of its discharge shape. But candlestick's ramp defect is
deeper than fill-between's: the two samples do not decorate the mark, they
*are* the mark — up versus down is the entire visual grammar, carried in
colours the tier cannot resolve. And the tier cannot replace them with a
colour pair either: `project_mark_style` yields exactly two
distinguishable inks, and the constant black, grey, and white palettes
collapse even those (`product_0145_fill_detail_tracks_anchor_distinctness_
across_plot_skins`). 0145 ruled that collapse valid-but-ugly for
*interior detail*; up-versus-down is not detail, and PRODUCT-0080 §230-233
already ruled that a distinction invisible in export "does not exist."

The resolution is to take direction out of colour entirely. The
century-old monochrome convention — hollow bodies rising, filled bodies
falling — encodes direction structurally and needs exactly one resolved
body colour. The honest scope of that claim: no **skin** can selectively
erase direction while leaving the candles visible — the failure mode a
two-ink colour pair admits — for every body above the raster legibility
threshold §3 states; at and below that threshold, hollow and filled
coincide as an explicit *geometry* exception, owned by the raster rule,
not by any palette. Under the constant black,
grey, and white palettes, mark ink equals canvas paper and the *entire*
candle disappears — bodies, wicks, and dojis together, exactly as every
other ramp-free mark disappears there. That whole-mark collapse is those
skins' standing semantics (the 0145 amendment's class), accepted and
documented, not a direction-encoding defect and not grounds for a
candlestick-only contrast fallback, which would recreate the per-mark
colour exception this arc retired. That is this record's central
decision.

### The open/close ticks are residue, and retiring them dissolves 0083's collision

The kernel layers OHLC-bar stubs — a left tick at open, a right tick at
close, in `mark_style.decoration` — on top of a filled body that already
expresses both values as its edges. PRODUCT-0083 §269-277 deferred the
resulting shared-ink collision only because the body fill was still a
ramp colour; migrating the body to the tier would trigger it immediately
(`decoration` and `fill` both project to ink). The ticks add no
information a candle body does not carry, they are the file's only tier
consumer today, and retiring them removes the collision without a tier
decision. The architecture pin requiring `mark_style.decoration` in the
file moves to the wick, which inherits the role honestly.

### The resolver and the kernel disagree about what x is

`resolve_automatic_kernel_fields` selects candlestick's x with
`category(0, "x")` — the nominal list: String, Categorical, Enum, Boolean
— while temporal columns live in the numeric list. A proper OHLC frame
over dates therefore refuses ("requires categorical field 1 for x"), and
when a String x *is* selected, preparation's cast fails into an empty
series and the plot renders silently blank. Meanwhile the kernel's
populated path derives honest `AxisType` and exact temporal domains from
the x dtype. The kernel is right and the resolver is wrong: x is
quantitative-or-temporal, the settled grouping every recent mark uses,
and the fix follows fill-between's no-reuse template.

### Ordering and width share one root: time with holes

The gap-policy decision, settled at census review: candlestick admits
quantitative or temporal x with **honest gaps** — weekends and missing
intervals render as absence, not compression. Ordinal session-compression
is the financial-chart convention but requires ordinal-from-temporal axis
machinery the language does not have; it is deferred by name. Under
honest time, strictly increasing authored order is required exactly as
0145 requires it — the nearest-x hit has no defined answer over unordered
x — and the width law must tolerate irregular spacing rather than being
set by whichever delta happens to come first.

### No prior vocabulary constrains this record

The pre-0075 picker authored candlestick as one string row with no field
vocabulary; no ADR sketches a surface or reserves roles; no ledger entry
covers it. Two recorded facts do bind: the `mark_style_oracles` bank of
the current three-operation rendering must consciously break, which
PRODUCT-0080 §199-201 pre-authorizes ("no reachable pixels to protect");
and the kernel's `"Value"` y-label fallback is PRODUCT-0085 §62-64's
*named example* of the kernel-fallback cascade step, so changing it
carries a scoped premise update to 0085, applied with attribution, not a
silent falsification.

## Decision

### 1. Preparation is fallible, role-attributed, and planning-wired

`prepare_candlestick_data` is replaced by a fallible preparation over the
roles the surface will spell: **x, open, high, low, close**. Column-level
validation, in that role order: a missing column refuses naming the role
and field; open, high, low, and close require a quantitative dtype — a
temporal or otherwise non-quantitative column refuses with role, field,
and dtype; x admits quantitative or temporal dtypes. Row-level validation
scans every row before any structural check, roles in order within a row:
null, non-finite, and inexactly projectable cells refuse with role,
field, and 1-based row; within a cell, null before non-finite before
inexact. The inexactness rule applies to the quantitative roles and to
quantitative x; a temporal x is validated and ordered on its typed values
under 0145 §4's discipline — f64 projection is the standing paint
representation beside the exact domain, never a fault, and the
adjacent-nanosecond discriminator applies here identically.

Every preparation error carries a typed role — `X`, `Open`, `High`,
`Low`, or `Close` — and its field; row-level variants additionally carry
their 1-based row, so PRODUCT-0148 maps faults onto authored spans by
attribution. The five prepared vectors are length-equal by construction —
the parallel-vector panic class (present x, absent close) becomes
unrepresentable, not merely untested. Preparation is wired through
`validate_prepared_contract` with the retained artifact token on the
branch the authored path will inherit, and the factory constructs the
kernel only from a planning-approved relation, gaining the
`.expect("PRODUCT-0147 planning validated ...")` posture its neighbours
already carry.

### 2. Structure is checked second: strict order, then containment

After every cell is valid, a second pass walks rows in producer order:

1. **strict x order** — this row's x strictly greater than the previous
   (typed comparison for temporal x); an equal or smaller x refuses
   naming both rows;
2. **containment** — `low <= min(open, close)` and
   `max(open, close) <= high`; a violating row refuses with its row
   number and all four values.

The first failing check at the first failing row wins. Attribution is
pinned by check: an ordering failure carries the `X` role, a lower-bound
containment failure carries `Low`, and an upper-bound failure carries
`High` — so PRODUCT-0148's span mapping lands structural faults on the
argument that owns the violated bound. Equalities are
admitted everywhere the comparisons allow: `high == low` (a zero-range
row) is valid and renders as its doji geometry when open and close also
coincide there; a body edge touching a wick end is valid-but-ordinary.

A schema-valid empty relation succeeds with zero rows, renders nothing,
carries the canonical `(0, 1)` empty-domain sentinel on both axes — and,
unlike today, still derives its **axis type from the x column's dtype**:
an empty Date-x relation is an empty temporal plot, not a linear one. Its
`x_temporal_domain` is honestly `None` — an exact domain needs endpoints
no rows supply, and the axis type alone carries the identity. The
dishonest empty branch is deleted.

### 3. Direction is geometry: hollow, filled, and the doji dash

Per row, direction is the trichotomy on close versus open:

- **`close > open` — hollow.** The body renders as a stroke-outlined
  rectangle in the resolved body colour with an unfilled interior — the
  canvas shows through.
- **`close < open` — filled.** The body renders as a solid rectangle in
  the same resolved body colour.
- **`close == open` — doji.** No rectangle: a single horizontal dash at
  the shared value, spanning the candle width, in the body stroke
  treatment. The doji is an explicit third geometry, not a degenerate
  bar, and its hit direction reports `unchanged` (§7).

No colour distinguishes the three — the same resolved body colour paints
all of them, and the distinction is structural: on every skin whose mark
ink differs from its canvas, direction reads wherever a body clears the
legibility threshold below, it satisfies PRODUCT-0083's opacity invariant
trivially, and it reads in monochrome export. Under the constant palettes
the whole mark vanishes with its siblings (§Context) — no *skin* ever
selectively loses direction; only the sub-threshold raster geometry
does.
Recreating bullish/bearish semantics through colour is rejected
permanently for this substrate (a future *optional authored* colour
treatment would be its own record against this one).

**The raster rule that keeps hollow legible.** The hollow outline and
the doji dash stroke at **1.0 logical pixel**; the wick keeps its
current 3.0. A centered outline consumes interior from both edges, so
the admitted degeneracy band is stated arithmetically: a body whose
pixel height is at or below three pixels may paint effectively solid —
two one-pixel outline edges plus no guaranteed interior row — and
within that band hollow and filled coincide, banked as the floor's
honest limit. At four pixels and above, a hollow body must show at
least one interior row of canvas, gated. Non-doji bodies with
`|close - open|` projecting below one pixel keep the one-pixel floor.
None of the three widths is an authored setting; a spelling for any of
them must amend this pin first.

**The wick splits at the body.** For hollow and filled candles the wick
draws as two segments — high down to the body top, body bottom down to
low — so no stroke crosses a hollow interior and fakes a doji cross.
For a doji the wick draws full-height and the dash crosses it: the
classic doji cross is the intended rendering, stated. The open/close
ticks are retired with their stroke.

### 4. One resolved colour authority

The body — outline, solid, and dash — paints the resolved
`mark_style.fill` role; the wick paints `mark_style.decoration`. Both are
opaque per PRODUCT-0083; under the projection both are ink, which is
correct — the mark is deliberately monochrome, and every distinction it
draws is geometric. No call to `color_theme.get_color` survives in the
file. The `mark_style_oracles` three-operation bank breaks and re-banks
against the new geometry under PRODUCT-0080 §199-201's pre-authorization,
and the architecture pin on `mark_style.decoration` re-anchors to the
wick.

### 5. The width law is the minimum positive delta, and the fallback is plot-space

For two or more rows, the candle body width is **0.7 × the minimum
positive adjacent x delta**, evaluated over all adjacent pairs (typed
deltas for temporal x, projected for pixel math), floored at one pixel.
A weekend or any missing interval therefore widens nothing: the trading
cadence, not the first gap, sets the width, and dense data degrades to
one-pixel bodies as valid-but-ugly rather than refusing.

The multi-row width is a data-space quantity projected through the
**active layout's** transform — authored axis bounds and pane viewport
overrides scale candle widths exactly as they scale everything else; the
width law reads the transform, never only the prepared range.

For a single row there is no delta, and this record refuses to invent a
data-space constant — including the superficially neutral `x ± 0.5`,
which collapses to zero width for nanosecond timestamps near the present
epoch (`x - 0.5 == x` in f64), the exact projection hazard §6 exists to
avoid. The single-row width is therefore defined **directly in plot
space**: 0.7 × the active plot rectangle's width, independent of data
units and of the domain. Separately, the single-row layout domain must
be representable, and the shared arithmetic padding does **not** suffice
— it collapses at nanosecond epochs exactly as `x ± 0.5` does. The
sufficient rule already exists in the substrate family: hexbin's
degenerate-range expansion through `next_down()`/`next_up()`
(`hexbin_viewport.rs:276`), which yields strictly ordered neighbouring
floats at any magnitude. This record requires that rule — extracted as a
shared helper or applied identically — so the single-row domain's
endpoints are representable neighbours of the datum, and no width
arithmetic depends on its span. Both the plot-space width and the
representable domain are gated, including under an authored x-bounds
override.

### 6. Temporal honesty completes, and the y side simplifies

The populated path already derives honest axis types and exact
PRODUCT-0128 domains; §2 extends that to the empty branch. The y side is
quantitative only, pinned `AxisType::Linear` by construction, and the
vestigial `y_temporal_domain`/`y_axis_type` carriers on the prepared
struct are deleted, as fill-between's were. The y label leaves the
`"Value"` fallback for the fill-between joined-names pattern —
`"open / high / low / close"` — and this record applies the **scoped
premise update to PRODUCT-0085** whose §62-64 cites the old fallback as
its named example: the citation is amended with attribution to this
record, and 0085's cascade-step argument survives untouched (the kernel
fallback is still the kernel's, merely a better string).

### 7. Hit is sound, axis-typed, and self-describing

Nearest-x binary search is retained, now sound under §2's strict order
and NaN-free by §1's validation — and the interaction is **defined as a
nearest-x column hit, explicitly**: the cursor targets the nearest
candle's column at any y, the trading-chart crosshair convention, stated
rather than left to look like body geometry it is not. The acceptance
radius is `max(10 px, half the candle body width)` — the current fixed
ten pixels is dishonest against wide candles, where most of a visible
body would miss its own column. Under temporal f64 collapse the
lowest-index row of a run wins, deterministically, per 0145 §4. The hit emits `MarkFamily::CANDLESTICK`
with `HitDimensionality::Area` and reports: X through
`axis_hit_semantic_type`/`axis_hit_value` — the hardcoded temporal label
is deleted — Open, High, Low, and Close as quantitative values, and a
sixth field, **Direction**, a nominal `up`/`down`/`unchanged` mirroring
§3's trichotomy, so the hit states what the geometry drew. Reporting
`low..high` as a single `HitValue::Interval` was considered and rejected:
the classic reading wants the four scalars, and the boxplot precedent
reports summary scalars individually.

### 8. No legend, decided

Direction is structural and self-describing at the mark; the body and
wick are chrome, not a data scale. Following the IG-05 reasoning both
prior arcs cite, `legend_content` and `legend_reservation_content` remain
`None` as a stated, gated decision.

### 9. Automatic resolution is repaired and stays substrate

`resolve_automatic_kernel_fields`' candlestick arm selects **x from the
quantitative-or-temporal candidates** — `category(0, "x")` is deleted —
and **open, high, low, close from the quantitative candidates excluding
the field x consumed**, in that positional order, the fill-between
no-reuse template. The variant keeps its five required strings. The
automatic path remains test substrate, unreachable from authored
channels; PRODUCT-0148 owns the authored resolver.

### 10. What this record does not do

No Rune constructor, registry row, handler, `MarkFields` variant,
envelope variant, authored resolver, settings membership, enumeration-site
arm, or exhibit — all PRODUCT-0148. No volume slot or second y axis:
`ResolvedKernelFields::Candlestick` carries no volume and
`PlotLayout::Cartesian` has one y range; a volume overlay is refused by
name as its own record. No ordinal session-compression (§Context; named
follow-on). No series/grouping channel, no authored width, wick, or
outline settings, no per-direction colour, no `AuthoredFamilyId` —
direction is a derived predicate on close versus open, not an authored
category, so the hexbin rejection transfers, and candlestick joins the
no-identity fixture list as its fifth member.

## Implementation sequence

**Stage 0 — bank the defects.** The missing-close panic, the categorical-x
refusal against a temporal OHLC frame and the silent blank from a String
x, the first-delta width against a leading-gap fixture and the
single-candle blowout, the doji-classified-bullish colour bank, a wrong
hit under unsorted x, and the linear-labelled empty Date frame. Negative
controls, not parity promises.

**Stage 1 — validated preparation.** The fallible preparation with typed
roles, cell validation, typed-temporal x, strict order and containment,
honest empty with dtype-derived axes, the planning seam, and deletion of
the old preparation with its fallbacks and vestigial y carriers.

**Stage 2 — geometry and colour.** The direction trichotomy, tick
retirement, pinned strokes, the width law with its plot-space fallback,
resolved fill/decoration colour, oracle re-bank, architecture-pin
re-anchor. The kernel gains its first unit tests.

**Stage 3 — hit and labels.** Axis-typed X, the Direction field, collapse
determinism, the joined y label, and the scoped 0085 premise update.

**Stage 4 — resolver repair and re-bank.** The quantitative-or-temporal x
arm with no-reuse, both fixtures, and the banked positive fixtures as
PRODUCT-0148's substrate oracle.

## Acceptance gates

1. **Validation precedes structure.** A relation with both a null cell
   and unsorted x refuses on the null; role-order and within-cell
   precedence are deterministic.
2. **Errors carry typed roles.** Missing, temporal, and non-quantitative
   columns refuse with role, field, and dtype; null, NaN, both
   infinities, and inexact cells refuse with role, field, and 1-based
   row; a temporal x cell is never refused for f64 inexactness.
3. **The parallel-vector panic is unrepresentable.** A present x with an
   absent close refuses at preparation; no code path can construct
   length-mismatched prepared vectors; the banked panic control fails
   against the repaired kernel.
4. **Strict order and the discriminator.** Equal and reversed x refuse
   naming both rows; two temporal rows one nanosecond apart validate,
   order on typed values, render, and hit deterministically.
5. **Containment refuses with the row.** `low > min(open, close)` and
   `max(open, close) > high` fixtures each refuse with row and all four
   values; touching equalities render.
6. **The trichotomy is geometric, not chromatic.** Up, down, and doji
   fixtures produce three distinct operation shapes — outlined rectangle
   with split wick, solid rectangle with split wick, full wick with
   crossing dash — while their resolved colours are identical, asserted
   on an anchor-distinct skin. On a constant palette the same three
   shapes and identical colours hold, and the whole-mark ink-equals-paper
   collapse is asserted as the standing class, documented — direction is
   never selectively lost while the mark renders.
7. **The ticks are gone and the wick owns decoration.** No open/close
   stub strokes exist; the wick strokes `mark_style.decoration`; the
   architecture pin anchors to the wick; the 0083 collision class has no
   candlestick instance.
8. **Colour is resolved and singular.** No `get_color` in the file; body
   and dash paint `fill`, wick paints `decoration`, all opaque; the old
   two-sample bank is re-banked against the new geometry with the 0080
   pre-authorization cited.
9. **The width law holds against the active layout.** A leading-weekend
   fixture sizes candles from the minimum positive delta, not the first;
   an authored x-bounds override scales widths through the active
   transform; dense data floors at one pixel and renders; the single-row
   fixture draws one candle at seventy percent of the active plot
   rectangle's width over a degenerate domain whose endpoints are
   strictly ordered neighbouring floats via the `next_down()`/`next_up()`
   rule — asserted for a nanosecond-epoch timestamp, where both `x ± 0.5`
   and the shared arithmetic padding collapse — and no data-space width
   constant exists.
10. **Pinned strokes and the legibility band.** Wick 3.0, outline and
    dash 1.0 logical pixels, as named constants with no authored
    spelling; a hollow body at four pixels or taller shows at least one
    interior row of canvas; the at-or-below-three-pixels coincidence
    band is banked as the stated degeneracy.
11. **Empty is honest and typed.** An empty Date-x relation carries a
    temporal axis type with `x_temporal_domain: None` and the `(0, 1)`
    sentinel; an empty numeric-x relation stays linear; missing required
    columns on an empty relation still refuse.
12. **Hit is a stated column interaction, axis-typed and
    self-describing.** Quantitative-x and temporal-x fixtures report X
    through the axis machinery — the hardcoded temporal label is gone —
    plus Open/High/Low/Close and a Direction of `up`, `down`, or
    `unchanged` matching the drawn geometry; the acceptance radius is
    `max(10 px, half the body width)`, discriminated by a wide-candle
    fixture whose body edge hits its own column; collapse resolves to
    the lowest-index row.
13. **No legend, stated.** Both legend methods return `None`, with this
    record cited.
14. **The resolver selects time and reuses nothing.** A temporal-x OHLC
    frame resolves automatically; an all-quantitative frame assigns x
    first and open does not re-select it; `category(0, "x")` is absent.
15. **The 0085 premise update is scoped.** 0085's citation names this
    record and the new fallback string; no other 0085 sentence changes;
    the joined y label renders.
16. **No premature surface.** Callable registry, `MarkFields`,
    `SpannedMarkFields`, settings membership, and the four enumeration
    sites contain no candlestick; the PRODUCT-0075 posture stands until
    PRODUCT-0148.
17. **Planning owns preparation.** A resolved candlestick validates
    through `validate_prepared_contract` with the retained token via the
    automatic substrate; the factory constructs only from an approved
    relation, with the expect posture its neighbours carry.
18. **Full battery.** Polariton unit, architecture, trybuild, Rune
    compiler, GUI, and workspace checks pass with no new ignored or
    tolerated failure.

## Guardrails and stop conditions

- If a hollow body at four pixels or taller cannot show interior canvas
  under the one-pixel outline, halt; the at-or-below-three-pixels band
  is the only admitted coincidence.
- If whole-mark visibility under constant palettes is wanted, that is a
  PRODUCT-0080 contrast-policy amendment for every ramp-free mark, not a
  candlestick fallback.
- If typed temporal ordering cannot reach the structural pass without
  projecting through f64 first, halt (0145's guardrail, verbatim).
- If the 0085 premise update cannot stay scoped to the citation, halt
  and negotiate the amendment separately.
- If any consumer depends on the retired ticks, the first-delta width,
  the doji-as-bullish classification, or the categorical-x resolver arm,
  that is its own finding — surface it, do not accommodate it.
- Volume overlays, ordinal compression, authored widths or strokes,
  per-direction colour, and a series channel each reopen a named
  boundary as a separate record.

## Rejected

**Colour-encoded direction.** The tier offers two inks that constant
palettes collapse to one, and up-versus-down is the mark's entire
meaning — 0080's export rule says an invisible distinction does not
exist. Structural encoding costs nothing at authoring time and no skin
can collapse it: above §3's raster threshold, direction reads wherever
the candles do — the selective erasure a colour pair admits is
impossible, and the only coincidence is the stated sub-four-pixel
geometry exception, which no palette choice can cause or cure.

**Alpha or hover as the direction carrier.** PRODUCT-0083 forbids
non-opaque roles, and 0080 §230-233 already refused hover-only
distinctions as invisible in export.

**Keeping the open/close ticks.** OHLC-bar residue on a candle body:
geometrically redundant with the body's own edges, and the sole source of
the 0083 shared-ink collision here.

**Uniform-spacing refusal.** Weekends are the headline use case, not a
data defect; refusing irregular cadence refuses the mark's purpose.
Honest gaps with a minimum-delta width serve it.

**First-delta or fixed data-space width.** The first delta is an
accident of row one (banked pathologies); a fixed data-space constant is
meaningless across domains and was explicitly refused at review — as was
`x ± 0.5`, whose f64 collapse at nanosecond epochs makes it a data-space
constant in disguise. The single-candle width is plot-space directly.

**Ordinal session compression now.** The convention is real, the
machinery (ordinal-from-temporal axes) is not; one mark may not smuggle
in a new axis identity. Deferred by name.

**An `Interval` hit for low..high.** Available and unused deliberately:
the classic reading wants four scalars plus direction, and collapsing the
wick to an interval would orphan the open/close pairing.

**Auto-sorting x.** 0054's and 0145's reasoning, verbatim: ordering is
relation-pipeline work, and the refusal names the defect.

## Consequences

PRODUCT-0148 inherits the last substrate with nothing to apologize for,
and the five-station surface it must add — registry, handler, transport,
resolver, exhibit under the live-drive standard — closes the PRODUCT-0075
restoration list entirely. The 0064 colour obligation is discharged for
the final time in this catalog; no kernel samples the legacy ramp for
marks any longer. Direction-as-geometry gives the language its first mark
whose core semantic no skin can selectively erase: above the stated
raster threshold, wherever a candle renders its direction renders with
it; the sub-threshold coincidence is a geometry exception every palette
shares; and the constant palettes that swallow the whole mark swallow it
no differently than every ramp-free sibling — the census question that
opened this arc, answered by tying direction to visibility rather than
by tolerated degradation of the distinction itself.
