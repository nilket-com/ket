# ADR 0145: the fill-between substrate

Status: accepted 2026-08-30. Fill-between is one of the last two catalog
kernels without a Rune spelling, and its substrate cannot honestly receive
one. The kernel samples the legacy `ColorTheme` ramp at paint time, its
preparation is infallible with silent fallbacks — an absent middle column
becomes a synthetic zero baseline and strokes a spurious line along `y = 0` —
its hit-testing binary-searches x and answers wrongly on unsorted input, a
crossed band silently mis-bounds the y-range, and the composed
fill-plus-line kernel draws the same middle series twice. No test covers any
of it beyond one hit-priority fixture that constructs the composite directly.

This record repairs the substrate so PRODUCT-0146 can add
`fill_between(x, lower, upper)` with a trailing optional `middle` as a
routine surface: validated fallible preparation with an optional middle by
type, strictly increasing x, ordered bands, resolved mark-style colour for
both the fill and the middle stroke, exact temporal x domains, an explicit
no-legend decision, and retirement of the double-drawing composite kernel
together with its composition admission. It adds no Rune word: the
constructor, field transport, and channel resolution are PRODUCT-0146's
subject, and the surface-absence posture from PRODUCT-0075 stands until that
record supersedes it.

## Context

### The colour repair is an inherited obligation, not a choice

`fill_between.rs:44-49` derives both colours by sampling the legacy ramp —
`settings.color_theme.get_color(0.9)` for the middle stroke and
`get_color(0.1)` with a forced alpha for the band. That is the defect class
PRODUCT-0029 §6 retired and PRODUCT-0054 closed for every then-authorable
mark; PRODUCT-0064 re-scoped that closure from a count to a boundary: any
record that makes a new renderer reachable inherits the obligation to prove
it consumes resolved colour. CI-03, the ledger item that once covered the
remaining ramp consumers in bulk, was closed won't-do on 2026-08-19 — no
pending record will do this repair on the surface's behalf. It therefore
lands here, before the word exists, so PRODUCT-0146 inherits a renderer that
is already honest. Candlestick's copy of the same defect stays with the
candlestick arc, which will cite this record as precedent.

### The kernel is four-column and the composite is vestigial

`FillBetweenPreparedData` (`fill_between.rs:171-184`) carries
`x, y_mid, y_lower, y_upper` plus axis types, temporal domains, and labels.
The kernel itself draws the band and then a 2.5-pixel stroke over `y_mid`
(`fill_between.rs:68-85`). The separate `FillBetweenLinePlotKernel`
(`fill_between_line.rs`) exists to overlay a line — but the factory
synthesizes that overlay from the same `middle` column
(`plot/factory.rs:326`), so the composed form draws the middle series twice:
once as the kernel's own stroke, once as the overlay. The composite also
takes axis types and temporal domains only from the fill side, silently
discarding the line's.

The composed form was never declarative. The retired dashboard authored it
imperatively — pick fill-between, click add-layer — which is why
`PlotComposition::add_layer` reorders marks and `can_add_layer_type` admits
the `Line`/`FillBetween` pairs. No Rune word has ever built a multi-layer
`PlotComposition`; `raw_mark` hardcodes `single`. A kernel that duplicates
behaviour the primary kernel already has, reachable by nothing, drawing its
one distinctive series twice, is not useful substrate. It is retired here,
and the optional middle stroke becomes the one authority for that series.

### Preparation lies where 0090 and 0143 refuse

`prepare_fill_between_data` cannot fail. A missing or empty middle column
falls back to `vec![0.0; n]` (`fill_between.rs:241-243`) — the synthetic
baseline that strokes `y = 0` through a band that never mentioned it. Nulls,
non-finite values, and inexact projections pass through silently. The
y-range is computed as `min(lower)..max(upper)` (`:294-295`), which is
simply wrong wherever the series cross. Hit-testing assumes sorted x
(`:100-113`); on unsorted input it reports membership for the wrong row —
not an ugly answer, a false one. PRODUCT-0090 established required-channel
discipline and PRODUCT-0143 §5 established validate-everything-first; this
substrate joins them.

### Ordering is a refusal here, deliberately unlike line

PRODUCT-0054 refused to auto-sort line input because point order is authored
intent — a phase plot is a legitimate line. A band is different: fill-between
asserts, by name, a vertical interval over a horizontal axis. Over
non-monotone x the fill path self-intersects and the nearest-x hit has no
defined answer, and "refusal means no defined answer" is exactly the
doctrine's cut. The divergence from 0054 is recorded as deliberate, with the
hit contract as the reason: line's hit does not depend on x order;
fill-between's does.

### No ledger entry reserves this surface

Unlike hexbin, no deferral ID covers fill-between; the closest entry, IG-05,
concerns categorical axis-band fills from PRODUCT-0029/0030 and was resolved
on the reasoning that a mark adding no colour encoding adds no legend item.
This record opens the ground explicitly and borrows IG-05's reasoning for
its own no-legend decision rather than inheriting silence.

## Decision

### 1. Preparation is fallible, validated, and optional-middle by type

`prepare_fill_between_data` is replaced by a fallible preparation with the
role order the surface will spell: **x, lower, upper, and a trailing
optional middle**. The prepared shape carries `y_mid: Option<Vec<f64>>` —
optionality by type, never by empty-vector convention. The zero-baseline
fallback is deleted; there is no code path that manufactures a middle
series.

Column-level validation, in role order x → lower → upper → middle:

- a missing column refuses, naming the role and field;
- lower, upper, and middle require a quantitative dtype; a temporal or
  otherwise non-quantitative column refuses, naming the role, field, and
  dtype;
- x admits quantitative or temporal dtypes (§4); anything else refuses.

Row-level validation scans every row before any structural check, in
producer order, roles x → lower → upper → middle within a row: a null,
non-finite, or inexactly projectable cell refuses with role, field, and
1-based row. Within a cell, null before non-finite before inexact. The
first fault wins deterministically. The inexact-projection refusal applies
to the quantitative roles and to quantitative x; a temporal x is validated
and ordered on its typed values instead (§4), where f64 projection is a
rendering representation, not a fault.

Every preparation error carries a typed role — `X`, `Lower`, `Upper`, or
`Middle` — and its field; row-level variants additionally carry their
1-based row. PRODUCT-0146 can then map faults onto authored spans by
attribution rather than field-name string comparison, the 0144 §7
discipline. Preparation is wired into planning, not beside it: any resolved
`FillBetween` — reachable in this record only through the automatic test
substrate — validates through `validate_prepared_contract`
(`plot/request.rs:146`) with the retained artifact token, and the factory
constructs the kernel only from a planning-approved relation, under the
existing validation-then-reconstruction rule. PRODUCT-0146's authored path
inherits that branch rather than adding one.

### 2. Structure is checked second, in row order, three checks per row

After every cell is valid, a second pass walks rows in producer order and
applies, at each row, in this order:

1. **strict x order** — this row's x must be strictly greater than the
   previous row's; an equal or smaller x refuses, naming both rows.
   Duplicates and reversals are the same refusal;
2. **band order** — `lower <= upper`; a crossed row refuses with its row
   number and both values;
3. **containment** — when middle is present, `lower <= middle <= upper`;
   an escaped middle refuses with its row number and all three values.

The first failing check at the first failing row wins. Equality is admitted
everywhere the comparisons allow it: a zero-width band (`lower == upper`)
and a middle lying on either edge are valid-but-ugly and render.

A schema-valid empty relation succeeds with zero rows and renders nothing:
no band, no stroke. Its Cartesian layout carries the canonical `(0, 1)`
empty-domain sentinel on both axes — the finite neutral range empty marks
already use — and nothing else: the sentinel is layout scaffolding for an
empty pane, never an invented data range. Empty is honest, not defaulted.

### 3. Both painters consume resolved mark-style colour, opaquely

The band fills with the resolved mark style's **fill** role and the middle
stroke paints the **fill_detail** role, through the PRODUCT-0080 tier. This
is a deliberate choice of opaque visual semantics, settled here rather than
deferred: `project_mark_style` (`plot/chrome.rs:407`) sends `fill` and
`decoration` to the same opaque ink — a decoration stroke over the band
would vanish — and PRODUCT-0083 requires every mark role to remain opaque,
so the old band's hardcoded translucency is not expressible in the tier and
is not preserved. `fill_detail` projects to paper, the tier's established
idiom for interior detail over a filled mark, so the middle stroke is
visible over the band by the contrast-pair construction, not by luck. The
band therefore changes from a near-opaque ramp sample to an opaque
mark-style fill — consistent with every migrated filled mark — and the
stroke keeps its 2.5-pixel width. No call to `color_theme.get_color`
survives in either paint path.

Geometry does not change in this repair — the same rows produce the same
paths — only the colour authority and treatment do, and the gate pins that
split (path-identical, colour-resolved).

**Amended at implementation — the distinctness halt fired, and the defect
was in the gate, not the tier.** The admitted skin inventory includes the
constant black, grey, and white palettes (every slot identical by
construction), whose anchor projection necessarily yields `fill ==
fill_detail`. That collapse is not a tier gap: it is the standing behaviour
of every paper-over-ink consumer — boxplot detail strokes
(`boxplot.rs:94,168`), histogram bin separators, pie slice separators, and
funnel detail are all already invisible under those skins, in migrated
marks, with no record calling it a defect — and it matches the S4
precedent of documenting a measured contrast failure rather than gating
it. A synthesized contrasting fallback was considered and rejected: it
would either repaint four shipped marks under those skins or become a
per-mark colour exception, the class this arc retired. The distinctness
obligation is therefore scoped to skins whose projected anchors differ;
under a constant palette the middle stroke renders in the same ink — the
deliberately monochrome skins' own semantics, valid-but-ugly. The original
gate's inventory-wide scope was this record's error, introduced by review
sharpening beyond the system's own standing behaviour.

### 4. Temporal x is admitted on typed values; projection is representation

The kernel already derives honest axis types and exact PRODUCT-0128
temporal domains for x (`fill_between.rs:220-229`), and this record admits
temporal x — a deliberate, stated divergence from hexbin's exclusion, whose
reason (a typed remote-reproducible lattice projection that did not exist)
does not transfer. But admission is not free the way the census first read
it, because exactness and f64 collide: a contemporary nanosecond `Datetime`
exceeds the f64 exact-integer range, so §1's inexactness rule applied to
projected temporal values would refuse every such column, and adjacent
nanosecond timestamps can collapse to one f64 value, spuriously tripping
the strict-order refusal.

The resolution is typed validation over representational projection:

- **Validation and structure run on the typed temporal values.** Nulls
  refuse as §1 rows; the strict-order check (§2) compares the source
  temporal integers, never their projections. Two timestamps one
  nanosecond apart are strictly ordered and pass.
- **Projection to f64 remains the standing paint representation**, exactly
  as line and scatter render temporal x today, with the exact temporal
  domain carrying bounds, ticks, and axis-typed hit values. A projection
  that rounds or collapses adjacent values is a rendering approximation
  bounded far below a pixel, not a data fault, and is not a refusal.
- **Hit is deterministic under collapse.** When several ordered rows
  project to one f64 x, nearest-x resolves to the lowest-index row of the
  run, stated and gated — an ambiguity narrower than the projection's own
  granularity may not become nondeterminism.

An adjacent-nanosecond fixture is the discriminator: two rows 1ns apart
must validate, order, render, and hit deterministically with no refusal —
the fixture that fails under naive projected-value checking and passes
under typed checking.

The y side is quantitative only. The current derivation of a y temporal
domain from the middle column is deleted along with the middle fallback:
`y_temporal_domain` is always `None`, by construction, because §1 refuses
temporal lower, upper, and middle.

### 5. Band-only axis identity is decided, not inherited

Today `y_label` and `y_axis_type` derive from the middle column
(`fill_between.rs:171` region) — an authority §1 makes optional. The y axis
is pinned `AxisType::Linear` unconditionally, by construction: every y-side
role is quantitative under §1, so no derivation from any column decides the
type. The y label: with middle present, the middle field name, unchanged;
band-only, the two field names joined as `"{lower} / {upper}"` — a
Polariton-chosen honest default naming what the axis actually spans, and an
authored `y_axis_label` setting overrides it like any other mark. The x
label remains the x field name.

### 6. Hit keeps its shape and drops what is absent

Hit-testing remains nearest-x with binary search — now sound under §2's
strict order — emitting `MarkFamily::FILL_BETWEEN`,
`HitDimensionality::Area`, the axis-typed X value, the band as
`HitValue::Interval { start, end, end_inclusive: true }`, and the cursor y.
The `Mid` field appears only when the middle series exists; a band-only
prepared shape reports no middle, rather than a zero. Paint and hit read the
same prepared vectors; no second data authority appears.

### 7. No legend, decided

Fill-between encodes no colour: the band and stroke are mark chrome, not a
data scale. Following IG-05's reasoning — a mark that adds no colour
encoding adds no legend item — both `legend_content` and
`legend_reservation_content` remain `None`, now as a stated decision with a
gate rather than an inherited default. A future series-split or
colour-encoded variant reopens this with its own record.

### 8. The composite kernel and its composition admission retire together

`FillBetweenLinePlotKernel` is deleted. `can_add_layer_type` drops both
`Line`/`FillBetween` pairs, `is_fill_between_with_line_overlay` and the
factory's overlay branch disappear, and the optional middle stroke (§1, §3)
is the sole authority for the middle series. `PlotComposition` retains the
scatter-line pairs, which remain the one live non-single path.

No durability hazard exists: the persistence arc shipped after PRODUCT-0075
retired the picker, so no durable session can contain a fill-between
composition — nothing could author one. PRODUCT-0062's session vocabulary
is unaffected; `PlotType::FillBetween` itself remains a valid type.

The one existing consumer is the PRODUCT-0133 hit-priority test, which
constructs the composite directly to prove `PlotHit::best_of` respects
paint order. That coverage is re-substrated onto the live scatter-line
composite in the same commit that deletes the fill-between composite —
retirement may not silently retire the assertion.

### 9. Automatic resolution follows the new shape, and stays substrate

`ResolvedKernelFields::FillBetween` becomes
`{ x, lower, upper, middle: Option<String> }`, and the automatic resolver's
positional arm assigns candidates in the new role order: **x selects from
the quantitative-or-temporal candidates; lower, upper, and middle then
select from the quantitative candidates excluding the field x consumed** —
no field serves two roles — with middle simply absent when only three
suitable candidates exist. The automatic path remains test substrate: it is
still unreachable from authored channels, PRODUCT-0146 owns the authored
resolver, and this record does not widen what tests can construct into
anything an author can say.

### 10. What this record does not do

No Rune constructor, registry row, handler, `MarkFields` variant, envelope
variant, authored resolver, `semantic_is_compatible` arm, settings
membership, or exhibit — all PRODUCT-0146. No `layers([...])` admission:
PRODUCT-0013 excludes fill-between and that exclusion stands. No
`AuthoredFamilyId`: the mark has no categorical colour lineage, and hexbin's
rejection (0144) is the standing precedent. No baseline-fill semantics —
filling from a series to zero is area's territory, not a degenerate band.
No series channel, no per-row colour, no smoothing or interpolation.

## Implementation sequence

**Stage 0 — bank the defects.** Bank the zero-baseline stroke from an
absent middle, the doubled middle line in the composed form, the wrong
y-range under a crossed band, a wrong hit answer under unsorted x, and the
two `get_color` samples. Negative controls, not parity promises.

**Stage 1 — validated preparation.** The fallible preparation with the new
role order, typed role enum on every error, typed optional middle, cell
validation, typed-temporal-x handling, structural checks, honest empty, and
the `validate_prepared_contract` planning seam. The old preparation and its
fallbacks delete in the same commit.

**Stage 2 — the kernel over the new shape.** Paint and hit over
`Option<Vec<f64>>` middle, resolved mark-style colour for both painters,
path-identical geometry gate, no-legend gate.

**Stage 3 — retire the composite.** Delete the kernel, the composition
pairs, the discriminator, and the factory branch; re-substrate the 0133
hit-priority test onto the scatter-line composite in the same commit.

**Stage 4 — align automatic resolution and re-bank.** The reshaped variant
and positional arm, plus banked positive fixtures as the substrate oracle
PRODUCT-0146 consumes.

## Acceptance gates

1. **Validation precedes structure.** A relation with both a null cell and
   an unsorted x refuses on the null; ordering is only checked once every
   cell is valid.
2. **Column faults refuse in role order.** Missing, temporal, and
   non-quantitative lower/upper/middle each refuse with role, field, and
   dtype; x → lower → upper → middle precedence is deterministic.
3. **Row faults carry typed roles and rows.** Null, NaN, both infinities,
   and inexact values each refuse with the typed role (`X`/`Lower`/`Upper`/
   `Middle`), field, and 1-based row; within a cell, null beats non-finite
   beats inexact. Inexactness applies to quantitative roles and
   quantitative x; a temporal x cell is never refused for f64 inexactness.
4. **Strict x order.** An equal adjacent x and a reversed pair each refuse
   naming both rows; a strictly increasing fixture passes.
5. **Band order and containment.** `lower > upper` refuses with its row;
   an escaped middle refuses with its row; `lower == upper` and a middle on
   either edge render.
6. **The baseline is dead.** No code path constructs a synthetic middle;
   a band-only preparation strokes nothing, and the banked zero-baseline
   negative control fails against the repaired kernel.
7. **Empty is honest.** A schema-valid empty relation yields zero rows, no
   band, no stroke, and the canonical `(0, 1)` empty-domain sentinel on
   both axes — nothing derived from data that does not exist; missing
   required columns on an empty relation still refuse.
8. **Colour is resolved and opaque.** Neither paint path calls
   `get_color`; the band paints the mark-style `fill` role and the middle
   stroke paints `fill_detail`, both opaque per PRODUCT-0083; projected
   `fill` and `fill_detail` differ under every admitted skin whose
   projected anchors differ, and under the constant black/grey/white
   palettes the stroke renders in the collapsed ink — the same standing
   degradation boxplot, histogram, pie, and funnel detail exhibit there,
   asserted, not repaired; architecture inspection finds no `ColorTheme`
   sampling in `fill_between.rs`.
9. **Repair is colour-only where geometry is preserved.** For a fixed
   valid **with-middle** fixture, the band and stroke paths are
   byte-identical before and after the colour migration; only fill and
   stroke colours differ. Band-only geometry is exempt by design: deleting
   the synthetic baseline stroke is an intended path change, covered by
   gate 6.
10. **Temporal x is typed-checked and exact-domained.** A `Date` and a
    zoned `Datetime` x fixture carry exact temporal domains into layout and
    axis-typed hit values; temporal lower/upper/middle refuse per gate 2.
    The adjacent-nanosecond discriminator: two rows one nanosecond apart
    validate, satisfy strict order on typed values, render, and hit
    deterministically (lowest-index row of a projection-collapsed run) with
    no refusal.
11. **Hit is order-sound and middle-honest.** Nearest-x hits are correct on
    a strictly increasing fixture at, between, and beyond sample points;
    the `Mid` field is present exactly when middle is; the band reports the
    inclusive interval.
12. **No legend, stated.** Both legend methods return `None` for band-only
    and with-middle preparations; the assertion message cites this record.
13. **The composite is gone.** `FillBetweenLinePlotKernel`,
    `is_fill_between_with_line_overlay`, the factory overlay branch, and
    both `Line`/`FillBetween` admission pairs are absent from production;
    `can_add_layer_type` still admits the scatter-line pairs.
14. **0133 coverage survives.** The hit-priority assertion runs against the
    scatter-line composite with the same paint-order discipline; the
    re-substrated test exists in the same commit that deletes the
    fill-between composite.
15. **Automatic resolution matches the shape without reuse.** Four
    candidates resolve to x, lower, upper, middle; three resolve with
    middle absent; no field serves two roles, proven on both an
    all-quantitative fixture (x consumes the first quantitative field,
    lower does not re-select it) and a temporal-x fixture; the variant
    carries `Option<String>` and remains unreachable from authored
    channels.
16. **No premature surface.** Callable registry, `MarkFields`,
    `SpannedMarkFields`, and settings membership contain no fill-between;
    the PRODUCT-0075 posture stands until PRODUCT-0146.
17. **Preparation is planning's, not paint's.** A resolved `FillBetween` —
    constructed through the automatic test substrate, the only route this
    record leaves reachable — validates through
    `validate_prepared_contract` with the retained artifact token, and the
    factory constructs the kernel only from a planning-approved relation;
    no preparation happens first at paint. PRODUCT-0146's authored path
    inherits this branch.
18. **Band-only axis identity is pinned.** `y_axis_type` is
    `AxisType::Linear` with and without middle; band-only `y_label`
    defaults to `"{lower} / {upper}"`, middle-present to the middle field
    name; the authored `y_axis_label` setting overrides both.
19. **Full battery.** Polariton unit, architecture, trybuild, Rune
    compiler, GUI, and workspace checks pass with no new ignored or
    tolerated failure.

## Guardrails and stop conditions

- If `fill_detail` does not project visibly distinct from `fill` under an
  admitted theme **whose projected anchors differ**, halt; that is a
  PRODUCT-0080 finding, and this record adds no tier machinery to route
  around it. (The unscoped form of this halt fired at implementation
  against the constant palettes and was resolved by the §3 amendment, not
  by tier machinery.)
- If typed temporal ordering cannot reach the structural pass without
  projecting through f64 first, halt rather than checking order on
  collapsed values.
- If the 0133 hit-priority coverage cannot be expressed against the
  scatter-line composite, halt; the composite may not be deleted while its
  assertion has no home.
- If any consumer is found depending on the synthetic zero baseline, the
  crossed-band y-range, or the composed double draw, that is its own
  finding — surface it, do not accommodate it.
- If exact temporal x domains cannot survive preparation without projecting
  through the middle-column machinery being deleted, halt rather than
  approximating the domain.
- A series channel, per-row colour, baseline fill, or smoothing request
  reopens its named boundary as a separate record.

## Rejected

**Auto-sorting x in preparation.** PRODUCT-0054 refused it for line because
order is authored intent; silently sorting here would hide the same
relation-stage decision and make the rendered band disagree with the
authored row order. Refusal names the defect; sorting would launder it.

**Rendering non-monotone x as valid-but-ugly.** The doctrine's cut is
whether a defined answer exists. The self-intersecting fill arguably renders,
but nearest-x hit membership has no defined answer over unordered x, and
one mark may not have a paint contract its hit contract contradicts.

**Rendering crossed bands.** `lower > upper` has no honest reading under a
mark whose name asserts the order, and the y-range arithmetic is silently
wrong there today. A crossing-band visual is a different mark.

**Empty-vector optionality for middle.** The current convention is exactly
what produced the zero-baseline defect: absence encoded as a value the
kernel then interprets. `Option` makes absence unrepresentable as data.

**Keeping the composite as unreachable substrate.** It double-draws its one
distinctive series, discards the line's temporal domain, and duplicates
behaviour the optional middle now owns. A broken unreachable composite is
not useful substrate; PRODUCT-0075 preserved kernels as valid direct-test
targets, and this one is not valid.

**A legend entry or gradient.** The mark encodes no colour; IG-05's
reasoning applies. Reserving guide geometry for chrome would spend plot
width on nothing.

**Preserving band translucency through new tier machinery.** PRODUCT-0083's
opacity invariant is a system-wide contract over every mark role, and the
old 0.9 alpha was a hardcoded accident of the ramp era, not a decided
aesthetic. Adding a per-mark alpha treatment to keep it would reopen the
tier for one mark's incidental look; the opaque fill is what every migrated
filled mark already does.

**Deferring the colour migration to CI-03 or the surface record.** CI-03 is
closed won't-do, and the 0064 obligation binds the record that makes the
renderer reachable; landing the repair in the substrate keeps PRODUCT-0146
purely additive.

## Consequences

PRODUCT-0146 inherits a substrate with nothing to apologize for: fallible
validated preparation in the exact role order the surface will spell,
resolved colour, exact temporal x, sound hits, an honest empty, and one
kernel where there were two. Its spelling —
`fill_between(x, lower, upper)` with trailing optional `middle` — becomes
pure five-station work: registry row, handler, origin injection, a
`FillBetweenFields` carrier with `Option<middle>`, an authored resolver,
and the exhibit.

The composition module shrinks to its one live purpose, the scatter-line
overlay, and the retired composite takes the codebase's last
double-authority rendering of a single series with it. The colour
migration is the first of the two remaining CI-03-class repairs done under
the 0064 obligation; candlestick's record inherits both the obligation and
this record's shape for discharging it.
