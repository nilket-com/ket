# ADR 0083: the `fill_detail` role

Status: proposed 2026-08-12. This record repairs a defect PRODUCT-0082 shipped —
the box plot's median line is invisible — by adding a fifth mark-style role,
`fill_detail`, for structure painted on a resolved fill. Box outlines, medians,
and outlier outlines move to it. Whiskers and caps stay `decoration`; the
histogram boundary stays `separator`. No geometry changes.

## Context

PRODUCT-0082 §4 moved the box body from the legacy ramp to `mark_style.fill`.
PRODUCT-0080 §4 resolves `fill`, `decoration`, and `label_text` all to
`opaque_rgb(anchors.ink)`. The consequence is arithmetic: the median, drawn in
`decoration` at width `3.5`, is painted **in exactly the fill colour, on the
fill**, under every skin.

Contact found it on `uat/test26.rn`. Two screenshots bank the pre-fix visual
oracle between them:

- a `y_bounds(0, 40)` detail view, where each box reads as a plain light
  rectangle with whiskers and caps attached and **no interior line**;
- the committed `y_bounds(-75, 225)` view, which confirms the rest of the
  contract holds — `lowhigh`'s outliers at −50 and 200, `twin`'s two `99`
  observations offset horizontally while vertically exact, `none` emitting no
  point, and `flat`/`single` surviving as collapsed marks.

Everything PRODUCT-0081 and PRODUCT-0082 promised renders correctly except the
median. Before PRODUCT-0082 the body was a ramp colour, so an ink median showed
against it: the repair that removed the last legacy-catalog sampling from this
kernel is what made its median disappear.

### Both prior records were right; the gap is between them

PRODUCT-0080 §2 named this mechanism when it created `separator`: *"An ink stroke
on an ink body is invisible."* PRODUCT-0080 §3 then declined a general contrast
claim for `decoration`, because its backdrop is "the mark's own fill in some
kernels and the canvas in others" — correct, and the reason no fixed colour can
guarantee a ratio.

What neither said is that **the backdrop distinction is per element, not per
kernel**. Within one box plot, four strokes come from a single `line_color`
(`plot/kernels/boxplot.rs:93`) and two different backdrops:

| element | site | width | painted on |
|---|---|---|---|
| box outline | `:104` | 1.0 | the fill's edge |
| median | `:116` | 3.5 | the fill |
| whisker | `:128` | 1.0 | the canvas |
| cap | `:145` | 3.5 | the canvas |
| outlier outline | `:166` | 1.0 | the point's fill edge |

One role cannot serve both backdrops, which is why the median vanished while the
whiskers did not.

## Decision

### 1. A fifth role, authorized by its own stop condition firing

`PlotMarkStyle` gains `fill_detail`. PRODUCT-0080 stop condition 2 reads: *"A
fifth mark role is wanted. Four roles are justified by eight existing sites and
one new boundary, each arriving with a consumer in hand. A fifth arriving
**without** one is the accumulation this tier exists to replace."*

The condition has fired in the manner it describes rather than the manner it
refuses: three consumers exist today — box outline, median, outlier outline —
and a shipped defect is waiting on them. That is the difference between a role
added on speculation and a role added on evidence.

### 2. Roles are separated by meaning, not by resolved colour

`fill_detail` resolves to the tier's paper counterpart, the same value
`separator` takes. They are still distinct fields.

This is PRODUCT-0080 §2's own principle, applied where it now bites: that record
separated `label_text` from `decoration` "on meaning, not on styling… even when
they happen to share a colour today". Folding detail-on-fill into `separator`
would abandon that to save a field, and would misname what it covers —
`separator` denotes an edge *between adjacent bodies*, which a median is not.
Two roles that coincide in value today may diverge under a future skin
projection precisely because they answer different questions.

### 3. The assignment

| element | role | backdrop |
|---|---|---|
| box outline | `fill_detail` | **straddles** — a centred stroke on the fill's edge, half over fill, half over canvas |
| median | `fill_detail` | **entirely the fill** |
| outlier point outline | `fill_detail` | **straddles** the point's fill edge |
| whisker | `decoration` | the canvas |
| cap | `decoration` | the canvas |
| histogram interval boundary | `separator` | between two adjacent bodies |

Only the median is wholly fill-backed. The two outlines are centred strokes, so
half of each lies over the canvas — which matters for what §4 can promise.

Widths, radii, offsets, and hit arbitration are untouched. This record changes
which role supplies a colour and nothing else.

### 4. Contrast: one new measured pair, of a shape already in use

`fill_detail` is measured against `fill` at the non-text `3:1` target — the same
pair, and the same `contrast(..)` helper, that PRODUCT-0080 §3 already applies to
`separator`. `visible_contrast_checks` therefore widens from
`[PlotContrastCheck; 2]` to `[PlotContrastCheck; 3]`.

The Polariton-chosen default must reach `3:1`; an authored skin producing a
low-contrast pair renders exactly as authored.

**What the pair does not cover:** per §3 the two outlines straddle their fill's
edge, so the assay guarantees contrast only for their **fill-facing half**. The
canvas-facing half is measured by nothing here, and the record does not pretend
otherwise — a claim of "the outline is visible" would be measuring one backdrop
and asserting two. The median is the only reassigned element the pair fully
covers, and it is the one the defect is about.

`decoration` and `label_text` still receive **no** contrast claim, and their
remaining consumers paint on the canvas or on backdrops this tier does not
resolve.

### 5. What does not change

The four existing roles, their anchor and legacy mappings, the atomic
`{chrome, mark_style}` install, the histogram boundary, and every geometric
decision in PRODUCT-0081 and PRODUCT-0082. No Rune surface, no persistence, no
colour identity.

## Implementation sequence

**Stage 0 — bank the invisible median.** Drive the production box plot renderer
with the **exact Tukey relation `test26.rn` produces — the relation, not the
file**. Executing a `.rn` buffer inside the probe would make this a live-Toron
gate, contrary to the precedent PRODUCT-0080 gate 7 set; `uat/test26.rn` stays
solely gate 8's live exhibit.

The fixture box is **`none`** (`q1 = 21`, `median = 23`, `q3 = 24`), chosen
because it is non-degenerate: in `flat` and `single` every summary coincides, so
the median band would sit exactly on the collapsed box's own edges and the crop
could not tell a median from an outline.

Census gate 6's band — a narrow strip at the median's transformed value, inset
from both vertical edges — for the **intended paper-counterpart colour**. The
field does not exist yet, so the bank cannot name `fill_detail`; it names the
value that role will carry. The census is zero today, and that is the negative
control.

**Stage 1 — the role.** `fill_detail` on `PlotMarkStyle`, in the anchor
projection and both legacy constructors, and in the widened contrast array — with
no consumer moved.

**Stage 2 — the assignment.** Split `line_color` at `boxplot.rs:93` per §3, and
move the outlier outline at `:166`.

**Stage 3 — the exhibit.** `uat/test26.rn` carries **both** views in one buffer:
a `y_bounds(0, 40)` detail panel and a `y_bounds(-75, 225)` outlier panel. The
two questions — central structure and extreme multiplicity — cannot be answered
by one domain, and the pair is what the existing screenshots already prove is
needed.

## Acceptance gates

**Gates 1–4 run against a synthetic `PlotMarkStyle` whose five roles carry five
distinct sentinel colours.** This is load-bearing, not convenience: `fill_detail`
and `separator` resolve identically by §2, so under the real projection an
emitted colour cannot reveal *which field supplied it*, and every routing
assertion below would pass whatever the routing was. Gate 5 separately verifies
that the real mappings are the ones §4 of PRODUCT-0080 specifies. Consequently
`fill_detail != fill` in gate 1 is a property **of the fixture**, not a universal
claim about the tier.

1. **The median is routed from `fill_detail`.** Under the sentinel style, the
   emitted median stroke carries the `fill_detail` sentinel — not the `fill`,
   `decoration`, or `separator` sentinel. Four-way discrimination, which is what
   the sentinels buy.
2. **Box outline and outlier outline are routed from `fill_detail`**, asserted
   per element against the same sentinels.
3. **Whiskers and caps remain routed from `decoration`**, so the repair does not
   over-reach into elements that were never broken. Under the real projection
   they additionally match PRODUCT-0080's bank byte for byte.
4. **The histogram boundary remains routed from `separator`.** Asserted under the
   sentinel style, because `separator` and `fill_detail` share a value in
   production and a colour-only assertion could not tell a correct routing from a
   wrong one. PRODUCT-0080's pixel probe still reports `584` unchanged.
5. **Five roles resolve at every entry point.** Each of the five is produced by
   the anchor projection and both legacy constructors — fifteen assertions
   against the mapping. Fixtures use anchors whose paper and ink differ, so
   `fill_detail != fill` is observable rather than coincidental.
6. **The median is visible in pixels, and the outline cannot stand in for it.**
   By PRODUCT-0057's classifier "an interior line is visible on a filled body" is
   a **backend gap** — whether ink separates from ink depends on realized
   rasterization, which the operation list never carries.

   The census region is **a narrow band around the median's transformed value,
   restricted to the central portion of the box width so that neither vertical
   edge's outline falls inside it**. Censusing "inside the box body" would not
   discriminate: once the outline also draws in `fill_detail`, a box with no
   median at all yields a positive count from its own edges. The band is measured
   against the paper-counterpart colour, and the Stage 0 capture of the same
   region is the negative control reading zero. Gate 1 is the anti-vacuity proof
   that a median stroke was emitted at all.
7. **Contrast scope is bounded.** `visible_contrast_checks` is
   `[PlotContrastCheck; 3]`; `fill_detail` against `fill` produces a default
   result `>= 3:1`; an authored low-contrast pair renders exactly; `decoration`
   and `label_text` remain unmeasured.
8. **The exhibit answers both questions.** `uat/test26.rn` renders the detail and
   outlier panels together, showing visible medians in the first and `twin`'s two
   offset `99` points in the second.

## Stop condition

1. **An element paints on a backdrop this tier does not resolve.** §4's pair
   works because both sides are tier-resolved. An element painted over a
   palette-assigned colour, or over another mark, is outside the claim.
2. **A sixth role is wanted.** Five roles now cover canvas, fill, between-bodies,
   and in-mark text. A sixth arriving without a consumer is what PRODUCT-0080
   stop condition 2 refuses, and this record's authorization was precisely that
   three consumers existed.
3. **A merger of `fill_detail` and `separator` is proposed.** The test is **not**
   that current projections map them alike — §2's own counterexample is
   `label_text` and `decoration`, which coincide in value and are rightly
   separate, so coincidence proves nothing about inertness. A merger must show
   the two share **both meaning and policy across every consumer**: that no
   consumer of one would want a value or a contrast obligation the other does
   not. Absent that proof, identical colours are a fact about today's
   projection, not an argument.

## Supersession

Supersedes PRODUCT-0080 §2's four-role enumeration and its
`[PlotContrastCheck; 2]` array, both widened by §§1 and 4. PRODUCT-0080's
mappings, atomic install, meaning-over-colour principle, and histogram assignment
are unchanged. Repairs a defect introduced by PRODUCT-0082 §4.

## Rejected

**Broaden `separator` to cover detail on a fill.** Attractive because it reuses a
pair already measured and adds no field, and it was the first remedy considered.
Rejected because PRODUCT-0080 separates roles by meaning even when their colours
coincide — the `label_text`/`decoration` split is exactly that — and because
`separator` names an edge between adjacent bodies, which a median is not. It
would save one field and cost the tier its organizing principle.

**Resolve `decoration` to paper.** Attractive as a one-line fix that makes the
median appear. Rejected because it inverts the defect: whiskers and caps paint on
the canvas, which is paper, so they would vanish instead. The problem is not
`decoration`'s value but that one role served two backdrops.

**Give the box body a non-ink fill so an ink median shows.** Attractive because
it restores the pre-0082 appearance without touching the tier. Rejected because
it reintroduces a mark-specific colour outside the tier — the thing PRODUCT-0080
exists to remove — and it would break again for any other mark that paints detail
on `mark_style.fill`.

**Leave the median invisible and document it.** Rejected without much
consideration, and recorded only so the option is visibly refused: a box plot
without a median is a rectangle, and the median is the one summary a reader looks
for first.

## Consequences

The box plot renders a visible median, which is the difference between a box plot
and a rectangle.

The tier now names five semantic needs explicitly — a body's **fill**, general
**decoration** on the canvas, **fill detail** painted over a resolved body,
**separation** between adjacent bodies, and **in-mark text** — so a future mark
painting detail on a fill has a role to take, chosen by what the element means
against what it sits on, rather than by resemblance to an existing mark.

**Violin, candlestick, and funnel already paint decoration over filled bodies**,
and an earlier draft wrongly said they paint only on the canvas: violin outlines
and medians cross the filled body, funnel outlines trace filled trapezoids, and
candlestick ticks overlap the body. They are outside this record for a different
and narrower reason — **their fills are legacy-ramp colours the tier does not
resolve**, so no shared-authority collision is imposed yet — a legacy ramp might
coincidentally equal `decoration`, but it is not identical to it by construction and §4's pair could not measure
them if it tried. Each acquires this record's question the moment its fill
migrates to `mark_style.fill`, which is exactly what happened to the box plot one
record ago. That is a scheduling fact, not a safety margin.

This is the second defect in this arc caused by *removing* a legacy-catalog
sampling. PRODUCT-0047's grouped bars sampled `ColorTheme` by `t`, and fixing it
exposed the resolved-colour bypass class; PRODUCT-0082's fill repair exposed the
shared-ink class. Both were latent while a legacy colour happened to differ from
the tier's, which is the general hazard of migrating a mark onto a single
authority: colours that used to differ by accident now coincide by construction.
