# ADR 0047: the series bar surface

Status: proposed 2026-08-07. This record decides one thing: the Rune spelling
that gives a bar an authored series color channel — the three-field form
`bar("cat", "val", "series")` — and the lowering that reaches the substrate
which has served it all along. It adds no kernel, no resolution rule, no
legend policy, and no modifier: every downstream decision already exists, and
this record is the arity that reaches them.

## Context

The surface asymmetry is measured, not suspected: scatter's recognition is
`2 | 3` at all three sites while bar's is `== 2` at all three — the
free-function `plot.bar`, the relation sugar `r.bar`, and the canonical
`.views([bar(...)])`. Below the envelope the three-field bar is not merely
possible but **already load-bearing in accepted records**: PRODUCT-0044
gate 3 tests "a three-field bar with an authored color channel retains its
series legend," PRODUCT-0029 §6 defines the authored-channel bar's color
semantics (channel authoritative, band fill absent), PRODUCT-0021's context
describes `BarPlotKernel` dividing each category band among series, and
PRODUCT-0046 §1 cited the three-field bar as its example of a substrate-only
encoding no author can write. This record retires that example by making it
spellable; 0046 §1's clause stands unchanged — the rule ranges over spellable
forms, and this form now is one.

Everything the spelling reaches was decided elsewhere and inherits unchanged:

- **channel resolution** — the series field is an ordinary `Named` color
  channel under PRODUCT-0007 §2: it resolves exactly or fails its sink, never
  substitutes, never infers;
- **color** — an authored categorical channel consumes the figure's palette
  selection through `ColorJob::Categorical` (PRODUCT-0030 §5), with stable
  positions, lineage, and capacity per PRODUCT-0029 §§1–5; band fill is
  absent because the authored encoding is authoritative (PRODUCT-0029 §6);
- **geometry** — grouped bars occupy per-series subgroup slots inside the
  category band, with paint and hit sharing one rectangle calculation and
  `bar_width` as the slot fraction (PRODUCT-0021 §§1, 3);
- **legend** — series entries are `Named`, `Swatch`-sampled, ordered by the
  display domain, unioned across facets into one figure legend
  (PRODUCT-0043, PRODUCT-0044) — the legend the two-field bar deliberately
  does not have (0044 §5), this form deliberately does;
- **modifier** — `.bar_width(...)` is admitted on this form **by rule, not by
  this record**: PRODUCT-0046 §1 attached registers to mark kinds precisely
  so the next spellable encoding would inherit them, and this is that rule's
  first application;
- **even the modifier's meaning** — the strongest instance of this record's
  thesis: PRODUCT-0021 §1 defines width as "the fraction of one resulting
  subgroup slot occupied by the visible rectangle," over a band divided
  among the resolved series, and the kernel agrees (`bar.rs:52-67`:
  `slot = band / num_groups`, `width = slot × fraction`). 0021 was written
  against subgroups no author could reach; three fields make `N > 1` real
  with the same definition, no extension needed.

The substrate check also closed a loop: `prepare_bar_data` takes a group
column already, and the two-field bar's `group_names` defaults to
`vec!["Value"]` — the two-field form has been the grouped path at `N = 1`
all along, and the "Value" legend PRODUCT-0044 §5 retired was literally its
single-group placeholder.

## Decision

### 1. Bar admits three fields wherever it admits two

```rune
plot.bar(r, "cat", "val", "series")
r.bar("cat", "val", "series")
r.views([bar("cat", "val", "series")])
```

The three spellings are exact projections of one normal form, per
PRODUCT-0008 §2's strict-sugar rule. Recognition changes from `== 2` to
`2 | 3` at the three existing sites and nowhere else; contextual recognition,
atomicity, and shadowing rules are PRODUCT-0008 §3's, untouched.

All three fields are string literals, and a bound name resolves per
PRODUCT-0040 once mark-argument coverage exists; until then the inline
literal is the spelling, exactly as for scatter's third field.

### 2. Lowering names the channel and adds nothing

The third field lowers to the authored specification's color channel as
`Named(series)` — precisely scatter's third-field lowering, applied to bar.
The envelope holds no opinion about what the field means: resolution decides
per PRODUCT-0007 §2 after execution, and a missing or incompatible field
fails that sink alone at its span while siblings render.

### 3. The two forms are one mark

`bar(c, v)` and `bar(c, v, s)` are the same mark kind with different
encodings — the same view in the same named or positional slot
(PRODUCT-0010 §§3–4: form is non-identifying), the same register
(PRODUCT-0046 §1), the same figure-member compatibility. Editing between the
forms retargets the same panel with different content.

Inside `layers([...])`, bar refuses at either arity, exactly as today —
PRODUCT-0014 §4's rule is about the shared Cartesian domain, not arity, and
this record does not touch it.

### 4. What this record does not authorize

No line series argument — PRODUCT-0012's deferral (DB-10) stands on its own
evidence requirement. No stacked bar spelling. No bar inside `layers([...])`.
No new modifier, no legend, palette, capacity, facet, or kernel-geometry
change. No change to the two-field bar's behavior, including its band fills
and its legend-free default.

## Acceptance gates

1. The three spellings with three fields normalize to one identical program;
   the compatibility equivalence of PRODUCT-0008 §2 is asserted across all
   three, and two-field behavior everywhere is byte-identical to today.
2. `bar("cat", "val", "series")` lowers to exact field identities with the
   color channel `Named(series)`; a missing series field refuses that sink
   at its span with siblings completing (PRODUCT-0007 §2's shape).
3. Six series over four categories render grouped bars in per-series
   subgroup slots with paint and hit sharing one rectangle per
   PRODUCT-0021 §3, and **no band-fill assignments exist** on the resolved
   artifact (PRODUCT-0029 §6 asserted through the Rune path).
4. Series colors consume the figure's `palette(...)` selection through
   `ColorJob::Categorical` with stable positions across rerun and filter
   (PRODUCT-0029 gates 3/7 re-observed through this spelling); palette and
   geometry capacity refusals attribute to the sink span.
5. The legend contains `Named`, `Swatch`-sampled series entries in
   display-domain order; the two-field control renders zero legends
   (PRODUCT-0044 §5 both halves, through Rune).
6. `.bar_width(0.5)` on the three-field form lowers and narrows subgroup
   slots — admitted by PRODUCT-0046 §1's rule, cited in the test as its
   first new-form application; duplicate and cross-register refusals are
   inherited unchanged.
7. A faceted three-field bar renders the template per cell over the shared
   domain with **one** figure-level legend (PRODUCT-0044 §4); faceting by
   the series field renders single-series cells without refusal
   (PRODUCT-0041 §11).
8. Editing `bar(c, v)` to `bar(c, v, s)` in the same slot reuses the view's
   identity, panel, group membership, and labels.
9. Wrong arity (four fields), a non-literal field, and bar at either arity
   inside `layers([...])` refuse at their own spans.
10. `../uat/test16.rn` — created with this record — renders a faceted,
    palette-driven grouped bar with one legend through the production head.

## Stop condition

> This record may extend bar's recognized arity at the three existing sites,
> lower the third field into the authored color channel, and add the gates
> and fixture. If the implementation requires a kernel or geometry change
> beyond the existing grouped path, a resolution or capacity rule, a legend
> or palette change, a layers admission, or any change to the two-field
> form, the work stops and the requirement returns as its own record.

The expected trigger has a precise precedent, and its outcome is the
instruction. From PRODUCT-0020's evidence: "Gate 6 caught a substrate seam
before closure: a styled single line changed from line-specific to
multi-layer scale padding. The PRODUCT-0020 stop condition fired,
PRODUCT-0019 was amended under its own presentation-only invariant, and the
correction landed with the surface commit." That is what the halt firing
looks like in practice — an accepted record amended under its own
invariant, the fix shipping with the surface rather than becoming an arc.
Stage 0 exists to find such a seam before the halt would: ten minutes
driving the grouped kernel with an authored channel below the envelope,
confirming colors, width, paint, and hit — because PRODUCT-0044 gate 3 and
PRODUCT-0029 §6 both legislated for a path no author could reach.

## Rejected

- **A separate `grouped_bar` constructor:** the kernel groups by the
  channel's presence, not by a different mark; a second name would make one
  mark two words (PRODUCT-0016 §1's one-word-one-concept rule).
- **Admitting the series line in the same record:** PRODUCT-0012 declined it
  pending evidence and a specified spelling; bundling it here would inherit
  bar's evidence for line's decision.
- **A stacked variant now:** stacking is a different geometry with its own
  ordering and capacity questions, not an arity.
- **Waiting for mark-argument binding coverage:** inline literals are the
  spelling today for scatter's third field; parity, not expansion.

## Consequences and sequencing

- The measured asymmetry closes: bar and scatter are `2 | 3` everywhere, and
  the substrate that five records legislated for becomes reachable from a
  buffer.
- PRODUCT-0046 §1's register rule sees its first new-form application, and
  its substrate-only example becomes a spellable one — the clause unchanged,
  the example graduated.
- The two-field bar keeps its band fills and its zero-legend default; the
  three-field bar is the form that *earns* a legend, which is 0044 §5's
  distinction now visible as an authoring choice.
- `test16.rn` becomes the eighth dogfood fixture, and the first showing
  grouped, palette-colored, faceted bars under one legend.
- Next in the agreed sequence: interior axis-label dedup (the 0041 §7
  deferral), then legend location and visibility with the `Presence::Hide`
  repair (DB-18) riding along.
