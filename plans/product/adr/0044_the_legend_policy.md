# ADR 0044: the legend policy

Status: accepted 2026-08-07. This record decides what a legend says: entry
ordering, merging across layers, one legend versus N across grid cells, what a
band-filled bar's legend contains, and the panel-label rule. Every decision is
policy over PRODUCT-0043's `LegendContent`, with **one explicitly authorized
mechanism addition** — a two-variant provenance discriminator on `LegendEntry`
(§1), without which the record's central rule is unevaluable. No other
mechanism, renderer geometry, or Rune surface is added. It replaces the legend half of PRODUCT-0013 §3's
bridge and the whole of PRODUCT-0041 §12's. The hover half of the 0013 bridge
stands until an interaction record exists.

## Context

PRODUCT-0043 reduced nineteen legend derivations to one typed model and one
renderer, so policy is now one decision instead of nineteen. Its docket
arrived pre-assembled, every item with a citation or an exhibit:

- **merging** — 0013 §3's last-authored-layer delegation, marked provisional
  for four days of records and named "the first thing a Guide record should
  replace";
- **one-versus-N** — the first faceted figure rendered the same "Value" legend
  four times, once per cell (0043 §4 deliberately preserved this to give the
  question a single seam);
- **the band-fill bar** — that same legend is a synthetic entry describing a
  color encoding that does not exist; 0029 §6 gave bands no color channel and
  no legend item, and the "Value" box is the pre-band vocabulary surviving out
  of inertia;
- **entry ordering** — the 0043 parity harness surfaced that multi-series
  line/scatter entry order is nondeterministic across processes, because it
  follows a partition iterator with no ordering contract;
- **panel labels** — 0041 §12 shipped the minimum to make cells identifiable,
  authorized "only until the Guide record."

One principle covers the docket, so it is stated once and applied five times.

## Decision

### 1. A legend exists to disambiguate; content that cannot, is omitted

> A legend entry earns its place by distinguishing one visual identity from
> another. A section that would contain exactly one entry whose label names no
> value from the data — a synthetic group like `Value` or `All Data` — is
> omitted, because it distinguishes nothing.

This is Polariton-chosen content, so PRODUCT-0035 §2's obligation applies —
our judgment, and it must be good — not the rope rule: no author wrote
`"Value"`, and no author can remove it. An entry whose label comes from the
data (a real series name, even alone) stays: naming what is drawn is
information; naming that something is drawn is not.

**The rule needs one field the model does not have, and this record
authorizes it.** "Names no value from the data" is not a property of a
`String` — the policy layer sees `"Value"` and `"Adelie"` as the same kind of
thing. `LegendEntry` therefore gains a provenance discriminator:

```text
LegendEntry { label, swatch, sample, provenance }
provenance  = Named | Synthetic
```

The kernel that manufactures a group name marks it `Synthetic`; a label read
from the data is `Named`. The omission decision is then made **in the policy
layer alone**, reading provenance in one place — never by a kernel declining
to emit — so it is auditable and structurally checkable. This is the same
resolution PRODUCT-0043's third pass reached for `sample`: when a policy
distinction turns out to be a content fact, the fact enters the model
explicitly rather than being reconstructed downstream. Provenance governs
§1's omission only; it does not participate in §3's merge identity.

Gradient and size sections are unaffected: a ramp's endpoints and a size
range are information at any cardinality.

### 2. Entry order is the display domain's order

Entries within a section appear in the order their subjects first appear in
the display domain — the same order that already governs categorical palette
assignment (0029 §3), facet cell order (0041 §5), and everything else in the
language that has an order. Sections appear in the order their kernel
declares them; layers contribute in paint order (§3); cells contribute in
cell order (§4).

This retires the nondeterminism the parity harness surfaced: entry order
stops following an iterator's accident and starts following the same oracle
as the marks. Re-sorting the relation reorders the legend with the panels —
sort is an authored statement about order, here as everywhere (0041 §5).

### 3. Layered legends merge by union, and identity is the whole entry

The layered composite's legend is the union of its layers' content, in paint
order, with **exact duplicates** — identical label, swatch, and sample —
collapsed to one entry.

Nothing else merges. Two entries sharing a label but differing in sample or
swatch both stay, because they differ in something the legend prints: the
motivating `layers([line, scatter])` shows one label twice, once as a line
sample and once as a point sample, which is exactly what is on the canvas.
Deduplication removes repetition; it never removes information. This is the
"one entry per distinct resolved series" question 0013 §3 refused to answer
ad hoc, answered where it belongs: identity is the entry triple, defined by
the model, not a similarity judgment.

When collapsing duplicates whose provenance differs, the survivor is
`Named`: a data-read name is never demoted to an omittable synthetic by
merging with a synthetic twin. Provenance stays outside merge identity —
this rule chooses the survivor's field, not the collapse. (Stated 2026-08-07
by the implementation review's record-accuracy pass; the rule shipped
documented in code and evidence but unstated here.)

The last-authored-layer delegation and its bridge tests retire. The hover
half of the bridge is untouched.

### 4. A figure has one legend

The grid composite stops drawing a legend per cell. It derives one
`LegendContent` — the union of its cells' content in cell order, deduplicated
by §3's rule — and renders it once at figure bounds, where the existing
`legend_x`/`legend_y` fractions already place it.

Cells share one body template, one shared domain, and panel-blind colors
(0029 §1), so cell contents differ only when a partition lacks a category;
the union restores the full set with each entry's stable color. Four
identical "Value" boxes were never four facts. Per-cell chrome — axes,
ticks, labels — is untouched; the legend is figure-level because its content
is figure-level: the same identities span every cell.

**A figure-bounds legend may overlay a cell, and that is the status quo, not
a defect.** Today's legend draws inside the plot area, over the marks, at
`legend_x`/`legend_y`; at figure bounds over a 2×2 grid the same fractions
land it over one panel's content. Nothing here reserves legend space, and
nothing before this record did either. Space reservation and authored
placement are the deferred placement work (§7, stop condition) — the first
faceted categorical scatter whose legend covers a panel's bars is evidence
for that record, not a defect of this one.

### 5. The band-filled bar has no legend, because the axis is its legend

A two-field bar with band fills emits no `Entries` section. Its colors
encode the categorical axis field, and the axis already labels every band
directly beneath its bar — a legend would repeat the axis, entry for entry,
at the cost of the chrome it occupies. 0029 §6 already ruled that band fill
"adds no color encoding, adds no legend item"; the surviving "Value" box was
never that legend, just the pre-band synthetic group that §1 now omits.

A bar with an **authored** color channel keeps its entries: a series legend
names identities the axis does not.

**The scope is the whole mark, not a special case.** Under PRODUCT-0030 §3
every two-field Rune bar receives band fills, so this section means the
two-field bar — the commonest bar in the product — has no legend by default.
Only an authored color channel gives a bar one. That is the intended reading,
stated here so it is a decision rather than a discovery by screenshot.

Applied together, §§1, 4, and 5 resolve the faceted screenshot by deletion
rather than merging: the correct legend count for that figure is zero.

### 6. Panel labels: the bridge becomes the rule

The 0041 §12 bridge behavior is accepted as the panel-label rule: each cell
shows its partition value, verbatim, in the cell's top-left inset, colored
`title_text`. No templating, prefixes, or format strings — a later record
may add authored label formatting; the value itself, like the entry labels
in §1, is data and needs no decoration to be information.

The bridge and its bridge-coverage markers retire; gate coverage becomes
ordinary.

### 7. Placement is the existing settings, accepted

`legend_x`/`legend_y`/`max_legend_rows` become the accepted placement policy,
applied at figure bounds. No Rune spelling for legend position, visibility,
or overrides ships here; authored legend control is a later surface record
with this policy as its default.

### 8. The 0043 parity corpus completes its job

PRODUCT-0043's corpus test pinned the mechanism migration; this record
changes *content* deliberately, which is the corpus test's success condition,
not its failure: it is narrowed to renderer-level parity (same content in,
same bytes out) and the content-level expectations move to this record's
gates. The corpus file remains as the migration's historical evidence.

## Acceptance gates

1. `layers([line("x","y"), scatter("x","y")])` produces one legend with the
   shared label twice — once `LineSample`, once `PointSample`, in paint
   order; an exact-duplicate control collapses to one entry.
2. A faceted figure over a categorical scatter renders exactly **one**
   legend at figure bounds containing the union of cell entries in display-
   domain order, each with its stable palette color — including a category
   absent from some cells.
3. The faceted band-fill bar figure — the screenshot exhibit — renders
   **zero** legends; its axis labels are unchanged. A three-field bar with an
   authored color channel retains its series legend.
4. A single-series scatter emits no `All Data` box; a single *named* series
   from a data field keeps its one-entry legend.
4a. Every kernel that manufactures a group name marks it `Synthetic` and
   every data-read label `Named`; the omission decision is made only in the
   policy layer reading provenance — no kernel declines to emit an entry —
   asserted structurally.
5. Entry order equals display-domain order and is stable across repeated
   resolutions in one process and across processes; re-sorting the relation
   reorders entries with the panels.
6. Gradient and size sections render at any cardinality, unchanged.
7. Panel labels render the partition value verbatim per cell under the
   accepted rule; the 0041 §12 bridge markers are gone, and the tests are no
   longer marked bridge coverage.
8. The 0013 §3 last-layer legend delegation and its bridge tests are
   retired; hover behavior and its bridge tests are byte-for-byte untouched.
9. The 0043 corpus test asserts renderer-level parity (fixed content in,
   same bytes out) and no longer constrains content policy; the corpus file
   is retained.
10. No Rune grammar, mechanism, renderer-geometry, chrome-role, mark,
    identity, placement, group, or lifecycle change; `legend_x`/`legend_y`/
    `max_legend_rows` behave as before at figure bounds.

## Stop condition

> This record may add the §1 provenance discriminator to `LegendEntry`,
> change legend content derivation (ordering, union, dedup, omission), move
> the grid legend to figure bounds, accept the panel-label and placement
> rules, retire the two bridges' legend halves, and re-scope the 0043 corpus
> test. If the implementation requires a hover
> or interaction decision, a Rune surface, a new chrome role or section
> kind, a renderer-geometry change beyond the call-site moves, or a change
> to marks, identity, placement, or lifecycle, the work stops and the
> requirement returns as its own record.

The expected trigger is authored legend control — a request to show, hide,
or place a legend from source. That is a surface record over this policy,
not a widening of it.

## Rejected

- **Keeping N per-cell legends as "locality":** the content is figure-level
  by construction — same template, same domain, panel-blind colors — so N
  copies assert nothing that one does not, in N times the chrome.
- **Merging entries by label alone:** collapses a line sample and a point
  sample into one entry and makes the legend say less than the canvas —
  the flattening 0043's third pass just reverted, arriving as policy.
- **A synthetic entry for band-filled bars ("Value", recolored):** repeats
  the axis at the cost of chrome and survives only as pre-band vocabulary;
  0029 §6 already declined the legend item.
- **Omitting every single-entry section:** a lone *named* series is
  information — the distinction is whether the label names a value from the
  data, not the entry count alone.
- **A synthetic-label string blocklist:** fragile, and wrong the first time
  a data column contains the word "Value" — provenance is a fact the kernel
  has, not a property a string can carry.
- **Per-kernel omission at production:** the knowledge lives in the kernels,
  but deciding there re-scatters across nineteen `legend_content`
  implementations exactly what PRODUCT-0043 collapsed, with no single place
  to audit and nothing stopping a future kernel from emitting `"Data"`.
- **First-seen-in-iteration entry order:** the nondeterminism the harness
  caught, kept on purpose. Order follows the display domain like every
  other order in the language.
- **Deciding hover ranking while the legend seam is open:** interaction is
  not in 0007 §5's pipeline; its record does not exist yet.
- **An authored legend surface in this record:** policy first, spelling on
  request — the arc's standing sequence, and no author has asked yet.

## Consequences and sequencing

- The live-bridge ledger shrinks by one and a half: 0041 §12 retires
  entirely, 0013 §3's legend half retires, and its hover half remains as
  the sole interaction bridge.
- The faceted dogfood figure loses four redundant boxes and gains nothing —
  which is the point; the categorical-scatter facet gains the single shared
  legend that makes cross-panel color identity legible.
- Legend entry order joins cells, palette positions, and tick lattices in
  deriving from one oracle; the last known cross-process rendering
  nondeterminism in the plotting language is retired.
- A later surface record may add authored legend visibility and placement;
  a later record may add panel-label templating; the interaction record
  inherits a hover bridge with no legend entanglement. Each inherits this
  policy as its default.
