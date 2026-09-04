# ADR 0155: the published hit projection

Status: accepted 2026-09-03, proposed and revised twice the same day: first
after review
found the sampled lattice neither exact nor bounded, the script forbidden to
reach the data it needed, selection material published ahead of the rung that
would use it, and a size rule that contradicted itself; then after review
found two accepted families whose prose depends on the pointer, a single
policy that cannot express the box plot's staged resolution, the projection
seam placed below the widget that owns hit dispatch, an embedding that
hostile markup could close, and unbounded integers in the hover DTO; and a
third time for bubble's first-containing rule, script permissions that
forbade the script's own work, and gates that must run the shipped asset
rather than a reference of it; and a fourth time to escape `&` as well as
`<`, to name and qualify the JavaScript engine, and to say what a figure over
the region limit becomes. Amended at implementation: rectangles cross as the
kernel's own four edges rather than an origin and size, because the desktop's
right and bottom edges are its own f32 sums; each stage carries the
widget-space origin of its regions and the asset subtracts it from a pointer
already rounded to 32 bits before any compare, because the desktop moves its
cursor into the plot area first and the two subtraction orders round
differently; and the asset performs every distance step under `Math.fround`,
so the shipped resolver reproduces the desktop's 32-bit arithmetic bit for
bit — gate 1 passed with zero disagreements only once all three held.
Reviewed again before browser acceptance: desktop hover now visits geometry
without collecting it, facet metadata and value displays pass the structural
guard, the embedding gate exercises hostile projected data and canonicalizes
it back through a JSON parse rather than string replacement, and Polariton's
own JSON parsing is made correctly rounded (`float_roundtrip`) so that round
trip is exact. Publication lane, second record; it advances the web corpus
row *inspectable published dashboard* and no other. PRODUCT-0154 publishes a
figure as ink, definition, presentation, and data; nothing in the package
says what is under a pointer. This record decides how the desktop's typed hit
crosses into the package, in what coordinate space, how the package carries
exact hit geometry so a browser resolves a pointer without plot logic, what
bounds the projection and orders it, what the thin script may and may not do,
and how ordinary hover is accepted separately from coordinated series
emphasis. It decides no click, no selection, no cross-figure linking, no
tooltip design, and no change to any desktop hit answer.
## Context

### What the dogfood said

PRODUCT-0154 was dogfooded on the four semantic showcases in a browser at
1920×1080. Hover was the dominant inspection gap, and unevenly so: heatmap,
scatter and bubble, box and violin, and radar needed it most; parallel
coordinates needed more than a tooltip — series identity and coordinated
emphasis through the crossings — and pie, funnel, dial, bar, histogram,
line, and area needed it least. Clipped subtitles and crowded labels were
the desktop's own limits, faithfully exported, and are not this record's.

### What the desktop can answer

PRODUCT-0133 §2 made a hit structured: `PlotHit`
(`crates/polariton/src/plot/common.rs`) carries the mark family, a
dimensionality — point, line, or area — labelled fields each with a semantic
type and a typed `HitValue`, an optional facet coordinate, the paint order
and sibling ordinal that broke the tie, the PRODUCT-0151 selection
candidate, and the prose presentation the desktop shows. Every one of the
twenty kernels implements the hit method. Radar answers a hit that carries
no labelled field today; parallel coordinates answers one labelled field.
The package can carry only what the desktop can answer, so those two
families are inspectable exactly as far as their desktop hit reaches.

### Why the answer cannot be sampled

The hit is a query, not a set: a kernel answers "what is under this
cursor" and nothing enumerates the regions a figure has. Answering a
lattice of cells at publish time was this record's first proposal, and it
fails twice. Scatter's hit is a nearest-point scan over every source point
within a radius of the point size plus two pixels
(`plot/kernels/scatter.rs`), so a quarter-million-cell lattice over a
fifty-thousand-point figure is billions of distance checks, not a cheap
export step. And a cell inherits its centre's answer, so every mark
boundary acquires false positives on one side and false negatives on the
other; "every mark is reachable" measures neither. Neither the cost nor
the error is acceptable for a rung named *inspectable*.

The exact source of hit geometry is the kernel's own prepared data, never
the SVG: above PRODUCT-0138's scatter vector threshold the SVG carries a
raster image of the marks while the hit method still answers per point.

Not every desktop answer is a property of a region. Bar and histogram
report a value inverted from the pointer's own coordinate
(`plot/kernels/histogram.rs` maps the cursor through the transform before
it formats), so their prose changes as the pointer moves inside one mark.
A region paired with one hit cannot reproduce that without scale inversion
in the browser, which is the plot logic this record forbids; those families
wait for a desktop answer that is constant per mark or for a later rung
that decides otherwise. Resolution is not one rule per figure either: the
box plot answers the nearest outlier circle, breaking an equal distance by
the lower ordinal, and only then the enclosing box; scatter's scan lets a
later point win an equal distance.

### What the desktop cannot answer yet

Coordinated emphasis on the desktop is projected from the selection
predicate (`project_selection_emphasis`), not from hover. There is no
desktop contract for "hovering one series emphasizes that series
everywhere". PRODUCT-0153 §5 says web interaction follows the desktop and
never invents a competing vocabulary; a hover-driven emphasis in the
browser would be exactly that.

## Decision

### 1. The hover DTO is narrow: what a tooltip shows, nothing that resolves it

A published hit carries: the mark family as its stable string; the
dimensionality as `point`, `line`, or `area`; the labelled fields, each
with its label, its semantic type in PRODUCT-0154's vocabulary, and its
value (§2); the facet coordinate when present; and the desktop's
presentation prose verbatim. It carries **no** selection candidate, no
source column names, no exact source scalars, no paint order, and no
sibling ordinal: hover uses none of them, resolution is settled before the
projection is written (§3), and disclosure of selection material is the
navigable rung's decision, made when something can send it back. The DTOs
live in the `publication` module under PRODUCT-0154 §2's discipline and
the same architecture guard.

### 2. Hit values keep their kind and their display

A hit value crosses as a tagged DTO: `nominal` and `ordinal` carry the
string; `quantitative`, `temporal`, and `percentage` carry the number and
the display string the desktop formatted; `count` carries its integer as
a decimal string, since a count may exceed what a browser number holds
exactly; `interval` carries its two ends and the inclusivity flag. Every
number in a hit value and every coordinate in a region must be finite; a
figure whose projection would carry a non-finite number publishes without
a projection and the manifest says why. The display string is
authoritative for presentation; no number is recomputed or reformatted in
the browser.

### 3. The widget projects exact hit regions in resolution stages

The projection seam is the one that already owns hit dispatch: `PlotWidget`
gains `hit_projection`, beside `structured_hit_at`, taking the same bounds
and the same resolved settings and delegating exactly as the hit does — to
the composite when the figure has one, so facets and grids project every
cell with its facet coordinate, and to the kernel otherwise. `PlotKernel`
and `PlotComposite` each gain one optional method, `hit_regions`, whose
default returns nothing, meaning the family is not yet inspectable in the
browser. A family becomes inspectable when its kernel builds both its hit
method and its region export from **one private geometry constructor**, so
the two cannot drift; a kernel that computes hit geometry twice does not
satisfy this record. A region is a rectangle or a circle with its centre
and radius, in `viewBox` units; segments and polygons arrive with the first
family that needs them. The cost of enumeration is the number of marks,
never a product with anything.

A projection is an ordered list of **resolution stages**. Each stage names
its policy — `first-in-order`, where the first region containing the
pointer wins, or `nearest-centre-within`, where among containing regions
the nearest centre wins — and its tie rule for equal distances,
`later-wins` or `lower-ordinal-wins`, and carries its regions in the order
the policy consumes them. The browser evaluates stages in order and stops
at the first stage that yields a hit. The package writes each stage's
regions already sorted by PRODUCT-0133 §1's tie-break — dimensional
specificity, then later paint order, then lower sibling ordinal — so
resolution in the browser is a scan, or a scan with one distance compare.
Regions sharing a hit share one hit index; hit indices are assigned in
stage-then-region order of first appearance, so the file is byte-stable
for a byte-stable figure.

This cut implements `hit_regions` for heatmap (one stage: first-in-order
over cell rectangles), scatter (one stage: nearest-centre-within over
point circles, later-wins), bubble (one stage: first-in-order over
observation circles in prepared-data order, because the desktop answers
the first containing observation, `plot/kernels/bubble.rs`), and box plot
(two stages: nearest
outlier circle with lower-ordinal-wins, then first-in-order over box
rectangles). Bar and histogram are deferred because their desktop prose
depends on the pointer (Context). Violin, radar, and parallel coordinates
follow once their polygon and series geometry is enumerated. A deferred
family's figure publishes without a projection and the manifest says so.

### 4. Selection is not published

Nothing in this record carries a candidate, a field reference, or an exact
scalar. PRODUCT-0151's exactness across the wire — including its time,
duration, and decimal scalars — is specified by the record that first
sends a selection back, not by one that only shows a tooltip.

### 5. Pointer mapping is arithmetic on the SVG's own transform

The shell inlines each figure's SVG with its `viewBox` and scales it by
CSS. A pointer at client coordinates maps to `viewBox` coordinates by the
SVG element's bounding rectangle and its `viewBox` size — the browser's
own transform, read back, never a Polariton coordinate system
re-implemented. Stage resolution is then evaluated in `viewBox` units.
Nothing sees desktop coordinates, pane cells, or the editor.

### 6. The projection is embedded, and the script is byte-pinned glue

Each figure's projection is written to `figures/<id>/hits.json` in
canonical JSON — that file is the projection's identity — and embedded in
the shell as a non-executable JSON data element beside its SVG, so the
script never fetches anything and the shell works from a file as well as
from the server. The embedded form is the canonical bytes with every `<`
written as `\u003c` and every `&` as `\u0026`, a total and reversible
escape that no label can undo: a label containing `</script><script>`
stays data. The shell is emitted as valid HTML and as well-formed XML at
once, so the XML parser already in the test suite proves its structure.
That polyglot form has three obligations a guard enforces: every HTML
void element is closed XML-style; the inline `inspect.js` contains no
literal `<` or `&`, because HTML script text is raw and escaping it would
break the byte identity §6 pins; and the data element's text is exactly
the escaped canonical bytes. The identity is the file; the
embedding is a derived encoding of it. The shell's single
script is a fixed asset in the repository, carried in the package as
`inspect.js` and listed in the manifest. It may: parse the embedded
projection; read the SVG root's bounding rectangle and `viewBox` to map a
pointer into `viewBox` units; attach pointer handlers; test the packaged
primitives — rectangle containment, circle containment, one distance
compare — under the stage policies; and position a tooltip element whose
text is set via `textContent`. It may not: derive any plot geometry of its
own, compute a semantic position, scale, domain, bin, or colour, inspect or
mutate any SVG mark, fetch, evaluate, or insert markup.

The shell declares a content security policy that forbids network
requests and evaluation and admits exactly one executable script, pinned
by its content hash. PRODUCT-0154 §9's no-script gate becomes exact rather
than absent: the shell's only **executable** script element must be
byte-identical to the packaged asset and to the hash the policy names; an
architecture test asserts the asset uses no DOM markup sink and no network
or evaluation primitive; and the operator's browser acceptance includes a
figure whose labels are hostile markup, the literal script-closing case
among them.

### 7. Bounds, and what a figure over the bound becomes

A figure's projection — hits, regions, and policy — is at most 1 MiB in
its canonical JSON encoding, measured on the encoded bytes, hit table
included, and at most 32,768 regions across its stages. A figure over
either bound is published **without** a projection, its manifest entry
records which bound and the observed size or region count, and the
publish succeeds for the rest of the dashboard; nothing is coarsened,
sampled, or truncated. A figure whose family has no `hit_regions`, or
whose projection would carry a non-finite number, is recorded the same
way with its own reason.

### 8. Ordinary hover and coordinated emphasis are accepted separately

Ordinary hover — pointer over a mark shows that mark's presentation — is
this record's deliverable and is accepted by gates 1 through 7.
Coordinated series emphasis — pointer over one series emphasizes that
series across a figure or across figures — is **not** delivered here. Its
precondition is a desktop contract for hover-driven emphasis, which does
not exist; when the desktop has one, the browser projects it, not before.
Parallel coordinates' crossings therefore remain a later rung.

### 9. What this record does not decide

Click, selection, or sending anything back; cross-figure linking; the
tooltip's typography beyond using the packaged font; region enumeration
for the families §3 defers; any viewer runtime; and any change to what a
kernel's hit method answers.

## Acceptance gates

1. **Exact parity, through the shipped asset.** For a heatmap, a scatter,
   a bubble, and a box plot figure, one thousand pointer positions per
   figure — uniform over the `viewBox`, placed just inside and just
   outside every region boundary, on deliberately equidistant scatter
   points, over overlapping bubbles, and across the box plot's two stages
   — are resolved by the actual `inspect.js`, executed in the test suite
   through `boa_engine` 0.22, Unlicense or MIT, pure Rust, taken as a
   dev-dependency of Polariton only. Before the harness is written, the
   implementation probe verifies and records its licence, that it carries
   no build script and no C, its dependency weight, and that it compiles
   in both heads; the product dependency guard then admits exactly that
   crate as a dev-dependency and nothing else. The harness loads the
   complete asset with its DOM wrapper disabled or minimally stubbed and
   invokes the shipped resolver; there is no second resolver. The vectors
   must yield the same hit DTO as the desktop's structured hit at the
   same position, including none where the desktop answers none. Zero
   disagreements; a disagreement between JavaScript's 64-bit and the
   desktop's 32-bit arithmetic at a boundary is a finding for guardrail 2,
   never a tolerance.
2. **Bounded work, counted.** Enumerating regions for a fifty-thousand-point
   scatter performs one region per point, counted; a projection admits at
   most 32,768 regions across its stages, so one pointer event performs at
   most one containment test and one distance compare per region, at most
   65,536 operations, counted by the engine-executed asset in ordinary CI.
   No wall-clock budget is a gate.
3. **Determinism.** Publishing the same dashboard twice yields
   byte-identical projections; a label change alters the presentation
   prose and no region.
4. **Bounds.** A figure whose projection exceeds 1 MiB, and separately one
   exceeding 32,768 regions, publishes without one, its entry names which
   bound and the observed size or count, and the rest of the dashboard
   publishes; a family without `hit_regions` is recorded the same way.
5. **Browser timing, recorded.** The operator hovers the largest projection
   admitted under the bound in the UAT browser and records the observed
   event latency in the implementation notes as a threshold for later
   records; it is evidence, not a CI gate, because it is host-bound.
6. **The script is glue.** The shell's only executable script element is
   byte-identical to the packaged asset and to the hash its policy names;
   the asset contains no DOM markup sink and no network or evaluation
   primitive, with a positive-control specimen the guard refuses. The
   emitted shell for a figure whose label is the literal script-closing
   sequence and an ampersand parses as XML with exactly one executable
   script element, the data element's text canonicalizes back to the exact
   `hits.json` bytes, the hostile label survives as text, every void
   element is XML-closed, and the inline asset contains no literal `<` or
   `&`.
7. **Both heads.** Publish and serve carry the projection in the default
   and reduced heads, under the PRODUCT-0152 gate.
8. **Browser acceptance.** The operator hovers the four showcases and sees
   the desktop's prose at the pointer for every implemented family, sees
   a hostile label rendered as text, and sees deferred families publish
   without a projection as the manifest states.
9. **The corpus moves.** The *inspectable published dashboard* row becomes
   executable citing the gates and naming the deferred families; the
   linked and navigable rows do not move.

## Guardrails and stop conditions

1. If the browser needs any computation beyond containment and one
   distance compare over packaged regions, stop; §6 is the boundary.
2. If a family's region enumeration cannot reproduce its hit method
   exactly under gate 1, or cannot share one geometry constructor with it,
   that family is deferred in this cut and reported; the tolerance is not
   widened and no sampling is introduced.
3. If a hit DTO needs geometry or a colour to be presented, stop and
   report; the desktop's prose is the presentation.
4. Recheck the record number before the plan commit; two commits, plan
   then one `impl:` cut, corrections amended under review, no push until
   the cut is called stable.

## Risks

- **Two hit paths per kernel.** Mitigated structurally by §3's single
  geometry constructor and behaviourally by gate 1 on every implemented
  family, ties included.
- **Large figures publish blind.** A fifty-thousand-point scatter exceeds
  the bound and publishes without inspection. That is stated in the
  manifest rather than hidden, and the bound is the record's, adjustable
  by a later record with evidence.
- **Prose drift.** The desktop's presentation prose is a kernel's
  formatting decision and may change with the plotting arc. The package
  carries the prose of its moment; that is what a snapshot is.

## Forward

With hover in the browser, the next observation is what people do next:
click, and expect the desktop's selection. That is the navigable rung's
question; it is where selection material first crosses the wire, where
PRODUCT-0151's exactness must be specified end to end, and where
PRODUCT-0153 §6's runtime question opens only if a click requires
recomputation. Coordinated series emphasis waits on the desktop.
