# ADR 0117: the per-class ASCII label budget

Status: proposed 2026-08-16. PRODUCT-0114 reserves axis-guide bands from a
flat `ASCII_LABEL_ADVANCE_EM = 1.0`. That bound is deliberately conservative:
it holds the widest ordinary ASCII letters, but it reserves the same width for
`W`, `i`, and a space. PRODUCT-0116 stopped that excess from moving rotated
tick ink, but an x-axis title must still clear the conservatively reserved tick
run. The remaining empty interval is visible rather than incorrect.

This record pins plot canvas text to one bundled proportional face, then
replaces the flat ASCII budget with a censused, exhaustive per-character-class
budget for that face. It keeps reservation conservative, introduces no runtime
text measurement, and spends the final precondition PRODUCT-0114 §6 put in
front of PRODUCT-0093 §7's backend-measurement trigger.

It exposes no Rune surface and changes no tick placement policy.

## Context

### The remaining gap is structurally real

The current lower-guide sequence is:

```text
tick_end  = x_label_offset + rotated_tick_height(estimated_width)
title_top = max(x_label_padding, tick_end + title_gap)
bottom    = title_top + x_label_size
```

The x-axis title is already anchored at the bottom of its band: `bottom` is
its bottom edge. Moving it "down" therefore recovers no space. Moving it up
would put it inside the tick run the band is meant to clear. With no realized
glyph width, the unused portion of a conservative tick reservation can only
appear between ticks and title.

PRODUCT-0116 fixed the harmful form of this problem: an estimate no longer
moves tick ink vertically. It did not, and cannot without measurement, make the
safe separation between tick ink and title exact.

### A flat ASCII bound is honest and visibly loose

Stage 0's Iced paragraph probe at 18 px found these representative advances:

| text | realized width | flat estimate | realized advance |
|---|---:|---:|---:|
| `WWWWWWWWWWWW` | 200.88 px | 216 px | 0.930 em |
| `MMMMMMMMMMMM` | 195.91 px | 216 px | 0.907 em |
| `llllllllllll` | 55.73 px | 216 px | 0.258 em |
| `iiiiiiiiiiii` | 55.73 px | 216 px | 0.258 em |
| `international partner enablement and certification` | 425.74 px | 900 px | 0.473 em |

These are **samples**, not bounds. `W` does not prove the uppercase maximum,
and the prose average cannot safely become the new global advance. A flat
tighter value would under-reserve an all-caps category and turn visible slack
into clipping.

They do prove the distinction is material. A class budget must still assume
the widest character in each class, so it cannot reach the prose sample's 0.47
em. It should bring lower-case prose nearer roughly 0.7 em than 1.0 em. The
expected visible gap is therefore to decrease by roughly half, **not vanish**.

### The missing fourth contract term is the face

PRODUCT-0114 §2 called an extent estimate a three-part contract: declared
alphabet, budget, and out-of-alphabet behaviour. That omits the condition that
makes a glyph budget meaningful: **the face for which it is conservative**.
The current one-em budget is not proven conservative for an unknown
platform-resolved sans face; it has only looked conservative on the one host
that rendered the probes.

Plot canvas `Text` leaves `font` at `Default::default()`. Iced's
`Font::DEFAULT` is generic `SansSerif`, not a named face. Polariton loads
`DejaVuSansMono.ttf` with `.font(...)`, but does not select it with
`.default_font(...)`; loading a face is not choosing it. On this Stage 0 host,
`sans-serif` resolves to Noto Sans, while another host may resolve differently.

This reaches backward to every production-pixel capture in the arc. Operation
traces remain portable, but PRODUCT-0057 captures, PRODUCT-0116's pixel
corroboration, and this record's visual exhibit have been host-specific without
saying so. A static budget cannot honestly improve that situation until its
face is explicit.

The one existing bundled face is DejaVu Sans Mono. Selecting it would make
ordinary ASCII advances effectively equal and erase the proportional-width
distinction this record needs. It is therefore not the plot face for this work.

Stage 0 also settled the glyph-coverage boundary rather than assuming it. A
`雪` paragraph requesting an explicit Noto Sans-derived face shaped through host
fallback as Noto Sans CJK SC on this machine, not as tofu. The pin therefore names the
primary face for the declared ASCII partition; it does not suppress cosmic-text
fallback for text outside that partition. That fallback remains host-resolved
and metric-unknown, exactly matching the broad out-of-partition budget.

### PRODUCT-0114 intentionally left this option unspent

PRODUCT-0114 §6 names per-character-class budgeting as the required
conservative refinement before backend measurement may be considered.
PRODUCT-0093 §7's trigger is not "a label is long" or "a plot has slack"; it
is an exhibit showing that a conservative estimate, including the available
class refinement, still visibly consumes the plot.

That ordering matters. Runtime measurement would be an engine capability and
would recover more space, but it is not honest to pay that cost while a static,
conservative alternative remains untried.

### The existing numeric budget stays separate

`format_label` labels keep PRODUCT-0114's formatted-label estimator. Its
domain is an enumerated, bounded output alphabet. This record changes only
category labels from data, whose arbitrary text currently selects the flat
ASCII budget. Choosing by label producer, rather than by x or y edge, remains
PRODUCT-0114 §5's rule.

## Decision

### 1. Plot canvas text is pinned to bundled Polariton Plot Sans

Polariton bundles **Polariton Plot Sans**, an OFL-1.1 Noto Sans Regular
derivative with only its family names changed, and declares one
`PLOT_TEXT_FONT` by that unique family name. Noto Sans is commonly installed
on hosts, so naming the original family would leave the bundled and host faces
competing in the font database and would not pin the bytes. Every canvas `Text`
emitted by `plot/` carries `PLOT_TEXT_FONT` explicitly; no plot text relies on
`Text::default()` or on the application renderer's generic default. The
application does **not** call
`.default_font(...)`: that would silently change editor and other GUI text,
which are outside this record.

Polariton Plot Sans has Noto Sans's glyph tables, and Noto Sans Regular is this
host's current resolved generic sans face. The development-host change is
therefore expected to be visually neutral while its source is now portable.
That is an expectation to test, not a claim of universal pixel parity: other
hosts may intentionally change from their former platform fallback to the
declared face. The bundled asset carries its SIL Open Font License 1.1 notice.

The face is the fourth part of every estimate contract in this authority:

1. declared alphabet or partition;
2. budget or per-class budgets;
3. out-of-alphabet behaviour; and
4. the resolved font face those budgets conservatively bound.

For this record, that fourth term is deliberately scoped: bundled Polariton
Plot Sans bounds printable ASCII. Characters outside that partition request the
same primary face but may shape through a host fallback. They are not assigned
its class maxima, and no claim of portable non-ASCII metrics is made.

### 2. Category labels use an exhaustive printable-ASCII partition

`estimate_ascii_label_width` becomes a sum of static advances selected per
character, rather than `chars × 1.0 em`. Its partition covers every printable
ASCII code point (`U+0020` through `U+007E`) exactly once. The implementation
may group characters only where Stage 0's census establishes one common bound;
the record does not pre-decide that uppercase, lowercase, digits, punctuation,
and space are necessarily the final classes.

Each named class receives a named, censused maximum advance. The estimate for a
string is the sum of those class maxima times its resolved font size. It is a
bound by construction, not an average chosen to make a screenshot compact.

### 3. The classifier is total, with an explicit fallback

Every character outside the printable-ASCII partition takes a declared
`OUT_OF_PARTITION_LABEL_ADVANCE_EM` fallback. The fallback is not the narrowest
class, a zero-width default, or an implicit match arm. It preserves the current
flat-budget behaviour for characters this record has not censused.

This is not a Unicode guarantee. CJK, emoji, combining marks, and any other
out-of-partition text may still overflow the reservation, just as PRODUCT-0114
§2 said a fixed budget can. The point is that they do so under a named broad
fallback, never because a new partition silently routed them through a narrow
ASCII class. They must nevertheless remain glyphs rather than tofu: requesting
the bundled primary face must preserve cosmic-text's fallback behaviour.

### 4. Stage 0 censuses maxima; production performs no measurement

Stage 0 measures every printable ASCII character using the same Iced paragraph
path that renders the product, at the chosen reference size, and records the
maximum for every declared class. It also verifies the advance scales with
font size within the existing point-size model. Those measurements establish
static constants for source control.

The shipped estimator is only character classification, addition, and
multiplication by resolved size. It does not query a paragraph, add a
`PlotBackend` capability, or inspect realized glyph bounds. Offline calibration
is evidence for a conservative constant; runtime measurement is the deferred
architecture capability PRODUCT-0093 §7 names.

### 5. One estimator remains the category-label authority

Every category-produced `AxisBandRequirement` continues to call the same
central estimator, on either axis and for both single and faceted figures.
There is no left-specific, bottom-specific, or renderer-local class table. A
different budget at one call site would let the band and its consumer disagree
plausibly until a long label reaches it.

### 6. Derived guide geometry changes by the estimate delta

This is an all-figures geometry change, the fourth in this arc. For an
ASCII-category requirement, the category contribution to its band is smaller
or equal to the former flat estimate. Any affected plot rectangle and x-axis
title position move by exactly the resulting derived-band delta; no unrelated
formula changes and no byte-identical parity claim is possible.

At 0° this affects a category axis only when its new class sum differs from
its old character count. At rotation it affects the bottom band through the
same angle-aware calculation PRODUCT-0114 already owns. The estimator never
changes PRODUCT-0116's placement: it may reserve space and pull an endpoint
inward, but it may not move tick ink vertically or detach a run from its tick.

### 7. This discharges the last cheap precondition on measurement

After the class partition lands, a representative long-ASCII exhibit decides
whether the remaining reservation is acceptable. It is expected to improve
materially but not disappear. If the remaining slack is still objectionable,
PRODUCT-0093 §7 may then fire: measurement is the honest next record, not a
shortcut taken before this conservative refinement.

This record neither opens a measurement API nor pre-commits to opening one. It
makes the later decision evidence-based by exhausting the named static option.

### 8. Legibility remains a separate authority

Tick thinning, abbreviation, elision, format changes, and a maximum plot-band
policy remain out of scope. A lower estimate addresses waste in a reservation;
it does not make fifty-character labels intrinsically legible at 90°. If text
still dominates a figure after its bound is honest, that is a content policy
question, not a reason to quietly under-reserve it.

## Implementation sequence

**Stage 0 — face, census, and bank.** Bundle and select the declared face in a
test renderer; verify that this development host's generic-sans capture matches
the explicit Polariton Plot Sans capture, then bank any intentional cross-host visual change
separately. Measure all printable ASCII in the Iced probe **using
`PLOT_TEXT_FONT`**; choose and record the exhaustive classes and each class
maximum. Verify that no maximum exceeds the current one-em bound. Capture
`test37` at 45° before the class change, including its title-to-tick interval
and derived plot bounds.

**Stage 1 — declared face and static partition.** Vendor the uniquely named
Polariton Plot Sans derivative with its license notice; emit `PLOT_TEXT_FONT`
at every plot canvas-text site; then replace the flat category estimator with
named class constants and the total fallback. Keep formatted-label estimation,
`AxisBandRequirement`, and all placement helpers otherwise unchanged.

**Stage 2 — derived geometry and exhibit.** Re-bank operation traces and the
long-label capture. Confirm the title clears the tick run, tick origins remain
common, and the gap decreases approximately as Stage 0 predicted without a
new measurement capability.

## Acceptance gates

1. **The face is explicit at every plot-text operation.** An operation-list
   probe asserts that headings, axis titles, ticks, legend text, kernel labels,
   and every other canvas `Text` emitted through `PlotBackend` carry
   `PLOT_TEXT_FONT`; no plot operation inherits generic `Font::DEFAULT`.
   This proves the shipped face, not merely that its asset was loaded.
2. **The Stage 0 host preserves its former face visibly.** The existing generic
   sans capture and an explicit Polariton Plot Sans capture match on the development host
   that supplied the census. If that expectation fails, the differing capture
   is banked as the face-pinning change; it cannot hide inside the class-budget
   geometry delta.
3. **Every printable ASCII character is classified exactly once.** The Stage 0
   census iterates `U+0020..=U+007E`; every character selects one declared
   class, no character selects two, and every class has a recorded maximum.
4. **Each class bound contains its census.** The static bound for every member
   is at least its measured advance at the reference size. The gate fails if a
   new class is named without being censused, or a sampled average is used as a
   maximum.
5. **No printable class needs more than the former bound.** If any censused
   printable-ASCII maximum exceeds one em, stop rather than silently retaining
   an estimator that Stage 0 proved non-conservative.
6. **The fallback is total and broad.** `雪`, an emoji, and a combining-mark
   fixture each select `OUT_OF_PARTITION_LABEL_ADVANCE_EM`, never a printable
   class, zero, or the narrowest bound. This is the gate that catches silent
   under-reservation at a partition fall-through. A pinned-Polariton-Plot-Sans capture of
   `雪` also shows a rendered CJK glyph through fallback rather than tofu; font
   fallback is host-resolved, but loss of it would be a regression this record
   cannot hide behind the budget contract.
7. **Class distinction changes the estimate for the right reason.** Equal
   length all-uppercase, all-lowercase, digit, punctuation, and space-bearing
   fixtures sum their named class bounds. The lower-case prose fixture estimates
   below its former flat budget while the uppercase fixture remains bounded.
8. **Formatted labels are unchanged.** Fixed, scientific, percentage, date,
   datetime, and time candidates still select PRODUCT-0114's formatted-label
   estimator on either axis. This record cannot accidentally apply prose
   classes to values `format_label` produced.
9. **There is one category estimator authority.** Structural: every
   category-produced `AxisBandRequirement`, in single and grid paths, reaches
   the central classifier. No edge-local class table or direct character-count
   multiplication remains.
10. **The geometry follows exactly the changed requirement.** A long ASCII
   category fixture asserts its bottom/left band, x-axis-title position, and
   plot bounds equal the former values adjusted by the derived estimate delta.
   The data mapping remains proportional within the resulting rectangle.
11. **Grid congruence survives unequal text.** Facets with unequal longest
   category labels still receive one grid-wide maximum requirement and
   congruent plot rectangles. This fails a per-cell refinement that makes the
   compact facet recover pixels the verbose facet does not.
12. **The 0116 placement invariant survives.** At ±45° and ±90°, all rotated
   runs retain one baseline y, interiors originate at their ticks, and any
   endpoint correction has `Δy = 0`. A smaller estimate must not become a new
   placement mechanism.
13. **No runtime measurement arrives by accident.** The estimator has only
   static constants and character classification; `PlotBackend` gains no text
   extent method and production code makes no paragraph-bound query.
14. **The exhibit improves in the stated direction.** `test37` at 45° shows
    the tick-to-title interval smaller than the flat-budget bank while the
    title clears the tick run and the plot remains the dominant element. It is
    not required to eliminate the interval; requiring that would turn the
    expected residual conservatism into a false failure.

## Stop conditions

> **If the declared face cannot be bundled and selected only for plot canvas
> text, stop.** Selecting the application default would change unrelated GUI
> surfaces; leaving plot `Text` generic leaves every class constant host-bound.

> **If pinning Polariton Plot Sans removes non-ASCII fallback or renders tofu, stop.** The
> class partition has no authority to trade existing data-label coverage for a
> portable ASCII bound.

> **If any printable ASCII character exceeds the existing one-em bound, stop.**
> The current estimator was not conservative over its declared alphabet; a
> narrower partition cannot safely repair a premise it disproves.

> **If a total fallback cannot remain static, stop.** Requiring a paragraph
> query to classify or bound ordinary production text is the backend-measurement
> decision PRODUCT-0093 §7 deferred, not an implementation detail of this
> record.

> **If the representative exhibit still visibly loses unacceptable plot area
> after the class refinement, stop and open PRODUCT-0093 §7's measurement
> decision.** §7 has then been spent. Do not keep tightening bounds toward
> samples, because that would exchange visible slack for hidden clipping.

> **If a proposed remedy changes which labels exist, stop.** Thinning,
> abbreviation, and elision are legibility policy, not a reservation fallback.

## Rejected

### Tightening the flat one-em value toward the prose average

The probe's 0.47 em prose sample is evidence of waste, not a bound. A global
0.7 em constant would under-reserve an all-caps category even though it makes
the showcase compact. The resulting clipping is less visible in a happy-path
figure and more dishonest than the present slack.

### Using realized paragraph width in the renderer

That would recover more space, but it is precisely a backend text-measurement
capability. It cannot be smuggled in as a local optimization while PRODUCT-0114
§6 remains untried; after this record it becomes a deliberate, separately
evidenced possibility.

### Moving the axis title to the band bottom

It is already there. `bottom = title_top + x_label_size`; moving it cannot
reclaim the interval above it without overlapping the conservatively reserved
tick run.

### Selecting bundled DejaVu Sans Mono

It would make the plot face portable, but its ordinary ASCII glyphs have one
advance. The class partition would then reduce nothing while visibly changing
the product from proportional plot text to monospace. A bundled proportional
face is the smallest coherent choice.

### Giving out-of-partition text the narrowest ASCII class

This makes the ordinary ASCII exhibit look excellent and causes CJK or emoji
to under-reserve silently. A broad fallback keeps the existing limitation
visible and the classifier total.

## Consequences

The long-label showcase should look more compact because its lower-case prose
no longer pays `W`'s width for every character. It should not look perfectly
tight: a conservative class maximum remains wider than most glyphs, and the
title must clear the reservation rather than the realized ink.

The geometry of category-axis figures changes intentionally. Captures must
assert the exact derived deltas, not byte parity. The remaining visual outcome
is useful evidence: if it is still objectionable after this record, a
measurement record is no longer premature. It is the next honest question.
