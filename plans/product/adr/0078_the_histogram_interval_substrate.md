# ADR 0078: the histogram interval substrate

Status: proposed 2026-08-11. This record replaces the histogram kernel's dense
indexed bin-count model with an explicit interval sequence, so that Polariton
renders an already-aggregated relation instead of binning raw values itself. It
is a substrate cut: no Rune word is added, no figure member is spelled, and no
colour decision is made. It moves no pixel in any reachable figure, because no
reachable figure exists — see Context.

## Context

Toron ADR 545 landed numeric `FLOOR` and alias-first bucketing, verified live on
2026-08-11 against a rebuilt server:

```sql
WITH binned AS (SELECT FLOOR(revenue / 10.0) * 10.0 AS bin_start FROM demo_sales)
SELECT bin_start, COUNT(*) AS n FROM binned GROUP BY bin_start ORDER BY bin_start
```

returning five rows summing to the relation's 64. Binning is now a database
capability. The renderer must not be a second binning authority.

### What exists today, and why it cannot consume that relation

`HistogramPreparedData` (`plot/kernels/histogram.rs:130`) holds
`bin_counts: Vec<u32>` and `max_count: u32`. The bin *is* the vector index;
intervals are implied by dividing the domain. `prepare_histogram_data` (`:141`)
takes a raw value column and a bin count, and `plot/factory.rs:126` calls it
with a hardcoded `16`.

A relation of `(bin_start, bin_end, count)` cannot be represented in that
structure without inventing the intervals Toron did not report — which is the
silent aggregation PRODUCT-0056, PRODUCT-0064 §8, PRODUCT-0069 §2, and
PRODUCT-0072 each forbid marks from performing.

Three further defects in the dense model, all fixed by construction below rather
than repaired in place:

- **Bin width is derived twice, independently.** `(x_max - x_min) / num_bins` is
  computed at `:40` for painting and again at `:98` for hover. Two derivations
  of one quantity are free to disagree, and nothing pins them together.
- **Position→index arithmetic is structural.** `bin_idx.min(num_bins - 1)`
  appears at `:105` (hover) and `:182` (preparation). Indexing by computed
  position is only valid when bins are contiguous and equal-width; it silently
  misattributes the moment a gap or a variable width exists.
- **Counts narrow to `u32`** at `:131` and `:136`, below the integer width a
  database `COUNT(*)` returns.

### Nothing reachable renders a histogram today

`PlotType::Histogram` has **no production producer**. Its references are
`PlotType::ALL` (`plot/core.rs:41`, `#[allow(dead_code)]`, documented "It is not
picker data" and guarded by PRODUCT-0075's architecture test), the `Display` arm
(`core.rs:67`), the resolution arm (`plot/spec/mod.rs:1963`), and one test
(`plot/factory.rs:1643`, inside `#[cfg(test)]`). PRODUCT-0075 retired the
dashboard picker and took the only producer with it. There is no `"histogram"`
string anywhere in `crates/polariton/src`.

This has a direct consequence for method: **there is no parity oracle to bank,
because there is no reachable behaviour to preserve.** The arc's usual
bank-before-refactor rule (PRODUCT-0042, PRODUCT-0064) is not skipped here on
convenience — it has no subject. Every other record in this arc that touched a
kernel had live pixels to protect; this one does not, and the gates below assert
the new contract directly rather than a diff against an old one.

## Decision

### 1. Prepared data is a sequence of explicit intervals

```rust
pub struct HistogramBin {
    pub start: f64,
    pub end: f64,
    pub count: u64,
}
```

The interval is the datum. `bin_counts: Vec<u32>` and `max_count: u32` are
removed. A bin's identity is its half-open interval `[start, end)`, not its
position in a vector.

### 2. Validity is established once, at construction

The sequence is held by a newtype whose only constructor validates and whose
field is private:

```rust
HistogramBins::new(Vec<HistogramBin>) -> Result<Self, HistogramBinsError>
```

Accepted iff every bin has finite `start` and `end` with `start < end`, and the
sequence is ordered and non-overlapping. The error names the failing invariant
and the index at which it failed.

**Ordering and overlap are two predicates, not one.** A single
`start >= previous.end` test collapses them and cannot produce the distinct
errors gate 6 requires:

| invariant | violated when | discriminating fixture |
|---|---|---|
| ordered | `current.start < previous.start` | `[2,3], [0,1]` |
| non-overlapping | ordered by `start`, yet `current.start < previous.end` | `[0,2], [1,3]` |

The second fixture is correctly ordered and still overlaps, so it can only be
caught by the overlap predicate; the first is not sorted at all. A sequence
failing both reports the ordering violation, as the earlier condition.

This is PRODUCT-0013's `NonEmpty` and `LayerMark` manoeuvre: an invalid state
excluded by the representation is not re-checked downstream. §6's four
consumers therefore contain no ordering, overlap, or finiteness checks, and
cannot encounter a violation to handle.

### 3. Gaps are preserved; Polariton never synthesizes a bin

A missing interval stays missing. Polariton does not insert zero-count bins to
make the sequence contiguous.

The reason is authority, not appearance, and the record states that precisely
rather than claiming a visual difference it does not have: **a synthesized
zero-count bin and a gap paint identically** — both are empty space, and the
current paint already skips zero counts outright (`histogram.rs:48`). They
differ in exactly two places:

- synthesizing requires inventing interval boundaries Toron did not report,
  which is the second binning authority this record exists to remove;
- hover and hit testing would report a bin that does not exist in the data.

A third argument — that synthesis would extend the derived domain — was
considered and is **false**: §6's envelope runs from the first `start` to the
last `end` and therefore spans internal gaps either way. Filling an internal gap
changes no domain. It is recorded as rejected reasoning so it is not
rediscovered as support.

### 4. Interval width is per-bin, never derived from the domain

Width is `end - start`, read from the bin. No code divides the domain by a bin
count. This deletes both independent derivations at `:40` and `:98` and makes
variable-width bins representable at no additional cost — the sequence never
promised uniformity.

### 5. Counts keep the database's integer width

`count: u64`. A `COUNT(*)` over a large relation is not narrowed on ingest to
suit a renderer field. Where a count must become `f64` for geometry, the
conversion happens at the point of use and is visible there.

### 6. One sequence serves domain, paint, hover/hit, and legend

Four consumers read the same `HistogramBins`: **domain**, **paint**,
**hover/hit** (one consumer — hover *performs* the hit lookup rather than
sitting beside it), and **legend**, which is present only while the temporary
count gradient of §10 survives. Export is **not** a fifth: it reuses the paint
path, so it inherits the sequence rather than consuming it independently —
stated precisely because listing either export or hit-testing as a peer would
imply a second traversal free to diverge, which is the failure mode this section
exists to exclude. In particular the domain is
**derived** from the sequence rather than stored beside it: the value axis spans
the first bin's `start` to the last bin's `end`, and the count axis spans zero to
the maximum count. Storing a domain alongside the bins would let the two
disagree — the defect shape PRODUCT-0017's raster cache key already produced once
in this arc by omitting an input it depended on.

Hit testing and hover locate a bin by searching the interval sequence for the one
containing the cursor's value, and report **no bin** when the cursor falls in a
gap or outside the sequence. There is no clamp to a nearest bin; `min(num_bins -
1)` does not survive, because attributing a gap to its neighbour is the same
misreport as synthesizing the bin.

Authored bounds continue to be governed by PRODUCT-0036's cascade and are not
this record's concern; §6 fixes only the kernel's *natural* domain.

### 7. A bar originates at zero, never at the visible minimum

The current paint derives its baseline from the viewport:

```rust
let y_base = if let PlotLayout::Cartesian { y_range, .. } = transform.layout {
    y_range.0
} else { 0.0 };                                   // histogram.rs:42-46
```

Under `y_bounds(20, 100)` every bar would originate at 20, so a bar of count 60
would paint four sevenths of the height its count deserves and each bar would
misstate its value by a different proportion. **A histogram bar's height *is* the
count**, measured from true zero; a viewport is a window onto that fact, not a
redefinition of it.

Bars are therefore painted from zero and the excluded region is handled by
clipping. This is the same defect class PRODUCT-0064 removed from stacked bars —
geometry silently rebased on a viewport — and it is repaired here rather than
handed to the surface record, because a mark that misreports its own value must
not become authorable in that state.

### 8. An empty or all-zero sequence draws nothing, over a pinned domain

Zero bins is a legitimate outcome — a filter that matched nothing. It renders no
marks rather than a placeholder, following PRODUCT-0072's rule that a zero-total
category draws nothing rather than inventing a share.

"Absent natural domain" is **not implementable**: `PlotLayout::Cartesian`
requires concrete ranges and there is no absent-domain path. The fallbacks are
therefore pinned rather than left to a caller, so no degenerate `0..0` transform
can be constructed:

| sequence | x range | y range | marks |
|---|---|---|---|
| empty | `(0, 1)` | `(0, 1)` | none |
| non-empty, all counts zero | first `start` .. last `end` | `(0, 1)` | none |
| non-empty, some count positive | first `start` .. last `end` | `(0, max count)` | one per positive bin |

Authored overrides apply on top of all three per PRODUCT-0036's cascade. The
middle row matters: an explicit relation of genuine zero-count intervals keeps
its real x extent, because the database did report those intervals — only the
count axis falls back.

**The legend result is pinned too.** With no positive count there is **no
gradient legend** — not an empty one, not `0…0`, and not the legacy `0…1`
fallback. Leaving this to "does not panic" would permit all three, and a `0…1`
gradient would assert a count range the data does not contain, which is the same
inventing-a-fact failure §3 refuses for bins. This follows PRODUCT-0044 §1:
a guide that disambiguates nothing is omitted.

`NonEmpty` is deliberately *not* used here, unlike PRODUCT-0013's layers: an
empty layer list is an authoring error, while an empty result set is data.

### 9. The raw-value preparation and the hardcoded 16 are deleted

`prepare_histogram_data` (`:141`) and the `16` at `factory.rs:126` are removed in
this cut, not deprecated. Joel's constraint is that they disappear **before**
Rune exposure, so that no authorable spelling ever reaches a second binning
authority. Deletion is available at no migration cost precisely because nothing
reachable calls them.

### 10. What this record does not decide

No Rune word, no figure member, no spelling for which relation columns supply
`start`, `end`, and `count` — that is the surface record, which also
reintroduces the resolved fields Stage 3 removes.

**Two theme-blind decorations are handed forward together**, both live in the
paint path and both inert only because nothing can author a histogram:

- **Count ramp.** `t = count / max_count` sampled through
  `settings.color_theme.get_color(t)` (`:55-56`), with a matching count gradient
  legend (`:85`). This is the resolved-colour bypass shape PRODUCT-0054 closed at
  three instances and PRODUCT-0064 met again as a fourth — and it samples the
  legacy `ColorTheme` catalog whose retirement is ledger item CI-03.
- **Hardcoded outline.** Every bar is stroked with
  `Color::from_rgba(0.0, 0.0, 0.0, 0.2)` at width `0.5` (`:71`), a 20%-black
  literal that consults no skin, no chrome role, and no palette. On a dark
  canvas it is invisible; on a light one it is an unauthored border.

**The surface record must discharge PRODUCT-0064's standing obligation that each
new authorable mark proves its renderer consumes resolved assignments**, and must
repair both before a histogram is authorable — a mark whose colour ignores the
theme cannot ship into a language whose whole colour arc is about resolved
identity. It should also decide the prior question the ramp begs: whether count
belongs in both height and colour at all. Naming both here keeps them from being
rediscovered as defects after the mark is live, which is how the resolved-colour
class was found the last two times.

No axis-type, tick, or label policy changes.

## Implementation sequence

**Stage 1 — the type.** `HistogramBin`, `HistogramBins`, the validating
constructor, and the derived domain, with no consumer changed. Gated
independently so the representation is proven before anything draws from it.

**Stage 2 — the consumers.** Repoint `plot()` (`:30`), `legend_content()`
(`:77`), and `hover()` (`:92`) at the interval sequence; delete the two width
derivations and both `min(num_bins - 1)` clamps.

**Stage 3 — the deletion.** Remove `prepare_histogram_data`, the `16` at
`factory.rs:126`, and the dense fields.

The automatic resolution is **not rewritten.** An earlier draft had Stage 3
repoint `ResolvedKernelFields::Histogram` at an already-aggregated relation —
which would have required deciding which columns mean `start`, `end`, and
`count`, the exact mapping §10 defers to the surface record. A stage cannot
implement a decision the record declines to make.

"Remove or refuse" is likewise not a contract: it names two different outcomes
and leaves the implementer to choose. The result is pinned to exactly four
changes:

1. `ResolvedKernelFields::Histogram` is **removed**.
2. Automatic resolution of `PlotType::Histogram` (`spec/mod.rs:1963`) returns a
   **typed refusal** naming the variant as having no producer.
3. The `factory.rs:125` arm is **removed**.
4. `HistogramPlotKernel` is constructed **directly from a validated
   `HistogramBins`** in tests, with no resolution path involved.

`PlotType::Histogram` stays in `PlotType::ALL`, so the renderer vocabulary is
preserved while the absence of a producer becomes explicit at the resolution
boundary rather than implied by a missing arm. The surface record reintroduces
resolved fields together with its authored mapping.

## Acceptance gates

1. **Intervals are the datum.** A three-bin sequence with **unequal widths**
   paints three rectangles whose widths are in the ratio of their `end - start`,
   asserted against the emitted operations. Equal-width bins cannot distinguish
   this model from the old one, so the fixture is deliberately unequal.
2. **A gap is preserved.** A sequence with a missing interval paints exactly the
   bins present, and the count of emitted mark rectangles equals the bin count —
   no synthesized bin appears.
3. **A gap is not attributable.** Hover at a value inside the gap returns
   `None`; hover just inside each adjacent bin returns that bin. The three
   probes together distinguish this rule from the old clamp, which would have
   returned a neighbour for all three.
4. **Hover outside the sequence returns `None`,** below the first `start` and at
   or above the last `end`.
5. **The half-open boundary is pinned.** A value exactly equal to a bin's `end`
   reports the *next* bin when one starts there, and `None` when none does.
6. **Construction refuses each invalid shape,** named individually with its
   index: unordered, overlapping, `start == end`, `start > end`, non-finite
   `start`, non-finite `end`. Six cases, six distinct errors.
7. **Validity is structural, not checked.** `HistogramBins`' field is private
   with no public mutating accessor, so no consumer can observe an invalid
   sequence; asserted by a compile-fail fixture in the trybuild suite, since
   PRODUCT-0049 established that polariton is binary-only and doctests never run.
8. **The domain is derived, not stored.** Changing the bin sequence changes the
   reported domain with no separate update call; there is no field that can hold
   a stale domain. Asserted by constructing two sequences and comparing reported
   domains, and structurally by the absence of a domain field.
9. **Counts survive `u32`.** A bin with a count exceeding `u32::MAX` round-trips
   through prepared data and reports exactly, proving the width is real rather
   than declared.
10. **Bars originate at zero under a raised floor.** With `y_bounds(20, 100)`
    and bins of counts 40 and 80, the **emitted rectangles** span zero to the
    count, so their heights are in the ratio 40:80 — *not* 20:60 measured from
    the floor. The two ratios differ, which is what makes this gate
    discriminating; a fixture with a zero floor, or with a single bar, would
    pass against the defect.

    **This is an operation-level oracle and must not be applied to a rendered
    capture.** After clipping to the `20..100` viewport the *visible* heights
    are 20 and 60, ratio 20:60 — which is correct behaviour, not the defect.
    Asserting 40:80 against realized pixels would therefore fail against a
    correct implementation and invite someone to "repair" working clipping. The
    gate reads emitted geometry; a companion assertion pins that the excluded
    region below the floor is clipped rather than omitted.
11. **Empty and all-zero sequences render nothing over the pinned domains.**
    Two cases per §8's table: empty → x `(0, 1)`, y `(0, 1)`; all-zero →
    x = first `start` .. last `end`, y `(0, 1)`. Both emit no mark rectangles,
    no transform is constructed with an equal-valued range, and **both return no
    gradient legend** — asserted as absence, not as an empty or `0…1` gradient,
    since "does not panic" would admit all three. Paint, hover, legend, and
    domain are each exercised.
12. **The second binning authority is gone.** `prepare_histogram_data`, the
    literal `16` at its call site, `bin_counts`, and `max_count` are absent from
    the tree; a structural guard asserts the histogram kernel contains no
    value-to-index arithmetic. Cite this guard at the strength PRODUCT-0077's
    gate 11 was cited: it constrains the direct source shape, not the transitive
    call graph.
13. **No reachable figure changed.** `PlotType::Histogram` still has no
    production producer after this record, asserted the same way Context
    establishes it, so the claim "moves no pixel" is gated rather than asserted
    in prose.

## Stop condition

Halt and report rather than widening this record if any of these fire:

1. **A consumer needs uniform bins.** If paint, hit testing, hover, or domain
   turns out to require equal widths or contiguity, the interval model is
   carrying an assumption it claims to have removed, and that is a finding about
   the model rather than a case for reinstating a bin count.
2. **The relation cannot supply an interval end.** §1 requires `start` and `end`.
   If the accepted producer yields only starts plus an implied width, deriving
   `end` in Polariton reintroduces the binning authority through the back door
   and needs its own decision.
3. **Colour is required to make the mark legible.** §10 defers colour; if the
   substrate cannot be exercised without deciding the ramp, the two records were
   drawn in the wrong place.

## Supersession

Supersedes no record. Replaces the histogram half of PRODUCT-0013's kernel
catalog implementation without changing PRODUCT-0013's decisions. Honours
PRODUCT-0056, PRODUCT-0064 §8, PRODUCT-0069 §2, and PRODUCT-0072's shared rule
that marks do not aggregate — this is the first record to remove an existing
violation of it rather than to avoid introducing one.

## Rejected

**Keep `Vec<u32>` and carry a parallel interval vector.** Attractive because it
touches less code and leaves paint's index loop intact. Rejected because two
vectors that must stay the same length and order is precisely the desynchronizing
representation §2 and §6 exist to eliminate; the invariant would move from the
compiler to a comment.

**Synthesize zero-count bins to make the sequence contiguous.** Attractive
because index arithmetic then still works, hover needs no search, and the painted
result is identical. Rejected because Polariton would be inventing interval
boundaries the database did not report, and would then answer hover questions
about them — a figure that reports a bin no observation supports. Identical
pixels do not make it the same claim.

**Store the domain alongside the bins.** Attractive as a small optimization and
matches the existing struct. Rejected: it is the raster-cache-key defect shape,
where a derived value kept beside its input drifts from it. Derivation is cheap
and cannot disagree.

**Require `NonEmpty` bins.** Attractive for symmetry with PRODUCT-0013's layers
and it would remove §8 entirely. Rejected because an empty layer list is an
authoring mistake while an empty result set is a legitimate answer from the
database, and refusing it would make an ordinary filter into an error.

**Defer the deletion of `prepare_histogram_data` to the surface record.**
Attractive as a smaller cut. Rejected because it would leave a reachable second
binning authority in the tree at the exact moment a Rune word makes histograms
authorable, which is the sequencing this record exists to prevent.

## Consequences

Histogram becomes a continuous-interval mark over an already-aggregated
relation. Variable-width bins are representable for free, which the dense model
could not express at any price.

No figure changes, because none can currently draw a histogram — and gate 13
holds that true rather than trusting it.

Three items are handed forward explicitly rather than left implicit: the Rune
spelling and column mapping; PRODUCT-0064's resolved-colour obligation together
with the question of whether count should be encoded twice; and hexbin, which
remains blocked on viewport-aware planning and is unaffected by this record even
though its kernel carries a structurally similar hardcoded constant
(`bins: HashMap<(i32,i32),u32>` with a data-space radius of `0.02` at
`plot/factory.rs:52`).
