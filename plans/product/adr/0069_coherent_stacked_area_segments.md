# ADR 0069: coherent stacked-area segment semantics

Status: proposed 2026-08-09. This record corrects three data semantics in the
stacked-area preparation and kernel that contradict accepted rules or produce
degenerate geometry. It adds no Rune surface, no mark word, and no schema
change. It is the prerequisite for PRODUCT-0070's area surface, in the same
relation PRODUCT-0065 held to PRODUCT-0064: an incoherent kernel is corrected
before a language surface exposes it.

## Context

Stage 0 for the area surface read `prepare_stacked_area_data` and
`StackedAreaPlotKernel` and found three defects. All are reachable today from
the dashboard picker; none required the surface to make them findable.

**Series order is alphabetical, not the display domain.** The preparation takes
the series column's `unique()` and then `sort(Default::default())`
(`kernels/stacked_area.rs:203-207`), so `categories` is alphabetical regardless
of resolution. That contradicts two accepted rules at once. PRODUCT-0044 §2
fixes legend entry order to the resolved display domain, and PRODUCT-0064 §6
requires stack order to match legend order from the baseline outward. An
alphabetical stack beneath a display-domain legend cannot satisfy both, and no
amount of care in the surface record repairs it.

**Duplicate `(x, series)` pairs are implicitly summed.** The preparation
aggregates before it fills:

```rust
.group_by(group_by_cols)                     // [col(x), col(cat)]
.agg([col(y_col).sum().alias("y_sum")])      // stacked_area.rs:261-265
```

By the time `category_values[ci][xi] = y` runs (`:331`) there is exactly one row
per key and `y` is already `y_sum`, so the assignment is not the duplicate
policy — the `group_by` is. This is mark geometry computing over data, which
PRODUCT-0064 §8 settled against for bars, and it means the two stacked families
disagree: bars keep the first row, areas silently total them.

**Non-positive values produce degenerate geometry, coherently.** Unlike the
stacked bar's three-way disagreement, all four consumers here accumulate the
same way:

```text
range     current_sum += values[x_idx]                          (:337)
paint     current_stacked_ys[x] = prev + category_values[c][x]   (:54)
boundary  strokes the same closure as paint                      (by construction)
hover     current_stack_y += val                                 (:124)
```

They agree, and the sequence they agree on is malformed. A negative value makes
a band fold back beneath its predecessor, producing a self-intersecting polygon
and a hover interval `[s, s + val]` that is empty and therefore unhittable
while still displacing the accumulator for every band above it. A zero value
emits a zero-height polygon and a doubled boundary stroke.

This is a different defect shape from PRODUCT-0064's and deserves saying
plainly: the consumers are consistent with each other and consistently wrong.
A gate that compared them would pass.

The 5% headroom multiplier is deliberate and stays. `y_range = (0.0,
max_sum.max(0.001) * 1.05)` (`:345`) means automatic bounds always expose
gridlines above the stack, which is why PRODUCT-0068's stratum needs no
exception for absolute areas.

## Decision

### 1. Series order is the resolved display domain

`categories` comes from one of two routes, and never from `unique().sort()`:

```text
resolved assignments present   the resolved assignment / display-domain order
no resolved assignments        first-appearance order in the relation
```

The second route is not a degraded case to be halted on — it is the automatic
dashboard path, which has no colour channel and therefore no resolved domain by
construction. Both routes are pinned and gated. The sort is removed outright
rather than retained as a fallback for either.

This makes stack order, legend order, and display domain the same sequence, so
PRODUCT-0044 §2 and PRODUCT-0064 §6 hold simultaneously. It is the reason this
record precedes the surface: PRODUCT-0070 promises baseline-outward agreement
between legend and stack, and cannot deliver it over an alphabetical kernel.

### 2. Implicit summation is superseded by first-row-wins

The upstream `group_by(...).agg(sum())` is removed. The first row for a given
`(x, series)` pair supplies the value; later rows for the same pair are ignored.

This matches the bar preparation and PRODUCT-0064 §8's posture that aggregation
is relation-stage work. An author who wants sums writes them in the relation,
where the intent is visible. The change is observable for data containing
duplicate pairs — those figures previously showed a total — and every such
baseline is enumerated in evidence rather than absorbed.

### 3. One canonical value matrix, clamped at preparation

An area is not a sequence of discrete segments. Each series is **one continuous
polygon spanning every x**, so PRODUCT-0064's stacked-bar remedy — skip the
segment everywhere — has no direct analogue here. A sample cannot be omitted
without deciding what the band does across the gap.

The bounded correction is to canonicalize once, in the prepared matrix: every
non-finite or non-positive sample becomes exactly `0.0`, and range, polygon,
boundary, and hover all consume that single matrix.

```text
before   negative folds the band beneath its predecessor and displaces
         every band above it; hover interval [s, s+val] is empty
after    the contribution is 0.0 everywhere; the band pinches to its
         baseline at that x and resumes
```

A zero sample therefore *pinches* a continuous band rather than emitting an
independent degenerate polygon, and a series that is non-positive at every x
contributes no polygon at all.

Eliminating the doubled boundary segment where a pinched band runs along its
own baseline would require splitting each series into positive runs — a new
path-topology decision with its own endpoint, join, and hit-testing questions.
This record does not take it. Clamping is the smallest change that removes the
folds and the unhittable intervals, and it leaves the geometry model
untouched.

### 4. What this record does not change

The 5% headroom multiplier, the `x` uniquing and sort, the straight-segment
geometry, the single-polygon-per-series topology, the axis types, the labels,
and the boundary stroke's colour and width — the last of which PRODUCT-0068 just made resolved chrome — are all
unchanged. No resolved-colour repair happens here either: `stacked_area.rs:53`
remains the fifth bypass instance and belongs to PRODUCT-0070, which is the
record that makes it reachable.

Separating them is deliberate. This record's oracle is byte identity on the
coincident cases; a colour repair would change bytes and destroy that oracle.

## Implementation sequence

1. Bank the byte-identity oracle first: existing dashboard stacked-area images,
   SVG, and draw operations over data whose series display domain is already
   alphabetical, whose `(x, series)` pairs are unique, and whose values are all
   positive. Those three conditions are exactly where old and new semantics
   coincide.
2. Replace the sort with the resolved display domain.
3. Remove the upstream `group_by(...).agg(sum())` and take the first row per
   `(x, series)` key.
4. Clamp non-finite and non-positive samples to `0.0` once in the prepared
   matrix, and confirm range, polygon, boundary, and hover all read it.
5. Enumerate every baseline that moves, with the reason for each.

Steps 2 through 4 must each leave the step-1 oracle green. A change that moves
a coincident-case pixel is a defect in the change, not in the oracle.

## Acceptance gates

1. **Coincidence is byte-identical.** With alphabetical display domain, unique
   pairs, and positive values, every existing stacked-area image, SVG, and
   draw-operation baseline reproduces exactly. This is the load-bearing oracle
   and no later step may refresh it.

2. **Both ordering routes are pinned.** With resolved assignments, a fixture
   whose display domain is deliberately non-alphabetical stacks and legends in
   assignment order, baseline outward, entry zero adjacent to zero. Without
   resolved assignments — the automatic dashboard path — a fixture whose
   relation order is deliberately non-alphabetical stacks in first-appearance
   order. The alphabetical result is asserted absent in **both**, so the
   fallback is proven not to be secretly sorted; testing only the resolved
   route would leave the likelier regression uncovered.

3. **Duplicates are first-row-wins.** A fixture with two rows for one
   `(x, series)` pair carrying `2` then `5` renders `2`. Both the previous
   implicit sum `7` and a last-row result `5` are asserted absent, so the gate
   distinguishes the new behaviour from the old *and* from the plausible
   mis-repair. A message names the behaviour as relation-stage.

4. **One sequence, four consumers.** With a category containing a negative and
   a zero among positives, the y range, the rendered polygons, the boundary
   strokes, and the hover text describe the same positive-only sequence:
   hovering each visible band reports the value and running total its geometry
   implies, and the range maximum equals the painted stack's top before the
   headroom multiplier. Asserting only that negatives are unpainted does not
   discharge this gate.

5. **Clamping removes folds, not geometry.** Four cases over the canonical
   matrix: a negative sample produces no fold and no accumulator displacement
   for the bands above it; a zero contribution yields no hover interval; a
   series non-positive at every x produces no polygon at all; and a series
   mixing positive and zero samples pinches to its baseline and resumes without
   self-intersecting. A doubled boundary segment along a pinched run is
   accepted and named — removing it needs positive-run path splitting, which
   this record explicitly does not take.

6. **Headroom is preserved.** The 5% multiplier is unchanged, so automatic
   bounds still expose gridlines above the stack under PRODUCT-0068's stratum.
   A constant assertion pins it.

7. **Colour is untouched.** No resolved colour *value* reaches paint or a
   legend sample: `stacked_area.rs:53` still samples the ramp, which remains
   the colour source until PRODUCT-0070. Resolved assignments are consumed for
   their category *order* under gate 2, and that is the only thing this record
   takes from them. The diff contains no colour change, so the byte oracle
   remains meaningful.

8. **Enumerated movement only.** Every refreshed baseline is listed in evidence
   with its cause — non-alphabetical display domain, duplicate pairs, or
   non-positive values. A baseline that moves for any other reason fails the
   gate. No Rune, table, scatter, bar, line, facet, or persistence baseline
   changes at all.

## Stop condition

> This record may reorder series to the resolved display domain, change
> duplicate handling to first-row-wins, introduce one canonical clamped value
> matrix read by range, polygon, boundary, and hover, and add the named gates. If
> implementation requires a Rune surface, a mark word, a schema change, a
> resolved-colour repair, a change to the headroom multiplier or straight-segment
> geometry, or aggregation of any kind, work stops.

> If the coincident-case byte oracle cannot be kept green through all three
> corrections, work stops. Losing it would leave no way to distinguish an
> intended semantic change from an accidental geometry change, which is the
> whole reason this record precedes the surface.

> If either ordering route cannot be honoured — resolved assignments present
> means assignment order, absent means first-appearance relation order — work
> stops rather than reinstating the sort for the case that fails. A silent
> alphabetical ordering is the defect being corrected, and a fallback that
> quietly re-sorts would reintroduce it on the path least likely to be looked
> at.

## Supersession

This record supersedes no accepted decision. It corrects an implementation that
contradicted PRODUCT-0044 §2 and PRODUCT-0064 §6 without any record having
authorized the contradiction.

It brings the area preparation into the posture PRODUCT-0064 §8 stated for
bars: mark geometry does not compute over data, and duplicate handling is
first-row-wins rather than any aggregation.

It leaves PRODUCT-0064 §4's resolved-colour obligation open for area. That
obligation is discharged by PRODUCT-0070, the record that makes the renderer
reachable.

## Rejected

- **Correct these inside the surface record:** bundles three semantic changes
  with a mark word and a schema migration, and destroys the byte-identity
  oracle that makes each correction checkable.
- **Keep alphabetical order and reverse the legend for areas:** makes legend
  order mark-dependent, the asymmetry PRODUCT-0056 removed for grouped bars.
- **Keep implicit summation:** makes mark geometry aggregate, which
  PRODUCT-0064 §8 rejected for bars, and leaves the two stacked families
  disagreeing about the same data for no stated reason.
- **Adopt summation for bars instead, to agree downward:** aggregation would
  then be a mark-geometry concern in both families, contradicting an accepted
  record to preserve an unrecorded implementation detail.
- **Let each consumer skip non-positives independently:** four loops that agree
  today and drift tomorrow. The stacked-bar case shows how that ends.
- **Repair the colour bypass here:** changes bytes and destroys the oracle,
  and belongs to the record that makes the renderer authorable.

## Consequences

- Stack order, legend order, and display domain become one sequence for areas,
  so PRODUCT-0070 can promise baseline-outward agreement without qualification.
- Area and bar families now share a duplicate-row posture. Neither aggregates.
- Non-positive values stop producing self-intersecting polygons and unhittable
  hover intervals. They contribute nothing, visibly and consistently.
- Existing dashboard area figures change only where their data has a
  non-alphabetical display domain, duplicate pairs, or non-positive values.
  Every such baseline is enumerated.
- The fifth resolved-colour bypass survives this record deliberately, so its
  byte oracle stays meaningful. PRODUCT-0070 inherits the obligation.
