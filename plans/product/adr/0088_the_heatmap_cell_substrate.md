# ADR 0088: the heatmap cell substrate

Status: proposed 2026-08-12. Heatmap silently resolves duplicate cells by row
order, fabricates absent cells as real zeros, and derives a single-ended domain
that collapses on signed data. This record makes cell identity unique, absence
explicit, and the domain two-ended over observed cells only. It is a substrate
record: no Rune word, no palette authoring, and heatmap remains producerless.

## Context

`prepare_heatmap_data` builds an `x × y` grid from two independent `.unique()`
category lists, so the grid is the **cartesian product** of observed x and y
values. Three defects follow, all verified 2026-08-12.

**Duplicate cells are resolved by row order.** The population loop writes
`values[xi][yi] = val` (`kernels/heatmap.rs:224`) with no guard, so the last row
for a cell wins. The defect is not *which* duplicate wins — it is that the same
multiset of rows in two different orders produces two different figures, and no
relation promises an order the mark never asked for.

**Absent cells are fabricated as zero.** `values` is initialised to
`vec![vec![0.0f64; num_y]; num_x]`, so any `(x, y)` pair the relation never
contained renders as a real `0.0` observation. There is no no-data state. This
grows worse under PRODUCT-0089: on a diverging ramp a fabricated zero paints the
**neutral** colour, so missing data will read as a meaningful mid-range
measurement.

**The domain is single-ended and collapses.** `max_val` starts at `0.0` and only
grows on `val > max_val` (`:225-226`); no minimum is tracked. Negative-only data
leaves `max_val == 0.0`, the paint guard `if max_val > 0.0` fails, and every cell
renders the ramp's first colour. PRODUCT-0087 gate 16 banked this exact state.

One structural fact shapes §6: heatmap's layout is `CategoricalXY`
(`heatmap.rs:16-19`) with **no numeric axis ranges**. The colour domain cannot
produce a degenerate transform here, which is why a `(0, 0)` fallback is safe on
this kernel where PRODUCT-0078 §7 needed `(0, 1)` for the histogram.

## Decision

### 1. Cell identity is unique, and duplicates refuse

A relation supplying two rows for one `(x, y)` pair **refuses**, and the
diagnostic names the remedy: aggregate in the relation.

**Equal-valued duplicates refuse too.** The rule is **key uniqueness**, not value
disagreement. Admitting equal duplicates would make acceptance depend on the
data rather than the shape, so the same query would succeed or fail as rows
changed — and it would leave the mark deciding that two observations are one
whenever they happen to agree.

This follows three records that placed aggregation in the relation:
PRODUCT-0078 removed histogram's binning, PRODUCT-0081 removed box's `group_by`,
and PRODUCT-0082 §6 refused unordered input while **naming `.sort(...)` as the
remedy** rather than sorting silently. Defining a heatmap-side aggregation would
reverse all three and make a mark an aggregation authority for the first time
since they were removed.

The diagnostic follows PRODUCT-0082 §6's shape: it names the offending `(x, y)`
pair and points at relation-side aggregation, because a refusal that only says
"malformed" sends the author looking inside the mark.

### 2. One validated cell collection, and a stated refusal boundary

`prepare_heatmap_data` is **infallible** today and `create_plot` returns a bare
`Arc<dyn PlotKernel>` (`plot/factory.rs:40`), so §1's refusal has nowhere to go
unless this record says where. Leaving that to implementation would mean
inventing a propagation path, which is how a refusal becomes a panic.

The boundary follows PRODUCT-0078 and PRODUCT-0081 exactly:

```rust
HeatmapCells::from_rows(rows) -> Result<HeatmapCells, HeatmapCellsError>

struct HeatmapCellRow {
    x: Option<String>,
    y: Option<String>,
    value: Option<f64>,
}
```

- construction takes **neutral rows** with optional keys and value, as above —
  not a prepared grid. `None` is structurally distinct from the literal
  `Some("null")`, so §3's key rule is representable rather than inferred from a
  display string;
- source ordinals are derived from row order at construction, rather than
  supplied by callers, so errors can attribute rows and a duplicate can retain
  both ordinals without creating a second identity authority;
- preparation returns a **typed error**;
- **planning** calls preparation to validate and surfaces the diagnostic;
- the **factory** receives the same approved snapshot, prepares again
  deterministically, and `expect`s — because planning already validated *that
  snapshot*.

**It is validation-then-reconstruction, not hand-off.** An earlier draft said the
factory receives an already-validated value; that is false. `ResolvedPlot`
carries the resolved spec and the projected `DataFrame`, **not** prepared kernel
data, so there is no carrier to hand one across. This record does not fund one —
it follows the existing precedent, where the factory's `expect` is safe because
the same deterministic preparation over the same snapshot already succeeded
during planning.

**The typed errors are enumerated, with two-phase precedence, so one malformed
relation always produces one diagnostic:**

| phase | order | error | attribution |
|---|---:|---|---|
| row-local | 1 | non-finite observed value | the offending **row** |
| row-local | 2 | null `x` key | the offending **row** |
| row-local | 3 | null `y` key | the offending **row** |
| cross-row | 4 | duplicate `(x, y)` | the offending **key**, and both rows |

The constructor first scans **every row in producer order** for row-local
faults. Within one row it checks non-finite value, then null `x`, then null `y`.
Only after that complete pass succeeds does it scan for duplicate keys. The
reason is determinism rather than severity: a row-local fault is a fact about
one row, while duplicate detection is inherently about pairs. A one-pass
implementation could report an early duplicate and hide a later row-local
fault; reversing two checks within a row could likewise change the diagnostic
without changing the input. Both are excluded here.

**The type is public-opaque with private fields**, like PRODUCT-0087's
`GradientStops` and for the same reason: `HeatmapPreparedData` is publicly
constructible with nested `Vec`s today, so a test or a future producer can build
mismatched dimensions, fabricated cells, or a domain that disagrees with its
grid. Construction and access are crate-scoped; gate 7 asserts an external struct
literal fails to compile, with an in-crate twin.

**The constructor derives the grid and the domain together.** Accepting a
caller-supplied domain would recreate the two-authority problem this record
exists to remove — a grid and a range that can disagree is the same shape as
PRODUCT-0084's two injectable truths.

### 3. A null value declares its categories and leaves the cell absent

`p_val.get(i).unwrap_or(0.0)` currently turns a null value into a real `0.0`
observation. Under §4 that is the fabricated-zero defect arriving by a second
route, so nulls need their own rule:

- a **null value** contributes its `x` and `y` to the category axes, and leaves
  that cell **absent**. The row says the category exists; it supplies no
  observation.
- **uniqueness applies to every row**, per §1 — including null-with-non-null and
  null-with-null pairs for the same key. A null does not exempt a row from being
  a duplicate, or the rule would depend on values again.
- a **non-finite observed value** (`NaN`, `±inf`) **refuses**. Left admitted it
  would poison the domain, or vanish through PRODUCT-0087's non-finite gradient
  omission — silently dropping the guide because of a data error, which reads as
  a policy decision rather than a fault.

**A null `x` or `y` — a null *key* — refuses.** Category values are stringified
today (`.unwrap_or(AnyValue::Null).to_string()`), so a null key and the literal
string `"null"` collapse onto one displayed category and would silently merge two
different cells — defeating §1's uniqueness rule through the axis rather than the
value. The alternative, giving null keys an identity distinct from their display
string, invents a category the axis cannot label.

The asymmetry with null values is deliberate and is the distinction that makes
both rules read correctly: **a null value means "this cell has no observation",
which is representable. A null key means "this row has no cell", which is not
placeable.**

**Empty input keeps today's synthetic axes**, with the cell absent rather than
zero: `["No Data"] × ["No Data"]` and one absent cell. §7 says category
derivation does not change, and this is that rule applied — the axes are
unchanged, only the fabricated `0.0` becomes absence. That is what makes gate 16's
entirely-no-data case reachable from real input rather than from a
hand-constructed prepared state.

### 4. Absent cells are no-data, everywhere

A cross-product pair the relation never contained is **absent**, and absence is
represented rather than filled. `values` stops being `Vec<Vec<f64>>`.

The contract binds **every** consumer, because a partial one is how a fabricated
zero survives:

| consumer | absent cell |
|---|---|
| fill | **no fill operation** |
| outline | **no outline operation** |
| hover (`heatmap.rs:64`) | **no value reported** — the cell is not a hover target |
| domain extrema (§5) | **no influence** |

An absent cell is not a zero, not a minimum, and not a hoverable object that
happens to read `0`. A sparse relation is the normal case for a heatmap — most
grids are not complete cross products — so this is the common path, not an edge.

### 5. The domain is two-ended, over observed cells only

```text
domain = (min over observed cells, max over observed cells)
```

Both ends are tracked, so signed data works: `-50…100` normalizes across its
real range instead of collapsing. Absent cells contribute nothing, per §4 — a
grid of `5.0` and `7.0` with three absent pairs has domain `(5.0, 7.0)`, not
`(0.0, 7.0)`.

This replaces `t = val / max_val` with `t = (val − min) / (max − min)`, the
normalization bubble already performs (`bubble.rs:63-69`).

### 6. Degenerate domains, and the one mechanism they share

| case | mark colour | domain | gradient |
|---|---|---|---|
| constant non-empty (all observed cells equal) | ramp's **first** colour, `t = 0` | `(v, v)` | **omitted** |
| entirely no-data (no observed cells) | **no mark operations at all** | `(0.0, 0.0)` | **omitted** |

**The first colour, matching bubble.** Bubble's `color_delta =
(max − min).max(EPSILON)` already yields `t = 0` on a collapsed domain
(`bubble.rs:63`). Heatmap today does the opposite — `t = val / max_val = 1.0`
paints a constant field entirely at the **high** end — so the two marks disagree
and this record must pick either way. Heatmap's high-end rendering is an artifact
of the formula §5 replaces, not a decision worth preserving, and since the
gradient is omitted the reader has no scale to misread the colour against.

**Both cases reach PRODUCT-0087's existing omission rule.** A zero-width domain
is exactly what its shared gradient constructor already omits, so neither case
needs a new guide branch — `(v, v)` and `(0.0, 0.0)` are both zero-width. One
mechanism covers both.

**But their rendering differs**, which is why gate 16 pairs them: a constant grid
paints every observed cell in one colour, while a no-data grid paints nothing.
Sharing a guide policy is not sharing a mark behaviour, and a gate that tested
only the shared half would let the two collapse into each other.

### 7. What this record does not decide

No Rune word; heatmap stays producerless. No sequential or diverging palette
authoring — that is **PRODUCT-0089**, and §5 only makes a signed domain *representable*,
not well-anchored. **A diverging ramp over `-50…100` still places its neutral at
data value 25**, exactly as PRODUCT-0087 gate 17 banked for bubble; zero
anchoring remains PRODUCT-0089's.

No change to `x`/`y` category derivation or ordering, no hexbin change despite
its similar `HashMap` binning, and no guide changes beyond consuming §5's domain
— PRODUCT-0087 owns the gradient.

## Implementation sequence

**Stage 0 — bank the nondeterminism.** Capture the same duplicate rows in **two
different orders** and record the two different figures they produce. Row order
is the evidence: a fixed-order fixture shows only one outcome and cannot
distinguish last-write-wins from any other rule. Bank a sparse grid's fabricated
zeros, and a negative-only grid's collapse to the first colour.

**Stage 1 — cell identity.** Unique-key enforcement and the refusal, with the
grid representation unchanged.

**Stage 2 — no-data and the domain.** The representation change, all four
consumers of §4, and the two-ended domain.

## Acceptance gates

1. **Duplicate cells refuse, and row order is banked as the defect.** The same
   duplicate rows in two orders produce two different figures **before** the
   repair and one refusal after. The reordered pair is the discriminating
   fixture — a single-order fixture cannot show that the current answer depends
   on order.
2. **Equal-valued duplicates refuse too.** Two rows for one `(x, y)` carrying the
   **same** value refuse, proving the rule is key uniqueness rather than value
   disagreement. Without this an implementation could dedupe silently when values
   agree and pass gate 1.
3. **The refusal names the remedy.** The diagnostic names the offending `(x, y)`
   pair and relation-side aggregation, not merely "malformed input".
4. **The refusal reaches the surface through planning, and the factory never
   sees an invalid snapshot.** A duplicate relation produces a typed error from
   preparation that **planning** surfaces as a diagnostic; the factory,
   re-preparing the same approved snapshot, succeeds. Asserted at both ends —
   a refusal that panicked in the factory would satisfy gates 1–3 while being
   unusable, and one that only refused in the factory would never reach an
   author.
5. **Error precedence is fixed.** Three fixtures distinguish the two-phase
   validator from plausible one-pass implementations:
   - one row containing both a non-finite value and a null key reports the
     non-finite error;
   - an earlier duplicate pair followed by a later row-local fault reports the
     row-local fault;
   - the same duplicate pair with every row locally valid reports the duplicate
     error and both source ordinals.

   A row with both keys null reports `x`, pinning the within-row order too.
   Without these cases, one malformed relation can yield a different diagnostic
   after a harmless validation refactor.
6. **A null category key refuses, and does not merge with `"null"`.** A row with
   a null `x` refuses; a relation containing the literal string `"null"` as a
   category renders normally. The second half is the discriminating one — an
   implementation that merely stringified nulls would pass the first only by
   accident and would silently merge the two.
7. **`HeatmapCells` cannot be built from outside the crate.** An external struct
   literal fails to compile, asserted by a trybuild fixture with banked stderr
   per PRODUCT-0049's binary-only finding, with an in-crate should-pass twin
   built from neutral rows. The grid and domain are derived by the constructor;
   no API accepts a caller-supplied domain.
8. **A null value declares categories and leaves the cell absent.** A row whose
   value is null contributes its `x` and `y` to the axes while emitting no fill,
   no hover, and no domain contribution. The discriminating half: the same
   relation with that value present renders the cell — so an implementation that
   dropped null rows entirely would lose a category and fail.
9. **Nulls are still duplicates.** Two rows for one `(x, y)` refuse when one
   value is null and the other is not, **and** when both are null. A null does
   not exempt a row from uniqueness, or the rule depends on values again.
10. **Non-finite observed values refuse.** A `NaN` or `±inf` value refuses at
   preparation rather than poisoning the domain or disappearing through
   PRODUCT-0087's non-finite gradient omission. The valid twin — the same
   relation with a finite value — renders.
11. **A unique sparse relation renders.** The valid twin: distinct keys with
   absent pairs ingest and draw, so the refusals above are not a blanket
   rejection of sparse data.
12. **Absent cells emit nothing.** For a sparse grid, the count of fill operations
   equals the number of **observed** cells, and no outline is emitted for an
   absent pair. Asserted as counts, since a zero-valued fill is still a fill.
13. **Absent cells are not hover targets.** Hovering an absent cell returns
   `None`; hovering an observed cell in the same grid returns its value. The
   second half is required — an implementation that disabled hover entirely
   would pass the first.
14. **Absent cells do not move the domain.** A grid of `5.0` and `7.0` with absent
   pairs reports domain `(5.0, 7.0)`, not `(0.0, 7.0)`. This is the gate that
   catches a residual fabricated zero, since such a zero is invisible in the
   marks once §2's fill rule holds but still drags the minimum.
15. **Signed data normalizes across its real range.** A `-50…100` grid reports
    that domain, and a negative-only grid containing **at least two distinct
    values** — say `-40` and `-10` — renders a full ramp rather than collapsing
    to the first colour, the state PRODUCT-0087 gate 16 banked. Two distinct
    values are required: a *constant* negative grid correctly takes §6's
    degenerate path and could never render a ramp, so it would fail a correct
    implementation.
16. **The two degenerate cases share a guide policy and differ in rendering.**
   Paired in one gate:
   - constant non-empty: every observed cell paints the ramp's **first** colour,
     domain `(v, v)`, **no** gradient section;
   - entirely no-data: **no mark operations at all**, no hover anywhere, domain
     `(0.0, 0.0)`, **no** gradient section.

   Pairing them is the point. They reach PRODUCT-0087's omission rule by the same
   zero-width path, and a gate asserting only that shared half would let a
   constant grid paint nothing or a no-data grid paint a colour.
17. **Bubble and the shared authorities are untouched.** Asserted as scope, not
    as a file count: `kernels/bubble.rs`, `plot/colors.rs`, `plot/legend.rs`, and
    `plot/gradient_stops.rs` are unchanged, and PRODUCT-0087's bubble oracles
    pass unmodified without being re-banked here — re-asserting bubble's colours
    in a heatmap record adds no discriminating power.

    `plot/request.rs` and `plot/factory.rs` **do** change, because §2's
    planning-time validation and the fallible preparation shape require it. An
    earlier draft asserted no source outside the heatmap kernel changed, which
    §2 makes impossible.
18. **The heatmap's `-50…100` neutral still sits at 25.** A heatmap containing
    observed values `-50`, `25`, and `100` under a diverging ramp paints the
    `25` cell with the neutral colour, and its guide carries the same neutral
    stop at `25`. Per §5, anchoring is not fixed here; both halves are asserted
    so PRODUCT-0089 inherits a stated position and this record cannot be read as having
    solved it.

## Stop conditions

> If a legitimate relation cannot express a heatmap without duplicate keys, stop.
> §1 assumes aggregation is expressible upstream; if some real shape cannot be
> aggregated into unique cells, the refusal is stranding users rather than
> directing them.

> If no-data cannot be represented without changing the categorical axes, stop.
> §2 is a value-representation change; if it forces category derivation or
> ordering to move, it has grown into the surface record's territory.

> If two-ended normalization changes any **currently reachable** figure, stop and
> report. Heatmap is producerless, so nothing authored can move; a moved pixel
> means something reaches this kernel that the census did not find.

## Supersession

Supersedes no record. Consumes PRODUCT-0087's gradient omission rule for both
degenerate cases without amending it. Discharges PRODUCT-0087 gate 16's banked
signed-clamping limitation. Leaves PRODUCT-0087 gate 17's bubble anchoring to PRODUCT-0089.

## Rejected

**Define a heatmap-side aggregation (sum, mean, or last).** Attractive because it
accepts every relation and needs no author change. Rejected because it makes a
mark an aggregation authority, reversing PRODUCT-0078, PRODUCT-0081, and
PRODUCT-0082 §6 — and because any choice among sum, mean, and last is a
statistical decision the mark has no basis to make.

**Admit equal-valued duplicates.** Attractive as a narrower rule that refuses
only genuine ambiguity. Rejected because acceptance would then depend on the
data rather than the shape: one query succeeds and fails as rows change, and the
mark decides two observations are one whenever they agree.

**Keep zero-filled absent cells and document it.** Attractive because it needs no
representation change and heatmaps "look complete". Rejected because a fabricated
zero is an invented observation, and PRODUCT-0089's diverging ramps would paint it the
**neutral** colour — the most confident-looking possible rendering of data that
does not exist.

**Paint a constant grid at the ramp's high end, preserving today's look.**
Attractive as parity. Rejected because today's high end is an artifact of
`val / max_val` — the formula §3 removes — and it disagrees with bubble, so it is
not parity with anything except a defect.

**Fix zero anchoring here.** Attractive because §3 makes signed domains real and
anchoring is the obvious next question. Rejected because anchoring is a palette
semantics decision spanning heatmap and bubble, and B2 has no authority over
bubble's normalization.

## Consequences

Heatmap becomes deterministic: the same relation renders the same figure
regardless of row order, because an order-dependent input now refuses instead of
resolving silently.

Sparse grids stop lying. Absent cells disappear from the figure, from hover, and
from the domain, which also makes the domain narrower and therefore the colour
range more useful on real data.

Signed data becomes representable, which is the prerequisite PRODUCT-0089 needs — and B2
deliberately stops there. A diverging ramp on `-50…100` still puts its neutral at
25, in heatmap exactly as PRODUCT-0087 banked it for bubble. The two marks now
share one shape of defect, stated identically, waiting on one decision.
