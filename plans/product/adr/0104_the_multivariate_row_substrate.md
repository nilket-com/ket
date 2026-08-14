# ADR 0104: the multivariate row substrate

Status: proposed 2026-08-13. Radar and parallel coordinates declare the **same
six prepared fields with the same names and types**, prepared by near-copy
functions, and both resolve `dimensions` as all numeric fields through the same
error. They are one substrate wearing two names. This record gives them one —
owning the ordered dimension list, validated rows, optional category identity,
and derived ranges — and leaves radial closure to radar and Cartesian polylines
to parallel.

It is step 19 of `plans/wip/plot-catalog-restoration.md`. Prerequisite **D**
belongs to the surface that follows, not here.

## Context

### The two marks are already one

`RadarPreparedData` (`radar.rs:184-191`) and `ParallelPreparedData`
(`parallel.rs:102-109`) declare `dimensions`, `ranges`, `data_matrix`,
`row_categories`, `category_names`, `num_categories` — identical names, identical
types. Both resolvers bind `dimensions: numeric.clone()` with the same
`ChannelError` on empty (`spec/mod.rs:2963-2978`). `prepare_parallel_data` and
its radar twin are near-copies.

The plan sequenced these two together to avoid "solving it twice and producing
two vocabularies." The vocabularies are not merely similar — they are the same
one, written twice.

### Six truths, three derived

`dimensions`, `data_matrix`, and `category_names` are primary. `ranges` derives
from `data_matrix`. `num_categories` is `category_names.len()` stored separately —
the `total_sum` pattern from PRODUCT-0102, at its second site. And
`row_categories: Vec<usize>` is an **index indirection** into `category_names`.

### The indirection fails silently into category zero

`row_categories.push(*cat_to_idx.get(&cat_val_str).unwrap_or(&0))`
(`parallel.rs:191`). A category that fails lookup is **attributed to category
zero** — not dropped, not refused, but drawn as a different series. The row still
renders, so nothing signals it.

That is the radial-dial `.get(0)` defect class in index form, and PRODUCT-0099
removed the original.

### Fabrications, in `parallel.rs` and mirrored in radar

Empty input invents `"No Data"` with `num_categories: 1` and `(0.0, 1.0)` ranges
per dimension (`:117-124`). A missing or uncastable dimension column becomes an
**empty series** (`:131-135`) whose `min`/`max` then yield `(0.0, 1.0)` and whose
every cell becomes `0.0`. A missing category column invents `"All Data"`.
`.unique()` and `.sort()` each fall back to the raw column (`:147-149`). Cell
nulls become `0.0` via `unwrap_or` (`:182`).

### Duplicates are not the question here

Unlike heatmap, funnel, radial dial, and pie, these marks are **inherently
multi-row per category** — each row is one polyline. Prerequisite **E**'s
uniqueness answer does not transfer, exactly as PRODUCT-0099 predicted when it
scoped E to three specific marks.

### Normalization lives in paint

Paint recomputes the ramp position from `num_categories` (`:37`), derives axis
spacing (`:79`), and computes `y_scale` from `ranges[i]` and bounds (`:84-85`) —
the split that let PRODUCT-0102's pie paint and hover disagree.

## Decision

### 1. One substrate for the data, two geometries

A shared `MultivariateRows` owns the **ordered dimension list**, the **validated
rows**, the **category catalog** (§3), and the **observed extrema** per dimension.

**Extrema, not display ranges.** The two marks do not agree on display ranges and
must not be made to: radar pads each observed range by 10% with
`(max - min).max(1e-6)`, parallel uses raw `min`/`max`. A shared `ranges` would
force one of them to move, and gate 17's parity claim would be false for that
mark. The substrate owns what is observed; **each kernel derives its own display
range from it, preserving its existing rule.**

Geometry stays per-mark: **radar owns radial closure**, **parallel owns Cartesian
polyline geometry**. This is PRODUCT-0094's split applied one layer down — share
what is identical, keep separate what differs by meaning.

### 2. Renderers receive resolved categories, not indices

`row_categories` and `num_categories` are **removed**. A row carries its resolved
category directly; a count is `len()`.

This deletes the `unwrap_or(&0)` fallback structurally rather than guarding it.
An index into a parallel vector is a second representation of an identity the row
already has, and the fallback existed only because the two could disagree.

### 3. The category catalog is `Vec<Option<String>>`

One slot per colour-bearing category, in a pinned catalog:

| relation | catalog |
|---|---|
| grouped | lexical `Some(name)` per distinct category |
| ungrouped, non-empty | `[None]` — exactly one slot |
| empty | `[]` |

Non-empty ungrouped rows carry `None` and still need **one** colour slot, which
`[None]` supplies without inventing a name. Presentation alone maps `None` to
`"All Data"`, per §8.

An earlier draft said rows carry `Option<String>` and stopped there, leaving the
catalog — and therefore the ungrouped colour slot — unspecified.

### 4. Dimension order is preserved exactly; **D** is the surface's

The substrate accepts an ordered `Vec<String>` and preserves it. It never sorts
dimensions.

**Automatic** figures continue supplying schema order. **Authored** `radar(...)`
and `parallel(...)` make the order explicit — and that spelling is prerequisite
**D**, which belongs to the surface record.

Sorting dimensions anywhere would erase authored meaning: the axis sequence is
the figure's design, and on a radar it determines the shape of every polygon.

**This is the first step in the arc where the substrate does not consume its
prerequisite.** D is an authoring problem; the substrate already receives the
list.

### 5. Three orderings, three answers, in one substrate

| ordering | answer | why |
|---|---|---|
| **dimension** axis order | **preserved exactly** | authored design; §4 |
| **category** order | **lexical** | colour and legend stability across runs |
| **row** order, **globally** | **relation order** | rows are observations, and paint order is compositing |

Row order is **global**, not per-category. Both kernels paint rows sequentially
and radar's polygons are **translucent**, so regrouping rows by category would
change what composites over what — a visible change with no authored cause.

Funnel, radial dial, and pie each answered *one* ordering question. This substrate
answers three at once, differently, and the reasons differ — so the table is
recorded together rather than distributed, for the reason PRODUCT-0102 §4 gave:
a later consistency pass must argue with three stated reasons rather than notice
three behaviours.

### 6. Many rows per category, never aggregated

Multiple rows sharing a category are **valid** and each draws its own polyline.
The mark does not aggregate them, does not average them, and does not select a
representative — the standing rule, here with no uniqueness counterpart.

### 7. A null or non-finite cell refuses the whole row

A selected dimension whose cell is null, non-finite, or inexact **refuses the
entire row**, naming the **dimension** and the **1-based row**.

**Not dropped.** Silently omitting the row would be another selection authority —
the mark deciding which observations count — which is the class PRODUCT-0099
removed from radial dial and PRODUCT-0102 from pie. A polyline missing one
vertex is not a shorter polyline; it is an unmeasured figure.

**Not zero-filled**, which is what `unwrap_or(0.0)` (`:182`) does today: a zero
cell on a normalized axis is a real reading at the axis minimum.

### 8. Category identity is honest, and absence is not a label

A **selected** category field containing null **refuses**. An **omitted** category
field is `None` — semantic absence carried through the substrate — with any
`"All Data"` label added **only in presentation**, per PRODUCT-0099 §5 and
PRODUCT-0100 §7.

### 9. Missing and uncastable fields refuse

No empty-series substitution (`:131-135`), no `(0.0, 1.0)` range invented from an
empty column, no `"All Data"` from an unreadable one. Conversion uses the shared
`exact_f64` (`plot/exact_numeric.rs:9`).

### 10. Extrema derive from validated rows; empty is honestly empty

Extrema are computed after §7 has refused, so no fabricated cell can widen or
narrow an axis.

**An empty relation yields no rows and no extrema** — honestly empty, with the
`"No Data"` figure removed. But **the layout substrate stays**: parallel's axis
renderer indexes `ranges[i]` and radar's tick-label path does the same, so
removing the invented ranges without a replacement would panic.

Each kernel therefore supplies its **existing `(0.0, 1.0)` display fallback** when
extrema are absent. The substrate stops inventing data; the geometry keeps
drawing axes.

### 11. One validated collection, two layers

```rust
prepare_multivariate_rows(fields, ...) -> Result<MultivariateRows, MultivariatePreparationError>
MultivariateRows::from_rows(fields, rows) -> Result<MultivariateRows, MultivariateRowsError>
```

Opaque per PRODUCT-0090 §6, with a trybuild compile-failure case and a valid
twin. Two layers per PRODUCT-0090 §5.

**Precedence**, field-first per PRODUCT-0096 §9:

| phase | order | error |
|---|---:|---|
| preparation | 1 | dimension field missing |
| preparation | 2 | dimension field not quantitative |
| preparation | 3 | category field missing (when selected) |
| preparation | 4 | category field not a supported nominal dtype |
| row-local | 5 | null category |
| row-local | 6 | null, non-finite, or inexact cell — **first dimension in list order** |

Dimension fields are checked in **list order**, so the reported dimension does not
depend on which fault kind exists elsewhere. Rows scan in producer order;
`category` before cells within a row.

### 12. What this record does not do

No Rune word and no variadic spelling — that is **D** and the surface's (§4). No
geometry: radial closure and polyline layout stay where they are, and **each
kernel keeps deriving its own display range** from the shared extrema, including
radar's 10% pad and each mark's `(0.0, 1.0)` empty-case fallback. No colour or
authored family. No aggregation (§6). Neither mark's paint changes beyond
consuming resolved categories instead of indices and reconstructing its display
range locally.

Both marks remain producerless.

## Implementation sequence

**Stage 0 — bank.** Every radar and parallel figure, as a **refactor oracle** —
both are producerless. The fabricating fixtures are banked as **negative
controls**.

**Stage 1 — the collection.** `MultivariateRows`, its error, §10's precedence.

**Stage 2 — the boundary.** Planning validates; the factory reconstructs.

**Stage 3 — both kernels.** Each consumes the shared collection; the six parallel
fields, the index indirection, and every fallback are deleted — not guarded.

**Stage 4 — re-bank.** The parity bank the two surface records consume.

## Acceptance gates

1. **One collection serves both marks.** Radar and parallel consume the same type;
   no second multivariate prepared struct exists. Structural — the record's
   premise is that two identical vocabularies became one.
2. **Geometry stays separate.** Radar's radial closure and parallel's polyline
   layout are unchanged, and neither appears in the shared collection.
3. **`row_categories` and `num_categories` are gone.** Rows carry resolved
   categories; the `unwrap_or(&0)` fallback does not exist. Structural, per §2 —
   a row silently attributed to category zero still renders, so no behavioural
   gate catches it.
4. **The index and its fallback are gone, and categories are retained.** Two
   parts, neither a new refusal: the `cat_to_idx` map and `unwrap_or(&0)` do not
   exist (structural), and a valid grouped relation retains **every** row's own
   category through the collection — asserted per row, not per catalog.

   An earlier draft asserted that a would-be index miss "now refuses." Under §2's
   direct representation there is no lookup to miss, so that gate was unreachable
   and invented a refusal this record does not define.
5. **Dimension order is preserved exactly.** A dimension list in non-schema,
   non-alphabetical order renders in **list** order on both marks. Asserted
   structurally too, since a fixture can coincide.
6. **Category catalog is lexical; global row order is relational.** One
   **interleaved** fixture — categories alternating down the relation — asserting
   the catalog is lexically sorted **and** that rows paint in unaltered **global**
   relation order. Interleaving is what discriminates: a fixture whose categories
   arrive already grouped cannot tell global order from per-category regrouping,
   and regrouping is visible on radar because its polygons are translucent.
7. **A null or non-finite cell refuses the row**, naming dimension and 1-based
   row — and the row is **not** dropped and **not** zero-filled. Three parts,
   since dropping and zero-filling both produce a figure.
8. **Exactness is shared.** `2^53` accepted, `2^53 + 1` refused, `2^53 + 2`
   accepted, through `exact_f64`.
9. **Selected null category refuses; omitted category is `None`.** The pair, and
   `"All Data"` appears **nowhere** in the collection or its errors.
10. **Missing and uncastable fields refuse, per role.** Dimension and category,
    each absent and each incompatible — four cases, no empty-series substitution.
11. **Many rows per category all render.** Three rows sharing a category draw
    three polylines; none is aggregated, averaged, or dropped.
12. **Extrema derive only from validated rows.** Asserted through **valid**
    `from_rows` input: a collection built from rows that all pass yields extrema
    equal to those rows' min and max per dimension.

    An earlier draft asserted extrema "excluding" a refused row's extreme value —
    unsatisfiable, since §7 refuses the whole *collection*, which then exposes no
    extrema at all. The refusal itself is gate 7's; this gate is derivation.
13. **An empty relation yields no rows and no extrema, and still draws axes.**
    Three parts: no rows, no extrema, no `"No Data"` invention — **and** each
    kernel's axis renderer completes over its `(0.0, 1.0)` display fallback rather
    than panicking on `ranges[i]`, which is what removing the invented ranges
    without a replacement would do. Paired with an empty relation missing a field,
    which refuses.
14. **The collection cannot be built by literal.** Trybuild failure with a valid
    twin.
15. **Precedence is deterministic.** Field-first, dimensions in list order, then
    row-local with category before cells — with a missing-dimension-plus-null-row
    pair and a two-bad-dimensions pair proving list order decides.
16. **All fabrications are gone, not guarded.** `"No Data"`, `"All Data"`,
    `Series::new_empty`, `.unique()`/`.sort()` fallbacks, and cell `unwrap_or` are
    absent from preparation.
17. **Well-formed non-empty figures are unmoved, both marks.** Every banked radar
    and parallel figure over a valid non-empty relation is byte-identical —
    including radar's padded ranges and parallel's raw ones, which §1 keeps
    kernel-local precisely so this holds.

    **The empty case is excepted**: §10 removes the `"No Data"` invention, so an
    empty relation renders axes over each kernel's `(0.0, 1.0)` fallback and **no
    series**, where it previously drew an invented category. Gate 13 asserts that
    change.

## Stop conditions

> If sharing the collection requires either mark's geometry to move into it, stop.
> §1's split is data-shared, geometry-separate; a geometry that must be shared
> means the two marks are more alike than their rendering suggests, and that is a
> finding rather than a refactor.

> If refusing a row for one bad cell breaks a shipped fixture, stop. §6 chose
> refusal over dropping deliberately, and a fixture that depends on partial rows
> is evidence the mark is being read as tolerant of missing dimensions — a surface
> question.

> If preserving dimension order requires the surface's variadic spelling to exist
> first, stop. §3 claims the substrate already receives an ordered list; needing
> **D** here means this record and its surface cannot be separated after all.

> If a third mark turns out to declare the same six fields, stop and widen the
> survey before landing. Two identical vocabularies justify one substrate; three
> would mean the shape is general and the naming should not be radar-and-parallel
> specific.

## Supersession

Supersedes no record. Removes the index-with-fallback defect class PRODUCT-0099
removed from radial dial, at its second site. Adopts PRODUCT-0090 §5's two layers
and §6's opacity, PRODUCT-0096 §9's field-first precedence, and the shared
exactness authority. **Does not consume prerequisite D**, which is the surface's.
Prerequisite **E** does not apply (§6).

## Rejected

**Two substrate records, one per mark.** Attractive because radar and parallel are
different figures and each could state its own contract. Rejected because their
contracts are already textually identical — writing them twice is how the two
vocabularies the plan warned about would appear, and the second would drift from
the first at its first amendment.

**One substrate including geometry.** Attractive because the split adds a seam.
Rejected because radial closure and Cartesian polylines differ by meaning, not by
detail, and a shared geometry would have to be parameterized by which mark is
asking — which is the mark distinction reintroduced as a flag.

**Keep `row_categories` as an optimization.** Attractive because index lookup is
cheaper than carrying resolved categories. Rejected because the fallback that made
it wrong exists precisely to paper over the two representations disagreeing, and
neither mark is in a hot path where the indirection was measured to matter.

**Drop rows with a bad cell instead of refusing.** Attractive because a polyline
missing one vertex could be drawn shorter, and most rows would survive. Rejected
because deciding which observations count is a selection authority — the class
removed from radial dial and pie — and a shortened polyline asserts a shape the
data does not have.

**Sort dimensions for a stable axis order.** Attractive because it would make
figures reproducible without authored order. Rejected because on a radar the axis
sequence determines the polygon's shape: sorting would silently redraw the figure
into a different one.

## Consequences

Two near-copy preparations become one, and the arc's most duplicated shape stops
being duplicated. The saving is not the lines — it is that the next amendment to
this contract lands once.

The index-with-fallback defect disappears structurally. A row can no longer be
attributed to a category it does not have, because there is no index to miss.

Three ordering questions are answered in one place with three different answers.
That is the sharpest example yet of why this catalog resists a uniform rule:
dimension order is authored design, category order is presentation stability, and
row order is the relation's — and a record that had picked one policy for
"ordering" would have been wrong twice.

Prerequisite **D** stays open, and this is the first step where the substrate does
not consume its own prerequisite. The variadic spelling is an authoring problem;
the substrate has always received a list and only needed to stop reordering it.

One mark remains after the radar and parallel surfaces: violin.
