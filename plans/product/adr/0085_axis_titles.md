# ADR 0085: axis titles — a derived step and an authored spelling

Status: proposed 2026-08-12. This record fixes an axis title that is false and
one that is absent. It inserts a **derived** step into the axis-title cascade
that already exists, and gives the already-existing authored tier a Rune
spelling — `x_label` and `y_label` — closing ledger item **DB-19** for axis
labels.

## Context

Two docketed consumers fail differently, and a single rule cannot serve both.

**The percent axis says something false.** `StackedBarPlotKernel::layout`
(`plot/kernels/stacked_bar.rs:168-174`) selects `AxisType::Percent` when
normalization is percent, while `y_label()` (`:311`) returns
`prepared_data.y_label` unconditionally — the raw column name. The kernel knows
it applied a transform and the title path never asks, so a percent chart of
`revenue` labels its value axis `revenue` against ticks reading `0%…100%`.
`stacked_area.rs:69` reaches the same scale by the same route.

**The box plot axis is absent.** PRODUCT-0082 §3 set `y_label` empty, because ADR
547's relation carries six summary columns and **no name for the measure they
summarize**. Labelling it `median` would name one of six things it shows.

The distinction is the record's spine: percent has a base name and a transform to
compose; the box plot has neither, so derivation has no input at all. DB-19's
trigger — "the first request for prose axis labels" — is PRODUCT-0082.

## Decision

### 1. One new step inside the existing `Presence` cascade

An earlier draft proposed a four-step ladder as though nothing existed. It does:
`PlotSettingsLayer` already carries `x_label: Presence<Arc<String>>` and
`y_label: Presence<Arc<String>>` (`plot/theme.rs:211-212`), and
`draw_axis_labels` already resolves them against the kernel's own label
(`plot/common.rs:1198-1200`). What is missing is a Rune spelling for the authored
tier and a **derived** step between the cascade and the kernel.

The rule, stated over what exists:

```
resolved Show      →  the selected cascade text
resolved Hide      →  no title
resolved Fallback  →  derived  →  kernel fallback  →  empty
```

**`Show` is not synonymous with "authored".** The `Presence` is resolved through
the five-tier cascade, so the selected text may come from the application theme,
the authored spec, a layer, or a pane override. `x_label` and `y_label`
contribute a `Show` **at the authored tier** and change no tier's precedence;
this record adds a way to write into an existing cascade, not a new winner over
it.

Two corrections this makes to the earlier draft:

- **`Hide` is a real outcome and outranks derivation.** A hidden axis title stays
  hidden on a percent chart; the derived text is not a floor that defeats a
  resolved `Hide`. There is deliberately no *authored* hiding surface (§9), so
  `Hide` reaches the cascade from a non-authored tier.
- **The third step is the *kernel fallback*, not "the source field".** It is
  usually one column name, but not always: PRODUCT-0147 replaces
  `CandlestickPlotKernel`'s former literal `"Value"` with the joined field-name
  fallback `"open / high / low / close"`. Calling the step a source field would
  still misdescribe it. (An earlier draft also cited `histogram.rs:282`; that is
  test-only — production histogram preparation sets `y_label: count.to_owned()`
  at `:179`, which *is* a column name.)

Percent is fixed at the *derived* step and needs no authoring; the box plot is
fixed at the *authored* step and cannot be fixed at any other.

### 2. The spelling is `x_label` and `y_label`

```rune
figure([
    y_label("Observed value"),
    box("category", "whisker_low", "q1", "median", "q3", "whisker_high", "outlier"),
])
```

Two new figure elements, registered as `StringConstructor` variants beside
`title` (`rune_presentation.rs:585`). The names match DB-19's own wording and the
kernel vocabulary — `PlotKernel::x_label()` / `y_label()` and the `x_label` /
`y_label` fields on every prepared-data struct. `title(...)` is taken and means
the figure title; reusing it for axes would collide with a shipped member.

### 3. An authored label names the **rendered** axis

`y_label(...)` titles whatever the y axis displays, not whatever the second
channel was bound to.

This follows what the kernels already do rather than inventing a rule:
`StackedBarPlotKernel`'s `x_label()` and `y_label()` **swap by orientation**
(`:304-315`), so the label methods have always named rendered axes. Under
PRODUCT-0055 a horizontal bar puts categories on y, and `y_label("Sales region")`
titles that categorical axis — which is also what an author looking at the figure
expects, since the rendered orientation is what they can see.

### 4. Percent-of-total derives `revenue (% of total)`

Derivation is keyed to **`StackNormalization::Percent`** — the transform that was
actually applied — and **not** to `AxisType::Percent`. Both percent kernels route
through **one derivation helper**, which takes explicit transform provenance and
the kernel's base label:

```rust
fn derive_axis_title(base: &str, transform: AxisTitleTransform) -> String;
enum AxisTitleTransform { PercentOfTotal }
```

Two local `format!` calls in `stacked_bar.rs` and `stacked_area.rs` would satisfy
every pixel gate while letting the "table of one" drift into two tables that
disagree. Gate 3 asserts both kernels consume the same helper structurally, not
merely that both produce the same string today.

That distinction is the section's whole content. `AxisType::Percent` is a **tick
formatter**, not transform provenance: a field whose values are already
percentages would render percent-formatted ticks without anything having been
normalized, and keying on the formatter would label it `(% of total)` — a false
statement produced by the very mechanism meant to stop one. Reading provenance
off a downstream artifact is the error this record exists to fix, and an earlier
draft committed it.

Given the transform, the title composes the kernel's base label with it:

```
revenue          →  revenue (% of total)
```

The measure is preserved and the transformation is named, which is what makes the
title true against `0%…100%` ticks. `% of total` alone would lose which measure;
leaving `revenue` is the present defect.

**This wording is scoped to percent-of-total and no further.** `AxisType` also
admits `Date`, `Datetime`, and `Time`, and any future transformed scale must earn
its own wording against its own exhibit rather than inherit a pattern chosen
here. Derivation is a table of one entry, deliberately.

### 5. Categorical axes accept authored labels

`x_label("Sales region")` on a bar chart is meaningful and is **not** refused. An
authored title is prose about what an axis shows, and a categorical axis shows
something nameable; refusing it would be a restriction with no reason behind it.

Both the categorical and the quantitative case are gated, because "labels work"
is easy to satisfy for one and miss for the other.

### 6. Authored text replaces the derivation verbatim

An authored label is used exactly as written. It is not composed with the
derivation, not suffixed, and not adjusted for the scale.

`y_label("Share")` on a percent chart renders `Share` — **not** `Share (% of
total)`. The author who overrides a derived title has seen the derivation and
chosen different words; appending to their text would be the record second
-guessing them, and would make the authored step a modifier of the derived step
rather than a replacement for it.

### 7. An empty or whitespace-only label refuses

`y_label("")` refuses at the argument's span. It is not accepted as a way to
suppress a title.

The reason is **not** that hiding already has a spelling — it does not. `Hide` is
a real state in the internal cascade, but no `hide_x_label()` or
`hide_y_label()` exists in `PRESENTATION_FREE`, so there is currently no way to
author it. The reason is the opposite: an empty string must not **smuggle in**
a hiding surface that has not been earned. The draw path already drops empty
text (`common.rs:1201`), so `y_label("")` would work — by accident, unnamed,
undiscoverable, and impossible to gate as a decision.

Whitespace-only is refused with it, since `y_label("   ")` differs from
`y_label("")` only in being harder to see. An explicit hiding surface is named in
§9 and stop condition 4; until one is earned, hiding is not authorable and this
record does not pretend otherwise.

### 8. No persistence change

Axis titles are authored in Rune source, which PRODUCT-0060 already persists as
the document. Nothing enters `polariton_session`: the source stays durable, a
restored panel is unresolved until Rune reruns, and the rerun re-derives the
title. Schema stays at v2.

### 9. What this record does not decide

No title for any transform other than percent-of-total. No authored figure title
changes — `title` and `subtitle` are untouched. No legend titles, despite DB-19
mentioning them in the same breath: a legend title has its own placement and
merging questions, and PRODUCT-0044's policy owns them. No refusal of a
categorical authored label, per §5. No axis-title styling — colour and size stay
with `axis_label_text` chrome.

**No authored hiding surface.** The cascade's `Hide` state exists internally and
is honoured (§1, gate 5), but no `hide_x_label()` / `hide_y_label()` is added.
An author wanting no title today omits the member and accepts the derived or
kernel text; wanting to *suppress* it is a real request nobody has made, and §7
refuses the empty string precisely so it cannot arrive unnamed.

## Implementation sequence

**Stage 1 — derivation.** The percent composition of §4, with no surface. Fixes
the false title on stacked bars and stacked areas before any spelling exists.

**Stage 2 — the spelling.** Both functions through the complete PRODUCT-0076
presentation path, not a direct-call shortcut: `PRESENTATION_FREE`
(`rune_view_compiler.rs:23`), origin injection, native registration,
`FigureElementKind` (`rune_presentation.rs:164`), `FigureParts`, and the authored
settings layer that `theme.rs:211-212` already defines. Gate 13 requires a
**helper-produced** label to compile, so the members are evaluated presentation
values rather than a spelling that only works when written literally.

**Stage 3 — the exhibit.** `uat/test26.rn`'s box-plot views author
`y_label("Observed value")` — **not** `"Revenue"`, which an earlier draft
proposed: the fixture summarizes `box_probe.v`, which is not revenue, and a
confidently wrong exported title is the exact failure class this record closes. A
percent view is added showing the derived title unauthored.

## Acceptance gates

1. **Derivation keys on the transform, not the formatter.** Three cases:
   a percent-normalized stacked bar over `revenue` renders `revenue (% of
   total)`; the **same figure without `.percent()`** renders `revenue`; and a
   figure whose axis carries **`AxisType::Percent` with no
   `StackNormalization::Percent`** renders its base title **unchanged**. The
   third case is the discriminating one — an implementation keyed on the tick
   formatter passes the first two and falsely labels the third.
2. **Derivation follows the rendered value axis, in every orientation.** Three
   cases, because a stacked-bar implementation deriving only `y_label()` passes
   every other derivation gate here:
   - a **vertical** percent bar carries the derived title on **y**;
   - a **horizontal** percent bar carries it on **x** (`stacked_bar.rs:174`);
   - a percent **stacked area** carries it on y (`stacked_area.rs:69`).

   In each case the **category axis keeps its unchanged fallback**, so derivation
   is asserted to move with the value axis rather than to be applied to both.
3. **One derivation authority.** Both kernels consume `derive_axis_title`, and no
   axis-title string is assembled by a local `format!` in either — asserted
   structurally, since two independent formatters produce identical output today
   and diverge on the next transform.
4. **Authored replaces, never composes.** `y_label("Share")` on a percent chart
   renders exactly `Share` — asserted as string equality, so an implementation
   appending `(% of total)` fails.
5. **The cascade resolves in order, including `Hide`.** A resolved `Show` — from
   whichever tier the cascade selected — beats derived; derived beats the kernel fallback; the kernel fallback beats empty;
   and a box plot with neither stays **empty** rather than inventing a title.
6. **`Hide` suppresses even a derived title.** An axis resolved to `Hide` on a
   **percent** chart renders no title at all — not `revenue (% of total)`.
   Without this, derivation would act as a floor that defeats an authored
   `Hide`.
7. **Categorical axes accept authored labels.** `x_label("Sales region")` renders
   on a vertical bar chart's category axis, and a quantitative axis accepts one
   too — both asserted, since satisfying one does not imply the other.
8. **Kernel fallbacks swap with orientation.** With no authored label, flipping
   a bar from vertical to horizontal moves the kernel's own labels with the
   channels, per the swap at `:304-315`.
9. **Authored labels do *not* swap.** Vertical and horizontal fixtures authoring
   the **identical** strings — `x_label("A")`, `y_label("B")` — both render `A`
   on the rendered x axis and `B` on the rendered y axis. Identical strings are
   required: independently tailored fixtures cannot distinguish rendered-axis
   semantics from channel semantics, because either reading produces a
   plausible-looking figure.
10. **Faceted grids title once per side.** Under PRODUCT-0048's hoisted axis text,
   an authored label appears once per side rather than once per cell.
11. **Duplicate authored labels refuse at the second span.** Two `y_label` members
   in one figure refuse, per PRODUCT-0016 §2's order-meaningless rule — no
   last-wins.
12. **Empty and whitespace-only labels refuse.** `y_label("")` and
   `y_label("   ")` each refuse at the argument's span; `y_label("Share")`
   renders verbatim. Two properties beyond the refusals: a nonempty twin is
   required or a blanket string refusal passes both refusal cases; and
   `y_label(" Share ")` renders **byte-for-byte unchanged, with its spaces** —
   distinguishing "trim only to test emptiness" from silently trimming authored
   prose.
13. **The members are evaluated presentation values.** A **helper-produced**
    label compiles and renders — `fn axis() { y_label("Observed value") }` used
    inside `figure([...])` — proving the spelling went through PRODUCT-0076's
    presentation path rather than being recognized only as a literal call. A
    direct-call-only implementation passes every other gate here.
14. **No persistence change.** A session containing an authored axis title is
   schema v2, the title appears nowhere in the `polariton_session` owner payload,
   the restored panel is unresolved, and rerunning restores the title from
   source.
15. **The exhibit reads correctly.** `uat/test26.rn` renders `Observed value` on
    the box plot's value axis — the fixture summarizes `box_probe.v`, so any
    revenue-flavoured title would be false — and a derived `revenue (% of total)`
    on the percent view.

## Stop condition

1. **A second scale transform wants a derived title.** §4 is deliberately a table
   of one. A temporal or log scale must decide its own wording against its own
   exhibit; inheriting `X (% of total)`'s pattern is not a decision.
2. **An author wants to compose with the derivation.** §6 makes authoring a
   replacement. A request to append to or wrap the derived text is a different
   feature — a title template — and needs its own record.
3. **Legend titles are requested.** DB-19 names them alongside axis labels; §9
   excludes them because their placement and merging belong to PRODUCT-0044.
4. **An author asks to suppress an axis title.** §7 refuses the empty string and
   §9 adds no hiding spelling, so the request has nowhere to land — which is the
   point. The internal `Hide` is ready and a record can spell it in one member
   once someone wants it; reaching for `y_label("")` again is the signal, not a
   workaround.

## Supersession

Closes ledger item **DB-19** for axis labels only; its legend-title half remains
open per §9. Amends no record: PRODUCT-0082 §3's blank value axis is now
authorable rather than overturned, and its reasoning — that the relation carries
no measure name — is exactly why the *authored* step is the one that fixes it.

## Rejected

**Derive `% of total` without the measure.** Attractive because it is short and
unambiguous about the transform. Rejected because it discards which measure the
axis shows, and a figure that travels as an exported image cannot recover it.

**Label the percent axis from the `median` column** (the box-plot analogue).
Attractive because it produces *a* name from data already present. Rejected for
PRODUCT-0082 §3's reason: naming one of six summary columns describes one of six
things the axis shows.

**Compose authored text with the derivation.** Attractive because
`Share (% of total)` looks more informative than `Share`. Rejected because the
author overriding a derived title has already seen it; appending would make
authoring a modifier rather than a replacement, and would leave no way to say
plainly `Share`.

**Refuse authored labels on categorical axes.** Attractive by symmetry with
PRODUCT-0036's bounds, which do refuse there. Rejected because the reason bounds
refuse — a band axis has no numeric domain to bound — has no analogue for prose:
a categorical axis shows something, and naming it is meaningful.

**Reuse `title(...)` with an axis argument.** Attractive as one member instead of
two. Rejected because `title` is a shipped member meaning the figure title, and
overloading it by argument shape is the kind of positional inference
PRODUCT-0082 §1 refused.

## Consequences

Every percent figure improves without being touched, since the derivation needs
no authoring — and the box plot's blank axis becomes the author's choice rather
than a limitation.

The ladder gives every future scale transform a defined place to put its title,
and every mark without a derivable name a defined way to get one. That is the
general shape the two docketed consumers were asking for from opposite
directions.

DB-19 has been open since the architectural review, on the trigger "the first
request for prose axis labels." PRODUCT-0082 was that request, arriving as a
deliberately blank axis rather than as a feature ask — which is the same way
PRODUCT-0079's stop condition produced PRODUCT-0080: a record declining to invent
something, and naming what would be needed instead.
