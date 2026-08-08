# ADR 0050: legend placement and visibility

Status: proposed 2026-08-08. This record decides three things that interlock:
the default legend placement moves to a reserved gutter so a legend stops
covering the marks it describes; the figure gains one legend property —
`legend_at(x, y)` and `hide_legend()` — spelled over `Presence`, giving the
language its **first authored `Hide`**; and the `Presence::Hide` resolution
pipeline is repaired (DB-18, the 0016 §6 defect), because a record cannot
ship an authored Hide through a pipeline that renders hidden things. It is
the record 0044's stop condition named as its expected trigger.

## Context

**The docket arrived with four exhibits.** 0044 §4 placed the single figure
legend at figure bounds and pre-filed the consequence: "a figure-bounds
legend may overlay a cell, and that is the status quo, not a defect …
evidence for the deferred placement record." The evidence came: the legend
covered North's panel in `test15.rn`, Beacon's in `test16.rn` twice, and
0048's tighter cells made it worse — pre-filed there too, "its fourth
exhibit, not a regression." 0044 §4 also named both halves of the remedy:
"space reservation and authored placement are the deferred placement work."
This is that record, for both halves.

**The visibility half has a prerequisite fifteen records old.** 0016 §6
documented the `Presence::Hide` round trip as broken — `theme.rs:283` maps
`Hide` to `None`, `common.rs:709` maps `None` to the generated label, so
hiding would *render* — and specified the repair: the resolved value must
preserve three states, `Fallback | Show(text) | Hide`, because an
`Option<String>` forecloses the fix. 0016 §11 named the repair as its
expected halt trigger, "a renderer and resolution change with its own
evidence." The evidence is here: an authored legend `Hide` is the first
production `Hide` in the language, and it cannot be honest until the
pipeline is.

**The algebra already exists.** `Presence<T> = Inherit | Show(T) | Hide` is
one of PRODUCT-0007 §6's two cascade algebras, defined for exactly the
properties "that can be turned off." A legend is one. No new algebra, no
new tier, no new mechanism beyond the repair the algebra was always owed.

## Decision

### 1. The default placement is a reserved legend gutter

A figure whose resolved legend content is non-empty reserves a **right-side
gutter** sized to that content, and the legend renders there — outside every
cell, top-aligned. Figures with no legend content (0044's omissions: bare
marks, band-filled bars) reserve nothing and are **byte-identical to
today**. Under a grid, the gutter joins 0048's gutter model on the remaining
side; cell congruence is untouched because the gutter is figure-level, not
per-cell.

This visibly changes every figure that has a legend, single plots included —
the wallpaper scatter's "All Data" box sat on the marks the same way the
faceted legends sat on Beacon. One rule for all figures, the 0021 posture:
the pixels change, and the record says so loudly. Draw baselines that embed
legend positions are refreshed as premise updates (0014 §9a), enumerated in
evidence.

### 2. One legend property, `Presence`-shaped

```text
legend: Presence<LegendPosition>

Inherit        the §1 default: gutter placement
Show(x, y)     authored position, fractions over figure bounds
Hide           no legend, no gutter
```

The property lives in the authored tier of the existing cascade beside the
resolved `legend_x`/`legend_y` it generalizes; application and pane tiers
are future producers of the same property, per the 0030 §3 staging. This is
the first authored `Hide` in the language — the `Presence` algebra's
turn-off arm, reached from source eight records after the algebra shipped.

An authored `Show(x, y)` places the legend at the authored fractions **over
the figure's content bounds, gutter forgone** — reproducing today's overlay
placement exactly when the author asks for it. An authored position may
cover marks; that is the rope (0030 §4, 0035 §1): overlap chosen by an
author renders, overlap chosen by a default was the defect.

### 3. The `Presence::Hide` pipeline is repaired for every consumer

The resolved heading type becomes the three-state 0016 §6 specified:

```text
Fallback | Show(text) | Hide
```

`apply_presence` stops flattening `Hide` into `None`, and the renderer maps
`Hide` to **nothing** — for the canvas and for the legend property alike.
The 0016 §6 defect dies at the exact boundary it was documented at.

**The rule is scoped to the canvas chain, and the chrome chain survives the
repair untouched.** 0016 §7 split the two chains deliberately, and chrome
already has a live, correct consumer: `editor.rs:888` folds an authored
`Hide` in with `Inherit` and falls through to the generated label — as it
must, because chrome's job is letting the author find the pane, and a
hidden canvas heading may not blank the panel's identifier in a tab strip.
That consumer does not depend on the flattening defect; it implements the
two-chain split, and this record's repair changes the canvas chain only:
`Hide` means nothing **on the canvas**; chrome treats an authored `Hide` as
"no authored title," exactly as it treats `Inherit`.

**No heading-hide spelling ships here.** 0016 §12's deferral of
`hide_title()` stands — this record repairs the mechanism and supplies its
first producer through the legend; a heading spelling is a small later
record that now needs no substrate, and adding it unasked would bundle a
surface nobody requested with one somebody did.

### 4. The spellings

```rune
figure([
    legend_at(0.98, 0.02),   // Show at authored fractions
    hide_legend(),           // Hide
    ...
])
```

- `legend_at` takes exactly two numeric literals in the PRODUCT-0036 §2
  grammar (unsuffixed, one optional unary negation), bindings resolving per
  PRODUCT-0040. Non-finite values refuse — no defined placement — and
  nothing else does: fractions outside `[0, 1]` place the legend partially
  or fully outside the figure, which is defined, authored, and rendered
  exactly (the 0036 §5 posture: the question is never how much is visible).
- `hide_legend` takes no arguments.
- Each appears at most once, and **together they refuse** at the second
  span: one property cannot be both `Show` and `Hide`, and last-wins would
  make member order semantic where 0016 §2 says it carries none.
- Both are figure members under the standing recognition rules; outside
  proven positions they remain ordinary Rune.

### 5. What this record does not authorize

No heading-hide or heading-show spelling. No legend styling, row-layout,
entry, or content change — 0044's policy is untouched; this record moves
and hides what 0044 says. No per-cell legends. No gutter-side choice or
legend-position vocabulary beyond fractions. No application or pane
producer. No interaction change.

## Acceptance gates

1. The faceted `test16.rn` fixture renders its legend in the right gutter
   with **zero overlap** against any cell's plot area, asserted
   geometrically — the four exhibits' defect, dead by default.
2. A figure with no legend content reserves no gutter and renders
   byte-identically to today; band-filled bars and bare marks are pixel-
   unchanged.
3. `hide_legend()` renders no legend and no gutter, and the freed width
   returns to the cells; the figure is byte-identical to the no-content
   case at equal content.
4. `legend_at(0.98, 0.02)` reproduces today's overlay placement exactly —
   the authored `Show` forgoes the gutter — and out-of-range fractions
   render clipped, not refused.
5. The three states are asserted at **both** boundaries they cross:
   `apply_presence` carries `Fallback | Show | Hide` through resolution
   undamaged (the state assertion), and the renderer at the `common.rs:709`
   boundary maps `Fallback` to the generated label, `Show` to its text, and
   `Hide` to **nothing** (the pixel assertion). A test proving only that
   the enum survives resolution does not discharge this gate. 0016 gate
   11's known-defect assertion is retired and replaced by this gate, cited.
5a. The chrome chain is regression-gated: with an authored `Hide` title, the
   panel's chrome label still resolves per 0016 §7 — view name, else the
   generated label — never blank. The `editor.rs:888` fold of `Hide` with
   `Inherit` survives the repair as correct behavior, asserted.
6. `legend_at` with `hide_legend` refuses at the second member's span;
   duplicate members, wrong arity, non-finite values, and non-literal
   arguments refuse per the standing rules; bound names resolve.
7. Grid cell congruence (0048 gate 4) and the one-cell/ungridded parity
   chain hold with the gutter present; refreshed draw baselines are
   enumerated in evidence as 0014 §9a premise updates with the placement
   change as the sole difference class — including the `theme.rs:500`
   cascade test, whose `pane_override.title = Presence::Hide` assertion
   changes meaning with the repair and is listed as a premise update, not
   a regression.
8. The gutter and placement geometry is written in the PRODUCT-0049 typed
   vocabulary — the record's first native consumer, asserted by its
   signatures. Implementation order is pinned, not assumed: this record's
   implementation begins on 0049's landed migration (verified in
   signatures: `hit_test_cell` is `Window`-typed, the grid composite
   `Widget`-typed), so the gutter math this record adds and the grid
   geometry it joins share one vocabulary rather than overlapping raw and
   typed on the same functions.
9. Every 0044 policy behavior — union, dedup, ordering, omission,
   provenance — is unchanged under all three `Presence` states.
10. No Rune behavior outside the two members, no identity, placement,
    group, kernel, or lifecycle change; the suite floor rises, none fall.

## Stop condition

> This record may add the legend gutter, the `Presence<LegendPosition>`
> property and its two spellings, the three-state `Presence` resolution
> repair with its renderer mapping, and the baseline refreshes. If the
> implementation requires a legend content or row-layout change, a heading
> spelling, a per-cell legend, a new cascade tier or algebra, an iced
> change, or a gutter interaction with 0048 that breaks cell congruence,
> the work stops and the requirement returns as its own record.

The expected trigger is the repair's blast radius: three-state resolution
touches every heading consumer, and if a **canvas-chain** consumer proves to
depend on the `Hide`-flattens-to-fallback defect, that dependency is a
finding for its own record, not a case to preserve silently. The chrome
chain's fold at `editor.rs:888` is not such a consumer — §3 classifies it as
correct two-chain behavior, gate 5a protects it, and halting on it would be
halting on the record working.

## Supersession

This record supersedes, **only** where they fixed placement:

- 0044 §4's figure-bounds placement and its overlay paragraph — fulfilled,
  not contradicted: that paragraph filed the exhibits this record retires.
- 0044 §7's acceptance of `legend_x`/`legend_y` at figure bounds as the
  placement policy — now the `Inherit` default's substrate, with the gutter
  in front of it.
- 0016 gate 11's assertion of the flattening defect as known-and-unrepaired
  — replaced by gate 5's assertion of the repair.

0044's content policy and 0016's heading semantics are otherwise untouched.

## Rejected

- **Authored placement without the gutter default:** leaves the four
  exhibits standing and makes every figure author repeat the same evasive
  `legend_at` — a default's job done by hand, the anti-pattern 0030 §3
  named when it made absence explicit.
- **A gutter only for grids:** the single-plot overlay is the same defect
  with one cell; two placement defaults would make figure count change
  legend position.
- **Modeling visibility outside `Presence`:** a second turn-off vocabulary
  beside the algebra built for turning things off, adopted to avoid a
  repair the algebra was owed anyway.
- **Shipping heading-hide spellings while the pipeline is open:** bundles
  an unrequested surface with a requested one; the mechanism now waits on
  nothing but a request.
- **Refusing out-of-range fractions:** a value threshold on placement,
  the posture 0034–0036 retired; clipped is defined.
- **Last-wins for `legend_at` + `hide_legend`:** makes member order
  semantic; 0016 §2 says it carries none.

## Consequences and sequencing

- The four-exhibit overlay docket closes by default; authored overlap
  survives as a choice, which is where overlap belonged.
- `Presence::Hide` becomes real: the arc's oldest documented defect (0016
  §6, fifteen records standing) is repaired with its first producer, and
  the heading-hide spelling becomes a request away, substrate-free.
- The legend gutter joins 0048's gutter model, and outer-edge chrome —
  color bars, size legends — inherits a second landing site.
- The typed-coordinate vocabulary gains its first native consumer, closing
  the loop on the 0049 ordering argument.
- The agreed sequence is complete: series bar, shared-edge dedup, legend
  placement/visibility. What remains on the board is debt retirement and
  on-request records — the plotting language's queue is, for the first time
  since 0007, empty of structural work.
