# ADR 0036: authored numeric axis bounds

Status: proposed 2026-08-06. This record decides one thing: the Rune spelling
that sets a figure's numeric axis bounds, and the lowering that reaches the
cascade slot already holding them. It adds no substrate, no kernel change, and no
new cascade algebra. Discrete axes are out of scope.

## Context

The setting exists and already cascades correctly. `PlotSettingsLayer` carries
`domains: Override<PlotDomainOverrides>` (`theme.rs:175`), whose four fields are
`x_min`, `x_max`, `y_min`, `y_max` (`theme.rs:145-150`), and resolution applies
the group under proper PRODUCT-0007 §6 algebra — `Override::Inherit` says
nothing, `Set` replaces the group (`theme.rs:356`).

Every Cartesian kernel already honors it. Of the twenty-two files in `kernels/`,
the only ones that never read `settings.x_min` or `settings.y_min` are `funnel`,
`heatmap`, `parallel`, `pie`, `radar`, and `radial_dial` — each non-Cartesian.
Nothing numeric ignores authored bounds.

So this is a surface record of the same shape as PRODUCT-0016: a cascade slot
with no spelling. Two Stage 0 findings shaped its rules, and both reversed a
position reached by reasoning alone.

**Equal bounds are not defined by the current transform.** With
`x_range = (5, 5)` over a 100px plot, `CoordinateTransformer::cartesian`
(`common.rs:70`) computes `x_delta` as `.abs().max(f64::EPSILON)` and then
`pixel_x` as `(data_x - x_range.0) * x_scale`. Measured: `x = 4` maps to
`-4.5035996e17`, `x = 6` to `+4.5035996e17`, while pixel `50` inverts to `x = 5`
which maps forward to pixel `0`. Forward and inverse disagree.

**Negative bounds are not literals.** Rune parses `-1.0` as
`Expr::Unary(Neg, Expr::Lit(Number))`, and the envelope has no numeric-literal
precedent at all — every field it recognizes today is a string.

## Decision

### 1. `x_bounds` and `y_bounds` are figure elements

```rune
figure([
    title("Eat, Sleep, Rave"),
    y_bounds(0.0, 10.0),
    layers([
        line("x", "y"),
        scatter("x", "y"),
    ]),
])
```

They join `title`, `subtitle`, `palette`, and `skin` as figure-wide settings
under PRODUCT-0016 §2's metadata group, and are recognized only as elements of a
`figure([...])` list.

Figure level is not a choice. PRODUCT-0013 §5 gives every layer of a plot **one
shared Cartesian domain**, so a per-mark bound would be a per-mark domain, which
that record makes unrepresentable. Bounds describe the whole picture, so they
attach where the whole picture is described.

### 2. Each takes exactly two numeric literals, optionally negated

An argument is an unsuffixed numeric literal, or one unary negation directly
wrapping an unsuffixed numeric literal. Nothing else — no expression, no
identifier, no computed value — for PRODUCT-0016 §4's reason: the envelope lowers
statically, before dispatch, so a computed bound has no value at the moment the
setting is constructed and no path into the settings layer afterward.

`x_bounds` and `y_bounds` may each appear at most once. Duplicates refuse.

### 3. Partial bounds are not exposed

`PlotDomainOverrides` holds four independent `Option<f64>`, but the surface takes
a pair. That capacity is a substrate fact, not a surface commitment.

Exposing halves would require a combining rule for `x_min` alone against an
inherited `x_max`, and the cascade replaces the group rather than merging it — so
a partial surface would either invent a merge algebra PRODUCT-0007 §6 does not
have, or silently reset the other half. Neither is worth a spelling nobody has
asked for.

### 4. Refusal means there is no defined answer

Consistent with PRODUCT-0030, PRODUCT-0034, and PRODUCT-0035, bounds refuse only
where no defined answer exists:

- a non-finite value — `NaN` and the infinities have no placement on an axis, the
  same reasoning by which `Srgb` rejects non-finite channels;
- `min >= max` — see §5;
- a duplicate `x_bounds` or `y_bounds`;
- a malformed argument, per §2;
- a bound for an axis that does not resolve as numeric.

Everything else renders exactly as authored.

### 5. Collapse and inversion refuse; exclusion renders

This is the section most likely to be re-litigated, because two of these cases
look identical on screen.

`x_bounds(100.0, 200.0)` over data spanning 1–6 renders an empty plot. A
collapsed domain would also show nothing. They are not the same:

- **Exclusion is defined.** The span is finite and nonzero, every datum has a
  real position off-canvas, and the inverse round-trips. The author asked to look
  at a region containing no data, and got exactly that.
- **Collapse is contradictory.** As measured above, forward and inverse disagree
  — the same pixel maps to two different data values depending on direction. That
  is not an empty result; it is no result.
- **Inversion is contradictory for the same reason.** `.abs()` supplies the span
  while the untransformed origin supplies the offset, so `(10, 0)` places `x = 0`
  off-canvas and inverts inconsistently. It is a broken axis, not a reversed one.

The distinguishing question is never *how much is visible*. It is whether the
transform is self-consistent.

### 6. `min < max` is the entire rule

`x_bounds(0.0, 1e-300)` satisfies strict ordering, produces a scale near `1e302`,
and can send off-domain coordinates beyond `f32`'s finite range. It still
renders, because the authored `f64` interval is finite, nonzero, and
**self-consistent**: in-domain forward and inverse coordinates round-trip
mathematically. Loss at the renderer's `f32` boundary is a clipping and
representation concern, not a reason to invent a different domain.

The tempting next move is a minimum-span threshold — `max - min > ε` — to "fix"
that case. This record prohibits it. Any such ε is an arbitrary number
gatekeeping a defined request, which is precisely the posture PRODUCT-0034 and
PRODUCT-0035 corrected for contrast.

> The line is transform consistency, not numeric magnitude.

### 7. Applicability is per axis, and never silent

`bar("cat", "val")` has a categorical x and a numeric y. `y_bounds` applies;
`x_bounds` refuses because a discrete band domain has no numeric bound to set.

The refusal is per axis, not per figure, and it must be a refusal. **A bound that
cannot apply is never silently dropped.** A silently ignored setting is the one
failure mode the rope principle does not cover: the author sees an artifact that
does not reflect what they wrote and has no way to learn why.

Reversed axes, collapsed domains, and bounds on discrete axes each need real
substrate semantics — transform, ticks, hover, hit-testing, and bar geometry —
and each is a later record rather than an accidental consequence of `abs()`.

### 8. What this record does not authorize

No reversed or collapsed axis semantics. No discrete-axis bounds or band
slicing. No partial bounds. No computed or non-literal bound values. No tick,
label, guide, or interaction change. No kernel change. No change to the cascade
algebra, to any other figure element, to marks, palettes, skins, identity,
placement, or lifecycle.

## Acceptance gates

1. `figure([y_bounds(0.0, 10.0), bar("cat", "val")])` lowers to exactly one
   `Override::Set(PlotDomainOverrides)` with `y_min` and `y_max` set and both `x`
   fields `None`, and the rendered axis honors it.
2. `y_bounds(-1.0, 1.0)` parses, exercising one unary negation over a numeric
   literal on both arguments' grammar, and lowers to `-1.0` and `1.0` exactly.
3. Each of these refuses at its own span with a distinct diagnostic: a non-finite
   argument, `x_bounds(10.0, 0.0)`, `x_bounds(5.0, 5.0)`, a duplicate
   `y_bounds`, a non-literal argument, and `x_bounds` on `bar("cat", "val")`.
4. `x_bounds(100.0, 200.0)` over data spanning 1–6 **renders** an empty plot with
   the authored axis range, proving exclusion is admitted while collapse is not.
5. `x_bounds(0.0, 1e-300)` renders; no minimum-span threshold exists anywhere in
   the implementation.
6. A figure with no bounds element produces `Override::Inherit` and renders
   byte-identically to today.
7. Bounds apply once to a `layers([...])` body through its existing shared
   Cartesian domain; no per-layer domain is constructed.
8. No kernel, cascade algebra, tick, guide, mark, palette, skin, identity,
   placement, or lifecycle behavior changes.

## Rejected

- **`xlim` / `ylim`:** shorter, and borrowed from a library whose surface is the
  accumulating-options ceiling PRODUCT-0007 §5 rejects. `x_bounds` names the
  thing rather than abbreviating it, and matches the vocabulary the other figure
  elements already use.
- **Four separate `x_min` / `x_max` elements:** the cascade replaces the group,
  so four elements would flatten into one anyway while inviting the partial-merge
  question §3 declines.
- **Rendering inverted bounds as a reversed axis:** the transform does not
  reverse; it breaks. Shipping it as a feature would make a defect into a
  contract.
- **Rendering collapsed bounds:** measured, forward and inverse disagree. An
  empty-looking plot is not evidence of a defined one.
- **A minimum-span threshold:** §6. An arbitrary number refusing a defined
  request.
- **Silently ignoring a bound on a discrete axis:** the author's text would have
  no effect and no explanation, which is worse than either applying or refusing.
- **Bounds as a mark modifier:** PRODUCT-0013 §5 gives layers one shared domain,
  so a per-mark bound is unrepresentable rather than merely unwise.

## Consequences and sequencing

- The smallest record since PRODUCT-0012: one constructor pair, one lowering, no
  substrate.
- The `domains` cascade slot acquires its first Rune producer, having been
  reachable only from application defaults — the third slot to do so, after
  headings in PRODUCT-0016 and the authored skin layer in PRODUCT-0025.
- Reversed and collapsed axes are now explicitly *unbuilt* rather than
  accidentally broken. A later substrate record may implement either, and it
  inherits a transform whose defect is documented with measurements rather than
  discovered again.
- Discrete-axis bounds — band slicing, category windows — remain unaddressed and
  are a different design question, not a widening of this one.
