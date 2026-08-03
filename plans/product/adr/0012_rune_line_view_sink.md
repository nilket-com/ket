# ADR 0012: Rune line view sink

Status: accepted 2026-08-03. This record adds one presentation sink to the Rune
relation envelope: a line plot with authored x and y fields. It extends
PRODUCT-0008's view vocabulary and inherits PRODUCT-0010's execution, identity,
ordering, and host-panel decisions unchanged.

## Context

Polariton already has `PlotType::Line`, a resolved line specification, a line
kernel, factory dispatch, and plot content hosted in ordinary libaryon panels.
The missing seam is the Rune language: its envelope recognizes only `table`,
`bar`, and `scatter`, and authored channel resolution currently admits explicit
x/y fields only for bar and scatter.

PRODUCT-0008 deliberately authorized no new sink form. Adding one is therefore
not another respelling under that record; it requires this narrow expansion.

## Decision

### 1. Line is a fourth Rune view form

These spellings are exact projections of one normal form:

```rune
plot.line(r, "x", "y")
r.line("x", "y")
r.views([line("x", "y")])
```

They produce `RuneViewForm::Line` and a single-layer
`PlotComposition::single(PlotType::Line)`. They retain the authored field names
exactly; no inference or substitution is authorized for an explicit field.

### 2. The first surface is exactly x and y

`line` takes exactly two string literals. A third series, group, color, order,
interpolation, or styling argument is outside this cut. Malformed arity or
non-string fields refuse at the sink span, and `.views([...])` remains atomic.

The constructor is contextual inside a proven `.views([...])` list. The method
sugar is recognized only on proven relation provenance. The legacy `plot.line`
form is recognized only while the top-level `plot` capability is unshadowed.
These are the recognition and shadowing rules already fixed by PRODUCT-0008.

### 3. Line reuses the existing plot pipeline

Authored x/y resolution admits `PlotType::Line` and projects it to:

```text
ResolvedKernelFields::Line { series: None, x, y }
```

Resolution, kernel creation, rendering, revisions, and host-panel realization
then follow the existing line path. Completing a line view must neither open nor
mutate the dashboard.

### 4. Everything else is inherited

This record changes neither origin-keyed execution nor shared-result identity.
Presentation order, optional view names, anonymous positional identity, panel-id
allocation, placement, staleness, and partial-failure behavior remain exactly as
PRODUCT-0010 and PRODUCT-0011 define them. Toron still executes one relation and
returns one frame per canonical origin; the new sink is a consumer of that frame,
not a new transport result.

## Acceptance gates

1. Legacy, method-sugar, and canonical line spellings normalize identically.
2. Contextual recognition, shadowing, arity, literal-field, and atomicity tests
   cover line without weakening the existing sink rules.
3. An authored line resolves the exact x/y identities to
   `ResolvedKernelFields::Line` with no series field.
4. A line may fan out beside table and other plot sinks while every sink over one
   origin holds the same stamped result by `Arc` identity.
5. The real head completes a line view into an ordinary plot host panel without
   opening or mutating the dashboard.
6. The two-origin dogfood program renders line + table for `test1` and bar +
   table for `test2`, preserving source presentation order.

## Rejected

- **Treat line as scatter with a connecting overlay.** Line already has its own
  resolved contract and kernel; falsifying the plot type would make provenance
  and behavior disagree.
- **Add an optional series argument now.** The underlying kernel can represent
  it, but the requested language cut is a line replacing the prior x/y scatter.
  Series identity deserves evidence and a separately specified spelling.
- **Build a new line renderer.** The existing renderer is the evidence that this
  language addition can stay narrow.

## Non-authorization and halt rule

This record authorizes the Rune envelope form and explicit line x/y channel
resolution only. It authorizes no kernel, renderer, Toron, transport, libaryon,
layout, dashboard, or durable-identity change. If those are required, work stops
and the requirement returns as a separate record.
