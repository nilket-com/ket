# ADR 0020: the stroke-width modifier surface

Status: accepted 2026-08-04. This record decides one thing: PRODUCT-0018's closed
postfix LayerModifier register gains `stroke_width` for line marks. It lowers
only into PRODUCT-0019's proved `LayerSettings`.

## Context

PRODUCT-0018 established `MarkForm ("." LayerModifier)*`, contextual and atomic
recognition, flat typed lowering, duplicate refusal, and styled-single host
completion. Its first member, `point_size`, proves scatter styling. PRODUCT-0019
now supplies the second governed Layer-stage setting.

This is the register's second specimen, not permission to generalize it into an
open option system. The implementation adds the second named case manually; a
third setting must provide evidence before any shared modifier abstraction is
accepted.

## Decision

### 1. Canonical spelling

```rune
layers([
    line("x", "y").stroke_width(2.0),
    scatter("x", "y").point_size(2.0),
])
```

`stroke_width` names the graphical property represented by the renderer's
`Stroke.width`. It is more precise than `line_size` or `line_thickness`, and
does not repeat the receiver like `line_width`.

### 2. Existing modifier rules apply exactly

The argument is one unsuffixed numeric literal validated only by
`StrokeWidth::new`. The complete `0.25..=8.0` factor domain, relative linear
meaning, contextual positions, atomic pruning, flat lowering, and duplicate
refusal are inherited from PRODUCT-0018/0019.

`stroke_width` is admitted only on a two-field line mark. It refuses on scatter;
`point_size` continues to refuse on line. Unknown methods and figure-wide
metadata remain outside the closed register.

Modifier order is irrelevant to the normal form. A chain containing settings
incompatible with its receiver refuses as a whole; it does not apply a compatible
prefix.

### 3. One styled line remains one view

A modified line is legal inside `layers`, directly in `.views`, and as a figure
body. Direct and figure forms use PRODUCT-0018's styled-single carrier and the
proved one-layer renderer while retaining exact `plot.line` presentation and
named or positional identity.

The setting never enters Toron source, a kernel type, prepared data, headings,
or panel metadata. It is pruned at the boundary and applied after the shared
stamped result returns.

Implementation of gate 6 exposed a missing PRODUCT-0019 boundary: the existing
styled-single carrier entered PRODUCT-0013's multi-layer domain padding, so
factor `1.0` changed a line's scale despite preserving its stroke. PRODUCT-0020's
stop condition fired. PRODUCT-0019 §3 and gate 5 were amended to require a
styled single layer to retain its mark-specific prepared layout; implementation
resumed under that substrate authorization. Multi-layer shared-domain behavior
remains unchanged.

## Acceptance gates

1. `.stroke_width(2.0)` lowers to the unchanged line mark/encoding plus exact
   `Override::Set(StrokeWidth(2.0))`.
2. It works in layers, direct-sink, and figure-body positions through one shared
   stamped result and the existing layer-aware output.
3. Bounds, literal, arity, duplicate, unknown-method, and computed-value
   refusals are exact and atomic.
4. Scatter receivers refuse `stroke_width`; line receivers still refuse
   `point_size`; bare controls remain unchanged.
5. Unproven calls remain ordinary Rune.
6. Bare line and factor `1.0` render byte-identically; `2.0` renders an exact 4px
   stroke through the production host path.
7. Changing only width preserves identity, `plot.line`, panel count, placement,
   headings, and no-dashboard behavior.
8. A high-density layered figure combines authored headings, a doubled line,
   and independently sized scatter points without changing paint order or
   execution sharing.

## Stop condition

> This record may extend the closed parser and styled-mark tests. Any renderer,
> cascade, plot model, Toron, execution, identity, placement, Guide, interaction,
> or dashboard change stops the work.

## Rejected

- **`.line_width(...)`:** repeats the receiver and can describe the legend
  sample's length rather than stroke thickness.
- **`.line_size(...)` / `.line_thickness(...)`:** less precise than the existing
  rendering vocabulary.
- **`.width(...)`:** collides with geometry and encoding meanings.
- **Generic modifier macro:** deferred until a third setting reveals a stable
  shared abstraction.
