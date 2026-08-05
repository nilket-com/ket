# ADR 0022: the bar-width modifier surface

Status: accepted 2026-08-04. This record decides one thing: PRODUCT-0018's
closed postfix register gains `bar_width` for standalone bar marks. It lowers
only into PRODUCT-0021's proved `LayerSettings` field.

## Context

The first two modifiers happened to target marks admitted by PRODUCT-0013's
`LayerMark = Line | Scatter`, so `RunePlotBody::StyledSingle(LayerSpec)` could
carry them. Bar is styleable but deliberately not layer-composable. Adding Bar
to `LayerMark` merely to reuse that carrier would destroy the type-level
guarantee behind PRODUCT-0013 gate 8.

This third specimen therefore establishes the missing distinction: a styled
standalone mark is a general envelope form, while an ordered layer remains the
narrow shared-Cartesian form.

## Decision

### 1. Canonical spelling

```rune
figure([
    title("Revenue"),
    bar("category", "value").bar_width(0.6),
])
```

The argument is one unsuffixed numeric literal validated only by
`BarWidth::new`. PRODUCT-0021 owns its normalized meaning and domain.

### 2. The modifier register remains closed and typed

`bar_width` is added as a third explicit case beside `point_size` and
`stroke_width`. It is valid only on a two-field bar. The other two receiver
rules remain exact. Unknown methods, duplicates, computed values, and any chain
containing a receiver-incompatible modifier refuse atomically.

Modifier order remains irrelevant and the complete chain flattens into one
typed `LayerSettings`; no nested method structure or open string map survives.

### 3. Styled single and ordered layer are separate forms

The envelope replaces its line/scatter-specific styled-single carrier with a
general styled standalone mark: one ordinary plot sink plus `LayerSettings`.
At the output boundary, line and scatter retain their established layer-aware
path; bar reaches the standalone bar kernel with its settings. This is an
envelope/handoff refactor, not a migration of `AuthoredPlotSpec` or
`LayeredPlotSpec`.

`bar(...).bar_width(...)` is legal directly in `.views` and as a figure body.
It refuses inside `layers([...])` for the same structural reason bare `bar(...)`
already refuses: Bar is not a shared-Cartesian `LayerMark`.

### 4. Identity and presentation do not change

A styled bar remains one `plot.bar` view. Width affects neither view identity,
panel labeling, headings, placement, nor execution. The modifier is pruned
before Rune dispatch and applied after the shared stamped result returns.

## Acceptance gates

1. `.bar_width(0.5)` lowers to the unchanged bar sink plus exact
   `Override::Set(BarWidth(0.5))`.
2. Direct-sink and figure-body positions reach the same shared stamped result
   and production host renderer.
3. `bar_width` inside `layers`, on line or scatter, and any chain combining it
   with their modifiers refuses atomically; bare controls remain unchanged.
4. Bounds, literal, arity, duplicate, and unknown-method refusals use the
   substrate domain without clamping or copied validation.
5. Unproven calls remain ordinary Rune.
6. Bare bar and `bar_width(0.9)` render byte-identically; `0.5` visibly narrows
   bars through the production host path and makes the new padding non-hittable.
7. Width-only revisions preserve `HostPanelId`, `plot.bar`, panel count,
   placement, headings, and no-dashboard completion.
8. Existing point-size and stroke-width surfaces, ordered layers, and every
   prior envelope behavior remain unchanged.

## Stop condition

> This record may generalize the envelope's styled-single carrier, extend the
> closed parser, add a styled-bar handoff, and add tests. If implementation
> requires Bar in `LayerMark`, bar inside `layers`, new substrate semantics,
> open modifier dispatch, plot-spec migration, Toron or libaryon, execution,
> identity, placement, Guide, heading, or dashboard change, it stops.

## Rejected

- **Add Bar to `LayerMark`:** confuses styleability with shared-Cartesian layer
  compatibility and makes an accepted impossibility representable.
- **Encode width as a figure option:** width belongs to one mark's Layer stage.
- **`bar_percent` / `gap_width`:** rejected by PRODUCT-0021's unit and ownership
  decisions.
- **Generic modifier macro:** three cases justify shared traversal and literal
  parsing, not open registration or erased typed dispatch.

## Consequences

- The third modifier closes the first proof of the postfix design across point,
  line, and rectangular marks.
- Future styleable marks no longer need admission to `LayerMark` merely to use a
  Layer-stage setting.
