# ADR 0034: non-blocking plot chrome contrast

Status: proposed 2026-08-06. This record changes plot-chrome contrast from an
admission gate to an observable quality fact.

## Context

PRODUCT-0031 correctly made five text and three non-text contrast measurements
part of the closed chrome model. PRODUCT-0032 incorrectly turned those facts
into destination refusal for authored skins. The live consequence is worse than
the projected artifact: a known expression such as `tab20c_s9` refuses the
whole plot because its title measures 3.526:1 rather than 4.5:1.

The compatibility default already establishes the right precedent. Its 1.66:1
grid is measured and classified without preventing rendering. Authored theme
colors should receive the same honest treatment. Structural validity and theme
identity are enforceable; aesthetic/perceptual quality is observable.

## Decision

- A syntactically valid known theme always returns its exact completed
  `PlotChrome` in Ordered and Duotone modes.
- `visible_contrast_checks`, `weakest_contrast`, and `passes_contrast` remain
  exact, alpha-composited facts.
- A below-threshold fact never refuses a destination, falls back, substitutes
  colors, clamps channels, changes projection, or repairs the theme.
- Unknown theme identity and malformed contextual syntax remain errors.
- No warning UI is added in this cut. A later surface may display the retained
  facts non-blockingly.

## Acceptance gates

1. `tab20c_s9` renders its exact Ordered paper/ink despite a 3.526:1 title fact.
2. A synthetic low-range pair returns complete chrome and reports failure
   through the unchanged assay.
3. Every accepted libaryon theme expression completes in both projections.
4. Unknown identities and malformed syntax still refuse at their exact spans.
5. No fallback, repair, mark/data recoloring, Guide change, default change, or
   lifecycle/identity change occurs.

## Supersession

This record supersedes PRODUCT-0032 §5 and acceptance gates 6-8 only where they
make contrast an admission condition. It supersedes PRODUCT-0033 gate 6's
structured suitability refusal. Their measurement and ownership rules remain.
