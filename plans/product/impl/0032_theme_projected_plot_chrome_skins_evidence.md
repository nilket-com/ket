# PRODUCT-0032 implementation evidence

Prepared 2026-08-06. Plan/ADR `P=8749ff13`; pre-surface oracle
`O=aa46a747`; indivisible implementation `I=71673752`. The parent check is
exact: `I^ == O == aa46a747`.

## Projection and catalog census

`PlotChrome::from_theme_name` calls `Theme::try_new` once, consumes its resolved
palette's `most_contrasting_pair`, assigns the darker anchor to paper and the
lighter anchor to every ink role, then chooses the first grid/sample alpha from
`[0.35, 0.4, 0.5, 0.6, 0.75, 1.0]` that admits the grid at 3:1. The complete
PRODUCT-0031 assay is the final success condition; there is no fallback,
alternative projection, neutral ink, local theme parser, or table treatment.

The executable grammar census covers 1,416 canonical/alias base, reverse,
clip, and `s0..s19` expressions. It observed 1,074 validated successes and 342
structured unsuitable results, with no panic or accepted identity reported as
unknown. Every success passes all eight completed-role checks.

Nord resolves anchors `#19 rgb(59,66,82)` and `#6 rgb(216,222,233)`. Its exact
role projection is:

| roles | RGBA |
|---|---|
| canvas and legend backgrounds | `(59,66,82,1.0)` |
| title, subtitle, axis-label, tick-label, legend text | `(216,222,233,1.0)` |
| axis line and legend-swatch border | `(216,222,233,1.0)` |
| gridline and neutral legend-swatch fill | `(216,222,233,0.5)` |

The five text checks are each `7.4481506372:1` against 4.5. Axis line and
legend-swatch border are each `7.4481506372:1` against 3.0; the composited grid
is `3.1391775809:1` against 3.0. The synthetic anchors `#3 rgb(100,100,100)` and
`#17 rgb(120,120,120)` prove structured unsuitability with names, indices,
colors, raw ratio, weakest role, observed ratio, and required threshold.
Unknown identity is a separate error variant and message.

## Surface and completion

One direct, duplicate-checked `skin(string_literal)` figure member lowers to
`plot_skin: Override<Arc<String>>`; source ordering is discarded. The envelope
adds no theme, palette, RGBA, renderer, or projection mode. A diagnostic-only
span is excluded from semantic equality. Zero/multiple/computed arguments,
duplicates, layers, mark postfixes, and malformed positions refuse at their
proven spans. Ordinary Rune `skin(...)` and table `.skin(...).duotone()` remain
independent.

The destination head resolves the name before plot construction and adds the
complete chrome producer to the authored settings carried by the existing
single, styled-single, and layered ready paths. Projection errors are attributed
to the exact outer `skin(...)` span. A two-destination assay has an unknown skin
fail while its Nord sibling completes from the shared result.

The precedence assay preserves:

```text
library compatibility chrome -> application theme -> authored skin -> pane override
```

The graphical-layer tier cannot select figure skin. Prepared chrome overrides
only `ResolvedPlotSettings.chrome`; mark-decoration and surrounding application
colors remain exact.

## Skin/palette sovereignty and lifecycle

The completed-value co-occurrence transition is one three-part observation:

```text
skin/palette viridis_r -> skin/palette viridis_s5
```

Categorical positions remain exact, bar RGB values change with ordered palette
projection, and all eleven plot-chrome roles remain byte-for-byte equal. This
pins the intentional set/order distinction where a user sees it. Independent
Nord/`tab20c` and Nord/`yellows` completions retain identical chrome while the
data RGB values differ; the awkward pair reaches content with no data/chrome
contrast check, warning, rotation, substitution, or refusal. Omitted skin
resolves exactly to PRODUCT-0031's compatibility chrome.

A real head/host lifecycle assay runs Nord, an unknown failure, Viridis, and
omission through one named destination. The panel identity is stable, failure
retains the prior SVG content/chrome pair stale, later success replaces both,
removal restores default rendering, and no dashboard appears. Existing
categorical continuation keys contain no skin identity.

No plot kernel, plot specification, factory, libaryon, table, adapter,
configuration, or protocol file changed in `I`. Consequently marks and encoded
swatches retain their prior sources; PRODUCT-0031's explicit background
compositing cases remain the only chrome-colored mark-adjacent pixels. The full
PRODUCT-0013 renderer corpus keeps legend content, order, labels, placement, and
last-layer bridge unchanged.

The external dogfood fixture `../uat/test9.rn` now contains the accepted
`skin("nord")` / `palette("tab20c")` bar shape. This headless implementation
session records the automated completion/render assays as sovereignty evidence;
it does not claim a human aesthetic observation from a live Toron/display.

## Acceptance gates

| gate | evidence |
|---:|---|
| 1 | contextual syntax/order/refusal and exact-span tests |
| 2 | envelope identity field plus architecture exclusions |
| 3 | exact omitted-skin default oracle |
| 4 | single `Theme::try_new`/resolved-palette architecture assay |
| 5 | one public projection; no enum/table/fallback path |
| 6 | eleven-role construction plus complete eight-check validation |
| 7 | exact Nord ledger and private low-range structured failure |
| 8 | exact-span unknown failure and completing sibling |
| 9 | `viridis_r == viridis_s5` chrome while ordered palettes differ; census clips |
| 10 | figure-level Nord/`tab20c` and paired Viridis transition |
| 11 | executable Nord/`yellows` rope control |
| 12 | prepared chrome changes only the closed chrome field; kernel/spec guard |
| 13 | unchanged PRODUCT-0013 and full renderer corpus |
| 14 | real head/host success, failure/stale, change, removal, identity assay |
| 15 | implementation file census and dependent/full suites |

## Verification

- `cargo fmt --all -- --check`: passed.
- `cargo test -p polariton --features toron`: 354 passed, 5 ignored.
- `cargo test -p polariton --test architecture_test`: 32 passed.
- `cargo test -p libaryon`: 1,437 passed, 22 ignored.
- `cargo check -p polariton --no-default-features`: passed with the existing
  feature-gated unused warnings.
- `cargo build -p polariton`: passed.
- `git diff --check`: passed.
