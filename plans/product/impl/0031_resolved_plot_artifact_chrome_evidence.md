# PRODUCT-0031 implementation evidence

Accepted 2026-08-06. Plan/ADR commit `8749ff13`; renderer oracle `6ea267e6`;
closed model `0f96e594`; renderer migration `eb37715b`.

## Boundary and compatibility evidence

The old pair was classified and replaced as follows:

| former use | resolved owner |
|---|---|
| canvas, margin masks, axis halo, scatter clear, pie separator | `chrome.canvas_background` |
| title and subtitle | `chrome.title_text`, `chrome.subtitle_text` |
| axis labels and tick labels | `chrome.axis_label_text`, `chrome.tick_label_text` |
| axes/ticks and grids | `chrome.axis_line`, `chrome.gridline` |
| legend backing, text, neutral sample, and framing | the four `legend_*` roles |
| box/violin/candlestick/funnel compatibility ink | `mark_decoration_color` |
| plot-pane and editor controls | the two `surrounding_*` compatibility colors |

`product-0031-pre-extraction-chrome.svg` is produced through the real
`PlotWidget`/`PlotBackend` path. It remained byte-for-byte unchanged after the
migration: ordered paths and text, bounds, strokes, alpha, and the independent
mark stroke all retain their baseline values. Source architecture assays reject
the retired renderer fields, require the closed chrome carrier, and prove the
four mark kernels read only the isolated mark ink.

The settings pane still expands its two historical edits into the corresponding
role families. Its own controls and the editor surround read only
`surrounding_background_color`/`surrounding_decoration_color`, so artifact
chrome cannot escape the plot bounds. Existing ready-message installation still
replaces kernel and complete resolved settings together; no identity or
placement key gained chrome state.

## Contrast ledger

`PlotRgba` rejects non-finite and out-of-range channels rather than clamping.
The assay composites declared alpha in normalized sRGB precision and checks:

| roles | count | minimum |
|---|---:|---:|
| title, subtitle, axis-label, tick-label, legend text | 5 | 4.5:1 |
| axis line, gridline, legend-swatch border | 3 | 3.0:1 |

For the application compatibility default, opaque white over black measures
21:1. The visible 0.2-alpha gridline measures `1.6620953314177012:1`; it is the
only failed applicable check, `passes_contrast()` is false, and the test names
it as legacy debt. Raising its alpha to 0.35 proves the same complete role set
passes. The default is parity content, never an authored-skin certification.

## Ownership results

- Chrome supplies legend framing only. Existing entry content, ordering,
  placement, and layered delegation are unchanged.
- Encoded categorical/continuous swatches remain data colors.
- Mark decoration remains exact in box plot, violin, candlestick, and funnel.
- Scatter clearing, axis halos, and pie separators are the explicit background
  compositing cases; they do not make chrome a semantic mark treatment.
- Theme names, libaryon palettes, table skins, Rune parsing, execution,
  transport, configuration, and durability are absent from this cut.

## Acceptance gates

| gate | evidence |
|---:|---|
| 1 | eleven public, required `PlotChrome` fields; architecture field census |
| 2 | unchanged checked-in real-widget SVG oracle |
| 3 | retired-field rejection plus direct role reads across common/kernels |
| 4 | exact five-text/three-non-text threshold assay after compositing |
| 5 | exact 1.662095 legacy-grid classification; no admission claim |
| 6 | closed-role renderer mapping; SVG preserves non-color operation structure |
| 7 | isolated mark field and four-kernel architecture assay |
| 8 | explicit canvas compositing reads in scatter/common/pie only |
| 9 | legend kernels changed only their resolved color inputs |
| 10 | separate surrounding colors and preserved two-control expansion |
| 11 | complete settings remain in the existing atomic plot-ready carrier |
| 12 | architecture refusal of figure `skin`; full dependent suites unchanged |

## Verification

- `cargo test -p polariton --features toron`: 343 passed, 5 ignored.
- `cargo test -p polariton --test architecture_test`: 30 passed.
- `cargo test -p libaryon`: 1,437 passed, 22 ignored.
- `cargo check -p polariton --no-default-features`: passed (existing warnings).
- `cargo build -p polariton`: passed.
- `git diff --check`: passed.
