# PRODUCT-0033 implementation evidence

Accepted implementation 2026-08-06. Plan `4a4a5089`; implementation
`5871be41`.

## Corrected projection behavior

Ordinary `skin(name)` consumes libaryon's resolved order. Palette position 19,
the semantic `Whitespace`/background slot, remains the canvas and legend paper;
its most contrasting resolved mate supplies text and structural ink. The paper
is not polarity-flipped, so shifts can select visibly light or dark canvases.
The existing closed alpha search and all eight PRODUCT-0031 contrast checks
remain the final admission gate.

Explicit `skin(name).duotone()` preserves PRODUCT-0032's original treatment:
the palette's global most-contrasting pair is oriented darker-paper and
lighter-ink. Because this projection intentionally consumes a set, reverse and
shift may collapse here.

The motivating exact differential is:

| expression | paper | ink | grid alpha | text ratio | grid ratio |
|---|---|---|---:|---:|---:|
| `skin("tab20c_s5")` | `rgb(63,0,125)` | `rgb(199,233,192)` | 0.50 | 10.51097 | 3.30584 |
| `skin("tab20c_s8")` | `rgb(158,154,200)` | `rgb(63,0,125)` | 0.75 | 5.24720 | 3.78886 |
| either with `.duotone()` | `rgb(63,0,125)` | `rgb(199,233,192)` | 0.50 | 10.51097 | 3.30584 |

Every text role shares the recorded text ratio; axis line and swatch border use
the same pair against the 3.0 non-text threshold. The grid ratio is measured
after its declared alpha composite. Base Nord ordinary skin is byte-for-byte
equal to accepted Nord Duotone, preserving PRODUCT-0032's positive oracle.

## Surface and normal form

The authored setting now carries one bounded `PlotSkinSelection` containing an
`Arc<String>` identity and exactly one `PlotChromeProjection::{Ordered,
Duotone}` choice. The envelope still carries no resolved theme, palette, RGBA,
or renderer value.

Only these direct figure members are recognized:

```rune
skin("theme")
skin("theme").duotone()
```

Duotone takes no arguments and may appear once. Duplicate modifiers, unknown
modifiers, computed names, invalid arity, mark receivers, layers, standalone
members, and use outside proven figure positions retain exact contextual
refusals. Outside the envelope the same calls remain ordinary Rune. Resolution
continues at the destination head before content construction.

## Co-occurrence and ownership

The paired Viridis figure assay now records both truthful observations:

- ordinary `viridis_r -> viridis_s5`: categorical positions stay fixed, bars
  recolor through ordered palette projection, and chrome also changes through
  its ordered background anchor;
- explicit Duotone over the same transition: positions stay fixed and bars
  recolor, while all eleven chrome roles remain identical.

Both modes write only `ResolvedPlotSettings.chrome`. Mark-decoration ink,
categorical and continuous data colors, encoded swatches, Guide contents,
application chrome, and pane controls remain disjoint. Omitted skin remains the
exact PRODUCT-0031 compatibility default. Existing destination-local failure,
atomic stale retention, identity, placement, focus, categorical continuation,
and no-dashboard lifecycle assays remain green.

## Acceptance gates

| gate | evidence |
|---:|---|
| 1 | exact ordinary/`.duotone()` parsing and normal-form assertions |
| 2 | modifier/refusal matrix and architecture-bounded two-variant register |
| 3 | exact Tab20c s5/s8 ordinary difference and Duotone equality |
| 4 | paired completed Viridis position/data/chrome assertions |
| 5 | exact Nord Ordered == Duotone control |
| 6 | two-mode catalog census; every success passes eight checks, failures structured |
| 7 | unchanged renderer/spec ownership and host lifecycle suites |

## Verification

- `cargo fmt --all -- --check`: passed.
- `cargo test -p polariton --features toron`: 354 passed, 5 ignored.
- `cargo test -p polariton --test architecture_test`: 32 passed.
- `cargo test -p libaryon`: 1,437 passed, 22 ignored; auxiliary suites passed.
- `cargo check -p polariton --no-default-features`: passed with existing
  feature-gated unused warnings.
- `cargo build -p polariton`: passed.
- `git diff --check`: passed.
