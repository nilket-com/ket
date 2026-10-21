# PRODUCT-0067 implementation evidence: default plot gridline contrast

Status: accepted 2026-08-09.

Plan: `296d35f0`
(`plans/product/adr/0067_default_plot_gridline_contrast.md`).

Co-landed rendering decision: PRODUCT-0068, gridlines beneath marks.

## Result

Every Polariton-owned compatibility chrome path now uses white at alpha `0.35`
for gridlines. `PlotChrome::DEFAULT`, `APPLICATION_DEFAULT`, and
`legacy_from_iced` therefore meet the existing `3:1` non-text contrast rule
against their accepted dark canvases. RGB, stroke geometry, tick generation,
and all ten sibling chrome roles are unchanged.

The historical alpha-`0.2` value remains in the test suite only as a synthetic
negative control. It still measures approximately `1.662095:1` and fails, so
the repaired test proves the rule can distinguish both states rather than
merely accepting the new constant.

PRODUCT-0067 and PRODUCT-0068 land in one implementation transition. A
marks-free operation oracle isolates this alpha repair from the strata change;
the raster and ordering oracles in PRODUCT-0068 independently prove the new
grid/mark relationship.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Defaults use and pass at `0.35` | `product_0067_defaults_pass_while_the_retired_grid_remains_a_falsifiable_control` checks both constants, the exact alpha, the `3.0` minimum, and the complete visible-role assay | pass |
| 2. Reconstruction cannot restore `0.2` | `product_0067_only_grid_alpha_supersedes_the_omitted_skin_compatibility_bank` constructs `legacy_from_iced(Color::BLACK, Color::WHITE)`, pins alpha `0.35`, and requires the full assay to pass | pass |
| 3. Sole role change | The same bank pins paper, all text roles, axis line, legend roles, swatch roles, and grid RGB exactly; only grid alpha differs from PRODUCT-0032 | pass |
| 4. Marks-free isolation | `product_0067_marks_free_render_changes_only_the_default_grid_alpha` compares backend-independent operation streams and permits differences only on grid strokes from alpha `0.20` to `0.35` | pass |
| 5. Named skins do not move | Existing named-theme projection and rendering banks pass unmodified; production projection code is untouched | pass |
| 6. Falsifiable historical control | The replaced PRODUCT-0031 test explicitly restores alpha `0.2`, pins its prior ratio, and requires it to fail | pass |
| 7. Debt retired | CI-09 is marked retired in both the deferred ledger and live bridge; the bounded PRODUCT-0031/0032 supersession is named there | pass |
| 8. Complete suite | Polariton, workspace, formatting, and the opt-in Vulkan evidence target pass with no new ignored test | pass |

## Baseline disposition

No checked-in SVG, draw-operation, or raster baseline was refreshed. The
combined landing is described by operation-level controls instead: exposed
compatibility grid strokes change only in alpha, and PRODUCT-0068 changes their
position relative to marks. Named skins retain their exact resolved chrome
values even though their grid operations join the new global stratum.

## Verification

- `cargo test -p polariton product_0067 -- --nocapture` — targeted contrast and
  marks-free gates passed.
- `cargo test -p polariton --bin polariton` — complete Polariton unit suite
  passed with only the five pre-existing live-Toron tests ignored.
- `cargo test --workspace --all-targets --no-fail-fast --quiet --
  --test-threads=1` and `cargo check --workspace --all-targets` — passed.
- `cargo test -p polariton --test product_0057_render_evidence --features
  product-0057-render-evidence -- --nocapture` — all backend-gap probes and the
  PRODUCT-0068 production controls passed on the Intel UHD 770 Vulkan adapter.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

Two default-parallel workspace sweeps each surfaced a different libaryon timing
fixture under the 1,505-test library load. The exact process-group test and the
complete libaryon library both passed immediately in isolation; the complete
one-test-thread workspace sweep then passed. No libaryon code or expectation was
changed under this plotting record.

## Implementation history

PRODUCT-0067 and PRODUCT-0068 are compressed into one implementation commit,
as their plans require, so permanent history contains only the final
grid-under-mark pixel state rather than a knowingly temporary stronger overlay.
