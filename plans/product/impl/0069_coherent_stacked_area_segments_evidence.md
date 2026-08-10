# PRODUCT-0069 implementation evidence: coherent stacked-area segment semantics

Status: accepted 2026-08-09.

Plan: `a98a73b6`
(`plans/product/adr/0069_coherent_stacked_area_segments.md`).

## Result

The dashboard-reachable stacked-area preparation now obtains series order from
resolved categorical assignments when they exist and otherwise preserves first
appearance in the relation. It no longer sorts series alphabetically.

Duplicate `(x, series)` rows now retain the first row rather than being summed
inside mark preparation. Every stored sample is canonicalized once: finite
positive values survive and every non-finite or non-positive value becomes
exactly zero. Range, polygon, boundary, and hover all consume that matrix. A
mixed series pinches to its baseline at a zero sample; an entirely zero series
emits no polygon or boundary.

The renderer still samples the existing colour ramp. Resolved assignments
contribute category order only; their colour values remain unconsumed for
PRODUCT-0070.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Coincidence is byte-identical | `product_0069_coincident_fallback_keeps_the_banked_ramp_and_headroom` compares the complete SVG from the new preparation with a separately constructed banked alphabetical/unique/positive preparation, then pins both ramp colours and the 5% range | pass |
| 2. Both ordering routes | `product_0069_resolved_and_automatic_orders_are_both_semantic` uses `Zulu, Alpha` first appearance for the automatic path and `Alpha, Zulu` synthetic assignment order for the resolved path; prepared matrices and legend labels follow each source exactly | pass |
| 3. First-row-wins | `product_0069_first_row_and_clamped_matrix_own_every_consumer` supplies duplicate values `2` then `5`, requires `2`, and asserts both the retired sum `7` and plausible last-row repair `5` absent | pass |
| 4. One matrix, four consumers | The same gate pins the prepared matrix and 5%-padded summed range, records every polygon and boundary, and hovers the positive band above a clamped zero contribution with the exact value and running total | pass |
| 5. Clamping removes folds | Negative, zero, NaN, mixed, and entirely non-positive series share one fixture. Every recorded upper vertex remains at or above its baseline, the mixed bands pinch and resume, the all-zero series emits nothing, and zero owns no hover interval | pass |
| 6. Headroom preserved | Both semantic and coincidence gates require the unchanged `max_sum * 1.05` result | pass |
| 7. Colour untouched | The coincidence gate requires the exact legacy ramp samples; production `StackedAreaPlotKernel` has no resolved-colour field in this commit | pass |
| 8. Enumerated movement only | No baseline was refreshed. The complete Polariton suite and PRODUCT-0068 boundary/strata gates pass; production changes are confined to stacked-area preparation, empty-series emission, zero hover exclusion, and resolved-order routing | pass |

## Implementation findings

The duplicate policy belongs to the producer, not the assignment that
materializes its result. The retired preparation grouped by `(x, series)` and
summed before `category_values[ci][xi] = y`; removing that producer, rather
than changing the final assignment, is what establishes first-row-wins.

Area geometry could not reuse PRODUCT-0064's discrete-segment producer. One
series is one polygon across every x, so a non-positive sample is represented
as a canonical zero vertex. This keeps the accepted single-polygon topology
and straight interpolation while removing negative folds and empty hover
intervals. Avoiding coincident boundary strokes along a zero run would require
the separately rejected positive-run path splitting.

Resolved ordering and resolved colour remain intentionally separate. The
factory passes categorical assignments into preparation only when present;
preparation reads their category sequence, while paint and legend continue to
derive colours from the ramp. This preserves the byte oracle that PRODUCT-0070
will use before adding the new authored surface.

## Baseline disposition

No curated baseline changed. Existing figures move only if they previously
depended on alphabetical series sorting, implicit duplicate summation, or
non-positive accumulation. No committed baseline exercises those difference
classes; the new semantic fixtures do so directly.

## Verification

- `cargo test -p polariton product_0069 -- --nocapture` — three targeted gates
  passed; zero ignored.
- `cargo test -p polariton product_0068 -- --nocapture` — four boundary and
  renderer-strata regression gates passed.
- `cargo test -p polariton --bin polariton` — 517 passed, five pre-existing
  live-Toron tests ignored.
- `cargo check -p polariton --all-targets`, `cargo fmt --all -- --check`, and
  `git diff --check` — passed.

## Implementation history

PRODUCT-0069 lands as one implementation commit before PRODUCT-0070. The
resolved-colour repair, Rune word, persistence vocabulary, and UAT exhibit are
absent by design so the coincidence oracle remains independently meaningful.
