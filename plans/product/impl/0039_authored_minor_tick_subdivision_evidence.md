# PRODUCT-0039 implementation evidence

Accepted 2026-08-06. Plan `34151471`; implementation `a914f46c`.

## Result

Rune figures now accept `x_minor_ticks(count)` and `y_minor_ticks(count)` as
figure-wide guide elements. The count is a non-negative integer literal,
including zero; integer and decimal spellings lower identically, while
negative, fractional, non-finite, malformed, and duplicate elements refuse at
their own spans.

Zero is the complete off switch. Existing renderer gates already require both
the visibility flag and a positive count, so `y_minor_ticks(0)` removes minor
marks and minor gridlines together while leaving the other axis unchanged.
Positive counts place exactly that many ticks strictly between each pair of
major positions. Count and Step major placement use the same subdivision
contract, so no minor position coincides with a major position.

`TickPlacement::minor_positions` now checks the combined major-plus-minor
lattice before allocating. Count and Step use their distinct interval counts;
the estimate uses checked arithmetic and returns an attributed
`MinorCapacity` error naming the placement, minor count, resolved range,
requested total, and 10,000-position limit. Rune resolution maps that refusal
to the authored minor element span. Categorical/band and temporal axes refuse
because they have no linear major lattice to subdivide.

## Gate ledger

| gate | evidence |
|---:|---|
| 1 | omitted minor elements preserve the existing Count(8)/minor-count-4 compatibility path; full regression suite remains green |
| 2 | zero-count bar fixture resolves with y count 0, x count 4, and renders through the existing plot path |
| 3 | Count and Step midpoint fixtures assert exactly `n` positions for `n = 1`, with no major-position collision |
| 4 | common generator tests cover both placement variants and strict-between-major semantics |
| 5 | Count and Step use their distinct interval formulas in the checked total estimate |
| 6 | `y_tick_step(0.001)` and `y_minor_ticks(9)` complete alone; together refuse at `y_minor_ticks(9)` with the 70,001-position total, and `u32::MAX` refuses before allocation |
| 7 | categorical/band and temporal x-axis requests refuse at their minor-element spans |
| 8 | arity, non-literal, duplicate, negative, fractional, and non-finite lowering refusals are distinct and span-attributed |
| 9 | x/y lowering is independent; only authored axes replace the default count |
| 10 | architecture ownership guard plus unchanged major placement, bounds, rendering, and cross-crate suites |

## Verification

- `cargo fmt --all -- --check`: passed.
- `cargo test -p polariton --features toron`: 373 passed, 5 ignored.
- `cargo test -p polariton --test architecture_test`: 37 passed.
- `cargo test -p polariton product_0039`: 4 unit tests passed.
- `cargo test -p libaryon`: 1,437 passed, 22 ignored; auxiliary suites passed.
- `cargo check -p polariton --no-default-features`: passed with existing
  feature-gated unused warnings.
- `cargo build -p polariton`: passed.
- `git diff --check`: passed.
