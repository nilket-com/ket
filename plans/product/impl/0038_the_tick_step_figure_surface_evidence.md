# PRODUCT-0038 implementation evidence

Accepted 2026-08-06. Plan `4488a3f8`; implementation `2385a8d5`; temporal
applicability evidence `3c1b277f`.

## Result

Rune figures now accept `x_tick_step(step)` and `y_tick_step(step)` as
figure-wide elements. Each takes one finite, strictly positive unsuffixed
numeric literal, with direct unary negation parsed so invalid negative values
receive a value diagnostic. Integer and decimal spellings lower to the same
`TickPlacement::Step` value; duplicates and malformed arguments refuse at the
envelope span.

The authored guide group reaches the existing renderer without a substrate or
kernel change. Bounds remain independent: `y_bounds(0, 7)` with
`y_tick_step(1)` produces the exact lattice `0..=7`, while an unbounded step
uses the existing zero-anchored generator over the data-derived domain. An
omitted step remains `Count(8)` and preserves the established compatibility
path.

Resolution-local checks refuse a step on categorical/band or temporal axes and
attribute the refusal to the exact constructor. Dense requests use the closed
generator's declared 10,000-position capacity and likewise refuse at the
constructor span; no request is silently ignored. Layered bodies retain one
figure-wide placement per axis.

## Gate ledger

| gate | evidence |
|---:|---|
| 1 | bounded vertical-bar fixture lowers `y_tick_step(1)` and renders the exact `0..=7` lattice through the existing SVG path |
| 2 | `1` and `1.0` lower to equal `TickPlacement::Step` values |
| 3 | unbounded line fixture completes and observes the zero-anchored lattice over `(1.4, 8.6)` |
| 4 | arity, non-literal, duplicate, zero, negative, and non-finite envelope cases each refuse with distinct diagnostics and source attribution |
| 5 | categorical/band, temporal, and dense-capacity cases refuse at their `x_tick_step`/`y_tick_step` spans; no silent drop |
| 6 | no-step compatibility path remains unchanged and avoids constructing new validation kernels |
| 7 | x/y lowering is independent; only the authored axis becomes `Step` |
| 8 | layered placement continues through the existing shared figure-wide guide group; no per-layer producer exists |
| 9 | architecture ownership guard plus full Polariton/libaryon and feature-gated verification suites |

## Verification

- `cargo fmt --all -- --check`: passed.
- `cargo test -p polariton --features toron`: 369 passed, 5 ignored.
- `cargo test -p polariton --test architecture_test`: 36 passed.
- `cargo test -p polariton product_0038`: 4 unit tests and 1 architecture test passed.
- `cargo test -p libaryon`: 1,437 passed, 22 ignored; auxiliary suites passed.
- `cargo check -p polariton --no-default-features`: passed with existing
  feature-gated unused warnings.
- `cargo build -p polariton`: passed.
- `git diff --check`: passed.
