# PRODUCT-0036 implementation evidence

Accepted 2026-08-06. Plan `d609bf90`; implementation `2ad9357e`.

## Result

Rune figures now accept `x_bounds(min, max)` and `y_bounds(min, max)` as
figure-wide elements. Each constructor takes exactly two unsuffixed numeric
literals, optionally directly unary-negated, and may occur at most once. Both
axes lower together through exactly one existing
`Override::Set(PlotDomainOverrides)`; omitting both retains `Override::Inherit`.

The completed authored settings reach the existing Cartesian kernels without a
substrate or renderer change. A vertical categorical bar accepts
`y_bounds(-1.0, 1.0)` and renders with that exact y range. The same bar refuses
`x_bounds` at the constructor's exact source span because its resolved x scale
is `Band`. Layered line and scatter marks receive one shared authored x/y
domain, with no per-layer settings or domain.

Admission is exactly the record's defined-answer line. Bounds must be finite and
strictly ordered. Equal and inverted pairs receive distinct attributed
diagnostics because the current transform is contradictory for both; malformed,
non-literal, and duplicate constructors likewise refuse locally. No process
panic is involved.

Every strictly ordered finite interval renders as authored. The exclusion range
`100.0..200.0` over data spanning 1–6 completes with an empty viewport, and
`0.0..1e-300` reaches layout and SVG rendering unchanged. The parser contains no
epsilon, absolute-span, subtraction, or minimum-span admission rule. A figure
without bounds remains structurally identical to its established bare body.

## Gate ledger

| gate | evidence |
|---:|---|
| 1 | vertical bar completion with exact `y_min` / `y_max`, exact categorical layout range, and SVG rendering |
| 2 | direct unary-negative grammar and exact `-1.0..1.0` lowering |
| 3 | distinct exact-span assays for non-finite, inverted, collapsed, duplicate, malformed, and discrete-axis refusals |
| 4 | `100.0..200.0` exact layout and SVG completion over data spanning 1–6 |
| 5 | `0.0..1e-300` exact layout and SVG completion plus architecture prohibition on magnitude gates |
| 6 | omitted bounds retain `Override::Inherit`; existing one-body figure/bare intent equivalence remains green |
| 7 | layered line/scatter completion through one exact shared Cartesian domain |
| 8 | architecture ownership guard and unchanged full Polariton/libaryon suites |

## Verification

- `cargo fmt --all -- --check`: passed.
- `cargo test -p polariton --features toron`: 361 passed, 5 ignored.
- `cargo test -p polariton --test architecture_test`: 34 passed.
- `cargo test -p libaryon`: 1,437 passed, 22 ignored; auxiliary suites passed.
- `cargo check -p polariton --no-default-features`: passed with existing
  feature-gated unused warnings.
- `cargo build -p polariton`: passed.
- `git diff --check`: passed.
