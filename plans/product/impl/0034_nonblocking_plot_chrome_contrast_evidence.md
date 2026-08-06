# PRODUCT-0034 implementation evidence

Accepted 2026-08-06. Plan `ae8ec23a`; implementation `f904536d`.

## Result

Plot skin projection is now fallible only while resolving the theme identity.
Once `Theme::try_new` succeeds, Ordered and Duotone projection always return the
exact completed `PlotChrome`. The alpha search still chooses the quietest listed
grid/sample alpha that reaches 3:1 when possible, but neither that search nor
the complete assay gates rendering.

`tab20c_s9` is the executable regression. It completes with paper
`rgb(117,107,177)` and ink `rgb(199,233,192)`. Its weakest fact remains title
text at approximately `3.526:1` against the recorded 4.5 threshold;
`passes_contrast()` remains false. The same below-threshold chrome reaches the
real destination output rather than producing an error.

A synthetic `rgb(100,100,100)` / `rgb(120,120,120)` pair likewise returns a
complete artifact and remains classified below threshold. The full accepted
theme-expression census completes in both Ordered and Duotone modes and proves
that below-threshold observations are present rather than silently repaired.

Unknown theme identity still returns `UnknownTheme`; malformed contextual Rune
still refuses at its proven span. The production source contains no
`UnsuitableTheme` variant, fallback, warning path, alternate projection probe,
or repair. Renderer roles, marks, data colors, Guide behavior, default chrome,
identity, placement, and stale lifecycle are unchanged.

## Gate ledger

| gate | evidence |
|---:|---|
| 1 | exact `tab20c_s9` projection and destination-completion assay |
| 2 | synthetic low-range complete chrome plus failed measurement |
| 3 | two-mode accepted-theme grammar census always completes |
| 4 | retained unknown-theme and contextual syntax failures |
| 5 | architecture guard and unchanged ownership/lifecycle suites |

## Verification

- `cargo fmt --all -- --check`: passed.
- `cargo test -p polariton --features toron`: 355 passed, 5 ignored.
- `cargo test -p polariton --test architecture_test`: 32 passed.
- `cargo test -p libaryon`: 1,437 passed, 22 ignored; auxiliary suites passed.
- `cargo check -p polariton --no-default-features`: passed with existing
  feature-gated unused warnings.
- `cargo build -p polariton`: passed.
- `git diff --check`: passed.
