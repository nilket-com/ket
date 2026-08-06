# PRODUCT-0035 implementation evidence

Accepted 2026-08-06. Plan `9f4478f9`; implementation `cb2daa89`.

## Result

Table-skin projection is now fallible only while resolving a theme identity.
Once `Theme::try_new` succeeds, both `PaletteForward` and `Duotone` return their
exact completed `TableSkin`. The projection algorithms, ordered role spending,
neutral maximin choice, panel identity, and frame-plus-skin lifecycle are
unchanged.

Contrast remains an ordinary seven-role assay on the completed skin through
`visible_contrast_checks()`, `weakest_contrast()`, and `passes_contrast()`.
Those methods have no production caller in the Rune destination path and cannot
gate admission, choose a fallback, repair a color, or emit advice.

Yellows proves the correction through both projections. Duotone now renders
with its exact weakest completed role, cell text on the selected odd row at
`3.1750332126087506:1`; PaletteForward remains distinct and measures
`15.790320818844014:1` at its weakest role. The checked-in 8,480-expression
Duotone oracle remains byte-identical. The two-mode census now completes every
expression while retaining the prior facts: 5,355 Duotone projections and
5,979 PaletteForward projections meet 4.5:1.

`dark2_s1` completes through both projection mechanisms and each remains
classified below threshold. A synthetic single-color palette produces equal
paper and ink at 1:1 without refusal or repair; registered `black` reaches the
same result through the public resolution seam.

The real Rune destination resolves an authored projection exactly once. It no
longer probes PaletteForward after a Duotone result, emits the teaching sentence,
or preserves stale content because of contrast. Below-threshold authored output
replaces the prior artifact and becomes fresh. Unknown theme identity retains
its existing diagnostic; grammar, categorical capability, capacity, and
undefined-projection refusals are unchanged.

Polariton-owned colors remain separately obligated: `TableSkin::BUILT_INS`
still contains `TableSkin::DEFAULT`, and PRODUCT-0023's test requires all seven
completed text roles to meet 4.5:1 after alpha compositing.

## Gate ledger

| gate | evidence |
|---:|---|
| 1 | exact Yellows Duotone completion at 3.175:1 with no destination diagnostic |
| 2 | both `dark2_s1` mechanisms complete and remain measurable below threshold |
| 3 | synthetic equal-anchor and registered `black` paper-equals-ink completion |
| 4 | unchanged seven-role 4.5:1 built-in/default quality gate |
| 5 | retained unknown identity and unchanged no-defined-answer suites |
| 6 | exact projection, position, panel identity, and lifecycle regressions |
| 7 | byte-identical 8,480-expression oracle and exact two-mode census counts |
| 8 | architecture guard and unchanged plot/libaryon suites |

## Verification

- `cargo fmt --all -- --check`: passed.
- `cargo test -p polariton --features toron`: 355 passed, 5 ignored.
- `cargo test -p polariton --test architecture_test`: 33 passed.
- `cargo test -p libaryon`: 1,437 passed, 22 ignored; auxiliary suites passed.
- `cargo check -p polariton --no-default-features`: passed with existing
  feature-gated unused warnings.
- `cargo build -p polariton`: passed.
- `git diff --check`: passed.
