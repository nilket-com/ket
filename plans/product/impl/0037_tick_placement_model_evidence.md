# PRODUCT-0037 implementation evidence

Accepted 2026-08-06. Plan `7db19744`; parity oracle `6879135f`; implementation
`eec3491e`, `22fa5305`, and `a02e20cd`.

## Result

Major tick placement is now a closed substrate model:
`TickPlacement::Count(NonZeroU32)` or a validated positive finite `Step`.
`Count(8)` remains the library default. The renderer no longer derives tick
positions at its sixteen independent sites; one generator supplies major
positions, and minor positions subdivide those generated major intervals.

The pre-refactor oracle records the exact legacy floating-point Count(8)
positions over both `0.0..7.0` and `1.4..8.6`, plus the legacy minor positions.
The post-refactor generator matches it exactly, and the established SVG oracle
remains byte-identical. No default pixel output changes.

`Step` is a zero-anchored lattice. The generator produces the four decided
examples (`0..7`, `1.4..8.6`, `0.2..7.2`, and `-3.5..2.5`), preserves all prior
positions when `0..7` widens to `0..10`, and subdivides each major interval for
minor ticks. Non-positive and non-finite values cannot construct a Step.

Dense lattices return an attributed capacity error carrying the requested step,
resolved range, estimated position count, and declared 10,000-position limit;
the generator never truncates or partially returns the lattice. Step placement
also rejects temporal axis types. Categorical/band applicability remains a
surface-resolution concern for PRODUCT-0038, where the resolved scale supplies
the attributed axis diagnostic.

No Rune constructor, kernel geometry, mark, palette, skin, bounds, identity,
placement, lifecycle, or cascade algebra changed. The plot-pane count editor
continues to edit the Count variant, and radar's legacy circle count remains
eight when no count placement is available.

## Gate ledger

| gate | evidence |
|---:|---|
| 1 | pre-refactor oracle plus post-refactor Count(8) exact parity and unchanged SVG oracle |
| 2 | architecture guard counts the single helper and all sixteen consumers; no inline tick arithmetic remains |
| 3 | exact four zero-anchored Step lattices |
| 4 | widening `0..7` to `0..10` preserves the original lattice and adds only new positions |
| 5 | Count minor parity and Step minor subdivision observe generator positions |
| 6 | dense `1e-9` over `0..7` returns capacity with step/range/request/limit and no truncation |
| 7 | temporal Step refusal plus surface-boundary reservation for categorical/band scales |
| 8 | invalid Step values are rejected at construction |
| 9 | full ownership architecture guard and unchanged cross-crate suites |

## Verification

- `cargo fmt --all -- --check`: passed.
- `cargo test -p polariton --features toron`: 365 passed, 5 ignored.
- `cargo test -p polariton --test architecture_test`: 35 passed.
- `cargo test -p libaryon`: 1,437 passed, 22 ignored; auxiliary suites passed.
- `cargo check -p polariton --no-default-features`: passed with existing
  feature-gated unused warnings.
- `cargo build -p polariton`: passed.
- `git diff --check`: passed.
