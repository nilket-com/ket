# PRODUCT-0147 candlestick substrate — implementation evidence

**Status:** implemented, 2026-08-31.

## Shipped boundary

Candlestick now prepares one validated OHLC authority in the future surface
order `x, open, high, low, close`. Preparation is fallible and every refusal
carries its typed role and field; row faults also carry their one-based row.
All cells validate before strict producer-order and OHLC-containment checks, so
the former parallel-vector panic and null-buffer reads are unrepresentable.

Quantitative cells require exact finite `f64` projection. Temporal x values are
validated and ordered as their source integers while paint retains the standing
`f64` projection and exact PRODUCT-0128 domain. Adjacent nanoseconds therefore
remain ordered even when their paint coordinates coincide, and nearest-column
hit chooses the first row of a collapsed run. Empty temporal input keeps its
temporal axis identity, carries no invented exact domain, and uses the canonical
empty range sentinel.

Direction is no longer encoded by two unowned ramp samples. Rising candles are
hollow outlined bodies, falling candles are solid bodies, and unchanged candles
are explicit crossing doji dashes. Non-doji wicks split at effective body edges,
including after the one-pixel body floor; doji wicks remain full height. Body and
dash use resolved `mark_style.fill`, wick uses resolved
`mark_style.decoration`, and no `ColorTheme` sampling survives. The operation
bank proves the trichotomy under an ordinary skin and proves the standing
whole-mark ink-equals-paper collapse under the constant black skin without a
per-mark fallback.

For multiple rows, width is 70% of the minimum positive adjacent typed delta
projected through the active layout, with a one-pixel floor. For a single row it
is directly 70% of the active plot rectangle. The independent degenerate domain
uses representable `next_down()`/`next_up()` neighbours, including at a
nanosecond epoch where arithmetic padding collapses. Paint and hit share the
same width authority; hit is explicitly a nearest-x column interaction with
radius `max(10px, half_width)` and reports axis-typed X, all four OHLC scalars,
and nominal `up`/`down`/`unchanged` direction.

Planning now validates candlestick preparation before factory reconstruction,
and the factory reconstructs only under a PRODUCT-0147 planning-approved
expectation. Automatic resolution consumes quantitative-or-temporal x and four
distinct quantitative fields in OHLC order. It no longer selects categorical x.
No Rune callable, authored transport, settings membership, or layer admission
was added; PRODUCT-0148 retains the complete surface boundary. Candlestick joins
the no-categorical-identity fixture set.

The kernel fallback y label is now `open / high / low / close`; PRODUCT-0085's
named `Value` example was updated with attribution while its cascade premise is
unchanged. Open/close tick stubs, vestigial y temporal carriers, and the old
three-operation ramp oracle were retired together.

## Discriminating evidence

- missing, temporal, unsupported, null, NaN, both infinities, and inexact input
  refuse through typed roles; a null outranks an unordered-x fault;
- equal and reversed x refuse, lower and upper containment failures attribute
  to `Low` and `High`, and touching/zero-range rows validate;
- a zoned adjacent-nanosecond fixture proves typed order, exact domain retention,
  representable projected range, and first-row collapsed hit;
- operation traces distinguish split-wick outline, split-wick fill, and
  full-wick doji while keeping one resolved colour authority;
- a four-pixel hollow body retains canvas interior under the pinned one-pixel
  outline, while the smaller coincidence band remains admitted;
- leading-gap, dense, axis-override, single-row, and nanosecond-epoch fixtures
  discriminate every branch of the width contract;
- wide-body edge hit, quantitative and temporal X semantics, all three direction
  labels, joined y label, and no-legend behavior are banked;
- all-quantitative and temporal-x automatic fixtures prove field order and
  no-reuse; and
- architecture guards pin planning ownership, resolved colour, geometric
  direction, representable padding, repaired automatic resolution, and the
  still-absent Rune surface.

## Verification at close

```text
cargo fmt --all -- --check                                      pass
cargo test -p polariton --bin polariton product_0147 \
  --no-fail-fast                                                 pass (11)
cargo test -p polariton --test architecture_test product_0147 \
  --no-fail-fast                                                 pass (2)
cargo test -p polariton --test architecture_test --no-fail-fast pass (109)
cargo test -p polariton --bin polariton --no-fail-fast \
  -- --test-threads=1                                            pass (1,065; 17 ignored)
cargo check --workspace --all-targets --all-features             pass
cargo test --workspace --all-targets --no-fail-fast \
  -- --test-threads=1                                            pass
```

The closing workspace battery passed across every crate and compile-fail suite.
Its warnings were pre-existing and PRODUCT-0147 adds no ignored or tolerated
failure.
