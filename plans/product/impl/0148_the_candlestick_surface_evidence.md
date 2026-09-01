# PRODUCT-0148 candlestick surface — implementation evidence

**Status:** implemented, 2026-09-01.

## Shipped boundary

Rune now authors candlestick plots as the fixed five-position spelling
`candlestick(x, open, high, low, close)`. The registry admits exactly five
arguments and lowers them through the seventeen-value injected call shape into
one `CandlestickFields` / `CandlestickFieldOrigins` transport. Bound and inline
relations converge on the same authored specification.

Resolution admits quantitative or temporal x and four distinct quantitative
OHLC fields. X and open use the shared required-channel resolver; high, low,
and close retain their named typed roles so every column refusal maps back to
the argument that authored it. No field is reused, no colour job is created,
and the surface changes no PRODUCT-0147 geometry or preparation authority.

Diagnostics use the established two-station contract. Resolution failures map
directly from attributed channel errors. Preparation is replayed only after
successful resolution, over the frame and authored fields, and maps typed
`X`, `Open`, `High`, `Low`, and `Close` failures without string comparison.
The structural containment attributions inherited from PRODUCT-0147 therefore
land ordering on x, a lower-bound tear on low, and an upper-bound tear on high.
The preparer takes no artifact token; the retained-token planning contract
remains owned by PRODUCT-0147.

All four closed-enumeration sites are complete: both display labels name
`plot.candlestick`, `layers([candlestick(...)])` returns the authored
not-a-layer-mark refusal, and layer decoding names `candlestick` rather than a
generic fallback. Relation-method sugar remains a named refusal. Candlestick
joins `ALL_MARKS` only; it remains absent from `BAR_MARKS`, `STROKE_MARKS`, and
the colour-job admission lists because PRODUCT-0147 pins its width, strokes,
and one-colour geometric direction semantics.

Session checkpoint/reopen retains the authored plot without manufacturing a
categorical colour identity. The existing no-identity proof now exercises the
authored spelling as its fifth fixture. PRODUCT-0147's four surface-absence
assertions were inverted precisely while its validation, no-reuse, geometric
colour, width, hit, and planning authorities remain banked.

The candlestick section is shipped in the README and the stale
`Scatter / Line / Bar plots` backlog line is retired. `test43.rn` authors a
sorted temporal relation spanning a weekend and a sorted torn companion. The
SQL probe is idempotent and carries rising, falling, and unchanged rows plus a
row-two upper-bound tear.

## Discriminating evidence

- the registry rewrite produces exactly seventeen VM values and rejects every
  non-five arity;
- bound and inline relations produce identical authored fields and origins;
- temporal x resolves, nominal x refuses at x, and open/high/low/close faults
  attribute to their own spans;
- preparation null/non-finite faults and ordering/lower/upper structural faults
  map through typed roles, with no field-name string comparison;
- directly constructed styled candlestick completes its defensive label and
  refuses layering rather than panicking;
- settings are exactly the hexbin row set: `ALL_MARKS` membership without bar,
  stroke, diverging, or diverging-at controls;
- checkpoint/reopen and the no-identity fixture prove no categorical colour
  footprint; and
- architecture guards pin one registry row, one transport, the closed sugar and
  layer boundaries, no colour job, and byte-identical PRODUCT-0147 substrate.

The substrate file remained byte-identical to the PRODUCT-0147 landing:

```text
crates/polariton/src/plot/kernels/candlestick.rs
sha256 6d8633b63402a5ef696e69d280dca1bba38462a148a8146cd7721884d9adaff1
```

## Live exhibit evidence

The probe was seeded through the real Toron SQL path, then `uat/test43.rn` was
driven through the live Polariton application. Contact was captured at:

```text
/home/me/Pictures/Screenshots/Screenshot From 2026-09-01 08-53-57.png
```

The healthy view visibly renders a hollow rising body, a filled falling body,
an unchanged doji cross, and the temporal weekend gap. The torn view refuses as
designed. Its visible live status is:

```text
Rune · plot.candlestick at bytes 560..566: workspace relation: candlestick OHLC upper
```

The complete row-two upper-bound message is banked by the checked-in probe
test; the status bar is intentionally width-truncated in this contact. Bytes
`560..566` are the torn call's authored `"high"` argument, proving the live
structural refusal reaches the correct source span rather than merely producing
a generic view failure. The screenshot, not compilation alone, discharges the
render half of the exhibit gate.

## Verification at close

```text
cargo fmt --all -- --check                                      pass
cargo test -p polariton --bin polariton product_0148 \
  --no-fail-fast                                                 pass (10)
cargo test -p polariton --test architecture_test product_0148 \
  --no-fail-fast                                                 pass (2)
cargo test -p polariton --test architecture_test --no-fail-fast pass (110)
cargo test -p polariton --bin polariton --no-fail-fast \
  -- --test-threads=1                                            pass (1,075; 17 ignored)
cargo check --workspace --all-targets --all-features             pass
cargo test --workspace --all-targets --no-fail-fast \
  -- --test-threads=1                                            pass
```

The closing workspace battery passed across every crate and compile-fail suite.
Its warnings were pre-existing and PRODUCT-0148 adds no ignored or tolerated
failure.
