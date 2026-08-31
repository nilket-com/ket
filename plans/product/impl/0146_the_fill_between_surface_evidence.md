# PRODUCT-0146 fill-between surface — implementation evidence

**Status:** implemented and live-verified, 2026-08-31.

## Shipped boundary

Polariton now authors fill-between plots through the free Rune constructors
`fill_between(x, lower, upper)` and
`fill_between(x, lower, upper, middle)`. Both arities preserve per-field source
origins; the optional middle is trailing in the Rune stack, field transport,
authored specification, resolved kernel fields, diagnostics, and persistence
proofs.

The authored resolver admits quantitative or temporal x and quantitative
lower, upper, and optional middle fields. It emits
`ResolvedKernelFields::FillBetween`, absolute normalization, no colour job, and
no categorical identity. Column failures stop at resolution; row and structural
failures stop at PRODUCT-0145 preparation. Both stations map typed channel or
role attribution onto the offending argument span without field-name matching.

Fill-between joins the universal mark settings inventory and no mark-specific
inventory. Its inspector is exactly hexbin's: universal plot controls and both
axis-bound members, with no colour, stroke, bar, stack, or point controls.
`diverging()` remains heatmap-and-bubble only, and `LayerMark` remains
line-and-scatter only.

The live `Single` presentation path and the defensive directly constructed
`StyledSingle` path label the mark `plot.fill_between`.
`layers([fill_between(..), ..])` now returns the authored refusal naming
`fill_between` instead of panicking or using the generic fallback, while
`relation.fill_between(..)` refuses by name because relation-shell sugar stays
closed.

`uat/fill_between_probe.sql` and `uat/test42.rn` form the checked-in exhibit.
The healthy zoned-timestamp relation exercises both arities; the companion band
crosses at row 2 and produces the substrate's attributed refusal. Both
relations author ascending sorts because Toron scans do not promise row order;
both repository READMEs carry that requirement.

The implementation's first live app drive exposed a gap in the original
evidence: compilation plus an in-memory crossing fixture had been recorded as
if they proved rendering. The bare table scan returned forecast rows out of
timestamp order, so the two healthy panels correctly refused PRODUCT-0145's
strict-x contract. The checked-in Rune now sorts the forecast by `stamp` and
the crossed companion by `x`, and the compiler gate pins both spellings. This
was an exhibit repair only—temporal dtype transport and the fill-between
substrate were unchanged. Gate 14 now requires live app verification rather
than treating compilation as render evidence. The post-amendment live drive
renders both temporal-x panels—the band-only view and the band with its middle
path—and retains the crossed companion's row-2 refusal.

## Discriminating evidence

- exact eleven- and fourteen-value Rune rewrites with argument and call spans;
- exact arity, field-type, relation-sugar, diverging-word, and layer refusals;
- whole-transport equality for inline and bound fields in both arities;
- quantitative and zoned temporal x admission, with nominal x and temporal y
  roles refusing at their authored spans;
- x/lower/upper/middle diagnostic spans for missing columns and reachable
  preparation failures, including row-attributed order, crossing, and
  containment faults;
- resolved no-colour/no-identity proofs and checkpoint/reopen persistence;
- inspector equality with hexbin and closed applicability memberships;
- precise inversion of PRODUCT-0145's four surface-absence assertions while
  its automatic no-reuse and substrate authority assertions survive;
- executable UAT compilation, source-pinned ascending sorts, and a probe
  fixture proving the row-2 crossing; the render claim is reserved for the
  live app drive; and
- unchanged PRODUCT-0145 substrate hash
  `2ce2e6d31a691dc2c0c80bc91f19bcc5d415c3a3782bd4bf0366781ba144a600`
  (`plot/kernels/fill_between.rs`).

## Verification at close

```text
cargo fmt --all -- --check                                      pass
cargo check -p polariton --all-targets --all-features          pass
cargo test -p polariton --bin polariton product_0146 \
  --all-features --no-fail-fast                                 pass (10)
cargo test -p polariton --test architecture_test \
  --all-features --no-fail-fast                                 pass (107)
cargo test -p polariton --bin polariton --all-features \
  --no-fail-fast -- --test-threads=1                            pass (1,054; 17 ignored)
cargo check --workspace --all-targets --all-features            pass
cargo test --workspace --all-targets --no-fail-fast \
  -- --test-threads=1                                            pass
```

The workspace retains its pre-existing ignored live, external-artifact,
benchmark, and repository-fixture tests. PRODUCT-0146 adds no ignored or
tolerated failure. Existing scatter documentation and PRODUCT-0057
render-evidence warnings are unchanged.

## Implementation-time exhibit amendment

```text
cargo fmt --all -- --check                                      pass
cargo test -p polariton product_0146 \
  --all-features --no-fail-fast                                 pass (12)
post-amendment live Toron/app drive                              pass
```
