# PRODUCT-0144 hexbin surface — implementation evidence

**Status:** implemented, 2026-08-30.

## Shipped boundary

Polariton now authors viewport-prepared hexbin plots through the free Rune
constructor `hexbin(x, y)`. The constructor accepts exactly two field strings,
retains each field's source span, transports `HexbinFields` through the authored
plot specification, and resolves both channels as quantitative before invoking
PRODUCT-0143's materialized preparation substrate.

The resolved spelling emits `ResolvedKernelFields::Hexbin { x, y }`, no colour
job, no categorical identity, and absolute normalization. Count colour remains
sequential; `diverging()` retains its heatmap-and-bubble-only refusal. Hexbin is
present in the universal mark settings inventory but absent from every
mark-specific geometry and composition list. Both quantitative axis-bound
members remain functional.

The live `Single` presentation path and the defensive directly-constructed
`StyledSingle` path label the mark `plot.hexbin`. `layers([hexbin(..), ..])`
returns the authored view-sink refusal rather than reaching an `unreachable!`,
while `relation.hexbin(..)` refuses by name because relation-shell sugar remains
closed.

Column faults stop during channel resolution and map from `ChannelError`
attribution to the authored x or y span. Row faults stop during observation
preparation and map from `HexbinPreparationError`'s carried channel. Diagnostic
preparation reuses the retained result's `artifact_token()`; it never mints a
replacement publication identity or compares field-name strings to decide a
span.

`uat/hexbin_probe.sql` and `uat/test41.rn` form the checked-in exhibit. The
numeric relation has repeated occupancy without collapsing all fourteen rows
into one cell at the default 600×400 lattice; the temporal companion refuses on
the authored `"stamp"` argument. Both repository READMEs carry the shipped
surface.

## Discriminating evidence

- exact eight-value Rune rewrite with two argument spans and one call span;
- free, named-view, named-figure, and bare-figure positions, plus exact arity,
  type, relation-sugar, diverging-word, and layer refusals;
- whole-resolved-spec equality for bound and inline field spellings;
- quantitative resolution and nominal, unsupported, Date, naive Datetime,
  zoned Datetime, and Time refusal in both channels;
- x/y diagnostic spans for missing, non-quantitative, temporal, null,
  non-finite, and inexact inputs, backed by a guard against field-name matching;
- inspector equality with heatmap minus `Diverging` and `DivergingAt`, including
  functional x and y bounds and absence of mark-specific geometry controls;
- authored planning and checkpoint/reopen proofs with no categorical colour
  footprint and no durable schema growth;
- precise inversion of PRODUCT-0143's three surface-absence claims while its
  renderer, factory, guide, and source-token architecture assertions survive;
- executable UAT compilation and a probe-lattice check proving
  `1 < max_count < N`; and
- unchanged PRODUCT-0143 substrate hashes:
  `c298c15653a673f881606ec804b09a056a287b3edf33cdacd99783b25d0a62c8`
  (`hexbin.rs`) and
  `b461a503934a59d81a96c88959a34c945a103f15972f330faba656856e96e550`
  (`hexbin_viewport.rs`).

## Verification at close

```text
cargo fmt --all -- --check                                      pass
cargo test -p polariton product_0144 --all-features             pass (10)
cargo test -p polariton --bin polariton --no-fail-fast \
  -- --test-threads=1                                            pass
cargo test -p polariton --test architecture_test --no-fail-fast pass (104)
cargo test -p polariton --test hexbin_observations_ui \
  --no-fail-fast                                                 pass
cargo check --workspace --all-targets --all-features            pass
cargo test --workspace --all-targets --no-fail-fast \
  -- --test-threads=1                                            pass
```

The workspace retains its pre-existing ignored live, external-artifact, and
git-fixture tests. PRODUCT-0144 adds no ignored or tolerated failure. Existing
scatter documentation and PRODUCT-0057 render-evidence warnings are unchanged.
