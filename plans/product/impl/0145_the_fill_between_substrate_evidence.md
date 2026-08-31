# PRODUCT-0145 fill-between substrate — implementation evidence

**Status:** implemented, 2026-08-30.

## Shipped boundary

Fill-between now prepares one validated band authority in the surface role
order `x, lower, upper, middle?`. Preparation is fallible, carries the typed
`X`/`Lower`/`Upper`/`Middle` role on every error, validates every cell before
structural checks, and represents the optional middle as `Option<Vec<f64>>`.
No missing field or empty vector manufactures a zero baseline.

Quantitative cells require exact finite `f64` projection. Temporal x cells are
validated and ordered on their source integers, while the standing `f64` paint
projection and exact PRODUCT-0128 domain remain separate. Adjacent nanosecond
timestamps therefore validate rather than collapsing into a false duplicate;
when their projected coordinates coincide, nearest-x hit selects the first row
of the run deterministically.

Strictly increasing x, `lower <= upper`, and optional
`lower <= middle <= upper` are checked in that order after cell validation.
Equality at band or middle edges remains renderable. Empty input retains the
canonical `(0, 1)` Cartesian sentinel and draws nothing. The y axis is always
linear; its fallback label is the middle field when present and
`"{lower} / {upper}"` otherwise.

The band now consumes resolved `mark_style.fill`; the optional 2.5-pixel middle
stroke consumes `mark_style.fill_detail`. Both are opaque and no legacy
`ColorTheme` sample survives. The implementation-time amendment is banked by a
catalogue-wide skin test: anchor-distinct themes keep the roles distinct, while
the constant black, grey, and white palettes retain the same collapsed
paper-over-ink treatment already shipped by boxplot, histogram, pie, and
funnel. No contrasting fallback or per-mark colour exception was introduced.

Planning invokes the fallible preparation before factory reconstruction; the
factory reconstructs only under the standing planning-approved expectation.
Band-only hit omits `Mid`, reports an inclusive `Range`, and both legend methods
remain `None`.

`FillBetweenLinePlotKernel`, its module, both composition admission pairs, its
composition discriminator, and its factory branch are deleted together. The
PRODUCT-0133 priority assertion now runs entirely against the live scatter-line
composite. Scatter-line remains the only non-single built-in composition path.

Automatic resolution now emits
`ResolvedKernelFields::FillBetween { x, lower, upper, middle: Option<_> }`.
The x field consumes the first quantitative-or-temporal candidate; remaining
roles consume quantitative fields excluding x, so no field serves two roles.
Three suitable fields produce a band-only kernel and four produce the optional
middle. No Rune constructor, authored field carrier, settings membership, or
layer admission was added; PRODUCT-0146 still owns the surface.

## Discriminating evidence

- missing, temporal, unsupported, null, NaN, both infinities, and inexact
  failures carry the expected role, field, and row where applicable;
- a null cell outranks an unsorted-x structural fault;
- duplicate and reversed x, crossed bands, and escaped middles refuse, while
  zero-width bands and middles on either edge pass;
- band-only and with-middle preparations prove optional paint, hit, legend,
  empty-domain, and axis-label behavior;
- a zoned adjacent-nanosecond fixture proves typed ordering, exact domain
  retention, projected collapse, and deterministic first-row hit;
- a paint bank proves the resolved fill/fill-detail roles and stroke width;
- the canonical theme inventory proves the amended distinct/collapsed colour
  branches without tier mutation;
- all-quantitative and temporal-x automatic fixtures prove role order and
  no-reuse;
- planning rejects malformed input before construction and reconstructs an
  approved valid relation; and
- architecture guards pin the validated authority, legacy-ramp removal,
  composite retirement, surviving scatter-line pairs, and absent Rune surface.

## Verification at close

```text
cargo fmt --all -- --check                                      pass
cargo test -p polariton --bin polariton product_0145 \
  --all-features --no-fail-fast                                 pass (11)
cargo test -p polariton --bin polariton \
  product_0133_overlay_composites_apply_the_same_hit_priority \
  --no-fail-fast                                                 pass (1)
cargo test -p polariton --test architecture_test --no-fail-fast pass (106)
cargo test -p polariton --bin polariton --no-fail-fast \
  -- --test-threads=1                                            pass (1,044; 17 ignored)
cargo check --workspace --all-targets --all-features            pass
cargo test --workspace --all-targets --no-fail-fast \
  -- --test-threads=1                                            pass
```

The workspace retains its pre-existing ignored live, external-artifact, and
git-fixture tests. PRODUCT-0145 adds no ignored or tolerated failure. Existing
scatter documentation and PRODUCT-0057 render-evidence warnings are unchanged.
