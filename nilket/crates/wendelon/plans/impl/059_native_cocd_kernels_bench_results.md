# ADR 059 Native COCD Kernel Bench Results

Status: full 8K / 65K / 1M evidence captured; narrowed
default-set promotion gate **passed**.

Command:

```bash
cargo bench --bench encoding_compute
```

Run shape:

- Date: 2026-05-07
- Tiers: 8,192 / 65,536 / 1,048,576 rows
- Iterations: 200 per row
- Harness note: `bench(...)` runs 5 untimed warmup iterations
  before measuring each row.

## Predicate CPU

Representative rows, in microseconds per operation:

| Fixture | Rows | Native COCD | Dense micro | Native / dense | Disposition |
| --- | ---: | ---: | ---: | ---: | --- |
| `bp_u8` | 8K | 4.922 | 31.454 | 6.39x faster | pass |
| `bp_u8` | 65K | 42.131 | 233.215 | 5.54x faster | pass |
| `bp_u8` | 1M | 652.372 | 3795.122 | 5.82x faster | pass |
| `bp_u32_low` | 8K | 37.688 | 36.392 | 1.04x slower | exclude sublane |
| `bp_u32_low` | 65K | 300.464 | 283.963 | 1.06x slower | exclude sublane |
| `bp_u32_low` | 1M | 4739.544 | 4540.633 | 1.04x slower | exclude sublane |
| `rle_plain` | 1M | 6679.676 | 4965.500 | 1.35x slower | exclude kind |
| `rle_bitpacked` | 1M | 6667.442 | 4906.283 | 1.36x slower | exclude kind |
| `dict_plain_lowcard` | 8K | 7.681 | 34.097 | 4.44x faster | pass |
| `dict_plain_lowcard` | 65K | 60.487 | 267.864 | 4.43x faster | pass |
| `dict_plain_lowcard` | 1M | 973.784 | 4422.740 | 4.54x faster | pass |
| `dict_bitpacked_lowcard` | 8K | 13.136 | 36.814 | 2.80x faster | pass |
| `dict_bitpacked_lowcard` | 65K | 104.468 | 295.328 | 2.83x faster | pass |
| `dict_bitpacked_lowcard` | 1M | 1700.175 | 4661.330 | 2.74x faster | pass |
| `dict_bitpacked_higher` | 8K | 40.225 | 38.845 | 1.04x slower | exclude sublane |
| `dict_bitpacked_higher` | 65K | 321.673 | 307.760 | 1.05x slower | exclude sublane |
| `dict_bitpacked_higher` | 1M | 5147.153 | 4996.957 | 1.03x slower | exclude sublane |
| `delta_plain` | 8K | 19.578 | 41.839 | 2.14x faster | pass |
| `delta_plain` | 65K | 155.753 | 327.993 | 2.11x faster | pass |
| `delta_plain` | 1M | 2509.218 | 5475.785 | 2.18x faster | pass |
| `delta_bitpacked` | 8K | 19.225 | 41.817 | 2.18x faster | pass |
| `delta_bitpacked` | 65K | 157.987 | 337.641 | 2.14x faster | pass |
| `delta_bitpacked` | 1M | 2470.977 | 5464.188 | 2.21x faster | pass |

BitPacked low-selectivity sweep:

| Selectivity fixture | 8K | 65K | 1M |
| --- | ---: | ---: | ---: |
| `bp_u8/p001` | 4.664 | 37.868 | 585.042 |
| `bp_u8/p005` | 4.699 | 38.047 | 618.280 |
| `bp_u8/p025` | 5.215 | 40.940 | 652.154 |
| `bp_u8/p050` | 6.257 | 49.285 | 801.211 |

Dense fallback overhead:

| Rows | Dense micro | Classify then fallback | Overhead |
| ---: | ---: | ---: | ---: |
| 8K | 31.454 | 29.891 | no positive overhead |
| 65K | 233.215 | 233.572 | 0.2% |
| 1M | 3795.122 | 3758.373 | no positive overhead |

Interpretation:

- Native predicate correctness is established by
  `tests/encoding_compute_kernel_invariants.rs`.
- BitPacked `UInt8` now clears the predicate CPU gate across all
  three tiers after the aligned bitmap-byte emission fast path.
  Low-cardinality Dictionary clears the predicate CPU gate across
  all three tiers.
- Delta's monotonic signed range fast path plus direct signed-bound
  comparison now clears all three predicate tiers.
- BitPacked `UInt32` at 12 packed bits and higher-cardinality
  Dictionary are excluded by pre-kernel native eligibility; the
  decode-scalar oracle now has a fixed-width integer fast path so
  excluded sublanes stay near dense fallback cost.
- RLE byte-run predicates do not clear the 2x default-inclusion gate
  and are omitted from the default `enabled_predicate_evaluators`
  allow-list.
- RLE is worse than dense because ADR 057 ships byte-run RLE, not
  value-run RLE; this is now empirical evidence for excluding RLE
  from the default set and considering a separate value-run follow-up.

## Aggregate CPU

Representative rows, in microseconds per operation:

| Fixture | Aggregate | Rows | Native encoded fold | Dense/plain control | Disposition |
| --- | --- | ---: | ---: | ---: | --- |
| `bp_u8` | unsigned sum | 8K | 4.143 | 17.565 | pass |
| `bp_u8` | unsigned sum | 65K | 31.837 | 143.623 | pass |
| `bp_u8` | unsigned sum | 1M | 500.632 | 2760.056 | pass |
| `bp_u32_low` | unsigned sum | 8K | 14.000 | 17.565 | exclude sublane |
| `bp_u32_low` | unsigned sum | 65K | 108.050 | 143.623 | exclude sublane |
| `bp_u32_low` | unsigned sum | 1M | 1714.334 | 2760.056 | exclude sublane |
| `rle_plain` | unsigned sum | 1M | 7726.828 | 2760.056 | exclude kind |
| `rle_bitpacked` | unsigned sum | 1M | 7671.178 | 2760.056 | exclude kind |
| `dict_plain_lowcard` | unsigned sum | 8K | 7.132 | 17.565 | pass |
| `dict_plain_lowcard` | unsigned sum | 65K | 53.381 | 143.623 | pass |
| `dict_plain_lowcard` | unsigned sum | 1M | 889.737 | 2760.056 | pass |
| `dict_bitpacked_lowcard` | unsigned sum | 8K | 8.816 | 17.565 | near miss |
| `dict_bitpacked_lowcard` | unsigned sum | 65K | 67.714 | 143.623 | pass |
| `dict_bitpacked_lowcard` | unsigned sum | 1M | 1094.532 | 2760.056 | pass |
| `dict_bitpacked_higher` | unsigned sum | 1M | 3816.798 | 2760.056 | exclude |
| `delta_plain` | signed sum | 1M | 4792.841 | no dense signed control | not default-routed |
| `delta_bitpacked` | signed sum | 1M | 4791.321 | no dense signed control | not default-routed |

Min/max smoke rows now use streaming native folds and encode the
winning partial once:

| Fixture | Rows | Min | Max |
| --- | ---: | ---: | ---: |
| `bp_u8` | 8K | 20.950 | 20.713 |
| `bp_u8` | 65K | 170.785 | 169.086 |
| `bp_u8` | 1M | 2692.470 | 2723.431 |
| `dict_bitpacked_lowcard` | 1M | 2517.493 | 2520.646 |

Interpretation:

- Aggregate partial bytes match the decoded oracle in the property
  suite.
- Sum/avg/min/max stream encoded values directly. Native aggregate
  contexts no longer materialize decoded value bytes before the fold;
  the old decoding constructor remains for oracle/fallback tests.
- No-validity BitPacked sums use a byte-aligned selected-row fast path;
  masked non-byte-aligned BitPacked sums use bit-level row lookup;
  no-validity Dictionary sums use a per-page index histogram and a
  weighted dictionary sum.
- `native_aggregate_cascade_eligible(...)` default-routes only
  measured pass-grade unsigned value aggregates: byte-aligned
  BitPacked `UInt8` and plain low-cardinality Dictionary. Bitpacked
  low-cardinality Dictionary remains implemented and benchmarked, but
  its 8K row is just under 2x in this run, so it is not default-routed.
- `MaskBuilder::push_many` fills whole selected bytes when row-aligned,
  reducing all/none and run-shaped mask emission overhead.
- Bitmap mask popcount is bytewise, and aggregate selected-row
  setup reuses that count instead of walking the decoded bool mask
  a second time.
- Native no-validity sum / avg requests consume `All` / `None` /
  `Bitmap` masks directly, avoiding `Vec<bool>` materialization
  for the common masked aggregate path.
- Dictionary aggregate histograms reuse a stack counts buffer for
  dictionaries up to 4,096 entries and count common Plain / 1-bit /
  2-bit / 4-bit / 8-bit index streams directly.
- Masked BitPacked sums and Dictionary histograms now walk selected
  bitmap set bits and read only selected rows; Dictionary uses direct
  index-at-row lookup for masked pages instead of checking every row.
- Aggregates pass for the narrowed default set. BitPacked `UInt8`
  and plain low-cardinality Dictionary sums clear 2x across all three
  tiers.
  Non-byte-aligned BitPacked `UInt32` sums now stay native and beat the
  dense/plain control at 1M, but remain below the 2x promotion gate
  and are excluded from production aggregate routing.
  RLE value aggregates, bitpacked Dictionary aggregates, Delta signed
  sums, and higher-cardinality Dictionary remain below promotion grade
  or lack the required dense signed control.
- Cascade-grain aggregate inclusion is enforced by
  `native_aggregate_cascade_eligible(...)`;
  `enabled_aggregate_kernels` is not narrowed because it is
  aggregate-kind-grain, not cascade-grain.

## Correctness Evidence

Passing focused checks after this bench:

```bash
cargo test --test encoding_compute_kernel_invariants
cargo test --test encoding_compute_test
cargo test --test architecture_test
cargo check --benches
git diff --check
```

## Promotion Decision

Promote ADR 059 to default-enabled with a narrowed default set.

`EncodingComputeConfig::default().enabled` flips to `true` in this
changeset. The default `enabled_predicate_evaluators` allow-list
contains `BitPacked`, `DictionaryPlain`, `DictionaryBitPacked`, and
`Delta`; it omits `RlePlain` and `RleBitPacked` because byte-run RLE
misses substantially. Pre-kernel eligibility further excludes
non-byte-aligned BitPacked sublanes and higher-cardinality Dictionary
sublanes from native dispatch before kernel entry. Aggregate
default-routing is cascade-grain through
`native_aggregate_cascade_eligible(...)`: count kernels remain enabled,
and unsigned value aggregates default-route only for byte-aligned
BitPacked `UInt8` and plain low-cardinality Dictionary.

Deferred after Phase 12:

- Add true RLE value-run support in a separate ADR before considering
  RLE for the default predicate or aggregate set.
- Improve aggregate RLE handling and non-byte-aligned BitPacked
  handling enough to beat dense AGG by the ADR gate, not merely
  reach parity or a sub-2x large-tier win.
- Revisit bitpacked low-cardinality Dictionary aggregate routing if
  its 8K row clears the 2x gate with stable margin.

This changeset includes the cascade-grain aggregate eligibility
layer. The raw encoded aggregate folds remain implemented and
benchmarked, and the native aggregate context avoids eager decoded
materialization; production aggregate planning admits only the
measured pass-grade default set above.
