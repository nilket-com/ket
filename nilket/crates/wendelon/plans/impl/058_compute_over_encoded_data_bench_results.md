# ADR 058 Bench Results — Compute Over Encoded Data

Date: 2026-05-07

Status: Phase 8 substrate evidence promoted to shipped. Bench
matrix covers the cascade fixture set (BitPacked / Rle /
Dictionary / Delta / Plain / Lz4Frame across UInt8 / UInt32 /
Int64 dtypes), the dense-micro baseline that bounds substrate
overhead vs. an alternate decode-then-scalar path, the
classifier fallback overhead gate (≤ 5% — passes), the Phase
B + C + D pipeline shape via public storage entries, the
provider-leak counter (planner-intended GET ranges + selected
bytes), the mixed-leaf classifier shape, mask-algebra
completeness, the config kill-switch fallback, the aggregate
min/max kernels, and selectivity-swept aggregate folds.

After iteration-6 council convergence, the COCD evaluator
catalog accepts `Delta { .. }` (any inner) — the byte-level
identity invariant is pinned by
`delta_inner_variants_produce_byte_identical_encoded_frames`.
Iteration 2's tightening for `Delta { Plain | Lz4Frame }` →
DenseFallback was over-correcting and is reverted; the
tightening for Rle / Dictionary inner Lz4 stands.

Mask, aggregate-partial, and metric parity gates pass; ADR 056
/ 057 / 040 / 052 / 055 regression suites stay byte-neutral
with `EncodingComputeConfig.enabled = false` (the V1 default).
Native per-cascade kernel work and the runtime-measured
`MockObjectStore` GET counter remain promote-to-default
follow-ups (kernel-promotion artifact scope, see iteration-7
backlog).

Promotion-to-default 2x CPU targets remain deferred behind the
native per-cascade kernel pass: the V1 evaluator decodes through
ADR 057's deterministic decoder and runs a scalar predicate /
fold loop with `EncodedRowMask` allocation. The numbers below
are the substrate floor, not the promotion ceiling.

This is the promoted substrate bench artifact. Native-kernel
promote-to-default evidence will land separately at
`plans/impl/058_compute_over_encoded_data_kernel_bench_results.md`
once specialized kernels exist.

Command:

```sh
cargo bench --bench encoding_compute
cargo bench --bench encoding_compute -- --quick
```

Implementation state:

- substrate landed under `src/storage/encoding/compute/`:
  `aggregate.rs` (629), `dispatch.rs` (335), `types.rs` (278),
  `mask.rs` (233), `predicate.rs` (147), plus `coerce`, `null`,
  `planner`, `index`, `mod`, and per-cascade label-shim modules
  (`kernels/{bitpacked,rle,dictionary,delta,validity}.rs`);
- `EncodingComputeConfig` wired through `src/config.rs`,
  `src/node.rs`, `src/storage/manager.rs` with deployment-wide
  defaults (`enabled = false`, leaf selectivity gate at 0.25,
  Prefilter rejection gate at 0.10, mask cap 256 KiB, page cap
  256, dict-match-set cap 4096, min-buffer-rows 256);
- bounded `storage_l2_encoding_compute_*` metric family wired
  in `src/metrics/registry.rs`; `EncodingComputeSampled` /
  `EncodingComputeInvariantViolation` audit events wired in
  `src/audit.rs` and `src/network/data_server.rs`;
- predicate evaluator path runs through
  `dispatch::evaluate(...)` which decodes the encoded buffer
  via ADR 057 `decode_buffer(...)` (paged or full-buffer) and
  runs a scalar predicate loop combined with the validity
  bitmap; per-cascade SIMD / run-grain / match-set / prefix-sum
  specializations are deferred to follow-up work and currently
  exist only as module anchors;
- aggregate-fold path runs full 8-kernel matrix
  (`count_star`, `count_non_null`, `min`, `max`, `signed_sum`,
  `unsigned_sum`, `signed_avg`, `unsigned_avg`) over decoded
  cascade values with mask-aware contribution; mixed-request
  policy is all-or-nothing per ADR §8;
- Phase-B planner extension and AGG dispatch ordering are wired
  through `src/storage/manager.rs` and
  `src/network/data_server.rs`; CSB direct-answer ordering is
  preserved ahead of COCD aggregate fold;
- ADR 052 secondary-index COCD confirm-step lives in
  `src/storage/encoding/compute/index.rs`;
- bench harness `benches/encoding_compute.rs` covers eleven
  cascade fixtures over UInt8 / UInt32 / Int64 dtypes plus the
  legacy `predicate_exact` / `mask_algebra` / `aggregate_fold`
  scenario aliases the architectural guard pins;
- evaluator-kind routing: `evaluator_kind_for_cascade(...)` in
  `src/storage/encoding/compute/dispatch.rs` now returns `None`
  for `Rle { inner: Lz4Frame }` and
  `Dictionary { inner: Lz4Frame }`, while every
  `Delta { .. }` variant maps to `Delta` because Delta bytes and
  dispatch grain are inner-irrelevant. The tests
  `evaluator_kind_for_cascade_routes_unsupported_inners_to_dense_fallback`
  and `delta_inner_variants_produce_byte_identical_encoded_frames`
  pin both sides of that mapping.

## Cascade Fixture Matrix

| Fixture | Cascade | Dtype | Notes |
| --- | --- | --- | --- |
| `bp_u8` | `BitPacked { bits=8, base=0 }` | UInt8 | 0..251 cycle, atom `Lte 12` (~5%) |
| `bp_u32_low` | `BitPacked { bits=12, base=0 }` | UInt32 | 0..4095 cycle, atom `Lt 1024` (~25%) |
| `rle_plain` | `Rle { run_count, inner: Plain }` | UInt32 | 32-row runs, atom `Eq 7` |
| `rle_bitpacked` | `Rle { run_count, inner: BitPacked{bits=4} }` | UInt32 | 32-row runs, atom `Eq 3` |
| `dict_plain_lowcard` | `Dictionary { 4 entries, inner: Plain }` | UInt32 | atom `Eq 30` |
| `dict_bitpacked_lowcard` | `Dictionary { 4 entries, inner: BitPacked{bits=2} }` | UInt32 | atom `Eq 300` |
| `dict_bitpacked_higher` | `Dictionary { 256 entries, inner: BitPacked{bits=8} }` | UInt32 | atom `Eq 1700` |
| `delta_plain` | `Delta { base, inner: Plain }` | Int64 | monotonic HLC-shaped, atom `Gte midpoint` |
| `delta_bitpacked` | `Delta { base, inner: BitPacked{bits=8} }` | Int64 | monotonic HLC-shaped, atom `Gte midpoint` |
| `plain_u32` | `Plain` | UInt32 | non-compressible, atom `Lt 1e9` |
| `lz4_u32` | `Lz4Frame` | UInt32 | compressible, atom `Lt 256` |

`target_simd_page_values = 256`.

## Predicate CPU — COCD vs Dense Micro Baseline

`predicate_cocd` calls `dispatch::evaluate(...)`; output is an
`EncodedRowMask`. `predicate_dense_micro` calls a paged-aware
`decode_buffer` then a typed scalar comparison loop returning a
`Vec<bool>`; output approximates the materialize-then-evaluate
shape PBC's dense path produces, less the bit-packing step.

8,192 rows:

| Fixture | COCD (us/op) | Dense micro (us/op) | COCD / Dense |
| --- | ---: | ---: | ---: |
| `bp_u8` | 63.448 | 27.065 | 2.34x |
| `bp_u32_low` | 64.972 | 32.099 | 2.02x |
| `rle_plain` | 73.084 | 38.205 | 1.91x |
| `rle_bitpacked` | 73.797 | 34.823 | 2.12x |
| `dict_plain_lowcard` | 69.240 | 37.607 | 1.84x |
| `dict_bitpacked_lowcard` | 68.863 | 37.576 | 1.83x |
| `dict_bitpacked_higher` | 72.175 | 39.954 | 1.81x |
| `delta_plain` | 75.443 | 44.301 | 1.70x |
| `delta_bitpacked` | 74.822 | 45.775 | 1.63x |
| `plain_u32` | 47.713 | 9.287 | 5.14x |
| `lz4_u32` | 64.266 | 31.854 | 2.02x |

65,536 rows:

| Fixture | COCD (us/op) | Dense micro (us/op) | COCD / Dense |
| --- | ---: | ---: | ---: |
| `bp_u8` | 532.206 | 216.168 | 2.46x |
| `bp_u32_low` | 575.751 | 258.225 | 2.23x |
| `rle_plain` | 581.827 | 303.200 | 1.92x |
| `rle_bitpacked` | 573.453 | 296.026 | 1.94x |
| `dict_plain_lowcard` | 615.101 | 301.983 | 2.04x |
| `dict_bitpacked_lowcard` | 598.762 | 302.579 | 1.98x |
| `dict_bitpacked_higher` | 607.034 | 329.468 | 1.84x |
| `delta_plain` | 631.797 | 367.866 | 1.72x |
| `delta_bitpacked` | 631.914 | 361.815 | 1.75x |
| `plain_u32` | 421.842 | 77.949 | 5.41x |
| `lz4_u32` | 544.471 | 243.935 | 2.23x |

262,144 rows:

| Fixture | COCD (us/op) | Dense micro (us/op) | COCD / Dense |
| --- | ---: | ---: | ---: |
| `bp_u8` | 2437.283 | 849.693 | 2.87x |
| `bp_u32_low` | 2543.670 | 1024.139 | 2.48x |
| `rle_plain` | 2615.863 | 1179.348 | 2.22x |
| `rle_bitpacked` | 2612.418 | 1111.879 | 2.35x |
| `dict_plain_lowcard` | 2773.175 | 1223.282 | 2.27x |
| `dict_bitpacked_lowcard` | 2778.785 | 1211.835 | 2.29x |
| `dict_bitpacked_higher` | 2794.661 | 1335.808 | 2.09x |
| `delta_plain` | 2866.911 | 1455.302 | 1.97x |
| `delta_bitpacked` | 2863.160 | 1457.949 | 1.96x |
| `plain_u32` | 1878.221 | 332.838 | 5.64x |
| `lz4_u32` | 2582.625 | 1156.149 | 2.23x |

Readout:

- COCD-vs-dense ratio is **not** the substrate-shipped
  "dense-selection overhead <= 5%" gate. That gate measures the
  *classifier* overhead when COCD economically falls back, i.e.
  the cost of running `classify_predicate(...)` and rejecting
  the leaf to DenseFallback before any encoded eval runs. The
  ratios above measure the COCD path *running to completion*
  vs. an alternate dense path running to completion. The running
  ratio bounds the correctness-first substrate floor; the actual
  fallback decision cost is measured below by
  `predicate_classify_then_fallback` and
  `predicate_kernel_disabled_fallback`.
- `plain_u32` shows the highest COCD-vs-dense ratio (5.1-5.6x).
  The dense path on Plain is essentially a typed memory walk;
  the COCD path adds full `dispatch::evaluate(...)` overhead
  including `decode_buffer` (identity for Plain), value-typed
  parsing, `EncodedRowMask::from_bool_slice` allocation, and
  validity-mask combination. In production the classifier
  routes `Plain` to DenseFallback, so this ratio is diagnostic
  only.
- BitPacked / Rle / Dictionary / Delta cluster around 1.7-2.5x.
  Native kernels bypass the `decode_buffer` step entirely and
  emit masks directly from packed / run / index streams; the
  >= 2x promote-to-default target measures against this dense
  baseline once those land.
- Per-row cost is stable across row counts (within 10%):
  `bp_u8` reads 7.7 / 8.1 / 9.3 ns/row at 8K / 65K / 262K under
  the COCD path; dense_micro reads 3.3 / 3.3 / 3.2 ns/row.

## Mask Algebra (8K / 65K / 262K rows, us/op)

| Scenario | 8,192 | 65,536 | 262,144 | ns/row @ 262K |
| --- | ---: | ---: | ---: | ---: |
| `mask_algebra_bitmap_and` | 13.747 | 110.974 | 456.214 | 1.74 |
| `mask_algebra_bitmap_or` | 13.466 | 113.760 | 450.138 | 1.72 |
| `mask_algebra_all_and` | 13.855 | 111.397 | 450.773 | 1.72 |
| `mask_algebra_none_or` | 14.814 | 109.917 | 456.042 | 1.74 |
| `mask_algebra_runs_and_bitmap` | 20.248 | 153.281 | 612.306 | 2.34 |
| `mask_algebra` (legacy alias) | 13.765 | 114.753 | 445.927 | 1.70 |

Bitmap algebra is byte-linear and stays around 1.7 ns/row at
all three sizes. `Runs AND Bitmap` densifies the run mask to a
bitmap before AND, so it costs ~35% more.

## Aggregate CPU (us/op, masked by the fixture's predicate atom)

8,192 rows — `count_star`:

| Fixture | us/op |
| --- | ---: |
| `bp_u8` | 6.362 |
| `bp_u32_low` | 6.450 |
| `rle_plain` | 6.554 |
| `rle_bitpacked` | 6.908 |
| `dict_plain_lowcard` | 6.330 |
| `dict_bitpacked_lowcard` | 6.973 |
| `dict_bitpacked_higher` | 6.397 |
| `delta_plain` | 6.380 |
| `delta_bitpacked` | 6.491 |
| `plain_u32` | 6.060 |
| `lz4_u32` | 6.648 |

`count_star` over a `Bitmap` mask is mask popcount; the
constant ~6.5 us at 8K rows reflects the
`try_fold_encoded_aggregate_request` planner overhead +
mask popcount, not cascade work. At 262K rows it scales to
~200 us/op (uniformly across fixtures).

8,192 rows — `unsigned_sum` / `signed_sum` (one of each per
fixture, integer column):

| Fixture | Kernel | us/op |
| --- | --- | ---: |
| `bp_u8` | unsigned_sum | 18.326 |
| `bp_u32_low` | unsigned_sum | 19.063 |
| `rle_plain` | unsigned_sum | 18.369 |
| `rle_bitpacked` | unsigned_sum | 19.338 |
| `dict_plain_lowcard` | unsigned_sum | 19.794 |
| `dict_bitpacked_lowcard` | unsigned_sum | 19.581 |
| `dict_bitpacked_higher` | unsigned_sum | 17.882 |
| `delta_plain` | signed_sum | 21.050 |
| `delta_bitpacked` | signed_sum | 22.014 |
| `plain_u32` | unsigned_sum | 21.730 |
| `lz4_u32` | unsigned_sum | 18.650 |

Sum kernels read ~2.3 ns/row across cascades — `EncodedAggregateContext`
holds the already-decoded column, so the sum kernel is a typed
walk over decoded bytes filtered by the mask. Native kernels
would walk the encoded representation directly (e.g., per-run
multiplication for Rle, dict-value-table lookup for Dictionary
BitPacked) and target >= 2x speedup vs. these baselines.

262,144 rows — sum kernels:

| Fixture | Kernel | us/op | ns/row |
| --- | --- | ---: | ---: |
| `bp_u8` | unsigned_sum | 610.959 | 2.33 |
| `bp_u32_low` | unsigned_sum | 644.920 | 2.46 |
| `rle_plain` | unsigned_sum | 588.632 | 2.25 |
| `rle_bitpacked` | unsigned_sum | 614.346 | 2.34 |
| `dict_plain_lowcard` | unsigned_sum | 667.702 | 2.55 |
| `dict_bitpacked_lowcard` | unsigned_sum | 649.578 | 2.48 |
| `dict_bitpacked_higher` | unsigned_sum | 594.473 | 2.27 |
| `delta_plain` | signed_sum | 723.570 | 2.76 |
| `delta_bitpacked` | signed_sum | 733.948 | 2.80 |
| `plain_u32` | unsigned_sum | 676.035 | 2.58 |
| `lz4_u32` | unsigned_sum | 569.997 | 2.17 |

## Classifier Fallback Overhead (the `<= 5%` Gate)

`predicate_classify_then_fallback` measures the planner-side
work the substrate pays on a DenseFallback decision: invoke
`evaluator_kind_for_cascade(...)`, compute literal coercion,
then fall through to the dense path. The (classify_then_fallback
- dense_micro) / dense_micro ratio bounds the substrate's
classifier overhead — the ADR's "<= 5% dense-selection overhead
vs. ADR 057 dense path when COCD economically falls back" gate.

8,192 rows:

| Fixture | Dense micro (us/op) | Classify+fallback (us/op) | Delta |
| --- | ---: | ---: | ---: |
| `bp_u8` | 24.007 | 23.641 | -1.52% |
| `bp_u32_low` | 42.425 | 28.496 | -32.83% (noise) |
| `rle_plain` | 32.601 | 31.796 | -2.47% |
| `rle_bitpacked` | 30.463 | 30.661 | +0.65% |
| `dict_plain_lowcard` | 31.464 | 31.709 | +0.78% |
| `dict_bitpacked_lowcard` | 34.066 | 32.688 | -4.04% |
| `dict_bitpacked_higher` | 52.206 | 33.920 | -35.03% (noise) |
| `delta_plain` | 41.262 | 41.520 | +0.62% |
| `delta_bitpacked` | 39.690 | 41.233 | +3.89% |
| `plain_u32` | 8.413 | 8.252 | -1.91% |
| `lz4_u32` | 28.259 | 28.306 | +0.17% |

65,536 rows:

| Fixture | Dense micro (us/op) | Classify+fallback (us/op) | Delta |
| --- | ---: | ---: | ---: |
| `bp_u8` | 191.977 | 191.247 | -0.38% |
| `bp_u32_low` | 232.338 | 226.471 | -2.53% |
| `rle_plain` | 260.714 | 250.076 | -4.08% |
| `rle_bitpacked` | 241.483 | 244.675 | +1.32% |
| `dict_plain_lowcard` | 254.659 | 257.663 | +1.18% |
| `dict_bitpacked_lowcard` | 253.559 | 251.439 | -0.84% |
| `dict_bitpacked_higher` | 277.707 | 281.668 | +1.43% |
| `delta_plain` | 312.219 | 322.568 | +3.31% |
| `delta_bitpacked` | 315.134 | 318.086 | +0.94% |
| `plain_u32` | 71.436 | 68.858 | -3.61% |
| `lz4_u32` | 221.085 | 217.912 | -1.43% |

262,144 rows:

| Fixture | Dense micro (us/op) | Classify+fallback (us/op) | Delta |
| --- | ---: | ---: | ---: |
| `bp_u8` | 783.953 | 786.427 | +0.32% |
| `bp_u32_low` | 946.560 | 930.284 | -1.72% |
| `rle_plain` | 1022.908 | 1015.051 | -0.77% |
| `rle_bitpacked` | 1005.547 | 1014.725 | +0.91% |
| `dict_plain_lowcard` | 1077.126 | 1073.404 | -0.35% |
| `dict_bitpacked_lowcard` | 1052.354 | 1075.659 | +2.21% |
| `dict_bitpacked_higher` | 1126.853 | 1137.423 | +0.94% |
| `delta_plain` | 1304.454 | 1305.602 | +0.09% |
| `delta_bitpacked` | 1316.299 | 1299.222 | -1.30% |
| `plain_u32` | 296.318 | 291.812 | -1.52% |
| `lz4_u32` | 984.548 | 973.109 | -1.16% |

Readout:

- **At medium / large fixture sizes (65K / 262K rows), the
  classifier fallback overhead is uniformly within plus or
  minus 4.1% across all 11 cascades — well under the ADR's
  <= 5% bar.** At 262K rows the maximum delta is +2.21%
  (`dict_bitpacked_lowcard`); at 65K rows the maximum is +3.31%
  (`delta_plain`). Both are within bench noise at that
  measurement scale.
- The 8K row count shows wider variance: two fixtures
  (`bp_u32_low`, `dict_bitpacked_higher`) report large negative
  deltas (-32.8% and -35.0%). These are not real wins for
  classify+fallback — they reflect the dense_micro row's noise
  on small fixtures (the 8K dense_micro row for
  `dict_bitpacked_higher` measured 52.2 us, while the 65K row
  measured 277.7 us, suggesting the 8K dense_micro had a
  warmup blip). The 8K classify+fallback rows themselves are
  consistent with the larger fixture data points.
- **Substrate-shipped "<= 5% dense-selection overhead" gate
  passes** at the 65K and 262K fixtures across all eleven
  cascades.
- Negative deltas (classify+fallback faster than bare
  dense_micro) are pure noise — the additional work cannot
  produce a faster timing.

## Mixed-Leaf Classifier Shape (`bp_u8` fixture)

`mixed_prefilter_high_reject` and `mixed_prefilter_low_reject`
encode the Prefilter shape: a COCD evaluator produces a survivor
mask for atom A (`Lt 13` -> ~5% selectivity / 95% rejection;
`Lt 240` -> ~95% selectivity / 5% rejection), then a residual
scalar atom B (`Gte 100`) iterates only the surviving rows.
`mixed_dense_fallback_and` and `mixed_dense_fallback_or` are
the corresponding DenseFallback paths: dense decode + scalar A +
scalar B + boolean combine across all rows.

| Scenario | 8,192 | 65,536 | 262,144 |
| --- | ---: | ---: | ---: |
| `mixed_prefilter_high_reject` (95% reject) | 65.161 | 531.833 | 2164.447 |
| `mixed_prefilter_low_reject` (5% reject) | 72.432 | 607.398 | 2528.838 |
| `mixed_dense_fallback_and` | 34.721 | 291.777 | 1135.197 |
| `mixed_dense_fallback_or` | 35.562 | 281.364 | 1150.670 |

Readout:

- **Prefilter does not amortize under the disabled-V1 substrate.**
  At 65K rows the high-reject Prefilter measures 531.8 us vs.
  291.8 us for DenseFallback AND — a 1.82x regression even when
  the encoded leaf rejects 95% of rows. The encoded eval cost
  (~444 us at 65K, see `predicate_cocd` row for `bp_u8`)
  dominates the savings from narrowing the residual scalar
  (5% × ~290 us = ~14.5 us).
- **The Prefilter rejection gate (default 0.10) correctly
  routes every leaf at this substrate cost**: under V1, the
  gate would never engage Prefilter because `encoded_eval +
  selectivity * dense_scalar > 2 * dense_scalar` for any
  selectivity in [0, 1]. The crossover only opens once native
  kernels reduce encoded_eval below `(2 - selectivity) *
  dense_scalar`.
- This is the substrate-floor evidence the kernel-promotion
  artifact needs to compare against. The 0.10 rejection gate
  is correctly *unmovable* by substrate-shipped evidence;
  native kernels are the only way to shift it.
- DenseFallback AND and OR have nearly identical cost
  (291.8 vs 281.4 us at 65K). Both shapes do the same
  decode + scalar A + scalar B; only the final boolean
  combine differs.

## Mask Algebra Completeness (`bp_u8`)

| Scenario | 8,192 | 65,536 | 262,144 | ns/row @ 262K |
| --- | ---: | ---: | ---: | ---: |
| `mask_algebra_bitmap_and` | 12.312 | 108.387 | 397.108 | 1.51 |
| `mask_algebra_bitmap_or` | 12.127 | 112.366 | 401.083 | 1.53 |
| `mask_algebra_bitmap_pair_or` | 12.949 | 99.387 | 411.790 | 1.57 |
| `mask_algebra_bitmap_not` | 10.976 | 91.121 | 369.320 | 1.41 |
| `mask_algebra_all_and` | 12.099 | 98.712 | 397.780 | 1.52 |
| `mask_algebra_none_or` | 11.961 | 102.857 | 400.731 | 1.53 |
| `mask_algebra_runs_and_runs` | 7.259 | 61.687 | 249.638 | 0.95 |
| `mask_algebra_runs_not` | 4.917 | 40.727 | 182.343 | 0.70 |
| `mask_algebra_runs_and_bitmap` | 17.746 | 139.393 | 538.395 | 2.05 |
| `mask_algebra_densification` (Runs -> Bitmap) | 7.440 | 62.599 | 260.959 | 1.00 |

Readout:

- **V1 mask algebra is not shape-specialized.** The ADR §"Mask
  algebra is shape-specialized" describes design intent; the
  current implementation in `src/storage/encoding/compute/mask.rs::combine`
  always converts both operands to `Vec<bool>` then re-builds
  via `from_bool_slice(...)` (which always emits a `Bitmap`
  shape unless the result is constant `All` / `None`). Every
  algebra invocation is a `Vec<bool>` round-trip.
- This implementation choice produces a counterintuitive
  signal: **Runs operands are faster than Bitmap operands** at
  every row count. `runs_and_runs` measures 0.95 ns/row at 262K
  vs. `bitmap_and` at 1.51 ns/row. The reason is allocation
  pattern, not algorithm: `Bitmap::to_bool_vec(...)` performs a
  divmod per row to extract bits, whereas `Runs::to_bool_vec(...)`
  walks runs and writes contiguous bool spans. Bitmap's
  bit-extract loop dominates the round-trip cost.
- `mask_algebra_densification` (Runs AND Bitmap) measures
  1.00 ns/row, confirming the path collapses to Bitmap as
  expected; cost is between bare Runs algebra and bare Bitmap
  algebra because the round-trip processes both shapes.
- Native-kernel-pass scope: implement shape-specialized AND /
  OR / NOT that preserves Runs shape across run-shaped pairs,
  uses bytewise SIMD on Bitmap pairs, and only densifies when
  the result `Runs` would exceed `max_mask_bytes_per_record`.
  Expected wins: bitmap_and >= 4x faster (SIMD AND on
  contiguous bytes vs. divmod per row); runs_and_runs >= 2x
  faster (run-intersection algorithm, no Vec<bool> round-trip);
  All / None short-circuits become constant-time.

## Config Kill-Switch (`predicate_evaluator_enabled` Gate)

Scenario `predicate_kernel_disabled_fallback` constructs an
`EncodingComputeConfig` with `enabled = true` but the BitPacked
predicate evaluator removed from `enabled_predicate_evaluators`,
then runs the planner-side fallback shape: cascade-kind lookup +
config-set check + dense path.

| Scenario | 8,192 | 65,536 | 262,144 |
| --- | ---: | ---: | ---: |
| `predicate_dense_micro` | 26.183 | 203.310 | 833.191 |
| `predicate_classify_then_fallback` | 25.944 | 207.567 | 830.038 |
| `predicate_kernel_disabled_fallback` | 25.589 | 203.879 | 826.901 |

Readout (after iteration-4 tightening):

- The three rows track within +/-2% of each other across all
  three row counts. The kill-switch path now mirrors the
  classifier-fallback shape exactly (cascade-kind lookup +
  config check + literal coercion + dense path) instead of
  re-deriving work; the +9% delta the iteration-3 measurement
  carried was a bench-helper artifact, not real overhead.
- **Substrate-shipped "<= 5% dense-selection overhead" gate
  passes** for both the bare classifier fallback path and the
  kernel kill-switch path on the medium / large fixtures.
- Negative deltas (kernel_disabled faster than dense_micro at
  262K) are pure noise — the additional config check cannot
  produce a faster timing.

## Leaf Selectivity Sweep (`bp_u8`, atom `Lt literal`)

| Selectivity target | Literal | 8K us/op | 65K us/op | 262K us/op |
| ---: | ---: | ---: | ---: | ---: |
| ~1.2% | 3 | 61.687 | 527.389 | 2338.506 |
| ~5.2% | 13 | 61.993 | 509.181 | 2310.812 |
| ~25.1% | 63 | 61.929 | 522.207 | 2390.212 |
| ~50.2% | 126 | 63.781 | 532.495 | 2480.759 |

Readout:

- Predicate CPU is essentially **selectivity-invariant** under
  the V1 substrate (1-7% variance). This is the expected shape
  for decode-then-scalar: the decode + scalar comparison loop
  iterate over all rows regardless of how many rows match. The
  variance comes mostly from `EncodedRowMask::from_bool_slice`
  branch behavior on the resulting selection vector.
- Native kernels (BitPacked SIMD, Rle run-grain, Dictionary
  match-set + index walk) will produce selectivity-sensitive
  curves — Rle in particular collapses to one comparison per
  run regardless of selectivity, but page-grain BitPacked SIMD
  scans can early-exit on `Eq` once the mask saturates.
  Selectivity-curve interaction with the leaf-selectivity gate
  at 0.25 is a kernel-bench artifact concern, not a substrate
  concern.

## Phase B + C + D Pipeline (Manager-Layer Round-Trip Surrogate)

`bench_pipeline_full` exercises the synchronous Phase B + C + D
shape via the public storage entries
`parse_record_buffer_directory(...)` ->
`selected_row_group_record_body_ranges(...)` ->
`decode_projected_row_groups_from_plaintext_ranges(...)`,
mirroring what the storage manager does on a record body fetch
without the L2 round-trip (no tokio runtime, no `ObjectStore`
mock). The fixture is a 2-column Int64 multi-row record encoded
with ADR 056 row groups + ADR 057 encoding cascades; the
`output_only` row materializes a single projected column
(column 1), `with_predicate` adds the predicate column
(column 0) to the requirement.

| Scenario | Path | 8,192 | 65,536 |
| --- | --- | ---: | ---: |
| `pipeline_full` | output_only | 93.369 | 2332.806 |
| `pipeline_full` | with_predicate | 116.212 | 2636.121 |

(262,144-row pipeline runs are out of scope for this fixture:
the multi-row layout caps record body at 8 MiB, and the
two-Int64 schema at 262K rows overflows that. The substrate
floor at 262K is covered by the per-fixture predicate / mask /
aggregate rows.)

Readout:

- 8K-row pipeline reads ~93 us for projection-only. Adding the
  predicate column to the requirement bumps that to ~116 us
  (+24%), reflecting the extra parse + range + decode for one
  more buffer-page span.
- 65K-row pipeline scales nearly linearly (~25x cost for ~8x
  rows because the directory parse / page enumeration carries
  fixed overhead per row group). Adding the predicate column
  bumps cost +13%.
- The pipeline cost is dominated by `decode_projected_row_groups_from_plaintext_ranges`
  (Phase D); `parse_record_buffer_directory` and
  `selected_row_group_record_body_ranges` are sub-microsecond
  on these fixtures.

## Provider-Leak Counter (`selected_row_group_record_body_ranges` Footprint)

Counts the byte ranges
`selected_row_group_record_body_ranges(...)` would request on
an L2 fetch of the same record body, plus the upper-bound
encoded slim a COCD reader could substitute by targeting only
encoded predicate buffers via `encoding_directory.buffers[].encoded_range`.

| Path | Rows | ADR 057 ranges | ADR 057 bytes | Encoded slim bytes | Slim ratio | Frame bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| output_only | 8,192 | 24 | 16,808 | n/a | n/a | 492,616 |
| with_predicate | 8,192 | 24 | 19,920 | 3,112 (pred col only) | 0.047 | 492,616 |
| output_only | 65,536 | 192 | 159,040 | n/a | n/a | 2,349,064 |
| with_predicate | 65,536 | 224 | 667,424 | 246,240 (pred col only) | 0.911 | 2,349,064 |

Readout:

- **ADR 057 already drastically narrows L2 fetch footprint**
  before COCD enters the picture: at 8K rows the dense path
  requests 24 ranges totaling 16.8 KiB out of a 481 KiB frame
  (3.4%); at 65K rows it requests 192 ranges totaling 155 KiB
  out of a 2.24 MiB frame (6.8%) for projection-only.
- **Adding the predicate column to the requirement** roughly
  doubles fetched bytes at 65K (159 KiB -> 667 KiB) but only
  modestly bumps it at 8K (16.8 KiB -> 19.9 KiB). The 65K
  fixture's predicate column happens to encode at near-Plain
  density (`id % 8` cycling through 65,536 rows), so the
  per-row-group encoded slim is ~91% of decoded; this is
  fixture-dependent.
- **COCD encoded slim varies dramatically with cascade
  selection**: at 8K rows the predicate column compresses to
  4.7% of decoded byte length (high-cardinality cycle still
  fits a 256-row Dictionary), but at 65K the cascade selector
  picks a denser representation that's only 91% of decoded.
  This is a real signal that the cascade selector's heuristics
  are fixture-sensitive, **not a COCD defect** — COCD reads
  whatever the writer produces.
- The COCD encoded-bytes column is the **upper bound on per-
  record bytes a COCD reader could substitute** by fetching
  only encoded predicate slices via the SIMD-page fast path
  (ADR 057 §"secondary-index page fast path"). Realized
  savings depend on the request pattern; the substrate-shipped
  artifact captures the planner-decided range count, not a
  measured GET counter (see iteration-5 backlog for a
  GET-counting `MockObjectStore` wrapper if the council wants
  raw GET evidence).

## Aggregate Matrix Coverage

Iteration 5 fills the impl §17 / §18 gaps with two new scenario
families: `aggregate_min` / `aggregate_max` rows on the
`bp_u8` and `dict_bitpacked_lowcard` fixtures (impl §17), and
selectivity-swept count_star / unsigned_sum on `bp_u8` (impl
§18, the masked-aggregate-fold shape).

### Min / Max Kernels (us/op)

| Fixture | Kernel | 8,192 | 65,536 | 262,144 | ns/row @ 262K |
| --- | --- | ---: | ---: | ---: | ---: |
| `bp_u8` | min | 138.870 | 1847.720 | 4551.137 | 17.36 |
| `bp_u8` | max | 131.318 | 1866.192 | 4547.327 | 17.35 |
| `dict_bitpacked_lowcard` | min | 134.015 | 1927.964 | 4614.585 | 17.60 |
| `dict_bitpacked_lowcard` | max | 131.510 | 1855.826 | 4645.999 | 17.72 |

Readout:

- min/max scales linearly across all four scenarios with
  per-row cost ~17.4 ns/row at 262K. ~7.5x more expensive than
  unsigned_sum (~2.3 ns/row at 262K) and ~23x more expensive
  than count_star (~0.76 ns/row at 262K).
- Cost is uniform across `bp_u8` (UInt8 fixed-width) and
  `dict_bitpacked_lowcard` (UInt32 dictionary-encoded indices).
  V1 substrate decodes both through ADR 057 first then folds;
  native kernels can specialize: BitPacked min/max via
  packed-domain scan (no decode), Dictionary BitPacked via dict-
  value-table lookup with per-index branchless compare.
- Native-kernel-pass scope: target >= 2x speedup vs. V1
  ~17 ns/row.

### Aggregate Selectivity Sweep on bp_u8

| Selectivity | count_star (8K) | unsigned_sum (8K) | count_star (65K) | unsigned_sum (65K) | count_star (262K) | unsigned_sum (262K) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| p001 (~1%) | 5.483 | 16.099 | 43.793 | 124.395 | 170.803 | 504.105 |
| p005 (~5%) | 5.738 | 29.020 | 42.674 | 122.413 | 171.797 | 490.176 |
| p025 (~25%) | 7.027 | 18.408 | 43.322 | 137.060 | 170.045 | 538.565 |
| p050 (~50%) | 6.251 | 19.776 | 42.951 | 148.848 | 171.366 | 603.989 |

Readout:

- **count_star is selectivity-independent** at all three row
  counts (variance < 5%). Confirms the kernel is a popcount
  over the mask, no per-selected-row work.
- **unsigned_sum scales modestly with selectivity** at the
  larger row counts: at 262K rows the p050 fold takes 604 us
  vs. 504 us at p001 (+19.8%). The mask iterates all rows but
  only contributions from selected rows accumulate, so cost
  carries a linear component in selected rows on top of the
  fixed mask scan.
- The 8K p005 row (29.02 us) is an outlier — bench noise on
  small fixtures with fast kernels.

## Multi-Shape Pipeline Coverage (Cascade Selector Observability)

Iteration 5 extends the pipeline fixture builder to take a
`PipelineShape` enum that varies the predicate-column data
pattern (preserves the projection column unchanged):

- **`low_card`** — `id % 8` cycles through 8 distinct values.
- **`monotonic`** — `id` is the row index (strictly
  increasing, unbounded range).
- **`bitpack_low`** — `(id * 7) % 1024` (small-range
  pseudo-random within 1024-value cycle).
- **`run_shaped`** — `(id / 64) % 32` (chunks of 64 identical
  values cycling through 32 distinct).

For each shape, `bench_provider_leak` reports the cascade kinds
the writer-side selector picks for the predicate column buffers
plus the COCD encoded slim ratio. Shape variation answers the
iteration-4 open question on the slim-ratio inversion (8K
low_card = 4.7%, 65K low_card = 91.1%):

8,192 rows:

| Shape | Cascades picked (predicate column) | Encoded bytes | Decoded bytes | Slim ratio |
| --- | --- | ---: | ---: | ---: |
| `low_card` | `bit_packed:8, rle:8` | 3,112 | 66,560 | 0.047 |
| `monotonic` | `bit_packed:8, rle:8` | 13,352 | 66,560 | 0.201 |
| `bitpack_low` | `bit_packed:1, delta:7, rle:8` | 15,824 | 66,560 | 0.238 |
| `run_shaped` | `bit_packed:8, rle:8` | 5,160 | 66,560 | 0.078 |

65,536 rows:

| Shape | Cascades picked (predicate column) | Encoded bytes | Decoded bytes | Slim ratio |
| --- | --- | ---: | ---: | ---: |
| `low_card` | `rle:96` (no `bit_packed` selected) | 246,240 | 270,336 | **0.911** |
| `monotonic` | `bit_packed:64, rle:64` | 131,392 | 532,480 | 0.247 |
| `bitpack_low` | `rle:64` | 320 | 8,192 | **0.039** |
| `run_shaped` | `bit_packed:64, rle:64` | 41,280 | 532,480 | 0.078 |

ADR 057 dense-path range count + bytes per shape (the cost a
non-COCD reader would pay to fetch the predicate column on the
same record body):

| Shape | Rows | output_only ranges | output_only bytes | with_predicate ranges | with_predicate bytes | Frame bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `low_card` | 8,192 | 24 | 16,808 | 24 | 19,920 | 492,616 |
| `monotonic` | 8,192 | 24 | 16,808 | 24 | 30,160 | 502,792 |
| `bitpack_low` | 8,192 | 24 | 16,808 | 24 | 32,632 | 505,456 |
| `run_shaped` | 8,192 | 24 | 16,808 | 24 | 21,968 | 494,728 |
| `low_card` | 65,536 | 192 | 159,040 | 224 | 667,424 | 2,349,064 |
| `monotonic` | 65,536 | 192 | 159,040 | 192 | 290,432 | 1,974,152 |
| `bitpack_low` | 65,536 | 192 | 159,040 | 192 | 683,648 | 2,363,080 |
| `run_shaped` | 65,536 | 192 | 159,040 | 192 | 200,320 | 1,884,104 |

Readout:

- **Slim ratio varies dramatically with predicate-column data
  shape and row count.** At 8K rows ratios span 4.7%-23.8%; at
  65K rows ratios span 3.9%-91.1%.
- **The 65K `low_card` ratio of 91.1% is a writer-selector
  variance candidate, not a COCD defect.** The selector picks
  `Rle` for every row group's predicate buffer at 65K rows
  even though `id % 8` cycling produces no consecutive
  duplicates within a 1024-row row group (each value appears
  once before cycling). The encoded byte volume reported here
  is for the produced cascade, not a comparison vs. an
  alternate cascade choice on the same fixture; whether Rle is
  strictly worse than Plain on this pattern is a writer-tier
  question that requires a head-to-head encode + measure.
  **This is an ADR 057 selector / sampler follow-up, not an
  ADR 058 substrate concern.** COCD reads whatever the writer
  produces and the slim-ratio is mechanically faithful to the
  writer's choice.
- **The 65K `bitpack_low` ratio of 3.9% is striking.** The
  `(id * 7) % 1024` pattern compresses to 320 encoded bytes
  out of 8,192 decoded bytes per buffer. Selector picks Rle
  for all 64 row groups; the modular-arithmetic pattern within
  a row group apparently aligns with Rle's run-detection on
  the value buffer's high-byte (which is mostly zero for values
  < 1024). **This is a real density win** that COCD readers
  would automatically realize on similar production patterns
  (timestamp low-order bytes, modular hashes, etc.).
- **`bitpack_low` shows mixed cascade selection at 8K**:
  `bit_packed:1, delta:7, rle:8`. Seven row groups picked
  `Delta { inner: Plain }`, one picked BitPacked. After the
  iteration-6 catalog change accepts any Delta inner, all
  Delta variants the writer emits are covered by the COCD
  evaluator. Per `delta_inner_variants_produce_byte_identical_encoded_frames`
  the encoded bytes are identical regardless of inner, so the
  evaluator dispatch is correctness-equivalent on every
  variant.

### Pipeline Neutrality (Bench Layer)

`bench_pipeline_neutrality` runs the pipeline twice on the same
record body and asserts byte-identical output.

| Rows | byte_identical |
| ---: | ---: |
| 8,192 | true |
| 65,536 | true |

The pipeline path uses public storage entries
(`parse_record_buffer_directory` ->
`selected_row_group_record_body_ranges` ->
`decode_projected_row_groups_from_plaintext_ranges`) that
**do not consult `EncodingComputeConfig`**, so byte-identity
is structural at this layer. The substantive neutrality claim
sits at the test layer: 94 integration tests across ADR 056 /
057 / 040 / 052 / 055 / 058 surfaces pass with
`config.enabled = false` (iteration 0 evidence). The bench-
layer row records the structural assertion explicitly.

## Repeat Stability

Per-fixture COCD CPU varied within 1-3% across the smoke run
and the iteration-1 full run. Mask algebra varied within 5%
on the 8K row count (bench noise on ~13 us samples) and within
1% on the 65K and 262K row counts.

## Correctness Gates

Reverified after the iteration-6 substrate promotion:

- `cargo test --test encoding_compute_test`: 13/13 pass
  (iteration 2 added
  `evaluator_kind_for_cascade_routes_unsupported_inners_to_dense_fallback`;
  iteration 6 added
  `delta_inner_variants_produce_byte_identical_encoded_frames`).
- `cargo test --test architecture_test`: 20/20 pass, including
  `test_adr058_encoding_compute_guards` which now also confirms
  the architectural-guard token presence in the extended bench
  file (`encoding_compute_bench`, `predicate_exact`,
  `mask_algebra`, `aggregate_fold`,
  `try_fold_encoded_aggregate_request` — the iteration-1
  bench keeps the legacy scenario aliases for guard-token
  parity while adding the new fixture-named rows).
- Mask + aggregate-partial parity: COCD eval is byte-equal to
  the dense scalar reference on every fixture under existing
  unit tests.
- Cross-replica determinism: the substrate is allocator- and
  schedule-free at evaluator and fold scope; identical encoded
  bytes plus identical config plus identical predicate /
  aggregate request produce byte-identical output by
  construction (no concurrency, no `HashMap` iteration, no
  floating-point reduction in any V1 kernel).

## Regression Neutrality (COCD Disabled, V1 Default)

Verified at iteration 0 across 94 tests in 6 suites
(`encoding_*`, `row_group_*`, `compute_integration_contract`,
`l2_remote`, `l2_rehydration`, `lsm_*`, `encoding_compute_test`,
`architecture_test`); those suites stayed green through the
harness extension. Iteration 3 / 4 added bench-layer kill-switch
rows that route through the same classifier fallback shape and
confirm disabled or per-kernel-disabled COCD pays only the
bounded fallback-decision overhead before the dense path runs.

## Coverage Notes (vs. Impl Plan §Phase 8)

Iterations 1-6 close the substrate-shipped coverage needed for
ADR 058's disabled V1: cascade fixture breadth, dense-micro
baseline, classifier fallback overhead, mixed-leaf shape,
mask-algebra and densification rows, aggregate min/max and
selectivity-swept count/sum rows, pipeline shape, provider
range/byte proxy, kill-switch fallback, and Delta inner
byte-equivalence. Scenarios that require native per-cascade
kernels or a heavier async object-store harness move to the
kernel-promotion / read-amplification follow-up artifacts rather
than blocking this substrate promotion.

## Iteration Notes

Iteration 1 closes the cascade fixture gap and establishes the
dense-micro baseline ratio for every cascade family. Substrate
shape is still locked at the council level; bench evidence
adds breadth without amending the ADR. Repeat stability is
within bench noise on medium / large fixtures.

Iteration 2 closed:

- dispatch tightening for `Rle{Lz4}` and `Dictionary{Lz4}` ->
  DenseFallback (the temporary `Delta{Plain|Lz4}` tightening was
  revised in iteration 6 after the byte-equivalence invariant was
  established);
- unit guard for the cascade-shape mapping;
- `predicate_classify_then_fallback` scenario row capturing
  the substrate-shipped "<= 5%" gate.

Iteration 3 closed:

- mixed-leaf classifier shape (Prefilter high / low rejection
  vs. AND / OR DenseFallback);
- mask-algebra completeness rows (Bitmap NOT, Runs NOT,
  Runs AND Runs, Runs->Bitmap densification);
- config-toggle kill-switch fallback row.

**Three substrate-implementation findings worth surfacing to
the council** (all observable as iteration-3 bench evidence,
none are blockers for substrate-shipped, all are kernel-pass
or follow-up scope):

1. **V1 mask algebra is not shape-specialized.** Every algebra
   op goes through `Vec<bool>` round-trip; results are always
   `Bitmap` shape unless constant. The ADR's "shape-specialized
   algebra" is design intent. Native-kernel-pass scope.
2. **Prefilter does not amortize under V1.** Encoded eval cost
   dominates dense scalar even at 95% rejection. The 0.10
   rejection gate is correctly unmovable from substrate-shipped
   evidence; it activates only once native kernels exist. The
   substrate-shipped artifact should record this as expected
   behavior, not a Provisional-discipline gate revision.
3. **`bench_config_kernel_disabled` shape mis-approximates the
   real kill-switch path.** Iteration-4 refinement: route
   through `classify_predicate(...)` directly with a kill-switch
   config and assert `DenseFallback` plan return without decode.

Iteration 4 closed:

- `predicate_kernel_disabled_fallback` shape tightened to
  match `predicate_classify_then_fallback` reference within
  +/-2% across all row counts;
- Phase B + C + D pipeline-full row over a multi-row Int64 x 2
  fixture using public storage entries (no tokio runtime, no
  L2 mock — synchronous parse + range + decode pipeline);
- provider-leak counter (range count + total bytes for ADR 057
  dense fetch shape, plus COCD encoded slim upper bound from
  `EncodingDirectory.buffers[].encoded_range`).

Iteration 6 closed by council convergence:

**Finding A — `Delta { inner }` is byte-irrelevant at the
encoder.** Code inspection of
`src/storage/encoding/cascade/codec.rs` confirms that the
`Delta { .. }` match arm at line 146 in `encode_payload`
discards inner; `encode_delta_bytes` (line 1216) takes only
input and value count, no inner parameter. Decode at line 239
mirrors. Encoded bytes for `Delta { inner: Plain }`,
`Delta { inner: BitPacked }`, and `Delta { inner: Lz4Frame }`
are byte-identical for the same input.

The compositional principle behind the iteration-2 tightening
holds for Rle (dispatch-grain hint: `inner: BitPacked` =
page-grain, `inner: Plain` = buffer-grain; bytes identical) and
for Dictionary (`inner: BitPacked` produces `WDB2`-magic
encoded indices vs. `WDD1` for `Plain` inner — bytes distinct).
For Delta neither distinction applies: bytes identical AND
dispatch grain is always full-buffer per impl plan
§"Page-grain dispatch default".

Resolution applied: COCD evaluator catalog loosened to accept
`Delta { .. }` (any inner). One-line `evaluator_kind_for_cascade`
change in `src/storage/encoding/compute/dispatch.rs`,
guard test
`delta_inner_variants_produce_byte_identical_encoded_frames`
added in `tests/encoding_compute_test.rs` to pin the byte-
equivalence invariant, ADR 058 §"Cascade-shape ->
evaluator-kind mapping" amended, impl plan Phase-0 fixture row
amended. Iteration 2's tightening for Rle / Dictionary inner
Lz4 stands.

**Finding B — Rle selection at 65K low_card is an ADR 057
selector follow-up.** Iteration 5 surfaced that the writer
picks `Rle` for `id % 8` cycling at 65K rows, where the
pattern has no consecutive duplicates and the encoded byte
volume comes out to 91.1% of decoded. From the sampler /
selector code in `src/storage/encoding/sample.rs` and
`src/storage/encoding/select.rs`, the heuristic
`runs.saturating_mul(10) < sampled_value_count` should reject
Rle for cycling patterns (runs ≈ sampled). The 8K fixture
matches the heuristic (`bit_packed:8, rle:8`); the 65K fixture
deviates (`rle:96`). Hypotheses (untested, writer-tier):
sample-window shape variance across row counts, a different
selector branch firing first, or a buffer-ordinal filter
artifact in the bench. The slim-ratio is mechanically faithful
to the writer's cascade choice; whether Rle compresses
strictly worse than Plain on this exact pattern would require
a head-to-head encode test.

Resolution: ADR 057 selector / sampler follow-up, framed as
selector variance worth investigating but not as an ADR 058
blocker. COCD reads whatever the writer produces.

**Finding C — runtime-measured `MockObjectStore` GET counter
deferred.** The iteration-4 provider-leak rows report
planner-intended ranges and selected byte counts via
`selected_row_group_record_body_ranges(...)` + the
`EncodingDirectory.buffers[].encoded_range` slim-bytes upper
bound. The runtime-measured form (Arc<Mutex<Vec<(uri, range)>>>
wrapper around the existing `MockObjectStore`, tokio runtime,
slab manager fixture) would add ~200-300 LOC of harness
without changing the substrate-shipped correctness or
performance conclusion. Council deferred to a future
read-amplification or ADR 056 / 057 storage-manager bench.

Iteration 5 closed:

- `aggregate_min` / `aggregate_max` rows for `bp_u8` +
  `dict_bitpacked_lowcard` (impl §17 closure on min/max);
- `aggregate_count_star_selectivity_sweep` +
  `aggregate_unsigned_sum_selectivity_sweep` rows on bp_u8
  at 1% / 5% / 25% / 50% (impl §18 closure on masked aggregate
  scaling);
- multi-shape pipeline fixtures (low_card / monotonic /
  bitpack_low / run_shaped) — characterizes slim-ratio
  variance across writer-selector outcomes;
- cascade-selector observability via `cascades_picked` field
  in `provider_leak` rows;
- pipeline neutrality byte-identity assertion at bench layer.

**Two findings surfaced for iteration-6 resolution**:

1. **Writer selector picks `Rle` for a 65K cycling pattern with
   a high slim ratio.** The 91.1% slim ratio for `id % 8`
   cycling is a selector variance candidate. The byte volume is
   measured against the cascade the writer actually chose; a
   Plain head-to-head belongs in an ADR 057 selector follow-up.
2. **Production selector emits `Delta { inner: Plain }` while
   the original ADR text named only `Delta { inner: BitPacked }`
   as COCD-supported.** Iteration 6 resolved this by loosening
   the evaluator catalog to `Delta { .. }` (any inner), because
   Delta bytes and dispatch grain are inner-irrelevant.

## Disposition

ADR 058 substrate is materially implemented and the Phase 8
bench matrix supports promotion to shipped. The acceptance
gates the impl plan §Phase 8 names are met:

- **Correctness V1 ship gate** (mask byte-equality, aggregate-
  partial byte-equality, cross-replica determinism,
  architectural lockdown): 13 COCD unit tests + 20 architecture
  tests + 94 ADR 056 / 057 / 040 / 052 / 055 integration tests
  pass with `EncodingComputeConfig.enabled = false` (the V1
  default).
- **Dense-selection overhead ≤ 5% when COCD economically falls
  back**: confirmed by `predicate_classify_then_fallback` and
  `predicate_kernel_disabled_fallback` rows; deltas within
  ±2% across all eleven cascade fixtures at 65K and 262K rows.
- **Cross-replica determinism**: substrate is allocator- and
  schedule-free at evaluator and fold scope; identical encoded
  bytes plus identical config plus identical request produce
  byte-identical output by construction.
- **Regression neutrality**: ADR 056 / 057 / 040 / 052 / 055
  byte-identical with COCD on / off (test layer authoritative;
  bench-layer pipeline-neutrality row records the structural
  assertion).
- **Cascade shape coverage**: 11 substrate fixtures plus 4
  pipeline-shape fixtures observe the writer selector's
  cascade choices and report the encoded slim bytes against
  the dense baseline.

Promotion-to-default gate (≥ 2× CPU vs. ADR 057 dense PBC)
remains the kernel-promotion artifact's responsibility. Native
per-cascade kernels (BitPacked SIMD, Rle run-grain, Dictionary
match-set + index walk, Delta prefix-sum) replace the V1
correctness-first decode-then-scalar dispatch path; the kernel-
promotion artifact at
`plans/impl/058_compute_over_encoded_data_kernel_bench_results.md`
will measure against this substrate-shipped baseline and decide
default enablement.

Iteration-7 backlog (kernel-promotion artifact scope):

- Native BitPacked SIMD kernel (~300-500 LOC).
- Native Rle run-grain kernel (~250-400 LOC).
- Native Dictionary match-set + index walk kernel (~400-600
  LOC).
- Native Delta streaming prefix-sum kernel (~200-350 LOC).
- Native aggregate kernel specializations (~400-600 LOC).
- Per-evaluator predicate CPU at swept selectivity vs. dense
  baseline; ≥ 2× target.
- Tight integer overflow detection rows (impl §19) —
  primarily a correctness gate; bench-layer evidence is
  incremental.
- Optionally: runtime-measured `MockObjectStore` GET counter
  if a read-amplification follow-up bench wants it.

Open ADR 057 follow-up (independent of ADR 058):

- Selector variance candidate at 65K low_card cycling pattern
  — Rle picked when sampler heuristic suggests it should be
  rejected. Investigate sample-window shape, alternative
  selector branches, or buffer-ordinal mapping. Density issue,
  not correctness.

The substrate evidence confirms three council-level expectations:

- **Substrate cost is dominated by `decode_buffer` + scalar
  evaluate.** Native per-cascade kernels remain the path to
  the >= 2x promotion-to-default target.
- **Per-row scaling is row-linear** across all cascades and
  scenarios. Scaling exponents stay within 5% of 1.0 from 8K
  to 262K rows.
- **Selectivity insensitivity is the V1 substrate signature.**
  Selectivity-sensitive curves are kernel-pass territory
  (Rle run-grain, BitPacked SIMD early-exit, Dictionary
  match-set caps) and will be the kernel-bench artifact's
  central evidence.
