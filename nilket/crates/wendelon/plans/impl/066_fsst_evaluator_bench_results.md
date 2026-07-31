# ADR 066 FSST Evaluator Bench Results

Date: 2026-05-10

Scope: local Phase 7 evidence for the default-off ADR 066
implementation. This file records runnable diagnostics and current
promotion-gate status; it is not council evidence for default-on.

Commands:

```sh
cargo bench --bench encoding_compute -- --quick
cargo bench --bench row_group_fetch -- --quick
cargo bench --bench encoding_compute
cargo bench --bench row_group_fetch
```

Environment:

- Local development machine.
- Bench profile, optimized build.
- Encoding-compute quick mode: 8,192 rows, 50 iterations.
- Encoding-compute full mode: 8,192 / 65,536 / 1,048,576 rows,
  200 iterations.
- Row-group quick mode: 8,192 rows, 10 measured iterations after
  warmup.
- Row-group full mode: 8,192 / 65,536 / 1,000,000 rows, 10
  measured iterations for 8K / 65K and 3 measured iterations for
  1M.

## Quick FSST Diagnostics

Quick `encoding_compute` is a smoke check for the bench surface. Gate
status below is based on the full matrices where available.

| Fixture | Kernel us/op | Dense us/op | Kernel/dense ratio |
| --- | ---: | ---: | ---: |
| `fsst_url_repeated_prefix` | 214.568 | 148.737 | 1.442602 |
| `fsst_url_eq_5p` | 201.719 | 150.005 | 1.344746 |
| `fsst_url_inlist4_5p` | 384.406 | 202.605 | 1.897316 |
| `fsst_short_string_diag` | 34.977 | 27.907 | 1.253319 |
| `fsst_weak_compression_diag` | 396.937 | 423.786 | 0.936646 |
| `fsst_disjoint_length_prefilter` | 162.885 | 136.181 | 1.196088 |

Quick declined-path rows:

| Route | Dense baseline us/op | Declined route us/op | Overhead |
| --- | ---: | ---: | ---: |
| `dense_fallback_atom_unsupported` | 149.937 | 150.100 | 0.1085% |
| `dense_fallback_type_unsupported_utf8_view` | 150.027 | 149.077 | 0.0000% |
| `dense_fallback_type_unsupported_binary_view` | 150.324 | 150.340 | 0.0110% |

Quick determinism row:

| Fixture | Rows | Mask equal | Encoded equal | Digest |
| --- | ---: | --- | --- | --- |
| `fsst_url_repeated_prefix` | 8,192 | true | true | `3758dabd2ac5834e33020bdbdec1a194f7441f8ca2dda1c0a6043027c0bcc885` |

## Full Encoding-Compute Matrix

Full `cargo bench --bench encoding_compute` was run for the current
FSST bench surface, including the representative 5% `Eq` and
`InList(4)` fixtures. Kernel/dense ratio is wall-clock ratio; values
below 1.0 mean the kernel was faster than dense decode.

| Rows | Fixture | Encoded bytes | Decoded bytes | Kernel us/op | Dense us/op | Kernel/dense ratio |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 8,192 | `fsst_url_repeated_prefix` | 180,772 | 360,448 | 209.942 | 153.226 | 1.370148 |
| 8,192 | `fsst_url_eq_5p` | 180,939 | 360,448 | 213.463 | 153.394 | 1.391604 |
| 8,192 | `fsst_url_inlist4_5p` | 180,939 | 360,448 | 400.691 | 205.285 | 1.951871 |
| 8,192 | `fsst_short_string_diag` | 10,563 | 16,384 | 35.941 | 28.582 | 1.257479 |
| 8,192 | `fsst_weak_compression_diag` | 122,782 | 196,608 | 404.626 | 426.519 | 0.948668 |
| 8,192 | `fsst_disjoint_length_prefilter` | 180,772 | 360,448 | 165.415 | 139.232 | 1.188051 |
| 65,536 | `fsst_url_repeated_prefix` | 1,446,120 | 2,883,584 | 1,682.746 | 1,245.271 | 1.351309 |
| 65,536 | `fsst_url_eq_5p` | 1,446,282 | 2,883,584 | 1,660.673 | 1,256.523 | 1.321641 |
| 65,536 | `fsst_url_inlist4_5p` | 1,446,282 | 2,883,584 | 3,210.531 | 1,711.407 | 1.875959 |
| 65,536 | `fsst_short_string_diag` | 82,691 | 131,072 | 285.397 | 226.965 | 1.257450 |
| 65,536 | `fsst_weak_compression_diag` | 990,017 | 1,572,864 | 3,642.161 | 3,657.277 | 0.995866 |
| 65,536 | `fsst_disjoint_length_prefilter` | 1,446,120 | 2,883,584 | 1,334.397 | 1,153.369 | 1.156956 |
| 1,048,576 | `fsst_url_repeated_prefix` | 23,418,565 | 46,185,920 | 27,276.352 | 33,691.141 | 0.809600 |
| 1,048,576 | `fsst_url_eq_5p` | 23,354,900 | 46,137,344 | 28,068.496 | 33,757.468 | 0.831475 |
| 1,048,576 | `fsst_url_inlist4_5p` | 23,366,059 | 46,137,344 | 51,619.905 | 39,859.161 | 1.295057 |
| 1,048,576 | `fsst_short_string_diag` | 1,319,171 | 2,097,152 | 4,910.105 | 3,722.442 | 1.319054 |
| 1,048,576 | `fsst_weak_compression_diag` | 16,270,807 | 25,165,824 | 61,749.120 | 61,830.968 | 0.998676 |
| 1,048,576 | `fsst_disjoint_length_prefilter` | 23,418,565 | 46,185,920 | 21,838.884 | 31,377.385 | 0.696007 |

Recorded 8K counters:

| Fixture | Length-mismatch rows | Byte comparisons | Decoded bytes skipped |
| --- | ---: | ---: | ---: |
| `fsst_url_repeated_prefix` | 0 | 271,454 | 88,994 |
| `fsst_url_eq_5p` | 0 | 259,269 | 101,179 |
| `fsst_url_inlist4_5p` | 0 | 260,496 | 101,179 |
| `fsst_disjoint_length_prefilter` | 8,192 | 0 | 360,448 |

Interpretation:

- The full matrix now includes the required 5% `Eq` /
  `InList(4)` fixtures.
- The predicate wall-clock gate is not cleared. The 5% `Eq` row is
  slower than dense at 8K and 65K and only about 1.20x faster at 1M;
  the optimized `InList(4)` row is still slower than dense at every
  row count.
- The short-string and weak-compression rows engage the kernel and
  record CPU overhead as diagnostics, not promotion gates.
- The encoded-payload-vs-decoded-plaintext byte ratio is recorded per
  fixture above. Kernel and dense fallback fetch the same encoded
  payload; these ratios are diagnostics, not a fetch-byte gate.

## Full Dense-Fallback Decline Matrix

| Rows | Route | Dense baseline us/op | Declined route us/op | Overhead |
| ---: | --- | ---: | ---: | ---: |
| 8,192 | `dense_fallback_atom_unsupported` | 150.050 | 150.100 | 0.0334% |
| 8,192 | `dense_fallback_type_unsupported_utf8_view` | 148.581 | 149.022 | 0.2970% |
| 8,192 | `dense_fallback_type_unsupported_binary_view` | 159.764 | 160.975 | 0.7579% |
| 65,536 | `dense_fallback_atom_unsupported` | 1,252.291 | 1,258.399 | 0.4877% |
| 65,536 | `dense_fallback_type_unsupported_utf8_view` | 1,264.541 | 1,245.435 | 0.0000% |
| 65,536 | `dense_fallback_type_unsupported_binary_view` | 1,238.212 | 1,238.319 | 0.0086% |
| 1,048,576 | `dense_fallback_atom_unsupported` | 33,694.171 | 33,693.184 | 0.0000% |
| 1,048,576 | `dense_fallback_type_unsupported_utf8_view` | 33,595.651 | 33,733.525 | 0.4103% |
| 1,048,576 | `dense_fallback_type_unsupported_binary_view` | 33,483.097 | 33,761.280 | 0.8308% |

Interpretation:

- Dense-fallback overhead is below the <= 5% gate for the current
  range/type declined rows across 8K / 65K / 1M.
- Future-LIKE analog rows and broader ineligible-layout rows remain
  follow-up evidence before default-on.

## Full Determinism Diagnostics

| Rows | Mask equal | Encoded equal | Digest |
| ---: | --- | --- | --- |
| 8,192 | true | true | `3758dabd2ac5834e33020bdbdec1a194f7441f8ca2dda1c0a6043027c0bcc885` |
| 65,536 | true | true | `b0d1dad3d54a65027e8309d08aa1e124910766bbafb7a37b55027d10fc2b279d` |
| 1,048,576 | true | true | `f8c7c23e02065b5bf13d33545f8833e78e2b4d895efbbc5397f20113de98b3b4` |

Interpretation:

- Cross-replica mask and encoded-payload determinism holds for the
  current URL-shaped fixture at all three row counts.
- Broader determinism rows across additional representative fixtures
  remain useful follow-up evidence before default-on.

## Row-Group Case A Diagnostics

Quick `row_group_fetch` evidence includes one ADR 066 end-to-end Case
A row (`scenario=adr066_fsst_row_group_fetch`,
`route=case_a_after_fsst`):

| Rows | Output rows | Mask equal | Encoded FSST bytes | Offset bytes | Decoded predicate bytes | Kernel/dense CPU ratio | Total dense bytes | Total sparse bytes | End-to-end speedup |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8,192 | 52 | true | 26,845 | 4,100 | 52,988 | 4.970234 | 59,244 | 32,353 | 1.263804 |

The same quick run also emits an A2 overlap diagnostic
(`route=dense_case_b_overlap_a2`, `kernel_engaged=false`):

| Rows | Output rows | Mask equal | Dense predicate bytes | Dense projection bytes | Dense total ms | Case B zip ms |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 8,192 | 52 | true | 52,988 | 6,256 | 0.802 | 0.089 |

Full `cargo bench --bench row_group_fetch` ADR 066 rows:

| Rows | Profile | Route | Output rows | Mask equal | Kernel/dense CPU ratio | Total dense bytes | Total sparse bytes | Dense total ms | Sparse total ms | End-to-end speedup |
| ---: | --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8,192 | point_read | `case_a_after_fsst` | 52 | true | 5.545547 | 59,244 | 32,353 | 0.703 | 0.592 | 1.187585 |
| 8,192 | point_read | `dense_case_b_overlap_a2` | 52 | true | n/a | 59,244 | n/a | 0.790 | n/a | n/a |
| 65,536 | scan | `case_a_after_fsst` | 3,277 | true | 5.642349 | 3,752,559 | 1,988,653 | 19.341 | 26.464 | 0.730848 |
| 65,536 | scan | `dense_case_b_overlap_a2` | 3,277 | true | n/a | 3,752,559 | n/a | 21.153 | n/a | n/a |
| 1,000,000 | point_read | `case_a_after_fsst` | 832 | true | 6.223236 | 947,904 | 517,648 | 9.599 | 3.037 | 3.160824 |
| 1,000,000 | point_read | `dense_case_b_overlap_a2` | 832 | true | n/a | 947,904 | n/a | 10.002 | n/a | n/a |

Interpretation:

- Case A composition is byte-correct across the current 8K / 65K /
  1M row-group matrix (`mask_equal=true`, `equality=true`,
  `null_count_equal=true`).
- Total plaintext bytes drop on every Case A row.
- End-to-end wall-clock is mixed: 1M point-read is well above the
  1.2x gate, 8K point-read is below it on this full run, and the
  65K scan row regresses. The end-to-end promotion gate is therefore
  not cleared.
- A2 overlap rows continue to report `kernel_engaged=false`.

## Promotion Gate Status

| Gate | Status |
| --- | --- |
| Predicate-evaluation wall-clock >= 1.5x on representative 5% `Eq` / `InList` fixtures | Not cleared by the full encoding matrix; 5% `Eq` is below the required speedup and `InList(4)` remains slower than dense at every row count |
| End-to-end query wall-clock >= 1.2x through ADR 065 -> ADR 060 / 064 Case A | Not cleared by the full row-group matrix; 65K scan regresses and 8K point-read is below 1.2x |
| Dense-fallback overhead <= 5% on truly declined paths | Cleared for the current 8K / 65K / 1M range/type rows; future-LIKE and broader layouts pending |

Default enablement decision:

- `EncodingComputeConfig::default()` keeps `Fsst` disabled.
- Operators can opt in by adding `"fsst"` to
  `enabled_predicate_evaluators`.

## V1 Perf Iteration 1 (2026-05-10) — Post-Mortem

Council-driven attempt to clear the failing predicate wall-
clock and end-to-end gates without leaving Strategy A
(streaming token decode). Outcome: hypothesis refuted by
bench evidence; V1 perf is structurally exhausted.

### Phase A — dispatch / setup hygiene (landed)

Three changes attempted; one kept.

- **A.1 op-hoist:** replaced the per-row
  `predicate_bit(matches, op)?` match with explicit
  Eq-vs-Neq inversion baked into four inline loop bodies
  (single-literal × invert / no-invert; multi-literal ×
  invert / no-invert). Council decision (Gemini): explicit
  bodies, not const-generic monomorphization, to avoid LLVM
  de-opt on the inner loop. **Bench: -5% kernel/dense
  ratio on `url_repeated_prefix`; ~0 on others (noise).**
  Code-clarity win; landed.
- **A.2 BTreeMap → Vec length buckets:** replaced the two-
  stage `BTreeMap → BTreeMap` length-bucket construction
  with a single sorted `Vec<(usize, LiteralGroup)>` and
  linear-scan `bucket_for_len`. **Bench: +8% regression on
  `InList(4)`.** `BTreeMap::get` on a 1-entry map already
  compiles to a single key compare; `Vec::iter().find()`
  added overhead. Reverted.
- **A.3 MaskBitWriter (8-bit batched mask write):**
  replaced per-row `MaskBuilder::push` with a u8 bit
  buffer flushed via `push_aligned_bitmap_byte` every 8
  rows. **Bench: +12-34% regression across all fixtures.**
  Per-row shift/OR/inc/cmp + occasional flush turned out
  more expensive than `push`'s bounds-check + bit-set.
  Reverted.

Net Phase A: code-clarity win on op-hoist, perf delta
within noise. LLVM was already optimizing the constant-op
match; the BTreeMap was already efficient for small N;
`MaskBuilder::push` was already tighter than batched
shift-and-flush.

### Phase B — decode-into-scratch + memcmp (refuted; reverted)

Replaced per-row `matches_one` / `matches_any` (byte-at-a-
time fused decode-and-compare with short-circuit on first
mismatch) with `decode_row_into_scratch + memcmp` (full-
row decode then vectorized slice eq).

**Hypothesis:** memcmp's auto-vectorization beats per-byte
branched compare; this would close the gap against dense
Arrow's vectorized equality scan.

**Bench evidence (8K rows):**

| Fixture | Pre-Phase-B ratio | Post-Phase-B ratio | Delta |
| --- | ---: | ---: | ---: |
| `fsst_url_repeated_prefix` | 1.443 | 2.116 | +47% |
| `fsst_url_eq_5p` | 1.345 | 2.205 | +64% |
| `fsst_url_inlist4_5p` | 1.897 | 1.888 | ~0 |
| `fsst_short_string_diag` | 1.253 | 3.066 | +145% |
| `fsst_weak_compression_diag` | 0.937 | 1.212 | +29% |

Massive regression on every single-literal `Eq` fixture;
`InList(4)` unchanged.

**Root cause:** at 5% selectivity, 95% of rows mismatch on
an early byte. The original `matches_one` decoded ~1-2
bytes per mismatched row before short-circuiting;
`skip_after_mismatch` walked the encoded payload past the
rest without writing decoded bytes. Phase B always decoded
the full row (~44 bytes for URLs) into scratch *before*
comparing, so the memcmp short-circuit ran over already-
decoded bytes — too late to skip the decode work. Net:
trade `1-2 bytes decoded + 1-2 bytes compared` for `44
bytes decoded into Vec + 44-byte memcmp`. SIMD memcmp
cannot recover ~40× extra decode work per row.

`InList(4)` was a wash because the original
`matches_any_bitmask`'s 4× per-byte cost balanced against
Phase B's "decode once, compare 4 times" structure.

Reverted in full.

### Phase C / Phase D — not executed

- **Phase C** (per-stage row-group instrumentation) was
  conditional on Phase B's encoding-compute gap closing
  and the row-group 65K scan still regressing. Phase B's
  failure means the encoding-compute gap remains the
  primary problem; Phase C diagnostic is moot.
- **Phase D** (`memchr` SIMD escalation) was conditional on
  A+B+C missing the gates. Skipped on diagnosis grounds:
  the bottleneck is per-byte branched *decode* (symbol /
  escape / literal lookup in `next_byte`), not byte
  *compare*. `memchr` accelerates needle-in-haystack
  search and equality compare; it cannot accelerate
  symbol-table-driven decode.

### V1 ceiling

The bench shape — kernel/dense ratio shrinks with row
count (worse at 8K, even at 65K, kernel wins at 1M) —
is a fixed-per-buffer overhead pattern. WFS1 frame parse,
offsets buffer decode, and per-buffer setup amortize only
at large row counts. At small/medium row counts the per-
buffer fixed cost dominates the per-row decode-and-compare
work, and the per-row work itself is already algorithmically
near-optimal for Strategy A (short-circuit on first byte
mismatch).

Within Strategy A's "stream encoded bytes, compare to
decoded" frame, there is no algorithmic improvement
available that closes the small/medium row-count gap. To
beat dense at those row counts would require either (a)
vectorized FSST decode (complex; symbol-table lookup is
not naturally vectorizable without table-in-register
tricks), or (b) avoiding decode entirely via Strategy B
(pre-encoded literal compare against encoded bytes).

Strategy B is structurally unsafe under the current WFS1
encoder per ADR 066 §"Five forces" #3: FSST symbols can
span Arrow row boundaries, so a literal's encoded bytes
do not have a fixed position relative to row boundaries.
Strategy B requires a writer amendment that enforces row-
safe FSST framing. That amendment is V2 work — its own
ADR.

### Promotion Gate Status — final

| Gate | Status |
| --- | --- |
| Predicate-evaluation wall-clock >= 1.5x on representative 5% `Eq` / `InList` fixtures | **Not cleared.** Structural V1 ceiling reached; Strategy A cannot beat dense at small/medium row counts. |
| End-to-end query wall-clock >= 1.2x through ADR 065 -> ADR 060 / 064 Case A | **Not cleared.** 1M point-read clears (3.16x); 8K and 65K do not. |
| Dense-fallback overhead <= 5% on truly declined paths | **Cleared.** |

### Default enablement decision

- `EncodingComputeConfig::default()` keeps `Fsst`
  disabled.
- Operators can opt in by adding `"fsst"` to
  `enabled_predicate_evaluators`.
- Default-on promotion is **deferred to V2 (Strategy B)**.

### V2 path

Council recommendation (Claude / Gemini, 2026-05-10): pause
ADR 066 V1 work and draft a follow-up ADR (ADR 067) for
the writer amendment that enforces row-safe FSST framing.
Once the writer guarantees symbols cannot cross Arrow row
boundaries, ADR 066 V2 implements Strategy B (pre-encoded
literal compare against encoded byte ranges) on top of
the amended frame. Strategy B avoids decode entirely on
the predicate path, which is the only known structural
lever capable of beating dense Arrow's vectorized
equality scan at small/medium row counts.

### Follow-up evidence (V2-aligned)

- Writer amendment ADR + implementation (row-safe FSST
  framing).
- Strategy B kernel implementation (pre-encoded literal
  compare).
- Re-bench at the V2 gate.
- Broader cross-replica determinism rows across
  representative fixtures (carry forward from V1 follow-
  up list).
- Future-LIKE analog declined-path rows.
