# ADR 471 implementation receipt

The mechanism landed end-to-end and moved the table decisively — three material wins
(one newly created), the flagship regression eliminated — but the predeclared rule
reads the table without discretion: **q19 still prices a material regression, so the
default stays off**. The residual is named to the column type.

## The mechanism, in three layers, each pinned

1. **Comparison scaling** (`comparison_scaled_bytes`, binder/join.rs): each side's
   byte basis scales by `estimated_row_count / row_upper_bound` when a discount
   exists; the unchanged 2× margin runs on scaled values; admission bounds are
   untouched; receipts carry the scaled values. Pinned by the q14-shaped synthetic
   (`adr471_filtered_side_scales_comparison_and_keeps_natural_build` — fails under
   bound-only comparison).
2. **The enabling data**: persisted table facts now carry `column_range` stats from
   parquet **footer statistics** (metadata-only, no data scan, no dataset epoch) —
   INT32/INT64/DATE32/FLOAT32/FLOAT64, encoded with Wendelon's own stat encoders
   against the selectivity decoder's exact contract; any doubt → absent → unscaled
   bound, never a guess.
3. **The real path**: `adr471_q14_binds_natural_with_flag_on` — the 469 table's
   −195% fixture now binds natural with the capability on, through the full
   persisted pipeline.

## Census and prices (post-fix comparator epoch; no ranking against 469's table)

Engaged set: q02, q10, q11, q17, q19, q20 — q14 disengaged (the fix), q10 newly
engaged. Digest equality at all 44 census children and all six gates.

| cell (sf=1) | HL OFF−ON | 95% interval | rel. | verdict |
|---|---:|---:|---:|---|
| q02 | +105.4 ms | [88.7, 121.2] | +17.3% | material win |
| q10 | +256.0 ms | [250.5, 260.5] | +34.6% | material win (new) |
| q11 | +116.3 ms | [104.8, 126.2] | +24.6% | material win |
| q17 | −42.7 ms | [−56.3, −32.2] | −2.1% | detectable, immaterial |
| q19 | −714.6 ms | [−806.6, −637.5] | −75.0% | **material regression** |
| q20 | −4.5 ms | [−52.8, 27.4] | −0.3% | undetectable |

## The decision (predeclared rule, applied mechanically)

"Default on iff no engaged cell prices a material regression" — q19 regresses
materially. **Default remains off.** The capability's value case strengthened
(three material wins totaling would-be gains at ~17–35%), but a default cannot
inflict −75% on q19-shaped workloads.

## The named residual

q19's (and q17's) filters sit on columns the stats path cannot cover: `l_quantity`
is Decimal128 — the selectivity decoder has no decimal tag — and the categorical
filters (brands, shipmodes, containers) are strings. Unestimated filters leave
those sides at their unfiltered bounds, and the comparator stays blind exactly
there. Two candidate closures, for the successor to choose with evidence: extend
range-stat coverage (decimal tag end-to-end; string equality via NDV), or a
conservative comparator guard — "a side whose facts carry no discount but whose
plan carries a filter is unknown-size: do not swap" — which requires a small facts
signal (`filtered-without-estimate`) the spine does not yet carry.

## Gates

Binder suite green (new fixture included), toron-tpch adr471 probe green,
architecture green, full engine suite green; production default unchanged (off), so
no epoch lands on any standing artifact; census/prices ran explicit arms with
digest equality as the armed invariance evidence.
