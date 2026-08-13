# ADR 550 receipt — the complete timing board

Generated from `timing_atlas_cad04fc4.jsonl` (build `cad04fc4fe79`,
sf=8.0, strict, tcp_loopback, 2 workers, 7 measured pairs).
**21 measured cells; q15 is recorded as `excluded_cell`
and is not measured.** All values are medians in ms.

Published in full because a 15-cell presentation is the `distinguished`-and-slower
subset S, not the board.

**Columns are independent medians and must not be summed row-wise.** Each of wall,
the five components, and remainder is a median taken separately over the measured
pairs, so a row is not an arithmetic partition of its own wall figure. Per-child and
per-path reconciliation was enforced by the atlas replay validator at seal time, not
re-derived here: this artifact sealed with `replay_violations: []`, `withheld_reasons: []`, `timing_authority: true`.

## Local arm

| q | wall | scan | operator | shuffle | dispatch | transport | remainder | rem% |
|---|---|---|---|---|---|---|---|---|
| q01 | 5283.6 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 5284.0 | 100% |
| q02 | 2238.6 | 107.5 | 1074.0 | 386.5 | 0.0 | 0.0 | 675.3 | 30% |
| q03 | 2144.2 | 14.8 | 899.1 | 0.6 | 0.0 | 0.0 | 1229.9 | 57% |
| q04 | 3368.4 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 3368.8 | 100% |
| q05 | 6216.0 | 748.4 | 2171.2 | 1686.2 | 0.0 | 0.0 | 1626.6 | 26% |
| q06 | 884.4 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 884.7 | 100% |
| q07 | 4464.7 | 77.4 | 4056.2 | 208.9 | 0.0 | 0.0 | 144.4 | 3% |
| q08 | 3856.6 | 109.8 | 3452.3 | 89.5 | 0.0 | 0.0 | 430.9 | 11% |
| q09 | 8211.9 | 142.3 | 6878.6 | 334.0 | 0.0 | 0.0 | 871.1 | 11% |
| q10 | 2574.1 | 1175.9 | 194.1 | 490.8 | 0.0 | 0.0 | 714.9 | 28% |
| q11 | 844.0 | 122.7 | 248.6 | 256.7 | 0.0 | 0.0 | 216.9 | 26% |
| q12 | 2221.0 | 1602.1 | 410.0 | 7.1 | 0.0 | 0.0 | 203.9 | 9% |
| q13 | 2999.3 | 4.6 | 42.5 | 46.5 | 0.0 | 0.0 | 2905.5 | 97% |
| q14 | 812.5 | 0.0 | 134.9 | 0.0 | 0.0 | 0.0 | 680.0 | 84% |
| q16 | 1329.0 | 62.3 | 93.4 | 113.0 | 0.0 | 0.0 | 1062.3 | 80% |
| q17 | 4079.2 | 1001.7 | 1087.1 | 951.9 | 0.0 | 0.0 | 1059.0 | 26% |
| q18 | 6919.2 | 504.8 | 1702.0 | 1213.9 | 0.0 | 0.0 | 3488.1 | 50% |
| q19 | 2076.3 | 0.0 | 432.5 | 0.0 | 0.0 | 0.0 | 1645.4 | 79% |
| q20 | 1681.7 | 750.4 | 192.7 | 413.6 | 0.0 | 0.0 | 326.5 | 19% |
| q21 | 12641.3 | 1.2 | 6623.3 | 0.2 | 0.0 | 0.0 | 6016.7 | 48% |
| q22 | 656.2 | 60.7 | 266.6 | 203.9 | 0.0 | 0.0 | 125.6 | 19% |

## Distributed arm

| q | wall | scan | operator | shuffle | dispatch | transport | remainder | rem% |
|---|---|---|---|---|---|---|---|---|
| q01 | 21820.2 | 1096.0 | 0.0 | 9605.6 | 21821.1 | 7082.4 | 3912.6 | 18% |
| q02 | 2089.3 | 74.7 | 1388.8 | 464.8 | 2090.2 | 1.0 | 149.7 | 7% |
| q03 | 5892.0 | 5342.4 | 127.6 | 261.2 | 5897.1 | 47.8 | 533.8 | 9% |
| q04 | 3158.5 | 447.0 | 268.5 | 1474.7 | 3159.6 | 789.2 | 193.3 | 6% |
| q05 | 14506.6 | 865.4 | 10182.2 | 2133.7 | 14507.9 | 34.3 | 1409.1 | 10% |
| q06 | 1261.4 | 599.5 | 0.0 | 82.8 | 1262.0 | 42.0 | 531.2 | 42% |
| q07 | 26959.3 | 887.0 | 16543.0 | 5581.2 | 26960.9 | 2661.5 | 2005.1 | 7% |
| q08 | 9516.7 | 996.3 | 1302.8 | 3985.8 | 9518.1 | 11.2 | 3467.9 | 36% |
| q09 | 15406.9 | 1106.6 | 2820.4 | 5991.2 | 15411.1 | 794.1 | 4834.2 | 31% |
| q10 | 3224.1 | 1183.0 | 190.4 | 883.2 | 3245.8 | 460.1 | 576.3 | 18% |
| q11 | 1228.7 | 69.4 | 667.4 | 298.0 | 1229.5 | 5.1 | 186.8 | 15% |
| q12 | 7150.2 | 5956.7 | 131.6 | 633.5 | 7158.5 | 292.7 | 742.3 | 10% |
| q13 | 2936.7 | 0.0 | 13.0 | 33.6 | 2976.5 | 9.6 | 2932.0 | 100% |
| q14 | 1127.0 | 734.0 | 6.8 | 145.9 | 1129.8 | 65.5 | 296.9 | 26% |
| q16 | 1292.3 | 61.1 | 519.7 | 270.3 | 1294.1 | 113.0 | 331.5 | 26% |
| q17 | 3277.4 | 611.2 | 539.1 | 1466.5 | 3278.1 | 1.3 | 654.5 | 20% |
| q18 | 10032.0 | 5420.3 | 1722.0 | 274.5 | 10033.0 | 0.5 | 2633.4 | 26% |
| q19 | 2167.6 | 1766.9 | 82.2 | 222.7 | 2171.2 | 93.4 | 403.9 | 19% |
| q20 | 2070.5 | 746.5 | 614.6 | 373.0 | 2071.1 | 1.4 | 340.7 | 16% |
| q21 | 23547.6 | 1270.7 | 16379.0 | 3748.6 | 23548.9 | 108.2 | 2012.5 | 9% |
| q22 | 489.3 | 43.9 | 102.7 | 269.2 | 490.1 | 62.0 | 11.6 | 2% |

## Pairing and the dominance fork

`verdict` and `median_paired_delta_ms` are the artifact's own. Component shares are
`contribution_ms` as a percentage of that delta — ADR 538's leading basis. Shares are
omitted where the delta is not positive. Shares may exceed 100% or go negative: they
are signed contributions to a *net* delta, not a partition of it.

| q | verdict | delta | scan% | operator% | shuffle% | transport% | in S | scan+operator% |
|---|---|---|---|---|---|---|---|---|
| q01 | distinguished | 16663.9 | 7% | 0% | 58% | 43% | yes | 7% |
| q02 | distinguished | -217.3 | — | — | — | — | no | — |
| q03 | distinguished | 4193.6 | 127% | -18% | 6% | 1% | yes | 109% |
| q04 | distinguished | -180.2 | — | — | — | — | no | — |
| q05 | distinguished | 8322.9 | 1% | 96% | 5% | 0% | yes | 97% |
| q06 | distinguished | 375.2 | 160% | 0% | 22% | 11% | yes | 160% |
| q07 | distinguished | 23186.4 | 3% | 54% | 23% | 11% | yes | 57% |
| q08 | distinguished | 5695.0 | 16% | -38% | 68% | 0% | yes | -22% |
| q09 | distinguished | 7353.8 | 13% | -55% | 77% | 11% | yes | -42% |
| q10 | distinguished | 712.3 | 1% | -1% | 55% | 65% | yes | 1% |
| q11 | distinguished | 384.4 | -14% | 110% | 11% | 1% | yes | 96% |
| q12 | distinguished | 5513.5 | 79% | -5% | 11% | 5% | yes | 74% |
| q13 | indistinguishable | 12.7 | -36% | -234% | -100% | 76% | no | -270% |
| q14 | distinguished | 436.8 | 168% | -29% | 33% | 15% | yes | 139% |
| q16 | indistinguishable | -34.2 | — | — | — | — | no | — |
| q17 | distinguished | -812.2 | — | — | — | — | no | — |
| q18 | distinguished | 3128.3 | 157% | 0% | -30% | 0% | yes | 157% |
| q19 | distinguished | 489.4 | 361% | -72% | 46% | 19% | yes | 289% |
| q20 | distinguished | 381.0 | -1% | 113% | -11% | 0% | yes | 112% |
| q21 | distinguished | 10909.9 | 12% | 89% | 34% | 1% | yes | 101% |
| q22 | distinguished | -166.3 | — | — | — | — | no | — |

**|S| = 15**, threshold = ceil(2/3 x 15) = **10**. This matches the
artifact's own `distinguished_slower_cells: 15`.

| component | cells led |
|---|---|
| scan | 6 |
| operator | 5 |
| shuffle | 4 |
| transport | 1 |

No component reaches 10, so no component leads and the no-work
branch wins — reproducing ADR 538's verdict over its stated inputs.

Merging `scan` and `operator` — the boundary ADR 550 shows to be scope-variant — gives
**11** cells, which crosses 10 and flips the fork. Recorded as
sensitivity evidence only; it is not a dominance result and licenses no
combined-component optimization.

## Cells pairing a zero median local `scan` counter against a nonzero distributed one

Reported as counter observations. Each component row summarizes 7 measured pairs,
so a `median_local_ms` of 0 is a property of the median and does **not** assert that
every pair recorded zero. No per-cell plan shape is known, and no claim is made here
about where those local reads were counted; see ADR 550 Decision 5.

| q | local scan | distributed scan | scan% of delta |
|---|---|---|---|
| q01 | 0.0 | 1096.0 | 7% |
| q04 | 0.0 | 447.0 | — |
| q06 | 0.0 | 599.5 | 160% |
| q14 | 0.0 | 734.0 | 168% |
| q19 | 0.0 | 1766.9 | 361% |

Distributed `scan` total across these cells: **4643.3 ms**. Each is marked `pairing: paired`, `eligible: true`, `additive: true`.

## Scan contribution exceeding the median paired delta

| q | scan% of delta |
|---|---|
| q03 | 127% |
| q06 | 160% |
| q14 | 168% |
| q18 | 157% |
| q19 | 361% |

This is **not** evidence of mismatched scope on its own: a valid positive component
can exceed a net delta when another component is negative, which is the case in each
of these cells. It measures how much weight the fork places on the scan/operator
boundary. The scope defect itself is established by the source mapping in ADR 550,
not by these figures.
