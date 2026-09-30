# ADR 609 - borrowed multi-key group lookup: results

Run 2026-09-29 on strix, at the ADR 608 coordinate, bound to ADR 608's
sealed board. The sequence was the all-cell gate, the q01 pair, the q09
breadth pair, and then the mechanism check. **Retained.**

On q01 the treatment's median is 3,810.5 ms against control's 4,524.0
ms. The median paired gain is **713.4 ms (15.8%)**, against a bar of
99.6 ms (`2 × range_control`, range 49.8 ms), and every pair is
strictly positive (686.8 to 747.4 ms). Every row's digest equals ADR
608's q01 Toron digest. Neither arm spilled. Peak RSS is the same on
both arms, 29,284–30,096 KiB (`above_parent_high_water`). The token
stays `Borrowed`.

The mechanism is as named. The gate's code-path counters read q01's
lookup `String` constructions as **94,627,302 under `Owned` against 8
under `Borrowed`** (4 groups × 2 keys; the receipt shows no NULL key).
In single diagnostic runs, `row_lookup_update` falls from 2,711 ms to
1,882 ms, with the other stages close to unchanged.

**Breadth (q09): not established, and no regression.** The median gain
is 7.9 ms against a bar of 45.3 ms, with pairs of mixed sign (−44.0 to
+21.6 ms). There is digest parity, no spill, and unchanged RSS.

q01's control median is 228.7 ms (4.8%) below ADR 608's sealed q01
cell (4,752.7 ms). That is between-run variation. The paired gain is
the token's evidence, and no 22-cell total is inferred from this
record.


## Identity

| identity | value |
|---|---|
| control | commit `4c6e5de9`, tree `928c85cf8635af7292d139301f15befca788a8f7`, tag `artifact/adr609-control`; executable `e1c7da408eea834b8a291a84b118a90796cdcfff4707a7504d530b72f31263e9`; manifest `adr609-control.manifest` (`5c2db20a…`); `GROUP_KEY_LOOKUP = Owned` |
| treatment | commit `927d15f1`, tree `78653233c519804b3ba87136d7a0dd34dac238ae`, tag `artifact/adr609-treatment`; executable `ce11be598bdf44a456468ad650f92f60019efd5c429074aac3843929fcabbe4f`; manifest `adr609-treatment.manifest` (`ae631320…`); `Borrowed` |
| difference | one line in `exec/aggregate/group_table.rs` |
| sealed baseline | `target/tpch/adr608-board/perf_board.jsonl.evidence.sealed` (artifact `7662f6f2…`) |
| ladders (bigfoot, both trees, fmt clean) | toron 5,498 passed, 0 failed, 3 ignored (6 new ADR 609 fixtures); toron-tpch 674 passed, 0 failed, 6 ignored |
| recipe / DuckDB | `9af51cbc…` / `c478c0f4…` |
| build host / run host | bigfoot / strix |

The implementation commit also amends the plan, reviewed before the
build:
- **NULL-aware counter relations.** Key-value products are exact.
  `String` counts are bounded, with equality only when there is no NULL
  UTF-8 key.
- **The `string_agg` path.** A fold with a `STRING_AGG` call keeps the
  owned lookup under both forms.
- **Route.** The receipt's `route` is `null`: node attribution has no
  route getter, and the function name identifies the loop.

## The gate

One `perf-run-one` of each of the 22 queries per arm, with
`TORON_GROUP_KEY_LOOKUP_RECEIPT_PATH` set to one file per arm and query
(`adr609-strix-host-record/gate`). All 44 runs passed, and every digest
and plan fingerprint equals ADR 608's sealed cells. The spill scratch
was empty afterwards.

Paths per cell (control → treatment):
- **`borrowed_multi`** (was `owned_generic`): q01, q07, q09, q10, q16
  (two instances), q18.
- **Unchanged:**
  - `owned_generic`: q03, q08, q20;
  - `single_utf8`: q04, q05, q12, q21, q22;
  - `single_int64`: q02, q11, q13, q15, q17;
  - no grouped fold: q06, q14, q19.

No cell took `string_agg`.

| cell | key types | rows | groups | `Owned` values / strings | `Borrowed` values / strings |
|---|---|---|---|---|---|
| q01 | Utf8, Utf8 | 47,313,651 | 4 | 94,627,302 / 94,627,302 | 8 / 8 |
| q09 | Utf8, Int32 | 2,610,723 | 175 | 5,221,446 / 2,610,723 | 350 / 175 |
| q16 (1) | Utf8, Utf8, Int32, Int64 | 947,748 | 947,557 | 3,790,992 / 1,895,496 | 3,790,228 / 1,895,114 |
| q16 (2) | Utf8, Utf8, Int32 | 947,557 | 27,838 | 2,842,671 / 1,895,114 | 83,514 / 55,676 |
| q10 | Int64, Utf8, Decimal128, Utf8 ×4 | 917,291 | 304,540 | 6,421,037 / 4,586,455 | 2,131,780 / 1,522,700 |
| q07 | Utf8, Utf8, Int32 | 46,056 | 4 | 138,168 / 92,112 | 12 / 8 |
| q18 | Utf8, Int64, Int64, Date32, Decimal128 | 3,507 | 501 | 17,535 / 3,507 | 2,505 / 501 |

Every relation held:
- rows and groups are equal between the forms;
- `Owned` values are rows × keys;
- under `Borrowed`, misses equal groups, and values are misses × keys;
- `String` counts are within their bounds;
- forms are labelled correctly, and no path changed within an instance.

For q01, strings equal values under `Owned`, which establishes that no
q01 key value is NULL. So the exact `String` products are stated:
94,627,302 = rows × 2, and 8 = groups × 2. On q10, the
`String` counts sit below their bounds by the NULL count. The ~95 M
figure the plan estimated is now a counter reading, 94,627,302. It is a
code-path counter, not an allocator reading.

**Breadth cell.** q09, which has the largest `borrowed_multi` row sum
outside q01 (2,610,723 ≥ 1 M), with its relations and digests passing.
q16 (1,895,305) was second.

## The q01 pair

Token `adr609q01-9aef28b9ca3c`, launched 21:58:19 CDT.
- **Timing:** the first child ran at 22:00:27, and completion arrived
  at 22:01:49 (`result=success exit_code=exited exit_status=0`).
- **Separated launch:** verified, with a margin of 128.3 s.
- **Measurement window:** verified, with no session events.
- **Start gate:** load 0.07 and 39 °C.
- **Before:** load 0.67 and 46 °C, scratch empty, output fresh.
- **Control variability:** ratio 0.011, within the 0.04 bound.

| pair | order | control ms | treatment ms | gain ms |
|---|---|---|---|---|
| 0 | C, T | 4,545.6 | 3,806.9 | 738.7 |
| 1 | T, C | 4,507.9 | 3,821.1 | 686.8 |
| 2 | C, T | 4,557.8 | 3,810.3 | 747.4 |
| 3 | T, C | 4,524.0 | 3,810.5 | 713.4 |
| 4 | C, T | 4,516.2 | 3,821.6 | 694.6 |
| **median** | | **4,524.0** | **3,810.5** | **713.4** (bar 99.6) |

Warmups: control 4,562.6 ms, treatment 3,805.6 ms. **Verdict:
retained** (ADR 588's rule; q01 decides the token).

## The q09 breadth pair

Token `adr609q09-1ca81e0fedc2`, launched 22:02:10 CDT.
- **Timing:** the first child ran at 22:04:18, and completion arrived
  at 22:06:13 (success).
- **Separation and window:** separation verified (margin 127.8 s), and
  the window verified with no session events.
- **Start gate:** load 0.06 and 37 °C.
- **Control variability:** ratio 0.0033.
- **Distance from the anchor:** control's median is 41.1 ms below ADR
  608's sealed q09 cell.

| pair | order | control ms | treatment ms | gain ms |
|---|---|---|---|---|
| 0 | C, T | 6,976.5 | 7,020.5 | −44.0 |
| 1 | T, C | 6,953.8 | 6,945.9 | 7.9 |
| 2 | C, T | 6,954.4 | 6,964.8 | −10.4 |
| 3 | T, C | 6,954.4 | 6,932.8 | 21.6 |
| 4 | C, T | 6,968.2 | 6,953.3 | 14.9 |
| **median** | | **6,954.4** | **6,953.3** | **7.9** (bar 45.3) |

The driver's verdict block reads `"consequence": "cut reverted in the
same commit; candidate closed"` and `"status": "not_established"`. As
in ADR 607, that consequence is ADR 588's generic action label. For
this coverage pair it does not apply: q01 decides the token.

| interpretation (predeclared) | outcome |
|---|---|
| breadth established | no: 7.9 ≤ 45.3, and three of five pairs positive |
| **not established, without regression** | **this one** |
| regression: digest mismatch; treatment-only spill; treatment slower by > 45.3 ms with every pair negative | none |

Peak RSS is 2,455,872–2,456,876 KiB across both arms: overlapping
ranges, with no material difference observed.
That is consistent with the gate: q09's reachable work is 2.6 M
single-`String` constructions in a 7-second cell.

## The mechanism check

A diagnostic q01 run per arm, after the pairs, with node attribution
and the receipt (`adr609-strix-host-record/mechanism`). These are
single diagnostic runs, not paired latency. Both digests match, and the
stage totals reconcile. Route `scan_grouped` in both.

| stage | control µs | treatment µs | change |
|---|---|---|---|
| `stream_pull` | 701,780 | 705,168 | +3,388 |
| `projection` | 189,108 | 197,224 | +8,116 |
| `arg_eval_and_readers` | 931,674 | 953,222 | +21,548 |
| **`row_lookup_update`** | **2,711,093** | **1,882,156** | **−828,937** |
| `fold` (parent) | 3,652,923 | 2,845,645 | −807,278 |
| aggregate exclusive | 4,556,382 | 3,760,432 | −795,950 |

The receipts repeat the gate's counters: 94,627,302 against 8 lookup
`String`s. In these single diagnostic runs, most of the observed stage
difference is in `row_lookup_update`. They are not a paired
decomposition of the 713.4 ms. What remains in `row_lookup_update` is
1,882 ms for 47.3 M rows into 4 groups, about 40 ns per row. That is
this diagnostic span divided by rows, not a pure kernel rate. The span
includes:
- the borrowed key reading;
- the per-row hash and probe;
- the per-aggregate `AggState` dispatch and update;
- any other code inside the row loop.

The next record addresses it.

## Consequence

- The closing commit on main is the treatment tree
  (`GROUP_KEY_LOOKUP = Borrowed`), with this record and the ADR's
  status.
- The token is global. The gate names six cells on the changed path.
  Only q01 (retained) and q09 (not established) were paired, and the
  others carry the change unmeasured.
- Next: batch group-id assignment with typed columnar accumulators,
  planned separately. It targets q01's remaining `row_lookup_update`
  (1,882 ms) and the per-aggregate update cost.
