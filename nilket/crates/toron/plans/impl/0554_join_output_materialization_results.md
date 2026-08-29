# ADR 554 - join output materialization: results

Pairs with `plans/adr/0554_remainder_has_two_mechanisms.md`. That record closed as an
attribution question; this one records what the attribution then bought. Branch A named
`framework_overhead` as in-task unbracketed work, and the join materialization brackets
(`c9d36f63`, on `4066df08`'s route receipts) located 13,525 ms of the distributed board's
18,680 ms of it - **input concatenation 7,146 ms, output gathering 6,379 ms**.

This document covers the **output gathering** half only: the per-side split that priced it,
the fast path that removed most of it, the proof that the fast path is correct, and one
method trap the line produced that is worth more than the milliseconds.

All figures are SF 8, strict mode, 7 measured pairs, two workers, per-cell medians over
pairs on the distributed arm, summed across cells for board totals.

## Outcome

Board output-gather time fell **6,140.6 ms -> 3,866.9 ms (-37.0%)**, effectively all of it
on the right side: **2,540.8 -> 362.3 ms (-85.7%)**. The mechanism is a zero-copy
`Array::slice` substituted for `take` when a join side's selection is an exact contiguous
run covering its whole source batch. 1,293,016 gathered columns were independently verified
against the `take` they replaced.

The board's distributed elapsed also fell, 173,414 -> 158,803 ms, and **that is not claimed
as this cut's effect** - see "What is not claimed".

## 1. The per-side split - `f15a3611`

`join_output_left_take_micros` and `join_output_right_take_micros` are **nested children** of
`join_output_materialization_micros`. They are absent from `recompute_framework_overhead`:
the parent already removed the time from the residual, and a child that subtracted again
would relocate time twice. Two guards hold that: one fails if a child joins the accounted
set, one asserts `left + right <= parent`. The inequality is deliberate - an equality would
assert the shared index/assembly cost is zero, which is exactly the unknown being measured.

One timer wraps each complete column loop, never each column: a per-column `Instant::now()`
pair is a material fraction of a side running at single-digit nanoseconds per row.

`SideTakeTiming` is an enum with no default, so all 15 `materialize_join_selection` call
sites state their accounting: the generic sites pass `Charged`, the nine spilled and
streaming sites pass `AccountedElsewhere` because their gathers already run inside Spill and
operator brackets.

Artifact `adr554_output_side_split_sf8.jsonl`, authoritative, 21/21, no replay violations,
comparability mean 0.982 (0.875-1.059). The identity holds on all 1,323 nodes; the mean
child/parent ratio is 0.722, so the brackets are placed inside real work rather than around
all of it.

Board: parent 6,140.6 ms = left 2,276.0 (37.1%) + right 2,540.8 (41.4%) + shared 1,323.8
(21.6%).

| cell | parent ms | left ms | right ms | shared ms |
|---|---:|---:|---:|---:|
| q09 | 2,042.4 | 620.5 | **1,025.6** | 396.3 |
| q08 | 1,284.4 | 196.4 | **724.4** | 363.6 |
| q07 | 876.4 | **504.1** | 233.1 | 139.2 |
| q21 | 827.3 | **361.7** | 208.3 | 257.3 |
| q02 | 437.1 | **312.7** | 83.1 | 41.3 |
| q05 | 187.1 | 33.7 | **124.9** | 28.6 |

**A prediction was falsified here.** From the gather bench
(`benches/join_output_gather_shape.rs`) the expectation was `right << left`: the probe side's
indices are contiguous, the build side's are hash-scattered, and scattered gathers priced
5-10x worse per row. The opposite holds on q09 and q08, where the contiguous side costs
2-3.7x the scattered one. Only q21 matches the prediction.

The cause is **not established**. Two candidates, neither tested: column-count and width
asymmetry between the sides (equal rows, different column sets, and the probe side is
usually the wide fact table), or build-side cache residency (the bench held both access
patterns at a 4M-row, 195 MB source, which is unrealistic for a small build side; scatter
over a cache-resident source may cost little). q21 is consistent with the second - its
self-join gives it a large build side, and it is the one cell where scatter dominates. That
the bench overstated the scatter penalty by holding source size fixed across patterns is a
defect in the instrument, recorded against it.

## 2. The whole-source slice fast path - `4c528583`

`ContiguousRun` refuses to slice unless every gathered row contributes a non-null index, the
indices ascend by exactly one end to end, the run's length equals the rows being gathered,
and the span lies inside the source. `checked_add` at the `u32::MAX` boundary. Anything else
falls through to `take`, byte-identical to the previous behaviour.

**The restriction to whole-source coverage is load-bearing, and it is a memory decision, not
a correctness one.** `Array::slice` retains the source's entire backing allocation. A 4-row
slice of a 4M-row batch is charged 4 rows by the materialization estimate and keeps 4M rows
alive - an amplification no value-equality check can see. `slice_offset` therefore layers
`offset == 0 && rows == source_rows` on top of the run arithmetic, and the test
`a_slice_retains_the_whole_source_allocation` pins the fact behind it: on a 1,024-row source,
a 4-row slice's buffer capacity equals the source's while `take` allocates for 4. Arbitrary
contiguous sub-slices are a later cut and need explicit retained-memory accounting first.

### The equivalence proof

`TORON_JOIN_SLICE_EQUIVALENCE` re-gathers every sliced column with the `take` it replaced and
compares `ArrayData` - data type, length, null bitmap, and values, not values alone - failing
the query on mismatch. Run against the full SF 8 cohort
(`adr554_slice_equivalence_sf8.jsonl.partial`):

| fact | value |
|---|---|
| finalized cells | 21/21 |
| replay violations | none |
| child attempts valid and identity-matching | 336/336 |
| columns verified | 1,293,016 |
| `timing_authority` | `false` |
| `withheld_reasons` | `["join_diagnostics_enabled"]` |

The run exits non-zero **by design**: withheld authority refuses publication, so the artifact
is retained as `.partial` and read there. That exit says nothing about equivalence; a genuine
mismatch fails inside a cell with the column named.

### The authoritative run

`adr554_slice_fast_path_sf8.jsonl`, build `4c528583a13d`, `timing_authority: true`,
`join_selection_locality_probe: false`, 21/21, no replay violations.

| term | side split `f15a3611` | slice `4c528583` | delta |
|---|---:|---:|---:|
| right take | 2,540.8 ms | 362.3 ms | **-2,178.5 (-85.7%)** |
| left take | 2,276.0 ms | 2,189.0 ms | -87.0 |
| shared (index + assembly) | 1,323.8 ms | 1,315.6 ms | -8.2 |
| parent | 6,140.6 ms | 3,866.9 ms | -2,273.7 (-37.0%) |

| cell | right before | right after | delta |
|---|---:|---:|---:|
| q09 | 1,025.6 | 133.8 | -891.8 |
| q08 | 724.4 | 17.5 | -706.9 |
| q21 | 208.3 | 0.0 | -208.3 |
| q07 | 233.1 | 36.2 | -196.9 |
| q11 | 83.2 | 0.3 | -82.9 |
| q02 | 83.1 | 4.1 | -79.0 |

The left and shared terms are the controls: neither moved materially, which is what a
right-side substitution should do.

Coverage is recorded, not inferred. `join_output_left_slice_rows` and
`join_output_right_slice_rows` count rows actually served by the slice, so a take time that
falls with zero rows here means something else moved. Per pair, **21 nodes slice 169.8 M rows**
(168.6 M right, 1.2 M left); across the seven measured pairs that is 147 node-instances and
1.19 G rows. The path is side-agnostic and does fire on left sides - q16, q11 and q05 each
have a left-side slice - it is simply that probe-side selections are the ones that usually
span their whole source.

### The mechanism check

Sliced rows equal, node for node, the `max_run` that the independent locality probe measured
for that same side in an earlier run:

| cell / node | side | sliced rows | probe `max_run` | probe `run_count` |
|---|---|---:|---:|---:|
| q09 0/0 | right | 47,989,007 | 47,989,007 | 7,108,334 |
| q08 0/0 | right | 47,989,007 | 47,989,007 | 705,456 |
| q21 0/0 | right | 30,343,541 | 30,343,541 | 1 |
| q07 0/0 | right | 14,584,179 | 14,584,179 | 1 |
| q02 0/0 | right | 6,400,000 | 6,400,000 | 47,586 |
| q05 0/0 | right | 1,820,547 | 1,820,547 | 1,837,407 |
| q16 0/0 | left | 948,284 | 948,284 | 1 |
| q11 2/3 | left | 240,861 | 240,861 | 1 |

All 21 slicing nodes match on the sliced side. Two instruments built for different purposes,
in different runs, agree on which gathers are whole-source runs - that plus the 1.29 M-column
equivalence proof covers both halves of the claim: the right shapes, and the right rows.

## 3. What is not claimed

Board distributed elapsed went 173,414 -> 158,803 ms (-8.4%), and this cut is **not** credited
with it. Every cell got faster, including q12, which shed 1,243 ms of elapsed on a right-take
change of 1.35 ms. That is drift between runs, not slicing. The defensible claim is the named
term: 2,178.5 ms of right-side gather, with the coverage counters and the equivalence proof
standing behind it.

## 4. The trap: a task-local aggregate cannot bound a per-invocation opportunity

The pre-cut estimate of the prize was **534 ms across 18 nodes**, derived by joining the
locality artifact to the side-split timings and keeping the nodes whose selection was fully
contiguous - `run_count == 1`. The measured result is 2,178.5 ms, 4.1x that figure. The
implementation was not slicing cases it should not; **the estimate was a floor, and was read
as a ceiling.**

The locality probe accumulates **per node, across every invocation of the gather**. A node
that gathers four times, once over a whole 6.4 M-row contiguous run and three times over
scattered selections, reports `run_count = 47,586` in aggregate and fails a
fully-contiguous filter - while containing exactly the invocation the fast path serves. q02's
stage 0 task 0 is that node literally. q08's and q09's leading nodes are the same shape, and
those two cells alone contributed 1,598.7 ms of the 2,178.5 ms won, both excluded by the
filter that produced 534 ms.

The rule the line leaves behind: **to bound a per-invocation opportunity, the counter must be
per-invocation.** An aggregate over invocations supports a floor and nothing more. `max_run`
survives aggregation only because it is an extremum - which is precisely why it works as the
mechanism check in §2 and does not work as a coverage bound.

## 5. Diagnostics and the authority rule

Two join diagnostics exist, both env-gated and both off by default:
`TORON_JOIN_SELECTION_LOCALITY` (per-side run structure) and `TORON_JOIN_SLICE_EQUIVALENCE`
(slice against `take`). Each performs work the thing it measures is meant to avoid, so a run
with either enabled cannot also time the board.

That is enforced rather than remembered. `TimingAuthorityConditions::diagnostics_disabled`,
fed by the shared `join_diagnostics_disabled()` helper, makes both the timing atlas and the
placement A/B withhold authority with the reason `join_diagnostics_enabled`, and the atlas
header stamps `join_selection_locality_probe` so all-zero counters cannot be misread as "no
contiguity". A new join diagnostic that does not join that helper will silently claim
authority it should not have.

## 6. Index construction, bracketed by eligibility - `65ffd733`

The 1,315.6 ms leftover above is a residual of three activities, and a residual
cannot say which one moved. Two more nested children split it:
`join_output_nonnull_index_build_micros` for the kinds whose selections cannot
carry a null - `Inner`, `Cross`, and the four semi/anti kinds, whose arms accept
only preserved rows and error on everything else - and
`join_output_nullable_index_build_micros` for the outer kinds, which must keep a
null buffer and which no substitution can reach. Both bracket the row loop AND
the `UInt32Array` it produces, because the null-buffer allocation is the subject.

**Eligibility is decided by kind, before any row is seen.** An all-`Paired`
`LeftOuter` still builds a nullable vector, and
`an_outer_gather_without_nulls_still_charges_the_control` fails if that is
charged by observed nulls instead - the mis-charge that would have overstated the
prize while passing every other test in the module.

Artifact `adr554_index_build_split_sf8.jsonl`, build `65ffd733301c`, binary mtime
1788023971, authority granted, 21/21, no replay violations. The four children
clear their parent on **every one of the 1,323 nodes**, checked per node rather
than per cell.

| term | ms | share of parent |
|---|---:|---:|
| left take | 2,351.3 | 57.1% |
| right take | 381.0 | 9.3% |
| index build, eligible | **1,358.0** | **33.0%** |
| index build, control | 5.1 | 0.1% |
| leftover (estimate + batch assembly) | 19.0 | 0.5% |
| parent | 4,114.4 | |

**Index construction was essentially the whole of the unnamed leftover**, and it
is eligible work almost everywhere: the control carries 5.1 ms, spread over q13,
q20, q17 and q02. `RecordBatch::try_new` and the materialization estimate
together account for half a percent.

Two limits on what this licenses, both material to scoping:

- **The control is too small to price the null buffer.** It proves the split
  fires - the board simply runs almost no outer-kind gathers on the charged path
  - but 5.1 ms against 1,358.0 ms supports no comparison of nullable against
  plain construction. What fraction of the eligible figure a `Vec<u32>`
  substitution could remove is UNMEASURED. The bracket also contains the
  `ContiguousRun` observation this line itself added, and the row match; a
  substitution reaches neither. 1,358.0 ms is an upper bound, not a target.
- **A sliced side's index array is built and never read** — `gather_side` slices
  instead — so the slice cut left dead work behind it. That is a mechanism,
  established from the code, and its **magnitude is unknown**.

  The tempting number here is wrong and is recorded as such: 1,275.1 ms of pair
  0's 1,385.9 ms falls on the 21 nodes that slice at least one side, which reads
  as "92% of index construction is discarded" and is not what the counter says.
  It is a per-NODE aggregate. Those nodes also run invocations that slice
  nothing, and every invocation builds BOTH sides while at most one is sliced —
  so the figure bounds where the time sits, never what it was spent on. This is
  §4's trap, one section later, in a record that had just written the rule down:
  a per-invocation opportunity needs a per-invocation counter, and no aggregate
  substitutes for one. The share of index construction that is actually
  discarded stays unmeasured until a counter reports it directly.

Comparability: this run is uniformly slower than the slice board (mean 1.09,
range 0.967-1.214) and level with the side-split board (mean 1.00). Cross-run
term comparisons are therefore refused here and the decomposition above is
within-run. That spread is also the second reminder that the slice board's
elapsed drop was drift, not a win - see "What is not claimed".

## 7. Deferring the index build - `e45f5e75`

§6 established the mechanism - a sliced side's index array is built and never read -
and could not size it. `LazyIndexBuilder` removes the allocation and, in doing so,
measures it: it holds nothing while a whole-source run is still reachable and
materializes the moment one is not, backfilling the deferred prefix from the run's own
state rather than rescanning the selection. Whether a slice is legal stays with
`ContiguousRun::slice_offset`, retention restriction included; the builder decides only
whether an allocation is still avoidable.

The equivalence diagnostic and the deferral are mutually exclusive by construction: that
check compares each sliced column against the `take` it replaced, so it needs the array
the deferral declines to build. `index_deferral_available()` is exactly
`!join_slice_equivalence_enabled()`.

Artifact `adr554_index_deferral_sf8.jsonl`, build `e45f5e75480f`, binary mtime
1788028349, authority granted, 21/21, no replay violations. Comparability against the
`65ffd733` baseline: mean 1.022, range 0.925-1.212.

### The controls, before the result

| control | requirement | measured |
|---|---|---|
| avoided rows only where a side sliced | no node with avoided rows and no slice rows | 0 such nodes |
| avoided rows match the slice | equality is not required, only containment | **exact on all 189 nodes**; board 169,827,032 = 1,248,135 left + 168,578,897 right |
| slice coverage unchanged | same nodes, same rows as the baseline | 0 nodes changed |
| rows walked reconcile | avoided + materialized covers every index row | 169,827,032 + 253,153,029 = 422,980,061, one counter charged per side |

The second row is the strongest of these: `join_output_index_rows_avoided` is written by
the builder and the slice-row counters by the gather, in different functions for
different reasons, and they agree row for row on every node.

### The result

Eligible index build **1,358.0 ms -> 1,124.9 ms, a reduction of 233.1 ms (-17.2%)**,
concentrated where the cut can act. Per-node, pair 0:

| node group | nodes | before | after | delta |
|---|---:|---:|---:|---:|
| slice-bearing | 21 | 1,275.1 | 1,016.0 | **-259.1** |
| zero-slice (drift control) | 168 | 110.8 | 125.3 | +14.5 |

The control did not fall. It rose, on a board running 2.2% slower on average, which is
the expected shape: the deferral cannot act where nothing slices, so a reduction there
would have meant the reduction was not the deferral.

Per cell: q09 -94.0, q08 -92.7, q21 -76.0, q07 -10.3. q02 (+7.7), q05 (+12.9) and q11
(+4.3) moved up, each inside its own drift ratio - q02's cell ran 13.1% slower in this
run. No claim is made on those.

### What this finally sizes, and what it does not

**The discarded share, measured per invocation:** 169.8 M of the 423.0 M index rows the
charged sites walk were being materialized and thrown away - 40.2% of the rows, worth
233.1 ms, or 17.2% of the eligible child. §6's node aggregate would have read as 92%.
The direct counter is a fifth of that, which is the whole reason §4's rule exists.

Board-wide that is **~1.37 ns per avoided row**, and ~2 ns/row on q08, q09 and q21 where
the effect is concentrated. That price is the `Vec` push, the `UInt32Array`, and its null
buffer - everything else the walk does survives, because an avoided row is still walked,
matched and observed for contiguity.

Which bounds the next cut rather than promising it: the plain `Vec<u32>` substitution
applies to the 253.2 M rows still materialized, and it keeps the vector - only narrowing
it from 8 bytes to 4 and dropping the null buffer. Its per-row ceiling is therefore
**below** the 1.37 ns/row the deferral removed by not building anything at all.

## 8. Forward

- **Index construction is 1,124.9 ms** after the deferral took 233.1 ms of it (§7). What
  remains is 253.2 M rows that genuinely need a vector, plus the walk, the row match and the
  contiguity observation, which no substitution reaches. The plain `Vec<u32>` substitution is
  the next candidate against it and its per-row ceiling is below the deferral's 1.37 ns/row,
  since it narrows a vector rather than skipping one — worth pricing in a bench before it is
  built, not after.
- **Carve before adding.** The join root took ~800 lines across the last two cuts and its
  ADR 352 bound was raised twice. The gather machinery — run tracking, lazy build, slice,
  equivalence — is a coherent body with its own tests and belongs in a submodule beside
  `spill` and `streaming`. That carve comes before the next addition to the join root, not
  after it.
- **The left side, 2,189.0 ms**, is intrinsically scattered and cannot be sliced. Before it is
  scoped, §1's unresolved question - why the contiguous side cost more than the scattered one
  - should be settled, because the answer decides whether the lever is locality at all.
- **Input concatenation, 7,146 ms**, is untouched and is now the larger half of the named
  materialization cost. Direct multi-batch join build/probe, eliminating concatenation, is the
  structural project there.
- **Arbitrary contiguous sub-slices** remain available and remain gated on retained-memory
  accounting, per §2.
