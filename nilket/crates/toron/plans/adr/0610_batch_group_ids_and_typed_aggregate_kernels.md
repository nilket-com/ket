# ADR 610 - batch group ids and typed aggregate kernels in the grouped fold

**Status:** CLOSED, RETAINED on q01, with a known q13 regression
pending ADR 611 (results in
`plans/impl/0610_batch_group_ids_and_typed_aggregate_kernels_results.md`).
- **q01:** 4,087.1 ms control against 3,325.1 ms treatment, a median
  paired gain of 754.9 ms (18.5%) against a bar of 24.7 ms. Every pair
  was positive, with digest parity and no material RSS difference
  observed.
- **q13 breadth pair:** the regression guard fired: a median paired
  gain of −122.4 ms against a bar of 76.3 ms, with all five pairs
  negative and RSS about 2.7 MB higher. That opens ADR 611.

The token stays `TwoPhase`. Successor to ADR 609, which retained borrowed
multi-key lookup: q01 went from 4,524.0 ms to 3,810.5 ms, a median
paired gain of 713.4 ms. Before this record, the grouped fold (this
record's control) processed one row at a time. For each row it looked
up the group, and then for each aggregate it:
- matched the state;
- read the input through a per-row type match into a `NumericValue`
  enum;
- updated through a second match on the state and value pair.

This record splits the fold into two phases per batch, as one token:
1. assign every row its group id;
2. update each aggregate with a loop specialized once per batch to its
   state kind and input type.

It projects no saving.


## Context

**Where q01's row loop spends, after ADR 609.** Diagnostic evidence
only:
- the ADR 508 method: `perf record -F 999 -e cpu-clock` on a
  symbol-rich `profiling` build of main `74d60c63`;
- five q01 runs on strix at the ADR 608 coordinate, each with node
  attribution;
- samples summed by period (CPU nanoseconds), per run.

Mean `row_lookup_update` in these runs is 2,193 ms. The profiling build
is slower than release and is not compared with it.

| leaf, within the row loop | ms/run | what it is |
|---|---|---|
| `fold_bound_grouped_aggregate_batch` (inlined body) | 1,310 | the row loop itself: the lookup inlined (borrowed-key hash, `matches`, bucket probe), per-row `match input`/`match state` dispatch, and loop control |
| `NumericArrayReader::value` | 438 | per row, per aggregate: an 11-arm null test, an 11-arm type match, a `NumericValue` construction |
| `add_sum_value` + `add_avg_value` | 409 | per row, per aggregate: a 20-arm match on `(state, value)`, then a checked `i128` add |

The single hottest source lines are all dispatch:
- `match state` in the loop (132 ms);
- the `(state, value)` matches in `add_avg_value` and `add_sum_value`
  (113 ms and 107 ms);
- the reader's type matches (75, 64, and 54 ms).

Lookup lines (`hash_into`, `matches`, the bucket read) total roughly
200–300 ms. These are shares of sampled CPU by leaf and line, not a
paired decomposition. The dispatch around seven aggregates per row is
the larger part, and the lookup the smaller.

Outside the row loop, `arg_eval_and_readers` (916 ms/run) is mostly
arrow's Decimal128 arithmetic for q01's derived arguments
(`try_binary_no_nulls`, 712 ms). There are smaller costs from building
a `Decimal128Array` from `Vec<Option<_>>` (118 ms) and from
`literal_array` (65 ms). This record does not touch them.
(`stable_file_hash`, 2,220 ms/run, is the harness verifying the dataset
before execution, outside `execute_ms`.)

**q01's aggregates** are SUM ×4 over Decimal128 (`SumInt`, `i128`), AVG
×3 over Decimal128 (`AvgDecimal`), and `COUNT(*)`. All three kinds are
among the kernels below.


## Decision

*The token.* `GroupedFoldShape { RowAtATime, TwoPhase }`, a `const` read
by `fold_bound_grouped_aggregate_batch`. `RowAtATime` is today's loop,
unchanged. The unbound `grouped_aggregate` is not changed.

*Eligibility, per batch.* `TwoPhase` applies to a batch only when
**every** aggregate in the call has a typed kernel (the table below)
for its state and input type. Otherwise the whole batch runs
`RowAtATime`, today's loop. So a batch with any per-row aggregate
never mixes shapes: MIN, MAX, spread, `STRING_AGG`, quantile, the
scalar-subquery value, or a type without a kernel. q01's eight
aggregates are all eligible.

*Phase 1: group ids.* The existing lookup paths, unchanged in
semantics: single-UTF-8, single-Int64 storage, borrowed multi-key, and
owned generic. Each gets an index-returning form that returns the
entry's index instead of `&mut Vec<AggState>`. The index forms keep
everything else unchanged:
- one logical profile lookup per row;
- ADR 609's receipt counters;
- miss-only initialization, with the same identity bytes, in row
  order;
- the u32 entry domain with `EMPTY_BUCKET` reserved, as `entry_or_init`
  enforces it today.

Indices are stable through growth: `states` and `keys` are append-only,
and growth rehashes only the buckets.

*The group-id buffer.* One `Vec<u32>`, owned by the group table and
reused across its batches. Capacity is requested fallibly
(`try_reserve_exact`, refused with a table error as other allocations
are) for the batch's row count. The requested size is 4 bytes per row,
32 KiB at 8,192 rows. `try_reserve_exact` does not promise that
capacity equals the request. So the guard checks the table's resident
bytes plus the request before reserving, and the **actual allocated
capacity** afterwards. The actual capacity bytes are what the resident
accounting holds, and the receipt reports the observed peak
capacity.

*Phase 2: one kernel per aggregate per batch.* For aggregate `i`, a
kernel is selected once per batch from:
- the state variant, which every group's `states[i]` shares, since
  `init_bound_states_with_seed` builds it from the call alone;
- the input's `NumericArrayReader` type;
- whether the input has nulls.

The typed kernels:

| state | input | per row |
|---|---|---|
| `SumInt` | Decimal128, Int8–64, UInt8–64 | `total = total.checked_add(v as i128)`, "integer SUM overflow"; `seen = true` |
| `SumFloat` | Float32/64, or an integer type | `total += v as f64`; `seen = true` |
| `AvgInt` / `AvgDecimal` | integer / Decimal128 | total `checked_add` with the same message per kind; `count.checked_add(1)` "AVG count overflow" |
| `AvgFloat` | Float32/64, or an integer type | `total += v as f64`; `count.checked_add(1)` |
| `Count` | `COUNT(*)` | `+= 1` |
| `Count` | `COUNT(x)` | `+= 1` when `x` is non-null |

Each kernel reads the typed values slice once. With `null_count() == 0`
it skips the per-row validity test. Otherwise it skips null rows, as
today (a null contributes nothing).

*Semantics that must equal today's.*
- **Results.** Integer and decimal sums are exact, and associative in
  `i128`: the same totals whatever the update order. Float SUM and AVG
  add the **same values in the same order per group**. Row order within
  a batch is preserved per group, and batches arrive in the same order,
  so each group's float total is bit-identical.
- **Errors.** An existing aggregate or table error keeps its kind and
  message when it is the first error raised. `TwoPhase` adds one new
  refusal: the group-id buffer's reservation or its budget guard. A
  batch that fits today's budget can be refused under `TwoPhase` when
  the buffer's bytes tip it over the budget. **Which error is reported
  first** can also differ within an eligible batch, in two ways, since
  per-row aggregates never run under `TwoPhase`:
  - **initialization against update:** phase 1 inserts every new group
    of the batch before any update. A later row's insert failure
    (growth budget, allocation, the u32 entry domain) can therefore be
    reported where today an earlier row's update overflow would have
    been;
  - **aggregate order:** phase 2 is aggregate-major. When two
    aggregates would overflow in the same batch, the error may name a
    different aggregate.

  Both are fixtured and stated. Both are limited to queries that fail
  under either shape. The buffer refusal is not: it is fixtured as a
  batch that succeeds under `RowAtATime` and is refused under
  `TwoPhase` with the named table error.
- **Counters and profile.** The probe profile counts one logical lookup
  per row, as before. ADR 609's receipt counters are unchanged: phase 1
  does the lookups. `record_fold_batch` still counts rows and
  aggregates.
- **Attribution.** `row_lookup_update` brackets both phases, so the
  stage vocabulary and reconciliation are unchanged.

*A diagnostic receipt.* With `TORON_GROUPED_FOLD_KERNEL_RECEIPT_PATH`
set, each aggregate instance writes one line at finalize, one file per
arm and query. Without it, nothing counts. The line holds:
- the shape;
- per aggregate: the function, state kind, input type, kernel
  (`typed:<name>` or `per_row`), and the aggregate-row updates through
  each;
- batches under each shape;
- `rows_folded`;
- the group-id buffer's peak bytes.

It is a code-path counter.

*The gate before any pair.* One run of each of the 22 queries per arm,
with the receipt, and digests checked against ADR 608's sealed cells.
It **fails closed** unless all of these hold:
- every digest matches on both arms;
- q01's eight aggregates report, under `TwoPhase`, exactly these
  kernels, each with updates equal to `rows_folded`, with **zero**
  per-row updates and zero `RowAtATime` batches:
  - `typed:sum_int_decimal128` ×4;
  - `typed:avg_decimal_decimal128` ×3;
  - `typed:count_star` ×1;
- under `RowAtATime`, all eight report `per_row`, with equal rows.

A digest alone would miss a partial fallback, and these assertions
catch one.

The breadth cell is the non-q01 cell with the most **aggregate-row
updates** through typed kernels (summed over kernels and aggregate
instances, not distinct rows). That must be at least 5 M, and the
cell's digests must pass. Otherwise no breadth pair runs.

*The pairs.* As in ADR 609:
- ADR 597's driver;
- `--sealed-manifest` bound to ADR 608's board;
- ADR 592/593/595/599 as there;
- one attempt each, q01 first, then the breadth cell.

q01 decides the token by ADR 588's rule. The breadth cell is coverage,
with ADR 607's interpretation and regression guards. Peak RSS, with its
label, and the group-id buffer's peak bytes are reported beside the
timing.

*The mechanism check.* After the pairs, single diagnostic runs per arm
with node attribution. They report `row_lookup_update` and its sibling
stages, with the kernel receipt. They are not a paired decomposition.

*Cleanup in this record's implementation* (behaviour-neutral, disclosed
separately). ADR 609's `GroupKeyLookupPath::classify` compares only
against `GroupKeyLookup::Borrowed`. With the token `Borrowed` on main,
`GroupKeyLookup::Owned` is never constructed outside tests, and the
build warns `dead_code`. `classify` becomes an exhaustive
`match form { Owned => OwnedGeneric, Borrowed => BorrowedMulti }`, and
the implementation shows the warning gone in both arms' builds.


## Acceptance

- Fixtures, `RowAtATime` against `TwoPhase` on the same batches:
  - identical groups, states, and finalized output, for every typed
    kernel and input type, with and without nulls;
  - float SUM and AVG bit-identical per group over multi-batch input;
  - groups created mid-batch initialized once, with their identity
    seeds;
  - overflow raising the same kind's message under both shapes;
  - the two ordering differences:
    - a batch whose later row's insert fails (a constructed growth
      budget) after an earlier row's update would overflow. It fails
      under both shapes, reporting the update error row-major and the
      insert error two-phase;
    - two aggregates overflowing in one batch, with the aggregate
      named under each shape;
  - the group-id buffer under a tight growth budget: `RowAtATime`
    succeeds; `TwoPhase` is refused with the named table error; the
    accounted bytes equal the actual capacity;
  - eligibility: a batch with any per-row aggregate (MIN beside SUM;
    `STRING_AGG`) runs `RowAtATime` whole, and its receipt shows it;
  - the index-returning lookups: growth and rehash with mid-batch
    inserts, ids still addressing the right states afterwards,
    single-UTF-8 and single-Int64 paths included, and profile and
    counters equal to the `&mut` forms;
  - the probe profile's lookup count and ADR 609's counters equal
    between shapes;
  - kernel selection per state and input;
  - the receipt's shape, and its write error.
- The token asserted by diff: one insertion, one deletion, the const
  line only. Both arm trees share the new plumbing and the
  behaviour-neutral warning cleanup.
- Ladders green on both trees on bigfoot, fmt clean, with no warnings
  in either arm's release build.
- The gate's receipts, or its refusal. The q01 pair, then the breadth
  pair or the statement that none is eligible. The mechanism check.
- Plan commit, then one implementation, results, and closure commit.


## Guardrails

- Grouping semantics, float summation order per group, overflow
  detection, NULL handling, and output are unchanged. The one new
  refusal is the group-id buffer's budget guard, stated and fixtured. Digest parity is
  checked on every gate run and every paired row.
- MIN, MAX, spread, `STRING_AGG`, quantile, and scalar-subquery keep
  their per-row code. A batch containing any of them runs today's loop
  whole.
- The state storage (`Vec<Vec<AggState>>`) is not changed in this
  record. A columnar accumulator layout is a possible later record,
  informed by this one's residual.
- ADR 588's rule, the twelve-row contract, and ADR 599's gate,
  verbatim. One attempt per cell.


## Risks

- Cells whose aggregates are not all typed see no change. Coverage is
  narrower than a mixed design, in exchange for today's error order in
  those cells.
- Phase 2 walks `group_ids` and indexes `states[g][i]` per row. With
  many groups, that is a scattered access per aggregate. Few-group
  cells (q01) are cache-resident. Many-group cells may gain less, or
  lose. The breadth pair and its regression guard read it.
- The per-group `Vec<AggState>` indirection stays. If it dominates
  what remains, the columnar layout is the next step.
- The two error-order differences are observable. They are stated,
  fixtured, and limited to queries that fail either way.


## Forward

- A columnar accumulator layout (per aggregate, a typed `Vec` indexed
  by group id), if the residual shows the state indirection.
- q01's argument evaluation (916 ms, mostly Decimal128 arithmetic), and
  the scan decode and projection (≈ 0.9 s): separate candidates.
