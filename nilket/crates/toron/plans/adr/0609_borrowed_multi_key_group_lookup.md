# ADR 609 - borrowed multi-key group lookup in the grouped fold

**Status:** PROPOSED. Successor to ADR 608. On the current-tree board
(ADR 608, sealed: Toron 62.706 s against DuckDB 15.600 s, 4.020×), q01
is Toron 4,752.7 ms against DuckDB 953.0 ms, 4.99×. q01 is join-free,
and ADR 604 attributed its time to the grouped fold. There the fold
builds an owned `String` for each UTF-8 group key on every row. This
record lets the fold hash and compare multi-key groups from the Arrow
arrays by reference, owning a key only when it inserts a new group. It
is one token, measured by a pair on q01 and a pair on one breadth cell.
It projects no saving.


## Context

**q01's attribution** (ADR 604 census, pre-606 executable; route
`scan_grouped`; stage totals reconciled):

| stage | µs |
|---|---|
| `stream_pull` (parquet decode 660,972) | 712,918 |
| `projection` | 186,409 |
| `fold` | 3,574,043 |
| of which `arg_eval_and_readers` | 919,724 |
| of which `row_lookup_update` | 2,643,267 |
| of which `group_expr_eval` | 6 |

DuckDB's `HASH_GROUP_BY` on q01 takes 433 ms (D1 profile). That is
diagnostic context, not a comparable rate: the two engines' operator
boundaries differ.

**The code.** Both grouped row loops take the same per-row owned-key
path: `grouped_aggregate` (`exec/aggregate.rs`, the generic streaming
fold) and `fold_bound_grouped_aggregate_batch` (the `scan_grouped` fold
q01 takes). On each row they do the following:
- clear a reused `Vec<GroupValue>`;
- push `GroupArrayReader::value(row)` for each key column. For UTF-8
  that is `GroupValue::Utf8(array.value(row).to_string())`, one heap
  allocation per UTF-8 key per row;
- call `BoundGroupedAggregateGroups::entry_or_init`, which hashes the
  owned slice (`RandomState::hash_one(&[GroupValue])`) and compares it
  with the stored key.

q01 groups on `l_returnflag` and `l_linestatus`, both UTF-8. By a
code-path estimate (2 keys × ~47.3 M folded rows, not an allocator
reading), that is about 95 M `String` constructions for 4 groups.

**The precedent.** ADR 494 added a borrowed lookup for exactly one UTF-8
key column (`entry_or_init_single_utf8`, `hash_single_utf8_key`). Its
hash reproduces the owned one-element slice's derived hash: the slice
length, then the `GroupValue` discriminant, then the string. A focused
test pins that identity. ADR 494's same-binary A/B on a 3-group,
single-key lineitem fold measured −24.6% (5 of 5 pairs). That was not
this campaign's paired procedure and is cited as mechanism precedent
only. Multi-key shapes, q01's included, never reach that path.

**What is not known.**
- How much of `row_lookup_update` is the owned construction, rather
  than hashing, probing, and the aggregate update. The pair measures
  the whole effect, and a stage receipt reads the residual.
- Which other cells fold many rows through multi-key UTF-8 groups.
  - q18 is not eligible on ADR 604's attribution. Its dominant
    aggregate (1,984 ms, `scan_grouped`) groups on one Int64 key, which
    has its own storage path. Its five-key aggregate is an
    `uncovered_generic_fold` with no stage attribution.
  - The breadth cell is chosen by receipt, below.


## Decision

*The token.* `GroupKeyLookup { Owned, Borrowed }`, a `const` read by both
row loops. `Owned` is today's path. Under `Borrowed`, a key set with
**two or more columns, at least one of them UTF-8**, is looked up
without constructing owned values, and everything else keeps its
current path:
- the single-UTF-8 path (ADR 494);
- the single-Int64 storage;
- numeric-only key sets, whose reused key buffer allocates nothing;
- `STRING_AGG`'s keys.

*The borrowed key.* `GroupKeyRef<'a>` mirrors `GroupValue` variant for
variant: `Null`, `Boolean(bool)`, `Int32`, `Int64`, `Float32(u32)`,
`Float64(u64)`, `Date32`, `Timestamp(TimeUnit, i64)`, `Utf8(&'a str)`,
and `Decimal128(i128)`. A reader method produces it per row with the
same null test and the same canonicalization as `GroupArrayReader::value`
(`canonical_group_f32_bits`/`_f64_bits` for floats), into a reused
buffer.

*Semantics, which must equal today's exactly:*
- **Hash.** It reproduces the derived `Hash` of the owned
  `[GroupValue]` slice: the slice length, then per element the
  `GroupValue` discriminant and its payload hashed as the owned payload
  hashes. For `&str` that is `str`'s `Hash`, which `String` delegates
  to. For `Timestamp` it is the unit, then the value. Table growth
  rehashes stored owned keys, so an equal key must hash equally either
  way. A test pins owned/borrowed hash identity for every variant:
  `Null`, both booleans, integer extremes, float `NaN` payloads, `-0.0`
  against `0.0`, every timestamp unit, empty and non-ASCII UTF-8, and
  `Decimal128` extremes.
- **Equality.** Element-wise against the stored `GroupValue` slice:
  - `Null` equals `Null`, since grouping puts NULLs together, as the
    derived `PartialEq` does today;
  - floats compare by their canonical bits;
  - UTF-8 compares by bytes. There is no collation: the owned path
    compares `String`s by bytes.

  Mismatched variants are unequal.
- **Insert.** On a miss, owned `GroupValue`s are built from the borrowed
  key once, and the existing `entry_or_init` insert runs unchanged:
  resident accounting, growth budget, growth census, quantile
  admission, the probe profile, and initialization with the same
  identity bytes. The storage's own copy on insert (`push`, which
  clones the slice) is the same under both forms.
- **Lookups, logical and physical.** As in ADR 494's path:
  - a borrowed hit records one lookup in the probe profile;
  - a borrowed miss records nothing, and falls through to
    `entry_or_init`, whose own lookup is recorded.

  So the profile counts one logical lookup per row under both forms.
  On a miss, `Borrowed` makes two physical lookups: the borrowed probe,
  then `entry_or_init`'s. Misses equal inserts, so the extra probes are
  as many as the groups.
- **Output.** `finalize_sorted` is unchanged, so group order, values,
  and output are identical.

*The diagnostic receipt.* The group table persists across batches, so
the counters live on `BoundGroupedAggregateGroups`, one set per
aggregate instance, accumulated over all its fold calls. With
`TORON_GROUP_KEY_LOOKUP_RECEIPT_PATH` set, each instance appends one
JSON line when it finalizes. Without it, the counters are not
incremented, and nothing is written. The line holds:
- the function;
- the node attribution route. It is written as `null`: node attribution
  exposes no route getter, and routes exist only while profiling. This
  is an implementation amendment, and the function name identifies the
  loop;
- the key column types;
- the UTF-8 key count;
- the form;
- the path taken: `borrowed_multi`, `single_utf8`, `single_int64`,
  `owned_generic`, or `string_agg`. The last is a fold with a
  `STRING_AGG` call whose key set would otherwise be `borrowed_multi`.
  It keeps the owned lookup under both forms, so ADR 609 leaves such
  folds untouched;
- `rows_folded`;
- `groups_final`;
- `lookup_key_values_built`: the owned `GroupValue`s built to form
  lookup keys, NULLs included. Under `Owned` on the multi-key path this
  is exactly rows × key columns. Under `Borrowed` it is exactly
  misses × key columns. The storage's insert copy is excluded, since it
  is the same under both forms;
- `lookup_utf8_strings_built`: the owned UTF-8 `String`s among those,
  one per **non-null** UTF-8 key value (a NULL builds no `String`).
  - Under `Owned` it is at most rows × UTF-8 key columns, with equality
    exactly when no UTF-8 key value is NULL.
  - Under `Borrowed` it is at most misses × UTF-8 key columns, with
    equality exactly when no inserted key has a NULL UTF-8 value;
- `misses`.

These are code-path counters, not allocator readings. q01's ~95 M
figure is this `lookup_utf8_strings_built` under `Owned`, by estimate
until the receipt reads it. A receipt write error is returned, not
swallowed.

*The pre-pair gate.* One run of each of the 22 queries per arm on
strix, at the ADR 608 coordinate, with the receipt, and each arm's
digests checked against ADR 608's sealed cells. It **fails closed**, and
no pair runs, unless all of these hold:
- every digest matches on both arms;
- q01's aggregate instance reports `borrowed_multi` under `Borrowed`
  and `owned_generic` under `Owned`, with equal `rows_folded` and
  `groups_final` between the forms;
- under `Owned`, q01's `lookup_key_values_built` equals
  `rows_folded` × 2;
- under `Borrowed`, q01's `misses` equals `groups_final`, and
  `lookup_key_values_built` equals `groups_final` × 2;
- both of q01's keys are UTF-8, so `lookup_utf8_strings_built` equal to
  `lookup_key_values_built` establishes that no q01 key value is NULL.
  Only then is q01's exact `String` product (`rows_folded` × 2 under
  `Owned`, `groups_final` × 2 under `Borrowed`) stated. Otherwise the
  gate reports the difference as the NULL count, and states the
  inequalities above.

For every cell, the gate sums `rows_folded` over all of that cell's
`borrowed_multi` instances. The **breadth cell** is the non-q01 cell
with the largest sum, provided that is at least 1 M rows, and provided
its receipts satisfy the form relations, NULLs allowed. Those are:
- `rows_folded` and `groups_final` equal between the forms;
- `lookup_key_values_built` exact: rows × keys under `Owned`, and
  misses × keys with misses = `groups_final` under `Borrowed`;
- `lookup_utf8_strings_built` within its bounds.

Both its digests must also pass. Otherwise no breadth pair runs, and the record says so.

*The pairs.* ADR 597's paired driver, as in ADR 606 and ADR 607:
- control `Owned`, treatment `Borrowed`, differing by the one token;
- `--sealed-manifest target/tpch/adr608-board/perf_board.jsonl.evidence.sealed`;
- ADR 592's validator on every row, ADR 593's readings, ADR 595's
  separated launch, and ADR 599's gate with `--gate-settle-seconds 5`;
- one attempt per cell: first q01, then the breadth cell.

Peak RSS, with its label, and spill are reported per row.

*The rule and its interpretation, predeclared.*
- **q01 decides the token** by ADR 588's rule. `Borrowed` is retained
  only if q01's median paired gain exceeds `2 × range_control` and
  every q01 pair is strictly positive. Otherwise the closing commit
  reverts the token to `Owned`.
- **The breadth cell is coverage.** It is established if it clears the
  same bar, and not established otherwise.
- A breadth regression opens a new record on the token and is never a
  silent revert. That means a digest mismatch, spill on a treatment row
  only, or a slower treatment by more than `2 × range_control` with
  every pair negative.

*The mechanism check, after the verdict.* On the treatment, q01 run
with node attribution: `row_lookup_update`, `arg_eval_and_readers`, and
`fold` against control's, together with the receipt counters. The
residual in `row_lookup_update` informs the batch group-id record that
follows.


## Acceptance

- Fixtures:
  - owned/borrowed hash identity for every variant, as listed;
  - equality over the same set, including mismatched variants and
    NULLs;
  - a multi-key UTF-8 fold under both forms giving identical groups,
    states, and sorted output;
  - inserts only on misses: over a multi-batch fold, `misses` equals
    `groups_final` and `lookup_utf8_strings_built` equals
    `groups_final` × the UTF-8 key count under `Borrowed`, and
    rows × that count under `Owned`, when no key value is NULL. With
    NULL keys, the key-value products stay exact and the `String`
    counts fall below their bounds by the NULL count (a fixture folds
    two NULL UTF-8 values: 22 values and 20 `String`s owned, 10 and 9
    borrowed);
  - the probe profile counts one logical lookup per row under both
    forms;
  - growth across a rehash under `Borrowed`, with lookups still hitting
    afterwards;
  - resident bytes identical between forms;
  - path classification for each shape named above;
  - the receipt's shape, and its unwritable-path error.
- The token asserted by diff: one insertion, one deletion.
- The ladder green on bigfoot on both trees (Toron and toron-tpch, fmt
  clean).
- The gate's receipts per arm, or its refusal.
- The q01 pair, then the breadth pair (or the statement that none is
  eligible), each with twelve rows, readings, separation and window,
  the range and bar, per-pair gains, and the verdict or interpretation.
- The mechanism check.
- Plan commit, then one implementation, results, and closure commit.


## Guardrails

- Only the multi-key UTF-8 path changes. Single-key, single-Int64,
  numeric-only, and `STRING_AGG` paths keep their code under both
  forms.
- Group semantics are unchanged: NULL grouping, float canonicalization,
  byte equality, sorted output. Digest parity on all rows and gate
  runs guards them.
- ADR 588's rule, the twelve-row contract, and ADR 599's gate, verbatim.
  One attempt per cell; no relaunch.
- The batch group-id and columnar-accumulator change is not in this
  record. It is planned after this one closes.


## Risks

- The owned construction may be a small part of `row_lookup_update`,
  with hashing the enum slice and the per-aggregate state match
  dominating. Then q01's gain may sit under the bar, and the token
  reverts. The mechanism check still records the residual for the next
  record.
- A hash mismatch between the borrowed and owned forms would split
  groups after a rehash. The identity fixtures, the rehash fixture, and
  digest parity on every row guard it.
- `exec/aggregate.rs` may approach an architecture size guard. If it
  does, the borrowed key and its hashing go in
  `exec/aggregate/group_table.rs` or a new submodule, stated in the
  implementation.


## Forward

- Retained or not, the q01 residual informs a separate plan: batch
  group-id assignment with typed columnar accumulators, one pass per
  batch per aggregate, with no per-row `AggState` dispatch.
- Scan-side filter and projection fusion, q01's remaining 0.9 s
  against DuckDB's 0.5 s, stays a later candidate.
