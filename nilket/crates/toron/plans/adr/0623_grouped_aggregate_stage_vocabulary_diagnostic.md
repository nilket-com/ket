# ADR 623 - stage vocabulary for the primitive scan-grouped and fallback grouped folds: a diagnostic

**Status:** CLOSED, LIMITED (results in
`plans/impl/0623_grouped_aggregate_stage_vocabulary_diagnostic_results.md`).
The instrument landed, and the 22-cell sweep is compatible apart from
the declared new contracts. q18's inner aggregate (primitive Int64 SUM)
is attributed: `fold` 1,617.9 ms of a 1,873.3 ms handler, and
`stream_pull` 241.2 ms. The outer aggregate's breakdown is withheld
because its bridge fails: the fallback handler takes about 0.37 ms,
while about 1.64 s of the Aggregate node lies outside it, as dispatch
residual. Successor to ADR 622. **A diagnostic record: no token, no
pair, no cut, no saving projected.**

ADR 622's sealed board ranks q18 second by absolute gap: Toron 6,022.0
ms against DuckDB 1,489.0 ms, a gap of 4,533 ms. ADR 614 found q18's time
**outside any stage vocabulary** (on ADR 613's executable, five runs).
Its two aggregate invocations are unreconciled, and the whole 5,977.7 ms
was uncovered:

| ADR 614 owner | ms |
|---|---|
| `scan_grouped` invocation (no stage vocabulary) | 1,867 |
| `uncovered_generic_fold` invocation (no handler span) | 1,645 |
| aggregate node kind, 2 calls | 3,515.7 |
| exchange node kind, 8 calls | 1,133.6 |
| join node kind, 3 calls | 839.7 |
| scan node kind, 3 calls | 378.9 |

DuckDB context only: q18's `HASH_GROUP_BY` was 1,050 ms, with
12,000,000 rows in. ADR 614 named the consequence: **q18 needs stage
vocabulary on its `scan_grouped` and generic-fold handlers before its
aggregate time can be decomposed.** This record supplies that
vocabulary and measures q18 with it.


## Context (from code; to be confirmed by the armed runs)

**Why `scan_grouped` has no vocabulary.** `try_streaming_scan_grouped_aggregate`
(`exec/aggregate.rs`) tries two primitive paths before its generic loop:
- the primitive Int64-key SUM, `execute_primitive_int64_sum_grouped_scan`;
- the primitive Int64-key COUNT, `execute_primitive_int64_count_grouped_scan`.

Only the generic loop declares a vocabulary (`scan_grouped_generic*`:
`stream_pull`, `projection`, `repartition`, `fold`, `finalize`). The
code comment there says the primitive paths "have their own lifecycles
and no stage instrumentation", and will report missing attribution
until they get a vocabulary of their own. q18's inner aggregate has an
Int64 key (`l_orderkey`) and sums a Decimal128 (`l_quantity`). From code,
it is expected to take the primitive SUM path. The armed runs confirm
which path it takes; this record does not assume it.

The primitive SUM path's natural, disjoint boundaries, none spanned
today:
1. scan request and open;
2. `stream.next().await`: the pull, including the parquet decode;
3. the row-limit slice and `apply_streaming_scan_input_ops`;
4. the fold: `accumulate_aggregate_batch`, i.e. the key and value
   downcasts and the per-row `groups.insert`;
5. the finish: `record_primitive_int64_sum_profile` (inert unless its
   own profile variable is set) and `groups.finish`.

The COUNT path has the same shape, finishing with
`finish_with_retained_cap`.

**Why the second invocation is `uncovered_generic_fold`.** When
`try_streaming_grouped_aggregate` declines, the executor (`exec.rs`)
runs the input node and then `bound_grouped_aggregate`. Nothing on that
fallback records a route, opens a handler span or declares a
vocabulary. So the invocation's exclusive time is uncovered, while its
bridge still passes as `uncovered_generic_fold`.

Its phases:
- the per-batch fold, `fold_bound_grouped_aggregate_batch` (ADR 610's
  two-phase typed fold, `AllTyped`);
- `finalize_bound_grouped_aggregate`.

The fold already carries refinement sub-stages (quantile admission,
`group_expr_eval`, `arg_eval_and_readers`, `row_lookup_update`). They
attach through `current_handle_requiring("fold")` whenever an active
vocabulary requires `fold`, and they are published only then.

**Gating.** Every node-attribution API returns early unless
`TORON_NODE_ATTRIBUTION_PROFILE_PATH` was set at process start. That is
a `OnceLock` read and an `Option` branch. Unarmed, the new spans do no
timer work.


## Decision

*The instrument: two vocabularies, opened only when node attribution is
armed.*

**1. Primitive scan-grouped vocabularies.** Each primitive path
declares its own single-phase vocabulary, like the generic loop does.
It uses `declare_stage_vocabulary`, not the two-phase commit, so task
receipt labels are untouched. The route stays `scan_grouped`. The
vocabulary name identifies the path:
- `scan_grouped_primitive_sum` and `scan_grouped_primitive_count`;
- required stages: `scan_open`, `stream_pull`, `input_ops`, `fold`,
  `finalize`;
- residual boundary 500 ms, the generic loop's.

Stage boundaries, all batch-level (no per-row token):
- `scan_open`: the scan request and `open_scan`;
- `stream_pull`: each `stream.next().await`;
- `input_ops`: each batch's row-limit slice and
  `apply_streaming_scan_input_ops`;
- `fold`: each batch's `accumulate_aggregate_batch` closure, opened
  inside it as in the generic loop. The batch-local span deliberately
  overlaps the task-telemetry accumulation counter; only the counter
  feeds the task-wall partition, as documented for `scan_grouped`;
- `finalize`: the profile record and `groups.finish` (or
  `finish_with_retained_cap`).

Repeated per-batch stages accumulate, as the generic loop's do.

**2. A route and vocabulary for the fallback grouped fold.** The
executor's fallback call to `bound_grouped_aggregate` is replaced, on
one line (`exec.rs` has 4 lines to spare under its architecture bound),
by a call to a new wrapper in `aggregate.rs`. The wrapper:
- opens a handler span and records the route `bound_grouped_fold`;
- declares the single-phase vocabulary `bound_grouped_fold`, with
  required stages `fold` and `finalize` and a 500 ms residual boundary;
- runs the same fold loop and finalize as `bound_grouped_aggregate`,
  with `fold` around the batch loop and `finalize` around
  `finalize_bound_grouped_aggregate`. Initialization, fold, finalize,
  quantile seeding and error ordering stay identical. There is no
  diagnostic template and no extra state construction;
- closes the handler span.

The existing fold refinements then publish under `fold`.

*Implementation adjustment (reviewed).* `bound_grouped_aggregate` is
not left literally unchanged. The production caller audit found the
executor fallback was its only caller outside tests. So the original
body is moved, not duplicated, into one shared method,
`staged_bound_grouped_aggregate(…, Option<&VocabularyHandle>)`:
- it keeps the same group owner, batch order, fold and finalize;
- its stages are inert without a vocabulary.

Two methods call that body:
- the wrapper, `attributed_bound_grouped_aggregate`, which is now the
  only production caller;
- `bound_grouped_aggregate`, which becomes a `#[cfg(test)]` delegate
  passing `None`.

ADR 435's syntactic fold and finalize site counts are unchanged. The input node runs before the handler span opens, and its
time stays the child's.

**What does not change.** No result, plan or route choice changes:
- no kernel, eligibility, admission or memory decision;
- no new allocation on the unarmed path.

The only route change is the intended one, that fallback invocations
report `bound_grouped_fold` instead of `uncovered_generic_fold`.
`exchange` and `join` node time stays node-level. This record adds no
vocabulary there.

*Fixtures* (unit, unarmed where node attribution cannot be armed
in-process, as in ADR 615):
- the new vocabularies' required sets, residual boundaries and names;
- a reconciliation fixture in which `fold` and `finalize` cover a
  handler, and in which a missing `finalize` fails closed;
- the fold refinements attach to the new `fold` stage (and to no
  other);
- the primitive SUM and COUNT results stay equal to the generic grouped
  fold (the existing parity tests);
- the fallback wrapper's output equals `bound_grouped_aggregate`'s on
  the existing grouped-fold fixtures;
- the updated pins:
  - the `uncovered_generic_fold` status fixture, which is kept for a
    route-less handler;
  - `static_scan_grouped_declarations_keep_their_behaviour`;
  - any census reader that names routes.

*Architecture.* `exec.rs` stays within its line bound by a one-line
call-site change. No query token appears in profiler or exec sources.
Any new module goes on the decomposition allowlist.

*The ladder and build.* Toron and toron-tpch `--no-fail-fast` and fmt
on bigfoot, then a warning-free release build recorded in
`target/tpch/evidence/build/adr623-instrumented.manifest`. Staged and
hash-verified on strix.

*Instrumentation compatibility: a 22-cell armed sweep,* one run per
cell, under both ADR 622's measured executable (`18df5747…`) and the
instrumented one, as ADR 615's. It establishes compatibility on the
observed outputs, plans, routes and statuses. It does not establish full
behaviour neutrality: digest and status parity cannot rule out timing,
codegen or error-order effects. Those are preserved by code review of
the actual kernels, allocations, admission checks and error paths, and
by the unarmed parity fixtures. Unarmed timing overhead stays
unmeasured, even though the clock and lock work is avoided by
construction. Per cell it requires:
- digest and plan fingerprint equal to ADR 622's sealed cell, and status
  pass;
- identical aggregate invocation counts and node topology;
- identical routes, except `uncovered_generic_fold` →
  `bound_grouped_fold`;
- identical reconciliation statuses, except on invocations whose
  vocabulary name is one of the three new names
  (`scan_grouped_primitive_sum`, `scan_grouped_primitive_count`,
  `bound_grouped_fold`) and whose old form had no vocabulary. The
  exception is tied to those names and invocations, not to every
  `scan_grouped` route;
- existing vocabularies, including every generic `scan_grouped_generic*`
  invocation, keep their status and their stage and refinement topology;
- no newly failed, missing or orphaned stage on any other invocation.

Each newly instrumented invocation's status, and any missing, failed or
orphaned stages, are reported explicitly. The exception does not hide
damage to an existing vocabulary.

Every sweep row is bound to:
- its executable, `full_hash` and the dataset fingerprint;
- the coordinate;
- the digest and plan fingerprint of ADR 622's sealed cell.

The sweep is not a timing run, and no latency comparison is made from
it.

*The runs.* On strix, at ADR 622's coordinate, on the instrumented
executable: **five armed q18 runs**, each gated on:
- digest and plan fingerprint equal to ADR 622's sealed q18 cell;
- status pass, `full_hash`, the coordinate;
- the clean-environment wrapper, with only node attribution armed;
- the instrumented executable's identity.

*The observation interval.* The runs use the same parent and harness
execution path as the board, where available, and record labelled peak
RSS. Where that path is not available, direct-child RSS is named
unavailable and not inferred.

They run in ADR 595's separated launch, one attempt, with fresh output,
the fail-closed scratch and leftover checks, and the scripts staged
before the launch. There is no SSH to strix during the run.

*Publication rule.* In the five q18 runs, a discovered intended
invocation's breakdown is published only if its required stages and
refinements reconcile. If any cannot, that breakdown is withheld, and
the record closes as limited or incomplete with the reason. It is never
given a forced passing label or summarised silently.

*Comparing across runs.* Stage figures are compared across the five
runs only after these agree:
- the invocation routes and vocabulary names;
- the node and invocation topology;
- the full stage sequences;
- the refinement-parent identities.

Repeated stage names are kept apart by invocation and ordinal. Parent
stages and their refinements are separate levels: they are never summed
together, and child node spans are not double-counted. Each handler's
stage residual is shown separately from the node-to-handler bridge
residual, and both are checked before coverage is claimed. Component
medians need not sum to the median total.

*The report, per run reconciled first, then median and range:*
- **Routes and paths:** each aggregate invocation's route and
  vocabulary name. This confirms or refutes the expected primitive SUM
  path, and names the fallback fold.
- **Per invocation:**
  - handler time;
  - each stage's time;
  - the fold refinements;
  - the named residual;
  - `arithmetic_reconciled`, `within_residual_boundary` and `passed`.

  Each run's stages plus residual equal the handler exactly, with the
  identity gap stated.
- **Node kinds:** exchange, join, scan and filter, as context. They
  stay unattributed.
- **The armed-versus-board difference:** the armed q18 median beside
  ADR 622's board cell, as an observed difference. It may be instrument
  overhead, other run variation, or both, and is not apportioned.
- DuckDB's `HASH_GROUP_BY` (ADR 614's profiling) as historical context,
  not as a new like-for-like timing.

Every figure is read from the artifacts by a fail-closed reader with
offline fixtures, including negative controls. A missing or malformed
field withholds the run. Provenance (paths, reader and source SHA-256s,
attempt tokens) is recorded.


## Acceptance

- The instrument and fixtures above. Ladders green, fmt clean, the
  release build warning-free.
- The compatibility sweep passing its criteria.
- Five gated armed q18 runs, reconciled and reported.
- Plan commit, then one implementation commit with the instrument, the
  results and the closure. The instrument stays in the tree.


## Guardrails

- A diagnostic: no token, no pair, no saving projected. A stage's size
  does not choose a mechanism. The successor's plan does that, from code
  and targeted evidence.
- Behaviour neutrality is established by the sweep and the parity
  fixtures, not assumed.
- Batch-level stages only; no per-row timer.
- ADR 622's board remains the baseline. The instrumented executable is
  not a board executable, and no board figure is restated from these
  runs.


## Risks

- **The path differs from code reading.** If q18's inner aggregate
  takes the generic loop or the COUNT path, the report names the path
  that ran. The generic loop's existing vocabulary then applies.
- **The fallback residual.** Work in the executor's Aggregate node
  outside `bound_grouped_aggregate` (beyond the input child) would show
  as a named residual, and possibly a bridge residual above 10 ms. It is
  reported as observed.
- **Armed overhead.** Per-batch stage spans add lock and clock work when
  armed. The armed medians are diagnostic, and are not compared with
  the board as latency.
