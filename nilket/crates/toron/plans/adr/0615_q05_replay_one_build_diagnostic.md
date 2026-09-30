# ADR 615 - q05's replay-1 build: a diagnostic

**Status:** CLOSED (results in
`plans/impl/0615_q05_replay_one_build_diagnostic_results.md`). The
instrument landed behaviour-neutral: the 22-cell sweep is identical
apart from the added refinements. Replay 1's `prep_build`, 3,681.7 ms,
divides into:
- `prep_exchange`, 1,645.5 ms;
- `prep_scan_decision`, 927.0 ms;
- `prep_table_build`, 776.6 ms (matching `build_micros`);
- `prep_input_materialize`, 241.5 ms;
- a residual of 77.9 ms.

The build input is all of `lineitem`: 48.0 M rows under 12 M keys. No
cut. Successor to ADR 614. **A diagnostic record: no
token, no pair, no cut, no saving projected.**

ADR 614 found `runtime_prep` to be the largest recurring observed owner,
9.2 s across q05, q21, and q09. In q05 a single stage dominates:
`prep_build` for replay 1, **3,804 ms** (median of five; range
3,787.9–3,819.2) of q05's 4,956 ms diagnostic execution. That is on
ADR 613's executable and cell. DuckDB's whole q05 is 611 ms, as context
only.

This record establishes what that span does:
- its build input;
- the rows before and after the filter and the exchange, and the scan's
  raw and decoded bytes;
- the hash-table form;
- how the span's time divides between its operations.

It uses stage-scoped evidence only.


## Context

**What the span encloses** (`exec/join/streaming.rs`, the reoriented
chain's prep loop). For each chain stage, `prep_build` (replay-scoped,
under `runtime_prep`) wraps three things:
1. **The scan-build decision.** For q05's wrapped arm this is
   `reoriented_inner_wrapped_stage_scan_build_decision`: the cap check,
   which materializes the build side's scan into batches. Under ADR 606's
   `Reuse`, those batches feed the build.
2. **The exchange.** The build input's `Exchange(parts=4)`, run by
   `exchange_input_batches` (ADR 606) on the reused path, or
   `bound_exchange` otherwise.
3. **The join-table build** from the exchanged batches.

**What ADR 606 recorded for q05's replay 1:**
- build child `Scan` of `lineitem` (TableId 1);
- decision `materialized_batches`;
- path `bound_exchange`;
- partitions 4;
- reused under `Reuse`.

ADR 605's census shows lineitem visited in full, 47,989,007 rows.

**What existing telemetry gives, and what it does not:**
- **The join-build profile** (`TORON_JOIN_BUILD_PROFILE_PATH`), per
  replay: strategy, key plan, build rows, unique, duplicate and overflow
  keys, maximum rows per key, row-list bytes, and **`build_micros`**,
  the table-build part.
- **The grouped-chain profile** (`TORON_GROUPED_CHAIN_PROFILE_DIR`), per
  runtime stage: build, probe, and output rows.
- **ADR 606's cap-check receipt**: the decision, path, partitions, and
  reuse per stage.
- **ADR 605's scan ledger and progress stream**: per scan and table,
  raw rows and bytes, output rows, and decode microseconds. That is
  decode only, not the scan's wall time inside the stage.
- **Not isolated today:**
  - the scan-build decision's wall time, which includes decode, filter,
    and materialization;
  - the exchange's time per replay. `exchange_repartition_micros` is
    reported only per query.

So the 3.8 s cannot yet be divided exactly.


## Decision

*A small diagnostic instrument.* Nested stages under each replay's
`prep_build`, opened only when node attribution is armed, as every
other stage is. They add no work otherwise. They are disjoint and
sequential, with exact boundaries:
- **`prep_scan_decision`:** only the
  `reoriented_inner_stage_scan_build_decision` call inside
  `reoriented_inner_wrapped_stage_scan_build_decision`: the cap check's
  scan and materialization.
- **`prep_exchange`:** only the reuse branch inside that same helper,
  which builds `reused_batches`. On the Reuse path with an `Exchange`,
  that is `exchange_input_batches`.
  - On any other path (the unwrapped arm, `Discard`, the streamed
    filter-exchange attempt), the exchange runs elsewhere and this stage
    is not opened. The vocabulary records it as optional, not missing.
  - q05's four wrapped stages are all on the Reuse path (ADR 606's
    receipt).
- **`prep_input_materialize`:** inside
  `prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline`,
  only the build batch's `concat_or_empty` (already timed per replay by
  `time_join_replay_input_materialization`) and `key_arrays`.
- **`prep_table_build`:** exactly the interval `build_micros` measures,
  from the build's `OperatorTransition` through
  `build_in_memory_join_rows`. So `prep_table_build` and the join-build
  profile's `build_micros` are two readings of the same operation.

Everything else inside `prep_build` stays in its **named parent
residual**:
- the memory-cap and estimate arithmetic;
- the key plan;
- the telemetry and profile publication after the build;
- the runtime's construction;
- the gaps between the helper and the prepare call.

Per run, the four sub-stages plus the residual equal `prep_build`
exactly. The sub-stages are shown not to overlap by their boundaries
and by the per-run identity.

The replay-scoped vocabulary gains a third level (`runtime_prep` →
`prep_build` → sub-stage). If node attribution cannot express a third
level, the implementation adds it, fixtured, and the change to the
vocabulary is stated.

The instrument lands in this record's implementation with fixtures:
- the stages open, close, and reconcile under a chain fold with a
  wrapped, reused, exchanged stage;
- `prep_exchange` is absent, and recorded as optional, on a path
  without the reuse branch;
- an unarmed run executes no timer code;
- every chain route's vocabulary still reconciles.

*Fixture amendment, reviewed before the build.* Node attribution is
armed by its environment variable at process start, so in-process unit
tests cannot arm it. No existing test drives a chain route armed, and
the chain fixture is not known to reach the wrapped Reuse-and-Exchange
path. The armed-chain fixtures above are therefore replaced as follows:
- **The reconciliation extension keeps its unit fixtures:**
  - a third level reconciles to its refinement parent;
  - an ambiguous parent fails closed.
- **The five armed q05 runs are the path evidence.** In every run:
  - all four expected replay IDs have a `prep_build`;
  - each present sub-stage reconciles to its own `prep_build`, with a
    non-negative residual;
  - the per-run identity gap is zero;
  - `refinements_reconciled` is true;
  - any path-dependent optional child is reported by name.
- **A separate 22-cell armed compatibility sweep,** one run per cell,
  with both ADR 613's executable and the instrumented one. It requires,
  per cell:
  - digest and fingerprint parity;
  - identical invocation routes and topology;
  - identical strict and top-level reconciliation statuses;
  - no newly failed, missing, or orphaned stage, apart from the intended
    added `prep_build` children.

  The sweep is not a timing run, and no latency comparison is made
  from it.
- **The unarmed path:** `prep_attribution` derives from the stage
  handle, which is `None` when attribution is off. The existing unarmed
  chain test still passes.

*The cap-check receipt, in an untimed preflight only.* Its equivalence
path re-executes the stage's build (`execute_bound_node`), so it must
not run inside a timed diagnostic. One separate q05 run, with the
receipt and not counted among the timed runs, records per stage:
- the decision, path, partitions, and reuse;
- its digest and fingerprint gate.

**The five timed runs never set
`TORON_REORIENTED_CAP_CHECK_RECEIPT_PATH`.**

*The runs.* On strix, at ADR 613's coordinate, with the new executable
built from this record's implementation tree. **Five runs of q05**,
each gated:
- digest and plan fingerprint equal to ADR 613's sealed q05 cell;
- status pass;
- spill scratch empty.

Each run records node attribution, the join-build profile, the
grouped-chain profile, and the scan progress stream and ledger. Each
run's environment is recorded to show the cap-check receipt unset.

*The report, per run reconciled first, then median and range:*
- **The span:** replay 1's `prep_build`, its four sub-stages, and the
  named parent residual. Each run's sub-stages plus residual equal the
  parent exactly, with the identity gap stated.
- **The build input:** from the untimed preflight's cap-check receipt,
  the grouped-chain profile, and the lineitem scan's ledger entries:
  - table, decision, path, and partitions;
  - rows visited and emitted;
  - raw and decoded scan bytes;
  - build rows after the filter and the exchange.

  **Bytes after the filter and after the exchange are not measured** by
  these sources. They are named as unavailable, not estimated.
- **The table's form:** from the join-build profile's replay-1 record:
  - strategy and key plan;
  - build rows;
  - unique, duplicate, and overflow keys;
  - maximum rows per key;
  - row-list bytes;
  - `build_micros`, set beside `prep_table_build`. They time the same
    interval, so any difference is timer placement and is stated;
  - the representation, when recorded.
- **Replays 2 to 4:** the same fields, for contrast.
- **A reading:** which sub-stage holds the time, and what the input's
  size and form are. It is stated as observed, with no mechanism
  claimed beyond what the stage spans and profiles show, and no saving
  projected.


## Acceptance

- The instrument, with fixtures, and ladders green on bigfoot (Toron
  and toron-tpch, `--no-fail-fast`, fmt clean, release warning-free).
- **Behaviour-neutral:**
  - the instrument's executable folds q05 to ADR 613's digest and
    fingerprint;
  - an unarmed run folds identically;
  - no production path changes.
- Five gated q05 runs, the per-run reconciliation, and the report
  above. Or the named reason a component could not be isolated.
- Plan commit, then one implementation commit carrying the instrument,
  the results, and the closure.


## Guardrails

- The instrument is diagnostic. It touches only the attribution calls,
  armed or not.
- No `perf` sample is assigned to a stage.
- No cut and no saving. A mechanism for a later cut is that record's own
  plan.
- q18's stage vocabulary is a separate, later record.


## Risks

- The decision call may interleave scan and materialization so that
  they cannot be split further. `prep_scan_decision` then holds both,
  and the scan ledger's decode microseconds are shown beside it for
  that scan only, as a sub-reading.
- Timer overhead adds to the span. The five runs' `prep_build` medians
  are shown against ADR 614's 3,804 ms as an observed difference, not
  apportioned.
