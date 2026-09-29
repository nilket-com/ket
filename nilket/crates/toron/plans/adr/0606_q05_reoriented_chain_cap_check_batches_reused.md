# ADR 606 - q05's reoriented chain: the cap check's batches reused for the build

**Status:** CLOSED, RETAINED. One paired attempt on strix (results in
`plans/impl/0606_q05_reoriented_chain_cap_check_batches_reused_results.md`):
q05 median 5,170.6 ms control, 4,735.0 ms treatment, a median paired
gain of 444.7 ms (8.6%) against a required 249.6 ms, every pair
positive, digest parity on all twelve rows. The mechanism check shows
lineitem, supplier, nation, and region scanned once instead of twice.
Peak RSS is 1.14 GiB (15%) higher on the treatment, an observed
tradeoff. The token stays `Reuse`. Successor to ADR 605. On the sealed anchor (ADR 603:
Toron 63.430 s against DuckDB 15.650 s), q05's gap is 4,689 ms. ADR 605
found Toron scanning each of q05's build-side tables twice in immediate
succession, with identical rows and batches: lineitem, supplier, nation,
and region, 48,069,037 extra rows, lineitem's second read 2,306 MB
decoded. A code audit placed the first scan of each pair: it is the
reoriented chain's scan-output cap check. The check is a useful
admission decision, but its batches are discarded, which forces a
second, full scan to build the join. This record measures reusing those
batches, one token, on q05. It projects no saving; the pair measures it.


## Context

**The route.** q05 takes `reoriented_inner_chain_grouped`
(`exec/join/streaming.rs`,
`try_streaming_reoriented_inner_chain_grouped_aggregate`). The SQL
planner wraps a grouped aggregate's input in an `Exchange`, and the
binder keeps it at this coordinate, so `aggregate_input_wrapped` is true.
In the chain's prep loop, each build stage takes the wrapped arm. That
decision was inferred from the plan shape and is verified before the
pair; see Acceptance.

```rust
} else if aggregate_input_wrapped {
	if self.reoriented_inner_stage_scan_build_exceeds_scan_output_cap(&stage, morsel).await? {
		ReorientedStageScanBuildDecision::Stream
	} else {
		ReorientedStageScanBuildDecision::Materialized
	}
}
```

The terminal stage has the same shape where it is deferred.

**What the check does.** `reoriented_inner_stage_scan_build_decision`
works in four steps:
- It decomposes `stage.build` into a scan input. If it cannot, it
  returns `Materialized` without scanning.
- It materializes the whole scan output
  (`materialize_streaming_scan_output_for_cap`). That applies the row
  limit and the `EvaluateInToron` filter, and holds every batch.
- It estimates the batches' Arrow bytes and their estimated hash-build
  bytes.
- It decides against `scan_output_cap` (16 × the task memory, 32 GiB at
  2 GiB):
  - `Stream` when the Arrow bytes exceed the cap;
  - `StreamMaterializedBatches(batches)` when the Arrow bytes fit but
    the hash bytes do not;
  - `MaterializedBatches(batches)` when both fit.

The wrapped arm keeps only whether the decision was `Stream` and
discards the batches. `MaterializedBatches` and
`StreamMaterializedBatches` both become `Materialized`, which runs
`prepare_reoriented_inner_join_runtime`. That calls
`execute_bound_node(stage.build)`, the second full scan, and builds from
its batches. The unwrapped arm, by contrast, uses the decision directly,
and a `MaterializedBatches(batches)` decision there builds from those
batches (`prepare_reoriented_inner_join_runtime_from_build_batches`)
with no second scan.

**The build's shape on q05.** Each of q05's four duplicated build sides
(lineitem, supplier, nation, region) is an `Exchange(parts=4, key)`
around its scan or filtered scan (the sealed plan, as ADR 604's census
recorded it). `decompose_streaming_scan_probe_input` accepts `Scan`,
`Filter(Scan)`, and one `Exchange` around either, returning the scan,
the Toron-side filter, and the exchange's partition count and key. The
cap check materializes the Exchange's *input*: the scan, filtered and
limited. It does not repartition. In control, the second scan runs the
whole `Exchange` through the executor. For a `Filter` child whose
predicate is evaluated in Toron, the executor first *attempts*
`try_streaming_join0_filter_exchange`, which streams the filtered chunks
and repartitions them under a streamed-exchange accumulator budget; that
helper can also decline when its input is not the join-0 shape it
recognizes. `Reuse` conservatively leaves every such stage on control,
whichever way that attempt would go. Every other shape falls through
to `bound_exchange`:
- a bare `Scan`;
- a `Filter` whose predicate was fully pushed into the scan, since the
  streamed path declines `FullyPushed` at once.

`bound_exchange` repartitions every input batch by the key into
`partition_count` partitions and finalizes them. Each partition's
batches are concatenated, and an empty partition becomes one empty
batch. It has no accumulator cap. **On q05, all four duplicated builds
take `bound_exchange`:** lineitem, supplier, and nation are `Exchange`
around a bare `Scan`, and region is `Exchange` around a `Filter` whose
`r_name` predicate is fully pushed. The build receives one batch per
partition, in partition order.

**What each path holds in memory.** In control, scan 1's batches are
held until the decision returns and dropped. The second scan's batches
and the exchange's partition buffers are then held while it
repartitions. In the treatment, scan 1's batches are held from the
decision through the repartition, and the partition buffers are held
alongside them until the input batches are released. The retained
batches and the repartitioned output can coexist. Nothing here bounds
q05's resident memory: the 32 GiB scan-output cap is a decision
threshold, not a measurement. The record reports peak RSS on both arms
and asserts no equality.

**What is not known.** No record isolates scan 1's time on q05. Toron's
node attribution folds the chain's work into its aggregate span (ADR
604), and the scan ledger's decode total for q05 (1,052 ms, both
lineitem reads included) bounds only the decode component, loosely. The
pair is the measurement.


## Decision

**In the wrapped arm, reuse an actual `MaterializedBatches(batches)`
decision's batches for the build. Every other decision keeps its current
control behavior. One token, one cell (q05), under ADR 597/599's
procedure and ADR 588's rule.**

*The token.* `WrappedCapCheckBatches { Discard, Reuse }` at the wrapped
arm, set at the call site. `Discard` is today's behavior, as the control
arm. Under `Reuse`, the arm calls `reoriented_inner_stage_scan_build_decision`
once and maps its result:
- `Stream` → `Stream` (unchanged);
- `StreamMaterializedBatches(_)` → `Materialized` (unchanged: the
  batches are dropped and the stage is re-read, exactly as in control);
- `MaterializedBatches(batches)`, for a stage whose control path is
  `bound_exchange` or no exchange at all → the stage's own `Exchange`
  applied to `batches` by `bound_exchange`'s repartition-and-finalize
  (the same partition count, key, and empty-partition finalize, and no
  cap that control does not have), then `MaterializedBatches(exchanged)`.
  A stage without an `Exchange` passes the batches through as they are.
  The build uses those, and `execute_bound_node(stage.build)` is not
  called;
- `MaterializedBatches(_)`, for a stage whose control path is the
  streamed `try_streaming_join0_filter_exchange` (an `Exchange` around a
  `Filter` evaluated in Toron) → `Materialized` (unchanged: control
  behavior, re-read). That keeps the streamed path's accumulator budget
  exactly where control has it, and brings in no new refusal;
- `Materialized` (the decision's early returns, with no scan) →
  `Materialized` (unchanged).

`bound_exchange`'s repartition-and-finalize over a batch list is
factored into one function, which `bound_exchange` and the treatment
both call. The refactor is behavior-neutral for control, and is
fixtured as such. The deferred terminal stage takes the same mapping.
Control is the tree with `Discard`; treatment is the same tree with
`Reuse`, one insertion and one deletion. The token applies only to the
wrapped arm; the unwrapped arm and the streamed route are untouched.
**The pair measures removing the second scan while keeping the
exchange's work, not bypassing the exchange.**

*What the treatment must keep identical.* The cap decision itself: the
same estimate, threshold, and outcomes. And the build's input, against
what control's `execute_bound_node(stage.build)` produces:
- the same schema;
- the same partition layout: `partition_count` batches in partition
  order, with an empty partition as one empty batch;
- the same rows in each partition, up to order within the partition
  (the join and fold do not depend on order);
- an empty input (every row filtered) yielding `partition_count` empty
  batches on both arms;
- the result digest and plan fingerprint equal to the anchor's on both
  arms.

A stage whose build is not one of the decomposable shapes never reaches
the reuse, since the decision returns `Materialized` without scanning.

*Telemetry, stated.* Under `Reuse`, the build node's
`execute_bound_node` attribution no longer fires for a reused stage. The
node-attribution tree loses that invocation, and its time moves into the
chain's prep span. The exchange's repartition time
(`exchange_repartition_micros`) is still recorded on both arms: by
`bound_exchange` in control, and by the shared function in the
treatment. The mechanism check reports both, and no receipt
rule treats the missing invocation as an error.

*The experiment.*
- Two release executables built on bigfoot from one recipe, staged and
  hash-verified on strix, with ADR 588's manifest gate.
- ADR 597's twelve-row paired driver on q05, run as ADR 597, 599, and
  601 ran theirs:
  - ADR 592's validator, with each arm's executable and the sealed
    digest checked on every row;
  - ADR 593's readings;
  - ADR 595's separated launch;
  - ADR 599's gate procedure with `--gate-settle-seconds 5`;
  - one attempt.
- The driver takes the query as an argument where it has one. Where it
  is q21-specific, the implementation extends it to take q05 without
  changing the pairing, gate, or rule, and states the change.
- **ADR 588's rule: retained only if the median paired gain exceeds
  `2 × range_control` and every pair is strictly positive.** Any other
  outcome reverts the token to `Discard` in the closing commit.

*The mechanism check, after a verdict.* The ADR 605 scan-work census on
q05 against the treatment executable:
- Each build-side table (lineitem, supplier, nation, region) is scanned
  once instead of twice.
- The progress stream and ledger reconcile.
- The digest and fingerprint match the anchor's.
- The route is still `reoriented_inner_chain_grouped`.
- The decision per stage (`MaterializedBatches` reused; any other
  decision named) is recorded, so the pair's delta can be attributed to
  the stages it touched.


## Acceptance

- Before the pair, a verification run on q05 with a diagnostic
  per-stage receipt, recorded under `Discard` and under `Reuse`, names
  for each of the four duplicated build stages:
  - its decision;
  - its shape (the `Exchange`'s partition count and key; `Scan`, or
    `Filter` with its disposition);
  - the executor path control takes (`bound_exchange` or
    `try_streaming_join0_filter_exchange`).

  It establishes that each stage takes `MaterializedBatches` and control
  path `bound_exchange`. It also establishes that under `Reuse` the
  exchanged batches equal control's `execute_bound_node(stage.build)`
  output in schema, partition layout, and per-partition row multiset. If
  any stage differs, the record stops and reports it before the pair.
- Fixtures:
  - The mapping, for all four decisions under both forms, and for both
    control paths.
  - The factored repartition-and-finalize: identical to
    `bound_exchange`'s previous output on the same input.
  - `Reuse` against control on `Exchange(Scan)` and on
    `Exchange(Filter(FullyPushed))` (both `bound_exchange`): the same
    schema, partition layout, per-partition rows, and key plan, with
    empty partitions present.
  - `Exchange(Filter(EvaluateInToron))` keeping control behavior under
    `Reuse`, with the streamed path and its accumulator budget
    unchanged.
  - The empty-input case on both paths.
  - The default is `Discard` on every other construction path.
- The token asserted by diff (one insertion, one deletion).
- The ladder green on bigfoot on both trees (Toron and toron-tpch, fmt
  clean).
- On strix, one paired run under the declared procedure: twelve rows;
  readings; separation and window verified; the control range and the
  bar; the gain per pair and its median; and the verdict. Or the
  refusal.
- After a verdict, the mechanism check as above.
- Plan commit, then one implementation, results, and closure commit.


## Guardrails

- Only `MaterializedBatches` is reused. `Stream`, `StreamMaterializedBatches`,
  and the early `Materialized` keep their control behavior, so streaming
  and spill admission do not change.
- The unwrapped arm and the streamed route are untouched.
- The retention rule, the twelve-row contract, the gate procedure, and
  the threshold are ADR 597/599/601's, verbatim. There is one attempt,
  and no relaunch under this record.
- No speed claim before the pair; no claim for q09 or any other cell
  from q05's pair.


## Risks

- Scan 1 may be cheap relative to q05's other work, and the gain may sit
  under twice the control range. Then the verdict is not retained, and
  the token reverts.
- The build input from the direct materialization may differ from the
  executor path's in a way the fixtures miss. The digest checks on all
  twelve rows and the mechanism check guard it.
- Holding scan 1's batches through the repartition keeps them alive
  alongside the partition buffers, so the treatment's peak may differ
  from control's. Peak RSS is reported on both arms; the cap decision is
  unchanged.
- The factored function must reproduce `bound_exchange` exactly. The
  equivalence fixture and the pre-pair receipt guard that. Stages on the
  streamed filter-exchange path are left on control, so no q05 stage
  depends on reproducing that path.


## Forward

- Retained: q09's doubled stages (part, partsupp, orders, and nation)
  are the next coverage, and other queries on this route are enumerated
  from route receipts before any claim beyond q05.
- Not retained: filter placement at Toron's scans (Date32 and `LIKE`
  in PBC, and join-derived filters at the scan) remains the other
  candidate from ADR 605, as a multi-step plan.
