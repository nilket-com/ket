# ADR 616 - reused build batches skip an exchange whose partitions are re-concatenated

**Status:** PROPOSED. Successor to ADR 615. One token, measured by a
pair. It projects no saving.

ADR 615 divided q05's replay-1 `prep_build` (3,681.7 ms) and found the
largest sub-stage is **`prep_exchange`, 1,645.5 ms**. That is the
repartitioning of the 47,989,007 reused `lineitem` rows into 4
partitions. ADR 615's single-run compatibility sweep read
`prep_exchange` in other cells too (diagnostic, not paired):

| cell | `prep_exchange` ms |
|---|---|
| q05 | 1,668 |
| q09 | 396 |
| q07 | 240 |
| q08 | 16 |


## Context

**What the exchange's output is used for on this path.** On the wrapped
arm's Reuse path (ADR 606), the cap check's batches pass through
`exchange_input_batches`:
1. every batch is repartitioned by the build key into `partition_count`
   partitions;
2. each partition's batches are concatenated into one batch;
3. the partition batches are returned, in partition order.

They go straight into
`prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline`.
That function concatenates all of them again (`concat_or_empty`, timed
by ADR 615 as `prep_input_materialize`) and builds **one** table:
- in memory, `build_in_memory_join_rows` over the single batch;
- or, over the memory cap, a `SpilledInnerJoinBuild` that partitions
  the build by its own scheme.

`ReorientedInnerJoinRuntime` keeps a single build and no partition
structure.

So on this path the exchange contributes no partitioning that anything
downstream reads. What it does contribute:
- the repartition work (hashing and taking rows per partition);
- a per-partition concatenation, a full copy of the build rows;
- **an order:** the build rows reach the table grouped by partition,
  rather than in the scan's order.

**What the order can affect.** The table maps each key to its rows. A
key's row list keeps the order its rows arrived in, so a probe that
matches several build rows emits them in that order. The join's output
order within a key can therefore differ. Downstream, the chain feeds a
grouped fold. An integer or decimal aggregate is exact under any order.
A float aggregate adds in arrival order, so it can differ in its low
bits. The digest gate on every cell detects any such difference.


## Decision

*The token.* `ReuseBuildExchange { Repartition, Bypass }`, a `const`
read in the wrapped decision helper's reuse branch.
- `Repartition` is today's behaviour: `exchange_input_batches` over the
  cap check's batches.
- `Bypass` passes the cap check's batches to the build unchanged. That
  skips the repartition and the per-partition concatenation.

Only that branch changes. The unwrapped arm, `Discard`, the streamed
filter-exchange attempt, and `bound_exchange` everywhere else keep
their code.

*Intended invariants.* The **logical multiset of build rows** and the
**plan fingerprint** are unchanged. The key plan is unchanged. The build
estimate is linear in row count and key width (`join.rs`,
`spilled_hash.rs`), so it is expected to agree, but it is **reported,
not assumed**.

*What `Bypass` can change, stated:*
- the build rows' order within a key, hence the join's output order
  within a key, hence the low bits of a float aggregate;
- batch shapes and buffer lifetimes, hence actual RSS;
- possibly resource admission, refusal, or spill behaviour;
- the physical batching of the output.

So the plan reports these rather than assuming them. Per gate run and
per paired row:
- the build estimate;
- the admission outcome and the spill path taken (in memory or
  `SpilledInnerJoinBuild`);
- spill bytes;
- peak RSS with its label.

A **treatment-only spill or resource refusal** in any gate run or
paired row is handled explicitly:
- in the gate, it fails the gate;
- in the q05 pair, it blocks retention;
- in the q09 pair, it is a regression under ADR 607's guard.

Each opens a new record, and none is a silent revert.

*Implementation amendment, stated before the build:*
- **The build estimate is recorded by the cap-check receipt** (the
  estimated build bytes, the memory cap, and the path they imply) in
  the untimed q05 preflight, on **both** arms. The paired rows and gate
  rows carry no per-row estimate: no row field exists for it, and
  adding one is a harness change this record does not make. Per row,
  the spill path and any refusal are read from each row's status,
  `spill_bytes`, and join-spill observations, beside peak RSS with its
  label.
- **`prep_exchange`'s absence under `Bypass`** is shown by armed
  attribution runs, as in ADR 615. Attribution cannot be armed inside
  in-process tests.
- **ADR 606's fixture** `adr606_reuse_builds_what_bound_exchange_would_build`
  now asserts by token:
  - under `Repartition`, today's per-partition match;
  - under `Bypass`, the same rows as control by the row-digest sketch,
    with equal row counts.

  Every related fixture passes under both token settings.

*Instrumentation, carried over.* ADR 615's `prep_exchange` stage is not
opened under `Bypass`. It is recorded as absent, as ADR 615's
vocabulary already allows.

*The equivalence check under `Bypass`: bounded and order-independent.*
The ADR 606 cap-check receipt compares matching partition batches, and
`Bypass` has no partitions. A full sorted materialization of
47,989,007 rows could itself exceed memory. So, under `Bypass`, the
receipt compares the reused build input with control's by a
**commutative row-digest sketch**, a mode named in the receipt as
`commutative_row_digest_sketch`:
- each row is encoded canonically with the Arrow row format over the
  build schema, including nulls;
- each encoded row is hashed to 128 bits;
- two accumulators combine the hashes: a wrapping sum and a XOR;
- the row count and the schema are compared exactly.

The comparison itself adds only one encoded batch at a time plus
constant state: it avoids an additional sort or materialization of all
the rows. That is not a bound on the preflight, which already holds all
the cap check's batches (Reuse) and the control's re-executed build
(`execute_bound_node`) as the ADR 606 receipt does.

**Qualification:** the check is a **probabilistic multiset sketch**, not
proof of multiset equality. Equal sum and XOR accumulators, row count,
and schema are consistent with equal multisets. But distinct multisets
can produce equal accumulators without any per-row hash collision (for
example, hashes {1, 2, 4} and {0, 3, 4} both sum and XOR to 7). No
numerical guarantee is claimed. The receipt names the mode as a sketch.
The per-cell result digest gate is separate, and remains the evidence
for result identity. If the sketch cannot complete (an encoding or
resource error), the preflight **fails**. The receipt stays confined to
untimed runs.

*The gate before any pair.* One run of each of the 22 queries per arm
at ADR 613's coordinate, with the ADR 610 kernel receipt off and ADR
606's cap-check receipt **off**. It **fails closed** unless:
- every digest and plan fingerprint equals ADR 613's sealed cell;
- no gate run shows a treatment-only spill or refusal;
- a separate untimed q05 preflight with the cap-check receipt shows,
  per stage, reuse with `Bypass` named and the
  `commutative_row_digest_sketch` check passed (row count, schema, and
  both accumulators equal; a sketch, not proof of multiset equality).

The cells whose reuse path is bypassed are named from ADR 615's
refinement cells (q03, q05, q07, q08, q09), confirmed by an armed
attribution run per arm. q09 is the breadth cell: after q05 it has the
largest `prep_exchange` in ADR 615's sweep, and its digests must pass.

*The pairs.* ADR 597's driver, bound to ADR 613's sealed board, with
ADR 592/593/595/599, one attempt each:
1. **q05 decides the token** by ADR 588's rule.
2. **q09 is coverage,** with ADR 607's interpretation and regression
   guard.

Peak RSS, with its label, and spill are reported beside each verdict.

*The mechanism check, after the verdicts.* Five armed q05 runs per arm
using ADR 615's instrument. They report replay 1's sub-stages, with
`prep_exchange` absent under `Bypass`, and `prep_input_materialize` and
`prep_table_build` compared. These are diagnostic, not paired.


## Acceptance

- Fixtures:
  - `Bypass` against `Repartition`: an identical multiset of built rows
    and identical join results for a chain fold. The built table is
    compared as key to multiset of rows; probe outputs as multisets.
  - Integer and decimal aggregates identical. Float-result identity is
    **not** claimed from a fixture: the 22-cell digest gate is what
    establishes it per cell.
  - A forced-spill fixture under both forms, with correct results; the
    estimates reported.
  - The commutative row digest: invariant under permutation; sensitive
    to a changed value, a changed null, a duplicated row, and a dropped
    row; equal row count and schema required; the named comparison
    mode; and a failure to complete reported as a failure.
  - `prep_exchange` absent under `Bypass`, with the refinement still
    reconciling.
  - The token asserted by diff: one insertion, one deletion.
- Ladders green on both trees, fmt clean, and release builds
  warning-free under both token settings.
- The gate, the q05 pair and verdict, the q09 pair and its
  interpretation, and the mechanism runs.
- Plan commit, then one implementation, results, and closure commit.


## Guardrails

- Only the wrapped Reuse branch changes. Every other exchange is
  untouched.
- The digest gate on all 22 cells and on every paired row. A cell whose
  digest changes fails the gate, and no pair runs.
- ADR 588's rule, the twelve-row contract, and ADR 599's gate, verbatim.
  One attempt per cell.


## Risks

- **A float aggregate's digest may change.** If a cell's final result
  depends on the order of float additions, its digest can change under
  `Bypass`. The gate fails closed. The record then names the cell, and
  any narrowing (for example, `Bypass` only where the chain's
  aggregates are exact) is a successor plan.
- **Probe locality.** The build table's layout changes. Probe locality
  may differ in either direction. The pair measures the whole effect.
- **The telemetry total.** `exchange_repartition_micros` stops
  accumulating for bypassed stages. The query-level total changes, as
  expected, and is stated.


## Forward

- If retained, the same question applies to the bound exchanges on
  other routes whose output is re-concatenated into a single build, for
  example q21's `runtime_prep`. That is established per route, by its
  own evidence.
