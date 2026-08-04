# ADR 341 - Implementation plan: Exchange input streaming (output-handling-gated)

Pairs with `plans/adr/0341_exchange_input_streaming_gated.md`. An engine cut in `crates/toron` (exec.rs): stream 340's
filtered batches **into** the Exchange (partition per batch) instead of the Filter accumulating a `Vec`, removing q21's
340 accumulator wall. **Gated:** Phase 0 proves the exchange output-handling before setting q21's acceptance to
green-or-next-wall. q07 = composition target.

## Phase 0 - the output-handling gate (decision point, before D1)
- Inspect the exchange consumer boundary: does `bound_exchange` (exec.rs:2239) have (or can 341 add without a broad
  rewrite) a path where its **consumer pulls partitions** without `bound_exchange` returning a full
  `Vec<RecordBatch>`? Today it returns `Vec`; the consumer (`Join(1)`) does `execute_bound(Exchange) -> Vec`.
- **Branch (record in results doc):**
  1. **Partition-forward possible** (consumer pulls, no returned Vec) -> 341 may **target q21 green**.
  2. **Must return `Vec`** -> 341 is a **wall-mover**: remove the Filter accumulator; the returned partition output is
     the new wall and **must be budgeted + named** (the 340 lesson); q21-green is 342's stage (consumer streamed)
     unless the partitioned output happens to fit.
- This gate bounds 341's size: branch 1 may require a streaming consumer interface (likely 342's core) - if so, keep
  341 to the input-streaming + budgeted-output (branch 2) and let 342 own the consumer interface.

## D1 - stream the filter batches into the Exchange
- For the matched `Exchange <- Filter <- Join(0)` shape, **before** `execute_bound(input) -> Vec`, take the streamed
  path: pull ADR 340's filtered chunks one at a time, **partition each batch on arrival** into the partition routing,
  drop the source batch. The full filtered Vec never materializes.
- Do **not** call `try_streaming_join0_filter` as-is and treat its result as the stream. It returns a `Vec<RecordBatch>`
  and is q21's current wall. Extract/refactor the existing chunk/filter loop
  (`materialize_join_selection_chunks_filter_accumulate`) into a shared visitor/producer used by:
  - the existing Filter path, which still accumulates filtered chunks when no Exchange consumes them;
  - the new Exchange-input path, which receives each filtered chunk before the accumulator exists.
- **Output handling per the gate branch:**
  - branch 1: partition-forward to the consumer (no returned Vec) - target q21 green.
  - branch 2: accumulate per-partition output as today, but **budget + name** it (`budget_exhausted`, the 340
    accumulator pattern) so an over-budget exchange output is a named `memory_budget_exhausted@execute`, not a silent OOM.
    Budget the **total retained returned exchange output**, not only individual partition batches; the caller retains
    the full returned `Vec<RecordBatch>`.
- Detection must precede `execute_bound(input)` (else inert). Non-matching shapes keep the existing path.

## D2 - order invariant (both branches)
- 341 must produce the **same per-partition row order** as the materialize-each exchange (partition batches in the same
  arrival order and return partitions in the same partition-index order, including empty partitions), so the downstream
  grouped aggregate folds each group identically -> bit-identical, **including dev Float64**.

## Tests (`toron` lib)
- **Exchange-stream parity (Float64 + Decimal):** a synthetic `Exchange <- Filter <- Join` over more than the chunk
  threshold -> streamed-partition result equals materialize-then-partition, **both** modes (per-partition order). The
  Float64 case is the one that breaks on reorder.
- **No silent exchange wall (branch 2):** an over-budget exchange-output accumulation emits the named
  `memory_budget_exhausted@execute` diagnostic, not a silent OOM; the test must fail on total retained returned bytes,
  not merely per-partition bytes.
- **Detection-before-execute + shape fence:** the streamed path replaces `execute_bound(input)` for the matched shape;
  other shapes unchanged.
- sf=0.01 reach/correctness unchanged.

## Phase 3 - re-measure (release, capped, sf=1)
```
scripts/cargo-cgroup build -p toron-tpch --release
# q21 @ sf=1: branch 1 -> pass@execute (target); branch 2 -> memory_budget_exhausted@execute at the named, budgeted
#   exchange-output producer (the 340 filter accumulator no longer the producer), verified via 332 retention.
# q07 @ sf=1: filtered output streams through the exchange; wall remains the later join (unchanged producer/bytes ~1.18 GB).
```
- Verify the q21 producer moved off the 340 filter accumulator (332 retention); record green (branch 1) or the next
  named wall (branch 2). Update the ADR 330 atlas q21 cell accordingly.
- q14/q19 unchanged; q17/q12/q18/q04/q20 unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix     # 22/22 @ sf=0.01 unchanged
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01   # order preserved
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
#   release/cgroup q21 sf=1 (green or named next wall); q07 unchanged later-join wall
```

## Results doc
`crates/toron/plans/impl/0341_exchange_input_streaming_gated_results.md`: the Phase-0 output-handling gate branch taken;
the exchange-input streaming cut; q21 sf=1 before/after (340 filter accumulator -> green, or named budgeted
exchange-output wall, via 332); the Float64/Decimal per-partition parity result; sf=0.01 q07/q21 pass@compare; q07
composes through the exchange (wall still the later join); the ADR 330 atlas q21 update.

## Risk / blast radius
- **Gate first** - prove output handling before promising q21 green; set acceptance to green-or-next-wall by the branch.
- **No silent exchange wall** - budget + name any returned/accumulated partition output (branch 2).
- **Wrong primitive trap** - extract the chunk/filter producer; do not call 340's Vec-returning helper and relabel that
  as streaming.
- **Order, not associativity** - the Float64/Decimal per-partition parity test, both branches.
- **Detection-before-execute** - the streamed path precedes `execute_bound(input)`.
- **Scope** - the `Exchange <- Filter <- Join(0)` boundary only; Join(1)/join-to-join (342), semi/anti, q17 untouched.

## Seal
`impl: adr 341 exchange input streaming gated`. Do not commit wip drafts.
