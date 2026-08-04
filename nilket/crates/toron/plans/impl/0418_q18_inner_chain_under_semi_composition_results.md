# ADR 418 results - q18 inner chain under semi composition

Base: `0162672 plan: adr 418 q18 inner chain under semi composition`

## Phase 0 - persisted classification before the cut

The exact release `perf-run-one --mode strict --sf 10 --data-source parquet` path was reproduced
under the fixed 4 GiB systemd envelope before production edits. The row retained fingerprint
`d8b60e3a03bfaf79` and failed at the original producer:

| producer | requested | available | over | execute ms | spill bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| `scan output accumulator` | 1,073,742,336 | 1,073,741,824 | 512 | 8,321.2 | 18,862,208 |

The temporary, off-by-default bound-plan capture was removed before the cut. Its persisted artifact
records the preserved side as:

```text
Exchange(
  Inner Join(1)(
    Exchange(Inner Join(0)(Exchange(Scan customer), Exchange(Scan orders))),
    Exchange(Scan lineitem[l_quantity,l_orderkey])))
```

The artifact-backed `estimated_byte_size` values are customer 1,999,516,544 bytes, orders
12,315,016,448 bytes, and terminal lineitem 68,489,092,352 bytes. Execution is morsel-free. With
the 64 MiB task budget, the scan-output cap is 1 GiB and the non-consuming shortcut threshold is
8 GiB; terminal lineitem clears it by about 8.5x. The gate therefore fires from plan metadata
without consuming a scan.

The membership side is a projected grouped lineitem aggregate:

```text
Exchange(Project(Filter(Aggregate(Exchange(Scan lineitem[l_orderkey,l_quantity])))))
```

It groups by `l_orderkey`, applies `SUM(l_quantity) > 300`, and projects the order key. Its
approximately 15 million groups had not executed at sf=10 before this cut, so Phase 0 recorded its
wall status as unknown rather than assuming the existing grouped-fold machinery would complete it.

## Implementation

The outer projected LeftSemi grouped recognizer now has one additional, fully fenced alias:

- the preserved input must have a reoriented-chain top beneath its exchange;
- extraction must yield exactly one replay-1 inner stage over replay 0;
- both replay stages retain the existing inner/hash/no-match-filter/non-null-safe fences;
- both replay-0 children and the replay-1 terminal build must decompose to bounded scan inputs;
- the terminal metadata shortcut must fire, with no morsel;
- the existing outer LeftSemi, projection-benefit, exchange/group ordering, key-plan, and schema
  fences must all hold.

Missing metadata, a malformed replay top, and a valid longer chain return `None` before scan I/O.
Once scan sizing or spill creation begins, the arm is committed: it returns output or an error after
cleanup and never falls through to vanilla execution.

That pre-I/O rule deliberately forfeits the pre-418 materialized-spill semi arm for preserved
inputs that have a reoriented-chain top but fail this alias's metadata or topology fences. This is
the explicit zero-scans-on-decline trade: those shapes decline the composite recognizer rather
than consuming their inputs and attempting the older materialized arm. The sf=10 controls show no
board impact, but the forfeited shape class is part of the contract.

### Awaitable sink and bounded handoff

`ReorientedChainOutputSink` is private to `exec/join/streaming.rs` and has two arms:

- `GroupedFold` preserves the existing grouped adaptive consumer;
- `ProjectedSpill` awaits `write_projected_batch_chunks` with the outer retained columns, remapped
  semi key, projected schema, and key plan.

The exact alias seeds the replay-1 retained build columns backward from the outer retained set. A
completed chain batch is awaited into the projected spill before the producer advances; no vector
accumulates completed q18 output.

Inputs where both builds select a single partition use the existing single-spill runtimes
directly. Inputs where both builds select multiple partitions use only existing encrypted spill
and partition primitives:

1. customer is sized and written once as the replay-0 build;
2. orders is scanned once into **unindexed**, customer-keyed handoff partitions;
3. each orders/customer partition pair is read and joined once, then its Join(0) output is written
   unindexed by the replay-1 order key;
4. terminal lineitem is written one planner-selected spill group at a time; each matching Join(0)
   partition is read once, joined once, and both input partitions are deleted before advancing;
5. completed Join(1) batches are awaited into the projected outer spill.

The handoff writers are deliberately unindexed because their consumers read pages sequentially.
Customer and terminal builds remain indexed because they perform keyed probes. This distinction
keeps row lookup state out of the handoff residency budget. Terminal group count is fixed by the
existing spill planner; it is not a scan-per-probe-batch fallback.

### Deliberate mixed-runtime stop-line

This cut supports the two runtime combinations exercised by its bounded drivers:
`Spilled/Spilled`, and `PartitionedScan/PartitionedScan` with more than one partition on both
builds. A mixed combination is reachable only after the sizing scans commit the alias and is
therefore returned as a loud `InvalidExpression` after cleanup rather than falling through to
vanilla execution. One realistic unexercised case is a very small replay-0 dimension under a
catastrophically large replay-1 fact: the base can choose a single spill while the terminal build
chooses partitioned scan.

TPC-H q18 scales both sides into the partitioned combination, and all controls remain green, so
this is not a correctness finding on an exercised path. Supporting a single-partition side in the
partition handoff is the bounded follow-on; until then, the mixed-combination hard-fail window is
an explicit limitation of this seal.

The preserved exchange above Join(1) is elided only after replaying the base exchange inside the
driver. This is behavior-neutral for the LeftSemi consumer: the projected spill is re-indexed by
the semi key, and the grouped fold applies its own output exchange using the original
`partition_count` and `exchange_key`. The multi-partition parity lock makes this proof
non-vacuous.

All scan opens use `bound_scan_request`. Every created customer, handoff, terminal, and outer spill
has an error cleanup path. A membership failure after the preserved pages are finished also cleans
the outer spill before returning the storage error.

## Persisted sf=10 outcome

The final release artifact, under the same fixed 4 GiB envelope, advances beyond the original scan
accumulator to a named next wall:

| plan fingerprint | post-cut producer | requested | available | over | execute ms | telemetry spill bytes | max RSS |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `d8b60e3a03bfaf79` | `streamed inner join build spill writer` | 536,899,968 | 536,870,912 | 29,056 | 187,370.2 | 1,091,648,144 | 2,200,300 KiB |

This is the final projected preserved-side writer, not the old scan-output accumulator. It reaches
the unchanged 512 MiB per-writer spill budget while draining the last terminal group. No cap was
raised, so q18 is **advanced to a named next wall**, not claimed pass@compare.

Progress-hook scan starts are:

| input | starts | explanation |
| --- | ---: | --- |
| customer | 3 | total estimate, partition sizing, one build pass |
| orders | 1 | one handoff write pass |
| preserved lineitem | 14 | total estimate, partition sizing, 12 bounded terminal groups |
| membership lineitem | 0 | the preserved projected writer reaches its cap first |

The original 1 GiB accumulator never reappears. Scratch contains zero files after the terminal
error. The membership-side wall remains unknown because this run still does not reach it.

The new per-writer spill wall is structurally adjacent to q22's join-writer residency family and
the q21/q22 residency-policy follow-on. q18 is now a third useful driver for that future family,
without expanding this seal into a cap or residency-policy change.

## Eligibility-discriminating locks

- `reoriented_chain_under_semi_is_directly_admitted_and_matches_materialized` invokes the
  recognizer directly and requires `Some(output)`, then compares against a shape-fenced
  materialized twin. It covers Utf8 and Int64 retained fields, duplicate membership keys,
  unmatched keys, duplicate preserved rows, LeftSemi once-semantics, and four-way exchanges.
- The same lock asserts customer/orders/terminal/membership scan counts separately. Its small
  single-spill shape expects 3/1/3/1 starts, so a recognizer miss or internal rescan fails locally.
- `reoriented_chain_under_semi_metadata_and_shape_fences_are_pre_io` proves missing terminal
  metadata, malformed replay topology, and a valid longer chain all return `None` with zero scan
  requests.
- `reoriented_chain_under_semi_cleans_spill_after_membership_error` removes the membership table,
  requires the post-preserved-write storage error, and asserts the unique scratch directory is
  empty.
- Existing reservoir, projected-runtime, adaptive-stage, grouped-consumer, and
  `bound_scan_request_carries_every_bound_scan_field` locks remain green.

The implementation plan also pinned a small-cap wall twin where vanilla preserved-side
materialization fails while the direct recognizer succeeds. That literal unit fixture was
consciously substituted, not silently satisfied: the metadata shortcut declines below a 64 MiB
task budget, while at 64 MiB the 16x materialization allowance makes the accumulator cap 1 GiB.
A vanilla-failing twin would therefore require more than 1 GiB of fixture data and is not a
responsible unit test. The direct-`Some` lock supplies the eligibility discrimination, and the
persisted sf=10 run supplies the actual wall/mechanism proof: the old 1 GiB scan accumulator is
gone and execution advances to the byte-reproduced spill-writer wall.

## sf=10 controls

All ten required controls passed persisted strict comparison on the final release binary. A second
`perf-run-one` pass on the same binary reproduced every forensic spill band exactly:

| query | strict compare ms | perf status | perf execute ms | spill bytes |
| --- | ---: | --- | ---: | ---: |
| q03 | 23,695.2 | pass | 19,118.3 | 233,338,304 |
| q07 | 32,379.1 | pass | 17,119.9 | 134,732,480 |
| q09 | 43,441.1 | pass | 39,057.4 | 108,992,256 |
| q10 | 23,467.8 | pass | 14,693.2 | 215,703,848 |
| q12 | 22,928.2 | pass | 7,204.2 | 159,510,976 |
| q13 | 192,016.9 | pass | 177,243.9 | 9,152,992 |
| q14 | 14,687.7 | pass | 6,050.4 | 36,464,000 |
| q16 | 12,104.4 | pass | 5,469.8 | 60,078,400 |
| q19 | 137,148.9 | pass | 111,706.8 | 51,471,552 |
| q20 | 41,603.7 | pass | 31,492.2 | 256,104,392 |

q20's fixed-cap residency watch remains green. Scratch is empty after both control loops.

## Verification and seal audit

Verification passed:

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron streaming --lib                 # 46 passed
scripts/cargo-cgroup test -p toron join --lib                      # 198 passed
scripts/cargo-cgroup test -p toron --test architecture_test        # 2,079 passed
scripts/cargo-cgroup test -p toron --no-fail-fast                  # passed, all targets
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
                                                                    # 286 passed
git diff --check
```

- No memory/spill cap, multiplier, query scope, binder projection, storage/wire surface, or oracle
  policy changed.
- The cut adds no scan spill without a puller and no executor-wide callback/pull abstraction.
- ScanRequest construction remains centralized in `bound_scan_request`.
- The temporary plan diagnostic is removed. Production source contains no query/ADR branding.
- `exec.rs` is unchanged at 7,499 / 7,500 lines under its architecture ratchet.
- No `.perf.data` is tracked.
- q21/q22, q02, q05, and q11 remain the named untouched wall families.
