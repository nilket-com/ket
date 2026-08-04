# ADR 424 results - q18 membership-first composition

## Outcome

ADR 424's engine cut is implemented. q18 now executes its guarded membership aggregate first,
reduces the 624 qualifying order keys to a 16,384-byte resident presence set, and streams the
preserved inner chain directly through that set into the existing grouped fold. The semi outer
projected spill is never constructed on the admitted path.

The final sf=10 release run completed under the standard 4 GiB envelope in 213,472.4 ms with
the unchanged plan fingerprint `d8b60e3a03bfaf79`. The independent oracle comparison then
OOMed under that same envelope and required 6 GiB of combined harness headroom to pass. Under
the board's uniform 4 GiB comparison rule, q18 is therefore **engine-green / reach-only** with
the named `q18_sf10_oracle_residency_gap`, not `pass@compare`. The board remains **17/22
green** until a frugal q18 oracle re-earns comparison inside 4 GiB.

| evidence rail | envelope | result | board meaning |
| --- | ---: | --- | --- |
| standalone Toron execution | 4 GiB | pass | engine cut succeeds |
| canonical in-process correctness | 4 GiB | kernel OOM during oracle construction | `q18_sf10_oracle_residency_gap`; not green |
| supplemental correctness | 6 GiB | `pass@compare` | semantic evidence only; no board credit |

## Phase 0 evidence

### Wall reproduction

The exact pre-cut release binary reproduced the ADR 418/423 wall under the 4 GiB cgroup:

| fingerprint | producer | requested | available | spill bytes | execute | max RSS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `d8b60e3a03bfaf79` | `streamed inner join build spill writer` | 536,899,968 | 536,870,912 | 1,091,648,144 | 128,498.9 ms | 2,201,352 KiB |

The failure occurred before the membership subtree opened. It was the semi outer projected
writer that existed only to defer membership probing, not a membership-fold or inner-chain
correctness failure.

### Census and exact membership subtree

The bounded generator census re-proved the post-`HAVING`, pre-outer-`LIMIT` membership shape:

| scale | lineitem rows | groups | qualifying keys | duplicate/null keys |
| --- | ---: | ---: | ---: | ---: |
| 1 | 6,001,215 | 1,500,000 | 57 | 0 / 0 |
| 10 | 59,986,052 | 15,000,000 | 624 | 0 / 0 |

The temporary exact-subtree harness returned one batch with schema
`l_orderkey: BIGINT, sum_quantity: DECIMAL(15,2)` at each scale. Full Arrow batch memory—not
logical key payload—was 1,560 bytes at sf=1 and 15,168 bytes at sf=10. The guarded primitive
fold selected at both scales:

| scale | entries / buckets | accounted | transition peak | fold elapsed |
| --- | --- | ---: | ---: | ---: |
| 1 | 2,097,152 / 4,194,304 | 67,108,864 | 100,663,296 | 179,725 us |
| 10 | 16,777,216 / 33,554,432 | 536,870,912 | 805,306,368 | 2,992,296 us |

The temporary harness and census code were removed before production work. The allocator
microfixture observed the one-shot presence-set request exactly: 1,024 `i64` entries and 2,048
`u32` buckets, 16,384 bytes at both scales. Production nevertheless accounts observed vector
capacities rather than assuming allocator capacity equals the request.

## The cut

### Commit and schedule

The existing ADR 418 recognizer still owns admission: one replay-1 reoriented inner stage with
the terminal scan-build shortcut. ADR 424 adds only pre-I/O direct-filter fences: one key on
each side, non-null Int64 schemas, `match_filter: None`, and non-null-safe equality. Nullable,
null-safe, composite, non-primitive, missing-metadata, replay-2, and longer-chain shapes decline
before a scan. The projected-spill key plan remains confined to the unchanged materialized
branch; the direct filter compiles against the original `semi_output_schema` and `left_keys`.

After the final fence, the path commits and executes this sequence:

1. flush `selected/committed` with `outer_writer_constructed=false`;
2. flush `membership_execute_started`, then execute the membership subtree;
3. short-circuit an empty membership result with one typed empty fold batch and no preserved
   scan or runtime allocation;
4. build the one-shot `Int64PresenceSet` while the measured membership batches remain live;
5. drop those batches only after all observed-capacity checks pass;
6. prepare the terminal inner runtime with the set footprint as owner baseline;
7. stream the preserved chain through the membership-filtered grouped-fold sink;
8. clean the runtime unconditionally and finalize the aggregate.

There is no late fallback. Losing the terminal runtime or returning `None` after commit is a
labeled error, and spill preparation/drive failures clean created scratch before returning.

### Presence set and two-peak accounting

`Int64PresenceSet` is local to this owner: `Vec<i64>` entries, `Vec<u32>` buckets with
`u32::MAX` empty, a seeded hash, linear probing, and 0.5 maximum load. It computes the checked
row upper bound once, reserves entries and buckets once, and rejects any post-reservation
capacity change. Duplicate keys are idempotent and the full Int64 domain is valid.

Construction guards these states against the unchanged retained-footprint cap:

- live batch bytes + requested entries + requested buckets;
- live batch bytes + observed entries + requested buckets, before bucket allocation;
- live batch bytes + observed entries + observed buckets.

The final release receipt measured 5,376 live batch bytes, 16,384 set bytes, and a 21,760-byte
construction peak. This is smaller than the temporary harness's batch capacity and is recorded
as a runtime fact, not retrofitted into the Phase 0 measurement.

During the chain drive, the completed set is passed as an explicit resident baseline through
the q18-exclusive runtime owner, partition reservoirs, loaded-partition checks, and every inner
spill it creates. Guards compare `baseline + local` to the existing cap and report the baseline
separately. Exported local retained bytes exclude the baseline, preventing owner double-count.
TaskBudget, partition/materialization/cache heuristics, and all other callers are unchanged;
they continue to pass a zero baseline.

### Direct sink

Each completed chain batch probes the presence set using the original semi key ordinal,
filters matching rows once, repartitions survivors with the aggregate's original exchange, and
feeds non-empty partitions to the existing grouped fold. LeftSemi duplicate suppression comes
from presence-only lookup: duplicate membership rows cannot multiply preserved rows. The old
`GroupedFold` and `ProjectedSpill` sink arms remain intact, and the non-chain materialized arm
still constructs the projected writer.

## Discriminating locks

The mechanism locks cover:

- direct `Some` admission and full-row parity with the materialized twin across mixed
  String/Int64 output and duplicate membership keys;
- zero semi outer-writer construction on the admitted path, while inner-stage spills remain
  legitimate;
- membership storage failure before any preserved table opens, with empty spill scratch;
- empty membership producing typed empty output with only the membership table scanned;
- one-shot presence layout/accounting, observed capacities, duplicate idempotence, no growth,
  full-domain Int64 lookup, absent lookup, and the u32-domain fence;
- a construction crossing where the set alone fits but the live batch plus set does not;
- a combined baseline/local spill crossing where each component fits alone, exact requested
  bytes, separately labeled detail, and no baseline double-count;
- durable receipt ordering and a simulated post-commit stop leaving a parseable final
  milestone;
- all metadata and direct-filter fence declines before I/O.

Focused spill and streaming suites passed 51/51 and 59/59 respectively.

## Final q18 engine evidence

The final release `perf-run-one` ran under `MemoryMax=4G`, `MemoryHigh=4G`, and the 600-second
gate:

| status | execute | wall | output rows | chain input / matches | spill bytes | max RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pass | 213,472.4 ms | 3:38.31 | 100 | 59,986,052 / 4,368 | 4,380,793,312 | 1,227,284 KiB |

The higher spill-byte telemetry is legitimate inner-chain partition traffic; the removed
536,899,968-byte semi outer writer and its `SpillBytes` producer are absent. The durable receipt
ended in this exact order:

```text
selected/committed (outer_writer_constructed=false)
membership_execute_started
membership_execute_completed (rows=624, batch_bytes=5376)
presence_set_completed (entries=624, accounted_bytes=16384, construction_peak_bytes=21760)
chain_runtime_prepare_started
chain_drive_started
chain_drive_completed (input_rows=59986052, matched_rows=4368)
finalized
```

The output digest is
`ArrowIpcDigest([73, 213, 103, 195, 117, 240, 147, 86, 208, 176, 255, 142, 61, 197, 201, 110, 167, 202, 161, 192, 127, 89, 16, 105, 113, 1, 240, 35, 50, 13, 112, 216])`.

The canonical correctness attempt used the board's 4 GiB outer cgroup. Toron completed, but
the in-process harness was then killed while retaining Toron's result and constructing the
independent q18 oracle: the unit reported `oom-kill` at exactly 4 GiB and emitted no terminal
row. The standalone engine result is valid, but this is not a board-passing comparison.

The cause is instrument residency. `strict_q18_oracle` holds the full orders relation and the
per-order quantity totals in co-resident 15-million-entry maps, plus the customer-name state.
That is independent of the admitted engine mechanism but still part of the board's comparison
contract.

Rerunning the unchanged binary and query with 6 GiB of outer harness headroom produced
`phase=compare status=pass` in 226,603.4 ms. Toron's TaskBudget and execution decisions were
unchanged. The combined process reached 4,365,652 KiB max RSS, which byte-explains the first
attempt and provides strong semantic evidence, but it does **not** upgrade the 4 GiB board cell
to `pass@compare`.

## Board controls

All twelve established sf=10 controls passed strict comparison on the same final release
binary and under independent 4 GiB cgroups:

| query | elapsed ms | query | elapsed ms |
| --- | ---: | --- | ---: |
| q03 | 23,166.6 | q13 | 142,293.0 |
| q07 | 33,468.9 | q14 | 12,074.7 |
| q09 | 43,789.0 | q16 | 9,684.5 |
| q10 | 22,461.7 | q19 | 102,140.8 |
| q11 | 9,409.0 | q20 | 30,940.0 |
| q12 | 18,600.0 | q22 | 10,607.2 |

## Verification

All Cargo commands used the repository cgroup wrapper. The final tree passed:

- `fmt --all -- --check`, `check -p toron --all-targets`, and `git diff --check`;
- focused aggregate 29/29, spill 51/51, join 219/219, and streaming 59/59;
- required full Toron library 1,258/1,258;
- full architecture 2,079/2,079;
- `test -p toron --no-fail-fast`, including every integration binary and doc tests (one
  pre-existing ignored simulated Wendelon admin fixture);
- base `toron-tpch`: library 27 passed/1 intentional heavy ignore, CLI 14/14, architecture
  12/12, correctness 175 passed/1 intentional heavy ignore, decimal 12/12, harness 44/44,
  probe 2/2, and doc tests;
- the same `toron-tpch` matrix with `polars-sql-reference --no-fail-fast`, including the
  feature-specific CLI test (15/15).

`exec.rs` remains exactly at its 7,500-line ratchet. Production source contains no query/ADR
branding, memory-note references, or debug output. Spill scratch is empty, no `.perf.data` is
tracked, and only the two production files plus this results document are changed.

## Honest forward arc

The q18 engine ordering/quota thread is closed. Its preserved-chain drive completed in about
213 seconds, so it does not create the accepted throughput wall; no q21 speedup is inferred
from that result.

The bounded follow-on is toron-tpch-only: make `strict_q18_oracle` frugal enough to avoid
co-resident full orders and quantity maps, then rerun the unchanged query and oracle under the
standard 4 GiB cgroup. Only that passing row retires `q18_sf10_oracle_residency_gap` and moves
the board to 18/22. Until then the board remains 17/22, alongside q17's separately named oracle
policy gap. q21 remains profiling-first, while q02 and q05 retain their correctness and
diagnosis threads.
