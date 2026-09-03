# ADR 584 - q21 attribution at the sealed coordinate results

ADR 584 resolves q21's next owner as work outside the profiled drive. The
instrumented query spent a median-scaled **7,887.0 ms**, **55.16%** of the
unprofiled execute median, in `outside_drive`. The next-largest term,
`inner_replay_1`, was 3,321.7 ms and did not clear the predeclared 3,443.4 ms
owner floor. The result opens one successor at the pre-drive lifecycle
decomposition boundary; it does not authorize a route change or wider
admission.

The diagnostic run is deliberately **unsealed** and has
`candidate_authority = false`. It attributes one already-sealed cell; it is not
a replacement performance baseline.

## Identity and coordinate

One instrumented executable ran both arms of every pair. The sealed ADR 583
executable is reported for identity only and was not run under this record.

| identity | value |
|---|---|
| sealed baseline executable | `b436a843b107b2f385d5e7c2486c41b1d1f89acfdb4af75f8a4793c823a05ee4` |
| instrumented source tree | `407488d022bfffa88336f96a083ec4ced76eb12b` |
| instrumented executable | `a173e82585ec7fe0507879f0ed4e7d4dcdbd4520eedfa797a90bd5cfd8bfa8d0` |
| build recipe | `4302132ba9c118768e83eda94783ffe64b177fcd093a0384ff970cb58e945e8b` |
| dataset manifest | `fnv1a64:0fc5c06d19c5ed23` |

The coordinate was q21 alone, strict SF=8 over persisted Parquet, explicit 2
GiB task memory, explicit 2 GiB spill reserve, partition parallelism 1, one
unprofiled warmup, then five adjacent pairs in alternating off/on order. The
deployment remained offline, and the run used its own empty spill scratch.
Artifacts are under `target/tpch/adr584-q21-final/`.

## Perturbation and completeness gates

The warmup execute time was 14,480.2 ms. The off-arm execute median was
14,298.5 ms with a 1,721.7 ms absolute range (12.04%). The on-arm median was
14,319.0 ms with a 1,409.3 ms range (9.84%). Median absolute movement was
**1.38%**, below the predeclared 25% rejection threshold.

The sealed ADR 583 q21 row was 14,956.0 ms; the instrumented executable's off
median was 14,298.5 ms, 657.5 ms or 4.40% faster. That distance is inside the
off arm's own 12.04% range, so it is reported rather than interpreted as an
engine movement.

The row total and child-process wall remain outer boundaries rather than being
folded into execute time:

| boundary | off median / range (ms) | on median / range (ms) |
|---|---:|---:|
| execute | 14,298.5 / 1,721.7 | 14,319.0 / 1,409.3 |
| row total | 14,317.6 / 1,722.2 | 14,339.0 / 1,409.3 |
| parent process wall | 17,316.0 / 1,905.8 | 17,423.6 / 1,435.1 |

| pair | off execute (ms) | on execute (ms) | movement | drive (us) | in-drive residual (us) | residual bound (us) | outside drive (us) | complete |
|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 0 | 13,652.010 | 13,662.639 | +0.08% | 6,313,816 | 218,719 | 326,320 | 7,348,823 | yes |
| 1 | 14,548.308 | 15,071.900 | +3.60% | 6,901,036 | 229,070 | 868,644 | 8,170,864 | yes |
| 2 | 15,373.701 | 14,318.956 | -6.86% | 6,509,955 | 214,823 | 325,498 | 7,809,001 | yes |
| 3 | 14,298.480 | 14,495.123 | +1.38% | 6,461,151 | 220,855 | 519,701 | 8,033,972 | yes |
| 4 | 13,977.652 | 14,156.844 | +1.28% | 6,476,867 | 222,362 | 503,035 | 7,679,977 | yes |

For every sample the seven stages plus the recorded residual equal
`total_drive_micros`, and drive plus `outside_drive` equals execute time in
microseconds. Every residual is below its pair-specific allowance. Attribution
is therefore accepted.

## Nine-term owner result

The owner floor is `max(500 ms, 2 * range_off) = 3,443.4 ms`. These are the
predeclared scaled medians, in descending order:

| term | median scaled ms | share of off execute | clears owner floor |
|---|---:|---:|:---:|
| `outside_drive` | 7,887.0 | 55.16% | yes |
| `inner_replay_1` | 3,321.7 | 23.23% | no |
| `inner_replay_0` | 1,487.3 | 10.40% | no |
| `base_scan` | 629.6 | 4.40% | no |
| `inner_replay_2` | 495.8 | 3.47% | no |
| `in_drive_residual` | 219.5 | 1.54% | no |
| `semi_replay_3` | 136.3 | 0.95% | no |
| `anti_replay_4` | 105.9 | 0.74% | no |
| `grouped_fold` | 9.5 | 0.07% | no |

There is one owner and no tie within the off-arm range. The former replay-1
owner is still material, but it is not authorized as this arc's successor.

## Join identities and ADR 580's residual

The trace-target-derived replay identities map the three local-inner decisions
to the persisted q21 chain:

| replay | logical join | physical build/probe | verdict | missing planner fact |
|---|---|---|---|---|
| replay 0 | `supplier` with filtered l1 `lineitem` | build supplier, probe l1 | admitted at 9.6 MB under 2 GiB | none |
| replay 1 | replay-0 output with filtered `orders` | build orders, probe replay-0 output | declined | build-row bound |
| replay 2 | replay-1 output with filtered `nation` | build nation, probe replay-1 output | declined | build-row bound |

The declines are planner facts, not measured owners. This record does not show
that supplying either estimate would change placement or improve q21.

The same-boundary input ledger assigns exactly one multi-batch concatenation:
replay 0's left/build side, 10 batches, 80,000 rows and 3,040,000 bytes. Its
five measured times were 132, 180, 138, 153 and 171 us. An independent counter
at the shared concat primitive observed exactly one in-handler concat in every
sample, equal to the ledger total; absence for replays 1 through 4 is therefore
complete for this route rather than inferred from three wrapped sites. Thus
ADR 580's historical 425.1 ms right-side
multi-batch row does not reproduce at this boundary; it remains historical
context from another executable and distributed coordinate and is not netted
against these measurements.

## ADR 514 reconciliation and successor

ADR 514's old 9.9 s deferred reservoir envelope is not reproducible here. The
current row has only 27,242,808 spill bytes, and the current drive is the direct
route. That is the expected post-514 plan-shape change, not a budget change.
Its separate runtime-preparation premise does survive structurally: before the
profiled drive begins, the handler prepares inner runtimes 1 and 2 and then
prepares the recognized semi and anti runtimes sequentially. The latter two
consume two aliases of the same lineitem source family, with distinct residual
predicates.

The successor is therefore **q21 outside-drive lifecycle decomposition**. It
separates inner preparation, semi preparation, anti preparation, base
preparation and finalization/cleanup at this exact coordinate. Replay 1's
partitioned build is inside that envelope and may be larger than either
membership-source scan. One source pass feeding both recognized semi/anti
summaries while preserving their separate predicates is a candidate mechanism,
not the conclusion: the measured full filtered-lineitem base scan is only 629.6
ms scaled, already close to the inherited **500 ms** mechanism floor. No fusion
opens unless the decomposition shows that its shared-source subterm clears the
floor independently. The premise handed forward is the 7,887.0 ms / 55.16%
enclosing owner, not a claim that all of it is removable.

## Implementation and verification

The implementation records an explicit profile receipt path on each sample,
exposes the per-replay ledger and trace-target-derived replay identities only
under the diagnostic flag, refuses a stripped admission receipt, and keeps all
diagnostic state out of distributed task telemetry. Its independent in-handler
concat total makes ledger coverage executable. The dedicated command fixes the
coordinate and applies the perturbation, algebra, completeness and owner gates
without manual arithmetic; an invalid negative outside-drive is retained in the
report and withholds attribution.

Verification on the measured tree:

- `cargo fmt --all -- --check`: clean;
- Toron library: 1,765 passed;
- Toron architecture: 2,151 passed;
- toron-tpch library: 224 passed, 4 ignored;
- ADR 584 command tests: 8 passed.

No engine route or admission changed. The result and its instrumentation land
together; there is no stamp-only commit.
