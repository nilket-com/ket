# ADR 585 - q21 outside-drive lifecycle decomposition results

ADR 585 resolves q21's former outside-drive envelope to one owner:
`runtime_prep_semi_write`, at a median-scaled **2,323.1 ms**, **16.64%** of
the unprofiled execute median. The anti-summary write follows at 1,954.0 ms
and replay 1's partitioned inner preparation at 1,810.2 ms. The result opens
one successor at the semi-summary transform-and-write boundary; it does not
yet name a removable mechanism inside that boundary.

The proposed shared lineitem-source pass closes here. Its pairwise removable
read median is **386.2 ms**, below the inherited **500 ms** floor. No fusion
cut opens on the strength of the larger summary-write terms.

The diagnostic run is deliberately **unsealed** and has
`candidate_authority = false`. It decomposes one already-sealed cell; it is
not a replacement performance baseline.

## Identity and coordinate

One instrumented executable ran both arms of every pair. The sealed ADR 583
and instrumented ADR 584 executables are reported for identity only and were
not run under this record.

| identity | value |
|---|---|
| sealed baseline executable | `b436a843b107b2f385d5e7c2486c41b1d1f89acfdb4af75f8a4793c823a05ee4` |
| ADR 584 instrumented executable | `a173e8252a3f37839166a98adad8005f22d590f858121c1fb6ff3f3f6a58ec3f` |
| instrumented source tree | `f761c601735a26eed95bf270b014ed7297875964` |
| instrumented executable | `2119272652c4c81f9e2ee9b45bda6236a59cf452ad2c3d1e37b4d5ce4485b444` |
| build recipe | `fcdf9ca12816d9da39a63948e6f24d6f08e6a55c923e337638b3f2634f0609ab` |
| dataset manifest | `fnv1a64:0fc5c06d19c5ed23` |

The coordinate was q21 alone, strict SF=8 over persisted Parquet, explicit 2
GiB task memory, explicit 2 GiB spill reserve, partition parallelism 1, one
unprofiled warmup, then five adjacent pairs in alternating off/on order. The
deployment remained offline, and the run used its own empty spill scratch.
Artifacts are under `target/tpch/adr585-q21-final/`.

## Perturbation and outer boundaries

The warmup execute time was 15,429.8 ms. The off-arm execute median was
13,958.6 ms with a 247.4 ms absolute range (1.77%). The on-arm median was
13,995.4 ms with a 294.3 ms range (2.10%). Median absolute movement was
**1.16%**, below the predeclared 25% rejection threshold.

The sealed ADR 583 q21 row was 14,956.0 ms. The instrumented executable's off
median was 997.4 ms, or 6.67%, faster. That distance exceeds this cohort's
off-arm range, so the sealed row remains identity and historical context; it
is not netted into the attribution.

Row total and parent-process wall remain outer boundaries rather than being
folded into execute time:

| boundary | off median / range (ms) | on median / range (ms) |
|---|---:|---:|
| execute | 13,958.6 / 247.4 | 13,995.4 / 294.3 |
| row total | 13,977.6 / 247.8 | 14,014.5 / 294.7 |
| parent process wall | 16,939.7 / 298.4 | 17,008.8 / 287.2 |

## Inclusive lifecycle receipt

Every pair satisfies all three additive bands. Times below are raw on-arm
microseconds. `bound` is the pair-specific upper bound applied independently
to both lifecycle residuals; `ok` also includes ADR 507's 10 ms outer bound.

| pair | off ms | on ms | move | runtime prep | base prep | drive | finalize | cleanup | lifecycle residual | handler | outside handler | root | outer residual | bound | ok |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 0 | 14,082.934 | 13,907.119 | -1.25% | 7,011,526 | 2,366 | 6,301,067 | 564 | 590,268 | 763 | 13,906,554 | 392 | 13,906,946 | 173 | 695,328 | yes |
| 1 | 13,835.532 | 13,995.445 | +1.16% | 7,027,742 | 2,243 | 6,378,540 | 561 | 584,977 | 778 | 13,994,841 | 426 | 13,995,267 | 178 | 859,654 | yes |
| 2 | 13,940.591 | 14,097.560 | +1.13% | 7,139,626 | 2,390 | 6,360,126 | 610 | 593,456 | 783 | 14,096,991 | 399 | 14,097,390 | 170 | 861,818 | yes |
| 3 | 13,958.601 | 13,879.745 | -0.56% | 7,022,674 | 2,402 | 6,263,535 | 601 | 589,239 | 740 | 13,879,191 | 380 | 13,879,571 | 174 | 693,960 | yes |
| 4 | 14,004.452 | 14,174.084 | +1.21% | 7,072,389 | 2,350 | 6,504,042 | 587 | 593,404 | 759 | 14,173,531 | 380 | 14,173,911 | 173 | 878,309 | yes |

The runtime-preparation decomposition also reconciles exactly in every pair:

| pair | inner 1 | inner 2 | semi read | semi write | anti read | anti write | runtime residual |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1,763,421 | 1,113 | 388,425 | 2,348,319 | 526,213 | 1,966,435 | 17,600 |
| 1 | 1,780,147 | 2,273 | 388,183 | 2,349,925 | 524,233 | 1,965,392 | 17,589 |
| 2 | 1,869,497 | 1,445 | 390,552 | 2,347,483 | 534,594 | 1,975,980 | 20,075 |
| 3 | 1,800,001 | 1,061 | 388,905 | 2,326,603 | 529,591 | 1,959,102 | 17,411 |
| 4 | 1,840,521 | 1,757 | 388,165 | 2,328,441 | 529,798 | 1,965,934 | 17,773 |

The receipt is attached to exactly one
`inner_prefix_semi_anti_grouped` invocation in every on arm, and its
`invocation_id` equals that profile entry's stable frame identity. The same
entry passes ADR 507's unchanged bridge and exclusive-stage reconciliations.
Every on-arm row carries a distinct profile path; every off arm has neither a
profile nor a receipt and retains `diagnostics_disabled = true`.

## Cross-instrument consistency

ADR 585's median-scaled inclusive drive is 6,305.7 ms, versus ADR 584's
6,394.9 ms chain-drive decomposition, a difference of 89.2 ms. ADR 585's
non-drive sum is 7,651.3 ms, versus ADR 584's 7,887.0 ms `outside_drive`, a
difference of 235.7 ms. Both differences are within this cohort's 247.4 ms
off-arm range. The independent clocks therefore agree at the record's noise
boundary; their terms are reported beside one another and never netted.

## Thirteen-term owner result

The owner floor is `max(500 ms, 2 * range_off) = 500 ms`. These are the
predeclared scaled medians, in descending order:

| term | median scaled ms | share of off execute | clears floor |
|---|---:|---:|:---:|
| `runtime_prep_semi_write` | 2,323.1 | 16.64% | yes |
| `runtime_prep_anti_write` | 1,954.0 | 14.00% | yes |
| `runtime_prep_inner_1` | 1,810.2 | 12.97% | yes |
| `cleanup` | 586.8 | 4.20% | yes |
| `runtime_prep_anti_read` | 528.6 | 3.79% | yes |
| `runtime_prep_semi_read` | 386.2 | 2.77% | no |
| `runtime_prep_residual` | 17.6 | 0.13% | no |
| `base_prep` | 2.4 | 0.02% | no |
| `runtime_prep_inner_2` | 1.4 | 0.01% | no |
| `lifecycle_residual` | 0.8 | 0.01% | no |
| `finalize` | 0.6 | <0.01% | no |
| `outside_handler_span` | 0.4 | <0.01% | no |
| `outer_residual` | 0.2 | <0.01% | no |

Five terms clear the floor, but `runtime_prep_semi_write` is the sole owner:
its 369.1 ms lead over `runtime_prep_anti_write` exceeds the 247.4 ms tie
range. No residual leads, so the result is a lifecycle owner rather than an
incomplete instrument.

The owner boundary contains the semi summary's filter, key evaluation,
repartition, and insertion into the not-equal existence summary. This record
does not separate those activities, so their containing term is not itself a
mechanism. The successor is **q21 semi-summary transform/write decomposition**,
at the same coordinate, before changing the writer or its route. The anti
write and replay-1 build remain material secondary terms rather than being
silently folded into the owner.

## Shared-source candidate

The shared-source candidate is priced from the subterm it could actually
remove, not from either summary's containing preparation. For each pair the
smaller read half was scaled to its off arm, then the median was taken. That
pairwise removable median is **386.2 ms**, below the 500 ms floor. The
candidate therefore closes; the larger 2.3 s and 2.0 s write terms do not
authorize source fusion.

## Implementation and verification

The node-attribution frame retains raw inclusive close-time samples beside
ADR 507's exclusive subtraction and moves them only when an invocation wins.
The q21 handler adds clocks only for the six runtime children and the drive
bracket. Summary reads and transform/writes accumulate independently per
batch. The additive receipt is emitted only on the winning invocation and
records both residuals after checked subtraction. `perf-run-one` records the
profile path, and enabling the node profile now voids candidate authority.

The discriminating nested-child test observes one raw lifecycle stage at 100
microseconds and its exclusive counterpart at 60 microseconds after a known
40-microsecond child; a non-zero-only test could not distinguish those
domains. Further tests pin exact invocation binding, all three sums, non-zero
summary halves, unique paths, the thirteen-term range floor, pairwise rather
than median-of-medians shared-read pricing, and residual-owner withholding.

Verification on the measured tree:

- `cargo fmt --all -- --check`: clean;
- Toron architecture: 2,151 passed;
- Toron library: 1,767 passed;
- Toron integration and doctests: all passed;
- toron-tpch package: 560 passed, 6 ignored;
- ADR 585 focused tests: 8 passed.

No engine route or admission changed. The result and its instrumentation land
together; there is no stamp-only commit.
