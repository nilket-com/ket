# ADR 587 - q21 existence-summary insert decomposition results

Measured 2026-09-14 at the sealed ADR 583 coordinate on the `profiling`
build profile, one instrumented executable for phase 0 and both arms of
every pair, the on arm under the sampling profiler. Three runs were made;
the first was rejected by an instrument gate, the second passed every gate
on a host under interactive load and returned below-floor, the third on a
quiet host is the accepted profile and this record's result. All three are
reported. Phase 0 did not trip in any of them.

**Accepted result.** The three-term owner rule resolves
**`semi_insert_rows_excluding_grow`, 1,122.8 median-scaled ms, 8.75% of the
12,827.1 ms unprofiled execute median**, leading `semi_insert_grow` (450.5
ms) by 672.3 ms against a 532.9 ms off-arm range and a 1,065.9 ms floor.
Growth, at 450.5 ms, clears neither this run's floor nor the 500 ms base
floor: the reservation candidate closes below the floor on this evidence.
The row loop is not removable as such; the exact counters and the sampled
classes select the successor. Every pair's counters say the same thing: of
12,004,370 lookups on the semi alias, **4,370 found a key and 12,000,000
appended**; the build consults the table positively 0.036% of the time,
walks it twice per miss, and 93.4% of the row loop's sampled CPU is the
hash-and-probe class in every pair. No engine route, admission, or writer
changed.

## Identity and coordinate

| identity | value |
|---|---|
| sealed baseline executable | `b436a843b107b2f385d5e7c2486c41b1d1f89acfdb4af75f8a4793c823a05ee4` |
| ADR 585 instrumented executable | `2119272652c4c81f9e2ee9b45bda6236a59cf452ad2c3d1e37b4d5ce4485b444` |
| ADR 586 instrumented executable | `03daafed26923778cc7011512d18bc0d30a43ac19056de004f1a3611ab571ace` |
| instrumented source tree (accepted run) | `d6dcea3d9365b4e9fa5212c411d019022acb6584` (commit `2585460c`, amended before closure) |
| instrumented executable (accepted run) | `3f3a5e2c786d18a8142f4cb8477fd1360566b62bbb23e65eb5fd177620e59c36` |
| build recipe | `a1fad89dc62bb822516be6221b8b771d2f7dcf3a522e82db113e5499889e5a04` (profile `profiling`) |
| dataset manifest | `fnv1a64:0fc5c06d19c5ed23` |
| profiler | `perf version 7.0.14`, kernel 7.0.0-31, `perf_event_paranoid = 2` |

The recipe differs from ADR 586's `54800bbb…` in the profile (`profiling`:
inherits `release`, `debug = 1`, `strip = false`) and in the `Cargo.lock`
hash, which moved when `toron` gained its direct `libc` dependency for
`clock_gettime` and `gettid`; the rustflags hash is unchanged. Codegen is
not assumed identical to `release`; both arms ran on this executable and
the cross-profile distance is reported below, never netted.

Coordinate: q21 alone, strict SF=8 over persisted Parquet, explicit 2 GiB
task memory and spill reserve, partition parallelism 1, full-hash dataset
verification, empty dedicated spill scratch (`target/tpch/adr587-join-spill`,
empty after every run), the local deployment stopped for each run and
restarted after it. Sampling: `perf record -e task-clock:u -F 10000 -k
CLOCK_MONOTONIC -o profiles/pair-NN.perf.data -- <child>` on the on arm
only, converted with `perf script --ns -F pid,tid,time,period,ip,sym,srcline`.
The self-check recording of the harness's own spin produced 9,000 samples
and lost none. Every pair's profile reported zero lost samples
(146,000–149,000 samples each).

Artifacts: accepted run `target/tpch/adr587-q21-final/` (`samples.jsonl`,
`summary.json`, `profiles/pair-NN-on.json`, `pair-NN.perf.data`,
`pair-NN.bins.json`, `selfcheck.perf.data`); rehearsals under
`target/tpch/adr587-q21-rejected-outer-boundary/` and
`target/tpch/adr587-q21-below-floor-under-load/`. All three are unsealed and
carry no candidate authority.

## The three runs

**Run 1, rejected by ADR 507's outer boundary.** Tree `03820adc`,
executable `750e00d6…`. Every band reconciled and the perturbation gate
accepted at 2.36%, but `execute_ms - execute_bound_span` was 14.2–15.9 ms in
every pair against the 10 ms boundary, where ADR 586 had 0.19 ms. The cause
was this cut's own instrument: the windows diagnostic carried 23,436 rows
windows per summary as pretty-printed JSON arrays, and the node profile,
which is written at the root pop inside `execute_ms`, grew from 9.8 KB to
5.7 MB. The rows windows are now encoded as one exact delta string per
summary (`tid@start:gap+duration;...`), the profile is 79 KB at SF=1, and
the outer residual returned to 0.3 ms there and 0.65–0.71 ms at SF=8. The
fix rides the implementation commit; the run is retained as the rehearsal
that found it. Its exact-term shares (rows-excluding-growth 8.87%, growth
3.42%) and class shares (93.9% hash-and-probe, 89.2% rehash) match the
accepted run within a point.

**Run 2, below floor under interactive load.** Tree `d6dcea3d`, executable
`3f3a5e2c…`, the accepted run's build. Every gate passed: profiles
complete, outer residual 0.64–1.12 ms, perturbation 3.84%, coverage 0.90.
Phase 0 read 14,240.9 ms, +8.47% from the reference and inside the 10%
tripwire; the pairs' warmup read 13,742.2 ms; the off arms then read
15,158–15,892 ms. During the pairs two browser content processes on the
host ran at 10–13% CPU each, the 15-minute load average reached 29, and
the package temperature 78 °C against an 80 °C high mark. The off range
was 733.4 ms, the floor 1,466.8 ms, and no exact term cleared it:
rows-excluding-growth 1,312.9 ms (8.33%), growth 492.6 ms (3.12%). The
outcome `below_floor_no_mechanism` is a valid result of the rule and is
retained; the operator authorised a third run on a quiet host, and the
class shares of run 2 (93.6% hash-and-probe, 88.8% rehash) agree with run
3's within a point, so the re-run moved the floor, not the attribution.

**Run 3, accepted.** Quiet host (load 0.27, package 46 °C at start), the
same build as run 2. Reported in full below.

## Phase 0 and perturbation (accepted run)

| step | execute (ms) |
|---|---:|
| host-check warmup | 12,766.3 |
| host-check sample | 12,826.1 |
| reference (ADR 586 off median) | 13,128.5 |

Movement −2.30%, threshold 10%: not tripped. One further unprofiled
warmup (12,694.6 ms), then five adjacent alternating pairs:

| pair | order | off ms | on ms | move | outer residual µs |
|---:|:---:|---:|---:|---:|---:|
| 0 | off, on | 13,158.2 | 13,204.4 | +0.35% | 656 |
| 1 | on, off | 12,625.2 | 13,099.7 | +3.76% | 710 |
| 2 | off, on | 12,683.3 | 13,074.3 | +3.08% | 660 |
| 3 | on, off | 12,946.8 | 13,378.3 | +3.33% | 649 |
| 4 | off, on | 12,827.1 | 13,352.9 | +4.10% | 651 |

Off median **12,827.1 ms**, range 532.9 ms (4.15%); on median 13,204.4 ms,
range 304.0 ms. Median absolute movement **3.33%**, below the 25% rejection
threshold and above ADR 586's 0.47%: the counters, 46,872 windows, and the
10 kHz sampler together cost about 3–4% of execute on the on arm, priced
by the pairs and not otherwise claimed. The off median is 301.4 ms (2.30%)
faster than ADR 586's 13,128.5 ms on the `release` recipe; the profiling
build is not slower, and the distance is reported as cross-profile drift,
not netted.

Every on arm carried a distinct profile path and a distinct `perf.data`;
every off arm carried neither a profile, a receipt, nor a sampler and
retained `diagnostics_disabled = true`. ADR 507's 10 ms outer boundary held
in every pair.

## The four earlier bands, re-reported

All of ADR 585's three sums and ADR 586's fourth reconciled exactly in every
pair; runtime-prep residuals 17,960–19,739 µs, lifecycle residuals under 1
ms, write residuals 3.1 ms semi and 6.5 ms anti, all inside their bounds.

| term | this run (ms) | ADR 586 (ms) | ADR 585 (ms) |
|---|---:|---:|---:|
| `runtime_prep_semi_write` | 2,199.1 | 2,251.6 | 2,323.1 |
| `runtime_prep_anti_write` | 1,857.1 | 1,869.8 | 1,954.0 |
| `runtime_prep_inner_1` | 1,654.0 | 1,711.3 | 1,810.2 |
| `cleanup` | 567.3 | 573.1 | 586.8 |
| `runtime_prep_anti_read` | 493.0 | 506.2 | 528.6 |
| `runtime_prep_semi_read` | 362.2 | 374.6 | 386.2 |
| `semi_write_insert` | 1,583.0 | 1,610.7 | — |
| `semi_write_repartition` | 613.2 | 636.0 | — |
| `semi_write_residual` | 3.1 | 3.3 | — |

Every term sits within this run's 532.9 ms range of both prior records, and
the thirteen-term and five-term rules on this run's receipts again name
`runtime_prep_semi_write` and `semi_write_insert` (12.34%; ADR 586: 12.27%).
Cross-profile, cross-recipe: consistent at the noise boundary, never netted.

## The fifth band: exact terms

Each summary opened **23,436 rows windows**, not the 5,859 the plan
projected: the Exchange op fans every source batch into four partition
batches and `write_batch` is called once per partition batch, so there are
four windows per source batch. The count is exact and the correction is
recorded here. Each summary grew **15 times**, from empty to 1,024 entries
and then by doubling to 16,777,216, with `key_count_hint: null` as
predicted; final entries 12,000,000 (semi) and 11,002,698 (anti) in
33,554,432 buckets, load factor 0.36 and 0.33.

Raw on-arm microseconds; `bound` is `overhead + 5% write_insert`.

| pair | semi write_insert | insert_rows | insert_grow | rows excl. grow | residual | bound | ok |
|---:|---:|---:|---:|---:|---:|---:|:---:|
| 0 | 1,627,591 | 1,610,912 | 453,698 | 1,157,214 | 16,679 | 127,590 | yes |
| 1 | 1,642,459 | 1,626,002 | 462,699 | 1,163,303 | 16,457 | 556,564 | yes |
| 2 | 1,631,575 | 1,615,038 | 467,496 | 1,147,542 | 16,537 | 472,552 | yes |
| 3 | 1,648,033 | 1,631,416 | 465,513 | 1,165,903 | 16,617 | 513,900 | yes |
| 4 | 1,647,373 | 1,630,603 | 461,779 | 1,168,824 | 16,770 | 608,238 | yes |

| pair | anti write_insert | insert_rows | insert_grow | rows excl. grow | residual | ok |
|---:|---:|---:|---:|---:|---:|:---:|
| 0 | 1,420,187 | 1,400,189 | 465,518 | 934,671 | 19,998 | yes |
| 1 | 1,411,677 | 1,391,738 | 450,913 | 940,825 | 19,939 | yes |
| 2 | 1,387,950 | 1,367,944 | 452,718 | 915,226 | 20,006 | yes |
| 3 | 1,422,727 | 1,402,795 | 455,375 | 947,420 | 19,932 | yes |
| 4 | 1,424,141 | 1,404,055 | 459,202 | 944,853 | 20,086 | yes |

Both insert identities held exactly in every pair. The residual (column
downcast and null check, batch disposal, loop overhead) is 16–20 ms, an
order of magnitude inside its bound. Every window in every pair sat on one
thread per summary, the process's main thread (the harness drives the
query with `block_on`, so the future runs on the calling thread); no growth
window escaped its rows window.

The growth series is geometric and its tail owns it: the last three
doublings (4 M, 8 M, 16 M entries) took 52.5, 117.2, and 255.2 ms on the
semi, 424.9 of 453.7 ms (94%); the anti's last three 56.5, 123.4, 259.6 ms,
439.5 of 465.5 ms (94%).

## Counters

Exact, and identical across the five on arms except `probe_steps`, which
varies with the per-run `ahash` seed:

| counter | semi | anti |
|---|---:|---:|
| `cache_hits` | 35,984,637 | 19,337,688 |
| `lookups` | 12,004,370 | 11,005,853 |
| `lookup_found` | 4,370 | 3,155 |
| `appended` | 12,000,000 | 11,002,698 |
| `probe_steps` (5 pairs) | 17,851,922–17,891,604 | 16,518,471–16,557,093 |
| `grow_events` | 15 | 15 |

Three facts the successor needs are now measured, not predicted. The
build almost never finds a key: 0.036% of semi lookups and 0.029% of anti
lookups meet an existing entry, so during the build the table is used
overwhelmingly to confirm absence before an append, twice per miss. Rows
per key is 4.0 on the semi and 2.76 on the filtered anti, so 75% and 64%
of rows take the cache path. And a walk is not one bucket: 17.9 M steps
beyond the home bucket over 24.0 M walks on the semi (0.74 per walk)
despite a final load factor of 0.36, because the load factor is at most
one half only at the moment after each doubling and rises toward it before
the next.

## Sampled classes inside the windows

Per summary, every sample on the window's thread inside a rows window was
classified; growth windows took precedence. Period-weighted shares of the
sampled CPU in each class group, per pair and over all five pairs, with the
all-pairs sample counts:

| semi, rows excluding growth | share per pair | all pairs | samples |
|---|---|---:|---:|
| `hash_and_probe` | 93.2, 93.6, 93.5, 93.6, 93.2 | **93.38%** | 52,400 |
| `other` | 5.0, 4.7, 4.6, 4.6, 4.8 | 4.72% | 2,651 |
| `shared` | 1.9, 1.7, 1.9, 1.9, 2.0 | 1.89% | 1,062 |

| semi, growth | share per pair | all pairs | samples |
|---|---|---:|---:|
| `rehash` | 88.8, 89.2, 90.1, 89.0, 89.0 | **89.22%** | 14,710 |
| `copy` | 8.1, 7.5, 6.5, 7.7, 7.7 | 7.50% | 1,236 |
| `growth_other` | 3.1, 3.3, 3.4, 3.3, 3.3 | 3.28% | 541 |

| anti, rows excluding growth | share per pair | all pairs |
|---|---|---:|
| `hash_and_probe` | 93.7, 94.2, 94.3, 94.0, 94.3 | **94.11%** |
| `other` | 4.7, 4.3, 4.1, 4.6, 4.1 | 4.38% |
| `shared` | 1.6, 1.5, 1.5, 1.4, 1.6 | 1.52% |

| anti, growth | share per pair | all pairs |
|---|---|---:|
| `rehash` | 88.3, 88.7, 88.7, 88.4, 89.6 | **88.73%** |
| `copy` | 8.2, 7.9, 8.0, 8.6, 7.8 | 8.11% |
| `growth_other` | 3.5, 3.4, 3.3, 3.0, 2.7 | 3.17% |

The ranking `hash_and_probe > other > shared` held in every pair for both
summaries and is reported as a consistent observation; `rehash > copy >
growth_other` likewise. No significance is claimed for any share:
frequency-mode periods vary, samples are correlated in time, and skid is
systematic. `shared` (the last-key compare, the loop's own lines,
`observe_value`) is reported unallocated between the paths; the cache
path's sampled share is bounded above by `shared + other`, 6.6% of the row
loop on the semi, a classification bound on the sampled attribution and
not a bound on the path's actual cost. The same rankings and shares held
in runs 1 and 2 within a point.

## Coverage, reported as an estimate

Sampled periods inside the rows windows on the window's thread against the
ledger's `insert_rows`:

| pair | semi coverage | semi discrepancy (ms) | anti coverage | anti discrepancy (ms) |
|---:|---:|---:|---:|---:|
| 0 | 0.8923 | 173.5 | 0.8862 | 159.3 |
| 1 | 0.8932 | 173.6 | 0.8889 | 154.6 |
| 2 | 0.8955 | 168.7 | 0.8856 | 156.4 |
| 3 | 0.8984 | 165.8 | 0.8855 | 160.6 |
| 4 | 0.8943 | 172.3 | 0.8814 | 166.6 |

Median 0.894 (semi) and 0.886 (anti), with 166–174 ms per pair unexplained
on the semi. The discrepancy is located, not attributed: inside the growth
windows the sampled periods cover 0.71 of the ledger's growth time
(324–335 ms sampled against 454–468 ms), while inside the rows windows
outside growth they cover 0.96 (for pair 0, 1,113.4 sampled against
1,157.2 ms). The sampler excludes kernel time by construction (`:u`), so
whatever the kernel does on the thread inside a growth window is invisible
to it; naming that work would need the thread CPU-time account the plan
identified, which this record did not take. On the thread outside every
window there were 131,835–134,521 samples per pair (the rest of the
query); other threads produced 83–145 samples per pair.

## Three-term owner result

Floor `max(500 ms, 2 × 532.9) = 1,065.9 ms`. Median-scaled milliseconds
over the five pairs:

| term | median scaled ms | share of off execute | clears floor |
|---|---:|---:|:---:|
| `semi_insert_rows_excluding_grow` | 1,122.8 | 8.75% | yes |
| `semi_insert_grow` | 450.5 | 3.51% | no |
| `semi_insert_residual` | 16.1 | 0.13% | no |

Per-pair scaled values: rows-excluding-growth 1,113.2–1,153.2 ms; growth
443.6–453.5 ms. **`semi_insert_rows_excluding_grow` is the owner**: its
672.3 ms lead over growth exceeds the 532.9 ms tie range. No residual
leads. The anti insert, reported beside it and not ranked:
rows-excluding-growth 907.6 ms (7.08%), growth 440.7 ms (3.44%), residual
19.3 ms; the same shape, with a shorter row loop for a filtered alias.

## Removable pricing and the successor

The row loop is not removable as such, so no mechanism opens on this
record. The counts and classes select the successor: a build that almost
never finds a key spends 93% of its row-loop CPU hashing and walking to
confirm absence, twice per miss, with 0.74 extra bucket steps per walk. The
candidates, in the order the evidence ranks them, are the second walk on a
miss (bounded above by roughly half the walks, since the first walk already
found the empty bucket the append needs), an order-aware build that
exploits the source's order-key ordering so that a new key is appended
without a walk at all, and the hash or load-factor question the probe-step
count raises. Each must show net execution improvement in paired off/on
runs beside value parity before any of them is more than a candidate.

Growth is priced as gross removable work at 450.5 ms and closes below the
500 ms floor on this evidence: a reservation from a row-count or NDV hint
would remove the fifteen doublings, but it still allocates, initialises,
and first-touches the 16.7 M-entry table, and the term it would remove does
not clear the floor. The observation that 94% of growth is the last three
rehashes stands for a later record. `semi_write_repartition` at 613.2 ms
stays open as ADR 586 left it, gross with net unpriced, and the order
question above governs it too. `cleanup` (567.3 ms) and `runtime_prep_inner_1`
(1,654.0 ms) stay open as their own candidates.

## Implementation and verification

The row loop is generic over `const INSTRUMENTED: bool`: `write_batch` and
`insert` call the `false` monomorphization, which is the loop as it was;
the profiled summary build calls the `true` one, which counts each path,
opens one rows window per `write_batch` around the row loop only, and
brackets each doubling in a nested growth window. Windows are half-open
integer `CLOCK_MONOTONIC` nanoseconds with the opening thread's PID and
TID; a growth window that escapes its rows window or a reversed window is
an instrument defect, not a measurement. The receipt's fifth band
computes `rows_excluding_grow` and the insert residual by checked
subtraction with `instrument_defect` on underflow and requires the windows
diagnostic. The driver gates the profiler (present, paranoid at most 2, a
zero-loss self-check), wraps every on-arm child in `perf record`, decodes
the windows, bins `perf script --ns` samples by thread, window, and
tree-pinned source-line class, and applies the three-term rule with all
three outcomes, the consistent-or-unresolved ranking, and the gross
pricing. Nothing was added to the engine for the sake of attribution: no
`#[inline(never)]`, no frame pointers, no unwind tables.

Tests on the measured tree: seven engine tests (the instrumented and
uninstrumented loops build the same summary and only the first counts;
hand-computed counters; growth windows nested with their capacities and
ordered, disjoint rows windows whose floors sum to the terms; escaping and
reversed windows refused; the write loop publishes one window per
`write_batch`; the compact rows-window encoding round-trips exactly), two
receipt tests (fifth band with exclusive growth; windows exceeding their
parent, or a missing diagnostic, as defects), and fourteen driver tests (line
classes pinned to the tree by their anchors; the `perf script` parser and
the header parser; the binner's half-open edges, thread binding, nested
precedence, class placement, exact identities, an absent thread, and a
summary spanning two threads with one unsampled, reported by id; the
decoder against the engine's encoding; growth owner priced gross; row-loop
owner not removable; tie and below-floor; ranking unresolved when it
changes, when `other` leads, when a pair has no samples in any class, or
when the top two classes tie; refusals for lost samples, absent threads,
and broken algebra; an incomplete insert residual withholding the owner).
Two of those checks were tightened under review after the runs: the
thread check had accepted any one sampled thread where it must require
every thread the windows name, and the ranking had compared class orders
without asking whether the periods were non-zero and strictly separated.
The retained bins of all three runs were re-read against the tightened
rules: every summary in every pair used one thread and every class
ranking, rows and growth, is strictly separated with a non-zero leader, so
no recorded verdict changes and no run was repeated for it. ADR
585's and 586's tests pass unchanged; their receipt fixtures gained the
fifth band's terms because the receipt now requires them.

Ladder on the measured tree (`RUST_MIN_STACK=67108864`): `cargo fmt --all
-- --check` clean; Toron library 1,784 passed; Toron architecture 2,151
passed (one guard caught a `tonic` substring in a helper name during
development; renamed); Toron integration 3,649 passed and doctests passed;
toron-tpch 582 passed, 6 ignored.

The result and its instrumentation land together; there is no stamp-only
commit.
