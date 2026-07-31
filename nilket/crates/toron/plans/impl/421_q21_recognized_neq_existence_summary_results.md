# ADR 421 results - q21 recognized-Neq existence summary

## Outcome

ADR 421 removes q21's `semi/anti existence-set spill resident state` producer and the latent
existence-page probe amplification without changing a cap or fallback producer. Both replay-3
LeftSemi and replay-4 LeftAnti builds select the chain-owned resident summary, write no spill
pages, and probe without page reads.

The primary sf=10 run advances to a new named wall rather than reaching output: **post-summary
q21 chain probe/fold runtime exceeds the unchanged 600-second harness gate**. This is not a Toron
budget producer. Durable build profiles are written before the timeout and prove that both
summaries completed in 6.87 seconds total; the remainder is downstream base/inner/semi/anti probe
and grouped-fold work. q21 completes strict `pass@compare` at sf=1, so the new summary semantics
are oracle-checked on a completing persisted run.

## Phase 0 and premise verification

The final sf=10 build profiles reproduce the council census exactly:

| stage | build rows | unique keys | Single keys | Multi keys | entry capacity | bucket capacity |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| replay 3, full lineitem | 59,986,052 | 15,000,000 | 2,143,942 | 12,856,058 | 16,777,216 | 33,554,432 |
| replay 4, receipt-after-commit | 37,929,348 | 13,753,474 | 3,372,250 | 10,381,224 | 16,777,216 | 33,554,432 |

Selection itself is the bound-program proof: the chain-owned preflight admits only a single
Boolean `Binary { op: Neq }` root whose direct children split to one preserved and one existence
column, with both keys and values schema-cross-checked as non-null Int64. Both operand orders are
locked. Any cast, wrapper, same-side pair, bad ordinal, multi-root program, equi operator,
nullable column, non-Int64 key/value, or membership-only stage rejects the pair before scan I/O.

The persisted plan does **not** expose a usable key-NDV hint at the summary seam
(`key_count_hint: null` for both profiles). The implementation therefore exercised its checked
dynamic-growth path at sf=10; the optional metadata reservation is not credited with the win.
Only `DistinctCountFact::Exact` may drive that reservation; approximate NDV is explicitly locked
to `None` so optimizer evidence cannot become hard allocation authority.
The clustered last-key cache avoids redundant hash probes for adjacent equal keys but is not an
ordering assumption: every key change still performs the full table lookup, and non-adjacent
duplicates are covered by the representation/parity locks.

## The cut

`NeqExistenceSummary` is a primitive open-addressed table:

```text
entries: { key: i64, representative: i64 }   # 16 bytes
buckets: u32 entry indices, max load 0.5
multi:   one bit per entry-capacity slot
```

For a key, missing means no residual match; a single representative matches iff it differs from
the probe value; `Multi` always matches. The existing LeftSemi/LeftAnti selection consumes that
matched bitmap unchanged. The representation is full-domain: no sentinel, u32 value narrowing,
or data-dependent eligibility decision exists.

Eligibility is atomic and chain-owned. Immediately after the q21 topology extractor and
projection marking, both stages are recognized together. Both select the summary or both retain
the existing `SpilledSemiAntiExistenceBuild`; the generic preparer cannot self-select and no mixed
summary/spilled pair exists.

Growth is explicit and guard-first. Entry, bitset, and bucket allocations are separate checked
transactions; current capacity plus the complete new allocation is charged before allocation,
then actual capacities are observed. Insertion cannot trigger implicit `Vec` growth. The owner
baseline includes every prepared inner runtime and, for replay 4, the completed replay-3 summary.
All post-preparation exits now pass through unconditional async runtime cleanup.

Shrink-at-finish was omitted. No in-place/no-double-allocation proof exists, and the ADR's
1,000,898,648-byte anti-shrink transient would reduce rather than improve peak headroom.

## Byte and mechanism evidence

Profile source: `target/tpch/adr421/q21_final.profile`, emitted through the off-by-default
`TORON_EXISTENCE_SUMMARY_PROFILE_PATH` diagnostic.

| stage | accounted bytes | owner baseline | guarded combined peak | page reads | build time |
| --- | ---: | ---: | ---: | ---: | ---: |
| replay 3 | 404,750,336 | 66,784 | 471,925,984 | 0 | 3,574,213 us |
| replay 4 | 404,750,336 | 404,817,120 | 876,676,320 | 0 | 3,297,806 us |
| completed pair | 809,500,672 | - | 876,676,320 / 1,073,741,824 | **0** | 6,872,019 us |

The completed pair is byte-identical to the plan arithmetic. The higher 876,676,320 peak is the
honest dynamic-growth allocation peak including the live first summary and inner-runtime
baseline. It leaves 197,065,504 bytes under the unchanged 1 GiB retained-footprint cap. Summary
mode owns no file/page object; the profile's `page_reads: 0` and the direct zero-page unit lock
are the discriminating proof.

The approximately sf=11 power-of-two ceiling remains. Neither the last-key cache nor optional
NDV reservation changes completed capacity or the next doubling. Sharding this summary remains
the structural follow-on.

## Correctness and locks

Focused locks (`recognized_neq_summary`, 8/8):

- 16-byte full-domain entry, exact bucket/entry/bitset accounting, and growth-boundary behavior;
- low-cap guard with the live baseline included;
- two-runtime combined guard where either summary fits alone but the pair fails;
- atomic two-stage, both-operand-order selection with zero scan requests;
- complete structure/type/nullability/membership reject matrix;
- LeftSemi and LeftAnti parity against the residual-evaluating in-memory twin for missing,
  repeated-Single, Multi, same-value, and different-value cases;
- page-free real scan preparation plus durable rows/keys/Multi/accounting profile;
- scan-NDV hint mapping and no-growth behavior when metadata is present.

Completing live correctness:

```text
q21 sf=1 strict: phase=compare status=pass elapsed_ms=7607.3
profile replay 3: 6,001,215 rows / 1,500,000 keys / 50,593,792 bytes / 0 page reads
profile replay 4: 3,793,296 rows / 1,375,365 keys / 50,593,792 bytes / 0 page reads
post-audit final release rebuild: phase=compare status=pass elapsed_ms=4178.4
```

Broader gates:

```text
spill lib filter:      50 / 50
join lib filter:      212 / 212
streaming lib filter:  53 / 53
architecture:        2079 / 2079
exec.rs ratchet:      7500 / 7500
toron --no-fail-fast: pass, all targets (lib 1247 / 1247)
toron-tpch + Polars:  pass, all targets (main lib 41 / 41; q14 probe 2 / 2)
```

## sf=10 next wall

The final run uses the unchanged cgroup and runtime contract:

```text
MemoryMax=4G MemoryHigh=4G TasksMax=512 RuntimeMaxSec=600s
q21 strict sf=10 parquet, persisted fixture
```

Result: `systemd Result=timeout` at 600 seconds after both profiles were durably emitted;
`CPUUsageNSec=603,224,021,000`, cgroup `MemoryPeak=2,242,555,904`. The removed producer never
appears, both summaries are complete, combined guarded peak is below cap, and both page-read
counters are zero. The next driver is therefore **post-summary q21 chain probe/fold runtime**,
not another existence representation or page-residency wall. External timeout termination left
the killed process's inner-spill scratch; the runbook cleanup removed it before controls. Typed
executor error exits remain covered by unconditional post-preparation cleanup.

## Controls and hygiene

q18 reproduces byte-exactly on the profiled release artifact:

```text
fingerprint d8b60e3a03bfaf79
streamed inner join build spill writer
536,899,968 / 536,870,912
```

All eleven spill-bearing controls pass strict sf=10 comparison and retain their exact forensic
spill signatures:

| query | compare | spill bytes |
| --- | --- | ---: |
| q03 | pass | 233,338,304 |
| q07 | pass | 134,732,480 |
| q09 | pass | 108,992,256 |
| q10 | pass | 215,703,848 |
| q12 | pass | 159,510,976 |
| q13 | pass | 9,152,992 |
| q14 | pass | 36,464,000 |
| q16 | pass | 60,078,400 |
| q19 | pass | 51,471,552 |
| q20 | pass | 256,104,392 |
| q22 | pass | 31,509,620 |

Final hygiene: fmt/check/diff clean, scratch empty, `exec.rs` exactly 7,500 lines, no debug
prints or query/ADR branding in production/tests, and no tracked `.perf.data`.

The final audit made one subsequent eligibility-narrowing edit: approximate NDV can no longer
preallocate. Both persisted q21 profiles have `key_count_hint: null`, so q21, q18, and the eleven
controls continue through byte-identical runtime branches. The release binary was rebuilt, q21
sf=1 was re-compared, and both no-fail-fast package gates were rerun after that edit.
