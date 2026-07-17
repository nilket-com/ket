# ADR 446 results — resident-base runtime-filter bridge

This receipt records the first verified implementation run after correcting the
persisted-Parquet harness. The harness now applies `ScanRequest.runtime_filter`
to the full source-schema batch before PBC/projection; without that step the
bridge could be present in the executor while the persisted reachability run
silently measured a no-op.

## Scope and provenance

- Source parent before this implementation receipt: `ae79222974dbe1218b356423588b952110d82f4d`.
- Dataset: persisted strict sf=10 Parquet, manifest fingerprint
  `fnv1a64:8920cf8f2e991230`.
- Query: q05, strict, sf=10, fresh `target/tpch/join_spill`.
- The runtime descriptor is attached to the scan request, not the bound plan;
  the plan fingerprint remains `757d7c625669b21a`.

## Before / after

Baseline is the retained ADR 413/415 q05 receipt: 59,986,052 reservoir input
rows, 5,353,985,328 task spill bytes, 177,026.577 ms, and the digest below.

The corrected implementation produced:

```json
{"query":"q05","status":"pass","elapsed_ms":163298.686344,
 "digest":"ArrowIpcDigest([91,92,150,245,67,32,163,17,163,188,220,35,218,160,201,31,203,54,179,238,52,151,29,167,39,211,38,101,64,253,52,158])",
 "plan_fingerprint":"757d7c625669b21a",
 "output_row_count":5}
```

The profile receipt records `reservoir_build_rows=17,893,398`,
`reservoir_spill_bytes=252,680,912`, `runtime_build_spill_bytes=26,081,536`,
and `task_spill_delta=278,762,448`. Thus the measured reductions are 70.1707%
in reservoir input and 94.7934% in task spill. The single unprofiled wall sample
is 7.7547% below the retained baseline, below q05's 23.86% materiality band;
it is corroboration only, not a speed claim.

The important negative result is explicit: a 94.7934% task-spill reduction did
not produce a measurable wall improvement in this sample. The 7.75% delta is
inside the established q05 noise band, so this cut does not validate a
volume-proportional spill-time model and must not be used to claim that the
reservoir was the wall.

Page geometry is not treated as a like-for-like before/after identity. The
recent 444 cohort reported 3,683,390 pages at roughly 1,431 bytes/page, while
this receipt reports 2,230 reservoir pages at roughly 113,310 bytes/page. The
before page count is from the older cohort and the after count is reservoir-only
(task spill also contains the runtime-build role); the 493x density change is
therefore recorded as an unresolved geometry discrepancy, not explained by the
3.35x row reduction. A future diagnosis must reconcile page construction before
using page counts to attribute time.

The five-child randomized wall cohort required by the plan was not run and is
not claimed. No 4 GiB/no-swap survival cohort was run in this receipt either.
The separate in-memory sf=10 correctness command returned an `unrecognized`
execution result under its own budgeted path; it is not treated as an oracle
receipt. The persisted run's digest/row-count parity and the focused source-
ordinal behavioral test are the correctness evidence recorded here.

## Filter activation and correctness evidence

The profile shows the bridge was active rather than silently declined: the
reservoir input is 17,893,398 rather than the full 59,986,052 lineitem rows,
and the persisted output digest and row count match the retained baseline.
The source-ordinal-before-projection behavioral test is
`adr446_harness_filter_uses_source_ordinal_before_projection`; the existing
executor ordinal/null-safe/context-preservation tests remain green.

The persisted profile also records 2,230 unique ciphertext pages and exactly
2,230 physical reads/writes, with read amplification 1.0 and zero cache hits.
Scratch was empty after the run.

## Gates and limitations

The implementation/library and focused harness tests pass. This receipt does
not convert the single wall sample into an article-grade performance claim, and
it does not claim the unrun survival or five-child rails. Those remain required
before sealing a throughput result or updating the benchmark baseline.

## Forward invalidation (ADR 447 harness correction)

ADR 447 later corrected the persisted-Parquet harness's runtime-filter
composition. The 446 path forced `pbc_projection` to all source columns whenever
the filter was present, so its wall/spill sample combined row reduction with a
full-column read and is not a like-for-like measurement of the bridge. On the
corrected harness, the seven-child q05 characterization cohort measured a
110158.488554 ms median and 3365847016 task spill bytes (with a separate scan
ledger measuring 17893398 filtered lineitem rows). Accordingly, this receipt's
94.7934% spill-reduction/7.7547% wall comparison and its negative
volume-proportional conclusion are withdrawn as invalidated performance
evidence. The historical 446 implementation and correctness evidence remain;
the 446 throughput verdict is not reclassified by this note.
