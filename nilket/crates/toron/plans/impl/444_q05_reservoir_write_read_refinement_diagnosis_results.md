# ADR 444 results — q05 reservoir write/read refinement diagnosis

## Disposition

The schema-2 instrument is retained as a diagnosis substrate. The predeclared
G1–G4 evaluator returned **G4 (honest red)** for the accepted five-child
cohort: the two leading sub-leaves, `write_ipc_encode` and
`write_physical_write`, were not separated by the required 1.15 median ratio,
and no selected sub-leaf cleared the 0.2386 mechanism envelope. No ADR 445
mechanism is selected. The physical-write endpoint comparison is recorded as
run-order drift, not as an intrinsic engine variance or causal mechanism.

## Conditions and provenance

- Plan: `ce7edba` (ADR 444), implementation uncommitted for review.
- Strict q05, sf=10, persisted Parquet, fresh spill scratch per scope.
- Five fresh sequential user scopes; `MemoryHigh/Max=infinity`, CPUs 0–1,
  `CPUQuota=150%`, `TasksMax=512`, `RuntimeMaxSec=3600`.
- Ordinary binary hash: `a3ed7d70d7df462129ebe8e9b262520315530fb3595a62e9f10af1727203fba6`.
- All children used the same plan fingerprint
  `757d7c625669b21a`, five output rows, schema
  `fnv1a64:3f31e3a6be8148ff`, spill `5,353,985,328` bytes, and empty scratch
  after scope completion.
- The final calibration was `calibration-page-selection`; earlier boundary
  calibrations are retained under `target/tpch/adr444/` and disclosed rather
  than mixed into the cohort.

## Cohort and evaluator

| child | root ms | write gap | read gap |
|---:|---:|---:|---:|
| 1 | 179,891.658 | 0.05744 | 0.09570 |
| 2 | 181,791.099 | 0.05583 | 0.09598 |
| 3 | 182,890.056 | 0.05558 | 0.09475 |
| 4 | 185,519.327 | 0.05353 | 0.09591 |
| 5 | 186,697.237 | 0.05255 | 0.09529 |

All parent gaps are below the fixed 10% ceiling and root residuals are about
0.01%. The evaluator reported median shares:

`write_ipc_encode` 0.15534, `write_physical_write` 0.15148,
`read_physical_read` 0.11462, `read_ipc_decode` 0.13608,
`read_cache_lru` 0.11907; all other leaves were smaller. The top write pair
therefore fails the 1.15 separation rule. The largest median share is also
below the 0.2386 envelope. The result is G4, not a producer selection.

The signed max-write/min-write decomposition reconciles exactly:

- parent delta: `6,445,321` µs;
- sub-leaf deltas: `4,938, 8,791, 95,250, -990,753, 11,234,
  5,018,229, 2,310,705` µs;
- gap delta: `-13,073` µs;
- positive contribution: `7,449,147` µs.

`write_physical_write` owns 67.37% of positive contribution under the
predeclared endpoint rule, but the five children were sequential scopes: both
root time and physical-write time increase monotonically from child 1 through
child 5, while IPC encode and physical read do not. The endpoint decomposition
therefore identifies where the sequential drift lands, not an engine-intrinsic
variance owner. Identical spill bytes confirm identical work; the data are
consistent with an accumulating environmental effect (for example storage
state or thermal behavior), but do not identify one.

This is a methodology finding for the next cut: any ADR 445 refinement that
uses this substrate must randomize or interleave scope order (and record the
order) before interpreting a write-path delta as a mechanism effect. The G4
red and the no-mechanism disposition are unchanged.

## Branch, page, and spill receipts

The cohort took the resident branch 64 times and streamed branch 0 times;
pin outcomes were all zero. The reservoir inventory contained 3,683,390
unique `(spill_id,page_index)` pages and `5,270,315,856` ciphertext bytes.
Physical reads and writes matched that inventory exactly, with amplification
1.0, zero cache hits, 3,683,390 misses, and 3,683,390 decodes. The spill-role
and row-flow identities from ADR 443 remained exact; no intermediate identity
was promoted to an oracle.

## Parity and survival

The disabled and both-profilers-enabled sf=4 runs had an exact status,
fingerprint, digest, schema, row-count, and spill tuple match; the 429
residency surface remained unchanged. A strict sf=10 q05 persisted
`pass@compare` under the 4 GiB/no-swap scope with empty scratch afterward:
`elapsed_ms=195629.1`, maximum RSS `3,539,420 KiB`, exit 0.

No mechanism or speed claim is made. The red discriminator is preserved as
written; ADR 445 would require a prospective refinement or a separately
grounded mechanism decision, with scope-order confounding controlled.

## Verification

Focused profile tests (4/4), evaluator tests (3/3; persisted-cohort evaluator
also green when run ignored), and the ADR 444 architecture guard pass. Format,
library, architecture, whole-suite, and Polars-reference gates all completed
green under the implementation-plan commands.
