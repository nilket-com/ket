# ADR 431 — q17 sf=10 oracle boundary fidelity: results

Base plan commit: `6db8ca2 plan: adr 431 q17 sf10 oracle boundary fidelity`.

## Outcome

ADR 431 produced clean outcome 1. At sf=10, Toron's typed per-part Decimal aggregates and its full
selected contribution multiset equal the generator oracle exactly. The independent exact-rational
classification also selects the same rows as the scale-6 Decimal AVG path. The q17 correctness pin
therefore now admits explicit sf=10, the persisted correctness rail reaches `pass@compare`, and the
verified board moves from **20/22 to 21/22**.

No engine execution path changed. The implementation is confined to the TPC-H harness/oracle and the
q17 correctness-scale policy.

## Structural census

The release census ran under `MemoryMax=4G`, `MemoryHigh=4G`, `TasksMax=512`, and
`RuntimeMaxSec=600s` against persisted strict sf=10 Parquet:

```text
target/release/toron-tpch q17-census --mode strict --sf 10 \
  --data-source parquet --data-dir target/tpch/data
```

| fact | result |
| --- | ---: |
| outcome | `clean` |
| common typed structures | exact equality |
| eligible / aggregate parts | 2,044 / 2,044 |
| selected contribution rows | 5,526 |
| rational below / equal / above | 5,526 / 8 / 55,851 |
| rounded below / equal / above | 5,526 / 8 / 55,851 |
| rational-versus-rounded membership differences | 0 |
| per-part aggregate fingerprint, Toron and oracle | `fnv1a64:c5a5d0f8effcb8a5` |
| selected-row fingerprint, Toron and oracle | `fnv1a64:e8e960a0a6f7b2ca` |
| outer wall / max RSS | 3:59.04 / 348,940 KiB |

The fingerprints are receipts only. The gate compared the sorted `Q17PartAggregate` and
`Q17SelectedRow` vectors cell-for-cell before computing the outcome. The selected-row tuple carries
part, order, supplier, quantity raw, and extended-price raw values, so equal counts or sums cannot
hide compensating membership changes.

The eight exact-boundary rows are useful positive evidence: both classifiers put them in the equal
bucket and strict `<` excludes them. No row crosses membership because of scale-6 AVG rounding.

### Bounded diagnostic shape

The plan's direct join-shaped aggregate query first reproduced a 512-byte scan-output accumulator
overrun (`1,073,742,336` requested versus `1,073,741,824` available). This is an unrelated query-shape
fallthrough: ADR 417's streaming recognizer covers the real q17 global aggregate but not this grouped
diagnostic.

The final census stayed harness-only. It materialized the 2,044 eligible part keys from the generator,
then ran eight 256-key chunks. Each storage predicate is composed from at-most-32-item `IN` lists,
staying under both the frontend's 32-item list limit and the 4,096-byte PBC cap. Toron still computes
every `SUM`, `COUNT`, scale-6 `AVG`, `0.2 * AVG`, and Decimal comparison; no oracle threshold enters
the SQL. The small-key sf=0.02 control produced the same exact receipts as the original join-shaped
diagnostic.

This bounded form measures Decimal boundary fidelity, not part-filter fidelity. The final persisted
q17 `pass@compare` below independently executes the real Brand#23/MED BOX join and correlated scalar
predicate.

## Scale policy

One pure `q17_correctness_scale(Option<f64>)` policy now owns the contract:

- absent scale → the existing non-vacuous 0.02 default;
- explicit 0.02 or 10 → accepted;
- 0.01 and every other scale → `q17_sf10_oracle_policy_gap`.

The CLI delegates to it, the data-source correctness entry validates through it, and the memory route
threads the validated scale into `check_strict_q17` instead of silently restoring 0.02. The former
empty-data-root policy-gap test was replaced by pure unit coverage; no sf=10 live run was smuggled into
the unit suite.

## Live board gate

Both commands used the final release binary and the same unchanged 4 GiB / 600 second envelope.

| rail | result | engine/compare wall | outer wall | max RSS | spill bytes |
| --- | --- | ---: | ---: | ---: | ---: |
| persisted `perf-run-one` | pass, one row | 55,717.5 ms | 59.49 s | 1,110,144 KiB | 961,584 |
| persisted correctness | `phase=compare status=pass` | 72,478.1 ms | 72.54 s | 1,170,428 KiB | n/a |

The reach row retained plan fingerprint `97ca111db986c14d`, Arrow IPC digest
`ArrowIpcDigest([186, 221, 92, 13, 22, 224, 68, 177, 232, 94, 233, 203, 226, 80, 127, 1, 252,
111, 190, 7, 239, 206, 27, 19, 109, 52, 87, 144, 17, 24, 72, 235])`, and output schema
fingerprint `fnv1a64:593a01f287d0e7d8`. Board authority comes from the persisted
`pass@compare`, which asserted the exact one-column `Decimal128(20,6)` schema and raw value.

Normal-path join scratch was empty after the runs.

## Verification

- `cargo fmt --all -- --check`: clean.
- `scripts/cargo-cgroup test -p toron --lib`: 1,282 passed, 0 failed.
- `scripts/cargo-cgroup test -p toron --test architecture_test`: 2,079 passed, 0 failed.
- `scripts/cargo-cgroup test -p toron --no-fail-fast`: all test binaries and doc tests green; no
  failures.
- `scripts/cargo-cgroup test -p toron-tpch --no-fail-fast`: 310 passed, 0 failed, 2 ignored.
- `git diff --check`: clean.
- `git ls-files '*.perf.data'`: empty.

## Forward arc

The board is **21/22**. q02 remains the final cell: the 1.738 GB monolithic join-output
materialization needs the execution-model join-output streaming/spill component split.
