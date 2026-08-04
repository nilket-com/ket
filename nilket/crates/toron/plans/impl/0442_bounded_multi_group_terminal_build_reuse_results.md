# ADR 442 results - bounded multi-group terminal-build reuse rejected

## Verdict

ADR 442 is an **honest red**. The mechanism worked exactly as designed, but it did not clear its
predeclared ADR 356 materiality gate. The implementation is removed; no engine or test change is
sealed.

The terminal build consistently merged the original 22 chooser groups into **12 passes**, reducing
terminal routed rows from **1,319,693,144** (`22 x 59,986,052`) to **719,832,624**
(`12 x 59,986,052`, -45.45%). All accepted children had identical whole-group boundaries, zero
pre-write rejections/retries, and a peak active terminal spill of **308,542,768 bytes**, 76.63% of the
unchanged 384 MiB active budget.

That mechanical win did not translate into a material, stable end-to-end win:

- the first implementation's stable retained retry measured **195,703.859 ms**, an **11.917%**
  reduction against the 222,182.319 ms baseline, short of the fixed 12.78% floor by **0.863 percentage
  points**;
- one bounded revision removed redundant aggregate-ownership scans while preserving the same safety
  checks and geometry; its final sample measured **213,570.799 ms**, only **3.876%** faster, while its
  0.15669 spread raised the dynamic band to **31.339%**.

The fixed 10% packing margin, unchanged 512 MiB reservation, dynamic-band formula, and retry ceiling
were never moved after measurement. Carrying the implementation would add a new spill reservation,
calibration/packing state machine, profiler schema, and fallback surface for a sub-band result, so the
correct disposition is removal rather than a cosmetic green.

The strict board remains **22/22** on the sealed ADR 441 engine.

## Conditions and provenance

| item | value |
|---|---|
| sealed engine baseline | ADR 441 implementation `de8d1cc` |
| committed ADR 442 plan | `1fc6e2b` |
| measurement date | 2026-07-16 CDT |
| final revision release binary SHA-256 | `65e966d93235d262f31f4a2c777112c7212958778d5ac7417d2d4795eadee5d2` |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| data | persisted strict sf=10 Parquet under `target/tpch/data` |
| throughput scope | `MemoryHigh=infinity`, `MemoryMax=infinity`, `AllowedCPUs=0-1`, `CPUQuota=150%`, `TasksMax=512`, `RuntimeMaxSec=3600` |
| receipt surface | `TORON_TERMINAL_BUILD_PACK_RECEIPT_DIR` (attempted implementation only) |
| baseline | 222,182.319 ms; spread 0.0639 |
| band | `max(0.10, 2 x max(0.0639, current_spread))` |

The pre-revision executable was overwritten by the source-matching final rebuild before its rejection
receipt was drafted, so no executable hash is claimed for those discarded attempts. Their raw JSON,
time output, scope metadata, and pass receipts remain under ignored `target/tpch/adr442/`; they do not
substitute for the final source-matching rail.

## Mechanism receipt

Every measured child in every attempt reported:

```text
original_group_count=22
pass_count=12
retry_count=0
group_runs=0-1,1-3,3-5,5-7,7-9,9-11,11-13,13-15,15-17,17-19,19-21,21-22
peak_active_spill_bytes=308542768
```

Pass 0 remained chooser group 0. Every later pass merged two contiguous original groups except the
final one-group suffix. The maximum-ratio calibration produced stable predicted/actual values; no
child entered the suffix fallback or wrote a rejected page. Cumulative spill stayed exactly
**4,380,793,312 bytes**.

The result tuple was invariant in every row:

- plan fingerprint `d8b60e3a03bfaf79`;
- Arrow IPC digest
  `ArrowIpcDigest([73, 213, 103, 195, 117, 240, 147, 86, 208, 176, 255, 142, 61, 197, 201, 110, 167, 202, 161, 192, 127, 89, 16, 105, 113, 1, 240, 35, 50, 13, 112, 216])`;
- schema fingerprint `fnv1a64:6bcce34314d4472c`;
- 100 output rows;
- spill bytes `4,380,793,312`;
- join-spill scratch empty after every scope.

## Measurement history

### First implementation

The 199,241.216 ms phase-0 child was used only as cache-conditioning warmup. Its first seven-row
attempt was unstable and therefore not banked:

| min ms | median ms | max ms | spread | baseline change | disposition |
|---:|---:|---:|---:|---:|---|
| 185,084.010 | 224,780.659 | 241,862.390 | 0.25259 | 1.17% slower | unstable; one retry required |

The one allowed retry was stable:

| min ms | median ms | max ms | spread | dynamic band | reduction | result |
|---:|---:|---:|---:|---:|---:|---|
| 188,935.565 | 195,703.859 | 199,558.669 | 0.05428 | 12.78% | 11.917% | **red, sub-band** |

No attempts were averaged. The faster retry was selected because it was the protocol-mandated stable
retry, not because it favored the mechanism.

### Bounded accounting revision

The initial implementation recomputed aggregate writer ownership after every partition write. The
only revision moved that actual check to the batch boundary, retaining the projected pre-growth check,
the actual post-batch check, and the existing per-writer guards. Packing, margin, reservation, and
fallback policy were unchanged.

After one discarded 186,396.580 ms warmup, the seven source-matching measured rows were:

| child | execute ms | peak RSS KiB | pass count | retries | invariant result |
|---:|---:|---:|---:|---:|---|
| 1 | 188,022.224 | 1,259,868 | 12 | 0 | yes |
| 2 | 191,527.434 | 1,257,540 | 12 | 0 | yes |
| 3 | 195,709.958 | 1,231,524 | 12 | 0 | yes |
| 4 | 213,570.799 | 1,259,024 | 12 | 0 | yes |
| 5 | 219,167.547 | 1,260,436 | 12 | 0 | yes |
| 6 | 221,487.259 | 1,232,504 | 12 | 0 | yes |
| 7 | 220,007.272 | 1,231,596 | 12 | 0 | yes |

| min ms | median ms | max ms | spread | dynamic band | reduction | result |
|---:|---:|---:|---:|---:|---:|---|
| 188,022.224 | 213,570.799 | 221,487.259 | 0.15669 | 31.339% | 3.876% | **red, sub-band** |

The sample is stable by the <=0.20 rail. Its higher current spread mechanically raises the dynamic
band; no fixed-floor substitution is made.

## Attempted implementation and focused gates

The removed implementation had:

- a terminal-only shared ciphertext reservation enforced before file write;
- whole-original-group packing with checked `u128` max-ratio projection and a fixed 10% margin;
- exact one-group suffix fallback after a wider-pack rejection;
- aggregate projected-pre-growth and actual-post-batch residency guards;
- schema-2 membership-chain pass receipts plus a timer-free speed-cohort receipt;
- focused calibration, overflow/fallback, pre-write file-length, atomic receipt, parity, and
  architecture tests.

Before removal, `cargo test -p toron --lib` passed **1,316/0** and
`cargo test -p toron --test architecture_test` passed **2,086/0**. The broader no-fail-fast,
toron-tpch, ledger reread, and 4 GiB survival rails were not run because the throughput seal gate is
terminal and failed first. Correctness was nevertheless non-vacuously invariant across all live
speed rows.

## Removal and forward arc

The implementation and its tests are removed after the red gate. Engine and test sources match the
ADR 441 seal; the canonical ADR/implementation plan and this results receipt remain as durable
provenance so the same bounded-packing spike is not repeated.

ADR 441's diagnosis remains valid, but bounded reuse under the unchanged disk reservation is not a
material enough end-to-end cut. Strict one-pass reuse remains separately deferred behind an explicit
disk-envelope authorization and like-for-like baseline. The next independent large-wall thread is
q05 diagnosis; q17 remeasurement and q19 re-diagnosis remain queued.
