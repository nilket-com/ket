# ADR 619 - q09's per-access cost growth: a diagnostic

**Status:** CLOSED (results in
`plans/impl/0619_q09_per_access_cost_growth_diagnostic_results.md`).

**The counters** (process-scoped) are consistent with the hypothesis.
Toron's instructions and dTLB-load events per visited row are
approximately flat, while its LLC loads rise about 9% per step. Its
cycles per instruction rise, its cycles per row rise ×1.12 and ×1.14,
and its LLC and dTLB load misses per row rise ×1.29 to ×1.57 per
doubling. DuckDB's misses stay within ×1.12. No stall event was
measured.

**The table-size benchmark** (restricted scope) gives mixed results:
- the synthetic generic build shows per-row growth of a similar order to
  replay 2's (×1.20 and ×1.27, against ×1.12 and ×1.27): a candidate,
  not an isolated cause;
- q09's larger Int64 lookup growth was not reproduced, and the reason is
  unresolved;
- the synthetic Int64 tables' mean probe slots are approximately
  unchanged across sizes.

The replay-2 table is synthetic, with accounting parity to the receipts.
Successor to ADR 618. **A diagnostic record: no
token, no pair, no cut, no saving projected.**

ADR 618 found q09 the one pilot cell whose gap to DuckDB widens at both
measured doublings: 4.13× → 4.52× → 4.93× at SF8 → SF16 → SF32, with
Toron growing ×2.24 and ×2.25. Its probe, materialize, and preparation
stages exceed ×2 at both steps, while scan, repartition, fold, and
cleanup stay near ×2. The cause is open. This record gathers evidence
about one narrowly stated hypothesis. Neither of its measurements can
separate a single explanation conclusively.


## Context: what ADR 618's gated runs already record

These counts come from ADR 618's identity-gated q09 attributed runs,
one per scale, extracted read-only by `adr619_pre_extract.py`
(`38987e6a…`). Each run's row equals its gated timed group's digest and
fingerprint.

| scale | row | node file | join-build file |
|---|---|---|---|
| SF8 | `b75725e6…` | `fee2ebd9…` | `aef414cc…` |
| SF16 | `4d452b22…` | `a61bf61b…` | `135725a4…` |
| SF32 | `30f3e5ff…` | `5e2d9313…` | `4209977f…` |

**The counts are approximately proportional to the scale,** not exact
doublings.

| replay | quantity | SF8 | SF16 | SF32 |
|---|---|---|---|---|
| 1 (`part`, filtered; `int64_primitive`) | build rows = distinct keys | 87,125 | 173,930 | 348,036 |
| | bucket capacity | 262,144 | 524,288 | 1,048,576 |
| | probe input rows (all non-null) | 47,989,007 | 95,988,640 | 192,000,551 |
| | matched (= emitted) | 2,610,723 | 5,218,781 | 10,441,453 |
| | misses (probe input − matched) | 45,378,284 | 90,769,859 | 181,559,098 |
| | lookup windows | 761,371 | 1,522,913 | 3,046,146 |
| 2 (`partsupp`; `generic_hash`, key width 0) | build rows = distinct keys | 6,400,000 | 12,800,000 | 25,600,000 |
| | bucket = entry capacity | 7,340,032 | 14,680,064 | 29,360,128 |
| | probe rows = matched | 2,610,723 | 5,218,781 | 10,441,453 |
| | lookup counters | not recorded (profile `unsupported`) | ″ | ″ |
| 3 (`orders`; `int64_primitive`) | build rows = distinct keys | 12,000,000 | 24,000,000 | 48,000,000 |
| | bucket capacity | 33,554,432 | 67,108,864 | 134,217,728 |
| | probe rows = matched (no misses) | 2,610,723 | 5,218,781 | 10,441,453 |
| 4 (`nation`) | 25 keys in 64 buckets | | | |

Every build has one row per key.
- **Replay 1 is miss-dominated:** about 94.6% of its probes miss, at
  every scale.
- **Replay 2's lookups are uncounted.** Its profile records no lookup
  timing or windows (as in ADR 614), so its probe work is unavailable,
  not zero.

**The displayed load fractions are constant:** 0.332, 0.872, 0.358,
and 0.391 for replays 1 to 4. A constant load fraction does **not**
establish an equal distribution of collisions or probe lengths.

**"Index accounted bytes"** is the profile's accounting of the table's
index: 6,279,152 → 12,542,944 → 25,094,336 bytes for replay 1;
792 MB → 1.58 GB → 3.17 GB for replay 2; and 844 MB → 1.69 GB →
3.38 GB for replay 3. It is an accounting quantity, separate from the
build batch bytes and the row-list bytes. **It is not the hot cache
footprint,** and it is not used as one.

**The timed stages** from the same runs (single-run observations):
- replay 1 lookup: 475.7 → 1,150.9 → 2,915.9 ms;
- replay 2 build: 835.1 → 1,875.2 → 4,760.1 ms, and its payload take
  115.2 → 274.3 → 708.9 ms;
- replay 3 lookup: 74.5 → 179.5 → 400.0 ms, and its build 394.6 →
  810.7 → 1,670.7 ms.

**The host.** strix is an i9-14900K. The measured affinity 0-15 is the
`cpu_core` (P-core) set. Each P-core has 2 MiB of L2, and all share a
36 MiB L3. Transparent huge pages are `madvise`; the base page is
4 KiB. `perf_event_paranoid` is 2, which permits user-space counting on
our own processes. It does **not** by itself prove that any specific
event is available; that is preflighted.

**The hypothesis, narrowly stated.** Per-operation cost in q09's replay
1 and 3 lookups, and in replay 2's build, rises as those tables grow
past cache and TLB reach. Alternatives remain open, and a table-size
change changes several of them at once:
- the capacity and layout;
- the key domain;
- the collision and probe-length distribution;
- the generic hash's key handling;
- allocation behaviour;
- interference from the streamed `lineitem` batches.


## Decision

Two measurements. Each can be **consistent or inconsistent with the
hypothesis, or unresolved**, and neither separates a single
explanation. Neither changes the measured path.

### 1. Hardware counters, process-scoped and named as such

**The events** are preflighted first on strix: the exact `cpu_core`
encodings for `cycles`, `instructions`, LLC loads and misses, and dTLB
loads and misses. For each event, the preflight records:
- its availability and permission;
- a non-zero running time;
- user-only scope, and inheritance to child tasks;
- its semantics and units.

An event reported `<not supported>`, `<not counted>`, with zero running
time, or with a running fraction below 90% is **unavailable, never
zero**. If the full set cannot meet the 90% gate together, it is split
across separate runs, and ratios are never formed between events from
different runs.

**The measured process scopes:**
- **Toron:** `perf stat` (user-only, inherited) around
  `toron-bench run --manifest <ADR 618's manifest> -- perf-run-one
  --engine toron --query q09 --sf N …`. The counted tree is the launcher
  and the staged `toron-tpch` process. Its interval covers startup,
  dataset opening, planning, execution, and digesting, not
  `execute_ms` alone.
  - To keep the file-hash work out of the counts, these runs set
    `TORON_TPCH_SKIP_DATASET_HASH_VERIFY`, which gives manifest-only
    verification. That is a **named departure** for the counter runs
    only.
  - Each row still gates on its digest and plan fingerprint against
    ADR 618's gated group. That is result identity, not file
    verification.
  - **File verification is kept outside the counted interval.** Before
    the counter session, all three datasets are fully hash-verified
    against their manifests. After it, they are verified again, so
    unchanged files are recorded. Each counter row keeps
    `dataset_verification: manifest_only` explicitly. Correctness and
    full-hash checks never run under `perf stat`.
  - What `toron-bench` itself hashes or validates inside the counted
    tree (for example, the manifest and the staged executable) is read
    from the code and recorded.
- **DuckDB:** `perf stat` around the staged DuckDB CLI alone, running
  the harness's exact q09 SQL, bound by the recomputed plan fingerprint
  as in ADR 617. The setup is the same (1 thread, views, a fresh temp
  directory, and the memory read-back). Its result digest is recomputed
  from the saved output and must equal ADR 618's timed digest, or the
  run is withheld. Only that one process is counted, with no harness
  parent. "No warm-up" means none inside the counted process; the cache
  procedure below applies to both engines.

**What is reported,** per engine and scale, over five runs interleaved
across scales, as median and range:
- the raw counts, running fractions, and elapsed time;
- instructions, cycles, and misses per visited row (each engine's own
  counter, as in ADR 618), and CPI.

These are broad process-scope quantities. **Normalizing by scan rows
does not make them query-only miss rates,** and they are never assigned
to a stage (ADR 612).

**The cache procedure, matched across engines.** Before each measured
run, both engines get one untimed execution of the same query at the
same scale, outside `perf stat`. This is a best-effort warm cache, not
equal cache state. Skipping Toron's full-file hash removes its former
incidental priming, and this warm-up replaces it for both engines.

**The protocol:**
- ADR 617/618's isolation: a quiet host, one process at a time, and the
  fail-closed scratch and leftover checks.
- The recorded requested and observed affinity.
- A launch separated from its SSH session, with host records before and
  after.
- **A declared balanced order,** fixed before launch, for the five runs
  per engine and scale.
- The event preflight is captured as its own separate run.
- The counters never run concurrently with other host work.

Counter claims stay process-scoped, and the difference in scope between
the two engines remains a stated limitation.

### 2. A controlled table-size benchmark (test-only)

It is a test-only benchmark in `toron`, not part of any measured
executable, and it **calls the production join-table constructors and
lookup paths.**

**It must reproduce q09's tables,** verified against the receipts
before any timing is read:
- for replays 1 and 3, the `int64_primitive` representation;
- for replay 2, the `generic_hash` representation over q09's bound
  replay-2 key. That key's actual columns and types are checked against
  the bound plan, not assumed to be two Int64 columns;
- the same key counts, bucket and entry capacities, load, and index
  accounting as the extraction above, at each scale's sizes.

A mismatch refuses that size's timings.

**Its method:**
- **The lookup scope is stated exactly.** The benchmark's own random
  number generation and setup allocations are excluded from the timed
  loop. Any allocation, hashing, or key encoding that the chosen
  production lookup path itself performs is kept. If the probes use
  pre-encoded production keys and bypass `join_key_at` or other query
  preparation, the scope is named as that **restricted table API**, and
  no claim is made about q09's complete probe work.
- **Capacity and load follow the table API's own definitions.** A
  usable-entry capacity can differ from the physical buckets, so the
  reported capacity and load are never converted into physical
  occupancy.
- Keys and probes are pre-generated with a fixed seed, outside the
  timed loop. The insertion order is stated. The hash state is held
  consistent across sizes where the production API allows it;
  otherwise the limit is reported, and independent builds are varied.
- The build and the lookup are timed separately.
- There are two lookup mixtures, named separately:
  - **synthetic successful lookups,** with uniform present keys;
  - **replay 1's observed mixture,** about 5.4% hits and 94.6% misses,
    with absent keys drawn from outside the key set.

  Lookup results are consumed into a checksum, and each fixture checks
  its expected hit count, miss count, and checksum. No random number
  generation or allocation happens inside timed lookups.
- Existing probe-work counters are recorded where the production path
  exposes them. Otherwise a test-only probe-length check is added, if
  feasible. The same code at the same load can still do different work
  (ADR 612).
- Each table is warmed before its measured probes. The size order is
  rotated across repetitions. It is pinned to one verified P-core
  hardware thread, with the SMT sibling left idle and host quietness
  recorded.
- **The uncertainty is stated:** 5 independent builds per size, each
  probed 5 times. The report separates build-to-build variation (the
  layout) from repeated-probe variation.
- **The budget:** the 48 M-key and 25.6 M-key tables need several GB
  each. Sizes are run one at a time. A refused allocation or an error is
  reported as that size's result.

**What it can say:** whether per-operation cost rises with table size
for these representations and mixtures on this host. It cannot say
that size alone explains q09. A size change also changes capacity,
layout, key domain, and collision distribution. And a result for
synthetic lookups does not isolate q09's remaining cause.

*Not in this record:* any change to the tables, their layout, hashing,
page size, or join order. A cut is its own plan, paired at the SF8
coordinate.


## Acceptance

- The event preflight, and the counter runs with every run gated, each
  event's running fraction, and the quantities above, or named
  unavailable events.
- The benchmark:
  - its code, test-only, with fixtures;
  - ladders green on bigfoot;
  - its representation checks against the receipts;
  - per size and mixture, build and lookup medians and ranges, with
    both kinds of variation.
- A reading per measurement: consistent or inconsistent with the
  hypothesis, or unresolved.
- Plan commit, then one implementation commit with the benchmark,
  scripts, read-outs, results, and closure.


## Guardrails

- A diagnostic: the measured path is unchanged, and the benchmark is
  test-only.
- Process counters are never attributed to a stage, and never read as
  query-only miss rates.
- Benchmark timings are never compared with query timings as the same
  work.
- The manifest-only verification applies to the counter runs only, and
  is named.
- No strix launch while another record is measuring.


## Risks

- **Probe order.** q09 probes in the `lineitem` stream's order, not in
  uniform random order. The benchmark's mixtures are stated as
  synthetic.
- **Replay 2's lookups are uncounted in production,** so the benchmark's
  replay-2 lookup has no production counterpart to check against. Its
  build does.
