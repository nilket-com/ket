# impl 525 - partsupp lifecycle bounds plan

The minimum sufficient attribution cut: per-replay prep and cleanup clocks around existing
lifecycle regions, one fail-closed partition rule for the reconciler, E computed from ADR
516's untouched replay-2 parent plus the two new shares. Generic probing is not touched in
any way. Two strikes — perturbation or reconciliation, either cell — park q09 attribution.

## Immutable inputs

- tree: post-524-red main (`ffde1074` lineage; executable source byte-identical to
  post-523), all activated knobs default on;
- exact coordinate: strict sf=8 persisted Parquet (format 2, full-hash), budgets 2 GiB /
  2 GiB explicit; fixed floor 3,554 ms;
- fresh grounding (at `8e927b92`): replay-2 parent 1,585 ms (reconciled); runtime_prep
  1,639 ms and cleanup 690 ms, both invocation-scoped and unsplit; builds part 87,125 /
  partsupp 6,400,000 / orders 12,000,000 / nation 25; E interval [1,585, 3,914];
- ADR 524's red receipt (`ffde1074`), binding on this cut: the generic probe's execution
  order must not change (no windowing, no staging, no per-row timers), and replay-scoped
  children under an invocation-scoped parent need an explicit partition rule;
- ADR 516 contracts: batch-scoped tokens, reconciliation-or-fail, perturbation gate at
  absolute 3% median over five interleaved pairs; 092e574a once-per-runtime construction.

## Step 1 - clocks and the partition rule

In exec/join/streaming.rs, profiler-gated:

- one `prep_build` child per suffix replay identity bracketing that build's existing
  preparation region (no code motion), recording elapsed, rows, retained bytes;
- one `cleanup_build` child per replay bracketing its existing teardown region likewise;
- the node-attribution reconciler learns one fail-closed rule: replay-scoped children
  partition an invocation-scoped parent iff replay IDs are unique, required coverage is
  complete, summed child time ≤ parent, and exactly one explicit global residual remains;
  any violation fails the profile with a named verdict.

No other stage, counter, or code path changes; the generic probe path is untouched.

## Step 2 - tests and seal

Structural: profiler-off byte-identity; once-per-runtime construction; partition-rule
batteries (duplicate replay ID, missing replay, child sum exceeding parent, absent residual —
each fails closed); q08 profiles gain only the new prep/cleanup children. Full battery with
direct exit statuses; seal `impl: adr 525 partsupp lifecycle bounds`, push, release build.

## Step 3 - gates and the envelope

From the sealed release binary at the exact coordinate:

- five interleaved profiler-off/on pairs for **q09** and five for **q08**; absolute median
  bound 3% per cell; identity (fingerprint, digest, rows, route, spill) required per pair;
- q08 must retain complete stage coverage; q09 may retain only ADR 516's three known,
  unsupported replay-2 generic leaves (`suffix_lookup`, `suffix_match_expand`,
  `suffix_payload_take`). They do not count as an ADR 525 rejection because the reconciled
  replay-2 `suffix_probe` parent is E's declared input. Any wider missing set, failed existing
  parent, or failed new lifecycle partition does count;
- **two strikes anywhere — perturbation or reconciliation, either cell — park q09
  attribution; the board moves to the shared-fold family with no third attempt;**
- on clean gates, one profiled q09 run supplies
  `E = prep_build(partsupp) + suffix_probe(replay 2) + cleanup_build(partsupp)`.

## Step 4 - receipt and the predeclared decision

`perf:` receipt with the per-build prep/cleanup table, the partition-rule verdicts, both
cells' gate tables, and E against 3,554 ms: below → q09 parked with the split recorded;
at-or-above → the compact composite-key lifecycle is the named successor target. No remedy
selected either way.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.

## Measured outcome - lifecycle envelope clears the floor

The mechanism sealed as `6ace39f5` after the plan amendment
`d643ebea` made ADR 516's inherited generic-key opacity explicit. Generic probing is unchanged:
the implementation adds only replay-scoped `prep_build` / `cleanup_build` clocks, cached owner
facts, and the invocation-parent partition receipt.

The seal battery passed with direct exit 0: both fmt legs; 1,458 library tests; all 2,095
architecture tests; the full `--no-fail-fast` suite; `toron-tpch`; the Toron telemetry, base
TPC-H, and Polars-reference TPC-H all-targets checks; and `git diff --check`. The release binary
was then rebuilt from the pushed seal before measurement.

### Perturbation gates

Both five-pair gates pass their predeclared absolute-median 3% bound:

| cell | pair | profiler off (ms) | profiler on (ms) | `(on - off) / off` |
|---|---:|---:|---:|---:|
| q09 | 1 | 10,825.602130 | 10,844.520661 | +0.174757% |
| q09 | 2 | 10,792.605168 | 11,049.042955 | +2.376051% |
| q09 | 3 | 10,788.775677 | 10,985.318328 | +1.821733% |
| q09 | 4 | 10,715.447201 | 10,971.284204 | +2.387553% |
| q09 | 5 | 10,735.289609 | 10,988.599383 | +2.359599% |
| q08 | 1 | 4,659.599710 | 4,851.054883 | +4.108833% |
| q08 | 2 | 4,664.733990 | 4,838.047435 | +3.715398% |
| q08 | 3 | 4,680.215102 | 4,813.163441 | +2.840646% |
| q08 | 4 | 4,729.821147 | 4,842.432366 | +2.380877% |
| q08 | 5 | 4,759.329177 | 4,785.752864 | +0.555198% |

The q09 absolute median is **2.359599%**; q08's is **2.840646%**. q08 has two individual
pairs above 3%, but the predeclared gate is the five-pair median, not an every-pair maximum;
the run set is neither extended nor corrected.

All 20 children report the sealed build, full-hash verification, pass status, identical
off/on fingerprints, digests, and local-admission receipts per cell, stable row counts (q09
175; q08 2), and zero spill. The profiler-off rows do not serialize a handler-route field; the
route witness is therefore every on-arm reporting `reoriented_inner_chain_grouped`, paired with
the off/on plan and admission identity — not a claim that an absent off-arm field is a recorded
route fact. Every on-arm publishes exactly one passing replay partition for each of
`runtime_prep` and `cleanup`; q08 retains complete stage coverage. q09 retains exactly the three
predeclared unsupported replay-2 leaves and no others, while its existing `suffix_probe` parent
and both new partitions reconcile. No strike fires.

### Partsupp lifecycle envelope

A separate, non-selected sealed q09 profile (`adr525_q09_envelope_6ace39f5`) supplies E:

| component | replay | rows | retained bytes | time (ms) |
|---|---:|---:|---:|---:|
| `prep_build` | 2 | 6,400,000 | 996,802,848 | 1,485.119 |
| existing `suffix_probe` parent | 2 | - | - | 1,918.675 |
| `cleanup_build` upper bound | 2 | 6,400,000 | 996,802,848 | 729.717 |
| **E** | | | | **4,133.511** |

Both invocation-parent partitions pass: runtime prep attributes 2,203,498 of 2,203,504 us
(6 us residual), and cleanup attributes 801,325 of 801,333 us (8 us residual). The five
gated q09 on-arms independently put E in **[4,088.985, 4,147.156] ms**.

E therefore clears the fixed 3,554 ms floor by **579.511 ms** in the decision profile. Per the
predeclared rule, q09 is not parked: the compact composite-key lifecycle is a legitimate
single-envelope successor target. This is maximum removable ownership, because teardown is
order-sensitive; ADR 525 selects no layout, probe, or cleanup remedy and assigns no activation
claim to the quarantined 64-row-window observation.
