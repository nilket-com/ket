# impl 516 - suffix-probe attribution plan

Attribution only: one vocabulary refinement on the reoriented-chain handler, reconciliation
receipts, flow counters, and interleaved perturbation-gated runs. No remedy, no knob, no
behavior change, no cohort.

## Immutable inputs

- tree: post-ADR-515 main (`9f9028b9` lineage), all activated knobs default on;
- exact dataset: strict sf=8 persisted Parquet with `full_hash`; budgets 2 GiB / 2 GiB explicit;
- grounded anchors: q08 handler 7,762.9 ms with suffix_probe 3,834.7; q09 handler 12,663.7 ms
  with suffix probes 4,028.7 / 1,781.7 / 272.0 at 23,436 calls each; runtime_prep 425.1 (q08) /
  1,855.7 (q09); zero spill both cells; RSS 0.8 / 2.4 GiB; `perf_event_paranoid = 4` (host
  sampling unavailable, left untouched);
- predeclared perturbation bound: over 5 adjacent off/on pairs per cell, the median paired relative
  movement `(on - off) / off` must be at most **3%** in absolute value; any run set beyond the bound
  is rejected evidence, and two consecutive fresh rejected sets close the ADR red on
  instrumentation cost. The first rejected set is retained, never edited or selectively extended.

## Step 1 - vocabulary refinement

In the reoriented-chain stage vocabulary (`streaming.rs`), refine each replay-scoped `suffix_probe`
leaf into a parent with three predeclared children plus residual:

- `suffix_lookup` — the existing Int32/Int64 table lookup for one probe window;
- `suffix_match_expand` — expansion of returned row lists into `JoinOutputRow` selections;
- `suffix_payload_take` — selection-index construction and Arrow `take` for one output chunk;
- residual = parent minus children, with the vocabulary's declared boundary; reconciliation
  failure fails the receipt.

The lookup and match phases are interleaved by the production probe driver. Add an optional local
observer to that existing driver: it opens clocks at the existing windows of at most 64 keys,
accumulates `Duration`s without touching the node ledger, and publishes one refinement record when
the enclosing suffix call finishes. Payload clocks wrap the already-separate output chunks. There
are no per-row clocks, no observer allocation when attribution is disabled, and no alternate probe
algorithm in the enabled arm.

The first cut is deliberately primitive-key only. Both q08 and all three q09 suffix runtimes commit
the Int64 representation. A profiled generic-key runtime publishes an explicit unsupported
diagnostic and cannot satisfy the three refinement requirements; it does not fall back to per-row
timing.

Add an invocation-bound suffix observation beside the node-attribution stage arithmetic. It merges
only within one replay identity and records checked sums for:

- suffix calls, input probe rows, non-null keys, and lookup windows;
- matched references, emitted rows, output batches, output column-values, and output array bytes;
- build representation, build rows, distinct keys, build-batch bytes, index-accounted bytes, entry
  capacity, bucket capacity, and fixed-width key bytes.

The output byte counter is `RecordBatch` array memory produced by the `take` phase. It is not labeled
as cache traffic. Locality values are published as integer facts; load is numerator/denominator,
never a rounded authority. All of it activates only under `TORON_NODE_ATTRIBUTION_PROFILE_PATH`.

## Step 2 - tests

- Reconciliation units: all three refinements reconcile only to the parent carrying the same replay
  id; a deliberately overrun child fails the invocation (the gate is live).
- Probe-driver units: Int32 and Int64 windows, a partial tail, nulls, misses, and duplicate-heavy
  builds preserve the existing selection exactly while reporting exact lookup and match counts.
- Counter exactness on execution fixtures: empty-match batches, one key with many references, and
  narrow/wide output schemas pin emitted rows, column-values, and output array bytes.
- Identity: a chain with reordered suffix stages keeps per-replay attribution attached to the
  right build.
- Disabled/unsupported paths: profiler-off constructs no observer; a generic-key profiled fixture
  names unsupported coverage rather than publishing a fabricated split; existing 508-era fields
  remain readable.
- Architecture: reconcile any stage-vocabulary guards; name retargets in the receipt.

## Step 3 - attribution runs and the ranked premises

From the sealed binary at the exact coordinate:

1. Census sanity, run once: profiler-off and profiler-on fingerprints, digests, row counts, route
   facts, and zero spill identical on q08 and q09.
2. Perturbation gate: 5 adjacent off/on pairs per cell; report all raw rows, the median paired
   relative movement, and the 3% verdict. Reject rather than correct or selectively extend.
3. Attribution receipts from the on arms: per cell and per suffix runtime — time split
   (lookup / match expansion / payload take / residual), flow counters, multiplicity,
   output-array volume, and locality proxies.
4. The deliverable table: independently ranked premises for q08's suffix join and each of q09's
   three, each row naming its dominant sub-stage, its share, its multiplicity, and its locality
   proxies. The receipt states plainly that no remedy is selected and the successor must follow
   this split.

## Steps 4-5 - seal and receipt

Cohesive seal of steps 1-2 (`impl: adr 516 suffix probe attribution`), full gate battery with
direct exit statuses, push, release build from the pushed identity; then the step 3 runs and a
receipt commit (`perf: adr 516 rank suffix probe premises`) appending the measured outcome. No
default flips, no knobs, no cohort.

## Repository gates

Run every Cargo leg through `scripts/cargo-cgroup`, never raw `cargo`.

Council seal:

```bash
scripts/cargo-cgroup fmt -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
git diff --check
```

Changed-surface checks:

```bash
scripts/cargo-cgroup fmt -p toron-tpch -- --check
scripts/cargo-cgroup check -p toron --features telemetry --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup check -p toron-tpch --features polars-sql-reference --all-targets
```

Preserve each command's direct exit status. Repeat the applicable gate after any code change made
in response; finish all gates before building the measurement binary.

## Measured outcome — the ranked premises

### Perturbation gate (both cells PASS)

Five adjacent off/on pairs per cell from the approved `092e574a` release binary, all 20 rows
passing, full-hash, one fingerprint and one digest per cell across arms, zero spill everywhere.

| cell | pair movements (on vs off) | median | verdict |
|---|---|---:|---|
| q08 | -0.053%, +1.800%, +0.840%, +1.661%, +1.774% | **+1.661%** | PASS (< 3%) |
| q09 | +2.093%, +0.950%, +1.283%, +1.613%, +1.537% | **+1.537%** | PASS (< 3%) |

The scalar-prefetch corner is recorded as agreed: under prefetch ablation the probe window is one
row, so profiler-plus-ablation would time per row; both cells run batched probing, so the corner
cannot affect this evidence. A future generic instrumentation pass may add the fail-closed scalar
decline.

### The four ranked premises (medians over the five on-runs)

| premise | parent | split (lookup / expand / take / residual) | flow | locality facts |
|---|---:|---|---|---|
| **1. q08 suffix 1 — part** | 3,759.3 ms | 1,481.4 / 618.7 / **1,549.0** / 110.3 — dominant **payload take (41%)**, lookup 39% | probe 47,989,007 = matched = emitted; **multiplicity 1.000**; output 431,901,063 values, **5,040,455,828 B** | int64_primitive, 1.6 M keys, load 0.38, idx 110.4 MB |
| **2. q09 suffix 1 — part** | 3,916.2 ms | 1,509.6 / 626.0 / **1,670.3** / 110.2 — dominant **payload take (43%)**, lookup 39% | probe 47,989,007 = matched = emitted; multiplicity 1.000; output 479,890,070 values, **6,393,624,036 B** | int64_primitive, 1.6 M keys, load 0.38 |
| **3. q09 suffix 2 — partsupp** | 1,743.8 ms | **split unsupported by contract** (generic composite key; status `unsupported`, `passed: false`, take-phase alone 153.5 ms) | probe 2,610,723 = matched = emitted | generic_hash, 6.4 M keys, load 0.87, idx 792.0 MB |
| **4. q09 suffixes 3-4 — orders, nation** | 297.1 + 133.9 ms | lookup 117.6 / 24.4 dominant shares; all components sub-materiality | probe 2,610,723, multiplicity 1.000 | int64_primitive, 12 M and 25 keys |

### What the attribution establishes (and no remedy is selected)

- **The shared premise is rows 1-2, and it is not lookup cost**: both cells' first suffix join
  probes all 47,989,007 lineitem rows against the part build at **multiplicity exactly 1.000**
  (referential integrity — no reduction, no fanout) and materializes the full 48 M-row,
  9-10-column output (5.0 / 6.4 GB of Arrow) which the immediately following `suffix_filter`
  then cuts (q09: to 2,610,723 rows, 5.4% survival). Payload take is the dominant sub-stage in
  both, with lookup second. The premise is pre-filter materialization volume; the mechanism
  family is the successor's to choose.
- **The build-size lookup hypothesis is falsified at the measured volumes**: q09's 12 M-key
  orders build costs only 117.6 ms of lookup for 2.6 M probes (suffix 3). Large builds are not
  intrinsically expensive to probe here; volume upstream of the filter is what costs.
- **Row 3 is honest opacity**: the partsupp composite key takes the generic path, which this
  first instrument declines by contract rather than fabricating a split. Its 1.6 s residual is
  a named gap for a future generic-path instrument, not a premise.
- The 508-era numbers reconcile: the prior lumped suffix figures (4,028.7 / 1,781.7 / 272.0 ms)
  match these parents within run noise.

Per the ADR: **no remedy is selected, sketched, or implemented in this cycle.** The successor
follows this split, with rows 1-2 as the board-level shared premise.
