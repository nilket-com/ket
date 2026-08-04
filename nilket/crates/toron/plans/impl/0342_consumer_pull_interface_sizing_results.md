# ADR 342 Results - consumer-pull interface sizing

## Outcome

ADR 342 completed the sizing/design probe. No engine cut was committed and no reachability cell moved.

The decisive finding is that the first downstream consumer is **not** a probe-side consumer. In both q07 and q21, the
large streamed intermediate coming from `Exchange <- Filter <- Join(0)` is the physical **left/build** input at
`Join(1)`, and it remains the build-side input through the downstream join chain.

That overturns the narrow "Join(1) pulls Exchange partitions as probe" route. A bounded Exchange-to-Join interface is
still possible without converting all of `execute_bound` into `execute_stream`, but for q07/q21 it is a **build-side**
interface: it can remove the returned Exchange `Vec`, then immediately has to retain/hash the streamed rows as the
join build. Therefore consumer-pull alone is not a q21-green cut.

## Probe Method

Temporary test-only probe code was added to `crates/toron-tpch/src/lib.rs`, run once, and removed before seal. The probe
bound strict q07 and strict q21 at `sf=1`, walked the resulting `ExecutionPlan`, and printed each `JoinReplayId` with:

- physical left child (the executor's hash-build side),
- physical right child (the executor's probe side),
- join kind, strategy, key ordinals, and child schemas.

Command:

```sh
scripts/cargo-cgroup test -p toron-tpch structural_join_map_q07_q21_sf1 -- --nocapture
```

Result: 1 probe test passed. The output below is **structural** evidence from the bound sf=1 plan. It is not sf=1
runtime retained-byte evidence past the current wall.

## Structural Join(1)+ Map

Executor grounding: `execute_hash_join` concatenates both child inputs, records left rows as build and right rows as
probe, builds the hash table from the left key arrays, then probes the right key arrays (`exec.rs:3398-3526`). So in
the bound plan below, physical left = build and physical right = probe.

### q07

| join | kind | strategy | build side | probe side | evidence | verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `Inner` | `HashPartitioned` | `Exchange(Filter(Join(0)))`, fields supplier + lineitem | `Exchange(Scan orders)` | structural | large intermediate is build-side, not pullable as probe |
| 2 | `Inner` | `HashPartitioned` | `Exchange(Join(1))`, fields supplier + lineitem + orders | `Exchange(Scan customer)` | structural | large intermediate remains build-side |
| 3 | `Inner` | `HashPartitioned` | `Exchange(Join(2))`, fields supplier + lineitem + orders + customer | `Exchange(Scan nation)` | structural | large intermediate remains build-side |
| 4 | `Inner` | `HashPartitioned` | `Exchange(Join(3))`, fields supplier + lineitem + orders + customer + nation | `Exchange(Scan nation)` | structural | large intermediate remains build-side |

ADR 341 already measured q07's current sf=1 wall as a later `join output materialization` at 1,177,978,914 requested
bytes. ADR 342 does not claim a new q07 wall; it sizes the downstream structure and shows that the remaining q07 chain
is not a simple probe-side pipeline.

### q21

| join | kind | strategy | build side | probe side | evidence | verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `Inner` | `HashPartitioned` | `Exchange(Filter(Join(0)))`, fields supplier + lineitem | `Exchange(Scan orders)` | structural | q21's current 1.07 GB Exchange-output wall feeds a build side |
| 2 | `Inner` | `HashPartitioned` | `Exchange(Filter(Join(1)))`, fields supplier + lineitem + orders | `Exchange(Scan nation)` | structural | large intermediate remains build-side |
| 3 | `LeftSemi` | `HashPartitioned` | `Exchange(Filter(Join(2)))`, fields supplier + lineitem + orders + nation | `Exchange(Scan lineitem l2)` | structural | semi join is build-side and semantics-fenced |
| 4 | `LeftAnti` | `HashPartitioned` | `Exchange(Join(3))`, fields supplier + lineitem + orders + nation | `Exchange(Filter(Scan lineitem l3))` | structural | anti join is build-side and semantics-fenced |

ADR 341 measured q21's current sf=1 wall as `streamed exchange output accumulator`, requested 1,074,142,886 bytes
against the 1,073,741,824 effective cap. ADR 342 predicts that removing the returned Exchange `Vec` exposes a
build-side wall at `Join(1)`, not q21 green, unless a separate cut changes the physical side choice or build-side
memory model.

## Bounded-vs-General Verdict

**Verdict: bounded, but not the originally imagined probe-pull cut.**

The evidence does not force a global `execute_stream` rewrite. A bounded interface can be scoped to the
`Join(1) <- Exchange <- Filter <- Join(0)` boundary:

- `Exchange` can produce partitioned batches incrementally, as ADR 341 already proved for the input side.
- A special-case `Join(1)` path could consume those batches without a returned Exchange `Vec`.
- Non-matching plans can keep the current `execute_bound(...) -> Vec<RecordBatch>` interface.

But for q07/q21, that bounded consumer is a **build-side** consumer. The join cannot simply pull and drop the Exchange
rows as probe input; it must retain/index them before probing the right side. So the next implementation should not
promise q21 green from consumer-pull. It should either:

- size/implement a build-side streaming or budgeted build accumulator for the `Join(1)` build input, or
- prove a safe physical reorientation for the inner joins so the large intermediate stays on the probe side.

The q21 `LeftSemi`/`LeftAnti` joins remain fenced. No reorientation is implied there.

## Minimal Interface Design

The smallest bounded surface is a boundary-family helper, not a source-wide executor rewrite:

1. A producer that streams `Exchange <- Filter <- Join(0)` partition batches. ADR 341 already has the useful lower
   primitive shape: the Join(0) filter path is visitor-based, and Exchange partitioning can happen batch-by-batch.
2. A `Join(1)`-specific consumer that accepts those partition batches as its left/build input. This avoids the returned
   Exchange `Vec`, but still must account for the retained build rows/hash state.
3. Existing `execute_bound` remains the fallback for all non-matching shapes.

The interface must name and budget the build-side retention. If it crosses the cap, the producer should be something
like `streamed join build accumulator` (exact name left to the implementing ADR), not a silent OOM and not a misleading
Exchange-output wall.

## Expected Coverage

| query | expected result of a bounded Exchange-to-Join(1) build-side cut | predicted producer if still red |
| --- | --- | --- |
| q21 | green is **not** expected from consumer-pull alone; the wall should move off `streamed exchange output accumulator` to a build-side wall at `Join(1)` | budgeted Join(1) build-side retention/hash-build accumulator |
| q07 | no green claim; q07 is already past the Exchange-output wall and remains a downstream join-output/build-side pipeline problem | current later `join output materialization` or a later named build/output wall, depending on the implementing cut |

This is the ADR 342 green-or-next-wall prediction. The implementing ADR should verify expected-vs-actual through ADR
332 diagnostic retention, not by assuming that consumer-pull is a pass.

## No-Overclaim

- `Join(1)+` rows are structural evidence from the sf=1 bound plan, not sf=1 runtime bytes beyond the current wall.
- No source change was kept.
- No reachability or correctness cell moved.
- ADR 342 sizes the next cut: the substrate remains containable as a bounded boundary-family, but the next real wall
  is build-side retention/reorientation, not generic probe-side pull.

## Seal

- Probe:
  `scripts/cargo-cgroup test -p toron-tpch structural_join_map_q07_q21_sf1 -- --nocapture`
  passed with the temporary structural probe in place. The probe code was removed before seal; no source
  instrumentation was kept.
- Formatting:
  `scripts/cargo-cgroup fmt` passed, then
  `scripts/cargo-cgroup fmt -- --check` passed.
- Compile checks:
  `scripts/cargo-cgroup check -p toron --all-targets` and
  `scripts/cargo-cgroup check -p toron-tpch --all-targets` passed.
- Regression tests:
  `scripts/cargo-cgroup test -p toron --lib` passed (`1141 passed`).
  `scripts/cargo-cgroup test -p toron-tpch` passed (package suite: lib, main, architecture, decimal, correctness,
  harness, q14 probe, and doctests).
- Diff hygiene:
  `git diff --check` passed before seal edits, and `git diff --cached --check` passed after staging this results
  document.
