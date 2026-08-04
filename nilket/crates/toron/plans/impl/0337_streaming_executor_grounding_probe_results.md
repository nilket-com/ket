# ADR 337 Results - streaming executor grounding probe

## Outcome

ADR 337 completed the streaming-boundary probe. No streaming implementation was added, and no reachability cell moved.
The probe names the first bounded streaming cut and separates it from the broader substrate work.

The initial "collapsing pipeline" hypothesis split into two families:

- **Direct collapsing segment:** q14 and q19 fail at a single join whose output flows directly through a filter into a
  global aggregate. A bounded join-output -> filter -> aggregate stream is expected to move these two cells.
- **Join/exchange pipeline before collapse:** q07, q17, and q21 fail below additional joins and/or exchanges before
  reaching their collapsing aggregate. They need a broader streaming substrate that can pass batches through
  join-to-join and exchange boundaries, not only a join-output-to-aggregate adapter.

So ADR 338 should be the bounded direct collapsing segment if the next cut optimizes for smallest proven movement
(expected coverage: q14, q19). If the next cut optimizes for highest coverage, the evidence says to name it honestly
as broader join/exchange streaming substrate work, not as the narrow segment.

## Probe Method

Temporary `TORON_ADR337_TRACE` instrumentation was added to:

- print compact bound-plan join paths from `toron-tpch` after bind;
- print compact join output guard rows at the ADR 335 `materialize_join_selection` site;
- print compact spill-entry retained footprint rows at the ADR 336 guard site.

The release binary was run under a 4G cgroup at `sf=1`:

```sh
scripts/cargo-cgroup build -p toron-tpch --release
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=900s \
  /bin/sh -lc 'for q in q07 q14 q17 q19 q21 q12 q18 q04 q20; do
    TORON_ADR337_TRACE=1 TORON_ADR337_QUERY="$q" \
      target/release/toron-tpch run --query "$q" --mode strict --sf 1 || true
  done'
```

Instrumentation was removed before seal.

## Output-Cell Peak Split

These are the failing ADR 335 output guard rows. "Retained inputs" are the exact immediate left/right batches held by
the failing join node; they are not base-table estimates.

| query | failing join | route | retained inputs | output estimate | downstream to collapse | ADR 338 expectation |
| --- | --- | --- | ---: | ---: | --- | --- |
| q07 | `JoinReplayId(0)` inner | in-memory | 807,554,519 | 1,975,900,039 | multiple joins + exchanges before grouped aggregate | expected-partial |
| q14 | `JoinReplayId(0)` inner | spill | 828,854,219 | 1,651,834,429 | filter -> global aggregate | expected-pass |
| q17 | `JoinReplayId(0)` inner | spill | 828,854,219 | 1,651,834,429 | exchange + left-outer recombine before global aggregate | expected-partial |
| q19 | `JoinReplayId(0)` inner | spill | 828,854,219 | 1,651,834,429 | filter -> global aggregate | expected-pass |
| q21 | `JoinReplayId(0)` inner | in-memory | 807,554,519 | 1,975,900,039 | multiple joins + semi/anti joins + exchanges before grouped aggregate | expected-partial |

The decisive measurement: all five output cells have retained immediate inputs below 4G, but only q14/q19 have a
direct collapsing consumer path. q07/q17/q21 are output-dominated at the failing node, yet fixing them requires
streaming across additional join/exchange boundaries.

## Plan-Shape Map

Important path summaries, root to failing node:

- q07: `Sort -> Exchange(1) -> Project -> Aggregate(grouped) -> Exchange(4) -> Project -> Filter -> Join(4) ->
  Exchange(4) -> Join(3) -> Exchange(4) -> Join(2) -> Exchange(4) -> Join(1) -> Exchange(4) -> Filter -> Join(0)`.
  The failing join is the first wide supplier-lineitem join, not the aggregate's direct child.
- q14: `Project -> Aggregate(global) -> Filter -> Join(0)`. Direct collapsing segment.
- q17: `Project -> Aggregate(global) -> Filter -> Join(1 LeftOuter) -> Exchange(4) -> Filter -> Join(0)`. The first
  failure is inside the correlated-scalar branch before the left-outer recombine.
- q19: `Project -> Aggregate(global) -> Filter -> Join(0)`. Direct collapsing segment.
- q21: `Sort -> Exchange(1) -> Project -> Aggregate(grouped) -> Exchange(4) -> Join(4 LeftAnti) -> Exchange(4) ->
  Join(3 LeftSemi) -> Exchange(4) -> Filter -> Join(2) -> Exchange(4) -> Filter -> Join(1) -> Exchange(4) ->
  Filter -> Join(0)`. The failing join is early in the join pipeline, below the semi/anti logic.

Exchange is therefore **not off-path** in the single-node reach plans. It appears in q07, q17, q21, q12, q18, q04,
and q20. q14/q19 are the clean no-exchange direct segment.

## Retained-Input Controls

The input/spill controls confirm the direct output cut should not claim q12/q18/q04/q20:

| query | failing producer | measured retained/input row | result |
| --- | --- | ---: | --- |
| q12 | ADR 336 spill-entry guard | 1,136,180,270 | retained-input wall |
| q18 | ADR 336 spill-entry guard at `JoinReplayId(1)` | 1,423,571,737 | retained-input wall |
| q04 | existing spill-writer resident check | 839,694,581 retained at spill entry, then 121,631,060 writer resident | writer-state wall |
| q20 | existing spill-writer resident check | 143,303,648 retained at spill entry, then 72,131,060 writer resident | writer-state wall |

q18 also emitted a prior non-failing output row at `JoinReplayId(0)`:
193,606,162 retained inputs and 484,875,000 output estimate. Its wall is the later retained-input spill entry.

## Named First Cut

**ADR 338 target:** direct collapsing aggregate stream for one-join output cells:

`Join output batches -> Filter(EvaluateInToron) -> Aggregate(global)`

Expected coverage: q14 and q19 should move from `memory_budget_exhausted@execute` to `pass@execute` if the stream
path avoids materializing the 1.65 GB join output while retaining only the measured 828.9 MB inputs plus bounded
batch scratch and scalar aggregate state.

Reason this is first:

- It is the only measured segment that is both output-dominated and directly collapsing.
- It avoids join-to-join, exchange, final sink, and retained-input streaming.
- It gives ADR 338 a bounded implementation surface and a clear expected-vs-actual coverage check.

## Bounded vs. Substrate Verdict

One narrow streaming segment is useful, but it does **not** move the whole sf=1 memory wall.

- q14/q19: bounded direct segment, expected-pass under ADR 338.
- q07/q17/q21: require broader streaming through join-to-join and/or exchange boundaries before the aggregate can
  collapse the stream. These are substrate work, not the direct segment.
- q12/q18: retained input/spill-entry walls; output streaming alone is not the first fit cut.
- q04/q20: spill-writer resident-state walls.

This preserves the no-overclaim gate: ADR 337 predicts coverage for ADR 338; it does not make any cell pass.
