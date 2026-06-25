# ADR 339 Results - streaming substrate sizing probe (q07/q21)

## Outcome

ADR 339 completed the sizing probe. No streaming implementation was added, and no reachability cell moved.

The decisive sf=1 finding is that both q07 and q21 stop at the same first executable boundary:

`JoinReplayId(0)` = `Inner` / `HashPartitioned`, physical build side = left small supplier partition, probe side =
right lineitem partition. The large 6M-row fact stays on the probe side at the first wall, and the wall is the
1.98 GB materialized join output, not a build-side retained-input wall.

So ADR 340 should be the first bounded substrate cut:

`probe-side stream through Join(0) output -> downstream exchange/join pipeline`

It should start at q07/q21 `Join(0)` and prove the first streaming boundary before attempting the later joins. q17 is
a non-driving control: its first wall puts the 6M-row lineitem side on the build side and uses the spilled route, so
it needs a different follow-up.

## Probe Method

Temporary `TORON_ADR339_TRACE` instrumentation was added to `crates/toron/src/exec.rs` and removed before seal:

- `ExecutionPlan::Join` emitted child plan kind, physical build/probe side, row counts, and retained bytes.
- `execute_hash_join` / `probe_spilled_hash_join` emitted the ADR 335 output estimate before
  `materialize_join_selection`.
- `bound_exchange` emitted the exchange input footprint and confirmed the current executor materializes exchange
  input before partitioning.

The release binary was built and run under a 4G cgroup:

```sh
scripts/cargo-cgroup build -p toron-tpch --release
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=900s \
  /bin/sh -lc 'TORON_ADR339_TRACE=1 TORON_ADR339_QUERY=q07 target/release/toron-tpch run --query q07 --mode strict --sf 1 || true'
```

The same command shape was run for q21 and q17.

## Gate 1 - Build/Probe Map

At sf=1, q07 and q21 both hit the ADR 335 output guard at `JoinReplayId(0)`. Later joins are not executable at
sf=1 until this first boundary streams, so rows beyond Join(0) are deliberately not filled with guessed sf=1 bytes.

| query | sf | stable_join_id | join_kind | strategy | build_side | probe_side | build_retained_bytes | probe_retained_bytes | output_rows | output_estimate_bytes | route | gate_verdict |
| --- | ---: | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| q07 | 1 | 0 | Inner | HashPartitioned | left (`Exchange` supplier) | right (`Exchange` lineitem) | 1,688,602 | 805,872,637 | 6,001,215 | 1,975,900,039 | in-memory | probe-side first boundary |
| q21 | 1 | 0 | Inner | HashPartitioned | left (`Exchange` supplier) | right (`Exchange` lineitem) | 1,688,602 | 805,872,637 | 6,001,215 | 1,975,900,039 | in-memory | probe-side first boundary |

Both emitted the expected row:

| query | result | requested | available | memory peak |
| --- | --- | ---: | ---: | ---: |
| q07 | `fail(memory_budget_exhausted@execute)` | 1,975,900,039 | 1,073,741,824 | 2.9 GB |
| q21 | `fail(memory_budget_exhausted@execute)` | 1,975,900,039 | 1,073,741,824 | 2.8 GB |

Small-scale completion traces confirm the downstream pipeline shape without pretending to observe downstream sf=1
bytes:

| query | sf | joins reached | downstream shape after Join(0) |
| --- | ---: | --- | --- |
| q07 | 0.01 | 0, 1, 2, 3, 4 | Join(0) -> Filter -> Exchange -> Join(1) -> Exchange -> Join(2) -> Exchange -> Join(3) -> Exchange -> Join(4) -> grouped aggregate path |
| q21 | 0.01 | 0, 1, 2, 3, 4 | Join(0) -> Filter -> Exchange -> Join(1) -> Filter -> Exchange -> Join(2) -> Filter -> Exchange -> LeftSemi Join(3) -> Exchange -> LeftAnti Join(4) -> grouped aggregate path |

The downstream q21 semi/anti joins are not reached at sf=1. ADR 340 must not re-orient them blindly; they remain
fenced until a later stage reaches them.

## q17 Control

q17 confirms this is not one universal output wall. Its first wall is still `JoinReplayId(0)`, but the large 6M-row
lineitem side is the physical build side and the join takes the spilled route.

| query | sf | stable_join_id | join_kind | strategy | build_side | probe_side | build_retained_bytes | probe_retained_bytes | output_rows | output_estimate_bytes | route | driving? |
| --- | ---: | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| q17 | 1 | 0 | Inner | HashPartitioned | left (`Exchange` lineitem) | right (`Exchange` part) | 805,872,637 | 22,988,386 | 6,001,215 | 1,651,834,429 | spilled | no |

q17 therefore stays a secondary/control branch. It validates that ADR 339's q07/q21 first cut is specifically a
probe-side pipeline cut, not a general q17 fix.

## Gate 2 - Exchange Verdict

Observed exchange rows:

| query | sf | exchange input | partition_count | input_rows | input_bytes | verdict |
| --- | ---: | --- | ---: | ---: | ---: | --- |
| q07 | 1 | supplier `Scan` | 4 | 10,000 | 2,520,320 | currently materializes; locally pipeable by batch partition-forward |
| q07 | 1 | lineitem `Scan` | 4 | 6,001,215 | 1,119,431,072 | currently materializes; locally pipeable by batch partition-forward |
| q21 | 1 | supplier `Scan` | 4 | 10,000 | 2,520,320 | currently materializes; locally pipeable by batch partition-forward |
| q21 | 1 | lineitem `Scan` | 4 | 6,001,215 | 1,119,431,072 | currently materializes; locally pipeable by batch partition-forward |

`bound_exchange` currently calls `execute_bound(input)` and then repartitions the full returned `Vec<RecordBatch>`.
That is a materializing boundary today. It is still locally pipeable in design terms: partitioning is per input batch
and can forward partition batches without needing the full child result first.

This means ADR 340 can start at the join output boundary, but the staged substrate must add exchange streaming before
expecting downstream join-to-join composition to move whole q07/q21.

## Grouped-Fold Feasibility

The q07/q21 grouped aggregates are not reached at sf=1 because Join(0) fails first. Group cardinality was therefore
measured from the existing independent strict oracles at sf=1:

| query | groups | contributing/selected rows | logical key payload | logical aggregate-state payload | verdict |
| --- | ---: | ---: | ---: | ---: | --- |
| q07 | 4 | 5,924 selected rows | 68 bytes | 64 bytes | grouped state is tiny; fold helpers should extend cleanly once the stream reaches it |
| q21 | 411 | 4,141 contributing rows | 7,398 bytes | 3,288 bytes | grouped state is small; not the first wall |

These payloads exclude BTreeMap/container overhead, but the cardinalities are low enough that grouped state is not the
scale wall. The real fit work is getting batches from Join(0) through exchanges and later joins to that grouped fold.

## First Executable Boundary and Staged Plan

**First ADR 340 boundary:** q07/q21 `JoinReplayId(0)` probe-side output streaming.

Why first:

- The large side is already the probe side for both q07 and q21.
- The physical build side is small (1.69 MB retained), so the first join can hold the build and stream probe batches.
- The current failure is the 1.98 GB materialized output, exactly the boundary streaming can remove.
- The q21 semi/anti joins are downstream and not reached; ADR 340 can avoid them initially.

Expected staged movement:

| stage | cut | expected q07/q21 movement | notes |
| --- | --- | --- | --- |
| 340 | Stream Join(0) output from probe batches into the next operator, preserving row order within probe chunks | expected partial | Should remove the first 1.98 GB materialization; next wall likely the immediately following exchange/materialized downstream join. |
| 341 | Stream exchange partition-forward over batches | expected partial-to-next-boundary | Needed because every observed exchange currently materializes its full input. |
| 342+ | Compose join-to-join streaming through later inner joins; then fence and handle q21 LeftSemi/LeftAnti | expected q07 first, q21 after semi/anti stage | Side re-orientation is forbidden for semi/anti unless proven semantics-preserving. |
| later | q17 build-side/spilled branch | separate | q17's large side is build-side at Join(0), so it is not covered by the q07/q21 probe-side cut. |

ADR 339 therefore sizes the substrate as a staged investment, not one broad rewrite and not one quick global fix.

## No-Overclaim

ADR 339 did not move any cell. It produced evidence and a first-cut target:

- q07/q21: first wall = probe-side Join(0) output materialization.
- exchange: currently materializes, locally pipeable but not implemented.
- grouped aggregate: low-cardinality state, not the first wall.
- q17: non-driving, build-side/spilled branch.

Instrumentation was temporary and removed before seal.
