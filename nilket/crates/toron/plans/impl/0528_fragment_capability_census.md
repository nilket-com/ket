# impl 528 - fragment-phase capability census plan

Attribution only: give the gauge a fragment-census rail that binds q01-q22 under the
distributed posture, calls `fragment_plan`, and classifies every outcome from typed receipts.
No workers, no execution, no fragmenter/planner/executor behavior change.

## Immutable inputs

- tree: post-527 main (`d384dd10` lineage), all activated knobs default on;
- dataset coordinate: strict sf=8 persisted Parquet metadata (bind-time facts only; no data is
  executed);
- the gauge funnel: `execute_query_with_metadata_timed` LOAD → PLAN → BIND
  (`toron-tpch/src/lib.rs:4266-4350`); the census forks after BIND and never executes;
- the posture delta: `distributed_planner_config` flips exactly one field,
  `single_partition_execution = false` (`coordinator.rs:1365-1369`); the census bind mirrors
  that and nothing else on top of `gauge_planner_config()` (`lib.rs:4598`);
- the fragmenter entry: `fragment_plan(plan, FragmentPlanConfig::new(query_id, task_budget))`
  (`distributed/fragment.rs:197`), budget via `TaskBudget::from_query_budget` exactly as
  `coordinator.rs:1222-1225` derives it, from the same explicit 2 GiB memory / 2 GiB spill
  envelope the board's censuses pin;
- the reserved phase: `Phase::Fragment` (`toron-tpch/src/telemetry.rs:7-18`), never yet
  constructed; architecture guard pins the string at `toron-tpch/tests/architecture.rs:99`;
- the reject taxonomy to classify: `FragmentError` variants (`fragment.rs:73-106`).

## Step 1 - census rail and typed classification

1. Add a `fragment-census` command to `toron-tpch/src/main.rs` that runs q01-q22 (or an
   explicit `--query` subset) through a new library entry beside the funnel: LOAD and PLAN
   unchanged, BIND with the distributed posture config, then `fragment_plan`. Stop there.
2. Add a typed `FragmentCensusOutcome` receipt: query, posture fingerprint, and either
   `Fragmented { stage_count, stage_task_counts, max_stage_width, single_task_producer_stages }`
   or `FragmentRejected { variant, message }` or `BindRejected { reason }`. Classification into
   `fragments_parallel` / `fragments_sequential` / `fragment_rejected` / `bind_rejected` is
   derived from the receipt, never hand-assigned.
3. Emit JSONL rows with `Phase::Fragment`; extend `reason_for_error`
   (`telemetry.rs:90-180`) with one arm per `FragmentError` variant via an exhaustive match, so
   no fragment failure lands in `unrecognized`.
4. Tests: the ADR 490 posture pin gains a census sibling (default rails keep
   `single_partition_execution = true`; census config differs in exactly that field); receipt
   serde stability; classification totality over all `FragmentError` variants; stage/width
   extraction on a known fragmenting shape and a known rejecting shape; default-rail
   invariance (no `Phase::Fragment` row from reachability/correctness/perf).

## Step 2 - seal and gates

Commit `impl: adr 528 fragment census rail`, run the standard full battery with direct exit
statuses, push, and build one release binary from that identity. Observability seal; no
engine behavior changes.

## Step 3 - census run

From the sealed binary, run the census over q01-q22 at the sf=8 persisted-metadata coordinate
with the explicit 2 GiB memory / 2 GiB spill envelope. One run per cell — fragmentation is
deterministic plan shape; no cohorts, no warmup, no timing collection.

## Step 4 - predeclared verdict and receipt

Append the full matrix here: per-cell classification, stage/task/width facts for fragmenting
cells, and the reject histogram by `FragmentError` variant. Then apply the predeclared gate:

- `fragments_parallel` empty → close ADR 528 red as a substrate-comparison precondition; the
  histogram stands as the prioritization evidence for fragmenter capability work.
- `fragments_parallel` non-empty → name the exact eligible set; a follow-up ADR must decide
  the three named comparison blockers (per-task budget multiplication, single-task producers,
  TCP-only transport) before any timing arm exists. Nothing in Step 3 authorizes a sweep.

## Repository gates

Formatting in both crates; all three `--all-targets` checks; Toron library tests; full
`architecture_test`; complete Toron `--no-fail-fast`; full `toron-tpch`; `git diff --check`.
Direct exit statuses and full logs preserved; the battery repeats after any code change made
in response.

## Measured outcome — fragments_parallel is non-empty (20 of 22 cells)

### Seal

The census rail sealed and pushed as `impl: adr 528 fragment census rail` (`e8be3fb1`). All
gates returned direct exit 0: both format checks; the three `--all-targets` checks (Toron,
base TPC-H, Polars-reference); Toron library tests (1,461 pass); full `architecture_test`
(2,095 pass); the complete Toron `--no-fail-fast` suite (46 green suites); the full
`toron-tpch` suite including the five new ADR 528 tests and the `adr528` posture/leak guard;
`git diff --check`. The census ran from the release binary built at `e8be3fb1` (rows stamp
`build_commit = e8be3fb1532b`) at strict sf=8 persisted Parquet with the explicit 2 GiB
memory / 2 GiB spill envelope. Artifact root:
`target/tpch/adr528_fragment_census_e8be3fb1/fragment_census.jsonl`.

### Exact capability matrix (sf=8, distributed posture)

| cell | classification | stages | per-stage widths | max width | single-task producers |
|---|---|---:|---|---:|---:|
| q01 | fragments_parallel | 3 | 1,4,1 | 4 | 1 |
| q02 | fragments_parallel | 4 | 1,1,4,1 | 4 | 2 |
| q03 | fragments_parallel | 4 | 1,1,4,1 | 4 | 2 |
| q04 | fragments_parallel | 5 | 1,1,4,4,1 | 4 | 2 |
| q05 | fragments_parallel | 5 | 1,1,4,4,1 | 4 | 2 |
| q06 | fragments_sequential | 1 | 1 | 1 | 0 |
| q07 | fragments_parallel | 5 | 1,1,4,4,1 | 4 | 2 |
| q08 | fragments_parallel | 5 | 1,1,4,4,1 | 4 | 2 |
| q09 | fragments_parallel | 5 | 1,1,4,4,1 | 4 | 2 |
| q10 | fragments_parallel | 5 | 1,1,4,4,1 | 4 | 2 |
| q11 | fragments_parallel | 4 | 1,1,4,1 | 4 | 2 |
| q12 | fragments_parallel | 5 | 1,1,4,4,1 | 4 | 2 |
| q13 | fragments_parallel | 5 | 1,1,4,4,1 | 4 | 2 |
| q14 | fragments_parallel | 3 | 1,1,4 | 4 | 2 |
| q15 | fragment_rejected (broadcast_probe_not_star_linear) | — | — | — | — |
| q16 | fragments_parallel | 6 | 1,1,4,4,4,1 | 4 | 2 |
| q17 | fragments_parallel | 3 | 1,1,4 | 4 | 2 |
| q18 | fragments_parallel | 4 | 1,1,4,1 | 4 | 2 |
| q19 | fragments_parallel | 3 | 1,1,4 | 4 | 2 |
| q20 | fragments_parallel | 4 | 1,1,4,1 | 4 | 2 |
| q21 | fragments_parallel | 5 | 1,1,4,4,1 | 4 | 2 |
| q22 | fragments_parallel | 5 | 1,1,4,4,1 | 4 | 2 |

Histogram: `fragments_parallel` 20; `fragments_sequential` 1 (q06 — a zero-exchange scalar
aggregate is a single-task stage by construction); `fragment_rejected` 1 (q15 —
`BroadcastProbeNotStarLinear`); `bind_rejected` 0.

Two sharpening facts. First, the census answer is scale-dependent in exactly the way that
justifies running it at the board coordinate: at a sf=0.01 in-memory smoke, q14 and q19 also
rejected with `BroadcastProbeNotStarLinear`; at sf=8 their size facts steer the binder away
from the offending broadcast shape and both fragment. Second, the feared
`JoinInputNotExchanged` wall never fired — under the distributed posture the binder interposes
exchanges above join inputs for every multi-join chain on the board.

### Verdict

**The predeclared gate does not close red: `fragments_parallel` = {q01-q05, q07-q14,
q16-q22} \ {q15} — 20 cells.** The distributed substrate can *plan* query-wide intra-query
parallelism for nearly the whole board. Per contract, no timing arm, worker execution, or
substrate comparison ran, and this census proves plan admission only — not executability
(Decimal128 exchange partition keys and worker budget semantics remain untested walls).

The admitted shape is uniform and narrow: maximum stage width 4 everywhere (the planner's
default aggregate/join partition count), with every scan-side producer pinned to one task.
The parallelism ceiling the current fragmenter admits is therefore consumer-side width 4
with sequential scans — the scan stages that dominate sf=8 cost run single-task even when
fragmented. A follow-up ADR must decide the three named comparison blockers (per-task budget
multiplication, single-task producers, TCP-only transport) before any timing arm exists;
the width facts here say producer parallelism is not just a fairness blocker but the
first-order one.
