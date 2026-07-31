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
