# impl 512 - reachable-key scalar aggregate reduction plan

One declining logical rewrite, shared by both decorrelation paths, measured directly on q17. No
new estimators, no join-order change, no change to either decorrelation shape when the rule
declines.

## Immutable inputs

- control tree: post-ADR-511 main (`9008070a` lineage) with `filtered_build_side_swap` default on;
- fixed materiality floor: 3,554 ms (inherited from the ADR 508 chain, not recomputed);
- exact dataset: strict sf=8 persisted Parquet with `full_hash`;
- task memory: 2,147,483,648 B, explicit; spill reserve: 2,147,483,648 B, explicit;
- grounded anchors: recombine constructions at sql_planner.rs:6461 (qualified) and
  binder.rs:2866-2913 (deferred), both `null_safe_keys: false`; null-safe constructors that must
  decline at sql_planner.rs:3566 and 7604; `JoinKind::LeftSemi` at ir.rs:1050 with existing
  frontend lowering and exec machinery; post-511 q17 diagnostic: inner `scan_grouped`
  9,533.8 ms of an 11,641.6 ms execute.

## Step 1 - the rewrite helper and config

Add `PlannerConfig::scalar_aggregate_input_reduction: bool`, compiled default **false** at the
seal. The field comment states the invariant: a semi build that is a superset of the recombine's
outer key domain preserves every aggregate result, because only never-matched groups are
eliminated.

Implement one helper over logical plans:

```text
reduce_scalar_aggregate_input(outer, inner, correlation_pairs) -> Option<inner'>
```

It returns `Some` only when every precondition holds:

1. the recombine keys are plain equality (`null_safe_keys` will be false at the join site — both
   call sites assert this before invoking the helper, and the helper is not called from any
   null-safe constructor);
2. every outer correlation key traces through equi-join pairs in the outer logical plan to one
   shared single base relation carrying only local filters along the path;
3. the traced relation and its filters re-plan as a fresh scan + filter + key projection.

The reduced inner is `LeftSemi(inner, source_projection)` on the correlation keys, placed so the
existing `Exchange` and `Aggregate` wrap it unchanged. On any failure the helper returns `None`
and the caller proceeds byte-identically.

Wire the helper into both engagement points **in the binder** (the frontend has no
`PlannerConfig`, and a binder-owned recognizer covers every frontend):

- deferred path: `bind_deferred_scalar_subquery`, before constructing the `Aggregate`;
- qualified path: the `LogicalPlan::Join` bind arm recognizes the frontend-constructed recombine
  shape (through the auto-join `Exchange` wrapper) and substitutes the reduced right side on the
  direct bind path only — an actuated graph re-bind forgoes the reduction, and the rewrite never
  re-enters the arm, so no shape oscillation with the root-swap passes is possible.

Two path asymmetries are load-bearing and pinned rather than papered over: the deferred frontier
rejects correlated COUNT (its coalesce does not compose with a wrapper), so COUNT coverage rides
the qualified path's empty-group coalesce — which composes with the reduction because eliminated
groups are exactly the unmatched keys the coalesce already handles; and at recognizer time the
qualified path has not yet pushed WHERE conjuncts below the recombine, so a qualified engagement
needs its filter inside the outer subtree (a filtered derived table is the natural spelling).

Gauge wiring mirrors ADR 511 exactly: `TORON_TPCH_SCALAR_AGG_INPUT_REDUCTION`, "0"/"1" explicit,
absence = compiled default, loud error otherwise; rows carry
`scalar_aggregate_input_reduction` and `scalar_aggregate_input_reduction_provenance`.

## Step 2 - tests

Equivalence battery (both paths, knob on vs off, identical digests):

- correlation keys present in the outer; keys absent from the outer; null inner keys; null outer
  keys (outer row survives with a NULL scalar); duplicate keys on both sides;
- COUNT: matched key whose group is emptied by an inner residual still yields its recombine row
  with the aggregate's empty-group value; unmatched keys yield NULL through the LeftOuter — both
  identical across arms;
- AVG and MIN with wrapper arithmetic over the scalar (`0.2 * value` shape);
- a discriminating under-approximation test: a hand-built source missing one matched key changes
  the result — proving the superset requirement is load-bearing and pinning why the derivation
  walk must be conservative.

Decline pins:

- null-safe recombine shape never engages (constructed via the null-safe join path);
- outer key with no equi-chain to a single base relation → plan byte-identical, knob on;
- composite keys tracing to different relations → decline;
- knob off → byte-identical plans on every fixture.

Placement and structure pins:

- the rewritten plan keeps exchange count, partition keys, and aggregate grouping unchanged above
  the inserted LeftSemi;
- q17-shape bind receipt: knob on inserts the semi with the filtered part key projection as its
  build; knob off reproduces today's plan;
- a distributed `fragment_plan` bind proof over an engaged shape.

Harness: knob-contract test beside the ADR 511 one.

Architecture: reconcile any guard scanning the recombine constructions or PlannerConfig line
bounds; name every retarget in the receipt.

## Step 3 - census diff and reached-shape correctness

From the sealed binary, both arms at the exact coordinate: q01-q22 route census and m00-m10
ladder census. Enumerate every fingerprint change with per-arm routes, admissions, spill
observations, and digests. No silent caps.

Engagement hypothesis (the census is authoritative, not this list): q17 via the deferred path;
q02 and q20 correlate with bare identifiers in the official text, so they are expected to ride
the deferred path too — whether their outer shapes admit a source is exactly what the census
decides.

Every changed cell runs its supported strict correctness rail in both arms with explicit budgets
(q17 at sf=0.02; others at their supported coordinates). q19 remains a named sentinel from
ADR 511 and is correctness-run in both arms regardless.

## Step 4 - implementation seal

Seal steps 1-3 as one cohesive commit, run all gates, push, never amend. Build the release binary
once from the pushed identity via `scripts/cargo-cgroup build --release -p toron-tpch`. Reject any
measured row whose build is dirty or differs from the seal.

## Step 5 - pre-cohort gates

Plan-distinct gate at the exact coordinate with explicit budgets and the paired switch
`TORON_TPCH_SCALAR_AGG_INPUT_REDUCTION` (`--paired-on 1 --paired-off 0`). One diagnostic on/off
pair with node attribution recording: inner `scan_grouped` invocation time, scan/join/aggregate
node exclusives, semi-join cost, exchange volume, and whole-query execute. The diagnostic
explains transfer; its times stay out of the primary estimator.

## Step 6 - primary paired cohort

```bash
TORON_TPCH_MEMORY_PER_TASK=2147483648 \
TORON_TPCH_SPILL_BYTES_RESERVED=2147483648 \
target/release/toron-tpch perf \
  --query q17 \
  --mode strict \
  --sf 8 \
  --data-source parquet \
  --data-dir target/tpch/data \
  --engines toron \
  --runs 20 \
  --warmup 1 \
  --paired-ablation \
  --paired-env TORON_TPCH_SCALAR_AGG_INPUT_REDUCTION \
  --paired-on 1 \
  --paired-off 0 \
  --paired-gate plan-distinct \
  --paired-materiality-floor-ms 3554 \
  --output-dir target/tpch/adr512_q17_paired_<build>
```

The plan-distinct rail counts `--runs` as child rows (ADR 511's receipt pinned this), so 20 rows
= 10 adjacent pairs. ADR 447 ordering, durable per-row fsync, Hodges-Lehmann point and two-sided
95% Wilcoxon signed-rank interval from the persisted journal rows.

Verdict:

| interval | point estimate | result |
|---|---|---|
| improving, excludes zero | at least 3,554 ms | activate |
| improving, excludes zero | below 3,554 ms | detectable, immaterial; close |
| includes zero | at least 3,554 ms | material, unresolved; close |
| includes zero | below 3,554 ms | close |

No post-hoc sample extension, alternate estimator, recomputed floor, or threshold negotiation.

## Step 7 - receipt and activation

Separate receipt commit citing the immutable identity and artifacts: census diff, changed-cell
correctness, gate record, diagnostic pair, all raw rows, paired statistics, verdicts. Iff the
verdict is `activate` and no engaged cell prices a material regression (ADR 472's rule), the
receipt flips the compiled default to true, retargets the knob-contract test, and re-runs the full
seal gate plus default-arm strict correctness smokes for every engaged cell and the q19 sentinel.
Otherwise production keeps the unreduced aggregate and the receipt contains no mechanism pivot.

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

Preserve every gate's direct exit status; never pipe a gate through `tail` or a zero-match
`grep`. Repeat the applicable seal gate after any code change made in response. Finish all gates
before starting measurement.
