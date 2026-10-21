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

**Semi build placement (added after the first sealed census).** The first census caught the rule
engaging correctly while losing on wall time: the reduced aggregate's fold collapsed from
10,614.6 ms to 1.2 ms, but the inserted semi built on the 48 M-row probe side — 11,796.2 ms and
379,378,656 spilled bytes — because the smaller-build comparator gated on `JoinKind::Inner` and
the semi's local admission is inner-only. The completing cut widens
`hash_join_decision_with_placement` to `Inner | LeftSemi`: a semi's build side is pure physical
placement (the swap is the executor's existing `RightSemi` via
`mirrored_broadcast_physical_kind`, which the broadcast path already uses), the membership side
is the natural build, and ADR 511's shrink-only rule applies unchanged. `LeftAnti` is the same
shape and is deliberately left for a follow-on with a motivating case.

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

## Measured outcome

### Two seals, one honest red between them

Steps 1-2 first sealed as `a1bf26789afb`. Its census engaged exactly one cell — q17, digests
identical on all 22 q-cells and 11 ladder cells — and proved the semantic goal outright: the inner
grouped fold collapsed from 10,614.6 ms (`scan_grouped` over 47,989,007 rows) to 1.2 ms over the
semi output. It also caught a real physical defect the plan had not priced: the inserted LeftSemi
built on the 48 M-row probe side (join node 11,796.2 ms carrying 379,378,656 spilled bytes, with
`join_spill_observations` empty — node-attributed spill, not a committed join route), because both
the smaller-build comparator and local join admission gated on `JoinKind::Inner`
(`declined(not_inner)`). Net wall moved +1.8 s. The completing cut — widening
`hash_join_decision_with_placement` to `Inner | LeftSemi`, whose swap is the executor's existing
`RightSemi` via `mirrored_broadcast_physical_kind`, with ADR 511's shrink-only rule unchanged and
a discriminating unit test that fails under the Inner-only gate — sealed and pushed as
`bf11c6e0b35b`. All evidence below is from the release binary built from that clean identity.

| evidence | artifact |
|---|---|
| first-seal census (the caught red) | `target/tpch/adr512_route_census_{off,on}_a1bf2678/` |
| corrected census, both arms | `target/tpch/adr512_route_census_{off,on}_bf11c6e0/` |
| ladder census, both arms | `target/tpch/adr512_ladder_census_{off,on}_bf11c6e0/` |
| diagnostic pair with node attribution | `target/tpch/adr512_q17_diag_{off,on}_bf11c6e0/` |
| plan-distinct gated 10-pair cohort | `target/tpch/adr512_q17_paired_bf11c6e0/` |

### Census, reach, and correctness

The corrected census: q17 is the only engaged cell (fingerprint `9062c1de21622849` →
`e30b1263f6a8e25e`), single-run execute 11,419 → 4,236 ms, spill **zero in both arms**, digest and
row count identical; 21 q-cells and all 11 ladder cells are plan-invariant with equal digests. The
plan's q02/q20 engagement hypothesis is **falsified**: their bare-identifier correlations ride the
deferred path, but their outer shapes do not admit a source under the conservative derivation
walk. The census is authoritative; their coverage is a named forward candidate, not a claim.

Strict correctness passed in both arms with direct exit 0: q17 at its supported sf=0.02 oracle
coordinate and the q19 sentinel at sf=0.01.

### Structural evidence (the operator-required pair)

Reduced aggregate input: the fold's 9,405.6 ms (`scan_grouped`, control) disappears entirely in
the candidate (generic fold below the 100 ms reporting threshold). Unchanged grouping and
exchange: pinned structurally by the binder tests (exchange count, partition keys, and grouping
above the semi are byte-identical wrappers) and corroborated by identical digests and clean
attribution reconciliation (zero unattributed spill, zero bridge/stage/interleaved mismatches) in
both arms. Candidate node profile: scan 1,296.6 ms, join 2,076.6 ms (join0 plus the swapped semi,
zero spill), exchange 890.6 ms.

### Locked paired cohort

The gate passed as gate-zero over the full-hash dataset; the journal holds one discarded warmup
and 20 measured rows in adjacent pairs — every row passing, full-hash, clean `bf11c6e0b35b`
release, one shared digest, per-arm fingerprints as above, zero spill on every row.

| pair | candidate | control | control - candidate |
|---:|---:|---:|---:|
| 1 | 4,278.369 ms | 11,463.539 ms | 7,185.170 ms |
| 2 | 4,224.342 ms | 11,461.469 ms | 7,237.127 ms |
| 3 | 4,241.414 ms | 11,528.230 ms | 7,286.817 ms |
| 4 | 4,233.966 ms | 11,466.883 ms | 7,232.917 ms |
| 5 | 4,238.979 ms | 11,456.834 ms | 7,217.855 ms |
| 6 | 4,239.913 ms | 11,525.464 ms | 7,285.551 ms |
| 7 | 4,275.752 ms | 11,472.038 ms | 7,196.285 ms |
| 8 | 4,243.002 ms | 11,610.308 ms | 7,367.306 ms |
| 9 | 4,258.915 ms | 11,439.391 ms | 7,180.476 ms |
| 10 | 4,246.511 ms | 11,476.383 ms | 7,229.872 ms |

```text
Hodges-Lehmann point        = 7,233.646 ms
two-sided 95% interval      = [7,205.174, 7,286.184] ms
fixed materiality floor     = 3,554 ms
interval excludes zero      = true
point reaches floor         = true (2.04x; the interval lower bound alone is 2.03x)
activate                    = true
```

### Activation

The verdict is `activate` under ADR 472's rule verbatim: the interval excludes zero at twice the
fixed floor, the only engaged cell improves 2.7x with held digests, and no other cell changed
plans. This receipt flips `PlannerConfig::scalar_aggregate_input_reduction` to compiled default
**true**, retargets the gauge knob-contract default arm, and re-runs the full seal gate plus
default-arm strict correctness smokes for q17 and the q19 sentinel from the flipped tree. The
implementation seals `a1bf26789afb` and `bf11c6e0b35b` remain the measurement identities; this
receipt commit is independently revertible.

q17's cumulative arc across ADRs 511-512: 35,541 ms (ADR 508 baseline) → ~11.6 s (511, build-side
swap) → ~4.24 s (512, reachable-key reduction) against DuckDB's one-thread 765 ms — the cell's
engine gap compressed from 46.5x to roughly 5.5x, with the remaining envelope now scan + join0 +
exchange rather than any single dominant defect.
