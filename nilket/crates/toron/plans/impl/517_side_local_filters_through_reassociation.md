# impl 517 - side-local filters through reassociation plan

One bounded completion of ADR 401's side-local rule at the cross-to-inner reassociation site,
gated three ways and measured per cell. Phase 0 has authority to close red before mechanism code.

## Immutable inputs

- tree: post-ADR-516 main (`2ad3ee70` lineage), all activated knobs default on;
- fixed materiality floor: 3,554 ms (inherited, not recomputed); per-cell, never summed;
- exact dataset: strict sf=8 persisted Parquet with `full_hash`; budgets 2 GiB / 2 GiB explicit;
- pinned anchors: conjunct classifier + `reassociated_residual.extend(left_only/right_only)`
  (binder/outer_join_demotion.rs:928 area); `runtime.post_filter` → `suffix_filter`
  (exec/join/streaming.rs:12635 area); ADR 401's side-local rule (outer_join_demotion.rs:1229);
  gate primitives `conjunct_column_sides`, `is_deterministic()`, `guard_filter_pushdown_safe`
  (outer_join_demotion.rs:2366 precedent);
- ADR 516 receipts: directly removable ≈ 2.47 s (q08) / 2.82 s (q09) — both below the floor on
  the priced component alone; expected part builds 1,600,000 → 10,801 (q08) / 87,125 (q09);
  expected suffix-1 emissions 47,989,007 → 323,097 (q08) / 2,610,723 (q09).

## Phase 0 - shape twins (no mechanism code)

Run the SQL twins with explicitly prefiltered part branches (derived-table form) at the exact
coordinate, both cells, plus the unmodified originals, interleaved:

1. Verify the twin binds the intended shape: filter below the part suffix build, spine keys /
   intents / exchanges / output schema otherwise unchanged (bind receipts).
2. Price the attainable per-cell wall-time delta between original and twin over 5 interleaved
   pairs each; report medians and raw rows. The twins are the ceiling of what routing can
   deliver.
3. Decision, predeclared: if **neither** cell's twin delta clears 3,554 ms at the median, close
   the ADR red here with the per-component receipt. If at least one clears, proceed.

Phase 0 results land in this document before step 1 code is written.

## Step 1 - the routing completion and control

`PlannerConfig::side_local_filter_routing`, compiled default **false** at the seal; gauge env
`TORON_TPCH_SIDE_LOCAL_FILTER_ROUTING`, loud "0"/"1" contract, row fields with provenance,
knob-contract test beside the prior ones.

At the reassociation site, replace the unconditional
`reassociated_residual.extend(left_only/right_only)` with the gated route: a conjunct passing
(single-branch ownership && `is_deterministic()` && `guard_filter_pushdown_safe`) becomes a
`Filter` on its source branch before `reassociate_lifted_cross_child`; failing conjuncts stay in
the residual exactly as today. The predicate moves once — never cloned — and the knob-off arm is
byte-identical.

## Step 2 - tests

Structural battery (bind receipts):

- left-cross and right-cross reassociation each route their side's predicate below the join;
- the routed predicate appears exactly once (one filter, no residual copy);
- join metadata identical to the knob-off plan except the branch filter: keys, strategy intents,
  exchange keys and counts, output schema;
- declines, plan byte-identical with the knob on: volatile (via the `is_deterministic` fence),
  ambiguous ownership, mixed-side conjuncts, strict-cast and arithmetic shapes that
  `guard_filter_pushdown_safe` rejects.

Semantic battery (execution, both arms identical results, extending the 512/515 fixture family):

- NULL-bearing filter columns (false and unknown both eliminate identically on either side);
- duplicate source rows and duplicate join matches (bag multiplicity preserved);
- always-false predicates (empty branch vs empty result — identical);
- an error-fence fixture: a predicate that would error on rows the join eliminates must remain
  residual through the gate, and results stay identical.

Architecture: reconcile any guards scanning the reassociation site; name retargets in the
receipt.

## Step 3 - census, correctness, and engagement evidence

Both arms, q01-q22 and m00-m10, exact coordinate, sealed binary. Engaged set is empirical.
Required q08/q09 evidence, per cell: the filter binds below the part build; part build rows
1,600,000 → 10,801 / 87,125; suffix-1 emissions 47,989,007 → 323,097 / 2,610,723 (ADR 516's
instrument is the witness); digests identical; every fingerprint-changed cell's strict
correctness rail green in both arms; q19 sentinel.

## Steps 4-7 - seal, gates, cohorts, receipt

Standard protocol: cohesive seal, full gate battery with direct exit statuses, push, release
build; then per-cell plan-distinct cohorts:

```bash
TORON_TPCH_MEMORY_PER_TASK=2147483648 \
TORON_TPCH_SPILL_BYTES_RESERVED=2147483648 \
target/release/toron-tpch perf \
  --query <q08|q09> --mode strict --sf 8 --data-source parquet --data-dir target/tpch/data \
  --engines toron --runs 20 --warmup 1 \
  --paired-ablation \
  --paired-env TORON_TPCH_SIDE_LOCAL_FILTER_ROUTING \
  --paired-on 1 --paired-off 0 \
  --paired-gate plan-distinct \
  --paired-materiality-floor-ms 3554 \
  --output-dir target/tpch/adr517_<cell>_paired_<build>
```

20 child rows = 10 adjacent pairs per cell; Hodges-Lehmann point and two-sided 95% Wilcoxon
interval per cell from the persisted journals; the standard verdict table per cell; no summing,
no post-hoc extension, no floor negotiation. Receipt flips the compiled default iff at least one
cell's verdict is `activate` and no engaged cell prices a material regression (ADR 472's rule),
then re-runs the full seal gate and default-arm smokes for q08, q09, and q19 from the flipped
tree. Both cells' verdicts are reported either way.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.

### Phase 0 result — both twins clear the floor; proceed, with the ceiling caveat

The plan's SQL twins proved grammar-unreachable: the prefiltered branch nests a derived table
inside the queries' existing derived-table wrapper (unsupported construct), and flattening the
wrapper requires GROUP BY expressions (also unsupported — the wrapper exists because of exactly
that restriction). The instrument substituted is a **dataset twin**: the same sealed binary and
original SQL against a full-hash-verified copy of the sf=8 dataset whose `part.parquet` holds
only the qualifying rows (10,801 for q08's p_type; 87,125 for q09's p_name — matching the
predicted counts exactly), all other tables shared byte-identically, manifest re-fingerprinted
with the harness's own fnv1a64 scheme. Twin digests equal the originals on every pair — the
semantic identity is proven, not assumed. No engine code was touched.

| cell | pair deltas (orig − twin) | median | floor | verdict |
|---|---|---:|---:|---|
| q08 | 3,983.1 / 3,948.1 / 3,766.2 / 3,793.7 / 3,750.3 ms | **+3,793.7 ms** | 3,554 | CLEARS |
| q09 | 3,996.7 / 3,919.7 / 3,888.7 / 3,943.4 / 3,931.1 ms | **+3,931.1 ms** | 3,554 | CLEARS |

Stated conservatively before the mechanism: the dataset twin is a slightly **generous ceiling** —
it also shrinks the part scan itself, which filter routing keeps as a full scan plus a (likely
scan-pushed) predicate. With medians only 6.7% / 10.6% above the floor, the cohorts may still
close below it; that outcome remains priced in and honest. Phase 0 authorizes proceeding.

## Measured outcome

### Seal and structural evidence

Steps 1-2 sealed and pushed as `6c445d61`; every gate green with direct exit statuses (fmt,
fmt-check, lib, full architecture_test, `--no-fail-fast` suite, toron-tpch, three `--all-targets`
checks, `git diff --check`), and the ADR 176 binder-root bound moved 6,190 → 6,195 with its
justification sentence — the only guard the cut touches. The structural battery pins both comma
orientations routing exactly one part-scan filter receipt with the knob on and zero with it off
(the defect shape needs the four-table left-deep form: a crossed pair with two bridging equis,
where the plain three-table form is already lifted by the existing push path), plus the
strict-cast fence. The semantic battery (scalar_reduction_test fixture family, extended with a
bridge table, a nullable filter column, and an overflow-bearing key) proves identical results
with NULL-bearing filters, duplicate rows and duplicate join matches, always-false predicates,
and the error fence: the strict-cast predicate binds byte-identical plans on both arms and
executes cleanly, while the routing test binds distinct plans with equal results.

### Census, correctness, and engagement evidence

Both arms from the sealed release binary at the exact coordinate. The census engaged exactly
three cells — all fingerprint-changed, all digest-identical, everything else plan-identical with
equal digests (19 route cells; all 11 ladder cells plan-invariant):

| cell | plan fingerprint | census wall (one run) |
|---|---|---:|
| q02 | `9b8b8e011663bda5` → `5193425acd966a33` | 2,889 → 2,091 ms |
| q08 | `06e7c08c71d79255` → `5e07510a975325d2` | 8,586 → 4,377 ms |
| q09 | `a80cd80fa01877c9` → `3d202b6e227b3a8b` | 14,338 → 10,167 ms |

q02 engaging is coverage the plan did not claim: its side-local supplier/part predicates ride
the same reassociation site. Strict correctness green in both arms for q02, q08, q09, and the
q19 sentinel (8/8). The ADR 516 instrument witnesses the exact predicted collapse, per cell:

| witness | q08 off → on | q09 off → on |
|---|---:|---:|
| part suffix build rows | 1,600,000 → **10,801** | 1,600,000 → **87,125** |
| suffix-1 emitted rows | 47,989,007 → **323,097** | 47,989,007 → **2,610,723** |

Both arrows land exactly on the ADR 516 predictions. Every downstream suffix stage carries
identical probe/emission flow between arms — the predicate moved below the build and nothing
else changed. Peak RSS drops with the volume (q08 804 → 649 MiB; q09 2,439 → 2,275 MiB); spill
zero in both arms everywhere.

### Locked paired cohorts

Per cell, 10 adjacent pairs under the plan-distinct gate at the fixed 3,554 ms floor;
Hodges-Lehmann point and two-sided 95% Wilcoxon interval from the persisted journals; the two
cells are never summed. q02 rides as the engaged-cell regression check, not a materiality claim.

| cell | HL point | 95% interval | floor | verdict |
|---|---:|---|---:|---|
| q08 | **+3,884.2 ms** | [+3,862.7, +3,898.4] | 3,554 | **activate** (1.09x floor at the interval lower bound) |
| q09 | **+3,958.9 ms** | [+3,921.1, +3,987.5] | 3,554 | **activate** (1.10x floor at the interval lower bound) |
| q02 | +691.6 ms | [+675.4, +705.6] | 3,554 | below floor; an improvement, not a regression |

q08 pairs (candidate / control / delta, ms): 4,284.9/8,093.7/3,808.7; 4,004.8/7,921.5/3,916.7;
3,989.9/7,868.7/3,878.8; 3,989.4/7,859.7/3,870.3; 3,998.1/7,896.6/3,898.5; 4,057.2/7,921.8/3,864.6;
3,988.4/7,860.6/3,872.2; 4,030.5/7,902.2/3,871.8; 3,996.5/7,894.5/3,898.0; 3,991.9/7,910.0/3,918.1.

q09 pairs: 8,877.2/12,932.7/4,055.5; 8,922.3/12,915.4/3,993.1; 8,945.5/12,885.9/3,940.4;
8,897.5/12,852.6/3,955.2; 8,989.2/12,891.1/3,901.9; 8,946.4/12,928.3/3,982.0;
8,985.9/12,852.2/3,866.3; 8,929.5/12,908.4/3,978.9; 8,891.8/12,869.2/3,977.3; 8,924.6/12,833.9/3,909.3.

q02 pairs: 1,958.8/2,644.0/685.1; 1,961.3/2,665.0/703.7; 1,964.6/2,637.0/672.4;
1,964.3/2,667.9/703.6; 1,973.5/2,684.3/710.8; 1,991.9/2,673.9/682.0; 1,973.5/2,678.6/705.1;
1,972.2/2,679.7/707.5; 2,027.4/2,674.4/647.0; 1,983.0/2,658.9/675.9.

Phase 0's ceiling caveat resolved in the mechanism's favor: the dataset-twin ceilings were
+3,793.7 / +3,931.1 ms and the routed mechanism delivered +3,884.2 / +3,958.9 — the routed
filter's scan-pushdown interaction recovered the scan cost the twin had pre-paid.

### Activation

Per ADR 472's rule: two `activate` verdicts, three engaged cells, no regression anywhere
(q02 improves; every other cell is plan-identical with equal digests). This receipt flips
`side_local_filter_routing` to compiled default true, retargets the gauge knob-contract default
arm, and re-runs the full seal gate plus default-arm strict correctness smokes for q02, q08,
q09, and q19 from the flipped tree. q08 moves from ~7.9 s to ~4.0 s and q09 from ~12.9 s to
~8.9 s at the coordinate; q02 from ~2.7 s to ~2.0 s.

Named residuals, none claimed here: the on-arm suffix-1 still probes all 47,989,007 lineitem
rows (the collapse is in the build and the emissions — probe-side volume is a different
mechanism); the partsupp generic-key instrumentation gap and the scalar-prefetch decline carry
forward from ADR 516; q20 did not engage, so the q02/q20 coverage follow-on from ADR 512 is now
half-closed.

| evidence | artifact |
|---|---|
| censuses, both arms | `target/tpch/adr517_{route,ladder}_census_{off,on}_6c445d61/` |
| instrument runs, both arms | `target/tpch/adr517_instr_{q08,q09}_{off,on}_6c445d61/` |
| locked cohorts | `target/tpch/adr517_{q08,q09,q02}_paired_6c445d61/` |
| Phase 0 twin datasets | `target/tpch/adr517_twin_{q08,q09}/strict/sf8/` |
