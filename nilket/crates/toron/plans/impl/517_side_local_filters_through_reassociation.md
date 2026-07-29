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
