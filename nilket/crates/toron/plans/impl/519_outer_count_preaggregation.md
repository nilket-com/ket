# impl 519 - outer count preaggregation plan

One bounded plan-shape rewrite: a grouped `COUNT` over a unique-key left outer join becomes
preaggregate-then-outer-join-then-coalesce, behind a six-condition declining gate. No Phase 0:
the operator's supported SQL twin already priced the economics at the exact coordinate
(+5,602.13 ms, clearing the floor), so the bind-level probes are the shape gate. The twin's
digest divergence is the design's sharpest constraint: the rewrite must emit the original
aggregate field contract, and census digest equality — which the twin could not meet — is a
hard acceptance rail.

## Immutable inputs

- tree: post-518 main (`ec095a23` lineage), all activated knobs default on;
- fixed materiality floor: 3,554 ms (inherited, not recomputed); per-cell, never summed;
- exact dataset: strict sf=8 persisted Parquet with `full_hash`; budgets 2 GiB / 2 GiB explicit;
- grounding receipts (operator, at `ec095a23`): q13 total 8.9-9.4 s; `left_outer_grouped`
  8.90 s owning all 5,131,808 spill bytes (preload/read/decode ~15 ms, pages pinned — spill is
  representation, not I/O); build 0.25 s; grouped fold 2.55 s; probe/materialization envelope
  ~5.77 s; 11,869,667 qualifying orders in, 12,269,685 rows materialized (incl. 400,018
  unmatched customers); twin: 8,937.96 → 3,335.83 ms, spill → 0, RSS ~650 → ~344 MiB, join
  work → 76 ms, orders aggregation 3.18 s; DuckDB runs the original shape in ~1.42 s;
- ADR 434-437: generic grouping-layout work is a poor bet — the remedy is plan shape.

## Step 1 - the rewrite and control

`PlannerConfig::outer_count_preaggregation`, compiled default **false** at the seal; gauge env
`TORON_TPCH_OUTER_COUNT_PREAGGREGATION`, loud "0"/"1" contract, row fields with provenance,
knob-contract test beside the prior ones.

At the grouped-aggregate bind site, gated by the knob and all six conditions (left outer +
plain equality + no residual/null-safe; group keys exactly the preserved join key; preserved
key proven unique from catalog evidence, fail-closed; exactly `COUNT(non-null right column)`;
deterministic right-side filters retained below; original output metadata preserved): rebuild
as `Project[group key, COALESCE(cnt, 0)]` over `L left_outer Aggregate[k_R; COUNT(col_R)](R)`,
emitting the original aggregate's field names, types, and nullability exactly. Anything
unproven declines; the knob-off arm is byte-identical. The pass body lives beside the
scalar-reduction family per the ADR 176 root discipline (root carries knob + hook only).

## Step 2 - tests

Structural (bind receipts): the q13 shape engages with the preaggregate below and the original
output schema on the projection; declines pinned per gate — join kind, residual ON, null-safe
keys, non-key or extra group keys, unproven-unique preserved key, non-`COUNT(right col)`
aggregates (`COUNT(*)`, left column, `SUM`), nondeterministic right filter; knob-off
byte-identity.

Semantic (execution, both arms identical, the established fixture family): unmatched preserved
rows → 0, duplicate right matches, NULLs in the counted column, NULL join keys both sides,
empty and all-filtered right sides; the engaged fixture asserts schema and nullability
identity between arms.

Architecture: reconcile any guards scanning the touched surfaces; name retargets in the
receipt.

## Step 3 - census, correctness, and engagement evidence

Both arms, q01-q22 and m00-m10, exact coordinate, sealed binary. Engaged set is empirical.
Required q13 evidence: digests identical (the rail the twin failed — this gate is the rewrite's
proof of the field contract); `left_outer_grouped`'s 8.90 s stage absent on-arm; spill
5,131,808 → 0; the orders preaggregate (~3.2 s class) and collapsed join (~76 ms class) as the
new envelope; fingerprint-changed cells run strict correctness in both arms; q19 sentinel.

## Steps 4-7 - seal, gates, cohorts, receipt

Standard protocol: cohesive seal, full gate battery with direct exit statuses, push, release
build; q13 paired cohort (20 runs / 10 adjacent pairs, plan-distinct, floor 3,554 ms,
Hodges-Lehmann + 95% Wilcoxon from the persisted journal) plus regression-check cohorts for
any other engaged cell; activation per ADR 472's rule, default flip + knob-test retarget +
full gates + default-arm smokes for the engaged set and q19 from the flipped tree; both-way
verdict reporting.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.

## Engagement result — closed red, recognizer not sealed

The load-bearing uniqueness gate is unavailable on q13's production-census catalog. The
persisted TPC-H `customer` schema declares `c_custkey` non-null and the manifest records
1,200,000 rows, but `customer_descriptor` uses `RowIdentityPolicy::AppendOnly`; the resulting
`TableBinding.primary_key_columns` is empty. The manifest carries no exact NDV/unique-key fact
that could prove the missing property. The rule therefore cannot prove `c_custkey` unique and
must decline exactly as predeclared in the ADR's red path.

A throwaway implementation established that this is the only blocking gate:

- the hand-built and SQL-planned nested q13 shapes engaged against an otherwise identical mock
  catalog declaring `c_custkey` as its primary key;
- the structural decline battery and execution equivalence fixtures were green, including
  duplicate right matches, unmatched rows, null right keys, empty/all-filtered right inputs,
  nullable-counted-column decline, and exact output field/nullability identity;
- at the exact sf=8 persisted-Parquet 2 GiB / 2 GiB coordinate, however, the official q13
  off/on pair remained plan-identical (`32c7b15a1c5a244e`), digest-identical, and retained
  5,131,808 B spill in both arms. The latched on arm therefore proved a recognizer decline,
  not an ineffective execution rewrite.

No uniqueness inference was invented and the benchmark descriptor was not retargeted: either
would weaken or move the proof obligation after the gate failed, and changing `customer` from
AppendOnly to PrimaryKey would affect every customer-using board cell rather than this bounded
mechanism alone. All throwaway mechanism, knob, and test changes were removed. No implementation
seal, census, or performance cohort exists for ADR 519.

The twin-proven economics and transformation theorem remain valid. A successor may reopen this
shape only after a separately justified catalog/descriptor cut supplies durable uniqueness
evidence (with its own board-wide census), or with a different transformation whose soundness
does not depend on preserved-key uniqueness.
