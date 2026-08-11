# ADR 0547 implementation receipt — 2026-08-11

ADR 0547 closes the database half of conventional Tukey box summaries as one ordinary SQL
relation. ADR 0546 supplied grouped approximate quantiles; this cut proves that quartiles alone
were not the consumer unblocker. The delivered unit also includes 1.5×IQR fences, observed
in-fence whiskers, zero-or-more outlier rows, and their final recombination.

## The reported lineage failure did not reproduce

The earlier report was probed first with eleven aggregate stand-in variants because
`APPROX_QUANTILE` was not then stable under `GROUP BY`. Once ADR 0546 landed, the canonical query
was run with the real q1/median/q3 aggregates. It planned, bound, and executed successfully. No
projection/scope lineage repair landed, and the ADR's conditional lineage section and acceptance
gate were removed rather than being made green by a substitute query that never failed.

The five-CTE query does overflow Rust's default test-thread stack. It succeeds on an explicit
64 MiB thread, the established harness posture for deep plans. Both durable controls own that
stack explicitly, separating a harness crash from a database lineage result.

## Semantic oracle

`adr547_tukey_relation_preserves_fences_whiskers_outliers_and_null_policy` executes the full
single-statement query against three nontrivial categories:

- `fence` puts observations exactly on both mathematical fences; both remain in-fence and the
  category emits one null-outlier row;
- `spread` has fences -80/160, observed whiskers 0/80, raw extrema -100/300, two identical low
  outliers, and two different high outliers;
- `zero` has q1=median=q3=5, whiskers 5/5, repeated ordinary observations, and unequal values 0/10
  as outliers.

Rows with a null category or null observation are excluded, including an all-null category. The
fixture asserts the exact seven-column Arrow schema and every ordered row, so a fence substituted
for a whisker, hidden `DISTINCT`, dropped occurrence, fabricated empty-category row, or independently
recomputed quartile changes the oracle. A neutral-alias spelling exposes `key`, `low`, `high`, and
`value`, proving that the names are authored relation columns rather than database keywords.

## Local/distributed correspondence

`adr547_tukey_relation_corresponds_locally_and_across_worker_counts` injects the same fixed test
seed into three separate execution contexts: local, two-worker, and four-worker. Each context
propagates that seed to its own fragments. The ordered Arrow IPC digests and three-row cardinality
agree in all configurations, and the worker census proves both distributed runs used every
declared worker.

Every fixture category is below the first compaction threshold, so all observations remain at
weight one and the quantiles are exact. The digest equality is therefore a distributed relation
correspondence oracle for this exact population, not evidence that independently seeded or
post-compaction executions are byte-identical. Growing this fixture past compaction requires a
rank-bound oracle rather than retaining the digest-equality claim unchanged. The fixture keeps the
pre-existing global/grouped ADR 0546 columns unchanged and adds trailing category/sample columns
solely to the test worker, so the older aggregate correspondence controls retain their oracles.

The request is one SQL statement. Quartiles, fence calculation, whisker selection, outlier
classification, and final join therefore share Toron's existing request snapshot; there is no
client loop or interpolated second request.

## Scope guard

The architecture gate rejects `BOX_PLOT`, Tukey, whisker, or Polariton concepts in the logical IR,
SQL planner, aggregate catalog, and executor. It also pins both semantic fixtures and the neutral
alias control. No production source changed: ADR 0547 composes the capabilities already delivered
by ADR 0546 and the existing CTE/join/conditional-aggregate surface. The only `src/` edit is the
test-only `toron_fixture_worker` binary's trailing fixture columns; no product runtime path changed.

## Verification

- focused local semantic fixture: pass;
- focused two/four-worker correspondence fixture: pass;
- ADR 0546/0547 architecture controls: pass;
- `group_by_test`: 235 passed, 0 failed;
- `distributed_process_test`: 51 passed, 0 failed;
- Toron architecture: 2,119 passed, 0 failed;
- complete workspace `--no-fail-fast`: exit 0;
- workspace all-target check: exit 0 with zero warnings;
- formatting and diff checks: clean.

All three ADR 0547 controls are required and ran; none is ignored or tolerated. Existing declared
environment and weekly evidence targets remain outside this record.
