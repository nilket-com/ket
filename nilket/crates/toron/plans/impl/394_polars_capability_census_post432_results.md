# ADR 394 follow-up results - post-432 Polars SQL capability census

## Verdict

The live persisted sf=10 census defines the current Rust Polars SQL surface before the ADR 356 article rail:

- **Five of 22 cells are capability-pass / peer-timing candidates:** q01, q03, q06, q14, and q19.
- **q03 is value-validated.** Its dedicated oracle gate passes at sf=10 with 114,003 rows and maximum revenue
  difference 267 raw scale-4 units against the 1,000-unit tolerance.
- **q01, q06, q14, and q19 are reach+schema-only.** They are eligible as explicitly labeled peer-substrate timing,
  not as correctness-validated results.
- **Three cells are classified `not_attempted`:** q04 and q15 on interval syntax, q22 on a correlated subquery.
- **Fourteen cells are classified `fail`:** thirteen canonical comma-join queries plus q13's non-equi join
  constraint. These are adapter/dialect classifications, not Toron board failures.

The result is 5/22, not ADR 394's earlier Python-probe prior of 12/22. The Python probe and the current pure-Rust
`polars_sql` adapter are different execution surfaces. No dialect mapping or classifier fix is included here.

## Conditions and provenance

| item | value |
|---|---|
| evidence commit | `574dee136f1a22986439f9467d85ecef889099dd` |
| engine-source seal | `7488ddff01b6dd9c04f2dd9eec0223b65f022529` |
| binary SHA-256 | `cca277b41ca6bf87dc401e0bb97cbdaff896d7cf751bd4859ac5539eda7a02ac` |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| Polars | `0.54.4` |
| mode / source | strict / persisted sf=10 Parquet |
| runs / warmup | 1 / 0 |
| artifact root | `target/tpch/polars_capability_post432/` |

Commit `574dee1` adds only the post-432 ADR 415 evidence document; its engine source is byte-identical to the ADR 432
seal. The already feature-enabled release binary was reused. The census command was:

```text
target/release/toron-tpch perf --engines polars_sql --query all --sf 10 --mode strict \
  --runs 1 --warmup 0 --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/polars_capability_post432
```

The parent completed successfully in 1:24.57 and retained exactly 22 raw rows. Each query ran in the perf rail's
fresh `perf-run-one` child. These elapsed and RSS observations are execution-provenance only; this one-run census
makes no timing claim. The successful q01 and q03 children peaked above 4 GiB, reinforcing that the later article
rail is the separate unconstrained performance profile, not the board's fixed-cap correctness envelope.

## Capability census

`pass` means Polars reached output and recorded row/schema metadata. It does not imply value correctness. Reasons,
fingerprints, row counts, and schema fingerprints below are copied from `latest_raw.jsonl`.

| query | status | classified reason | Polars | plan fingerprint | rows | schema fingerprint | adapter SQL source |
|---|---|---|---|---|---:|---|---|
| q01 | pass | none | 0.54.4 | `fnv1a64:026e3dcfdf0f47ad` | 4 | `fnv1a64:eba8c5cc1c04ce95` | canonical `export.sql` |
| q02 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:62f55f3d17655f75` | n/a | n/a | canonical `export.sql` |
| q03 | pass | none | 0.54.4 | `fnv1a64:e0ebcbbbe9bf7f08` | 114,003 | `fnv1a64:e6f7a214e444a631` | dedicated `POLARS_Q03_SQL` |
| q04 | not_attempted | `polars_sql_unsupported_interval_syntax` | 0.54.4 | `fnv1a64:0501a54a11aa34c6` | n/a | n/a | canonical `export.sql` |
| q05 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:4d891196319c0507` | n/a | n/a | canonical `export.sql` |
| q06 | pass | none | 0.54.4 | `fnv1a64:453bd3ede2da1d2e` | 1 | `fnv1a64:58f76a07f91646c0` | canonical `export.sql` |
| q07 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:146ddc5a8257e5f6` | n/a | n/a | canonical `export.sql` |
| q08 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:a9348fc65d7b7fb7` | n/a | n/a | canonical `export.sql` |
| q09 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:82687fbef117a476` | n/a | n/a | canonical `export.sql` |
| q10 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:4b78fb4660b11c6b` | n/a | n/a | canonical `export.sql` |
| q11 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:eab0e69f0dee9e48` | n/a | n/a | canonical `export.sql` |
| q12 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:181f97a1e6f16210` | n/a | n/a | canonical `export.sql` |
| q13 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:e8f5d53edb5dba86` | n/a | n/a | canonical `export.sql` |
| q14 | pass | none | 0.54.4 | `fnv1a64:a34a80b77bb3e0dc` | 1 | `fnv1a64:d05f175c3df109fd` | dedicated `POLARS_Q14_SQL` |
| q15 | not_attempted | `polars_sql_unsupported_interval_syntax` | 0.54.4 | `fnv1a64:fabcc71e1481d211` | n/a | n/a | canonical `export.sql` |
| q16 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:f63a99c66e723cfe` | n/a | n/a | canonical `export.sql` |
| q17 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:64d23d150008a0c5` | n/a | n/a | canonical `export.sql` |
| q18 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:068bfbf049af2b96` | n/a | n/a | canonical `export.sql` |
| q19 | pass | none | 0.54.4 | `fnv1a64:0d0958ce73500530` | 1 | `fnv1a64:58f76a07f91646c0` | dedicated `POLARS_Q19_SQL` |
| q20 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:716897ee8e41711e` | n/a | n/a | canonical `export.sql` |
| q21 | fail | `polars_execute_failed` | 0.54.4 | `fnv1a64:55c65d9620e6ef18` | n/a | n/a | canonical `export.sql` |
| q22 | not_attempted | `polars_sql_unsupported_correlated_subquery` | 0.54.4 | `fnv1a64:8f624dcbfedb17bb` | n/a | n/a | canonical `export.sql` |

### Failure taxonomy

Thirteen `polars_execute_failed` rows report that multiple tables in the canonical comma-join `FROM` clause are
"not currently supported" and request explicit `JOIN` syntax: q02, q05, q07-q12, q16-q18, q20, and q21. q13 reports
that only equi-join constraints are currently supported because its canonical form exposes the `NOT LIKE` condition
as a join constraint.

The adapter's known-unsupported classifier recognizes the contiguous phrases `not supported` or `unsupported`.
Polars' `not currently supported` and `only ... currently supported` messages do not match, so these dialect
limitations become `fail(polars_execute_failed)` rather than `not_attempted`. This is a taxonomy finding, not a
capability difference, and the census preserves the emitted status honestly. Reclassifying it is optional harness
cleanup outside this measurement.

q04 and q15 do match the interval-syntax classifier. q22 matches the correlated-subquery classifier. No process or
harness failure erased a later cell; the parent command completed all 22.

## Validation level

Polars perf rows deliberately carry `digest: null`. There is no generic Polars-to-oracle comparator. The q03-only
gate is therefore the sole value-validation row; other passing cells remain valid peer-timing candidates at the
explicitly weaker reach+schema level.

| query | validation level | gate mechanism | gate result | evidence |
|---|---|---|---|---|
| q01 | reach+schema-only | none | n/a | 4 rows; schema `fnv1a64:eba8c5cc1c04ce95` |
| q03 | value-validated | dedicated q03 oracle | pass | 114,003 rows; max revenue diff 267 <= 1,000; order/date/priority checks pass |
| q06 | reach+schema-only | none | n/a | 1 row; schema `fnv1a64:58f76a07f91646c0` |
| q14 | reach+schema-only | none | n/a | 1 row; schema `fnv1a64:d05f175c3df109fd` |
| q19 | reach+schema-only | none | n/a | 1 row; schema `fnv1a64:58f76a07f91646c0` |

The q03 value-gate row exactly matches the capability row's Polars version, plan fingerprint, row count, and schema
fingerprint. Its separate command exited successfully in 10.35 seconds.

## Derived sets

- **Capability-pass / ADR 356 Polars perf-candidate:** `{ q01, q03, q06, q14, q19 }`.
- **Value-validated:** `{ q03 }`.
- **Reach+schema-only:** `{ q01, q06, q14, q19 }`.
- **Not attempted:** `{ q04: interval_syntax, q15: interval_syntax, q22: correlated_subquery }`.
- **Fail:** `{ q02, q05, q07, q08, q09, q10, q11, q12, q13, q16, q17, q18, q20, q21 }`, with exact
  `polars_execute_failed` diagnostics retained in the raw artifact.

The ADR 356 article rail may include the five capability-pass cells as Polars peer timing with a mandatory
validation-level column. Only q03 may be called value-validated. Toron and DuckDB remain full-board and correctness
authoritative. Requiring value validation for every Polars number would be new comparator implementation work and
would silently strengthen ADR 394's accepted contract.

## Hygiene

- Live persisted sf=10 rows only; no cached matrix or correctness row.
- Measurement-only: no Toron or Polars-adapter source changed.
- Explicit `--engines polars_sql`; `--engines all` remains Toron + DuckDB.
- One run and no warmup; no timing, ratio, median, or stability claim.
- Exactly 22 raw capability rows plus one separate q03 value-gate row are retained under the artifact root.
- No dialect expansion, classifier change, or per-query comparator was added.
- No `.perf.data` is part of this result.

## Forward arc

The ADR 356 article rail now has a measured Polars matrix: full Toron/DuckDB 22-query coverage plus Polars
`{q01,q03,q06,q14,q19}`, each carrying its validation level. The repeated rail, not this census, owns seven measured
runs, one warmup, medians/minima/spread, plan stability, and performance ratios.

Three distinct follow-ons remain optional and must not be conflated with the rail: explicit-join dialect mappings
for selected canonical failures, classifier reconciliation for Polars' `not currently supported` wording, and
per-query value comparators beyond q03. The census supplies evidence for those decisions but makes none of them.
