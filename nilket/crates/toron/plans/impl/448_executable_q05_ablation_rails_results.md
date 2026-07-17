# ADR 448 implementation receipt

The three executable rails are implemented; this cut makes no q05 wall, spill,
or materiality claim.

Rail 1 uses one shared source-column union for filtered and control persisted-
Parquet scans, then applies one post-filter projection. The architecture guard
`adr448_filtered_path_projects_exactly_once` protects the prior double-
projection failure mode.

Rail 2 carries spill scope as a typed value. Whole-query totals and grouped-root
deltas cannot be divided by the API; the summary labels its whole-query value as
`whole_query_total`.

Rail 3 provides a default-on local runtime-filter ablation, per-child
`Command::env` overrides, deterministic on/off alternation, arm metadata, and a
capability probe. The corrected q06 smoke confirms the schedule: warmup `on`,
measured `on` then `off`; both outputs have identical digest/schema/row count.

Observed q06 smoke (debug, sf=0.01, two measured runs) is only an executable
protocol check: 10.259 ms (`on`) and 10.606 ms (`off`), with zero spill. It is
not a performance result and is not compared with any baseline.

## Verification

Directly observed on the final implementation tree:

- `cargo fmt --all -- --check` — green.
- `cargo test -p toron --lib` — 1323 passed, 0 failed.
- `cargo test -p toron --test architecture_test` — 2091 passed, 0 failed.
- `cargo test -p toron --no-fail-fast` — all suites green.
- `cargo test -p toron-tpch --no-fail-fast` — all suites green (48/23/15/5/176/12/44/2 result groups; 0 failed).
- `cargo test -p toron-tpch --features polars-sql-reference --no-fail-fast` — all suites green (48/24/15/5/176/12/44/2 result groups; 0 failed).
- `git diff --check` — clean.

No q05 paired cohort was run here. Existing q05 timings predate Rail 1 and are
not comparable to any future post-Rail-1 measurement. ADR 449 owns the paired
ablation and any wall or spill attribution.

## Join-path correctness check

The q06 smoke above is intentionally only a driver check: q06 has no join and
does not reach the local runtime-filter admission site. A persisted strict q05
check at sf=1 was therefore run with two measured arms. The `on` and `off` rows
have identical digest, schema fingerprint, row count, plan fingerprint, and
status; the recorded arm metadata is `on` at run index 0 and `off` at index 1.
This demonstrates that both arms reach the same correctness surface on the
admitted join-reservoir path. Its 6.736 s versus 6.775 s execution times are
protocol evidence only, not a speed claim. A production sf=10 attempt was not
used as a timing result: its unfiltered arm exceeded the short-check budget
after the filtered arm completed. That divergence is retained as qualitative
evidence that the ablation arms are not identical; ADR 449 must attach the
grouped-chain profile and verify the distinction directly through
`reservoir_build_rows` before any paired cohort or wall claim.
