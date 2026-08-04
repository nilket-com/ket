# ADR 365 - persisted TPC-H correctness surface

## Status
Proposed.

## Context

ADR 360 made sf=10 measurable by adding a persisted Parquet data surface for `perf`, but the correctness rail did not
get the same surface. `toron-tpch correctness` still has no `--data-source` / `--data-dir` arguments and calls the
memory-fixture `check_strict_*` functions directly. At sf=10, that means correctness hits ADR 359's old in-memory
fixture wall before it can verify any Toron pass row from the persisted perf rail.

Running the sf=10 atlas without this cut would produce reach/perf rows but no live sf=10 Toron correctness provenance.
That is too weak for the next engine fixes: a future cell could become `pass@execute` while the answer is wrong, and the
atlas would not catch it.

## Decision

Add a behavior-neutral persisted correctness path to the TPC-H gauge:

- `toron-tpch correctness --data-source memory|parquet --data-dir target/tpch/data`
- memory remains the default and stays byte-identical for existing correctness uses;
- parquet/persisted uses ADR 360's manifest, metadata, and `ParquetHarnessScan`;
- the same independent query oracles and schema comparators are used after execution;
- no `crates/toron` engine behavior changes.

ADR 366, the sf=10 atlas redux, depends on this. The atlas should not bank reach-green cells without live correctness on
the same persisted data surface.

## Scope

In scope:

- CLI plumbing in `crates/toron-tpch`.
- A shared correctness execution helper that can run a strict query either from in-memory fixtures or persisted metadata
  and storage.
- Per-query correctness dispatch preserved for schema/oracle comparison.
- Live persisted correctness smoke at sf=10 for:
  - q06: expected pass.
  - q14: expected pass.
  - q21: expected current named wall until the q21 spill-volume wall is fixed.
- q17 correctness-scale policy reviewed explicitly.

Out of scope:

- engine fixes;
- new oracles;
- DuckDB cross-engine digest oracle;
- performance optimization;
- running the full sf=10 atlas.

## Design

The existing correctness functions combine three steps:

1. build tables;
2. execute the strict SQL through the shared execution substrate;
3. assert schema and compare to the independent oracle.

ADR 365 separates step 1 from steps 2/3. The compare logic remains per query; only the execution input changes.

For `--data-source memory`, preserve the current path.

For `--data-source parquet|persisted`:

1. Build the persisted query spec with the same table set and SQL as the perf path.
2. Execute via the metadata/storage path, not `HarnessTable` batches.
3. Feed the returned batches to the existing schema assertion and oracle comparator for that query.
4. Emit the same `tpch_correctness` telemetry row.

Avoid a second query registry if possible. The persisted correctness path should reuse the existing persisted perf query
spec and the existing strict SQL/table-family mapping, so perf and correctness cannot drift on table selection.

## Phase 0 - q17 policy and API shape

Before implementing broadly:

1. Confirm the persisted query spec can be built for each correctness-supported query.
2. Confirm each `check_strict_*` function can be split into "execute input" + "compare batches" without changing the
   oracle.
3. Review q17's correctness-scale policy. If sf=10 can be safely checked with the independent oracle and a non-vacuity
   guard, extend the policy. If not, record q17 as a named `q17_sf10_oracle_policy_gap` for ADR 366 rather than hiding
   it.

## Acceptance

- Existing memory correctness behavior is unchanged:
  - q06 at default scale still passes.
  - existing correctness tests still pass.
- Persisted correctness works under the fixed 4 GB sf=10 surface:
  - `correctness --query q06 --sf 10 --data-source parquet --data-dir target/tpch/data` -> `pass@compare`.
  - `correctness --query q14 --sf 10 --data-source parquet --data-dir target/tpch/data` -> `pass@compare`.
  - `correctness --query q21 --sf 10 --data-source parquet --data-dir target/tpch/data` reaches the current named
    q21 wall, not the old in-memory fixture wall.
- The correctness row records the same `query`, `sf`, `mode`, `phase`, `status`, and `reason` semantics as before.
- `correctness-matrix` remains a cache renderer and is not promoted to sf=10 evidence.
- No `crates/toron/src` changes.

## Verification

- `scripts/cargo-cgroup fmt -- --check`
- `scripts/cargo-cgroup check -p toron-tpch --all-targets`
- `scripts/cargo-cgroup test -p toron-tpch`
- persisted q06/q14/q21 correctness commands above, run against the ADR 360 dataset under the fixed 4 GB cap where
  appropriate
- `git diff --check`

If implementation touches shared gauge execution in a way that could alter engine behavior, also run
`scripts/cargo-cgroup test -p toron --no-fail-fast`.

## Guardrails

- Behavior-neutral CLI plumbing: no engine fixes and no budget changes.
- Persisted correctness must use the same Parquet files as perf.
- Do not use cached `correctness-matrix` rows as sf=10 evidence.
- Do not call sf=10 cells correctness-green unless the persisted correctness command actually compared the answer.

## Forward

After ADR 365, ADR 366 can run the sf=10 atlas with reach, perf, DuckDB, and live correctness provenance on the same
persisted data surface. Any query still lacking a correctness route will be a named oracle-policy gap, not a hidden
weakness in the atlas.
