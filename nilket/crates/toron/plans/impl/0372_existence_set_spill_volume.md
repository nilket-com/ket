# ADR 372 - Implementation plan: semi/anti existence-set spill-volume

Pairs with `plans/adr/0372_existence_set_spill_volume.md`. This is a common-wall cut for q04 and q21: both are just over
the 512 MiB spill budget at the `semi/anti existence-set spill writer`. Measure the writer by page/column first, then
reduce with a measured lever or name the irreducible wall. No cap bump.

## Phase 0 - spill breakdown and lever classification

For q04 and q21, run fixed-cap sf=10 persisted probes with temporary/env-gated tracing around
`SpilledSemiAntiExistenceBuild::write_batch_chunks` / `write_page_batch`:

- producer bytes, page count, per-page plaintext/ciphertext, rows, compression ratio;
- current written schema/column ordinals;
- required existence columns: join keys plus residual columns used by `match_filter`;
- q04 vs q21 shape differences: no residual / residual payload;
- responsible path in `write_streaming_semi_anti_existence` (filter/project/exchange ops before the writer).

Fill this table in the results doc:

| query | rows | pages | written columns | required columns | lever classification | before bytes |
| --- | ---: | ---: | --- | --- | --- | ---: |
| q04 | (measure) | (measure) | (measure) | (measure) | projection / layout / irreducible | 536,881,184 |
| q21 | (measure) | (measure) | (measure) | (measure) | projection / layout / irreducible | 536,876,880 |

Decision:

- **Dead columns present:** implement projected existence writes.
- **Already minimal, layout reducible:** apply the measured page/layout reduction.
- **Already minimal and no measured layout lever:** results-only named irreducible spill-volume wall.
- **Resident state appears instead of spill volume:** stop and record a named next wall; do not hide it inside this cut.

## D1 - projected existence writer, if measured-reducible

Add the minimal projected-write path to `SpilledSemiAntiExistenceBuild`:

- project each existence chunk to the required columns before IPC encoding;
- remap `existence_keys` into projected ordinals before building `JoinKeyPlan` / row refs;
- retain residual columns needed by `match_filter` and reconstruct any batch shape needed for residual evaluation;
- keep no-residual semi shapes keys-only when measured safe;
- reuse existing page writing/encryption/accounting and the same producer name.

Route only the measured semi/anti streaming existence path through the projected writer. If q04 and q21 need different
retained-column sets, compute them per stage rather than hard-coding a shared set. Topology-named; no qNN references in
executor core.

## D2 - page/layout lever, only if Phase 0 proves it

If the writer is already minimal and the 6-10 KiB excess is page/layout overhead, apply only a measured narrow change
(for example page sizing or compression setting) that preserves the existing spill budget and reader invariants.

If there is no measured lever, do not edit the engine for green. Seal the named irreducible verdict with the evidence.

## Tests / safety net

- **Projected existence correctness:** synthetic semi and anti cases where the existence input has dead payload columns;
  projected writer == unprojected writer, same selected preserved rows.
- **Residual preservation:** q21-style residual (`<>` and late conjunct) still evaluates per candidate from the projected
  payload; key-only membership is rejected for residual shapes.
- **Keys-only semi:** q04-style no-residual semi can retain only keys and still matches the materialized path.
- **Budget assertion:** spill budget remains 512 MiB; the producer goes under budget or remains named irreducible.
- **Regression family:** full `toron --no-fail-fast` is mandatory because this touches spill pages and semi/anti
  materialization.
- **Persisted correctness:** any newly green sf=10 q04/q21 row gets `correctness --data-source parquet` verification.
- **De-branding:** no qNN / ADR372 / temporary trace strings in executor core.

## Verification

Run via `scripts/cargo-cgroup` after formatting:

```text
scripts/cargo-cgroup fmt -- --check
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup check -p toron --all-targets
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --lib
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --test architecture_test
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --no-fail-fast
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron-tpch
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch -- correctness --query q04 --sf 0.01
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01
```

Release / fixed-cap persisted probes:

```text
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
    --query q04,q21 --engines toron --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr372
```

For any newly green row:

```text
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness --query q04 --sf 10 --data-source parquet --data-dir target/tpch/data
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness --query q21 --sf 10 --data-source parquet --data-dir target/tpch/data
```

Finish with:

```text
git diff --check
```

## Results doc

`crates/toron/plans/impl/0372_existence_set_spill_volume_results.md` must include:

- q04/q21 Phase-0 page/column breakdown and required-column sets;
- lever classification per cell: projection / page-layout / irreducible;
- implementation summary or explicit no-code irreducible verdict;
- before/after sf=10 producer bytes for q04 and q21;
- persisted correctness for any new green;
- confirmation no cap bump, no q18/q22 claim, `--no-fail-fast` green, temporary instrumentation removed, and no qNN in
  executor core.

## Risk / blast radius

- **Projection residual bug:** wrong answers if residual payload columns are pruned. Residual-specific tests and q21
  persisted correctness gate this.
- **Divergent cells:** q04 and q21 may need different retained columns; compute per-stage.
- **No reducible slack:** name the wall; do not raise the budget for a few KiB.
- **Spill reader invariant drift:** projected pages must still satisfy `SpilledSemiAntiExistenceBuild` page/index/cache
  invariants.
- **Regression:** full `--no-fail-fast` required.

## Seal

`impl: adr 372 existence set spill volume`. Do not commit wip drafts.
