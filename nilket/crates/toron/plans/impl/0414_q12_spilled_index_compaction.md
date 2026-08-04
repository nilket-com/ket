# ADR 414 - Implementation plan: q12 spilled-build resident-index compaction

Pairs with `plans/adr/0414_q12_spilled_index_compaction.md`. Apply the 405 recipe (primitive keys, packed refs, inline
small lists) to the spilled build's resident row index (`HashMap<JoinKey, Vec<SpilledBatchRowRef>>`). Honesty bar:
>=2x measured `resident_index_bytes` reduction on q12's build - not a squeak under the ~67 KB edge. Faithful
accounting; guard stays armed; fallbacks (sharded/deferred index) named, not smuggled.

## Phase 0 - index shape (before code)
1. Reproduce q12 (~8 s) and capture the component payload; instrument (one-off or extended payload) the walling
   build's index shape: rows, unique keys, key plan, refs/key distribution, bytes/ref, and the overhead split
   (enum-key + HashMap bucket + Vec overhead vs packed-ref payload).
   ```bash
   systemd-run --user --same-dir --wait --collect --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=360s \
     /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q12 \
       --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr414_q12_sf10_pre.json 2> target/tpch/adr414_q12_sf10_pre.err || true
   ```
2. Project the compaction arithmetic (target representation bytes vs current 1,074,377,480). If the projection cannot
   clear ~2x -> reconsider before building (the fallbacks are a different mechanism).
3. Record before code.

## Phase 1 - the compact representation
- Primitive-key plans: replace the enum-keyed std HashMap with the 405-family structure (generalize
  `PrimitiveBuildTable<K>` or a sibling: open addressing over raw K, packed 8-byte refs `(u32 page, u32 row)` -
  guarded conversion from `usize` page index - inline-small ref lists, `BuildRowList`-style).
- `Generic`/null-safe keys: keep a correct fallback (current representation acceptable there).
- **Faithful accounting:** recompute `resident_index_bytes` from the new structure's actual footprint (buckets +
  entries + ref storage); write-site guard unchanged and armed.
- **Loaded-index sibling:** `LoadedSpilledInnerBuild` has the same `HashMap<JoinKey, Vec<SpilledBatchRowRef>>`
  resident-index shape on the decoded-pages path. Fold it into the same representation only if the abstraction is
  mechanically identical; otherwise name it as a follow-on so the old index shape is not left untracked.
- **Existence-set sibling:** fold in only if the identical mechanical change applies cleanly; else name as follow-on.
- No cap/multiplier/scope change; no qNN branch.

## Tests (discriminating)
- **Lookup equality:** same refs in the same order across old/new representations on a duplicate-heavy fixture -
  fails under a reordered/lossy index;
- **Accounting fidelity:** accounted `resident_index_bytes` equals the structure's computed footprint (assert both
  directions on a synthetic build);
- **Guard armed:** a genuinely-over compact index still fails with the same producer + component payload;
- **Determinism:** output/probe tests prove random hash seeding or table layout cannot affect row order; the executor
  must look up by probe key and preserve per-key ref insertion order, never iterate the index table for output;
- **Packed-ref bounds:** page_index > u32::MAX (synthetic) is handled (guarded conversion - error or fallback, not
  truncation);
- **Fence:** Generic-key builds keep the fallback representation;
- carry spill/join/reservoir suites green (q05/q19 machinery).

## Phase 2 - re-measure
```bash
# driver: q12 sf=10 (component payload = the headroom proof)
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q12 \
    --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr414_q12_sf10_post.json 2> target/tpch/adr414_q12_sf10_post.err || true
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  target/release/toron-tpch correctness --query q12 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data
# headroom proof: if q12 passes, force a payload observation (temporary low-cap probe or debug assertion) showing the
# new resident_index_bytes on the same build - the >=2x bar is measured, not inferred from the pass.
# controls
for q in q03 q04 q05 q08 q10 q19; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=360s \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data
done
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr414_q03_sf1_control
```
- Primary: q12 pass@compare (or named + reproduced next wall) **and** the measured >=2x index reduction.
- Controls: all six greens pass@compare (q05/q19 = machinery sentinels, q19 spill byte-sane); q03 sf=1 in band
  (fp `666e0ac028036f70`).
- Side observation (not a claim): q05/q19 execute deltas, since they probe this index on their hot paths.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron spill --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (core executor cut); JOBS=4 if oomd
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast   # REQUIRED (full crate)
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)
`crates/toron/plans/impl/0414_q12_spilled_index_compaction_results.md`:
- Phase 0: index shape + overhead split + projected arithmetic - before code;
- the representation (structure, packed refs, inline lists, fallback fence) + the measured before/after
  `resident_index_bytes` (>=2x bar) + lookup-equality/accounting/guard tests;
- q12 reach-board move + all controls + the q05/q19 side observation;
- **or** the honest non-bank (compaction under bar) + the named sharded/deferred-index mechanism;
- confirmation: faithful accounting, guard armed, no cap bump, loaded-index/existence-index siblings folded-or-named,
  full gates, no `.perf.data`, scope honesty (the 22-query sweep is the separate closing artifact).

## Risk / blast radius
- **Under the bar** -> honest non-bank; fallbacks named.
- **Accounting drift / formula-gaming** -> fidelity test both directions.
- **Probe order/duplicate change** -> lookup-equality test + q05/q19 sentinels.
- **Sibling index left behind** -> loaded-index/existence-index fold-or-name requirement.
- **Next wall behind the index** -> reach-board honesty.
- **usize->u32 page truncation** -> guarded conversion + bounds test.

## Seal
`plan: adr 414 q12 spilled index compaction` (the dual). Implementation:
`impl: adr 414 q12 spilled index compaction`. Do not commit `.perf.data`.
