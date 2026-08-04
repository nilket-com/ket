# ADR 439 impl - primitive resident spill index on the partitioned scan-build path (cut B)

Behavior change scoped to the build-index *representation* on one path. Cut **B** (surgical guard).

## The change

Live owner: `try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks` (`streaming.rs:4774`).
Writes build pages via `write_batch_chunks_unindexed`, then `finish`, then `rebuild_index_from_pages` per
partition (`streaming.rs:~4931`). Nothing indexes during write, so the opt-in goes **immediately before
rebuild**.

1. `key_plan`/`null_safe_keys` are loop-invariant (`streaming.rs:4850`). Compute eligibility once before
   the group loop:
   ```rust
   let use_primitive_resident_index =
       !null_safe_keys && matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64);
   ```
2. In the per-partition finish/rebuild loop, immediately before `spilled.rebuild_index_from_pages(...)`:
   ```rust
   if use_primitive_resident_index {
       spilled.use_compact_resident_index();
   }
   ```

No change to `rebuild_index_from_pages`, the lookup accessor, or residency accounting.

## Why correct (grounded)

- Rebuild honors the flag (`spill.rs:1747`); probe reads compact (`spill.rs:2071`); residency accounted +
  guarded per page (`spill.rs:1783-1788`).
- `from_build_schema_and_probe_schema` returns `Generic` for null-safe and for non-single/mismatched keys
  (`join.rs:1232-1256`), so the fence only fires for single-column non-null-safe `Int32`/`Int64`.
- Precedent: enable→rebuild→lookup runs gate-green at `streaming.rs:13650`.

## Tests (`-p toron --lib`)

- **Production sequence selects primitive:** `write_batch_chunks_unindexed` → `finish` → guarded opt-in →
  `rebuild_index_from_pages` over ≥2 non-null-safe `Int64` partitions; assert `compact_index ==
  Int64(_)`, `self.index` empty, lookup multiset identical to the legacy build.
- **Generic untouched:** `Generic` plan → `compact_index == None`, `self.index` populated as before.
- **Null-safe untouched (honest representation):** build **nullable** `Int64` build/probe schemas, derive
  the plan via `from_build_schema_and_probe_schema(..., null_safe_keys=true)`, **assert it is `Generic`**,
  write batches containing **actual null keys**, rebuild, and assert `compact_index == None` with null-safe
  lookup parity on `self.index`. Do **not** construct `JoinKeyPlan::Int64` + `null_safe=true` (incoherent;
  `join_key_at`'s Int64 arm is not the null-safe encoding).
- **Residency:** `resident_index_bytes == compact_index.accounted_bytes()` post-rebuild; guard fires under
  a tight budget.

## Architecture guard (`--test architecture_test`)

**Slice the live owner and check order** — not mere co-presence. Extract the body of
`try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks`, and assert a guarded
`use_compact_resident_index()` (gated by `matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64)` and
`!null_safe_keys`) appears **before** the adjacent `rebuild_index_from_pages` call within that slice. This
prevents spill-unit tests from passing while the production site is unwired, and pins the fence beside the
enum arm it mirrors. No query literal.

## Speed rail (the seal gate) — ADR 356 dynamic band

- **Predeclare before measuring** (437 lesson). The band is **`max(0.10, 2 × max(0.0556, current_spread))`**
  computed from the measured sample; **0.1112 is only the minimum** (when `current_spread ≤ 0.0556`), not a
  fixed threshold.
- Unprofiled ADR 356-shaped strict sf=10 q19: 1 warmup + 7 measured, exact 356 envelope, fingerprint
  `65c3c2c0b0a99063` / 51,471,552 spill bytes invariant across measured rows.
- **≤0.20 stability gate; one retained retry; never average attempts.**
- Seal iff the median **execute wall** improvement clears the band computed from the actual spread.
  Sub-band → honest-red → revise/remove; never a moved band; never a leaf-only claim.

## Survival rail (operational)

One strict sf=10 q19 **persisted-Parquet pass@compare**:
- `MemoryHigh=4G`, `MemoryMax=4G`, `MemorySwapMax=0`, `TasksMax=512`, `RuntimeMaxSec=600`;
- `target/tpch/join_spill` fresh/empty **before and after** (with the post-exit drain);
- fingerprint `65c3c2c0b0a99063` / digest / schema / 1 row / 51,471,552 spill bytes invariant.

Required because resident representation changed. No full-board redux.

## Diagnostic reread (corroborative only — absolute + relative)

Five fresh 438-ledger children (`TORON_GLOBAL_FOLD_JOIN_FILTER_PROFILE_DIR`) must show **both**:
- normalized shares of `probe_lookup_select` and D1 fall vs. the 438 receipt (58.1% / 35.3%), **and**
- **absolute** median `probe_lookup_select` micros and D1 micros fall vs. 438's five-child receipt.

A share can fall merely because another leaf grew, so absolute time is required. **Not** the speed gate —
the unprofiled seven-run wall is.

## Seal gate (council.md, via scripts/cargo-cgroup, after fmt)

```
scripts/cargo-cgroup fmt  -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
```

- `exec.rs` ratchet 7,499; join root ratchet 3,713 unchanged (a few lines in the existing owner — no new
  held state, no async-body growth, no new boxing).

## Implementation contingencies

- If the (A) generic-accounting parity check later comes back identical, A becomes a clean follow-on
  `refactor:`; B ships now without it.
- Wall clears the band → 438 before-receipts (D1 58.1%, `probe_lookup_select` 35.3%, generic
  `HashMap::get` frame) are the paired before/after for the benchmarks-article record.
- Wall sub-band → preserve the honest-red receipt, decide revise/remove; do not seal a below-band win
  (437 precedent).
