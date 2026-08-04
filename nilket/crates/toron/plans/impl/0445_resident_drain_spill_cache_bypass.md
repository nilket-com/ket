# impl 445 - resident-drain spill-cache bypass

Paired with `adr/0445_resident_drain_spill_cache_bypass.md`. Off-band,
additive-by-subtraction; no engine behavior change outside the admitted resident arm.

## Seams

| seam | file:line | role |
|---|---|---|
| arm fence | `exec/join/streaming.rs:15690-15693` | the decision; `record_branch(true/false)` at `:15695` / `:15740` |
| resident drain scan | `exec/join/streaming.rs:15703` | `read_all_batch_pages_profiled` — the single touch |
| resident work entry | `exec/join/streaming.rs:15730` → `:2480` `run_loaded_partition_work` | takes `work`, **not** `spilled` |
| resident index rebuild | `exec/join/streaming.rs:2505` | rebuilds from in-memory `build_pages` |
| streamed index rebuild | `exec/join/streaming.rs:15748` | streamed arm only |
| streamed page reload | `exec/join/streaming.rs:2209` `load_streamed_inner_match_pages` | live-cache path — do not touch |
| single-touch loops | `exec/join/spill.rs:1834`, `:1964` | `for page_index in 0..self.pages.len()` |
| plaintext insert | `exec/join/spill.rs:2493` | cap-zero early-return at `:2495` |
| decoded insert | `exec/join/spill.rs:2523` | cap-zero early-return at `:2528` |
| the O(n) cost | `exec/join/spill.rs:2488` `touch_cache_entry` | `lru.retain(...)`; called at `:2510`, `:2558` |
| cap construction | `exec/join/spill.rs:1351` | `JOIN_SPILL_CACHE_TARGET_BYTES.min(task_budget.memory.get())` |

## Structural preconditions (re-verify after any rebase)

The cut rests on these four facts. If any fails, abandon the cut — do not fence around it.

1. The resident arm reads once via `read_all_batch_pages_profiled` (`:15703`) and **moves**
   `build_pages` into `LoadedPartitionWork` (`:15711`).
2. `run_loaded_partition_work(work, membership_profile, grouped_chain_observer)` (`:15730`)
   does **not take `spilled`** — the resident consumer cannot call `read_batch_page_profiled`
   because it does not hold the build. Type-level, not convention.
3. The resident index rebuild reads from in-memory `build_pages` (`:2505`), not disk.
4. `partitioned_builds` (`:15680`) are per-group and consumed within the group — a build
   disabled on the resident arm is never handed to a streamed arm.

Numeric corroboration: `read_index_rebuild_cpu` logs exactly 64 calls in all five ADR 444
children, matching `resident_partitions = 64` one-for-one. Any rebuild from the streamed
site (`spill.rs:1837`) would force `streamed_partitions > 0`; it is 0.

## Cut

**1. One setter on `SpilledInnerJoinBuild`** (`spill.rs`):

```rust
pub(in crate::exec) fn disable_page_cache(&mut self) {
    self.cache_cap = 0;
    self.cache.clear();
    self.lru.clear();
    self.cache_bytes = 0;
}
```

Reuses the existing `cache_cap == 0` early-returns at `:2495` and `:2528`. No new branch in
the hot path — the suppression point is the guard already there.

**2. Call it on the admitted resident arm only** (`streaming.rs:15693`), adjacent to the
existing `record_branch(true)` and before the drain scan:

```rust
if resident_baseline_bytes.saturating_add(loaded_resident) <= loaded_cap {
    if let Some(observer) = grouped_chain_observer {
        observer.record_branch(true)?;
    }
    spilled.disable_page_cache();
    let build_pages = spilled
        .read_all_batch_pages_profiled(grouped_chain_observer)
        .await?;
    // ... LoadedPartitionWork { build_pages, .. } / run_loaded_partition_work ...
} else {
    // streamed arm unchanged — cache stays enabled
}
```

The `else` arm is not edited. That is the fence.

**3. Nothing else.** No LRU rewrite, no cap retune, no `record_batch_memory_bytes` change,
no ADR 444 instrument change.

## Tests

**Discriminating (locks the design — must fail under the rejected alternative):**

- `adr445_streamed_arm_retains_page_cache_hits` — force the streamed arm (partition larger
  than `loaded_cap`); assert plaintext cache inserts > 0 **and** hits > 0. Fails if the
  bypass is unconditional or the cache is deleted, locking "shape-fenced" over both rejected
  alternatives.
- `adr445_resident_arm_performs_no_cache_inserts` — force the resident arm; assert zero
  inserts, zero evictions, empty LRU across the drain.

**Regression:**

- `adr445_resident_arm_reads_each_page_exactly_once` — assert `physical_read_calls == unique
  pages` on the resident arm with the bypass on, pinning the single-touch property the cut
  depends on.
- Existing spill/cache unit tests stay green unmodified — they cover the streamed arm.

**Architecture guard** (`tests/architecture/exec_decomposition.rs`, shaped like 439's
ordering fence, `adr/439:44-46`):

- `adr445_page_cache_bypass_is_fenced_to_the_resident_arm` — slice the live owner block;
  assert `disable_page_cache` appears **after** `record_branch(true)` and **before**
  `read_all_batch_pages_profiled`; assert it is **absent** from the streamed arm block and
  from `load_streamed_inner_match_pages`. Ordering and arm-identity, not co-presence.
- Assert `touch_cache_entry` still exists and remains reachable from `insert_cache_entry` —
  the cut must not silently become an LRU deletion.

`streaming.rs:21043` already asserts `!drain.contains("read_all_batch_pages")` for a
different drain owner. The two guards cover different owners and coexist; slice the ADR 445
guard narrowly so it cannot capture that owner's block.

Avoid the string `monotonic` in any test or source name — it contains `tonic` and trips
`adr043_no_grpc_protobuf_or_arrow_flight`, which source-scans `src` and `tests`. Use
"strictly increasing".

## Evidence to produce

- Strict q05 sf=10 `pass@compare` before/after: status, plan fingerprint, digest, schema,
  row count, spill tuple — all must match exactly.
- 4 GiB / no-swap survival, scratch empty before and after.
- **Corroboration only, not a gate:** unprofiled before/after wall (profilers off), five
  children each, scope order randomized or interleaved and recorded. The receipt must state
  plainly that 11.9% sits under the 23.86% band, that no materiality claim is made, and that
  a null result does not invalidate the cut. The accepted result is the structural bypass
  plus streamed-path preservation.
- Direct receipt that the dead work is gone (444 profiler on, one child, disclosed
  separately): `read_cache_lru` ≈ 0, plaintext/decoded inserts 0, resident branch count
  unchanged at 64, streamed 0, amplification still 1.0, `read_index_rebuild_cpu` calls still
  64.

Clear `target/tpch/join_spill` before any sweep — post-430 the gauge spills to disk there,
not `/tmp`, and stale scratch breaks sf=10 runs.

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

Run the FULL `architecture_test` after `fmt`, never a filtered subset — the guards
source-scan and `fmt` mutates the scanned text. Record gates as green only after observing
them green, never from a prior session's summary.
