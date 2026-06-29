# ADR 374 - Implementation plan: incremental spill resident accounting

Pairs with `plans/adr/374_incremental_spill_resident_accounting.md`. Replace the O(index) re-fold in
`guard_resident_state` with O(1) maintained `resident_index_bytes` counters on `SpilledInnerJoinBuild` and
`SpilledSemiAntiExistenceBuild`. Behavior-preserving perf cut; no cap change. Success = re-profile shows the
accounting stacks gone + the next hot stack, not cells-green.

## Phase 0 - mutation-site census + third-fold decision (before code)
- For each index (`HashMap<JoinKey, Vec<SpilledBatchRowRef>>`, `HashMap<JoinKey, Vec<SpilledExistenceRowRef>>`),
  enumerate **every** mutation: new-key insert, ref append, clear/remove/repartition. List the call sites; the counter
  is updated at all of them.
- Inspect `join_spill_index_bytes` (spill.rs:1548, used at 181 by `resident_without_cache`): is it on a per-batch/
  per-mutation hot guard path, or only pre/post-write accounting? Record the verdict:
  - cold/pre-post -> **out-of-scope** (left as a fold);
  - hot per-mutation -> **same counter treatment or a named sibling** (decide before implementation).
- Record the census + verdict in the results doc.

## D1 - add the counters
- Add `resident_index_bytes: u64` to `SpilledInnerJoinBuild` and `SpilledSemiAntiExistenceBuild`.
- At each censused mutation site:
  - new key: `resident_index_bytes = resident_index_bytes.saturating_add(64 + join_key_heap_bytes(&key) as u64)`;
  - ref append: `resident_index_bytes = resident_index_bytes.saturating_add(size_of::<RowRef>() as u64)`;
  - membership-only duplicate key: no resident-byte change (the semi/anti key-only path stores no refs);
  - clear/reset/repartition: reset or recompute explicitly (per the census).
- `guard_resident_state` reads `self.resident_index_bytes` instead of calling the fold. Keep the fold functions
  available for the equivalence tests (or as `#[cfg(test)]` reference).
- Preserve the budget producers/names and caps. Topology-named; no qNN in core.

## D2 - third fold (only if Phase 0 says hot)
- If `join_spill_index_bytes` is hot per-mutation: maintain a counter for its index too, or land it as a named sibling
  cut first. If cold: leave it, note out-of-scope in results.

## Tests / safety net (build through the REAL APIs, never construct maps directly)
- **new key adds base cost once:** insert one key via the real write/index API -> counter == fold == `64 +
  key_heap_bytes + 1*ref_size`.
- **duplicate key with stored refs adds only ref cost:** append a second ref to an existing key -> counter grows by
  exactly `ref_size` (no second base cost).
- **duplicate membership-only key adds zero:** insert a duplicate through `write_membership_key_chunks` /
  `write_projected_membership_key_chunks` -> counter is unchanged because the existing fold stores an empty ref vector.
- **multi-batch/page equality:** after **each** write/index step over several batches, `counter == old_fold(index)`.
- **guard-decision equivalence:** at under / equal / over cap, `guard_resident_state` returns the same decision as the
  fold-based guard.
- **reset/clear/drop/repartition:** the counter has no stale carryover (absent or explicitly reset), verified through
  the real path.
- Run for **both** builds (inner-join + semi/anti-existence).

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch
# re-profile (373 method), per cell, inside systemd-run --scope, debug symbols:
#   systemd-run --user --scope ... perf record -F 99 --call-graph dwarf -D 60000 -o ...q04.perf.data -- \
#     timeout --signal=INT 120s target/release/toron-tpch perf --sf 10 --query q04 --data-source parquet ...
#   render + confirm the resident-accounting stack is gone; record the next hot stack
git diff --check
#   census recorded; counters == fold (real-API tests); guard decisions unchanged; third fold resolved; re-profile
#   shows accounting gone + next stack; no cap bump; --no-fail-fast green; no qNN
```

## Results doc
`crates/toron/plans/impl/374_incremental_spill_resident_accounting_results.md`:
- the Phase-0 mutation-site census + `join_spill_index_bytes` in/out-of-scope verdict;
- the counter maintenance points;
- equivalence + guard-decision + reset test results (built through real APIs);
- **before/after re-profile** for q04/q07/q17/q21: the resident-accounting stack share before, and the next hot stack
  after (the next named perf cut);
- whether any cell greened (incidental, not the success criterion);
- confirmation no cap bump, budget producers unchanged, `--no-fail-fast` green, no qNN in core.

## Risk / blast radius
- **Missed mutation site (drift)** - census + per-step real-API equivalence; drift => wrong budget decision.
- **Direct-construction test blind spot** - tests mandated through real write/index APIs.
- **Stale counter after reset/repartition** - explicit reset test.
- **Third fold left hot** - Phase 0 resolves in/out-of-scope.
- **Regression** - `--no-fail-fast` required.

## Seal
`impl: adr 374 incremental spill resident accounting`. Do not commit wip drafts or large `.perf.data`.
