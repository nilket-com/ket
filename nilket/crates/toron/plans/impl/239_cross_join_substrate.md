# Impl Plan — Toron ADR 239: CROSS JOIN Substrate

Pairs with `plans/adr/239_cross_join_substrate.md`. A new `JoinKind::Cross` on the existing `Join`
node + an explicit keyless match-all execution path that rides the broadcast join. Lifts the CROSS
JOIN + comma-join fences. **No new physical operator; no optimizer/durable/Wendelon/Rune surface.**

## Touch surface

- `ir.rs` — add `JoinKind::Cross`; add `Cross` arms to every exhaustive `JoinKind` match
  (compile-driven — the 235/237 lesson).
- `sql_planner.rs` — `lower_join_operator`: `JoinOperator::CrossJoin(_)` → `(JoinKind::Cross, no ON)`;
  lift `from.len() > 1` (3536) by folding the FROM list into a left-deep `Cross` chain. Keep
  APPLY/natural/USING/non-equi rejected; `WHERE` lowers after the cartesian.
- `binder.rs` / `binder/join.rs` — bind `Cross`: empty keys, output = inner concat schema; auto
  strategy → `JoinStrategy::Broadcast` with deterministic SQL right-build represented by
  `SwappedRightBuild`; reuse `restore_declared_join_order_project` so physical swap does not change
  SQL-visible column order; the binder-side broadcast shape guard accepts `Cross`; oversized/ineligible
  build → explicit error.
- `distributed/fragment.rs` — `safe_broadcast_physical_join_kind` accepts `Cross`; Cross fragments via
  `fragment_broadcast_join_plan` (build → single-partition exchange, probe distributed).
- `exec.rs` — explicit match-all path for `JoinKind::Cross` (`JoinKeyPlan::Cross` / pre-key branch)
  emitting every build×probe pair; in-memory and spilled produce the same cartesian.
- `coordinator.rs` / `server/mod.rs` / `audit.rs` / walkers — `Cross` arms where `JoinKind` is matched.
- `tests/{cross_join_test or group_by_test}.rs` + `tests/distributed_process_test.rs` +
  `tests/architecture/*`.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| JoinKind enum | `pub enum JoinKind` (ir.rs) |
| CROSS JOIN fence | `lower_join_operator` (sql_planner.rs:3837) |
| Comma-join fence (separate) | `from.len() > 1` (sql_planner.rs:3536) |
| Accidental empty-key match-all (do NOT rely on for distributed) | `join_key_bytes` (exec.rs) |
| Join executor (add match-all) | `execute_hash_join` (exec.rs:2506), `JoinKeyPlan` (:2800) |
| Broadcast fragment (reuse) | `fragment_broadcast_join_plan` (fragment.rs:767) |
| Broadcast Inner-only guards (retarget BOTH) | `safe_broadcast_physical_join_kind` (fragment.rs:84) + binder-side broadcast shape validation |
| Broadcast strategy decision | `choose_auto_join_strategy` (binder/join.rs) |
| Cross-producer discipline | `assert_cross_producer_cardinal_is_value_discriminating` (236) |

## Phase 0 — Red discriminators

`cross_join_produces_cartesian_product` (3×4=12, dup values); `n_ary_comma_join_folds_left_deep_cross`
(2×3×4=24); `distributed_cross_join_cartesian_is_value_discriminating`. Red on `main` (fences reject).

## Phase 1 — `JoinKind::Cross` + lowering

Add `JoinKind::Cross`; lower `CROSS JOIN` and comma joins (left-deep fold) to it with `on: []`; add
`Cross` arms to all exhaustive `JoinKind` matches. Keep the rejected variants.

## Phase 2 — Match-all execution

Explicit match-all path (`JoinKeyPlan::Cross` / branch) emitting every build×probe pair without
hashing; in-memory and spilled agree; budget the output size (no unbounded allocation).

## Phase 3 — Broadcast-only distributed

`safe_broadcast_physical_join_kind` + the binder broadcast shape guard accept `Cross`;
`choose_auto_join_strategy` forces `Broadcast` for `Cross` (deterministic SQL right-build via
`SwappedRightBuild` + declared-order restore); oversized/ineligible build → explicit error. Fragment via
`fragment_broadcast_join_plan`. Assert no
`HashPartitioned`/empty-key-exchange cross join.

## Phase 4 — Behavior coverage

- Cartesian (3×4=12, dup values → multiplicity); comma join; n-ary comma (2×3×4=24); `WHERE` after
  cartesian; column order left-then-right **after `SwappedRightBuild` restoration**; reject
  APPLY/natural/USING/non-equi.
- **Distributed:** a cartesian whose rows require the broadcast build side + both probe workers
  (236/238 discipline, via the helper; value single-producer-unreachable); oversized build fails loud.

## Phase 5 — Guards

Add the eight `adr239_*` guards. Retarget the CROSS/comma fences and **both** broadcast Inner-only
guards (binder + fragmenter) to "inner or cross". Assert 235 join guards reused; no new physical
operator (Cross rides `ExecutionPlan::Join`).

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test cross_join_test   # (or join_kind_test / group_by_test if folded)
scripts/cargo-cgroup test  -p toron --test distributed_process_test cross_join
scripts/cargo-cgroup test  -p toron --test architecture_test adr239
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL
scripts/cargo-cgroup test  -p toron --test distributed_process_test   # FULL (broadcast path)
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Not a cartesian — `cross_join_produces_cartesian_product` ≠ 12 / loses duplicates.
2. Comma / n-ary not cartesian — `comma_join_*` / `n_ary_comma_join_folds_left_deep_cross` fail.
3. Empty-key hash-partition instead of broadcast — `..._does_not_hash_partition_empty_keys` fails.
4. Accidental (non-explicit) match-all — `..._match_all_path_is_explicit` fails.
5. Column order drifts under broadcast — `..._preserves_sql_column_order_under_broadcast` fails.
6. Oversized build silently single-tasks — `cross_join_oversized_build_fails_loud` fails.
7. Distributed cartesian wrong across workers — the distributed gate fails (rows must require the
   broadcast build + both probe workers; 236/238 discipline).
8. A new physical operator instead of `JoinKind::Cross` — `..._without_new_physical_operator` fails.

## Hygiene

- No m-word; no version-suffix idiom; new join *kind*, not a new operator; goldens limited to
  cartesian result rows.
- `--all-targets` (new `JoinKind` variant → exhaustive matches everywhere); cargo via
  `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Confirm BOTH broadcast Inner-only gates (`safe_broadcast_physical_join_kind` + the binder shape
   validation) accept `Cross` cleanly.
2. Confirm `choose_auto_join_strategy` forces Broadcast for `Cross` with no facts (SQL right-build via
   `SwappedRightBuild`) and never falls back to empty-key hash-partition.
3. Confirm output column order is left-then-right after the declared-order restoration projection; a
   raw physical right-build swap must not leak right-then-left columns.
4. Confirm comma-join fold and that `FROM a, b WHERE a.k = b.k` stays cartesian + filter (no equi
   extraction).
5. Confirm the distributed cartesian fixture requires the broadcast build + both probe workers.
6. Confirm oversized/ineligible build fails loud; in-memory and spilled match-all agree.
