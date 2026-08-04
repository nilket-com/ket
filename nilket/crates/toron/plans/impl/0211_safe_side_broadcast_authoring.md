# Impl Plan - ADR 211: Safe-Side Broadcast Authoring

Pairs with `plans/adr/0211_safe_side_broadcast_authoring.md`.

## Anchors

| Concern | Anchor |
|---|---|
| Pinned intent shape | `JoinStrategyIntent::Pinned { strategy, source }` ir.rs:890 |
| Existing build-side enum | `BroadcastBuildSide` binder/join.rs:16 |
| Safe matrix | `broadcast_physical_placement` binder/join.rs (209) |
| Auto broadcast physicalization | `bind_scoped_join` binder.rs:1994; `decision.placement` swap/restore at :2114-2160 |
| Pinned non-inner blanket reject | the 209-D7 gate in `bind_scoped_join` (`Pinned { strategy: Broadcast, .. } && kind != Inner`) |
| Frontend pre-shaping | `build_join_plan` broadcast arm adds left `Exchange { partition_count: 1 }` sql_planner.rs:5096 |
| SQL hint gate | `join_strategy_intent_for_hint` sql_planner.rs:7416 - right-target reject :7441, inner-only reject :7453 |
| Rune broadcast surface | `broadcast_join` rune_engine.rs:257 hardcodes `JoinKind::Inner`; kind constructors (:284+) emit `Auto` |
| Replay and force-hash wrinkles | `broadcast_replay_eligibility` binder/join.rs:414; `force_demoted_sql_broadcast` + `strip_root_exchange` binder.rs:2114-2123 (the D2 special case to retire) |
| Author-pin receipt (exists - no new variant) | `JoinStrategyDecisionReason::PinnedByAuthor` binder/join.rs:31, emitted :447, rendered binder/explain.rs:2030 |
| Guard home | `tests/architecture/outer_join_completion.rs`, `optimizer.rs`, and existing SQL/Rune architecture modules if line ceilings require split placement |

All anchors verified at promotion review (2026-06-12).

## Phase 0 - Red Tests

Add failing discriminators before implementation:

1. SQL planner/binder:
   - `sql_left_outer_broadcast_hint_on_null_supplied_side_binds_preserved_probe_broadcast`
   - `sql_left_outer_broadcast_hint_on_preserved_side_rejects_loudly`
   - `sql_right_outer_broadcast_hint_on_null_supplied_side_binds_preserved_probe_broadcast`
   - `sql_full_outer_broadcast_hint_rejects_without_demotion`
   - `sql_full_outer_broadcast_hint_is_honored_after_both_sides_null_rejecting_demote`
   - `sql_inner_broadcast_hint_can_target_either_immediate_side`
2. Direct logical plan:
   - `direct_pinned_non_inner_safe_side_binds`
   - `direct_pinned_non_inner_wrong_side_rejects`
3. Rune:
   - `rune_post_join_broadcast_pin_matches_sql_hint`
   - `rune_preserved_side_broadcast_rejects_like_sql`
4. Runtime/evidence:
   - `safe_side_hinted_outer_join_distributed_result_matches_hash`
   - `hinted_safe_side_outer_join_explain_reports_pinned_by_author`
   - `force_hash_join_ids_still_demotes_sql_hint_without_frontend_exchange_strip`

Expected initial failures:

- SQL rejects right-side hints and non-inner hints in `join_strategy_intent_for_hint` / `build_join_plan`.
- Rune has no post-join broadcast pin annotation.
- Direct pinned intent cannot express build side.

## Phase 1 - Move Build-Side Identity Into IR

1. Move `BroadcastBuildSide` out of `binder/join.rs` into `ir.rs` beside `JoinStrategyIntent`.
2. Extend pinned intent:

```rust
pub enum JoinStrategyIntent {
    Auto,
    Pinned {
        strategy: JoinStrategy,
        source: JoinStrategyPinSource,
        build_side: Option<BroadcastBuildSide>,
    },
}
```

Rules:

- `Some(_)` is required for `Pinned Broadcast`.
- `None` is required for `Pinned HashPartitioned`.
- No version suffixes or compatibility envelope.

Update all pattern matches mechanically first, keeping current behavior until binder validation changes:

- `broadcast_replay_eligibility`
- `choose_join_strategy`
- `force_hash_join_ids` checks
- tests constructing direct pinned plans
- SQL/Rune `build_join_plan` call sites

Gate: `scripts/cargo-cgroup check -p toron --all-targets`.

## Phase 2 - Binder Owns Pinned Broadcast Physicalization

1. Add a helper in `binder/join.rs`, for example:

```rust
fn pinned_broadcast_placement(kind: JoinKind, build_side: BroadcastBuildSide)
    -> Result<JoinPhysicalPlacement, BindingError>
```

It must use only `broadcast_physical_placement(kind)`:

- `SmallerBuild`: left -> `Natural`, right -> `SwappedRightBuild`.
- `NaturalLeftBuild`: left -> `Natural`, right -> reject.
- `SwappedRightBuild`: right -> `SwappedRightBuild`, left -> reject.
- `Never`: reject.

2. Change `choose_join_strategy` so pinned broadcast returns `JoinStrategy::Broadcast`,
   the validated placement, and `PinnedByAuthor`. SQL force-hash still maps SQL-hint pins to hash when the
   replay id is forced.
3. In `bind_scoped_join`, remove the blanket `Pinned Broadcast && kind != Inner` reject.
4. Apply the exchange/swap/restore physicalization branch for both auto broadcast and pinned broadcast
   decisions. Do not rely on frontend-shaped exchanges.
5. Delete or retire `strip_root_exchange` use for forced SQL broadcast demotion once frontends stop
   pre-shaping broadcast pins.
6. Keep `validate_strategy_shape` as the final physical-shape assertion.

Failure mode to guard: a direct logical plan with `Pinned Broadcast build_side=Left` on `LeftOuter` must
reject before any physical plan can leak through.

## Phase 3 - Frontend Lowering Stops Preshaping Broadcast Pins

SQL:

1. Refactor `build_join_plan` so `JoinStrategy::Broadcast` no longer injects a left single-partition
   exchange. It should derive join keys and emit a logical `Join` with pinned intent carrying build side.
2. Update `join_strategy_intent_for_hint`:
   - resolve target alias against immediate left/right input qualifiers.
   - reject ambiguous or composite-side matches loudly.
   - no kind-only rejection here; emit the side and let binder validate effective kind after demotion.
   - allow inner hints to target either side.
3. Retarget `sql_broadcast_hint_targeting_probe_rejects`,
   `sql_broadcast_hint_non_inner_rejects`, and `build_join_plan_rejects_broadcast_non_inner_at_frontend`.

Rune:

1. Keep `broadcast_join` as current inner left-build shorthand.
2. Add a post-join annotation method, name final after checking Rune API vocabulary, for example:

```rust
df.left_join(dim, on).broadcast_build(BroadcastBuildSide::Right)
```

3. The method pattern-matches the receiver as a just-built logical `Join` and replaces `Auto` with
   `Pinned { strategy: Broadcast, source: Direct, build_side: Some(side) }`. Reject non-join receivers and
   already-pinned joins with a clear Rune error.
4. Route the annotation through the same logical pinned intent builder as SQL.
5. Document in tests that a preserved-side authoring attempt is rejected by binder validation, not by a
   Rune-only policy branch.

## Phase 4 - Demotion Interaction

ADR 209/210 demotion runs before binding. Preserve pinned intent through the normalizer and validate it
against the effective kind:

- `FullOuter` + both-side null rejection + `BROADCAST(c)` can demote to `Inner` and honor right-build.
- true `FullOuter` + `BROADCAST(c)` rejects with the safe-placement message.
- `LeftOuter` + null-rejecting right filter demotes to `Inner`; either side pin valid for inner, but the
  chosen side still comes from the author target.

Pin this with SQL tests and, if cheap, one EXPLAIN row that shows both the outer demotion receipt and the
`PinnedByAuthor` join decision.

## Phase 5 - Architecture Guards

Add ADR211 guards, preferably in `tests/architecture/outer_join_completion.rs` unless the line ceiling
requires a small companion module:

- `adr211_pinned_broadcast_intent_carries_build_side`
- `adr211_frontends_do_not_preshape_pinned_broadcast`
- `adr211_sql_hint_resolves_immediate_safe_side`
- `adr211_binder_validates_author_pins_through_209_placement_table`
- `adr211_preserved_side_and_full_outer_pins_reject_loudly`
- `adr211_rune_post_join_pin_shares_sql_admission_path`
- `adr211_no_fragmenter_executor_wire_runtime_filter_or_recognizer_widening`

Retargets:

- ADR209 author-pinned non-inner guard: from blanket reject to table-validated safe-side admission.
- SQL planner right-side/non-inner hint guards: from rejection to side-sensitive behavior.
- Any ADR125/ADR128 authoring guard that pins "broadcast hints are inner-only."
- ADR176 later-ADR allowlist only if the touched-file set trips the dirty-tree architecture guard.

## Verification Gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib broadcast_hint
scripts/cargo-cgroup test  -p toron --lib safe_side
scripts/cargo-cgroup test  -p toron --lib pinned
scripts/cargo-cgroup test  -p toron --lib rune_safe_side
scripts/cargo-cgroup test  -p toron --test architecture_test adr211
scripts/cargo-cgroup test  -p toron --test architecture_test adr209
scripts/cargo-cgroup test  -p toron --test architecture_test adr210
scripts/cargo-cgroup test  -p toron --test architecture_test          # full binary
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

If a live distributed/hash A/B fixture is added outside `--lib`, run that named integration test explicitly
and include it in the evidence.

## Post-Implementation Evidence Notes

Promoted gate names that landed under consolidated implementation names:

| Promoted behavior | Implemented proof |
|---|---|
| `force_hash_join_ids_still_demotes_sql_hint_without_frontend_exchange_strip` | `force_hash_join_ids_demotes_auto_and_sql_broadcast_only`, plus terminal-exchange assertions in the same test proving the post-D2 shape |
| `rune_post_join_broadcast_pin_matches_sql_hint` | `rune_post_join_broadcast_pin_matches_sql_hint_and_rejects_preserved_side` |
| `rune_preserved_side_broadcast_rejects_like_sql` | `rune_post_join_broadcast_pin_matches_sql_hint_and_rejects_preserved_side` |

The inner either-side rule is covered compositionally rather than by a single end-to-end gate:

- SQL resolver proof: `sql_broadcast_hint_targeting_probe_rejects` was retargeted to prove
  `BROADCAST(o)` on the right side of an inner join lowers to `build_side: Some(Right)`.
- Binder matrix proof: `bind_join_applies_broadcast_side_matrix_for_non_inner_kind` and
  `direct_pinned_non_inner_safe_side_binds` prove side-sensitive admission/physicalization through the
  binder's table. Together with the SQL resolver proof, this covers inner either-side authoring.

## Hygiene

- No new fragmenter, executor, wire, runtime-filter, telemetry, or recognizer behavior.
- No new strategy variant; `JoinStrategy` remains `HashPartitioned | Broadcast`.
- No version suffixes.
- Keep rejection messages descriptive but stable enough for tests.
- Avoid module line-ceiling regressions in architecture tests.
- Re-run full architecture after `cargo fmt`; several guards inspect formatted source.
