# Impl Plan - ADR 190: Operator-Applied Configurable Soft Margin

Pairs with `plans/adr/0190_operator_applied_configurable_soft_margin.md`.

Promote the soft margin from a const to a bounded `PlannerConfig` ladder step (default 4/5),
thread the resolved margin into every join-order classifier consumer, record the applied margin in
ranking/evidence receipts (render has no config), and reflect it in the 189 advisory + explain
detail.
Behavior-neutral by default; operator-applied (reproducible); no executor/storage/Wendelon change.

## Anchors

| Concern | Anchor |
|---|---|
| Margin const + gate | `STAR_ORDER_SOFT_MARGIN_NUMERATOR/DENOMINATOR`, `classify_star_order_action` (`binder/join_order.rs:5,1056`) |
| Decision chain (thread here) | `classify_star_order_action` <- `rank_star_order` (`:940`) <- star actuation / observed `analyze*` / verdict re-rank |
| Verdict re-rank (reads captured margin) | `measured_order_rerank` -> `rank_star_order` (`:1078`) |
| Margin types (189, reuse) | `SoftMarginLadderStep`, `SoftMargin`, `CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP` (`binder/join_order.rs:12,55,59`) |
| Ranking/evidence (add field) | `StarOrderRanking`, `StarOrderEvidence`, `SnowflakeOrderEvidence` (`binder/join_order.rs`) |
| Render detail (show applied) | explain verdict detail using the consts (`binder/explain.rs:1156,1222`) |
| Config | `PlannerConfig` (`binder.rs`) |
| Advisory (189) | `recommend_soft_margin(snapshot, current)` |
| Existing guard to retarget | `adr189_advisory_does_not_change_margin_or_decision` |
| Tests | `binder/tests.rs`; `tests/architecture/optimizer_accrual.rs` |

## Phase 1 - Config field (`binder.rs`)

- Make the ladder step usable in a public config surface: either make/re-export
  `SoftMarginLadderStep` publicly, or introduce a public alias such as `JoinOrderSoftMarginStep`.
  Keep raw `SoftMargin` fields private.
- Add `pub join_order_soft_margin_step: SoftMarginLadderStep` to `PlannerConfig`, `Default` =
  `CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP` (4/5).
- **Run `cargo check -p toron --all-targets`** (new field breadth - literal `PlannerConfig { .. }`
  in benches/tests need the field or `..Default::default()`).

## Phase 2 - Thread the margin into the decision (`binder/join_order.rs`)

- `classify_star_order_action(declared_cost, preferred_cost, orders_agree, confidence, margin:
  SoftMargin)` - apply `margin.numerator/denominator` instead of the consts.
- `rank_star_order(probe, dimensions, confidence, margin_step)` - derive the private `SoftMargin`
  inside `join_order.rs`, pass it to the classifier, and store it on the returned ranking.
- `recognized_star_order_outcome(..., margin_step)` - pass through (~5 call sites).
- The actuation path reads `self.config.join_order_soft_margin_step` and passes the step down.
- `analyze(edges, margin_step)`, `analyze_snowflake(edges, decisions, margin_step)`, and recursive
  `snowflake_order_level(..., margin_step)` use the configured margin for explain-only
  observation. `bind_with_explain_trace` passes `config.join_order_soft_margin_step` into those
  observed paths.

## Phase 3 - Record + reuse the applied margin (`binder/join_order.rs`, `binder/explain.rs`)

- Add `applied_margin: SoftMargin` to `StarOrderRanking`.
- Add `applied_margin: SoftMargin` to `StarOrderEvidence`; set/copy it from the ranking in
  `recognized_star_order_outcome` and `star_order_evidence_from_ranking`.
- `measured_order_rerank`: pass `ranking.applied_margin` to `rank_star_order` (verdict uses the
  same margin the decision used).
- Explain detail (`explain.rs:1156,1222`): render `evidence.applied_margin` for flat star and
  `evidence.ranking.applied_margin` for snowflake (its `label()`, e.g. "4/5") instead of the
  consts.
- Tests constructing `StarOrderRanking`, `StarOrderEvidence`, or `SnowflakeOrderEvidence` literally
  set `applied_margin` (default-step helper).

## Phase 4 - Advisory reflects applied (`binder.rs`)

- The `information_schema.optimizer_margin_advisory` bind passes
  `config.join_order_soft_margin_step` as `current` to `recommend_soft_margin`; `current_margin` =
  the applied value, `recommended_margin` relative to it.

## Phase 5 - Behavioral tests (`binder/tests.rs`)

- `operator_set_margin_changes_reorder_threshold` - a borderline star: default 4/5 reorders;
  config 3/5 keeps declared; config 9/10 reorders. (Pick costs so 4/5 and 9/10 fire but 3/5
  does not.)
- `default_margin_is_behavior_neutral` - default config produces `ExecutionPlan` + actions byte-identical
  to a baseline using the const value.
- `applied_margin_recorded_and_displayed` - the receipt's `applied_margin` matches config; explain
  detail shows it; the verdict re-rank uses it.
- `operator_margin_bounded_to_ladder` - the field type is `SoftMarginLadderStep`; off-ladder is not
  constructible (type-level).
- `verdict_uses_applied_margin` - decide under 3/5, verify the verdict re-rank reflects 3/5 (e.g. a
  win that would clear 4/5 but not 3/5 yields a different verdict than under default).
- `advisory_current_is_applied_margin` - set config to 9/10; advisory `current_margin` = "9/10".
- `observed_snowflake_uses_configured_margin` - set config to 3/5 or 9/10; explain-only snowflake
  evidence renders that margin and classifies with that threshold through `StarOrderRanking`.
- `rune_respects_operator_margin` - Rune honors the configured margin like SQL.

## Phase 6 - Architecture guards (`tests/architecture/optimizer_accrual.rs`)

- Retarget `adr189_advisory_does_not_change_margin_or_decision`: stop asserting the classifier
  directly reads `STAR_ORDER_SOFT_MARGIN_*`; keep the 189 invariant that the advisory function is
  read-only and not referenced by classifier/accrual paths.
- `adr190_margin_is_config_not_const` - `classify_star_order_action`/`rank_star_order` take a
  margin param/step; their bodies do not reference `STAR_ORDER_SOFT_MARGIN_*` (only the
  `Default`/baseline definition does).
- `adr190_applied_margin_recorded_in_ranking_and_evidence` - `StarOrderRanking` and
  `StarOrderEvidence` have `applied_margin`; verdict re-rank and explain detail read it.
- `adr190_observed_paths_receive_configured_margin` - `bind_with_explain_trace` passes
  `config.join_order_soft_margin_step` into both `analyze` and `analyze_snowflake`.
- `adr190_advisory_current_is_configured_margin` - advisory bind passes
  `config.join_order_soft_margin_step`, not `CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP`.
- `adr190_margin_bounded_to_ladder` - `PlannerConfig.join_order_soft_margin_step:
  SoftMarginLadderStep` (or public alias), with no public raw ratio fields.
- `adr190_operator_applied_not_auto` - no accrual/verdict path writes
  `join_order_soft_margin_step` (it is set only from config construction).
- `adr190_no_executor_storage_or_wendelon_change` - marker sweep.
- `adr190_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets        # PlannerConfig + ranking/evidence field breadth
scripts/cargo-cgroup test -p toron --lib operator_set_margin join_order_soft_margin applied_margin
scripts/cargo-cgroup test -p toron --lib star_order snowflake hash_snowflake   # 177-186 byte-identical (default)
scripts/cargo-cgroup test -p toron --lib optimizer_decision optimizer_verdict margin_advisory  # 187-189 unchanged
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --test architecture_test adr190
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

## Risks

- **Public config type.** `PlannerConfig` is public; the step type must be public/re-exported. Do
  not expose raw margin ratios.
- **New fields (PlannerConfig + StarOrderRanking + StarOrderEvidence) breadth.** These break
  literal constructions (E0063, `--all-targets`). Default the config field; give a default-step
  helper for ranking/evidence literals in tests. Run `--all-targets`.
- **Behavior-neutral default.** Default step must equal the const value exactly (4/5) so 177-189 is
  byte-identical. Pin with `default_margin_is_behavior_neutral` + green star/snowflake/187-189.
- **Decision/verdict/display consistency.** The verdict re-rank and explain detail must read the
  *receipt's* applied margin, not the const. Pin with `applied_margin_recorded_and_displayed` +
  `verdict_uses_applied_margin`.
- **Snowflake drift.** Snowflake observations store `StarOrderRanking` directly, so ranking must
  carry the margin too. Pin with `observed_snowflake_uses_configured_margin`.
- **Reproducible / not auto.** The margin is set only from config; nothing self-modifies it. Pin
  with `adr190_operator_applied_not_auto`.
- **Bounded.** Off-ladder margins must be unrepresentable (type-level). Pin with
  `operator_margin_bounded_to_ladder`.

## Deferred

Auto-apply self-tuning (191); per-kind margins; off-ladder margins; broadcast cliff; mixed-strategy
cost model; galaxy/bridge.
