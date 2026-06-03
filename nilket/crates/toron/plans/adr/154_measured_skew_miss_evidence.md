# ADR 154: Measured Skew Miss Evidence

**Status:** Promoted. **Genuine 3-of-3 (Claude+Codex+Gemini); two minor Gemini
fixture divergences resolved on grounding.** A **Toron-local evidence cut**, the
disciplined resolution of a gate the council has deferred for ~13 ADRs (*"measured-skew
is gated on a measured miss we don't have"*). ADRs 141/148 spend NDV facts for equality
and positive `IN` filters under a **uniform** assumption (`ceil(base_rows / NDV)`,
`min(base_rows, k·ceil(base_rows/NDV))`); a hot key can make that under-count → a cheap-
looking `BroadcastEstimated` that trips `BroadcastReplication` at runtime. ADR 154 turns
that standing theoretical gap into a **checked-in measured artifact** and a **precise
next-fact contract** — it changes **no** selectivity, chooser, replay, or Wendelon
behavior. **The next ADR is the producer; this one measures and specifies it.**

## Grounded current state

- Uniform estimate: `equality_ndv_estimated_row_count` (`binder.rs:3998`),
  `in_list_ndv_estimated_row_count` (`:4041`), `estimated_rows_for_ndv` (`:4683`) →
  `facts.estimated_row_count` (observable; asserted in many tests `:10534+`). NDV rides
  `ExecutionPlan::Scan { column_ndv }` (`:4147`).
- Chooser: `JoinStrategyDecisionReason::BroadcastEstimated` (`:465`).
- Over-cap → `BudgetDimension::BroadcastReplication` (`distributed/budget.rs:63`) →
  139/140 replay (`coordinator.rs broadcast_cap_replay_trigger:1350`,
  `force_hash_join_ids`). An **existing generic test** already proves NDV-underestimate→
  replay: `ndv_underestimate_over_cap_replays_as_hash` (`lib.rs:961`).
- The join bench (`benches/distributed_inner_equi_join.rs`) has **no skew knob**.

## Decisions

**D1 — Evidence-generation cut, not a planner fix.** The uniform NDV model, the chooser,
the hard/estimate channel split, Wendelon facts, and 139/140 replay are **untouched**.
The output is evidence: a deterministic miss measurement, a broadcast/replay consequence
when the cap is crossed, the honest absorbed case, and a documented fact shape for ADR 155.

**D2 — Scope: the current uniform NDV consumers only.** Positive single-column equality
and positive single-column `IN` over a `PlanOrigin::SingleBaseRelation` with a hard
`row_upper_bound` and a column NDV — exactly `filter_equality_ndv_estimated_row_count` /
`in_list_ndv_estimated_row_count` / `estimated_rows_for_ndv`. Ranges, joins, `OR`,
`NOT IN`, multi-column correlation, range-uniformity misses: out (Codex's grounding).

**D3 — Separate raw estimate error from decision-boundary harm; be honest about
"absorbed."** Two distinct claims, measured separately (Claude's framing, 3-of-3):
- **(a) raw miss** — the binder's uniform estimate (`facts.estimated_row_count`,
  `= ceil(base_rows/NDV)`) is materially smaller than the fixture's **actual** selected
  hot-key rows. Always measurable; proves the model is wrong for the constructed skew.
- **(b) decision-boundary crossing** — that under-estimate produces an auto
  `BroadcastEstimated` build whose *actual* bytes exceed the runtime cap → replay. The
  miss **only matters at (b)**: 139/140 give ~64× headroom by design, so a miss *within*
  headroom is **absorbed** (broadcast completes; no harm). The cut reports truthfully —
  including *"wrong by Nx but within headroom, does not currently change a decision,"*
  which **refutes urgency** and is a legitimate, gate-resolving finding.

**D4 — Deterministic hot-key fixture, direct `LogicalPlan`.** Build-side dim with seeded
`column_ndv = D` (the optimizer believes uniform → estimate `ceil(N/D)`) but a **hot
literal** occurring `H ≫ ceil(N/D)` times; remaining rows over the other `D−1` keys.
**RNG-free / seeded-by-index** so estimate, actual, and replay are exact in CI. **Use a
direct `LogicalPlan` to place the filter on the build side** (as ADR 141 did) — *not* a
SQL `WHERE` over a join, which only lands on the build side if 143 pushdown fires for that
exact all-inner shape (Gemini's iter-1 SQL fixture is that fragile shape; the direct plan
is deterministic). Tests assert a *ratio threshold*, not mere inequality, so the evidence
can't be weakened accidentally.

**D5 — Demonstrate the consequence through existing replay (extend, don't replace).** A
gate puts the skewed filtered dim on the auto-selected build side, sized so the actual
build crosses the cap while `ceil(N/D)` sits under it: assert `BroadcastEstimated` →
`BudgetExhausted(BroadcastReplication)` → 139/140 hash replay with the forced-hash digest.
**Keep** `ndv_underestimate_over_cap_replays_as_hash` (`lib.rs:961`); 154 adds the *skew
distribution + estimate-vs-actual* evidence on top, not a replacement.

**D6 — Include the wrong-but-absorbed gate.** The same skew sized *under* the cap
completes as `BroadcastEstimated` without replay. Makes the ADR decision-resolving rather
than assuming every raw miss is harmful (Claude+Codex; Gemini omitted it — promoted in).

**D7 — Name the next fact shape without implementing it (the forward deliverable).** An
**inert, not-wired** contract for ADR 155:
> `HeavyHitterFact { column_ordinal, entries: Vec<{ value_key, frequency: u64 }>,
>   exactness: Exact | Approx{rel_err_ppm}, coverage }` — per-column top-k by count,
>   keyed by a future serializable `value_key` with the same canonical scalar semantics
>   as ADR 148's binder-local `InListLiteralKey` (not that private helper type itself).
>   A filtered literal that **is** a tracked hot key estimates `frequency`; an untracked
>   literal uses the uniform tail `(base_rows − Σ frequency)/(D − k)` (checked
>   zero/coverage). Exact top-k is the simplest first consumer; Count-Min acceptable
>   only if its error bound is exposed so Toron never under-counts a hot literal.
>   All-contributors coverage fence (142). Estimate-channel only. **A contract, not a
>   live fact in 154.**

**D8 — No public surface; fences intact.** Measurement is **observational via existing
`facts.estimated_row_count`** — **no** new public EXPLAIN filter row (`optimizer_explain_schema`
`:5622` stays join-decision-only). NDV stays an estimate (no hard bound, no `byte_upper_bound`);
hard-byte candidates still beat estimated-byte; replay keys only on `BroadcastReplication`.
No Wendelon/fact-schema/producer/wire change.

## Headline gates (Codex's split)

- `skewed_hot_key_filter_records_uniform_ndv_miss` — (a): seeded `column_ndv = D`, hot-key
  equality filter, `facts.estimated_row_count == ceil(N/D)` while actual hot rows ≫ that;
  the ratio is asserted.
- `uniform_ndv_hot_key_miss_triggers_broadcast_replay` — (b): the same miss sized to cross
  the cap → `BroadcastEstimated` → `BudgetExhausted(BroadcastReplication)` → replay digest.

## Supporting gates

- `uniform_ndv_hot_key_in_list_miss_is_measured` (the 148 `IN` analogue);
- `uniform_ndv_miss_within_headroom_is_absorbed_no_replay` (D6 honesty);
- `skew_miss_measurement_does_not_change_estimated_row_count` (observational, not a fix);
- `non_ndv_filters_do_not_emit_skew_evidence` (D2 scope);
- architecture: `adr154_uniform_ndv_math_stays_current_behavior` (equality/`IN` still route
  through `estimated_rows_for_ndv`, no heavy-hitter branch); `adr154_no_live_heavy_hitter_or_count_min_producer`;
  `adr154_no_public_filter_explain_surface` (`optimizer_explain_schema` unchanged);
  `adr154_estimate_channel_and_replay_fences_untouched`.

## Consensus trail

Genuine 3-of-3 on the topic, the checked-in-test-not-benchmark deliverable (Codex), the
two-claim split + wrong-but-absorbed honesty (Claude — adopted by Codex), and the
HeavyHitter/Top-K fact contract for ADR 155 (Codex + Gemini). **Resolved Gemini fixture
divergences:** (1) the build-side filter uses a **direct `LogicalPlan`**, not SQL `WHERE`
over a join (the 141/143 pushdown-fragility trap Codex flagged); (2) the **wrong-but-
absorbed gate is included** (Gemini omitted it). The fact-shape contract uses
ADR148-compatible canonical literal-key semantics (Codex's grounding — `ScalarValue` isn't
hashable and the current `InListLiteralKey` is binder-local) over Gemini's
`BTreeMap<ScalarValue,u64>`.

## Forward arc

`141 equality NDV → 148 IN-list NDV → 154 measured skew miss evidence → (if (b) fires) 155
heavy-hitter/top-k producer + consumer [builds to D7 contract] → equality/IN selectivity
correction → KLL/quantile range evidence → Theta join-key intersection → join selectivity
→ reordering` · *(parallel breadth track: CONVERT/units)*.
