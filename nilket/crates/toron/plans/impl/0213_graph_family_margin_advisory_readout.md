# Impl Plan — ADR 213: Graph-Family Margin Advisory Readout

Pairs with `plans/adr/0213_graph_family_margin_advisory_readout.md`. A consumer/report cut:
`binder/join_order.rs` (scope helpers + scope-keyed report builder), `binder/explain.rs` (schema/rows),
tests, guards. No evidence-store, PlannerConfig, SystemTableKind, or application-path changes.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Family-neutral math (untouched) | `recommend_soft_margin` join_order.rs:103-165; `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE = 20` :7 |
| 208 scope boundary (factors) | `is_tree_margin_advisory_kind` + `tree_margin_advisory_input` |
| Report builder (generalizes) | `margin_advisory_report` + `MarginAdvisoryReport { …, evidence_scope, excluded_sample_size }` :242-260; the 191 double-step-trap comment travels |
| Graph knob (read-only; untouched) | `graph_join_order_soft_margin_step` :231; thin-margin gate read :2538 |
| Info-schema surface (goes multi-row) | `optimizer_margin_advisory` binder.rs:102/:111, bind branch :1911-1915; schema/rows binder/explain.rs |
| SystemTableKind (NO new variant) | `InformationSchemaOptimizerMarginAdvisory` ir/execution.rs:220 — adr102 count unchanged |
| Kind partition source | decision_log.rs:8-19 (7 real kinds; graph = GalaxyRoot/BridgeRoot/BridgeChainRoot); `GraphShape::decision_kind` for the 206 partition |
| Row-count assertions to retarget | grep `optimizer_margin_advisory` + `rows.len(), 1` + row-0 indexing across binder/tests.rs (the :8502-region hits are the start, not the inventory) |
| 208 continuity gate (re-pins) | `apply_advisory_does_not_steer_graph_actuation` (binder/tests.rs) |
| Guard home | tests/architecture/optimizer_accrual.rs (the 208 advisory guards' home) |

## Phase 0 — Red tests first

1. `graph_margin_advisory_reports_graph_scope` — red on main (no graph row exists anywhere).
2. `per_kind_detail_rows_aggregate_independently` — red on main (galaxy-loosen + chain-tighten diverge).
3. `tree_row_selected_by_scope_matches_208_contract` — capture the pre-213 tree row fixture first; after
   the builder goes multi-row, selecting `evidence_scope = 'tree_join_order'` yields exactly those values.
4. `graph_readout_is_read_only_no_plan_change` — byte-identical bind across verdict seedings × all
   application modes.
5. `kind_detail_rows_are_not_actionable` — the `row_kind` column distinguishes structurally.

## Phase 1 — Scope helpers (join_order.rs)

`is_graph_margin_advisory_kind(kind)` — `GalaxyRoot | BridgeRoot | BridgeChainRoot => true`; exhaustive
match (tree kinds + `Overflow` => false). Partition unit test: every kind in exactly one family,
`Overflow` in neither. Factor `tree_margin_advisory_input` into scope-parameterized
`margin_advisory_input(snapshot, scope)` (208's helper becomes the tree-scoped call; per-kind scopes are
single-kind predicates). Symmetric `excluded_sample_size`: each scope counts sampled verdicts
(`MissedWin + OverEager + Vindicated`) outside its own scope — one definition, all five rows.

## Phase 2 — Scope-keyed report builder (join_order.rs)

`margin_advisory_rows(config) -> Vec<MarginAdvisoryRow>` from ONE snapshot read:
- `tree_join_order` knob row — existing semantics verbatim (application mode + applied step meaningful);
- `graph_join_order` knob row — recommendation from graph-family evidence; **read-only rendering**:
  `application_mode = manual`, `applied_margin = current_margin` (D4's honest truth);
- `galaxy_root` / `bridge_root` / `bridge_chain_root` detail rows — same math per kind;
  `row_kind = kind_detail`; read-only rendering as above.
`MarginAdvisoryReport` generalizes in place or a `MarginAdvisoryRow` supersedes it — **verify-first:**
grep the caller count before choosing; keep the never-recompute-from-applied discipline either way.

## Phase 3 — Schema/rows (binder/explain.rs)

Schema gains `row_kind` (Utf8, non-null) **appended at the tail** — every existing column index preserved
(Codex's tightening; the 189/191/208 discipline). Rows builder emits the five scope-keyed rows in fixed
order: `tree_join_order`, `graph_join_order`, `galaxy_root`, `bridge_root`, `bridge_chain_root`. Pin that
order with one shape test; keep behavioral assertions scope-selected so semantics do not depend on row
position.

## Phase 4 — Test retargets + parity

Retarget the 208-era advisory tests: one-row asserts and row-0 indexing → a shared select-by-scope test
helper (one helper, used everywhere). Add: `graph_margin_advisory_ignores_tree_verdicts` + tree mirror;
`insufficient_evidence_renders_per_scope` (thin/empty scopes); `rune_queries_graph_margin_advisory` (the
parity loop); re-pin `apply_advisory_does_not_steer_graph_actuation` as the 213 continuity gate.

## Phase 5 — Guards (tests/architecture/optimizer_accrual.rs)

The seven `adr213_*` from the ADR, including
`adr213_margin_advisory_reads_evidence_snapshot_once` to pin D2's single report-builder snapshot read.
Retargets: 208's report-shape guard → scope-keyed with the tree contract intact; 208's scoped-helper guard
text → acknowledges the second consumer. Assert `adr102` unchanged (no new SystemTableKind variant — the
extension dividend, verified not assumed). Keep the ADR 191 resolve-once guard green.

## Named falsifications (for the review)

1. Drop the graph kind filter (scope → all kinds): `graph_margin_advisory_ignores_tree_verdicts` must
   fail.
2. Wire the graph knob to the graph recommendation (the accidental-application edit):
   `graph_readout_is_read_only_no_plan_change` must fail; the re-pinned
   `apply_advisory_does_not_steer_graph_actuation` fixture also seeds graph evidence so it independently
   catches the same accidental application while retaining the tree-evidence continuity assertion.
3. Collapse `row_kind` (emit all rows as `knob_advisory`): `kind_detail_rows_are_not_actionable` must
   fail.
4. Move `runtime_evidence.verdict_snapshot()` into a per-row helper:
   `adr213_margin_advisory_reads_evidence_snapshot_once` must fail.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib margin_advisory
scripts/cargo-cgroup test  -p toron --lib graph_margin
scripts/cargo-cgroup test  -p toron --lib advisory
scripts/cargo-cgroup test  -p toron --test architecture_test adr213
scripts/cargo-cgroup test  -p toron --test architecture_test adr208
scripts/cargo-cgroup test  -p toron --test architecture_test adr191
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — non-optional
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Hygiene (the bitten-before list)

- No m-word (adr043); margins "loosen/tighten".
- Report rows are in-memory info-schema state — zero goldens; a shift is a finding.
- `--all-targets` if `MarginAdvisoryReport` changes shape (constructed in tests).
- Plain names, no V1 suffixes; cargo via `scripts/cargo-cgroup`.
- Advisory fixtures seed locally via `record_verdict` — no LiveWendelonFixture needed.

## Open for the implementing session (verify-first)

1. `MarginAdvisoryReport` generalize-in-place vs row-struct supersede (caller-count grep decides).
2. The full inventory of one-row/row-0 assertions (grep, don't trust the :8502-region list).
3. `row_kind` vocabulary render-width at the explain site (the established pattern).
