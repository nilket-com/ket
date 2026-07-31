# Impl Plan — ADR 208: Shape-Scoped Optimizer Advisory

Pairs with `plans/adr/208_shape_scoped_optimizer_advisory.md`. Three production files are expected:
`binder/join_order.rs`, `binder.rs`, and the report tail in `binder/explain.rs`; everything else is tests
and guards. The 207 surfaces (evidence handle, coordinator, daemon, observability) do not open. If the cut
grows beyond this plan's touch list, that is the swap-trigger the council named — stop and re-scope before
continuing.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Unscoped sum | `recommend_soft_margin` join_order.rs:103-165 — match on `row.verdict` only (:112-129) |
| Advisory consumers | `effective_join_order_soft_margin_step` :180-193; `margin_advisory_report` :204-218 |
| Single resolved step | binder.rs:394 `join_order_soft_margin_step: effective_join_order_soft_margin_step(config)` (propagates via the one clone site :405) |
| Graph thin-margin gate | join_order.rs:2496 `self.join_order_soft_margin_step.margin()` → `GraphActuationResult::KeptThinMargin` |
| Tree margin sites | join_order.rs:3070, :3223 (`recognized_star_order_outcome` callers) |
| Kind split | tree: `StarCandidate/FlatStar/SnowflakeRoot/SnowflakeBranch`; graph: `GalaxyRoot/BridgeRoot/BridgeChainRoot` (decision_log.rs:8-19) |
| Advisory report schema/rows | `information_schema_optimizer_margin_advisory_schema` / `_rows` (binder/explain.rs) |
| Sample math | `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE`; sample = `MissedWin + OverEager + Vindicated` (join_order.rs:131-133) |
| Guards home | `tests/architecture/optimizer_accrual.rs` (189/191 guards live here) |
| Dirty-tree guard | `tests/architecture/binder_decomposition.rs` `adr176_no_runtime_or_wire_surface_change` already allows binder-family and plans; only non-binder runtime/test files would need a later-ADR allowlist entry |

## Phase 0 — Discriminating tests first (red before green)

Write the failing tests before touching production code:

1. `graph_or_chain_verdicts_do_not_drive_soft_margin_advisory` — **honesty gate; must fail on `main`**
   (graph-only `MissedWin` seeds ≥ min-sample loosen the margin today).
2. `apply_advisory_does_not_steer_graph_actuation` — **must fail under input-only filtering** (the
   peer-draft discriminator): tree seeds that loosen the advisory + a graph actuation near the boundary →
   graph gates on the base step. Build the fixture so the loosened-vs-base margin flips
   `Actuated`/`KeptThinMargin`; assert as close to `GraphActuationResult` as existing tests allow.
3. `tree_verdicts_still_drive_soft_margin_advisory`.
4. `optimizer_margin_advisory_reports_scope_and_exclusions`.
5. `optimizer_runtime_verdicts_still_reports_all_kinds`.
6. `manual_mode_remains_byte_identical_for_both_families`.

## Phase 1 — Scoped advisory helpers (`join_order.rs`)

- `fn is_tree_margin_advisory_kind(kind: OptimizerDecisionKind) -> bool` — the four tree kinds, exhaustive
  match (a new kind variant must force a decision here, not silently default).
- A helper deriving `(scoped_rows, excluded_sample_size)` from a snapshot — excluded counts mirror the
  advisory sample math (`MissedWin + OverEager + Vindicated` of excluded kinds). Policy lives beside
  `recommend_soft_margin`; the evidence store stays scope-free (D1/D3).
- `recommend_soft_margin` itself unchanged: verdict-class math over the provided slice.
- Retarget both consumers (:186, :205) to feed the scoped rows.

## Phase 2 — Split the resolved step (`binder.rs` + `join_order.rs`)

- Binder construction (:394): replace the single field with `tree_join_order_soft_margin_step` (base under
  `Manual`, scoped-advisory step under `ApplyAdvisory`) and `graph_join_order_soft_margin_step` (**always**
  base). Update the clone site (:405).
- Read-site retargets: :3070 and :3223 → tree step; :2496 → graph step. Explicit field names at the read
  sites — no mode check, no bool (the invariant must be legible in review).
- Rename/document `effective_join_order_soft_margin_step` as tree-scoped (e.g.
  `effective_tree_join_order_soft_margin_step`) — no compatibility wrapper; the caller set is closed.
- `scripts/cargo-cgroup check -p toron --all-targets` after the field change (the ADR 116 lesson).

## Phase 3 — Report tail (`join_order.rs` + `binder/explain.rs`)

- `MarginAdvisoryReport` gains `evidence_scope: &'static str` (`"tree_join_order"`) and
  `excluded_sample_size: u64`; `applied_step` stays the tree-applied step.
- Schema + rows append the two columns at the tail; existing prefix order untouched. Update
  `margin_advisory_report_projects_like_information_schema` for the tail.

## Phase 4 — Test retargets (`binder/tests.rs`)

Existing advisory tests already seed tree kinds — verify they stay green un-edited where possible (they
prove tree inclusion). Add the Phase-0 suite. Keep the 35-key completeness assertions on
`optimizer_runtime_verdicts` untouched.

## Phase 5 — Architecture guards (`tests/architecture/optimizer_accrual.rs`)

The seven `adr208_*` guards from the ADR, plus the two named retargets: 189's advisory-reuses-log guard →
pins the scoped helper (raw whole-snapshot consumption by the advisory becomes the violation); 191's
resolve-once guard → also pins the two-step split and the :2496 base-step read. Check
`adr176_no_runtime_or_wire_surface_change`: binder-family, architecture tests, and plans are already allowed;
if any other runtime or test file enters the cut, add the allowlist entry **in this cut** (the 207 review
lesson, F2).

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib margin_advisory
scripts/cargo-cgroup test  -p toron --lib graph
scripts/cargo-cgroup test  -p toron --test architecture_test adr208
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — non-optional (207 lesson)
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Hygiene (the bitten-before list)

- No m-word in src/tests (adr043 substring scan); margins "loosen/tighten", counters "strictly increase".
- No postcard preimage changes (report columns are Arrow schema, not wire/golden surfaces) — a shifted
  golden is a finding.
- All cargo through `scripts/cargo-cgroup`; plain names, no V1 suffixes.
- Advisory fixtures are local (no LiveWendelonFixture needed — evidence seeds via `record_verdict`); if a
  graph-boundary fixture flakes, isolate the assertion at the `GraphActuationResult`, don't widen the test.

## Non-goals

No Wendelon rf>1 counter replication (next cycle, Wendelon sequence); no new durable keys; no counter
reset/decay; no graph-family advisory; no persisted margins; no executor / fragmenter / storage / PBC /
wire / grammar change.
