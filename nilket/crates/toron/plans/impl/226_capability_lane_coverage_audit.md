# Impl Plan — Toron ADR 226: Capability-Lane Coverage Audit

Pairs with `plans/adr/226_capability_lane_coverage_audit.md`. A **docs + tests + guards**
cut that proves the negative — no `capability_limited` decline passes the production-home
test today — and installs the tripwire for any future `admissible` row. **No production logic
change: `bounded_admission_candidate_contract` stays all-zero, zero `admissible`, no new
decline reason, decision matrix stays 140/220, no schema/durable/Wendelon change.**

Touch surface:
- `binder/tests.rs` — three real-query behavioral audit gates + the two limit tests.
- `tests/architecture/optimizer_memo_admission.rs` — the four `adr226_*` guards + the
  223/224/225 "future bounded contract" → production-home-gated retargets.

No `join_order.rs` / `decision_log.rs` / `explain.rs` / `runtime_evidence.rs` source change.
No daemon / executor / fragmenter / storage / wire / Wendelon change.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Contract stub (must stay all-zero) | `bounded_admission_candidate_contract` (join_order.rs) → `bounded_candidate_count: 0`, `bounded_candidate_contract_unbuilt` |
| `MultipleProbeRelations` is render-only | emitted only in `analyze()` (join_order.rs:2405); callers binder.rs:270 (render fallback) / :431 (counterfactual) |
| Graph sibling coverage | `GraphShape::Galaxy` → `galaxy_root` decision; `reroot_galaxy_join_tree` actuates |
| `MixedStrategyStar` lane home | `bind_local_star_actuation` declines (join_order.rs:4801, 4834) → `accrue_star_order_decline(root_decision_kind, …)`; ADR 193 authorization note at the site |
| `ProbeNotScanLinear` lane home | `accrue_star_order_decline(StarCandidate, ProbeNotScanLinear)` (join_order.rs:4217) on `!broadcast_probe_is_scan_linear` |
| Decision matrix (unchanged) | `adr216_decision_matrix_is_140_keys_and_evidence_matrix_220` |
| Real-query fixtures | the conformed-galaxy / mixed-strategy / non-scan-linear fixtures already used by existing decline tests; decision-lane proof uses normal `bind` / `bind_with_trace` plus `information_schema.optimizer_decisions`, with EXPLAIN only as secondary receipt evidence |

## Phase 0 — Confirm the reverted baseline

Confirm on HEAD before adding anything: `bounded_admission_candidate_contract` returns
zero for all inputs; `grep -c TwoProbeRelations` = 0; the decision matrix is 140/220. This
cut must not reintroduce any of the falsified mechanism.

## Phase 1 — Real-query behavioral audit gates (tests.rs)

Each gate binds a real query through the normal production bind path (no seeded decision rows)
and proves the audit verdict from the durable decision lane. Use `bind` / `bind_with_trace`
plus `information_schema.optimizer_decisions` for the lane assertion; use EXPLAIN only as
secondary shape/receipt evidence where useful.

1. `memo_audit_multiple_probe_relations_commits_graph_decision_not_star_decline` — bind a
   conformed multi-fact galaxy; assert the durable lane has `galaxy_root` (reordered) and **no**
   `*/multiple_probe_relations` decline row. (Proves no production home + sibling coverage.)
2. `memo_audit_mixed_strategy_star_is_authorization_fence_not_missing_candidate` — bind a query
   that records a real `mixed_strategy_star` decline; assert the bound plan is valid and the
   decline is an authorization boundary (the selected order was preflighted, and the legal
   mixed-strategy cases already actuate under ADR 193). (Proves home-but-fenced #4.)
3. `memo_audit_probe_not_scan_linear_is_physical_fence_not_missing_candidate` — bind a query
   that records a real `probe_not_scan_linear` decline; assert binding falls back to a valid
   hash plan. (Proves home-but-fenced #4.)

## Phase 2 — Limit tests (tests.rs)

- `memo_admission_still_renders_zero_admissible` — absolute; no row is `admissible`.
- `bounded_admission_candidate_contract_returns_zero_for_all_inputs` — the stub is zero across
  the full `(kind, provenance)` manifest.

## Phase 3 — Guards + retargets (architecture/optimizer_memo_admission.rs)

- `adr226_no_capability_decline_is_currently_contractable` — scans that
  `bounded_admission_candidate_contract` has no nonzero return path.
- `adr226_memo_admission_still_renders_zero_admissible`.
- `adr226_future_admissible_row_requires_production_home_proof` — documents/scans that the
  all-zero lock holds and that the production-home fixture is the retarget bar.
- `adr226_capability_audit_makes_no_source_logic_or_matrix_change` — asserts the matrix stays
  140/220 and no new decline reason was added.
- Retarget the 223/224/225 "future bounded contract" guard language to production-home-gated;
  keep the zero-admissible guards absolute.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib memo_audit
scripts/cargo-cgroup test  -p toron --lib memo_admission
scripts/cargo-cgroup test  -p toron --test architecture_test adr226
scripts/cargo-cgroup test  -p toron --test architecture_test adr225
scripts/cargo-cgroup test  -p toron --test architecture_test adr224
scripts/cargo-cgroup test  -p toron --test architecture_test adr216
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL — retargets participate
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. A behavioral gate seeds a decision row, or proves only an EXPLAIN/render receipt without a
   durable decision-lane row: it no longer proves a production home — the whole point; gates
   must use normal production bind plus `information_schema.optimizer_decisions`.
2. Any `admissible` row / nonzero `bounded_candidate_count`: the zero-admissible + all-zero
   guards fail.
3. Reintroduce `TwoProbeRelations` or move the matrix off 140/220:
   `adr226_capability_audit_makes_no_source_logic_or_matrix_change` + `adr216` fail.
4. The `MultipleProbeRelations` gate finds a real `*/multiple_probe_relations` lane row: the
   audit's no-home verdict is wrong — re-open, do not promote a contract.
5. The mixed-strategy / scan-linear gates show an *invalid* bound plan: the home-but-fenced
   verdict is wrong (it would be a real candidate gap) — re-open.

## Hygiene

- No m-word; no version-suffix idiom; no `memo`/`Cascades`/`src/optimizer.rs` (none added —
  this is a docs/tests cut).
- No new durable keys; no goldens; no matrix change → nothing to re-pin.
- `--all-targets` (cheap here — no production source change, but confirms the retargets).
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. The three gates must bind through the normal **production bind path**, not a test shortcut
   or an EXPLAIN-only render proof — the audit rests on what the durable lane records, not on
   `analyze()` called in isolation.
2. If a gate cannot be written from a real query (e.g. no reachable fixture records a real
   `mixed_strategy_star` / `probe_not_scan_linear` decline), that itself is a finding — fold it
   into the audit (the reason has no production home either) rather than seeding a row.
3. Confirm the `adr226_no_capability_decline_is_currently_contractable` guard scans the actual
   return shape of the stub (so a future nonzero branch trips it).
