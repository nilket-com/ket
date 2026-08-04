# Impl Plan — Toron ADR 236: Distributed Projection-Fidelity Substrate

Pairs with `plans/adr/0236_distributed_projection_fidelity_substrate.md`. **Test-only hardening:**
guard the projection invariant the 235 fix landed, add distributed projection-fidelity regressions,
and formalize the cross-producer cardinal rule. **No production storage/wire/planner/Wendelon design
change unless a real bug surfaces. No new SQL surface.**

## Touch surface

- `tests/distributed_process_test.rs` — projection-fidelity regressions (non-zero column,
  nullable preservation) + the value-discriminating cross-producer cardinal pin; the cross-producer
  rule as a documented header/checklist beside these tests.
- `tests/architecture/distributed_fixture.rs` (registered in `tests/architecture/mod.rs`) — the
  `adr236_*` guards. Use `set_ops.rs` only if the local architecture layout makes a new file
  disproportionate.
- `src/bin/toron_fixture_worker.rs` — **no behavior change expected** (235 already added projection +
  morsel handling); only touch if a nullable-preservation or projection-before-morsel ordering bug is
  found.

No `ir.rs`, `binder.rs`, `exec.rs`, `sql_planner.rs`, or `distributed/fragment.rs` changes expected.
The guardable boundary is no new SQL/IR/operator surface, not a commit-diff assertion that no source
file changed.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Worker projection handling (the invariant to guard) | `toron_fixture_worker.rs:139` (`if let Some(columns) = request.projected_columns`) |
| Projection-before-morsel ordering | `toron_fixture_worker.rs` scan(): project, then morsel-slice |
| Scan contract fields | `ScanRequest { projected_columns, pbc_predicate, morsel, … }` (storage.rs:50-62) |
| Weak worker-count proxy (to supplement, not replace) | `unique_selected_worker_count` (distributed_process_test.rs:2044) |
| Nullable fixtures already present | `set_left`/`set_right` (`set_key` nullable) — `toron_fixture_worker.rs` |
| Distributed scan-share path | `union_producer_tasks` / `ScanShare` (distributed/fragment.rs) |

## Phase 0 — Red discriminators

`distributed_projection_fidelity_selects_nonzero_column` (cardinal — red if the worker is reverted to
ignore projection); `distributed_projection_fidelity_preserves_nullable_column`. Confirm red against a
worker that returns all columns.

## Phase 1 — Projection-fidelity regressions (D2/D3)

1. A process query selecting a **non-zero** projected column (e.g. `SELECT set_left.set_key FROM
   set_left` — `set_key` is ordinal 1) through the scan-share path; assert the returned values are
   `set_key`'s, not `id`'s (column 0). Wrong if projection is ignored.
2. A nullable projected column stays nullable and preserves NULLs across the shuffle.

## Phase 2 — Cross-producer cardinal rule (D4)

1. Document the rule as a checklist beside the distributed tests (header comment / short doc): a
   cross-producer cardinal's **expected value must be single-producer-unreachable**;
   `worker_count > 1` is supporting evidence, not proof; the fixture places a discriminating key on
   >1 producer.
2. Add a small helper/checklist marker (for example,
   `assert_cross_producer_cardinal_is_value_discriminating`) that records the discriminator reason
   next to the expected rows. Use it from a representative existing cardinal (e.g.
   `intersect_null_safe_matching_across_partitions` → `[None, 9]`) so the review can see why the
   value, not the worker count, is the proof.

## Phase 3 — Guards (D1)

- `adr236_fixture_worker_honors_projected_columns` — the worker `scan()` applies `projected_columns`
  (behavioral-leaning; assert the projected result via a test, plus a light source check that the
  field is read — not a brittle exact-text pin).
- `adr236_distributed_projection_fidelity_tests_exist` — the Phase-1/2 test names are present.
- `adr236_cross_producer_cardinal_rule_documented` — the checklist marker exists beside the tests.
- `adr236_no_new_sql_ir_or_operator_surface` — assert the cut adds no feature surface (no parser
  support for set-op ALL, no new `LogicalPlan`/`ExecutionPlan` variant, no `Repeat`). Do not try to
  enforce "no production file changed" as an architecture invariant.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test distributed_process_test projection_fidelity
scripts/cargo-cgroup test  -p toron --test distributed_process_test cross_producer
scripts/cargo-cgroup test  -p toron --test architecture_test adr236
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL
scripts/cargo-cgroup test  -p toron --test distributed_process_test   # FULL (fixture blast radius)
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Worker ignores projection — `distributed_projection_fidelity_selects_nonzero_column` returns
   column 0's values (the 235 bug, now pinned).
2. Nullable lost through projection — the nullable test fails.
3. A cardinal passes on a single producer — `..._is_value_discriminating` / the rule check exposes
   that the expected value didn't require cross-producer merge.
4. The cut grew a feature surface — `adr236_no_new_sql_ir_or_operator_surface` fails (no `Repeat`,
   no set-op-ALL, no new logical/physical operator here).

## Hygiene

- No m-word; no version-suffix idiom; **test-only** — no new physical op / IR variant / SQL surface;
  no goldens. Behavioral-leaning guards (don't overfit fixture internals — Codex's risk note).
- `--all-targets`; cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Confirm the worker projects **before** morsel-slicing (ordering matters: project then slice).
2. Confirm a non-zero-column projection genuinely flows through scan-share (not collapsed) — assert
   `worker_count > 1` *and* a column-discriminating expected value.
3. Keep the projection guard behavioral; a brittle exact-text guard on `toron_fixture_worker.rs` is
   the main risk (Codex) — prefer the observed projected result.
4. Confirm no SQL/IR/operator surface changes; if a real projection/nullable bug is found outside the
   fixture, surface it (D5 allows the fix) rather than papering over it in the fixture.
