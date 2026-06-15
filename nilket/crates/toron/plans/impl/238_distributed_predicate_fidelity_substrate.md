# Impl Plan — Toron ADR 238: Distributed Predicate-Fidelity Substrate

Pairs with `plans/adr/238_distributed_predicate_fidelity_substrate.md`. Test-substrate hardening:
make the process-backed fixture worker honor existing `ScanRequest.pbc_predicate` before projection,
then add distributed tests proving predicate + projection fidelity. No new SQL, IR, physical operator,
optimizer evidence, Wendelon wire, or production storage surface.

## Touch surface

- `src/bin/toron_fixture_worker.rs` — in `FixtureStorageScan::scan`, apply
  `request.pbc_predicate` to the unprojected source batch before `projected_columns`; map malformed
  bytecode to `StorageError::InvalidPbcProgram`.
- `tests/distributed_process_test.rs` — add predicate-fidelity process tests using existing fixture
  tables and ADR 236's cross-producer helper.
- `tests/architecture/distributed_fixture.rs` — add `adr238_*` guards and retarget the ADR 236
  "projection-only" substrate guard so the later predicate sibling is recognized as an intentional
  fixture hardening, not feature-surface drift.

No changes expected in `ir.rs`, `ir/execution.rs`, `sql_planner.rs`, `binder.rs`, `exec.rs`, or
production `storage.rs`.

## Anchors

| Concern | Anchor |
|---|---|
| Existing scan contract | `ScanRequest { projected_columns, pbc_predicate, ... }` in `storage.rs` |
| Production ordering to mirror | local scan applies `pbc_predicate` before `projected_columns` in `storage.rs` |
| Fixture gap | `toron_fixture_worker.rs` reads `projected_columns` but not `pbc_predicate` |
| Projection substrate | ADR 236 tests and guards in `distributed_process_test.rs` / `distributed_fixture.rs` |
| Cross-producer discipline | `assert_cross_producer_cardinal_is_value_discriminating` |

## Phase 0 — Red discriminators

Add tests first, or locally confirm they fail if the worker ignores `pbc_predicate`:

- `distributed_predicate_fidelity_filters_before_projection`
- `distributed_predicate_fidelity_does_not_ignore_pbc_predicate`
- `distributed_predicate_fidelity_preserves_null_semantics`
- `distributed_predicate_fidelity_composes_with_scan_share`

The key discriminator should project `set_key` while filtering on `id`, so projection-before-filter
uses the wrong ordinal domain.

## Phase 1 — Fixture worker predicate application

In `FixtureStorageScan::scan`:

1. Load the full table batch.
2. If `request.pbc_predicate` is `Some`, run `wendelon::schema::pbc::execute(&pbc, &batch)`.
3. Map PBC compile/execution errors to `StorageError::InvalidPbcProgram`.
4. Apply `projected_columns` after filtering, preserving ADR 236 behavior.
5. Apply morsel slicing after projection as today.

This mirrors the non-row-identity branch of production local scan. Do not add row identity or runtime
filter support in this ADR.

## Phase 2 — Distributed behavior gates

Use existing process fixture tables where possible:

- `set_left(id, set_key)` has nonzero projected column `set_key` and IDs split across morsels.
- `set_right(id, set_key)` can provide distinct values and nullable cases.

Candidate SQL:

```sql
SELECT set_left.set_key FROM set_left WHERE set_left.id > 1
```

Expected result should be `[NULL, NULL, 9]` for `set_left`, proving:

- PBC was applied (`7` from `id = 1` is gone);
- predicate used source `id`, not projected `set_key`;
- projection still returned nullable `set_key`;
- rows were gathered across process workers.

**Each gate must first confirm the predicate genuinely pushes** — assert the fragmented scan request
carries `pbc_predicate: Some(..)` (inspect the fragmented plan / scan request). `WHERE id > 1` is a
range predicate that should be PBC-eligible; if it does not push for the fixture, pick a shape that
does — otherwise the gate is hollow no matter how faithfully the worker honors PBC (Codex risk #1).

Add at least one nullable-predicate case, for example filtering `set_key IS NULL` if the existing PBC
compiler emits bytecode for that shape. If not, use an eligible equality/range predicate and document
the unsupported nullable form as deferred rather than faking coverage.

## Phase 3 — Architecture guards

Add guards in `tests/architecture/distributed_fixture.rs`:

- `adr238_fixture_worker_applies_pbc_before_projection`
  - **behavioral-leaning (the 236 F1 lesson):** lightly check the worker `scan()` reads
    `request.pbc_predicate` and calls the PBC engine. Do **not** pin a brittle
    `find(pbc) < find(project)` source-ordering; the `..._filters_before_projection`
    value-discriminating test is the real ordering proof.
- `adr238_predicate_fidelity_tests_are_value_discriminating_and_assert_pushed`
  - distributed test names exist;
  - test text includes the wrong-result discriminators (`ignoring pbc_predicate`, projection-before
    predicate, or equivalent);
  - **each gate asserts the fragmented scan request carries `pbc_predicate: Some(..)`** (D5 — else a
    perfectly-honoring fixture proves nothing over an un-pushed query).
- `adr238_no_new_sql_ir_operator_or_wendelon_production_surface`
  - no new `LogicalPlan` / `ExecutionPlan` variants beyond ADR 237's current surface;
  - no new SQL planner predicate grammar or Wendelon production file change.
- `adr238_reuses_production_pbc_evaluator`
  - the worker calls `wendelon::schema::pbc::execute` (the production evaluator), not a fixture-local
    predicate interpreter.
- `adr238_projection_fidelity_guards_remain_intact`
  - ADR 236 projection guard/test names still exist.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --test distributed_process_test predicate_fidelity
scripts/cargo-cgroup test -p toron --test architecture_test adr238
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --test distributed_process_test
scripts/cargo-cgroup test -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications

1. Worker ignores PBC — filtered query returns unfiltered `set_key` values.
2. Worker projects before PBC — predicate is evaluated against projected ordinals and returns the
   wrong rows or errors.
3. Projection regresses — result returns `id` values rather than projected `set_key`.
4. Nullable semantics drift — nullable predicate gate returns rows that SQL/PBC should discard or
   drops rows that should survive.
5. Feature creep — architecture guard catches new SQL/IR/operator/Wendelon production surface.
6. Hollow gate — a query that never pushes a predicate: the missing `pbc_predicate: Some(..)`
   assertion lets a perfectly-honoring fixture pass while proving nothing (Codex risk #1).
7. Shadow evaluator — a fixture-local predicate interpreter instead of `wendelon::schema::pbc::execute`
   (`adr238_reuses_production_pbc_evaluator` fails).

## Hygiene

- Keep this as a fixture/test-substrate ADR.
- No m-word; no version suffix idiom; no broad PBC emulator.
- Prefer value-discriminating assertions over worker-count-only checks.
