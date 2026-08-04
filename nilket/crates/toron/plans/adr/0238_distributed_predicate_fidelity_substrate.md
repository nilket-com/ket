# Toron ADR 238: Distributed Predicate-Fidelity Substrate

**Status:** Promoted. **Migration:** `plans/impl/0238_distributed_predicate_fidelity_substrate.md`.
**Category:** Toron distributed **test substrate** — a hardening cut. Codify that process-backed
distributed fixtures honor `ScanRequest.pbc_predicate` in the same source-column domain as production
local scans, and prove that predicate filtering composes with ADR 236 projection fidelity. **No new
SQL grammar, logical/physical operator, optimizer evidence, durable surface, storage wire, or
Wendelon production change.**

## Why this cut exists

ADR 236 fixed and guarded projection fidelity: a process fixture scan must honor
`projected_columns`, otherwise distributed tests can pass while reading the wrong ordinal. ADR 237
then used that substrate for set-op `ALL` multiplicity. The named sibling left behind by both cuts is
predicate fidelity: the process fixture worker still ignores `pbc_predicate`, so any distributed test
that relies on pushed scan predicates can pass for the wrong reason or fail to exercise the intended
scan contract.

The problem is not SQL semantics. Predicate pushdown already exists and local in-memory scans apply
`pbc_predicate` before projection. The gap is distributed fixture honesty.

## Grounding (verified)

- `ScanRequest` already carries both `projected_columns` and `pbc_predicate`.
- Production local in-memory scan applies runtime filtering, then `pbc_predicate`, then
  `projected_columns` (`storage.rs`), so predicate bytecode is evaluated against the source batch
  before projection changes the output ordinal layout.
- `toron_fixture_worker.rs` currently applies `projected_columns` and morsel slicing but never reads
  `request.pbc_predicate`.
- ADR 236 already guards projection fidelity and documents the cross-producer cardinal rule:
  expected values must be discriminating; worker count is supporting evidence only.

## Decisions

**D1 — Fixture predicate fidelity, not a feature surface.** The cut hardens process-backed
distributed fixtures around the existing `ScanRequest.pbc_predicate` contract. No SQL grammar,
planner feature, IR variant, physical operator, optimizer evidence, Wendelon wire, or production
storage change is introduced.

**D2 — Predicate domain is the source batch.** The fixture worker evaluates `pbc_predicate` against
the unprojected table batch, before applying `projected_columns`. This mirrors production local scan
ordering and avoids stale-ordinal bugs when a query filters on one source column but projects another.

**D3 — Projection still owns output shape.** After predicate filtering, `projected_columns` determines
the output schema and column order exactly as ADR 236 pinned. This ADR composes with projection
fidelity; it does not replace or relax it.

**D4 — Use the existing PBC engine, not a shadow evaluator.** The process fixture worker should use
`wendelon::schema::pbc::execute` for `pbc_predicate`, matching the existing local scan behavior for
non-row-identity scans. Unsupported or malformed bytecode must fail loudly as `InvalidPbcProgram`, not
silently return unfiltered rows.

**D5 — Tests must be value-discriminating, and must confirm the predicate actually pushes.** A passing
distributed predicate gate must fail if the fixture ignores `pbc_predicate`, applies projection before
predicate evaluation, returns source ordinal 0 instead of the requested projected column, or
mishandles nullable predicate semantics. **Critically, each gate must also confirm the query genuinely
produces a pushed predicate — assert the fragmented scan request carries `pbc_predicate: Some(..)` (or
use a point/range predicate known to be PBC-eligible).** A fixture that perfectly honors
`pbc_predicate` proves nothing if no query ever pushes one (the whole substrate would be hollow).
`unique_selected_worker_count > 1` remains supporting evidence, not the proof.

**D6 — Row identity and runtime filter fidelity remain out of scope.** The fixture worker does not
grow row-identity projection or runtime-filter emulation here. Those are separate scan-contract
surfaces if future distributed gates need them.

## Semantics

For a process fixture table with source columns `(id, set_key)`:

```sql
SELECT set_key FROM set_left WHERE id > 1
```

must evaluate `id > 1` against source ordinal 0, then project output ordinal 1. Applying projection
first makes ordinal 0 mean `set_key`, which is a different predicate domain.

## Honesty gates

- **`distributed_predicate_fidelity_filters_before_projection`** — projects a non-zero column while
  filtering on a different source column; expected rows are wrong if projection happens before PBC.
- **`distributed_predicate_fidelity_does_not_ignore_pbc_predicate`** — expected rows are a strict
  subset; ignoring PBC returns known extra values.
- **`distributed_predicate_fidelity_preserves_null_semantics`** — nullable source predicates preserve
  SQL/PBC null behavior in the distributed fixture path.
- **`distributed_predicate_fidelity_composes_with_scan_share`** — uses the ADR 236 cross-producer
  helper and explains why the expected value is unreachable from an unfiltered or single-producer
  path.

## Architecture guards

- `adr238_fixture_worker_applies_pbc_before_projection` — behavioral-leaning (the 236 F1 lesson): the
  `..._filters_before_projection` value-discriminating test is the real ordering proof; the source
  guard only lightly checks the worker reads `pbc_predicate` and calls the PBC engine, **not** a
  brittle exact-text / `find(pbc) < find(project)` source-ordering pin.
- `adr238_predicate_fidelity_tests_are_value_discriminating_and_assert_pushed` (each gate confirms
  `pbc_predicate: Some(..)` is genuinely produced — D5)
- `adr238_no_new_sql_ir_operator_or_wendelon_production_surface`
- `adr238_reuses_production_pbc_evaluator` (the worker calls `wendelon::schema::pbc::execute`, not a
  fixture-local predicate interpreter — D4's anti-shadow-evaluator point)
- `adr238_projection_fidelity_guards_remain_intact`

## Consensus notes

Codex iter-0 framed this as the direct sibling to ADR 236: projection fidelity proved
`projected_columns`; predicate fidelity should prove `pbc_predicate` in the source-column domain.
The Council-selected topic aligns with ADR 236 and ADR 237 forward arcs. The important scoping
boundary is to keep this as substrate work, not a new predicate pushdown feature.

## Forward arc

```text
236 projection fidelity
-> 237 set-op ALL multiplicity
-> 238 predicate fidelity (this)
-> multi-dedup distinct aggregates (234 deferred)
-> window functions
```

## Deferred

Runtime-filter fixture fidelity; row-identity fixture fidelity; predicate remapping through
projection for an early-projecting worker; new PBC opcodes or broader predicate pushdown coverage;
multi-dedup distinct aggregates; window functions.
