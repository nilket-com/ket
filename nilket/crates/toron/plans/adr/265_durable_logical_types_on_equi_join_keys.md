# ADR 265 — Durable logical types on equi-join keys

## Status

Proposed.

## Context

ADR 264 planted a known-gap tripwire: a logical-typed column through a join-bodied derived table errored
at bind with `unknown logical type LogicalTypeId(1025)`. Grounding widened the scope. The root cause is a
seed-only resolver on the bind-time logical-type validation, and the failure is **not**
derived-table-specific.

**Root cause.** `validate_logical_ascription_physical` (`binder/arithmetic.rs:980`) resolves the logical
type via the free fn `logical_type_spec` (`ir.rs:70`), which uses `LogicalTypeResolver::seed_only()` — the
hardcoded seed types only (`CUSTOMER_ID`, `PRODUCT_ID`), never the catalog's durable, runtime-registered
types. So any expression whose logical type is **durable** (e.g. `PersonId` = `LogicalTypeId(1025)`)
fails that validation.

**Scope (probed).** Against the `setup_person_id_people` fixture: a *plain* `people JOIN people AS other
ON people.id = other.id` fails at bind with the same `unknown logical type 1025`; so does the
plain-join-plus-`WHERE`; so does ADR 264's derived-table case. Only the join-free `WHERE id < 0` control
binds. The bug is **any equi-join whose key carries a durable logical type** — the derived-table join was
just the symptom ADR 264 happened to surface (a direct scan's `WHERE` resolves logical types later with
the full registry, which is why the control passes).

**Audit (every seed-only resolver caller).** Exactly **one** correctness offender:
`validate_logical_ascription_physical`. It is the shared choke point for *both* ways a durable type
reaches bind-time logical resolution — `resolved_join_key` (equi-join keys, `binder/join.rs:1177`/`:1183`)
and `bind_logical_ascription` (`Expr::LogicalAscription`, `arithmetic.rs:765`) — so a single fix covers
both, and there is **no twin correctness bug** one call-site over. SQL logical `CAST` is not part of this
offender path: it resolves named logical targets through `ctx.logical_type_specs` and then reconciles the
identity. The only other spec-via-`seed_only` use is `logical_type_name` (`ir.rs:84`), which is
display-only (it renders a durable id as raw `LogicalTypeId(N)` in error text); `catalog.rs:882`'s
`seed_only()` is a no-durable-types descriptor constructor; the `sql_planner`/`rune`
`*_logical_type_name` fns are frontend name parsing — none are correctness offenders.

## Decision

1. **Make `validate_logical_ascription_physical` registry-aware.** It takes a `&LogicalTypeRegistry` and
   resolves via `LogicalTypeResolver::with_registry(registry).spec(id)` (`ir.rs:220`) — which resolves a
   seed **or** a durable type — instead of the seed-only `logical_type_spec`. Its physical-match check is
   unchanged; only the resolver gains the durable types.
2. **Thread the registry to its two callers** (the value is already in hand, just not passed):
   - logical ascription — `bind_logical_ascription` passes `ctx.logical_type_registry` (the
     `ExprBindContext` already carries it, `binder.rs:871`). SQL logical `CAST` stays on its existing
     `ctx.logical_type_specs` path.
   - equi-join — `bind_scoped_join` → `bind_join_key_pair` (`binder/join.rs:430`) → `resolved_join_key`
     (`:1173`) thread `self.catalog.logical_type_registry()` down.
3. **Leave the `logical_type_spec` / `seed_only` free fns unchanged.** Seed-only stays correct for display
   and for genuinely-seed-only callers; only the binder validation switches to registry-aware. No new IR,
   opcode, or engine change — a resolver swap plus parameter threading.

**No-silent-fallback guard (operator).** `validate_logical_ascription_physical` must resolve through the
registry-threaded resolver only — it must **not** wrap the registry lookup in an `.or_else(seed_only)` or
any fallback that would let a missing/empty registry still "succeed" via seeds and thereby mask a catalog
threading bug. `with_registry` legitimately includes seeds (a seed-typed join key must still resolve), so
the guard is enforced by the **tests**: they assert a durable **non-seed** id resolves through the path,
which fails outright if the registry isn't actually threaded.

## Tests

- **Flip the tripwire.** ADR 264's `join_bodied_derived_table_with_logical_type_is_adr265_known_gap` →
  positive **bind + execute** (renamed `…_now_binds_and_executes`). It currently asserts the
  `unknown logical type` error, so it fails the instant the fix lands, forcing the flip.
- **Plain join positive (the corrected headline).** `SELECT people.id FROM people JOIN people AS other ON
  people.id = other.id` binds + executes. `PersonId` is a durable **non-seed** type, so this proves the
  registry is threaded (a seed-typed key would resolve via the seed leg and mask a threading regression —
  this test would not).
- **Direct logical-ascription positive (the second caller).** Build a logical plan with
  `Expr::LogicalAscription { logical_type: PersonId, expr: id }` over the `people` fixture and assert it
  binds (and preserves the projected logical identity). This fails today on the same seed-only
  `validate_logical_ascription_physical` call and proves the non-join caller received the real registry.
- **Discriminating negative.** A join whose two keys carry **different durable** logical types must still
  reject with a logical-type-mismatch error (needs a second durable type in the fixture). This proves the
  fix expands *resolution* without loosening *compatibility* — it fails if the fix over-reaches and merely
  disables the check.

## Deferred (named)

- **Durable-type names in binder error messages** — make `logical_type_name` registry-aware so error text
  shows `PersonId` rather than `LogicalTypeId(1025)`. Cosmetic; out of this correctness cut.

## Why toron-home
Toron binder / logical-type resolution. No cross-ring surface.
