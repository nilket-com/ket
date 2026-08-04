# ADR 012: Mixed-Wildcard Projection Accounting and Planner Hardening

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Codex
**Companion implementation plan:**
`plans/impl/0012_mixed_wildcard_projection_accounting_and_planner_hardening.md`

## Status

Promoted to `plans/adr/` and implemented. Promotion evidence is
behavioral plus a focused source-shape guard (see "Promotion
evidence"). No bench-results artifact — ADR 012 introduces no
operator and makes no performance claim.

Three-way AI Swarm Council convergence reached across five
iteration rounds (`plans/wip/{claude,codex,gemini}_iter_*`).
Codex iter-5 consolidated the frozen contract; Gemini iter-5 and
Claude iter-5 ratified it. Iteration artifacts retire when this
plan is promoted — git holds the narrative.

The selection path is part of the narrative. Claude's iter-1
finding (the mixed-wildcard silent drop) was withdrawn by Claude
at iter-2 as "a bugfix, not an ADR" and picked up by Codex and
Gemini; Codex briefly re-opened toward runtime-filter application
at iter-4; the council reconsolidated at iter-5 on the original
finding. The thrash was a convergence-history desync between
council members, not a merits dispute — and it settled the
question the desync raised: a one-arm planner fix that *closes a
named accounting invariant* is ADR-shaped, because the invariant
is the architecture, not the line count. ADR 010 set that
precedent (no operator, no bench artifact).

Load-bearing antecedents: Claude's iter-1 mixed-wildcard
silent-drop finding and the iter-4 precision points
(rejection is order-dependent today / order-independent after the
fix; the aggregate projection paths are *already* honest, not
out-of-scope-by-omission; the pre-loop `matches!` fast path);
Codex's iter-3 honest-error framing, the `TIME_BUCKET` arity
inclusion, the iter-5 consolidation, and the "exactly two
arguments" sharpening; Gemini's iter-3/iter-4 scope-containment
argument (honest error over a binder/catalog seam) and iter-5
ratification; Claude's iter-4 tiebreaks on the three open
disagreements (exactly-two arity with the self-consistency
reasoning, source-shape guard with no `syn`, invariant-named test
file) and the iter-5 final review note on the qualified-wildcard
rejection-message asymmetry.

This ADR closes the last known projection-item silent drop in
the Toron frontend planner — the third in the ADR 010 → 011 → 012
sequence — and hardens one nearby planner panic. It introduces
no new IR, no new operator, and no cross-ring change.

## Goal

Close two frontend-planner defects, both contained in
`SqlPlanner`:

1. **Mixed-wildcard projection accounting (the integrity fix).**
   `SqlPlanner::lower_select`'s non-aggregate projection loop
   returns the input plan unconditionally on the first
   `SelectItem::Wildcard(_)` it sees, silently discarding every
   sibling projection item. This is the ADR 010 / ADR 011
   accounting invariant pushed one level further: ADR 010 made
   every `Query` / `Select` *field* accounted; ADR 011 made every
   *item within* an aggregate `Select.projection` accounted;
   ADR 012 makes every item within a *non-aggregate*
   `Select.projection` accounted — lowered or loudly rejected,
   never silently dropped.
2. **`TIME_BUCKET` arity hardening (the robustness fix).**
   `SqlPlanner::lower_expr` indexes `TIME_BUCKET` arguments
   `args[0]` and `args[1]` without checking arity, so malformed
   user SQL panics the planner instead of returning
   `FrontendError::Unsupported`.

## Scale boundary

**Primary:** frontend planner.

ADR 012 is contained in `SqlPlanner::lower_select`'s
non-aggregate projection path and `SqlPlanner::lower_expr`'s
`TIME_BUCKET` argument validation.

**Explicitly unchanged:** binder, executor, distributed
fragmenter, IR, wire, Wendelon scan contract, runtime-filter
contract, and task-failure taxonomy.

No bench artifact. ADR 012 introduces no operator and makes no
performance claim.

## Context

ADR 010 closed silent top-level `Query` / `Select` field drops.
ADR 011 closed silent aggregate projection-item drops. The
non-aggregate projection path still has one silent drop:

```rust
let mut exprs = Vec::new();
for item in projection {
    match item {
        SelectItem::UnnamedExpr(sql_expr) => {
            exprs.push(self.lower_expr(sql_expr)?);
        }
        SelectItem::Wildcard(_) => {
            return Ok(plan);
        }
        _ => return Err(unsupported("unsupported select item")),
    }
}
```

For bare `SELECT *`, returning the already-lowered input plan is
intentional. In a mixed projection, the same early return
silently drops sibling projection items:

```sql
SELECT region, * FROM sales;
SELECT *, amount + tax FROM sales;
SELECT *, * FROM sales;
SELECT *, sales.* FROM sales;
```

The current defect is also position-dependent. `SELECT *,
sales.*` returns the input plan before visiting `sales.*`;
`SELECT sales.*, *` rejects on the qualified wildcard before
reaching `*`. ADR 012 makes the rule order-independent: any
wildcard with any sibling rejects.

The aggregate projection paths are already honest after ADR 011.
`SELECT *, COUNT(*) FROM t` routes through aggregate projection
classification and rejects there. ADR 012 touches only the
non-aggregate `Project` path — the aggregate side is not in
scope because it is already correct, not because it was missed.

A nearby robustness defect exists in `TIME_BUCKET`: the lowering
code indexes `args[0]` and `args[1]` without checking arity.
Malformed user SQL can panic instead of returning
`FrontendError::Unsupported`.

## Decision

### 1. Bare wildcard remains supported

`SELECT * FROM t` remains supported and lowers byte-identically
to today's plan: the input subtree is returned with no
`LogicalPlan::Project`.

### 2. Mixed wildcard projections reject

Any `SelectItem::Wildcard(_)` in a projection list with any
sibling rejects through `FrontendError::Unsupported`.

Rejected examples:

```sql
SELECT a, * FROM t;
SELECT *, a FROM t;
SELECT *, * FROM t;
SELECT *, a + 1 FROM t;
SELECT *, t.* FROM t;
SELECT t.*, * FROM t;
```

This is an honest-error contract, not wildcard expansion.

### 3. Qualified wildcard remains unsupported

`QualifiedWildcard` (`t.*`) was unsupported before ADR 012 and
remains unsupported. Supporting it requires schema/name expansion
and duplicate-column policy.

### 4. Wildcard expansion and aliases stay out of scope

ADR 012 does not support:

- `SELECT a AS x`;
- `SELECT *, a AS x`;
- `SELECT t.*`;
- explicit expansion of `*` into a `Project`;
- deferred wildcard-expansion IR;
- catalog access from `SqlPlanner`;
- duplicate-column policy.

Full wildcard expansion is a binder/catalog seam decision for a
future ADR if it earns evidence.

### 5. `TIME_BUCKET` requires exactly two arguments

`TIME_BUCKET` accepts exactly:

```sql
TIME_BUCKET(<expr>, '1m' | '1h' | '1d')
```

Wrong arity rejects through `FrontendError::Unsupported`.
`TIME_BUCKET(ts)`, `TIME_BUCKET()`, and
`TIME_BUCKET(ts, '1h', extra)` all reject. The "extra argument"
case is load-bearing: accepting at least two args would silently
drop the trailing argument, reproducing the same defect class
this ADR exists to close. The arity check cannot itself contain
the defect.

Existing interval validation remains unchanged.

## Implementation contract

`lower_select` uses an explicit pre-loop fast path for the only
legal wildcard case:

```rust
if matches!(projection, [SelectItem::Wildcard(_)]) {
    return Ok(plan);
}
```

Then the non-aggregate projection loop treats `Wildcard` as an
error. This shape is preferred over loop state because it makes
the supported case one visible pattern and makes the loop
account for every item.

`lower_expr` checks `TIME_BUCKET` arity before indexing:

```rust
if args.len() != 2 {
    return Err(unsupported("TIME_BUCKET expects exactly two arguments"));
}
```

One implementation note: `SELECT sales.*, *` rejects via the
*existing* `_ => unsupported("unsupported select item")` arm
(the loop reaches the leading `QualifiedWildcard` before the
trailing `*`), not via the new mixed-wildcard arm. Both orderings
reject as honest errors; only the error string differs, on a
construct (`QualifiedWildcard`) that was never supported. This is
intentional and not worth a loop rewrite to unify — the
companion impl plan records it so the asymmetry does not read as
a bug later.

## Promotion evidence

Promotion evidence is behavioral plus a focused source guard:

1. `SELECT * FROM sales` lowers byte-identically to the pre-ADR
   plan.
2. `SELECT region FROM sales` and `SELECT region, amount FROM
   sales WHERE amount > 10` lower byte-identically to the pre-ADR
   plans.
3. `SELECT region, * FROM sales` rejects.
4. `SELECT *, region FROM sales` rejects.
5. `SELECT *, * FROM sales` rejects.
6. `SELECT *, sales.* FROM sales` rejects.
7. `SELECT sales.*, * FROM sales` rejects.
8. ADR 011 aggregate wildcard rejection remains green.
9. `TIME_BUCKET()` rejects.
10. `TIME_BUCKET(ts)` rejects.
11. `TIME_BUCKET(ts, '1h', extra)` rejects.
12. Valid `TIME_BUCKET(ts, '1h')` behavior remains unchanged.
13. A source guard verifies the old wildcard early-return
    anti-pattern inside the projection loop is absent, without
    forbidding the legitimate pre-loop exact-wildcard fast path.

## Risks

**R1: Bare `SELECT *` regresses.**
Mitigation: byte-identity test for the bare wildcard plan.

**R2: The implementation drifts into wildcard expansion.**
Mitigation: no catalog access, no wildcard IR, no binder work.

**R3: Architecture guard brittleness.**
Mitigation: use the existing source-shape style. Guard only the
old anti-pattern; do not add `syn`. If source extraction gets
brittle, keep the behavioral tests and drop the source guard
rather than adding a parser dependency.

**R4: `TIME_BUCKET` hardening silently ignores extra args.**
Mitigation: require exactly two args, not at least two.

## Non-goals

- No `QualifiedWildcard` support.
- No aliases.
- No wildcard expansion.
- No new IR.
- No binder, executor, fragmenter, wire, or Wendelon changes.
- No ADR 011 follow-ons.
- No ADR 006 runtime-filter production work.

## ADR 013 handoff

The runtime-filter application investigation surfaced during
ADR 012 selection is not discarded — it is the ADR 013 lead
candidate:

- ADR 006 generation, merge, carrier, and the two-wave barrier
  are wired.
- `ScanRequest.runtime_filter` reaches real storage scans.
- Both current `StorageScan` implementations
  (`InMemoryStorageScan`, `WendelonStorageScan`) ignore it.
- The only row-rejection path is a test-local fixture
  (`RuntimeFilterFixture`) that reimplements scan.

So ADR 006's runtime filter is, in production, generation and
carriage with no application. This is ADR-shaped — a missing
trait contract, a missing table-query wire field, and a missing
scan-engine application site — and is recorded as the ADR 013
lead candidate, ahead of the post-ADR-009 A–E list, on the same
evidence-beats-the-list discipline that selected ADR 010, 011,
and 012.

Seed drafts: `claude_iter_3_{adr,impl}_012.md` and
`codex_iter_4_{adr,impl}_012.md` (retired with the other
iteration artifacts; git holds them).
