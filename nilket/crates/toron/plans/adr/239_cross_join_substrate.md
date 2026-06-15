# Toron ADR 239: CROSS JOIN Substrate

**Status:** Promoted. **Migration:** `plans/impl/239_cross_join_substrate.md`.
**Category:** Toron core query surface — the cartesian-product join primitive. **A new
`JoinKind::Cross` on the existing `Join` node + an explicit keyless match-all execution path that
rides the broadcast join; no new physical operator.** No new optimizer-evidence surface, durable key,
system table, Rune-only surface, or Wendelon change. The missing relational primitive that lets global
multi-dedup (240) recombine keyless aggregate branches.

## Why this cut exists

Multi-dedup distinct aggregates (234 deferred, the arc's next) recombine per-branch aggregates; the
**grouped** case recombines on group keys (235 null-safe join), but the **global** case has no key —
it is a cartesian of one-row aggregate branches. Rather than smuggle a private one-row cartesian into
240, land the real relational primitive. CROSS JOIN is also a standard SQL feature, and the
scan-contract substrate (236/238) makes its distributed tests faithful.

## Grounding (verified)

- **No `JoinKind::Cross`** (ir.rs: Inner/Outer/Semi/Anti only). SQL `CROSS JOIN` is rejected in
  `lower_join_operator` (sql_planner.rs:3837); **comma joins are a separate fence** — `from.len() > 1`
  (sql_planner.rs:3536) — so lifting them is an n-ary-`FROM` → left-deep-Cross fold, not just the
  join-operator arm.
- **The accidental empty-key match-all is real but a distributed trap.** `join_key_bytes([])` returns
  `Some(vec![])` (exec.rs) → every row shares one key → the generic hash join produces the cartesian
  *locally*. But a distributed hash-partition on `[]` collapses all rows to one bucket — a
  single-bucket bottleneck, not a deliberate cartesian. So Cross must **not** ride the empty-key hash
  path; it forces broadcast and uses an *explicit* match-all.
- **Broadcast is the reuse path, but is Inner-only today in two places.**
  `fragment_broadcast_join_plan` (distributed/fragment.rs:767) exists; `safe_broadcast_physical_join_kind`
  rejects non-Inner (`broadcast strategy is only supported for INNER joins`, :84); the binder-side
  broadcast shape validation is likewise Inner-gated. Both retarget to "inner or cross".
- **Precedent:** 235 added a `null_safe_keys` *mode* rather than a new operator; `Cross` is the
  analogous keyless *kind*.

## Decisions (promotion-locked)

**D1 — Add `JoinKind::Cross` on the existing logical + physical `Join` node.** A new join *kind*, not
a new operator (`LogicalPlan`/`ExecutionPlan::Join` unchanged in shape). Exhaustive `JoinKind` matches
compile-drive the implementation. Output schema = the inner-join concatenation (left columns then
right).

**D2 — `CROSS JOIN` lowers to `JoinKind::Cross` with `on: []`; comma joins fold left-deep.** `FROM a,
b, c` → `Cross(Cross(a, b), c)` (lift the `from.len() > 1` fence). No `ON`/`USING`/natural; `CROSS
APPLY`, `OUTER APPLY`, array/as-of/straight joins, and non-equi `ON` stay rejected.

**D3 — Keyless and predicate-free; `WHERE` filters after the cartesian.** Cross does not reuse
equi-join predicate pushdown. `FROM a, b WHERE a.k = b.k` stays cartesian + filter — no theta/equi
extraction smuggled into this cut.

**D4 — Broadcast-only distributed; fail loud otherwise.** Auto strategy for `Cross` forces
`JoinStrategy::Broadcast` (a `HashPartitioned` cross join is invalid; empty-key hash-partition is
forbidden). The binder + fragmenter broadcast guards accept `Cross`. If no broadcast-eligible side
exists under the cap, **fail with an explicit error** — never a hidden single-task or empty-key-hash
fallback (the 236/238 D8 discipline).

**D5 — Explicit match-all execution; in-memory and spilled agree.** Add `JoinKeyPlan::Cross` (or a
`JoinKind::Cross` branch before key extraction) so Cross emits every build×probe pair deliberately —
not via `join_key_bytes([])`. In-memory and spilled paths must produce the same cartesian rows (or
Cross routes through one bounded match-all path); never one explicit and the other accidentally
empty-key-hashing.

**D6 — Right-build via the existing swap+restore path (column-order safety).** Toron's broadcast
executor builds the physical left input, so a SQL right-build is represented as `SwappedRightBuild`.
That must be paired with the existing declared-order restoration projection
(`restore_declared_join_order_project`) so the SQL-visible schema/values stay left-then-right. Do not
invent a special "right-build without physical swap" path. A smaller-side build is allowed only if
both placements use an explicit declared-order restoration check; otherwise keep deterministic
right-build for the first cut.

**D7 — Inner cartesian only.** No `LEFT/RIGHT/FULL CROSS`. Substrate for ADR 240 (global recombine =
`CrossJoin` of one-row branches; grouped recombine = 235 null-safe group-key join), not multi-dedup
itself.

**D8 — No new optimizer / durable / Wendelon / Rune surface.** Reuse the broadcast join + the `Join`
node; `Cross` adds no optimizer-evidence.

## Semantics

```sql
SELECT * FROM a CROSS JOIN b              -- cartesian product (|a| * |b| rows)
SELECT * FROM a, b                        -- comma join == CROSS JOIN
SELECT * FROM a, b, c                     -- left-deep Cross(Cross(a, b), c)
SELECT * FROM a CROSS JOIN b WHERE a.x=1  -- Filter(CrossJoin(a, b), a.x = 1)
-- still rejected
SELECT * FROM a CROSS APPLY b             -- APPLY / natural / USING / non-equi ON
```

## Honesty gates (discriminating)

- **`cross_join_produces_cartesian_product`** — `3 × 4 = 12`, with **duplicate values** so it proves
  row multiplicity, not set membership.
- `comma_join_produces_cartesian_product`; **`n_ary_comma_join_folds_left_deep_cross`** (`2 × 3 × 4 =
  24`, proving the separate `from.len() > 1` fence was lifted).
- `cross_join_where_filters_after_cartesian` — `WHERE` composes without an `ON`.
- **`cross_join_preserves_sql_column_order_under_broadcast`** — left columns before right (D6).
- `cross_join_never_hash_partitions_empty_keys` — plan-shape guard.
- **`distributed_cross_join_cartesian_is_value_discriminating`** — 236/238 discipline: expected rows
  require the broadcast build side *and* both probe-side workers; worker count is not the proof.
- `cross_join_oversized_build_fails_loud` (D4); `cross_join_rejects_apply_natural_using_on_variants`.

## Architecture guards (promotion-locked)

- `adr239_adds_join_kind_cross_without_new_physical_operator`
- `adr239_cross_join_and_comma_lower_to_keyless_cross_join`
- `adr239_cross_join_forces_broadcast_strategy`
- `adr239_cross_join_does_not_hash_partition_empty_keys`
- `adr239_cross_join_executor_match_all_path_is_explicit`
- `adr239_cross_join_preserves_sql_column_order_under_broadcast`
- `adr239_distributed_cross_join_gate_is_value_discriminating`
- `adr239_no_optimizer_durable_wendelon_or_rune_surface`

**Reconciled prior guards (named):**
- `CROSS JOIN` reject (sql_planner.rs:3837) + comma-join `from.len() > 1` reject (:3536) → retargeted:
  both lower to `JoinKind::Cross`; APPLY/natural/USING/non-equi stay rejected.
- Broadcast "INNER-only" guards — `safe_broadcast_physical_join_kind` (fragment.rs:84) **and** the
  binder-side broadcast shape validation → retargeted to "inner or cross".
- 235 null-safe-join / broadcast-join / ordinary-join NULL guards → reused unchanged.

## Consensus (transparent)

3-of-3. Claude's iter-0 menu recommended CROSS JOIN as the unblocker for full multi-dedup; Codex
iter-1/2 produced the load-bearing design (explicit `JoinKind::Cross`; broadcast-only — the accidental
empty-key hash is a single-bucket distributed trap; explicit match-all; fail-loud on oversized build);
Gemini iter-1 independently converged on every point. The one resolved nuance is column order under
broadcast build-swap: **SQL right-build uses the existing `SwappedRightBuild` physical swap plus
declared-order restoration projection**; column-order safety is not a bespoke no-swap path. Claude
grounded the two implementation specifics: the comma-join `from.len() > 1` fence is a separate
planner fold, and the broadcast Inner-only gating exists in *both* the binder and the fragmenter. No
residual divergence.

## Forward arc

```text
239 CROSS JOIN substrate (this)
-> 240 multi-dedup distinct aggregates (global: CrossJoin of branches; grouped: 235 null-safe key join)
-> window functions
```

## Deferred (named)

`CROSS APPLY` / `OUTER APPLY`; natural joins / `USING`; non-equi/theta join planning; `LEFT/RIGHT/FULL
CROSS`; costed smaller-side broadcast selection for Cross beyond the deterministic first cut; a
costed block-nested-loop strategy for oversized-both-sides cartesians; multi-dedup distinct
aggregates; window functions.
