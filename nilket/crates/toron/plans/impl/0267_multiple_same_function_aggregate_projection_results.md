# ADR 267 — Results: Multiple same-function aggregate projection

## The scope correction (honest, during implementation)

The promoted plan called this **frontend-only**, resting on the premise that "the binder already
synthesizes unique per-aggregate names downstream (`__md_b0_a0`…)." That premise is true only for ADR 240's
**distinct-rewrite** path. The **general** grouped-aggregate path re-derives the `Aggregate` node's output
field names in the binder's `aggregate_schema` (`binder/aggregate.rs:235`) straight from the natural name
(`SUM→"sum"`), so two `SUM`s independently produce two `"sum"` fields there too. Renaming only the frontend
catalog left the binder re-colliding, and q01 surfaced it precisely:

```
SELECT l_returnflag, SUM(l_quantity), SUM(l_extendedprice) FROM lineitem GROUP BY l_returnflag
  →  UnknownColumn { column: "__agg_0" }     (frontend referenced __agg_0; binder re-named the field "sum")
```

So the fix applies the **same** disambiguation in two name-deriving sites that must agree. This is a
grounding-miss in the plan's premise (same family as 119/120/121); the promoted ADR + impl docs were
reconciled in this seal commit, not silently.

## Mechanism — one rule, two sites, keyed on aggregate position

Both sites rename colliding same-natural-name aggregate outputs to `__agg_<i>`, where `i` is the
aggregate's **position** in the aggregate list. The key is identical between the two because the binder's
`aggr_expr` is built from the frontend `AggregateOutputCatalog` entries in order, so frontend entry `i` and
binder field `i` are the same aggregate.

- **Frontend** (`sql_planner.rs`): `AggregateOutputEntry` gains an `output_name`. `finalize_output_names`
  (called from `lower_aggregate_projection` after discovery) counts natural names across entries; a name
  shared by >1 entry → `__agg_<i>`, otherwise the natural name. It also re-points each entry's
  `order_by_visible_names` slot equal to the natural name to the new `output_name`.
- **Binder** (`binder/aggregate.rs`): `aggregate_schema` collects `agg_fields`, counts natural-name
  collisions, and renames colliding fields with `field.with_name(format!("__agg_{index}"))`.

A **single** aggregate keeps its natural name (`"sum"`) — unchanged surface. A SELECT **alias** still names
the final column via the SELECT `Project` on top; the internal `__agg_<i>` never leaks when an alias
exists. No `ExecutionPlan` / `LogicalPlan` IR naming field; aggregate **values** are unchanged.

## Resolvers (all three)

- `resolve_natural` (projection + HAVING): finds the entry **by signature** (so `SUM(a)`/`SUM(b)` are
  distinct), returns `Found(entry.output_name)`. `Ambiguous` now fires **only** on a genuine residual: an
  aggregate output name that clashes with a GROUP BY **key** name.
- `resolve_for_order_by`: finds the entry by signature, returns its (re-pointed) visible output name.
  `ORDER BY SUM(b)` resolves to `SUM(b)`'s own column, never the sibling `SUM(a)`'s shared `"sum"`. A
  residual `Ambiguous` remains when the **final projection** carries two equally-named columns (e.g. two
  identical `COUNT(*)` projection items) — that is a real duplicate-output ambiguity, not an entry collision.

## Behavior deltas (fence reconciles)

- Distinct same-function aggregate **calls** in HAVING / ORDER BY now **resolve** (were
  `ambiguous {HAVING,ORDER BY} aggregate-call`): `having_aggregate_call_resolves_unless_group_key_clash`,
  `order_by_aggregate_call_resolves_unless_group_key_clash`,
  `computed_aggregate_arguments_resolve_by_distinct_signature`.
- An unaliased duplicate aggregate's natural name is no longer addressable: `ORDER BY count` / `HAVING count`
  over two unaliased `COUNT`s → `UnknownColumn { "count" }` (was `AmbiguousColumn`), since both are now
  internal `__agg_<i>`. Alias to address them.
- Still fenced (unchanged): aggregate output vs GROUP-BY-key name clash; two identical projection columns
  under ORDER BY.

## Gauge

```
q01 dev=pass@execute  strict=blocked(unsupported_decimal_avg@bind)
q06 dev=pass@execute  strict=pass@execute   (unchanged)
```

dev q01 — four `SUM`s, three `AVG`s, `COUNT`, GROUP BY, ORDER BY, all `Float64` — now reaches **execute**
end-to-end (the strongest proof same-function aggregates disambiguate). strict q01 advances from the
projection wall to the measured `unsupported_decimal_avg@bind`: 266's decimal `+`/`-` are now exercised in
the q01 path, and `AVG(Decimal)` is the next wall. Gauge gate: `q01_advances_past_aggregate_projection`.

## Tests

Landed in `crates/toron/tests/group_by_test.rs` (it owns the `sales` fixture + execute helpers), not the
plan's hypothetical `aggregate_projection_accounting_test.rs`. New discriminating set (rank-inverting
fixture: `SUM(amount)` ranks east>west, `SUM(rating32)` ranks west>east):
- `same_function_aggregate_projection_binds_distinct_internal_names` — unaliased bind → `__agg_0`/`__agg_1`.
- `aliased_same_function_aggregates_use_aliases_not_internal_names` — aliased execute → labels are the
  aliases (no `__agg_` leak); distinct correct values.
- `order_by_same_function_aggregate_resolves_to_its_own_column` — `ORDER BY SUM(rating32)` yields
  `[west, east, null]`; sorting by the same-named `SUM(amount)` would give `[east, west, null]`.
- `having_same_function_aggregate_filters_by_its_own_column` — `HAVING SUM(rating32) > 6` → `[west]` only.

## Guard reconciles

- `adr089_…` / `adr169_…` (`nominal_type_identity.rs`): pinned literal `aggregate_field(call,
  logical_types)?` → `aggregate_field(call, logical_types)` (the `?` moved onto `.collect::<Result<_,_>>()?`).
- `adr080_duplicate_aggregate_output_names_use_existing_ambiguous_error` →
  `adr267_duplicate_same_function_aggregate_output_is_unaddressable` (`rune_frontend.rs`): the rune duplicate
  case is now `UnknownColumn` (renamed test `rune_duplicate_aggregate_output_name_is_not_addressable`),
  since same-function duplicates disambiguate internally. ADR 080's "reuse the existing ambiguous error" is
  superseded; documented inline.

## Naming (operator)

`q01` is kept. The `toron-tpch` crate **is** the `tpch` namespace — `q01` is referenced only as
`toron_tpch::run_dev_q1` / the gauge's own matrix + JSONL output and never escapes into general `toron`
scope, so it is fully specific in the only place it is used (operator: "tpch.q01 is fine … in a scope where
we are certain only tpch queries are the topic"). If a consumer ever references it from outside, prefix then.

## Gate
fmt + `git diff --check` clean · `architecture_test` 2052 green · full `--no-fail-fast` green
(group_by 220, rune 106, …) · `--lib` 1103 · `toron-tpch` green (harness 8 incl. q01 advance).

## Next blocker (named)
- **ADR 268 — AVG(Decimal):** decimal division (SUM/COUNT) + finalize/rounding policy; q01's next wall
  (`unsupported_decimal_avg`, already classified).
