# ADR 247 — Stacked Exchange Boundaries (Seed Fragmenter)

## Status

Proposed.

## Context

The distributed **seed fragmenter** (`distributed/fragment.rs`) supports at most **one
exchange boundary** in a plan. `fragment_base_plan` counts exchanges and rejects more than
one:

```
fragment.rs:361  if exchange_count > 1 { return Err(FragmentError::MultipleExchangeBoundaries { count }); }
```

This is the substrate limit behind several deferred features. It is named explicitly in the
window lowering doc-comment (`sql_planner.rs:3798`: *"multiple distinct specs are deferred —
the seed fragmenter allows one exchange boundary"*). Any plan that needs two re-partitions
stacked — multiple window specifications over different partitions (ADR 248), nested
aggregation on different keys — produces `Exchange → … → Exchange → Scan` and is rejected
before it can run distributed.

The local planning/execution path has no such limit (it can carry a `Window → Exchange →
Window → Exchange → Scan` shape), which is why the gap is invisible until a query is
fragmented for the cluster.

### Grounding — the recursion already exists

The machinery to fragment a stacked plan is already present; it is only reachable through the
join dispatch:

- `fragment_post_join_exchange_plan` (`fragment.rs:278`) **peels the outermost exchange**,
  recursively fragments `boundary.input` as a `ShuffleProducer` (so inner exchanges fragment
  too), then builds a consumer stage via `replace_exchange_with_scan` over a shuffle-input
  scan. This is a fully general peel-and-recurse — nothing about it is join-specific except the
  `leading_exchange_before_join` guard that gates it.
- `find_exchange` / `replace_exchange_with_scan` already operate on the **outermost** exchange
  (top-down, stopping at the first `Exchange`), walking through `Window`/`Project`/`Filter`/
  `Sort`/etc.
- `shuffle_input_scan` (`fragment.rs:1538`) already anchors the consumer's shuffle scan on
  `boundary.input.schema()` — the **intermediate** (post-inner-stage) schema — with
  `first_scan_metadata` supplying only the base-table identity (table + fingerprint) for
  routing. So a shuffle point in the middle of a stack already gets the right schema.

The only thing missing is the dispatch: `fragment_base_plan` errors on `exchange_count > 1`
instead of recursing.

## Decision

**The seed fragmenter fragments a linear stack of exchange boundaries by recursively peeling
the outermost exchange.** Concretely:

- Extract the peel-and-recurse body of `fragment_post_join_exchange_plan` into a shared
  `fragment_leading_exchange_plan(plan, boundary, terminal_output, allocator, config)`.
- In `fragment_base_plan`, when `exchange_count > 1` (and the plan is not a join or union —
  those keep their dedicated paths), call the shared routine on the outermost exchange instead
  of returning `MultipleExchangeBoundaries`. The recursion fragments the inner exchanges; each
  peel reduces the count by one until the existing single-exchange base case applies.
- The post-join path calls the same shared routine, so there is one peel implementation.

A 2-exchange stack `Window₂ → Exchange₂ → Window₁ → Exchange₁ → Scan` fragments into three
stages: the innermost producer (`Exchange₁` over `Scan`), the middle stage (`Window₁` over the
exchange₁ shuffle scan, itself producing into exchange₂), and the outermost consumer (`Window₂`
over the exchange₂ shuffle scan, emitting the final result). The middle stage is both a
consumer of exchange₁ and a producer into exchange₂ — the existing `ShuffleProducer`
terminal-output threading already supports this.

`MultipleExchangeBoundaries` is **not** removed: it remains the error for shapes the seed
fragmenter still does not handle (e.g. multiple exchanges straddling a join in a way the join
path does not cover). The change is that a linear non-join stack no longer trips it.

## Scope

- **In**: linear stacks of exchanges under row-preserving / aggregate / project / filter /
  sort / `Window` / `Take` operators (the `find_exchange` walk set). Recursion depth =
  number of stacked exchanges.
- **Out**: join- and union-rooted multi-exchange shapes beyond what `fragment_join_plan` /
  `fragment_union_plan` already do; cross-stage optimization (sharing an exchange across specs
  with the same partition key). Those keep current behavior.

## Consequences

- Nested re-partition plans fragment and run distributed. ADR 248 (multiple window
  specifications) rides directly on this — its lowering already emits the stacked plan; the
  fragmenter was the blocker.
- Each stacked exchange is one extra shuffle stage — the inherent cost of an extra
  re-partition, the same shape the engine already pays for an aggregate or a join shuffle.

## Discriminators (each fails on `main`)

- **`stacked_exchanges_fragment_into_n_plus_one_stages`** — a hand-built
  `Exchange → Exchange → Scan` fragments into three stages instead of returning
  `MultipleExchangeBoundaries`. (Reconciles `multiple_exchange_boundaries_are_rejected_for_seed_fragmenter`.)
- **`three_stacked_exchanges_fragment_into_four_stages`** — depth generalizes; recursion is not
  hard-capped at two.
- **`stacked_exchange_stage_wiring_is_chained`** — the middle stage consumes the inner exchange
  and produces into the outer one (upstream-stage links and exchange ids chain producer→consumer
  through the stack).
- **`single_exchange_plan_still_fragments_to_two_stages`** — the existing one-exchange behavior
  is unchanged (regression guard).
- **End-to-end** (verify-first probe; see impl): if a non-window query already produces a
  stacked plan (nested GROUP BY on different keys), a distributed process test executes it. If
  none exists yet, end-to-end stacked execution is validated by ADR 248's multiple-spec
  distributed gate — noted, not hidden.

## Architecture guards

- `adr247_seed_fragmenter_recurses_on_stacked_exchanges` — `fragment_base_plan` routes
  `exchange_count > 1` (non-join/union) through the shared peel-and-recurse, not an error.
- `adr247_one_shared_leading_exchange_peel` — the post-join and stacked paths call one shared
  `fragment_leading_exchange_plan` (no duplicated peel logic).
- `adr247_multiple_exchange_error_retained_for_unhandled_shapes` — `MultipleExchangeBoundaries`
  still exists and is still reachable for shapes outside the linear-stack scope.
- `adr247_no_new_ir_or_frontend_surface` — fragmenter-only; no IR node, no binder/lowering
  change, no Wendelon/Rune surface.

## Deferred

- Exchange sharing across specs with identical partition keys (optimization).
- Join/union multi-exchange shapes beyond current coverage.

## Forward arc

247 stacked exchange boundaries (this) → 248 multiple window specifications (rides on 247) →
249 NTILE (+ named windows).
