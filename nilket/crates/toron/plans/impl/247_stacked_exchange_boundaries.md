# Implementation Plan — ADR 247: Stacked Exchange Boundaries (Seed Fragmenter)

## Summary

Let the seed fragmenter fragment a linear stack of exchange boundaries by recursively peeling
the outermost exchange, reusing the peel-and-recurse already present in
`fragment_post_join_exchange_plan`. Fragmenter-only: no IR, binder, or lowering change.

## Verify-first probe

The ADR's load-bearing claim is "the peel-and-recurse machinery is already general." Confirm
before editing:

1. In a scratch fragmenter test, hand-build `Exchange(k2) → Exchange(k1) → Scan` (two distinct
   keys) and call `fragment_plan`. Today it returns `MultipleExchangeBoundaries { count: 2 }`
   (pinned by `multiple_exchange_boundaries_are_rejected_for_seed_fragmenter`, `fragment.rs:1917`).
2. Confirm `fragment_post_join_exchange_plan` (`fragment.rs:278`) recurses via
   `fragment_execution_plan(boundary.input, ShuffleProducer{…})` and builds its consumer with
   `replace_exchange_with_scan` + `shuffle_input_scan(_, boundary.input.schema(), _)` — i.e. it
   is not join-specific in its body.
3. **End-to-end candidate**: plan a nested aggregation on different keys
   (`SELECT k, SUM(c) FROM (SELECT a AS k, COUNT(*) AS c FROM <t> GROUP BY a) GROUP BY k`) and
   `count_exchanges` it. If it yields 2, this is a window-independent end-to-end test for the
   distributed process suite. If the planner collapses it to ≤1, record that end-to-end stacked
   execution validates with ADR 248's multiple-spec gate and keep 247's proof at the fragmenter
   unit level.

## Phase 1 — Extract the shared peel-and-recurse

Refactor `fragment_post_join_exchange_plan` into
`fragment_leading_exchange_plan(plan, boundary, terminal_output, allocator, config)` (rename;
the body is unchanged — it already peels the outermost exchange, recurses on `boundary.input`
as a `ShuffleProducer`, and wraps the consumer). The post-join dispatch (`fragment.rs:265`)
calls the renamed routine. Pure refactor — existing join tests must stay green.

## Phase 2 — Route stacked plans through it

In `fragment_base_plan` (`fragment.rs:339`), after the join and union dispatches, replace the
`exchange_count > 1 → Err(MultipleExchangeBoundaries)` branch:

```
let exchange_count = count_exchanges(&plan);
if exchange_count == 0 { /* single-stage, unchanged */ }
if exchange_count > 1 {
    let boundary = find_exchange(&plan).expect("exchange count > 1");
    return fragment_leading_exchange_plan(plan, boundary, terminal_output, allocator, config);
}
// exchange_count == 1: existing producer/consumer path, unchanged.
```

`find_exchange` returns the outermost boundary; `fragment_leading_exchange_plan` recurses on
`boundary.input` (which still holds the inner exchange) — each peel drops the count by one
until the `== 1` base case. The consumer's shuffle scan uses `boundary.input.schema()` (the
intermediate schema) — already correct, no change to `shuffle_input_scan`.

Keep `ZeroPartitionCount` checks. `MultipleExchangeBoundaries` stays defined and reachable for
shapes still outside scope (it is no longer raised for a linear non-join stack).

## Phase 3 — Tests, guards, verification, seal

### Fragmenter unit tests (`fragment.rs` test module — primary proof)

Reuse `scan_plan()`, `key_expr()`, `exchanged_scan()`, `config()`.

- Reconcile `multiple_exchange_boundaries_are_rejected_for_seed_fragmenter` →
  `stacked_exchanges_fragment_into_n_plus_one_stages`: `Exchange → Exchange → Scan` returns 3
  stages; assert producer/consumer exchange ids chain (inner exchange id consumed by the middle
  stage, outer produced by it).
- `three_stacked_exchanges_fragment_into_four_stages` — depth generalizes.
- `stacked_exchange_stage_wiring_is_chained` — middle stage's `upstream_stages` links the inner
  producer; its output shuffles into the outer exchange.
- `single_exchange_plan_still_fragments_to_two_stages` — regression guard for the unchanged
  one-exchange path.

### End-to-end (conditional on the probe)

If nested GROUP BY produces a stacked plan, add a `distributed_process_test` that executes it
across two workers and checks the value + (optionally) `assert_cross_producer_cardinal_is_value_discriminating`.
Otherwise document that end-to-end lands with ADR 248.

### Architecture guards (`tests/architecture/` — distributed/fragmenter module)

- `adr247_seed_fragmenter_recurses_on_stacked_exchanges`
- `adr247_one_shared_leading_exchange_peel`
- `adr247_multiple_exchange_error_retained_for_unhandled_shapes`
- `adr247_no_new_ir_or_frontend_surface`

### Verification gate (route every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test architecture_test adr247
scripts/cargo-cgroup test  -p toron --test architecture_test            # FULL
scripts/cargo-cgroup test  -p toron --lib distributed::fragment         # fragmenter unit tests
scripts/cargo-cgroup test  -p toron --test distributed_process_test     # FULL (multi-stage shuffles)
scripts/cargo-cgroup test  -p toron --lib                               # no wire/golden shape change expected
cargo fmt -p toron -- --check
git diff --check
```

### Seal

Commit the implementation as `impl: adr 247 stacked exchange boundaries` with the
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.

## Risk notes

- **Recursion termination**: each peel removes one exchange; base case is the unchanged
  `== 1` path. A malformed plan cannot loop (count strictly decreases).
- **Middle stage = consumer + producer**: relies on `ShuffleProducer` terminal-output
  threading through the recursion — already exercised by the post-join path; the wiring test
  pins it.
- **Scan-metadata identity**: the shuffle-input scan carries the base-table identity from
  `first_scan_metadata`; for a mid-stack shuffle this is the same base table the inner stage
  read — consistent with the post-join path, which anchors on its subtree's first scan.
- **Out-of-scope shapes**: keep `MultipleExchangeBoundaries` for join/union multi-exchange
  shapes the dedicated paths do not cover, so the fragmenter fails loudly rather than
  mis-fragmenting.
