# ADR 462 implementation receipt

The silent path is gone: `impl Default for QueryBudget` is deleted, all nine call
sites are explicit `QueryBudget::test_default()` choices, and the `adr462`
architecture guard pins both the impl and the call pattern as forbidden strings in
scanned source. Behavior-neutral by construction — every site received the byte-same
value it silently received before, now visibly.

## The sites

- `daemon.rs` (coordinator construction) and `catalog.rs` (descriptor-sync
  accountants ×2): explicit, with comments naming the follow-on — config-derived
  budgets — and the daemon wiring point (`config.worker.memory_budget_bytes` already
  reaches `TenantBudgetPolicy`, not the QueryBudget).
- `binder/join.rs` (broadcast cap ceiling): explicit via a local binding, preserving
  the `adr138_default_threshold_unchanged` guard's pinned expression shape.
- `distributed/budget.rs` (`TenantBudgetPolicy::omitted_config`), `exec/join/spill.rs`
  test helpers, `config_test.rs`: explicit.
- Historical plan documents were left untouched (an over-broad rewrite briefly
  touched them and was reverted before commit; docs describe their eras).

## Guard interactions, resolved per their own patterns

- `adr138_default_threshold_unchanged`: fmt line-wrapping broke the guard's pinned
  substring; restored with a local binding, not by weakening the guard.
- `adr176_no_runtime_or_wire_surface_change`: the three newly-touched files joined
  its later-ADR surface allowlist with an ADR 462 comment, the guard's established
  evolution mechanism.

## Gates

- `adr462_query_budget_has_no_silent_default` green; full architecture_test
  **2,092/0**; workspace `check --all-targets` clean (public-API ripple contained);
  full `toron --no-fail-fast` engine suite green.
- No perf re-measurement claimed or needed: no constructed value changed anywhere.

## Forward

Config-derived production budgets (the daemon wiring point, derivations argued in
the 455 frontier vocabulary) is the named successor governance cut.
