# Rune relation view fan-out evidence

Status: automated implementation evidence complete 2026-08-01; installed
profile and human acceptance remain pending. Parent: `0003_rune_view_fanout.md`.
The acceptance-corpus row stays pending until the live three-view buffer runs.

## Implemented contract

A contiguous trailing block containing at most one `table`, `plot.bar`, and
`plot.scatter` is stripped by Rune's own AST. Every call must carry the exact
same relation-argument source. Polariton sends the transformed program through
one existing `ExecuteRune` request and fans the returned bounded frame into all
sinks.

The view set owns one `Arc<DataFrame>`. Every sink holds a clone of that same
Arc, and the update boundary asserts pointer identity before rendering. Plot
projections are derived only after that assertion. A table and its charts can
therefore be different presentations, but cannot originate from different
executions.

No Toron, `toron-client`, protocol, relation, or plot-grammar code changed.

## Automated evidence

- `trailing_three_view_block_strips_once_and_retains_each_span` proves one
  three-sink envelope, one scheduler execution (`1 / 1 / 0`), and retained
  source spans for every view.
- `mixed_relations_and_duplicate_forms_refuse_with_both_spans` proves the
  one-relation and one-unnamed-view-per-form bounds. The mixed refusal carries
  both argument ranges; the duplicate refusal points toward named views rather
  than inventing their grammar.
- `a_view_before_a_non_view_statement_is_not_in_the_trailing_block` proves the
  block is contiguous and bounded by the first non-view statement from the
  end.
- `scheduler_executes_in_flight_then_only_the_newest_pending_view` now uses
  complete three-view sets and preserves the deterministic `3 / 2 / 1`
  submitted/executed/superseded census. Cancellation clears the whole set.
- `scheduler_completion_without_in_flight_work_is_a_hard_invariant` is the
  release-mode panicking witness for out-of-order completion.
- `rune_relation_results_feed_the_existing_table_and_plot_surfaces` proves all
  three sinks share the same result pointer and reach the existing table, bar,
  and scatter products.
- `bad_plot_field_fails_only_its_spanned_sink_while_table_survives` proves a
  failed plot projection retains its own call span while the sibling table
  outcome remains successful. The update path preserves that table instead of
  replacing it with the plot error.
- `live_rune_relation_reaches_table_bar_and_scatter_surfaces` is now one
  ignored, read-only dispatch containing all three views. It replaces the
  first-contact fixture's former three-query loop.

## Validation snapshot

- Polariton with Toron: 146 passed, 3 live fixtures ignored.
- Polariton default: 132 passed.
- Polariton architecture: 10 passed in both configurations.
- Focused Clippy with Toron and `-D warnings`: clean.

The local deployment was neither restarted nor queried during implementation.
`test2.sql` and `test3.rn` remained user-owned and untouched.
