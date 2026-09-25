# ADR 0171 implementation evidence

Status: implemented 2026-09-24, before ADR 0171 was written; the record
remains proposed. The ADR's plan commit `d996125c` precedes this
implementation in history but was written after it: the work first landed
as a single implementation commit and was then restructured into plan and
implementation. Its decisions took shape during implementation and review
(Claude implementing, Codex reviewing each slice) and are recorded
retrospectively. rnx side: record 0107, plan `f87494c` and
implementation `b08645d`, over record 0056's `server-runtime` API.

## What shipped

**rnx 0107** (additive to `rnx::server`): `Program::compile_source(name,
text, extensions)` compiles caller-held text (the name appears in
diagnostics and is never opened; the text counts against the 8 MiB source
allowance; a `mod` declaration that would load an external file is
refused, since the text has no directory), `Program::prepare_with` passes
any number of arguments, and a spent budget is reported as "the budget of
N instructions was exhausted". ket pins
`b08645d89146a16c9b41a6b4fddd9307aa23b04b` in `[workspace.dependencies]`
with default features off; the lockfile change only adds rnx and its own
dependencies.

**Dispatch** (`rune_local::mode`, `gui/state.rs`): a top-level `main` with
two parameters routes a run to `run_local_rune_code`; the wrong arity is
refused; no top-level `main`, or text that parses only as a statement body,
keeps the existing path; text that parses as neither is `Unparsed` and keeps
the existing path; where that path is unavailable (no Toron connection),
`run_source` admission reports its parse error.
Local sources need no Toron connection at admission
(`gui/state/notebook_actions.rs`).

**Execution** (`rune_local::execute`): instrumentation, then
`Program::compile_source`, `prepare_with(main, [workspace, viz])`, and a
current-thread runtime on a blocking worker that races the run against a
cancellation channel. The invocation is created, run and closed on that
worker; a cleanup failure outranks cancellation and the script's result.
`AppState.local_rune_runs` holds each buffer's run (revision, document
generation, cancellation). Any new run of the buffer, empty text included,
cancels it and advances the revision; closing the source cancels it, checked
after every message (`reap_closed_local_runs`). An edit does not cancel a
run: when its result or pane attachment arrives, a closed or edited source
causes it to be discarded (`local_run_is_current`).

**Instrumentation** (`rune_instrument.rs`): a walk over `rune::ast` with
Rune's scoping rewrites a bare constructor call to
`polariton_presentation::__name(args.., origins)` only where no binding of
the name is visible; presentation methods become a receiver-typed branch
that makes the original call for any other receiver. Origins are original
buffer offsets; the insertion point comes from the syntax tree, so trailing
commas and comments survive. The `__pz` identifier prefix is reserved; a
visible glob import beside a bare constructor call is refused; macros are
not entered; Rune's `#[non_exhaustive]` syntax enums fall back to a refusal,
not a skip. Per-argument mark origins come from one owner, each mark's
`callable!` entry in `presentation_registry.rs`, read by both this walk and
`rune_view_compiler::rewrite_calls_mapped`.

**Vocabulary** (`rune_presentation.rs`): `install_vocabulary` is the one
registration both the Toron evaluator and a local run install;
`presentation_types!` lists the value types once for registration and for
the method guard; `install_local_views` adds the local `view(name, body)`
carrier. `frame.views([...])` (`rune_local::cell_views`) decodes entries
with the shared slot decoding, refuses a duplicate name where it is written,
and queues the set with a place in the run's output sequence.

**Batch** (`gui/state.rs`): the worker builds legacy `show()` plots and
`views` sets in source order; views go through
`rune_view_outcome_with_palettes` with palettes from
`categorical_palettes_for` over a snapshot of the buffer's retained colour
identities, then through `build_facet_resolved_plot_kernel` or
`build_facet_layered_plot_kernel` (the synchronous cores the Toron tasks now
wrap). A table view, or any failure, fails the batch. `commit_local_panes`
prepares exactly the batch's panels, attaches every kernel, writes colour
continuations back, and records panel and incarnation pairs for
`settle_local_batch`. Local view panes attach without a source attachment
and carry the note "settings of a local source are edited in its source;
rerun it to apply them".

**Surface and fixes**: `HostPlotExport.status` and
`FigureDescription.status`; `prepare_publish` drops orphaned exports;
`view_name` falls back to the panel's registered identity; the line kernel
skips null vertices; the scatter kernel takes a temporal column's own axis
type (`axis_type_for`); `HeadlessApplication::open_source` waits for the
projected text.

## Verification

Linux, this desktop, debug build, the plan commit `d996125c` with this
implementation in place (the code tested is this commit's; only these
records' wording changed after testing):
`cargo test --locked -p polariton` passed 1528, failed 0, ignored 26;
`cargo check --locked -p polariton --no-default-features --tests` reported
no warnings.

Acceptance 1. On the committed exhibit (FRED closes since 2020-01-01),
`polariton drive --execute by-command uat/treasury_rates.rn` with
`[run_source, describe, publish]` and no Toron connection applied all three
with four fresh figures named `Yields by maturity`, `10Y decomposition`,
`Yield curve` and `10Y daily change`. The package's yields dataset was
joined in duckdb against independent downloads of DGS3MO, DGS2, DGS10 and
DGS30: 7,020 rows on each side, 0 mismatches, 288 null rows (holidays),
2020-01-02 through 2026-09-23, last 10-year close 5.11. The native window
was also driven through `polariton act` (`run_source`, then `resize` by
view name) and captured at 3840×2128.

Acceptance 2. `rune_local::tests::existing_view_sources_keep_the_view_path`
classifies every `uat/*.rn` exhibit except `treasury_rates.rn` as a view
source. The Toron driver tests and the view compiler's tests are unchanged
and pass, as does
`product_0158_driver_tests::a_toron_view_orphaned_by_its_source_is_labelled_and_not_published`.

Acceptance 3, by test:

- cancellation: `gui::state::local_rune_tests::closing_the_source_cancels_its_fetch_and_publishes_nothing`,
  `rune_local::tests::a_rerun_cancels_a_pending_request_promptly`,
  `a_dropped_sender_cancels_like_a_rerun`, `a_failed_cleanup_outranks_cancellation`;
- stale output: `an_edit_during_the_fetch_discards_its_output`,
  `an_edit_before_the_batch_commits_discards_it`,
  `an_empty_run_retracts_a_queued_batch`,
  `an_empty_run_retracts_an_already_built_plain_plot`;
- batch: `a_batch_replaces_the_panes_whole`,
  `a_bad_view_fails_the_whole_batch_and_keeps_prior_panes`,
  `an_unknown_skin_fails_the_batch_at_its_view`,
  `show_and_views_keep_source_order`;
- receipts: `run_source_settles_after_the_whole_batch`,
  `agent_api::a_same_count_rerun_reports_the_new_figures`,
  `agent_api::receipts_describe_and_publish_only_current_output_as_current`;
- vocabulary and diagnostics: `rune_local::tests::views::*` (a subtitle
  computed from fetched data, a helper-built figure and names computed in a
  loop, a view not taken, a duplicate name, errors at the original line and
  column after a UTF-8 literal, `for line in …` beside `line(...)`, the
  legacy chain, a receiver evaluated once in either branch, `views` refused
  outside a local run),
  `rune_presentation::tests::the_method_guard_recognises_every_presentation_value_family`,
  `a_views_batch_commits_named_panes_that_refuse_writeback`;
- instrumentation: the 20 tests in `rune_instrument::tests`, including every
  binding form shadowing within its scope and the same call outside it
  rewritten; and
  `rune_view_compiler::tests::static_and_local_rewrites_inject_the_same_origins`;
- frames and fixes: `rune_local::tests` for frame typing and dates,
  `plot::kernels::line` `null_observations_are_skipped_never_zero`,
  `plot::kernels::scatter::axis_type_tests`.

## Scope discovered during implementation

The exhibit exposed three existing defects, fixed in the same commit: the
line kernel unwrapped every value and panicked on a null; the scatter
kernel labelled a Date axis as milliseconds because it took the axis type
from the scale family alone; and `open_source` treated a Rune source as open
before its text was projected, so `drive` could refuse its first
`run_source`. A test fixture in `notebook_actions` had the same projection
race and now waits for the text.

Unchanged and flagged for a separate issue: when Toron relation views
attach, `state.rs` stores the generated relation program as
`rune_presentation_sources` while slicing it with buffer-coordinate spans,
which looks like a mis-slice of the writeback expression. It is not
reproduced here.
