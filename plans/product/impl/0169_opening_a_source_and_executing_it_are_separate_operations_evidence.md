# ADR 0169 implementation evidence

Status: implemented 2026-09-12. The record remains proposed.

Polariton only. `run_source` joins the command vocabulary (`driver.rs`).
Admission (`gui/state/notebook_actions.rs`) refuses a notebook (toward
`run_cell`), a non-Rune source, and a window with no Rune connection,
before anything executes; an overlapping request is refused naming the
execution in flight. The run is started for the source buffer directly
through the window's own Rune entry point, never through the focused
pane, so a person's focus elsewhere cannot redirect it. The execution is
identified by the buffer, the document generation the run read, the
database connection generation (ADR 0166 §3), and the window's revision
for that buffer, which becomes the execution id once the run starts; the
receipt carries all four as `execution`. Completion is that execution's
alone (§3): a newer run of the buffer, a document generation that no
longer matches, or a connection generation that changed fails it naming
the cause; a reported error fails it as `execution <id> failed: <error>`;
it completes when the scheduler is idle, the retained relation for the
buffer carries this revision with every artifact produced, and no panel
is still running. Without the `toron` feature the run settles on its
single result message. A run that does not settle within 600 s reports
"the execution did not settle within 600 s; its late outcome is dropped",
and any later settle finds no pending work and changes nothing. A
completion that is not `applied` attaches no figures, in both the batch
driver and the live connection.

`drive` takes `--execute on-open` (the default, PRODUCT-0158's order:
open, run, then the script) or `--execute by-command` (open, then the
script, executing only at `run_source`); the mode is the invocation's,
never inferred from the script, and the batch receipt states it as
`execution`. `describe` keeps on-open. Interactive startup is untouched.

## Scope discovered during implementation

The published manifest for a driven Rune source named its dataset's
producer "unknown" although the relation's dispatch had bound the
producer: each view sink rebuilt its artifact from the relation's
projection with `from_polars`, which starts with no producer. The sink's
artifact now takes the relation's producer, and the relation binds
`Toron` with the same spelling the SQL cell path uses (it bound
lowercase `toron` before, which no test pinned).

A valid Rune source that yields no views (`let x = 1;`) completes as an
execution with no figures, distinct from a failure; the tests record
both.

## Verification

Windows: this desktop, `x86_64-pc-windows-msvc`, debug build, the plan
commit `762eec78` with this implementation in place (snapshot
`3583528f`, pushed to Bigfoot as `ci/0169-1`). Linux: Bigfoot, the same
snapshot.

Acceptance 1. `driver::tests::adr0169_by_command_opens_without_executing_and_on_open_keeps_its_refusal`
(the head without a local workspace): `by-command` with `[describe]` opens
a Rune source, executes nothing (`loaded` empty), applies the command, and
states `execution: by-command`; `on-open` with the same script refuses
the invocation as PRODUCT-0158 did on that head. `tests::adr0169_drive_execution_mode_is_explicit_and_defaults_to_on_open`:
the flag, its default, its two values, and its once-only rule. The full
`[connect, run_source, describe]` sequence on a live Toron is acceptance
4 below; no headless fixture holds a Toron, so that sequence is not
claimed from a unit test.

Acceptance 2. `adr0169_run_source_is_refused_without_a_rune_connection_or_on_a_notebook`
and `adr0169_a_failed_execution_reports_its_error_and_no_figures`: a
notebook and a window with no Rune connection are refused before any
execution; a source that does not compile fails with `execution <id>
failed: …`, its identity, and no figures, in a window where a valid
no-view source had just completed with figures `[]`.

Acceptance 3. `adr0169_stale_executions_are_named_and_never_report_figures`
starts a request, refuses an overlapping one naming the execution, then
for each of a newer run, a document generation change, and a connection
generation change settles the execution and reads the receipt naming
that cause and no figures; a further settle finds no work, and a later
command's receipt carries no execution. `adr0169_a_timed_out_execution_says_its_late_outcome_is_dropped`
reads the deadline report. Windows suites: the Toron head 1,237 passed, 0 failed, 25 ignored;
the reduced head 957 passed, 0 failed, 12 ignored; the architecture
tests 129 passed. Linux, the
Ket lane on Bigfoot: architecture tests 129; the default head 1,310
passed, 0 failed; the reduced head 958 passed, 0 failed; the portable
Toron head 1,240 passed, 0 failed; fmt and all three checks clean. The
lane's `cargo-ctrl` step (1,477 general tests passed) had one controlled
test fail, the libaryon `set_language_re_resolves_auto_theme` rust-analyzer
reactor panic also seen once during ADR 0166 and untouched by this cut;
a rerun of the whole `cargo-ctrl` step on the same snapshot exited 0 with
no failures. Logs: `~/ci-logs/adr-0169-ket.log`,
`~/ci-logs/adr-0169-cargo-ctrl-2.log`.

Acceptance 4. From this desktop, `polariton drive --execute by-command
uat/plot_trends_showcase.rn` with `[connect (Bigfoot tenant 1, mTLS),
run_source, describe, publish]`: exit 0; the receipt states
`execution: by-command`, `loaded: []`, four applied commands, and the
execution `{buffer 2, generation 1, connection_generation 1, id 1}`;
`describe` lists the four showcase views; the package holds
`trend-lines`, `trend-area`, `trend-bubbles`, and `trend-scatter`, and its
manifest names the dataset's producer `Toron` with 24 rows (before the
producer fix above, "unknown"). Commands, receipt, manifest, and the four
figures are kept under `plans/product/impl/exhibits/0169_source_execution/`.
This run is ADR 0165's driver path and is cited in ADR 0165's evidence.
