# ADR 0168 implementation evidence

Status: implemented 2026-09-12. The record remains proposed.

Polariton only. `CommandReceipt` gained `result: Option<ResultSummary>`
(`driver.rs`): columns as `{name, dtype}`, `rows`, `preview` as rows of
cells each `null` or text, `truncated`, and `truncation_reasons` naming
`rows`, `bytes`, `columns`, or `value`. The summary is built by
`result_summary::summarize` from the artifact the run bound: in
`notebook_task`, the `RunMarkdownCellResult` message for this run carries
its `ExecutionResult::Rows { artifact }`, which is captured there and
handed to `Completion::Done { result }`; no lookup of the cell's retained
artifact by identity occurs, so an earlier run's answer cannot be
reported. The summary reads the artifact's memoized projection (built on
the run path for the grid already) through the grid's own cell formatter,
lifted into `gui::table::cell_text`, which returns `None` for a SQL NULL
and text otherwise; the grid's `cell_str` now calls it and maps `None` to
an empty string as before. The formatter charges a byte budget
(`RESULT_BUDGET_BYTES`, 16 KiB, a quarter of the live line limit) for the
envelope, then each column entry, then each cell's encoded JSON length,
and stops when the budget is spent: a schema that does not fit keeps the
leading columns and no preview (`columns`); a row that does not fit ends
the preview (`bytes`) unless its value can be cut at a character boundary
to the remaining budget (`value`, with `bytes`); more rows than the
twenty-row bound is `rows`. Column types are the projected frame's Polars
names (`i64`, `f64`, `str`); `open_data` spells Arrow names, and that
difference stands as a follow-up rather than a claim. Every other
command's receipt is unchanged, and old clients decoding new receipts are
unaffected because `CommandReceipt` does not deny unknown fields.

## Scope discovered during implementation

The editor accessor the plan's cut listed (a cell's artifact by identity)
was not needed: capturing the artifact from this run's result message is
both simpler and the binding §1 demands, so it was not added.

The plan's §3 said types are spelled as `open_data` spells them; the
projection is a Polars frame and its type names differ from the Arrow
names `open_data` reports. This cut uses the Polars names and records the
mismatch instead of translating under a claim of equality.

The architecture guard `product_0062` names the files allowed to use
serde; the summarizer's first draft measured encoded lengths with
`serde_json` and was refused. The encoded length is now computed from the
escaping rules directly, and a test checks that rule against the live
encoder for quotes, backslashes, control characters, and multi-byte text.

On Linux only, a notebook whose one SQL cell was a single ~22 KB line
made the editor refuse every guarded action on it ("the editor could not
apply the source edit", a libaryon `MutationFailed` whose inner cause the
message drops). Windows applied the same source. The wide-answer test now
writes one column per line, which both platforms accept; the refusal is
bookmarked as its own item and not chased here.

## Verification

Windows: this desktop, `x86_64-pc-windows-msvc`, debug build, the plan
commit `4fec4c57` with this implementation in place (snapshot
`25b95cdd`, pushed to Bigfoot as `ci/0168-2`; an earlier snapshot's lane
found the two items above). Linux: Bigfoot, the same snapshot.

Acceptance 1 and 2, `gui::state::notebook_actions::tests`
(headless, SQLite scratch):
`adr0168_a_sql_cells_receipt_carries_its_answer` runs `SELECT 1 AS x,
NULL AS n, 'null' AS s` and reads columns `x`, `n`, `s` with non-empty
types, one row, the preview `[["1", null, "null"]]`, no truncation, and a
`describe_notebook` receipt with no `result`; then `SELECT 1 AS x WHERE
0` reports one column and zero rows with an empty preview; then a
thousand-row recursive query reports 1000 rows, twenty preview rows, and
reason `rows`. `adr0168_an_earlier_answer_is_never_reported` runs `SELECT
7 AS seven` (preview `[["7"]]`), replaces the cell with `CREATE TABLE`
and runs it (applied, no `result`), then with a `SELECT` from a missing
table (failed, no `result`).

Acceptance 3, `result_summary::tests` on constructed frames and
`adr0168_a_wide_answer_keeps_the_live_event_under_its_line_limit`
headless: a 300-column relation of 60-character strings truncates with
reason `bytes` and the encoded `Event::Completed` carrying its receipt
is under `MAX_LINE`; a 2000-column schema truncates with reason
`columns`, some leading columns kept and no preview; a single 40,000
character value of a multi-byte character is cut at a character boundary
with reason `value` alongside `bytes`; each summary encodes within the
budget. The formatter's work is bounded by construction: it visits at
most twenty rows and stops at the budget, and the only relation-sized
object it touches is the projection the run path had already built.

Windows suites: the Toron head 1,230 passed, 0 failed, 25 ignored; the
reduced head 953 passed, 0 failed, 12 ignored; the architecture tests 129
passed. Linux, the Ket
lane on Bigfoot: `cargo-ctrl` 1,476 passed; architecture tests 129; the
default head 1,304 passed, 0 failed; the portable Toron head 1,233
passed, 0 failed; fmt and all three checks clean. The reduced head had
one failure, `reporting::spool::tests::unpublished_progress_temporary_does_not_skip_a_report`
with "reporting queue is owned by another drainer": a pre-existing
intermittent in a module this cut does not touch (the spool module alone
then went pass, two failures, pass; the whole reduced head passed twice,
954 and 954). It is bookmarked as its own item: `lock()` maps every
`try_lock` error to that message, so the true cause is not yet visible.
Log: `~/ci-logs/adr-0168-ket-2.log`.

Acceptance 4, live: a window opened with `--agents` on a one-cell
notebook (`SELECT period, SUM(value) AS value FROM trend_showcase GROUP
BY period ORDER BY period`), driven with `polariton act`: `connect` to
Bigfoot's tenant 1 over ADR 0165's mTLS connection applied (generation
1), and `run_cell` completed with `result`: columns `period` (`i64`) and
`value` (`f64`), 8 rows, preview `1 113.0`, `2 121.0`, `3 121.0`, `4
136.0`, `5 148.0`, `6 147.0`, `7 166.0`, `8 179.0`, `truncated` false.
Those are the weekly sums ADR 0167's evidence recorded, read from the
receipt with no plot and no publish. The notebook and the live events are
kept under `plans/product/impl/exhibits/0168_result_inspection/`.
