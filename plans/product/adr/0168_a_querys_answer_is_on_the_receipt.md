# ADR 0168: a query's answer is on the receipt

Status: proposed 2026-09-12. Application lane; a bounded slice of ADR
0166. ADR 0167's acceptance showed the gap: `run_cell` on a SQL cell
reports that the cell applied or failed and nothing else, so an agent that
wants to know what a query answered has to plot the result and `publish`
it, then read the package's Arrow data with a tool of its own. A person
sees the rows in the grid the moment the cell runs; the agent equivalent
of that sight does not exist.

## Decision

### 1. A SQL cell's receipt carries this execution's answer, bounded

When `run_cell` runs a SQL cell and it produces a relation, the receipt
carries a `result`: the columns with their names and data types, the row
count, a preview of the first rows, and a truncation record. The result
is the one this execution produced: the completion captures the artifact
the run bound, and the receipt reads that captured artifact, never the
cell's retained artifact by identity, so a run that fails or a statement
that yields no relation after an earlier success reports no rows from the
earlier run. An empty relation is a result with its columns and zero
rows, distinct from a cell that produced no relation. A cell that
produces no relation (a statement, a Rune cell) has no `result`; nothing
else on the receipt changes, and every other command's receipt is
byte-for-byte what it was.

### 2. The bound is on the encoded event

The live connection carries one event per line under a fixed limit, so
the bound is a byte budget on the encoded `result` as it will appear in
the event, envelope included, with a margin below the line limit; the
budget and the row bound (twenty) are named constants. Formatting
proceeds row by row and cell by cell against the remaining budget and
stops when it is exhausted; nothing is formatted, allocated, or encoded
beyond what fits, and no whole-relation string exists at any point. The
schema is charged first: if the column names and types alone exceed the
budget, `result` carries the row count, as many leading columns as fit,
and no preview. A single value that would exceed the remaining budget is
cut at a character boundary with its cut marked in the truncation
record. The truncation record is one shape: `truncated` (bool) and
`truncation_reasons`, a list naming each bound that applied (`rows`,
`bytes`, `columns`, `value`), so a preview cut by both the row and byte
bounds says both.

### 3. Presentation is shared; nullness is data

The preview's cell text is produced by the same function that renders
the grid's cells, lifted from the grid rather than written again, so a
string, a number, and a date read the same to the agent as to the person
(ADR 0166 §2). Nullness is carried explicitly: a preview cell is either
null or a text, so SQL NULL is never confused with a string that displays
as "null" or as empty. Column types are the projected frame's types,
spelled as `open_data` already spells them.

### 4. The answer is read where it already is

The receipt reads the captured artifact's memoized projection, which the
run path already built for the grid; there is no second execution and no
copy of the relation. What the receipt costs is bounded by the byte
budget, not by the relation.

## This cut

`result` on `CommandReceipt` with columns, row count, preview cells
(null or text), and the truncation record; the completion carrying the
run's artifact; the shared cell-text function with nullness separated
from display; the budgeted formatter; the `run_cell` completion in both
the batch driver and the live connection. No paging, no cursor, no full
export through the receipt (`publish` and `export_csv` remain the
full-data surfaces), no data for Rune cells.

## Acceptance

1. A test runs a SQL cell headless and reads the receipt: the columns
   carry the names and types the projected frame has, `rows` is the
   relation's height, the preview cells equal the grid's cell text for
   the same rows with a SQL NULL reported as null and a string "null"
   reported as text, and `truncated` is false with no reasons. The same
   cell with a thousand-row result reports a thousand rows, twenty
   preview rows, `truncated` true with reason `rows`. An empty `SELECT`
   reports its columns and zero rows.
2. A test runs a successful query, then a statement in the same cell,
   then a failing query: the statement's receipt and the failure's
   receipt carry no `result`, and no receipt reports the earlier rows.
3. A test runs cells whose results are wide, whose schema alone exceeds
   the budget, and whose single value is enormous: each receipt's
   encoded event is under the line limit, the truncation reasons name
   `bytes`, `columns`, and `value` respectively, and the formatter's work
   is bounded (no allocation proportional to the relation). A statement
   cell, a Rune cell, and every non-run command carry no `result`.
4. From the Windows desktop through `polariton act` against Bigfoot's
   tenant 1, `run_cell` on `SELECT period, SUM(value) AS value FROM
   trend_showcase GROUP BY period ORDER BY period` returns a receipt whose
   preview is the eight weekly sums ADR 0167's evidence recorded, with no
   plot and no publish.

## Limits and follow-up

A result larger than the preview is read in full through `publish` or
`export_csv`, as today; a paged read is a separate decision. The same
summary on `open_data` and on Rune cells that yield tables is follow-up.
Result inspection for the SQL pane's own query (not a notebook cell) is
not on the command surface yet and stays on ADR 0166's remaining-distance
list.
