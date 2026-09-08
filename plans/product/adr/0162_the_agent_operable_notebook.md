# ADR 0162: the agent-operable notebook

Status: proposed 2026-09-07. Application lane. This cut extends
PRODUCT-0158/0160 in response to PRODUCT-0161's implementation: the agent
could describe and publish a figure, but could not open the person's data,
inspect the notebook, change its query, or run its cells. Falling back to
pointer automation hid the missing product surface. The user chose to pivot
back to self-driving work before finishing first-run acceptance.

## Decision

### 1. Four actions, on the existing application surface

`describe_notebook`, `open_data`, `replace_cell`, and `run_cell` join the
batch driver's commands and the same-user live connection. They do not add
a transport or bypass the accepting-agents gate, connection generation,
request-id policy, or bounded replies. The application owns their admission
and completion. Both clients call that owner.

`describe_notebook` reports the notebook source buffer, its engine document
generation, and its executable cells: identity, language, and source body.
The snapshot comes from the editor's attributed text and libaryon's own
fence projection. It is validated against the live buffer generation before
being returned. It never derives cell coordinates from a different revision.
Ambiguous notebooks require a buffer selector. Source bodies are bounded to
32 KiB in total in this cut; an oversized notebook refuses explicitly.
This is trusted local authoring data, never reporting telemetry.

`open_data { path }` connects a Parquet or CSV file through the file adapter
and reports the SQL table name and its columns. It does not execute a query.
The connection path is also used by the window's file adapter. A later
first-run UI can use it to author its suggested query; the agent can author
that query using the returned schema and the notebook actions. Opening data
does not replace source text or discard an unsaved buffer.

`replace_cell` names a buffer, an engine document generation, a cell
identity, and replacement body text. The engine's existing
`ApplyDocumentEdits` applies the replacement as one undoable transaction,
checking that generation at the mutation owner. Typing queued before the
request cannot be overwritten using an earlier projected generation. The
replacement stays inside that cell's body; replacing a whole notebook or
changing fences is outside this cut.

`run_cell` names the same buffer, generation, and cell. An empty guarded edit
transaction validates that snapshot without changing source or undo history;
then the application invokes its existing SQL/Rune cell runner on the
captured body. Focus need not be on the cell. Completion is correlated with
that run's task messages, including the kernel-building continuation for a
plot. It is never inferred from global scheduler idleness or another cell's
result. The receipt identifies the source generation executed; later edits
do not retrospectively change that statement or roll back SQL effects.

### 2. Admission and completion are different

Only one notebook/data operation is admitted at a time per application;
another refuses as busy. The engine owns edit acceptance. Source requests
receive their final reply after that decision, rather than an early
`applied` claiming an edit which the engine might refuse. A successful live
request ends in `completed`; batch returns the corresponding applied
receipt. A generation mismatch refuses without mutation or execution.

File reads and cell execution run off the update thread. A file load whose
original connection has since been replaced cannot install over that new
connection. A failed load reports failure and preserves the earlier target.
SQL/Rune failure after execution starts is `failed`, not `refused`; the
receipt carries the local diagnostic and makes no rollback claim.

Each pending operation has a bounded deadline. A timeout after dispatch
reports failure with the outcome explicitly unknown; it does not assert
that execution stopped, and clients never replay it automatically. Requests
which have started continue under the ordinary application ownership after
connection loss. There is no cross-connection result ledger.

### 3. Notebook batch startup does not require Toron

A Markdown source opens headless on the scratch database and waits for
commands. Its cells run only when requested. Rune application startup keeps
PRODUCT-0158's existing run behavior. Reading and editing notebook source,
opening a local file, and running SQL plus notebook plots also work in the
reduced feature head.

### 4. Proof through the product

The acceptance sequence opens a running window on a small notebook, uses
its advertised live socket to inspect it, opens a Parquet/CSV file, edits
and runs the data cell, runs the plot cell, makes a deliberate post-render
change, reruns, describes, and publishes. It records wire receipts and the
package. There are no pointer coordinates, keyboard injection, test-only
message injection, or remote-desktop steps in this acceptance sequence.
Tests may inject messages to distinguish races and refusals; they are not a
substitute for that sequence. PRODUCT-0161 then consumes these actions to
exercise its own first-run journey.

## Acceptance

1. The live sequence above completes, including a real file and a published
   figure after a deliberate source edit. The same source/action owner is
   exercised by a headless script without a workspace.
2. An edit queued before an agent's replacement causes a generation refusal
   at the engine, leaving the person's edit intact. Wrong buffer, missing
   cell, duplicate cell identity, unsupported language, empty run, excessive
   source, and busy admission refuse explicitly.
3. SQL/Rune errors and file-read errors have final answers; failed work is
   never reported as a refusal or as success. Cell completion waits for its
   own result and, for a plot, its kernel installation.
4. Gate closure/reopening and request-id rules still apply to the new
   actions. Disconnect does not auto-replay or fabricate a known outcome.
5. The full Polariton units, architecture guards, reduced head, workspace
   check, and formatting pass. Guards hold one application dispatch path,
   engine-owned edit validation, and the existing serialization ownership.

## Limits and follow-up

This is notebook authoring over a local file, not arbitrary GUI remote
control. Source bodies are local-agent material and may contain private
queries; none enters PRODUCT-0161 reports. Loading large CSVs uses the
existing Arrow/Polars materialization capabilities and is not an unbounded
streaming-ingest claim. General connection management, creating sessions,
full-source editing, and operating every editor affordance are later
self-driving cuts. The first-run work remains unaccepted until its journey
has been exercised through this surface.
