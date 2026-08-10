# impl 0544 — SQL files execute as attributed one-shot requests

Implementation receipt for `plans/adr/0544_sql_file_execution.md`. The predecessor repair
already made the server reject more than one statement in a request. This cut adds the file
source without weakening that boundary: a file becomes ordered client composition over the
existing one-statement request.

## Source and boundary

`sql_planner/script.rs` tokenizes the entire UTF-8 source before runtime or client creation.
Only `Token::SemiColon` ends a statement. Token spans are converted to UTF-8 byte offsets in
one source pass; the resulting exact trimmed slices borrow the one source buffer. The lexical
matrix covers quoted strings and identifiers, escaped strings, both comment forms, adjacent
empty delimiters, Unicode before a delimiter, and Toron's `CREATE LOGICAL TYPE` extension.
An unterminated later literal rejects the whole source before a dead endpoint is touched.

The first architecture run rejected a sibling `sql_script.rs`: ADR 073 quarantines
`sqlparser` under the SQL planner. The implementation moved the splitter beneath
`sql_planner/` and retargeted the guard to that semantic directory. No tokenizer or AST use
was licensed in the CLI, IR, wire, Wendelon, or client crates.

Positional SQL uses the same splitter and locally requires exactly one statement. The server
still checks independently: generic SQL requires one parser AST and the syntax-specific
logical-type fast path cannot lower before its lexical cardinality check succeeds.

## File execution and outcomes

`--file` and `-f` are exclusive SQL sources. Path and source failures—including repeated or
mixed sources, stdin, directory, missing/unreadable input, invalid UTF-8, empty/comment-only
input, and lexical failure—return usage exit 2 before connection. The bounded reader consumes
at most `SQL_STATEMENT_BYTES_HARD_CEILING + 1` bytes and refuses the extra byte without using
path metadata.

One configured `ToronClient` issues each exact slice through `execute_with_timezone`, awaiting
request completion and local decode/render before dispatching the next. A recording mTLS
server holds each response and proves the next connection does not arrive early; it also pins
source order and the authored timezone on every request. File stdout prefixes each successful
existing renderer result with `statement n/N`; positional output remains unchanged.

Failure reporting is phase-aware. A request failure before any completed response retains its
ordinary exit 1 or 2. A completed prefix, or a successful current response followed by local
decode/render failure, returns exit 3. The stderr block carries the exact failing source slice
and explicitly says prior effects were not rolled back. The source is supplied only to that
local `eprintln!` reporter; architecture controls exclude it from audit, tracing, telemetry,
metrics, IR and wire surfaces.

## Real-stack acceptance

Both environment-gated local-profile fixtures were run explicitly, not merely left ignored:

- One `--file` invocation created `file_one` and `file_two`, inserted two and three rows
  respectively, emitted four ordered headings/results, and exited zero. Independent queries
  observed the exact scalar counts 2 and 3.
- A four-statement `-f` invocation created and populated `partial_one`, failed on the exact
  multiline third statement, and exited 3. The two-row prefix remained durable and
  `partial_four` was absent from `information_schema.tables`, proving fail-fast rather than a
  diagnostic-only stop.
- A non-live sibling fails on statement one with ordinary exit 1, no success heading, and no
  partial-application reclassification.

The PRODUCT-0072 evidence now names
`nilket toron sql --file ../uat/demo_percent_stacks.sql` directly; shell command substitution
and shell statement splitting are no longer the blessed loading path.

## Architecture

No sequence logical/execution plan, multi-statement wire message, transaction command,
rollback loop, pipeline, persistent session, or continue-after-error mode was added. The
architecture controls pin ordered one-shot composition, bounded borrowed ingestion, CLI-only
failure disclosure, direct help/UAT spelling, and the retained planner quarantine.

## Verification

The ADR-owned gates are green: Toron library and architecture targets, nilket CLI integration,
workspace all-target compilation with zero warnings, and formatting. Both ignored real-profile
fixtures were also invoked explicitly and passed.

The complete no-fail-fast workspace run reached every target but did not produce a literal zero:
`toron-tpch --bin toron-tpch` exposed an inherited process-environment race between the ADR 449
journal test and the ADR 455 budget-provenance test. Each mutates
`TORON_TPCH_MEMORY_PER_TASK` while Rust's default test runner executes them concurrently. The
failure is bidirectional—the journal observed ADR 455's `"lots"`, while ADR 455 observed the
journal's `268435456`—and neither test nor that crate is changed by ADR 0544. The isolated ADR
449 test and the complete 52-test binary with one test thread pass; four of five additional
default-thread binary runs passed, with the fifth reproducing both sides of the race. This
receipt records the inherited red rather than claiming a full-suite zero or folding an unrelated
harness repair into SQL-file execution.
