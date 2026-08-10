# ADR 544 - SQL file execution with explicit partial application

**Status:** Proposed 2026-08-10.

The first live PRODUCT-0072 exhibit needed two relations and four SQL statements.
The natural command appeared to work:

```text
nilket toron sql "$(<demo_percent_stacks.sql)"
OK
```

It had executed only the first `CREATE TABLE`. The next three statements were
silently ignored, leaving an empty first table and no second table. Polariton
reported the missing relation only when the Rune buffer ran.

This is not merely absent file convenience. `SqlPlanner::plan`
(`sql_planner.rs:2237-2250`) calls `Parser::parse_sql`, accepts the resulting
vector, and lowers `ast[0]` without checking its length. ADR 079 already found
this class and closed it for `ctx.sql` through `parse_one_statement`; the
terminal SQL entry point remained permissive. A direct client can therefore
send several statements in one execute frame and receive success for a prefix
whose remainder never ran. That live correctness defect is repaired first in
its own red-first `fix:` commit, independent of this feature: ordinary SQL and
the `CREATE LOGICAL TYPE` fast path both refuse a second lexical statement
before lowering anything.

ADR 043 D17 made one statement per CLI invocation deliberate: no persistent
session, pipelining, transaction state, or multi-plan wire message. This record
partially supersedes only the *invocation* part of that decision. One file-mode
invocation may issue several ordered one-shot requests. Every wire request and
every planner call still contains exactly one statement.

## Decision

### 1. `--file` is a second, exclusive SQL source

Toron accepts:

```text
nilket toron sql --file <path>
nilket toron sql -f <path>
```

Endpoint, timezone, and TLS flags retain their current meanings and apply to
every statement in the file. `--file`/`-f` is accepted exactly once and is
mutually exclusive with positional SQL. A missing path, repeated file flag,
file plus positional text, unreadable path, directory, non-UTF-8 input, and the
special path `-` all refuse before any connection or execution. Standard input
is not introduced by this record.

An empty or comments-only file refuses rather than reporting a vacuous success.
The root `nilket` dispatcher continues to delegate the complete argument vector
to Toron; no duplicate file parsing enters the root crate.

### 2. Statement boundaries are lexical, source-preserving, and shared

A reusable Toron SQL-script splitter tokenizes the complete UTF-8 source using
the existing `sqlparser` tokenizer and the same generic lexical posture as the
planner. Only a real `Token::SemiColon` ends a statement. Semicolons inside
single-quoted strings, quoted identifiers, escaped strings, and line or block
comments never split. Empty delimiters and comment-only slices do not become
statements.

The tokenizer finds boundaries; it does not become a second SQL frontend. Each
result is the trimmed slice of the original source, with its terminating
delimiter removed. The client does not render an AST back into SQL. This keeps
comments, literal spelling, Toron-specific `CREATE LOGICAL TYPE` syntax, and the
exact failing text available without requiring the generic parser to understand
every Toron extension.

The whole file is tokenized before the first request. An unterminated quote,
comment, or other lexical failure therefore applies nothing. Full syntactic,
schema, and execution validation remains statement-local at the authoritative
server; pre-planning the entire file is rejected because it would duplicate the
frontend and reject Toron-specific syntax.

### 3. One statement is enforced at every ordinary entry

Positional `nilket toron sql <statement...>` runs the same splitter and requires
exactly one nonempty statement. More than one refuses locally before connecting.

`SqlPlanner::plan` independently requires exactly one statement before either
ordinary AST lowering or the `try_lower_create_logical_type` fast path. This is
the server-side boundary for direct and future clients; client validation is not
trusted as authority. `plan_relation` retains its existing one-statement check.

The predecessor repair uses the stable server refusal:

```text
Toron SQL execution accepts exactly one statement per request
```

It does not advertise a CLI feature that did not exist when that repair
shipped. Once this record lands, positional CLI validation adds the actionable
suffix:

```text
Toron SQL execution accepts exactly one statement per request; use `nilket toron sql --file <path>` for a script
```

A trailing delimiter is still one statement. The current behavior and output
of every accepted positional command remain byte-identical.

### 4. Files execute sequentially and stop at the first failure

After successful lexical splitting, file statements are sent in source order,
one request at a time. Statement `n + 1` is never dispatched before statement
`n` completes and its response is decoded and rendered. The same endpoint, TLS
configuration, and session timezone are used for the whole run.

The first connection, transport, server, result-decoding, or rendering failure
stops the file. Later statements are never sent. There is no
`--continue-on-error` mode.

File mode does not add `LogicalPlan::Sequence`, an execution-plan sequence, a
multi-statement wire frame, prepared statements, pipelining, or a persistent SQL
session. Reusing one configured `ToronClient` value is an implementation detail,
not session state; each `execute_with_timezone` remains an independent request.

### 5. Prior success is never presented as rollback

Toron has no `START TRANSACTION`, `COMMIT`, or rollback handling. Successful
DDL/DML requests before a later failure may already be durable. File mode neither
undoes them nor claims atomicity.

On failure, stderr carries all of the information needed to understand the
partial application:

```text
toron sql --file demo.sql: statement 3/4 failed after 2 completed statements; prior effects were not rolled back
--- failing SQL ---
INSERT INTO missing VALUES (1)
--- end failing SQL ---
toron sql request failed: Toron CatalogError: ...
```

The SQL between the markers is the exact trimmed source slice sent for that
statement, not an AST reconstruction or a one-line summary. The statement index
alone does not discharge the diagnostic. The ordinary typed error and its
existing exit-code classification follow the context unchanged when no earlier
statement is known to have completed.

Exit code `3` is the machine-readable `SQL_FILE_PARTIAL_APPLICATION` outcome. It
is returned whenever at least one earlier statement received a successful
server response before a later statement failed, or when the current statement
received a successful response but local decoding/rendering then failed. This
code says a prefix is known to have completed; it does not claim that prefix
mutated data. The underlying typed cause remains on stderr. If no response has
completed, the ordinary exit code remains, and a transport failure after
dispatch may still leave the current statement indeterminate. This record does
not convert lack of knowledge into a claim that nothing applied.

Fail-fast makes re-running a partially applied, non-idempotent fixture manual.
That cost is accepted and visible. If Toron later gains transaction statements,
the relationship between file boundaries and transaction state becomes a new
decision; this record does not simulate it client-side.

### 6. File output is attributed; positional output is unchanged

Before each successful file-mode result, stdout prints:

```text
statement 1/4
```

The existing result renderer follows unchanged: `OK`, `OK rows=<n>`, or the
pretty table plus row count. A statement that fails gets no success heading;
its index appears in the failure block. The heading makes four identical `OK`
lines attributable without changing the one-shot surface.

No summary line claims the file committed. Successful completion means every
independent request completed successfully.

### 7. File ingestion is bounded

`MAX_SQL_FILE_BYTES` is the existing
`SQL_STATEMENT_BYTES_HARD_CEILING` (64 MiB). The reader consumes at most the
ceiling plus one byte rather than trusting path metadata; larger input refuses
before tokenization or execution. This is a client-source bound, not a promise
that a remote listener accepts a statement of that size. Each statement remains
subject to the server's configured `sql.max_statement_bytes` and request-frame
limits.

The source buffer and statement slices are the only file-sized allocations.
The implementation must not clone the complete file once per statement.

### 8. Documentation uses the feature that ships

Toron help names both accepted forms and their exclusivity. The PRODUCT-0072 UAT
fixture instruction becomes:

```text
nilket toron sql --file ../uat/demo_percent_stacks.sql
```

No shell command substitution or semicolon-splitting loop remains as the blessed
way to load a fixture.

## Acceptance

1. **Real four-statement file.** An integration fixture creates two tables and
   inserts different row counts into both through one `--file` invocation. The
   command emits four ordered headings/results, exits zero, and independent
   queries observe both exact counts. This is the PRODUCT-0072 failure shape,
   not a parser-only test.
2. **Fail-fast partial application.** A four-statement file succeeds twice,
   fails third, and has a valid fourth statement. The command exits with
   `SQL_FILE_PARTIAL_APPLICATION` (`3`); stderr names `statement 3/4`, says two
   statements completed without rollback, includes the underlying typed cause,
   and includes the exact multiline third source slice. A subsequent query proves
   statement two's effect remains and statement four never ran. A sibling whose
   first statement fails retains the ordinary underlying exit classification,
   proving the partial code is evidence-driven rather than a generic file error.
3. **Lexical anti-vacuity matrix.** Strings and quoted identifiers containing
   semicolons, `--` and `/* ... */` comments containing semicolons, adjacent
   empty delimiters, Unicode before a boundary, and a Toron-specific
   `CREATE LOGICAL TYPE` statement split into the intended exact source slices.
   Controls prove a genuine delimiter does split. An unterminated literal later
   in the file refuses before a mock client observes any request.
4. **Silent truncation closes twice.** Positional CLI input containing two
   statements refuses before connecting, and a direct `SqlPlanner::plan` call
   containing two statements refuses rather than lowering the first. A second
   planner fixture starts with `CREATE LOGICAL TYPE` so the fast path cannot
   bypass the invariant. No catalog mutation occurs in the integration control.
5. **Single-statement parity.** Existing accepted positional SQL commands retain
   byte-identical stdout, stderr, exit codes, endpoint selection, timezone, and
   TLS behavior. A trailing semicolon remains admitted.
6. **Source selection.** `--file` and `-f` each work. Missing, repeated, mixed
   positional/file, `-`, directory, unreadable, non-UTF-8, empty, and
   comments-only sources refuse with stable usage diagnostics and zero requests.
7. **Options reach every request.** A recording client receives every file
   statement in order with the same authored endpoint/TLS selection and exact
   timezone. No statement is dispatched concurrently.
8. **Output attribution.** DDL/DML and query results retain their existing
   renderer beneath ordered `statement n/N` headings. A failure prints no
   success heading for the failed statement and never prints a file-level
   `committed` claim.
9. **Bounded read.** A parameterized reader accepts exactly its supplied byte
   limit, refuses limit plus one before tokenization, and does not rely on file
   metadata. A constant assertion pins production to the 64 MiB hard ceiling
   without allocating it in the test.
10. **No transaction-shaped or persistent-disclosure mechanism.** Architecture
    gates prove no sequence logical/execution plan, multi-statement wire variant,
    transaction command, rollback loop, or `continue-on-error` option enters this
    cut. They also prove the exact failing source slice is written only by the CLI
    stderr renderer and never supplied to tracing, audit events, telemetry fields,
    or metric labels.
11. **Help and dogfood.** Toron help documents `--file`; the UAT fixture names
    the direct command and contains no shell statement splitter.
12. **Complete battery.** Toron unit/integration/architecture tests, nilket CLI
    integration, formatting, and the full workspace `--no-fail-fast` sweep pass.

## Guardrails

- The server planner refuses multiple statements independently of every client,
  and no syntax-specific fast path may lower a plan unless its statement-cardinality
  check has passed.
- Exit code `3` requires evidence of a completed response. It is never used merely
  because the source was a file, and an ordinary failure is never described as
  proof that the current statement did not commit.
- The exact authored failing slice is a local, ephemeral CLI stderr diagnostic
  only. It must not enter tracing, audit records, telemetry, metric labels, or any
  persisted evidence artifact.
- File execution remains ordered one-shot requests. No transaction, rollback,
  sequence plan, pipeline, or continue-after-error mechanism may hide beneath it.

## Rejected

- **Send the whole file in one execute frame:** that is today's silent-prefix
  defect, not file execution.
- **Split on `;` in the shell or with `str::split`:** breaks on literals,
  identifiers, and comments; it was sufficient only to repair one live fixture
  manually.
- **Parse and re-render every AST:** rejects Toron-specific grammar before the
  authoritative planner sees it and loses exact authored text.
- **Pre-plan every statement before execution:** duplicates server authority and
  still cannot guarantee later schema validity without applying earlier DDL.
- **Client-side transaction emulation:** no server transaction exists; rollback
  claims would be fictional.
- **Continue after error:** maximizes partial and causally-confused state; no
  exhibit asks for batch migration behavior.
- **Reuse the ordinary failure exit for a known applied prefix:** hides the one
  state automation most needs to distinguish and forces scripts to parse human
  stderr to learn whether recovery is required.
- **Implicitly infer a file from one positional path:** SQL text and filesystem
  paths remain distinguishable by syntax, not existence checks.
- **Standard input in this cut:** useful, but it adds streaming/TTY and source-
  naming decisions that the motivating file does not require.

## Consequences

- A multi-statement file becomes convenient without making the wire or planner
  multi-statement.
- The CLI can no longer report success while silently discarding a suffix.
- A later failure can leave a valid applied prefix; the diagnostic makes that
  state explicit and reloading remains the author's responsibility.
- The exact failing SQL may contain authored literal values and therefore appears
  on stderr by explicit request. Operators who collect CLI stderr inherit that
  disclosure; redaction would contradict the diagnostic chosen here and needs a
  separate mode if an exhibit demands it.
- ADR 043's one-shot transport, no-session, and no-transaction decisions remain;
  only one-statement-per-*CLI-invocation* is superseded.

## Stop conditions

- If the existing tokenizer cannot identify exact source boundaries across the
  SQL forms Toron accepts, halt rather than ship a naïve delimiter scan or an
  AST-rendered substitute.
- If file execution requires a new wire message, planner sequence, or persistent
  server session, halt: the proposed client composition is not the mechanism
  described here.
- If a failure path cannot return its existing typed error without printing it
  inside the one-shot helper, refactor the helper first; do not string-match
  stderr to recover classification.
- If 0543 introduces overlapping SQL CLI or planner changes, rebase and re-ground
  this record after 0543 rather than guessing at a concurrent seam.
