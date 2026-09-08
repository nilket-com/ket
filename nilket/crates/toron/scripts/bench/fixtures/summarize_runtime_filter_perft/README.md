# summarize_runtime_filter_perft comparison fixtures

The Rune port of `summarize_runtime_filter_perft.py` is checked against the
original here. Both read the same rows and must print the same report.

`rows.log` is real bench output, not a hand-made sample: 25 rows lifted from
the two results records that the bench produced.

    plans/impl/0004_distributed_inner_equi_join_bench_results.md
    plans/impl/0006_runtime_filter_pushdown_bench_results.md

The 0004-era rows predate the `probe_key_domain` field and do not carry a
full grid of filter settings, so most of their groups report no delta and the
run exits 1. That is the input as it really is, and it exercises both the
complete and the incomplete path in one file. The rest of the corpus is
derived from it:

| File | What it is |
| --- | --- |
| `rows.log` | all 25 real rows; 2 complete groups, 9 incomplete |
| `rows_complete.log` | the 8 rows whose groups have all four filter settings |
| `rows_digest_mismatch.log` | `rows_complete.log` with one digest altered |
| `rows_not_numeric.log` | `rows_complete.log` with one `wall_ms` set to `oops` |
| `rows_missing_field.log` | `rows_complete.log` with one `wall_ms` removed |
| `empty.log` | no rows at all |

## Running the comparison

```sh
RNX=/path/to/rnx ./compare.sh
```

It runs nineteen cases from wherever you call it, works in a temporary
directory it removes on the way out, and bounds each invocation with a timeout
that counts as a failure. Set `TIMEOUT` to change the limit from 60 seconds.

Three cases pipe a file in and pass no path, which is how both implementations
are asked to read standard input. A terminal on standard input is refused by
the port and would block the original, so there is nothing to compare and no
case for it here; rnx tests that refusal itself, in `tests/stdin.rs` and in
its session gate.

Each case checks four things:

- Standard output is identical.
- The exit code is identical, with no exceptions.
- The port's standard error equals the expectation in `expected/`.
- Where the original says nothing, the port says nothing.

## Why the diagnostics are checked and not just reported

An earlier version of this script printed whether standard error differed and
never failed on it. That column could not fail, so it was not a check. A
runner substituted for `rnx` that preserved standard output and the exit
status, threw every diagnostic away, and printed one fixed line instead
passed the gate.

Now each case has a recorded expectation in `expected/NAME.err`, in three
groups:

| Group | Cases | Expected |
| --- | --- | --- |
| success | `complete`, `digest-required`, `two-paths`, `piped-complete` | nothing at all |
| validation failure | `mixed`, `digest-mismatch`, `probe-floor`, `every-floor`, `inline-value`, `piped-rows` | nothing at all: the report is on standard output |
| input or usage failure | the other nine | the diagnostic naming what went wrong |

Paths are stripped of any absolute prefix before comparing, so an expectation
recorded here holds from another checkout.

Ten of the nineteen expect nothing at all from the port, which is what the
original says in those cases too.

## There are no recorded exceptions any more

There used to be four. `unknown-flag`, `flag-without-value`,
`value-not-a-number`, and `value-on-a-switch` are usage errors, `argparse`
exits 2 for those, and a Rune script could only exit 1, because failure meant
returning an error and every error exited 1. rnx record 0013 gave a script its
own exit status. The port now exits 2 as the original does, and the
exceptions went with it.

The same record removed the bare `error: ` line. Six cases fail with a
complete report on standard output and nothing on standard error, exactly as
the original does, and the gate now requires that: where the original says
nothing, the port says nothing.

Findings from the exercise are in rnx records 0011, 0012, and 0013.
