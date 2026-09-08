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

It runs from wherever you call it, works in a temporary directory it removes
on the way out, and bounds each invocation with a timeout that counts as a
failure. Set `TIMEOUT` to change the limit from 60 seconds.

Each case checks four things:

- Standard output is identical.
- The exit code is identical, apart from four recorded exceptions.
- The port's standard error equals the expectation in `expected/`.
- Where the original succeeds, neither implementation wrote to standard error.

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
| success | `complete`, `digest-required`, `two-paths` | nothing at all |
| validation failure | `mixed`, `digest-mismatch`, `probe-floor`, `every-floor`, `inline-value` | the bare `error: ` line, below |
| input or usage failure | the other eight | the diagnostic naming what went wrong |

Paths are stripped of any absolute prefix before comparing, so an expectation
recorded here holds from another checkout.

The bare `error: ` line is not something to keep. The original exits 1 with a
complete report on standard output and nothing on standard error. A Rune
script can only exit nonzero by returning an error, and the runner prints
every error, so the port cannot stay silent. The expectation records what the
port does today rather than what it should do; rnx record 0011 asks for an
exit status the script chooses.

## The four recorded exceptions

`unknown-flag`, `flag-without-value`, `value-not-a-number`, and
`value-on-a-switch` are usage errors. `argparse` exits 2 for those. A Rune
script signals failure by returning an error, and the runner exits 1 for
every error, so 2 cannot be expressed. Those cases assert instead that the
original exits 2, the port exits 1, both print nothing to standard output,
and the port prints its recorded diagnostic. If that ever stops being true
the script fails and asks for this note to be updated.

Findings from the exercise are in rnx record 0011.
