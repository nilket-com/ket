# check_blake3_confinement comparison fixtures

The Rune port of `check_blake3_confinement.sh` is checked against the original
here. Both run the same `grep` over the same tree, so unlike the ported Python
scripts these two must agree on standard error as well: every line either of
them writes there is its own, not a runtime's diagnostic.

## The trees

| Tree | What it holds |
| --- | --- |
| `violating` | the three boundary files, the recorded exception, two violations, one file reaching BLAKE3 through the boundary module, and one that never mentions it |
| `clean` | only the boundary files and the exception |
| `empty` | a source directory with no mention of BLAKE3 |
| `nosrc` | no source directory at all |

`violating` carries the case that separates a real violation from a false one:
`src/storage/qualified.rs` says `crate::hash::blake3::content(v)`, which is a
use of the boundary rather than of BLAKE3, and must not appear in the
inventory at all.

## Running the comparison

```sh
RNX=/path/to/rnx ./compare.sh
```

It runs the four trees above and then the crate as it actually is, which no
fixture stands in for: it has thirteen real violations today, and both
implementations must report the same thirteen. Every case must agree on
standard output, on standard error, and on the exit code, with no exceptions.
Each invocation is bounded by a timeout that counts as a failure, and the
script works in a temporary directory it removes on the way out.

## Three cases the original has no equivalent for

The original has no capture limit, no timeout, and no interrupt handling, so
there is nothing to compare. These assert instead that the port **refuses**:

| Case | How it is driven |
| --- | --- |
| `truncated` | one file holding a boundary-qualified line larger than the capture limit, then a violation; both lines are in the same file so `grep` emits them in file order |
| `timed-out` | a named pipe nothing writes to, so `grep` blocks, run against a copy of the port differing only in its timeout constant |
| `cancelled` | the same blocked tree, interrupted a second in |

The truncation case first asserts that the **original** reports the violation,
so it cannot pass by there being nothing to find, and the timeout case fails
if the constant it substitutes has moved. Removing any one of the three guards
in the port fails exactly its own case.

The port refuses before printing anything. A violation it never saw looks
exactly like a tree that has none, and a partial inventory would be as
misleading as a clean one.

## Two behaviours of the original that the port matches

A tree with no source directory **passes**. `grep` complains to standard
error, the original does not redirect or inspect it, and the check reports
success with nothing to check. The port forwards the same complaint and
returns the same status.

A tree with no matches passes for the same reason. Neither is a defect the
port is allowed to fix: the comparison is the acceptance bar, and a port that
improved on its original would no longer be comparable to it.

Findings from the exercise are in rnx record 0015.
