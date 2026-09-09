# verify_nilket_graft comparison fixtures

The Rune port of `verify_nilket_graft.py` is checked against the original
here.

The real inputs to this migration were not kept: the frozen source repository
and the commit map it produced are both gone. So `build.sh` builds a lineage
with exactly the relationship the verifier checks, and every case mutates a
fresh copy of it.

## What `build.sh` makes

Three source commits, and a target whose every commit wraps the source tree
under a single `nilket` entry, carries the same author and committer, maps its
parents, and has no signature. The tip of the source is rewritten by hand to
carry a `gpgsig` header, so the case where a rewrite drops a signature is
reached without needing a key.

The build is deterministic. That is what lets a diagnostic naming a commit be
recorded in `expected/` rather than matched loosely.

## Running the comparison

```sh
RNX=/path/to/rnx ./compare.sh
```

Nine cases. Eight are built: the verified lineage, wrong arguments, a message
the rewrite did not preserve, a wrapper tree under the wrong name, a rewritten
commit that carries a signature, a commit map missing an entry, and the same
map written with tabs and with carriage returns at the ends of its lines. The
original splits a map line on any whitespace, so all three spellings say the
same thing.

Every case must agree on standard output and on the exit code. A killed
invocation is reported as a timeout rather than compared, because two sides
that both time out would otherwise look like agreement.

The ninth uses the real commit map and this repository as the target, with a
source that no longer exists. It proves one thing: that a missing source is
refused, and that both refuse it alike. It is **not** a check of the map or of
the target, because the port resolves the source path before it opens the map
or looks at the target, so neither is ever read.

The port's standard error is compared against `expected/NAME.err`, and the
original's is not compared at all. The two say different things by design:
the original raises `AssertionError` with a commit hash, and the port says
what differs. Only one of those is worth pinning.

`RECORD=1 ./compare.sh` rewrites the expectations. Read the diff before
keeping it: a recorded expectation is a claim about what the port should say.

Findings from the exercise are in rnx record 0017.
