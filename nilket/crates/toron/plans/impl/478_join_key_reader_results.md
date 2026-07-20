# ADR 478 implementation receipt — MEASURED, REVERTED

The join-key hoist was implemented, verified correct, measured **1.7% slower** under
a controlled A/B, and reverted. The finding stands; the code does not.

## The measurement (the part that mattered)

The first ladder run showed every primitive ~14% slower — including m02/m09, which
this cut never touched — a moving-baseline artifact from higher machine load, not a
verdict. A code change this small cannot be judged against a shifted baseline.

Controlled A/B: the 477 and 478 binaries built separately, then run **alternating,
back-to-back, under identical load**, m06 at sf=8, six paired rounds:

| round | 477 | 478 | Δ |
|---|---:|---:|---:|
| 1–6 medians | 22,634 ms | 23,026 ms | **+1.7%** |

All six paired deltas positive (+262 to +480 ms), distributions non-overlapping.
The regression is real, small, and consistent.

## Why the family pattern did not transfer

ADR 475/477 removed 11- and 12-branch `downcast_ref` chains — large per-row costs.
`join_key_at` was a **single** downcast, cheap enough that the compiler already
handled it well. Wrapping it in a `JoinKeyReader` enum added a per-row match on the
reader variant that costs more than the one downcast it replaced. And m06's real
cost is not key extraction: at ~534 ns/probe-row it lives in the hash lookup, page
fetch, and match-set construction — none of which this cut touched.

## Process note (recorded, not hidden)

Building the 477 comparison binary meant `git checkout <file>` on the join sources,
which discarded the uncommitted 478 working tree — the exact hazard the standing
"never checkout a file to revert a probe" rule names. The impl was never committed,
so it was not in git; it was reconstructed from the transcript and the deterministic
hoist script, and the rebuilt binary's SHA256 matched the saved 478 binary exactly
(`c0dad067…`) before anything else proceeded. Lesson reinforced: commit a cut's code
before building an A/B counterpart from a sibling revision.

## What survives

- m06's 18.6× is **not** in key extraction. The next join cut targets the probe
  path proper (hash lookup, page fetch, match-set building), chosen by profiling m06
  the way ADR 474 profiled m02 — not by assuming the aggregate family pattern
  repeats.
- The ladder's A/B-under-shared-load method is the durable takeaway: small changes
  near machine-noise magnitude require alternating binaries, not sequential runs.
