# ADR 479 - fixed-width fast path for partition-key hashing

m06 was profiled before any code change (ADR 478's lesson). The owner is **not** the
join proper — hash lookup and match-set building barely register. It is
**partition-key hashing**: `partition_bucket_with_scratch` +
`partition_bucket_if_non_null_with_scratch` are ~24% of m06 self-time and
`encode_partition_key_value` another 7%. For a single fixed-width integer key
(m06 joins on `l_orderkey`/`o_orderkey`, both Int64) the path serializes a tag byte
plus 8 little-endian bytes into a `Vec<u8>` and runs `XxHash3_64::oneshot` on the
9-byte buffer — per row. This cut hashes the integer directly.

## Why this is not ADR 478 again

478 wrapped a single, predictable downcast and lost — no real work removed. Here the
work removed is real: a `Vec<u8>` build (`clear` + tag push + `extend_from_slice`)
plus a byte-slice hash, replaced by hashing the raw fixed-width value. The profile
attributes ~31% of m06 to this path, and the serialization is redundant computation,
not a branch the compiler already predicts. The A/B will still decide it — but the
mechanism is a different kind.

## The invariant that must not break

Build rows and probe rows must land in the **same** partition for the join to find
matches. Both sides call the same partition function, so a new hashing scheme is safe
**iff build and probe use it identically** — the fast path must be reached on both
sides for a given key type, or not at all. The cut adds the fast path inside the
shared function so both sides get it by construction; it never changes one call site
without the other.

## Decision

1. In `partition_key_hash*_with_scratch`, when `key_arrays` is a single column of a
   supported fixed-width integer type (Int32/Int64, matching the existing
   `JoinKeyPlan` fast-path set), hash the value directly (a stable, documented
   encoding — the raw value fed to `XxHash3_64`), bypassing the `Vec<u8>` path.
   All other shapes (multi-column, strings, decimals, floats) keep the exact
   current serialize-then-hash path.
2. Null handling unchanged: the `_if_non_null` variant still returns `None` on a
   null key; the non-null variant still encodes a null marker for the generic path.
3. The fast path lives inside the shared partition function so build and probe are
   guaranteed consistent; a divergence would be a correctness defect, not a
   perf regression.

## Acceptance

- **Correctness is the gate, not speed.** The armed 22-query parquet sweep green,
  full engine suite green, architecture green — a join that loses matches because
  build and probe partitioned differently would surface as a wrong answer, and the
  sweep is what catches it.
- A unit test pins that the fast path and the generic path place the **same rows in
  the same partitions** for Int32/Int64 keys across a range of values including
  negatives and zero (internal consistency is all correctness needs; matching the
  old bucket numbers is not required, but same-scheme determinism is).
- **A/B under shared load, 478 method**: commit the candidate, build 478-tip and 479
  binaries, alternate them on m06 back-to-back, six paired rounds. The verdict is the
  paired median delta, not a single ladder run.
- Full ladder recorded after, to confirm no untouched primitive regressed.

## Guardrails

- **Commit before A/B** (478's process lesson): the candidate is committed so a
  sibling-binary build can never discard it.
- If the paired A/B shows no gain or a regression, revert and record — the profile
  said ~31%, but the hash may already be cheap enough that serialization is not the
  real cost; the A/B decides.
- No change to partition count, spill format, page layout, or the join algorithm.

## Forward

If the fast path wins, the remaining m06 cost (lookup closure, index insert, page
fetch) is the next target. If it does not, the profile's ~31% is re-attributed and
the buffer build was cheaper than it looked.
