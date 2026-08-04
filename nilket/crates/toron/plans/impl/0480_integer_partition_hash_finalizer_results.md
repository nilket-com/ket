# ADR 480 implementation receipt — BLOCKED BY A STABLE-CONTRACT GUARD, NO CHANGE SHIPPED

The profile-directed target was correct and the mechanism worked in isolation
(splitmix64 finalizer, determinism + near-uniform distribution tests green). But it
was **stopped by an architecture guard**, and the guard is right: the partition hash
output is a deliberately pinned stable contract, not a perf knob. No code shipped;
the finding is the result.

## What the guard caught

`exchange_partition_bucket_uses_stable_xxh3_64_encoding{,_for_int64,_for_decimal128}`
(`exec.rs:5770+`) pin the exact `XxHash3_64` partition encoding. ADR 275's comment is
explicit: the Decimal pin exists "so a future serialization change can't silently
re-bucket keys." Replacing the Int32/Int64 hash with splitmix changed that output and
tripped both integer pins.

## Why the guard is load-bearing, not stale

The exchange partition hash feeds the **distributed shuffle** (`distributed/
fragment.rs`: ExchangeId/PartitionId). A stable, cross-process partition function is
what lets build-side and probe-side rows for the same key reach the same partition
regardless of which worker computed the assignment. Changing its output is a
distributed/versioning decision (cross-node agreement, potentially persisted or
resumed partition state), not something to change on a single-node profile reading.
The 15% self-cost the profile flagged **is the XxHash3 call itself** — the very thing
the contract fixes — so the big prize is not addressable without that decision.

## Disposition

- The splitmix implementation is reverted; the tree is exactly the 479 state (spill
  tests 68/0). Nothing shipped.
- The armed sweep and A/B were **not** run: correctness is not the issue; the change
  is out of scope by contract, which a green sweep would not have revealed.

## What remains addressable (smaller, contract-preserving)

The partition **wrappers** (`partition_bucket_with_scratch` +
`partition_bucket_if_non_null_with_scratch`, ~10% combined) carry per-row
`len`/`is_null`/re-downcast overhead that a batch-resolved reader (the 475/477
pattern) could hoist **without changing the hash output**. That is a legitimate
future cut — modest, contract-safe, and it must still be A/B'd under settled load
because 478 showed a single hoist can regress. The XxHash3 replacement itself needs a
separate, deliberate distributed-hashing ADR that owns the cross-version question —
far more than a perf pass.

## The session lesson this reinforces

Profile-first names the *cost*; it does not license the *change*. The largest cost
sat behind a stability contract, and the correct move was to stop at the guard rather
than bulldoze it for a single-primitive gain — the same discipline as grounding a
premise before scoping, one layer down.
