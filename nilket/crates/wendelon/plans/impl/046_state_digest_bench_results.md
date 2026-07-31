# ADR 046 StateDigest Bench Results

Date: 2026-04-28
Command: `cargo bench --bench state_digest`
Host: Linux `slim` 6.17.0-22-generic, x86_64
CPU: 12th Gen Intel Core i9-12900K, 24 logical CPUs
Toolchain: `rustc 1.93.0`, `cargo 1.93.0`

## Summary

ADR 046 clears the cut-2 promotion throughput gate on this host.

| Rows | Per-element | Projection | Batched accumulate | Projection + batched | LtHash baseline |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10k | 70.687 ms | 1.408 ms | 0.452 ms | 1.860 ms | 14.151 ms |
| 100k | 742.050 ms | 13.864 ms | 4.366 ms | 18.230 ms | 142.287 ms |
| 1M | 7.445 s | 139.872 ms | 44.883 ms | 184.755 ms | 1.420 s |

Compressed digest validation stayed below the ADR target:

| Rows | Validation cost |
| ---: | ---: |
| 10k | 2.854 us |
| 100k | 2.874 us |
| 1M | 2.936 us |

## Acceptance Criteria

- Minimum projected-element throughput target: 45-60k/sec/core.
  - Per-element Ristretto path measured 121k-141k/sec across the
    run.
  - Projection plus batched accumulation measured 5.4M/sec at 1M
    rows.
- 10k rows <= 100 ms.
  - Per-element: 70.687 ms.
  - Projection plus batched: 1.860 ms.
- 100k rows <= 1 s.
  - Per-element: 742.050 ms.
  - Projection plus batched: 18.230 ms.
- Batched path >= 2x per-element throughput.
  - 100k rows: 22.9M/sec batched accumulation vs 134.8k/sec
    per-element, before counting projection.
  - Projection plus batched still exceeds per-element by about 40x
    at 100k rows.
- Finish / compressed-validation overhead < 5 us.
  - Worst measured validation cost: 2.936 us.

## 64 MiB Slab Shape

Element count dominates cost. At the measured 1M-row projection plus
batched rate, representative 64 MiB slab sizes estimate to:

| Row width | Rows in 64 MiB | Projection + batched estimate |
| ---: | ---: | ---: |
| 256 B | 262,144 | 48.4 ms |
| 512 B | 131,072 | 24.2 ms |
| 1 KiB | 65,536 | 12.1 ms |

The ergonomic per-element API remains acceptable for smaller or less
latency-sensitive cold-path consumers, but the L2-seal-shaped path
should use projected scalar accumulation.

## LtHash Comparison

The LtHash reference is faster than the per-element Ristretto path but
slower than projection plus batched accumulation on this host. It also
uses a 2 KiB accumulator state, compared with a 32-byte compressed
`StateDigest`. The LtHash baseline measures the add path only; that is
enough for the ADR 046 density-vs-CPU comparison, but a future LtHash
revisit should add subtraction and split/merge measurements. The
benchmark supports the ADR decision to keep LtHash as evidence only,
not as the production state identity.

## Security Review Note

Cut-2 implementation review approves the hash-to-scalar projection for
ADR 046's digest-only use:

- The primitive is not used for signatures, KDF output, key exchange,
  authentication tags, or secrecy-bearing material.
- The scalar input is BLAKE3-256 over a fixed projection tag, explicit
  domain-tag length, domain-tag bytes, preimage length, and preimage
  bytes.
- The modulo bias from `Scalar::from_bytes_mod_order` does not affect
  the required algebraic properties: commutativity, associativity,
  additive merge, and subtraction correctness.
- Domain separation is enforced by golden fixtures and architecture
  tests. Identical preimage bytes under visible-row and physical-row
  domains produce different points.

This approval is scoped to ADR 046. If a future ADR tries to use these
points for a cryptographic protocol rather than state comparison, that
ADR must run a separate review and must not inherit this decision.

## Recommendation

Promote ADR 046's substrate implementation. Durable placement,
anti-entropy exchange, row-tombstone semantics, retention semantics,
and secondary-index preimages remain deferred to their owning ADRs.

## Cut 3 Multi-Row Smoke

Cut 3 added a test-only compaction-shape smoke:
`visible_digest_survives_multirow_repacking_boundaries` in
`tests/state_digest_invariants.rs`.

The test packs the same append-only logical rows with two different
`MultiRowLayoutDescriptor` target-row settings, verifies the grouping
count differs, decodes the resulting frames, extracts the existing
stored `__row_key_id__` metadata, and computes visible-row digest
elements from that metadata. The two visible digests and sorted
row-key sets match.

This validates ADR 046 §5 against the current multi-row machinery
without adding a production storage helper, sealer field, on-disk
shape, or control-ring surface.
