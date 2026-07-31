# ADR 046 StateDigest Substrate

Owner: `H6`
Depends On: ADR 045

## When To Use This

Use this note when reviewing ADR 046 promotion evidence or when a
future consumer ADR starts using `StateDigest` for lifecycle,
secondary-index, compaction, or anti-entropy state comparison.

ADR 046 itself has no production metric, wire frame, slab footer
field, descriptor field, or control-ring capability bit. It ships a
hash-layer substrate and a benchmark surface only.

## What StateDigest Is

`StateDigest` is a 32-byte compressed Ristretto255 point that
summarizes a multiset of domain-separated state elements. The
accumulator supports add, subtract, and merge operations so later
features can compare or reconcile state without redefining digest
semantics.

The V1 helper is limited to visible table-row membership:

```text
tenant_id || table_object_id || __row_key_id__
```

It proves logical row membership only. It does not prove payload
equality, tombstone policy, retention policy, or secondary-index
entry equality.

## What Must Remain Absent In ADR 046

- No slab-footer or sidecar digest field.
- No `TieredSlab`, `TieredSlabPointer`, catalog, or descriptor
  field.
- No HRFT frame, node capability bit, or reject-code family.
- No `__row_tombstone__` synthetic column.
- No content-hash identity was allocated by ADR 046 itself; ADR 049
  owns `AppendOnlyContentHash`.
- No curve math in `src/network` or `src/storage`.

The architecture test suite enforces these boundaries.

## Benchmark Surface

StateDigest CPU is measured by:

```sh
cargo bench --bench state_digest -- --quick
cargo bench --bench state_digest
```

The bench prints one line per case:

- `per-element`: ergonomic API, one projected element at a time.
- `projection`: BLAKE3 domain-separated scalar derivation.
- `batched-accumulate`: reduced scalar accumulation plus one
  basepoint multiplication.
- `compressed-validation`: compressed digest decompression cost.
- `split-merge`: 16 partition digests merged into one digest.
- `subtract-10-percent`: deletion-algebra smoke shape.
- `lthash-baseline`: 2 KiB reference accumulator for density vs CPU
  comparison only.

The cut-2 reference run is recorded in
`plans/impl/046_state_digest_bench_results.md`.

Cut 3 also adds a test-only multi-row smoke proving the visible digest
does not change when the same logical rows are packed under different
multi-row grouping boundaries.

Council promotion checks the full run against the ADR target:
10k rows within 100 ms, 100k rows within 1 s, and batched
accumulation materially faster than per-element accumulation. If the
numbers miss that band, pause consumer rollout and reopen primitive
or batching review before any durable placement depends on it.

Consumer ADRs that digest large row sets should use
`accumulate_projected_scalars`, not a loop over
`StateDigestAccumulator::add_element`. The ergonomic per-element API
does one basepoint multiplication per element; the batched path folds
scalars and performs one precomputed-basepoint multiplication for the
whole set.

## Operational Interpretation

Until a consumer ADR persists or exchanges these digests, an operator
should not expect dashboard changes. A production cluster with ADR 046
only should look exactly like the pre-046 cluster from the storage and
network telemetry surfaces.

If StateDigest symbols appear in storage metadata, network protocol
code, or capability negotiation before the owning consumer ADR lands,
treat that as a format-surface regression, not an observability issue.
