# ADR 437 - Implementation plan: order-preserving sole compact group key

Pairs with `plans/adr/0437_compact_fixed_width_lookup_key.md`. This replaces the measured-but-rejected
first implementation, which retained compact and full keys together.

## Phase 0 - order-faithful codec

1. Keep the 16-byte layout admission boundary and exact generic fallback.
2. Change admitted payloads to lexicographically order-preserving encodings for the existing derived
   `GroupValue::Ord`: null marker, Boolean byte, sign-flipped big-endian signed values, and big-endian
   canonical unsigned float bits. Fully zero unused bytes.
3. Add per-column direct Arrow decoders. Validate the caller's `DataType` against the configured
   layout, preserve UTC timestamp metadata, and never reconstruct a full `Vec<GroupValue>` key.
4. Lock both Eq-faithfulness and order-faithfulness pairwise over nulls, signed extrema, canonical
   signed zero / NaN, timestamp units, and within-width multi-column boundaries.

## Phase 1 - compact-only retained state

1. Remove the full-key constructor from `entry_or_init_compact`. An admitted entry stores only its
   compact key and aggregate states.
2. Make table length and alignment route-aware. Generic mode owns full keys + states; compact mode
   owns compact keys + states. Wrong-route operations remain loud.
3. In compact mode, growth reserves only compact keys, states, and buckets. Rehash uses compact keys.
   Generic growth remains the ADR 435 path.
4. Re-derive current and transient rehash-peak accounting for both routes. Forced compact rejection
   must precede state construction and preserve every table vector and byte counter.

## Phase 2 - route-aware borrowed finalization

1. Replace the one borrowed full-key entry vector with a finalized-result enum:
   - generic borrowed `(full_key, states)` entries sorted by `GroupValue::Ord`;
   - compact borrowed `(compact_key, states)` entries sorted by compact lexicographic order.
2. Expose route-neutral length/state access for COUNT/SUM/MIN/MAX/AVG output.
3. Build group columns through the finalized result. Generic mode uses the existing full-key column
   builder; compact mode decodes each column directly into Arrow arrays.
4. Remove the two production callers' `entry.key.to_vec()` copies. Preserve empty-input behavior and
   exact raw batch order.
5. Guard finalize-sort scratch before allocation and record it separately from table ownership.

## Phase 3 - receipt and architecture rails

1. Version the grouped-fold profile to schema v4. Record lookup route, finalize route, retained compact
   keys, retained full keys, compact row encodes, full-key materializations, and direct compact-finalize
   rows. Retain the four probe phases and exact ADR 436 reconciliation identities.
2. Keep disabled profiling state/sink-free and preserve output and telemetry parity.
3. Extend the architecture test to reject a compact-mode full-key constructor or retained full-key
   insertion and require direct compact finalization. Keep query branding forbidden.

## Tests

- Pairwise Eq and `Ord` parity for every admitted codec, including null/non-null boundaries,
  negative/zero/positive signed values, canonical float bit ordering, timestamp units, and
  multi-column lexicographic boundaries.
- Exact compact decoder arrays and raw sorted output batches for all admitted types.
- Decimal128, UTF-8, and over-width fallback parity.
- Compact occupied/vacant lookup retains zero full keys; generic lookup remains unchanged.
- Compact and generic forced-growth rejection preserve state and exact incremental/recomputed bytes.
- Route-aware finalization preserves key/state alignment across all aggregate kinds.
- Schema-v4 reconciliation, loud receipt publication, disabled parity, and the prospective mechanical
  event-floor rule.
- Existing deep-recursion and streamed/materialized grouped-fold parity guards.

## Live proof

1. Run one schema-v4 q13 receipt and prove zero retained/full-key materializations on both compact
   folds. Confirm final current ownership and projected rehash peak are below the ADR 435 baselines.
2. Run five fresh schema-v4 ledger children. Require exact reconciliation, zero overflow, and every
   mechanically selected tail-rate spread at most 0.25.
3. Run five fresh symbol-rich, disabled-ledger q13 profiles under the ADR 356 compute envelope. Require
   invariant outputs, zero lost samples, and an inclusive lookup/equality frame materially below ADR
   436's old range.
4. Run one warmup plus seven unprofiled q13 rows. Require invariant outputs, spread at most 0.20, and
   median improvement beyond 10% against 22,257.249 ms. Use at most one retained retry.
5. Re-enumerate the compact subset from schema-v4 receipts and run persisted strict comparison for
   every admitted cell. Run q13 once under 4 GiB/no swap with clean scratch.

## Remove-on-failure rule

If final current ownership or rehash peak does not beat ADR 435, the q13 median does not clear 10%, or
any event-floored tail gate remains red, remove the compact mechanism and restore the sealed ADR
436/435 source. Preserve the rejected receipts in the results document; do not loosen a threshold or
bank a partial win.

## Seal gate

Run formatting, `cargo check --all-targets`, Toron library tests, the full architecture suite, the
serial whole-workspace no-fail-fast suite, toron-tpch including Polars, `git diff --check`, the
`exec.rs` line ratchet, and the tracked `.perf.data` rejection. Write the revised results receipt
before any implementation seal.
