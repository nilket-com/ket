# ADR 376 - q07 partition-bucket re-encode churn (named-site perf cut)

## Context

The third q07 perf peel (374 accounting, 375 encode buffer). ADR 375's re-profile named the next stack:
**partition/key churn (`partition_bucket` / `encode_partition_key_value`)**. Two redundancies, confirmed in code:
- `partition_key_hash` (exec.rs:2922) allocates a **fresh `Vec::new()` per call** (2923) - a per-row malloc.
- `select_join_partition_batch` (join.rs:2139) recomputes `partition_bucket` per row, called **per partition** (the
  `for partition in 0..partition_count` loop, join.rs:2019) -> **O(rows x partitions)** encodes/hashes. The exchange
  partitioner (exec.rs:708/1150) is one-pass O(rows) - **out of scope**.

Diagnostic-first from `adr375_q07.perf.data`; scope = partition/key encoding churn only; **no green claim**. Then
reassess parallelism from the cleaned profile.

## Decision

### Phase 0 fork (strict)
- **A - always-in:** reuse a scratch buffer for `partition_key_hash`'s per-call `Vec::new()` **if it is mechanically
  local and parity-safe** (thread a `&mut` scratch through `partition_bucket`/`partition_key_hash`). Eliminates the
  per-row hash-scratch allocation.
- **B - conditional:** precompute/cache the per-row bucket (compute once, reuse across partition passes) **only if
  Phase 0 proves the row-order / key alignment is obvious and testable** (held batch -> hoist a bucket array; re-read ->
  a per-row bucket cache or spill-time bucket store with a clear row identity).
- **B null-key discriminator:** `select_join_partition_batch` currently calls `join_key_bytes(...).is_some()` before
  `partition_bucket(...)` for non-null-safe joins. If B ships, the precompute must produce a single
  `Option<usize>` per row (null-key eligibility + bucket) so it does not keep a hidden second key encode.
- **Stop line:** if bucket caching requires **reshaping spill reads** or **binding cached buckets to page/row identity
  in a way that spreads beyond the named partitioned path**, do **not** force it. Record B as **residual** and reassess
  parallelism after shipping A.

### The cut
Ship A (if local+parity-safe). Ship B only if it clears the Phase-0 fork without crossing the stop line. Scope: the
partitioned-spill re-scan key path only - not the exchange partitioner, q17, q21, or parallelism. Topology-named; no qNN
in core.

## Acceptance (A and B separated)
- **A success:** the fresh per-row hash-scratch allocation is gone (`partition_key_hash` no longer allocates per call).
  If shared `partition_bucket` signature plumbing touches the one-pass exchange call sites, that is mechanical only;
  no exchange algorithm change or exchange profile claim is part of this ADR.
- **B success (if shipped):** bucket computation count drops from **rows x partitions to rows** (asserted by a counter
  or by the profile), and the non-null-safe path computes null eligibility + bucket in one encode pass, or B is
  recorded residual with the stop-line reason.
- **Overall success (profile):** re-profile q07 (373 method) - the `partition_bucket`/`encode_partition_key_value` share
  **drops materially**, and the **next hot stack is named**. Green incidental.
- **Load-bearing parity test:** **precomputed/reused bucket decisions match the recompute** for every row, across
  **nulls, decimal / string / date-ish keys, and multiple partition counts** - through the real partitioned path.
- **Partition semantics unchanged** (same count, same assignment, same producers); q07 small-scale strict parity
  preserved; **no cap bump**; **`--no-fail-fast` required**; de-branded; prior greens unchanged.

## Guardrails
- **A always (if clean); B only past the Phase-0 fork; stop line respected** - residual + reassess, never a forced
  spill-read reshape.
- **Scope = partition/key churn only** - not the exchange partitioner, q17/q21, parallelism.
- **Bucket parity is the correctness gate** - precomputed == recompute across key types + partition counts.
- **No hidden double-encode** - if B ships, the `join_key_bytes` null-check encode must not remain beside the cached
  bucket encode in the partition selector.
- **Profile-based success, not green; no cap bump; --no-fail-fast; de-branded; no qNN.**

## Risks
- **B alignment subtlety** - re-read order vs cached bucket identity; the stop line + the parity test guard it; if
  unclear, ship A only.
- **Over-claiming** - A and B success are separated; overall success is the profile share, not green.
- **Key-type coverage gap** - the parity test mandates nulls + decimal/string/date keys + multiple partition counts.
- **Regression** - `--no-fail-fast` required.

## Forward arc
376 (A + B-or-residual) -> **reassess parallelism** from the cleaned q07 profile: if q07 is now mostly real
spill/read/write/Arrow work, parallelism (the deferred one-thread systemic, 373) is the next lever rather than
peeling further. Else the next named stack. q17 (grouped fold + spill-read), q19 (spill-read I/O), q21 (712-byte
resident-state) remain queued.
