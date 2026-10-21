# ADR 374 - incremental spill resident accounting (first perf cut)

## Context

The first perf-arc cut, named by ADR 373's profile. The resident budget guard recomputes spill-index bytes by **folding
the whole index on every check** - 84-98% of the q04/q07/q17/q21 engine samples (`guard_resident_state` ->
`spilled_batch_join_index_bytes` / `semi_anti_existence_spill_index_bytes`, both `.fold` over the full index, spill.rs:
615/1118/1562/1581). Replace the O(index) re-fold with **O(1) maintained counters**, updated as the index mutates. This
is a **behavior-preserving perf change, not a budget-semantics change**. Parallelism stays deferred (it would only
spread the waste over cores). Success is **not** "cells green" - it is **the resident-accounting stacks disappear and the
next real hot stack is exposed** on re-profile.

## Decision

### The two counters (scope)
- **`SpilledInnerJoinBuild.resident_index_bytes`** for `HashMap<JoinKey, Vec<SpilledBatchRowRef>>` - replaces
  `spilled_batch_join_index_bytes` (q07/q17).
- **`SpilledSemiAntiExistenceBuild.resident_index_bytes`** for `HashMap<JoinKey, Vec<SpilledExistenceRowRef>>` -
  replaces `semi_anti_existence_spill_index_bytes` (q04/q21).

Scope is **these two hot folds only**. The third fold `join_spill_index_bytes` (spill.rs:1548, used by
`resident_without_cache` at 181) is decided in Phase 0 (below), not assumed in.

### Maintenance (exact, matched to the old formula)
Per-entry formula (unchanged): `64 + key_heap_bytes + refs.len() * size_of::<RowRef>()`, saturating (spill.rs:1551-1595).
The counter updates:
- **new-key insert -> `+= 64 + join_key_heap_bytes(&key)`**;
- **each ref append -> `+= size_of::<RowRef>()`**;
- **membership-only duplicate key -> no resident-byte change** (the semi/anti key-only path stores an empty ref vector;
  repeated rows increment row counters but do not add stored refs);
- saturating; `guard_resident_state` reads the counter (O(1)).

### Phase 0 (load-bearing) - mutation-site census + the third fold
1. **Census every mutation site** of each index (new-key insert, ref append, any clear/remove/repartition). The counter
   must be updated at **all** of them; a missed site drifts it and corrupts a budget decision.
2. **The third fold `join_spill_index_bytes`:**
   - **If it is only pre/post-write accounting and not in the sampled hot path -> record it out-of-scope** (cold; left
     as a fold).
   - **If it is called per batch / per mutation in a hot resident guard -> include the same incremental-counter
     treatment, or split it as a named sibling before implementation** - do not leave a hot re-fold unaddressed and do
     not silently fold it in.

### Tests build through real APIs
The bug class is **"map changed but counter not updated"**, so tests must mutate via the **real write/index APIs**, never
by constructing maps directly. Required (at least):
- **new key** adds the base key cost once;
- **duplicate key with a stored ref** adds only the ref cost (no second base cost);
- **duplicate membership-only key** adds zero resident bytes (no second base cost and no ref cost);
- **multiple batches/pages**: counter `==` old fold after **each** write/index step;
- **guard decision equivalence** at under / equal / over cap;
- **any reset/clear/drop/repartition** path either has no reusable counter state or resets it explicitly.

## Acceptance
- **Counter-vs-old-fold equivalence** via the real mutation APIs (not direct map construction), checked after each
  write/index step - the correctness gate.
- **Guard-decision unchanged** at under/equal/over cap (accept/spill/reject identical to the fold).
- **Reset/clear/drop/repartition** paths verified: counter state is absent or explicitly reset (no stale carryover).
- **Third fold resolved:** `join_spill_index_bytes` recorded out-of-scope (cold) OR given the same treatment / named
  sibling - per the Phase 0 census, decided before implementation.
- **Re-profile q04/q07/q17/q21** (373 method): the resident-accounting stacks **disappear**; the **next hot stack** per
  cell is recorded (the next evidence-backed perf cut). This is the success metric, not cells-green; cells may green or
  advance.
- **No cap bump; budget producers/names unchanged** (perf cut); **`--no-fail-fast` required**; de-branded; no qNN in
  core; prior greens (q01/q06/q13/q14/q15/q16/q20) unchanged.

## Guardrails
- **Maintain at every mutation site** - the Phase 0 census is the foundation; a missed site drifts the counter.
- **Counter == old fold** - exact formula + saturation; equivalence tested through real APIs after each step.
- **Behavior-preserving** - same budget decisions, same producers/caps; this is perf, not semantics.
- **Third fold decided, not assumed** - in-scope only if Phase 0 proves it hot.
- **Success = stacks-gone + next-stack** (re-profile), not green. No cap change; --no-fail-fast; de-branded.

## Risks
- **Missed mutation site (drift)** - the headline risk; the census + per-step equivalence + real-API tests guard it. A
  drift undercount => silent OOM; overcount => false reject.
- **Direct-construction tests miss the bug class** - mandated to use the real APIs.
- **Reset/repartition stale counter** - explicit reset test.
- **Third fold left hot** - Phase 0 resolves it; not silently folded in or ignored.
- **Saturation mismatch at extremes** - unrealistic per-entry overflow; the formula matches and tests cover realistic
  sizes.

## Forward arc
374 (incremental accounting) -> re-profile -> the next hot stack drives the next perf cut per cell -> ... -> when the
algorithmic waste is gone, **parallelism** (the deeper one-thread systemic) -> q19 spill-read/decrypt/decompress I/O
(separate) -> harness-rehash `stable_file_hash` (separate, outside execute_ms). The profile evidence drives each cut.
