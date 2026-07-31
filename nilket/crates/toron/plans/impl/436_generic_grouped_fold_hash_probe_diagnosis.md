# ADR 436 - Implementation plan: generic grouped-fold hash-probe throughput diagnosis

Pairs with `plans/adr/436_generic_grouped_fold_hash_probe_diagnosis.md`. **Diagnosis-only** — no engine
behavior change, no mechanism, no speed claim. perf names the cycle category across 5 fresh children; a
required parity-locked ledger measures the four-phase × cost geometry and the matching-vs-collision
equality split, reconciling exactly to q13's inner-fold operation domains. Deliverable: a named producer
+ the ADR 437 mechanism selection.

## Phase 0 - external perf, 5 fresh children

1. Symbol-rich `perf record` on **5 fresh q13 children** under the exact 356 compute profile
   (`AllowedCPUs=0-1`, `CPUQuota=150%`, `MemoryHigh/MemoryMax=infinity`, `TasksMax=512`,
   `RuntimeMaxSec=3600`); each child re-seeds `RandomState` per table instance.
2. Confirm the **sampled cycle-category** (equality vs hash vs residual-body) share and its stability
   across the 5 seeds. Record that perf establishes category stability **only** — probe-tail stability
   comes from the ledger. All perf timing is diagnostic overhead, excluded from any speed comparison.

## Phase 1 - required parity-locked probe ledger

1. Extend the off-by-default generic-fold receipt (env-gated; disabled → no sink/state, locked parity)
   with a **phases × costs** model for `find_bucket_in`:
   - phases: primary-occupied, primary-vacant, vacant-placement, rehash;
   - per phase: hash calls, bucket visits, equality invocations, matching equalities, collision
     mismatches, and a **probe-length histogram** (exact bounded bins + one overflow bucket; no stored
     samples, no per-row timers).
2. **Upstream key construction** (`Vec<GroupValue>` build/alloc) is a **separate line**, not attributed
   to `find_bucket_in`.
3. **Geometry:** emit a **per-growth-epoch** probe histogram and a **final occupied-cluster run-length**
   histogram.
4. **Overflow rule:** if the histogram overflow bucket reaches **0.1% of operations**, widen the bins
   and rerun; p99.9 must be *measured*, not *bounded*.

## Reconciliation self-checks (q13 inner fold; outer fold separate)

The ledger asserts, and the results doc reports:

- total calls = total hash computations = **18,933,731** (15,337,603 primary + 1,500,000 vacant
  placement + 2,096,128 rehash);
- primary-occupied matching equalities = occupied-lookup count (exactly one each);
- primary-vacant, vacant-placement, rehash matching equalities = 0;
- collision mismatches = equality invocations − matching equalities (per phase);
- vacant-placement & rehash: equality invocations = bucket visits − calls (terminal empty bucket does no
  equality);
- the 46-group outer fold is reported on its own, never pooled into these domains.

A domain that fails to reconcile invalidates the receipt.

## Stability gate (locked, per-child)

- N = 5; per-child results gate; pooled = volume summary only.
- Same dominant sampled cost in all 5 children.
- `(max − min)/median ≤ 0.25` across the 5 for normalized p99, p99.9, and predeclared tail rates;
  median 0 → all 5 must be 0.
- `max` reported, never gated.

## Tests

- **Reconciliation test:** a fixture group table exercising occupied/vacant/placement/rehash asserts the
  per-phase identities above (matching = 1 per occupied lookup; 0 elsewhere; collisions = invocations −
  matches; placement/rehash invocations = visits − calls).
- **Histogram fidelity:** exact bins + overflow; a forced-overflow fixture proves the widen-and-rerun
  trigger fires at 0.1%.
- **Disabled parity:** env unset → no sink/state, output/telemetry/fingerprint byte-identical.
- **Loud publish failure:** an unwritable receipt path returns `ExecutionError`.
- Existing aggregate, group_table, streaming, architecture, Toron, and TPC-H suites remain green; the
  deep-recursion lib test passes.

## Phase 2 - name the producer, select ADR 437

Attribute `find_bucket_in`'s 23.83% across the four phases with the matching-vs-collision split and the
per-load geometry, stable across all 5 children. State the ADR 437 candidate the evidence selects
(collision → hash/fingerprint; matching → cheaper key; hash → hoisted/cached hash; upstream → key
construction). No mechanism lands in 436.

## Results document

`plans/impl/436_generic_grouped_fold_hash_probe_diagnosis_results.md` records the 5-child perf category
+ stability, the four-phase × cost ledger, the reconciliation (18,933,731), the matching-vs-collision
equality split, per-epoch + run-length geometry, the histogram overflow status, the p99/p99.9/tail
5-child spread, the named producer, and the ADR 437 selection. All profiling timing labeled diagnostic.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Seal

Plan dual: `plan: adr 436 generic grouped fold hash probe diagnosis`. Implementation seal:
`impl: adr 436 generic grouped fold hash probe diagnosis`. Do not commit `.perf.data`. If the ledger is
left in-tree dormant, its parity + reconciliation tests ride the seal.
