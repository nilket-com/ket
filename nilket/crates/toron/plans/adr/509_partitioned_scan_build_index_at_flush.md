# ADR 509 - q17 partitioned build: gate indexed-at-flush on measured removal and residency

ADR 508 left three premise classes and four plausible cuts. Three cuts still need a decomposition
before they name a mechanism: q08/q09 join-probe, q20/q17-inner grouped-fold lookup/update, and q21's
split spill lifecycle. The fourth, q17's outer `partitioned_scan_build` invocation, already exposes a
specific lifecycle seam:

| clean sf=8 q17 fact | value |
|---|---:|
| Toron median | 35,541 ms |
| Toron / DuckDB-1t | 46.46x |
| whole-query spill | 622,544,968 B |
| outer handler | 24,674 ms |
| `join0_build` | 19,595.5 ms, 79.4% of handler |
| `join0_probe` | 1,022.3 ms, 4.14% of handler |

These are from the clean, pushed ADR 508 identity
`632777908780f17bf15d1d7d43125b6e28f67d99`, at strict sf=8 with persisted Parquet,
`full_hash` dataset verification, and explicit 2 GiB task / 2 GiB spill-reserve budgets.

The selection question is not which cell is largest. It is which premise can reach one falsifiable
mechanism, correctness proof, ablation, and movement verdict without becoming ADR 508 part two.

## Decision

Take q17's outer `partitioned_scan_build` invocation.

First refine its existing invocation-bound `join0_build` stage at the spill layer and prove the
candidate's overlapping resident state fits the unchanged task budget. Proceed only when:

1. the gross build-side physical reread + decrypt + IPC-decode envelope plausibly exceeds
   **3,554 ms**, ADR 447's 10% materiality floor against the locked 35,541 ms baseline; and
2. the complete indexed-at-flush overlap fits under the existing 2 GiB resident accounting with
   explicit headroom.

If both gates are green, build the resident index while flushing the existing coalesced spill pages
and skip the immediate `rebuild_index_from_pages` pass. If either gate is red, close ADR 509 with the
measured reason. Do not pivot inside this ADR to encryption, direct I/O, a key sidecar, async writes,
or another query.

No cap, group count, partition count, spill reserve, page geometry, planner shape, or query-specific
executor branch may change.

## Why this mechanism is plausible

On the committed route,
`try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks` handles one partition group at a
time:

1. create one `SpilledInnerJoinBuild` per active partition;
2. scan and route rows into coalesced unindexed pages;
3. flush partial pages, `finish`, and release writer scratch;
4. reread every nonempty partition through `rebuild_index_from_pages`;
5. probe and clean the group.

`write_page_batch_inner` already has each key plus its page and row coordinates while writing. The
unindexed mode discards those references. `rebuild_index_from_pages` then reads each page through
physical I/O, decrypts and IPC-decodes it, reconstructs keys with `join_key_at`, and inserts the same
page/row references into the resident index.

The proposed arm preserves the coalescing writer and its page boundaries. At each existing flush it
retains and inserts the references already in hand. It does not substitute the older direct indexed
writer, whose input-batch page geometry would confound the ablation.

## What Phase 0 must establish

### One attribution chain, not a second independent profile

`GroupedChainSubLeaf` already names the relevant spill leaves and the spill layer already owns the
hook locations. `GroupedChainSpillObserver` is not directly reusable: it belongs to a separate
`GroupedChainProfile`, and its leaves reconcile to reservoir-specific parents rather than q17's
matched aggregate invocation.

Reuse the leaf enum and hook locations through an adapter whose timers are ADR 508 `StageToken`s bound
to q17's invocation and refine `join0_build`. The full chain must stay green:

```text
spill subleaves
    subset of join0_build
    subset of correlated-scalar handler exclusive
    subset of matched Aggregate-node exclusive
    subset of execute_bound span
    subset of execute_ms
```

Every residual remains explicit. Missing, inverted, mismatched, orphaned, or over-attributed input
fails closed. Existing q21 `GroupedChainProfile` behavior and output remain unchanged.

### The removable envelope is narrower than index rebuild

The gross build-side envelope is:

```text
physical reread + decrypt + IPC decode
```

`ReadIndexRebuildCpu` is not wholly removable. Indexed-at-flush still pays index insertion. Its
duplicate key extraction and insertion share one per-row loop: `key_arrays` is page-level setup,
while `join_key_at` and `index.insert` are both per-row. Separating them would require per-row clock
reads or a two-pass restructure that changes the lifecycle being measured.

ADR 508 already rejected this instrument class after sampled per-row components summed to 119% and
203% of their parents. A red gross-envelope gate is not rescued by rebuilding that perturbing
estimator one function over.

### Cache warming may relocate cost

`read_batch_page_profiled` inserts decoded pages into the owning build's LRU. Removing the rebuild
therefore removes both work and cache warming. Probe-side misses may relocate some gross saving into
`join0_probe`.

The cache is not one global 8 MiB object. `cache_cap` belongs to each `SpilledInnerJoinBuild`, and the
partitioned route holds one build object per active partition. Probe access is match-selective, so
warm-cache value is not proportional to its byte share.

Record cache hits, misses, pages, and decoded bytes under both build and probe parents in both arms.
Report separately:

1. gross build-side read/decrypt/decode removed;
2. probe-side read/decrypt/decode and misses added;
3. net diagnostic leaf movement;
4. paired whole-query movement.

The diagnostic leaves explain transfer. They are not a point prediction for unprofiled wall time.

### Residency is a hard gate, at the board coordinate

ADR 377 made unindexed writing intentional. Releasing writer scratch before rebuilding avoids
overlapping the final indexes of the active group with the live plaintext/ref buffers and page
reserve used during writing.

The candidate changes that overlap:

```text
current rebuild peak:
    group page/file metadata + final resident indexes

candidate write peak:
    group page/file metadata + growing resident indexes + live writer scratch/page reserve
```

Partitioning is budget-derived. The 64 MiB exploratory artifacts used elsewhere during ADR 508
created a different group/partition geometry and are inadmissible here. At 2 GiB there are fewer,
larger groups, so more partitions—and their per-object indexes and caches—may coexist within a group.
A low-budget derivation would be optimistic in exactly the direction this gate must forbid.

Phase 0 therefore emits group count, total partition count, partitions per group, per-object cache
state, final summed resident index bytes, maximum summed writer scratch/page-reserve bytes, metadata
already charged by the guard, candidate overlap, and remaining headroom. Admission comes from the
same production resident accounting that guards the live path, not a key-count estimate.

Every Phase 0 numeric input must come from strict sf=8, persisted Parquet, `full_hash`, with explicit:

```text
memory_per_task      = 2,147,483,648 B
spill_bytes_reserved = 2,147,483,648 B
```

No number from `claude_iter_0`, `claude_iter_1`, or the exploratory
`target/tpch/adr508/corrscalar/` and `q17probe/` artifacts may enter the gate. Those runs used the
harness's 64 MiB / 512 MiB defaults and establish lifecycle shape only.

## Mechanism

Add an explicit indexed-at-flush mode to the coalescing spill writer:

1. select the existing compact or legacy resident-index representation before the first page flush;
2. when `flush_unindexed_page_buffer` calls `write_page_batch_inner`, retain and insert each
   key/page/row reference already computed for that page;
3. preserve row order, page payload, page boundary, encryption, physical write, spill accounting,
   and partial-tail flush;
4. after `finish` and scratch release, assert the resident index is complete and skip
   `rebuild_index_from_pages`;
5. keep every existing resident guard active while the index grows.

The mode applies to the query-neutral `PartitionedScanBuild` leaf only. Other unindexed spill users
remain unchanged.

One binary carries both arms behind an explicit environment switch. The control remains
rebuild-after-write until every acceptance rail passes.

## Acceptance

### Phase 0

- Subleaves reconcile to `join0_build` inside a boundary declared before measurement, and all outer
  ADR 507/508 links remain green.
- Flow spill reconciles exactly to the handler's 622,544,968 B at the clean coordinate.
- Profile on/off preserves route, plan fingerprint, digest, schema fingerprint, row count, spill
  bytes, and both budgets.
- Group and partition cardinality are emitted; no default-provenance budget is admitted.
- The gross envelope clears 3,554 ms under the predeclared Phase 0 rule.
- Production resident accounting proves the candidate overlap fits 2 GiB with explicit headroom.
- A red on either final bullet closes the ADR without a replacement mechanism.

### Correctness and lifecycle

- Unit parity covers duplicate keys, nulls, partial final pages, multiple source batches per page,
  multiple pages, and at least two active partitions.
- Int32/Int64 compact-index and Generic/null-safe legacy-index cases preserve exact lookup multisets
  and duplicate order.
- The candidate performs zero build-envelope page rereads and zero
  `rebuild_index_from_pages` calls; the control performs the expected nonzero counts.
- Page count, per-page row count, ciphertext/spill bytes, route, plan fingerprint, digest, schema,
  and row count are invariant across arms.
- Tight-budget fixtures fail at the existing typed resident guard. No saturation, cap change,
  fallback, or post-write silent switch to control is allowed.
- Spill scratch is empty after success and every injected failure.

### Performance

- Measure q17 from one clean, pushed release binary at strict sf=8, persisted Parquet, `full_hash`,
  explicit 2 GiB / 2 GiB budgets, with one discarded warmup and ADR 447's 10 systematic adjacent
  A/B pairs.
- Primary statistic is the Hodges-Lehmann estimate on `control - candidate` paired `execute_ms`
  differences with its two-sided 95% Wilcoxon signed-rank interval.
- Detectable means the interval excludes zero in the improving direction. Material means the point
  estimate is at least 3,554 ms. Both must pass.
- The 3,554 ms floor is fixed from the immutable ADR 508 baseline before the cohort; it is not
  recomputed from the observed control arm after measurement.
- A detectable but immaterial result, a material but undetectable result, or neither leaves the
  production default unchanged. No post-hoc sample extension or threshold negotiation occurs.
- Raw samples, run order, the durable journal, configuration lock, and both-parent cache/read
  diagnostics are retained.

The current custom `--paired-env` gate is not sufficient as written: it invokes ADR 469's
plan-distinctness gate, while this mechanism intentionally preserves the plan fingerprint. The
implementation must add an explicit shape-preserving mechanism gate. Both arms must pass and agree on
dataset, plan, route, digest, schema, row count, page geometry, and spill bytes, while lifecycle
counters prove that the candidate indexed during flush and skipped rebuild and the control did the
opposite. This gate choice is part of the paired journal's configuration identity.

### Scope

- Run a post-change sf=8 route census. Any newly affected cell becomes a required correctness
  control, not retroactive evidence of reach.
- Direct board reach is q17 only. No full-board movement claim follows from this cohort.
- Executor code remains shape-named; no query number or query-specific branch.
- q21, compute-fold, join-probe, task-multiplicity/RSS, and the sf=10 publication board remain parked.

## Alternatives not selected

- **q21:** already decomposed, but its 6.29% clean spread is high and its work divides across runtime
  preparation, reservoir buffering, and replay rather than one majority component.
- **Compute-fold:** `row_lookup_update` remains unsplit. Per-row timing already proved perturbing.
- **Join-probe:** `suffix_probe` still needs a lifecycle-grounded lookup/materialization/read-back
  split.
- **Direct indexed writer:** changes coalesced page geometry and confounds the cut.
- **Key sidecar or two-pass extraction/insertion split:** adds a second mechanism or changes
  residency before the simpler existing-reference path has passed its gate.

## Outcome

Closed at Phase 0 on `a52b4773f4f7`.

The residency gate passed, but the removable-time gate failed in every valid profiled run:

| Phase 0 gate | measured result | verdict |
|---|---:|---|
| gross build reread | 1,064.448-1,123.861 ms; median 1,077.722 ms | red against fixed 3,554 ms |
| indexed-at-flush overlap | 188,622,572-189,470,823 B | green |
| minimum literal 2 GiB task-budget headroom | 1,958,012,825 B | green |

All three interleaved profile-on/profile-off pairs preserved digest, schema, row count, plan, route,
622,544,968 spill bytes, 16 partitions in four groups, both explicit budgets, and `full_hash`
verification. The instrument reconciled on both axes with no mismatches or unattributed spill.

The gross envelope was only 29.9-31.6% of the predeclared materiality floor. The much larger
`read_index_rebuild_cpu` envelope is not a rescue: it still combines removable key reconstruction
with index insertion the candidate must retain, and ADR 508 already rejected the per-row estimator
needed to split those operations in place.

Per the stop/go rule, indexed-at-flush was not implemented or ablated, production remains
rebuild-after-write, and ADR 509 does not pivot to another spill mechanism or query. The complete
measurement receipt is in
`impl/509_partitioned_scan_build_index_at_flush.md`.
