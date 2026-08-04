# ADR 510 - Partitioned scan build: directly ablate coalesced index-at-flush

ADR 509 closed correctly at its predeclared Phase 0 gate. On the clean sf=8 q17 coordinate, removing
the complete build-side physical reread, decrypt, and IPC-decode envelope offered only
1,064-1,124 ms against a fixed 3,554 ms materiality floor. The indexed-at-flush mechanism was not
admitted.

The same instrument also exposed an unresolved rebuild envelope:

| q17 `join0_build` fact | clean ADR 509 result |
|---|---:|
| parent median | 19,296.9 ms |
| gross removable reread median | 1,077.7 ms |
| `read_index_rebuild_cpu` | 6,276-6,317 ms |
| physical read + write + encrypt + decrypt | 5.4% of parent |
| spill | 622,544,968 B |
| pages / indexed rows | 6,059 / 47,989,007 |

`read_index_rebuild_cpu` combines removable key reconstruction with index insertion that any
candidate must retain. ADR 509 deliberately refused to count it. That was conservative and correct.

A later read of the same artifact also falsified an apparent adjacent proxy.
`write_partition_routing` was not one key-extraction phase. It merged three hooks:

| hidden phase | multiple of base rows |
|---|---:|
| exchange repartition | 4x |
| active-group partitioning | 4x |
| page-key extraction | 1x |
| merged leaf | 9x |

The artifact reports 431,901,063 merged write row-visits against 47,989,007 rebuild rows, exactly
9.0000x. The merged 6.8-second leaf therefore cannot price duplicate reconstruction.

There is still a structural duplicate: the coalescing writer extracts each page's keys into
`(key, row_index)` references, clears them, and rebuild later extracts keys from the same pages and
rows before inserting those references. No valid pre-build measurement separates retained insertion
from removable reconstruction inside rebuild.

## Decision

Authorize one bounded, explicitly unpriced direct ablation of coalesced indexed-at-flush on the
query-neutral `PartitionedScanBuild` leaf.

The candidate selects the existing resident-index representation before the first page flush, inserts
the references already computed for each coalesced page, and skips the immediate
`rebuild_index_from_pages` pass after finish. The control remains byte-for-byte on
rebuild-after-write.

This decision does **not** assert that the candidate will clear 3,554 ms. It admits implementation
because:

1. the low-level indexed writer is live and tested;
2. the missing work is bounded plumbing around the existing coalescing lifecycle, not a new index;
3. ADR 509 proved large conservative resident headroom at the exact coordinate;
4. an end-to-end paired experiment gives a decisive answer without a perturbing per-row estimator.

The fixed q17 materiality floor remains **3,554 ms**, inherited from the immutable ADR 508 baseline.
It is not recomputed from this cycle's control cohort.

Production remains rebuild-after-write until every correctness, residency, shape, and paired
performance rail passes.

## Evidence boundary

The ADR 509 timers establish that explicitly timed storage and cryptography are not the dominant
envelopes. They do not distinguish arithmetic from cache misses, allocator work, or other memory
stalls. The supported description is **CPU-shaped**, not CPU-bound and not a mechanism selection
among vectorization, layout, or prefetch.

The only conservative pre-build saving is the roughly 1.08-second reread. Neither
`write_partition_routing` nor `read_index_rebuild_cpu` supplies a material lower bound:

- the former merges three phases with different row multiplicities;
- the latter retains index insertion in the candidate;
- the two use different batch geometry and call granularity.

No proxy arithmetic, per-row timers, or sampled estimator may turn this direct ablation into a
predicted win.

## Mechanism boundary

`write_page_batch_inner(index_rows: bool)` is a low-level capability, not an end-to-end switch.
Direct indexed writers pass `true`, but both coalescing paths hardcode `false`, and the partitioned
route chooses the compact resident index only after writing and scratch release.

Implement a typed route-owned mode:

```text
CoalescedPageIndexMode
    RebuildAfterWrite
    IndexAtFlush
```

The mode is latched once before the first write and passed explicitly through:

- direct full-page handling inside the coalescing writer;
- buffered full-page flush;
- partial final-page flush;
- finish and rebuild selection.

For primitive, non-null-safe keys, the candidate selects the same compact representation the control
would select before rebuild. Generic or null-safe keys retain the same legacy representation. The
candidate may not change representation, page size, page boundaries, row order, IPC payload,
encryption, spill accounting, partition count, group count, task memory, or spill reserve.

After finish, the candidate asserts that its resident index is complete and performs no rebuild. A
failure during incremental insertion is typed and cleans up; it may not silently fall back to
control.

## Attribution hygiene

Split the overloaded `write_partition_routing` observation into:

- `write_exchange_repartition`;
- `write_active_group_partitioning`;
- `write_page_key_extraction`.

The exchange stage is required only when the committed structural path contains it. A missing real
phase fails coverage; an inapplicable phase is not required.

These names are diagnostic, not a new materiality gate. Do not delay the direct ablation to infer a
win from them.

The shared hook surface also feeds the existing q21 `GroupedChainProfile`. Preserve that profiler's
published schema through adapter mapping, or make any schema change explicit and versioned with
compatibility tests. A diagnostic cleanup must not silently invalidate the q21 receipt.

## Residency

ADR 377 intentionally released writer scratch before constructing the resident index. The candidate
overlaps the growing index with live page/ref/plaintext writer state.

ADR 509's exact-owner counterfactual measured:

| residency fact | value |
|---|---:|
| maximum complete candidate overlap | 189,470,823 B |
| minimum headroom under literal 2 GiB task budget | 1,958,012,825 B |
| partitions / groups | 16 / 4 |

That is feasibility evidence, not permission to bypass the live guard. The candidate must exercise
the existing checked resident guard after every indexed flush. The exact-owner profile is rerun on
the real lifecycle, and every group must fit both production accounting and the literal 2 GiB task
budget without changing geometry or policy.

A resident failure closes ADR 510 before the performance cohort.

## Reach

The clean q01-q22 board reaches `PartitionedScanBuild` through q17. The executor leaf is broader:
ADR 491 and ADR 492 record m06 on the same route with 88,559,536 B of spill at the 2 GiB coordinate.

Before the implementation seal, enumerate all current q-series and primitive-ladder cells committing
the leaf. Every reached shape becomes a correctness and lifecycle control. q17 remains the sole
performance verdict unless another cell is explicitly admitted before measurement.

No query number, benchmark name, or correlated-scalar-only condition may narrow the executor change.

## Shape-preserving paired rail

ADR 509 specified but did not implement the required harness gate. ADR 510 adds an explicit
shape-preserving gate rather than weakening ADR 469's plan-distinctness rule.

One diagnostic child per arm must agree on:

- clean build and `full_hash` dataset identity;
- digest, schema, row count, and plan fingerprint;
- executor route and join-spill observation;
- group and partition counts;
- page count, rows per page, ciphertext bytes, and total spill bytes;
- explicit task-memory and spill-reserve fields.

Lifecycle counters must differ in the predeclared direction:

| control | candidate |
|---|---|
| unindexed coalesced flush | indexed coalesced flush |
| nonzero rebuild calls | zero rebuild calls |
| nonzero build-parent physical rereads | zero build-parent physical rereads |
| complete index after rebuild | complete index after finish |

The selected gate, switch, budgets, and fixed 3,554 ms floor are part of the paired journal's
configuration identity. A misread switch, stale profile, missing counter, inverted band, or wrong
arm fails closed.

## Acceptance

### Lifecycle and correctness

- Compact Int32 and Int64 plus legacy Generic/null-safe indexes preserve exact lookup multisets and
  duplicate order.
- Null skipping, multiple source fragments, page-boundary coalescing, a direct full page, and a
  partial final page are covered.
- At least two active partitions, including an empty partition, preserve page and index invariants.
- Candidate and control agree on page count, per-page row count, ciphertext/spill bytes, route, plan,
  digest, schema, and output rows.
- Candidate performs zero rebuild calls and zero build-parent page rereads; control performs
  nonzero counts for a nonempty spill.
- Tight-budget and injected write, finish, index-insert, and cleanup failures remain typed and leave
  no join-spill scratch.
- Every reached q-series or primitive shape passes its supported correctness rail. The exact sf=8
  q17 shape gate owns result invariants because the q17 oracle rail does not admit sf=8.

### Performance

- Measure one discarded warmup and ADR 447's 10 systematic adjacent q17 pairs from one clean, pushed
  release binary.
- Use strict sf=8, persisted Parquet, `full_hash`, and explicit 2 GiB task / 2 GiB spill-reserve
  budgets.
- Primary statistic is the Hodges-Lehmann estimate of control minus candidate paired `execute_ms`
  with the predeclared two-sided 95% Wilcoxon signed-rank interval.
- Detectable means the interval excludes zero in the improving direction.
- Material means the point estimate is at least the fixed 3,554 ms.
- Activation requires both conditions plus every correctness, shape, residency, and cleanup gate.

Any other result leaves production on rebuild-after-write. No extra samples, threshold negotiation,
per-row estimator, proxy rescue, or pivot is allowed after seeing the result.

## Stop conditions

Close ADR 510 without widening if:

- preserving page geometry requires replacing the coalescing writer;
- early index selection changes the resident representation or memory policy;
- any reached shape loses result or lifecycle parity;
- any active group violates the unchanged resident guard or literal task budget;
- the q17 paired result is undetectable or below 3,554 ms.

Do not pivot to direct I/O, a key sidecar, async writes, another index, layout/prefetch work, a
query-specific branch, or another query.

## Alternatives not selected

- **Another rebuild attribution cycle:** no non-perturbing in-place split can establish a conservative
  lower bound on removable reconstruction; the bounded direct ablation is the measurement.
- **q21:** multiple similarly sized lifecycle components and a noisier clean baseline still require
  stable repeated attribution.
- **Grouped fold / suffix probe:** both remain one non-perturbing decomposition short of a mechanism.
- **Direct indexed writer:** changes coalesced page geometry and confounds the cut.
- **Sidecar or two-pass split:** introduces a second mechanism or changes residency before the
  existing-reference path is tested.

## Outcome

Open. A red lifecycle or residency gate closes before performance. Otherwise the fixed paired verdict
decides activation. Direct ablation is authorized; production activation is not.
