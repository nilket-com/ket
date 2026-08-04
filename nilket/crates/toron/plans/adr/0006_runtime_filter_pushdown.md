# ADR 006: Toron Runtime Filter Pushdown

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion implementation plan:**
`plans/impl/0006_runtime_filter_pushdown.md`

## Status

Promoted to `plans/adr/`. Implementation pending. Promotion bench
evidence will land at
`plans/impl/0006_runtime_filter_pushdown_bench_results.md`.

Builds on five WIP iteration rounds across the council
(`plans/wip/{claude,codex,gemini}_topics_006.md`,
`plans/wip/gemini_{adr,impl}_006.md`). Three-way final convergence
reached by iter-5; iteration artifacts retired (git holds the
narrative). Codex's stage-order-honesty correction (`execute_inner_join`
is too late for storage pushdown) and the concrete
`distributed/fragment.rs` stage-graph mapping, Gemini's
two-wave-execution framing and Bloom-only V1 discipline with dual
benchmark shape, and Claude's three-band conditional ADR 006 rule
applied retroactively (ADR 005 landed in the operator-adjacent band,
so runtime-plumbing investigation is not ADR 006) plus the
scale-boundary convention proposal are the load-bearing antecedents.

This ADR is a **cross-ring optimization contract**, not a new
relational semantics, executor-model, or kernel contract. It
introduces advisory Bloom runtime filters for hash-partitioned inner
equi-joins, changes Toron's distributed stage order for
filter-enabled joins, and pins a shared cross-ring descriptor
surface understood by both Toron and Wendelon. Every ADR 002 / 003 /
004 / 005 hard gate and architecture guard G1-G18 must continue to
pass byte-identically.

It does **not** decide: exact `InList` filters (V1.x), cost-based
filter selection or planner-stats opt-in (V1.x — planner-stats
surface doesn't exist yet), cost-based build/probe swap (V1.x —
requires Wendelon stats + Toron cost model), runtime filters for
outer / semi / anti joins (own ADRs along with the join kinds
themselves), runtime filters for sort-merge or non-equi joins (V1.x),
row-group skip heuristics beyond independently-sound cases (V1.x),
Wendelon-side internal scan-engine changes (Wendelon-owned ADR), or
the residual `framework_overhead_ms` runtime-plumbing question (ADR
005 resolved this to operator-adjacent; no follow-on ADR).

## Scale boundary

**Primary: cross-ring** (Toron ↔ Wendelon storage layer) — this is
the load-bearing axis. The contract surface includes Toron's
distributed scheduler / fragmenter (cluster-scale), Toron's task
manifest wire (transport-scale), and Wendelon's storage scan
boundary (cross-ring-scale).

**Secondary:** scheduler / fragmenter (cluster-wide) — the two-wave
stage-order change is a cluster-scheduling contract change for
filter-enabled joins. **Transport** — `PartitionManifest` and
`ScanRequest` carry new optional fields.

ADR 006 is the first ADR drafted under the council's new
scale-boundary convention (adopted iter-5; this section is the
proposed structural template position).

## V1 contract summary

- **Decision shape.** Advisory Bloom runtime filters for
  hash-partitioned inner equi-joins. Two-wave execution model when
  filters are enabled; filter-disabled queries retain today's
  concurrent-producer stage graph.
- **Stage order (filter-enabled hash-partitioned only).**
  Build wave → coordinator merge → probe wave → join wave. The
  build-wave barrier opens probe dispatch; probe-side
  `ScanRequest` carries the merged filter descriptor.
- **Filter type V1.** Bloom only. Exact `InList` deferred to V1.x.
- **Key encoding.** Canonical join-key bytes (ADR 003 §13
  normalization reuse) with explicit logical-type identity —
  ADR 005's type-tag invariant generalizes to filter-key encoding.
  `Int32(1)` and `Int64(1)` must never collide.
- **V1 filter eligibility (narrow).** Single-column join keys
  with type in {`Int8`, `Int16`, `Int32`, `Int64`, `UInt8`,
  `UInt16`, `UInt32`, `UInt64`, `Utf8`}. Composite keys,
  floats (`Float32`/`Float64`), `Date`/`Timestamp`/`Decimal`,
  and other types are V1-ineligible — floats wait for the
  floating-point canonical-encoding ADR (NaN / signed-zero /
  subnormal semantics); composites are V1.x. Ineligible joins
  retain the concurrent stage graph and skip filter generation.
- **Filter descriptor.** Stable serialized Bloom descriptor with:
  key encoding version; build-key schema (or logical type vector);
  probe-side key column ordinals or equivalent scan-time key
  encoding plan; hash seed; bit count; hash count; byte budget;
  target false-positive rate; filter payload bytes; probe-side
  `physical_descriptor_fingerprint` for safety validation.
- **Bloom parameter sizing (adaptive).** Target false-positive
  rate is **1%** at the observed distinct build-key count `n`.
  Optimal sizing follows the standard formula
  `m ≈ -n · ln(p) / (ln 2)²` (≈ 9.59 bits per key at p = 0.01).
  Byte budget scales with `n`:
  - **Minimum 16 KiB** serialized payload (good for `n ≤ 13K`
    distinct keys at 1% FP).
  - **Cap 1 MiB** serialized payload (good for `n ≤ 870K`
    distinct keys at 1% FP).
  - At very high build cardinality (`n > ~870K`), the cap binds
    and the measured FP rate degrades above 1% — the filter
    remains advisory and correct; selectivity reduces with FP
    growth. Phase 0 calibrates the curve against the fixture
    and pins the workload-appropriate budget.
  - The selective benchmark workload assumes a small dimension
    build (10K–100K rows) where the Bloom comfortably fits the
    minimum or low-mid range. A 64 KiB sized filter handles
    ~54K distinct keys cleanly; this is the V1 sweet spot.
- **Descriptor location.** Shared cross-ring schema surface —
  either an existing Wendelon-published schema crate or a new
  minimal common module (`nilket-shared` is the proposed name).
  Wendelon must **not** deserialize a Toron-private type. Default
  recommendation: extend an existing Wendelon-published schema
  crate (avoids spawning a new crate before multi-consumer
  evidence justifies it, per the roadmap's
  "Wendelon-side crate extraction" convention). Final crate
  ownership pinned by Phase 1.
- **Build-fragment surface.**
  `runtime_filter_fragment: Option<RuntimeFilterFragment>` on
  `PartitionManifest`, carried by
  `TaskCompletionOutput::ShuffleProducer { manifest }` inside
  `TaskCompletePayload`. Reuses existing barrier semantics; no new
  envelope variant.
- **Probe-wave carrier surface (load-bearing).** The merged
  filter must reach the worker that constructs `ScanRequest`,
  but Toron's coordinator dispatches `TaskSpec` to workers and
  `ScanRequest` is *constructed on the worker* inside
  `Executor::execute_bound` (`crates/toron/src/exec.rs:460`),
  not on the coordinator. Adding the descriptor to
  `ScanRequest` alone is insufficient. The descriptor therefore
  rides three surfaces:
  1. **`TaskInputBinding::Scan` gains
     `runtime_filter: Option<RuntimeFilterDescriptor>`.** The
     coordinator injects the merged filter into the probe-side
     task's binding at probe-wave dispatch.
  2. **`Executor::execute_bound` reads** the optional filter
     from the task's `TaskInputBinding::Scan` and threads it
     through to the `ScanRequest` it constructs for the
     `ExecutionPlan::Scan` variant.
  3. **`ScanRequest` gains `runtime_filter:
     Option<RuntimeFilterDescriptor>`** as the wire-level
     surface to Wendelon.
  Filter-disabled queries omit the field at all three surfaces
  (`None`); concurrent-producer stage graph and byte-identical
  wire shape preserved.
- **Coordinator merge.** Compatible Bloom fragments are merged
  deterministically (bitwise OR over identical-parameter Bloom
  filters) **from accepted build-stage attempts only**. Stale
  retry attempts do not participate (ADR 003 task-failure taxonomy
  preserved). Incompatible fragments drop the filter rather than
  failing the query.
- **Wendelon enforcement.** **Row-level reject** is the sound V1
  enforcement path (Bloom proves non-membership per encoded key).
  Row-group skip is opportunistic and only when row-group stats
  independently make the skip sound (min/max stats do not
  automatically prove Bloom absence for a range). Unsupported scan
  paths silently drop the filter — no new wire error variant.
- **Advisory invariant (load-bearing correctness).** Runtime
  filters are advisory. False positives are allowed; false
  negatives are contract violations. If Wendelon ignores or drops
  the filter for any reason, output is byte-identical to
  filter-disabled execution. Toron's join operator remains the
  sole correctness authority.
- **Planner control.** Always two-wave when the filter is enabled
  in V1. Planner / cost-model opt-in is V1.x once a planner-stats
  surface exists. The non-selective guard benchmark proves the
  always-on barrier cost is acceptable.
- **Architecture invariant.** Runtime-filter hash / encoding must
  not replace ADR 003 §5 exchange-partition hashing
  (`xxh3_64(canonical_encoded_bytes) mod partition_count`). Helper
  names are semantic ("runtime filter key" vs "exchange partition
  hash"). No generic "fast hash" helper blurs the boundary.
  Generalizes ADR 005's pattern for the operator-local join-build
  key to the cross-ring runtime-filter key.
- **Wendelon coordination posture.** Toron ships its half of the
  contract regardless of Wendelon-side schedule. The advisory
  invariant makes split-shipment safe:
  filter-enabled-with-Wendelon-disabled is byte-identical to
  filter-disabled. ADR 006 promotion can cite partial bench
  evidence locally; the full shuffle-byte reduction row arrives
  in a bench-results update once Wendelon-side application is in.
- **No new contract surface beyond the runtime filter.** No new
  `MessageKind`, IR variant, `BudgetDimension`, task telemetry
  field, or `JoinKind`. No new architecture guard by default;
  conditional escape if shared-helper extraction makes
  runtime-filter-vs-exchange-partition hash drift plausible (same
  pattern as ADR 005 hard gate 14).
- **Correctness preservation.** ADR 005 canonical 10M × 1M
  hash-partitioned join digest remains
  `2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289`
  with filters enabled or disabled.

## Context

ADR 005 promoted with a 79.0% `build_ms` reduction at 8 workers
(2581.550 ms → 542.787 ms) and 4.76× build throughput on the
10M × 1M hash-partitioned canonical fixture. Digest preserved.
The conditional ADR 006 rule landed in the **operator-adjacent**
band: `framework_overhead_ms` fell 78.8% — effectively proportional
to the build drop — so the prior residual was operator-adjacent
allocation/runtime cost, not a proven scheduler/poll bottleneck.
ADR 006 therefore does **not** investigate runtime plumbing.

Post-ADR-005 8-worker cumulative attribution (1637.966 ms wall):

| Phase | ms | % of wall |
|---|---:|---:|
| build | 542.787 | 33.1% |
| framework_overhead | 432.429 | 26.4% |
| batch_send | 185.123 | 11.3% |
| shuffle_pull_decode | 156.441 | 9.6% |
| output_publish | 143.318 | 8.8% |
| probe | 80.191 | 4.9% |
| batch_encode | 73.630 | 4.5% |

The **bytes-movement cluster** (`batch_send` +
`shuffle_pull_decode` + `output_publish` + `batch_encode`
= 558.512 ms, 34.1% of wall) is now slightly larger than build.
This is the sharpest evidence-driven target after ADR 005.

The user's direction is **Evidence Pressure > Launch Pressure**,
which selects Topic A (runtime filter pushdown) over launch-driven
candidates (outer joins, multi-tenant governance, Rune sandbox).
Topic A attacks the bytes-movement cluster *at the source* — by
making Wendelon emit fewer probe-side rows — rather than moving
the same bytes faster (Topic E, demoted). On a selective
star-schema-like workload, the leverage is qualitative: a 0.1%-
selectivity join reads 0.1% of the probe bytes, regardless of
wire speed.

The current hash-join fragmentation in
`crates/toron/src/distributed/fragment.rs` produces independent
left and right producer stages, then a join consumer:

```text
stage 0: left  (build) exchange producer  upstream []
stage 1: right (probe) exchange producer  upstream []
stage 2: join consumer                     upstream [0, 1]
```

Both producers run concurrently. A filter built inside
`execute_inner_join` (stage 2) runs *after* both producers have
already scanned and shuffled — too late to reduce Wendelon
probe-side scan bytes or right-side shuffle bytes (Codex iter-2
stage-order-honesty correction). The contract therefore must
change stage ordering for filter-enabled joins, not just add
filter generation.

This ADR is the natural completion of the smart-storage pushdown
thesis ADR 002 introduced. ADR 002 gave us *static* predicate
pushdown — `WHERE` clauses reach Wendelon at plan time. ADR 006
adds *runtime* pushdown — keys observed during execution reach
Wendelon before the probe-side scan.

## Decision

### 1. Stage-order change (two-wave hash-partitioned execution)

For runtime-filter-enabled hash-partitioned inner equi-joins, the
distributed stage graph changes from concurrent producers to a
two-wave shape:

1. **Build wave.** Left (build) exchange producer runs first.
   Publishes normal shuffle partitions *plus* an optional Bloom
   filter fragment over ADR 004's build-side join keys. The
   fragment lives on the producer's `PartitionManifest` as an
   optional field; manifests flow to the coordinator via
   `TaskCompletionOutput::ShuffleProducer { manifest }` inside
   `TaskCompletePayload`.
2. **Coordinator merge.** The coordinator merges compatible Bloom
   fragments deterministically (bitwise OR over
   identical-parameter filters) from **accepted build-stage
   attempts only**. Stale retry attempts do not participate.
   Incompatible fragments drop the filter rather than failing the
   query.
3. **Probe wave.** Right (probe) exchange producer dispatches
   *after* the build-wave barrier opens. The merged filter
   descriptor is attached to its `ScanRequest`. Wendelon validates
   the descriptor against the probe-side
   `physical_descriptor_fingerprint` already present on
   `ScanRequest` before applying.
4. **Join wave.** Join consumer runs after both shuffle-producing
   stages are accepted, as today. Remains the sole correctness
   authority for join semantics.

Filter-disabled queries retain today's concurrent-producer stage
graph unchanged. The two-wave barrier is paid only when the filter
is enabled.

### 2. Filter type and key encoding

**Bloom filters only in V1.** Exact `InList` deferred to V1.x.
The first cross-ring runtime-filter ADR proves the contract; it
does not multiply filter kinds.

**Canonical key encoding.** Filter-key bytes reuse ADR 003 §13
canonical normalization and ADR 005's logical-type-tag invariant.
`Int32(1)` and `Int64(1)` are distinct filter keys.

**V1 filter-key eligibility (narrow).** Single-column join keys
with type in the integer + Utf8 set:

- Signed integers: `Int8`, `Int16`, `Int32`, `Int64`.
- Unsigned integers: `UInt8`, `UInt16`, `UInt32`, `UInt64`.
- UTF-8 strings: `Utf8`.

**Explicitly excluded from V1 eligibility:**

- **Composite keys.** V1 is single-column only; composite-key
  filter eligibility is V1.x (it requires a multi-column
  canonical encoding plan in the descriptor).
- **`Float32` / `Float64`.** Floating-point join keys are
  deferred to their own canonical-encoding ADR (roadmap
  V1.x — needs NaN, signed-zero, subnormal semantics). The
  canonical encoder supports them at the byte level, but
  using them as filter keys without a pinned semantics ADR
  invites silent-wrong-results.
- **`Date` / `Timestamp` / `Decimal`.** Canonical encoder
  does not support these yet; filter eligibility extends
  when encoder support lands (V1.x).

Filter-eligible joins skip filter generation when their key
type is outside the V1 set — the concurrent stage graph is
preserved unchanged for ineligible joins.

### 3. Filter descriptor and cross-ring boundary

The merged runtime-filter descriptor is a **serialized
cross-ring type** understood by both Toron and Wendelon. Schema
fields:

- key encoding version
- build-key schema (or logical type vector)
- probe-side key column ordinals or equivalent scan-time key
  encoding plan (Wendelon needs to compute hashes against the
  same canonical bytes Toron used)
- hash seed
- bit count (sized adaptively per V1 contract — see §V1
  contract summary; `m ≈ 9.59 · n` for target FP 1%, bounded
  by min 16 KiB and max 1 MiB serialized payload)
- hash count (`k ≈ 7` at target FP 1%, scaled per the sizing
  formula when the cap binds)
- byte budget (adaptive: 16 KiB min, 1 MiB cap; sized per
  observed distinct build-key count)
- target false-positive rate (1% for V1; degrades above 1%
  when the cap binds at high cardinality)
- filter payload bytes
- probe-side `physical_descriptor_fingerprint` (reuses Toron's
  existing field on `ScanRequest` for safety validation)

The descriptor lives in a shared cross-ring schema surface.
Default recommendation: extend an existing Wendelon-published
schema crate (avoids creating a new crate before multi-consumer
evidence). Alternative: a minimal `nilket-shared` common schema
module. Final crate ownership is pinned by Phase 1 of the impl
plan and is not a contract-level decision — both options
preserve the boundary invariant.

### 4. Coordinator merge semantics

- Merge order: workers complete in any order; merge result is
  invariant under permutation (bitwise OR is commutative and
  associative).
- Stale attempts: only fragments from accepted build-stage
  attempts participate. ADR 003 task-failure taxonomy is the
  authoritative attempt-acceptance contract.
- Incompatibility: fragments with mismatched `bit_count`,
  `hash_count`, `hash_seed`, or `key_encoding_version` drop the
  filter for the entire join. Drop is silent (no wire error),
  preserves the advisory invariant.
- Worker loss / retry determinism: filter merge is reproducible
  across retries — re-executing the build wave with the same
  task seeds produces the same merged filter byte-for-byte.

### 5. Wendelon enforcement contract

- **Row-level reject is the required V1 enforcement path.** For
  every probe row, Wendelon computes the canonical filter-key
  bytes per the descriptor's encoding plan, hashes via the
  descriptor's `hash_seed` / `bit_count` / `hash_count`, and
  rejects rows whose hash bits are not all set. Bloom proves
  non-membership soundly per encoded key.
- **Row-group skip is opportunistic** and only when row-group
  min/max statistics (or equivalent independent skip evidence)
  guarantee skip soundness. Bloom membership is per-encoded-key;
  range stats do not automatically prove Bloom absence for a
  range — the implementation must independently verify before
  skipping.
- **Fallback (filter dropped):** if Wendelon cannot apply the
  filter (unsupported version, fingerprint mismatch, missing
  feature, error), the scan proceeds without the filter and
  emits all rows. Toron remains correct.

### 6. Advisory invariant

Runtime filters are **advisory** under all conditions:

- False positives are allowed (probe rows may be emitted that
  don't ultimately join).
- False negatives are contract violations (a probe row whose key
  matches a build key must never be rejected by the filter).
- If Wendelon ignores or drops the filter for any reason, output
  is **byte-identical** to filter-disabled execution.
- Toron's join operator is the sole correctness authority. The
  filter is a performance hint, not a correctness contract.

### 7. Planner control

Filter-eligible joins use two-wave execution **always** in V1.
Planner / cost-model opt-in to filter-enable-or-disable is V1.x
once a planner-stats surface exists. The non-selective guard
benchmark must show that the always-on barrier cost is bounded
and does not regress ADR 005's `build_ms` win.

A join is **filter-eligible** when:

- The join kind is `Inner` (ADR 004's only V1 kind).
- The join strategy is `HashPartitioned` (broadcast-strategy
  runtime filters deferred — broadcast joins already complete
  their build before probe, so the two-wave-stage-order
  motivation doesn't apply; broadcast filters can ship in V1.x
  with the same descriptor contract).
- The join keys are **single-column** (composite-key
  eligibility V1.x).
- The single-column key `DataType` is in the V1 set: signed
  integers (`Int8`/`Int16`/`Int32`/`Int64`), unsigned integers
  (`UInt8`/`UInt16`/`UInt32`/`UInt64`), or `Utf8`. **Floats,
  `Date`, `Timestamp`, `Decimal`, and any other type are
  ineligible in V1** (per §2).
- The query has not explicitly disabled runtime filtering via a
  config flag (a small V1 escape hatch for debugging; default
  enabled).

## Hard gates

ADR 006 is acceptable only if all of these hold:

### Correctness preservation

1. `cargo test -p toron` passes.
2. `cargo test --workspace` passes.
3. Existing architecture guards G1-G18 pass unchanged.
4. ADR 004 distributed process SIGKILL replay tests produce
   byte-identical normalized digests.
5. ADR 005 canonical 10M × 1M hash-partitioned join row remains
   digest-identical with filters enabled or disabled
   (`2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289`).
6. Filter-enabled vs filter-disabled digest parity on every
   ADR 004 / 005 correctness fixture.
7. Wendelon forced-ignore fallback returns byte-identical output
   to filter-disabled execution.
8. Descriptor / fingerprint mismatch drops the filter and remains
   correct.
9. Saturated / all-positive Bloom filter (degenerate case) emits
   no false negatives.
10. **No-false-negative unit tests for generated Bloom filters.**
    Load-bearing — false negatives are contract violations.
11. Deterministic merge across worker completion order.
12. Worker loss / retry preserves final digest; coordinator does
    not merge stale-attempt fragments.
13. **Probe-side dispatch does not start before the build-side
    runtime-filter barrier opens when filters are enabled.** The
    two-wave barrier is contractually load-bearing.
14. **Type-tag invariant.** Filter-key encoding preserves logical
    type identity (`Int32(1)` and `Int64(1)` distinct). Unit-test
    enforced. Generalizes ADR 005 hard gate 7.
15. **Runtime-filter hash separation.** Runtime-filter hashing
    must not replace ADR 003 §5 exchange-partition hashing.
    Helper names are semantic. Conditional architecture-guard
    escape: a `partition_key_hash` call-target grep guard is
    added in `tests/architecture_test.rs` if shared-helper
    extraction makes drift plausible. Default expectation: no
    new guard.
16. No public API, wire-incompatibility-introducing, scheduler,
    task identity, or `JoinKind` changes beyond the additive
    optional fields named in §V1 contract summary
    (`runtime_filter_fragment` on `PartitionManifest`,
    `runtime_filter` on `TaskInputBinding::Scan`,
    `runtime_filter` on `ScanRequest`).
17. **No new contract surface.** No new `MessageKind`, IR
    variant, `BudgetDimension`, or Wendelon scan contract
    beyond the three additive optional fields named in §V1
    contract summary (`PartitionManifest.runtime_filter_fragment`,
    `TaskInputBinding::Scan.runtime_filter`,
    `ScanRequest.runtime_filter`). Semantic identifier names
    per `no-planning-phase-names.md`.

### Bench evidence (dual shape)

18. **Selective row (Wendelon-enabled target; not a Toron-side
    promotion blocker).** Star-schema-like hash-partitioned
    join with **small dimension build** (10K–100K rows) ×
    **large fact probe** (e.g., 100M rows) with ~1–10% match
    rate. Primary success signal: when Wendelon-side
    application is in, the bytes-movement cluster drops `≥50%`
    versus filter-disabled at 8 workers — measurable in
    `shuffle_pull_decode_ms`, `batch_send_ms`, and computed
    shuffle bytes per probe row. **This row is the target the
    bench artifact must record**; it ships as a follow-up
    update if Wendelon-side application post-dates Toron-side
    promotion. A miss with measured reduction is acceptable
    if the bench artifact records the gap explicitly; the
    council selects accept-narrower / scope-down (narrow
    filter eligibility) / shelve. Wall-clock-only improvement
    without bytes-cluster movement is **not** sufficient.
19. **Non-selective guard row (Toron-side promotion blocker).**
    10M × 10M (or 10M × 1M non-selective variant — same shape
    as ADR 005 canonical but match rate ~100%). Primary
    signal: Bloom generation + merge + Wendelon application
    (or Wendelon-disabled execution) + two-wave barrier
    overhead does **not** erase ADR 005's `build_ms` win.
    Default target: `build_ms` within 10% of ADR 005
    post-promotion baseline at 8 workers. Load-bearing for
    Toron-side promotion.
20. **ADR 005 canonical row (Toron-side promotion blocker).**
    10M × 1M hash-partitioned, digest-identical with filters
    enabled or disabled.
21. **Wendelon-disabled gate (Toron-side promotion blocker).**
    Filter-enabled-with-Wendelon-disabled execution emits
    byte-identical output to filter-disabled execution on
    every fixture (selective + non-selective + ADR 005
    canonical). This proves the Toron-side advisory contract
    is sound independent of Wendelon-side state and is the
    load-bearing safety gate that makes split-shipment safe.
    The Wendelon-disabled gate must pass at promotion time;
    the Wendelon-enabled selective row (gate 18) follows
    when Wendelon-side application ships.

## Bench evidence

The promotion bench requires the **dual-shape** fixture above plus
the ADR 005 canonical row. Command shape mirrors ADR 005:

```text
cargo bench -p toron --bench distributed_inner_equi_join \
    -- --distributed-inner-equi-join-only \
       --strategy=hash --workers=2,4,8 \
       --runtime-filter={enabled,disabled} \
       --build-rows=<selectivity-tuned> --probe-rows=<selectivity-tuned>
```

The bench artifact at
`plans/impl/0006_runtime_filter_pushdown_bench_results.md` must
record:

- ADR 005 post-promotion baseline table (2/4/8 workers, 10M × 1M).
- ADR 006 selective-row results, filter-enabled and
  filter-disabled, for both Wendelon-enabled and Wendelon-disabled
  configurations.
- ADR 006 non-selective guard-row results.
- ADR 005 canonical row rerun (digest equality is the primary
  signal).
- Bytes-movement cluster delta (selective row, filter-enabled vs
  filter-disabled).
- Computed shuffle bytes / probe row (baseline and post-change)
  to make the leverage explicit.
- Phase 0 calibration notes (selectivity profile of the
  benchmark fixture; bytes budget calibration).
- Profiler note if available; ADR 005's profiler note recorded
  that the local environment ptrace/Yama policy blocks
  `samply`/`perf`/`gdb` attach. This may carry forward.
- Explicit conditional band: if the non-selective guard row
  shows `build_ms` regression beyond the 10% threshold (gate 19),
  the bench artifact records the regression and the council
  decides accept-narrower / scope-down (e.g., filter-eligibility
  narrowed) / shelve.

## Rationale

### Why two-wave execution and not local filter generation

Codex's iter-2 stage-order honesty correction is the load-bearing
observation: in ADR 004's current stage graph, both producers run
concurrently with the join consumer downstream. A filter built
inside `execute_inner_join` runs *after* both shuffles are
complete. The bytes-movement bucket is paid before the join sees
its first build key. Reducing the bytes requires moving filter
generation *upstream* to the build-side exchange producer, which
forces a stage-order change.

### Why Bloom-only V1

Exact `InList` is qualitatively precise (zero false positives)
and useful for small dimension joins. It also expands the
cross-ring contract surface — Wendelon must implement exact
set-matching, possibly pushed into row-group dictionaries, before
the contract is proven. The first cross-ring runtime-filter ADR
proves the contract; it does not multiply filter kinds. V1.x
extends with `InList` once the Bloom path is stable.

### Why row-level reject as V1 enforcement

Bloom proves non-membership soundly per encoded key. Row-level
reject is the natural application: for each probe row, compute
the filter-key hash and reject if the hash bits aren't all set.
Row-group skip is tempting (skip whole row groups whose key range
doesn't intersect any matching key), but soundness requires
independent evidence — Bloom membership is per-encoded-key, not
per-range. V1 keeps row-level reject required and row-group skip
opportunistic.

### Why the advisory invariant is load-bearing

The advisory contract makes split-shipment safe: Toron can ship
filter emission whether or not Wendelon applies it. Without the
advisory invariant, ADR 006 would require lockstep Wendelon-side
landing for any bench evidence. With it, the bench evidence can
ship in two phases (Toron-only first, end-to-end second) without
breaking the contract.

The invariant also future-proofs version skew: when Wendelon
deploys a new version that drops the descriptor (e.g., during
rolling upgrade), queries succeed correctly without intervention.

### Why scale-boundary is now a structural section

Per the council's iter-5 convention adoption (Claude's iter-4
proposal, confirmed by all three voices), ADR 006 explicitly
classifies its scale boundary. ADR 006 touches three boundaries
(cross-ring, scheduler/fragmenter, transport), making it a clean
motivating case for the convention. Future ADRs use the same
section to declare their scale-boundary scope and to make
scope-creep into adjacent boundaries visible at review time.

### Why always-two-wave in V1 and not planner opt-in

The planner-stats surface doesn't exist yet (V1.x work). Without
selectivity stats, the planner can't make an informed opt-in.
Always-two-wave gives a simple contract: filter-enabled joins
pay the barrier universally. The non-selective guard benchmark
proves the cost is bounded. V1.x can add planner-opt-in once
stats exist; the descriptor contract is forward-compatible.

### Why split Wendelon-side coordination is safe

The advisory invariant + split-shipment posture means ADR 006
promotes against the Toron-side contract first, with the
Wendelon-side bench-evidence row landing as a follow-up. This
is structurally similar to ADR 002's pushdown contract, which
also predated Wendelon-side support for some predicate types.
The council pinned this posture explicitly to avoid a coupled
promotion that blocks on out-of-repo coordination.

## What Not To Do

- **Do not generate the runtime filter inside `execute_inner_join`.**
  Too late in the stage graph; can't reduce shuffle bytes.
- **Do not replace `partition_key_hash` or any exchange-partition
  hashing path.** ADR 003 §5 contract holds.
- **Do not name helpers generically** (e.g., "fast hash") in a
  way that blurs runtime-filter hashing and exchange-partition
  hashing.
- **Do not let filter-key encoding lose the logical-type tag.**
  Generalizes ADR 005's invariant.
- **Do not let the runtime filter affect join correctness under
  any condition.** Advisory always — if you find yourself relying
  on filter behavior for correctness, the design is wrong.
- **Do not regress digest equality** on any ADR 002 / 003 /
  004 / 005 fixture, filter-enabled or filter-disabled.
- **Do not start probe-side dispatch before the build-side
  runtime-filter barrier opens.** Hard gate 13.
- **Do not implement row-group skip without independent
  soundness evidence.** Row-level reject is V1; row-group skip
  is opportunistic and separately proven.
- **Do not deserialize the runtime-filter descriptor as a
  Toron-private type in Wendelon.** Shared cross-ring schema
  surface only.
- **Do not couple ADR 006 promotion to lockstep
  Wendelon-side landing.** Toron ships its half; Wendelon-side
  application is a follow-up bench row.
- **Do not multiply filter kinds in V1.** Bloom only. `InList`
  is V1.x.
- **Do not add planner opt-in in V1.** Always two-wave when the
  filter is enabled.
- **Do not skip the non-selective guard benchmark.** The
  two-wave barrier cost is real; the guard proves it's bounded.
- **Do not soften a `≥50%` selective-row bytes-cluster
  reduction miss** into a vague "improved" narrative in the
  bench artifact.
- **Do not extend filter eligibility to outer / semi / anti /
  sort-merge / non-equi joins in ADR 006.** Each is its own
  future ADR.

## Rejected and Deferred

### Streaming push-based executor (re-rejected)

Already rejected by ADR 005. ADR 005's post-promotion telemetry
confirms probe + output combined is under 15% of wall. No
probe-side dominance. Rejected outright again.

### Runtime-plumbing investigation (resolved)

ADR 005's conditional rule landed in the operator-adjacent band
(`framework_overhead_ms` fell 78.8%, proportional to build's
79.0%). Not an ADR 006 candidate.

### Second-pass hash-build optimization

ADR 005 took the obvious local win. A second-pass build ADR
would target ~270 ms further reduction with substantially more
invasive changes (concurrent build, JIT kernels) — lower leverage
than ADR 006's cross-ring approach at this evidence point.

### Exact `InList` filters

Deferred to V1.x. Adds Wendelon exact-set-matching surface; the
first cross-ring runtime-filter ADR proves Bloom, then `InList`
extends the same descriptor contract.

### Cost-based filter selection / planner opt-in

Deferred to V1.x. Planner-stats surface doesn't exist yet.

### Runtime filters for non-inner joins

Deferred. Outer / semi / anti / sort-merge / non-equi joins each
have their own ADRs; runtime filters for those join kinds belong
in those ADRs (or their own follow-on ADRs once they're stable).

### Runtime filters for broadcast joins

Deferred to V1.x. Broadcast joins already complete the build
before probe, so the two-wave-stage-order motivation doesn't
apply. The descriptor contract is forward-compatible —
broadcast-side filter generation can ship in V1.x reusing the
same `RuntimeFilterFragment` shape, with the application path
adapted (filter generated post-broadcast-build, attached to
probe-side scans of unicast-pull workers).

### Row-group skip heuristics beyond independently-sound cases

Deferred. The advisory contract permits row-group skip; the
implementation must independently prove skip soundness for any
heuristic beyond Bloom row-level reject.

### Cross-ring crate ownership (resolved as scheduling, not contract)

The descriptor lives in a shared cross-ring schema surface.
Final choice between extending an existing Wendelon-published
schema crate and creating a minimal `nilket-shared` is a Phase 1
implementation decision, not a contract-level one. Both options
preserve the boundary invariant.

### Wendelon-side internal scan-engine changes

Out of scope. ADR 006 pins the cross-ring contract surface
(descriptor + application semantics). Wendelon's internal
implementation of row-level reject and any row-group skip
heuristics is Wendelon-owned and ships under its own ADR
process.

### Topic E (cross-process wire efficiency)

Deferred. Lower leverage than runtime filters — Topic E moves
bytes faster; Topic A avoids moving rejected bytes. If ADR 006's
selective bench misses its target or non-selective guard regresses
beyond threshold, Topic E reopens as ADR 007.

### Launch-driven candidates (B / C / D)

Deferred per user direction (Evidence Pressure > Launch Pressure).
Topic B (outer / semi / anti joins) remains the leading
launch-driven candidate; Topic C (multi-tenant governance) wants
workload evidence; Topic D (Rune sandbox) is self-contained and
ships under its own narrow ADR when launch input promotes it.

## Council decisions

Three-way final convergence at iter-5. Iteration artifacts
retired.

1. **ADR 006 = Topic A: runtime filter pushdown into Wendelon.**
   User direction iter-4 (Evidence Pressure > Launch Pressure).
2. **Two-wave hash-partitioned execution** as the load-bearing
   stage-order shape (Codex iter-2 correction; three-way iter-3).
3. **Bloom-only V1, exact `InList` deferred** (three-way iter-2).
4. **Dual-benchmark shape** — selective + non-selective guard
   (Gemini iter-2 / three-way iter-3).
5. **Row-level reject as sound V1 enforcement; row-group skip
   opportunistic** (Codex / Gemini iter-2 row-group caution).
6. **`PartitionManifest` carries the build-side fragment**, via
   `TaskCompletionOutput::ShuffleProducer { manifest }` inside
   `TaskCompletePayload` (Codex iter-3 specificity).
7. **Always-two-wave V1; planner opt-in V1.x** (planner-stats
   surface doesn't exist).
8. **Shared cross-ring descriptor location**, with default
   recommendation to extend an existing Wendelon-published schema
   crate rather than spawn `nilket-shared` (Claude iter-5
   recommendation; final crate choice is Phase 1 implementation
   detail, not contract-level).
9. **Advisory invariant load-bearing**; split-shipment posture
   safe — Toron ships first, Wendelon-side bench evidence
   follows.
10. **Architecture invariant** — runtime-filter hash separation
    from exchange-partition hash. Generalizes ADR 005 pattern.
11. **Type-tag invariant generalizes** from ADR 005 (operator-
    local join-build key) to ADR 006 (cross-ring runtime-filter
    key). Hard gate 14.
12. **Adaptive Bloom sizing: 1% target FP rate, 16 KiB
    minimum, 1 MiB cap.** Optimal `m ≈ 9.59 · n` bits per
    distinct build-key count `n`; FP rate degrades above 1%
    only when the cap binds at high cardinality (advisory
    invariant preserves correctness). Codex iter-3 /
    Gemini iter-4 originally proposed fixed 64 KiB defaults;
    post-promotion review corrected the sizing math (64 KiB
    saturates at ~54K distinct keys, not 1M+). The 64 KiB
    sweet-spot remains the small-dimension build case
    (10K–100K).
13. **Probe-side `physical_descriptor_fingerprint` reuse** for
    descriptor validation (Codex iter-3 continuity with existing
    `ScanRequest` field).
14. **Stale retry attempts do not participate in coordinator
    merge** (Codex iter-3 ADR 003 task-failure taxonomy
    preservation).
15. **Barrier-correctness hard gate** — probe-side dispatch does
    not start before build-side barrier opens (Codex iter-3 hard
    gate 13).
16. **Scale-boundary as structural ADR section** — adopted as
    convention for this ADR forward; ADR 006 is the first ADR
    drafted under it (Claude iter-4 proposal; three-way iter-5).
17. **Wendelon-side coordination is scheduling, not contract** —
    ADR 006 promotion is not coupled to lockstep Wendelon-side
    landing. Hard gate 21.
18. **Filter eligibility narrow in V1** — inner equi-joins,
    hash-partitioned strategy, single-column canonical-encoder-
    supported key types. Broadcast filters, non-inner kinds,
    composite-key filter eligibility, and unsupported-encoder
    types deferred.
