# impl 509 - q17 partitioned build indexed-at-flush plan

This is the execution and measurement plan for ADR 509. It intentionally separates three durable
deliverables:

1. an exact-coordinate Phase 0 attribution/residency instrument and receipt;
2. only after a green Phase 0, the indexed-at-flush mechanism with a same-binary control;
3. a paired performance receipt and, only on a green verdict, production activation.

Review iterations within a deliverable stay in the working tree and squash into its seal. A second
commit earns its place only when it is independently useful and revertible. Every timing identity is
clean, pushed, and never amended after measurement.

## Locked inputs

The comparison baseline is ADR 508's clean q17 row:

| field | locked value |
|---|---|
| implementation | `632777908780f17bf15d1d7d43125b6e28f67d99` |
| coordinate | strict sf=8, persisted Parquet |
| dataset verification | `full_hash` |
| memory per task | 2,147,483,648 B, explicit |
| spill reserve | 2,147,483,648 B, explicit |
| Toron median | 35,541 ms |
| materiality floor | 3,554 ms |
| spill | 622,544,968 B |
| route | `partitioned_streaming_scan_build`, replay 0 |
| `join0_build` | 19,595.5 ms |

The existing 64 MiB / 512 MiB exploratory runs are not baseline evidence. They may identify source
branches or stage names but may not contribute a time, share, partition count, group count, cache
bound, or resident byte to Phase 0.

Every command in this plan supplies:

```bash
TORON_TPCH_MEMORY_PER_TASK=2147483648
TORON_TPCH_SPILL_BYTES_RESERVED=2147483648
```

The receipt rejects a row whose numeric fields differ, whose budget provenance says `default`, whose
dataset verification is not `full_hash`, whose build stamp is dirty, or whose route differs. The
paired journal already locks both numeric budgets; its manifest is extended to lock the selected
shape-preserving gate and ADR 509's fixed 3,554 ms materiality floor as well.

## Phase 0A - invocation-bound spill refinements

### Adapter

Keep `GroupedChainSubLeaf` as the common vocabulary, but separate timing ownership from the existing
`GroupedChainSpillObserver`.

Add a small spill-observer interface used by the existing hook locations. Two adapters implement it:

- the current grouped-chain adapter, preserving q21 fields and parent semantics byte-for-byte;
- an ADR 508 stage adapter holding the correlated-scalar invocation's `VocabularyHandle`, stable
  invocation id, and `join0_build` refinement parent.

The q17 adapter opens and closes `StageToken`s; callers never submit durations. `WriteFinishFsync`
gets an explicit caller-side token around `finish`, because `finish` currently has no observer
parameter. The adapter is removed or closed before the parent stage closes.

Required time leaves for the committed route:

- `write_scan_decode`
- `write_partition_routing`
- `write_ipc_encode`
- `write_encryption`
- `write_physical_write`
- `write_finish_fsync`
- `read_physical_read`
- `read_decrypt`
- `read_ipc_decode`
- `read_cache_lru`
- `read_index_rebuild_cpu`

`writer_setup` remains a refinement of `join0_group_open`. Probe lookup and materialization remain
under `join0_probe`. A required leaf that is absent or failed is coverage-incomplete even when the
arithmetic sum happens to close.

Declare a `Flow` byte domain and the time residual boundary before the first profiled q17 run.
Repeated leaves sum with checked arithmetic. Identity mismatches, poisoned telemetry, inverted
bands, orphaned tokens, and overflow publish no trusted value and fail the invocation.

### Cache and I/O counters

Counters use explicit domains and parent ownership:

| counter | kind | parents |
|---|---|---|
| physical read calls/pages/bytes | flow | `join0_build`, `join0_probe` |
| decrypt calls/bytes | flow | `join0_build`, `join0_probe` |
| IPC decode calls/rows/bytes | flow | `join0_build`, `join0_probe` |
| decoded-cache hits/misses/pages/bytes | flow | `join0_build`, `join0_probe` |
| decoded-cache resident bytes | peak/current | per build object and summed active group |
| spill ciphertext bytes | flow | handler |

Flow counters sum; peaks reconcile by maximum and are never added across time. Per-object cache
fields carry stable partition identity so a group sum cannot accidentally treat one 8 MiB cap as a
global cap.

The profile emits:

- total partition count;
- group count and each group's partition range;
- active partitions per group;
- per-partition cache cap/current/peak;
- build and probe cache/read counters;
- q17 subleaves and their parent reconciliation;
- every outer ADR 507/508 reconciliation verdict.

### Phase 0 profiling and pricing

Build one clean release binary from a pushed Phase 0 seal. Use a fresh output directory and clear the
profile path and join-spill scratch before each child.

Run at least three systematically interleaved subleaf-off/subleaf-on pairs with node/handler
attribution active in both arms. The on arm must preserve digest, schema, row count, plan, route,
spill bytes, group count, partition count, and both budget fields. Record parent-stage and
whole-query movement; do not call an unresolved point estimate an overhead bound.

For each valid on run compute:

```text
gross_build_reread =
    read_physical_read + read_decrypt + read_ipc_decode
```

The Phase 0 time gate is green only if every valid run's gross envelope exceeds 3,554 ms. The receipt
also reports median, range, residual, and any observed profiler perturbation. `ReadIndexRebuildCpu`
is reported but excluded from the removable gate.

No per-row timer or sampled estimator is authorized. External CPU classification may be recorded as
diagnostic context but cannot turn a red gross-envelope gate green.

## Phase 0B - resident-overlap proof

Instrument the exact production owners rather than deriving from row counts alone. For every active
group record:

- `partition_count`, `group_count`, group range, and active partition count;
- task memory budget and spill reserve with explicit provenance;
- executor/task resident baseline before group creation;
- page/file/path/handle metadata charged by the resident guard;
- summed current and peak writer plaintext buffers, scratch refs, and page reserve;
- summed current and final resident index bytes across all active partitions;
- summed current and peak decoded-cache bytes across the same objects;
- current production resident charge;
- counterfactual indexed-at-flush overlap;
- minimum remaining headroom.

The counterfactual must use the same checked arithmetic and ownership definitions as the production
resident guard. It may not double-count a component already included in a parent charge or omit an
object because its partition has no matching probe rows.

Required tests:

- a synthetic multi-partition group whose summed per-object caches exceed one cache cap;
- a group where the largest writer peak and largest index do not belong to the same partition;
- empty partitions, one-row partitions, and a partial final page;
- checked failure on every overflow or missing component;
- a fixture that fits after scratch release but fails when index and scratch overlap;
- dynamic attribution disabled allocates no partition/counter vectors beyond production state.

The residency gate is green only when every q17 group at 2 GiB fits with positive, explicitly
reported headroom under the existing guard. Neither group count nor any budget may change to make it
fit.

## Phase 0C - stop/go receipt

The receipt has exactly four verdicts:

| time gate | residency gate | result |
|---|---|---|
| green | green | admit indexed-at-flush |
| red | green | close: removable envelope too small |
| green | red | close: overlap violates resident contract |
| red | red | close with both reasons |

A red receipt ends ADR 509. Do not implement and price a substitute.

## Phase 1 - indexed-at-flush mechanism

### Writer mode

Introduce a typed writer mode rather than reading the environment in spill code:

```text
CoalescedPageIndexMode
    RebuildAfterWrite
    IndexAtFlush
```

The correlated-scalar dispatcher resolves the production/ablation setting once and passes the mode
only to the `PartitionedScanBuild` leaf. Other routes and unindexed reservoirs retain
`RebuildAfterWrite`.

For `IndexAtFlush`:

1. select compact `Int32`/`Int64` or legacy Generic/null-safe index representation before the first
   flush;
2. retain the key/page/row refs produced by `write_page_batch_inner`;
3. insert them only after the page write has succeeded, so no index can refer to an unwritten page;
4. update resident-index bytes and run the existing guard after every flushed page;
5. preserve the coalesced buffer's page boundaries and tail flush;
6. after `finish`, prove the indexed row count matches the accepted non-null build-row count;
7. release writer scratch and skip rebuild.

An error after a physical page write but before all refs are inserted fails and cleans the build; it
does not retry, fall back, or expose a partial index. An error before the write leaves both page and
index unchanged. Existing fail-loud checks remain.

### Same-binary switch

Use one pure-parser environment switch, provisionally
`TORON_PARTITIONED_SCAN_BUILD_INDEX_AT_FLUSH`:

- `1`: candidate `IndexAtFlush`;
- absent or `0`: control `RebuildAfterWrite`.

Do not latch it in a process-global `OnceLock`; deterministic tests must exercise both arms in one
process. Production remains control until the final paired verdict.

### Unit and integration tests

Pin:

- identical page count, per-page rows, payload ordering, ciphertext/spill byte count, and finish
  state across modes;
- exact lookup multiset and duplicate order for compact Int32, compact Int64, Generic, null-safe,
  duplicate, and null-skipping keys;
- input fragments coalescing across batch boundaries and a partial final page;
- at least two active partitions, including an empty partition;
- candidate index completeness after every successful flush sequence;
- candidate zero rebuild calls and zero build-side page rereads;
- control nonzero rebuild/read counts for a nonempty spill;
- cache counters before probe for both arms;
- typed resident failure during index growth with no fallback;
- injected write, finish, index-insert, and cleanup failures;
- empty join-spill scratch after success and all failures.

Run strict q17 correctness at exact 2 GiB / 2 GiB with both arms. Require identical digest, schema,
row count, plan fingerprint, route, partition/group counts, spill bytes, and join-spill observations.

## Phase 2 - paired rail and measurement

### Shape-preserving gate

Today a custom `--paired-env` invokes ADR 469's gate, which requires distinct plan fingerprints.
Indexed-at-flush intentionally preserves the plan, so reusing that gate would reject the correct
implementation.

Add an explicit paired-gate selection to the harness and configuration-locked journal. The new
shape-preserving mechanism gate runs one child per arm and requires:

- both pass from the same binary and full-hash dataset;
- equal plan fingerprint, route, digest, schema fingerprint, row count, group/partition cardinality,
  page geometry, ciphertext/spill bytes, and budgets;
- candidate: index-at-flush count is nonzero, rebuild count and build reread count are zero;
- control: index-at-flush count is zero, rebuild count and build reread count are nonzero.

Missing lifecycle counters fail the gate. A stale profile is impossible because the harness clears
its path before each child and associates it with build stamp, query, run index, arm, and invocation
id. The selected gate and its expected invariants are serialized into the paired manifest so resume
cannot mix gate semantics.

Keep ADR 449's durable per-row journal, resume config lock, full-hash admission, systematic arm order,
and estimator unchanged. ADR 509 predeclares a fixed 3,554 ms engineering floor from the immutable
ADR 508 baseline; the generic harness currently recomputes `10% * current OFF median`. Add an explicit
fixed-floor input (or an equivalent receipt-side verdict) and serialize that policy and value into
the manifest. The final ADR 509 verdict uses 3,554 ms even if the generic, cohort-relative `material`
field differs.

### Immutable measurement identity

After Phase 1 tests pass:

1. squash review WIP into one implementation seal with control still the production default;
2. run formatting, architecture, Toron library, workspace, telemetry, and both TPC-H feature gates;
3. push that seal and never amend it;
4. build the release binary from that clean pushed identity;
5. record binary hash/mtime and verify every row carries that clean build stamp.

The paired run uses a fresh output directory:

```bash
TORON_TPCH_MEMORY_PER_TASK=2147483648 \
TORON_TPCH_SPILL_BYTES_RESERVED=2147483648 \
target/release/toron-tpch perf \
  --sf 8 --mode strict --runs 20 --warmup 1 --query q17 \
  --engines toron --data-source parquet --data-dir target/tpch/data \
  --paired-ablation \
  --paired-env TORON_PARTITIONED_SCAN_BUILD_INDEX_AT_FLUSH \
  --paired-on 1 --paired-off 0 \
  --paired-gate shape-preserving-spill-index \
  --paired-materiality-floor-ms 3554 \
  --output-dir target/tpch/adr509_q17_paired_<build>
```

The exact gate spelling is implementation detail; the semantic choice and manifest lock are not.
Clear join-spill scratch around every pair and require it empty at cohort end.

Primary differences are `control - candidate` q17 `execute_ms`. Use the existing deterministic
Hodges-Lehmann point estimate and two-sided 95% Wilcoxon signed-rank interval over 10 adjacent pairs.

Verdict:

- **activate:** interval excludes zero in the improving direction and point estimate is at least
  3,554 ms;
- **detectable, immaterial:** interval excludes zero but point is below 3,554 ms;
- **material, unresolved:** point is at least 3,554 ms but interval includes zero;
- **close:** neither.

Only `activate` changes the production default. No extra samples are appended after seeing the
result.

### Diagnostic accounting

Run a separate profiled cohort from the same binary and coordinate. Do not mix its times into the
primary paired estimator. Report:

| diagnostic | control | candidate | candidate - control |
|---|---:|---:|---:|
| build physical read/decrypt/decode | | | |
| probe physical read/decrypt/decode | | | |
| build/probe cache hits and misses | | | |
| pages and decoded bytes by parent | | | |
| `join0_build` and `join0_probe` | | | |
| net diagnostic leaf movement | | | |

State profile overhead/pricing and both cohorts' provenance. A leaf decrease without an absolute-time
decrease is not corroboration. Cache relocation is a measured result, not inferred from cache
capacity.

### Activation and receipt

If green, land a cohesive receipt/default-activation commit. The default flip selects the exact
candidate path already measured; run a clean default-arm q17 correctness smoke from the activation
commit and cite it. If not green, land a receipt with control unchanged.

In either result, record:

- immutable implementation identity and binary identity;
- all Phase 0 artifacts and verdicts;
- exact coordinate and dataset fingerprint;
- all 20 raw measured rows, order, paired differences, HL estimate, interval, and materiality floor;
- shape-preserving gate record;
- page/spill/residency/cache diagnostics;
- post-change sf=8 route census;
- gate results and join-spill cleanup;
- the final production default.

No full q01-q22 performance board is required. Any newly reached cell from the route census gets a
correctness control before activation.

## Repository gates

Run every Cargo leg through `scripts/cargo-cgroup`, never raw `cargo`. Complete build and test gates
before starting a measurement cohort; no wrapped build may compete with a perf child.

Council seal gate:

```bash
scripts/cargo-cgroup fmt -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
git diff --check
```

Feature/build legs for the surfaces this ADR changes:

```bash
scripts/cargo-cgroup fmt -p toron-tpch -- --check
scripts/cargo-cgroup check -p toron --features telemetry --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup check -p toron-tpch --features polars-sql-reference --all-targets
```

Also run strict q17 correctness for both arms at explicit 2 GiB / 2 GiB. Repeat the applicable seal
gate after any code change made in response.

Read the exit status of the asserted command itself. Do not pipe the gate through `tail` or a
zero-match `grep` whose status changes the meaning of the check.

## Phase 0 measured outcome

### Seal and coordinate

Phase 0 was implemented, sealed, and pushed as
`a52b4773f4f7`. The release binary was built from that clean, unamended identity after all build and
test processes had stopped. The cohort used:

```text
query                    = q17
mode                     = strict
scale                    = 8
data source              = persisted Parquet
dataset verification     = full_hash
memory per task          = 2,147,483,648 B (explicit)
spill reserve            = 2,147,483,648 B (explicit)
route                    = partitioned_streaming_scan_build
profile switch           = TORON_PARTITIONED_BUILD_SPILL_PROFILE
fixed materiality floor  = 3,554 ms
```

Artifacts are under `target/tpch/adr509/phase0_a52b4773/`. Every child used a unique node-profile
path, and `target/tpch/join_spill` was empty before it started. The three adjacent pairs alternated
order:

| pair | order | off `execute_ms` | on `execute_ms` | off - on |
|---:|---|---:|---:|---:|
| 1 | off, on | 36,122.636 | 35,174.178 | 948.458 ms |
| 2 | on, off | 35,711.535 | 34,984.398 | 727.137 ms |
| 3 | off, on | 35,170.121 | 34,812.718 | 357.403 ms |

The point movement favors the profiled arm in all three pairs, but three pairs at this spread do not
resolve profiler overhead. No overhead bound or performance improvement is claimed.

All six rows carry:

- `build_commit = a52b4773f4f7`, without `-dirty`;
- `dataset_verification = full_hash`;
- identical digest, output schema fingerprint, one-row count, and plan fingerprint;
- the same `scan_grouped` plus `correlated_scalar_left_outer_global` invocation routes;
- the same `partitioned_streaming_scan_build` spill observation at replay 0;
- exactly 622,544,968 B whole-query and invocation spill;
- explicit 2 GiB task and 2 GiB spill-reserve fields;
- zero in-tree time residual, zero unattributed node spill, zero interleaved frames, zero bridge
  mismatches, and zero stage mismatches;
- a passing outer reconciliation, with 674-707 microseconds outside the node-tree span against the
  predeclared 10 ms boundary.

The off arm contains no required build refinements, spill-I/O domains, or residency diagnostic. The
on arm contains 11 required `join0_build` refinements, two parent-scoped spill-I/O domains, and one
residency diagnostic. Both arms pass their declared contracts, which proves the switch changed the
measurement lifecycle without changing the query shape or answer.

### Removable-time gate

The gate excludes `read_index_rebuild_cpu` and sums only the parent-scoped build reread:

```text
read_physical_read + read_decrypt + read_ipc_decode
```

| on run | physical read | decrypt | IPC decode | gross reread | fixed floor | verdict |
|---:|---:|---:|---:|---:|---:|---|
| 1 | 375.085 ms | 108.680 ms | 640.096 ms | 1,123.861 ms | 3,554 ms | red |
| 2 | 333.914 ms | 108.292 ms | 635.516 ms | 1,077.722 ms | 3,554 ms | red |
| 3 | 321.745 ms | 107.460 ms | 635.243 ms | 1,064.448 ms | 3,554 ms | red |

Median gross reread is 1,077.722 ms; the range is 1,064.448-1,123.861 ms. Even the largest run is
2,430.139 ms short of the fixed floor and reaches only 31.6% of it. Therefore the time gate is red.

The build reread is complete and stable as a byte domain in every on run: 6,059 pages,
622,544,968 physical bytes, and 1,921,305,272 decoded bytes. Its enclosing `join0_build` is
19,142.517-19,538.679 ms and its refinement residual is 457.099-466.256 ms. The separately reported
`read_index_rebuild_cpu` envelope is 6,275.730-6,316.786 ms, but remains inadmissible to the
removable gate because it combines key reconstruction with required index insertion.

The probe parent independently reports 6,059 physical misses and 45,150 decoded-cache hits, plus a
second 622,544,968 physical bytes and 1,921,305,272 decoded bytes. These control-path counters make
future cache-transfer reasoning explicit; without a candidate arm they do not predict net saving.

### Residency gate

Every run reports 16 partitions in four groups of four. All four groups in all three runs pass both
the production retained-footprint guard and the stricter literal 2 GiB task-budget comparison.

Across all groups and runs:

| resident fact | value |
|---|---:|
| candidate complete-owner overlap | 188,622,572-189,470,823 B |
| minimum literal task-budget headroom | 1,958,012,825 B |
| minimum production retained-cap headroom | 34,170,267,545 B |

The largest observed group carried four per-object 8 MiB cache caps, a 154,593,408 B summed final
index, 1,329,152 B summed writer high-water, 262,144 B page reserve, and 80,047 B metadata. Its
complete candidate overlap was 189,470,823 B. The residency gate is green without changing the task
budget, spill reserve, group count, partition count, or accounting owner.

### Stop/go verdict

| time gate | residency gate | result |
|---|---|---|
| red | green | close: removable envelope too small |

This is the complete ADR 509 result. Phase 1 and Phase 2 were not entered: no indexed-at-flush mode,
paired shape-preserving rail, route census, mechanism correctness matrix, or production-default
change was built. Production remains rebuild-after-write. The red result is not used to negotiate a
lower threshold, add samples, rescue `read_index_rebuild_cpu` with a perturbing per-row estimator,
or pivot to another mechanism.

The repository seal before measurement was green: `toron` lib 1,416/1,416, architecture
2,093/2,093, the complete `toron` and `toron-tpch` suites, both formatting legs, telemetry and
all-target checks, and `polars-sql-reference`. Strict q17 correctness also passed in profiler-off and
profiler-on arms at its supported sf=0.02 oracle coordinate with explicit 2 GiB / 2 GiB budgets. The
sf=8 Phase 0 rows themselves provide the persisted-data shape/result invariants; the existing q17
correctness rail rejects sf=8 rather than silently substituting an oracle coordinate.
