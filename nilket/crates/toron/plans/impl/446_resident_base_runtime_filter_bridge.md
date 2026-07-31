# claude iter-3 — impl 446: shape-fenced local runtime-filter bridge

Converges iter-2 with the operator's request-plumbing and filter-composition
seam. Paired with `claude_iter_2_adr_446.md` (ADR text unchanged except the
composition rule and ordinal-space note folded into Decision/Guardrails —
see "ADR deltas" at the end). Toron-home; no Wendelon change expected.

## Seams (grounded)

| seam | file:line | role |
|---|---|---|
| base accumulation completes first | `exec/join/streaming.rs:12050-12076` | awaited loop fills `base_batches`; capped at `:12053` |
| consumer takes the complete slice | `:12255` | `base_batches: &[RecordBatch]` |
| terminal lineitem scan | `:12345` | `build_partitioned_streaming_scan_inner_build_reservoir` |
| the fence parameter | `:15425` | `null_safe_keys: bool`, already threaded |
| **request construction** | `exec.rs:260-289` `bound_scan_request` | supplies `physical_descriptor_fingerprint` (`:285`), `projected_columns` (`:287`), and `runtime_filter: self.context.runtime_filter.clone()` (`:289`) |
| second construction site | `exec.rs:382` | also clones `context.runtime_filter` |
| **context-wide filter** | `exec.rs:110`, `:184` | `ExecutionContext.runtime_filter`, set by `with_runtime_filter` |
| program-keyed entry (does not fit) | `exec/runtime_filter.rs:25` | needs `&BoundExprProgram` |
| ordinal-keyed path (private today) | `exec/runtime_filter.rs:164` | `build_bloom_runtime_filter_fragment` |
| descriptor conversion (reused unchanged) | `exec/runtime_filter.rs:264` | `descriptor_from_fragment(fragment, probe_key_columns: Vec<u32>, fingerprint)` |
| scheduler precedent (does not fit) | `distributed/scheduler.rs:975-1005` | needs `TaskSpec`/`ShuffleProducer`/`runtime_filter_merges`/shuffle `partition_key` |
| **filter applied here** | `storage.rs:636-643` | `validate_runtime_filter_descriptor(rf, batch.schema(), request.physical_descriptor_fingerprint)` then `build_runtime_filter_mask_for_batch` |
| **projection applied AFTER** | `storage.rs:680` (and `:889` via PBC) | `request.projected_columns` |

## The ordinal space — settled: pre-projection source ordinals

`probe_key_columns` are **pre-projection source (logical) scan ordinals**, on both
scan paths.

- **In-memory path** (`scan_in_memory_batch`, `storage.rs:582-706`): filter at
  `:636`, projection at `:680` — the filter runs against the pre-projection batch.
- **Wendelon path** (the one q05/sf=10 persisted Parquet actually uses,
  `storage.rs:860+`): `pbc` (with projection appended at `:889`) and
  `request.runtime_filter` are both handed to
  `table_query_stream_with_runtime_filter_and_attempt_hook`. Server-side,
  `runtime_filter_mask_for_projected`
  (`crates/wendelon/src/network/data_server.rs:10335-10355`) resolves each
  `probe_key_columns` ordinal via `ProjectedRecordBatch::physical_for(*ordinal)` —
  a **logical→physical** mapping. The descriptor speaks source ordinals; Wendelon
  does the mapping.

**Projection reordering or omission is therefore safe and is NOT a reason to
decline.** If projection drops the column, `physical_for` returns `None` →
`RuntimeFilterDropReason::MaterializationIncompatible` → the server calls
`observe_storage_runtime_filter_advisory_dropped` and **returns the unfiltered
batch** (`data_server.rs:10383-10390`). The filter is advisory at the storage ring.

**The trap is the ordinal space itself, not projection.** `l_orderkey: Int64` and
`l_suppkey: Int64` (`toron-tpch/src/lib.rs:776,778`) are both in q05's lineitem
projection. Wendelon checks only the **type at the resolved ordinal**
(`data_server.rs:10349-10351`, `schema/runtime_filter.rs:136-138`), so an ordinal
in the wrong space that lands on the sibling `Int64` **passes validation** and
filters lineitem by whether `l_suppkey` is in the dated-**orderkey** bloom —
silently dropping correct rows. Pin this with a test; do not trust the validator.

`bound_scan_request` supplies the fingerprint directly
(`physical_descriptor_fingerprint`, `exec.rs:285`). Take it from *that* request.

## The composition rule — never silently overwrite

`bound_scan_request` sets `runtime_filter: self.context.runtime_filter.clone()`
(`exec.rs:289`), and `ExecutionContext.runtime_filter` (`:110`) is cloned into
**every** scan request the context builds (`:289`, `:382`). Two consequences:

1. **The local descriptor must not go through the context.** A context-level
   assignment would apply the lineitem filter to every scan in the query. It must
   be threaded as an explicit optional parameter and assigned to the **terminal
   scan request only**.
2. **`ScanRequest.runtime_filter` is a single `Option`** — two filters cannot
   coexist. If `context.runtime_filter` is already `Some` (the distributed-worker
   case), the local bridge **declines** and leaves the existing descriptor intact.

Declining is correct, not merely cautious: a runtime filter is always an
optimization — dropping it admits extra rows the join's equality rejects — so
refusing costs reduction, never correctness. **Merging is rejected**:
`fragments_compatible` (`exec/runtime_filter.rs:286`) and `merge_fragments`
(`:306`) check kind/parameter compatibility, **not probe-key identity**, so
merging two filters keyed on different columns would produce a descriptor whose
`probe_key_columns` is a lie. Never overwrite; never merge.

## Grounding steps before the diff (blocking; abandon if unresolvable)

1. **Base-side key ordinal.** Which of `partition_build_keys` / `write_build_keys`
   / the runtime's `probe_keys` names the base's `o_orderkey` in `base_batches`'
   projected schema? Do not infer from the join's logical keys.
2. **Terminal scan source (pre-projection) probe ordinal.** Trace
   `StreamingScanProbeInput` → the `PartitionedScan` → `bound_scan_request`
   (`exec.rs:260`). The ordinal must be in the **source scan schema** per above —
   Wendelon maps it to physical via `physical_for`. **Least-grounded link.**
3. **Fingerprint provenance.** Take `physical_descriptor_fingerprint` from that
   exact request (`exec.rs:285`), never a separately-resolved pin.
4. **Source key availability.** Decline only when the join key is **unavailable in
   the source scan schema** or its type is unsupported/unvalidatable. Output
   projection reordering or omitting the key is **not** a decline condition —
   Wendelon resolves via `physical_for` and drops advisorily if absent.
5. **`context.runtime_filter` already `Some`** → decline, preserve the existing
   descriptor.

## Cut

**1. Behavior-neutral refactor** (`exec/runtime_filter.rs`, travels with the ADR):
split the private ordinal path out as
`build_runtime_filter_fragment_for_ordinal(batches, ordinal, key_type)`; make
`build_runtime_filter_fragment(batches, key)` a thin wrapper resolving
`single_supported_key` and delegating. Scheduler path unchanged.

**2. Thread an optional local descriptor** through the reservoir/write helpers —
`try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir`
(`streaming.rs:12255`) → `build_partitioned_streaming_scan_inner_build_reservoir`
(`:15417`) → the terminal scan request construction. Pointer-sized `Option`,
`None` on every other path (zero cost), mirroring ADR 444's observer discipline.

**3. Assign at the terminal request only:**

```rust
let mut req = self.bound_scan_request(...)?;
if req.runtime_filter.is_none() {
    req.runtime_filter = local_descriptor;      // never overwrite context's
}
```

Build the local descriptor in `:12255` before `:12345`, only when
`!null_safe_keys`, the key type is supported, the join key is present in the
**source** scan schema, and `context.runtime_filter` is `None`. Convert with
`descriptor_from_fragment(&fragment, vec![source_probe_ordinal],
req.physical_descriptor_fingerprint)`. Output projection is not consulted —
Wendelon resolves via `physical_for`.

**4. Nothing else.** No cap change, no reorientation, no ADR 132 comparator
change, no Wendelon edit.

## Tests

**Mandatory — failures nothing downstream catches:**

- `adr446_descriptor_names_the_join_key_not_a_sibling_int64` — fixture with two
  `Int64` key columns (mirroring `l_orderkey`/`l_suppkey`); assert
  `probe_key_columns` names the join key. Wendelon's type check passes either way.
- `adr446_probe_ordinal_is_a_source_scan_ordinal` — a projection that **reorders**
  columns so the source and physical positions differ; assert the emitted ordinal
  is the **source** one and the filtered output equals the unfiltered output.
  **Fails if a post-projection/physical ordinal is used** — the exact
  silent-wrong-answer path, and the reorder is what makes the two spaces
  distinguishable.
- `adr446_projection_omitting_the_probe_key_still_returns_correct_rows` — projection
  drops the key; assert the filter is advisorily dropped
  (`MaterializationIncompatible`) and rows are correct **without** the bridge
  declining. Pins that omission is Wendelon's job, not ours.
- `adr446_existing_context_filter_is_preserved` — with `context.runtime_filter =
  Some(..)`, assert the local bridge declines and the original descriptor survives
  on the request. **Fails if the local filter overwrites.**

**Discriminating (must fail under the rejected alternative):**

- `adr446_null_safe_chain_declines_the_runtime_filter` — null-safe chain with null
  probe keys retains every row. **Fails if the `!null_safe_keys` fence is
  removed** — the one way this cut loses rows.
- `adr446_unsupported_key_type_declines`.
- `adr446_missing_source_key_declines` — the join key is absent from the source
  scan schema → no descriptor emitted.

**Correctness:**

- `adr446_filtered_output_equals_unfiltered_output` — identical rows with/without
  the filter, on a fixture with null probe keys and a forced bloom false positive.
- `adr446_rejected_descriptor_drops_filter_and_stays_correct` — deliberate
  fingerprint mismatch → `FingerprintMismatch` → dropped, rows correct.

**Regression:**

- `adr446_reservoir_input_rows_fall_under_the_filter` — measured build rows drop
  on a filtered fixture. Verify-first probe; rides the impl commit.
- `bound_scan_request_carries_every_bound_scan_field` (`exec.rs:3334`) stays green
  — it already pins `runtime_filter` propagation and must not regress.
- Scheduler-path fragment tests (`exec/runtime_filter.rs:508,563,601,648`) stay
  green **unmodified** — proof the refactor is behavior-neutral.

**Architecture guard** (`tests/architecture/exec_decomposition.rs`, shaped like
439's and 445's ordering fences):

- `adr446_runtime_filter_is_fenced_to_non_null_safe_chains` — slice the owner
  block; assert the fragment build occurs **after** base accumulation and
  **before** the terminal scan, guarded by `!null_safe_keys`; assert the assign
  site is guarded by `req.runtime_filter.is_none()`. Ordering and fence identity,
  not co-presence.
- Assert the bridge is absent from the streamed and other join paths.

Avoid the string `monotonic` in any test or source name (contains `tonic`; trips
`adr043_no_grpc_protobuf_or_arrow_flight`, which source-scans `src` and `tests`).

## Evidence to produce

- Strict q05 sf=10 before/after: status, digest, schema, **row count** identical.
  The descriptor is attached at request execution, so record and cross-check the
  unchanged plan fingerprint `757d7c625669b21a`.
- **Measured** reservoir input: `build_rows` before (59,986,052) and after (~17.9M
  expected at the 1 MiB cap). The 17.3% FPR is computed, not observed.
- Spill bytes before (5,353,985,328) and after (~1.57 GB expected).
- Persisted-Parquet harness scans must apply the descriptor to the full
  source-schema batch before PBC/projection; the behavioral source-ordinal test
  and the q05 receipt are required to prove the bridge is not a no-op.
- Unprofiled five-child wall, scope order randomized/interleaved and recorded. A
  wall claim **is** permitted if it clears 23.86% — state band and measured delta.
- 4 GiB / no-swap survival, scratch empty before and after.

Clear `target/tpch/join_spill` before any sweep — post-430 the gauge spills there,
not `/tmp`; stale scratch breaks sf=10 runs.

## ADR pairing

Superseded by `claude_iter_3_adr_446.md`, which folds the composition rule, the
source-ordinal statement, and the advisory-drop semantics directly. Use the iter-3
pair for promotion; `claude_iter_2_adr_446.md` is retained only for the
convergence trail.

## Seal gate (council.md, via scripts/cargo-cgroup, after fmt)

```
scripts/cargo-cgroup fmt  -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
```

Widen to `-p wendelon` only if a Wendelon change proves unavoidable — which would
mean this cut's placement premise failed. Run the FULL `architecture_test` after
`fmt`. Record gates green only after observing them green, never from a summary.
