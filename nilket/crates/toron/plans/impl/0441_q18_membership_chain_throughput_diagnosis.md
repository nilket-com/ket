# ADR 441 implementation - q18 membership-chain exclusive diagnosis

Diagnosis-only. Promote and commit this plan before source changes. Leave implementation and results
uncommitted for independent review.

## Phase 0: current-tree evidence

1. Build one release binary from the committed plan parent and retain its SHA-256 plus the sf=10
   manifest hash.
2. Run persisted strict q18 sf=10 under the exact ADR 356 compute profile with fresh
   `target/tpch/join_spill`.
3. Persist the bound plan and prove the live chain:
   `try_streaming_projected_left_semi_grouped_aggregate` -> committed membership-first branch ->
   `try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` -> grouped finalization.
4. Reconcile fingerprint `d8b60e3a03bfaf79`, digest, schema, 100 external rows, and
   4,380,793,312 spill bytes. Record replay IDs, partition/group counts, and orientation. Stop and
   revise if any load-bearing item drifts.
5. Run one separate `perf record --freq 99 --call-graph dwarf,16384` child (or the host-supported
   equivalent) and retain zero-lost-sample status plus broad categories. Do not compare its wall to
   the unprofiled child and do not gate on category stability.

## Phase 1: independent oracle counter

In toron-tpch:

- add checked group-row counting to `Q18LineitemGroups`;
- add `qualifying_lineitem_rows` to `Q18Oracle`;
- checked-add a group's row count only when its sum is strictly above the q18 threshold;
- update the test-only dense oracle and require dense/frugal equality at sf=0.01 and sf=1;
- lock the exact-threshold failure, above-threshold success, multi-row group, final group, missing
  order, and counter-overflow cases;
- keep `compare_strict_q18` value semantics unchanged.

The current sf=10 oracle receipt must independently report 624 qualifying orders,
59,986,052 selected lineitems, and 4,368 qualifying lineitems.

## Phase 2: profile owner and module

Add `exec/join/membership_chain_profile.rs` and expose only the query-neutral types needed by
`streaming.rs`. Add the file to the ADR 176 semantic-module allowlist.

Locked configuration: `TORON_MEMBERSHIP_CHAIN_PROFILE_DIR`.

`MembershipChainProfile::from_lookup` returns `Ok(None)` for missing/empty input. `from_env` is called
only after the committed membership-first shape fences. Store the optional profile behind a pointer if
the deep-recursion test shows async-state growth.

The module owns:

- schema version and per-process profile ID;
- `MembershipChainPhase` and `MembershipChainOperation` enums;
- one active exclusive `(phase, operation)` leaf at a time;
- checked elapsed/call/row/byte counters per pair;
- root start/finish and checked residual;
- role-specific spill counters;
- row-flow and aggregate-output counters;
- atomic temporary-write/rename publication with propagated errors.

Begin the root after the existing `selected/committed` milestone and finish it after grouped
finalization. Persist only after root completion. Record outside-root time in the results harness,
not as a ledger leaf.

Unit locks:

- disabled construction creates no state or directory;
- empty configuration is disabled;
- nested/overlapping leaves fail loudly in release behavior;
- leaf outside root, double root, overflow, and leaf-sum-over-root fail loudly;
- exact residual and domain roll-up;
- four-role spill checked sum, including an unknown-role mismatch;
- atomic publication and loud failure;
- schema round trip and phase/operation name stability.

## Phase 3: live-path hooks

Thread `Option<&MembershipChainProfile>` only through the admitted chain owner and helpers used by the
current q18 branch. Do not add a query identity check.

Instrument the phase-by-operation matrix at batch/partition/helper-call boundaries, never per row.
At minimum:

- owner: membership subtree, presence construction, terminal runtime preparation, finalization,
  cleanup, root aggregate rows;
- base-to-sink: base-runtime preparation and the base/intermediate/terminal phase transitions;
- scan-build helper: storage scan/decode separately from partition/encode/spill write, tagged with
  base-build or terminal-build role;
- base-probe input loop: storage scan/decode separately from partition/encode/spill write;
- reservoir probe helper and loaded-partition work: spill read/decode, index build,
  lookup/selection, and Arrow materialization as distinct operations, tagged base-join or
  terminal-join;
- intermediate path: partition/encode/write, later spill read/decode, and post-read concat as distinct
  operations;
- sink: presence lookup/filter, exchange/repartition, and grouped fold as distinct operations.

Where a helper currently fuses two operations, add internal boundaries rather than naming the wrapper
as one composite leaf. The active-leaf guard proves the timers do not overlap. Preserve existing task
telemetry and storage spans; the new ledger observes but does not replace them.

Count spill bytes at their existing owners:

- base-build reservoir;
- base-probe input partitions;
- intermediate partitions;
- terminal-build partitions.

Do not reuse the currently combined `intermediate_spill_bytes` value for two roles; retain its
components, then check their sum against final engine telemetry.

Record presence entries, sink input/matching rows, root aggregate rows, scan rows, page reads, index
builds, selected rows, and materialized rows. Every child must reconcile to the independent oracle and
external result.

## Phase 4: parity and architecture tests

- A synthetic admitted membership-first chain executes with the profile absent and enabled; output
  batches, canonical digest, schema, root rows, spill bytes, task telemetry, and cleanup are identical.
- The enabled fixture exercises at least one storage-scan, spill-read, lookup, materialization, sink,
  finalization, and cleanup leaf with exact root arithmetic.
- The legacy `TORON_MEMBERSHIP_SCHEDULE_RECEIPT_PATH` fixture remains unchanged.
- An architecture gate requires the new module, env-gated post-fence construction, phase/operation
  routing, and absence of qNN/TPC-H branding; it rejects a composite leaf combining scan/read with
  materialization.
- Run the existing deep-recursion/reoriented-chain regression explicitly.

## Phase 5: live diagnosis

Build one final release binary after all gates and record its hash. Discard any earlier live receipts if
the binary changes.

Run five fresh ledger-enabled, perf-disabled q18 children under the ADR 356 compute profile. Each child
must provide:

- the invariant fingerprint/digest/schema/100-row/spill tuple;
- 624 presence entries, 59,986,052 sink rows, 4,368 matches, and 624 root aggregate rows;
- the four-role spill sum equal to 4,380,793,312;
- root = exclusive leaves + residual, residual <= 10%;
- phase/operation counters and domain roll-ups.

Compute per-child dominance. Gate on the same dominant domain and `(phase, operation)` in all five and
`(max-min)/median <= 0.25` for all domain shares plus the dominant leaf share. Report maximums but do not
gate on them. If the residual exceeds 10%, add the missing operation leaf and rerun all five accepted
children; never move the ceiling or mix pre/post-schema receipts.

Run one persisted strict q18 `pass@compare` under the standard 4 GiB/no-swap envelope to prove the
diagnostic state did not disturb survival. This is a q18 control, not a full-board redux.

## Seal gates

Run via `scripts/cargo-cgroup` after formatting:

```text
fmt -p toron -- --check
test -p toron --lib
test -p toron --test architecture_test
test -p toron --no-fail-fast
test -p toron-tpch
test -p toron-tpch --features polars-sql-reference --no-fail-fast
```

Also run `cargo fmt --all -- --check`, `git diff --check`, the deep-recursion guard, no tracked
`.perf.data`, line-count ratchets, no query branding, and fresh/empty scratch checks.

## Results and seal

Write `plans/impl/0441_q18_membership_chain_throughput_diagnosis_results.md` with:

- Phase 0 signature and current-tree wall;
- qualitative perf categories, explicitly outside the stability gate;
- independent oracle counters;
- five per-child phase×operation matrices, domain roll-ups, residuals, row/spill reconciliation, and
  stability arithmetic;
- the single selected ADR 442 mechanism or an honest under-coverage result;
- disabled/enabled parity, 4 GiB q18 control, test gates, hygiene, and discarded-attempt disclosure.

Commit only the canonical ADR and implementation plan before implementation. Leave source, tests, and
the results document uncommitted for independent review.
