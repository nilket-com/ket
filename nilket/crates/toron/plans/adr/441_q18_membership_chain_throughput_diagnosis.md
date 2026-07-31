# ADR 441 - q18 membership-first chain throughput diagnosis

## Context

The strict sf=10 board is 22/22. ADR 441 is diagnosis-only: no execution mechanism, no speed claim,
and no ordinary-path behavior change.

q18 is the largest retained throughput wall: ADR 356 measured a stable 222,182.319 ms median,
114.06x the retained 1,948 ms DuckDB median, 4,380,793,312 spill bytes, and 1,261,608 KiB peak
RSS. That median predates ADR 435's shared generic-fold migration, so it is a target-selection prior,
not a current-tree speed claim. ADR 441 first refreshes the live path and signature.

The owner is bounded. `try_streaming_projected_left_semi_grouped_aggregate`
(`exec/join/streaming.rs:8647`) executes the membership subtree, builds an `Int64PresenceSet`, and
drives the preserved chain through
`try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` (`streaming.rs:10568`).
`prepare_reoriented_inner_join_runtime_with_streamed_scan_build_baseline`
(`streaming.rs:13401`) prepares metadata and partitioning; the live spill work occurs inside the
base-to-sink drive.

ADR 424 supplies exact structural anchors: 624 qualifying order keys, a 16,384-byte presence set,
59,986,052 sink-input rows, 4,368 membership matches, about 624 aggregate rows before the outer
Sort/Limit, and 100 external rows. The presence filter currently runs in
`emit_reoriented_chain_output` after each chain batch has been materialized. That is a placement
observation, not a cost finding. Earlier membership application is only one possible ADR 442
mechanism and cannot be selected before the diagnosis.

The existing best-effort `TORON_MEMBERSHIP_SCHEDULE_RECEIPT_PATH` records milestones and row flow,
not exclusive elapsed time. It also deliberately swallows publication errors. It is retained as a
historical surface and is not reused for the ADR 441 acceptance receipt.

## Decision

### Phase 0: refresh and hard-stop on drift

Run one current release child with persisted strict sf=10 Parquet under the ADR 356 compute profile:

- `MemoryHigh=infinity`, `MemoryMax=infinity`;
- `AllowedCPUs=0-1`, `CPUQuota=150%`, `TasksMax=512`;
- `RuntimeMaxSec=3600` and fresh disk-backed join scratch.

Reconfirm the bound plan, fingerprint `d8b60e3a03bfaf79`, digest, schema, 100 external rows,
4,380,793,312 spill bytes, and the live
`try_streaming_projected_left_semi_grouped_aggregate ->
try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` branch. Record the live replay IDs,
partition/group geometry, and build/probe orientation. If any load-bearing path or signature has
drifted, stop and revise ADR 441 rather than instrumenting a stale branch.

Run one separate external `perf` child for qualitative cycle/syscall/category corroboration. It is
diagnostic overhead, is not compared to the unprofiled wall, and is not part of the five-child
stability gate.

### Required exclusive-span ledger

Add a query-neutral, off-by-default `MembershipChainProfile` in
`exec/join/membership_chain_profile.rs`, selected by the locked
`TORON_MEMBERSHIP_CHAIN_PROFILE_DIR`. Construct it only after every membership-first chain fence has
passed, immediately after the existing `selected/committed` point. Missing or empty input constructs
no profile state, sink, or clock.

The root begins at that committed point and ends after `finalize_bound_grouped_aggregate` returns.
The outer Sort/Limit is outside-root. Report `execute_ms - root_span_micros` separately with checked
subtraction; never fold it into the in-root residual.

The ledger is a phase-by-operation matrix. A leaf is one `(phase, operation)` pair, never a compound
label containing two separately actionable costs. At minimum the live path must cover these pairs:

| phase | required exclusive operations |
|---|---|
| membership | subtree execute; presence-set construction |
| lifecycle | terminal-runtime prepare; base-runtime prepare; finalization; cleanup |
| base build | storage scan/decode; partition/encode/spill write |
| base probe input | storage scan/decode; partition/encode/spill write |
| base join | spill-page read/decode; index build; lookup/selection; Arrow materialization |
| intermediate | partition/encode/spill write; spill-page read/decode; post-read concat/materialization |
| terminal build | storage scan/decode; partition/encode/spill write |
| terminal join | spill-page read/decode; index build; lookup/selection; Arrow materialization |
| sink | presence lookup/filter; exchange/repartition; grouped fold |

Only live pairs need nonzero counters, but every elapsed operation must belong to one pair or the
residual. Storage scanning, spill-page reads, index work, lookup/selection, and post-read/output
materialization have distinct timers. A release-path active-leaf check rejects nested or overlapping
leaves loudly. No per-row timer is allowed; accumulate at batch, partition, or helper-call boundaries.

Roll the pairs into stable reporting domains (membership, scan/spill, index/probe, materialization,
sink/fold, lifecycle). The mechanism discriminator uses both the dominant domain and the dominant
`(phase, operation)` leaf.

The checked in-root identity is:

`root_span_micros = sum(exclusive_leaf_micros) + residual_micros`.

The predeclared residual ceiling is 10% in every accepted child. Over-ceiling means add the missing
leaf and rerun; never move the ceiling. Overflow, overlap, a leaf outside the root, or a leaf sum over
the root invalidates the receipt loudly.

### Independent row-flow and spill reconciliation

The engine ledger does not own its expected q18 values. Extend the independent toron-tpch q18 oracle
receipt with `qualifying_lineitem_rows`: `Q18LineitemGroups` carries the row count of each order group,
and the oracle checked-adds that count only when the group passes `SUM(l_quantity) > 300`. Dense/frugal
oracle parity and boundary fixtures cover the new counter. The correctness comparator remains unchanged.

Each accepted child reconciles:

| independent/harness value | engine/root value | expected |
|---|---|---:|
| oracle qualifying orders | presence entries | 624 |
| oracle selected lineitems | sink input rows | 59,986,052 |
| oracle qualifying lineitems | sink matching rows | 4,368 |
| oracle qualifying orders | root aggregate rows | 624 |
| external result | post-Sort/Limit rows | 100 |

Count spill bytes separately for base-build, base-probe-input, intermediate, and terminal-build
roles. Checked addition of those four roles must equal the engine's 4,380,793,312-byte total. If a
fifth spill owner appears, name and add it rather than forcing the four-role identity.

Storage rows/bytes, page reads, index builds, selected/materialized rows, and spill bytes are
operation counters, not elapsed spans. Keep them separate from time and reconcile them by phase.

### Stability

Run five fresh ledger-enabled, perf-disabled strict sf=10 children under the same ADR 356 compute
profile. Acceptance requires:

- the same dominant reporting domain and same dominant `(phase, operation)` leaf in all five;
- `(max - min) / median <= 0.25` for every rolled-up domain share and the dominant leaf share;
- in-root residual at or below 10% in every child;
- fingerprint, digest, schema, external rows, root rows, row-flow counters, and spill totals invariant;
- maximum values reported but never used as the stability gate.

The single external-perf child remains qualitative and cannot veto or manufacture the ledger finding.

## Mechanism discriminator

ADR 441 names one dominant `(domain, phase, operation)` and hands ADR 442 one mechanism:

- storage scan/decode -> earliest safe key-preserving filter or scan/decode mechanism, selected by phase;
- spill encode/write or spill read/decode -> the named spill role and direction only;
- index build -> index reuse/construction mechanism for the named join phase;
- lookup/selection -> the named join-probe primitive;
- Arrow or post-read materialization -> selection-before-materialization for the named phase;
- presence lookup/filter, exchange, or grouped fold -> that single sink primitive;
- membership subtree, preparation, finalization, or cleanup -> that exact lifecycle owner;
- residual above 10% -> instrumentation widening, not ADR 442.

Earlier membership pushdown is eligible only if a pre-sink phase/operation dominates and the row-flow
geometry proves it can remove that work. Placement alone is insufficient.

## Acceptance

- A current-binary, five-child exclusive receipt names one stable dominant domain and
  `(phase, operation)` leaf with the 10% residual ceiling intact.
- The independent 624 / 59,986,052 / 4,368 / root-624 / external-100 identities pass in every child.
- The role-specific spill sum equals the invariant 4,380,793,312-byte engine total.
- Missing/empty profile configuration has zero state and sink; enabled/disabled execution has exact
  output, digest, schema, row, plan, spill, and ordinary telemetry parity.
- Atomic snapshot publication is loud on create/serialize/write/rename failure.
- No execution mechanism, cap change, query-number branch, speed claim, or early-filter change lands.
- `exec.rs` and the join-root ratchet remain unchanged; the new semantic module is reconciled on the
  ADR 176 allowlist; new async-held state is boxed if needed by the deep-recursion guard.
- Full Toron, architecture, toron-tpch, and Polars-reference matrices plus formatting and
  `git diff --check` pass. No profiler artifact is tracked and scratch is clean.

## Guardrails and risks

- Placement is not cost. The ledger, not the 59.99M-to-4,368 visual ratio, selects ADR 442.
- Composite timing labels are forbidden: they cannot distinguish scan from spill, or lookup from
  materialization.
- The ADR 356 median is retained history. Profiled timing is diagnostic and never replaces it.
- The legacy schedule receipt stays separate and unchanged; ADR 441's sink is durable and loud.
- Board status remains 22/22; this diagnosis does not imply a full-board rerun.

## Forward arc

ADR 442 owns the one selected q18 mechanism. q05 is the next large-wall diagnosis. q17 remeasurement
is a parallel evidence chore and does not imply a producer. A future q19 diagnosis must first widen
the ADR 438 profiler's leaf coverage.
