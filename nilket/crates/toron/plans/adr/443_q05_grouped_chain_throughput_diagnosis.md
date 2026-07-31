# ADR 443 - q05 grouped-chain throughput diagnosis

Diagnosis-only (433/438/441 discipline). No mechanism, no engine behavior change, no speed claim. q05 is
the largest un-diagnosed wall and the worst retained ratio.

## Context

q05: **177,026.577 ms / 270.68x DuckDB**, 5,353,985,328 spill bytes, ~3.06 GiB peak RSS, and the
noisiest stable baseline on the board (**spread 0.1193**, 159.6-180.8 s). Its lineage is residency, not
throughput: ADR 429 diagnosed the 4 GiB OOM (tmpfs shmem charging); ADR 430 moved spill to disk-backed
scratch. **Where the completed run's 177 s goes is unmeasured**, and any future mechanism faces a
`max(0.10, 2 × max(0.1193, …))` ≥ **23.86%** band — so the diagnosis must name a producer big enough to
matter at that band and characterize the wall's variance as evidence in its own right.

**Owner geometry (grounded — the root must cover the whole wall).** The reservoir sub-owner
`try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir` (`streaming.rs:11967`) receives
**already-built base batches**: base accumulation happens earlier in
`try_streaming_reoriented_inner_chain_generic_base_grouped` (`streaming.rs:11793`), runtime preparation
earlier still, and aggregate finalization in the **top-level grouped-chain owner**
`try_streaming_reoriented_inner_chain_grouped_aggregate` (`streaming.rs:9506`). A root at the reservoir
sub-owner could neither carry a base-accumulation leaf nor explain the 177 s wall.

**Two spill domains (grounded).** Task telemetry records **5,353,985,328** total spill bytes, but ADR
430's receipt attributes only **5,270,315,856** to the reservoir — a **83,669,472-byte** difference
owned by earlier runtime/build spill. A reservoir-only reconciliation cannot honestly claim the total.

**Independent oracle (what exists, what doesn't).** q05 already has `strict_q5_oracle`
(`toron-tpch/src/lib.rs:5798`) exposing **final selected rows and groups** (`Q5Oracle`, `:5791`). It has
no intermediate stage counters, and ADR 429 was not a generator census — so intermediate row-flow
identities are **not predeclared here**; Phase 0 maps the physical stages first and the oracle is
extended to exactly the counters those stages need.

## Decision — perf names the category (qualitative); a REQUIRED exclusive-span ledger measures the geometry

**Root boundary.** The root is the **top-level grouped-chain owner** (`streaming.rs:9506`): from
**after structural admission** through **aggregate finalization and runtime cleanup**. It includes
runtime preparation/build, base accumulation, the reservoir phases, suffix/fold, and finalization. The
**outer Sort is outside-root** (`execute_ms − root` reported separately).

**Phase 0 — reconfirm and map before any hook.**
1. One current strict sf=10 run under the exact 356 profile: live owner chain
   (`9506 → 11793 → 11967`), fingerprint, digest, schema, rows, and the 5,353,985,328-byte spill
   signature on the current binary (the 356 median is retained, not post-435/440/441).
   Stop-and-revise if the path drifted.
2. **Map the physical stages** of the live chain (runtime prep/build, base accumulation, reservoir
   write/read/probe, suffix, fold, finalize) with their row/byte boundaries — this mapping, not
   invention, determines the ledger leaves **and** the oracle extension.
3. **Extend `strict_q5_oracle` (or add a sibling census)** with exactly the independent stage counters
   the mapping shows are needed; the predeclarable identities today are the existing final
   `selected_rows` and `groups`.
4. One qualitative external perf child — outside every gate.

**Phase 1 — REQUIRED off-by-default exclusive-span ledger** on a **new query-neutral surface**
(`TORON_GROUPED_CHAIN_PROFILE_DIR`; name locked at promotion), constructed only after shape admission;
absent/empty ⇒ no state; atomic tmp-then-rename publication with loud failures. **The ADR 429 residency
profiler and its schema-1 receipts are preserved untouched** — residency evidence remains valid; the
throughput ledger coexists on its own surface and does not reinterpret schema 1.

Nine leaves (named by role; anchored by the Phase-0 mapping; mutually exclusive; timer-nesting rejected):

| leaf | role |
|---|---|
| runtime preparation / build (incl. earlier build spill write) | pre-reservoir runtime + its spill |
| base accumulation | the `:11793` owner's accumulation work |
| reservoir partition + spill write | reservoir write phase |
| base concat | reservoir-input concat |
| reservoir read + probe | reservoir read/decode + probe |
| suffix join | post-reservoir suffix stages |
| grouped fold | aggregate fold |
| finalization | grouped finalization |
| cleanup | teardown |

The **residual is a derived value, not a leaf**: `residual = root − Σ(nine leaves)`, predeclared
**≤ 10%**, widen-instrument-never-move. Storage scan/read counters and spill bytes recorded
**separately** from elapsed spans.

**Two-domain spill reconciliation (delta form).** The ledger records
`task_spill_at_root_start` and `task_spill_at_root_end`; the reconciled quantity is the **delta**
`task_spill_at_root_end − task_spill_at_root_start`, which must equal the sum of the two ledger
domains (`earlier runtime/build spill + reservoir spill`) **and** equal the run's final
5,353,985,328-byte total (no other spill producer outside the root). The reservoir domain is expected
≈ the 430-shaped 5,270,315,856 with the earlier domain owning the 83,669,472 delta — exact values
refreshed on the current binary in Phase 0. Delta form removes absolute-counter ambiguity.

**Row-flow identity:** the existing oracle's final `selected_rows` / `groups` equal the engine's final
counts, 5/5; the Phase-0 oracle extension adds the mapped intermediate counters, which are then exact
5/5. No invented intermediate identity is predeclared.

**Stability (5-child ledger gate).** Five fresh children: the **same dominant leaf in all five**, and
the **dominant leaf's normalized share spread ≤ 0.25**; all nine leaf shares reported; residual ≤ 10%
each; result tuple invariant. (No domain rollup — the discriminator maps each leaf to one mechanism, so
domains add nothing.) Given the 0.1193 baseline spread, the cohort's **wall distribution is itself
recorded evidence** — the stability picture the eventual ADR 444 band is built on. The qualitative perf
child is not in the gate.

**Isolation.** q17 remeasurement may proceed as a parallel workstream, but its measurements are
**temporally isolated** from every q05 profiling child — never concurrent on this host.

## Mechanism discriminator (for ADR 444, not 443)

- **reservoir partition/spill write dominates** → one-pass/write-side reservoir mechanism (with the 442
  lesson: only a decisively large producer justifies a cut against a ≥23.86% band);
- **reservoir read/probe dominates** → read-side/index mechanism;
- **base accumulation or runtime prep/build dominates** → upstream accumulation/build cut;
- **suffix join or grouped fold dominates** → optimize that measured primitive;
- **residual > 10%** → widen the ledger and rerun; do not promote an under-covered finding.

## Acceptance

- **Named dominant leaf** for the grouped-chain root — **same leaf 5/5, normalized share spread
  ≤ 0.25** — backed by the nine-leaf partition (derived residual ≤ 10%), the **delta-form two-domain
  spill reconciliation to 5,353,985,328**, the oracle-anchored row-flow identities (existing finals +
  Phase-0-mapped intermediates), and the qualitative perf category.
- **Off-by-default, query-neutral, coexisting with the untouched 429 residency profiler** (guard-pinned:
  schema 1 and its env surface unmodified); artifacts under `target/tpch/adr443/`.
- **No engine behavior change, no mechanism, no speed claim.** Strict sf=10 under the exact 356
  envelope; one persisted strict sf=10 q05 `pass@compare` under 4 GiB/no-swap as the survival control.
- **No query branding.** `exec.rs` 7,499; join-root ratchet holds; box new held state if an async body
  grows.

## Guardrails and risks

- **Root honesty** — the top-level owner is the root precisely so the 177 s is fully explained; a
  sub-owner root would silently orphan accumulation and preparation.
- **Oracle honesty** — intermediate identities come from the Phase-0 mapping, not predeclaration.
- **429 evidence preserved** — "superseded" is the wrong disposition for still-valid residency receipts.
- **Variance honesty** — the cohort wall distribution is reported even if ugly; it is the future band.

## Forward arc

**ADR 444** builds the one mechanism the discriminator selects, against a ≥23.86% band. **q17
remeasurement** proceeds in parallel, temporally isolated. q18 strict one-pass and bimodality
characterization, q21 residual refinement, q19 re-diagnosis + 438-profiler widening, q13 fresh
diagnosis, and Cut A (439 mirror) remain queued.
