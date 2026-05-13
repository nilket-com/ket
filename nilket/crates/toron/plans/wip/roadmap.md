# Toron Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archeology, this file looks forward.

**Update cadence.** The roadmap is rewritten at planning
sessions, not at implementation commits. The council operates
on a block-chain rhythm: plan → implement → plan → implement,
and this file is a snapshot of the most recent plan. Between
sessions the roadmap lags the code on purpose — an item can
read "ADR 002 implementation phases pending" while several
phases have already landed on `main`. That is expected, not a
discrepancy. Don't open a session by flagging the gap as if it
were news; check `git log` and the relevant impl plan /
bench-results file for actual status, and roll roadmap edits
into the next planning session along with the forward-sequence
rewrite.

## Status

ADR 001 (Toron Foundation and Engine Contract) is promoted at
`plans/adr/001_toron_foundation_and_engine_contract.md`. The
seed implementation (`feat: adr 001 M0`–`M11`) landed in one
stretch to anchor the codebase against architectural
deliberation. The seed is **scaffold-grade, not V1-grade**:

- M0 (monorepo scaffold) and M10 (encrypted spill) meet their
  ADR 001 hard gates.
- M1–M9, M11 are seeded but do not satisfy ADR 001's fixture
  discipline. Notable gaps:
  - **PBC pushdown is dead in the runtime path** — the
    compiler exists, the executor never engages it.
  - **String-column eval-time lookup** is the hot path.
  - **`TenantId(0)` and `HlcBound(0)` are dummies** in every
    `ScanRequest`.
  - **`GROUP BY` errors `NotImplemented`.**
  - **M5 / M6 distributed mechanics** use in-process duplex
    mocks; no crash-only gate, no real shuffle determinism.
  - **M9 RDMA transport** is a placeholder.
  - **Rune sandbox fuel** is commented out.
- **Errors are `.to_string()`'d** at every module boundary.

ADR 002 (Storage-Backed Execution Contract) is council-
converged across Claude / Codex / Gemini after three topic
iterations and three ADR-draft iterations under `plans/wip/`.
Source plans live at
`plans/adr/002_toron_storage_backed_execution_contract.md`
and
`plans/impl/002_toron_storage_backed_execution_contract.md`.
Promotion is gated on a Wendelon-native fixture matrix
delivering 10 hard gates and 4 soft gates.

V1 target is **"First Query with Integrity"**: correct
analytical results against a Wendelon-native cluster with
pushdown maximized, identity flow real, and a stream-shaped
storage boundary that does not bake in full-result buffering.
End-to-end bounded memory (executor streaming, shuffle
backpressure, spill triggers) is **ADR 003 + multi-tenant
governance** territory, not an ADR 002 promise. Advanced
shuffles (spilling shuffles, RDMA) and distributed joins are
V1.5 / V2.

## Shipped foundations

One-line index. Full context in each ADR; bench evidence under
`plans/impl/<id>_*_bench_results.md`.

- **ADR 001 Toron Foundation and Engine Contract**
  (2026-05-12) — Greenfield Arrow-native execution, no
  DataFusion, single `crates/toron` package, `toron-ir` as the
  engine contract, `StorageScan` boundary trait. M0 monorepo
  scaffold landed cleanly. M1–M11 seeded as scaffold, not as
  satisfying ADR 001's hard gates.
- **(ADR 002 promotion pending — see §Forward sequence #1.)**

## Brainstorms & High-Level Vision

- **Brainstorm 001: Pure Cranelift JIT as a First-Class Query Specializer (Speculative)** — JIT-compiling hot kernels (predicates, UDFs) into native machine code for zero-copy, bare-metal performance. Reserved for post-V1 deliberation.

## Prioritization philosophy

Toron is the compute ring of the Nilket stack. Wendelon is the
storage ring. We control both ends. The boundary exists for
scaling dynamics and resource isolation, not because storage
and compute are independent products.

The seed implementation made a deliberate choice: ship M0–M11
as scaffold to anchor architectural deliberation in real code
rather than deliberating every commit. The council session that
produced ADR 002 was the first to engage that anchor. Going
forward, we expect a slower per-ADR cadence with stronger
fixture discipline, matching Wendelon's pattern (ADR 070
template).

**Toron's reason for existence is smart-storage pushdown.**
Until the runtime path engages Wendelon's PBC, Toron is an
Arrow runtime that happens to call Wendelon. ADR 002 fixes
that. Every other improvement (executor model, distributed
shuffle, advanced kernels, frontend hardening) polishes a
system that doesn't do its primary job until PBC engagement
is load-bearing.

**ADR 003 scope is evidence-driven.** Codex iter-2's framing:
ADR 002's bench evidence determines whether ADR 003 attacks
the executor model (if post-pushdown workload still stresses
the buffered executor) or the distributed runtime (if pushdown
reduces executor pressure and M5 / M6 mocks are the loudest
gap). The most likely outcome is distributed runtime, but the
council commits only after evidence.

## Forward sequence

1. **ADR 002: Storage-Backed Execution Contract (Pushdown
   Engagement).** Council-converged; pre-implementation.
   Plans at
   `plans/adr/002_toron_storage_backed_execution_contract.md`
   and
   `plans/impl/002_toron_storage_backed_execution_contract.md`.
   **Keystone:** PBC pushdown is load-bearing in the runtime
   path. **Scope:** schema-bound IR via binder pass
   (`LogicalPlan` → `ExecutionPlan`), minimum catalog with
   `MockCatalog` + `WendelonCatalog`, descriptor-fingerprint
   pinning protocol, all-or-nothing pushdown with IR-reserved
   residual via `FilterDisposition` enum, projection
   dual-path with minimality gate, `ScanStream` streaming
   interface, V1 aggregate contract (`CountAll`,
   `Count(expr)`, `Sum(expr)`, `GROUP BY`), Arrow null
   semantics pinned (including `SUM(all-null) = Null`
   uniformly across `Int{8..64}` / `UInt{8..64}` /
   `Float{32,64}`), structured `thiserror` taxonomy, 7
   architecture guards, 10 hard / 4 soft promotion gates.
   **Implementation phases:** P0 audit and `chore:` cleanup,
   P1 structured errors, P2 IR split + binder, P3 catalog,
   P4 PBC pushdown wiring, P5 grouped aggregate, P6
   fingerprint-pin coordinator, P7 architecture guards, P8
   bench matrix and promotion.

2. **ADR 003: Distributed Runtime and Data Movement
   Contract** *(scope evidence-driven from ADR 002 bench
   results).* Candidates:
   - **Most likely scope: distributed runtime.** Multi-process
     coordinator / worker mechanics, real QUIC transport (not
     in-process duplex), task identity newtypes (`QueryId`,
     `StageId`, `MorselId`, `AttemptId`, `PartitionId`),
     retry / idempotence rules, worker failure detection +
     redispatch (ADR 001 §M5 crash-only hard gate), result-
     stream lifecycle + cancellation, shuffle resilience
     (push vs pull vs persistent shuffle service), receive-
     side backpressure with real resource accounting (not the
     seed's `tokio::time::sleep` mock).
   - **Alternative scope: executor model.** If ADR 002's
     bench evidence shows the buffered executor is the
     limiting shape, ADR 003 instead pins streaming /
     morsel-driven / pull-based execution. Less likely; ADR
     002's pushdown should narrow most filter workloads
     significantly.
   - **Alternative scope: catalog / schema registry.** If
     descriptor evolution proves frequent in V1 deployment,
     ADR 003 hardens the catalog beyond fingerprint-pin.
     Least likely; ADR 002's minimum-viable catalog is
     designed to defer this.

3. **ADR 004+ candidates** *(scope and ordering deferred
   pending ADR 002 / 003 evidence)*. Each ships its own
   contract surface; explicitly **not** to be bundled into
   ADR 003:
   - **Distributed join topology** *(Gemini topics iter-1).*
     Broadcast vs hash-partitioned join, bloom filter
     pushdown into Wendelon, skew detection, join-key null
     semantics, zero-copy join materialization. Depends on
     ADR 002 grouped-aggregate semantics and ADR 003
     shuffle.
   - **Multi-tenant resource governance** *(Gemini topics
     iter-1).* Per-query / per-tenant CPU + memory + spill +
     stream-bytes budgets. Token-bucket vs scheduler-slot
     admission. Memory-rejection vs spill-trigger
     thresholds. Depends on ADR 002 identity flow and ADR
     003 query lifecycle.
   - **Rune sandbox enforcement.** Native-function
     allow-list, fuel engagement (currently commented out at
     `rune_engine.rs:188`), per-tenant CPU + memory budgets,
     no filesystem / no network gating. V1 launch
     prerequisite; localized to `rune_engine.rs`.
   - **Advanced kernel graduations** (one ADR per kernel,
     Wendelon ADR 070 narrow-decision pattern):
     - **`LinearRegress` stable contract** — current
       implementation is straight OLS without condition-
       number guard, no centered formula, no rank check.
       Numerical hardening + stability across numeric types.
     - **`ApproxQuantile` stable contract** — current
       extraction is `sort + index` of KLL retained values,
       which discards the rank statistics KLL was designed
       to preserve. Real rank-based estimation + cross-impl
       parity with Wendelon's KLL substrate.
     - **`TimeBucket` stable contract** — bucket-alignment
       semantics across timezone, duration, and pre-epoch
       inputs.
   - **Wendelon-side crate extraction** *(Codex topics
     iter-1 §Topic 6).* When V1.x consumer pressure (Polariton,
     Baryon, external Toron SDK) materializes. Candidate
     extractions: `wendelon-types` (PBC ops, identity,
     fingerprint), `wendelon-net` (transport facade),
     `wendelon-schema` (descriptor types). Defer until
     consumer requirements force the boundaries.
   - **Subtree-split pushdown** (V1.x). Lifts ADR 002's
     `FilterDisposition::FullyPushed | EvaluateInToron` to
     add `PartialResidual { residual_predicate }`. Requires
     a planner-side cost model.
   - **Encrypted spill governance** (V1.x). Spill lifecycle
     beyond M10 seed: key rotation, AAD policy, crash
     recovery, observability, multi-query isolation. Seed
     uses AES-256-GCM-SIV without rotation.
   - **Visualization-as-compute IR operators.** Density
     tiles, 2D binning, quantile-per-pixel. Waits for
     Polariton's tile protocol.
   - **Hybrid vector search.** Vector ANN with metadata
     pushdown into Wendelon. Waits for Wendelon vector-
     aware placement (Wendelon roadmap §"set aside in the
     Toron-pivot stretch").
   - **Full DDL propagation.** Async invalidation
     broadcasts, multi-coordinator schema consensus,
     descriptor versioning beyond fingerprint-pin.
   - **External federation** (Postgres / S3 / BigQuery
     adapters). Explicit ADR 001 §"Deferrals" item — V2+,
     "one external adapter as a capability proof," never on
     the Wendelon-native hot path.

4. **Refinement plumbing (opportunistic, no ordering
   constraint).** Items that improve the existing seed but
   don't fit a single ADR:
   - **Decimal numeric types** in IR `ScalarValue` and
     aggregate semantics (currently absent; deferred by
     ADR 002 §"Resolved Q2").
   - **Capability-table aggregate dispatch** replacing the
     enum (when the aggregate catalog grows beyond
     hand-listable).
   - **Newtype hygiene pass** — `Symbol` / `ColumnId`
     instead of bare `String` for column refs in narrower
     contexts; `PbcProgram` already a newtype as of ADR 002.
   - **Wire-protocol versioning policy** when the
     coordinator-worker wire (ADR 003) needs cross-version
     compatibility.
   - **Streaming end-to-end** if ADR 003 chooses the
     buffered executor — V1.x flip without ADR 002 wire
     break.

## What Not To Do Next

Enduring guardrails for upcoming work. Items tied to specific
shipped ADRs are recorded in those ADRs' §"What Not To Do"
sections.

- **Don't adapt DataFusion / Ballista.** ADR 001 rejected
  this; ADR 002 reinforces with `datafusion-functions` also
  rejected for V0. The greenfield decision is final unless
  a future ADR cites concrete evidence reversing the
  surface-area-mismatch argument.

- **Don't promote ADR 002 without a Wendelon-native fixture
  matrix.** TPC-H or hand-rolled in-memory fixtures are not
  sufficient evidence for the pushdown contract. The fixture
  corpus must exercise ADR 060 / 063 / 064 / 069 / 070
  storage-side investments (sparse projection, page stats,
  encoded validity, all-null elision, FSST). Without this
  evidence, ADR 002 has not proven Toron's smart-storage
  premise.

- **Don't bundle ADR 003's scope before ADR 002 evidence.**
  The council has agreed ADR 003 scope is evidence-driven.
  Picking executor model vs distributed runtime vs schema
  registry before ADR 002's bench results is speculation.

- **Don't open ADR 003 with a mega-scope.** Distributed
  runtime / executor model / shuffle resilience / wire
  protocol versioning are real V1 concerns, but they have
  their own contract surfaces. Bundling them defers real
  decisions behind a larger one. Land them independently
  when evidence drives each.

- **Don't ungate experimental kernels by default.** ADR 002
  requires `PlannerConfig::allow_experimental_kernels =
  false` as the default. Each of `LinearRegress`,
  `ApproxQuantile`, `TimeBucket` graduates to stable
  contract under its own narrow ADR with bench evidence —
  the Wendelon ADR 070 pattern, not the seed's
  ship-and-iterate pattern.

- **Don't put `Result<_, String>` in any public or
  module-crossing signature.** ADR 002 architecture guard
  #3 enforces. The seed's `.to_string()` boundaries are
  technical debt being paid off in ADR 002 Phase 1.

- **Don't call `RecordBatch::column_by_name` in operator
  evaluation.** ADR 002 architecture guard #1.
  `ResolvedColumn::ordinal` is the only column access path
  in `ExecutionExpr`.

- **Don't skip descriptor-fingerprint validation on PBC
  pushdown.** Silently-wrong-results is the failure mode ADR
  002 pays complexity to prevent.

- **Don't extract Wendelon types into a shared crate before
  V1.x consumer pressure.** ADR 001 deferred this; ADR 002
  uses the impl-plan import allowlist instead. Extraction
  needs more than one consumer (Polariton, Baryon, external
  Toron SDK) to earn its keep.

- **Don't surface `PushdownDeclineReason` publicly.**
  Crate-private diagnostic only; no metric label, no
  fallback variant, no operator-visible toggle. Production
  behavior collapses every decline to the non-pushdown
  Toron path.

- **Don't put projection logic in `Scan`.** `Scan` carries
  the column ordinals it fetches via
  `ScanRequest.projected_columns`. `Project` is a separate
  operator that produces final output shape and order.
  Conflating them muddles the dual-path contract.

- **Don't promote stabilization watch items into roadmap
  items.** Observability metrics, architectural-test
  guards, post-merge dashboards belong in runbooks and
  impl plans. Decision-enabling measurement belongs inside
  the owning ADR.

- **Don't reopen the DataFusion question without citing the
  greenfield-distance evidence from ADR 002 bench
  results.** Compile-time cost, dependency tree size, and
  ecosystem-pull arguments are not sufficient grounds —
  ADR 001's rejection cited surface-area mismatch with
  Wendelon's pushdown vocabulary, which ADR 002 makes
  load-bearing.

- **Don't let `ScanStream` collapse to materialization at
  every callsite.** ADR 002 architecture guard #7
  enforces. `.collect()` is allowed only at the
  coordinator's result-materialization edge; intermediate
  operators consume the stream shape directly.

- **Don't write the parity normalizer as a per-test
  helper.** One canonical concat-and-reslice helper shared
  across every parity test (ADR 002 §"Resolved Q4").
  Scattering normalization logic across tests defeats the
  point.

- **Don't introduce per-operator error types.** One
  crate-level `ExecutionError` enum, structured via
  `thiserror`. ADR 002 §10 enforces.

- **Don't bypass the Wendelon import allowlist.** Production
  code may consume `wendelon::schema::{pbc, *types}`,
  `wendelon_client::*`, `wendelon::network::pool::*`, and
  `wendelon::storage::file_ops::*`. Forbidden:
  `wendelon::network::raft::*`, `wendelon::network::quic::*`,
  `wendelon::storage::lsm::*`, ad-hoc page readers, anything
  inside Wendelon's node / raft / transport internals.

- **Don't enable RDMA / RoCEv2 / SIW / RXE in the V0
  transport.** QUIC only through M6 (ADR 001) and through
  ADR 003 unless ADR 003 explicitly opens the second
  transport tier. The seed's M9 "RDMA transport seams" is a
  placeholder; treat it as such.

- **Don't promise byte-identical Arrow output to dense
  fallback without applying the canonical normalization.**
  PBC-engaged and pure-Toron paths may produce different
  batch boundaries; the normalization (concat + reslice)
  is the contract, not raw batch equality.
