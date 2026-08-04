# ADR 438 - q19 throughput diagnosis (keyed join → OR residual → global Decimal SUM)

## Context

**Fresh independent thread.** q13's thread closed with the 437 rejection. q19 is **not** the next-slowest
gauge query — q05/q18/q21 are slower — but it is the **cleanest unaffected high-ratio compute target**: a
stable baseline untouched by recent cuts, a compute signature, and a well-understood plan shape.

**Baseline (grounded).** The current strict q19 baseline is the completed **ADR 356 strict sf=10** run:
**83.45 s**, fingerprint `65c3c2c0b0a99063`, **51,471,552 spill bytes**, seven stable samples, **116x
DuckDB**. Its **89x Polars** figure is **reach + schema-only, not correctness-validated**, and is
labelled as such wherever it appears. (ADR 366's 2,031 s sf=10 timeout is historical and superseded.)

**Unaffected-baseline rationale.** ADR 435 did not touch q19 because **q19 is an ungrouped global
aggregate** and 435 migrated the *grouped* fold path. q19 is **not** on the specialized primitive
grouped-SUM path (`aggregate.rs:36-317`, grouped-only).

**Signature — compute wall, I/O softened.** The 356 parent shows 99% CPU and **zero filesystem-input
blocks**: that rules out **cold filesystem reads only** — not scan/decode CPU, not page-cache-hot spill.
D4 (scan/build/spill) stays a real candidate.

**Known shape.** `Aggregate(global) → Filter(residual OR) → Join(Inner, keyed on partkey)`
(`toron-tpch/src/lib.rs:243-268`). ADR 319 (`adr/0319_q19_or_key_extraction.md:12,16`) **extracted the
bridging `p_partkey = l_partkey` out of every OR branch and removed it from the residual** to expose the
keyed Inner join, so the residual the engine evaluates is `(A) OR (B) OR (C)` with **no partkey
equality**. Each of A/B/C still AND-s a distinct brand / container-IN / quantity-range / size-BETWEEN
plus the **common** `l_shipinstruct = 'DELIVER IN PERSON'` and `l_shipmode IN ('AIR','AIR REG')`. Whether
those common conjuncts are **physically** repeated is a Phase-0 census question (see below). The final
aggregate is a single global `SUM(l_extendedprice * (1 - l_discount))` over Decimal128; the arg is not a
kernel — `evaluate_bound_single_root(&call.args, …)` per batch, folded row-by-row (`aggregate.rs:776-782`).

**Repetition is structural and physical, not textual.** `BoundExprProgram` is a **node-indexed DAG**;
`evaluate_bound_program_without_storage` (`expr.rs:374`) allocates one output slot per node and evaluates
each **physical node exactly once per batch** (children by index, `evaluated_child`). There is no runtime
short-circuit. The SQL/AST repeats the common conjuncts, but the bound program **could share node IDs** —
so the eager cost is **one evaluation per physical node per batch**, and the physical repetition factor is
a Phase-0 finding, not an assumption.

**Fused dispatch is a Phase-0 finding.** `try_streaming_collapsing_global_aggregate` (`aggregate.rs:428`)
is **eligible** but selects several scan/spill/in-memory/fallback branches. The **live branch, build/probe
orientation, replay ID, and fingerprint `65c3c2c0b0a99063` / 51,471,552 spill bytes are reconfirmed
before any ledger hook is placed.**

**Instrumentation gap.** `execute_ms` is one undifferentiated span (`lib.rs:3433-3454`).
`TaskTelemetryPhases::probe_micros` (`task.rs:175`) **cannot represent D1** — on the streamed-spilled path
it encloses probing, materialization, residual filtering, and the aggregate callback; it is an **outer
cross-check only**. The OR residual and the Decimal multiply + SUM fold have **no timer today**, and every
grouped profiler is grouped-only. The ledger must own **mutually exclusive leaf spans** over the whole
fused loop.

## Decision — perf names the category; a REQUIRED exclusive-span ledger measures the geometry

Diagnosis-only (429/433/436 discipline). No mechanism, no engine behavior change, no speed claim.
**Profile: strict sf=10 under the exact ADR 356 compute envelope.**

**Two disjoint 5-child cohorts, never elapsed-compared.** Cohort A: external perf with the **ledger
disabled**. Cohort B: the **ledger enabled with perf disabled**. perf perturbs timing and the ledger
perturbs timing differently; their elapsed numbers are never placed side by side. perf names the sampled
cycle category (Cohort A); the ledger owns exclusive wall attribution (Cohort B).

**Phase 0 — reconfirm dispatch, then run Cohort A.** Reconfirm the live fused branch, build/probe
orientation, replay ID, fingerprint. Then external perf (`perf_event_paranoid=1`, the 437 host gate)
names the cycle category and its cross-seed stability.

**Phase 1 — REQUIRED off-by-default exclusive-span ledger (Cohort B).** Eight mutually exclusive leaves
rolled into four domains, residual taken **inside one explicit root span**:

| exclusive leaf | → domain |
|---|---|
| probe lookup / selection | **D1** |
| Arrow materialization / interleave (after pages available) | **D1** |
| predicate evaluation / filter | **D2** |
| Decimal expression | **D3** |
| numeric fold | **D3** |
| scan polling / decode | **D4** |
| spill-page read / decode | **D4** |
| build / index / spill encode-write | **D4** |

**D1 owns selection + post-read materialization; D4 owns scan, build/spill, and page read/decode** — the
leaves are disjoint, so spilled-output page reads land in D4, never double-counted in D1's materialization.

**Root = entry-to-return of the Phase-0-confirmed fused path** (not the harness `execute_ms` boundary).
Report three quantities: the **exclusive leaves**, the **in-root residual** (`root − Σ leaves`, predeclared
**≤ 10%**, widen-instrumentation-never-move-the-ceiling), and **`execute_ms − root` as outside-root
overhead** (a separate reported line, not folded into the residual). All additions and subtractions are
checked; overlap, overflow, or `root > execute_ms` invalidates the receipt loudly rather than saturating.

**Exact M → S row-flow reconciliation (engine-generic).**

| invariant | rule |
|---|---|
| predicate input rows | **M** = join matches |
| predicate output rows = SUM-argument rows = numeric-fold rows | **S** |
| S | `== oracle.qualifying_rows` (`Q19Oracle`, `toron-tpch/src/lib.rs:12683`) |
| oracle-internal cross-check (independent) | `sum(branch_hits) == qualifying_rows` (`lib.rs:12764`) |

Engine-side `[b0,b1,b2]` is **deliberately not collected** — it would need q19-specific instrumentation.
The oracle owns the per-branch proof; the engine ledger owns only generic M → S flow.

**Structural residual-program census (Phase 0).** Walk the bound residual DAG: report **physical node
count** and **reference multiplicity** per node, establishing whether the common shipmode/shipinstruct
conjuncts are physically repeated or CSE-shared. Cite the eval loop in
`evaluate_bound_program_without_storage` (`expr.rs:374`). The census is a structural fact emitted once.

## Stability rule (locked before measurement)

- **N = 5** per cohort. Per-child results gate; pooled results are volume summaries only.
- **Same dominant sampled cost (Cohort A) and same dominant ledger domain *and dominant exclusive leaf*
  (Cohort B) in all five.**
- **`(max − min) / median ≤ 0.25`** across five for normalized D1..D4 shares, the dominant leaf's share,
  and the perf category share. Median zero ⇒ all five zero.
- **Report `max`, never gate on it.** Output/digest/schema/rows/spill invariant across all five, matching
  `65c3c2c0b0a99063` / 51,471,552 spill bytes.

## Mechanism discriminator (for ADR 439, not 438) — one mechanism per dominant (domain, leaf)

438 names the dominant **domain** *and* its dominant **exclusive leaf**; the pair selects exactly one
mechanism — no "and/or":

- **D2 predicate evaluation/filter** → **factor the common conjuncts once** in the residual program.
- **D1 Arrow materialization/interleave** → **evaluate the residual selection before full output
  materialization**.
- **D3** → **Decimal-expression** leaf ⇒ vectorized Decimal expression; **numeric-fold** leaf ⇒
  accumulator work.
- **D4** → **scan/decode** subleaf ⇒ scan/decode; **spill-page read** subleaf ⇒ spill placement/read;
  **build/spill encode-write** subleaf ⇒ build/index/spill-write work.

Evidence chain: **perf names the category → the exclusive-span ledger names the dominant (domain, leaf)
within the 10% in-root residual and reconciles M → S against the oracle → ADR 439 receives one mechanism.**

## Acceptance

- **Named dominant (domain, leaf)** for the fused root span, backed by the eight-leaf partition (in-root
  residual ≤ 10%, `execute_ms − root` reported separately), the **M → S reconciliation** with `S ==
  oracle.qualifying_rows`, the **physical-node/reference-multiplicity census**, and the perf category —
  **stable across all 5 children per cohort** by `(max−min)/median ≤ 0.25`, not one seed's `max`.
- **Two disjoint cohorts, elapsed never cross-compared.**
- **Off-by-default with locked output/telemetry parity when disabled**; no sink or held state when the env
  var (`TORON_GLOBAL_FOLD_JOIN_FILTER_PROFILE_DIR`, locked) is absent; artifacts under `target/tpch/adr438/`.
- **No engine behavior change, no mechanism, no speed claim.** Strict sf=10 under the exact 356 envelope;
  the 356 baseline is not rewritten.
- **No query branding in engine guards** — the ledger hooks the fused streaming-collapsing-global-aggregate
  + join0 path generically. `exec.rs` line ratchet held; box new held state if the async body grows.

## Guardrails and risks

- **Exclusive-span discipline is the correctness proof.** Disjoint leaves + a single-root in-root residual
  under 10% prove the leaves partition the loop; overlapping spill-page reads with materialization would
  double-count, which the D1/D4 split forbids.
- **Dispatch reconfirmation is load-bearing** — hooks on a non-live branch measure nothing.
- **Cohort separation is load-bearing** — perf and ledger perturb differently; their elapsed numbers are
  never compared.
- **Scale honesty.** Strict sf=10 matches the completing 356 run exactly.
- **Seed dependence.** The join hasher varies per child; the 5-child gate turns a hot span into a stable
  finding — or refutes it.

## Forward arc

Opens the q19 thread, independent of q13 (433→437) and q17. **ADR 439** builds the one mechanism the
(domain, leaf) discriminator selects. **q17 remeasurement** is a named prerequisite on its own thread —
not folded into 438.

## Deferred (named)

- **q17 remeasurement** before q17 is diagnosed.
- **Lifting `TaskTelemetryPhases` into the harness record** — a candidate standalone `refactor:`, not
  required by 438.
- **The 439 mechanism** — selected by 438's discriminator, not pre-committed.
