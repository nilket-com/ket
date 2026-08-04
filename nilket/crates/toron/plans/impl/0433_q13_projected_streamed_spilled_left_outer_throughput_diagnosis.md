# ADR 433 - Implementation plan: q13 projected streamed-spilled LeftOuter throughput diagnosis

Pairs with `plans/adr/0433_q13_projected_streamed_spilled_left_outer_throughput_diagnosis.md`.
**Diagnosis-only** — this cut changes no engine behavior. It profiles q13's projected streamed-spilled
LeftOuter path under the exact 356 compute envelope, and if external profiling cannot attribute the
77M-voluntary-context-switch signature, lands a parity-locked, off-by-default operation ledger. The
deliverable is a **named dominant producer**, not a mechanism.

## Phase 0 - reconfirm shape and path

1. Re-run q13 persisted strict sf=10 and confirm plan fingerprint `ac3784c060c3c226`.
2. Confirm the path is the **projected** streamed-spilled LeftOuter:
   `visit_streaming_spilled_left_outer_scan_probe_chunks` (`streaming.rs:7368`) →
   `visit_streaming_spilled_left_outer_probe_batch` (`:7472`) →
   `materialize_projected_streamed_spilled_left_outer_matches` (`:2879`) / `_unmatched` (`:2986`).
3. Confirm the `NOT LIKE` residual is physically `Filter(EvaluateInToron) → Exchange` on the orders
   probe input (recognizer `match_filter.is_none()` branch, `streaming.rs:7114`), not a join
   `match_filter`.
4. Record the decoded working-set context: serialized spill `9,152,992` bytes vs cache target
   `8,388,608` bytes (`spill.rs:6`); 8,192-row chunk (`join.rs:722`); no `try_pin_decoded_pages` on
   this path. The decoded working-set size itself is captured in Phase 1/2, not inferred here.

## Phase 1 - external profile under the complete 356 envelope

1. systemd scope: `AllowedCPUs=0-1`, `CPUQuota=150%`, `MemoryMax=infinity`, `MemoryHigh=infinity`,
   `TasksMax=512`, `RuntimeMaxSec=3600` — the complete 356 envelope.
2. **Warm the cache separately, then profile one fresh measured child** (not the eight-child aggregate)
   for per-run attribution.
3. Capture a phase profile (probe / materialize / fold micros already in task telemetry) plus
   **hardware, software, and system counters**: cycles, instructions, IPC (hardware); context
   switches, syscalls, file operations (software/system). `perf stat`/`perf record` on the single
   measured child.
4. Treat all Phase-1 timing as **diagnostic** (profiling overhead); do not compare it to the 356
   baseline or bank it as a speed number.

## Phase 2 - required ledger IF Phase 1 cannot attribute the switches

Only if the external profile does not localize the 77M switches to a phase/operation. An
**off-by-default** counter ledger (env-gated, e.g. `TORON_Q13_SPILLED_LEFT_OUTER_LEDGER_PATH`),
**REQUIRED** in that case, counting **actual operations** (not awaits):

- lookup calls and matched references;
- unique pages requested per chunk;
- **plaintext** cache hits / misses / evictions / resident bytes;
- **decoded** cache hits / misses / evictions / resident bytes (a decoded miss may be a plaintext hit
  with no file read);
- actual file-read calls, bytes, latency, pending completions;
- page decodes;
- materialized batches and rows; grouped-fold rows.

**Parity when disabled:** with the ledger off, output, telemetry, plan fingerprint, and timing behavior
are byte-for-byte unchanged. A parity test asserts this. The ledger is reconciled in the ADR 176
allowlist if it adds a file, and removed or left dormant-and-parity-locked before seal.

## Phase 3 - name the producer, select the next cut

Attribute the 77M switches to a specific operation (decoded-page eviction/re-read, probe/materialize,
decode, predicate, or fold) with counter evidence. State the ADR 434 candidate the evidence selects:

- decoded-page eviction/re-read dominates → page pinning / preload / residency;
- probe/materialize dominates → vectorize the projected probe path;
- predicate dominates → `NOT LIKE` kernel (physically the `Filter(EvaluateInToron)` on probe input);
- fold dominates → double-group fold.

No mechanism lands in 433.

## Tests

- Phase 0 fingerprint/path reconfirmation retained as a named grounding probe if it proves a
  load-bearing plan claim.
- If the ledger lands: a **parity test** (ledger disabled → identical output/telemetry/fingerprint) and
  a counter-sanity test (enabled → counts are internally consistent, e.g. matched refs ≤ lookups).
- Existing streaming, spill, join, architecture, Toron, and TPC-H suites remain green.

## Results document

`plans/impl/0433_q13_projected_streamed_spilled_left_outer_throughput_diagnosis_results.md` records the
reconfirmed fingerprint/path, the decoded working-set measurement, the phase profile + hardware/
software/system counters from the single measured child, the ledger output (if required), the **named
dominant producer** with the 77M-switch attribution, and the selected ADR 434 candidate. Profiling
timing is labeled diagnostic; no speed claim is banked without a later unprofiled 356-shaped run.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Seal

Plan dual: `plan: adr 433 q13 projected streamed spilled left outer throughput diagnosis`.
Implementation seal: `impl: adr 433 q13 projected streamed spilled left outer throughput diagnosis`.
Do not commit `.perf.data`. If the ledger is left in-tree dormant, its parity test rides the seal.
