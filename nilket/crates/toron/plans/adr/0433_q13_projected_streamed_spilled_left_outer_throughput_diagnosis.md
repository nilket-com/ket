# ADR 433 - q13 projected streamed-spilled LeftOuter throughput diagnosis

## Context

The sf=10 board is closed 22/22 and the article evidence base is banked (415 redux survival rail,
Polars capability census, 356 three-way throughput baseline). The **speed arc** opens; the article's
thesis is a narrowing DuckDB gap. The 356 baseline ranks the targets, and **q13 is the flagship** —
3rd slowest at **134.5 s median**, near-worst ratio **97.31x DuckDB**, yet only **9 MB spill**.

**The signature that makes q13 its own problem (356 telemetry).** 100% CPU, **980.81 s user + 138.37 s
system**, **77,272,687 voluntary context switches** — against q17's 57,716 and q19's 376,057, three
orders of magnitude apart — and **zero filesystem-input blocks for the entire eight-child parent
command**, while execution runs the **projected** streamed-spilled LeftOuter path. These are
**aggregate parent telemetry over one warmup + seven measured children**, not a single raw sample; the
comparison to q17/q19 holds because all three share the identical eight-child frame. The shape is
**low-byte / high-operation** — page-cache-hot re-reads, decoded-page churn, or tiny file operations
could dominate at small spill *volume*. High spill *bytes* is not the cost; high spill *operations*
might be — to be counted, not asserted.

**q13 grounded (shape + path).** TPC-H customer-distribution: an outer subquery `LEFT OUTER JOIN
customer ← orders ON c_custkey = o_custkey` counting orders per customer (`c_count`), then a **second**
`GROUP BY c_count` for `custdist` — a double-group fold (`Q13_DEV_SQL`/`STRICT_Q13_SQL`,
`toron-tpch/src/lib.rs:364`; fingerprint `ac3784c060c3c226`). The `o_comment NOT LIKE '%…%'` condition
is **semantically an ON residual but physically not a join `match_filter`**: ADR 417's current plan
pushes it into `Filter(EvaluateInToron) → Exchange` on the **orders probe input**, and the recognizer
takes the `match_filter.is_none()` branch (`streaming.rs:7114`), decomposing the probe input
separately. The engine drives the **projected** streamed-spilled LeftOuter build batch-by-batch:
`visit_streaming_spilled_left_outer_scan_probe_chunks` (`streaming.rs:7368`) →
`visit_streaming_spilled_left_outer_probe_batch` (`:7472`) → materializing via
`materialize_projected_streamed_spilled_left_outer_matches` (`:2879`, from `:7624`) and `_unmatched`
(`:2986`, from `:7559`/`:7584`) — the **projected** helpers in `streaming.rs`, distinct from the
unprojected `join.rs` near-namesakes.

**Cache geometry (byte domains differ — a hypothesis to measure, not assert).** q13's **serialized
spill is 9,152,992 bytes**, which exceeds the **8 MiB (8,388,608) cache target**
(`JOIN_SPILL_CACHE_TARGET_BYTES`, `spill.rs:6`) by **764,384 bytes**. But the cache accounts
**decoded/plaintext batch memory**, not serialized bytes — `insert_decoded_cache_entry` replaces
plaintext accounting with decoded-memory accounting — so **serialized spill exceeds the cache target by
764,384 bytes; the decoded working-set size is unknown and must be measured.** With an 8,192-row
probe/materialization chunk (`CROSS_JOIN_OUTPUT_CHUNK_ROWS`, `join.rs:722`) and **no decoded-page
pinning on q13's path** (`try_pin_decoded_pages`, `spill.rs:1843`, is not called here; the pin at
`streaming.rs:6492` is a different path), random-key probe access could churn eviction → read → decode
at tiny byte volume. This is the leading candidate for the 77M switches — the diagnosis must measure
it.

## Decision — diagnosis-only (the 429 pattern); name the producer, select nothing

ADR 433 changes **no engine behavior** (mirrors ADR 429's q05 residency diagnosis and 415's
measurement discipline). Four ordered phases:

1. **Reconfirm** fingerprint `ac3784c060c3c226` and the exact **projected** streamed-spilled LeftOuter
   path from the persisted sf=10 invocation — the probe traverses `visit_streaming_spilled_left_outer_*`
   → `materialize_projected_streamed_spilled_left_outer_*`, with the `NOT LIKE` residual on the probe
   input (not a `match_filter`).
2. **Capture** a phase profile plus **hardware, software, and system counters** (cycles, instructions,
   IPC are hardware; context switches, syscalls, and file operations are software/system) under the
   **complete** 356 envelope — `AllowedCPUs=0-1`, `CPUQuota=150%`, `MemoryMax=infinity`,
   `MemoryHigh=infinity`, `TasksMax=512`, `RuntimeMaxSec=3600`. **Warm the cache separately, then
   profile one fresh measured child** for per-run attribution — never read per-run behavior from the
   eight-child aggregate. Counter runs are **diagnostic**: profiling adds overhead, so their *timing*
   is not directly comparable to the baseline.
3. **If external profiling cannot attribute the switches, an off-by-default ledger is REQUIRED** (not
   optional). It counts **actual operations**, not "awaits" (an await can resolve without yielding):
   lookup calls and matched references; unique pages requested per chunk; **both plaintext and decoded**
   cache hits, misses, evictions, and resident bytes (a decoded miss can be a plaintext hit with no
   file read); actual file-read calls, bytes, latency, and pending completions; page decodes;
   materialized batches/rows; grouped-fold rows. **Disabled, the ledger must hold locked output and
   telemetry parity** (zero behavior change when off).
4. **Name the dominant producer** with counter evidence, attributing the 77M switches to a specific
   operation. No cut in 433.

## Forks — the next cut, selected by the diagnosis (not pre-committed)

- **Cache-thrash / page-decode amplification** → decoded-page pinning on q13's path (it skips
  `try_pin_decoded_pages`), preload, or batch page reuse — leading candidate *if* the decoded working
  set is measured to exceed the cache target.
- **Join-selection / materialization** → vectorize the batch-by-batch projected probe/materialize path.
- **Predicate cost** → optimize the `NOT LIKE` kernel; note it is physically a `Filter(EvaluateInToron)`
  on the probe input, so if predicate evaluation wins the profile the cut is there, not in a
  `match_filter`.
- **Grouping cost** → optimize the double-group fold.
- **Spill-path necessity** → whether the decoded working set is small enough to keep resident rather
  than spill, avoiding the eviction/read/decode cycle.

The 77M-vs-57K switch gap is the discriminator: decoded-page eviction/re-read → pinning/residency;
probe/materialize, decode, or predicate → the corresponding compute kernel. The diagnosis decides.

## Acceptance

- **Named dominant producer** for q13's 134.5 s / 97.31x, with the **77M voluntary switches attributed**
  to a specific operation by counter evidence — the headline deliverable, exactly as 429 named q05's
  shmem source.
- **No engine behavior change**, no cap/envelope change, `exec.rs` ratchet held; any ledger is
  off-by-default with **locked output/telemetry parity when disabled**, reconciled/removed before seal;
  diagnosis artifacts under `target/tpch/adr433/`.
- **Byte-domain honesty** — serialized spill and decoded working set are distinct; the decoded set is
  measured, never inferred from the 9,152,992 serialized figure.
- **Profiling is diagnostic, not a timing claim** — counter runs carry overhead; the 356 baseline is
  not rewritten, and any resulting speed claim returns to an **unprofiled 356-shaped run**.
- **q17/q19 not folded in** — 433 is q13-only; whether they share a producer is decided by their own
  numbers later.

## Guardrails and risks

- Measurement-only, mirroring 429/415; the named wall becomes the mechanism cut (434+), not a fix
  smuggled into a diagnosis.
- The cache-thrash hypothesis is geometrically motivated but unproven until the decoded working set is
  measured; a competing producer (predicate, fold, materialize) may win — the ledger is built to tell
  them apart.
- An off-by-default ledger must not perturb the very timing it measures; parity-when-disabled is a
  guardrail, and the ledger's own overhead is disclosed when enabled.

## Forward arc

The named producer selects the ADR 434 mechanism cut. q17/q19 are diagnosed on their own numbers, not
assumed to share q13's producer. In parallel (non-ADR): begin the article outline, freezing the
"before optimization" narrative while fresh. DuckDB q11's unstable baseline point is not re-run into the
sealed rail; if the article needs a clean point, a separately labeled noise-floor micro-rail supplies
it — never a rewrite of the banked baseline.
