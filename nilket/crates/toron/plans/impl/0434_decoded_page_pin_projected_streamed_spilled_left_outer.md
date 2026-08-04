# ADR 434 - Implementation plan: decoded-page pin for the projected streamed-spilled LeftOuter grouped shape

Pairs with `plans/adr/0434_decoded_page_pin_projected_streamed_spilled_left_outer.md`. Extend the landed
`try_pin_decoded_pages` substrate to the projected streamed-spilled LeftOuter grouped branch, with a
typed pin receipt and split preload/probe ledger counters, so the probe-phase eviction/reread loop
collapses. Candidate first speed win on q13 (134.5 s / 97.31x baseline); the ledger collapse + an
unprofiled 356 before/after decide the claim.

## Phase 0 - typed pin outcome + receipt

1. Give `try_pin_decoded_pages` a **typed outcome** instead of a bare `bool`:
   `PinOutcome::{Admitted, RejectedPreflight, RejectedMidDecode}` (keep a compatibility accessor for
   existing callers). The preflight branch (`spill.rs:1881`) → `RejectedPreflight`; the mid-decode
   branch (`:1896`) → `RejectedMidDecode`; success → `Admitted`.
2. Emit a **pin receipt** (into the existing throughput-profile sink, gated by the same env var)
   recording: `memory_cap`, `retained_cap`, `resident_baseline`, `resident_index`, `writer_peak`,
   `page_reserve`, `decoded_bytes`, `preflight_estimate`, `pages_attempted`, `pages_completed`, and the
   typed outcome. When the profile is disabled, no receipt is constructed (parity preserved).

## Phase 1 - the shape-fenced pin call

1. At the **parent probe boundary** — after `probe_started`, before
   `visit_streaming_spilled_left_outer_scan_probe_chunks` (`streaming.rs:~7178`) — call
   `try_pin_decoded_pages(self.context.task_budget.memory.get())` on the finished build, once. Mirror
   the landed caller at `streaming.rs:6496`.
2. **Fence** to the projected streamed-spilled LeftOuter **grouped** branch (the recognizer's
   `match_filter.is_none()` grouped shape); no other path gains the call.
3. On `Admitted`, the existing probe/materialize reads pinned pages (already handled by
   `read_batch_page` once `pinned_decoded_pages` is `Some`). On either rejection, `pinned_decoded_pages`
   stays `None` and the probe runs the unchanged cache path.
4. Guard the async-body size: if the added `.await`/held state enlarges the parent async body, box the
   new state (the 433 stack-overflow lesson). Reconcile any new file in the ADR 176 allowlist.

## Phase 2 - split preload vs probe ledger counters

1. In `throughput_profile.rs`, tag cache/read/decode/eviction counters by **phase**
   (`Preload` vs `Probe`) so the ledger distinguishes the pin's one-time preload from probe-time work.
2. This makes the acceptance directly assertable: **probe-phase evictions and rereads = 0** when pinned;
   preload reads/decodes ≈ page count (~5,860). Keep the disabled-path parity (no counters when off).

## Tests

- **Typed reject, both stages:** a preflight-reject fixture (footprint over cap at preflight) and a
  mid-decode-reject fixture (crosses cap mid-decode) each assert `pinned_decoded_pages` is `None` after
  the call **and** subsequent cached execution is value-identical (same output batches) to the
  no-pin path.
- **Admitted collapse:** an admitted fixture asserts probe-phase evictions = 0 and rereads = 0, and
  total reads/decodes ≤ page count.
- **Pin receipt serialization:** enabled → receipt carries all named fields and the typed outcome;
  disabled → no receipt, no state (parity).
- **Fence:** an off-shape plan (non-grouped, or a `match_filter` present) does not call the pin.
- Existing streaming, spill, join, architecture, Toron, and TPC-H suites remain green; the deep-recursion
  lib test passes.

## Phase 3 - live proof and the speed claim

1. **Ledger collapse (diagnostic):** re-run q13 with `TORON_STREAMED_SPILLED_LEFT_OUTER_PROFILE_PATH`
   set; confirm probe-phase evictions/rereads = 0, preload reads ≈ page count, and voluntary context
   switches collapse from ~9.66M toward the preload floor. Retain the pin receipt.
2. **Correctness parity:** q13 fingerprint `ac3784c060c3c226`, digest, schema, 46 rows, 9,152,992 spill
   bytes unchanged.
3. **Speed (unprofiled 356-shaped):** q13 under `AllowedCPUs=0-1`, `CPUQuota=150%`,
   `MemoryHigh/MemoryMax=infinity`, `TasksMax=512`, `RuntimeMaxSec=3600`, 7 measured + 1 warmup,
   per-row fingerprint/digest/schema/rows/spill invariance, stability `spread ≤ 0.20`, one-retry rule;
   report **before 134.5 s → after** with band `max(0.10, 2 × max(spreads))`. No profiled timing banked.
4. **Survival:** q13 `pass@compare` under 4 GiB / no-swap; board stays 22/22.

## Results document

`plans/impl/0434_decoded_page_pin_projected_streamed_spilled_left_outer_results.md` records the pin
receipt (admission inputs + typed outcome), the split-phase ledger collapse, correctness parity, the
unprofiled 356 before/after with the band, the 4 GiB q13 `pass@compare`, and the full gate battery. If
pinning does not fully close q13's time, name the grouped fold as the next wall with evidence.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Seal

Plan dual: `plan: adr 434 decoded page pin projected streamed spilled left outer`. Implementation seal:
`impl: adr 434 decoded page pin projected streamed spilled left outer`. Do not commit `.perf.data`.
