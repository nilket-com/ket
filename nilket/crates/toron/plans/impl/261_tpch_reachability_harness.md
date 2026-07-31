# Implementation Plan — ADR 261: TPC-H Reachability Harness

## Summary

A new workspace bin crate **`crates/toron-tpch`** (picked up by the `crates/*` glob) that depends on
`toron`, generates the Q6-needed `LINEITEM` columns via `tpchgen` row structs, builds workspace-Arrow
`RecordBatch`es locally (`TPCHDecimal→Float64`, `TPCHDate→Date32`), runs a Toron-authored Q6 proxy
single-node through `plan → bind → execute`, and emits stable phase/status telemetry (human + JSONL)
under `target/tpch/`. No `toron`-library change; `toron` must not depend on `toron-tpch`.

## Verify-first probe (do this before building the CLI)

Confirm the generation + load + execute spine works end-to-end on a hand-built batch *before* wiring
the CLI:

1. `tpchgen::generators::LineItemGenerator` emits `LINEITEM` rows at `sf=0.01`; build a workspace-Arrow
   `RecordBatch` with only the Q6 columns (`l_quantity`, `l_extendedprice`, `l_discount`,
   `l_shipdate`), converting `TPCHDecimal → Float64` (apply scale `/100.0`) and `TPCHDate → Date32`.
2. Register a `lineitem` descriptor in a catalog and serve those batches through a storage scan, then
   `SqlPlanner::plan(Q6 proxy) → bind(&catalog) → Executor::execute_bound` and read one `revenue` row.

This pins the **load seam** (Arrow batch → Toron-executable scan) — the only non-trivial integration —
and proves dev Q6 reaches `execute` before any telemetry/CLI scaffolding is written.

Do **not** start from `tpchgen-arrow` in this cut: `tpchgen-arrow 2.0.2` depends on Arrow 57.1 while
Toron is on Arrow 58.3.0, so its `RecordBatch` is not Toron's `RecordBatch`. Revisit `tpchgen-arrow`
only after version alignment or an explicit conversion layer.

## Phase 1 — the crate skeleton (`crates/toron-tpch`)

- `Cargo.toml`: `name = "toron-tpch"`, a `[[bin]]`, `toron` (path dep), `tpchgen`, workspace `arrow`,
  `serde` + `serde_json` (JSONL). **Manual CLI** (no clap — Toron's no-extra-framework style).
- `main.rs` dispatches `status` / `prepare` / `run` by hand-parsed args; unknown args → nonzero exit
  with a usage line (a *harness* error, distinct from a query `blocked`).

## Phase 2 — telemetry contract (the honesty core)

- `enum Phase { Generate, Load, Plan, Bind, Execute, Compare }` (+ a reserved `Fragment` variant, unused
  this cut) and `enum Status { Pass, Blocked, Fail, NotAttempted }`, each with a stable string repr.
- `struct Attempt { suite, query, scale_factor, mode, phase, status, reason: Option<String>,
  elapsed_ms, digest: Option<String> }` → one human line + one JSONL line (`serde_json`).
- **Recognized-blocker allowlist**: `const RECOGNIZED_BLOCKERS: &[&str] = ["strict_inputs_required",
  "unsupported_decimal_ddl", "unsupported_decimal_arrow", "unsupported_date_interval",
  "unsupported_query_surface"]`. The classifier maps a stage `Err` to a `reason`; if the reason is
  **not** in the allowlist (or the stage panicked / produced a malformed row) the attempt is
  **`Fail`**, regardless of `--allow-blocked`. This is the load-bearing rule — keep the mapping in one
  function so the guard can scan it.

## Phase 3 — dev data generation + load seam

- `gen.rs`: drive `tpchgen::generators::LineItemGenerator` for `LINEITEM` at `--sf`; build the four Q6
  columns directly into workspace-Arrow arrays; convert the `TPCHDecimal` money/qty/discount fields to
  `Float64` (apply scale); convert `TPCHDate` with `to_unix_epoch()` to `Date32`; **never** touch the
  string fields. Cache the projected Arrow IPC under `target/tpch/sf_<sf>/`.
- `load.rs`: register a `lineitem` descriptor (Float64/Date32 dev schema) in a catalog and serve the
  cached batches through `toron::storage::InMemoryStorageScan` or an equivalent harness-local
  `StorageScan`. **Reuse the existing single-node rail** (the `bind(&catalog, …)` +
  `Executor::execute_bound` path the test suite uses). Name the load seam clearly; it is the one place
  the harness reaches into Toron internals.

## Phase 4 — the run + status commands

- `query.rs`: the hardcoded Toron-authored Q6 proxy string (ADR's dev SQL — `Float64`, literal upper
  date bound, no `INTERVAL`).
- `run.rs`: ensure the dev cache exists first (equivalent to `prepare` on demand), then execute the
  proxy stage-by-stage, timing each; on success emit `pass phase=execute` with a result digest (reuse
  `canonical_arrow_ipc_digest`); on a recognized stage error emit `blocked phase=<stage>
  reason=<allowlisted>`; on an unrecognized error emit `fail`. `--allow-blocked` controls only whether
  a recognized `blocked` exits 0.
- strict mode with no `--schema/--query-dir/--answers-dir` → `not_attempted reason=strict_inputs_required`
  (not a fabricated blocker); with inputs → record the first real phase reached (do **not** predeclare).
- `status.rs`: print a static support inventory for `q01`–`q22` with explicit dev/strict state (mostly
  `not_attempted`; `q06` dev implemented/proxy, strict input-gated). Do not add "last run" persistence
  semantics in ADR 261.

## Phase 5 — tests, guards, evidence, verification, seal

### Behavioral tests (`crates/toron-tpch/tests/`)

- `dev_q6_reaches_execute` — from an empty cache, run dev Q6 at tiny `sf` → assert `status=pass
  phase=execute`, one row, a digest.
- `status_lists_all_22` — `status` output names `q01`…`q22`.
- `unrecognized_error_is_fail_not_blocked` — inject a synthetic non-allowlisted stage error → assert
  `status=fail` even with `--allow-blocked` (locks the honesty invariant).
- `strict_without_inputs_is_not_attempted` — strict Q6, no inputs → `not_attempted
  reason=strict_inputs_required`, exit 0.

### Architecture guards (`crates/toron-tpch/tests/architecture.rs`)

- `adr261_tpch_helper_is_not_a_toron_library_dependency` — read
  `Path::new(env!("CARGO_MANIFEST_DIR")).join("../toron/Cargo.toml")`; assert no `toron-tpch`
  dependency edge.
- `adr261_no_official_tpc_tooling_in_tree` — no `dbgen`/`qgen`/official-TPC source vendored; no
  auto-fetch (`reqwest`/download) in the gen path.
- `adr261_no_second_arrow_recordbatch_boundary` — no `tpchgen-arrow` dependency in the first-cut helper;
  dev data is built into workspace-Arrow arrays from `tpchgen` rows.
- `adr261_blocked_requires_recognized_reason` — the classifier source contains the allowlist and routes
  non-allowlisted errors to `Fail`.
- `adr261_stable_phase_status_telemetry` — the `Phase`/`Status` string reprs are present; `Fragment` is
  reserved/unused.
- `adr261_cache_root_is_target_not_tracked` — the default cache root constant is under `target/tpch`;
  tests compute paths from `CARGO_MANIFEST_DIR`, not brittle process-relative `../` paths.

### Evidence

Create `plans/impl/261_tpch_reachability_harness_results.md` with the first real matrix: dev Q6's row
(expected `pass phase=execute`) and strict Q6's row (`not_attempted` until inputs, then whatever the
gauge reads), plus a plain statement that **dev-mode output is not a TPC-H result**.

### Verification gate (cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test  -p toron-tpch
scripts/cargo-cgroup run   -p toron-tpch -- status
scripts/cargo-cgroup run   -p toron-tpch -- run --query q06 --mode dev --sf 0.01 --allow-blocked
scripts/cargo-cgroup check -p toron --lib            # toron unchanged (no edge to the helper)
cargo fmt -p toron-tpch -- --check
git diff --check
```

### Seal

`feat: adr 261 tpch reachability harness`,
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. Generated data stays under
`target/tpch/` (gitignored if needed) — never staged.

## Risk notes

- **The load seam is the only real integration** — turning `tpchgen` rows into workspace-Arrow batches
  and then a Toron-executable scan. Probe it first (Verify-first); everything else is CLI + telemetry
  plumbing.
- **Generator/schema drift** — pin `tpchgen`; the helper reads only the Q6 `LineItem` row fields and
  constructs workspace-Arrow arrays itself. Do not introduce `tpchgen-arrow` until its Arrow version is
  aligned with Toron's or there is an explicit conversion layer.
- **Don't let `blocked` swallow bugs** — the single-function classifier + the
  `unrecognized_error_is_fail_not_blocked` test are the guardrail; keep them tight.
- **No `toron` library change** — if the load seam tempts a `pub` addition to `toron`, prefer a
  harness-local scan impl; only widen `toron`'s surface if unavoidable, and note it (it would touch the
  "frontend/library-only" framing).
