# ADR 362 - Implementation plan: scan-streaming consumer extension (q21's remaining edge)

Pairs with `plans/adr/362_scan_streaming_consumer_extension.md`. A `crates/toron` engine cut: name q21's remaining
materialized scan edge with provenance, then **widen the 361 shape-named fence** (preferred, if same-family/too-tight)
**or** add a **bounded topology-named consumer edge** (if genuinely different) - reusing `bound_scan_request`.
Green-or-next-wall; correctness strict-via-command + dev-via-parity-test; topology-named core.

## Phase 0 - name the edge with provenance (the go/no-go; before any code)
- From q21's **bound plan**, build the path/table/consumer map: which scan (table, ~1 GB at sf=10) materializes into the
  core `scan output accumulator`, and **which consumer** it feeds. Use a temporary trace if the bound plan alone is
  ambiguous; **do not implement off the generic producer name**.
- Compare the edge to 361's handled shapes (`Scan` / `Filter(Scan)` / `Exchange(Scan|Filter(Scan))` -> streaming-join
  consumer):
  - **Same family, fence too tight** -> plan a **single widening** of the shape-named fence (the consumer match).
  - **Genuinely different consumer** -> plan a **bounded new topology-named edge** (`scan-to-existence-build` /
    `scan-to-semi-anti-input` / `scan-to-exchange-input`, per the map).
- Record the decision + the provenance in the results doc.

## D1 - the extension (widen or new edge, per Phase 0)
- Stream the named edge: direct scan via `bound_scan_request -> ScanStream`, consume batch-by-batch, drop - no
  `materialize_scan_stream -> Vec`.
- **Widen path:** loosen the existing fence's consumer match exactly enough to include the named edge; one shape-named
  recognizer, not a duplicate.
- **New-edge path:** a bounded topology-named consumer edge; shape-fenced; no general recognizer.
- Topology-named symbols/comments only; no q21/qNN in `crates/toron/src`.

## Tests / safety net
- **Strict correctness:** q21 `pass@compare` at sf=0.01 (strict oracle) stays green.
- **Dev correctness (parity test, not the command):** a focused test that the streamed edge output == the materialized
  edge output in **dev (Float64)** mode at sf=0.01 (digest/batch equality) - the existing streamed-scan tests
  (streaming.rs / exec.rs) extended to the new/widened edge, both modes.
- **ScanRequest parity:** the widened/new edge uses `bound_scan_request` (no new inline builder); the drift test still
  passes.
- **Bounded:** the streamed edge's peak RSS bounded vs the materialized ~1 GB (synthetic multi-row-group fixture).
- **Shape fence / no over-reach:** non-matched shapes still materialize; if widening, a fixture confirms the widened
  fence doesn't capture unintended consumers.
- **De-branding guard:** no q21/qNN in core executor (the existing architecture/no-qNN-reference guard).
- **Behavior-neutral at sf=1:** affected-query live checks unchanged; non-matched plans unchanged. Do not use
  `matrix` / `correctness-matrix` cache renders as evidence.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01   # strict oracle stays green
# dev parity is a unit/parity test (above), NOT `correctness --mode dev`.
# sf=1 behavior-neutral evidence must be live perf/correctness checks, not matrix/correctness-matrix cache renders.
# sf=10, fixed 4 GB, persisted surface (ADR 360), live: the core scan output accumulator must be GONE for q21:
#   systemd-run --user --property=MemoryMax=4G ... target/release/toron-tpch perf --sf 10 --mode strict \
#     --runs N --warmup K --query q21 --engines toron --data-source parquet --data-dir target/tpch/data \
#     --output-dir target/tpch/perf_adr362
git diff --check
#   q21 remaining edge named (provenance); widened-or-new-edge streamed; accumulator gone at sf=10 (green/named wall);
#   strict pass@compare + dev parity test; no q21 in core; sf=1 unchanged
```

## Results doc
`crates/toron/plans/impl/362_scan_streaming_consumer_extension_results.md`: Phase 0's provenance map (the exact scan
edge - table, ~bytes, consumer - and why 361 missed it); the fork decision (widen vs new edge) + rationale; the
implementation (the widened fence or the topology-named edge); the sf=10 result (core `scan output accumulator` **gone**
for q21 - green or the named next wall via 332, with producer/bytes); strict `pass@compare` + the dev parity-test
result; bounded peak RSS vs the materialized ~1 GB; confirmation sf=1 behavior-neutral from live checks, no q21 in
core.

## Risk / blast radius
- **Per-query momentum** - bias to structural widening; new edge only if genuinely different (Phase 0 justified).
- **Off-the-generic-producer guessing** - Phase 0 provenance map/trace required.
- **Dev-correctness overclaim** - dev parity is a test, not `correctness --mode dev`.
- **Drift** - reuse `bound_scan_request`; drift test covers it.
- **Over-reach when widening** - a fixture confirms the widened fence's bounds.
- **No broader materialize-each change** - the named edge only; deeper terms are 359-redux/ADR 349.

## Seal
`impl: adr 362 scan streaming consumer extension`. Do not commit wip drafts.
