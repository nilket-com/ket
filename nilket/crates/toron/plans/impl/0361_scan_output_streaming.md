# ADR 361 - Implementation plan: scan-output streaming (q14/q21)

Pairs with `plans/adr/0361_scan_output_streaming.md`. A `crates/toron` engine cut in ordered parts: **(Gate 1) factor a
shared `ScanRequest` builder** (both inline builders converge, drift test), **(Gate 2) the q14/q21 bound-plan edge
map**, then **stream the matched q14/q21 scan edge into its consumer** (direct scan, no `materialize_scan_stream`).
Shape-fenced; green-or-next-wall; digest-identical correctness.

## Gate 1 (lands first) - shared `ScanRequest` builder
- Extract one builder (e.g. `fn build_scan_request(&self, scan_fields..., morsel) -> ScanRequest`) carrying **every**
  field: projection, pbc_predicate, runtime_filter, row_identity, profile_hint, morsel, row_limit, trace_context,
  downstream_cancellation, session/query/tenant/read_bound/table ids/fingerprint.
- Make **both** existing inline builders use it: the `ExecutionPlan::Scan` arm (exec.rs:743-757) and the
  streaming.rs:1897 existence-side direct scan.
- Inventory any other `ScanRequest` construction sites (including the test-only/logical scan path). If a site is the
  same semantic scan surface, route it through the builder; if it is intentionally different, record that in the
  results doc so it is not mistaken for builder drift.
- **Drift test:** a test that constructs a `ScanRequest` via the builder from a fully-populated input and asserts every
  field is carried (so a future field added to `ScanRequest` but not the builder fails the test). Behavior-neutral: the
  existing scan/existence paths produce the identical request (a before/after equality check on a fixture).

## Gate 2 (Phase 0) - the q14/q21 edge map
- Read q14/q21's **bound plans**; record the scan->consumer edge(s): which streaming operator consumes the scan, and
  whether the edge is a **bare `Scan`** or a **`Filter(Scan)`**.
- Decide residual handling: bare `Scan` -> no residual; `Filter(Scan)` -> stream the scan + apply the residual
  on-the-fly (340's chunk visitor). Implement the residual path **only if** the map shows it.

## D1 - stream the matched edge
- For the q14/q21 matched scan->streaming-consumer edge: the consumer scans directly (the shared builder ->
  `storage.scan(req)` -> `ScanStream`) and consumes batch-by-batch (build / fold / probe), dropping each batch -
  **no `materialize_scan_stream -> Vec`**.
- Shape-fenced: a precise match on the q14/q21 edge shape; everything else stays on `materialize_scan_stream` (the
  broader materialize-each model untouched).
- Core-code de-branding: implementation symbols/comments are topology-named (no q14/q21/qNN identifiers or comments in
  `crates/toron/src`); the query-number mapping belongs in the results doc / TPC-H harness.

## Tests / safety net
- **Gate 1 drift test** (above) - every `ScanRequest` field carried; both paths use the builder.
- **Correctness parity (load-bearing):** q14 + q21 streamed-scan output is **digest-identical** to the materialize path
  at sf=0.01 and sf=1, **both dev and strict** (a direct test + the oracle).
- **ScanRequest parity in the streamed path:** a test that the direct-scan edge builds the same request (projection/
  PBC/runtime-filter/...) as the materialize path for the q14/q21 shape.
- **Bounded:** a synthetic multi-row-group scan into the streaming consumer keeps peak RSS bounded (no full scan Vec).
- **Shape fence:** a non-matched scan->consumer shape still materializes (the recognizer doesn't over-reach).
- **De-branding:** architecture/no-qNN guard or equivalent check confirms the core executor code uses topology names,
  not TPC-H query numbers.
- **Behavior-neutral at sf=1:** live `matrix` + `correctness-matrix`-class checks for the affected queries unchanged;
  non-matched plans unchanged.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q14 --sf 0.01   # digest-identical (both modes)
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01
# sf=10, fixed 4 GB, persisted surface (ADR 360), live: the scan-accumulator producer must be GONE for q14/q21
#   systemd-run --user --property=MemoryMax=4G ... target/release/toron-tpch perf --sf 10 --mode strict \
#     --runs N --warmup K --query q14,q21 --engines toron --data-source parquet --output-dir target/tpch/perf_adr361
git diff --check
#   shared ScanRequest builder (drift test); q14/q21 edge map; scan streamed; accumulator producer gone at sf=10
#   (green or named next wall); digest-identical at small scale; sf=1 unchanged
```

## Results doc
`crates/toron/plans/impl/0361_scan_output_streaming_results.md`: Gate 1 (the shared `ScanRequest` builder + the drift
test + before/after request equality); Gate 2 (the q14/q21 edge map - which edge, bare Scan vs Filter(Scan)); the
streamed-scan implementation; the sf=10 result (the "persisted parquet scan output accumulator" producer **gone** for
q14/q21 - green or the named next wall via 332, with producer/bytes); the digest-identical correctness evidence
(sf=0.01/sf=1, both modes); peak RSS bounded vs materialize; confirmation sf=1 behavior-neutral and the shape fence
holds.

## Risk / blast radius
- **Silent answer change** - the shared builder + drift test + digest-identical gate close the dropped-field trap.
- **Over-reach** - shape-fenced to q14/q21; non-matched shapes still materialize.
- **Speculative filter** - Filter(Scan) only if the edge map shows it.
- **Green overpromise** - green-or-next-wall; the accumulator-gone is the bar.
- **No broader materialize-each change** - the leaf scan edge only; the deeper terms are later (359-redux/ADR 349).

## Seal
`impl: adr 361 scan output streaming`. Do not commit wip drafts.
