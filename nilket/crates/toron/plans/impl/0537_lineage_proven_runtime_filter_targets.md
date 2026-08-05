# impl 0537 - Lineage-proven runtime-filter targets

Implementation receipt for `plans/adr/0537_lineage_proven_runtime_filter_targets.md`. Two
sealed commits: `8015ec1e` (the mechanism and its controls) and `9945e8f7` (runtime-filter
configuration coherence). No timing authority is created; elapsed stays diagnostic.

## Mechanism (`8015ec1e`)

`RuntimeFilterProbeTarget { probe_descriptor_fingerprint, physical_column_ordinal, key_type }`
rides `TaskOutputBinding::ShuffleProducer` under `#[serde(default)]`, so a task encoded before
the proof existed decodes as `None` and builds no filter — absence of proof fails safe, in the
same shape as ADR 535's publication mode.

`trace_runtime_filter_probe_target` (`distributed/fragment.rs`) establishes the proof at
fragmentation. It follows only ordinal-preserving nodes (`Filter`, `Take`, `Sort`, `Exchange`)
and direct column projections, translating the ordinal through `Project` via a new
`BoundExprProgram::root_column_at` — the existing `root_columns` is all-or-nothing and would
have refused any mixed projection. It stops at literals, computed expressions, aggregates,
unions and joins. At `Scan` it binds the fingerprint, maps the output position through the
scan's own `projection` to the **physical** ordinal, and verifies the key type from that same
scan. Uniqueness is structural, not asserted: the walk is driven by one ordinal down a single
path, so it cannot reach two scans and never picks "the first". Eligibility became
`probe_target.is_some()` — the shape predicate is necessary but no longer sufficient.

`runtime_filter_descriptor_for_probe` consumes the carried target, and
**`first_scan_fingerprint` was deleted outright** rather than left unused. Reintroducing the
`first_scan_fingerprint` plus post-projection-ordinal pairing now requires re-adding a
function, which is visible in a diff.

## Configuration coherence (`9945e8f7`)

`PlannerConfig` gained `runtime_filters_enabled` (default `true`), resolved once at query
entry. The coordinator's `fragment_config`, `run_streaming_read_driver`'s inline config, and
the census preflight all read it from there. Previously each derived the mode independently,
so a caller disabling runtime filters changed only the preflight graph while execution
re-fragmented with the flag defaulted back on. That defect cost two withdrawn conclusions
during grounding and would have made Acceptance 4 untestable.

The ADR 176 binder-root cap moved 6,275 → 6,278: exactly the three lines this field costs
(switch, default, one doc line), with an in-place note that it is a configuration field rather
than a new pass or IR node. Not rounded up.

## Acceptance walk

1. **q11.** `target/tpch/adr537_census/execution_census.jsonl`: `executes_identical`, 7,508
   rows on both arms, digest and schema fingerprint equal, 1.40 s distributed. No
   runtime-filter dependency edge on the scalar stage and no descriptor on its worker scan.
2. **Same-typed literal key is ineligible.**
   `adr537_literal_probe_key_yields_no_target` — q11's exact shape, a literal projected as the
   join key. It passes the type-only predicate and fails the lineage one, so it discriminates
   the decision rather than restating it.
3. **Genuine base-column join stays eligible.**
   `adr537_base_column_probe_key_yields_exact_target` pins fingerprint, ordinal and key type.
   The reconstruction-defeating probe is split across two controls:
   `adr537_reordered_projection_resolves_physical_ordinal` (physical ordinal 2, post-projection
   position 0) and `adr537_probe_target_is_not_the_first_scan_in_the_plan` (two scans of
   distinct identity; the target names the probe side, not the first reachable scan). Both
   fail under the removed reconstruction. The second asserts the two fingerprints differ
   before relying on the difference — an earlier draft shared one descriptor and was vacuous.
4. **Runtime-filter-disabled execution is coherent.**
   `adr537_coordinator_execution_honors_runtime_filter_mode` drives a real coordinator with two
   workers through both modes and asserts a filter is requested iff enabled. Driving the
   coordinator rather than `fragment_plan` is the point: a direct-fragment test passed while
   execution re-fragmented with the flag back on. `adr537_runtime_filter_target_is_carried_not_reconstructed`
   guards the three source properties — `first_scan_fingerprint` absent, the descriptor built
   from the carried target's ordinal *and* fingerprint, and the tracer's fail-closed `_ => None`.
5. **Board.** Sealed census at `9945e8f73e94`, strict sf=8 persisted, unchanged ADR 529
   envelope: **21 `executes_identical`, 0 `executes_divergent`, q15
   `execution_rejected(BroadcastProbeNotStarLinear)` as the sole typed rejection.** Every cell
   that ran both arms has equal digests. No other cell changed classification.
6. **Full battery.** `cargo test --workspace --no-fail-fast` from the sealed tree:
   **7,125 passed, 7 failed.** All seven are inherited and individually attributed — see below.
   This is not a clean battery and is not recorded as one.

## The seven failures, attributed

**Five are the standing Wendelon red**, reproduced in a clean worktree at `09a6f29f` earlier in
this arc with the identical split and the identical names — matched by name, not by count:
`adr071_nilket_root_stays_runtime_free`, `adr072_nilket_root_stays_runtime_free`,
`adr073_nilket_root_stays_runtime_free`, `adr072_descriptor_get_msg_types_are_unique`,
`adr102_descriptor_list_surface_is_latest_only_metadata`. Root: a `tls-pki` runtime dependency
in the nilket root manifest plus two descriptor-surface guards. Unrelated to this ADR and
untouched by it.

**Two are environmental, from an operator deployment holding a fixed port.**
`local::tests::preflight_conflict_precedes_identity_and_leaves_an_absent_profile` binds
`SQL_ENDPOINT` (`127.0.0.1:7742`) to simulate a conflict; a live `nilket local` deployment
already held that UDP port, so the bind returned `AddrInUse`.
`local::tests::successful_preflight_allows_identity_and_pki_generation` then failed on a
`PoisonError` from the `PREFLIGHT_TEST_LOCK` mutex the first panic poisoned — a cascade, not an
independent fault. Verified independent of this cut by running the first test at `8bc5e8d6`
(immediately before the mechanism) with the deployment still up: it fails identically.

Both tests bind a fixed port rather than an ephemeral one, so they fail for anyone testing
while running `nilket local`. That is a test-isolation defect worth its own fix; it is named
here rather than absorbed.

## Forward

The execution census now carries **zero correctness findings**. That was the comparison
precondition ADR 529 established and it has blocked every timing claim since, so the **ADR 529
timing follow-up is reopened** from a 21 / 0 / 1 correctness baseline.

Fragmented-plan fingerprint coverage remains out of scope and unfixed: the distributed plan
fingerprint identifies the bound execution plan, not runtime-filter edges or scheduler
metadata, so two materially different fragmented graphs share a fingerprint. It is recorded as
its own item and needs its own decision.
