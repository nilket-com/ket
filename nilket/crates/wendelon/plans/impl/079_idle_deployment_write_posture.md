# Impl Plan — Wendelon ADR 079: Idle Deployment Write Posture

Pairs with `plans/adr/079_idle_deployment_write_posture.md`. Scope: expose the ADR 054 heartbeat
cadence, give `nilket local` an explicit 30-second teaching-artifact value, stop materializing exact
heartbeat markers as data-access audit events, and make idle write rate a repeatable soak artifact.
**No CDC disable switch, no audit sampling, no audit-file rotation/deletion, no wire/schema change,
and no live-deployment restart during implementation.**

## Phase 0 — Preserve the before evidence

Record the 2026-08-01 dogfood baseline in the implementation evidence:

- `audit.ndjson`: 132,358,933 bytes / 89,554 records;
- `data.write_finalized`: 88,792 records (99.1%);
- snapshots observed around Raft indexes 114,999 and 119,999;
- repeated QUIC `connection lost` warnings were separately fixed by TORON impl 533;
- queries and charts remained usable throughout.

Do not copy or mutate the live file. Tests construct bounded fixtures. The running `nilket local`
process remains untouched until the user chooses the next overnight acceptance.

## Phase 1 — Make cadence explicit, preserving the serious default

1. Add `cdc_locality_heartbeat_interval: Duration` to `NodeBootstrapConfig`; `dev()` and ordinary
   TOML omission resolve to `DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS` (1,000 ms).
2. Add nonzero `[tuning].cdc_locality_heartbeat_interval_millis` parsing and validation. Keep the
   serialized spelling stable and operation-specific in errors.
3. Pass the configured duration to `spawn_cdc_locality_heartbeat_supervisor`; remove the hardcoded
   `Duration::from_millis(DEFAULT_...)` from `node.rs`.
4. Make `render_local_profile_config` write
   `cdc_locality_heartbeat_interval_millis = 30000` in the generated `[tuning]` table, with a comment
   naming the bounded CDC-latency trade-off. Do not change the checked-in ordinary example's default
   behavior and do not put profile magic in `NodeBootstrapConfig::default`/`dev`.

Tests: omitted/default = 1,000 ms; explicit = exact duration; zero refuses; local generated config =
30,000 ms and parses; serious example remains 1,000 ms; node wiring observes the configured value.

## Phase 2 — Give the heartbeat one canonical identity seam

Move or expose heartbeat construction primitives from the data-server tail into the CDC/consensus
domain so the producer and audit classifier share them. The seam should provide the equivalent of:

- canonical heartbeat identity for a locality;
- canonical payload hash for locality + commit HLC + epoch;
- `is_cdc_locality_heartbeat_finalization(&TenantFinalization) -> bool`.

The classifier recomputes the exact identity/hash and also requires tenant zero, charged bytes zero,
and record count zero. It must not depend on log index/term placeholders, request ordering, tracing
messages, or a new wire field. Keep `TenantFinalization` and persisted Raft bytes unchanged.

Positive controls: canonical construction recognizes across multiple localities/HLCs/epochs.
Negative controls: independently perturb tenant, identity/locality, payload hash, epoch, HLC,
charged bytes, and record count; every perturbation refuses recognition. Retain the existing
locality/hash domain-separation tests at their new owner.

## Phase 3 — Correct audit classification at the Raft boundary

In `raft_audit_event_with_context`, return `None` for a successful exact heartbeat finalization
before the ordinary `Finalize` data-write arm. Failed heartbeats already emit no mutation audit under
the existing rule. Every non-heartbeat finalization—including zero-row and tenant-zero near-misses—
continues to emit the canonical `DataWriteFinalized` data-access event.

Tests must prove:

1. an exact committed heartbeat produces no `AuditEvent`;
2. each near-miss produces `DataWriteFinalized` with its existing schema/fields;
3. ordinary successful and failed finalization behavior is unchanged;
4. heartbeat application advances state/CDC frontiers and remains present in Raft—the change is
   materialization classification only;
5. `AuditMaterializationTracker` in-flight/lag counts remain zero for an excluded heartbeat and
   reconcile for an emitted near-miss;
6. `audit.chain_root`, local-security, admin, read, range-query, and real-write events are untouched.

Add an architecture source-audit guard that rejects any second ad-hoc heartbeat predicate at an
audit site. Include a synthetic forbidden predicate as the positive control for the guard.

## Phase 4 — Observe idle progress directly

Add a monotonic, bounded-label (prefer no labels) counter for committed CDC locality heartbeat
finalizations. Increment only after Raft accepts the heartbeat; rejected/failed proposals do not
count. Export it through the existing metrics registry and pin the metric name in `metrics_smoke`.
Do not derive the count from audit output.

Clock-compressed tests run the heartbeat worker with a short/controlled interval and prove:

- no emission while a locality is active;
- one emission per quiet interval, not per table;
- configured cadence changes the bound;
- missed ticks do not produce an unbounded catch-up burst (set and test the ticker policy
  explicitly rather than inheriting Tokio's default by accident).

## Phase 5 — Add a local-idle soak evidence mode

Extend the existing soak family rather than inventing an unrelated report. The implementation may
use a sibling `run_local_idle_soak.sh`/helper, but the artifact envelope and names remain the existing
ones: `soak_config.json`, `samples.jsonl`, `summary.json`, with an explicit `mode = local_idle` and a
format version. Factor shared serialization/parsing code if two producers would otherwise drift.

The local-idle runner owns an isolated `nilket local` deployment (never attaches to or stops an
external one), captures attributed Toron/Wendelon logs, samples the heartbeat metric and audit file
by operation, probes health/queryability, and stops its own supervisor cleanly. Each sample records:

- elapsed time and configured heartbeat interval;
- locality count and committed-heartbeat delta;
- audit byte/record delta plus per-operation counts;
- audit materialization lag and health state;
- WARN count/source since the previous sample.

The summary fails the run when heartbeat rate exceeds cadence/locality tolerance, an exact heartbeat
appears as `data.write_finalized`, materialization lag is nonzero, the deployment is unhealthy/not
queryable, or an idle-path WARN repeats. A synthetic fixture containing both a clean run and each
failure class proves the evaluator can reject evidence; do not validate only the happy path.

Total idle audit growth is independently budgeted; heartbeat-shaped growth is not the only ceiling.
The summary enumerates and enforces these per-operation maxima for the owned run:

- `crypto.startup`: exactly one per Wendelon process start;
- `audit.chain_root`: at most `ceil(duration / 60 seconds) + 1` for the configured 60-second
  rollup, including a scheduling-boundary allowance;
- `observability.ready` and `observability.metrics`: at most one each per evaluator-owned probe,
  with probe counts recorded in `soak_config.json` rather than hidden in a total;
- every other operation, including `data.write_finalized`: zero while the workload is idle. The
  queryability probe uses a storage-free statement so the assay does not manufacture data-access
  evidence merely by observing the deployment.

The total-record budget is the sum of those named maxima and fails even when no record resembles a
heartbeat. A synthetic unexpected-operation fixture must trip this ceiling, alongside the existing
heartbeat-growth and WARN controls. This keeps a future accidental idle writer from hiding behind
the known heartbeat correction.

The fast CI witness uses compressed cadence and a short isolated deployment. The nightly/weekly
workflow relocation lost during the monorepo graft remains a separate CI plan; this pair produces a
command ready for that workflow but does not mix CI archaeology into runtime semantics.

## Architecture guards

Add `adr079_*` guards to the established Wendelon architecture suite:

- local generated config contains the explicit 30,000 ms value and parses;
- `node.rs` does not construct the heartbeat duration from the default constant;
- audit classification calls the canonical predicate and contains no field-only imitation;
- no production path samples/drops generic `DataWriteFinalized` events;
- no local audit rotation, truncation, deletion, or replacement path is introduced;
- soak evidence retains the shared format/version and its rejection controls.

Every guard includes a fixture that makes it fail and cites ADR 079 in its failure message.

## Verification gate

```text
scripts/cargo-cgroup check -p wendelon --all-targets
scripts/cargo-cgroup test  -p wendelon --lib cdc
scripts/cargo-cgroup test  -p wendelon --lib audit
scripts/cargo-cgroup test  -p wendelon --test audit_verify_cli
scripts/cargo-cgroup test  -p wendelon --test backup_restore
scripts/cargo-cgroup test  -p wendelon --test metrics_smoke
scripts/cargo-cgroup test  -p wendelon --test architecture_test adr079
scripts/cargo-cgroup test  -p wendelon --test architecture_test
scripts/cargo-cgroup test  -p wendelon --tests
cargo fmt -p wendelon -- --check
git diff --check
```

Also run the short isolated local-idle soak and attach its three evidence artifacts. The user's next
overnight dogfood run is the product acceptance: morning logs quiet, audit heartbeat growth zero,
configured heartbeat rate bounded, queries still work.

## Stop conditions

Stop and return to design if implementation requires any of:

- changing `TenantFinalization` or persisted/wire bytes;
- weakening the exact heartbeat predicate;
- suppressing any real data-access event;
- rotating/deleting/replacing the authoritative local audit baseline;
- disabling heartbeat progress;
- attaching the soak runner to a deployment it does not own.

The audit-rotation follow-up, if evidence still justifies it after this cut, starts with a new ADR
covering multi-segment chain continuity, verifier input order, restart recovery, signed trust anchors,
retention/WORM policy, deletion authorization, and fail-closed behavior.
