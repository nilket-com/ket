# W10 — FIPS Profile + Simulator/Chaos Productionization

**Effort band:** M (4–8 eng-weeks)
**Predecessors:** W1 through W9
**Folds:** `plans/026_rolling_upgrade.md`, `plans/027_backup_dr.md`

W10 is the MVP-cut closeout. It ships the second build profile (FIPS), productionizes W1's simulator seam into a turmoil-style harness with Elle + monotonic checkers, lands the coverage ladder on CI, exercises the chaos scenario matrix, and proves the rolling-upgrade and backup/restore procedures against the harnesses.

## 1. ADRs consumed

- **ADR 026 (FIPS 140-3 Certification Target).** Two build profiles (default, FIPS); compile-out of non-validated primitives; FIPS-only CMVP module boundary; mixed-fleet is diagnostic-only per `plans/ops/observability.md` §7.
- **ADR 028 (beyond foundation).** Simulator network layer (turmoil-style), mocked `ObjectStore`, Elle + monotonic correctness checkers, coverage ladder (10k / 1M / 100M) wired to CI, chaos harness on a kernel-capable runner, seed registry with retention policy.

## 2. Deliverables

1. **FIPS build profile.** A second cargo feature profile (or build target) that compiles out AES-256-GCM-SIV. The audit chain hash primitive is parameterized by build profile (`blake3` default; SHA-256 under FIPS per ADR 025 §3 / ADR 026). The chain head exposes `hash_algorithm_tag` so downstream tooling distinguishes the two.
2. **Profile-change rolling-restart procedure** documented in `plans/ops/upgrades.md` (folded from `026_rolling_upgrade.md`). Per ADR 026 Consequences, mixed-profile membership is not a supported *steady state*, but a rolling restart from `default` to `fips` (or vice versa) is the supported operation; during the roll the cluster is in a documented transient mixed state. `fips_mode` on `/ready` and `fips_module_version_info` make the transient state observable per `plans/ops/observability.md` §7.
3. **Simulator network layer** (turmoil-style) over W2's QUIC transport and W1's `AsyncFileOps` seam; a cluster runs deterministically under the simulator with seed-reproducible scenarios.
4. **Mocked `ObjectStore`** implementation for the simulator, covering the full trait surface including multipart and list operations.
5. **Elle linearizability checker** integrated with the simulator's history output; the monotonic-counter checker runs on every simulator scope.
6. **Coverage ladder** wired to CI: 10k per-PR gate (hard), 1M nightly, 100M weekly. Ladder is the authoritative exit criterion for a workstream's simulator-tier acceptance.
7. **Chaos harness** on a kernel-capable runner with scenarios enumerated in `plans/ops/testing.md`: disk-full, clock-skew, partition, leader-churn, process-kill, packet-drop, packet-reorder.
8. **Seed registry** wired to the retention policy in `plans/ops/testing.md` §7.2 (retire only after: issue closed ≥90d AND ≥1000 consecutive passes AND explicit retirement PR with human review). Retirement is exclusively via PR; no automated retirement.
9. **Seed-retention advisory report** — a scheduled read-only job that lists seeds which currently meet the retention bar, emitted as a comment on a tracking issue (or similar). The report does not delete, close, or modify any seed; it is advisory input for the explicit retirement PR. Explicitly not an automated retirement.
10. **Backup / restore baseline** per `plans/ops/backup_dr.md`: the baseline backup unit is a full node image rooted at one data directory (Control Ring artifacts + local slab files + manifest recording tiered-slab pointers). Restore writes the image into a fresh directory and boots the node. Tiered slab payloads are treated as external durability; the backup manifest records pointers, not remote-tier bodies. Remote-tier payload validation during restore is deferred per `plans/ops/backup_dr.md` §Deferred.
11. **Backup / restore chaos scenario** — a whole-node-loss scenario that restores from the baseline unit and verifies the node boots in recovery mode with Control Ring membership, epoch history, and local reads against backed-up slab data intact. Audit chain verification across the restore boundary is in scope; full-cluster-loss restore choreography is deferred.

## 3. Acceptance criteria (ADR-referential)

- (ADR 026) FIPS-profile binary has zero symbols from the SIV primitive — post-link symbol sweep is a CI check.
- (ADR 026) FIPS-profile audit chain uses SHA-256; chain replay under FIPS-profile matches chain replay under default-profile on the same event stream modulo the declared `hash_algorithm_tag`.
- (ADR 026 Consequences / ops/observability.md §7) A profile change is performed via rolling restart per `plans/ops/upgrades.md`; during the roll, `fips_mode` on `/ready` and `fips_module_version_info` expose the transient mixed state. Post-roll, uniformity of `fips_mode` across the cluster is a ready condition; non-uniformity after the roll window is a monitored misconfiguration.
- (ADR 028 §2–5) Every data-path disk I/O in the simulator goes through `AsyncFileOps` and every L2 op goes through the mocked `ObjectStore`. No code path escapes to real disk or network under simulator mode.
- (ADR 028 §6) Elle produces linearizability verdicts on every scenario in the 1M nightly tier; a linearizability violation blocks the nightly.
- (ADR 028 §7) 10k per-PR gate fails the build; 1M nightly fails the nightly; 100M weekly fails the weekly.
- (ADR 028 §8) Chaos scenarios run on a kernel-capable runner (not containerized) so `io_uring` + real fsync semantics are exercised.
- (ops/testing.md §7.2) Retirement requires an explicit PR; the advisory report lists candidates but does not retire.
- (ops/backup_dr.md) Restoring a node from the baseline unit produces a node that boots in recovery mode, retains Control Ring membership and epoch history, and serves local reads against backed-up slab data. New writes and new Control Ring commits succeed post-restore.

## 4. Explicit non-goals

- Lab engagement for FIPS 140-3 validation. **Commercial procurement, outside workstream scope.**
- Archival-tier reclamation (ADR 024 Track D). **Post-pilot.**
- Client-authored WPBC. **Post-pilot.**
- End-user-facing API documentation. Separate track.
- **Multi-node cluster-replacement choreography and remote-tier payload validation during restore.** Deferred per `plans/ops/backup_dr.md` §Deferred.
- **Automated retirement of seeds.** Retirement is explicit PR only per `plans/ops/testing.md` §7.2.

## 5. Open items

- **ADR 026 Open Question — FIPS lab procurement.** Commercial, tracked outside.
- **ADR 028 Open Question — simulator runner budget.** Validate the 100M weekly tier fits the CI budget; tune the scenario matrix if not.
- **Seed-retention advisory report shape.** Decide whether the advisory lives as a comment on a tracking issue, a scheduled markdown artifact in the repo, or a Grafana-rendered dashboard. Close before W10 acceptance.

## 6. Test harness tier

- **W10 is the harness.** All prior workstreams' simulator-tier acceptance is re-run here against the productionized harness.
- **Chaos:** the full scenario matrix, gated by the coverage ladder.
- **Fuzz:** W8's WPBC fuzz surface runs in the ladder.

## 7. Phasing (internal)

- **W10.a (S):** FIPS build profile + build-profile-parameterized chain hash.
- **W10.b (M):** simulator network layer + mocked `ObjectStore` + checkers.
- **W10.c (S):** coverage ladder + CI wiring + seed registry.
- **W10.d (S):** chaos harness + rolling-upgrade + backup/restore scenarios.
