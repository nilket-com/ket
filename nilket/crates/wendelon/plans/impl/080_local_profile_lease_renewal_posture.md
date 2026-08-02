# Impl Plan — Wendelon ADR 080: Local-Profile Lease-Renewal Posture

Pairs with `plans/adr/080_local_profile_lease_renewal_posture.md`. Scope: make the existing per-range
lease TTL explicit, give the generated single-node local profile a longer visible value, remove the
manager's duplicate startup renewal, and extend local-idle soak evidence with a total Raft-append
budget. The implementation preserves fencing through Raft and changes no snapshot threshold,
lease wire layout, audit policy, epoch rule, or serious-deployment default.

## Ground truth before editing

- `node.rs` defines `LEASE_TTL_MS = 3_000` and `LEASE_RENEW_INTERVAL = TTL / 3`.
- `Node::bootstrap` passes the constant independently to `WendelonControlServer::new` and
  `spawn_lease_manager`.
- `refresh_leases_once` commits one `AcquireLease` for every primary range on every tick; a
  same-holder/range renewal overwrites replicated expiry and signature in `raft_storage.rs`.
- `spawn_lease_manager` performs an explicit seed and then constructs `tokio::time::interval`, whose
  first tick is immediately ready, so startup can commit a redundant adjacent renewal.
- Existing metrics already expose `wendelon_data_ring_lease_grants_total` and
  `wendelon_cdc_locality_heartbeats_committed_total`; no total applied-entry metric is exported.
- ADR 079 local-idle evidence format is version 1 and currently budgets heartbeats plus audit, not
  total Raft growth.

The 2026-08-02 live profile remains running throughout implementation. Do not attach to, restart,
stop, reset, or mutate `/home/me/.local/share/nilket/local`. Product acceptance happens only after
the owner installs and intentionally restarts the finished binary.

## Phase 1 — Add one validated TTL authority

1. Add `data_ring_lease_ttl: Duration` to `NodeBootstrapConfig`; ordinary/default/dev construction
   remains 3,000 ms.
2. Parse optional `[tuning].data_ring_lease_ttl_millis`. Omission means 3,000. Reject zero, values
   above the `u32` wire carrier, and values whose integer `TTL / 3` renewal interval is zero.
3. Centralize the conversion and derived cadence in one validated helper/value. Do not introduce an
   independently configurable renewal interval.
4. Pass the configured TTL to both `WendelonControlServer::new` and `spawn_lease_manager`. Remove
   direct production-path use of `LEASE_TTL_MS`/`LEASE_RENEW_INTERVAL`; a named ordinary-default
   constant may remain as the config default and test reference.
5. Extend the generated `nilket local` Wendelon config with the teaching-artifact line:

   ```toml
   # Local single-node convenience trades distinct-successor failover latency for quiet idle operation.
   data_ring_lease_ttl_millis = 90000
   ```

   The renderer must not rewrite retained profiles. Document the one-time manual line needed for the
   existing dogfood profile.

Tests cover omission, explicit 90,000, zero, sub-three-millisecond, overflow, TOML round-trip, and
the rendered local profile. A focused integration witness injects a nondefault TTL and observes the
same value at both the local and remote grant seams; source matching alone is not the authority.

## Phase 2 — Make lease scheduling newest-deadline honest

Refactor `spawn_lease_manager` without changing its ownership or fencing semantics:

1. Seed exactly once immediately so the data gate is warm during bootstrap.
2. Schedule the first ordinary renewal at `now + TTL / 3`, not Tokio interval's immediately-ready
   first tick.
3. Keep delayed missed-tick behavior; a stalled runtime must not burst every missed renewal.
4. An epoch-watch change refreshes promptly and resets the next renewal deadline. If a timer and
   epoch change become ready together, coalesce them into one refresh; do not commit adjacent
   same-deadline renewals.
5. Preserve FIFO/fair treatment of all primary ranges within a refresh pass and the existing
   success/denial/error metrics.

Use paused Tokio time and an injected refresh witness/work log. Required deterministic cases:

- startup records one refresh before the first derived deadline;
- the first deadline records the second refresh, not a duplicate at time zero;
- an epoch change refreshes immediately and postpones the stale timer deadline;
- a long executor pause causes one delayed refresh rather than a burst;
- shutdown/watch closure exits without another refresh.

## Phase 3 — Expose total applied Raft growth

Add a no-label monotonic metric for total Raft entries applied by the state machine during the
process lifetime. Increment it at the common apply boundary exactly once per applied entry,
regardless of request variant; snapshot installation/recovery must not manufacture increments for
historical entries. Export it with the existing metrics registry and pin the metric name in
`metrics_smoke`.

The metric is the substrate numerator. Do not derive it from audit records, snapshots, WAL byte
size, or the sum of currently known request counters. Tests apply different request variants and
prove the total advances while only the matching specialized counters advance.

## Phase 4 — Upgrade local-idle soak evidence

Bump the shared soak evidence format deliberately; update commodity and local-idle producers,
fixtures, parsers, and format guards together rather than silently reading the new fields as the old
version. Extend the local-idle structures and JSONL samples with:

- `lease_ttl_millis` and derived `lease_renew_interval_millis`;
- `primary_range_count`;
- `committed_lease_grant_delta`;
- `raft_entries_applied_delta`; and
- the computed named and residual Raft deltas in `summary.json`.

The runner baselines all monotonic metrics only after `LOCAL_READY`, then samples:

- total applied Raft entries;
- lease grants/renewals;
- CDC locality heartbeats;
- the existing audit, health, queryability, and WARN evidence.

The evaluator enforces both rate and decomposition:

```text
lease_max = primary_range_count * ceil(duration / derived_renew_interval) + boundary_allowance
heartbeat_max = locality_count * ceil(duration / heartbeat_interval) + boundary_allowance
known_raft = committed_lease_grants + committed_heartbeats + evaluator_owned_raft_entries
residual_raft = total_raft_entries_applied - known_raft
```

`residual_raft` must be zero after the documented bounded scrape/apply race allowance; the allowance
is enumerated by in-flight named writer, never a percentage and never large enough to hide a
periodic writer. The storage-free `SELECT 1` probe must remain nonmutating. `audit.chain_root` stays
in the independent audit budget and is never counted as a Raft explanation.

Synthetic evaluator fixtures must independently trip:

1. excessive lease-renewal cadence;
2. excessive heartbeat cadence (ADR 079 control retained);
3. a total applied-entry excess with otherwise-clean audit;
4. an unknown residual Raft writer;
5. audit operation/total budget excess; and
6. repeated WARN/unhealthy/queryability failures.

Each negative fixture has a clean positive companion using the same format.

## Phase 5 — Architecture guards

Add `adr080_*` guards to `crates/wendelon/tests/architecture_test.rs`:

- generated local config spells `data_ring_lease_ttl_millis = 90000`;
- both runtime grant paths consume the validated config authority and do not pass the ordinary
  constant directly;
- no independent lease-renewal config key exists;
- the lease manager has no seed-plus-immediate-first-tick construction;
- the total-applied metric is incremented only at the common state-machine apply boundary;
- local-idle evidence contains and evaluates the total/lease/residual Raft fields; and
- snapshot thresholds, lease wire structs, and audit classifiers were not changed in this cut.

Every guard includes a synthetic positive control and cites ADR 080 in its failure text.

## Verification gate

```text
scripts/cargo-cgroup test -p wendelon --lib
scripts/cargo-cgroup test -p wendelon --test config_loader
scripts/cargo-cgroup test -p wendelon --test metrics_smoke
scripts/cargo-cgroup test -p wendelon --test architecture_test adr080
scripts/cargo-cgroup test -p wendelon --test architecture_test
scripts/cargo-cgroup test -p wendelon --tests
cargo fmt -p wendelon -- --check
git diff --check
```

Run the clock-compressed local-idle evidence command in an isolated temporary profile. It must never
attach to the owner's deployment. After implementation review, product acceptance is owner-driven:

1. install the new `nilket`/Wendelon binaries;
2. add `data_ring_lease_ttl_millis = 90000` to the retained profile's `[tuning]` table;
3. stop and restart cleanly;
4. verify tables and Rune table/plot fan-out still work;
5. run overnight and confirm approximately `0.1` named idle Raft entries/second, zero residual,
   no heartbeat audit records, bounded audit roots, and no repeated idle WARN.

## Explicit non-goals

- lease-on-demand or activity-aware expiry;
- a second local node or successor identity;
- changing the serious/default 3-second fencing window;
- suppressing or batching authoritative lease renewals outside Raft;
- snapshot-threshold tuning;
- control-WAL or audit rotation;
- changing lease wire layout/signatures, epoch invalidation, or data-gate admission semantics; and
- restarting or modifying the live dogfood deployment during implementation.

## Review stop conditions

Stop and return to the ADR if implementation requires any of the following:

- the local and remote grant paths cannot consume one TTL authority;
- 90 seconds can delay a different successor in the stated single-node profile despite its
  ownership/identity constraints;
- total applied entries cannot be counted at one common boundary without double-counting replay;
- the first post-idle write requires new retry semantics (that belongs to lease-on-demand); or
- a proposed test passes only by increasing snapshot thresholds or residual allowances.
