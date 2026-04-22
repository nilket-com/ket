# Upgrade Strategy

Wendelon supports controlled rolling upgrades inside a bounded compatibility
window. The rules below are the operator contract for Workstream E.5.

## 1. Version Axes

- `FrameHeader.version` is the HRFT envelope version.
- The envelope version must match exactly.
- Cluster rolling upgrades use a separate protocol-compatibility window:
  `{ current, min_compatible }`.
- A peer is compatible only when the two windows overlap.

## 2. Rolling Upgrade Window

- Restart nodes one at a time.
- Keep the live cluster within one protocol step.
- The intended steady-state contract is:
  - old cluster at protocol `N`
  - new binary advertises `current = N + 1`, `min_compatible = N`
  - mixed `N` / `N+1` operation is allowed during the rollout
- A peer outside the overlap window must fail explicitly. It is not allowed to
  join and then fail later through ambiguous request decode errors.

## 3. Join And Reconnect Behavior

- A joining node advertises its protocol window on `JOIN_REQ`.
- The leader rejects unsupported skew with
  `CONTROL_REASON_INCOMPATIBLE_VERSION`.
- The rejection includes the leader’s local protocol window so the operator can
  identify whether the joiner is too old or too new.
- Unsupported envelope versions fail immediately on frame read before payload
  decode.

## 4. Crypto-Profile Rolling Restart

The `default` and `fips` crypto profiles are a supported **rolling-restart
operation**, not a supported mixed-fleet steady state.

The required procedure is:

1. Pick one target profile for the whole cluster: `default` or `fips`.
2. Restart one node at a time onto the target profile. Do not batch-restart
   quorum peers. Prefer follower nodes first and restart the current leader
   last unless an explicit incident forces a different order.
3. For each node: put it into drain, wait for `/ready` to report the expected
   drain state, restart it onto the target profile, then wait for `/ready` to
   return the steady-state readiness signal before moving to the next node.
4. During the roll, scrape `/ready` on every node:
   - FIPS-profile nodes report `fips_mode=true` and `fips_self_test_last_ts`.
   - Default-profile nodes omit the FIPS-only fields.
5. During the same window, scrape `fips_module_version_info{version}` from the
   nodes that have already moved to the FIPS profile.
6. Treat any post-roll non-uniformity as a misconfiguration. The ready
   condition after the maintenance window is:
   - either every node exposes `fips_mode=true`
   - or no node exposes the FIPS-only signals because the whole cluster is back
     on `default`
7. Downstream audit tooling must accept the declared `hash_algorithm_tag`
   during the roll window. The canonical record format is stable; only the
   chain hash primitive changes across the profile boundary.

There is no supported in-place hot toggle. Changing crypto profile means
restarting the node onto a different build.

## 5. Rollback Rules

- Supported rollback window is one protocol step.
- During an `N` to `N+1` rollout, a restarted node may be rolled back from `N+1`
  to `N`.
- Rollback past `min_compatible` is unsupported and must fail explicitly on join
  or reconnect.
- Do not activate new cluster-wide features until every node has converged on
  the newer protocol window.

## 6. Feature Gates

- New architectural features remain behind Raft feature flags.
- Activation happens only after every node in the Control Ring has converged on
  a binary that supports the feature.

## 7. Storage Migrations

- If a catastrophic change is required to the local NVMe slab layout, Wendelon
  uses the lazy-migration path.
- The upgraded node treats its local disk as legacy data and rewrites it through
  the normal background storage machinery rather than requiring an offline export
  step.

## 8. redb And Local Schema Procedure

The redb file is a local acceleration structure, not an independent source of
truth. Upgrades follow these rules:

- WAL + snapshot stay authoritative throughout the rollout.
- redb schema changes must be additive on the first release that introduces
  them.
- A node running release `N+1` must still be able to read the redb file written
  by release `N`.
- A node running release `N` is not required to understand a destructive
  redb-only rewrite from `N+1`; destructive cleanup is deferred until the whole
  cluster has converged and the next release window opens.

The required procedure for a local-schema change is:

1. Add new tables, fields, or metadata keys under a new schema version marker.
2. Keep reads backward-compatible for one rolling-upgrade window.
3. During the mixed-version rollout, rebuild missing derived rows from WAL /
   snapshot / slab authority rather than requiring an offline conversion step.
4. Only after the cluster has fully converged may the new binary treat the new
   shape as the preferred write path.
5. Removal of old tables or old field interpretations is a later release, never
   the same release that introduces the replacement.

If redb fails to open or the schema marker is corrupt, the node must fail over
to rebuild from authoritative state rather than attempting an in-place repair.

## 9. Schema-Change Checklist

Every rolling-upgrade PR that changes local persistent state must include:

- the new schema marker or metadata key
- backward-compatible read logic for the prior on-disk version
- an explicit statement of whether the change is additive or cleanup-only
- recovery tests proving replay-forward works when redb is missing or stale
- an update to `plans/ops/durability.md` if the recovery budget changes

This is the operator contract behind ADR 017's "stable across one rolling
upgrade" acceptance bar.
