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

## 4. Rollback Rules

- Supported rollback window is one protocol step.
- During an `N` to `N+1` rollout, a restarted node may be rolled back from `N+1`
  to `N`.
- Rollback past `min_compatible` is unsupported and must fail explicitly on join
  or reconnect.
- Do not activate new cluster-wide features until every node has converged on
  the newer protocol window.

## 5. Feature Gates

- New architectural features remain behind Raft feature flags.
- Activation happens only after every node in the Control Ring has converged on
  a binary that supports the feature.

## 6. Storage Migrations

- If a catastrophic change is required to the local NVMe slab layout, Wendelon
  uses the lazy-migration path.
- The upgraded node treats its local disk as legacy data and rewrites it through
  the normal background storage machinery rather than requiring an offline export
  step.
