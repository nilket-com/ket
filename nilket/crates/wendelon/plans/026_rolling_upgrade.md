# Plan 026: Rolling Upgrade Protocol

## Objective

Define a controlled upgrade window so nodes can be restarted one at a time
without ambiguous wire failures or accidental cluster splits.

## Scope

- define the supported skew rules
- define explicit incompatible-peer failure behavior
- define rollback behavior for the supported window
- implement negotiation on join and explicit wire-envelope validation

## Non-Goals

- backup or restore semantics
- multi-tenancy
- RDMA transport work
- feature-flag activation beyond documenting the operator rule

## Version Model

Wendelon now distinguishes two version axes:

1. `FrameHeader.version`
   - fixed HRFT envelope version
   - must match exactly
   - protects framing and message parsing

2. Cluster protocol compatibility window
   - carried as `{ current, min_compatible }`
   - each node supports the contiguous interval
     `[min_compatible, current]`
   - two peers are compatible iff their supported intervals overlap
   - the negotiated protocol is the highest value in that overlap

## Version Skew Rules

- A node may communicate only with peers whose cluster-protocol windows overlap.
- The supported operational window is one protocol step backward:
  - a fresh build advertises `current = N`
  - it must keep `min_compatible = N - 1` while rolling upgrade support is promised
- Cluster operators must keep the live cluster within one protocol step.
- `FrameHeader.version` is not part of rolling skew; it is the hard gate for the
  stable HRFT envelope.

## Join Behavior

- `JOIN_REQ` carries the joiner’s protocol compatibility window.
- The leader validates overlap against its own local window.
- On success, `JOIN_RESP` includes the negotiated protocol version.
- On incompatibility, `JOIN_RESP` returns an explicit
  `CONTROL_REASON_INCOMPATIBLE_VERSION` plus the leader’s local window.
- Followers forward the request to the leader as they do today; the leader remains
  the source of truth for the compatibility decision.

## Connection / Read Safety

- Every inbound HRFT frame validates `FrameHeader.version` before payload decode.
- Unsupported envelope versions fail immediately with an explicit error rather than
  a downstream postcard or bytemuck parse failure.
- Rolling-upgrade compatibility does not change Data Ring or Control Ring request
  semantics in this slice; it only gates admission and decoding.

## Thrash / Ambiguity Prevention

- Version mismatch must never surface as a generic `InvalidData` from a random
  payload decode when the envelope or join window already proves incompatibility.
- Join rejection uses a dedicated reason code so the operator can distinguish:
  - not leader
  - invalid config
  - incompatible version
  - internal failure

## Rollback Rules

- Supported rollback window is one protocol step.
- If the cluster is mixed across `N` and `N+1`, operators may roll a node back from
  `N+1` to `N` because both sides still overlap on `N`.
- Rollback beyond the advertised `min_compatible` boundary is unsupported and must
  fail explicitly at join / reconnect time.
- New feature activation still waits until every node has converged on the newer
  protocol window; rollback after feature activation remains out of scope here.

## Implementation Notes

- add protocol compatibility structs and constants under `src/schema/core.rs`
- add validation / negotiation helpers under `src/network/wire.rs`
- thread the local compatibility window through bootstrap in `src/node.rs`
- extend `JOIN_REQ` / `JOIN_RESP` to carry compatibility details
- hard-fail unsupported envelope versions during frame header validation

## Tests

- compatible windows negotiate and the joining node can join and communicate
- incompatible windows fail fast with `CONTROL_REASON_INCOMPATIBLE_VERSION`
- unsupported frame-envelope version fails before payload decode
