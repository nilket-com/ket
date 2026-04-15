# Plan: Control Ring Server Surface

## Summary
This plan defines the server-side Control Ring surface that the current client
and roadmap already assume exists.

The recommended decision is:

- Extend the existing single HRFT listener and `MsgRouter` demux rather than
  inventing a second listener in the first execution wave.
- Add a dedicated server-side Control Ring handler under `src/network/` that
  implements `ControlRingServer`.
- Freeze `STATUS_REQ` / `STATUS_RESP` slice first, then lease acquisition,
  then leadership transfer and epoch advance.
- Treat Control Ring RPCs as cluster-internal mTLS-authenticated operations in
  the first wave.

## Motivation
The code already has most of the scaffolding:

- `src/network/router.rs` already reserves `0x10..=0x1F` for the Control Ring.
- `src/network/traits.rs` already defines the `ControlRing` trait.
- `src/client/topology.rs` already sends `STATUS_REQ` and expects `STATUS_RESP`.
- `src/node.rs` currently does not install a Control Ring handler into the
  router.

The result is a structural hole: the client is speaking to a server surface
that does not exist yet. This plan closes that hole with the least disruptive
shape.

## Goals
- Freeze the ownership and routing shape of the server-side Control Ring.
- Decide the slice order for Control Ring RPC implementation.
- Keep the initial wire format compatible with the existing smart-client status
  code.
- Define the authorization rule for state-changing Control Ring RPCs.

## Non-Goals
- Push topology subscription.
- Full admin/authz model for external operators.
- Separate control-port versus data-port operational policy.
- Transport-provider expansion beyond the current QUIC path.

## Must Settle
1. **Routing strategy**
   Use the existing single HRFT listener and `MsgRouter` control slot in the
   first execution wave. Do not split the listener surface yet. A separate bind
   or port remains an ops concern for later if needed.

2. **Server ownership**
   Introduce a dedicated Control Ring server module in `src/network/` rather
   than overloading `WendelonDataServer` or `WendelonNode` directly with decode
   logic.

3. **Slice order**
   The recommended order is:
   - slice 1: `STATUS_REQ` / `STATUS_RESP`
   - slice 2: `ACQUIRE_LEASE_REQ` / response
   - slice 3: `TRANSFER_LEADERSHIP_REQ`
   - slice 4: `ADVANCE_EPOCH_REQ`

4. **Status wire shape**
   Keep `STATUS_RESP` compatible with the current client by returning
   postcard-encoded `ClusterStatusWire` in the wire path for slice 1. Do not
   block on freezing a POD layout for status metadata.

5. **Authorization model**
   In the first wave, Control Ring RPCs are cluster-internal mTLS operations.
   `STATUS_REQ` may be served to any authenticated cluster peer/client.
   State-changing RPCs are leader-handled and may redirect or reject when
   received by the wrong node.

## Implementation
1. **Install the missing ring**
   Add a real `ControlRingServer` implementation and wire it into
   `MsgRouterBuilder::with_control_ring(...)` from `src/node.rs`.

2. **Freeze slice 1 first**
   Implement `STATUS_REQ` / `STATUS_RESP` with the current smart-client
   projection (`ClusterStatusWire`) so `ControlRingTopologySource` succeeds
   against a live peer immediately.

3. **Add state-changing RPCs in increasing order of dependency**
   Lease acquisition before leadership transfer and epoch advance, because the
   former directly unblocks the existing lease-manager machinery.

4. **Keep Raft as the source of truth**
   State-changing Control Ring RPCs remain wrappers around committed state
   transitions, not side channels around Raft.

## Risks and Mitigations
- **Risk: conflict between current code and older docs that mention a separate control port**
  - **Mitigation:** the first execution wave follows the current code shape: one
    HRFT listener, per-ring demux inside the router. Ops can still choose
    dedicated binds later without changing the server-side handler contract.

- **Risk: waiting too long to freeze the status wire**
  - **Mitigation:** explicitly reuse the client's current `ClusterStatusWire`
    postcard shape in slice 1.

- **Risk: over-scoping authz**
  - **Mitigation:** keep the first wave cluster-internal and mTLS-only. Richer
    operator auth can land later.

## Timeline
- M0: approve this contract and slice order.
- Workstream B.1/B.2: status and lease acquisition.
- Workstream B.3/B.4: leadership, epoch, and membership-driven actions.

## References
- `plans/gap/codex_execution_20260415.md`
- `plans/gap/codex_execution_workstream_B_20260415.md`
- `plans/network/rpc_traits.md`
- `src/network/router.rs`
- `src/network/traits.rs`
- `src/client/topology.rs`
- `src/node.rs`
