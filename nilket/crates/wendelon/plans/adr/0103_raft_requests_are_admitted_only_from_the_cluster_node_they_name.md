# ADR 0103 - Raft requests are admitted only from the cluster node they name

## Status

Accepted for implementation, 2026-11-15 (DC-243 step 1; Codex 01a1de3f). Not delivered.

## Context

`WendelonRaftInternalServer::handle_raft_frame` ignored the request context. It decoded Vote, AppendEntries and InstallSnapshot and handed them to OpenRaft, with no check that the sender:
- is an unscoped cluster node, or
- is the node the request names as its candidate or leader.

Ingress only requires `InterNodeTransport`, which every `DataNode` actor holds, including a tenant-scoped storage certificate. An in-process RED batch showed all three request kinds dispatched to OpenRaft for:
- a tenant-scoped storage identity naming member 41;
- member 43 naming member 41.

## Decision

Raft request admission, in both decision modes, before any effect of the request (the AppendEntries HLC harvest, OpenRaft dispatch, replacement-replay confirmation):

1. The actor is a `DataNode` with no tenant claim, and its node id equals the authenticated remote node.
2. The request's claimed sender equals that node: `vote.leader_id.node_id` of a Vote (the candidate), and of an AppendEntries or InstallSnapshot (the leader). Entry log ids and a snapshot's last log id are history, not the sender, and are not compared.

A refused request returns `PermissionDenied`, the existing error path: the stream ends with no response frame and nothing else happens. The frame version check and malformed-frame handling stay as they are, and so do the global `InterNodeTransport` rule and the application storage RPCs. No wire, durable or version change.

## Limits

- This binds a Raft request to an authenticated cluster node. It does NOT check that node's membership: an unscoped node certificate of a non-member id can still send Raft as itself. That stays open under DC-243 (step 2, designed separately).
- It does not make a disk-erased voter safe to restart (member replacement, gate 1).

## Verification

- The admission table: tenant-scoped as another id, tenant-scoped as itself, node N as node M, and node N as itself, for each request kind, through the real router and handler.
- A denied AppendEntries leaves no HLC trace.
- A direct-handler context mismatch is refused.
- Formation and join controls in each mode.
- The repository's one real three-daemon run (`three_daemons_from_configuration_files_form_a_parent_decisions_cluster_and_serve_a_client`).
