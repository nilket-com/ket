# Workstream F Revocation Behavior Note

Date: 2026-04-23
Author Role: Codex
Workstream: `F4`

## Scope Landed

F4 now uses replicated Control Ring state for actor-certificate revocation.

The live contract is:

- revocation is keyed by `(trust_domain, key_id)`
- revocation commits through Raft as `WendelonRequest::RevokeActorKey`
- the state machine publishes a live `RevocationSnapshot` watch
- transport admission and observability admission consume that watch directly
- operators manage the set through `REVOCATION_ADMIN_REQ/RESP`

This closes the old “best-effort external refresh” gap for correctness. A node no longer needs a local CRL fetch to deny a revoked actor certificate once the revoke has committed.

## Runtime Behavior

What is enforced today:

- QUIC inter-node handshake rejects revoked `data_node` certificates
- TCP+TLS compatibility transport rejects revoked `data_node` certificates
- observability mTLS rejects revoked operator and observer certificates
- verification fails closed with `PermissionDenied` and the message `actor certificate key_id is revoked`

Operator surface:

- `RevocationAdmin::Revoke { trust_domain, key_id }`
- `RevocationAdmin::List`

Authorization:

- same `ClusterControl` boundary as other cluster-scoped control operations
- tenant-scoped principals are rejected

## Deliberate Boundaries

This slice is intentionally narrow:

- revoke-only, no un-revoke mutation yet
- no external CRL file/HTTP distribution surface
- revoked entries carry a commit timestamp and are pruned once they age past the maximum supported actor-cert lifetime when newer revocations land
- no revocation-reason taxonomy beyond `(trust_domain, key_id)`

That is acceptable for the pilot baseline because certificates are already short-lived and the correctness requirement was replicated denial, not full PKI product breadth.

## Evidence

- `tests/tls_identity.rs::revoked_actor_key_ids_are_rejected_by_identity_verifier`
- `tests/control_ring_rpc.rs::revocation_admin_commits_and_lists_replicated_revocations`
- `tests/observability_listeners.rs::observability_rejects_revoked_operator_certificate`
- `src/consensus/raft_storage.rs::tests::revocation_snapshot_watch_updates_on_revoke_actor_key`
- `src/consensus/raft_storage.rs::tests::revoke_actor_key_prunes_expired_entries_on_new_revoke`

## Follow-Ons

- destructive/admin policy layering in `F5`
- broader break-glass issuance policy and runtime scope contract are covered in the F6 guide
- any future un-revoke workflow should be explicit and auditable, not implicit garbage collection
