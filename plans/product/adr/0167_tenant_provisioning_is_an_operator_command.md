# ADR 0167: tenant provisioning is an operator command

Status: proposed 2026-09-12. Nilket lane. Wendelon ADR 0021 decided that a
tenant is a Raft-committed record and that `TenantAdmin` operations reach
the cluster as control-ring RPCs; the record, the `TENANT_ADMIN_REQ`
opcode, its leader-only handler, the operator and tenant-admin roles, and
the policy cache that projects a committed record into admission all
exist and are tested. What does not exist is any way for an operator to
invoke them: no client sends the opcode, no CLI exposes it, and the only
identity the handler admits outside the local development profile cannot
be issued. A fresh cluster therefore holds tenant 0 alone, and a worker
for any other tenant answers every query with `tenant_unavailable` until
someone hand-builds a frame in a test. ADR 0165's acceptance is blocked on
exactly this.

## Decision

### 1. Non-bootstrap tenants are created through authorized, Raft-committed tenant administration

The existing `TENANT_ADMIN_REQ` handler, with its role policy and its
commit through Raft, is the authority: a tenant other than the bootstrap
tenant exists because an authorized actor submitted `Create` and the
cluster committed it. Configuration files do not create tenants (ADR 0021
§8), and the product does not choose tenancy (ADR 0006). This cut's
operator surface for that authority is `nilket wendelon tenant create`,
with `get` and `list` so an operator can see what exists. `create` takes
`--tenant <id>`; `--quota-bytes` and `--replication-factor` are optional
and default to what the bootstrap tenant receives (unlimited quota, the
cluster's default replication factor), with the effective values shown in
the command's output.

### 2. The operator identity is issued, not borrowed

The command presents an operator certificate: role `operator`, no tenant
claim, issued from the cluster's operator CA by `nilket toron pki
issue-operator`, with the key held owner-only as every other key is. The
development profile's hard-coded operator leaf serves `nilket local` only;
a deployment beyond it has an issued operator identity or has no way to
provision tenants, by design. A tenant-scoped client identity is refused
by the handler and stays refused.

### 3. Provisioning is observed by a running deployment

Creating a tenant changes nothing about a running Toron worker except
that its requests for that tenant stop being refused: the policy cache
projects the committed record within its refresh interval, and no process
restarts. Worker startup behavior (the example configuration's default of
tenant 1, and whether a worker should check its configured tenant against
its storage identity's claim or against the cluster) is not decided here;
it is tracked as its own record, because it changes configuration
compatibility.

### 4. A companion SQL file is a recipe, not a command

Creating a tenant creates no rows. Relation data for a tenant is loaded
with the existing `nilket toron sql --file` against that tenant,
presenting a client identity for it. Nothing new is needed to seed a
showcase; the evidence records the recipe.

## This cut

`nilket wendelon tenant create|get|list` in the Wendelon CLI, over a new
`WendelonClient` tenant-admin call with the same leader redirect the schema
update call has. `nilket toron pki issue-operator`. Nothing in Toron's
startup and nothing in Polariton changes here. ADR 0165's remaining work
(the batch driver executes a source before it applies `connect`, so a
remote relation notebook cannot yet be driven end to end) is ADR 0165's,
completed against a tenant this ADR provisions.

## Acceptance

On the Bigfoot instance as it runs today: Wendelon holding tenant 0 only,
a Toron worker configured for tenant 1 with a storage identity claiming
tenant 1, both left running throughout.

1. An authenticated query from the Windows Polariton over ADR 0165's
   connection answers `tenant_unavailable`.
2. `nilket wendelon tenant create --tenant 1` with an identity from
   `issue-operator` succeeds and `tenant list` shows tenant 1 `Active`.
   The same command with the tenant-1 client identity is refused; `create`
   for a tenant that exists is refused as existing, not applied.
3. `nilket toron sql --file uat/plot_trends_showcase.sql` presenting the
   tenant-1 client identity loads the relation.
4. The query of step 1, repeated from Windows without restarting Toron,
   Wendelon, or Polariton, returns the seeded rows. Rendering that relation
   is not this ADR's claim.

## Limits and follow-up

Suspend, resume, quota update, KEK rotation, and delete keep their handler
semantics and are not given CLI verbs here. Worker startup validation of
the configured tenant is the separate record named in §3. Tenant 0 remains
unissuable as a client or storage token (`validate_tenant_token`), which
is correct for the bootstrap tenant and is not changed. The `actor_claims`
OID is still the pilot placeholder ADR 0021 notes.
