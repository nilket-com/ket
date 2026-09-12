# ADR 0165: native Polariton authenticates to a Linux Toron

Status: proposed 2026-09-12. Application lane. ADR 0163 made Polariton a
native desktop application and scoped the Nilket deployment to Linux. The
Windows head reaches no Toron today: its only connection path is
`ToronClient::loopback`, which refuses any non-loopback address by design,
and the feature that carries Toron at all also carries Linux-only process
supervision. This ADR defines the boundary between the two.

## Decision

### 1. Portable remote access is separate from local supervision

Native Polariton authenticates to a Linux Toron service without depending
on Linux process supervision or development-PKI provisioning. The client
transport, the SQL adapter, and the connection form are portable; the
local workspace supervisor (`nilket-local`, signals, process groups) and
the development PKI writer are Linux and stay behind their own gate. A
Windows build that cannot supervise a workspace can still connect to one.

### 2. The connection is authenticated mutual TLS, never a loopback shortcut

A remote connection uses Toron's authenticated QUIC transport: the client
verifies the listener's certificate against the deployment's CA under the
configured server name, and the listener verifies the client's certificate
against its `peer_ca`. The client SAN is `client-<tenant>.<cluster_domain>`
where `<tenant>` is the tenant id as a positive integer without leading
zeros; it names the tenant whose data the connection reaches, not the
machine. Polariton never relaxes verification to reach a remote endpoint.

### 3. Each client holds its own identity

A Windows client presents a certificate issued to it for the intended
tenant, signed by the CA the listener trusts. Client private keys are never
copied between machines. Before use, the key file's access control is
checked and a file that fails the check is refused: on Unix, mode `0600`
owned by the running user; on Windows, an ACL whose only access entries
are the running user, `SYSTEM`, and `BUILTIN\Administrators`, with no entry
for `Users`, `Authenticated Users`, or `Everyone`. The key loader does not
enforce this today; the check and its tests are implementation work.

### 4. Authenticated configuration is complete or refused

The four authenticated fields select the authenticated transport together.
A configuration with some but not all of them is refused with a message
naming the missing fields; it never falls back to the loopback path, and
the loopback path never accepts a non-loopback address.

### 5. Failures are reported for what the client knows

A server-name mismatch is detected locally and reported precisely as that.
A rejection by the peer is reported as the peer's rejection of the
client's authentication, without inventing a cause the peer did not
disclose.

### 6. A running deployment is not reconfigured underneath its operator

Establishing this contract does not touch a deployment that is in use for
other work. The remote endpoint is a separate Toron instance, or a
reconfiguration the operator schedules explicitly.

### 7. The scratch notebook keeps standing alone

ADR 0163 §2 holds: SQLite scratch and plotting work with no Nilket present
and no Toron configured, in the same build that can connect remotely.

## Mechanisms in this cut

The Toron adapter configuration gains the authenticated form's fields
(`server_name`, client certificate, key, CA) beside `endpoint`;
`ToronAdapter::connect` selects `ToronClient::authenticated` when all four
are present, refuses when some are, and uses `loopback` when none are. The
`toron` feature's portable half (`toron-client`, `tls-pki` minus its
development writer) and its Linux half (`nilket-local`, `nix`) are gated
separately so the Windows head can carry the first. `tls-pki`'s key loader
gains the access-control check of §3 for both platforms. On the Linux
side, the Toron listener is bound to a LAN address with a `[tls]` section
naming a listener certificate whose SAN covers that address's name, and a
client certificate with the tenant SAN of §2 is issued for the Windows
machine from the deployment's CA. Whether that issuance is a Nilket command
or an operator procedure is decided in the Nilket implementation,
separately. Each mechanism may be replaced while the decision stands.

## Acceptance

1. Native Windows Polariton, through its own connection form and adapter
   flow, executes an authenticated query against a separate Linux Toron
   instance and shows rows that belong to the tenant its certificate names.
   Not a standalone `ToronClient` probe.
2. A client certificate the listener does not trust is rejected and
   reported as the peer's rejection; a configured server name that the
   listener's certificate does not cover is rejected and reported as a
   server-name mismatch. The two reports are distinct.
3. A configuration with one to three of the four authenticated fields is
   refused naming the missing fields, and does not connect by loopback.
4. A client key file that fails §3's access-control check is refused
   before any connection attempt, on both platforms.
5. SQLite scratch and plotting work in the same Windows build with no Toron
   configured.
6. Ket changes and Nilket changes land in separate implementation commits,
   each with its lane passing on Bigfoot.

## Limits and follow-up

This is one client to one Toron over mTLS on a LAN. Certificate issuance at
scale, revocation, and remote Wendelon access are not decided here. The
loopback path and the Linux local workspace are unchanged. Whether the
Windows head should later supervise a remote workspace's lifecycle is a
separate plan.
