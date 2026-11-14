# Configuration Schema

This document describes the configuration schema implemented by the config loader.

`tuning.storage_shutdown_timeout_seconds` bounds the request-quiescence wait and,
separately, storage write drain and worker joins during shutdown (default 30,
must be positive and at most 3600). If storage cannot prove every writer stopped,
the daemon terminates itself with exit status 86 instead of lingering with the
directory lock held; the next start finds no clean-shutdown record and runs
recovery. Retained storage handles reject new writes once shutdown has closed
write admission.

Nodes are bootstrapped from a static TOML file such as `wendelon.toml`. The
current loader is intentionally narrow: it covers the single-node bootstrap and
identity material required by the Control Ring and Data Ring signing paths.

## 1. TOML Structure

```toml
[node]
node_id = 1
advertise_address = "127.0.0.1:7001"

[storage]
data_dir = "data"
slab_max_bytes = 67108864
lsm_compaction_paused = false

[tuning]
replication_factor = 1
wal_io_depth = 64
slab_io_depth = 64
max_inbound_connections = 1024
max_inbound_connections_per_ip = 64
max_inbound_streams_per_connection = 64
max_concurrent_stream_tasks = 4096

[identity]
# Default false. Set true only for intentionally insecure local dev keys.
allow_dev_keys = false
lease_signing_secret = "<64 hex chars>"
transfer_signing_secret = "<64 hex chars>"
audit_root_signing_secret = "<64 hex chars>"

[identity.trusted_lease_signers]
"1" = "<64 hex chars>"

[identity.trusted_transfer_signers]
"1" = "<64 hex chars>"

[identity.trusted_audit_root_keys]
"1" = "<64 hex chars>"
```

## 2. Notes

- `data_dir` may be absolute or relative. Relative paths resolve from the
  directory containing the TOML file.
- A node holds an exclusive, nonblocking `flock` on the persistent
  `<data_dir>/LOCK` sentinel for its lifetime and while any storage worker can
  write. A second node using the same canonical directory fails with a busy
  error. Do not remove or replace `LOCK` during cleanup or reset. NFS behavior
  is not assumed safe; verify cross-client lock propagation for the exact mount
  and server before placing a Wendelon data directory there.
- The trusted signer maps must contain an entry for `node_id`, or bootstrap
  validation fails.
- `slab_max_bytes`, `rf`, `wal_io_depth`, and `slab_io_depth` default to
  `67108864`, `1`, `64`, and `64` respectively when omitted.
- `tier_history_budget_bytes` is the ADR 0692 §7 node budget for tier
  history, publication scratch, proposer tokens and relocation redirects. It
  defaults to `268435456` (256 MiB) and must be greater than zero. A
  publication that cannot reserve its history within the budget is fenced and
  retried rather than allowed to grow past it. A budget too small to hold the
  manager's fixed proposer-token registry makes the node refuse to start.
- `lsm_compaction_paused` is a node-local ADR 055 maintenance switch. It
  defaults to `false`; when set, the LSM compaction task starts but does not
  launch new jobs until resumed through the node's service control handle.
- Transport admission defaults to `1024` total inbound connections, `64`
  inbound connections per peer IP, `64` streams per connection, and `4096`
  concurrent stream-handler tasks when the corresponding `[tuning]` fields are
  omitted.
- The current binary can generate a starter file from the checked-in example
  template:

```text
wendelon --print-example-config
wendelon --write-example-config ./wendelon.toml
```

- The generated starter config uses dev-derived key material for local bring-up.
  Operators should replace that identity material before any non-development
  deployment.
- Audit sink posture is asymmetric by design: `<data_dir>/audit.ndjson` is the
  authoritative local chain, while `Syslog` and `Kafka` are additive shipping
  sinks. See `plans/ops/audit.md` for operator-facing sink schema details.

`identity.allow_dev_keys` defaults to `false` when absent. The generated starter
config and `NodeBootstrapConfig::dev()` explicitly enable it for insecure local
bring-up. Direct `NodeBootstrapConfig` literals must choose the field explicitly.
The flag bypasses only recognized dev-key refusal: local trusted entries must match
the signing secrets, lease, transfer and audit public-key sets must be pairwise disjoint across all
ids, and each trusted map must use a distinct key per id.

Dev-key recognition checks all three role derivations for the finite set
of the local id and all ids in all three trusted maps, against all three roles. A dev-derived
key for an id outside that set is not detected; arbitrary weak-key detection needs
separate key provenance. Startup errors retain a typed `IdentityKeyError` inside
an `InvalidInput` I/O error and contain roles and node ids, never key material.

Publishing this policy does not rekey a running deployment. Before restarting with
it, an operator using dev keys must explicitly authorize that insecure configuration
or provision non-dev keys. File custody and key rotation are separate work.

### Audit root trust

`[identity] audit_root_signing_secret` is required and independent of lease and transfer signing.
`[identity.trusted_audit_root_keys]` is a required numeric-node map. Numeric aliases such as
`1` and `01` collide and are refused. All three roles require a matching local entry,
separate secrets and pairwise disjoint public keys, and no repeated key within a role.
`allow_dev_keys` bypasses only finite-set recognition of the three development derivation
families (local node plus all three map ID sets); it never bypasses consistency or separation.

Offline verification uses `nilket wendelon audit verify <path> --trusted-audit-roots <file>`.
The trust file is at most 1 MiB and has exactly `{"version":1,"keys":{"n-01":"<64 lowercase hex>"}}`.
Node spelling must be canonical, object keys unique, and enrolled public keys canonical,
nonweak and torsion-free. Signed roots without explicit trust yield Unanchored issues and
exit 1. Rootless evidence is labelled NoRoots and verifies integrity only. Anchored means
every root verified against its enrolled key and record metadata; it does not assert custody.

An invocation selects one historical key per node. Old transfer-signed audit evidence can
be checked only by explicitly selecting that historical transfer public key as audit trust
for the chosen file/span and recording the migration-of-trust note. No key is inferred from
records, configuration role fallback or epochs. Logs are never rewritten by verification.
Custody, rotation, runtime configuration and redeployment are separate operator work.

### Query no-progress interval

`[tuning] query_stall_bound_millis` is how long one awaited unit of a query may go
without completing before the node answers `QUERY_STALLED`. Default 250; accepted
range 1 to 600000; set per node (ADR 541, ADR 0719). A unit is, for example, one
record's whole read from wherever it is stored, or one slab's statistics load. When a
record is in a remote object store, that one read is several requests made one after
another (the object's size, its tail, sometimes its index, then the record's bytes;
ADR 0730 removed the last when the tail already holds them), and they all count
against the one interval.

It is not a deadline for the query: a query of many units runs for as long as its
units keep completing. The caller's own deadline is separate and, when it is nearer,
it is what ends the query. A larger interval means only that a read which has truly
stopped is reported that much later. No value guarantees that every request to a
store is answered in time.

#### Working profile for the development RustFS store

Agreed 2026-11-14 as the working day-one profile for nodes that tier to the
development RustFS store. Put it in the configuration of every node of the cluster:

```toml
[tuning]
query_stall_bound_millis = 2000
```

What it has been tested with, and no more:

- The real-daemon workflow `three_daemons_offload_to_the_object_store_and_serve_their_rows_across_restarts`
  (`wendelon-client/tests/parent_decisions_processes.rs`): three nodes in the
  ParentDecisions mode, 64 single-row writes into 8 KiB slabs (a stress shape, nearly
  one object per row, not deployment sizing), every node offloading its slabs to
  RustFS over a LAN, an exact read of all rows, each node stopped and started again
  in turn, and an exact read again.
- It passed once with the three daemons on one host and once with one daemon on each
  of three hosts. One execution each: not a rate.
- At the default 250 the same workflow's reads ended `QUERY_STALLED` in three of five
  judged queries. Those results stand; the general default is unchanged; 2000 ms is
  the working RustFS profile documented here.

Limits: both reads in each passing run were served by one node; nothing was measured
about how close any read came to the interval; a crash, a partition, a node that does
not return and any other store or workload are not covered. A deployment with another
object store, network or data shape needs its own evidence for its own value. The
configuration loader does not check this setting against the storage configuration:
nothing refuses a node that tiers to a remote store at the default.

### Consensus timing profile

`[tuning] consensus_timing_base_millis` declares how patient the control group's
consensus is (ADR 0731). Default 50; accepted range 10 to 1000; a value outside it
refuses startup. It is set per node and should be the same on every node: nothing
checks that, and a node with a smaller value than the others times out first and
disturbs them. Each node's readiness answer (`nilket wendelon inspect --json`) shows
its own profile under `consensus_timing`; one node's answer says nothing about the
others.

Every consensus timing value is derived from this one number:

| Value | Derived as | At the default (50) |
|---|---|---|
| heartbeat, and the nominal timeout of each replication call and of each call of a read's leadership confirmation | base | 50 ms |
| election timeout, and the nominal timeout of a vote request (the lower edge) | 3 to 6 times base | 150 to 300 ms |
| nominal timeout of one snapshot chunk | 4 times base, never under 200 ms | 200 ms |

These are nominal values, not guarantees. A timeout covers a whole call (reaching the
peer, the peer's storage and processing, scheduling on both sides), not a network
round trip, and a call that completes late may still be accepted.

What a larger base trades, as arithmetic from the ratios and not as a bound on
detecting a failure or restoring service (electing, replicating and applying take
further time):

| base | a follower that had a leader starts an election after silence of about | a node that knows of a longer log waits a further |
|---|---|---|
| 50 | 0.45 to 0.68 s | 0.6 s |
| 250 | 2.25 to 3.4 s | 3 s |
| 500 | 4.5 to 6.75 s | 6 s |
| 1000 | 9 to 13.5 s | 12 s |

A larger base gives up on slow replies later and deposes a live leader less readily;
it also hears from an idle leader less often and replaces a lost one later. No value
gives both. For any value there are networks whose replies arrive later than it, and
on those the group may fail to keep or to re-establish a leader.

Other timers do not follow this setting and keep their values:

- The range lease (`data_ring_lease_ttl_millis`, default 3000) is renewed through
  consensus. When a leader change lasts longer than the lease has left, leases lapse:
  the node reports ranges not leased and is not ready until they are renewed, and
  other replicas may become eligible to take a range over. A longer lease avoids that
  and also delays takeover after a real failure. It is a separate trade to assess
  together with the base; no pairing is recommended here.
- Forwarded decisions, cuts and lookups have fixed deadlines of 5 to 10 seconds and
  answer that the outcome is not known when they pass; during a long leader change
  more of them do.
- The client library makes three attempts tens of milliseconds apart and then reports
  the attempts exhausted. On a slow profile that happens well before a new leader
  exists; a caller that must ride through a leader change retries itself, inside its
  own deadline. An answer that the attempts are exhausted or that the outcome is not
  known does not say the operation did not commit. Such a retry repeats the same
  operation with the same identity and the same request, within the identity's
  validity and the caller's deadline, and never starts a fresh identity only because
  the answer was uncertain. When the outcome cannot be resolved that way, the caller
  reports it as unknown; it does not report that nothing was written.

No profile other than the default has been tested.

### Running under systemd

Run Wendelon so the service manager owns its whole process tree and waits out its
bounded shutdown before escalating:

```text
[Service]
KillMode=control-group
TimeoutStopSec=100
Restart=on-failure
```

- `KillMode=control-group` delivers the stop signal and the final SIGKILL to every
  process in the unit's cgroup, not only the main PID.
- `TimeoutStopSec` must exceed Wendelon's own shutdown watchdog, which fires at
  2 x `tuning.storage_shutdown_timeout_seconds` + 25 s (quiescence wait and storage
  close are each bounded by the timeout). Use 2 x the timeout + 40 s: 100 for the
  default 30.
- The daemon drains on either SIGTERM (systemd's default stop signal) or SIGINT.
- Exit status 86 means the process self-terminated after a shutdown that could not
  prove every writer stopped. It is not a configuration error: `Restart=on-failure`
  restarts it, and the next start finds no clean-shutdown record and runs recovery.
  `nilket local` applies the same grace and reports a supervisor SIGKILL as
  "unclean stop (killed by supervisor)".
