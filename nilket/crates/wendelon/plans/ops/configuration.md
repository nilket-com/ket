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
