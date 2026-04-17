# Configuration Schema

This document describes the configuration schema implemented by Workstream E.1.

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

[tuning]
rf = 1
io_depth = 64

[identity]
lease_signing_secret = "<64 hex chars>"
transfer_signing_secret = "<64 hex chars>"

[identity.trusted_lease_signers]
"1" = "<64 hex chars>"

[identity.trusted_transfer_signers]
"1" = "<64 hex chars>"
```

## 2. Notes

- `data_dir` may be absolute or relative. Relative paths resolve from the
  directory containing the TOML file.
- The trusted signer maps must contain an entry for `node_id`, or bootstrap
  validation fails.
- `slab_max_bytes`, `rf`, and `io_depth` default to `67108864`, `1`, and `64`
  respectively when omitted.
- The current binary can generate a starter file from the checked-in example
  template:

```text
wendelon --print-example-config
wendelon --write-example-config ./wendelon.toml
```

- The generated starter config uses dev-derived key material for local bring-up.
  Operators should replace that identity material before any non-development
  deployment.
