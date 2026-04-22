# Chaos Seed Registry

Frozen chaos regressions live here as TOML files named `{issue_number}.seed`.

Supported `scenario` values in the current catalog are:

- `leader_partition_and_rejoin`
- `rolling_kill`
- `packet_drop_single_stream`

`packet_drop_single_stream` is the in-process smoke approximation of the
packet-loss bug class. The kernel-backed `packet_drop` / `packet_reorder`
scenarios remain separate deferred work at higher fidelity.

Seed files use this shape:

```toml
issue = 1234
scenario = "leader_partition_and_rejoin"
seed = 424242
introduced_at = "2026-04-21"
notes = "Short note describing the regression."
```

The filename stem must match `issue` exactly. Validation and replay happen in
`tests/chaos.rs`, and CI runs them through `ci/check_chaos_harness.sh`.

The weekly `seed-retention-advisory.yml` workflow scans this registry and the
simulator registry to produce an advisory retirement report; retiring a seed
still requires an explicit PR.
