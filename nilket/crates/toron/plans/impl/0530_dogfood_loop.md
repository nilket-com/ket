# impl 530 - durable Toron dogfood loop

Cross-repo contract: `ket/crates/libaryon/plans/impl/toron-dogfood-loop.md`.
This plan owns the nilket half: a real two-process local deployment, a public
SQL client crate, and the hard durability witness. It is product dogfooding,
not a performance experiment and not a Toron-default promotion claim.

## Corrected starting facts

- `nilket wendelon <config>` and `nilket toron start <config>` exist, but the
  generated non-TLS Toron storage client has no transport providers and seeds
  `leader: None`; those configs can start without becoming queryable.
- Wendelon fresh bootstrap provisions durable tenant `0`; Toron's example
  config selects tenant `1`. Local mode must align on `0`, not invent tenant
  administration as a prerequisite.
- Brainstorm 005 already decides the local shape: one parent command, two OS
  child processes, localhost networking. No in-process Wendelon harness may
  become a product launcher.
- Toron's SQL wire types are public, but reusable client behavior is trapped
  in `toron::cli`; Polariton consequently copied the protocol and loopback
  transport. That duplicate dies in this pair.
- Tenant-scoped catalog browsing already exists as ordinary SQL over
  `information_schema.schemata`, `.tables`, and `.columns`; no catalog RPC is
  required.

## 1. Public `toron-client` crate

Create a workspace crate that owns the client-facing SQL protocol and client:

- `QueryId`, `SqlFrame`, message/error types, and bounded frame I/O move behind
  this crate; Toron re-exports them at the old paths to keep server callers and
  tests source-compatible.
- `ToronClient` supports loopback and authenticated configurations, a bounded
  connect/health probe, execute, and cancellation frames. Errors distinguish
  configuration, connect timeout, query timeout, transport, protocol, and
  typed remote `{ code, message }` failures.
- Result batches remain Arrow IPC bytes plus row count. The client does not
  depend on Arrow or Polars and does not interpret products.
- Limits are explicit: request <= 4 MiB, result <= 128 MiB, response frame <=
  result + 1 MiB; batch order, query id, and terminal-frame rules are checked.
- Toron's CLI adopts the crate rather than retaining a second network loop.
  Polariton can therefore depend only on the lightweight client crate.

Tests pin wire round trips, out-of-order and oversized responses, full typed
remote errors, connect-vs-query timeout wording, health probe, cancel shape,
and both loopback/authenticated configuration rejection rules.

## 2. `nilket local`

Add a developer-laptop command:

```text
nilket local [--data-dir PATH] [--reset]
```

- Default durable state follows the platform data directory under
  `nilket/local`; `--data-dir` makes tests and parallel checkouts explicit.
- First run creates development-only node/operator CAs and leaves, Wendelon
  and Toron role configs, and stable data directories. Generated files are
  reused on restart. The command labels this custody as local development.
- Spawn the current `nilket` executable twice: Wendelon first, then Toron only
  after the Wendelon transport port accepts. Inherit child stdout/stderr so
  readiness and failures remain visible. The parent owns shutdown/reaping.
- Wendelon binds real loopback mTLS at `127.0.0.1:7001`; Toron connects with a
  node-CA-signed data-node client and serves loopback SQL at
  `127.0.0.1:7742`. Both select tenant `0`.
- `--reset` is explicit and confined to the resolved local data directory. It
  refuses filesystem root, home, and an empty path; no implicit data loss.
- Port occupation, child early exit, malformed retained config, missing TLS
  tools/material, and readiness timeout fail loudly with actionable messages.

The launcher may use the repository's established OpenSSL certificate-fixture
shape, but production trust policy remains untouched. Local credentials are a
developer artifact, never an auth design.

## 3. Hard acceptance and honest surface census

An ignored live test and a checked-in SQL script drive:

1. launch into a fresh explicit data directory;
2. `CREATE TABLE dogfood ...`;
3. `INSERT` three rows;
4. stop both processes cleanly;
5. relaunch the same directory;
6. `SELECT` returns the same rows and provenance.

This durability path is a hard gate. Then run grouped aggregate, `UPDATE`,
`DELETE`, and `DROP`; each either executes or records its typed refusal in the
ket diary/nilket result section. No claim is inferred from parser tests.

Catalog acceptance executes ordinary queries against the three supported
`information_schema` relations after restart and sees `dogfood`.

## Explicit exclusions

- UI cancellation and adapter-trait widening;
- `CREATE USER`, authn/authz, remote connections, or production PKI;
- Arrow/Polars product interpretation in `toron-client`;
- performance tuning, ToronScratch, or default-promotion gate credit;
- an in-process standalone daemon.

## Gates

- `toron-client` tests and `--all-targets` Clippy;
- Toron library and SQL-listener tests;
- complete nilket architecture suite;
- launcher unit tests plus the ignored fresh/restart durability fixture;
- direct CLI smoke: one command, query through `nilket toron sql`, Ctrl-C,
  relaunch, query again;
- formatting and `git diff --check`.
