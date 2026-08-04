# impl 532 - Local Nilket deployment profile

Parent: `../adr/0532_local_deployment_profile.md`. This plan supersedes the
unimplemented execution shape in impl 530 without modifying that historical
record. Scope is one foreground, durable, Linux local profile plus the shared
Toron client it requires. The `ket` router, background services, ToronScratch,
and automatic Polariton lifecycle remain out of scope.

Deliverable: one `impl:` commit spanning the nilket workspace and Polariton,
followed by review against the gates below. No production-performance claim is
created.

## Verified starting state (2026-07-31)

- `nilket/crates/nilket/src/main.rs` is ADR 040's synchronous two-role router;
  no arguments print usage and fail.
- `nilket toron start` requires a Toron TOML; `nilket wendelon` requires a
  Wendelon TOML.
- Wendelon's generated single-node config is durable and seeds tenant `0`, but
  Toron's example selects tenant `1` and its non-TLS storage path cannot
  discover a live Wendelon leader/provider.
- Toron's SQL listener defaults to loopback QUIC `127.0.0.1:7742` and emits
  `READY` / `SQL_READY`; its wire has Execute, Cancel, ResultBatch, Complete,
  and Error but no side-effect-free health message.
- Client connection/framing lives privately in `toron::cli`; Polariton's
  feature-gated first-contact client duplicates those protocol enums.
- The first-contact harness uses
  `start_with_wendelon_client_for_harness`, a simulated transport, and
  process-temporary state. None is a permitted launcher dependency.
- Wendelon's runtime is Linux-specific through `io_uring` and `O_DIRECT`.

## Stage 1 - Public `toron-client` and one health definition

Create `nilket/crates/toron-client` and add it to the nilket workspace. It
owns the client-facing SQL protocol and bounded client behavior:

- move `QueryId`, `SqlFrame`, protocol/message/error types, and bounded frame
  I/O behind the client crate; Toron re-exports old paths during migration;
- append typed `Health` / `Healthy` messages after the existing postcard enum
  variants so old discriminants remain byte-identical; they open a normal SQL
  stream, touch no catalog/query state, and close without the warning-shaped
  empty handshake;
- expose loopback and authenticated client configurations, bounded connect and
  health, execute, cancellation frame, ordered batch collection, and terminal
  validation;
- preserve explicit limits: request <= 4 MiB, result <= 128 MiB, response
  frame <= result + 1 MiB; expose typed configuration, timeout, transport,
  protocol, and remote `{code,message}` failures;
- remain free of Arrow, Polars, editor, and document-engine dependencies;
- provide the asynchronous client plus a narrowly scoped blocking health
  entrypoint so the synchronous Nilket supervisor and CLI do not acquire a
  shared role runtime.

Toron's CLI adopts this crate. Polariton's `toron` feature depends directly on
it, deletes `adapters/toron/client.rs`'s duplicate wire/framing, and keeps Arrow
IPC -> Polars conversion quarantined in its existing compatibility module.

Tests:

1. round-trip every message and typed error, including Health/Healthy, while
   golden pre-532 frames prove all existing postcard bytes are unchanged;
2. reject wrong query ids, duplicate/out-of-order batches, missing terminal,
   oversized request/response/result, and response after terminal;
3. distinguish connect timeout, health timeout, query timeout, transport, and
   typed remote refusal text;
4. live loopback and authenticated health probes are side-effect-free and
   produce no empty-connection warning;
5. Polariton, integration tests, and the local supervisor call the same client
   health API (source audit plus a scanner-positive control);
6. manifest denylist rejects Arrow, Polars, libaryon, Baryon, and Polariton,
   including renamed/package and workspace-inherited dependencies; synthetic
   violating manifests prove the guard fires and its failure cites ADR 532.

## Stage 2 - Generated local profile

Add a local-profile module owned by the `nilket` leaf binary. It resolves:

```text
--data-dir PATH
or ${XDG_DATA_HOME}/nilket/local
or ${HOME}/.local/share/nilket/local
```

If neither XDG data nor a usable home exists, refuse with the `--data-dir`
remedy. The module creates directories with restrictive permissions and a
create-new marker carrying `format_version = 1` plus a stable profile UUID.

Layout:

```text
<profile>/profile.toml
<profile>/config/wendelon.toml
<profile>/config/toron.toml
<profile>/pki/...
<profile>/data/wendelon/...
<profile>/logs/...
<profile>/local.lock
<profile>/runtime.json
```

Role crates retain configuration ownership: add local-profile render helpers
that produce ordinary validated role TOML rather than constructing role
runtimes in the supervisor. Both configs select tenant `0`; Wendelon exposes
real loopback mTLS at `127.0.0.1:7001` plus its existing observability listeners
at `127.0.0.1:7431` (health), `:7432` (authenticated readiness), and `:7433`
(authenticated metrics). Toron uses a node-CA-authenticated storage client and
loopback SQL `127.0.0.1:7742`.

Extract the authenticated `/ready` client behind `wendelon inspect` into one
Wendelon-owned library seam used by inspect, the local supervisor, and its
integration tests. This remains separate from `toron-client`: role readiness
and SQL service health are distinct contracts. A raw TCP-connect check is not
readiness.

Extract the repository's existing rcgen fixture shape into a production
development-PKI helper under `tls-pki`. Generate node/operator CAs and leaves
once, write private keys create-new with mode 0600 and directories mode 0700,
and reuse them. Do not shell out to OpenSSL. Production PKI policy is
unchanged, and every startup labels these credentials development-only.

Retained profile files are never overwritten. Unsupported format, malformed
config, missing files, or bad permissions refuse with the exact profile path
and remedy; no implicit regeneration or reset.

Tests cover XDG/home/override resolution, missing-home refusal, create-new
idempotence, tenant and endpoint values after parsing through the real role
loaders, permissions, retained edits, malformed/version refusal, and a source
guard (with synthetic positive control) forbidding an OpenSSL subprocess.

## Stage 3 - Lock, runtime identity, and reset boundary

Acquire an OS advisory exclusive lock on `local.lock` before mutating or
starting the profile. Open it close-on-exec and prove neither role inherits the
descriptor. Publish `runtime.json` by write + flush + atomic rename:

```text
version, profile_uuid, session_nonce, pid, process_start_token,
started_unix_ms, wendelon_endpoint, sql_endpoint
```

The lock is authority; PID is display. Never infer ownership from PID or port
alone. On a busy lock, read the runtime record and call the shared Toron health
probe. A matching healthy record reports/reuses the existing deployment,
emits its existing LOCAL_READY identity, and exits successfully without
ownership. Failed health refuses; it never steals the lock or signals the
recorded PID. A free lock makes any runtime record stale and safe to replace
after validation.

`--reset` runs only while holding the lock and before children start. It
refuses:

- empty/unresolved paths, filesystem root, or the user's home;
- symlinks whose resolved target escapes the selected profile parent;
- missing/mismatched profile marker;
- a held live-profile lock.

Print the resolved target before deleting it, but preserve the profile root and
the open `local.lock` inode throughout. Delete only marked generated children,
regenerate them under the same lock, then continue startup. Never
unlink-and-recreate the lock path while relying on the old inode's advisory
lock. Tests include root/home/empty, symlink escape, foreign directory, held
lock, stale manifest containing the current live PID, and successful marked
reset. A competing launcher held at a barrier throughout reset must remain
excluded; this is the positive control for the lock-inode rule. The
recycled-PID fixture separately proves PID alone grants no authority.

## Stage 4 - Foreground two-process supervisor

Resolve `current_exe` and spawn explicit child commands—never role objects:

```text
<self> wendelon <profile>/config/wendelon.toml
<self> toron start <profile>/config/toron.toml
```

Pipe both stdout/stderr. Dedicated bounded reader threads prevent child pipe
backpressure, consume readiness lines, and forward ordinary lines to parent
stderr with `wendelon:` / `toron:` prefixes. The parent's stdout is reserved
for its composite contract. Bound the bytes retained for any one unterminated
child line; overlong output is chunked with attribution rather than accumulated
without limit.

Place each child in a distinct process group so terminal Ctrl-C reaches only
the supervisor's lifecycle policy, not both roles at once. The supervisor
signals the owned Toron group, waits through its named drain deadline, and only
then signals the owned Wendelon group. A Linux integration fixture records the
signal/exit order and proves Wendelon receives no termination signal while
Toron is still inside its allowed drain window.

Install Linux parent-death signaling before each child exec, with the usual
parent-PID recheck closing the fork/configure race. A crash fixture kills the
supervisor, proves both role processes exit and no child retains `local.lock`,
then starts the same profile successfully from the stale runtime manifest.

Startup sequence:

1. preflight all fixed transport, observability, and SQL endpoints; a foreign
   occupant refuses—never choose another default port;
2. spawn Wendelon;
3. wait for authenticated Wendelon readiness, not merely an open socket;
4. spawn Toron;
5. wait through `toron-client::health`;
6. atomically publish runtime identity;
7. print exactly:

   ```text
   LOCAL_READY v=1 profile=local sql=127.0.0.1:7742 session=<nonce>
   ```

The LOCAL_READY formatter and parser are one tested library seam. Field order,
unknown-version rejection, missing/duplicate fields, nonce shape, and newline
termination are pinned. A malformed-line fixture is the positive control for
the parser rather than a string-contains-only test.

On Ctrl-C, request Toron shutdown and await its drain deadline, then Wendelon;
escalate only after named deadlines and report each escalation. If either child
exits first, capture its status, drain the sibling, print profile/log paths, and
return non-zero. Never signal a pre-existing deployment discovered through a
busy lock.

The supervisor module must not name or construct `WendelonNode`, `ToronDaemon`,
their harness constructors, or simulated transports. A source audit scans the
production module; its synthetic forbidden snippet proves the assay fires and
the diagnostic points to ADR 532. The stronger integration proof observes two
distinct child PIDs and real loopback traffic.

## Stage 5 - CLI surface and honest UI handoff

Change the root dispatcher:

- no arguments -> local profile with no options;
- `local [--data-dir PATH] [--reset]` -> local profile parser;
- `toron ...` and `wendelon ...` -> existing delegated role runners;
- root `--help` / `--version` remain side-effect-free;
- unknown root commands still fail with usage.

`nilket toron` without a verb must not start the local profile. Replace ADR
040's now-superseded no-args/no-local guards with ADR 532 guards; do not edit
ADR 040 or its historical tests into a new story. Every new negative source
guard has a synthetic positive control and an actionable ADR-532 failure
message.

Polariton's unavailable-local message names the stable endpoint and says:

```text
Start local Nilket with: nilket
```

Polariton does not spawn or stop it. The future `ket` router/default profile is
not introduced here.

## Stage 6 - Durability and dogfood acceptance

Check in the shared contract SQL script and an ignored Linux live fixture:

1. start bare `nilket` against a fresh explicit profile;
2. wait for parsed LOCAL_READY;
3. health-probe through public `toron-client`;
4. CREATE `dogfood`, INSERT three rows;
5. Ctrl-C and prove both child PIDs exit cleanly;
6. restart the same profile;
7. SELECT and prove the same rows and provenance;
8. query `information_schema.schemata`, `.tables`, and `.columns` and observe
   `dogfood`;
9. run grouped aggregate, UPDATE, DELETE, and DROP, recording each as executed
   or a typed, full-text refusal rather than treating unsupported SQL as a
   launcher failure.

Run the equivalent session from Polariton through the adopted client. Result
materialization remains under the first-contact 128 MiB ceiling; Arrow
streaming and UI cancellation stay reserved for the analytics seam.

## Explicit exclusions

- no in-process/fused daemon or simulated local transport;
- no daemonization, service manager, background ownership, or remote profile;
- no random default ports or automatic port hunting;
- no CREATE USER/authz design or production PKI claim;
- no Polariton auto-start, `ket` router, or typed router defaults;
- no ToronScratch/default-promotion claim;
- no Arrow/Polars interpretation inside `toron-client`;
- no performance tuning or TPC-H gate credit.

## Review gates

- [ ] Bare `nilket` and `nilket local` converge on the same local profile;
      `nilket toron` never starts Wendelon.
- [ ] Two real child PIDs, real loopback mTLS, tenant `0`, stable SQL endpoint;
      no role runtime or harness type in the supervisor.
- [ ] Public health is side-effect-free and one implementation is reused by
      supervisor, Polariton, and tests; Wendelon readiness separately reuses
      the authenticated `wendelon inspect` seam.
- [ ] Profile artifacts are explicit, retained, permissioned, and parsed by
      real role loaders; production config defaults remain unchanged.
- [ ] Lock + runtime generation defeats stale/recycled PID identity; a second
      launch never acquires or kills an existing deployment, and close-on-exec
      prevents child lock inheritance.
- [ ] LOCAL_READY v1 is parsed/formatted through one contract with positive
      malformed controls; child output is attributed on stderr.
- [ ] Child process groups prevent terminal signal fan-out; Ctrl-C and
      child-failure paths drain/reap both owned children in the proved order;
      parent death leaves no orphan roles.
- [ ] Reset refuses every unsafe target and a live lock before deletion, and
      preserves one continuously held lock inode throughout reset.
- [ ] CREATE + INSERT + stop + restart + SELECT proves durable rows; catalog
      queries see the table; remaining SQL outcomes are typed and recorded.
- [ ] Every negative architecture guard has a positive control and cites ADR
      532 in its failure message.
- [ ] `toron-client` manifest is free of analytics/editor dependencies, with
      alias/workspace/target-table evasions tested.
- [ ] Linux is the only success claim; unsupported platforms refuse honestly.
- [ ] Nilket full tests/architecture suite, Toron SQL listener tests, Polariton
      Toron-feature tests, cross-product ignored fixture, focused Clippy,
      formatting, and `git diff --check` pass.
