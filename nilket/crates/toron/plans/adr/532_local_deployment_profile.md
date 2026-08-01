# ADR 532 - Local Nilket deployment profile

**Status:** Accepted 2026-07-31 after user/Claude/Codex brainstorm convergence.
Implementation has not begun. The implementation contract is
`../impl/532_local_deployment_profile.md`.

ADR 040 established Nilket's role router and deliberately excluded local
orchestration. This record adds the deployment-profile grammar that later
dogfooding made necessary. It references and partially supersedes ADR 040; it
does not edit that historical decision into prescience. Impl 530 remains the
earlier dogfood proposal and is likewise unchanged.

ADR 531's Forward section called the q11 follow-up an "ADR 532 candidate."
Candidate labels do not reserve identifiers. This accepted decision therefore
uses 532; the q11 decision takes the next available number when promoted.

## Context

The explicit role commands exist:

```text
nilket wendelon <wendelon.toml>
nilket toron start <toron.toml>
```

They are operator primitives, not a queryable local product. The generated
non-TLS Toron storage client has no usable provider/leader discovery path,
Toron's example selects tenant `1` while fresh Wendelon durably provisions
tenant `0`, and users must coordinate two configurations, identities, ports,
readiness states, processes, and shutdown order themselves.

The temporary first-contact harness proved Polariton -> Toron SQL -> Wendelon
storage -> plot, but it embeds a simulated Wendelon transport behind Toron's
test-only constructor and creates process-temporary state. It is evidence, not
a product launcher.

Daily dogfooding needs a foreground local database with SQLite-like activation
energy while preserving Nilket's actual two-role physics. The name must tell
the truth: Toron is compute; Wendelon is storage; the local database containing
both is Nilket.

## Decision

### 1. Roles and profiles are distinct grammar

`toron` and `wendelon` remain role nouns. `local` is a deployment profile.

```text
nilket                              # exactly equivalent to `nilket local`
nilket local [--data-dir PATH] [--reset]
nilket toron start <toron.toml>     # explicit compute role
nilket wendelon <wendelon.toml>     # explicit storage role
```

Root `--help` and `--version` retain their meanings. Local options require the
explicit `local` spelling; bare `nilket` selects the default local profile with
no arguments. `nilket toron` without a Toron verb remains role help/error and
never starts Wendelon.

This partially supersedes ADR 040 D2/D3/D5 for the local profile only. The
foreground parent is an orchestrator hosting neither role runtime; it starts
two child processes through the explicit role commands. ADR 040's delegated
role parsing, role-scoped verbs, leaf aggregation, and no shared role runtime
remain in force.

### 2. Smart defaults are generated artifacts, not configuration defaults

The launcher writes explicit, inspectable role configurations. It does not add
`Default` implementations that silently select development custody, tenant
identity, or storage topology inside production configuration types.

On Linux, the default profile root is:

```text
${XDG_DATA_HOME:-$HOME/.local/share}/nilket/local/
```

It contains a format/version marker, Wendelon data, role configs, local PKI,
logs, a held lock, and an atomic runtime manifest. `--data-dir` replaces the
profile root for tests, parallel checkouts, and explicit operator choice.
Generated configs are create-new and then reused; startup validates them but
never silently overwrites local edits.

The generated files are a teaching artifact: the local deployment is fully
described by the same role configuration surfaces used explicitly elsewhere.

### 3. One foreground supervisor, two real processes

The parent resolves its current executable and spawns:

1. `nilket wendelon <generated-wendelon.toml>`;
2. `nilket toron start <generated-toron.toml>` only after authenticated
   Wendelon readiness.

The roles communicate only over loopback networking with real mTLS. No
`WendelonNode`, `ToronDaemon`, simulated transport, or harness constructor may
enter the local supervisor. The parent owns process supervision, readiness,
output attribution, draining, and reaping; it owns no query or storage state.

Each child runs in a parent-owned process group isolated from the terminal's
direct signal fan-out. Ctrl-C reaches the supervisor, which drains Toron before
Wendelon; otherwise the terminal could signal both children simultaneously and
make the promised order fictional. Unexpected exit of either child drains the
other and exits non-zero. V1 is foreground-only: no daemonization, service
installation, or background PID management. Linux parent-death signaling keeps
an abruptly killed supervisor from orphaning either role indefinitely.

### 4. Stable local identity and endpoints

First run generates and persists development-only node/operator CAs and the
required leaves using Rust PKI machinery. Private material receives restrictive
permissions. Startup labels the custody as local development; it is never a
production authentication design.

Both roles select durable tenant `0`. Wendelon uses its stable loopback
transport endpoint at `127.0.0.1:7001` and its existing observability trio at
`127.0.0.1:7431` (health), `:7432` (authenticated readiness), and `:7433`
(authenticated metrics). Toron exposes SQL at `127.0.0.1:7742`. The default
profile never falls back silently to an ephemeral port: stable discovery is
part of the product contract. Test-only or explicit fixtures may still request
port `0`.

### 5. Readiness is layered and reusable

Wendelon role readiness and Toron client health are different contracts:

- the supervisor proves authenticated Wendelon readiness before spawning
  Toron by reusing the readiness client behind `wendelon inspect` rather than
  inventing a port-open check or second readiness definition;
- the public `toron-client` crate owns one side-effect-free Toron health
  exchange reused by the supervisor, Polariton, and integration tests.

The public client lives in the nilket workspace beside its protocol, as
PRODUCT-0001 requires. It also replaces Toron's private client loop and
Polariton's temporary duplicated wire client. It owns framing, limits, typed
remote errors, health, execution, and cancellation shapes without Arrow or
Polars product interpretation.

### 6. Runtime identity is lock plus generation, never PID alone

The profile holds an OS advisory lock for the supervisor's lifetime and
atomically publishes a versioned `runtime.json` containing at least PID,
session nonce, profile identity, endpoints, and start time. PID is diagnostic,
not authority: a stale manifest containing a recycled live PID cannot establish
ownership without the held lock and successful health proof. The lock
descriptor is close-on-exec and must not leak into either child.

A second invocation behaves deterministically:

- held lock + matching healthy runtime: report the existing endpoint and exit
  successfully without taking ownership, emitting the same LOCAL_READY
  contract with the existing session identity;
- held lock + failed health: refuse with the recorded identity and diagnostic;
- free lock + stale manifest: clean the runtime record and start normally.

No invocation stops a deployment it did not start.

### 7. Stdout is a versioned protocol; stderr is the human surface

The supervisor consumes child readiness lines internally. It prefixes all
ordinary child output on stderr (`wendelon:` / `toron:`), then emits exactly
one composite readiness line on stdout in stable field order:

```text
LOCAL_READY v=1 profile=local sql=127.0.0.1:7742 session=<nonce>
```

The line is an interface for the cross-product test and future launchers.
Human startup resolution, paths, custody warning, child failures, and log
locations remain on stderr. The runtime JSON is the structured local discovery
record; the readiness line is the publication event.

### 8. Reset is explicit, marked, locked, and recoverably scoped

`--reset` refuses an empty path, filesystem root, the user's home directory,
symlinks escaping the resolved profile, directories without Nilket's generated
profile marker, and any profile whose lock is held. Before deletion it prints
the exact resolved target. The reset keeps the profile root and the held
`local.lock` inode in place while removing only marked generated contents,
then regenerates them. Unlinking the held lock would permit a concurrent
launcher to create and lock a different inode at the same path, so it is
forbidden. There is no implicit reset after malformed config, version drift,
or startup failure.

### 9. Explicit local selection is not Toron promotion

Bare `nilket`, `nilket local`, and a future user-selected `ket` startup profile
are explicit database choices. They do not make Toron Polariton's scratch
default and earn no KET-0005 promotion-gate credit. Polariton does not
auto-start or own the deployment in this decision; when unavailable it may
point users to `nilket`.

### 10. V1 platform scope is Linux

Wendelon's current durability substrate is `io_uring`/`O_DIRECT`; process
supervision portability does not imply storage-engine portability. Unsupported
platforms refuse honestly. macOS and Windows require separate evidence and
decisions.

## Rejected

- **Zero-argument `nilket toron` as local mode:** starts storage under a
  compute-role name and makes role help dangerous.
- **In-process standalone:** violates the disaggregated role boundary and
  teaches tests the wrong physics.
- **Implicit defaults inside role config types:** contaminates production
  semantics with development custody and topology.
- **Random-port fallback:** makes Polariton discovery and restart behavior
  nondeterministic; occupation is an actionable refusal.
- **External OpenSSL orchestration:** adds a platform/tool dependency where
  the workspace already carries Rust PKI machinery.
- **Daemon mode in v1:** obscures ownership and failure while the product is
  still learning from foreground dogfooding.
- **Polariton auto-start:** process ownership and lifecycle UX remain diary-
  gated; a future visible offer can compose the command without changing it.

## Consequences

- The shortest local database command is one word: `nilket`.
- Explicit role deployments remain byte-for-byte grammatical descendants of
  ADR 040.
- Local startup becomes a durable, inspectable product path rather than a
  harness.
- The client/probe extraction removes duplicated protocol ownership before it
  can diverge across the monorepo.
- A future PRODUCT record may add the dependency-free `ket` router and typed
  startup profiles. It does not gate this decision and receives its number at
  acceptance, not by reservation.
