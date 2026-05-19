# Brainstorm 005: Unified Nilket CLI Entrypoint

**Date:** 2026-05-19
**Context:** Operator-facing surface of the storage/compute wedge —
"one algebraic ring over various sets of hardware" (cf. ADR 039 D14
disjoint-hardware framing; brainstorm 003 disaggregated compute
roles).

## The Concept

A unified entrypoint for the entire distributed database product:

```bash
> nilket wendelon start --join-ring=...
> nilket toron start    --join-ring=...
> nilket query "SELECT ... FROM ..."
> nilket inspect ring   --ring=...
```

Rather than shipping disjoint binaries (`toron-server`,
`wendelon-node`, `toron-cli`, `wendelon-admin`), we ship a single
`nilket` multiplexer binary. The first positional argument selects
the **ring role** (`wendelon` / `toron`) or **client verb**
(`query`, `inspect`); the second positional argument is the action
within that role.

## Subcommand shape

Three families of subcommands, mirroring kubectl's `<resource>
<verb>` ergonomics:

| Family | Examples | Role |
|---|---|---|
| **Ring roles** | `nilket wendelon start \| stop \| status` | Boots a node into a specific ring role. The process runs as a long-lived daemon. |
| **Client verbs** | `nilket query "SELECT ..."`, `nilket sql -f script.sql` | Connects to an existing cluster as a client; no node provisioned. |
| **Diagnostic verbs** | `nilket inspect ring`, `nilket inspect table foo`, `nilket diag dump` | Read-only introspection over an existing cluster. |

The exact verb list is open; the convention is `nilket <noun>
<verb>` for ring-role subcommands and `nilket <verb> <args>` for
client/diagnostic subcommands.

## Architectural Implications

1. **Explicit Hardware Roles.** The operator explicitly defines the
   role of the hardware at boot time. `nilket wendelon start`
   provisions the hardware as a stateful, consensus-participating
   storage node. `nilket toron start` provisions the hardware as a
   stateless, query-planning compute node.

2. **One Algebraic Ring.** Both subcommands join the same logical
   cluster ring (`--join-ring=...`), but their runtime behaviors,
   memory constraints, and network profiles are totally disjoint.
   This is the operator-surface counterpart of brainstorm 003's
   "Strong Ring (Wendelon) / Weak Ring (Toron-Only)" algebra.

3. **Strict Decoupling — Anti-Pattern Guard.** Packaging the
   roles together conceptually enforces the rule that they
   communicate exclusively over the network. `nilket toron` does
   **not** spawn a Wendelon state machine in-process, even when
   convenient (e.g., for tests). The binary is one shipping
   artifact; the running processes are still disjoint. This
   mirrors the ADR 039 D14 boundary at the OS-process layer: Toron
   cannot peek into Wendelon's process state, because a `nilket
   toron` process physically does not host the Wendelon state
   machine.

4. **Operational Simplicity.** Deployments, Kubernetes operators,
   and diagnostic tools only need to manage and understand one
   artifact. Image size grows modestly (two daemon binaries' worth
   of code in one image), but ops surface area shrinks
   significantly.

5. **Single Version Coordinate.** A unified binary means
   Toron and Wendelon ship with a single version coordinate at any
   given moment. This is a feature (rolling Toron and Wendelon
   together avoids the protocol-skew matrix) and a constraint
   (independent crate releases lose flexibility). The crate
   workspace stays split internally; the public shipping
   artifact stays unified. Cluster upgrades remain full-fleet
   (already established by ADR 008 rolling-mixed-version reject).

## Refinements

### `nilket standalone` for developer laptops — two processes, not one

A `nilket standalone` (or `nilket local`) subcommand is tempting for
developer laptops: one command, both rings running locally, no ring
membership ceremony. But the architectural argument behind ADR 039
D14 + the disjoint-hardware framing says compute and storage are
disjoint over the network — and a single in-process daemon would
silently violate that.

The right shape: `nilket standalone` spawns **two child processes**
(`nilket wendelon start` + `nilket toron start`) connected via
localhost loopback, not a single in-process daemon. Developers get
one-command convenience; the architectural invariant is preserved.
A future operator who refactors Toron to "peek into Wendelon's
in-process state" because they tested locally with `standalone`
discovers the invariant violation at the first cloud deployment.

### Diagnostic surface lives on `nilket inspect` / `nilket diag`

Brainstorm 002 (weak control ring) and the existing observability
work imply diagnostic queries against ring state — gossip, lease,
descriptor caches, raft state, etc. These are read-only and
naturally belong as client-verb subcommands (`nilket inspect
...`), connected to an existing cluster over the network. No
direct in-process introspection from `nilket inspect` into a
running `nilket wendelon` process; the diagnostic surface is the
same RPC surface a third-party tool would use.

### Shared configuration scope

A single binary suggests shared configuration concepts (TLS certs,
ring membership secrets, log/metrics endpoints), but the
configuration *files* per role should remain role-specific. A
`nilket wendelon` process reading a `wendelon.toml`; a `nilket
toron` process reading a `toron.toml`. The shared concept is
**discovery** (`--join-ring=...`) and **identity** (cluster CA,
node identity). Per-role tuning (LSM cache sizes for Wendelon,
hash-table memory caps for Toron) stays role-scoped.

### Subcommand naming pin

`nilket wendelon start` (not `nilket-wendelon start`, not `nilket
start wendelon`). The kubectl precedent (`kubectl <noun> <verb>`)
makes shell history readable and tab-completion natural. Client
verbs go on the top-level (`nilket query`) because they don't
target a specific ring role — they target the cluster.

## Future Exploration

- **How does the `nilket` binary orchestrate shared configuration**
  (TLS certs, ring membership secrets) across both ring-role
  subcommands? Discovery + identity shared; per-role tuning
  role-scoped (above).

- **Should `nilket standalone` exist at all,** or do we expect
  developers to run `nilket wendelon start` + `nilket toron start`
  in two terminal panes? Argument for `standalone`: lower friction
  for first-time contributors. Argument against: erodes the
  two-process invariant if implemented carelessly. **If it
  exists, it must spawn two children, not run one in-process
  daemon.**

- **`nilket query`'s connection model.** Does it cache a cluster
  endpoint (like `kubectl`'s kubeconfig), or require explicit
  `--host` every time? Convention: cluster context file at
  `~/.nilket/contexts.toml`, with `nilket context use ...` to
  switch.

- **Cross-cutting concerns** like logging, metrics emission, and
  panic handling: Toron and Wendelon have different observability
  needs but now share a binary. Likely a shared `nilket-common`
  internal crate with role-conditional config. Worth a separate
  brainstorm if the surface grows.

- **`nilket` as a distribution artifact** — single statically-
  linked binary per arch / per OS, or Docker image as primary
  delivery? Single static binary keeps the "one artifact" claim
  honest; Docker image is downstream.

## Relation to other brainstorms

- **003 Disaggregated compute roles** — the architectural argument
  for separating Toron and Wendelon at the role level. 005 is its
  operator-facing CLI surface.
- **002 Weak control ring** — the runtime infrastructure that lets
  `nilket toron` participate in the ring without holding durable
  state. 005 surfaces this at the CLI as the
  `nilket toron start --join-ring=...` ergonomic.
- **ADR 039 D14** — the same boundary memorialized at the Rust
  trait surface (Wendelon admin choreography lives inside
  Wendelon; Toron makes one RPC). 005 expresses the boundary at
  the OS-process surface (Toron cannot host Wendelon's state
  machine because the process doesn't include it).
