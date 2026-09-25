# ADR 0172: a macOS desktop runs Nilket in a Podman container

Status: proposed 2026-09-25. Platform lane; builds on ADR 0165 and ADR
0170. ADR 0170 made macOS a native desktop and kept the Nilket deployment
a Linux service; ADR 0165 gave native Polariton an authenticated path to a
Linux Toron, established from Windows against the Bigfoot lab. A macOS
desktop has no Linux Toron of its own to reach. Wendelon does not build on
macOS (`io-uring` is an unconditional dependency, storage assumes Linux
`O_DIRECT`), and `nilket local` refuses to run off Linux. This ADR puts the
Linux service beside the macOS desktop instead of porting it.

## Decision

### 1. Nilket stays a Linux service; the desktop hosts it in a container

A macOS desktop runs Wendelon and Toron as Linux processes inside a Podman
container, in the Linux VM Podman Machine provides. Nilket is not ported to
macOS: no macOS storage backend, no macOS workspace supervisor. Polariton
runs natively on the Mac and reaches the container through ADR 0165's
authenticated remote path, exactly as it reaches any other Linux Toron.

Podman is chosen because it is common on Linux, not only because it runs
on a Mac. The image and its entrypoint are therefore Linux artifacts that
assume nothing about macOS, and a Linux host running Podman runs them
unchanged. The seccomp and SELinux policy tooling is likewise reusable, but
the policy itself is generated and installed for each host, and the SELinux
part applies only where SELinux is enforced. What is macOS-specific is the
VM: installing policy into it, and carrying the published port across it
to the Mac.

### 2. This is a single-user development deployment

The container serves one person's desktop. It uses development-grade key
custody, as the `nilket local` profile and the Bigfoot lab do, and makes no
promise of production rotation, backup, or multi-user access. It is not
`nilket local` made remotely reachable: `nilket local` remains the
loopback-only Linux profile, and the container is a separate deployment
derived from it. Because it is a development deployment, it reuses the
local profile's development operator identity to create its tenant, rather
than issuing a separate one as ADR 0167 §2 asks of deployments beyond that
profile.

### 3. The container is reached only through the authenticated SQL listener

Toron's SQL listener is the one published port, bound on the Mac to
loopback and carried by QUIC over UDP. It runs with a `[tls]` section and
mutual TLS as ADR 0165 §2 requires; the loopback shortcut is never used
across the VM boundary. Wendelon's transport and every health, readiness,
and metrics listener stay inside the container.

### 4. Tenant 1 is provisioned and named consistently

The deployment serves a positive tenant, 1. Toron's node configuration,
its storage identity, and the Mac's client certificate
(`client-1.cluster.local`) all name tenant 1, and Wendelon holds a
committed, active tenant-1 record created by an operator command (ADR
0167). Configuration and certificates alone do not create the tenant.

### 5. Each side holds its own keys

The CA key, the listener key, the Wendelon, storage, and operator keys, and
the container's own tenant-1 client key for loading fixtures are generated
inside the container and never leave its volume. The Mac
generates the client key, keeps it at mode `0600` owned by the user (ADR
0165 §3), and sends only a certificate signing request into the container;
it receives back the client certificate and the CA certificate.

### 6. Restarts keep identity and data

Stopping and starting the container, or rebuilding its image, keeps the CA,
every issued identity, the worker id, the tenant, and Wendelon's data. A
normal start never mints a new CA or overwrites an identity; replacing them
is an explicit reset. Data lives in a Podman named volume inside the VM,
never on a Mac bind mount, whose direct-I/O and durability behavior is not
Wendelon's validated storage.

### 7. The container's privileges are the narrowest that work

Wendelon uses io_uring. The container runs under Podman's default seccomp
profile plus exactly `io_uring_setup`, `io_uring_enter`, and
`io_uring_register`, and under enforcing SELinux in a Nilket container
domain: the standard container domain plus what io_uring needs. The grant
belongs to that domain, not to every container in the VM. Unconfined
seccomp and `label=disable` are diagnostics, not the supported setup.

## Mechanisms in this cut

These may be replaced while the decision stands.

**Image side** (`packaging/podman/image/`): a Containerfile that builds the
`nilket` binary from the ket checkout in a Fedora build stage and ships it
in a Fedora runtime image as an unprivileged `nilket` user (UID/GID 1000).
An entrypoint provides:
- `init`: offline and idempotent; starts no process. Materializes a local
  profile under the volume root (`/var/lib/nilket`) with `nilket local
  init` (below), generates a worker id
  once, creates the deployment CA, listener identity
  (`toron-<worker-id>.cluster.local`), and tenant-1 storage identity, and
  rewrites the retained Toron config for tenant 1,
  `listener_bind = "0.0.0.0:7742"`, and `[tls]`. Toron accepts an
  unspecified bind address when `[tls]` is present. On an initialized
  volume it verifies the files and changes nothing.
- `run`: the default command, and the only supervisor. Runs `init`,
  starts `nilket wendelon`, waits for it, and ensures tenant 1: `tenant
  get`, then `tenant create` only when the tenant is absent, each with
  explicit `--bootstrap`, `--cert`, `--key`, and `--ca` for the operator
  identity. It stops with an error, starting nothing further, if the
  tenant is neither active nor creatable. It then starts `nilket toron
  start` and stops both on the container's stop signal. It does not use
  `nilket local` as the supervisor, whose readiness probe expects the
  loopback SQL listener this deployment replaces.
- `sign-client --tenant 1`: reads a CSR on standard input and writes the
  signed client certificate on standard output (`nilket toron pki
  sign-client`).
- `ca`: writes the CA certificate on standard output.
- `sql --tenant 1`: runs the SQL statements on standard input against the
  container's own authenticated listener, with a tenant-1 client identity
  held in the container for this purpose. `nilket toron sql` reads only a
  statement argument or `--file <path>` (it refuses `--file -`), so the
  verb copies standard input, up to a size bound, to a temporary file,
  passes it with `--file`, and removes it on every exit.
  It is how a fixture is loaded; `run` never loads one, because
  `uat/plot_trends_showcase.sql` begins by dropping its table.
- `status`: prints the worker id, server name, tenant, SQL port, and
  whether Toron is accepting SQL, in a stable key-value form the host side
  parses.

**Nilket side**: `nilket local init --data-dir DIR` writes the local
profile's configuration and development PKI exactly as `nilket local` does
before it starts anything, then exits. Today only `nilket local` reaches
that code (`write_profile_development_pki`, `render_local_profile_config`),
and it always goes on to start Wendelon and Toron. Starting `nilket local`
and killing it once the files appear is not an acceptable substitute: it
breaks `init`'s offline guarantee and can leave a partial start behind.

**Policy** (`packaging/podman/policy/`), for whichever Linux runs the
container:
- a generator that derives the seccomp profile from that Linux's current
  `/usr/share/containers/seccomp.json` plus the three io_uring syscalls;
- an SELinux module defining the Nilket container domain of §7, run with
  `--security-opt label=type:` that domain;
- a check that compares an installed profile with the current baseline and
  the module with the loaded policy, and runs a live io_uring probe in the
  Nilket domain.

**Host side** (`packaging/podman/macos/`):
- a script that installs the policy inside the VM, under `/etc`, at every
  setup: it runs the policy check and installs or regenerates what is
  missing or stale. Podman opens the seccomp profile path inside the VM,
  not on the Mac: a Mac path works only when the VM happens to share it
  (checked 2026-09-25, a path under `/private` opened and one under
  `/opt/homebrew` did not). Fedora CoreOS keeps `/etc` across restarts and
  updates. Recreating the machine drops the policy, and a base-policy
  update can stop an old module loading; the check reports either case;
- a setup script that checks the Podman and VM versions, sizes the VM,
  creates the named volume, and runs the container with the profile and
  `-p 127.0.0.1:7742:7742/udp`;
- a client script that creates the key (`0600`) and CSR under Polariton's
  per-user state directory (ADR 0170 §3), obtains the certificate and CA
  through `sign-client` and `ca`, and prints the five connection-form
  values: endpoint `127.0.0.1:7742`, server name, client certificate,
  client key, and CA. It refuses to overwrite an existing key, and detects
  a certificate whose tenant or CA no longer matches the container instead
  of printing a stale recipe.

**Docs**: a macOS guide to the above, and `docs/windows.md` corrected: ADR
0165's evidence established remote Toron from native Windows.

The environment validated on 2026-09-25 is Podman 6.1.2 with a Fedora
CoreOS VM (kernel 7.1.10, `io_uring_disabled = 0`) on Apple silicon.

## Acceptance

1. The Podman prerequisites hold under the supported setup, not the
   diagnostics of §7. With the §7 seccomp profile and the Nilket SELinux
   domain in enforcing mode, Wendelon starts and completes io_uring direct
   I/O on the named volume. Under the default seccomp profile io_uring
   setup fails (`ENOSYS`), and in the standard container domain under
   enforcing SELinux it fails (`EACCES`), so each part of §7 is shown to
   be needed. A second container in the standard domain still cannot
   create an io_uring instance.
2. On a fresh volume, `uat/plot_trends_showcase.sql` is loaded into tenant
   1 through the entrypoint's `sql` verb, as ADR 0167's evidence loaded it
   on Bigfoot. Native macOS Polariton then connects through its own
   connection form with the five values the client script printed, runs
   `SELECT period, SUM(value) AS value FROM trend_showcase GROUP BY period
   ORDER BY period`, and shows the eight weekly sums for periods 1 to 8:
   113, 121, 121, 136, 148, 147, 166, 179. Performed by the operator on
   the Mac; this is the first macOS proof of ADR 0165's path, which the
   Windows run establishes only as precedent. An automated `polariton act`
   run of the same query is also recorded for repeatability; it does not
   replace the form check.
3. On the Mac, a client certificate from a CA the listener does not trust
   is reported as the peer's rejection, and a server name the listener's
   certificate does not cover is reported as a server-name mismatch; the
   two reports are distinct (ADR 0165 acceptance 2).
4. A form with one to three of the four TLS fields is refused naming the
   missing fields before any transport, and a client key whose mode or
   owner fails ADR 0165 §3 is refused before any connection attempt.
5. After `podman stop` and `podman start`, after rebuilding the image and
   recreating the container on the same volume, and after restarting the
   Podman machine, the same client certificate connects and the same eight
   rows return, with no new CA issued and the tenant not created again.
   After a restart the setup script's checks pass without reinstalling the
   seccomp profile or the SELinux module.
6. Only UDP 7742 is published on the Mac, on loopback.
7. The VM memory floor is recorded from Nilket's measured peak during
   acceptance 2, and the setup script enforces it.
8. SQLite scratch and plotting still work in the same macOS build with no
   container running (ADR 0163 §2).
9. The Nilket change (`nilket local init`), the image side, and the host
   side land in separate implementation commits, each reviewed by the
   partner who did not write it, and the Nilket change passes its lane on
   Bigfoot.

## Limits and follow-up

One desktop, one tenant, one Toron worker, one Wendelon node. Polariton
does not start, stop, or observe the container; the operator runs the
scripts, and one-click lifecycle from Polariton is a separate plan. The
host side targets Podman Machine on Apple silicon; other runtimes (Docker
Desktop, OrbStack, Colima, Apple's `container`) are not covered.

Three follow-ups reuse this cut and are not decided here:
- **Podman on a Linux host.** Rootless Podman runs the image directly,
  with no VM to forward through: the container's port is published as
  `127.0.0.1:7742:7742/udp` on the host itself. The host needs the policy
  generated and installed by the `policy/` tooling (the SELinux module
  only on hosts that enforce SELinux) and a host script beside `macos/`.
  It needs its own acceptance run.
- **Podman on Windows.** WSL2 hosts the Linux side, and ADR 0165 already
  connects native Windows Polariton to a Linux Toron. It needs PowerShell
  host scripts and a live check that published UDP reaches the Windows
  host, which WSL2's localhost forwarding has not always done.
- **Kubernetes.** A production deployment, and its own ADR: identities
  from outside the pod, Wendelon and Toron as separate workloads, a
  per-node seccomp profile, volumes shown to support `O_DIRECT`, and QUIC
  reachable without TLS termination in between. Where a platform forbids
  io_uring, Wendelon would need a storage backend that does not use it,
  which is a separate storage decision.

Fedora CoreOS updates inside the VM can change its seccomp baseline or
SELinux policy; the host script re-verifies them at each setup, and a
failure there is reported, not worked around. Acceptance restarts the
machine but does not stage an OS update; the first real update is checked
when it arrives. ADR 0165's human form check on Windows remains pending
and is not established here. A native macOS Nilket is not ruled out
forever, but it needs its own storage and durability decision
(`F_FULLFSYNC`, no `O_DIRECT`) and is not planned.
