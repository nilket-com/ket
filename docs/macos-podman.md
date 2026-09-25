# Nilket beside native macOS Polariton

ADR 0172 runs Nilket in a Fedora CoreOS Podman Machine on Apple silicon.
Polariton remains a native Mac process and connects to Toron's authenticated
SQL listener over QUIC. The only published port is UDP 7742 on Mac loopback.
Wendelon's data and the deployment CA stay in a Podman named volume inside the
Linux VM.

## Prerequisites

Install Podman 6.1.2 or newer, Bash, jq, OpenSSL, and the Apple command line
tools.
Initialize a rootless Podman Machine if one does not exist:

```sh
podman machine init
```

The host script expects the default machine name, `podman-machine-default`,
Fedora CoreOS on aarch64, and SELinux in enforcing mode. It checks that the
VM permits io_uring and has enough memory. To use a different machine or
image, pass `--machine` or `--image` to the setup script.

From the Ket checkout, prepare the VM with enough memory for the Rust image
build, build the image, and start Nilket:

```sh
packaging/podman/macos/setup.sh --prepare-only --memory-mib 8192
packaging/podman/image/build
packaging/podman/macos/setup.sh
```

The 8192 MiB allocation is for compiling Nilket inside the arm64 VM; the
runtime memory floor is measured separately during ADR 0172 acceptance.

The setup script installs a seccomp profile derived from this VM's current
Podman baseline under `/etc/nilket-podman`. It also installs a Nilket-specific
SELinux container domain from the VM's udica base and network templates. Live
probes check io_uring and direct I/O, reject those operations in a standard
container and under baseline seccomp, and check Nilket's listener binds. The
script then creates the
`nilket-data` named volume and starts `nilket-dev` as UID/GID 1000. It waits
until the SQL listener is ready. The VM keeps the installed policy across
normal restarts; recreating the VM requires running setup again.

## Connect Polariton

```sh
packaging/podman/macos/client.sh
```

The script creates a Mac-owned private key at mode `0600` under
`~/Library/Application Support/polariton/nilket-podman`. Only its CSR enters
the container. The script verifies the returned tenant-1 certificate against
the container's current CA, checks that it matches the key, and prints the
five values for Polariton's Toron connection form: endpoint, server name,
client certificate, client key, and CA certificate. If the volume is reset
and the CA changes, the script refuses to print stale connection values.
Move the old client directory aside deliberately before issuing a new key.

To load the development trend fixture into tenant 1:

```sh
podman exec -i nilket-dev /usr/local/bin/nilket-container sql --tenant 1 < uat/plot_trends_showcase.sql
```

Load the fixture once on a fresh tenant. Re-running it on the retained volume
currently fails at `CREATE TABLE`, even though `DROP TABLE IF EXISTS` reports
OK. Use a fresh volume to reload the fixture.
In Polariton, run:

```sql
SELECT period, SUM(value) AS value
FROM trend_showcase
GROUP BY period
ORDER BY period
```

The expected eight values for periods 1 through 8 are 113, 121, 121, 136,
148, 147, 166, and 179.

## Restart and update

```sh
podman stop nilket-dev
podman start nilket-dev
packaging/podman/macos/setup.sh
```

After rebuilding the image, use `packaging/podman/macos/setup.sh
--recreate` to replace the container while keeping its named volume. Run
setup again after a Podman Machine restart or Fedora CoreOS update so it
checks the current seccomp baseline and loaded SELinux policy. The setup
script reports a policy failure before starting Nilket.

Inspect `podman logs nilket-dev` if the listener does not become ready.
The VM's disk is the only copy of the development CA and Wendelon data in
this setup; removing `nilket-data` discards both.
