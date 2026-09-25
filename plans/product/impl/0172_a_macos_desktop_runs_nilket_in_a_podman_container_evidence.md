# ADR 0172 implementation evidence

Status: implemented and accepted on Apple silicon macOS on 2026-09-25,
including the operator's connection through Polariton's form (acceptance 2).
The Fedora CoreOS update check remains due when an update arrives, as the
ADR's Limits require.

The implementation is split across the Nilket offline-init commit
`6892a9c0`, the arm64 `O_DIRECT` correction `76e284c7`, the Podman image
commit `dcdd9ee0`, and the host policy/scripts/docs commit `03287422`.
The latter is Bash and jq, with no Python tooling.

## Acceptance

1. **Established:** the Nilket SELinux domain and VM-derived seccomp profile
   passed the io_uring, direct-I/O, and fsync probes; each default-policy
   negative probe failed as expected.
2. **Established:** a fresh volume loaded 24 tenant-1 rows and returned
   all eight expected sums. Native macOS Polariton `act` returned those sums
   through the same connection operation the form uses. The operator then
   typed the five fields into Polariton's Toron connection form on the Mac,
   in a window with agents off, ran the cell, and confirmed the eight sums
   in the rendered preview.
3. **Established:** an untrusted client certificate and a wrong server name
   produced distinct authenticated-connection failures.
4. **Established:** partial TLS fields and a mode-0644 client key were
   refused before transport.
5. **Established:** stop/start, an image rebuild followed by container
   recreation, and a Podman Machine restart retained the CA, client
   identity, worker, tenant, and query result; policy persisted in the VM.
6. **Established:** only UDP 7742 was published on Mac loopback. gvproxy's
   other listener was the VM's TCP SSH port 53314.
7. **Established for this workload:** measured container `memory.peak` was
   97,390,592 bytes (92.9 MiB) after the query; setup enforces a 2048 MiB
   runtime VM floor. Image builds are a separate, larger memory workload.
8. **Established:** native SQLite and Rune plotting completed with Nilket
   stopped and no container running.
9. **Established:** separate, cross-reviewed commits landed for Nilket init
   (`6892a9c0`), the arm64 direct-I/O fix (`76e284c7`), the image
   (`dcdd9ee0`), and the host (`03287422`). On Bigfoot x86_64, Nilket's
   unit tests passed 16/16 and 2/2; CLI integration passed 23 tests plus
   all four fixed-port ignored tests. Formatting was clean and no new
   Clippy warnings were introduced. Wendelon's storage tests passed
   737/737, including the direct-I/O flag guard; formatting and Clippy
   checks were clean for `file_ops.rs`.

## Environment and mechanism

The Mac ran Podman 6.1.2 with a rootless Fedora CoreOS 44 arm64 machine,
kernel `7.1.10-200.fc44.aarch64`, six VM CPUs, and 6144 MiB VM memory for
the acceptance run. The guide recommends 8192 MiB for building the image.
The first build's fat-LTO, one-codegen-unit release link was OOM-killed at
about 5.25 GiB RSS in the 6 GiB VM. The development image deliberately
uses thin LTO and 16 codegen units, so its code generation differs from
the shipping release profile; that profile is unchanged. The first arm64
Wendelon bootstrap also found a hard-coded x86_64 `O_DIRECT = 0o40000`
that means `O_DIRECTORY` on aarch64. strace showed slab opens with
`O_RDWR|O_CREAT|O_DIRECTORY` failing `EINVAL`. This affected arm64 Linux
outside containers too; `libc::O_DIRECT` fixed it and kept the x86_64
value unchanged. The first runtime image also lacked `cmp`, which the
entrypoint uses to preserve an unchanged Toron configuration; adding
`diffutils` supplied it.
The running `nilket-dev` used `nilket-data` mounted at `/var/lib/nilket`,
UID/GID 1000, the VM-derived seccomp profile, and the
`nilket_container.process` SELinux domain in enforcing mode. Podman
reported only `7742/udp -> 127.0.0.1:7742` on the host.

The VM's udica `base_container.cil` lacks network permissions. The host
policy installs and checks both its base and network templates and gives
only Nilket's domain the additional io_uring anon-inode permissions. The
missing network template initially denied Wendelon's listener binds;
inheriting `net_container.cil` fixed them under enforcing SELinux. That
template adds `sandbox_net_domain`, the same network attribute Fedora's
standard `container_t` has; the Nilket domain adds only io_uring access.
The default seccomp profile still rejects io_uring with `ENOSYS`; with the
custom seccomp profile, the standard container domain rejects it with
`EACCES`. fio completed io_uring, `O_DIRECT`, and fsync-every-16 writes on
an XFS-backed named volume in the Nilket domain. Live bind probes passed
for Wendelon TCP and UDP 7001, TCP 7431–7433, Toron UDP 7742, and TCP and
UDP port zero. The setup check passed again after a VM restart without
reinstalling either policy module or the seccomp file.

## Tenant, identity, and native client

On an isolated fresh named volume, the current image initialized a new
worker and created active tenant 1 in about three seconds. The entrypoint's
`sql --tenant 1` loaded `uat/plot_trends_showcase.sql`: all three
statements succeeded and the insert reported 24 rows. Its query returned
period sums `113, 121, 121, 136, 148, 147, 166, 179`. That proof
container and proof volume were removed after review.

The principal `nilket-dev` deployment also returned those eight sums.
The Mac client script generated a Mac-owned P-256 key and sent only its
CSR to the container. macOS LibreSSL needed named-curve encoding:
without `ec_param_enc:named_curve`, its explicit-parameter CSR passed
OpenSSL verification but rcgen's ring verifier rejected it. With the
named-curve option, `sign-client --tenant 1` issued the certificate. The
script verified the CA, tenant-1 DNS name, certificate/key match, and
expiry; key, certificate, and CA files had mode `0600`. A second run kept
their inodes and modification times unchanged.

Native macOS Polariton release build opened the ADR 0168 `sums.md`
notebook with `--agents`. `polariton act` connected to
`127.0.0.1:7742` using the five values printed by the client script,
then ran the notebook's SQL cell. The completed receipt had columns
`period` (`i64`) and `value` (`f64`), eight rows with the sums above, and
`truncated: false`. This is the automated repeatability proof.

The operator check followed on 2026-09-25. A first look used a window with
`--agents`, where agent `connect` entered the values; that confirmed the
displayed result but not the form. The operator then opened a window with
agents off on a scratch copy of the same notebook, typed the five values
into Polariton's Toron connection form, ran the cell, and viewed the result
in the Markdown preview (`:mdpreview`). The sums matched. A cell's default
`output=inline` renders only in that preview, not in the raw source view,
which at first looked like a missing result.

The same native connect operation gave distinct failure receipts: a bad
server name said the listener certificate did not cover that name; a
client leaf signed by an untrusted test CA ended during the authenticated
handshake, with Toron logging `UnknownIssuer`; partial TLS fields were
refused before transport and named `server_name`, `client_key`, and
`ca_cert` as missing; a mode `0644` test key was refused before transport
with the expected `0600` mode named. The working client identity
reconnected afterward.

## Retention and independent scratch

After `podman stop`/`start`, after rebuilding the image and recreating the
container on the same volume, and after restarting the Podman machine,
setup checks passed,
the worker ID and Mac CA/client identities remained unchanged, and native
Polariton returned the same eight sums each time. The VM restart did not
reinstall policy.

With `nilket-dev` stopped and no container running, the same macOS
Polariton release build ran `uat/native_plot_showcase.md` through
`polariton act`: the two SQLite SQL cells returned six and eight rows,
and both Rune plotting cells completed with one figure each. Nilket was
then restarted and the authenticated Toron query passed again.

The container cgroup's highest observed `memory.peak` after the fixture
and native query was 97,390,592 bytes (about 92.9 MiB); the VM then
reported 732 MiB used. The setup script enforces a 2048 MiB runtime
floor, leaving room for Fedora CoreOS and Podman beyond that measured
Nilket workload. The 8192 MiB build recommendation is separate.

The fixture's `DROP TABLE IF EXISTS` reported OK on a second feed into
the retained tenant, but the following `CREATE TABLE` failed because the
table still existed; the original 24 rows remained queryable. The guide
therefore says to load the fixture once on a fresh tenant. This is a
suspected Toron defect for separate follow-up; it is outside the
first-load acceptance proof. A fresh volume is needed to reload this
fixture with the current behavior.
