# ADR 0166 implementation evidence

Status: implemented 2026-09-12, acceptance 4 partly pending (the cross-account
refusal awaits a second account). The record remains proposed.

Ket side. The live module (`windows`, `observe`, `act`, the per-window
advertisement) is no longer Unix-only. Discovery is platform-neutral: the
runtime directory is `$XDG_RUNTIME_DIR/polariton` on Unix and
`%LOCALAPPDATA%\polariton\run` on Windows, made private through
`key_guard::ensure_private_directory`; a window advertises a Unix socket
path or the named pipe `\\.\pipe\polariton-<window>`; a manifest is trusted
only when it is the running user's own. The transport is generic over the
platform stream: on Windows a named pipe created with an owner-only DACL,
the first instance reserving the name, remote clients rejected, and the
client's user checked on accept; the accept is cancel-safe. The client
verifies the pipe server's pid and process-start token against the
advertisement before sending a byte; the process-start token is one shared
implementation (`process_identity.rs`) for discovery and the reporting
spool. Three commands join the batch and live vocabularies: `connect`
(adapter selection plus the form's fields), `disconnect` (naming a database
connection generation), and `connection_status`. They enter the connection
operation the form uses (`gui/state/connection.rs`): the same validation,
the generation-gated settle, the same install; every receipt for them
carries the connection description with its generation. The Connect menu
gained Disconnect, the human form of `disconnect`. A `connect` command is judged as a
candidate (selection plus fields) before the form's state is touched, so
a refused command leaves the installed connection, the reported adapter,
and the generation as they were. Toron connect errors reported by quinn
in rustls' Display form are classified as the Debug form was, and the
classifier states a cause only where the error establishes one.

Nilket side. `key-guard` gained `ensure_private_directory` (Unix `0700`;
Windows a protected owner-only DACL that files inherit) and a `windows`
module: `current_user_only_security_descriptor` (the SDDL the pipe is
created with: a protected DACL admitting the running user alone, not
SYSTEM or Administrators), `pipe_client_is_current_user` (the client's
process token compared to the server's, not impersonation), and
`pipe_admits_current_user_only` (the descriptor of a pipe instance judged
by a pipe policy distinct from the private-key policy: owned by the
running user, every entry an allow entry for that user only). The
descriptor reader works from a handle so a pipe can be read back.

## Scope discovered during implementation

Six things the tests, the acceptance runs, and review found, each fixed
in this cut.

Minting a database connection generation on an adapter selection did not
retire the attempt in flight, so a connect result arriving after the
person's newer choice installed itself. The §3 test
`a_connect_result_delayed_past_a_newer_choice_does_not_install` caught it;
`next_connection_generation` now clears the in-flight marker, and the
local-workspace attach path goes through the same function.

The Windows pipe accept took the waiting instance before awaiting the
client. The subscription's `select!` drops a pending accept whenever a
session ends, so the instance was lost with it and every second `act`
against a window found no pipe. The accept now takes the instance only
after a client has connected; the pipe test abandons an accept mid-wait
and connects a third client.

quinn reports rustls errors in Display form (`certificate not valid for
name`, `unknown issuer`), not the Debug names the classifier matched, so a
server-name mismatch was reported as a certificate failing CA
verification. The classifier matches both forms. Review then found it overclaiming:
any `invalid peer certificate` became "not signed by the configured CA",
and a closed, reset, or lost connection during an authenticated handshake
became "the listener rejected this client's authentication". Only an
unknown issuer or a failed signature now yields "could not be verified
against the configured CA", retaining the underlying finding; any
other certificate finding is reported as rustls stated it; a connection
that ends during the handshake is reported as ending, with the listener's
rejection and its unavailability both named as possibilities.

Review found the `connect` command selecting the adapter and replacing
the fields before validating them. Selection leaves an installed
connection in place and a refusal mints no generation, so from a
connection to database A, a refused Toron candidate left SQL running
against A while status described Toron. The candidate is now judged
first, and the regression
`a_refused_connect_leaves_the_established_connection_and_its_report`
starts connected. In the same area, a remote endpoint with no TLS fields
is now refused before any transport rather than failing inside it.

Review found the pipe descriptor judged by the private-key policy, which
admits SYSTEM and Administrators, so the check did not verify the stricter
DACL §4 promises. The pipe is now created admitting the running user
alone and judged by its own policy.

The Connect menu snapshot test lists the new Disconnect item.

## Verification

Windows: this desktop, Windows 11 Pro, `x86_64-pc-windows-msvc`, debug
build, the plan commit `7b449c90` with this implementation in place (the
tree snapshot `bbef07e6`, pushed to Bigfoot as `ci/0166-3`). Linux:
Bigfoot, Ubuntu, 24 CPUs, Rust 1.98.1, the same snapshot checked out.

A first Linux pass on an earlier snapshot found three architecture checks
to update for the `toron`/`toron-local` split (the render-evidence module
mirror, the dogfood default feature, and a canonical local endpoint literal
that my test helpers had duplicated) and, because the Nilket lane was
running on the same box at a load average of 68, four deadline-based tests
timed out (three local-workspace signal fixtures, the git observer); each
passed alone on the quiet box. The Ket lane below is the rerun on the
final snapshot, alone, launched with default signal dispositions.

Acceptance 1. A window launched as a detached process with `--agents`
(no keystroke or pointer tooling exists in the repository; none was used)
and driven only with `polariton act <window> --commands <file>` against the
separate Toron instance on Bigfoot (`192.168.0.2:7742`, listener SAN
`toron-00000000-0000-0000-0000-000000000042.cluster.local`), with the
Windows-issued tenant-1 client identity in `%USERPROFILE%\.nilket\pki`.

- `connect` with all five fields: `completed`, outcome `applied`,
  connection `Toron`/`bigfoot-0165`, stage `connected`, generation 1;
  `connection_status` in the same script reported the same description.
- Partial TLS (endpoint and server name only): outcome `refused`, kind
  `connection_refused`, "needs all of server_name, client_cert, client_key,
  and ca_cert; missing: client_cert, client_key, ca_cert". No transport
  was attempted.
- Mismatched server name (`...000099`): outcome `failed`, kind
  `connection`, "server name mismatch: the listener's certificate does not
  cover toron-00000000-0000-0000-0000-000000000099.cluster.local";
  Toron logged the handshake aborted by the peer (alert 42).
- Untrusted client certificate (a certificate for `client-1` signed by a
  rogue CA, its key owner-only): outcome `failed`, kind `connection`, "the
  connection ended during the authenticated handshake (connection lost);
  the listener may have rejected this client's certificate, or it may be
  unavailable". Toron's log establishes what the client cannot: `invalid
  peer certificate: UnknownIssuer`.

The three are distinct and each is the text the form shows. The first two
state their cause because the client established it; the third keeps its
uncertainty because the client cannot tell a rejection from an outage.

Acceptance 2. In the same window after the failed attempts:
`connection_status` reported stage `configured`, generation 3, with the
last error; `disconnect` naming generation 1 was refused, "disconnect
names generation 1; the current connection is generation 3"; a second
authenticated `connect` completed as generation 4 and `disconnect` naming
4 was applied, leaving stage `configured`, generation 5. `disconnect`
naming the current generation with nothing installed was refused, "no
external connection to disconnect". From a connected SQLite (generation 1),
a `connect` naming Toron with a remote endpoint and no TLS fields was
refused, "Toron loopback client requires a loopback endpoint; use
authenticated mTLS remotely", and `connection_status` still reported
SQLite, `kept`, connected, generation 1. The delayed-result case is the §3
test below; it cannot be produced deterministically against a live
network.

Acceptance 3. `gui::state::connection::tests`:
`the_form_and_the_command_enter_the_same_connection_operation` (the form
path and `Command::Connect` on two applications from the same start
produce identical connection descriptions, the receipt equal to the
owner's state, the form minting one extra generation on selection),
`connection_status_and_a_stale_disconnect_are_generation_gated`,
`a_connect_result_delayed_past_a_newer_choice_does_not_install`, and
`a_partial_toron_tls_configuration_is_refused_before_any_transport`;
`driver::tests::product_0158_commands_and_refusals_are_structured` covers
the wire forms of the three commands. Windows: the full native Polariton
suite (`--no-default-features --features toron`) 1,223 passed, 0 failed,
25 ignored; the reduced head (`--no-default-features`) 946 passed, 0
failed, 12 ignored; the architecture tests 129 passed. Linux: the Ket lane
on Bigfoot, exit 0 with no failing target: `scripts/cargo-ctrl` 1,476
passed; architecture tests 129 passed; the default head 1,297 passed, 0
failed, 26 ignored; the reduced head 947 passed, 0 failed, 12 ignored; the
portable Toron head 1,226 passed, 0 failed, 25 ignored; `cargo fmt --check`
and all three `cargo check --all-targets` clean. Log:
`~/ci-logs/adr-0166-ket-3.log`. (The run before the review fixes, on
snapshot `dba66f8e`, had a single failure in the libaryon controlled test
`set_language_re_resolves_auto_theme`, a rust-analyzer semantic-reactor
panic in code this cut does not touch; it passed three reruns alone and a
rerun of the whole `cargo-ctrl` step, and did not recur.)

Acceptance 4, partly pending. `live::transport::tests::adr_0166_windows_pipe_admits_the_running_user_alone`:
the descriptor of the instance a client reached is judged by the pipe
policy (owned by the running user; the only allow entry is that user's),
a second first-instance creator of the name is refused, the same-user
client is a peer (`Accepted::Peer`, the variant the Unix socket produces;
another user's is `Accepted::Foreign` on both), the name survives a
session and an abandoned accept.
`live::client::tests::adr_0166_client_refuses_a_pipe_whose_server_is_not_the_advertised_process`:
a pipe under the window's name served by another incarnation of the pid,
or another pid, is refused with "pipe server is not the advertised window
process" before any byte; the advertised process is accepted. Pending: no
second Windows account exists on this desktop, so no foreign-account
connection has been attempted. The DACL read-back and the accept-time
token check are the mechanism, not proof; the cross-account half of this
acceptance stays open until an actual foreign-account client is refused
and the refusal is compared with the Unix socket's. The Nilket lane on
Bigfoot (`ci/product-check.sh nilket`, fmt, check, and the workspace
tests): exit 0, 183 targets, zero failures. Log:
`~/ci-logs/adr-0166-nilket-3.log`. Windows `key-guard` tests: 12 passed, including the pipe policy cases.

## Remaining distance to §1

Menu actions with no command yet, listed from the menu roots: profile
management (`connect:save-profile`, `connect:profiles`, and the per-profile
connect, edit, and delete), session checkpoint, close without checkpoint,
and copy id, `settings:preferences`, the agents gate toggle
(`plot:agents`), the three plot export actions (`publish` covers the
publication only), and `code:cancel` and `code:run-snippet` (`run_cell`
and `replace_cell` cover the notebook cells). ADR 0165's acceptance 1
completes through `connect` once tenant provisioning lands.
