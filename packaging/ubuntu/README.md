# Ubuntu local service

The local zero-scale deployment is one user service: `nilket local`. Nilket
remains the sole supervisor for Wendelon and Toron, including profile ownership,
PKI generation, readiness, failure propagation, and the ordered Toron-before-
Wendelon drain. Separate role units belong to a future distributed deployment,
not to the generated local profile.

## Install

From the repository root, install the current Nilket binary first:

```bash
CARGO_TARGET_DIR="$PWD/nilket/target" \
  cargo install --locked --force --path nilket/crates/nilket
```

Then install and start the user unit:

```bash
packaging/ubuntu/install-nilket-local-user-service.sh
```

Stop any terminal- or Polariton-owned local deployment first; the profile lock
and fixed local ports intentionally prevent two supervisors from coexisting.

The default service starts with the user's systemd manager. To start it at
machine boot before login, explicitly request lingering:

```bash
packaging/ubuntu/install-nilket-local-user-service.sh --enable-linger
```

Lingering runs the local workspace unattended from boot while it still uses the
`dev-insecure` development custody path. This remains a development-only
deployment and is not a production custody boundary.

The installer never invokes `sudo`. If policy does not authorize `loginctl`, run
this separately:

```bash
sudo loginctl enable-linger "$USER"
```

Useful commands:

```bash
systemctl --user status nilket-local.service
systemctl --user restart nilket-local.service
systemctl --user stop nilket-local.service
journalctl --user -u nilket-local.service -f
```

Persistent startup failures are bounded to five attempts in one minute, after
which the unit stays visibly failed. After correcting the cause, clear that
state and start it again:

```bash
systemctl --user reset-failed nilket-local.service
systemctl --user start nilket-local.service
```

Polariton discovers and attaches to a running service without acquiring stop
authority. While an enabled unit is deliberately stopped or between restart
attempts, do not accept Polariton's session-owned creation offer; persistent
service preference is a later product decision.

## Installer controls

The installer accepts `--nilket PATH`, `--profile-root PATH`, `--unit-dir PATH`,
`--install-only`, and `--force`. Environment equivalents are documented by
`--help`. Existing modified units are refused unless replacement is explicitly
authorized with `--force`.

`nilket-local.service.in` is the canonical unit. In particular,
`KillSignal=SIGINT` enters Nilket's ordered drain, while `KillMode=mixed` keeps
systemd from broadcasting that first signal to both roles. The 90-second stop
deadline sits above Nilket's two 35-second role deadlines and leaves systemd as
the final orphan backstop. `StartLimitIntervalSec=60s` and
`StartLimitBurst=5` turn recurring startup failures into a terminal, inspectable
state instead of indefinite restart flapping.

No Debian package is required for this user-local path. A future machine-owned
package may install a system unit and binary under `/usr`; that is a distinct
distribution and custody posture.
