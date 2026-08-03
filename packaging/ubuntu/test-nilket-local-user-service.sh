#!/usr/bin/env bash
set -euo pipefail

[[ "$(uname -s)" == "Linux" ]] || exit 0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALLER="$SCRIPT_DIR/install-nilket-local-user-service.sh"
fixture="$(mktemp -d)"
trap 'rm -rf -- "$fixture"' EXIT

install -d "$fixture/bin" "$fixture/units" "$fixture/profile with spaces and %percent"
install -m 755 /bin/true "$fixture/bin/nilket"
install -m 755 /bin/true "$fixture/bin/systemctl"
install -m 755 /bin/true "$fixture/bin/loginctl"

installer=(
	"$INSTALLER"
	--nilket "$fixture/bin/nilket"
	--profile-root "$fixture/profile with spaces and %percent"
	--unit-dir "$fixture/units"
)
mock_path="$fixture/bin:/usr/bin:/bin"

PATH="$mock_path" "${installer[@]}" --enable-linger
unit="$fixture/units/nilket-local.service"
grep -Fqx "ExecStart=\"$fixture/bin/nilket\" local --data-dir \"$fixture/profile with spaces and %%percent\"" "$unit"
for directive in \
	"StartLimitIntervalSec=60s" \
	"StartLimitBurst=5" \
	"Type=exec" \
	"Restart=on-failure" \
	"KillSignal=SIGINT" \
	"KillMode=mixed" \
	"TimeoutStopSec=90s" \
	"WantedBy=default.target"; do
	grep -Fqx "$directive" "$unit"
done

PATH="$mock_path" "${installer[@]}" --install-only
sed -i 's/RestartSec=5s/RestartSec=7s/' "$unit"
if PATH="$mock_path" "${installer[@]}" --install-only; then
	echo "modified-unit refusal did not fire" >&2
	exit 1
fi
PATH="$mock_path" "${installer[@]}" --install-only --force
grep -Fqx "RestartSec=5s" "$unit"

echo "nilket-local Ubuntu user-service installer: PASS"
