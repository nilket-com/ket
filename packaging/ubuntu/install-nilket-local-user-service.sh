#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT_TEMPLATE="$SCRIPT_DIR/nilket-local.service.in"
UNIT_NAME="nilket-local.service"

nilket_executable="${NILKET_EXECUTABLE:-}"
profile_root="${NILKET_PROFILE_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/nilket/local}"
unit_dir="${NILKET_SYSTEMD_USER_DIR:-${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user}"
enable_service=1
enable_linger=0
force=0
service_user="${USER:-$(id -un)}"

usage() {
	cat <<'EOF'
Usage: packaging/ubuntu/install-nilket-local-user-service.sh [OPTIONS]

Install and start the user-owned nilket-local systemd service.

Options:
  --nilket PATH       Nilket executable (default: PATH, then ~/.cargo/bin/nilket).
  --profile-root PATH Local profile root (default: XDG data home/nilket/local).
  --unit-dir PATH     User unit directory (default: XDG config home/systemd/user).
  --install-only      Render and verify the unit without invoking systemctl.
  --enable-linger     Ask loginctl to start this user's manager at machine boot.
  --force             Replace an existing unit whose contents differ.
  -h, --help          Show this help.

Environment equivalents:
  NILKET_EXECUTABLE, NILKET_PROFILE_ROOT, NILKET_SYSTEMD_USER_DIR

The installer never invokes sudo. If loginctl needs administrator approval,
run `sudo loginctl enable-linger "$USER"` separately.
EOF
}

while (($# > 0)); do
	case "$1" in
		--nilket)
			[[ $# -ge 2 ]] || {
				echo "--nilket requires PATH" >&2
				exit 2
			}
			nilket_executable="$2"
			shift 2
			;;
		--profile-root)
			[[ $# -ge 2 ]] || {
				echo "--profile-root requires PATH" >&2
				exit 2
			}
			profile_root="$2"
			shift 2
			;;
		--unit-dir)
			[[ $# -ge 2 ]] || {
				echo "--unit-dir requires PATH" >&2
				exit 2
			}
			unit_dir="$2"
			shift 2
			;;
		--install-only)
			enable_service=0
			shift
			;;
		--enable-linger)
			enable_linger=1
			shift
			;;
		--force)
			force=1
			shift
			;;
		-h | --help)
			usage
			exit 0
			;;
		*)
			echo "unknown option: $1" >&2
			usage >&2
			exit 2
			;;
	esac
done

[[ "$(uname -s)" == "Linux" ]] || {
	echo "nilket-local user service installation currently supports Linux only" >&2
	exit 1
}
[[ -r "$UNIT_TEMPLATE" ]] || {
	echo "unit template is unavailable: $UNIT_TEMPLATE" >&2
	exit 1
}

if [[ -z "$nilket_executable" ]]; then
	nilket_executable="$(command -v nilket 2>/dev/null || true)"
	if [[ -z "$nilket_executable" && -x "$HOME/.cargo/bin/nilket" ]]; then
		nilket_executable="$HOME/.cargo/bin/nilket"
	fi
fi
[[ -n "$nilket_executable" ]] || {
	echo "nilket executable not found; install it or pass --nilket PATH" >&2
	exit 1
}
nilket_executable="$(realpath -e "$nilket_executable")"
[[ -f "$nilket_executable" && -x "$nilket_executable" ]] || {
	echo "nilket executable is not an executable regular file: $nilket_executable" >&2
	exit 1
}
for path_value in "$nilket_executable" "$profile_root" "$unit_dir"; do
	[[ "$path_value" != *$'\n'* && "$path_value" != *$'\r'* ]] || {
		echo "service paths must not contain line breaks" >&2
		exit 1
	}
done
[[ "$profile_root" == /* ]] || {
	echo "profile root must be absolute: $profile_root" >&2
	exit 1
}
[[ "$unit_dir" == /* ]] || {
	echo "user unit directory must be absolute: $unit_dir" >&2
	exit 1
}
if ((enable_service == 0 && enable_linger == 1)); then
	echo "--enable-linger cannot be combined with --install-only" >&2
	exit 2
fi

systemd_quote() {
	local value="$1"
	value="${value//\\/\\\\}"
	value="${value//\"/\\\"}"
	value="${value//%/%%}"
	value="${value//\$/\$\$}"
	printf '"%s"' "$value"
}

render_unit() {
	local line
	local marker_count=0
	local quoted_executable
	local quoted_profile
	quoted_executable="$(systemd_quote "$nilket_executable")"
	quoted_profile="$(systemd_quote "$profile_root")"
	while IFS= read -r line || [[ -n "$line" ]]; do
		if [[ "$line" == "ExecStart=@NILKET_EXECUTABLE@ local --data-dir @PROFILE_ROOT@" ]]; then
			printf 'ExecStart=%s local --data-dir %s\n' "$quoted_executable" "$quoted_profile"
			marker_count=$((marker_count + 1))
		else
			printf '%s\n' "$line"
		fi
	done <"$UNIT_TEMPLATE"
	((marker_count == 1)) || {
		echo "unit template must contain exactly one ExecStart marker" >&2
		return 1
	}
}

install -d -m 700 "$unit_dir"
unit_path="$unit_dir/$UNIT_NAME"
temporary="$(mktemp "$unit_dir/nilket-local-install-XXXXXX.service")"
trap 'rm -f "$temporary"' EXIT
render_unit >"$temporary"
chmod 600 "$temporary"

if command -v systemd-analyze >/dev/null 2>&1; then
	systemd-analyze verify "$temporary"
fi

changed=1
if [[ -e "$unit_path" ]]; then
	[[ -f "$unit_path" && ! -L "$unit_path" ]] || {
		echo "refusing non-regular existing unit: $unit_path" >&2
		exit 1
	}
	if cmp -s "$temporary" "$unit_path"; then
		changed=0
	elif ((force == 0)); then
		echo "existing unit differs; inspect it or rerun with --force: $unit_path" >&2
		exit 1
	fi
fi
if ((changed == 1)); then
	mv -f "$temporary" "$unit_path"
	chmod 600 "$unit_path"
else
	rm -f "$temporary"
fi
trap - EXIT

echo "installed $unit_path"
if ((enable_service == 0)); then
	echo "install-only: systemctl was not invoked"
	exit 0
fi

command -v systemctl >/dev/null 2>&1 || {
	echo "systemctl is required to enable the user service" >&2
	exit 1
}
systemctl --user daemon-reload
systemctl --user enable "$UNIT_NAME"
systemctl --user restart "$UNIT_NAME"

if ((enable_linger == 1)); then
	command -v loginctl >/dev/null 2>&1 || {
		echo "loginctl is required for --enable-linger" >&2
		exit 1
	}
	loginctl enable-linger "$service_user"
elif command -v loginctl >/dev/null 2>&1 \
	&& [[ "$(loginctl show-user "$service_user" --property=Linger --value 2>/dev/null || true)" != "yes" ]]; then
	echo "note: service starts with your user manager, but boot-before-login is not enabled"
	echo "      run: sudo loginctl enable-linger \"$service_user\""
fi

echo "started $UNIT_NAME"
echo "logs: journalctl --user -u $UNIT_NAME -f"
