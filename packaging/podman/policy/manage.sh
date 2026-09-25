#!/usr/bin/env bash
# Install/check the per-host Podman policy used by Nilket.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
action=${1:-}
if [[ $action != install && $action != check ]]; then
	printf 'usage: %s {install|check} [--machine NAME] [--probe-image IMAGE]\n' "$0" >&2
	exit 2
fi
shift
machine=
probe_image=localhost/nilket-policy-probe:44
while (($#)); do
	case $1 in
	--machine) machine=${2:?missing machine name}; shift 2 ;;
	--probe-image) probe_image=${2:?missing probe image}; shift 2 ;;
	*) printf 'unknown argument: %s\n' "$1" >&2; exit 2 ;;
	esac
done

policy_dir=/etc/nilket-podman
profile=$policy_dir/seccomp.json
base_module=$policy_dir/base_container.cil
net_module=$policy_dir/net_container.cil
nilket_module=$policy_dir/nilket_io_uring.cil
baseline_path=/usr/share/containers/seccomp.json
template_path=/usr/share/udica/templates/base_container.cil
net_template_path=/usr/share/udica/templates/net_container.cil
scratch=$(mktemp -d)
volume=
bind_container=
cleanup() {
	[[ -z $bind_container ]] || podman rm -f -t 0 "$bind_container" >/dev/null 2>&1 || true
	[[ -z $volume ]] || podman volume rm -f "$volume" >/dev/null 2>&1 || true
	rm -rf "$scratch"
}
trap cleanup EXIT

host() {
	if [[ -n $machine ]]; then
		podman machine ssh "$machine" "$@"
	else
		"$@"
	fi
}

# podman machine ssh joins command arguments before passing them to the VM.
# Quote a whole constant shell script as one remote argument.
host_shell() {
	local script=$1 quoted
	if [[ -n $machine ]]; then
		printf -v quoted '%q' "$script"
		podman machine ssh "$machine" sudo sh -c "$quoted"
	else
		sudo sh -c "$script"
	fi
}

extract_module() {
	local name=$1
	[[ $name == base_container || $name == net_container || $name == nilket_io_uring ]] || return 2
	host_shell "set -e; d=\$(mktemp -d); trap 'rm -rf \"\$d\"' EXIT; cd \"\$d\"; semodule -X 400 --cil -E $name >/dev/null 2>&1; cat $name.cil"
}

host_write() {
	local source=$1 destination=$2
	host sudo install -d -m 0755 "$policy_dir"
	host sudo tee "$destination.new" < "$source" >/dev/null
	host sudo chmod 0644 "$destination.new"
	host sudo mv "$destination.new" "$destination"
}

host cat "$baseline_path" > "$scratch/baseline.json"
"$here/seccomp.sh" "$scratch/baseline.json" "$scratch/seccomp.json"
selinux=$(host getenforce 2>/dev/null || printf 'Disabled')
if [[ -n $machine && $selinux != Enforcing ]]; then
	printf 'Podman Machine must have SELinux enforcing (found %s)\n' "$selinux" >&2
	exit 1
fi
if [[ $selinux == Permissive ]]; then
	printf 'SELinux is permissive; use enforcing mode or disable it on this Linux host\n' >&2
	exit 1
fi
if [[ $selinux == Enforcing ]]; then
	host cat "$template_path" > "$scratch/base_container.cil"
	host cat "$net_template_path" > "$scratch/net_container.cil"
fi

if [[ $action == install ]]; then
	if ! host cat "$profile" 2>/dev/null | cmp -s - "$scratch/seccomp.json"; then
		host_write "$scratch/seccomp.json" "$profile"
		printf 'installed %s\n' "$profile"
	fi
	if [[ $selinux == Enforcing ]]; then
		if ! extract_module base_container 2>/dev/null | cmp -s - "$scratch/base_container.cil" ||
		   ! extract_module net_container 2>/dev/null | cmp -s - "$scratch/net_container.cil" ||
		   ! extract_module nilket_io_uring 2>/dev/null | cmp -s - "$here/nilket_io_uring.cil"; then
			host_write "$scratch/base_container.cil" "$base_module"
			host_write "$scratch/net_container.cil" "$net_module"
			host_write "$here/nilket_io_uring.cil" "$nilket_module"
			host sudo semodule -X 400 -i "$base_module" "$net_module" "$nilket_module"
			printf 'installed udica base, network, and Nilket SELinux modules\n'
		fi
	fi
fi

host cat "$profile" | cmp -s - "$scratch/seccomp.json" || {
	printf 'installed seccomp profile differs from the current baseline\n' >&2
	exit 1
}
if [[ $selinux == Enforcing ]]; then
	extract_module base_container | cmp -s - "$scratch/base_container.cil" || {
		printf 'loaded udica base module differs from the current template\n' >&2
		exit 1
	}
	extract_module net_container | cmp -s - "$scratch/net_container.cil" || {
		printf 'loaded udica network module differs from the current template\n' >&2
		exit 1
	}
	extract_module nilket_io_uring | cmp -s - "$here/nilket_io_uring.cil" || {
		printf 'loaded Nilket SELinux module differs from the source\n' >&2
		exit 1
	}
fi

if ! podman image exists "$probe_image"; then
	if [[ $probe_image != localhost/nilket-policy-probe:44 ]]; then
		printf 'probe image is missing: %s\n' "$probe_image" >&2
		exit 1
	fi
	podman build -q -f "$here/Containerfile.probe" -t "$probe_image" "$here" >/dev/null
fi

volume="nilket-policy-probe-$$"
podman volume create "$volume" >/dev/null
fio_probe() {
	local label=$1 seccomp=$2 filename=$3
	local -a options=(--rm --user 1000:1000 --volume "$volume:/data:Z,U")
	[[ -z $label ]] || options+=(--security-opt "label=type:$label")
	[[ -z $seccomp ]] || options+=(--security-opt "seccomp=$seccomp")
	podman run "${options[@]}" "$probe_image" fio \
		--name=nilket-policy --filename="/data/$filename" --rw=randwrite \
		--bs=4k --size=16m --ioengine=io_uring --direct=1 \
		--iodepth=8 --fsync=16 --runtime=2 --time_based=1 \
		--numjobs=1 --group_reporting=1 --output-format=json
}

label=
[[ $selinux != Enforcing ]] || label=nilket_container.process
fio_probe "$label" "$profile" allowed > "$scratch/allowed.json"
jq -e '.jobs[0].error == 0 and .jobs[0].write.io_bytes > 0' "$scratch/allowed.json" >/dev/null
if [[ $selinux == Enforcing ]]; then
	if fio_probe "" "$profile" standard > "$scratch/standard.txt" 2>&1; then
		printf 'standard container unexpectedly used io_uring\n' >&2
		exit 1
	fi
	grep -q 'err=13/' "$scratch/standard.txt" || {
		printf 'standard container did not fail with EACCES:\n' >&2
		cat "$scratch/standard.txt" >&2
		exit 1
	}
fi
if fio_probe "$label" "" baseline > "$scratch/baseline.txt" 2>&1; then
	printf 'baseline seccomp unexpectedly allowed io_uring\n' >&2
	exit 1
fi
grep -q 'err=38/' "$scratch/baseline.txt" || {
	printf 'baseline seccomp did not fail with ENOSYS:\n' >&2
	cat "$scratch/baseline.txt" >&2
	exit 1
}

# udica's base template lacks socket bind permissions. Check every fixed
# Nilket listener and loopback port zero, which client sockets also use.
if [[ $selinux == Enforcing ]]; then
	for spec in 'tcp 7001' 'udp 7001' 'tcp 7431' 'tcp 7432' 'tcp 7433' 'udp 7742' 'tcp 0' 'udp 0'; do
		read -r protocol port <<< "$spec"
		bind_container="nilket-policy-bind-$$-$protocol-$port"
		ncat_args=(-4 -l -n)
		[[ $protocol == udp ]] && ncat_args+=(-u) || ncat_args+=(-k)
		podman run -d --name "$bind_container" \
			--security-opt label=type:nilket_container.process \
			--security-opt "seccomp=$profile" \
			"$probe_image" ncat "${ncat_args[@]}" 127.0.0.1 "$port" >/dev/null
		sleep 0.2
		if [[ $(podman inspect --format '{{.State.Running}}' "$bind_container") != true ]]; then
			podman logs "$bind_container" >&2 || true
			printf 'Nilket domain cannot bind %s port %s\n' "$protocol" "$port" >&2
			exit 1
		fi
		podman rm -f -t 0 "$bind_container" >/dev/null
		bind_container=
	done
fi
printf 'seccomp, SELinux, direct-I/O io_uring, and listener-bind probes passed\n'
