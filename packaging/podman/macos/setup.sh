#!/usr/bin/env bash
# Prepare Podman Machine and run the Nilket development container on macOS.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
machine=podman-machine-default
image=localhost/nilket:dev
memory_mib=2048 # Recheck against the measured peak in ADR 0172 acceptance 7.
probe_image=
recreate=no
prepare_only=no
while (($#)); do
	case $1 in
	--machine) machine=${2:?missing machine}; shift 2 ;;
	--image) image=${2:?missing image}; shift 2 ;;
	--memory-mib) memory_mib=${2:?missing memory size}; shift 2 ;;
	--probe-image) probe_image=${2:?missing probe image}; shift 2 ;;
	--recreate) recreate=yes; shift ;;
	--prepare-only) prepare_only=yes; shift ;;
	*) printf 'unknown argument: %s\n' "$1" >&2; exit 2 ;;
	esac
done

[[ $(uname -s) == Darwin && $(uname -m) == arm64 ]] || {
	printf 'setup.sh supports Apple silicon macOS\n' >&2
	exit 1
}
[[ $memory_mib =~ ^[0-9]+$ ]] && ((memory_mib >= 2048)) || {
	printf 'Nilket requires at least 2048 MiB in Podman Machine\n' >&2
	exit 1
}
version=$(podman --version)
[[ $version =~ podman[[:space:]]version[[:space:]]([0-9]+)\.([0-9]+)\.([0-9]+) ]] || {
	printf 'cannot parse Podman version: %s\n' "$version" >&2
	exit 1
}
major=${BASH_REMATCH[1]} minor=${BASH_REMATCH[2]} patch=${BASH_REMATCH[3]}
if ((major < 6 || (major == 6 && minor < 1) || (major == 6 && minor == 1 && patch < 2))); then
	printf 'Podman 6.1.2 or newer is required; found %s\n' "$version" >&2
	exit 1
fi

inspect=$(podman machine inspect "$machine" --format '{{json .}}')
state=$(jq -er '.State' <<< "$inspect")
current_mib=$(jq -er '.Resources.Memory' <<< "$inspect")
if ((current_mib < memory_mib)); then
	[[ $state == stopped ]] || {
		printf '%s has %s MiB; stop it before resizing to %s MiB\n' "$machine" "$current_mib" "$memory_mib" >&2
		exit 1
	}
	podman machine set --memory "$memory_mib" "$machine"
	printf 'sized %s to %s MiB\n' "$machine" "$memory_mib"
fi
case $state in
stopped) podman machine start "$machine" ;;
running) ;;
*) printf '%s is %s\n' "$machine" "$state" >&2; exit 1 ;;
esac
os_release=$(podman machine ssh "$machine" cat /etc/os-release)
[[ $os_release == *'PRETTY_NAME="Fedora CoreOS '* ]] || {
	printf 'expected a Fedora CoreOS Podman Machine\n' >&2
	exit 1
}
[[ $(podman machine ssh "$machine" uname -m) == aarch64 ]] || {
	printf 'expected an aarch64 Podman Machine\n' >&2
	exit 1
}
[[ $(podman machine ssh "$machine" cat /proc/sys/kernel/io_uring_disabled) == 0 ]] || {
	printf 'Podman Machine has io_uring disabled\n' >&2
	exit 1
}

policy_args=(install --machine "$machine")
[[ -z $probe_image ]] || policy_args+=(--probe-image "$probe_image")
policy_output=$("$here/../policy/manage.sh" "${policy_args[@]}")
printf '%s\n' "$policy_output"
if grep -q '^installed ' <<< "$policy_output"; then
	recreate=yes # OCI seccomp settings are fixed when a container is created.
fi
if [[ $prepare_only == yes ]]; then
	printf '%s is ready for the Nilket image build\n' "$machine"
	exit 0
fi

container=nilket-dev
if podman container exists "$container"; then
	if [[ $recreate == yes ]]; then
		podman rm -f "$container"
	else
		[[ $(podman inspect --format '{{.State.Running}}' "$container") == true ]] ||
			podman start "$container"
	fi
fi
if ! podman container exists "$container"; then
	podman volume create --ignore nilket-data >/dev/null
	podman run -d --name "$container" --user 1000:1000 \
		--stop-timeout 80 \
		--volume nilket-data:/var/lib/nilket:Z,U \
		--security-opt seccomp=/etc/nilket-podman/seccomp.json \
		--security-opt label=type:nilket_container.process \
		--publish 127.0.0.1:7742:7742/udp "$image" run >/dev/null
fi

for ((attempt = 0; attempt < 120; attempt++)); do
	if status=$(podman exec "$container" /usr/local/bin/nilket-container status 2>/dev/null) &&
	   grep -qx 'sql_ready=yes' <<< "$status"; then
		printf '%s\n%s is ready on 127.0.0.1:7742/udp\n' "$status" "$container"
		exit 0
	fi
	if [[ $(podman inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true) != true ]]; then
		podman logs --tail 40 "$container" >&2 || true
		printf '%s exited before SQL became ready\n' "$container" >&2
		exit 1
	fi
	sleep 2
done
printf '%s did not accept SQL within 240 seconds; inspect podman logs %s\n' "$container" "$container" >&2
exit 1
