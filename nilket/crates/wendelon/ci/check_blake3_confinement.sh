#!/usr/bin/env bash
set -euo pipefail

is_boundary_file() {
	case "$1" in
		src/hash/blake3.rs|src/hash/content.rs|src/hash/identity.rs)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

exception_reason() {
	case "$1" in
		src/network/ring.rs)
			echo "vnode placement hash pending compatibility decision"
			;;
		*)
			return 1
			;;
	esac
}

mapfile -t files < <(
	grep -R -n 'blake3::' src \
		| grep -v 'hash::blake3::' \
		| cut -d: -f1 \
		| sort -u
)
status=0

echo "BLAKE3 confinement inventory"
for file in "${files[@]}"; do
	if is_boundary_file "${file}"; then
		echo "  boundary  ${file}"
		continue
	fi

	if reason="$(exception_reason "${file}")"; then
		echo "  exception ${file} (${reason})"
		continue
	fi

	echo "unexpected raw BLAKE3 usage outside the confinement boundary: ${file}" >&2
	status=1
done

if [[ "${status}" -ne 0 ]]; then
	exit "${status}"
fi

echo "BLAKE3 confinement check passed"
