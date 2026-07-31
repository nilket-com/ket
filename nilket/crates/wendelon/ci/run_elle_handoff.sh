#!/usr/bin/env bash
set -euo pipefail

history_dir="${1:?usage: ci/run_elle_handoff.sh <history-dir>}"
runner="${WENDELON_ELLE_RUNNER:-}"

mapfile -t artifacts < <(find "${history_dir}" -type f -name '*register-history.json' | sort)

if [[ "${#artifacts[@]}" -eq 0 ]]; then
  echo "No register-history artifacts found in ${history_dir}" >&2
  exit 1
fi

for artifact in "${artifacts[@]}"; do
  if [[ ! -s "${artifact}" ]]; then
    echo "History artifact is empty: ${artifact}" >&2
    exit 1
  fi
  echo "ELLE_HANDOFF artifact=${artifact}"
  if [[ -n "${runner}" ]]; then
    "${runner}" "${artifact}"
  fi
done

if [[ -z "${runner}" ]]; then
  echo "Elle runner not configured; validated handoff artifacts only"
fi
