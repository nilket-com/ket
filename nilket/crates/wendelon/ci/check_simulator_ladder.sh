#!/usr/bin/env bash
set -euo pipefail

ops="${1:-${WENDELON_SIM_OPS:-10000}}"
seed="${2:-${WENDELON_SIM_SEED:-}}"
history_root="${WENDELON_SIM_HISTORY_DIR:-}"
history_dir=""

if [[ -z "${seed}" ]]; then
  seed="$(date +%s)"
fi

if [[ -n "${history_root}" ]]; then
  history_dir="${history_root%/}/ops-${ops}-seed-${seed}"
  mkdir -p "${history_dir}"
fi

echo "Validating simulator seed registry"
cargo test --test simulator_ladder simulator_seed_registry_files_are_well_formed -- --nocapture

echo "Replaying frozen simulator seeds"
cargo test --test simulator_ladder simulator_frozen_seed_registry_replays -- --nocapture

echo "Running simulator coverage ladder with ops=${ops} seed=${seed}"
if [[ -n "${history_dir}" ]]; then
  WENDELON_SIM_OPS="${ops}" \
  WENDELON_SIM_SEED="${seed}" \
  WENDELON_SIM_HISTORY_DIR="${history_dir}" \
    cargo test --test simulator_ladder coverage_ladder -- --ignored --nocapture
  bash ci/run_elle_handoff.sh "${history_dir}"
else
  WENDELON_SIM_OPS="${ops}" \
  WENDELON_SIM_SEED="${seed}" \
    cargo test --test simulator_ladder coverage_ladder -- --ignored --nocapture
fi
