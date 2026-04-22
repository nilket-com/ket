#!/usr/bin/env bash
set -euo pipefail

seed="${1:-}"

if [[ -n "${seed}" ]]; then
	echo "==> chaos seed: ${seed}"
	WENDELON_CHAOS_SEED="${seed}" cargo test --test chaos -- --nocapture
else
	echo "==> chaos seed: default"
	cargo test --test chaos -- --nocapture
fi

echo "==> rolling restart scenario"
cargo test --test upgrade_compatibility rolling_restart_with_compatible_protocol_window_preserves_control_writes -- --nocapture

echo "==> backup/restore scenario"
cargo test --test backup_restore whole_node_loss_restore_bootstraps_in_recovery_mode_and_serves_recovered_state -- --nocapture
