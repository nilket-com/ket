#!/usr/bin/env bash
# Startup latency gate (crates/libaryon/plans/impl/startup-latency.md): the libaryon engine test
# and the baryon pty test, from a release build, with budgets enforced. Run it on a quiet machine,
# outside cargo-ctrl's parallel lane. Each run prints one JSON line per scenario and milestone.
#
#   ci/startup-latency-check.sh                 enforce budgets
#   ci/startup-latency-check.sh --report-only   measure and print, never fail on a budget
#
# BARYON_STARTUP_S3_64GIB=1 adds the 64 GiB sparse characterization. Per-budget overrides are
# listed in each test's header.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENFORCE=1

case "${1:-}" in
	"") ;;
	--report-only) ENFORCE=0 ;;
	*)
		echo "Usage: ci/startup-latency-check.sh [--report-only]" >&2
		exit 2
		;;
esac

if [[ "$(uname -s)" != "Linux" ]]; then
	echo "startup latency check skipped: Linux is required"
	exit 0
fi

cd "$ROOT_DIR"
cargo test --release --locked -p libaryon --test startup_latency_test --no-run
cargo test --release --locked -p baryon --test startup_latency_pty_test --no-run

export BARYON_STARTUP_LATENCY_ENFORCE="$ENFORCE"
timeout 300s cargo test --release --locked -p libaryon --test startup_latency_test \
	-- --exact engine_startup_latency_stays_within_budget --nocapture
timeout 300s cargo test --release --locked -p baryon --test startup_latency_pty_test \
	-- --exact terminal_startup_latency_stays_within_budget --nocapture
