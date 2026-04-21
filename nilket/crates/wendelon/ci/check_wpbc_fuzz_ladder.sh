#!/usr/bin/env bash
set -euo pipefail

samples="${1:-${WPBC_FUZZ_SAMPLES:-1000000}}"

echo "Running WPBC fuzz ladder with ${samples} samples"
WPBC_FUZZ_SAMPLES="${samples}" \
  cargo test wpbc_surface_survives_weekly_fuzz_ladder -- --ignored --nocapture
