#!/usr/bin/env bash
set -euo pipefail

samples="${1:-${PBC_FUZZ_SAMPLES:-1000000}}"

echo "Running PBC fuzz ladder with ${samples} samples"
PBC_FUZZ_SAMPLES="${samples}" \
  cargo test pbc_surface_survives_weekly_fuzz_ladder -- --ignored --nocapture
