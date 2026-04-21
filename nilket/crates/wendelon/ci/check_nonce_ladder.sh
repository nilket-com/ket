#!/usr/bin/env bash
set -euo pipefail

samples="${1:-${NONCE_INJECTIVE_SAMPLES:-100000000}}"

echo "Running nonce injectivity ladder with ${samples} samples"
NONCE_INJECTIVE_SAMPLES="${samples}" \
  cargo test nonce_encoding_is_injective_for_weekly_ladder -- --ignored --nocapture
