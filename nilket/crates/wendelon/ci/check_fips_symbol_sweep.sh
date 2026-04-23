#!/usr/bin/env bash
set -euo pipefail

if ! command -v clang >/dev/null 2>&1; then
  echo "clang is required for the FIPS symbol sweep" >&2
  exit 1
fi

if ! command -v nm >/dev/null 2>&1; then
  echo "nm is required for the FIPS symbol sweep" >&2
  exit 1
fi

if ! command -v strings >/dev/null 2>&1; then
  echo "strings is required for the FIPS symbol sweep" >&2
  exit 1
fi

if ! command -v rg >/dev/null 2>&1; then
  echo "rg is required for the FIPS symbol sweep" >&2
  exit 1
fi

export CC="${CC:-clang}"
export CXX="${CXX:-clang++}"
export CARGO_PROFILE_FIPS_STRIP="${CARGO_PROFILE_FIPS_STRIP:-none}"
target_dir="${CARGO_TARGET_DIR:-target}"

echo "Building FIPS release artifact with CC=${CC} CXX=${CXX}"
cargo build --locked --profile fips --no-default-features --features fips --bin wendelon

binary="${target_dir}/fips/wendelon"

echo "Sweeping ${binary} for AES-GCM-SIV symbols"
if nm "${binary}" | rg -i 'aesgcmsiv|AESIV|GCM_SIV'; then
  echo "unexpected AES-GCM-SIV symbols found in ${binary}" >&2
  exit 1
fi

echo "Sweeping ${binary} for AES-GCM-SIV strings"
if strings "${binary}" | rg -i 'aes.?gcm.?siv|AESIV'; then
  echo "unexpected AES-GCM-SIV strings found in ${binary}" >&2
  exit 1
fi

echo "Sweeping ${binary} for non-FIPS audit hash tags"
if strings "${binary}" | rg -F 'blake3-256'; then
  echo "unexpected non-FIPS audit hash tag found in ${binary}" >&2
  exit 1
fi

echo "FIPS symbol sweep passed"
