#!/usr/bin/env bash
set -euo pipefail

profile="${1:-fips}"
out_dir="${2:-target/workstream_J/${profile}}"

case "${profile}" in
  default|fips)
    ;;
  *)
    echo "usage: $0 [default|fips] [out_dir]" >&2
    exit 2
    ;;
esac

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required to generate regulated release evidence" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  sha256_cmd=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  sha256_cmd=(shasum -a 256)
else
  echo "sha256sum or shasum is required to hash release artifacts" >&2
  exit 1
fi

mkdir -p "${out_dir}"
target_dir="${CARGO_TARGET_DIR:-target}"

git_rev="$(git rev-parse HEAD 2>/dev/null || printf 'unknown')"
build_time_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if git_status="$(git status --porcelain --untracked-files=all 2>/dev/null)" && [[ -z "${git_status}" ]]; then
  git_dirty="false"
else
  git_dirty="true"
fi

echo "Generating locked Cargo metadata"
cargo metadata --locked --format-version 1 > "${out_dir}/cargo-metadata.json"

echo "Generating SPDX SBOM when cargo-sbom is available"
if command -v cargo-sbom >/dev/null 2>&1; then
  cargo sbom --output-format spdx_json_2_3 > "${out_dir}/sbom.spdx.json"
  rm -f "${out_dir}/sbom.spdx.json.missing"
else
  cat > "${out_dir}/sbom.spdx.json.missing" <<'EOF'
cargo-sbom is not installed, so SPDX SBOM generation did not run.
Install the release-approved cargo-sbom version and rerun this script.
EOF
fi

case "${profile}" in
  default)
    echo "Building default release artifact"
    cargo build --locked --release --bin wendelon
    binary="${target_dir}/release/wendelon"
    ;;
  fips)
    echo "Building FIPS release artifact"
    cargo build --locked --profile fips --no-default-features --features fips --bin wendelon
    binary="${target_dir}/fips/wendelon"
    ;;
esac

cp "${binary}" "${out_dir}/wendelon"
artifact_sha256="$("${sha256_cmd[@]}" "${out_dir}/wendelon" | awk '{print $1}')"
printf '%s  wendelon\n' "${artifact_sha256}" > "${out_dir}/wendelon.sha256"

if [[ -n "${WENDELON_ATTESTATION_STATEMENT:-}" ]]; then
  cp "${WENDELON_ATTESTATION_STATEMENT}" "${out_dir}/attestation.intoto.jsonl"
  rm -f "${out_dir}/attestation.intoto.jsonl.missing"
  attestation_provenance_line='  "attestation_path": "attestation.intoto.jsonl",'
else
  cat > "${out_dir}/attestation.intoto.jsonl.missing" <<'EOF'
WENDELON_ATTESTATION_STATEMENT is not set, so no in-toto/SLSA attestation was attached.
Produce the release-approved attestation statement and rerun this script.
EOF
  attestation_provenance_line='  "attestation_missing": "attestation.intoto.jsonl.missing",'
fi

cat > "${out_dir}/provenance.json" <<EOF
{
  "artifact": "wendelon",
  "profile": "${profile}",
  "git_rev": "${git_rev}",
  "git_dirty": ${git_dirty},
  "build_time_utc": "${build_time_utc}",
  "artifact_sha256": "${artifact_sha256}",
${attestation_provenance_line}
  "cargo_metadata": "cargo-metadata.json",
  "sbom": "sbom.spdx.json",
  "sha256": "wendelon.sha256"
}
EOF

manifest_inputs=(wendelon wendelon.sha256 cargo-metadata.json provenance.json)
if [[ -s "${out_dir}/sbom.spdx.json" ]]; then
  manifest_inputs+=(sbom.spdx.json)
fi
if [[ -s "${out_dir}/attestation.intoto.jsonl" ]]; then
  manifest_inputs+=(attestation.intoto.jsonl)
fi
(cd "${out_dir}" && "${sha256_cmd[@]}" "${manifest_inputs[@]}" > release-manifest.sha256)

if [[ -n "${WENDELON_RELEASE_SIGNING_KEY:-}" ]]; then
  if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required when WENDELON_RELEASE_SIGNING_KEY is set" >&2
    exit 1
  fi
  for signed_file in wendelon provenance.json release-manifest.sha256; do
    openssl dgst -sha256 \
      -sign "${WENDELON_RELEASE_SIGNING_KEY}" \
      -out "${out_dir}/${signed_file}.sig" \
      "${out_dir}/${signed_file}"
  done
  rm -f "${out_dir}/wendelon.sig.missing"
  rm -f "${out_dir}/provenance.json.sig.missing"
  rm -f "${out_dir}/release-manifest.sha256.sig.missing"
else
  cat > "${out_dir}/wendelon.sig.missing" <<'EOF'
WENDELON_RELEASE_SIGNING_KEY is not set, so detached artifact signing did not run.
Set it to a release-approved private key path and rerun this script.
EOF
  cat > "${out_dir}/provenance.json.sig.missing" <<'EOF'
WENDELON_RELEASE_SIGNING_KEY is not set, so provenance signing did not run.
Set it to a release-approved private key path and rerun this script.
EOF
  cat > "${out_dir}/release-manifest.sha256.sig.missing" <<'EOF'
WENDELON_RELEASE_SIGNING_KEY is not set, so release-manifest signing did not run.
Set it to a release-approved private key path and rerun this script.
EOF
fi

echo "Regulated release evidence written to ${out_dir}"
