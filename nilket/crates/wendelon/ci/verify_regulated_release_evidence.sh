#!/usr/bin/env bash
set -euo pipefail

profile="${1:-fips}"
out_dir="${2:-target/regulated_release/${profile}}"

case "${profile}" in
  default|fips)
    ;;
  *)
    echo "usage: $0 [default|fips] [out_dir]" >&2
    exit 2
    ;;
esac

for required in cargo-metadata.json provenance.json release-manifest.sha256 wendelon wendelon.sha256; do
  if [[ ! -s "${out_dir}/${required}" ]]; then
    echo "missing required release evidence file: ${out_dir}/${required}" >&2
    exit 1
  fi
done

if command -v sha256sum >/dev/null 2>&1; then
  sha256_check=(sha256sum -c)
elif command -v shasum >/dev/null 2>&1; then
  sha256_check=(shasum -a 256 -c)
else
  echo "sha256sum or shasum is required to verify release artifacts" >&2
  exit 1
fi

manifest_lists() {
  awk '{print $2}' "${out_dir}/release-manifest.sha256" | grep -Fxq "$1"
}

for expected_manifest_entry in wendelon wendelon.sha256 cargo-metadata.json provenance.json; do
  if ! manifest_lists "${expected_manifest_entry}"; then
    echo "release-manifest.sha256 is missing required entry: ${expected_manifest_entry}" >&2
    exit 1
  fi
done

if [[ -s "${out_dir}/sbom.spdx.json" ]] && ! manifest_lists "sbom.spdx.json"; then
  echo "release-manifest.sha256 is missing present SBOM: sbom.spdx.json" >&2
  exit 1
fi

if [[ -s "${out_dir}/attestation.intoto.jsonl" ]] && ! manifest_lists "attestation.intoto.jsonl"; then
  echo "release-manifest.sha256 is missing present attestation: attestation.intoto.jsonl" >&2
  exit 1
fi

(cd "${out_dir}" && "${sha256_check[@]}" wendelon.sha256)
(cd "${out_dir}" && "${sha256_check[@]}" release-manifest.sha256)

artifact_sha256="$(awk '{print $1}' "${out_dir}/wendelon.sha256")"
if ! grep -Fq "\"artifact_sha256\": \"${artifact_sha256}\"" "${out_dir}/provenance.json"; then
  echo "provenance.json does not embed the binary digest from wendelon.sha256" >&2
  exit 1
fi

has_attestation_path="false"
has_attestation_missing="false"
if grep -Fq '"attestation_path": "attestation.intoto.jsonl"' "${out_dir}/provenance.json"; then
  has_attestation_path="true"
fi
if grep -Fq '"attestation_missing": "attestation.intoto.jsonl.missing"' "${out_dir}/provenance.json"; then
  has_attestation_missing="true"
fi

if [[ "${has_attestation_path}" == "true" && "${has_attestation_missing}" == "true" ]]; then
  echo "provenance.json must not contain both attestation_path and attestation_missing" >&2
  exit 1
elif [[ "${has_attestation_path}" == "true" ]]; then
  if [[ ! -s "${out_dir}/attestation.intoto.jsonl" ]]; then
    echo "provenance.json names attestation_path but attestation.intoto.jsonl is missing" >&2
    exit 1
  fi
elif [[ "${has_attestation_missing}" == "true" ]]; then
  if [[ -s "${out_dir}/attestation.intoto.jsonl" ]]; then
    echo "provenance.json names attestation_missing but attestation.intoto.jsonl is present" >&2
    exit 1
  fi
else
  echo "provenance.json must contain attestation_path or attestation_missing" >&2
  exit 1
fi

if [[ -s "${out_dir}/sbom.spdx.json" ]]; then
  if ! grep -Eq '"spdxVersion"|"SPDXID"' "${out_dir}/sbom.spdx.json"; then
    echo "SBOM does not look like an SPDX document: ${out_dir}/sbom.spdx.json" >&2
    exit 1
  fi
elif [[ "${REQUIRE_SPDX_SBOM:-0}" == "1" ]]; then
  echo "SPDX SBOM is required but missing" >&2
  exit 1
else
  echo "SPDX SBOM missing; see ${out_dir}/sbom.spdx.json.missing" >&2
fi

signature_files=(wendelon.sig provenance.json.sig release-manifest.sha256.sig)
signature_present="false"
for signature_file in "${signature_files[@]}"; do
  if [[ -e "${out_dir}/${signature_file}" ]]; then
    signature_present="true"
  fi
done

if [[ "${signature_present}" == "true" ]]; then
  if [[ -z "${WENDELON_RELEASE_VERIFY_KEY:-}" ]]; then
    echo "signature exists but WENDELON_RELEASE_VERIFY_KEY is not set" >&2
    exit 1
  fi
  if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required when WENDELON_RELEASE_VERIFY_KEY is set" >&2
    exit 1
  fi
  for signature_file in "${signature_files[@]}"; do
    if [[ ! -s "${out_dir}/${signature_file}" ]]; then
      echo "incomplete signature set: ${out_dir}/${signature_file} is missing" >&2
      exit 1
    fi
  done
  for signed_file in wendelon provenance.json release-manifest.sha256; do
    openssl dgst -sha256 \
      -verify "${WENDELON_RELEASE_VERIFY_KEY}" \
      -signature "${out_dir}/${signed_file}.sig" \
      "${out_dir}/${signed_file}"
  done
elif [[ "${REQUIRE_RELEASE_SIGNING:-0}" == "1" ]]; then
  echo "release signatures are required but missing" >&2
  exit 1
else
  echo "release signatures missing; see ${out_dir}/*.sig.missing" >&2
fi

if [[ ! -s "${out_dir}/attestation.intoto.jsonl" ]]; then
  if [[ "${REQUIRE_ATTESTATION:-0}" == "1" ]]; then
    echo "attestation is required but missing" >&2
    exit 1
  fi
  echo "attestation missing; see ${out_dir}/attestation.intoto.jsonl.missing" >&2
fi

echo "Regulated release evidence verification completed for ${out_dir}"
