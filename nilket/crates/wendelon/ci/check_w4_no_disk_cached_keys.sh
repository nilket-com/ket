#!/usr/bin/env bash
# W4 invariant: KEK material lives in mlock'd memory only. No
# branch is allowed to introduce a code path that writes a wrapped
# DEK, raw KEK, or unwrapped key to local disk — the slab-DEK
# rewrap is always Raft-committed and the L1/L2 footers carry only
# the wrapped DEK that's already part of the envelope.
#
# This script is a coarse static guard. It greps the source tree
# for suspicious file-write call patterns colocated with key
# material identifiers. False positives must be silenced by giving
# the offending line a `// w4-no-disk-cache-ok: <reason>` comment
# explaining why the write is safe (e.g., test fixtures that mint
# a synthetic non-secret).
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v rg >/dev/null 2>&1; then
  echo "ripgrep (rg) is required for the W4 disk-cache key sweep" >&2
  exit 1
fi

# Identifiers that name actual key material in this codebase. If you
# rename one, mirror the change here.
KEY_IDENTS=(
  'LockedKeyMaterial'
  'TenantKekMaterial'
  'RootKek'
  'TenantKek'
  'unwrapped_dek'
  'raw_kek'
)

# Filesystem write surfaces that must not coexist with the above.
WRITE_SURFACES=(
  'std::fs::write'
  'tokio::fs::write'
  'File::create'
  'OpenOptions::new'
  'write_all'
)

ident_pattern="$(IFS='|'; echo "${KEY_IDENTS[*]}")"
write_pattern="$(IFS='|'; echo "${WRITE_SURFACES[*]}")"

found_any=0
while IFS= read -r file; do
  # Find lines that mention a key identifier AND a write surface
  # within +/- 8 lines of each other. -U so multiline ranges are
  # considered. The `w4-no-disk-cache-ok` escape hatch suppresses
  # acknowledged exceptions.
  matches="$(rg -nU --no-heading \
      -e "($ident_pattern)" \
      "${file}" || true)"
  while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    lineno="${line%%:*}"
    [[ -z "${lineno}" ]] && continue
    [[ ! "${lineno}" =~ ^[0-9]+$ ]] && continue
    start=$(( lineno - 8 ))
    end=$(( lineno + 8 ))
    [[ "${start}" -lt 1 ]] && start=1
    window="$(rg -n --no-heading -e "($write_pattern)" \
      "${file}" 2>/dev/null \
      | awk -F: -v lo="${start}" -v hi="${end}" '$1+0>=lo && $1+0<=hi' || true)"
    if [[ -n "${window}" ]]; then
      while IFS= read -r hit; do
        hit_line="${hit%%:*}"
        # honor the per-line escape hatch
        if sed -n "${hit_line}p" "${file}" \
             | rg -q 'w4-no-disk-cache-ok'; then
          continue
        fi
        echo "POSSIBLE on-disk KEK material write:"
        echo "  ident@${file}:${lineno}"
        echo "  write@${file}:${hit_line}: ${hit#*:*:}"
        found_any=1
      done <<< "${window}"
    fi
  done <<< "${matches}"
done < <(rg -l --no-heading -e "($ident_pattern)" src tests 2>/dev/null || true)

if [[ "${found_any}" -ne 0 ]]; then
  echo
  echo "W4 disk-cache key sweep FAILED — see above. Either remove the" >&2
  echo "write or add a // w4-no-disk-cache-ok: <reason> trailing comment" >&2
  echo "on the write line, with a justification a reviewer can audit." >&2
  exit 1
fi

echo "W4 disk-cache key sweep passed"
