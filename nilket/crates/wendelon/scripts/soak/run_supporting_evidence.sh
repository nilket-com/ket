#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  cat <<'EOF' >&2
usage:
  scripts/soak/run_supporting_evidence.sh <output-dir> [bench-mode]

notes:
  - writes command output into the target directory
  - bench-mode defaults to both
EOF
  exit 2
fi

output_dir=$1
bench_mode=${2:-both}

mkdir -p "$output_dir"

run_and_capture() {
  local name=$1
  shift
  echo "== $name =="
  {
    printf 'COMMAND:'
    printf ' %q' "$@"
    printf '\n\n'
    "$@"
  } 2>&1 | tee "$output_dir/$name.log"
}

run_and_capture property_suite cargo test --quiet --test property_suite
run_and_capture chaos_smoke cargo test --quiet --test chaos chaos_smoke_catalog -- --nocapture
run_and_capture simulator_ladder cargo test --quiet --test simulator_ladder
run_and_capture io_topology cargo bench --bench io_topology -- --mode "$bench_mode"

cat >"$output_dir/supporting_evidence_manifest.txt" <<EOF
property_suite.log
chaos_smoke.log
simulator_ladder.log
io_topology.log
EOF

echo "supporting evidence written to $output_dir"
