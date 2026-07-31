#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF' >&2
usage:
  crates/toron/scripts/bench/run_runtime_filter_perft.sh [--mode smoke|selective|full|custom] [--out-dir DIR] [--samply] [-- BENCH_ARGS...]

optional env:
  TORON_PERFT_MODE=smoke|selective|full|custom
  TORON_PERFT_OUT_DIR=target/toron-perft/<run-name>
  TORON_PERFT_SAMPLY=0|1
  TORON_PERFT_WORKERS=8
  TORON_PERFT_BUILD_ROWS=100000
  TORON_PERFT_PROBE_ROWS=100000000
  TORON_PERFT_PROBE_KEY_DOMAIN=10000000
  TORON_PERFT_MIN_PROBE_REDUCTION_PCT=50
  TORON_PERFT_MIN_SHUFFLE_REDUCTION_PCT=50
  TORON_PERFT_MIN_CLUSTER_REDUCTION_PCT=50

modes:
  smoke     2 workers, 2K build rows, 8K probe rows, 32K probe key domain
  selective 8 workers, 100K build rows, 10M probe rows, 10M probe key domain
  full      8 workers, 100K build rows, 100M probe rows, 10M probe key domain
  custom    all sizing comes from TORON_PERFT_* env vars

notes:
  - runs the hash-partitioned runtime-filter matrix:
    enabled/enabled, enabled/disabled, disabled/enabled, disabled/disabled
  - writes environment.txt, command.txt, raw.log, rows.csv, and summary.md
  - nonzero exit means the run did not satisfy the local validation gates
EOF
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../../../.." && pwd)

mode=${TORON_PERFT_MODE:-smoke}
output_dir=${TORON_PERFT_OUT_DIR:-}
profile_with_samply=${TORON_PERFT_SAMPLY:-0}
extra_bench_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      if [[ $# -lt 2 ]]; then
        echo "--mode requires a value" >&2
        exit 2
      fi
      mode=$2
      shift 2
      ;;
    --mode=*)
      mode=${1#*=}
      shift
      ;;
    --out-dir)
      if [[ $# -lt 2 ]]; then
        echo "--out-dir requires a value" >&2
        exit 2
      fi
      output_dir=$2
      shift 2
      ;;
    --out-dir=*)
      output_dir=${1#*=}
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --samply)
      profile_with_samply=1
      shift
      ;;
    --)
      shift
      extra_bench_args+=("$@")
      break
      ;;
    *)
      extra_bench_args+=("$1")
      shift
      ;;
  esac
done

case "$mode" in
  smoke)
    default_workers=2
    default_build_rows=2048
    default_probe_rows=8192
    default_probe_key_domain=32768
    default_min_cluster_reduction_pct=
    ;;
  selective)
    default_workers=8
    default_build_rows=100000
    default_probe_rows=10000000
    default_probe_key_domain=10000000
    default_min_cluster_reduction_pct=50
    ;;
  full)
    default_workers=8
    default_build_rows=100000
    default_probe_rows=100000000
    default_probe_key_domain=10000000
    default_min_cluster_reduction_pct=50
    ;;
  custom)
    default_workers=${TORON_PERFT_WORKERS:-}
    default_build_rows=${TORON_PERFT_BUILD_ROWS:-}
    default_probe_rows=${TORON_PERFT_PROBE_ROWS:-}
    default_probe_key_domain=${TORON_PERFT_PROBE_KEY_DOMAIN:-}
    default_min_cluster_reduction_pct=
    ;;
  *)
    echo "unknown mode: $mode" >&2
    usage
    exit 2
    ;;
esac

workers=${TORON_PERFT_WORKERS:-$default_workers}
build_rows=${TORON_PERFT_BUILD_ROWS:-$default_build_rows}
probe_rows=${TORON_PERFT_PROBE_ROWS:-$default_probe_rows}
probe_key_domain=${TORON_PERFT_PROBE_KEY_DOMAIN:-$default_probe_key_domain}
min_probe_reduction_pct=${TORON_PERFT_MIN_PROBE_REDUCTION_PCT:-50}
min_shuffle_reduction_pct=${TORON_PERFT_MIN_SHUFFLE_REDUCTION_PCT:-50}
min_cluster_reduction_pct=${TORON_PERFT_MIN_CLUSTER_REDUCTION_PCT-$default_min_cluster_reduction_pct}

if ! [[ "$workers" =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]]; then
  echo "workers must be a comma-separated list of positive integers; got '$workers'" >&2
  exit 2
fi

for name in build_rows probe_rows probe_key_domain; do
  value=${!name}
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name must be a positive integer; got '$value'" >&2
    exit 2
  fi
done

for name in min_probe_reduction_pct min_shuffle_reduction_pct; do
  value=${!name}
  if ! [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "$name must be numeric; got '$value'" >&2
    exit 2
  fi
done

if [[ -n "$min_cluster_reduction_pct" ]] \
  && ! [[ "$min_cluster_reduction_pct" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "TORON_PERFT_MIN_CLUSTER_REDUCTION_PCT must be numeric or empty; got '$min_cluster_reduction_pct'" >&2
  exit 2
fi

case "$profile_with_samply" in
  0|1|false|true|off|on)
    ;;
  *)
    echo "TORON_PERFT_SAMPLY must be 0 or 1; got '$profile_with_samply'" >&2
    exit 2
    ;;
esac

if [[ -z "$output_dir" ]]; then
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  output_dir="$repo_root/target/toron-perft/${mode}-${timestamp}"
elif [[ "$output_dir" != /* ]]; then
  output_dir="$repo_root/$output_dir"
fi

mkdir -p "$output_dir"

environment_file="$output_dir/environment.txt"
command_file="$output_dir/command.txt"
raw_log="$output_dir/raw.log"
rows_file="$output_dir/rows.csv"
summary_file="$output_dir/summary.md"
samply_output="$output_dir/samply.json.gz"
samply_build_log="$output_dir/samply-build.log"

bench_args=(
  --distributed-inner-equi-join-only
  --strategy=hash
  --workers="$workers"
  --build-rows="$build_rows"
  --probe-rows="$probe_rows"
  --probe-key-domain="$probe_key_domain"
  --runtime-filter=enabled,disabled
  --wendelon-runtime-filter=enabled,disabled
)

if [[ "$mode" == smoke ]]; then
  bench_args=(--quick "${bench_args[@]}")
fi

if [[ ${#extra_bench_args[@]} -gt 0 ]]; then
  bench_args+=("${extra_bench_args[@]}")
fi

if [[ "$profile_with_samply" == 1 || "$profile_with_samply" == true || "$profile_with_samply" == on ]]; then
  if ! command -v samply >/dev/null 2>&1; then
    echo "samply requested but not found on PATH" >&2
    exit 2
  fi

  set +e
  {
    cd "$repo_root"
    cargo bench -p toron --bench distributed_inner_equi_join --no-run
  } 2>&1 | tee "$samply_build_log"
  build_status=${PIPESTATUS[0]}
  set -e

  if [[ $build_status -ne 0 ]]; then
    echo "bench target compilation for samply failed; see $samply_build_log" >&2
    exit "$build_status"
  fi

  bench_exe=$(
    find "$repo_root/target/release/deps" \
      -maxdepth 1 \
      -type f \
      -perm -111 \
      -name 'distributed_inner_equi_join-*' \
      -printf '%T@ %p\n' \
      | sort -nr \
      | awk 'NR == 1 { $1=""; sub(/^ /, ""); print }'
  )
  if [[ -z "$bench_exe" ]]; then
    echo "could not locate compiled distributed_inner_equi_join bench executable" >&2
    exit 1
  fi

  command=(samply record --save-only -o "$samply_output" -- "$bench_exe" "${bench_args[@]}")
else
  command=(cargo bench -p toron --bench distributed_inner_equi_join -- "${bench_args[@]}")
fi

{
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_revision=$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "git_status_short_begin"
  git -C "$repo_root" status --short 2>/dev/null || true
  echo "git_status_short_end"
  echo "uname=$(uname -a)"
  echo "rustc_version_begin"
  rustc -Vv 2>/dev/null || true
  echo "rustc_version_end"
  echo "cargo_version=$(cargo -V 2>/dev/null || echo unknown)"
  echo "cpu_governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
  echo "mode=$mode"
  echo "workers=$workers"
  echo "build_rows=$build_rows"
  echo "probe_rows=$probe_rows"
  echo "probe_key_domain=$probe_key_domain"
  echo "min_probe_reduction_pct=$min_probe_reduction_pct"
  echo "min_shuffle_reduction_pct=$min_shuffle_reduction_pct"
  echo "min_cluster_reduction_pct=$min_cluster_reduction_pct"
  echo "profile_with_samply=$profile_with_samply"
  echo "samply_output=$samply_output"
} >"$environment_file"

{
  printf 'COMMAND:'
  printf ' %q' "${command[@]}"
  printf '\n'
} >"$command_file"

set +e
{
  cat "$command_file"
  printf '\n'
  cd "$repo_root"
  "${command[@]}"
} 2>&1 | tee "$raw_log"
bench_status=${PIPESTATUS[0]}
set -e

if [[ $bench_status -ne 0 ]]; then
  echo "runtime-filter perft bench exited with status $bench_status; see $raw_log" >&2
  exit "$bench_status"
fi

if ! grep '^adr004_distributed_inner_equi_join,' "$raw_log" >"$rows_file"; then
  echo "runtime-filter perft produced no structured rows; see $raw_log" >&2
  exit 1
fi

summary_args=(
  --require-digest-equality
  --min-probe-reduction-pct "$min_probe_reduction_pct"
  --min-shuffle-reduction-pct "$min_shuffle_reduction_pct"
)

if [[ -n "$min_cluster_reduction_pct" ]]; then
  summary_args+=(--min-cluster-reduction-pct "$min_cluster_reduction_pct")
fi

"$script_dir/summarize_runtime_filter_perft.py" \
  "${summary_args[@]}" \
  "$rows_file" | tee "$summary_file"

echo "runtime-filter perft evidence written to $output_dir"
