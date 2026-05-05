#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF' >&2
usage:
  scripts/bench/run_cdc_subscription_acceptance.sh <output-dir> <one-way-quic-rtt-micros>

optional env:
  CDC_ACCEPTANCE_BENCH_ARGS='--full'
  CDC_ACCEPTANCE_MATRIX_ROWS='1000 10000 100000'

notes:
  - one-way-quic-rtt-micros must be measured on the representative
    QUIC deployment path before running this script
  - output-dir receives environment, raw bench, and validation logs
  - nonzero exit means the output is not ADR 054 promotion evidence
EOF
}

if [[ $# -ne 2 ]]; then
  usage
  exit 2
fi

output_dir=$1
one_way_quic_rtt_micros=$2

if ! [[ "$one_way_quic_rtt_micros" =~ ^[1-9][0-9]*$ ]]; then
  echo "one-way-quic-rtt-micros must be a positive integer" >&2
  exit 2
fi

bench_args_text=${CDC_ACCEPTANCE_BENCH_ARGS:-"--full"}
matrix_rows_text=${CDC_ACCEPTANCE_MATRIX_ROWS:-"1000 10000 100000"}
read -r -a bench_args <<<"$bench_args_text"
read -r -a matrix_rows <<<"$matrix_rows_text"

if [[ ${#matrix_rows[@]} -eq 0 ]]; then
  echo "CDC_ACCEPTANCE_MATRIX_ROWS must contain at least one row count" >&2
  exit 2
fi

mkdir -p "$output_dir"

environment_file="$output_dir/environment.txt"
bench_log="$output_dir/cdc_subscription_acceptance.log"
validation_log="$output_dir/validation.log"
summary_file="$output_dir/acceptance_summary.txt"

{
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_revision=$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "git_status_short_begin"
  git status --short 2>/dev/null || true
  echo "git_status_short_end"
  echo "uname=$(uname -a)"
  echo "rustc_version_begin"
  rustc -Vv 2>/dev/null || true
  echo "rustc_version_end"
  echo "cargo_version=$(cargo -V 2>/dev/null || echo unknown)"
  echo "cpu_governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
  echo "one_way_quic_rtt_micros=$one_way_quic_rtt_micros"
  echo "bench_args=$bench_args_text"
  echo "matrix_rows=$matrix_rows_text"
} >"$environment_file"

command=(
  cargo bench --bench cdc_subscription --
  "${bench_args[@]}"
  --acceptance-one-way-quic-rtt-micros "$one_way_quic_rtt_micros"
)

set +e
{
  printf 'COMMAND:'
  printf ' %q' "${command[@]}"
  printf '\n\n'
  "${command[@]}"
} 2>&1 | tee "$bench_log"
bench_status=${PIPESTATUS[0]}
set -e

validation_failed=0
: >"$validation_log"

log_validation() {
  echo "$*" | tee -a "$validation_log"
}

validate_acceptance_output() {
  if [[ $bench_status -ne 0 ]]; then
    log_validation "bench command exited with status $bench_status"
    validation_failed=1
  fi

  if grep -Eq '^cdc_acceptance_.*status=fail$' "$bench_log"; then
    log_validation "one or more cdc_acceptance lines reported status=fail"
    validation_failed=1
  fi

  for rows in "${matrix_rows[@]}"; do
    if ! grep -Eq "^cdc_bench_matrix_case rows=${rows} " "$bench_log" \
      && [[ "$bench_args_text" == *"--full"* ]]; then
      log_validation "missing cdc_bench_matrix_case for rows=$rows"
      validation_failed=1
    fi
    for metric in \
      cdc_acceptance_live_tail_latency \
      cdc_acceptance_catchup_throughput \
      cdc_acceptance_ring_overflow_nonblocking \
      cdc_acceptance_admission_isolation
    do
      if ! grep -Eq "^${metric} rows=${rows} .*status=pass$" "$bench_log"; then
        log_validation "missing passing ${metric} line for rows=$rows"
        validation_failed=1
      fi
    done
  done

  if [[ $validation_failed -eq 0 ]]; then
    log_validation "validation_status=pass"
  else
    log_validation "validation_status=fail"
  fi
}

validate_acceptance_output

{
  echo "environment=$environment_file"
  echo "bench_log=$bench_log"
  echo "validation_log=$validation_log"
  echo "bench_status=$bench_status"
  if [[ $validation_failed -eq 0 ]]; then
    echo "validation_status=pass"
    echo "promotion_candidate=true"
  else
    echo "validation_status=fail"
    echo "promotion_candidate=false"
  fi
} >"$summary_file"

if [[ $validation_failed -ne 0 ]]; then
  echo "CDC subscription acceptance evidence failed validation; see $validation_log" >&2
  exit 1
fi

echo "CDC subscription acceptance evidence written to $output_dir"
