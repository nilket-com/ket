#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF' >&2
usage:
  OUTPUT_DIR=<dir> \
  BOOTSTRAP="1=host1:7443,2=host2:7443,3=host3:7443" \
  TLS_CERT=<path> TLS_KEY=<path> TLS_CA=<path> \
  scripts/soak/run_commodity_soak.sh

optional env:
  BIND_ADDR=0.0.0.0:0
  CLUSTER_DOMAIN=cluster.local
  DURATION_SECS=3600
  KEY_COUNT=100000
  PREFILL_KEYS=1000
  CONCURRENCY=16
  STATS_INTERVAL_SECS=5
  TARGET_OPS_PER_SEC=0
  SMALL_VALUE_BYTES=256
  MEDIUM_VALUE_BYTES=4096
  MEDIUM_WRITE_EVERY=10
  QUERY_WIDTH_LOCALITIES=1024
  WRITE_PERCENT=40
  READ_PERCENT=40
  QUERY_PERCENT=20
  READ_CONSISTENCY=linearizable
  QUERY_CONSISTENCY=stale
  AWAIT_FINALIZATION=0
  SEED=0
  RUN_SUPPORTING_EVIDENCE=1
  BENCH_MODE=both
  CHURN_INTERVAL_SECS=300
  CHURN_COMMAND='ssh node1 sudo systemctl restart wendelon'

notes:
  - if CHURN_COMMAND is unset, the script records that manual churn is required
  - CHURN_COMMAND runs repeatedly while the soak driver is alive
EOF
}

: "${OUTPUT_DIR:=}"
: "${BOOTSTRAP:=}"
: "${TLS_CERT:=}"
: "${TLS_KEY:=}"
: "${TLS_CA:=}"

if [[ -z "$OUTPUT_DIR" || -z "$BOOTSTRAP" || -z "$TLS_CERT" || -z "$TLS_KEY" || -z "$TLS_CA" ]]; then
  usage
  exit 2
fi

BIND_ADDR=${BIND_ADDR:-0.0.0.0:0}
CLUSTER_DOMAIN=${CLUSTER_DOMAIN:-cluster.local}
DURATION_SECS=${DURATION_SECS:-3600}
KEY_COUNT=${KEY_COUNT:-100000}
PREFILL_KEYS=${PREFILL_KEYS:-1000}
CONCURRENCY=${CONCURRENCY:-16}
STATS_INTERVAL_SECS=${STATS_INTERVAL_SECS:-5}
TARGET_OPS_PER_SEC=${TARGET_OPS_PER_SEC:-0}
SMALL_VALUE_BYTES=${SMALL_VALUE_BYTES:-256}
MEDIUM_VALUE_BYTES=${MEDIUM_VALUE_BYTES:-4096}
MEDIUM_WRITE_EVERY=${MEDIUM_WRITE_EVERY:-10}
QUERY_WIDTH_LOCALITIES=${QUERY_WIDTH_LOCALITIES:-1024}
WRITE_PERCENT=${WRITE_PERCENT:-40}
READ_PERCENT=${READ_PERCENT:-40}
QUERY_PERCENT=${QUERY_PERCENT:-20}
READ_CONSISTENCY=${READ_CONSISTENCY:-linearizable}
QUERY_CONSISTENCY=${QUERY_CONSISTENCY:-stale}
AWAIT_FINALIZATION=${AWAIT_FINALIZATION:-0}
SEED=${SEED:-0}
RUN_SUPPORTING_EVIDENCE=${RUN_SUPPORTING_EVIDENCE:-1}
BENCH_MODE=${BENCH_MODE:-both}
CHURN_INTERVAL_SECS=${CHURN_INTERVAL_SECS:-300}

mkdir -p "$OUTPUT_DIR"

if [[ "$RUN_SUPPORTING_EVIDENCE" == "1" ]]; then
  scripts/soak/run_supporting_evidence.sh "$OUTPUT_DIR/supporting" "$BENCH_MODE"
fi

driver_cmd=(
  cargo run --example soak_driver --
  --bootstrap "$BOOTSTRAP"
  --tls-cert "$TLS_CERT"
  --tls-key "$TLS_KEY"
  --tls-ca "$TLS_CA"
  --bind-addr "$BIND_ADDR"
  --cluster-domain "$CLUSTER_DOMAIN"
  --duration-secs "$DURATION_SECS"
  --key-count "$KEY_COUNT"
  --prefill-keys "$PREFILL_KEYS"
  --concurrency "$CONCURRENCY"
  --stats-interval-secs "$STATS_INTERVAL_SECS"
  --target-ops-per-sec "$TARGET_OPS_PER_SEC"
  --small-value-bytes "$SMALL_VALUE_BYTES"
  --medium-value-bytes "$MEDIUM_VALUE_BYTES"
  --medium-write-every "$MEDIUM_WRITE_EVERY"
  --query-width-localities "$QUERY_WIDTH_LOCALITIES"
  --write-percent "$WRITE_PERCENT"
  --read-percent "$READ_PERCENT"
  --query-percent "$QUERY_PERCENT"
  --read-consistency "$READ_CONSISTENCY"
  --query-consistency "$QUERY_CONSISTENCY"
  --seed "$SEED"
  --output-dir "$OUTPUT_DIR/driver"
)

if [[ "$AWAIT_FINALIZATION" == "1" ]]; then
  driver_cmd+=(--await-finalization)
fi

{
  printf 'COMMAND:'
  printf ' %q' "${driver_cmd[@]}"
  printf '\n\n'
  "${driver_cmd[@]}"
} 2>&1 | tee "$OUTPUT_DIR/soak_driver.log" &
driver_pid=$!

cleanup() {
  if kill -0 "$driver_pid" 2>/dev/null; then
    kill "$driver_pid" 2>/dev/null || true
  fi
}
trap cleanup INT TERM

if [[ -n "${CHURN_COMMAND:-}" ]]; then
  event_index=0
  : >"$OUTPUT_DIR/churn.log"
  while kill -0 "$driver_pid" 2>/dev/null; do
    sleep "$CHURN_INTERVAL_SECS"
    if ! kill -0 "$driver_pid" 2>/dev/null; then
      break
    fi
    event_index=$((event_index + 1))
    {
      echo "EVENT $event_index $(date -u +%Y-%m-%dT%H:%M:%SZ)"
      SOAK_EVENT_INDEX=$event_index OUTPUT_DIR="$OUTPUT_DIR" bash -c "$CHURN_COMMAND"
      echo
    } 2>&1 | tee -a "$OUTPUT_DIR/churn.log"
  done
else
  cat >"$OUTPUT_DIR/manual_churn_required.txt" <<EOF
No CHURN_COMMAND was supplied.

This soak run does not satisfy the D6 churn requirement unless you induce leader loss and rejoin manually while the driver is running.
EOF
fi

wait "$driver_pid"
trap - INT TERM

cat >"$OUTPUT_DIR/next_steps.txt" <<EOF
1. Copy the environment and command lines into plans/impl/workstream_D_soak_report.md
2. Fill in the churn timeline from:
   - $OUTPUT_DIR/churn.log (if present)
   - your manual operator notes otherwise
3. Attach summary data from:
   - $OUTPUT_DIR/driver/summary.json
   - $OUTPUT_DIR/driver/samples.jsonl
   - $OUTPUT_DIR/supporting/*.log
EOF

echo "soak artifacts written to $OUTPUT_DIR"
