#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF' >&2
usage:
  OUTPUT_DIR=<new-directory> [NILKET_BIN=<path>] \
  crates/wendelon/scripts/soak/run_local_idle_soak.sh

optional env:
  DURATION_SECS=3
  SAMPLE_INTERVAL_MILLIS=500
  HEARTBEAT_INTERVAL_MILLIS=100

The runner owns a fresh `nilket local` profile and refuses if the fixed local
ports are already occupied. It never attaches to or stops an existing profile.
EOF
}

: "${OUTPUT_DIR:=}"
: "${NILKET_BIN:=target/debug/nilket}"
DURATION_SECS=${DURATION_SECS:-3}
SAMPLE_INTERVAL_MILLIS=${SAMPLE_INTERVAL_MILLIS:-500}
HEARTBEAT_INTERVAL_MILLIS=${HEARTBEAT_INTERVAL_MILLIS:-100}

if [[ -z "$OUTPUT_DIR" ]]; then
  usage
  exit 2
fi
for tool in curl jq rg sed; do
  command -v "$tool" >/dev/null || {
    echo "local-idle soak requires $tool" >&2
    exit 2
  }
done
if [[ -e "$OUTPUT_DIR" ]]; then
  echo "local-idle soak refuses existing output directory: $OUTPUT_DIR" >&2
  exit 2
fi
mkdir -p "$OUTPUT_DIR"
profile="$OUTPUT_DIR/profile"
samples="$OUTPUT_DIR/samples.jsonl"
: >"$samples"

if [[ ! -x "$NILKET_BIN" ]]; then
  cargo build -p nilket
fi

owned_pid=""
stop_owned() {
  if [[ -n "$owned_pid" ]] && kill -0 "$owned_pid" 2>/dev/null; then
    kill -INT "$owned_pid" 2>/dev/null || true
    for _ in $(seq 1 750); do
      kill -0 "$owned_pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$owned_pid" 2>/dev/null; then
      echo "local-idle soak: owned nilket did not drain within 75 seconds" >&2
      kill -TERM "$owned_pid" 2>/dev/null || true
      return 1
    fi
    wait "$owned_pid" || true
  fi
  owned_pid=""
}
trap stop_owned EXIT INT TERM

wait_ready() {
  local stdout_path=$1
  for _ in $(seq 1 450); do
    if rg -q '^LOCAL_READY v=1 ' "$stdout_path" 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "$owned_pid" 2>/dev/null; then
      wait "$owned_pid" || true
      echo "local-idle soak: nilket exited before LOCAL_READY; see $stdout_path" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "local-idle soak: timed out waiting for LOCAL_READY" >&2
  return 1
}

start_owned() {
  local tag=$1
  "$NILKET_BIN" local --data-dir "$profile" \
    >"$OUTPUT_DIR/${tag}.stdout.log" \
    2>"$OUTPUT_DIR/${tag}.stderr.log" &
  owned_pid=$!
  wait_ready "$OUTPUT_DIR/${tag}.stdout.log"
}

# First start creates the generated teaching artifact. It is outside the
# measured interval; the measured process begins after the compressed cadence
# is written visibly into the retained profile.
start_owned warmup
stop_owned
config_path="$profile/config/wendelon.toml"
sed -i \
  "s/cdc_locality_heartbeat_interval_millis = 30000/cdc_locality_heartbeat_interval_millis = ${HEARTBEAT_INTERVAL_MILLIS}/" \
  "$config_path"
rg -q "^cdc_locality_heartbeat_interval_millis = ${HEARTBEAT_INTERVAL_MILLIS}$" "$config_path" || {
  echo "local-idle soak: failed to install compressed heartbeat cadence" >&2
  exit 2
}

audit_path="$profile/data/wendelon/audit.ndjson"
audit_offset=$(wc -c <"$audit_path")
start_owned measured

pki="$profile/pki"
curl_base=(--silent --show-error --fail --http1.1 \
  --cert "$pki/local-operator.crt" --key "$pki/local-operator.key" \
  --cacert "$pki/node-ca.crt")
metrics_url="https://node-1.cluster.local:7433/metrics"
ready_url="https://node-1.cluster.local:7432/ready"
metrics_resolve="node-1.cluster.local:7433:127.0.0.1"
ready_resolve="node-1.cluster.local:7432:127.0.0.1"

metric_value() {
  local body=$1
  local name=$2
  awk -v metric="$name" '$1 == metric { print $2; found=1 } END { if (!found) print 0 }' <<<"$body"
}

metrics_body=$(curl "${curl_base[@]}" --resolve "$metrics_resolve" "$metrics_url")
previous_heartbeats=$(metric_value "$metrics_body" wendelon_cdc_locality_heartbeats_committed_total)
previous_wendelon_warns=0
previous_toron_warns=0
started_ns=$(date +%s%N)
requested_duration_millis=$((DURATION_SECS * 1000))
sample_count=$(((requested_duration_millis + SAMPLE_INTERVAL_MILLIS - 1) / SAMPLE_INTERVAL_MILLIS))

for _ in $(seq 1 "$sample_count"); do
  sleep "$(awk -v ms="$SAMPLE_INTERVAL_MILLIS" 'BEGIN { printf "%.3f", ms / 1000 }')"
  healthy=true
  queryable=true
  curl "${curl_base[@]}" --resolve "$ready_resolve" "$ready_url" >/dev/null || healthy=false
  metrics_body=$(curl "${curl_base[@]}" --resolve "$metrics_resolve" "$metrics_url") || healthy=false
  "$NILKET_BIN" toron sql --endpoint 127.0.0.1:7742 "SELECT 1" >/dev/null 2>&1 || queryable=false

  heartbeats=$(metric_value "$metrics_body" wendelon_cdc_locality_heartbeats_committed_total)
  lag=$(metric_value "$metrics_body" wendelon_audit_materialization_lag_entries)
  heartbeat_delta=$((heartbeats - previous_heartbeats))
  previous_heartbeats=$heartbeats

  audit_end=$(wc -c <"$audit_path")
  delta_path="$OUTPUT_DIR/audit-delta.ndjson"
  if (( audit_end > audit_offset )); then
    tail -c "+$((audit_offset + 1))" "$audit_path" >"$delta_path"
  else
    : >"$delta_path"
  fi
  audit_bytes=$((audit_end - audit_offset))
  audit_offset=$audit_end
  audit_records=$(wc -l <"$delta_path")
  audit_operations=$(jq -s 'map(.operation) | group_by(.) | map({(.[0]): length}) | add // {}' "$delta_path")
  heartbeat_audits=$(jq -s '[.[] | select(.operation == "data.write_finalized")] | length' "$delta_path")

  wendelon_warns=$(rg -c '^wendelon: .* WARN ' "$OUTPUT_DIR/measured.stderr.log" || true)
  toron_warns=$(rg -c '^toron: .* WARN ' "$OUTPUT_DIR/measured.stderr.log" || true)
  wendelon_warn_delta=$((wendelon_warns - previous_wendelon_warns))
  toron_warn_delta=$((toron_warns - previous_toron_warns))
  previous_wendelon_warns=$wendelon_warns
  previous_toron_warns=$toron_warns
  warn_counts=$(jq -cn \
    --argjson w "$wendelon_warn_delta" --argjson t "$toron_warn_delta" \
    '({} + (if $w > 0 then {wendelon:$w} else {} end) + (if $t > 0 then {toron:$t} else {} end))')
  now_ns=$(date +%s%N)
  elapsed_millis=$(((now_ns - started_ns) / 1000000))

  jq -cn \
    --argjson elapsed "$elapsed_millis" \
    --argjson interval "$HEARTBEAT_INTERVAL_MILLIS" \
    --argjson heartbeat_delta "$heartbeat_delta" \
    --argjson audit_bytes "$audit_bytes" \
    --argjson audit_records "$audit_records" \
    --argjson operations "$audit_operations" \
    --argjson heartbeat_audits "$heartbeat_audits" \
    --argjson lag "$lag" \
    --argjson healthy "$healthy" \
    --argjson queryable "$queryable" \
    --argjson warns "$warn_counts" \
    '{elapsed_millis:$elapsed, heartbeat_interval_millis:$interval, locality_count:1,
      committed_heartbeat_delta:$heartbeat_delta, audit_byte_delta:$audit_bytes,
      audit_record_delta:$audit_records, audit_operations:$operations,
      heartbeat_data_write_audit_records:$heartbeat_audits,
      audit_materialization_lag_entries:$lag, healthy:$healthy,
      queryable:$queryable, warn_counts:$warns}' >>"$samples"
done

stop_owned
trap - EXIT INT TERM
rm -f "$OUTPUT_DIR/audit-delta.ndjson"

# Nilket's startup readiness polling is also owned by this runner. Record its
# successful probe count explicitly from the audit evidence rather than hiding
# it in the total budget.
ready_probes=$(jq -s '[.[].audit_operations["observability.ready"] // 0] | add // 0' "$samples")
metrics_probes=$((sample_count + 1))
measured_duration_millis=$(jq -s 'last.elapsed_millis' "$samples")
jq -n \
  --arg mode local_idle \
  --argjson duration "$measured_duration_millis" \
  --argjson interval "$HEARTBEAT_INTERVAL_MILLIS" \
  --argjson ready "$ready_probes" \
  --argjson metrics "$metrics_probes" \
  --argjson queries "$sample_count" \
  '{format_version:1, mode:$mode, duration_millis:$duration,
    heartbeat_interval_millis:$interval, locality_count:1,
    wendelon_process_starts:1, ready_probe_count:$ready,
    metrics_probe_count:$metrics, query_probe_count:$queries}' \
  >"$OUTPUT_DIR/soak_config.json"

cargo run -q -p wendelon --example local_idle_soak -- --evidence-dir "$OUTPUT_DIR"
echo "local-idle soak artifacts written to $OUTPUT_DIR"
