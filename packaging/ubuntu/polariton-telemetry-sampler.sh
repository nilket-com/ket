#!/usr/bin/env bash
# Polariton telemetry sampler: the deployment's own telemetry, landed in Toron.
#
# Dogfood rule: internal telemetry is data, and data lives in Toron — not in CSVs,
# not only in journald. Every sample this script takes becomes rows in the running
# deployment, queryable through the same SQL surface the team ships:
#
#   polariton_process_samples  (sampled_at, process, pid, rss_kb, vsz_kb, fds, threads, cpu_secs)
#   polariton_health_samples   (sampled_at, endpoint, status, latency_ms)
#   polariton_metric_samples   (sampled_at, name, labels, value)
#
# sampled_at is unix seconds. Metric rows are the Wendelon Prometheus scrape,
# flattened one series per row. The sampler is deliberately tolerant: while the
# deployment is down it keeps ticking and retrying rather than exiting, so a soak
# that includes a restart keeps its record.

set -u

NILKET_BIN="${POLARITON_NILKET_BIN:-$HOME/.cargo/bin/nilket}"
PROFILE_ROOT="${POLARITON_PROFILE_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/nilket/local}"
SERVICE_NAME="${POLARITON_SERVICE_NAME:-nilket-local.service}"
INTERVAL_SECS="${POLARITON_TELEMETRY_INTERVAL_SECS:-60}"
# Metrics are scraped every Nth tick; process and health samples land every tick.
METRICS_EVERY_TICKS="${POLARITON_TELEMETRY_METRICS_EVERY_TICKS:-5}"
METRICS_URL="${POLARITON_METRICS_URL:-https://127.0.0.1:7433/metrics}"
# Curated by default while the second-flush read-wedge defect is open (see
# plans/impl/polariton_soak_findings_20260807.md): a full scrape is ~2,300 rows and a
# table's second multi-thousand-row ingest cycle silently becomes write-only. The
# curated set (~70 rows) keeps the viewport's key series queryable; set the filter to
# '.' to ingest the full scrape — which currently doubles as the reproduction harness.
METRICS_NAME_FILTER="${POLARITON_METRICS_NAME_FILTER:-^wendelon_(raft_|liveness_|lsm_|metrics_scrapes|cdc_|storage_secondary_index_(coverage_gap_slabs|lookup_refusals_total)$)}"
HEALTH_URL="${POLARITON_HEALTH_URL:-http://127.0.0.1:7431/health}"
READY_URL="${POLARITON_READY_URL:-https://127.0.0.1:7432/ready}"
PKI_DIR="$PROFILE_ROOT/pki"
CLK_TCK="$(getconf CLK_TCK 2>/dev/null || echo 100)"
BATCH_ROWS=50

sql() {
	# One statement, quietly; the caller decides whether failure matters. TERM with a
	# KILL backstop, never KILL first: this sampler's own first hour proved that a
	# client killed mid-ingest can wedge the target table's storage (reads fail with
	# "storage unavailable" and the LSM compaction service loops on a missing slab).
	timeout -k 5 -s TERM 30 "$NILKET_BIN" toron sql "$1" >/dev/null 2>&1
}

# Probe existence rather than matching error strings. Attempting the DDL and
# classifying the failure text is brittle: once a schema generation advances (a
# secondary index declaration is enough), `CREATE TABLE` on an existing table is
# rejected with a generation mismatch rather than "already exists". That text did
# not match, `tables_ready` stayed 0, and the sampler then silently sampled nothing
# for as long as it ran. A read probe cannot be fooled that way.
table_exists() {
	timeout -k 5 -s TERM 30 "$NILKET_BIN" toron sql "SELECT * FROM $1 LIMIT 0" >/dev/null 2>&1
}

ensure_tables() {
	local out name
	for ddl in \
		"CREATE TABLE polariton_process_samples (sampled_at BIGINT, process TEXT, pid BIGINT, rss_kb BIGINT, vsz_kb BIGINT, fds BIGINT, threads BIGINT, cpu_secs DOUBLE)" \
		"CREATE TABLE polariton_health_samples (sampled_at BIGINT, endpoint TEXT, status TEXT, latency_ms DOUBLE)" \
		"CREATE TABLE polariton_metric_samples (sampled_at BIGINT, name TEXT, labels TEXT, value DOUBLE)"; do
		name="${ddl#CREATE TABLE }"
		name="${name%% *}"
		table_exists "$name" && continue
		out="$(timeout -k 5 -s TERM 30 "$NILKET_BIN" toron sql "$ddl" 2>&1)" || {
			case "$out" in
			*"already exists"*) ;;
			*)
				echo "sampler: ensure_tables failed for $name: $out" >&2
				return 1
				;;
			esac
		}
	done
	return 0
}

service_pids() {
	local cgroup="/sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/app.slice/$SERVICE_NAME/cgroup.procs"
	[ -r "$cgroup" ] && cat "$cgroup"
}

classify_process() {
	# The supervisor, storage and compute processes share one binary; the role is
	# in the argv.
	local cmdline
	cmdline="$(tr '\0' ' ' <"/proc/$1/cmdline" 2>/dev/null)"
	case "$cmdline" in
	*" wendelon "*) echo wendelon ;;
	*" toron "*) echo toron ;;
	*" local "*) echo supervisor ;;
	*) echo other ;;
	esac
}

sample_processes() {
	local now="$1" rows="" pid role rss vsz threads fds utime stime cpu
	for pid in $(service_pids); do
		[ -r "/proc/$pid/stat" ] || continue
		role="$(classify_process "$pid")"
		read -r rss vsz threads utime stime <<<"$(awk '{print $24, $23, $20, $14, $15}' "/proc/$pid/stat")"
		rss=$((rss * 4096 / 1024))
		vsz=$((vsz / 1024))
		fds="$(ls "/proc/$pid/fd" 2>/dev/null | wc -l)"
		cpu="$(awk -v u="$utime" -v s="$stime" -v hz="$CLK_TCK" 'BEGIN { printf "%.2f", (u + s) / hz }')"
		rows="$rows,($now,'$role',$pid,$rss,$vsz,$fds,$threads,$cpu)"
	done
	[ -n "$rows" ] && sql "INSERT INTO polariton_process_samples VALUES ${rows#,}"
}

sample_health() {
	local now="$1" code latency
	read -r code latency <<<"$(curl -s -m 5 -o /dev/null -w '%{http_code} %{time_total}' "$HEALTH_URL" 2>/dev/null || echo "000 0")"
	sql "INSERT INTO polariton_health_samples VALUES ($now,'health','$code',$(awk -v t="$latency" 'BEGIN { printf "%.1f", t * 1000 }'))"
	read -r code latency <<<"$(curl -sk -m 5 -o /dev/null -w '%{http_code} %{time_total}' \
		--cert "$PKI_DIR/local-operator.crt" --key "$PKI_DIR/local-operator.key" "$READY_URL" 2>/dev/null || echo "000 0")"
	sql "INSERT INTO polariton_health_samples VALUES ($now,'ready','$code',$(awk -v t="$latency" 'BEGIN { printf "%.1f", t * 1000 }'))"
}

sample_metrics() {
	local now="$1" count=0 rows=""
	while IFS= read -r line; do
		rows="$rows,$line"
		count=$((count + 1))
		if [ "$count" -ge "$BATCH_ROWS" ]; then
			sql "INSERT INTO polariton_metric_samples VALUES ${rows#,}" || return 1
			rows=""
			count=0
		fi
	done < <(curl -sk -m 10 --cert "$PKI_DIR/local-operator.crt" --key "$PKI_DIR/local-operator.key" \
		"$METRICS_URL" 2>/dev/null | awk -v now="$now" -v keep="$METRICS_NAME_FILTER" '
		/^#/ { next }
		{
			value = $NF
			if (value !~ /^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$/) next
			series = substr($0, 1, length($0) - length(value) - 1)
			labels = ""
			name = series
			open = index(series, "{")
			if (open > 0) {
				name = substr(series, 1, open - 1)
				labels = substr(series, open)
			}
			if (name !~ keep) next
			if (name ~ /_bucket$/) next
			gsub(/'\''/, "'\'''\''", labels)
			printf "(%s,'\''%s'\'','\''%s'\'',%s)\n", now, name, labels, value
		}')
	[ -n "$rows" ] && sql "INSERT INTO polariton_metric_samples VALUES ${rows#,}"
	return 0
}

run_sampler() {
	local tick=0 tables_ready=0 now
	while :; do
		now="$(date +%s)"
		if [ "$tables_ready" -eq 0 ]; then
			ensure_tables && tables_ready=1
		fi
		if [ "$tables_ready" -eq 1 ]; then
			sample_processes "$now"
			sample_health "$now"
			if [ $((tick % METRICS_EVERY_TICKS)) -eq 0 ]; then
				sample_metrics "$now"
			fi
		fi
		tick=$((tick + 1))
		sleep "$INTERVAL_SECS"
	done
}

# Keep the functions sourceable for the discriminating sampler controls. Production
# execution still enters the same endless loop; sourcing the file only defines its seams.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	run_sampler
fi
