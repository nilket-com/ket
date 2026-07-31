#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Usage:
  run_node_replacement_contract.sh \
    --trigger-source <source> \
    --failed-node-id <id> \
    --replacement-node-id <id> \
    --replacement-config <path> \
    --remove-member-cmd <cmd> \
    --provision-replacement-cmd <cmd> \
    --start-replacement-cmd <cmd> \
    --membership-check-cmd <cmd> \
    --ready-check-cmd <cmd> \
    --catch-up-check-cmd <cmd> \
    [--backup-source-node-id <id>] \
    [--backup-artifact <path>] \
    [--remove-timeout-sec <n>] \
    [--provision-timeout-sec <n>] \
    [--start-timeout-sec <n>] \
    [--membership-timeout-sec <n>] \
    [--ready-timeout-sec <n>] \
    [--catch-up-timeout-sec <n>] \
    [--poll-interval-sec <n>] \
    [--output-dir <dir>]

This script is a fallback external-automation harness for the H4
remove-and-replace contract. It does not provision machines or speak
Wendelon-native admin RPCs itself; instead it executes the operator's
supplied commands in the required order and enforces timing / verification.
EOF
}

die() {
	echo "replacement-contract: $*" >&2
	exit 2
}

timestamp() {
	date -u +"%Y-%m-%dT%H:%M:%SZ"
}

shell_quote() {
	printf "%q" "$1"
}

require_tool() {
	command -v "$1" >/dev/null 2>&1 || die "required tool not found in PATH: $1"
}

run_once() {
	local label="$1"
	local cmd="$2"
	local timeout_sec="$3"
	local started_at
	local ended_at
	local status

	started_at="$(timestamp)"
	echo "[$started_at] $label: start"
	if timeout "${timeout_sec}s" bash -c "$cmd"; then
		status="complete"
	else
		status="$?"
		ended_at="$(timestamp)"
		if [[ "$status" == "124" ]]; then
			echo "[$ended_at] $label: timeout after ${timeout_sec}s"
		else
			echo "[$ended_at] $label: failed"
		fi
		return 1
	fi
	ended_at="$(timestamp)"
	echo "[$ended_at] $label: complete"
	return 0
}

poll_until() {
	local label="$1"
	local cmd="$2"
	local timeout_sec="$3"
	local poll_interval_sec="$4"
	local started_epoch
	local now_epoch

	started_epoch="$(date +%s)"
	echo "[$(timestamp)] $label: polling"
	while true; do
		if bash -c "$cmd"; then
			echo "[$(timestamp)] $label: satisfied"
			return 0
		fi
		now_epoch="$(date +%s)"
		if (( now_epoch - started_epoch >= timeout_sec )); then
			echo "[$(timestamp)] $label: timeout after ${timeout_sec}s"
			return 1
		fi
		sleep "$poll_interval_sec"
	done
}

TRIGGER_SOURCE=""
FAILED_NODE_ID=""
REPLACEMENT_NODE_ID=""
REPLACEMENT_CONFIG=""
BACKUP_SOURCE_NODE_ID=""
BACKUP_ARTIFACT=""
REMOVE_MEMBER_CMD=""
PROVISION_REPLACEMENT_CMD=""
START_REPLACEMENT_CMD=""
MEMBERSHIP_CHECK_CMD=""
READY_CHECK_CMD=""
CATCH_UP_CHECK_CMD=""
REMOVE_TIMEOUT_SEC=60
PROVISION_TIMEOUT_SEC=600
START_TIMEOUT_SEC=120
MEMBERSHIP_TIMEOUT_SEC=120
READY_TIMEOUT_SEC=300
CATCH_UP_TIMEOUT_SEC=300
POLL_INTERVAL_SEC=5
OUTPUT_DIR=""
__rc_step="init"
__rc_success=0

while (($# > 0)); do
	case "$1" in
		--trigger-source)
			TRIGGER_SOURCE="$2"
			shift 2
			;;
		--failed-node-id)
			FAILED_NODE_ID="$2"
			shift 2
			;;
		--replacement-node-id)
			REPLACEMENT_NODE_ID="$2"
			shift 2
			;;
		--replacement-config)
			REPLACEMENT_CONFIG="$2"
			shift 2
			;;
		--backup-source-node-id)
			BACKUP_SOURCE_NODE_ID="$2"
			shift 2
			;;
		--backup-artifact)
			BACKUP_ARTIFACT="$2"
			shift 2
			;;
		--remove-member-cmd)
			REMOVE_MEMBER_CMD="$2"
			shift 2
			;;
		--provision-replacement-cmd)
			PROVISION_REPLACEMENT_CMD="$2"
			shift 2
			;;
		--start-replacement-cmd)
			START_REPLACEMENT_CMD="$2"
			shift 2
			;;
		--membership-check-cmd)
			MEMBERSHIP_CHECK_CMD="$2"
			shift 2
			;;
		--ready-check-cmd)
			READY_CHECK_CMD="$2"
			shift 2
			;;
		--catch-up-check-cmd)
			CATCH_UP_CHECK_CMD="$2"
			shift 2
			;;
		--remove-timeout-sec)
			REMOVE_TIMEOUT_SEC="$2"
			shift 2
			;;
		--provision-timeout-sec)
			PROVISION_TIMEOUT_SEC="$2"
			shift 2
			;;
		--start-timeout-sec)
			START_TIMEOUT_SEC="$2"
			shift 2
			;;
		--membership-timeout-sec)
			MEMBERSHIP_TIMEOUT_SEC="$2"
			shift 2
			;;
		--ready-timeout-sec)
			READY_TIMEOUT_SEC="$2"
			shift 2
			;;
		--catch-up-timeout-sec)
			CATCH_UP_TIMEOUT_SEC="$2"
			shift 2
			;;
		--poll-interval-sec)
			POLL_INTERVAL_SEC="$2"
			shift 2
			;;
		--output-dir)
			OUTPUT_DIR="$2"
			shift 2
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			die "unknown argument: $1"
			;;
	esac
done

require_tool timeout

[[ -n "$TRIGGER_SOURCE" ]] || die "--trigger-source is required"
[[ -n "$FAILED_NODE_ID" ]] || die "--failed-node-id is required"
[[ -n "$REPLACEMENT_NODE_ID" ]] || die "--replacement-node-id is required"
[[ -n "$REPLACEMENT_CONFIG" ]] || die "--replacement-config is required"
[[ -n "$REMOVE_MEMBER_CMD" ]] || die "--remove-member-cmd is required"
[[ -n "$PROVISION_REPLACEMENT_CMD" ]] || die "--provision-replacement-cmd is required"
[[ -n "$START_REPLACEMENT_CMD" ]] || die "--start-replacement-cmd is required"
[[ -n "$MEMBERSHIP_CHECK_CMD" ]] || die "--membership-check-cmd is required"
[[ -n "$READY_CHECK_CMD" ]] || die "--ready-check-cmd is required"
[[ -n "$CATCH_UP_CHECK_CMD" ]] || die "--catch-up-check-cmd is required"

if [[ -z "$OUTPUT_DIR" ]]; then
	OUTPUT_DIR="./replacement-contract-$(date -u +%Y%m%dT%H%M%SZ)"
fi

mkdir -p "$OUTPUT_DIR"
REPORT_PATH="$OUTPUT_DIR/report.env"

trap 'if [[ "${__rc_success:-0}" != "1" ]]; then
	{
		echo "completed_at=$(shell_quote "$(timestamp)")"
		echo "status=failed"
		echo "failed_step=$(shell_quote "${__rc_step:-unknown}")"
	} >>"$REPORT_PATH"
fi' EXIT

{
	echo "trigger_source=$(shell_quote "$TRIGGER_SOURCE")"
	echo "failed_node_id=$(shell_quote "$FAILED_NODE_ID")"
	echo "replacement_node_id=$(shell_quote "$REPLACEMENT_NODE_ID")"
	echo "replacement_config=$(shell_quote "$REPLACEMENT_CONFIG")"
	echo "backup_source_node_id=$(shell_quote "$BACKUP_SOURCE_NODE_ID")"
	echo "backup_artifact=$(shell_quote "$BACKUP_ARTIFACT")"
	echo "remove_timeout_sec=$(shell_quote "$REMOVE_TIMEOUT_SEC")"
	echo "provision_timeout_sec=$(shell_quote "$PROVISION_TIMEOUT_SEC")"
	echo "start_timeout_sec=$(shell_quote "$START_TIMEOUT_SEC")"
	echo "membership_timeout_sec=$(shell_quote "$MEMBERSHIP_TIMEOUT_SEC")"
	echo "ready_timeout_sec=$(shell_quote "$READY_TIMEOUT_SEC")"
	echo "catch_up_timeout_sec=$(shell_quote "$CATCH_UP_TIMEOUT_SEC")"
	echo "poll_interval_sec=$(shell_quote "$POLL_INTERVAL_SEC")"
	echo "started_at=$(shell_quote "$(timestamp)")"
} >"$REPORT_PATH"

__rc_step="remove_member"
run_once "remove_member" "$REMOVE_MEMBER_CMD" "$REMOVE_TIMEOUT_SEC"
__rc_step="provision_replacement"
run_once "provision_replacement" "$PROVISION_REPLACEMENT_CMD" "$PROVISION_TIMEOUT_SEC"
__rc_step="start_replacement"
run_once "start_replacement" "$START_REPLACEMENT_CMD" "$START_TIMEOUT_SEC"
__rc_step="membership_check"
poll_until "membership_check" "$MEMBERSHIP_CHECK_CMD" "$MEMBERSHIP_TIMEOUT_SEC" "$POLL_INTERVAL_SEC"
__rc_step="ready_check"
poll_until "ready_check" "$READY_CHECK_CMD" "$READY_TIMEOUT_SEC" "$POLL_INTERVAL_SEC"
__rc_step="catch_up_check"
poll_until "catch_up_check" "$CATCH_UP_CHECK_CMD" "$CATCH_UP_TIMEOUT_SEC" "$POLL_INTERVAL_SEC"

__rc_success=1
{
	echo "completed_at=$(shell_quote "$(timestamp)")"
	echo "status=success"
} >>"$REPORT_PATH"

echo "replacement-contract: success"
echo "replacement-contract: report written to $REPORT_PATH"
