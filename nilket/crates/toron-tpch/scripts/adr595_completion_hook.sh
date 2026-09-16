#!/bin/sh
# ADR 595: runs in ExecStopPost of the measurement service, after it has stopped for any reason.
# Writes the completion record beside the output directory and sends the same record as one UDP
# datagram to the workstation's listener. A message, not a login: no session opens on strix.
#
# usage: adr595_completion_hook.sh <marker path> <listener host> <listener port>
# environment (from the transient service): ADR595_ATTEMPT_TOKEN, SERVICE_RESULT, EXIT_CODE, EXIT_STATUS
set -u
marker="$1"
host="$2"
port="$3"
record="adr595 token=${ADR595_ATTEMPT_TOKEN:-missing} result=${SERVICE_RESULT:-unknown} exit_code=${EXIT_CODE:-unknown} exit_status=${EXIT_STATUS:-unknown} unix_ms=$(date +%s%N | cut -c1-13)"
printf '%s\n' "$record" > "$marker"
# bash opens /dev/udp; a POSIX sh may not, so the datagram is sent through bash explicitly.
bash -c 'printf "%s\n" "$1" > "/dev/udp/$2/$3"' _ "$record" "$host" "$port" 2>>"$marker.send.err" || printf 'send failed\n' >> "$marker"
exit 0
