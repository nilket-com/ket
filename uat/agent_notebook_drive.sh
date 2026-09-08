#!/usr/bin/env bash
# Drive PRODUCT-0162 exclusively through public polariton act receipts.
set -Eeuo pipefail

binary= window= data= output=
data_cell=data plot_cell=plot sequence=0 receipt=
die() { printf '%s\n' "$*" >&2; exit 1; }
while (( $# )); do
    (( $# >= 2 )) || die "Missing value for $1"
    case $1 in
        --binary) binary=$2 ;;
        --window) window=$2 ;;
        --data) data=$2 ;;
        --output) output=$2 ;;
        --data-cell) data_cell=$2 ;;
        --plot-cell) plot_cell=$2 ;;
        *) die "Unknown argument: $1" ;;
    esac
    shift 2
done
[[ -n $binary && -n $window && -n $data && -n $output ]] ||
    die "Required: --binary PATH --window ID --data PATH --output NEW_DIRECTORY"
command -v jq >/dev/null || die 'jq is required'
if command -v timeout >/dev/null; then
    timeout_command=timeout
elif command -v gtimeout >/dev/null; then
    timeout_command=gtimeout
else
    die 'GNU timeout is required (gtimeout from coreutils on macOS)'
fi
[[ -f $data ]] || die "Data file does not exist: $data"
data=$(cd -- "$(dirname -- "$data")" && printf '%s/%s' "$PWD" "$(basename -- "$data")")
mkdir -p -- "$(dirname -- "$output")"
mkdir -- "$output" # Existing evidence must never be overwritten.
output=$(cd -- "$output" && pwd -P)
trap 'printf "Action %s stopped; inspect %s. No action was replayed.\n" "$sequence" "$output" >&2' ERR

# Functions return receipts in this shell: command substitution would lose the
# sequence counter. JSON is always constructed with jq arguments, never eval.
act() {
    sequence=$((sequence + 1))
    local prefix final
    printf -v prefix '%s/%02d' "$output" "$sequence"
    jq -n --argjson command "$1" '{commands: [$command]}' > "$prefix-commands.json"
    "$timeout_command" --kill-after=5s 90s "$binary" act "$window" \
        --commands "$prefix-commands.json" > "$prefix-events.ndjson" 2> "$prefix-stderr.txt"
    final=$(jq -ces '[.[] | select(.id == 1) | select(
        (.event == "applied" and .terminal == true) or
        (.event as $event | ["completed", "refused", "failed", "superseded", "unknown"] | index($event))
    )] | if length == 1 then .[0] else error("Need one final answer") end' "$prefix-events.ndjson")
    receipt=$(jq -ce 'select(.event == "completed" or (.event == "applied" and .terminal == true))
        | .receipt | select(.outcome == "applied")' <<< "$final")
}
inspect() { act '{"describe_notebook":{}}'; notebook=$(jq -ce '.notebook' <<< "$receipt"); }
target() {
    jq -ce --arg name "$1" '
        . as $n | [.cells[] | select(.name == $name)] |
        if length == 1 then {buffer: $n.buffer, generation: $n.generation, cell: .[0].cell}
        else error("Need exactly one cell named " + $name) end' <<< "$notebook"
}
run() {
    inspect
    act "$(jq -n --argjson target "$(target "$1")" '{run_cell: {target: $target}}')"
}
figure() { jq -ce '.figures[0].figure // error("Run must complete with a projected figure")' <<< "$receipt"; }

run "$data_cell"
run "$plot_cell"
previous_figure=$(figure)
act "$(jq -n --arg path "$data" '{open_data: {path: $path}}')"
opened=$(jq -ce '.data' <<< "$receipt")
numeric=$(jq -ce '[.columns[] | select(.[1] | test("^(i8|i16|i32|i64|i128|u8|u16|u32|u64|f32|f64|decimal)")) | .[0]]
    | if length >= 2 then . else error("Choose a file with two numeric columns") end' <<< "$opened")
for multiplier in 1 2; do
    inspect
    # Keep the required trailing newline inside JSON, not shell substitution.
    act "$(jq -n --argjson target "$(target "$data_cell")" --argjson columns "$numeric" \
        --argjson opened "$opened" --arg multiplier "$multiplier" '
        def quote: "\"" + gsub("\""; "\"\"") + "\"";
        {replace_cell: {target: $target, source:
            ("SELECT " + ($columns[0] | quote) + " AS x, " + ($columns[1] | quote) +
             " * " + $multiplier + " AS y FROM " + ($opened.table | quote) + ";\n")}}')"
    run "$data_cell"
    run "$plot_cell"
    current_figure=$(figure)
    [[ $current_figure != "$previous_figure" ]] || die 'Completion must carry a new plot incarnation'
    previous_figure=$current_figure
done
act '{"describe":{}}'
act "$(jq -n --arg directory "$output/package" '{publish: {directory: $directory}}')"
jq -e '.publish != null' <<< "$receipt" >/dev/null
printf 'Completed %s acknowledged actions. Receipts and package: %s\n' "$sequence" "$output"
