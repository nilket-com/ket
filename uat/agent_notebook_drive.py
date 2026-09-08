#!/usr/bin/env python3
"""Drive the PRODUCT-0162 journey exclusively through polariton act receipts."""
import argparse
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--binary", required=True)
parser.add_argument("--window", required=True)
parser.add_argument("--data", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=False)
sequence = 0


def act(command):
    global sequence
    sequence += 1
    commands = args.output / f"{sequence:02d}-commands.json"
    commands.write_text(json.dumps({"commands": [command]}) + "\n")
    process = subprocess.run(
        [args.binary, "act", args.window, "--commands", str(commands)],
        capture_output=True, text=True, timeout=90,
    )
    (args.output / f"{sequence:02d}-events.ndjson").write_text(process.stdout)
    (args.output / f"{sequence:02d}-stderr.txt").write_text(process.stderr)
    events = [json.loads(line) for line in process.stdout.splitlines() if line.strip()]
    final = [e for e in events if e.get("id") == 1 and (
        e["event"] in {"completed", "refused", "failed", "superseded", "unknown"}
        or (e["event"] == "applied" and e.get("terminal"))
    )]
    if process.returncode or not final or final[-1]["event"] in {"refused", "failed", "superseded", "unknown"}:
        raise SystemExit(f"Action {sequence} stopped; inspect {args.output}. No action was replayed.")
    receipt = final[-1]["receipt"]
    if receipt["outcome"] != "applied":
        raise SystemExit(f"Action {sequence} did not apply: {receipt}")
    return receipt


def inspect():
    return act({"describe_notebook": {}})["notebook"]


def target(notebook, name):
    cells = [c for c in notebook["cells"] if c.get("name") == name]
    if len(cells) != 1:
        raise SystemExit(f"Need exactly one cell named {name}")
    return {"buffer": notebook["buffer"], "generation": notebook["generation"], "cell": cells[0]["cell"]}


def run(name):
    notebook = inspect()
    return act({"run_cell": {"target": target(notebook, name)}})


run("data")
example = run("plot")
assert example["figures"], "The example must produce a projected figure"
opened = act({"open_data": {"path": str(args.data.resolve())}})["data"]
numeric = [name for name, kind in opened["columns"] if kind.startswith(("i8", "i16", "i32", "i64", "i128", "u8", "u16", "u32", "u64", "f32", "f64", "decimal"))]
if len(numeric) < 2:
    raise SystemExit("Choose a file with at least two numeric columns for this journey")


def quote(value):
    return '"' + value.replace('"', '""') + '"'


previous_figure = example["figures"][0]["figure"]
for multiplier in (1, 2):
    notebook = inspect()
    sql = f"SELECT {quote(numeric[0])} AS x, {quote(numeric[1])} * {multiplier} AS y FROM {quote(opened['table'])};\n"
    act({"replace_cell": {"target": target(notebook, "data"), "source": sql}})
    run("data")
    result = run("plot")
    assert result["figures"], "Each run must complete with its figure projected"
    current_figure = result["figures"][0]["figure"]
    assert current_figure != previous_figure, "Completion must carry the new plot incarnation"
    previous_figure = current_figure
act({"describe": {}})
published = act({"publish": {"directory": str((args.output / "package").resolve())}})
assert published.get("publish"), "Publisher must return a package receipt"
print(f"Completed {sequence} acknowledged actions. Receipts and package: {args.output}")
