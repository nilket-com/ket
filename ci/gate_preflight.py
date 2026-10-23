#!/usr/bin/env python3
"""Check new history before starting the pinned advisory evaluator."""
import argparse
import json
import subprocess
import sys
from pathlib import Path

from commit_subjects import HistoryError, evaluate


def run(repo, commit, advisory_args):
    try:
        report = evaluate(repo, commit)
    except (HistoryError, OSError, UnicodeError, subprocess.TimeoutExpired) as error:
        report = {"schema": 1, "verdict": "HOLD", "error": str(error)}
    print(json.dumps({"commit_preflight": report}), flush=True)
    if report["verdict"] != "PASS":
        return 1 if report["verdict"] == "FAIL" else 2
    # One exact candidate flows through both checks; callers cannot override it.
    allowed = {"--run", "--tool", "--mode", "--database", "--at",
               "--watch-state", "--historical-empty-allowlist"}
    if any(arg.startswith("-") and arg.split("=", 1)[0] not in allowed
           for arg in advisory_args):
        print("unsupported advisory option or source-pin override", file=sys.stderr)
        return 2
    gate = Path(__file__).resolve().parent / "advisories" / "gate.py"
    return subprocess.run(
        [sys.executable, "-B", str(gate), "--repo", str(Path(repo).resolve()),
         "--commit", report["candidate"], *advisory_args],
        check=False,
    ).returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("advisory_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    tail = args.advisory_args
    if tail[:1] == ["--"]:
        tail = tail[1:]
    return run(args.repo, args.commit, tail)


if __name__ == "__main__":
    raise SystemExit(main())
