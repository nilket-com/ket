#!/usr/bin/env python3
"""Render a read-only seed-retention advisory report.

The report is intentionally advisory only: it never modifies seed files.
Retirement remains an explicit PR-only action.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
from pathlib import Path
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request

PASS_THRESHOLD = 1000
CLOSED_AGE_THRESHOLD_DAYS = 90
SEED_DIRS = {
    "sim": Path("tests/sim/seeds"),
    "chaos": Path("tests/chaos/seeds"),
}
WORKFLOW_FILES = {
    "sim": ["rust-ci.yml", "simulator-nightly.yml", "simulator-weekly.yml"],
    # Count the standard PR and scheduled smoke lanes. Exclude the
    # self-hosted `chaos-nightly.yml` lane because it is manual-only and
    # would skew the consecutive-pass count with human-triggered runs.
    "chaos": ["rust-ci.yml", "simulator-nightly.yml"],
}


@dataclasses.dataclass(frozen=True)
class SeedRecord:
    harness: str
    issue: int
    scenario: str
    introduced_at: dt.date
    path: Path


@dataclasses.dataclass(frozen=True)
class WorkflowRun:
    workflow: str
    created_at: dt.datetime
    conclusion: str | None
    html_url: str


@dataclasses.dataclass(frozen=True)
class IssueState:
    number: int
    title: str
    html_url: str
    state: str
    closed_at: dt.datetime | None


@dataclasses.dataclass(frozen=True)
class SeedEvaluation:
    seed: SeedRecord
    issue: IssueState
    consecutive_passes: int
    closed_age_days: int | None
    eligible: bool
    blockers: list[str]
    last_blocking_run: WorkflowRun | None


class GitHubClient:
    def __init__(self, token: str | None) -> None:
        self.token = token

    def get_json(self, url: str) -> dict:
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "wendelon-seed-retention-advisory",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise SystemExit(f"GitHub API request failed: {exc.code} {url}\n{body}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, help="owner/repo")
    parser.add_argument("--default-branch", default="", help="default branch override")
    parser.add_argument("--output", required=True, help="markdown report path")
    return parser.parse_args()


def parse_date(raw: str) -> dt.date:
    return dt.date.fromisoformat(raw)


def parse_datetime(raw: str | None) -> dt.datetime | None:
    if raw is None:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    return dt.datetime.fromisoformat(raw).astimezone(dt.timezone.utc)


def load_seed_records() -> list[SeedRecord]:
    records: list[SeedRecord] = []
    for harness, directory in SEED_DIRS.items():
        if not directory.exists():
            continue
        for path in sorted(directory.glob("*.seed")):
            with path.open("rb") as handle:
                payload = tomllib.load(handle)
            records.append(
                SeedRecord(
                    harness=harness,
                    issue=int(payload["issue"]),
                    scenario=str(payload["scenario"]),
                    introduced_at=parse_date(str(payload["introduced_at"])),
                    path=path,
                )
            )
    return records


def resolve_default_branch(
    client: GitHubClient, repo: str, requested_branch: str
) -> str:
    if requested_branch:
        return requested_branch
    response = client.get_json(f"https://api.github.com/repos/{repo}")
    return str(response["default_branch"])


def fetch_issue_state(client: GitHubClient, repo: str, number: int) -> IssueState:
    payload = client.get_json(f"https://api.github.com/repos/{repo}/issues/{number}")
    return IssueState(
        number=number,
        title=str(payload["title"]),
        html_url=str(payload["html_url"]),
        state=str(payload["state"]),
        closed_at=parse_datetime(payload.get("closed_at")),
    )


def fetch_workflow_runs(
    client: GitHubClient,
    repo: str,
    default_branch: str,
    workflow_file: str,
    earliest_date: dt.date,
) -> list[WorkflowRun]:
    runs: list[WorkflowRun] = []
    page = 1
    while True:
        params = urllib.parse.urlencode(
            {
                "branch": default_branch,
                "status": "completed",
                "per_page": 100,
                "page": page,
            }
        )
        payload = client.get_json(
            f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/runs?{params}"
        )
        batch = payload.get("workflow_runs", [])
        if not batch:
            break
        stop = False
        for item in batch:
            created_at = parse_datetime(item["created_at"])
            if created_at is None:
                continue
            if created_at.date() < earliest_date:
                stop = True
                continue
            runs.append(
                WorkflowRun(
                    workflow=workflow_file,
                    created_at=created_at,
                    conclusion=item.get("conclusion"),
                    html_url=str(item["html_url"]),
                )
            )
        if stop:
            break
        page += 1
    return runs


def count_consecutive_passes(runs: list[WorkflowRun]) -> tuple[int, WorkflowRun | None]:
    ordered = sorted(runs, key=lambda run: run.created_at, reverse=True)
    count = 0
    for run in ordered:
        if run.conclusion == "success":
            count += 1
            continue
        return count, run
    return count, None


def evaluate_seed(
    seed: SeedRecord,
    issue: IssueState,
    all_runs: dict[str, list[WorkflowRun]],
    today: dt.date,
) -> SeedEvaluation:
    relevant_runs: list[WorkflowRun] = []
    for workflow_file in WORKFLOW_FILES[seed.harness]:
        relevant_runs.extend(
            run
            for run in all_runs.get(workflow_file, [])
            if run.created_at.date() >= seed.introduced_at
        )
    consecutive_passes, last_blocking_run = count_consecutive_passes(relevant_runs)
    blockers: list[str] = []

    if issue.state != "closed" or issue.closed_at is None:
        blockers.append("issue still open")
        closed_age_days = None
    else:
        closed_age_days = (today - issue.closed_at.date()).days
        if closed_age_days < CLOSED_AGE_THRESHOLD_DAYS:
            blockers.append(
                f"issue closed for {closed_age_days}d (< {CLOSED_AGE_THRESHOLD_DAYS}d)"
            )

    if consecutive_passes < PASS_THRESHOLD:
        blockers.append(
            f"{consecutive_passes} consecutive relevant workflow passes (< {PASS_THRESHOLD})"
        )

    return SeedEvaluation(
        seed=seed,
        issue=issue,
        consecutive_passes=consecutive_passes,
        closed_age_days=closed_age_days,
        eligible=not blockers,
        blockers=blockers,
        last_blocking_run=last_blocking_run,
    )


def escape_table(raw: str) -> str:
    return raw.replace("|", "\\|").replace("\n", " ")


def render_rows(rows: list[SeedEvaluation]) -> list[str]:
    if not rows:
        return ["_None._"]
    lines = [
        "| Harness | Seed | Issue | Scenario | Closed Age | Consecutive Passes | Notes |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for row in rows:
        closed_age = "-" if row.closed_age_days is None else str(row.closed_age_days)
        notes = "eligible"
        if row.blockers:
            notes = "; ".join(row.blockers)
        if row.last_blocking_run is not None:
            notes = (
                f"{notes}; last blocking run: "
                f"[{escape_table(row.last_blocking_run.workflow)}]"
                f"({row.last_blocking_run.html_url})"
            )
        lines.append(
            "| {harness} | `{seed_file}` | [#{issue}]({issue_url}) {title} | `{scenario}` | "
            "{closed_age} | {passes} | {notes} |".format(
                harness=row.seed.harness,
                seed_file=escape_table(row.seed.path.name),
                issue=row.issue.number,
                issue_url=row.issue.html_url,
                title=escape_table(row.issue.title),
                scenario=escape_table(row.seed.scenario),
                closed_age=closed_age,
                passes=row.consecutive_passes,
                notes=notes,
            )
        )
    return lines


def render_report(
    repo: str,
    default_branch: str,
    evaluations: list[SeedEvaluation],
    generated_at: dt.datetime,
) -> str:
    eligible = [row for row in evaluations if row.eligible]
    blocked = [row for row in evaluations if not row.eligible]
    lines = [
        "# Seed Retention Advisory",
        "",
        f"Generated: `{generated_at.isoformat()}`",
        f"Repository: `{repo}`",
        f"Default branch: `{default_branch}`",
        "",
        "Policy bar:",
        f"- Issue closed for at least `{CLOSED_AGE_THRESHOLD_DAYS}` days.",
        f"- At least `{PASS_THRESHOLD}` consecutive successful relevant workflow runs since the seed was introduced.",
        "- Retirement remains PR-only; this report is advisory only.",
        "",
    ]
    if not evaluations:
        lines.extend(
            [
                "No frozen simulator or chaos seeds are present, so there are no retirement candidates.",
                "",
            ]
        )
        return "\n".join(lines)

    lines.extend(["## Eligible Now", ""])
    lines.extend(render_rows(eligible))
    lines.extend(["", "## Not Yet Eligible", ""])
    lines.extend(render_rows(blocked))
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    output = Path(args.output)
    seeds = load_seed_records()
    generated_at = dt.datetime.now(dt.timezone.utc)

    if not seeds:
        report = render_report(args.repo, args.default_branch or "-", [], generated_at)
        output.write_text(report + "\n", encoding="utf-8")
        print(report)
        return 0

    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN or GH_TOKEN is required when frozen seeds exist")

    client = GitHubClient(token)
    default_branch = resolve_default_branch(client, args.repo, args.default_branch)
    earliest_date = min(seed.introduced_at for seed in seeds)
    workflow_runs: dict[str, list[WorkflowRun]] = {}
    for workflow_file in sorted({name for names in WORKFLOW_FILES.values() for name in names}):
        workflow_runs[workflow_file] = fetch_workflow_runs(
            client, args.repo, default_branch, workflow_file, earliest_date
        )

    today = generated_at.date()
    evaluations = [
        evaluate_seed(
            seed,
            fetch_issue_state(client, args.repo, seed.issue),
            workflow_runs,
            today,
        )
        for seed in seeds
    ]
    report = render_report(args.repo, default_branch, evaluations, generated_at)
    output.write_text(report + "\n", encoding="utf-8")
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
