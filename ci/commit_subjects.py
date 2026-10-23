#!/usr/bin/env python3
"""Read-only commit, identity, ADR lineage and decision-size preflight."""
import argparse
import json
import re
import subprocess
from pathlib import Path

IDENTITY = ("Joel Bondurant", "em@joelbondurant.com")
SUMMARY = r"[a-z0-9][\x20-\x7e]*"
ADR = re.compile(r"^(plan|impl|docs): adr ([0-9]{4}) (" + SUMMARY + r")$")
PATCH = re.compile(r"^(fix|test|docs): (" + SUMMARY + r")$")
LABELS = re.compile(r"\b(?:cut [a-z][0-9]*|e[0-9]+(?:[._-][0-9]+)*|(?:crypto|pd|f-own|f-ctr)[-_][0-9]+)\b", re.I)
LIMITS = "ci/commit-subject-allowlist.json"


class HistoryError(Exception):
    pass


def git(repo, *args):
    result = subprocess.run(
        ["git", "--no-replace-objects", "-C", str(repo), *args],
        capture_output=True, text=True, encoding="utf-8", errors="strict", timeout=60,
    )
    if result.returncode:
        raise HistoryError(result.stderr.strip() or "git command failed")
    return result.stdout


def resolve(repo, ref):
    return git(repo, "rev-parse", "--verify", "--end-of-options", ref + "^{commit}").strip()


def metadata(repo, commit):
    values = git(repo, "show", "-s", "--format=%H%x00%P%x00%an%x00%ae%x00%cn%x00%ce", commit).rstrip("\n").split("\0")
    if len(values) != 6 or values[0] != commit:
        raise HistoryError("malformed commit metadata: " + commit)
    message = git(repo, "show", "-s", "--format=%B", commit)
    return {"commit": commit, "parents": values[1].split(),
            "author": values[2:4], "committer": values[4:6],
            "subject": message.split("\n", 1)[0],
            "has_body": bool(message.partition("\n")[2].strip())}


def summary_error(summary):
    if not re.fullmatch(SUMMARY, summary):
        return "summary must start lowercase or a digit; remainder must be printable ASCII"
    if "(" in summary or ")" in summary:
        return "parentheses are forbidden"
    return None


def subject_error(subject):
    if "(" in subject or ")" in subject:
        return "scopes and parentheses are forbidden"
    match = ADR.fullmatch(subject) or PATCH.fullmatch(subject)
    if not match:
        return "expected plan/impl/docs: adr NNNN summary or fix/test/docs: summary"
    if subject.startswith("docs: adr ") and not ADR.fullmatch(subject):
        return "numbered docs subject must name a four-digit ADR"
    if LABELS.search(subject):
        return "delivery cut or incident labels are forbidden"
    if re.match(r"^docs: (?:record|reconcile|register|close)\b.*\bevidence\b", subject, re.I):
        return "separate execution-evidence subjects are forbidden"
    summary = match.groups()[-1]
    return summary_error(summary)


def adr_path(number=None):
    number = re.escape(number) if number is not None else r"([0-9]{4})"
    return re.compile(r"(?:plans/product/adr/|(?:[^/]+/)*plans/adr/)" +
                      number + r"[_-][^/]+(?<!_evidence)\.md$")


def documents(repo, commit, number):
    return [p for p in git(repo, "ls-tree", "-r", "--name-only", commit).splitlines()
            if adr_path(number).fullmatch(p)]


def changed(repo, commit):
    return [line.partition("\t")[::2] for line in
            git(repo, "diff-tree", "--no-commit-id", "--name-status", "--root",
                "--no-renames", "-r", commit).splitlines()]


def first_parent_subjects(repo, commit):
    return [line.partition("\0")[::2] for line in
            git(repo, "log", "--first-parent", "--format=%H%x00%s", commit).splitlines()]


def earlier_plan(repo, parents, number):
    if parents:
        for commit, subject in first_parent_subjects(repo, parents[0]):
            m = ADR.fullmatch(subject)
            if m and m.group(1) == "plan" and m.group(2) == number:
                raw = ADR.fullmatch(metadata(repo, commit)["subject"])
                docs = documents(repo, commit, number)
                if raw and raw.group(1) == "plan" and raw.group(2) == number and docs:
                    return {"commit": commit, "documents": docs}
    return None


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise HistoryError("duplicate policy key: " + key)
        result[key] = value
    return result


def limits(repo, head):
    if not git(repo, "ls-tree", "--name-only", head, "--", LIMITS).strip():
        return {}, {}
    try:
        data = json.loads(git(repo, "show", head + ":" + LIMITS), object_pairs_hook=unique_object)
        if set(data) != {"schema", "oversize", "impl_limits"} or type(data["schema"]) is not int or data["schema"] != 1:
            raise ValueError("policy schema")
        oversize, counts = {}, {}
        for entry in data["oversize"]:
            if set(entry) != {"path", "blob", "reason", "reviewer"}:
                raise ValueError("oversize fields")
            if not all(isinstance(entry[k], str) and entry[k].strip() for k in entry):
                raise ValueError("empty oversize field")
            if not adr_path().fullmatch(entry["path"]) or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", entry["blob"]):
                raise ValueError("oversize identity")
            key = entry["path"], entry["blob"]
            if key in oversize:
                raise ValueError("duplicate oversize entry")
            oversize[key] = entry
        for entry in data["impl_limits"]:
            if set(entry) != {"adr", "max_impl", "reason", "reviewer"}:
                raise ValueError("impl limit fields")
            if not isinstance(entry["adr"], str) or not re.fullmatch(r"[0-9]{4}", entry["adr"]):
                raise ValueError("impl ADR number")
            if type(entry["max_impl"]) is not int or entry["max_impl"] < 5:
                raise ValueError("impl limit")
            if not all(isinstance(entry[k], str) and entry[k].strip() for k in ["reason", "reviewer"]):
                raise ValueError("empty impl review")
            if entry["adr"] in counts:
                raise ValueError("duplicate impl limit")
            counts[entry["adr"]] = entry
        return oversize, counts
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        raise HistoryError("invalid commit/ADR limit policy: " + str(error)) from error


def evaluate(repo, candidate):
    repo = Path(repo).resolve()
    base, head = resolve(repo, "refs/remotes/origin/main"), resolve(repo, candidate)
    if git(repo, "rev-parse", "--is-shallow-repository").strip() != "false":
        raise HistoryError("complete history required; shallow repository")
    if git(repo, "merge-base", base, head).strip() != base:
        raise HistoryError("origin/main is not an ancestor of candidate")
    oversize, count_limits = limits(repo, head)
    commits = git(repo, "rev-list", "--reverse", "--topo-order", base + ".." + head).splitlines()
    checked = []
    for commit in commits:
        item = metadata(repo, commit)
        errors = []
        error = subject_error(item["subject"])
        if error:
            errors.append(error)
        if item["has_body"]:
            errors.append("commit messages must have no body")
        for role in ["author", "committer"]:
            if tuple(item[role]) != IDENTITY:
                errors.append(role + " must be Joel Bondurant <em@joelbondurant.com>")
        match = ADR.fullmatch(item["subject"])
        if match and not error:
            kind, number, _ = match.groups()
            current = documents(repo, commit, number)
            if kind == "plan":
                changes = changed(repo, commit)
                if len(changes) != 1 or changes[0][0] not in {"A", "M"} or not adr_path(number).fullmatch(changes[0][1]):
                    errors.append("plan must add or amend exactly one ADR Markdown file and nothing else")
                else:
                    status, path = changes[0]
                    item["adr_document"] = path
                    blob = git(repo, "rev-parse", commit + ":" + path).strip()
                    lines = len(git(repo, "show", commit + ":" + path).splitlines())
                    item["adr_lines"] = lines
                    if lines > 400 and (path, blob) not in oversize:
                        errors.append("ADR exceeds 400 lines without an exact reviewed path/blob exception")
                    if status == "A" and (int(number) < 630 or len(current) != 1):
                        errors.append("new ADR requires number >=0630 and repository-wide uniqueness")
            elif kind == "impl":
                plan = earlier_plan(repo, item["parents"], number)
                if plan is None:
                    errors.append("implementation requires an earlier plan and ADR file in first-parent ancestry")
                else:
                    item["plan"] = plan
                if not current:
                    errors.append("ADR file must exist at implementation commit")
                total = sum(bool(m and m.group(1) == "impl" and m.group(2) == number)
                            for _, s in first_parent_subjects(repo, commit)
                            for m in [ADR.fullmatch(s)])
                limit = count_limits.get(number, {}).get("max_impl", 5)
                item["impl_count"], item["impl_limit"] = total, limit
                if total > limit:
                    errors.append("ADR exceeds five implementations without a reviewed count exception")
                paths = [path for _, path in changed(repo, commit)]
                if paths and all(path.endswith("_evidence.md") for path in paths):
                    errors.append("separate evidence-only implementation commit is forbidden")
            elif not current:
                errors.append("numbered docs require an ADR file at that commit")
        checked.append({**item, "errors": errors})
    return {"schema": 1, "base_ref": "refs/remotes/origin/main", "base": base,
            "candidate": head, "commits": checked,
            "verdict": "FAIL" if any(item["errors"] for item in checked) else "PASS"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    try:
        report = evaluate(args.repo, args.commit)
    except (HistoryError, OSError, UnicodeError, subprocess.TimeoutExpired) as error:
        report = {"schema": 1, "verdict": "HOLD", "error": str(error)}
    print(json.dumps(report, indent=2))
    return {"PASS": 0, "FAIL": 1, "HOLD": 2}[report["verdict"]]


if __name__ == "__main__":
    raise SystemExit(main())
