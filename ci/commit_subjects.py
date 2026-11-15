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
    number = re.escape(number) if number is not None else r"[0-9]{4}"
    return re.compile(r"(plans/product/adr|(?:[^/]+/)*plans/adr)/(" +
                      number + r")[_-][^/]+(?<!_evidence)\.md$")


ADR_DIRECTORY = re.compile(r"plans/product/adr|(?:[^/]+/)*plans/adr")


def adr_tree(repo, commit, cache):
    """{directory: {number: [paths]}} for the ADR documents at commit that are
    regular files; a tree, link or submodule with an ADR's name is none."""
    key = "tree", commit
    if key not in cache:
        tree = {}
        for entry in git(repo, "ls-tree", "-r", "-z", commit).split("\0"):
            meta, _, path = entry.partition("\t")
            match = adr_path().fullmatch(path)
            if match and meta.split(" ")[:2] in (["100644", "blob"], ["100755", "blob"]):
                tree.setdefault(match.group(1), {}).setdefault(match.group(2), []).append(path)
        cache[key] = tree
    return cache[key]


def directory_name(directory):
    """The name a subject uses for an ADR directory: product, or the
    directory that holds plans/adr (libaryon, toron, wendelon)."""
    if directory == "plans/product/adr":
        return "product"
    prefix = directory[:-len("plans/adr")].rstrip("/")
    return prefix.rsplit("/", 1)[-1] if prefix else None


def decision(repo, commit, number, summary, cache):
    """Resolve `adr NNNN {name}? summary` at commit to (directory, error).

    A first summary word that names an ADR directory at commit qualifies the
    number. Unqualified, the number must be held by exactly one directory."""
    tree = adr_tree(repo, commit, cache)
    names = {}
    for directory in tree:
        name = directory_name(directory)
        if name:
            names.setdefault(name, []).append(directory)
    first, _, rest = summary.partition(" ")
    if first in names:
        if len(names[first]) > 1:
            return None, "directory name " + first + " is ambiguous: " + ", ".join(sorted(names[first]))
        if not rest:
            return names[first][0], "a terse summary must follow the directory name " + first
        return names[first][0], summary_error(rest)
    holders = sorted(directory for directory in tree if number in tree[directory])
    if len(holders) == 1:
        return holders[0], None
    if not holders:
        return None, "no ADR document " + number + " exists at this commit"
    return None, ("ADR " + number + " exists in " + ", ".join(holders) + "; name its directory: adr " +
                  number + " <" + "|".join(sorted(filter(None, map(directory_name, holders)))) + "> summary")


def duplicate_numbers(repo, commit, cache):
    """(directory, number) -> sorted paths, for every number held by two or
    more ADR documents in one directory at commit."""
    return {(directory, number): sorted(paths)
            for directory, numbers in adr_tree(repo, commit, cache).items()
            for number, paths in numbers.items() if len(paths) > 1}


def changed(repo, commit):
    return [line.partition("\t")[::2] for line in
            git(repo, "diff-tree", "--no-commit-id", "--name-status", "--root",
                "--no-renames", "-r", commit).splitlines()]


def first_parent_subjects(repo, commit):
    return [line.partition("\0")[::2] for line in
            git(repo, "log", "--first-parent", "--format=%H%x00%s", commit).splitlines()]


def plan_document(repo, commit, number, cache):
    """The one ADR document a plan commit adds or amends, as a regular file
    at that commit, else None."""
    key = "plan", commit, number
    if key not in cache:
        changes = changed(repo, commit)
        path = changes[0][1] if len(changes) == 1 and changes[0][0] in {"A", "M"} else None
        match = path and adr_path(number).fullmatch(path)
        regular = match and path in adr_tree(repo, commit, cache).get(match.group(1), {}).get(number, [])
        cache[key] = path if regular else None
    return cache[key]


def lineage(repo, parent, directory, number, cache):
    """Walk parent's first-parent history for decision (directory, number):
    the nearest plan whose document is in that directory, and the
    implementations whose subjects resolve to that decision. A plan is known
    by its document; an implementation by its subject at its own commit."""
    plan, count = None, 0
    for sha, subject in first_parent_subjects(repo, parent):
        match = ADR.fullmatch(subject)
        if not match or match.group(2) != number:
            continue
        kind, _, summary = match.groups()
        if kind == "plan" and plan is None:
            path = plan_document(repo, sha, number, cache)
            if path and adr_path(number).fullmatch(path).group(1) == directory:
                plan = {"commit": sha, "document": path}
        elif kind == "impl" and decision(repo, sha, number, summary, cache)[0] == directory:
            count += 1
    return plan, count


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
        if set(data) != {"schema", "oversize", "impl_limits"} or type(data["schema"]) is not int or data["schema"] not in {1, 2}:
            raise ValueError("policy schema")
        if data["schema"] == 1 and data["impl_limits"] != []:
            # Schema 1 keyed count limits by number, which no longer names one
            # ADR; with no limits there is nothing to reinterpret.
            raise ValueError("schema 1 impl limits are keyed by number; rewrite them as schema 2 documents")
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
            if set(entry) != {"directory", "adr", "max_impl", "reason", "reviewer"}:
                raise ValueError("impl limit fields")
            if not isinstance(entry["directory"], str) or not ADR_DIRECTORY.fullmatch(entry["directory"]):
                raise ValueError("impl ADR directory")
            if not isinstance(entry["adr"], str) or not re.fullmatch(r"[0-9]{4}", entry["adr"]):
                raise ValueError("impl ADR number")
            if type(entry["max_impl"]) is not int or entry["max_impl"] < 5:
                raise ValueError("impl limit")
            if not all(isinstance(entry[k], str) and entry[k].strip() for k in ["reason", "reviewer"]):
                raise ValueError("empty impl review")
            key = entry["directory"], entry["adr"]
            if key in counts:
                raise ValueError("duplicate impl limit")
            counts[key] = entry
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
    checked, cache = [], {}
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
            kind, number, summary = match.groups()
            directory, problem = decision(repo, commit, number, summary, cache)
            if problem:
                errors.append(problem)
            if kind == "plan":
                changes = changed(repo, commit)
                if len(changes) != 1 or changes[0][0] not in {"A", "M"} or not adr_path(number).fullmatch(changes[0][1]):
                    errors.append("plan must add or amend exactly one ADR Markdown file and nothing else")
                elif plan_document(repo, commit, number, cache) is None:
                    errors.append("plan document " + changes[0][1] + " must be a regular file")
                else:
                    status, path = changes[0]
                    item["adr_document"] = path
                    own = adr_path(number).fullmatch(path).group(1)
                    if directory and directory != own:
                        errors.append("subject names " + directory + " but the plan changes " + path)
                    blob = git(repo, "rev-parse", commit + ":" + path).strip()
                    lines = len(git(repo, "show", commit + ":" + path).splitlines())
                    item["adr_lines"] = lines
                    if lines > 400 and (path, blob) not in oversize:
                        errors.append("ADR exceeds 400 lines without an exact reviewed path/blob exception")
                    if status == "A":
                        before = adr_tree(repo, item["parents"][0], cache).get(own, {}) if item["parents"] else {}
                        expected = "%04d" % (max(map(int, before), default=0) + 1)
                        if number in before:
                            errors.append("ADR number " + number + " already exists in " + own)
                        elif number != expected:
                            errors.append("new ADR in " + own + " must be the next number there, " + expected)
            elif directory:
                item["decision"] = {"directory": directory, "adr": number}
                documents = adr_tree(repo, commit, cache).get(directory, {}).get(number, [])
                if len(documents) != 1:
                    errors.append("ADR " + number + " in " + directory + " must be exactly one file at this commit")
                plan, count = lineage(repo, item["parents"][0], directory, number, cache) if item["parents"] else (None, 0)
                if plan is None:
                    errors.append(("implementation" if kind == "impl" else "numbered docs") +
                                  " requires an earlier plan of ADR " + number + " in " + directory + " in first-parent ancestry")
                else:
                    item["plan"] = plan
                if kind == "impl":
                    limit = count_limits.get((directory, number), {}).get("max_impl", 5)
                    item["impl_count"], item["impl_limit"] = count + 1, limit
                    if count + 1 > limit:
                        errors.append("ADR exceeds five implementations without a reviewed count exception")
            if kind == "impl":
                paths = [path for _, path in changed(repo, commit)]
                if paths and all(path.endswith("_evidence.md") for path in paths):
                    errors.append("separate evidence-only implementation commit is forbidden")
        if item["parents"]:
            before = duplicate_numbers(repo, item["parents"][0], cache)
            for (directory, number), paths in sorted(duplicate_numbers(repo, commit, cache).items()):
                if before.get((directory, number)) != paths:
                    errors.append("introduces a second ADR " + number + " in " + directory + ": " + ", ".join(paths))
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
