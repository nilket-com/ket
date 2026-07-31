#!/usr/bin/env python3
"""Verify a prefix-rewritten nilket lineage against its frozen source."""

import pathlib
import subprocess
import sys


if len(sys.argv) != 4:
    raise SystemExit(
        "usage: verify_nilket_graft.py SOURCE_GIT TARGET_GIT COMMIT_MAP"
    )

SOURCE = pathlib.Path(sys.argv[1]).resolve()
TARGET = pathlib.Path(sys.argv[2]).resolve()
MAP = pathlib.Path(sys.argv[3]).resolve()


def git(repo: pathlib.Path, *args: str) -> bytes:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    ).stdout


def batch(repo: pathlib.Path, object_ids: list[str]) -> dict[str, bytes]:
    proc = subprocess.Popen(
        ["git", "-C", str(repo), "cat-file", "--batch"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert proc.stdin is not None and proc.stdout is not None
    proc.stdin.write(("\n".join(object_ids) + "\n").encode())
    proc.stdin.close()
    found: dict[str, bytes] = {}
    for expected in object_ids:
        header = proc.stdout.readline().decode().rstrip("\n")
        actual, object_type, size_text = header.split()
        assert actual == expected, (actual, expected)
        assert object_type in {"commit", "tree"}, object_type
        size = int(size_text)
        payload = proc.stdout.read(size)
        assert proc.stdout.read(1) == b"\n"
        found[actual] = payload
    stderr = proc.stderr.read() if proc.stderr is not None else b""
    assert proc.wait() == 0, stderr.decode(errors="replace")
    return found


def parse_commit(raw: bytes) -> tuple[str, list[str], bytes, bytes, bool]:
    headers, message = raw.split(b"\n\n", 1)
    lines = headers.splitlines()
    tree = lines[0].split(maxsplit=1)[1].decode()
    parents: list[str] = []
    retained: list[bytes] = []
    skip_continuation = False
    signed = False
    for line in lines[1:]:
        if line.startswith(b"parent "):
            parents.append(line.split(maxsplit=1)[1].decode())
            skip_continuation = False
        elif line.startswith(b" "):
            if not skip_continuation:
                retained.append(line)
        else:
            skip_continuation = line.startswith(b"gpgsig ")
            signed |= skip_continuation
            if not skip_continuation:
                retained.append(line)
    return tree, parents, b"\n".join(retained), message, signed


def parse_single_prefixed_tree(raw: bytes) -> tuple[str, str, str]:
    nul = raw.index(b"\0")
    mode, name = raw[:nul].split(b" ", 1)
    oid = raw[nul + 1 :]
    assert len(oid) == 20, "the recorded migration uses SHA-1 object ids"
    return mode.decode(), name.decode(), oid.hex()


lines = MAP.read_text().splitlines()
assert lines[0].split() == ["old", "new"]
mapping = dict(line.split() for line in lines[1:])
assert all(new != "0" * 40 for new in mapping.values())

source_tip = git(SOURCE, "rev-parse", "refs/heads/main").decode().strip()
target_tip = mapping[source_tip]
source_commits = git(SOURCE, "rev-list", source_tip).decode().splitlines()
target_commits = git(TARGET, "rev-list", target_tip).decode().splitlines()
assert set(source_commits) == set(mapping), "commit-map source coverage differs"
assert {mapping[old] for old in source_commits} == set(target_commits), (
    "target coverage differs"
)
assert len(source_commits) == len(target_commits) == len(mapping)

source_raw = batch(SOURCE, source_commits)
target_ids = [mapping[old] for old in source_commits]
target_raw = batch(TARGET, target_ids)
source_parsed = {old: parse_commit(source_raw[old]) for old in source_commits}
target_parsed = {
    mapping[old]: parse_commit(target_raw[mapping[old]]) for old in source_commits
}
target_tree_ids = [target_parsed[mapping[old]][0] for old in source_commits]
target_trees = batch(TARGET, target_tree_ids)

signed_commits = 0
for old in source_commits:
    new = mapping[old]
    old_tree, old_parents, old_headers, old_message, old_signed = source_parsed[old]
    new_tree, new_parents, new_headers, new_message, new_signed = target_parsed[new]
    signed_commits += int(old_signed)
    assert not new_signed, new
    assert [mapping[parent] for parent in old_parents] == new_parents, old
    assert old_headers == new_headers, old
    assert old_message == new_message, old
    mode, name, wrapped_tree = parse_single_prefixed_tree(target_trees[new_tree])
    assert mode == "40000" and name == "nilket" and wrapped_tree == old_tree, old

print(f"verified_commits={len(mapping)}")
print(f"source_tip={source_tip}")
print(f"rewritten_tip={target_tip}")
print("metadata_except_invalidated_signatures=exact")
print(f"source_signatures_removed_as_invalid_after_rewrite={signed_commits}")
print("parent_topology=exact_modulo_commit_map")
print("trees=exact_under_nilket_prefix")
