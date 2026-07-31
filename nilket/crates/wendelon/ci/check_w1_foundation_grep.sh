#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if rg -n 'highway_hash\b' src tests; then
	echo "W1 grep gate failed: retired symbol 'highway_hash' is still present in src/tests." >&2
	exit 1
fi

if rg -n 'key_hash\s*:\s*u64\b' src tests; then
	echo "W1 grep gate failed: bare 'key_hash: u64' survives in src/tests." >&2
	exit 1
fi

if rg -n 'tokio-uring' Cargo.toml Cargo.lock; then
	echo "W1 dependency gate failed: ADR 008 forbids tokio-uring." >&2
	exit 1
fi

raw_fs_hits="$(
	rg -n 'tokio::fs::|std::fs::' src \
	| rg -v '^src/storage/file_ops.rs:' \
	| rg -v '^src/config.rs:' \
	| rg -v '^src/consensus/raft_storage.rs:\d+:\s*use std::fs::OpenOptions;$' \
	| rg -v '^\S+:\d+:\s*std::fs::write\(path,' \
	| rg -v '^\S+:\d+:\s*std::fs::create_dir_all\(&[a-z_]+\)\.unwrap\(\);$' \
	| rg -v '^src/consensus/raft_storage.rs:\d+:.*std::fs::read_dir\(dir\)' \
	| rg -v '^src/consensus/raft_storage.rs:\d+:.*std::fs::metadata\(&' \
	| rg -v '^\S+:\d+:\s*std::fs::remove_file\(&redb_path\)\.unwrap\(\);$' \
	| rg -v '^\S+:\d+:\s*let file = std::fs::OpenOptions::new\(\).write\(true\).open\(&path\)\.unwrap\(\);$' \
	|| true
)"
if [[ -n "$raw_fs_hits" ]]; then
	echo "$raw_fs_hits" >&2
	echo "W1 grep gate failed: raw filesystem paths remain outside AsyncFileOps impls or the documented bootstrap/test allowlist." >&2
	exit 1
fi

echo "W1 grep gates passed."
