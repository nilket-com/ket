#!/usr/bin/env bash
# Build a source lineage of any size and the prefix-rewritten target it grafts
# to, for measuring the verifiers on the workload they were written for. The
# relationship is the one they verify: every source tree wrapped under
# `nilket`, the same metadata, parents mapped, and a signature on the tip that
# the target drops.
#
#   ./build_lineage.sh DIRECTORY COUNT
#
# This is NOT build.sh with a parameter. build.sh builds the three-commit
# fixture whose commit hashes appear in compare.sh's recorded diagnostics, so
# its construction must not change; this one exists beside it for sizing, and
# record 0021 is why.
set -euo pipefail
out=${1:?usage: build_n.sh DIRECTORY COUNT}
count=${2:?usage: build_n.sh DIRECTORY COUNT}
mkdir -p "$out"; out=$(cd "$out" && pwd)
source_git="$out/source"; target_git="$out/target"
rm -rf "$source_git" "$target_git" "$out/commit_map"

export GIT_AUTHOR_NAME="A Author" GIT_AUTHOR_EMAIL="a@example.invalid"
export GIT_COMMITTER_NAME="C Committer" GIT_COMMITTER_EMAIL="c@example.invalid"
# Exported so that the assignment-only commands in the target loop below
# reach git: `VAR=x new=$(...)` sets a shell variable, and only an already
# exported name is in the environment the command substitution inherits.
export GIT_AUTHOR_DATE="2020-01-01T00:00:00+00:00"
export GIT_COMMITTER_DATE="2020-01-01T00:00:00+00:00"

git init -q -b main "$source_git"
cd "$source_git"
git config commit.gpgsign false
for n in $(seq "$count"); do
	mkdir -p "dir$((n % 7))"
	printf 'contents %s\n' "$n" >"dir$((n % 7))/file.txt"
	git add -A
	stamp=$(printf '2020-01-01T%02d:%02d:%02d+00:00' $(( n / 3600 % 24 )) $(( n / 60 % 60 )) $(( n % 60 )))
	GIT_AUTHOR_DATE=$stamp GIT_COMMITTER_DATE=$stamp \
		git commit -q -m "commit number $n

a body line for $n"
done

# A signature on the tip, as the fixture does: the header is what is read.
tip=$(git rev-parse refs/heads/main)
raw=$(mktemp); signed=$(mktemp)
git cat-file commit "$tip" >"$raw"
{
	sed -n '1,/^committer /p' "$raw"
	printf 'gpgsig -----BEGIN PGP SIGNATURE-----\n \n not a real signature\n -----END PGP SIGNATURE-----\n'
	sed -n '/^committer /,$p' "$raw" | tail -n +2
} >"$signed"
git update-ref refs/heads/main "$(git hash-object -t commit -w --stdin <"$signed")"
rm -f "$raw" "$signed"

git init -q -b main "$target_git"
cd "$target_git"
git config commit.gpgsign false
git fetch -q "$source_git" refs/heads/main
printf 'old new\n' >"$out/commit_map"
previous=""; mapped=""
for old in $(git -C "$source_git" rev-list --reverse refs/heads/main); do
	tree=$(git -C "$source_git" rev-parse "$old^{tree}")
	wrapped=$(printf '040000 tree %s\tnilket\n' "$tree" | git mktree)
	raw=$(git -C "$source_git" cat-file commit "$old")
	author=$(printf '%s\n' "$raw" | sed -n 's/^author //p')
	committer=$(printf '%s\n' "$raw" | sed -n 's/^committer //p')
	message=$(printf '%s\n' "$raw" | sed -n '/^$/,$p' | tail -n +2)
	GIT_AUTHOR_NAME=${author%% <*} \
	GIT_AUTHOR_EMAIL=$(printf '%s' "$author" | sed -n 's/.*<\(.*\)>.*/\1/p') \
	GIT_AUTHOR_DATE=$(printf '%s' "$author" | sed -n 's/.*> //p') \
	GIT_COMMITTER_NAME=${committer%% <*} \
	GIT_COMMITTER_EMAIL=$(printf '%s' "$committer" | sed -n 's/.*<\(.*\)>.*/\1/p') \
	GIT_COMMITTER_DATE=$(printf '%s' "$committer" | sed -n 's/.*> //p') \
	new=$(if [ -n "$previous" ]; then printf '%s\n' "$message" | git commit-tree "$wrapped" -p "$previous"
	      else printf '%s\n' "$message" | git commit-tree "$wrapped"; fi)
	printf '%s %s\n' "$old" "$new" >>"$out/commit_map"
	previous=$new; mapped=$new
done
git update-ref refs/heads/main "$mapped"
echo "built $out with $count commits"
