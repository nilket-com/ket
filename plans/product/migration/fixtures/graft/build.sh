#!/usr/bin/env bash
# Build a source lineage and the prefix-rewritten target it grafts to, plus
# the commit map between them. The real inputs to this migration were not
# kept, so the verifier is checked against a lineage built to have exactly the
# relationship it verifies.
#
#   ./build.sh DIRECTORY
#
# leaves DIRECTORY/source, DIRECTORY/target, and DIRECTORY/commit_map.
set -euo pipefail
out=${1:?usage: build.sh DIRECTORY}
mkdir -p "$out"
out=$(cd "$out" && pwd)
source_git="$out/source"
target_git="$out/target"
rm -rf "$source_git" "$target_git" "$out/commit_map"

export GIT_AUTHOR_NAME="A Author" GIT_AUTHOR_EMAIL="a@example.invalid"
export GIT_COMMITTER_NAME="C Committer" GIT_COMMITTER_EMAIL="c@example.invalid"
export GIT_AUTHOR_DATE="2020-01-01T00:00:00+00:00"
export GIT_COMMITTER_DATE="2020-01-01T00:00:00+00:00"

git init -q -b main "$source_git"
cd "$source_git"
git config commit.gpgsign false
for n in 1 2 3; do
	mkdir -p "dir$n"
	printf 'contents %s\n' "$n" >"dir$n/file.txt"
	git add -A
	export GIT_AUTHOR_DATE="2020-01-0${n}T00:00:00+00:00"
	export GIT_COMMITTER_DATE="2020-01-0${n}T00:00:00+00:00"
	git commit -q -m "commit number $n

a body line for $n"
done

# The third commit is rewritten by hand to carry a signature, so the verifier
# meets one and can report that signatures were dropped. Making a real one
# would need a key; the header is what the verifier reads.
tip=$(git rev-parse refs/heads/main)
raw=$(mktemp)
git cat-file commit "$tip" >"$raw"
signed=$(mktemp)
{
	sed -n '1,/^committer /p' "$raw"
	printf 'gpgsig -----BEGIN PGP SIGNATURE-----\n \n not a real signature\n -----END PGP SIGNATURE-----\n'
	sed -n '/^committer /,$p' "$raw" | tail -n +2
} >"$signed"
signed_tip=$(git hash-object -t commit -w --stdin <"$signed")
git update-ref refs/heads/main "$signed_tip"
rm -f "$raw" "$signed"

# The target: every commit's tree wrapped under a `nilket` entry, the same
# metadata, no signature, and parents mapped.
git init -q -b main "$target_git"
cd "$target_git"
git config commit.gpgsign false
git fetch -q "$source_git" refs/heads/main
printf 'old new\n' >"$out/commit_map"
mapped=""
previous=""
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
	new=$(if [ -n "$previous" ]; then
		printf '%s\n' "$message" | git commit-tree "$wrapped" -p "$previous"
	else
		printf '%s\n' "$message" | git commit-tree "$wrapped"
	fi)
	printf '%s %s\n' "$old" "$new" >>"$out/commit_map"
	previous=$new
	mapped=$new
done
git update-ref refs/heads/main "$mapped"
echo "built $out"
