# Nilket graft freeze certificate

This receipt records the immutable source and the verified rewrite used by
PRODUCT-0001. The migration joined nilket to ket; it did not squash, replay,
or linearly order the two histories.

## Frozen source

- source: `git@github.com:nilket-com/nilket.git`
- ref: `refs/heads/main`
- commit: `7d978dbf7b32cda3c7eef342a1146be13745f56e`
- subject: `plan: adr 531 global aggregate gather`
- reachable commits: 1,346
- complete ref inventory: one branch (`refs/heads/main`), no tags
- object format: SHA-1
- source audit: `git fsck --full` passed

The source was cloned afresh from the authoritative remote after the user
declared development frozen. The pre-existing sibling checkout was not used
as migration input.

## Recovery artifact

- bundle: `/home/me/work/nilket-freeze-7d978dbf.bundle`
- bundle SHA-256:
  `ba809ad3569059214525f77b4ac25d7a2c926bad731bbc98d390cf1059a3f593`
- `git bundle verify`: complete history, branch plus `HEAD`, passed

The bundle deliberately lives outside the destination repository. The
original GitHub repository remains the forge-level archive for issues,
settings, Actions history, and the original signed commit objects.

## Rewrite

- tool: `git-filter-repo` 2.47.0 (`a40bce548d2c`)
- operation: `--to-subdirectory-filter nilket`
- preservation flags: `--preserve-commit-hashes --preserve-commit-encoding`
- rewritten tip: `475e0d960b635efed870b22979571562f019fcae`
- commit map: `0001_nilket_commit_map.tsv`
- commit-map SHA-256:
  `5712bde2f21ad93e0a7ab68a70a81f0f79d6085d60b23a849d2644b8145f45ed`

The preservation flags keep commit messages byte-exact, including historical
references to original object IDs. The adjacent map is their translation.

Rewriting a tree changes its commit object ID and therefore invalidates its
signature. `git-filter-repo` removed `gpgsig` headers from 1,181 source
commits. This is the only accepted metadata loss; the recovery bundle and
archived source retain every original signature.

## Exhaustive verification

`verify_nilket_graft.py` checked every mapped commit and reported:

```text
verified_commits=1346
source_tip=7d978dbf7b32cda3c7eef342a1146be13745f56e
rewritten_tip=475e0d960b635efed870b22979571562f019fcae
metadata_except_invalidated_signatures=exact
source_signatures_removed_as_invalid_after_rewrite=1181
parent_topology=exact_modulo_commit_map
trees=exact_under_nilket_prefix
```

The tree proof is exhaustive rather than sampled: each rewritten commit has
one root entry named `nilket` whose tree object is exactly the corresponding
source commit's root tree. Blob identities, modes, names, and contents are
therefore unchanged below the prefix.

To rerun the proof, clone the recovery bundle into a temporary bare source
repository and run:

```text
python3 plans/product/migration/verify_nilket_graft.py \
  SOURCE.git .git plans/product/migration/0001_nilket_commit_map.tsv
```

Or the Rune port, which says the same thing and needs a budget to do it:

```text
rnx run --budget 40000000 plans/product/migration/verify_nilket_graft.rn \
  SOURCE.git .git plans/product/migration/0001_nilket_commit_map.tsv
```

`--budget` is not optional at this size. `rnx run` allows two million
instructions unless told otherwise, and the port spends between 34 and 36
million on 1,346 commits — measured, on a lineage of that size built by
`fixtures/graft/build_lineage.sh`. Without the flag it halts partway with
`halted: 2000000 instructions exceeded`, which is the answer to a different
question than the one being asked. Forty million is the figure used for the
measurement and leaves room; rnx records 0021 and 0022 are where the number
comes from.

