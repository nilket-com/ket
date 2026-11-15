# Commit history preflight

ADR 0639 defines the commit convention and its relationship to the advisory
gate; ADR 0737 replaces its repository-wide numbering with numbering per ADR
directory. The checker inspects the complete range refs/remotes/origin/main..candidate
without changing refs or fetching. Gate ownership must fetch and freeze its
base before invoking it. Both names and emails must match Joel's identity.

Run the read-only history check with:

    python3 -B ci/commit_subjects.py --repo . --commit HEAD

The gate preflight entry point first checks history, then forwards one exact
candidate to the existing advisory evaluator:

    python3 -B ci/gate_preflight.py --repo . --commit HEAD -- --run /owned/new-run --tool /reviewed/cargo-audit --mode live

History errors return FAIL1 or HOLD2 before any advisory subprocess starts.
The existing evaluator's exit semantics remain unchanged. Its replay options
can be passed for isolated controls; the live adoption path uses live mode.
Neither this wrapper nor the checker installs timers or adopts itself into
already frozen external gate packages.

The lexical rule refuses scopes, parentheses, capital first summary characters,
unsupported prefixes, delivery/incident labels and separate evidence subjects.
Product nouns and internal acronyms are valid. The remainder of a summary is
printable ASCII. Bare cut as a verb is valid. Numbered docs require an ADR. A plan must add or amend exactly one numbered ADR Markdown file and nothing
else. Messages have no body. A decision is (ADR directory, number); each
directory (plans/product/adr or any <prefix>/plans/adr) is its own number
sequence, and renaming or amending the file keeps the decision. Subjects are
`plan|impl|docs: adr NNNN {name}? terse summary`, where the optional name right
after the number is product or the directory holding plans/adr (libaryon,
toron, wendelon), for example `plan: adr 0012 libaryon content-aware table
column widths`. Without a name the number must be held by exactly one
directory. A plan's document is the one ADR file it adds or amends, in the
subject's directory. An implementation or numbered doc needs exactly one
regular file for its decision at the commit and an earlier plan of that
decision in first-parent ancestry. Multiple implementations with different
summaries and interleaved lanes are accepted.
Every introduced side-branch commit is checked too.
Shallow, missing and non-ancestor histories refuse the check.

This lint cannot infer whether a syntactically conforming fix hides an
architecture change, or whether prose is actually a run record. Code review
must enforce those semantic distinctions; passing syntax does not replace it.

Controls use isolated temporary repositories and mocked advisory processes:

    python3 -B ci/test_commit_subjects.py

No control rewrites product history, fetches a remote, runs cargo-audit or
sends a message. Execution receipts belong outside Git.

A new ADR file takes the next number in its directory: the largest there
plus one, 0001 in a new directory. Duplicate, skipped, backfilled and 0000
numbers fail, and the second of two racing plans renumbers before its push. No
commit, merges included, may newly hold two ADR documents with one number in
one directory. The same number in different directories is not a conflict.
Plans have a
400-line bound; an exact path/blob entry with reason/reviewer can grandfather
an unchanged oversized document. The sixth first-parent implementation of one
decision fails unless a reviewed count limit for that decision permits it. Policy is pinned from
ci/commit-subject-allowlist.json at the candidate; absent policy grants no
exceptions, and malformed policy makes the check HOLD. Schema 2 keys count
limits by directory and number; a schema 1 file is read only while it has no count
limits. Roadmaps index programs;
each decision belongs in its own ADR. One short human evidence summary may
accompany an implementation; an evidence-only implementation is refused.
