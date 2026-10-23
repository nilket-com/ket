# Commit history preflight

ADR 0639 defines the commit convention and its relationship to the advisory
gate. The checker inspects the complete range refs/remotes/origin/main..candidate
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
else. Messages have no body. An implementation requires an earlier plan with
the same number in first-parent ancestry and an ADR file at that plan commit.
Multiple implementations with different summaries and interleaved lanes are
accepted. New ADR numbers are repository-wide from 0630. Every introduced side-branch commit is checked too.
Shallow, missing and non-ancestor histories refuse the check.

This lint cannot infer whether a syntactically conforming fix hides an
architecture change, or whether prose is actually a run record. Code review
must enforce those semantic distinctions; passing syntax does not replace it.

Controls use isolated temporary repositories and mocked advisory processes:

    python3 -B ci/test_commit_subjects.py

No control rewrites product history, fetches a remote, runs cargo-audit or
sends a message. Execution receipts belong outside Git.

New ADR files require a unique repository-wide number >=0630. Plans have a
400-line bound; an exact path/blob entry with reason/reviewer can grandfather
an unchanged oversized document. The sixth first-parent implementation fails
unless a reviewed per-number count limit permits it. Policy is pinned from
ci/commit-subject-allowlist.json at the candidate; absent policy grants no
exceptions, and malformed policy makes the check HOLD. Roadmaps index programs;
each decision belongs in its own ADR. One short human evidence summary may
accompany an implementation; an evidence-only implementation is refused.
