# PRODUCT-0162 implementation evidence

Status: implemented 2026-09-07. The record remains proposed.

Review correction 2026-09-08: the historical Python harness below is replaced
by `uat/agent_notebook_drive.sh` (Bash, jq, GNU timeout). The replacement completed
the same 19 actions in a fresh live first-run window, saving every command and
answer under `/tmp/polariton-bash-qHHllq/receipt with spaces`. An unknown-answer
control stopped after one invocation without replaying. This removes the Python
dependency from the current acceptance instructions.

The missing surface was notebook authoring: PRODUCT-0158/0160 could act on
figures but could not inspect and change the SQL that produced them. This
cut adds four commands to their shared application owner: describe_notebook,
open_data, replace_cell, and run_cell. It uses the engine's attributed text,
fence projector, and guarded edit transaction. Both transports await that
transaction's answer; neither turns a cached generation into write authority.

The edit guard also validates a run's captured source through an empty edit
transaction. SQL/Rune work keeps the window's existing handlers, with task
messages correlated to the request. Pane plots wait for the incarnation
installed by that request to reach projection. Inspection and replacement
receipts identify the acknowledged snapshot, not a promise that no person
has edited since. File reads run off the update thread and cannot overwrite
a connection replaced while they were loading.

CSV input uses Arrow's reader and the existing ResultArtifact projection;
Parquet keeps the existing lazy scan and directory partition behavior. The
window connection form and agent call the same opener. There is no second
SQL runner or publisher. The serialization and source-ownership guards
cover these boundaries.

## Scope discovered during implementation

The engine already supplied the guarded edit operation, including an empty
transaction that validates without mutating or creating undo history. No new
editor mutation protocol was necessary. The frontend now exports the same
fence projector to avoid a second parser.

Notebook result names remain application-scoped. Execution therefore refuses
multiple projected notebooks, even though inspection and editing can select
a buffer. Source selection alone cannot isolate results. A later cut must
scope result identities before lifting that restriction.

The headless notebook opens on scratch without workspace discovery and does
not run cells implicitly. Rune startup still attaches and runs normally.
The first-run implementation is preserved separately and consumes this cut;
this evidence does not accept PRODUCT-0161.

## Verification

The real-window exercise used `uat/agent_notebook_drive.py` through the
shipped `polariton act` command, targeting advertised window
`44b4abf4ddd141073d5826ea122a0f27`. Its external file was
`nilket/crates/toron-tpch/unused/strict/sf0_01/nation.parquet`, opened as
`nation` with the returned column schema. All 19 commands received successful
final answers. Source generations advanced 1 → 2 → 3; plot incarnations
advanced 1 → 2 → 3. The second source edit doubled the selected y column
after the own-file figure had rendered. Completion receipts carried real
plot dimensions, approximately 1883.2 × 432 logical pixels.

The final publisher returned digest
`b617bfc0fba127bc31ee581803328f92c5805ac257058d8ddd2b17a891c6eadf`.
Commands, wire receipts, and package are retained locally under
`/tmp/polariton-0162-live/journey-final`. The checked-in harness recreates
them with a new output directory. The package has one scatter+line figure,
its SVG, presentation and definition, and Arrow data. Two fresh-window runs
produced the same digest. Neither used pointer input or keyboard injection.

Eight new application tests cover the complete headless file/edit/run/publish
path; an engine-queued human edit winning over the stale agent request;
SQL/Rune/file failures versus preflight refusals; invalid targets and body
limits; duplicate names and oversized inspection; unsupported languages;
busy admission and a connection change overtaking file loading; and an
unknown deadline with a late result prevented from installing. The existing
gate-generation test now uses the new notebook command across off/on.

Final Polariton ladder: 1,261 binary tests passed, 25 existing evidence tests
ignored; 128 architecture guards passed; workspace all-target check, reduced
head all-target check, and formatting passed. This is the application ladder,
not a TPC-H performance run.
