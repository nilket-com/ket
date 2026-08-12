# ADR 0548 implementation receipt — 2026-08-12

CTAS now publishes one composite operation across the catalog and row rings
without exposing an empty partial table. `CREATE OR REPLACE TABLE ... AS SELECT`
is restored: the replacement is populated under an invisible provisional
object, atomically bound by a control-ring compare-and-swap, and only then
retires the old object through the existing fenced drop lifecycle.

## Grounded Stage 0 disposition

The implementation inventory found that provisional visibility could be closed
at the catalog authority rather than at every downstream consumer. Ordinary
`get`, latest-by-id, name resolution, tenant enumeration, and descriptor sync
exclude objects held in `provisional_by_id`. Query planning, CDC, statistics,
secondary indexes, migrations, retention, and descriptor refresh consume those
filtered reads. The sole including-provisional row-admission path additionally
requires the write's derived row identity to match the owning saga. A
provisional object is therefore neither queryable nor writable by an unrelated
operation.

Publication is one raft-applied mutation. It compares the complete captured
binding (object and generation), verifies the provisional owner, requires the
current ownership epoch, and requires the durable rows-committed attestation
before replacing `latest_by_name`. A changed binding, missing or foreign
provisional object, unattested row phase, or stale owner rejects without
touching the winner.

Row evidence is pinned in the existing redb mutation-outcome store. Pins are
durable across restart, bypass ordinary TTL/capacity eviction, are admitted
under an independent 1,024-pin ceiling, and are acknowledged only after the
control-ring attestation is durable. Toron fans pin and acknowledgement to the
current data nodes; a partial fanout leaves successful pins intact so retry can
complete the set.

Retirement reuses the table-drop prepare/fence/reclaim/finalize state machine.
An unbound old object is admitted to that path only when a `RetiringOld` saga
proves ownership, and finalization does not unbind the newly published name.
This preserves old-or-new visibility across the swap while retaining the
existing reader-drain fence before physical reclamation.

The caller deadline is scoped to the originating activation. It stops phases
not yet dispatched and is never reconstructed across restart as though a
process-local `Instant` were durable. Recovery is instead bounded by the
control-ring ownership lease and per-activation attempt budget. Attempt
exhaustion records retry debt, releases ownership, and remains nonterminal.

## Durable saga and recovery

The control-ring ledger records the caller UUIDv7, immutable intent
fingerprint, captured prior binding and descriptor, provisional descriptor,
reserved row sub-identity, expected row count, phase, typed cause, ownership
epoch and lease, retry debt, and terminal timestamp. The row sub-identity keeps
the caller UUIDv7 bytes and domain-separates through its reserved sequence
number, preserving post-retention expiry proof.

Active sagas plus retained terminal detail have a 4,096-entry ceiling and a
15-minute detail horizon. Only aged terminal detail is pruned; active work is
never evicted. A durable per-tenant UUIDv7 issuance high-water makes a reclaimed
identity resolve as expired rather than fresh. A full live pool refuses as the
typed `resource_exhausted` surface without evicting existing work.

A recovery activation claims an expired ownership epoch and resolves the
derived row outcome rather than re-running the source. This is load-bearing:
the materialized source is not durable and could produce different rows after a
restart. Rows known committed proceed to attestation and publication; rows
known not committed reclaim with a row-phase cause. If committed rows lose the
publication CAS, the populated provisional object is reclaimed and the saga
terminates as a publication conflict, leaving the concurrent winner untouched.

## Public resolution and replacement

The public SQL protocol appends a composite CTAS outcome without renumbering
historical messages. Resolution reports catalog, row, publication, and
retirement commit knowledge independently, with phase-specific causes and the
row count. `execute_with_query_id` and `resolve_mutation` use the same
caller-retained UUIDv7 end to end.

The live listener control executes CTAS through Toron and authenticated
Wendelon nodes, resolves all four committed phases by id, queries the published
rows, replaces the table, resolves the second complete outcome, and observes
only the replacement rows. That control exposed two boundaries that unit
fixtures could not: Data-node saga/descriptor/drop traffic requires the
inter-node authorization class, and post-swap retirement must admit an unbound
old object only under saga proof. Both are pinned in the landed path.

`IF NOT EXISTS` retains compatibility-based race recovery. Plain
`CREATE OR REPLACE TABLE` remains refused: ADR 0548 restores the staged CTAS
form, not the unrelated schema-only replace surface.

## Discriminating controls

- pre-provision source and validation failures leave no saga or target;
- a write failure leaves no public binding, while an empty source publishes an
  explicitly complete queryable empty table;
- provisional descriptors are absent from ordinary by-id consumers;
- publication rejects unattested, changed-binding, missing, foreign, and
  stale-owner cases without changing the winner;
- replacement publishes before retiring and preserves the old binding at every
  prepublication failure;
- restart recovery publishes committed rows after the source storage has been
  removed, proving it resolves the retained outcome rather than re-executing;
- outcome pins survive restart and ordinary expiry until acknowledgement;
- equal operation ids with different intent refuse, ownership takeover is
  epoch-fenced, retry debt releases ownership, and aged terminal replay is
  expired;
- the 4,097th live saga refuses without evicting any of the first 4,096;
- composite protocol and client collection round-trip all four axes; and
- the public listener completes initial CTAS and replacement over live
  Wendelon, then reads the exact final relation.

## Verification

- Wendelon ADR 0548 focused controls: pass;
- Toron CTAS integration target: pass;
- live public-SQL/Wendelon composite-resolution control: pass;
- toron-client unit and architecture targets: pass;
- Toron architecture corpus: 2,121 passed, 0 failed;
- Wendelon architecture corpus: 168 passed, 0 failed;
- workspace all-target check: pass, including benches;
- complete workspace correctness suite (libraries, binaries, integration tests,
  and examples): 171 targets, 7,410 passed, 0 failed, 19 ignored, with the
  repository's 64 MiB test-stack requirement applied inside the cgroup;
- formatting and diff checks: clean.
