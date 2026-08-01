# impl 534 - Snapshot log identity recovery

Scope: restore an aged `nilket local` deployment whose valid Wendelon snapshot cannot pass restart
validation because the fixed snapshot header stores only `(term, index)` while recovery fabricates
the missing OpenRaft leader node id as zero. This is a focused implementation correction, not a new
architecture decision and not a snapshot-format revision. Deliverable is one `impl:` commit,
reviewed against the gates below, followed by recovery of the backed-up dogfood profile.

The plan lives in Toron's 530–534 dogfood sequence because this incident blocks Polariton's Toron
connection. Production changes belong entirely to Wendelon's control-ring storage.

## Incident evidence

The first overnight dogfood deployment shut down in the intended order:

```text
nilket local: draining Toron before Wendelon
toron: shutdown complete
wendelon: shutdown complete
```

Both restart attempts then failed before readiness:

```text
snapshot.wnd last_applied Some(T1-N1-134999)
disagrees with header Some(T1-N0-134999)
```

`decode_snapshot_reader` verified the snapshot-body digest before decoding the body and reaching
this comparison. The body therefore passed its BLAKE3/SHA-256 integrity check and contains the full
`LogId`; the disagreement is solely the invented node component. The retained WAL begins at index
131,073 and earlier entries have been purged, so deleting, renaming, or ignoring the snapshot is not
a recovery option.

The shutdown-time OpenRaft log `error reporting metrics error=channel closed` is separate teardown
noise: Raft and Wendelon both completed shutdown. It neither explains nor repairs the deterministic
restart mismatch and is out of scope.

## Backup gate — completed before planning

The deployment was confirmed stopped. Before any fixed binary or source edit touched the profile,
the complete tree was copied with metadata and reflink support:

```text
source: /home/me/.local/share/nilket/local
backup: /home/me/.local/share/nilket/local.restart-bug-backup
size:   162 MiB each
```

Source and backup file manifests (relative path + length) matched. Both `snapshot.wnd` files hash to:

```text
605f98818cc19265ecc65e96dc94c26f552cb82ff2299e52870fd3b83b28df08
```

The backup is incident evidence. Implementation and tests must not modify it. The original profile
is not opened by Wendelon until all synthetic recovery tests pass and the user explicitly proceeds
with the real recovery acceptance.

## Root cause: identity invented from insufficient bits

`SnapshotHeader` truthfully stores:

- `last_included_term`;
- `last_included_index`.

It does **not** store `CommittedLeaderId.node_id`. `log_id_from_snapshot_header` nevertheless builds
`CommittedLeaderId::new(header.last_included_term, 0)`, converting partial coordinates into a false
full identity. Local load compares that invention with `SmState.last_applied_log_id`; snapshot
installation compares it with OpenRaft's full `SnapshotMeta.last_log_id`.

The unit helper `log_id(term, index)` also always constructs node zero, so every local snapshot test
makes the invention accidentally true. `simulator_ladder` repeats the same helper choice. Chaos
tests use real nonzero node ids but do not force the aged state—snapshot built, old WAL prefix
purged, then process reopened—that makes the header load-bearing.

There is a second representational ambiguity: header `(term=0, index=0)` can describe either no
last-applied log or `Some(LogId(T0-N?-0))`. Recovery must not manufacture presence from those bits
either. The hash-verified body resolves both missing dimensions.

This is the established bug class **identity invented from insufficient bits**: PID without
start-time nonce, arena slot without generation, or partial log coordinates promoted to a complete
identifier. The correction follows the house rule: the complete, integrity-verified object is the
identity authority; a partial representation validates only the fields it actually contains.

## Required correction

### 1. Snapshot headers validate coordinates only

Delete `log_id_from_snapshot_header` or replace it with a helper that cannot return a `LogId`.
Validation compares only:

```text
header.last_included_term  == authoritative_log_id.leader_id.term
header.last_included_index == authoritative_log_id.index
```

For local artifact loading, `SmState.last_applied_log_id` from the hash-verified postcard body is the
full authority. It supplies presence, term, leader node id, and index for `LoadedSnapshot`,
`HeldSnapshot.meta`, WAL replay boundaries, recovered commit state, and OpenRaft snapshot metadata.
The header checks only that its term/index coordinates agree.

For snapshot installation, two complete identities exist and must agree exactly:

- `SnapshotMeta.last_log_id` supplied by OpenRaft;
- `SmState.last_applied_log_id` from the hash-verified snapshot body.

The fixed header's term/index must agree with that full identity. Membership continues to agree
exactly between meta and body. Do not overwrite a disagreeing body identity with install metadata;
reject before publication and before renaming the received snapshot into place.

The validation helper must handle all combinations explicitly:

- body/meta `None` accepts only header `(0, 0)`;
- body/meta `Some(T0-N0-0)` also accepts header `(0, 0)` while retaining `Some(T0-N0-0)`;
- nonzero term/index compare exactly;
- term or index disagreement rejects as corruption;
- local body identity and install-meta identity disagreement rejects even when their coordinates
  happen to match but their leader node ids differ.

### 2. Do not change the snapshot format

No field is added, repurposed, or versioned. In particular, do not store leader node id in
`last_included_epoch`, `snapshot_hlc.node_id`, padding, or a new suffix. Those fields have different
contracts, and changing the 80-byte prefix would strand the exact artifact being recovered.

The header never promised a node id. Validating only its declared coordinates is the correct model,
not a compatibility workaround. A future format change may duplicate the full identity only if
multi-node evidence justifies the migration and authority rules; this incident does not.

### 3. Make production identity the fixture default

In `raft_storage.rs` tests, replace the degenerate helper with an explicit shape such as:

```text
log_id(term, index)                 -> a fixed nonzero leader node id
log_id_for(node_id, term, index)    -> cases that care which leader authored the log
```

Node zero is reserved for tests whose subject is the identity element or genesis semantics. Update
the snapshot/restart/install fixtures mechanically so ordinary recovery always exercises a
nonzero leader identity. Apply the same correction to `simulator_ladder`'s log-id helper or explain
with a test-specific invariant why any remaining zero is intentional.

Audit the dormant chaos/simulator soak paths for two independent gaps:

1. use of leader node zero in snapshot/recovery fixtures;
2. restart scenarios that never cross snapshot + purge before reopening.

Do not mechanically replace unrelated `node_id: 0` values: HLC genesis, zero tenant/locality
sentinels, and storage-domain fixtures may use zero honestly. The rule is specific to identities
whose production authority cannot be reconstructed from zero.

## Required tests

1. **Exact incident reproduction.** Persist a snapshot whose body has `Some(T1-N1-134999)` and
   header coordinates `(1, 134999)`. The pre-fix loader rejects it as `T1-N0`; the fixed loader
   returns the full `T1-N1-134999` identity.
2. **Aged local restart.** On a nonzero-node fixture, commit durable data, build a snapshot, purge the
   WAL prefix through the snapshot boundary, close every handle, reopen the same artifact paths, and
   read the data. Prove recovery actually needs the snapshot—the retained WAL alone is insufficient.
   The negative control must withhold the snapshot from a copy of the fixture and observe an
   `InvalidData` refusal. If the first retained named WAL segment starts beyond the initial log
   boundary while no snapshot exists, recovery has no authority for the missing prefix and must
   fail closed rather than replaying the tail onto an empty state.
3. **Install preserves full identity.** Install a real file-backed snapshot with nonzero leader node
   id; meta, body, held snapshot, and installed state retain it exactly.
4. **Install identity mismatch rejects.** Header coordinates and body may match while install meta
   carries a different leader node id; reject before rename/publication.
5. **Coordinate corruption still rejects.** Independently alter header term and index (recompute no
   body hash—the header is outside that digest); both local load and install refuse.
6. **Body corruption still rejects.** Alter postcard bytes without updating the digest; recovery
   fails before identity validation.
7. **Zero-coordinate ambiguity.** Test both `None` and `Some(T0-N0-0)` bodies against header `(0,0)`;
   preserve the body's presence and full identity.
8. **Membership mismatch remains rejected.** The identity correction must not weaken snapshot
   membership validation.
9. **Restart corpus guard.** At least one simulator/chaos or focused integration witness performs
   the aged sequence using nonzero node identities. If the expensive soak remains dormant, the fast
   focused witness is mandatory and the CI relocation stays a separately named follow-up.

All corruption tests require positive controls in the same module: the unmodified artifact opens,
then each deliberate mutation fails for its intended reason.

## Primary recovery acceptance

After every synthetic test and the full Wendelon suite pass:

1. Verify the original and backup snapshot hashes still equal the receipt above.
2. Build/install the fixed `nilket` without `--reset` and without altering generated profile config.
3. Start the **original** profile normally.
4. Require Wendelon and Toron readiness; no recovery fallback or artifact rewrite command is used.
5. In Polariton, reconnect and select every previously known fixture/table, including the user's
   known first-contact fixture; make one chart from recovered data.
6. Stop and restart once more, then repeat a select. The first start proves old-artifact recovery;
   the second proves artifacts written by the fixed binary remain recoverable.
7. Record snapshot/WAL coordinates, readiness, and query results in implementation evidence without
   copying row contents or private key material.

Any failure stops the drill. Do not retry with reset, artifact deletion, snapshot renaming, WAL-only
replay, or a hand-edited binary file. The immutable backup remains the return point.

The disposable-copy drill exposed one second aged-recovery defect after the identity correction:
the retained segment can contain a valid checkpoint older than the newer snapshot boundary. Such a
checkpoint remains checksum-validated but is superseded recovery metadata, not corruption and not
authority. Recovery ignores it while continuing to scan the segment for corruption and tail entries;
a focused test pins this exact snapshot-overtakes-checkpoint ordering before the real drill resumes.

## Architecture guard

Add a source-audit guard around snapshot identity reconstruction:

- production snapshot-load/install code may not construct `CommittedLeaderId` from
  `SnapshotHeader` fields;
- the guard cites impl 534 and explains that the header owns coordinates, not identity;
- a synthetic forbidden snippet proves the guard fires;
- fixture helpers used by restart/install tests default to a nonzero leader node id, with an
  explicit zero helper for genesis-only cases.

The guard is deliberately narrow. It must not reject legitimate OpenRaft entry decoding where a
complete leader id is actually present.

## Verification gate

```text
scripts/cargo-cgroup check -p wendelon --all-targets
scripts/cargo-cgroup test  -p wendelon --lib raft_storage
scripts/cargo-cgroup test  -p wendelon --test simulator_ladder
scripts/cargo-cgroup test  -p wendelon --test architecture_test impl534
scripts/cargo-cgroup test  -p wendelon --test architecture_test
scripts/cargo-cgroup test  -p wendelon --test backup_restore
scripts/cargo-cgroup test  -p wendelon --test control_ring_retention
scripts/cargo-cgroup test  -p wendelon --tests
cargo fmt -p wendelon -- --check
git diff --check
```

The existing Rust 1.97 warning debt remains quarantined; touched code must add no warning. No test
may pass by changing the 80-byte header, weakening digest verification, retaining all WAL forever,
or skipping the real backed-up-profile acceptance.

## Sequencing and non-goals

This implementation jumps ahead of Wendelon ADR/impl 079. The dogfood deployment is down, so
recovery restores the feedback loop before heartbeat cadence/audit classification work resumes.

Non-goals:

- snapshot-header format revision or migration;
- automatic repair/rewrite of existing artifacts;
- WAL retention-policy changes;
- OpenRaft shutdown metrics-log cleanup;
- CDC heartbeat cadence or audit classification (Wendelon 079);
- reset/reseed/recreate paths;
- general replacement of zero-valued identities across Wendelon.
