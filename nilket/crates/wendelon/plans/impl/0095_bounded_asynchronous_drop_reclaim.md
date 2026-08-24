# Impl — Wendelon ADR 0095: Bounded asynchronous drop reclaim

Pairs with `plans/adr/0095_bounded_asynchronous_drop_reclaim.md`. Wendelon
owns lifecycle, reclaim, consensus, participant coordination, and physical
evidence. Toron retains the DROP contract and consumes the faster logical
completion.

## Step 0 — grounding probes

1. **P1 — lease and lock order:** map the current per-slab lease acquisition
   and the lock order required to acquire before rewrite across LSM,
   retention, and sweeper paths. Pin the missing-slab site before changing it.
2. **P2 — admission census:** enumerate every fence admission site across
   reads, streaming, writes, deletes, CDC, and Raft apply. This is the complete
   tombstone retarget list and becomes a structural guard.
3. **P3 — staging guards:** enumerate every ADR 0093 guard that pins
   reclaim-before-finalize and name each deliberate inversion.
4. **P4 — participant anchors:** anchor descriptor-to-locality ranges,
   partition ownership, raw-locality ring-arc intersection, and the replicated
   migration-ledger scan. Fixtures cover partition owner, distinct legacy
   owner, pending source, completed source, and over-included empty node.
5. **P5 — examined-work cost:** prove existing `max_slabs` counts compacted
   rather than examined slabs, then measure one examined unit to size the new
   API's named budgets.
6. **P6 — mutation admission census:** enumerate leader and replica writes,
   deletes, streaming and bulk writes, spawned durable work, and recovery of
   durable mutation work. The resulting guard requires every path to use the
   per-object registry.

P1 and P4 are promotion gates for implementation: if the lock order or
replicated participant inputs differ from the ADR, stop and amend the record.

## Step 1 — lifecycle and deterministic participant stamping

- Add the logical-drop tombstone and one combined fence-or-tombstone admission
  query. Retarget every P2 site through it.
- Finalize prevalidates, converts fence to tombstone, unbinds the name, retains
  the terminal outcome, and records debt plus the participant stamp in one
  apply.
- Compute the stamp from replicated finalize-time state:
  - partition owners for every possible descriptor partition;
  - raw-locality owners for every ring arc intersecting the descriptor's
    possible locality ranges; and
  - all `source_replicas` from every intersecting migration entry in every
    state. No source-erasure predicate exists in this cut.
- On sweeper activation, submit finalize for every durable fence lacking a
  terminal outcome/debt. This is the prepared-DROP roll-forward rule.
- Settlement prevalidates owner epoch and the complete attestation set, then
  atomically removes debt and tombstone, calls `retire_unbound_object`, and
  retains the outcome.
- Move snapshot v4 to v5 and WAL body v3 to v4 with frozen spines, total
  upconversion, immutable fixtures, and exact-set corpus guards.

**Controls:** rejected finalize and settlement leave full state unchanged;
finalize atomically records tombstone, outcome, debt, and stamp; a crash-planted
fence rolls forward; every participant inclusion class is discriminating;
settlement refuses missing attestations or the wrong epoch and applies all
effects atomically.

## Step 2 — per-object mutation admission and restart-safe drain

- Add a node-local `(tenant, table_object_id)` registry with generation-tagged
  in-flight counts and RAII guards.
- Register under the same lifecycle admission synchronization used to inspect
  fence/tombstone state. Finalize closes that generation; later admissions are
  refused and cannot increment it.
- Gate receipt production on a startup reconciliation barrier. Recovery first
  enumerates pending durable mutation work and registers every resumed task;
  only then may the barrier open. Receipt production additionally waits for
  the closed generation count to reach zero.
- Add the P6 structural census so a new mutation or recovery path cannot skip
  the registry.

**Controls:** an in-flight pre-finalize guard deterministically blocks a
receipt until dropped; a post-tombstone mutation is refused without increment;
a restart with durable pre-finalize work cannot attest before reconciliation,
registration, and completion; the census fails when any admission or recovery
site is injected around the registry.

## Step 3 — bounded reclaim slices and progress format

Create new slice functions with input `{phase, cursor, examined_budget}` and
output `{cursor_next, examined, rewritten, done}`. Count examined work at the
API boundary and yield at the budget. Do not add a parameter to existing
`max_slabs` entry points.

Add node-local redb table `drop_reclaim_progress_v1`, keyed by canonical
operation identity. Pin a fixed versioned value carrying phase, cursor,
examined total, owner epoch, completion state, corruption marker, and receipt
provenance. Old-epoch cursors may resume idempotently; only current-epoch work
may persist progress or attest.

An absent key starts at zero. An undecodable value records a named corruption
event and marker, then runs an explicit reset-from-zero rebuild through the
same bounded API. Future incompatible formats fork the table name.

**Controls:** examined count never exceeds budget against many nonmatching
slabs; cursor resume passes prior work; progress survives restart and epoch
change; absent and malformed records produce distinguishable outcomes; a
malformed record's rebuild remains bounded.

## Step 4 — deterministic sweeper, RPCs, and settlement evidence

- Run one leader-resident sweeper at concurrency one, ordered by finalize log
  index. Validate owner epoch on every RPC, progress write, attestation, and
  settlement.
- Add a reclaim-slice RPC over the existing node transport:

  ```text
  request  { operation_id, finalize_log_index, ownership_epoch, phase, budget }
  response { examined, done }
  ```

  The request has no cursor. The receiving node advances only from its own
  durable cursor. RF=1 invokes the same handler locally.
- A node records provenance only after finalize is applied, startup mutation
  reconciliation is open, the closed mutation generation is zero, reclaim
  phases are complete, and bounded index checks are empty.
- The current leader gathers the complete stamped set and proposes settlement
  under the crash-fault trust model. The implementation receipt names that the
  values are not independently verifiable during apply.

**Controls:** stale-epoch slice and settlement are refused; a replayed current
slice cannot skip or move the local cursor backward; an over-included node
returns an empty attestation through bounded work; withholding any stamped
participant blocks settlement without losing progress; independent checks on
every participant find no target reference in memory, persistent index, L1,
or L2 after mutation drain.

## Step 5 — migration closure

At migration materialization, consult the logical-drop tombstone and filter
rows within decoded records. Never copy a tombstoned-object row; preserve live
and legacy rows from the same packed record. The participant stamp remains
unchanged after finalize because no new holder receives target bytes.

**Control C8:** construct one packed record containing live rows, rows from the
dropped object, and legacy raw-locality rows. Induce migration between finalize
and settlement. Assert live and legacy rows arrive intact, dropped rows do not,
the stamp does not widen, and receipts remain required from the pre-finalize
partition/raw owners and pending/completed migration sources.

## Step 6 — slab exclusion and statement path

- Generalize the existing slab lease to acquire before rewrite under P1's
  grounded lock order. Compaction and reclaim use the same claim.
- Delete `drive_table_drop_physical_reclaim` from the statement path. DROP now
  fences, finalizes, and responds. Retries against tombstone/debt observe
  same-operation success without spawning work.
- Remove the DROP-path global tombstone pass; retain the bounded ordinary
  sweeper as its sole owner.

**Controls:** deterministic barriers at the slab-claim seam prove both
serialization orders clean with the lease and reproduce the missing-slab class
with it disabled; the one-million-row DROP returns in-bound and a same-name
replacement reads successfully while debt remains; restart resumes debt and
settled debt stays settled.

## Step 7 — architecture guards

- Fence admission sites equal tombstone admission sites.
- Every mutation admission and durable-recovery path uses the generation
  registry; receipt production is dominated by startup reconciliation.
- Participant derivation covers partitioned and raw-locality ownership plus
  every intersecting migration source using replicated state only.
- Migration filters tombstoned rows within packed records.
- No statement-reachable path has an unbounded examined budget or invokes the
  global tombstone pass.
- The ordinary sweeper remains the sole owner of migrated-range tombstones.
- Snapshot v5 and WAL v4 corpora retain exact-set guards.
- ADR 0093's non-default `test-support` gate covers deterministic barriers.

## Step 8 — verification gate

```text
scripts/cargo-cgroup fmt  -p toron -p wendelon -- --check
scripts/cargo-cgroup test -p wendelon --lib
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p wendelon --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p wendelon --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
# Narrow feature-enabled controls to their named targets and filters:
scripts/cargo-cgroup test -p wendelon --features test-support --test <adr0095_target> adr0095_
scripts/cargo-cgroup test -p toron --features test-support --test <adr0095_target> adr0095_
git diff --check
```

The implementation receipt records P1–P6, admission-registry overhead,
measured drain time under concurrency one, snapshot/WAL compatibility,
crash-fault receipt trust, the lockstep disposition of the additive RPC frame,
and the viz cleanup exhibit. The exhibit is labeled RF=1; multi-node evidence
comes from the participant, restart, migration, and receipt controls.

## Guard reconciliation

Enumerate every deliberate inversion of ADR 0093's reclaim-before-finalize
guards. Record each architecture-bound change beside its guard. The new guards
cover lifecycle admission, mutation drain, participant totality, migration
closure, bounded statement work, sole global-sweep ownership, and consensus
fixture corpora.
