# Bounded slab recovery and empty-slab retirement

## Status

Proposed 2026-08-14. No implementation.

## Context

A local Nilket workspace at `/home/me/.local/share/nilket/local` can no longer
start. Bootstrap exhausts the process file-descriptor limit while recovering the
slab directory:

```
wendelon: bootstrap failed: Too many open files (os error 24)
nilket local: Wendelon exited before readiness: exit status: 1
```

Each attempt peaks at 8.4 GiB, runs about ten seconds, and crash-loops. The
service is configured with `LimitNOFILE=524288`, so raising the limit is not a
fix. The incident is recorded in `plans/wip/toron-local-wendelon-slab-exhaustion.md`;
this record is the Wendelon-side decision. Nothing here crosses into Toron — the
failure is entirely interior to this ring's storage bootstrap and LSM driver.

**Observed state.** 668,700 slab files, of which 668,693 are zero-byte. The seven
non-empty files total 4–28 KiB each; the whole profile is 191 MiB against a
configured `storage.slab_max_bytes` of 64 MiB. The volume is not append rotation.

**Why startup fails.** `SlabManager::open` splits the discovered ids with
`split_last`, opens *every* id below the maximum via `LocalSlabStore::open`, and
retains each in the `sealed` map. Descriptors therefore scale linearly with file
count, and 668,700 crosses 524,288 before readiness. The per-attempt memory peak
has the same origin.

**The producer.** `advance_active_after_lsm_outputs` in
`storage/manager/lsm_driver.rs` seals the outgoing active slab unconditionally:

```rust
let sealed_slab = std::mem::replace(&mut state.active, next_slab);
let sealed_id = sealed_slab.slab_id();
let sealed_size = sealed_slab.write_cursor();   // computed
state.sealed.insert(sealed_id, sealed_slab);    // inserted regardless
```

`write_cursor()` is read and then never tested. An active slab that received no
writes is sealed and retained exactly like one that did.

**The rate corroborates it.** The default compaction tick is 100 ms
(`lsm_picker_tick_millis: 100`). The slab mtimes span 2026-08-12 22:38:27 to
2026-08-14 04:13:47 — 106,520 seconds — over which 668,700 files appeared: **6.28
files per second against a 10 per second tick ceiling.** Roughly two thirds of
ticks produced a slab, which is the shape expected if most compaction rounds
install an empty active slab and the next round seals it.

## Decision

1. **Do not seal and retain an empty active slab.** `advance_active_after_lsm_outputs`
   gains the guard its own `sealed_size` read implies: an outgoing active slab
   with a zero write cursor and no installed output is not admitted to the sealed
   set, and does not leave a file behind. No maintenance path may leave an
   unbounded sequence of empty slabs.

2. **Bootstrap recovery is bounded by the persistent index, not by a directory
   scan.** `PersistentLocalIndex` already exists and `load_snapshot()` already
   returns the authoritative record, tiered, and secondary-index picture — but
   `SlabManager::open` opens it *after* the loop that exhausts descriptors.
   Recovery consults the index first and opens only the slabs it references. This
   is a reordering of machinery that exists, not new machinery.

3. **Where recovery cannot be index-bounded, it fails early with a diagnostic.**
   A bootstrap that would open more slabs than a stated bound stops and reports
   the slab count and a safe recovery path, rather than consuming every descriptor
   and 8.4 GiB before dying with `os error 24`. The current failure names the
   symptom and not the cause.

4. **A regression proves the bound.** Compaction runs over a small local workload
   for enough ticks that the defect would reproduce, and asserts slab-file count
   stays bounded and restart still succeeds. A test that merely runs compaction
   without asserting a file-count ceiling would pass against the defect and is not
   the control.

5. **Repair for an affected non-development profile is defined before it is
   needed**, and is not "delete the zero-byte files". See the hazard below.

## Rationale

The empty-slab guard is the root fix; everything else limits blast radius. Fixing
only the producer leaves every already-affected profile unstartable, and fixing
only recovery leaves the directory growing at six files a second.

Bounding recovery by the index is worth doing on its own merits. Descriptor use
proportional to historical file count is a property no bootstrap wants, whatever
produced the files — a future defect, or an operator restoring a large directory,
would hit the same wall.

**Framing GC as eligibility, not absence.** The incident report suspected that no
GC path retires these slabs. That is not what the code shows: `run_lsm_gc_tick`
exists, and `can_retire_local_slab` returns true for a slab with zero record and
zero secondary-index references — which an empty slab has. So the live question is
whether empty slabs ever enter the candidate set, and whether they can be retired
faster than they are created, not whether retirement exists. Instrumentation
should test eligibility and candidate admission; an implementation that adds a
second retirement path without first establishing why the existing one does not
apply is likely building around a defect rather than fixing it.

The instrumentation the incident report asks for stands, with item 4 read as an
eligibility question:

1. active-slab creation and sealing;
2. LSM output reservation and install;
3. manifest admission of the resulting ids;
4. **whether zero-byte sealed slabs become GC-eligible candidates**, and if not,
   where they are excluded.

## Consequences (The "Negatives")

Suppressing an empty seal makes slab ids non-contiguous with respect to compaction
rounds. Anything that infers round count or ordering from id sequence loses that
inference; ids remain strictly increasing and unique, which is what they are
guaranteed to be.

Index-bounded recovery makes the persistent index load-bearing for startup. When
it is missing or unreadable the current code already warns and continues with
in-memory recovery; that path still needs a bound of its own, which is decision 3
rather than an assumption that the index is always present.

An early-failure bound converts one class of slow crash-loop into a refusal to
start. That is the intended trade — a diagnostic naming the slab count is more
actionable than a descriptor exhaustion ten seconds in — but it does mean a
profile that would eventually have opened now stops sooner.

## Open Questions

**Repair hazard, to be closed before any repair is published.** Deleting zero-byte
slab files is **not** safe as stated. `SlabManager::open` takes
`existing_ids.split_last()`, so the **highest-numbered file becomes the active
slab** — and in an affected profile that file is almost certainly zero-byte.
Deleting it destroys the active slab. Any repair must establish the
manifest/index relationship first and preserve the active slab explicitly.

For the disposable local development profile in the incident, the practical
recovery is a reset, which is appropriate only because its few tables are
recreated from scripts:

```sh
systemctl --user stop nilket-local.service
nilket local --data-dir /home/me/.local/share/nilket/local --reset
systemctl --user start nilket-local.service
```

This is not a repair for a non-disposable workspace, and this record does not
propose it as one.

**Whether the empty slab is created or merely sealed empty.** The guard in
decision 1 prevents retention of an empty seal. Whether the preceding round should
have installed a new active slab at all is a separate question about compaction
round structure, and the instrumentation above should answer it before anyone
concludes the guard is sufficient.
