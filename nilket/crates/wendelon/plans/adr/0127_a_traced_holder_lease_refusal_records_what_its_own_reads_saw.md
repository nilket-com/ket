# ADR 0127 - A traced holder lease refusal records what its own reads saw

## Status
Proposed (local), 2026-11-23.
- **Per Codex:**
  - 01a20a7d and 01a20a80: the holder-side evidence design and its narrower contract;
  - 01a20a83: GO for one plan and one opt-in implementation, with the amendments folded in below.
  - 01a20af1 (R2, with 01a20ab9 and 01a20af0): one scan; the class and the provenance bound to the row exported in full; the reader rejects contradictory claims.
- **Base:** origin/main `926012a9d`.
- **Unchanged:** DC-287, the Section 8 rate and the small-node gates stay open. This ADR attributes nothing.

## Context
- **What NEXT5 recorded:** the S8-NEXT5 block recorded two failed writes whose Finalize attempts stalled with the local holder refusing `LeaseMissing`.
- **What the trace cannot say about such a refusal:**
  - **which check refused:**
    - **site 1** is plan admission in `child_write_keyed`: it reads the lease view first, then the decision epoch in a separate read;
    - **site 2** is `holder_authority` under the stripe: it reads the epoch and lineage first, then the lease view;
  - **what grant row the refusing read saw;**
  - **whether that row agrees with a renewal this node issued.**
- **What the local control showed** (DC287-LEASE-LATE-APPLY-CONTROL): a committed `AcquireLeases` applied after its own expiry is dead on arrival and gives exactly this signature. That is one mechanism among others: a missing row, a non-covering row, an epoch crossing, or a per-target denial.
- **Why it cannot be reconstructed afterwards:** the lease view drops expired and other-epoch rows before either site sees them, and a later read races the refusal.

## Decision
**Scope:**
- An **opt-in observation** behind the existing node flag `write_stall_trace`.
- **Recorded:** the local holder's **first** `LeaseMissing` per coordinated attempt, at the read that refused it.
- **Not changed:**
  - the holder authority, the order of its reads, its sampled time and its HLC predicate;
  - verification placement;
  - `ChildRefusal::LeaseMissing` (its code and wire);
  - the lease, cache and retry policy;
  - every answer.
- **With the flag off:** nothing new is allocated or recorded.

### D1. The issue witness (renewal side)
- **What it holds:** a fixed ring of 8 slots plus `evicted_through`, kept inside the node's `WriteStallTrace`.
- **One ordering counter.** An **issue** takes its tickets and publishes its slots **under the ring mutex**, immediately before `raft.client_write(AcquireLeases)` in `propose_own_leases`. The mutex is never held across a Raft call or a state read.
- **One ticket per target.** Each slot holds:
  - the ticket;
  - the target range, epoch and issuer;
  - the request start HLC (physical, logical, node);
  - the blake3 digest of the **complete signed descriptor**;
  - the outcome, and the outcome ticket.
- **Outcomes,** from the actual hook:
  - `CallSiteReached` (written with the slot; **not** a claim of submission to Raft);
  - then one of:
    - `Granted`, or `NotGranted`: **only** from a validated in-budget results vector, by `granted`;
    - `Error`: `client_write` failed, or a wrong response, length, range or order inside `propose_own_leases`;
    - `Unanswered`: the pass's budget ended. This is recorded by the caller from the tickets it was handed, although the bounded future was dropped.
  - **Pre-call failures create no slot.**
- **Outcome writes:** they take their ticket under the mutex, and apply only if the slot still holds **the same ticket and target**. A reused slot is never modified by an older issue.

### D2. The capture (holder side, at both sites)
- **When:** with a sink present, i.e. the coordinator's **local** holder only, with the trace on.
- **What is unchanged:** the lease view samples `now_ms` as today.
- **The ticket:** it takes an **observation ticket** (lock-free) **before** its `read_state`. This is a cutoff at lease-view entry; the applied state read may be later.
- **Inside the same read and the same pass:** `applied_leases` becomes one scan that hands each of this holder's rows, with **its own filter result**, to a visitor. The capture is that visitor, so nothing is walked or evaluated twice. It copies:
  - `E_view`;
  - `applied_index` (`last_applied_log_id`);
  - **at most 2 rows of this holder overlapping the target**, with `truncated` and a saturating overlap count.
- **The rows kept are ranked:** a row covering the whole target first, then one of the view's epoch, then one the view admitted, then the newest grant, then by range. Whenever a covering row of the view's epoch exists, the first kept row is one, however many rows overlap, and the first row is the one the class and the provenance speak for. Only two rows are kept, and more are flagged as truncated (Codex 01a20b27).
- **Verification:** today's `verify_lease` loop after the read records the **actual** result per captured row that the filter returned. A filtered row is `NotEvaluated`.

**The target:**
- **Site 2:** the plan's range.
- **Site 1:** `ChildWriteMeta` carries no range. The coordinator supplies the child's bounds as a **diagnostic hint** through the sink, checked against the actual `(start, end)` passed to `lease_covers`.
  - **On a mismatch:** the authority result stands, and the derived row and provenance claims are **withheld**.

### D3. The evidence and its claims
- **Built at the refusing site on its own `false` coverage, from:**
  - the capture;
  - the site's actual `E_dec` (site 1 from the admission read; site 2 from its first read);
  - the actual range;
  - per row: `covered`, `epoch == E_dec`, `HLC live` (the read's actual filter) and the verify state.
- **Row facts, decided by the FIRST row alone** (the second is exported as facts only), and usable without provenance:
  - **live:** the refusal came from `E_view != E_dec`;
  - **expired;**
  - **unverified;**
  - **absent:** no deciding first row, and only when not truncated; otherwise incomplete. A covering row of another epoch only is absent, and its rows show it;
  - **withheld:** a hint mismatch.
- **Provenance for that same first row, given only when it decided the class** (live, expired or unverified), otherwise withheld, from a witness snapshot taken under `try_lock` (contention gives unknown):
  - slots with ticket < the observation ticket, their outcome **as of** it, and `complete_from`;
  - **agreement:** a retained slot whose digest equals the row's, plus the **retained-window** match count. **Never unique.**
  - **later issue:** the latest retained slot for the same target, epoch and issuer with a request HLC above the row's start, with its ticket, HLC and outcome as of the observation.
  - **Both are kept:** a row may agree with an older issue while a later one exists.

### D4. Publication and export
- **One evidence per attempt:** the sink keeps the first evidence; the coordinator keeps it only from the child whose refusal is the first one it records (`HolderSeen` is unchanged and `Copy`).
- **Published** with the attempt's record under the trace's existing rule (only when the stall is the answer). The ring keeps it in a slot-aligned parallel array.
- **Exported** as a new family `wendelon_data_ring_write_stall_lease{node, slot, field}`: scalars, predicate states and **full digests** (8 × u32 words). No raw descriptor or signature bytes.
  - **Every u64 is two 32-bit words** (`_hi`/`_lo`), exact through an f64 reader.
  - **Old seals,** without the family, stay readable.

### D5. Controls
- **The late-apply control,** extended to assert the evidence:
  - **W1 and W2:** expired, plus a later issue with its outcome as of the observation;
  - **W1 resumed:** expired, plus agreement with the late batch;
  - **none** where nothing refused.
- **Witness unit tests:**
  - a slot reused, with an older issue's outcome refused;
  - the cutoff;
  - an issue holding the mutex during an observation;
  - a duplicate descriptor (never unique).
- **Outcome classification:** granted, not granted (a test-only stale-incarnation seam), malformed results (`Error`), and the budget (`Unanswered`).
- **Classifier tests:** epoch crossings at both sites; the HLC equality boundary; truncation; a hint mismatch; agreement together with a later issue.
- **Export:** exact round-trip of 0, 2^53 + 1 and 2^64 − 1; a fully populated scrape measured in series and bytes; type sizes reported.
- **Off path.**
- **Binding:** two captured rows with distinct starts and digests (the class, agreement and exported digest name the same first row), and no provenance without a deciding first row.
- **The reader delta** is in the harness checkout, identified separately. It rejects contradictory claims (a class its own fields do not decide, provenance without a deciding row, a ticket at or after the cutoff, a later issue not after the row's start) through the normal reader.
- **Lanes:** fmt, clippy, arch, focused, the full gate with `daemons_worker_stack`, the real three-daemon smoke, and the cluster controls repeated.

## Consequences
- **What improves:** a future opted-in block can say, per refusing attempt:
  - which check refused, and what its own reads saw;
  - whether the seen row agrees with a renewal this process issued, or a later one was witnessed, and with what outcome as of that moment.
- **What it does not establish:** commit or append timing, the apply instant, unique provenance, anything across a restart or eviction, or remote holders.
- **Cost with the flag on:** a small fixed ring, one boxed evidence per refusing attempt, one stack copy of at most two descriptors per local holder lease view, and an extra metrics family, all measured in the implementation's packet. With the flag off, nothing.
