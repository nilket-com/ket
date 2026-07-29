# ADR 0002: registry slot reuse and optional page decommit

Status: accepted 2026-07-29; mandatory stages 5a–5c implemented 2026-07-29.
This document decides the safety model and sequencing. Optional stage 5d
remains deferred pending the measurements and separate design required below.

Prerequisites now in place (ADR 0001 stages 1–4): exact owner, parked, and
orphan accounting; publication-fenced payload reclaim that visits every
retired node; per-node content invalidation; and exact
reserve-before-mutate capacity refusal.

## Decision summary

Address-space reuse and returning committed pages to the allocator are
different operations:

- **Slot reuse** prevents the 16M-slot high-water from being a one-way
  allocation limit. It reuses already-committed column storage, so steady
  churn plateaus, but it does not reduce RSS after a peak.
- **Page decommit** can reduce RSS after a peak. It does not create ids, and
  the current `PagedArray` cannot decommit safely: its
  `OnceLock<Box<[T]>>` pages deliberately provide stable references forever.

Build bounded, generation-checked slot reuse first. A `NodeId` remains a
`NonZeroU32`, encoded as a 24-bit slot code and an 8-bit object generation.
**A generation value is never repeated for one slot.** A slot that would wrap
is burned permanently rather than reused. At the current 16M-slot capacity
this raises the worst-case address lifetime from 16M allocations to roughly
four billion while preserving a proof stronger than “256 is probably enough.”
Stage 4 remains the graceful backstop if the live set, pending set, or burned
set eventually exhausts capacity.

Page decommit is deferred and conditional on post-reuse measurements. If it
is ever justified, it requires a new page-ownership/read-quiescence design;
it is not an extension of the current `PagedArray`.

## Corrections made during acceptance review

The proposed design contained four unsafe assumptions:

1. Packing a generation into `NodeId` does not validate it. Today
   `NodeId::index()` is used directly against public component arrays. Without
   a per-slot current-generation value and a checked dereference boundary, a
   stale handle aliases the new object on the **first** reuse.
2. The stage-3b publication fence is not I/O quiescence. Resolver completions
   reject stale payload by `content_generation`, but still write
   `dma_in_flight` and perform scan/timing cleanup afterward. Reuse can race
   those writes unless the old lifetime has no outstanding I/O lease.
3. `RegistryChunk` contiguity is semantic today. `begin_metric_scan`
   reconstructs every initial leaf with
   `NodeId::from_index(first.index()..=last.index())`, and chunk ownership
   checks numeric ranges. An arbitrary free-list id set is not a
   behavior-neutral replacement.
4. Stage-1 owner allocation accounting samples `next_id` high-water.
   Recycled allocations would not advance it, breaking
   `allocated - retired == live + parked`.

The delivery sequence below makes each correction a prerequisite to reuse.

## Slot-reuse proof obligations

### R1. Non-aliasing identity

The low 24 bits encode `slot + 1`; zero remains unavailable, preserving the
`Option<NodeId>` niche. Capacity stays below the low-field maximum. The high
8 bits encode the slot lifetime generation.

Each slot has a current object-generation column that is never decommitted or
reset. Fresh bump-tail slots start at generation 0. Reuse advances the value
exactly once before publication. Generation 255 is the final usable
generation; its retirement burns the slot. No live `NodeId` is ever issued
with a `(slot, generation)` pair issued before.

This deliberately rejects the proposed “wrap degrades to pre-reuse behavior”
argument. Before reuse a stale id could never name another object; after a
wrap it would name one exactly. Revision stamps remain useful context guards,
but packed identity does not depend on every present and future consumer
remembering a second guard.

### R2. Checked dereference boundary

`NodeId::index()` masking is not a safety boundary. Before reuse:

- registry component arrays stop being externally indexable by arbitrary
  `NodeId`s;
- production `NodeId::from_index` is removed or restricted to fresh-slot
  construction inside the allocator;
- registry entry points validate the packed generation against the slot's
  current generation before reading or mutating it;
- deliberately unchecked access is private, accepts a validated
  lifetime-scoped capability, and is justified at each use.

An architecture test rejects direct component indexing outside the registry
and reservation implementation. A holder audit covers topology edges,
child tails, carets, `FoldSet`, UAST rows/graph snapshots, navigation anchors,
metric scans, pending metric inflation, whole-leaf outcomes, root-child line
indexes, viewport pins, resident LRU links, high-priority timing, and every
resolver request/completion.

One known required fix is `FoldSet`: it sorts and compares only
`node.index()`, which discards the generation. It must compare full identity
and reject or re-resolve stale entries before dereference.

### R3. Reuse eligibility is a conjunction, not one fence

A retired slot becomes reusable only after all of these are true:

1. **Publication fence:** the two-cohort stage-3b quarantine has advanced.
2. **Reclaim visit:** payload/residency is gone and the reclaim cursor no
   longer needs the node's topology.
3. **I/O quiescence:** the old object lifetime has zero resolver leases.

Request admission acquires an object-lifetime lease before enqueue. Every
success, failure, cancellation, and queue-refusal path releases it only after
its final slot or side-table write. Retirement prevents new leases. A reclaim
visit with outstanding leases creates a pending-reuse candidate; the slot
reaches the allocator only after the last lease releases. This is bounded
state, driven by completion wakes, and is distinct from adding an arbitrary
time delay.

Queued engine-thread metric results also carry the packed `NodeId` and
`content_generation`; their application validates both. No completion may
use “validate, then later write” across a reuse race. It must either hold the
lease through the write, use a generation-qualified CAS, or hold a lock that
spans validation and mutation.

Never-published candidates may skip the publication fence, but not reset or
reuse while an I/O lease exists. A path that cannot prove eligibility leaks
the slot rather than risking aliasing.

### R4. Recycled initialization and side-table cleanup

One centralized recycled-slot initializer resets every per-object column:
kinds, provider kinds, span, metrics, topology, child tail, authoritative and
hydrated data, partial residency, residency accounting/LRU links, DMA and
metric flags, terminal-newline state, and any column added later.

The lifetime transition also removes the old identity from registry-side
indexes and queues: metric scan state, incomplete roots, pending metric
inflations, whole-leaf outcomes, root-child line indexes, viewport pins,
resident LRU, and high-priority timing. Resetting SoA columns alone is
insufficient.

The object-generation column and content-generation column are not reset.
Tests compare a recycled slot with a fresh slot for all per-object state, and
a source audit makes additions to `UastRegistry` choose explicitly between
“reset per object,” “monotonic across reuse,” and “registry-global.”

### R5. Content generation remains a separate lease

Packed object generation answers “is this the same node lifetime?”
`content_generation` answers “is this result for the current bytes within
that lifetime?” They are not interchangeable.

Reclaim and reuse advance content generation; no reuse resets it. Its current
`fetch_add(...).saturating_add(1)` presentation is not a mathematical
no-wrap guarantee because the atomic addition itself wraps. Before reuse, the
increment becomes checked/fail-closed: exhaustion burns the slot. Tests cover
an old object generation, an old content generation within the current
object, and a completion racing retirement.

### R6. Exact reservations without adjacency

`RegistryChunk` becomes a reservation over an exact ordered id set, not an
arithmetic interval. Reservation is one transaction under allocator state:
take the requested count from reusable slots plus the bump tail, or change
nothing. A dropped/partially-consumed reservation returns unpublished slots
without issuing their identities.

Before non-contiguous reservations serve ingest or graft:

- initial metric scan accepts the explicit reserved leaf ids; it never
  fabricates ids from a numeric range;
- local-child ownership checks reservation membership, not numeric bounds;
- all production uses of adjacency and `NodeId::from_index` are removed;
- graft order and sibling order remain explicit topology, independent of id
  order.

The reservation remains the exclusive write capability behind
`UastRegistry`'s `UnsafeCell` safety argument. A recycled slot is reservable
only after R3, so no old resolver can still write while the new owner
initializes it.

### R7. Accounting distinguishes slots from allocation events

`next_id - 1` becomes explicitly **slot high-water**, not “allocated nodes.”
A monotonic `u64` allocation-event counter advances for every published
fresh or recycled node. Owner accounting samples allocation events so its
existing lifetime algebra remains exact:

```text
owner allocated lifetimes - owner retired lifetimes = live + parked
```

Registry diagnostics expose at least:

- slot high-water / capacity;
- occupied live + parked lifetimes;
- retired but not yet reusable candidates;
- reusable free-list depth;
- burned slots;
- cumulative allocation events.

Capacity admission uses `bump remaining + reusable`, excluding pending and
burned slots. The minimap currently uses `next_id` as a topology revision;
it moves to a real monotonic revision before `next_id` changes meaning.

## Page-decommit proof obligations

Page decommit remains optional stage 5d and is not authorized by this ADR
alone.

### P1. Replace the stable-reference page model

`PagedArray` stores pages in `OnceLock<Box<[T]>>`, returns `&T`, and promises
that pages never move. `OnceLock` cannot be cleared safely, and concurrent
readers can retain references. Decommit therefore needs a replacement page
ownership model plus a reader-quiescence proof (for example epoch/RCU,
reference-counted page leases, or locking that spans every borrow). A page
counter and `take()` are not sufficient.

### P2. Identity and content generations never decommit

Even with P1, the object-generation and content-generation pages remain
resident. Re-materializing either at zero can make an old completion match a
new object or new content. Keeping these small columns preserves R1 and R5.

### P3. Retired-read contract changes again

Stage 3b leaves retired topology and metrics readable but payload inert.
Decommitting those columns makes late reads return defaults or fail. Page
death requires every slot to be reclaimed, no reclaim cursor reference, and
no page read lease. `object-identity.md` must state the chosen inert/failure
contract before implementation.

### P4. Evidence and allocator policy

Slot reuse fills holes in already-committed pages, so wholly dead pages may
be rare. Page decommit proceeds only if dashboard measurements show material
post-peak sparse-page cost. Page-aware allocation/clustering is a separate
policy with its own fragmentation measurements; it is not smuggled into slot
reuse.

## Delivery sequence

1. **Stage 5a — identity and accounting foundations.** Introduce the packed
   non-repeating identity, current-generation validation seam, holder/source
   audits, I/O lifetime leases, allocation-event accounting, and corrected
   diagnostics. Generation remains 0 for fresh slots; no slot is reused yet.
2. **Stage 5b — exact-set reservations.** Remove semantic contiguity
   assumptions, generalize the reservation capability, and preserve exact
   reserve-or-refuse behavior. Still no reuse.
3. **Stage 5c — slot reuse.** Feed only R3-eligible slots to the allocator,
   centralize reset/scrub, burn rather than wrap, and expose free/pending/burn
   gauges. Stage 4 refusal remains enabled throughout.
4. **Stage 5d — optional page decommit.** Write a new design/implementation
   plan only if measurements justify P1–P4.

Stages 5a–5c were sealed as one implementation after their proof boundaries
were exercised together: exact reservations are the allocator capability, and
I/O-quiescent reclamation is the sole producer of reusable tickets. Stage 5d
still requires its own `plan:`/`impl:` pair if measurements ever justify it.

## Acceptance tests for the eventual implementation

- A stale `NodeId` never reads or mutates a reused object; first reuse and
  generation-255 retirement are both covered.
- A resolver completion held across retirement cannot publish payload,
  clear the new object's flags, alter its timing, or complete its metric scan.
- Reclaim-before-I/O-completion remains pending; the final completion makes
  the slot eligible and wakes maintenance.
- A recycled slot matches fresh per-object state, while both monotonic
  generations advance.
- Fragmented free slots satisfy an exact multi-node graft reservation; an
  insufficient mixed free/tail reservation changes no allocator or tree
  state.
- Initial metric scans visit the explicit leaves exactly once regardless of
  numeric id order.
- Owner accounting reconciles after fresh allocation, recycled allocation,
  graft retirement, parking/revival, and abandoned candidates.
- Reuse churn holds slot high-water flat; burn and refusal tests remain
  panic-free.

## Consequences

- The non-aliasing invariant from ADR 0001 remains true by construction,
  including at the 8-bit boundary; safety does not rest on probabilistic
  wraparound timing or universal revision discipline.
- The design is more staged than the proposal because validation,
  quiescence, contiguity, and accounting are real prerequisites, not details
  of a free list.
- Slot reuse makes steady-state registry memory and address use plateau, but
  does not claim to return memory to the OS.
- “There is no ceiling” is intentionally not claimed. The practical ceiling
  rises by roughly 256× and remains observable; stage 4 handles it honestly.
