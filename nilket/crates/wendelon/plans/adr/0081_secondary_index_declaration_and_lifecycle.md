# Wendelon ADR 081: Secondary-Index Declaration and Lifecycle Orchestration

**Status:** Proposed 2026-08-09.
**Category:** Capability gap discovered while attempting the ADR 0539/0541 confirmation rerun,
whose window opened 2026-08-08 and closed 2026-08-09. Wendelon owns a complete secondary-index
mechanism that no supported production path can declare or drive.

## Evidence that forced the decision

The ADR 0541 confirmation rerun requires creating a secondary index explicitly on a clean
profile. It cannot. The gap is narrower and more specific than "no wire format exists":

| Layer | State |
| --- | --- |
| Control-ring carrier | **Exists.** ADR 052 routes index mutations through `SchemaUpdateEnvelope` with `SchemaObjectKind::Index` |
| Durable catalog and lifecycle | **Exists.** `SecondaryIndexState { Building, Ready, Dropping }` persists in `SecondaryIndexCatalogRow` |
| Backfill driver | `run_secondary_index_backfill_once` has **only test callers** |
| Drop-GC driver | `gc_secondary_index_segments_for_dropping_index` has **only test callers** |
| Typed client / operator surface | **Absent.** No Wendelon CLI verb, no typed client method |
| Toron | contains zero references to `secondary_index`; SQL has no index DDL |
| `SecondaryIndexDescriptor` | constructed only in benches and test modules |

So the substrate is built and the *drivers and surface* are missing. Declaration alone would
leave an index `Building` forever, because nothing in production ever advances it.

Two consequences, both load-bearing:

1. **The long-open provenance question is answered.** No production path declares a secondary
   index, so whatever the wedged soak profile held did not arrive through one.
2. **ADR 0539's Acceptance 6 cannot pass until this closes.** The declared gap-gauge series can
   never appear on a clean profile. The 2026-08-09 rerun recorded ADR 0541's Acceptance 5 as
   **failed**, not waived, for this reason.

This record is Wendelon-home under the placement test: the storage ring would need declaration
and lifecycle orchestration whether Toron existed or not, and ADR 052 homes the feature here.

## Decisions

1. **The supported surface is a named set of typed client methods and CLI verbs.** ADR 052
   explicitly rejected a DDL parser, so this record names the surface rather than deferring it:
   client `create_secondary_index`, `drop_secondary_index`, and `list_secondary_indexes`
   (returning lifecycle state and per-replica coverage), carried by CLI `wendelon index create`,
   `wendelon index drop`, and `wendelon index status`. Observation is part of the surface, not an
   afterthought: an operator who can create an index but cannot see whether it is `Building`,
   `Ready`, or gapped cannot run the confirmation sequence this record exists to unblock. Index mutations continue to ride the existing
   `SchemaUpdateEnvelope` / `SchemaObjectKind::Index` carrier rather than gaining a parallel
   message family. Toron DDL, if ever wanted, is a separate successor decision and is not
   required to close this gap.

2. **ADR 052's durable lifecycle is preserved unchanged.** `Building → Ready → Dropping` stays
   the catalog's own state, durably stored, and declaration and drop *intent* remain recorded
   facts. What is derived is **coverage eligibility** — whether every historical source slab has
   a segment — exactly as ADR 0539 established. This record does not replace a durable state
   machine with a computed one; it separates intent, which is declared, from completeness,
   which is proven.

3. **Production drivers advance the lifecycle.** A backfill driver moves an index from
   `Building` to `Ready` once coverage is provably complete, and a drop-GC driver retires a
   `Dropping` index through ADR 0539's reconciliation gating. Both call the existing functions
   that today only tests call. Without this decision the surface in Decision 1 would create
   indexes that never become usable.

4. **Declaration seals the active slab.** Backfill covers historical slabs while declaration
   creates only an empty active buffer, so declaring against a non-empty active slab can omit
   rows written before declaration while still reporting historical completeness. Of the two
   candidate cuts, this record chooses **sealing at declaration** over a persisted high-water
   mark: sealing makes "historical" mean exactly "everything written before this declaration"
   with no new ordering concept, reuses the rotation path that already exists, and needs no
   per-row position tracking inside the active buffer. The cost is a forced rotation at
   declaration, which is bounded and self-limiting.

   Sealing the old slab and publishing `Building` together with the new active buffer are
   **one serialized admission cut**, not two steps. Every concurrent write lands wholly
   before the cut — and so within the sealed set the backfill will cover — or wholly after
   it, into the buffer the index maintains from then on. No write may land between them. A
   two-step ordering reopens precisely the omission this decision exists to close.

5. **`Ready` requires proof from every current voter, replacing ADR 052's no-global-readiness
   rule.** Today `IndexBackfillComplete` discards the `replica_id` it decodes and promotes on the
   first completion from any replica, while the drop-GC path is already voter-aware. That
   asymmetry is the defect: one replica finishing flips the catalog `Ready` for the cluster while
   its peers still carry coverage gaps. `mark_backfill_complete` takes `replica_id` and the
   current voter set, exactly as `mark_drop_gc_complete` does, and promotes only when every
   current voter has proven completion. Membership change is defined, not incidental: removing a
   voter drops its outstanding requirement; adding one does **not** demote a `Ready` index,
   because Decision 6 already keeps that replica correct.

6. **Planner eligibility turns on a durable per-replica initial-backfill receipt, not on
   coverage alone.** "Incomplete coverage" describes two states that demand opposite behaviour,
   and coverage alone cannot tell them apart:

   - **No completion receipt on this replica yet** — including a newly added voter still
     catching up. The replica has never had this index; it selects the scan it would have used
     anyway. Falling back is correct, and refusing would break queries for the ordinary reason
     that an index is new.
   - **A completion receipt exists and a durable segment has since disappeared.** The replica
     did have the index and has lost part of it. That is integrity loss, so the planner still
     selects the index and ADR 0539's gate refuses — exactly as Acceptance 6 requires.

   The receipt is durable and per-replica, recorded when that replica first proves complete
   coverage. Without it the two states are indistinguishable and this record would silently
   bypass ADR 0539's refusal, which is the opposite of what it intends.

7. **Declaration is observable immediately.** From declaration, this index's
   `coverage_gap_slabs` and `lookup_refusals_total` series are emitted with labels sufficient to
   attribute them to the index and its table. Without this the 0539/0541 confirmation gates stay
   unrunnable, since their measured window opens only once the series exists.

## Guards and controls

- The supported interface is exercised end to end on a clean profile; no test-only constructor
  or hand-edited profile discharges any gate here.
- A control declares an index **under concurrent writes against a non-empty active slab** and
  proves no row written before declaration is missing from the index once `Ready`.
- A control proves a `Building` index is not selected by the planner, and that the same index
  becomes selectable at `Ready`.
- A control proves declaration and `Building`/`Ready`/`Dropping` survive restart, with coverage
  eligibility recomputed rather than restored from memory.
- A control proves the declared series appear at declaration and carry per-index attribution.
- A control proves drop refuses while reconciliation cannot prove retirement is safe.

## Acceptance

1. An index is created through the supported production interface on a clean profile, with no
   test-only construction anywhere in the path.
2. A production driver advances it `Building → Ready` without test invocation, and promotion
   requires completion from every current voter — a control holds one voter incomplete and
   proves the catalog stays `Building`.
3. Its `coverage_gap_slabs` and `lookup_refusals_total` series exist and are attributable.
4. Queries fall back to scan while `Building`, then use the index at `Ready`; neither state
   refuses merely because the index is new. Two controls separate the receipt states: a replica
   with **no** completion receipt falls back to scan even when the catalog reads `Ready`, and a
   replica **with** a receipt that then loses a durable segment selects the index and refuses
   under ADR 0539.
5. Declaration seals the active slab, and a control declares under concurrent writes against a
   non-empty active slab and proves no pre-declaration row is missing at `Ready`.
6. A `Ready` index that develops a coverage gap still refuses, unchanged from ADR 0539.
7. Declaration, lifecycle state, and the per-replica completion receipt all survive restart,
   while coverage eligibility itself is recomputed rather than restored. A control follows a
   newly added voter end to end: it scans while it has no receipt, its production driver
   backfills while the catalog stays `Ready`, it records a durable receipt, and only then
   does it select the index — and that receipt still holds after a restart.
8. Drop retires cleanly through ADR 0539's reconciliation gating, driven in production.
9. The complete ADR 0539 / 0541 confirmation sequence becomes runnable on a clean profile.
10. Standard Wendelon, Toron, architecture, format and full `--no-fail-fast` gates pass.

## Rejected and deferred

- **Replacing ADR 052's durable lifecycle with a computed one.** Intent is a declared fact;
  only completeness is derived. ADR 052's *no-global-readiness* rule is a separate matter and is
  deliberately replaced by Decision 5, because single-replica promotion is unsafe once ADR 0539's
  refusal gate exists.
- **A persisted declaration high-water mark.** Rejected in favour of sealing (Decision 4); it
  introduces a per-row ordering concept the slab lifecycle does not otherwise need.
- **A Toron DDL parser.** ADR 052 rejected it; it remains a separate successor decision.
- **A parallel index message family.** The schema-update carrier already covers index objects.
- **Auto-creating indexes from query shape.** Declaring durable state as a side effect of
  reading is a much larger decision and is not needed here.
- **Exposing the descriptor constructor as the surface.** It carries no lifecycle, no backfill
  ordering, and no restart story; it is the shape that made this gap invisible.

## Consequences

Closing this unblocks ADR 0539's Acceptance 6 and ADR 0541's Acceptance 5, both currently
unmeetable by any permitted means. Until then the secondary-index apparatus is correct by
construction and unreachable in production — ADR 0539's integrity work protects a feature no
caller can turn on, and no operator can turn on.
