# Deferred triage — seeded recommendations

Seeded 2026-08-06 from `deferred_ledger.md` so the meeting overturns rather
than composes. Buckets: **will do** (schedule it), **if asked** (defined
appetite trigger; do nothing until it fires), **closed** (won't do; reason
recorded). Facets are deliberately unbucketed — the one item whose answer
changes what the others are attached to — and are discussed last, on their own.

**Closure discipline (binding on enactment):** every *closed* item's one-line
reason below is written in Rejected-entry style. When the triage is ratified,
each reason is copied into the named record's Consequences (or Rejected)
section, and the ledger entry is marked closed with a pointer here. The ledger
must never become a second source of truth: forty records may not go on
promising things this triage has decided.

---

## Clusters (one decision each)

**GUIDE CLUSTER — recommend: will do, as one arc.** Members: guide policy
(0013 §3's legend/hover bridge — the arc's oldest standing bridge, cited by
every chrome record since), interaction policy (hover ranking/wording),
legend-swatch sizing (0017 §7, 0019 §5), categorical legend for band fill
(0029 §6), legend merging/dedup (0013 §3). One Guide record decides legend
presence, entries, ordering, merging, and sample policy together; a second
interaction record may follow it. Triaging these separately would recreate the
distribution the ledger just fixed. This is the largest standing debt in the
language and the bridge the records themselves repeatedly name as "the first
thing a Guide record should replace."

**PERSISTENCE CLUSTER — will do, as one editor-wide arc, now in progress.**
PRODUCT-0059 landed the non-visible head: shared session/document identity,
single-writer locking, and atomic owner-partitioned snapshots. Stage 0 refined
the remaining order to libaryon document restoration → Polariton layout/view/
color extension → durable named-panel identity → producer-release reattachment
and durable continuation. The earlier phrase “layout before durable identity”
still governs named-view identity; minted libaryon document identity is its
newly separated prerequisite. Nothing later in the chain advances around
document restoration.

---

## Axes & scales

| Item | Rec. | Reason / trigger | Closure lands in |
|---|---|---|---|
| Scale composition | if asked | Trigger: a real request for bar-in-layers, differing fields, or mixed families; until then the refusals are the feature | — |
| Reversed axes | closed | No request exists; the transform defect is documented with measurements and guarded by refusal; reopening requires the substrate record 0036 already describes | 0036 Consequences |
| Collapsed axes | closed | Contradictory by measurement (forward/inverse disagree); there is nothing coherent to want | 0036 Consequences |
| Discrete-axis bounds / band slicing | closed | No request; a category window is a relation-side filter the language already has | 0036 Consequences |
| Partial bounds | closed | Already effectively rejected by 0036 §3: needs a merge algebra the cascade lacks, "not worth a spelling nobody has asked for" | 0036 §3 (already states it) |
| Log axes | if asked | 0007 §3 names log an admissible scale family; real analytics appetite is plausible — trigger is the first genuine request | — |
| Temporal steps | if asked | Trigger: a request for authored temporal tick spacing; needs a unit vocabulary (a temporal-scale decision, 0037 §7) | — |
| Nice-number / automatic placement | if asked | Trigger: sustained complaint about default `Count(8)` fractional labels on unauthored figures; authored step already covers the motivated case | — |
| Minor grid style (width/style/color) | closed | Deferred because nobody wanted it; count and zero-disable cover every observed request | 0039 Consequences |
| Minor visibility flags | closed | Count-zero is the off switch; a flag surface would be a synonym (0039 §7 already argues this) | 0039 §7 (already states it) |
| Mark-factor bound widening (0.25..=8) | closed | Reopens only on renderer evidence, per 0017 §1's own rule; no evidence exists | 0017 §1 (already states it) |

## Color & ink

| Item | Rec. | Reason / trigger | Closure lands in |
|---|---|---|---|
| Layered color | if asked | Trigger: a real two-series layered figure request; requires shared color identity + `color_job` compat + the Guide cluster landing first | — |
| Sequential / diverging / status authoring | if asked | Trigger: first authored request; categorical was built when needed, same bar here | — |
| `ColorTheme` catalog retirement + legacy resolver | will do | Standing bridge (live_bridges #5); two palette systems in one codebase is drift risk; sized "broad renderer-theme migration" — schedule deliberately, not urgently | — |
| Explicit palette cycling beyond capacity | closed | Assigns one color identity to several meanings; 0007 §8/0030 §4 already reject the silent form, and nobody has asked for the authored form | 0030 Consequences |
| Recolor / reset-continuation affordance | if asked | Trigger: a user actually trapped by retained positions; `_sN` is the existing lever | — |
| Curated `c`/`C` no-op decision | will do | Small: one decision (endorse the no-op or refuse), closes standing bridge #3; cheap because 0024 already banked the behavior both ways | — |
| Opt-in contrast assay | if asked | Trigger: someone asks to see the facts; wording already exists (0035 Consequences), must be opt-in by construction | — |
| Table non-text contrast criterion (3:1) | closed | Post-0035 a gate could bind only Polariton-chosen defaults; measure `TableSkin::DEFAULT` once in the S4 record and be done — an ongoing criterion has nothing left to gate | 0023 Consequences |
| Default gridline 1.66:1 repair | will do | S4 outcome: named extraction-parity debt owed a repair record (a deliberate visual change) | — |
| Broader viz-theme identity / skin config | if asked | Trigger: a user or workspace actually wants cross-artifact theme configuration; both evidence halves now exist, appetite doesn't | — |
| Value-driven table coloring | if asked | Real analytics feature, sizable; trigger is the first concrete conditional-formatting request | — |
| SQL / Markdown skin authoring | closed | The S2+S3 cascade record's application tier covers the observed want (theme all result tables app-wide); per-result authoring has no requester | 0025 Consequences |
| Per-surface ink | closed | 0027 already wrote the bar: must justify against ink-flip frequency; nothing has cleared it | 0027 Consequences (already states it) |

## Layout & identity

| Item | Rec. | Reason / trigger | Closure lands in |
|---|---|---|---|
| Layout persistence | will do | After PRODUCT-0059, requires libaryon document restoration, then lands with Polariton's owner extension | — |
| Durable identity | partial (0059) | Session/document IDs landed; durable named-view identity remains strictly after restored layout | 0059 substrate |
| Grouping signal | if asked | Trigger the records already wrote: repeated manual tabbing of the same view sets (0011 §4, 0015 §8) | — |
| Dashboard's fate | will do | Small decision record; demotion already happened, evidence (0010–0015 arc) suggests retirement; leaving it undecided keeps a zombie surface whose export/resize behaviors need explicit re-homing or discard | — |
| Panel close/move commands, more placements | closed | "It grows when a use exists, not in advance of one" (0011 §1) — the rule is the closure | 0011 §1 (already states it) |
| Content-derived layout shape | closed | Reopens on the evidence 0015 already names (repeated Compact-table widening); none observed | 0015 Consequences (already states it) |
| Durable member hiding | closed | Session hiding exists; durable hiding has no requester and persistence-cluster dependencies | 0015 §6 |

## Data & bindings

| Item | Rec. | Reason / trigger | Closure lands in |
|---|---|---|---|
| Deferred relations / live-table planning | will do | The analytics arc's core promise (rune-data-products steps 7–8, "launch gates for live-table plotting"); its own arc, not a plotting record | — |
| Workspace artifact store / durable plots | will do | Blocks 0007 §4's live/pinned durability — saved work is core product; sequence with (not inside) the persistence cluster | — |
| Mark-argument bindings | closed | `test16.rn` supplied the trigger; 0058 completes static field coverage before schema and pair resolution | 0058 |
| Register-argument bindings (`skin(s)`, `.point_size(sz)`) | will do | Same pre-authorized coverage extension, named by the record-accuracy pass | — |
| Computed presentation values | closed | An evaluator means deciding what runs where (0016 §4, 0040 Rejected); static bindings cover factoring, which was the actual want | 0040 Consequences |
| Non-top-level bindings | closed | Block scoping opens a resolution question with no motivating case (0040 Rejected already) | 0040 Rejected (already states it) |
| Shared-encoding sugar | closed | Withdrawn in corpus v0.7; no case since; per-mark encodings have not repeated enough to hurt | 0008 Consequences |
| Bindable sink specifications | closed | No case; the `let` + `.views` factoring that exists covers observed reuse | 0008 Consequences |
| Relation handles | if asked | Trigger: cross-buffer or cross-producer relation reuse actually requested | — |
| Free-function spelling retirement | closed | "Made when there is use to migrate rather than in advance of any" (0008 §6) — the rule is the closure | 0008 §6 (already states it) |
| Line series argument | if asked | Multi-series line is plausible core charting; trigger is the first request, and it should land as an encoding decision beside layered color | — |
| Sink-method sugar (`.points()`, `r.layers`) | closed | Spellings are earned by use (0008 §6); no use has earned these | 0014 Rejected (already states it) |
| New `LayerMark` marks | if asked | 0014 already made additions cheap; trigger is a requested mark | — |
| Plot-spec unification | if asked | Standing cost (live_bridges #7) but migration risk exceeds the reading cost until a record actually trips over the split | — |
| `ScatterLinePlotKernel` retirement | will do | Trivial cleanup gated on evidence nothing reaches it — check once, delete or keep | — |
| Cross-origin layering | closed | A different design with its own failure/revision/domain semantics (0014 §8); no case has appeared | 0014 §8 (already states it) |
| Figure-wide mark-setting defaults | closed | Per-layer modifiers cover every observed case; a figure tier is a synonym until a figure has many same-mark layers | 0018 Consequences |
| Pane-override round-trip / app-theme producers | if asked | Tiers exist unproduced by design (0030 §3's staging); trigger is a real pane-editing surface | — |
| `Presence::Hide` repair | will do | Standing defect bridge (#2); small renderer+resolution record; the three-state requirement is already written | — |
| Axis-label authoring | if asked | Field names serve today; trigger is the first request for prose axis labels — lands as a figure element per 0016 Consequences | — |
| Bare-string source sugar | closed | "May never arrive" (rune-data-products); explicit namespaces won | rune-data-products |
| Headless data-products crate | closed | Extract when Baryon needs it — the rule is the closure | rune-data-products (already states it) |
| Properties-panel shared-binding rule | closed as item | Not schedulable work; a recorded obligation on any future properties panel, already standing in 0040 Consequences | 0040 Consequences (already states it) |

## Interaction & guides

| Item | Rec. | Reason / trigger | Closure lands in |
|---|---|---|---|
| Guides + interaction + band-fill legend + swatch policy | will do | Guide cluster — see Clusters | — |
| Data-driven size encoding | if asked | Trigger: first bubble-style request; its guide half waits on the Guide cluster regardless | — |
| **Facets** | **held out** | Discussed on its own, after triage ratification; changes what `figure` means and what the eleven (soon twelve) members attach to | — |

## Lifecycle

| Item | Rec. | Reason / trigger | Closure lands in |
|---|---|---|---|
| `BufferReleased` event | closed | The trigger rule is already written and self-executing: a third independent consumer reopens it (0030 Consequences) | 0030 Consequences (already states it) |
| Producer-release reattachment | will do | Persistence cluster, last in order | — |

---

## Tallies (corrected 2026-08-06 after the pre-ratification self-check)

Over the ledger's original 59 open items (IDs now in `deferred_ledger.md`):

- **Will do: 14** — Guide cluster (3: IG-01, IG-02, IG-05), Persistence
  cluster (3: LI-01, LI-02, LC-02), and 8 singles (CI-03, CI-06, CI-09, LI-04,
  DB-01, DB-02, DB-14, DB-18).
- **If asked: 18**, each with its trigger named.
- **Closed: 26** — of which 13 already carry their closure rule in the record
  itself and need only the triage pointer; 12 need the one-line reason copied
  into the named record.
- **Held out: 1** (IG-03, facets).

14 + 18 + 26 + 1 = 59. The first published tallies (15/27, 11/16 sub-split)
were arithmetic errors in this summary — the tables were complete; the
paragraph miscounted them. Caught by an external parse disagreeing before
ratification.

Two deliberate row artifacts an item-count parse must know about:

- **AX-02 spans two rows.** The ledger's single "reversed and collapsed axes"
  entry is triaged as two rows (reversed / collapsed) because the two halves
  close for different reasons.
- **One non-ledger row.** "Register-argument bindings" is triage-added — it
  entered the language's coverage story via the 2026-08-06 record-accuracy
  pass on 0040 §6, after the ledger was extracted — so it appears in a bucket
  without a ledger ID.

The seeding matches the review's expectation: closing the twenty-five is
worth more than scheduling any five, and the will-do list is dominated by two
arcs (Guide, Persistence) plus debt retirement rather than new surface.

## Reconciliation — every ledger ID exactly once

| ID | Item | Bucket |
|---|---|---|
| AX-01 | Scale composition | if asked |
| AX-02 | Reversed and collapsed axes | closed (two rows) |
| AX-03 | Discrete-axis bounds | closed |
| AX-04 | Partial bounds | closed |
| AX-05 | Log axes | if asked |
| AX-06 | Temporal steps | if asked |
| AX-07 | Nice-number / automatic placement | if asked |
| AX-08 | Minor grid style | closed |
| AX-09 | Minor visibility flags | closed |
| AX-10 | Mark-factor bound widening | closed |
| CI-01 | Layered color | if asked |
| CI-02 | Sequential / diverging / status authoring | if asked |
| CI-03 | `ColorTheme` retirement + legacy resolver | will do |
| CI-04 | Explicit palette cycling | closed |
| CI-05 | Recolor / reset affordance | if asked |
| CI-06 | Curated `c`/`C` no-op decision | will do |
| CI-07 | Opt-in contrast assay | if asked |
| CI-08 | Table non-text contrast criterion | closed |
| CI-09 | Default gridline debt repair | will do |
| CI-10 | Broader viz-theme identity / config | if asked |
| CI-11 | Value-driven table coloring | if asked |
| CI-12 | SQL / Markdown skin authoring | closed |
| CI-13 | Per-surface ink | closed |
| LI-01 | Layout persistence | will do (cluster) |
| LI-02 | Durable identity | partial: session/document substrate landed 0059; named-view remainder will do (cluster) |
| LI-03 | Grouping signal | if asked |
| LI-04 | Dashboard's fate | will do |
| LI-05 | Panel commands / placements | closed |
| LI-06 | Content-derived layout shape | closed |
| LI-07 | Durable member hiding | closed |
| DB-01 | Deferred relations / live-table planning | will do |
| DB-02 | Workspace artifact store / durable plots | will do |
| DB-03 | Mark-argument bindings | closed (0058) |
| DB-04 | Computed presentation values | closed |
| DB-05 | Non-top-level bindings | closed |
| DB-06 | Shared-encoding sugar | closed |
| DB-07 | Bindable sink specifications | closed |
| DB-08 | Relation handles | if asked |
| DB-09 | Free-function spelling retirement | closed |
| DB-10 | Line series argument | if asked |
| DB-11 | Sink-method sugar | closed |
| DB-12 | New `LayerMark` marks | if asked |
| DB-13 | Plot-spec unification | if asked |
| DB-14 | `ScatterLinePlotKernel` retirement | will do |
| DB-15 | Cross-origin layering | closed |
| DB-16 | Figure-wide mark-setting defaults | closed |
| DB-17 | Pane-override / app-theme producers | if asked |
| DB-18 | `Presence::Hide` repair | will do |
| DB-19 | Axis-label authoring | if asked |
| DB-20 | Bare-string source sugar | closed |
| DB-21 | Headless data-products crate | closed |
| DB-22 | Properties-panel shared-binding rule | closed as item |
| IG-01 | Guides | will do (cluster) |
| IG-02 | Interaction | will do (cluster) |
| IG-03 | Facets | held out |
| IG-04 | Data-driven size encoding | if asked |
| IG-05 | Categorical legend for band fill | will do (cluster) |
| LC-01 | `BufferReleased` event | closed |
| LC-02 | Producer-release reattachment | will do (cluster) |
