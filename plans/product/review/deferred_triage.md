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

**PERSISTENCE CLUSTER — recommend: will do, as one editor-wide arc, already
internally ordered.** Members: workspace layout persistence (0015 §11:
"editor-wide workspace work") → durable named-view identity (0010 §5) →
producer-release reattachment (0015 §5, 0030 §7) → durable continuation
(0030 §12). The dependency order is stated in the records and is not
renegotiable item by item; the arc starts when layout persistence starts, and
nothing in it can be advanced out of order. Not plotting work — sequence it as
its own editor arc.

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
| Layout persistence | will do | Persistence cluster head — see Clusters | — |
| Durable identity | will do | Persistence cluster, strictly after layout persistence | — |
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
| Mark-argument bindings | will do | Cheap coverage extension 0040 §6 pre-authorized ("needs no record"); first fixture that factors a field name triggers it | — |
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

## Tallies

- **Will do:** 2 clusters (Guide; Persistence×4) + 9 items — `ColorTheme`
  retirement, curated `c`/`C` decision, gridline-debt repair record, dashboard
  fate, deferred relations arc, artifact store, mark/register binding coverage,
  kernel-retirement check, `Presence::Hide` repair.
- **If asked:** 15 items, each with its trigger named.
- **Closed:** 27 items — of which 11 already carry their closure rule in the
  record itself and need only the triage pointer; 16 need the one-line reason
  copied into the named record.
- **Held out:** facets.

The seeding matches the review's expectation: closing the twenty-seven is
worth more than scheduling any five, and the will-do list is dominated by two
arcs (Guide, Persistence) plus debt retirement rather than new surface.
