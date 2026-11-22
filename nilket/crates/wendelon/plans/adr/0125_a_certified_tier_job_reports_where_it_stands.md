# ADR 0125 - A certified tier job reports where it stands

## Status
Proposed (local), 2026-11-22.
- **Per Codex:**
  - 01a204fe: DC-289 next; GO for a bounded diagnostic observer on `39e26664d` with focused offline controls.
  - 01a20520: HOLD of the first candidate for three corrections, folded in below:
    - admitted work visible before selection, and the claim wait split from the work under the claim;
    - one coherent snapshot per scrape, with `idle` rendered on a fresh registry and rendered values checked;
    - readings narrowed to what each path establishes.
- **Base:** origin/main `39e26664d`.
- **Unchanged:** the Section 8 launch, the 1 GiB qualification, and DC-286 (due before a campaign build). Retrospective attribution of DC-289's sealed run stays open.

## Context
- **DC-289:** the sealed ParentDecisions control advanced generations, but no certified tier delivery reached superseded-object retention (`storage_l2_superseded_objects_retained_total` stayed 0) and no step logged an error.
- **The certified tier step** runs on every leader LSM tick (100 ms). It stops silently at any of several points, and only errors log. The seal could not say where a step stopped, nor whether one stood mid-way (DC289-TRACE, missing evidence 1–4).

## Decision
### Scope
Diagnostic only, for the certified tier step. Nothing reads the observer to decide. There is no format, wire, durable-state or behaviour change.

### D1. A per-store observer: counters and one owned snapshot, fixed size
`storage/manager/certified_observer.rs`, a `CertifiedTierObserver` on each `SlabManager`:
- **Steps by outcome:** 13 relaxed atomic counters: `not_admitted`, `not_in_mode`, `fenced`, `not_leader`, `no_canonical_source`, `no_debt`, `no_eligible_slab`, `claim_lost`, `nothing_selected`, `generation_moved`, `rewritten`, `retired`, `failed`.
- **One snapshot** (`TierStepSnapshot`, `Copy`, fixed size) under a short mutex: the observed step's stage, stage entry (unix ms), start (unix ms), slab, op_seq and active gid (0 where not yet known), and the **last completed observed step that obtained an op** (its outcome, slab, op_seq, gid, duration and finish). The retention rule: an op-less step (no debt, nothing selected, claim lost, a refusal, a failure before allocation) never replaces that slot; a later op-bearing step (a failure after allocation included) does (Codex 01a2053f). The one identity tying a delivery to its install, credit and retention so survives the 100 ms idle ticks between samples.
- **One owner at a time.** `begin_step()` claims the snapshot only when no step is observed, and returns a `StepToken`. Transitions and `end_step` change the snapshot only under the owning token. A step begun while another is observed gets no token and is **counted only**; it never replaces the observed step.

### D2. Transitions, each recorded before the work or wait it names
**Admission** (`WendelonDataServer::certified_tier_step` / `_unpaused`):
- a tick not admitted is counted `not_admitted`;
- an admitted step calls `begin_step()` (`selecting`) **before** its test pause, the state snapshot, the debt check and slab selection, and passes its token to `run_certified_tier_step_observed`.
- The three-argument `run_certified_tier_step` is test-only now: it begins its own step for the storage tests that drive one without a data server.

**Stages, entered in order** (`claim_waiting` and `submitting` also record the slab and the op):

| Stage | Entered |
|---|---|
| `selecting` | admitted; snapshot, debt check, slab selection; no slab yet |
| `claim_waiting` | a slab is chosen; recorded; waiting for its exclusive rewrite claim |
| `reading` | the claim is held; eligibility rechecked; the published object read and decoded |
| `deciding` | the selection; the active gid is noted after it |
| `preparing` | rewrite build, upload and read-back, or retirement staging |
| `allocating` | preflight, history reservation, allocation; **no op yet** |
| `submitting` | the op is allocated (op_seq recorded); submission, verified answer, handoff |
| `delivering` | accepted; local install or retire |
| `crediting` | the ADR 0124 aged-row credit |
| `finishing` | after the credit: recording the replacement, retaining the superseded object |

**The end:** `run_certified_tier_step_observed` records exactly one outcome per step from its result:
- a refusal by its typed `CertifiedTierRefusal`;
- `Idle` by its cause (`no_debt` / `no_eligible_slab` / `claim_lost`);
- the named returns;
- `failed` for any other error.

The snapshot returns to `idle`; the observed step becomes the last step only if it obtained an op (D1's retention rule). A cancelled or stuck step is never ended, so its stage and entry stay visible.

### D3. Exported series (one store per production process)
| Series | Type | Contents |
|---|---|---|
| `wendelon_certified_tier_steps_total{outcome}` | counter | the 13 outcomes |
| `wendelon_certified_tier_job_stage{stage}` | gauge | one-hot over the 11 stages; `idle` on a fresh registry |
| `wendelon_certified_tier_job{field}` | gauge | `stage_entered_unix_ms`, `started_unix_ms`, `slab`, `op_seq`, `active_gid` |
| `wendelon_certified_tier_last_job_outcome{outcome}` | gauge | one-hot, the last op-bearing step; all 0 before any |
| `wendelon_certified_tier_last_job{field}` | gauge | `slab`, `op_seq`, `active_gid`, `duration_ms`, `finished_unix_ms` |

The four snapshot families render from **one copy** taken under a short lock, with formatting outside it (`render_certified_tier_snapshot`). Every transition publishes the whole snapshot (`set_certified_tier_snapshot`). Counters stay lock-free.

### D4. What each observation establishes (and does not)
**Steps:**

| Observed | Establishes | Does not establish |
|---|---|---|
| `not_admitted` only | the leader's LSM service ticks and the data server does not admit certified cleanup | — |
| `not_in_mode`, `fenced`, `not_leader`, `no_canonical_source` | refused at entry, for the typed reason | — |
| `no_debt` | at that step, `debt_page` was empty | why (look upstream: activation and debt labelling) |
| `no_eligible_slab` | debt was visible, but no tiered slab passed `certified_tier_source` | which condition excluded it (compare `unresolved_tier_cleanup_intents`) |
| `claim_lost` | the chosen slab failed eligibility once its claim was held | — |
| `nothing_selected` | an eligible, claimed slab, but the selection named nothing to reclaim | — |
| `generation_moved` | the active generation moved before allocation | — |
| `rewritten` / `retired` | the step **returned** that value: its certified op was decided Applied, handed off and delivered | local installation, the aged-row credit (ADR 0124) and superseded-object retention, which each have their own conditions and series |
| `failed` | an error | which one (the node log has its line) |

**A step that does not end,** by its stage and entry time against the scrape time:

| Stage | Reading |
|---|---|
| `selecting` | before any slab is chosen |
| `claim_waiting` | waiting for the recorded slab's claim |
| `reading` | the claim is held; reading or decoding |
| `deciding` | the selection |
| `preparing` | building, uploading or staging |
| `allocating` | before an op exists |
| `submitting` | the recorded op awaits its answer or handoff |
| `delivering` | accepted, installing locally |
| `crediting` | aged credit |
| `finishing` | recording the replacement or retaining the superseded object |

`last_job` gives the last completed observed step that obtained an op (store = the node's series): its outcome, slab, op, generation, duration and finish. Op-less ticks since then are in the counters, not in this slot. A value of 0 means unknown.

### D5. Cost
- **Memory:** fixed; 13 counters and one `Copy` snapshot per store, the same in the registry.
- **Per transition:** one short mutex section and one wall-clock read.
- **Idle:** with nothing to do, an admitted tick costs `begin_step`, one counter and `end_step`.

## Controls (existing Wendelon lane)
**Unit (observer and registry):**
- `a_concurrent_step_never_replaces_the_observed_one`: a step begun while another is observed gets no token; its transitions and end change nothing but its count; only the observed step's end makes it the last step.
- `a_fresh_registry_renders_idle`: one stage (`idle`), every field 0, every last outcome 0.
- `the_last_slot_keeps_the_last_step_that_obtained_an_op`: a rewrite with an op fills the slot; a following `no_debt` and a `nothing_selected` (slab chosen, no op) leave it while their counters advance and the stage is `idle`; a failure after allocation replaces it.
- `every_render_is_one_coherent_snapshot`: a writer alternates two distinct snapshots (`delivering` S1/op 101/gid 1001; `claim_waiting` S2), and 2,000 concurrent renders each show exactly one stage with that snapshot's slab, op, gid and entry time, never a mix.

**Cluster (ADR 0122's `aged_l2`; the node's own sweeper, op driver and LSM tick running). Each checks actual rendered values** (the store's own snapshot rendered by the exported formatter):
- `an_idle_step_records_why_it_stopped`:
  - while paused, the worker's ticks are counted `not_admitted` and `idle` is rendered;
  - after S2 is retired and S1 rewritten, a further, op-less step records exactly one stop, once (observed: debt visible, `nothing_selected`): its counter advances and `idle` is rendered, while the rendered last step is still S1's rewrite (`rewritten` 1, the stop's outcome 0, S1, its op, its gid).
- `a_step_is_visible_before_selection_and_either_side_of_its_claim`:
  - held at its pause before the state snapshot: `selecting`, slab 0;
  - with S1's claim held by the test: `claim_waiting` on S1, op 0 and gid 0, unfinished and not counted, still so after 300 ms;
  - held after acquiring the claim (`CertifiedTierCut::AfterClaim`): `reading` on S1;
  - released: `rewritten` +1, and the last step is (S1, its op, gid above 0, duration of at least 300 ms), rendered `idle`.
- `a_delivery_is_visible_by_stage_op_and_generation`:
  - S2 is `retired` +1;
  - S1, held after acceptance, renders `delivering` with exactly S1, the committed provenance's op and its gid, with `rewritten` not yet counted;
  - released: `rewritten` +1, and the rendered last step is (`rewritten`, S1, op, gid), with `idle`.

**Build:** a non-test `cargo check -p wendelon --lib` is compared with the DC-286 baseline (4 warnings): this change adds none.

## Consequences
- A future control can name where certified tier work stopped, or stands, from its scrapes alone.
- The wall clock dates entries; it does not order events.

## Open Questions
- Whether the step's per-tick `state_snapshot()` (a whole-state clone on the leader) bears on DC-290's periodic stalls. Not addressed here.
