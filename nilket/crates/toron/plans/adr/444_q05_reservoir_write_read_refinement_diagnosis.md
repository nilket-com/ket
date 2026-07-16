# ADR 444 - q05 reservoir write/read refinement diagnosis

Refinement diagnosis (the disposition ADR 443 predeclared). No mechanism, no engine behavior change,
no speed claim. The 443 substrate is extended additively; 443's red discriminator row is not reopened.

## Context

**443 banked the bounded finding and left the discriminator red.** Reservoir write + read/probe own
**86.64-88.08%** of the q05 grouped-chain root in every child, but the leading leaf flips: read/probe
leads 3/5 (median **77.98 s**, spread **0.01018**), write leads 2/5 (median **75.06 s**, spread
**0.28821**). Structural facts steering this refinement:

1. **The rank flip is variance-driven.** Write is the profiled cohort's variance engine (removing it
   leaves the rest at spread 0.00850). This **localizes variance within the 443 cohort**; it
   *suggests* — but does not directly prove — the source of ADR 356's historical 0.1193 q05 spread.
2. **Both leaves are composites, and the read side is branched.** The write leaf spans writer setup,
   scan/decode, partition routing, IPC encode, encryption, physical write, and finish
   (`build_partitioned_streaming_scan_inner_build_reservoir`, `streaming.rs:15226`). The read/probe
   leaf (`probe_partitioned_streamed_inner_scan_build_reservoir`, `streaming.rs:15424`) takes a
   **resident branch** (`read_all_batch_pages` when the loaded set fits the cap) or a **streamed
   branch** (`rebuild_index_from_pages`, then probing), and **probing itself calls
   `try_pin_decoded_pages`**. The 443 qualitative perf child put
   `SpilledInnerJoinBuild::touch_cache_entry` at **12.46%** — cache/LRU accounting is a known hot
   frame and must not be folded into page-read/decode.
3. Owner chain (refreshed): `9507 → 11853 → 12072 → 15226/15424`.

## Decision — nest exclusive sub-leaves under the measured parents; declare a total, stability-qualified gate

**Schema-2 additive refinement.** The sealed `GroupedChainProfile` **retains the existing measured
parent timers**. Mutually exclusive sub-leaves nest **beneath** the two co-dominant parents; each
parent's **gap = parent − Σ(sub-leaves)** is recorded separately with a **≤ 10% ceiling per parent**.
The root residual rule is unchanged. `PROFILE_SCHEMA_VERSION` bumps; `GroupedChainSnapshot` is
serialize-only, so compatibility is proven by an **additive-field test** (schema-2 output is a strict
superset). The 429 surface stays untouched.

**Sub-leaf taxonomy at real, already-separable seams** (anchors fixed by Phase 0; an inseparable seam
merges with disclosure):

| parent | sub-leaf |
|---|---|
| write | writer setup |
| write | scan/decode |
| write | partition routing |
| write | IPC encode |
| write | encryption |
| write | physical write |
| write | finish/fsync |
| read/probe | probe partitioning |
| read/probe | physical read |
| read/probe | decrypt |
| read/probe | IPC decode |
| read/probe | cache/LRU accounting |
| read/probe | index rebuild CPU |
| read/probe | lookup |
| read/probe | materialization |

**The observer seam (locked).** The fifteen sub-leaves require hooks inside **shared spill code**. The
cut threads **one query-neutral, pointer-sized optional observer** from the admitted grouped-reservoir
path only — **no process-global lookup, no profiler field constructed for ordinary spill users**
(other call sites pass `None`, zero-cost). **CPU timers pause around nested I/O:** the index-rebuild
CPU sub-leaf never wraps the page-read/decode sub-leaves it interleaves with, and routing never wraps
encode/write — exclusivity is enforced at the observer, with overlap rejected.

**Phase 0 must census the read branches:** resident vs streamed branch counts per partition and
`try_pin_decoded_pages` outcomes; the ledger covers **both** paths (zero-taken branches report zero,
disclosed).

**Byte reconciliation (identity-keyed, amplification-aware).** Page indices are **local to each
`SpilledInnerJoinBuild`**, so the inventory identity is keyed by **`(spill_id, page_index)` with
ciphertext length**: unique ciphertext pages/bytes reconcile exactly to the reservoir spill role.
Physical ciphertext read calls/bytes, cache hits/misses, and decode counts are reported separately;
**read amplification = physical ciphertext read bytes ÷ unique ciphertext bytes**, unbounded by the
identity.

**The predeclared gate — mechanically total, stability-qualified, and defined at ties and zeroes**
(evaluated by a **pure, table-tested cohort evaluator outside the engine**):

- **Definitions.** Spread = `(max − min) / median`; a **zero median passes stability only when all
  five values are zero**, otherwise the quantity is unstable. All inputs are validated **finite and
  nonnegative**. G1's "largest" requires a **unique** largest sub-leaf in each child (a within-child
  tie fails G1). A zero/zero median comparison **cannot** satisfy G2's separation.
- **G1:** the same sub-leaf is the unique largest in **all 5** children **and** its share spread
  ≤ 0.25 → dominant.
- **G2:** else the top **median share** ≥ **1.15×** the runner-up (zero/zero excluded) **and** the
  selected share spread ≤ 0.25 → dominant.
- **G3:** else (top two within 1.15×) **exactly one** of the top two has absolute-time spread ≤ 0.05
  **and** that candidate's share spread ≤ 0.25 → it is dominant.
- **G4:** every other outcome is **red** — proving only that no unique sub-leaf was selected; two
  independent peers remain possible; **no shared round-trip producer is inferred**. Any round-trip
  mechanism needs its own causal evidence and an explicit disk/residency policy.
- **Table tests include:** tie, all-zero, median-zero/nonzero, runner-up-zero, boundary ratios and
  spreads, and each G-arm hit/miss.

**Affected-cost envelope (predeclared).** A 445 mechanism hand-off additionally requires the selected
sub-leaf's **median share > 0.2386** (the q05 band floor); a producer that cannot clear the band even
if fully eliminated is banked as a finding, not handed off.

**Variance ownership — decidable under signed deltas.** Between the maximum-write and minimum-write
children, the **signed** per-sub-leaf deltas plus the parent-gap delta must **sum exactly** to the
parent delta (checked arithmetic). Ownership is defined against **total positive contribution**:
`positive_total = Σ max(delta, 0)`. One unique sub-leaf **owns** the variance only if its positive
delta ≥ **60% of positive_total**; negative offsets are reported separately. **If the parent gap
contributes ≥ 60% of positive_total, the result is under-instrumented — widen the taxonomy rather
than name a mechanism.** Otherwise multi-source variance is reported honestly. Five-child correlation
is context, not the test.

## Phase 0 — map and census before hooking

1. One current strict sf=10 run confirms the live chain (`9507 → 11853 → 12072 → 15226/15424`) and
   the 443-pinned signature on the current binary. Drift ⇒ stop and revise.
2. Seam map for all fifteen sub-leaves; resident/streamed branch census + pin outcomes.
3. No new oracle identities — the 443 census anchors stage row flow.
4. One qualitative external perf child — outside every gate.

## Stability (5-child ledger gate)

Five fresh children, 356 envelope, temporally isolated. Report: all sub-leaf shares + absolute times,
measured parents + per-parent gaps (each ≤ 10%), root residual (unchanged rule), branch/pin census,
`(spill_id, page_index)` inventory identity + physical-read/cache/decode/amplification counters,
result tuple + delta-form spill + oracle identities exact 5/5, the evaluator verdict, the variance
decomposition, and the cohort wall distribution (future ≥23.86% band evidence).

## Mechanism discriminator (for ADR 445, not 444)

Selected sub-leaf (G1-G3 + envelope) maps to exactly one mechanism family: physical write / physical
read → I/O-side cut; encryption / IPC encode / decode → codec cut; partition routing / probe
partitioning → routing cut; cache/LRU accounting → cache-policy cut; index rebuild → index cut;
lookup → probe cut (439 family); materialization → materialization cut; scan/decode or writer setup →
upstream cut. **G4 red** → back to the council with two-peer evidence; no producer inferred.
**Gap-owned variance** → widen instrumentation.

## Acceptance

- **G1-G4 evaluated verbatim by the pure evaluator** (table tests sealed with the cut, including the
  tie/zero cases), with the selected outcome or honest red named; **envelope applied before any 445
  hand-off**.
- **Variance decomposition delivered** (signed exact-sum; positive-contribution ownership at ≥ 60%;
  gap-owned ⇒ under-instrumented verdict; else multi-source reported).
- Measured parents retained; per-parent gap ≤ 10% each; root residual rule unchanged; sub-leaf
  exclusivity + CPU-pause-around-I/O enforced at the observer.
- `(spill_id, page_index)` inventory identity exact; amplification counters reported; delta-form
  spill + oracle identities exact 5/5.
- Off-by-default schema-2 with additive-field compatibility; observer threaded only from the admitted
  path (ordinary spill users unaffected); 429 untouched; both-enabled/both-disabled parity.
- One persisted strict sf=10 q05 `pass@compare` under 4 GiB/no-swap.
- No mechanism, no speed claim, no query branding; `exec.rs` 7,499; join-root ratchet holds.

## Guardrails and risks

- **Parents stay measured** — gaps are evidence, and a gap that owns the variance is an
  under-instrumentation verdict, not a finding.
- **Observer discipline** — pointer-sized optional threading only; no global state; ordinary spill
  paths pay nothing.
- **Branch coverage honesty** — the resident/streamed census decides coverage; zero-taken branches
  disclosed.
- **The gate is total** — ties, zeroes, and invalid inputs all have defined outcomes.
- **443's red is not reopened**; the 356-spread attribution stays a suggestion until directly proven.

## Forward arc

**ADR 445** builds the one mechanism the evaluator + envelope select (or, on G4 red / gap-owned
variance, the council weighs next steps). q17 remeasurement stays a parallel, temporally isolated
chore. q18 one-pass + bimodality, q21 residual, q19 re-diagnosis + 438-profiler widening, q13 fresh
diagnosis, and Cut A (439 mirror) remain queued.
