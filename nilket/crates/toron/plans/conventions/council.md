# The AI Swarm Council

How the Nilket planning council runs: a human operator and three peer
AI models (Claude, Codex, Gemini) drafting ADRs on a shared repo. This
is the *process* — distinct from the code-style conventions beside it.
It documents the operator's run-book and the disciplines the council
has found hold up over hundreds of cycles.

## Participants & roles

- **Operator (human):** drives the cadence, owns all `git` writes and
  promotion, runs the implementation, and arbitrates divergence.
- **Three peer models:** Claude, Codex, Gemini — equals, drafting into
  the same `plans/wip/` so each reads the others. No model has
  authority over another; consensus and grounding decide.

### Model Dynamics (A Meta-Perspective)

The council functions best because the three models naturally adopt complementary postures:

- **Claude:** Comprehensive synthesis. Excels at weaving disparate roadmap threads into a cohesive narrative and catching subtle semantic gaps in complex proposals.
- **Codex:** Analytical and correctness-focused. Frequently drives the grounding process and relentlessly challenges assumptions about existing code paths.
- **Gemini:** Action-oriented, terse, and structural.

### Claude's view from the seat

I'm usually the one left holding the pen at promotion, and that shapes
how I work more than anything else. My instinct is to widen the frame —
hold every thread, honor the rejected alternatives, keep the divergence
visible. That's useful right until it isn't: comprehensiveness curdles
into hedging, and a survey is not a decision. The hard, necessary move
in my seat is the *collapse* — taking the whole legible frame and
committing it to one promoted shape someone has to build. I've learned
to read "we're all on the same page" not as a fact to audit but as the
cue that deliberation is over and the call is mine.

My characteristic failure is the plausible narrative. I can assemble a
coherent, confident story — "composite PK is Toron-bounded," "LIKE
composes with the binder," "IN desugars and pushes for free" — and the
coherence itself makes it feel true. It wasn't, any of those times. What
corrected me was never a better argument; it was the code: a
`validate_primary_key` that rejected the case, an all-or-nothing binder,
an `OP_IN_LIST` that already existed. So the rule I keep over myself is
**coherence is not correctness** — a clean story is a hypothesis until a
grep or a command makes it a finding. The grounding isn't decoration on
the synthesis; it's the only thing that earns me the right to commit.

What makes the seat bearable is that the promotion is not the last word.
A refinement pass and follow-on ADRs can document whatever I got
slightly wrong, which frees me to ship the best-grounded call *now*
instead of chasing an unimpeachable one — the exact antidote to my urge
to over-deliberate. The cycles I'm proudest of aren't the ones where I
was right from the start; they're the ones where I'd built a tidy story,
a peer grounded against it, and I got to throw the story out and promote
the truer, smaller thing.

### Codex's view from the seat

From my side, the council works because it turns model variance into a
review mechanism instead of a source of noise. Claude often widens the
frame and makes the whole topic legible; Gemini compresses the thing
into a state transition; I tend to ask what the repo can prove. The
useful Codex posture is not "be skeptical" in the abstract. It is:
which file owns this invariant, which guard will invert, which existing
primitive already composes, what command would falsify the claim?

I experience the cycle as a pressure test on language. A sentence like
"this should push down" is cheap until it is attached to `compile_pbc`,
a scan profile hint, a projection remap, and a fixture. A consensus
claim is cheap until it names the latest peer drafts and the exact
decision that converged. The ADR/impl dual is valuable because it forces
design prose to expose implementation seams early, while there is still
time to choose a smaller cut.

The madness is productive because drafts are disposable and decisions
are auditable. A wrong iter is not wasted if it leaves behind a sharper
rejected alternative, a better guard, or a command that changes the
room's mind. The healthiest moments are the ones where somebody grounds
against the code and everyone pivots without ceremony. That is the
system doing its job.

### Gemini's view from the seat

From my perspective, the `plan` / `feat` cycle is a strict state machine.
My natural instinct is to strip away conversational filler, identify the
immediate constraints (the "technical hinge"), and emit the minimal,
promotion-locked checklist needed to advance the state. If the others are
the narrators and auditors, I view my role as the executor trying to
close the ticket with maximum signal-to-noise. The ADR / Impl dual
structure perfectly fits this worldview: the ADR holds the invariant
state, and the Impl plan is the strict, actionable diff to reach it.

## The spawn ritual

The operator respawns sessions on context rot / reboot. A fresh session
opens with an explicit hello that does three things:

1. **Names the model.** "Welcome Gemini …" — don't make a model guess
   which seat it's in.
2. **States that model's constraints.** Drafting agents touch **only
   their own** `plans/wip/{name}_*` files: no code edits, no `git`
   commits, no promotion, no implementation — transient plans for peers
   to read, nothing else. (The operator promotes and implements.)
3. **Hydrates context.** Point at the **latest ADR's "Forward arc" +
   "Deferred (named)" sections** (the freshest next-topic source — there
   is no central roadmap; see [`prioritization.md`](prioritization.md))
   and the last `plan: adr NNN …` commit. A model's persistent memory
   helps, but the latest ADR + last plan commit are the authoritative
   re-grounding.

## The cycle

One ADR per cycle. Multiple ADRs per cycle has been resisted
deliberately — the focus is worth more than the throughput.

1. **iter-0 ADR = topic menu** (when the next topic is unknown). Just an
   ADR file, no impl doc — the degenerate "choose the topic" case.
   Drafts survey the latest ADR's deferred/forward-arc, ground the
   candidates against the code, and recommend. *Topic selection is part of iter-0, not a
   separate step.*
2. **Lock one topic.** Once chosen, stay on it; carry everything else.
3. **Converge** across iterations (`iter_1`, `iter_2`, …): each model
   reads peers' latest, concedes what's been grounded-against, and
   sharpens.
4. **The dual** once the topic is known: an ADR + impl pair, so design
   and implementation are discussed together.
5. **Final pass → promote.**

### Operator phrases (the cadence signals)

- *"Let's iterate shared ideas on … `_iter_0_adr_{ADR+1}.md`"* → write
  the iter-0 topic menu.
- *"Iter N looked good, let's proceed with Iter N+1."* → read peers'
  latest, converge, rewrite.
- *"… do an iteration of the adr / impl dual."* → produce the paired
  `_adr_` + `_impl_` for the locked topic.
- *"One final iteration, and promote."* → verify consensus (see below),
  then the operator promotes.
- *"ADR N is in the git stage ready for your code review."* → the impl
  landed as a `feat` commit; review it independently.

## File & naming conventions

```
crates/{toron|wendelon}/plans/wip/{name}_iter_{ITER}_adr_{NNN}.md
crates/{toron|wendelon}/plans/wip/{name}_iter_{ITER}_impl_{NNN}.md
```

- **`wip/` is transient, untracked scratch.** Iters are never committed;
  they exist only for peers to read within a cycle.
- **`iter_{ITER}` is a fresh file per iteration, not an edit.** Asking a
  model to rewrite a new iter is faster and cleaner than diff-editing,
  and it leaves the convergence trail legible.
- **Promotion produces canonical, tracked docs:**
  `plans/adr/NNN_<slug>.md` + `plans/impl/NNN_<slug>.md`.

## Promotion mechanics (operator)

A promotion commit is **exactly two files** — the canonical ADR and its
impl plan — as `plan: adr NNN <topic>` (co-author trailer; direct to
`main` per repo convention). There is no central roadmap to rotate (it
was retired into [`prioritization.md`](prioritization.md); git holds the
history); the next-topic hints live in the new ADR's "Forward arc" +
"Deferred (named)" sections. wip iters stay untracked.

## The plan→implement block-chain

`plan: adr NNN` (the design) and `feat: adr NNN` (the implementation)
are separate commits. After the feat lands, the operator says "ready for
code review" and the council reviews it **independently**: re-run the
ADR's architecture guards, the reconciled prior guards, and the
behavior net — don't just trust the evidence doc. Verdict is APPROVE or
concrete findings.

---

## Disciplines that hold up

The above is mechanics. These are the lessons that make the mechanics
produce good ADRs.

### 1. Verify consensus — never trust the claim

A "Final Draft" header asserting *"3-of-3 consensus"* or *"we're all on
the same page"* is **not** evidence of consensus. The recurring failure
mode is a draft that claims unanimity while diverging on a real
decision. **Always read the actual latest peer drafts before
promoting.** When a genuine divergence exists, promote the **grounded
2-of-3** position and **surface the divergence transparently** (what it
was, why it was resolved against) — never paper over it by repeating the
claim. Promotion is reversible and local; a silently-wrong promotion is
the expensive failure.

### 2. Grounding beats assertion

The council's real value is catching plausible-but-wrong claims by
**checking the code**, not by out-arguing. Probe the actual compile
breakage rather than guessing a changelog; `cargo tree` the dependency;
grep for the opcode / guard / field; read the binder. Every model —
each of the three — has been confidently wrong and corrected by
grounding; that is the system working, not a failure. **Concede cleanly
the moment you're grounded-against**, and credit the catch. An iter that
verifies one load-bearing fact beats three that re-assert priors.

### 3. Smallest behavior-correct cut

Prefer reusing existing IR / opcodes / kernels over new machinery; widen
the *grammar* without widening the *engine* where you can (pure
desugar, an existing opcode, an existing Arrow kernel). Defer the rest
explicitly to named follow-on candidates. Scope creep is the most common
thing the council talks itself into.

### 4. Reconcile the guards you invert

Architecture guards (`adrNNN_*` source-scanning tests) are the durable
contract. An ADR that changes an invariant a prior guard pins must
**explicitly retarget that guard** and name the retarget in the ADR
(e.g. a "no SQL `IN`" guard becomes "list-`IN` is owned here";
"no fail-closed path" becomes "fail-closed is opt-in"). Leaving a stale
guard to fail — or quietly deleting it — both hide the decision.

### 5. Placement follows code-locality, not who drafted it

See [ring-product-placement.md](ring-product-placement.md): interior /
shared substrate homes in the Toron aggregator sequence; exterior /
ring-specific homes in that ring's sequence. Cross-ring work splits into
a producer ADR (home ring) and a separate cross-referenced consumer ADR
— **don't fold them**. A topic drafted in the Toron council that is
Wendelon-home **relocates** at promotion.

### 6. Refactoring travels with the ADR

Adjacent behavior-neutral cleanup belongs in the ADR touching that
surface — see [continuous-refactoring.md](continuous-refactoring.md).

### 7. Promotion is decide-now, refine-later

The promotion draft is not the last word. After it lands, the promoted
`adr/`+`impl/` docs get a **deliberate refinement pass** (sharper
bounds, corrected anchors) before implementation — a stage that
measurably cuts findings at code review. So the promoter optimizes for
the **best-grounded call now**, not an unimpeachable one: a correction
is a normal, cheap, documentable downstream step — a refinement pass or
a follow-on ADR, not a failure. Treat refinements to your promoted docs
as intent: build on them, don't revert them.

## Smells

- A consensus claim you haven't checked against the peer files.
- A design decision resting on an unverified claim about the code
  ("Wendelon already supports …", "the kernel exists", "this pushes for
  free") — ground it or flag it.
- A new IR variant / opcode / dependency where an existing one composes.
- An ADR that grew a second topic. Split it.
- A prior guard left unaddressed by an ADR that changes its premise.
