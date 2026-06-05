# Prioritization doctrine

The durable "what to build next, and why" doctrine — extracted from the retired
`plans/wip/roadmap.md` (in git history). The roadmap's *forward predictions* were
deleted on purpose: across the 154–164 arc, grounding the code at iter-0 overturned
the roadmap's pre-written lead candidate almost every cycle (160 Theta→containment,
161 evidence→range-overlap, 163 →multi-key-total-gap, 164 evidence→build). The value
was always in the grounding + the operator's steer, never in the stale candidate list.

**So there is no central roadmap.** Forward candidates live where they're freshest: in
each ADR's **"Forward arc"** and **"Deferred (named)"** sections. iter-0 seeds its topic
menu by grounding the *latest* ADR's deferred/forward-arc against the current code — not
by reading an aggregate that's stale the moment it's written.

## North star (V1 target)

"First Query with Integrity" + "Distributed Execution Correctness": correct analytical
results against a Wendelon-native cluster with pushdown maximized, identity flow real,
stream-shaped storage boundary, multi-process coordinator/worker mechanics surviving a
worker SIGKILL with byte-identical normalized output, and the first two-input relational
operator working across hash-partitioned and bounded broadcast strategies. Multi-tenant
governance, executor-model upgrade, sort-merge, output-side join streaming, and full
end-to-end natural backpressure remain post-V1.

## Prioritization philosophy

Toron is the compute ring of Nilket; Wendelon is the storage ring. We control both. The
boundary exists for scaling dynamics and resource isolation, not because storage and
compute are independent products.

- **Toron's reason for existence is smart-storage pushdown.** Every cut polishes a system
  doing its primary job along its primary axis.
- **Evidence drives scope.** The most informative artifact for the next ADR is the previous
  ADR's bench results or a code-evidence silent-drop / inert-feature defect surfaced during
  selection. Ground the candidate against the code before sketching scope — the from-scratch
  framing is usually 1/3–1/10 the real cut.
- **Minimize strategic rework.** Two narrow ADR shapes otherwise equivalent — prefer the one
  preserving plausible long-term seams.
- **Ring-product placement** (`prioritization` defers to [`ring-product-placement.md`](ring-product-placement.md)):
  placement follows *provenance* — would the storage ring build this if Toron didn't exist?
  No → Toron-sequence (even when the producer code lands in Wendelon, the 156/159 pattern);
  Yes → that ring's own sequence. Not code-locality, not who drafted it.
- **Complete cut, not granular slice.** Fold adjacent scope; defer only at genuine
  mechanism/topic boundaries — not to stay small.
