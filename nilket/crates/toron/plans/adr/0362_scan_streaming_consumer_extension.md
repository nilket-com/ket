# ADR 362 - scan-streaming consumer extension (clear q21's remaining scan-output wall)

## Status
Proposed.

## Context

Press on the sf=10 fix arc. ADR 361 greened q14 and advanced q21: the gauge accumulator is gone; q21 now hits the
**core `scan output accumulator`** on a **later materialized scan edge** 361's fence (the streaming-join consumer)
didn't match - **~20 KB over 1 GB** (requested 1,073,762,000 / available 1,073,741,824). This cut **streams q21's
remaining scan edge** to clear it. The fork rule (operator): **prefer measured structural widening over a per-query
edge** when the evidence supports it. Core stays **topology-named, no q21 identifiers**.

## Decision

### The fork rule (Phase 0 decides; bias to structural)
- **Same family, fence too tight:** if the remaining edge is the **same shape ADR 361 already handles** and only missed
  because the recognizer's consumer match is too tight, **widen the shape-named fence once** - cleaner than a second
  narrow path duplicating the concept.
- **Genuinely different consumer:** keep ADR 362 as a **bounded new consumer edge**, named by **topology** (e.g.
  `scan-to-existence-build` / `scan-to-semi-anti-input` / `scan-to-exchange-input` - whatever Phase 0 proves), not by
  q21.
- Bias: **structural widening** when justified; per-query edge only when the consumer is genuinely different. No
  per-query momentum.

### Tightening 1 - Phase 0 names the exact edge with provenance
The producer only says `scan output accumulator` (generic). **Do not implement off the generic producer alone.** Phase 0
must name the **exact** materializing scan edge with provenance: a **bound-plan path / table / consumer map** (or a
temporary trace) showing which scan (which table, ~1 GB) feeds which consumer, and why 361's streaming-join fence missed
it. That map decides widen-vs-new-edge.

### The fix
Stream the named edge into its consumer via the existing `bound_scan_request` + direct-scan mechanism (no new inline
`ScanRequest`; Gate 1's drift test still covers it), bypassing `materialize_scan_stream -> Vec`. Either the widened
fence or the new topology-named consumer edge, per Phase 0.

### Tightening 2 - correctness wording (what the harness actually proves)
The `correctness` command is **strict-oracle only** for these queries. So:
- **Strict parity:** `correctness --query q21 --sf 0.01` (strict) - q21 already passes; it must stay digest-identical.
- **Dev parity:** covered by a **focused parity/unit test** (streamed-scan == materialized-scan output, dev mode) or a
  **live perf digest comparison**, **not** by claiming `correctness --mode dev`.
Do not assert "both modes via correctness"; strict via the command, dev via the parity test/digest.

## Acceptance (green-or-next-wall; correctness exact, precisely scoped)
- q21's core `scan output accumulator` wall is **gone** at sf=10 under fixed 4 GB; q21 is **green OR advances to a named
  next wall** (332).
- **Strict correctness:** q21 `pass@compare` at sf=0.01 (the strict oracle) stays green.
- **Dev correctness:** a focused parity test proves the streamed edge == materialized edge in **dev (Float64)** mode
  (sf=0.01) - not via `correctness --mode dev`.
- **Provenance:** the exact remaining scan edge named (bound-plan/trace), not the generic producer.
- **Reuse Gate 1:** the edge's scan goes through the shared `bound_scan_request`.
- **De-branded core:** topology-named symbols/comments; no q21/qNN in `crates/toron/src`.
- **Behavior-neutral at sf=1** via live checks (not `matrix` / `correctness-matrix` cache renders); non-matched plans
  unchanged; bounded peak RSS vs the materialized ~1 GB.

## Guardrails
- **Prefer structural widening** - widen the shape-named fence once if same-family/too-tight; a new edge only if the
  consumer is genuinely different (topology-named).
- **Provenance-named edge** - the exact scan/consumer, not the generic `scan output accumulator`.
- **Correctness scoped** - strict via the command, dev via a parity test/digest; no `correctness --mode dev` claim.
- **Reuse the shared builder** - no new inline `ScanRequest`; drift test covers it.
- **Topology-named core** - no q21/qNN in the executor.
- **Green-or-next-wall; behavior-neutral at sf=1** via live evidence, not cached matrix renders.

## Risks
- **Per-query momentum** - the fork rule biases to structural widening; a second narrow path needs the genuinely-
  different-consumer justification.
- **Implementing off the generic producer** - Tightening 1 (provenance map/trace) prevents guessing the edge.
- **Overclaiming dev correctness** - Tightening 2; dev parity is a test/digest, not the strict-oracle command.
- **Silent answer change** - the shared builder + the dev/strict parity gates.

## Forward arc
362 (q21 remaining edge -> q21 green or its next named wall) -> the **deeper materialize-each** terms (intermediate
operator outputs, exchange buffers - ADR 349) on the 359-redux atlas -> sf=10 fix arc -> sf=100 / Rune. If 362 widens
the fence, that widened scan-streaming recognizer is the reusable substrate for the next edges.
