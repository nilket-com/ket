# ADR 506 - m10 per-row decimal arithmetic: decompose dispatch-vs-arithmetic before any kernel

The band re-grounding (`8bdd1c70`) left m10 as the one self-contained engine target in the residual
band: `SELECT SUM(l_extendedprice * l_discount) FROM lineitem` — a global SUM over a per-row
Decimal128 × Decimal128 multiply across 48M rows, ~96 ms of real engine above the shared scan/decode
gauge floor. Bounded, single-surface, no cross-ring dependency (unlike the encoded-column contract,
which is council-bound). This ADR gates that cut; no kernel is pre-authorized.

## Grounded boundary

m10 is compute-bound and cache-resident (no join, no group table — a global scalar SUM), so the
ADR 505 miss-boundedness precondition is settled up front: **it is compute, not latency** (m10's
sibling m04 was IPC 4.38; expect the same class). A prefetch/MLP kernel is therefore *a priori* the
wrong tool — the same trap ADR 505 fell into. The open question is the mechanism *within* the compute.

## Gate — decompose first

Two independent annotate records (profiling binary, the ADR 502/505 method) split the ~96 ms into:

- **dispatch** — the bound-expression interpreter per row: walking the `Multiply` node, loading the two
  operands, dispatching to the decimal op, the SUM `AggState` match, `numeric_value`/`add_sum_value`
  call overhead;
- **arithmetic** — the actual i128 decimal multiply + scale reconciliation + the SUM i128 add.

The kernel is chosen from that split, not assumed:

- **dispatch owns the ~96 ms** ⇒ the cut is hoisting the per-row dispatch — evaluate
  `extendedprice * discount` as a **vectorized Arrow Decimal128 multiply** (batch kernel) feeding the
  SUM, so the expression-tree walk is paid once per batch, not once per row. Clean mechanism.
- **arithmetic owns it** ⇒ a vectorized-arithmetic cut is marginal (i128 multiply is already cheap);
  the honest outcome may be "no profitable cut," and the ~96 ms is recorded as an arithmetic floor.

## Acceptance rail (if a kernel is built)

- Same-binary ablation (`=0` restores the per-row path) with matched alternating A/B pairs.
- The measured dispatch/arithmetic **subcomponent shrinks explicitly** — not just execute_ms.
- **Checked parity:** precision, scale, overflow (i128), null propagation, and rounding must be
  bit-identical to the per-row path — decimal correctness is the whole risk surface here.
- Identical digest and output cardinality; zero spill.
- **No changes to storage, scan, or wire contracts** — this is a pure execution-layer cut (the
  encoded-column contract is the separate council arc).

## Forward

After m10 lands (or is recorded as an arithmetic floor with no profitable cut), the encoded-column
dict/RLE-preserving scan contract becomes the sole substantive engine lever and enters **council** as
a grounding-first design cycle — with the interior/exterior placement question and cross-ring
obligations the ADR 505 receipt named, and human steer since it is the campaign's first cross-ring
commitment. The primitive ladder will then be saturated (every rung closed, boundary-pinned, or
gauge-floor); the next measurement frontier is the composed q-series, which is also where the
encoded-column contract should be designed against rather than m04 alone.
