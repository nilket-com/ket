# ADR 478 - JoinKeyReader: stop re-deriving the join key type per row

m06 (one equi-join, `lineitem ⋈ orders`) is the largest absolute gap on the ladder:
23.3 s against 1.25 s for the best reference, 18.6×. The same anti-pattern ADR 475
and ADR 477 removed from the value and group-key paths is present a third time in
the join key path — and here it is more clearly redundant, because the type is
already resolved at plan time.

## The mechanism

`join_key_at` (`join.rs:1270`) switches on `JoinKeyPlan` — a **plan-time** typed
decision — and then calls `single_join_key_array::<Int32Array>(key_arrays, "Int32")`
(`join.rs:1493`), which performs `as_any().downcast_ref::<T>()` on every call.
`join_key_at` is called per row from 19 sites across `join.rs` and `join/spill.rs`
(probe loops at `join.rs:1877,2270`, spill paths at `spill.rs:22,1835,1983`, …). So
each row re-derives, dynamically, a type the planner already knew statically.

Unlike ADR 475's eleven-branch chain this is a single downcast per row, so the
per-row cost is smaller — the ladder will price it rather than assume it.

## Decision

1. `JoinKeyReader` resolves the key arrays **once** per array set — one match on
   `JoinKeyPlan`, one downcast — and exposes `key(row) -> Option<JoinKey>`,
   mirroring `join_key_at`'s null contract, null-safe handling, generic-bytes path,
   and error text exactly.
2. The per-row loops construct one reader outside the loop and call `key(row)`.
   `join_key_at` is retained for single-shot callers and resolves through the same
   construction so the two cannot drift.
3. Scope is key extraction only: no change to hashing, build-row layout, probe
   strategy, spill format, or `JoinKey` representation.

## Acceptance

- Ladder movement on **m06** recorded before/after, with per-probe-row cost as the
  headline and the residual attributed rather than left as one number.
- A parity test across every `JoinKeyPlan` variant (Int32, Int64, Generic, and
  Generic under `null_safe_keys`) including null rows, asserting reader and
  `join_key_at` agree and that unsupported arrays produce identical error text.
- **Spill behaviour and memory accounting unchanged** — the spill paths are among
  the call sites, so identical keys must reach identical build rows.
- Full engine suite, architecture, and the armed 22-query parquet correctness sweep
  green.

## Guardrails

- Semantics first: a faster key extractor that changes a null, a null-safe
  grouping, or a spill partition assignment is a regression regardless of speed.
- If the measured win is small, that is the result — the ladder prices it, and m06's
  remaining gap gets attributed to build/probe/materialisation instead.
- No board wall claim; the ladder's arithmetic is the evidence.

## Forward

Whatever m06 retains after key extraction is fixed is the next join question —
build-side layout, probe strategy, or output materialisation — chosen by the same
subtraction method.
