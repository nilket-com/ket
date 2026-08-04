# ADR 504 - m04 grouped-fold kernel: decompose first, dict-arm gated on a producer

The ADR 503 re-pin left m04 the #2 recoverable target (478 ms gap, **3.6×** — the worst ratio on the
board that isn't gauge-floor). ADR 499 bypassed its exchange and ADR 500 attributed its *residual* to
gauge framework, but the **~420 ms grouped fold itself was never optimized**. This ADR decides how the
fold cut is grounded and gated; no kernel is pre-authorized.

## Grounded from code (before any measurement)

- **ADR 494's borrowed-Utf8 path is default ON** (`borrow_single_utf8_group_key_enabled_from_env`:
  `!matches!(value, Some("0"))`). m04's fold dispatches through `entry_or_init_single_utf8` — a
  borrowed `&str`, `to_owned()` only on the cold insert — so on m04's 3 always-hit groups it pays **no
  per-row String allocation**. Allocation-elimination is already banked; it is not the target.
- **The array reaching the fold is a hydrated `StringArray`, proven as a theorem, not a probe.**
  `GroupArrayReader` has no `Dictionary` variant, and `group_array_reader` downcasts a Utf8 column to
  `StringArray`; m04 folds successfully through `GroupArrayReader::Utf8`, so a `DictionaryArray` would
  have errored at `group_array_reader`. It works ⇒ the parquet reader **hydrated 48M dict pages into
  concrete strings** (the Utf8 schema forces it), which the fold then re-hashes per row.

## The fork (three candidates)

1. **Dict-aware fold arm** — group-by-code over a `DictionaryArray`: map each distinct code to a group
   slot once per batch (≤3 lookups), then a vectorized count over the u32 code buffer. DuckDB's actual
   class of mechanism, and **general** (any low-cardinality dict column). Real production engine
   surface — but only if a live producer hands the fold a dict array.
2. **Vectorized hash/probe/update over the borrowed `StringArray`** — the ADR 497 pattern applied to
   grouping. Helps the *current* hydrated path regardless of the dict question.
3. **Compact-3-group specialization** — ruled out: benchmark-shaped, "a cut must deliver its name."

## Gate

1. **Decode-floor separation (mandatory).** Reconcile `parquet_decode (shared ~156 ms gauge floor) +
   fold (key-extract / hash / group-probe / count-update / per-row loop) + residual ≈ execute`. Scope
   the kernel to what the *fold* owns; if the fold is only ~250 ms of the 478 ms gap, the honest
   ceiling is stated as ~250, not 478. Do not let m04 re-inherit the gauge inflation it shed at 1,024.
2. **Borrow on/off A/B** — price what ADR 494 already banked, so the receipt can't double-count it.
3. **Annotate the fold** (two independent records) — the **hash-share is the ceiling** for "dict codes
   delete the hash"; the rest bounds the vectorized-hash win.
4. **Production dict-contract check** — the decision-maker between arm #1 and #2, grounded at Toron's
   storage/IPC boundary, not the gauge: does any native scan / Arrow-IPC / descriptor path produce or
   accept `DictionaryArray` / `DataType::Dictionary`?
   - **No live producer** ⇒ the honest sequencing is the **#2 vectorized fold now** (it helps the real
     hydrated path), with the **dict arm recorded as a forward capability gated on a producer
     existing** — not built speculatively (mind the ADR 500/501 gauge-vs-production line: making the
     *gauge* reader preserve dict would be optimizing the proxy).
   - **IPC admits dict arrays today** ⇒ arm #1 has a real feeder and is first pick.

## Forward

The kernel is chosen by the annotate split + the contract check, not here. The m06 layout kernels stay
held. Whichever fold arm lands, its acceptance rail is the standing one: ledger/annotate shows the
targeted fold component shrink, digest / 3 rows / zero spill invariant, on the 8,192 coordinate.

## Measured baseline + scope refinements (post-grounding, ADR 504 addendum)

Contract check resolved to **arm #2 (vectorized fold over the borrowed `StringArray`)** — no
`DictionaryArray` producer exists anywhere in the engine (only `binder.rs:896`, a size-estimation
fallthrough). Borrow A/B priced ADR 494's already-banked win at **−534 ms** (938→404 ms fold); the
receipt baseline is the **404 ms borrow-on fold**, not the raw ladder gap.

Loop-mode annotate (28k samples) splits the 404 ms fold: **probe ~72% (~290 ms)** (hash ~70 ms +
bucket-load + key-compare), **count-update ~24% (~97 ms)** (`aggregate.rs:1953` per-row `AggState`
enum-match + `Vec::get_mut`), loop ~4%. Precision ±a few points (decode-dilution). **The dict arm's
forward value is bounded at ~70 ms** — the hash-share a group-by-code fold would delete over the
vectorized win — recorded as a producer-gated capability, not built.

Two scope-honesty refinements to the kernel build:

1. **Batched key-resolution mutates.** Unlike the join probe, `entry_or_init_single_utf8` cold-inserts
   groups, so a within-window first occurrence can insert a group that later same-window rows hit. The
   window resolve must re-walk the live table per key (same argument as `insert_batch`), with a
   collision-dense equivalence test (the ADR 494 borrowed-hash-identity pattern extended).
2. **Scope this cut to COUNT-class aggregates.** Vectorized count-update (scan resolved group-index
   window, bump per-group counters) is clean for COUNT; SUM/AVG need per-group accumulator vectors and
   per-aggregate kernels — a bigger surface. The batched *window resolution* is shared by all
   aggregates; the *vectorized update* lands for COUNT-class now, with general-aggregate extension on
   the forward arc (ADR 495 precedent: the name matches the delivery).

Acceptance rail: **sub-component shrink** (probe and count-update, not just `fold_micros`), digest / 3
rows / zero spill invariant, borrow A/B not double-counted (baseline = 404 ms borrow-on), **MLP check
on the resolve path**, full workspace `--no-fail-fast` claimed explicitly.
