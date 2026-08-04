# ADR 420 - q22 singleton-row resident locator: eliminate join-spill page-probe amplification

## Context

Post-419 board: 15/22 green. ADR 419 removed q22's resident-state wall (compact locator,
50,331,648 / 67,108,864) but the cell stays red at **join spill page-probe amplification** - a
600-second timeout, not a Toron producer: every one of ~15M orders probes resolves one page
through the locator and `decode_join_spill_page_for_key` linearly scans the full page
(`exec/join/spill.rs:2676-2701`, no ordering, no offsets, no early exit), for 704-915 GiB of
decode volume. `read_page` clones even on cache hit (`:616`), so caching does not remove the
term; only ~16.6 MiB of cache headroom remains against a 31.5 MiB decoded working set, so with
random keys across 481 pages even a perfect page-local mini-index leaves hundreds of GiB of
per-miss decrypt volume. The mini-index / key-clustered-pages / sharded-locator directions named
by 419's forward arc optimize the wrong term **for this shape** (they remain right for q21,
whose refs genuinely span pages).

### The reframing fact: q22's pages carry nothing the probe cannot already hold resident

Council grounding this cycle converged on a stronger move than shrinking the page cost -
removing the page from the probe entirely:

- 419 measured q22 at **1,500,000 one-row build-map entries** (21-byte page entries: key + a
  single u32).
- The stored u32 values are row ordinals into `left_batch`, constructed from
  `0..left_batch.num_rows()` at all three production writers (`exec/join.rs:3020-3034`,
  `:3136-3150`, `:3260-3274`), and that same batch is passed **by reference** into each probe
  (`:3085-3095`, `:3207-3221`, `:3330-3343`). No production writer has a detached-row lifetime.
- The probe loops feed looked-up u32s to `append_join_matches_for_kind` (`:3382-3390`,
  `:3441-3449`, `:3508-3516`), which applies the residual against the resident batch and updates
  the build-match bitmap - Inner/outer/semi/anti semantics never depend on page decoding.
- The writer still holds each key's row list at the flush site where it currently inserts the
  page ordinal (`spill.rs:495-497`, `:530-532`) - for a singleton build it can insert `rows[0]`
  instead, with the page payload and row ordering untouched.

For an all-singleton build, the entire semantic content of the spill pages fits in the locator
419 already pays for. Reinterpreting the existing u32 value as a **build-row ordinal** keeps the
resident arithmetic exactly at 50,331,648 and makes the probe a pure resident lookup: zero page
reads, zero decryption, zero linear scans. The amplification term is not reduced - it is gone.

### Two traps the design must respect (council-grounded)

- **Per-entry tagging breaks the Int32 layout.** `CompactJoinSpillPageEntry<i64>` has 4 free
  padding bytes, but `CompactJoinSpillPageEntry<i32>` is exactly `{ i32, u32 }` = 8 bytes - a
  discriminator grows it 50%. The mode is therefore **locator-wide**, decided once per build.
- **`Vec<u32>` at the lookup boundary re-creates amplification.** `SpilledJoinBuild::lookup`
  returns `Vec<u32>` (`spill.rs:586-605`); returning `vec![row]` would trade page amplification
  for ~15M singleton heap allocations. All three probe loops consume `matches.into_iter()`, so an
  allocation-free result slots in without touching them.

## Decision

### Phase 0 - record before the cut

1. On the real q22 sf=10 writer: primitive plan, key count, **singleton count, multi-row count**,
   maximum row ordinal; the unchanged 8-byte Int32 / 16-byte Int64 entry sizes and the completed
   50,331,648-byte locator accounting.
2. Re-verify the production ownership proof above (three call sites, resident left batch,
   bitmap-based match completion) at current HEAD.
3. Baseline observability through the 600-second boundary: page reads, decrypted bytes,
   page-cache hits/misses, probe time. Temporary instrumentation is removed before the cut.
4. If q22's build is not all-singleton at HEAD (contradicting the 419 census): stop and re-scope.

### The cut - a value reinterpretation behind a whole-locator mode

**1. Mode as storage variants, not a bool.** Extend the existing enum so the unwanted state is
unrepresentable rather than guarded:

```text
JoinSpillPageLocatorStorage
  Encoded(HashMap<Vec<u8>, Vec<usize>>)     # unchanged fallback
  Int32Pages(...)   Int64Pages(...)          # 419, unchanged
  Int32Rows(...)    Int64Rows(...)           # 420: same entry structs, value = build-row ordinal
```

A row-mode locator cannot be asked for a page ordinal - there is no such arm. Selection happens
once, before construction, from the metadata-only check
`build_rows.values().all(|rows| rows.len() == 1)` composed with the 419 key-plan fence:
primitive + all-singleton -> Rows; primitive + any multi-row -> Pages (the proven path,
conservatively); Generic/composite/null-safe -> Encoded. Per-key specialization is explicitly
deferred until a real mixed-shape driver appears.

**2. Insert seam.** The flush site already holds both the page ordinal and each entry's row list;
the Rows variants insert `rows[0]`, the Pages variants insert `page_index`. Page payload bytes,
entry encoding, page assignment, and build-row order are untouched.

**3. Allocation-free lookup result.** Replace the `Vec<u32>` return with
`NoMatch | OneRow(u32) | Rows(Vec<u32>)` (the in-file `None/One/Many` idiom), exposing an
iterator the three probe loops consume unchanged. The iterator is itself a concrete enum, not a
boxed trait object; otherwise the cut would merely move the per-probe allocation from the row
container to iterator construction. The page-decode arm keeps `Rows(Vec<u32>)`; the resident
singleton arm is structurally scalar - no singleton `Vec` or iterator box exists to allocate.

**4. Everything else unchanged.** Page writes are kept (write-elision for fully-resident builds
is a named separable optimization - it would change spill_bytes telemetry, writer/guard/cleanup
observability, for a ~2 s win that is noise next to the probe). The 64 MiB budget, resident
guard, producer string, writer peak and page reserve terms, cache, cleanup, truncation and
malformed-key fences, and the 419 capacity accounting all stay byte-identical.

### Stop-lines

Phase 0 finds q22 not all-singleton -> re-scope before code. The seam demands a typed
`JoinBuildRows` build-map rework -> stop, name the follow-on (419's standing stop-line).

## Acceptance

- **Primary:** q22 sf=10 reaches persisted strict `pass@compare`, or advances to a **named,
  reproduced next wall** with the page-probe amplification gone (zero probe-side page reads on
  a temporary instrumented real-q22 profile, followed by the same outcome on the rebuilt,
  hook-free final binary). Reach honesty: the probe going resident may expose the next real
  limit; that is a wall move, not a failure.
- **Eligibility-discriminating locks:**
  - a production-helper all-singleton fixture selects row mode **before the first locator
    insert**, answers present/missing/anti probes correctly, and completes with
    `page_read_count == 0` - the amplification lock, backed by a `#[cfg(test)]` read counter on
    `read_page` (the 419 `legacy_insert_count` pattern);
  - a primitive mixed/multi-row fixture selects page mode for the whole locator, performs page
    I/O (`page_read_count > 0`), and preserves duplicate build-row order across page boundaries;
  - **LeftAnti + residual parity** on the row-mode path - q22's semantics rest on build-match
    completion, not paired output alone;
  - the scalar resident arm is **structurally allocation-free** (no singleton-`Vec` arm and no
    boxed iterator exist);
  - Int32/Int64 entry sizes and capacity-accounted bytes remain **exactly** at 419 values;
  - Generic, composite, and null-safe plans remain encoded/page-backed; the one-page-per-key
    pinning lock stays green;
  - page bytes, spill telemetry, writer peak, reserve, guard, producer, cleanup, and
    truncation/malformed-key fences unchanged.
- **Controls:** q21 (`semi/anti existence-set spill resident state`, 1,073,742,536 /
  1,073,741,824) and q18 (`streamed inner join build spill writer`, 536,899,968 / 536,870,912)
  reproduce their exact producers and bytes; the ten spill-bearing greens (q03 q07 q09 q10 q12
  q13 q14 q16 q19 q20) pass@compare with forensic bands.
- **Full engine gate** after fmt (`--no-fail-fast` both crates + architecture); no cap,
  multiplier, guard-term, producer, or page-format change; de-branded; no `.perf.data`.

## Guardrails

- **Locator-wide mode via enum variants** - no runtime mode bool, no per-entry tags, both 419
  entry layouts preserved bit-for-bit.
- **No allocation regression at the lookup boundary** - the structural lock, not a benchmark.
- **Page writes stay; elision is a named follow-on** - observability before micro-wins.
- **Mixed builds keep the proven page path** - conservative until a driver exists.
- **q21 untouched** - its representation ADR owns the duplicate-heavy contract.
- **Won ground is a control** - 419's exact accounting values re-asserted, greens re-proven.

## Risks

- **A wall behind the wall** - with the probe resident, q22's next limit (if any) surfaces for
  the first time -> reach honesty; named and reproduced is an accepted outcome.
- **Mixed-build forfeit** - a future mixed primitive build gets pages, not rows -> conservative
  by design; per-key specialization is the named extension if a driver appears.
- **Asymptote unchanged** - O(unique keys) resident, re-walls ~sf=15 like 419; the honest ceiling
  stays named (sharded/deferred index, q21's arc).
- **Census drift** - Phase 0 re-verifies all-singleton at HEAD before code.

## Forward Arc

q22 green moves the board to 16/22 and closes the page-probe amplification story with zero new
resident bytes. q21's representation ADR (421 candidate) opens with 419's completed-shape
evidence (15M keys, 59,986,052 refs; CSR-at-finish vs page-locator-with-residual-filtering vs
sharded/deferred existence - genuine competing contracts). Behind it: q18's spill-quota +
membership threads and the mixed-combo handoff follow-on, q02's join-output C-spec driver, q05's
diagnosis-first OOM, q11/q17's correctness-policy threads, and the q10 speed thread. The
sharded/deferred index remains the family's structural answer to unbounded key counts.

## Grounding evidence (file:line)

Amplification: `exec/join/spill.rs:2676-2701` (linear full-page decode), `:616` (cache-hit
clone), 419 results (481 pages, 31.5 MB spill, 704-915 GiB decode volume, 600 s timeout,
1,837,846,528-byte peak). Singleton census: 419 results (1,500,000 one-row entries, 21-byte page
entries). Ownership: `exec/join.rs:3020-3034/:3136-3150/:3260-3274` (u32 from
`0..left_batch.num_rows()`), `:3085-3095/:3207-3221/:3330-3343` (resident batch into probe),
`:3382-3390/:3441-3449/:3508-3516` (`append_join_matches_for_kind` + build-match bitmap).
Flush-site row lists: `spill.rs:495-497`, `:530-532`. Entry layouts: `spill.rs:177-180`
(`{key, u32}`; Int32 8 B zero-padding, Int64 16 B). Lookup boundary: `spill.rs:586-605`
(`Vec<u32>` return; `None/One/Many` idiom at `:267-272`). 419 accounting: 50,331,648 =
4,194,304 x 4 + 2,097,152 x 16, headroom 16,646,156.
