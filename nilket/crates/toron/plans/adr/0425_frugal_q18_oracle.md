# ADR 425 - frugal q18 oracle: a membership-first stream-merge that validates sf=10 under 4 GiB

## Context

Post-424 board: 17/22 green. ADR 424 made q18's **engine** green under the standard 4 GiB
cgroup (the membership-first composition; ~1.2 GB engine peak, answer validated correct at
sf=10). But the canonical in-process correctness comparison still OOMs at 4 GiB and only passes
at 6 GiB, so q18 is recorded `engine-green / reach-only` with the named
`q18_sf10_oracle_residency_gap`; the 6 GiB pass@compare is supplemental semantic evidence
carrying no board credit. This ADR retires that gap.

The OOM is entirely the **instrument**, not the engine. `q18_oracle_from_rows`
(`toron-tpch/src/lib.rs:11065`) holds three co-resident structures at sf=10:

| structure | entries | ~bytes |
| --- | ---: | ---: |
| `orders: HashMap<i64, (i64,i32,i128)>` | 15,000,000 | ~840 MB |
| `quantity_by_order: HashMap<i64, i128>` | 15,000,000 | ~600 MB |
| `customers: HashMap<i64, String>` | 1,500,000 | ~90-150 MB |

~1.5-2 GB of oracle state, co-resident with the generators and Toron's retained result -> the
4,365,652 KiB combined peak (424 results). The two 15M maps are the whole problem. This is a
**toron-tpch-only instrument cut, no engine surface** - ADR-worthy by the ADR 422 precedent (a
toron-tpch-only oracle/fixture correctness cut run through the full council cadence).

The qualifying set is 624 orders at sf=10 (424 census). A membership-first oracle - mirroring the
very engine schedule it validates - keeps resident only O(qualifying) rows plus a customer-key
membership set. The final 4 GiB run, not payload arithmetic, decides whether that representation
clears the board envelope.

## Decision

### The frugal oracle: a monotonic stream-merge

The existing public `strict_q18_oracle` name becomes the frugal production implementation and
computes the identical `Q18Oracle` (rows plus its three counter fields) without either 15M map.
Keeping the public name preserves the current correctness rail and integration-test API:

1. **First customer pass -> custkey membership set** (1.5M `i64`; 12 MB is key payload only,
   not `HashSet` residency; requested/observed capacity and process peak are measured in Phase
   0). The set is order-independent and required by the merge's customer-known filter.
2. **Streaming lineitem group-by** yields `(orderkey, sum)` lazily on key change, and increments
   **`selected_lineitem_rows` per raw lineitem row** (every row, matched or not). The stream is
   ordered in `l_orderkey` and **asserted so**. Repeated contiguous keys are normal lineitems
   in one group; only a new finalized group whose key is not strictly greater is a violation.
3. **Stream-merge** that grouped-sum stream with the orders stream (monotonic in `o_orderkey`,
   **also asserted**) by a two-cursor key merge, O(1) state. For each order key present in
   **both** streams whose custkey is in the membership set, classify by `sum > 30_000`:
   `qualifying` (retain the full row) or `threshold_fail` (scalar++). Order keys present in only
   one stream, or with a custkey not in the set, are skipped - exactly mirroring the dense oracle's
   three `continue`s (unknown customer / missing lineitems / missing order). Because a row
   contributes to a counter only when all three conditions hold, the merge's check order is
   irrelevant to the result.
4. **Second customer pass -> names** for only the distinct qualifying custkeys (624 at the
   sf=10 target; O(qualifying) at other accepted scales). The
   two passes are explicit iterator inputs/factories; one consumed customer iterator is never
   treated as reusable. Pending order metadata lives in a small candidate type until names are
   attached and `Q18Row` is constructed.
5. **Retain all qualifying rows**, sorted by the existing key; the **comparator alone applies
   `LIMIT 100`** (unchanged).

Peak resident is the observed customer-key container plus O(qualifying) rows and scalar
counters; Phase 0 records it and the canonical 4 GiB run proves the final envelope. Only the
orders and lineitem streams carry the monotonic assertion; both customer passes are
order-independent set/filter operations.

### The honesty spine (the oracle is the tripwire, not the optimand)

Per the standing doctrine that a correctness oracle must mirror the executor completely and an
honest red beats a lying green:

- **Only the dense reference becomes genuinely `#[cfg(test)]`** - rename/move it to an internal
  `lib.rs` test module as `q18_oracle_dense_from_rows` / `strict_q18_oracle_dense`, compiled out
  of the shipped crate. The public `strict_q18_oracle` remains production and is frugal at
  every scale.
- **Dense-vs-frugal parity is structural equality across sorted rows and the three counters**
  (`qualifying_orders`, `threshold_fail_orders`, `selected_lineitem_rows`) at sf=0.01 and sf=1.
  At sf=10 only the frugal oracle runs (the dense one would OOM - that is the point), backed by
  the 424 census counts.
- **Both order assertions are load-bearing** - shuffled input, a duplicate order row, or a
  previously finalized lineitem group key reappearing fails loudly rather than silently
  miscounting. Same-key lineitems inside the current group remain valid.
- **The non-vacuity guard stays armed and unchanged.**

## Acceptance

- **Primary:** the canonical in-process q18 sf=10 correctness comparison **passes under the
  standard 4 GiB cgroup**; `q18_sf10_oracle_residency_gap` is retired; the board moves to
  **18/22**. The historical 6 GiB run remains recorded in ADR 424 as supplemental evidence and
  continues to earn no board credit; ADR 425's new 4 GiB row supersedes the gap.
- **Parity locks:** dense-vs-frugal structural equality (rows in full sorted order plus the
  three counter fields) at sf=0.01 and sf=1.
- **Order-assertion locks:** shuffled orders, duplicate order rows, and a lineitem group key
  that reappears after another group fail loudly; contiguous same-key lineitems succeed.
- **Counter-fidelity locks:** `threshold_fail_orders` and `selected_lineitem_rows` match the
  dense oracle exactly, including the missing-lineitem, missing-order, and unknown-customer
  skips; the non-vacuity guard remains armed.
- **Scope locks:** the twelve established board controls and q17's oracle path are untouched;
  the frugal path is q18-only.
- **Full `toron-tpch` gate**; **no engine surface changed**; no `.perf.data`.

## Guardrails

- **Instrument-only** - no `crates/toron` engine change; the comparator, LIMIT handling, and
  non-vacuity guard are unchanged.
- **Structural parity or it does not ship** - the frugal oracle is validated against the dense
  reference at every scale small enough to run both; "frugal" never silently becomes "wrong".
- **Assert, do not assume** - generator emission order is a checked invariant on both merge
  inputs.
- **4 GiB is the bar** - a 6 GiB pass earns no board credit; only the 4 GiB canonical
  comparison retires the gap.
- **Won ground is a control** - the twelve established controls and q17's gap are untouched.

## Risks

- **A generator whose orders or lineitems are not monotonic in order key** -> the assertion
  fails loudly at once; the fix is a bounded sort or a documented ordering contract, not a
  silent wrong answer. (The 423 census already relied on and accepted this property for
  lineitems; 425 extends the assertion to the orders stream and makes both explicit.)
- **A frugal-rewrite bug** -> the dense reference parity locks catch it at sf=0.01/sf=1 before
  it can reach the unvalidated sf=10 path.
- **Referential-integrity assumptions** -> the merge applies the customer-known filter
  explicitly via the membership set, exactly as the dense oracle does; it never assumes every
  order has a live customer.
- **Two customer passes cost CPU** -> deliberate memory-for-CPU trade; the passes are streaming
  and cheap relative to the sf=10 comparison, and the memory win is the entire point.

## Forward Arc

q18 clean-green at 18/22 closes the ordering/quota thread and its instrument follow-on together.
Remaining: q21 (throughput - profiling-first, separating fold time from the probe stages), q02
(join-output C-spec driver), q05 (diagnosis-first OOM), and q17 (the board's other named
oracle-instrument thread - `q17_sf10_oracle_policy_gap`; the frugal stream-merge pattern here is
a candidate template if q17's gap turns out to be oracle-residency-shaped rather than policy).

## Grounding evidence (file:line)

Dense oracle + the three co-resident maps: `toron-tpch/src/lib.rs:11065-11125`
(`orders`/`quantity_by_order`/`customers`), threshold/qualifying/selected counters at
`:11078-:11110`, comparator + non-vacuity guard `:11188-11202`, `strict_q18_oracle` generator
wiring `:11127-11144`. The tpchgen 2.0.2 dependency exposes streaming `.iter()`, derives both
streams' keys from `OrderGenerator::make_order_key`, and finishes all lineitems for one order
before advancing `LineItemGeneratorIterator`'s order index. 424 evidence:
census 624 qualifying keys at sf=10 / 57 at sf=1, zero dup/null; canonical 4 GiB OOM at
4,365,652 KiB combined; `q18_sf10_oracle_residency_gap` recorded
(`plans/impl/0424_q18_membership_first_composition_results.md`). Precedent for a toron-tpch-only
oracle ADR: ADR 422.
