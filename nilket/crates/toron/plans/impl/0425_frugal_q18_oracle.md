# ADR 425 - Implementation plan: frugal q18 oracle

Pairs with `plans/adr/0425_frugal_q18_oracle.md`. One toron-tpch-only instrument cut: replace the
sf=10 correctness path's dense q18 oracle (two co-resident 15M-entry HashMaps + 1.5M names) with
a membership-first monotonic stream-merge that keeps O(customers) key membership plus
O(qualifying) rows resident while eliminating both O(orders) maps, so the
canonical in-process comparison passes under the standard 4 GiB cgroup and retires
`q18_sf10_oracle_residency_gap` (board 17/22 -> 18/22). No `crates/toron` engine surface.

## Phase 0 - reproduce the gap and re-confirm the shape (before code)

1. **Reproduce the 4 GiB canonical OOM** on the current binary (424's documented behavior):
   ```bash
   scripts/cargo-cgroup build -p toron-tpch --release
   mkdir -p target/tpch/adr425
   unit="toron-adr425-q18-dense"
   systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=600s \
     /usr/bin/time -v target/release/toron-tpch correctness --query q18 --sf 10 --mode strict \
       --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr425/q18_dense.json 2> target/tpch/adr425/q18_dense.err
   { echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
       --property=ExecMainStatus --property=MemoryPeak; } > target/tpch/adr425/q18_dense.meta
   systemctl --user reset-failed "$unit" 2>/dev/null || true
   ```
   Expect `Result=oom-kill` at ~4 GiB before a terminal compare row.
2. **Re-confirm the census** (bounded generator pass): 624 qualifying keys at sf=10 / 57 at
   sf=1, zero duplicate/null keys; record the dense oracle's sorted rows and three counter
   fields at sf=0.01 and sf=1 as the parity targets.
3. **Assert-order probe:** confirm `OrderGenerator` and the `LineItemGenerator` group-by both
   emit monotonically non-decreasing order keys at sf=1/sf=10 (record; the assertion will make
   this a hard invariant, not a hope).
4. **Customer-set measurement:** build the proposed standard `HashSet<i64>` at sf=10 and record
   key payload, requested/observed capacity, and process peak. The 12 MB key payload is not a
   container-footprint claim. If the measured set does not leave material headroom for the
   canonical 4 GiB comparison, stop and compare a compact representation before coding.
5. Record everything before code.

## Phase 1 - the cut (toron-tpch only)

1. **Public `strict_q18_oracle(scale_factor) -> Result<Q18Oracle, String>` becomes frugal** and
   continues producing the same `Q18Oracle` type at every scale. Preserve this public name so
   the existing correctness rail and integration tests do not become test-only. Its internal
   row seam accepts two explicit customer iterators/factories (membership and names), plus the
   orders and lineitems iterators:
   - **customer membership pass:** stream the first `CustomerGenerator`, collect the measured
     standard `HashSet<i64>` of custkeys, and reject duplicate customer keys loudly; retain no
     names yet.
   - **lineitem group-by (asserted monotonic):** stream `LineItemGenerator`, increment
     `selected_lineitem_rows` per raw row, accumulate the current order's checked `i128`
     quantity sum, and yield `(orderkey, sum)` on key change with one-item lookahead; assert
     each newly finalized group key strictly exceeds the prior finalized group. Contiguous
     same-key lineitems are the normal group and must succeed. Flush the final group explicitly.
   - **merge with the orders stream (asserted monotonic):** stream `OrderGenerator`, assert
     monotonic `o_orderkey`; two-cursor merge against the grouped-sum stream. For a key present
     in both with custkey in the membership set: `sum > 30_000` -> push a `Q18Row` (custkey,
     orderdate, totalprice, sum) and `qualifying_orders += 1`; else `threshold_fail_orders +=
     1`. Keys in only one stream, or unknown custkey, are skipped (mirroring the dense oracle's
     three `continue`s). Use checked increments for all three counter fields.
   - **name pass:** retain qualifying order data in a pending-candidate type, collect its
     distinct custkeys (624 at sf=10; O(qualifying) generally), stream the second
     `CustomerGenerator`, and retain names for those keys only; error if a qualifying custkey
     has no name (it passed the membership set, so this is an invariant breach, not a skip).
     Construct `Q18Row` only after attaching the name; do not use dummy strings.
   - **sort** the retained rows by the existing key
     (`o_totalprice DESC, o_orderdate, o_orderkey`) and return the `Q18Oracle`.
2. **Rail wiring:** all strict q18 correctness calls keep using the public
   `strict_q18_oracle`; `compare_strict_q18`, the non-vacuity guard, and LIMIT handling are
   byte-unchanged (the comparator still owns `LIMIT 100`).
3. **Dense reference -> internal `#[cfg(test)]`:** move/rename the dense implementation to
   `q18_oracle_dense_from_rows` / `strict_q18_oracle_dense` inside a `lib.rs` test module. Put
   dense-vs-frugal generator parity there so the reference is genuinely compiled out of the
   shipped crate. External `tests/correctness_test.rs` continues importing and exercising the
   public frugal `strict_q18_oracle`.
4. No `crates/toron` change; no comparator, guard, LIMIT, or schema change.

## Tests (discriminating)

- **Dense-vs-frugal parity:** in an internal `lib.rs` test module, at sf=0.01 and sf=1, the
  public frugal `strict_q18_oracle` structurally equals `strict_q18_oracle_dense` - full sorted
  `rows` and the three counter fields. Derive/implement `PartialEq, Eq` for `Q18Oracle`; do not
  call this byte equality.
- **Counter-fidelity edge locks** (the same synthetic inputs run through frugal and dense test
  seams): an order with a known customer and lineitems summing `<= 30_000` (threshold_fail);
  an order with lineitems but unknown customer (skipped, neither counter); an order with a known
  customer but no lineitems (skipped); a lineitem group with no order (counted in
  `selected_lineitem_rows`, no row/counter); the exact `sum == 30_000` boundary excluded and
  `30_001` included.
- **Invariant/error locks:** duplicate customer keys reject; checked quantity/counter overflow
  is loud; a one-group input proves the final lineitem group is flushed.
- **Order-assertion locks:** shuffled or duplicate order rows fail loudly; a lineitem group key
  that reappears after a different finalized group fails loudly; contiguous same-key
  lineitems succeed. Error text names the stream and previous/current keys.
- **Non-vacuity lock:** the guard still fires on a vacuous synthetic oracle.
- **Fidelity of the retained set:** all qualifying rows retained pre-LIMIT; the comparator
  applies `LIMIT 100`.
- Carry the `toron-tpch` correctness/harness suites green.

## Phase 2 - re-measure

```bash
scripts/cargo-cgroup build -p toron-tpch --release
mkdir -p target/tpch/adr425
# the board target: canonical in-process strict q18 sf=10 under the STANDARD 4 GiB cgroup
unit="toron-adr425-q18-frugal"
systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/time -v target/release/toron-tpch correctness --query q18 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr425/q18_frugal.json 2> target/tpch/adr425/q18_frugal.err
{ echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
    --property=ExecMainStatus --property=MemoryPeak; } > target/tpch/adr425/q18_frugal.meta
systemctl --user reset-failed "$unit" 2>/dev/null || true
rm -rf /tmp/toron_join_spill
# small-scale frugal correctness (parity path also exercised by unit tests)
for sf in 0.01 1; do
  target/release/toron-tpch correctness --query q18 --sf "$sf" --mode strict \
    --data-source memory >> target/tpch/adr425/q18_small.out 2>&1
done
```
- q18 sf=10: `phase=compare status=pass` under 4 GiB, `MemoryPeak` well below 4 GiB (record it);
  the gap is retired -> board 18/22.
- Controls: the twelve established controls are unchanged by a toron-tpch-only q18-oracle cut;
  a light re-run of a couple is optional confirmation only (no engine surface touched).

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
scripts/cargo-cgroup test -p toron --test architecture_test        # source-scan guards
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)

`crates/toron/plans/impl/0425_frugal_q18_oracle_results.md`:
- Phase 0: the reproduced 4 GiB dense OOM, the census/counter parity targets, the assert-order
  probe - before code;
- the cut: the frugal stream-merge (membership pass, asserted group-by, merge, name pass),
  the `#[cfg(test)]` dense reference, unchanged comparator/guard/LIMIT;
- q18 sf=10 canonical `pass@compare` under 4 GiB with the recorded `MemoryPeak`; the retired
  gap; board 18/22;
- locks: parity, counter-fidelity edges, order-assertion, non-vacuity - why each discriminates;
- confirmation: no engine surface, twelve controls and q17 untouched, full `toron-tpch` gate,
  no `.perf.data`; scope honesty (q21/q02/q05/q17 remain named).

## Risk / blast radius

- **Frugal-rewrite bug** -> dense parity locks catch it at sf=0.01/sf=1 before the unvalidated
  sf=10 path.
- **Non-monotonic generator emission** -> loud assertion, never a silent miscount.
- **Public/test ownership drift** -> `--all-targets` proves external tests still resolve the
  public frugal `strict_q18_oracle`, while the dense symbols exist only in the internal test
  module.
- **Counter drift under the merge** -> the counter-fidelity edge matrix pins each dense
  `continue` case.

## Seal

`plan: adr 425 frugal q18 oracle` (the dual). Implementation:
`impl: adr 425 frugal q18 oracle`. Do not commit `.perf.data`.
