# ADR 432 - q02 join-output wall: results

Pairs with `plans/adr/432_q02_join_output_wall.md` and its implementation plan. ADR 432 removes
q02's 1.738 GB join-output materialization under the unchanged envelope and closes the verified
sf=10 strict board from **21/22 to 22/22**.

## Outcome

Phase 0 selected branch (c), not the terminal-recombine hypothesis. The failing owner was the
earliest outer-chain join: replay 0, an `Inner` `HashPartitioned` supplier-to-partsupp join. It
selected all 8,000,000 partsupp rows and attempted to materialize the supplier payload once per
match before the part predicate, nation/region joins, correlated `MIN` recombine, or final Sort.

The shipped path recognizes the confirmed terminal consumer and drives that entire preserved-side
reoriented inner chain from replay 0 in chunks. Each chunk continues through the later inner joins,
the correlated `LeftOuter`, the `ps_supplycost = MIN(...)` filter, and the final projection. Only
projected survivors are retained for the existing Sort. The final release run returns all 4,667
rows in 7.37 seconds and passes the independent persisted correctness comparison. The board is
**22/22**.

## Phase 0 - capture and attribution

The promoted plan commit was `cb79e67d66e08d1c7714a160e2581731ad0ea0cc`. The failing persisted
sf=10 bound plan retained fingerprint `cf586e71755bbca3` and this relevant topology:

```text
Sort
  <- Exchange(parts=1)
  <- Project
  <- Filter(ps_supplycost = correlated MIN value)
  <- LeftOuter replay 7
     <- Exchange(parts=4)
     <- Inner replay 3 <- Inner replay 2 <- Filter(part) <- Inner replay 1
        <- part
        <- Exchange(parts=4) <- Inner replay 0(supplier, partsupp)
```

The temporary env-gated attribution hook recorded the failing join before any wall-mover code:

| field | receipt |
| --- | ---: |
| replay / kind / strategy | `0` / `Inner` / `HashPartitioned` |
| left input | 100,000 rows, 7 columns, 16,839,275 resident bytes (supplier) |
| right input | 8,000,000 rows, 3 columns, 256,000,288 resident bytes (partsupp) |
| selected rows | 8,000,000 |
| left take index | 33,000,000 bytes |
| right take index | 33,000,000 bytes |
| repeated left payload | 1,392,000,000 bytes |
| right payload | 280,000,000 bytes |
| total / cap | **1,738,000,000 / 1,073,741,824 bytes** |

The captured plan supplies the parent path omitted from the generic payload: replay 0 feeds a
four-way `Exchange`, then replay 1 joins part and applies the size/type filter. Because the owner is
an earlier join, the conditional post-`MIN` survivor probe was correctly skipped; its selectivity
could not choose the mechanism. Both temporary diagnostic hooks were removed before the final
build. No q02-specific diagnostic, environment variable, or generic join-error expansion ships.

## Implementation

The cut reuses the landed reoriented-chain and correlated-scalar primitives:

- `exec.rs` delegates Sort binding to `bound_sort`; the delegation is boxed so the recursive
  executor keeps its prior stack footprint. The file is 7,499 lines, below its 7,500-line ratchet.
- The terminal recognizer admits only a single-partition Sort input with exact
  `Exchange <- Project <- Filter(EvaluateInToron) <- LeftOuter(HashPartitioned)` structure and a
  preserved-side `Exchange` over a contiguous replay-0-based reoriented inner chain.
- The existing replay-0 scan-probe driver now accepts a batch visitor. The grouped-aggregate path
  uses the same visitor adapter, while the new consumer prepares the later reoriented runtimes and
  streams the early join through them.
- The correlated scalar side is built once. Preserved chunks are repartitioned with the existing
  exchange key, recombined, filtered, projected, and appended only when rows survive.
- Survivor ownership uses checked byte addition and the named producer
  `streamed correlated sort input accumulator`. The existing Sort performs its own index-inclusive
  materialization guard, so a later survivor overflow remains the distinct, named
  `sort materialization` wall.
- The existing Sort establishes the final comparator and limit/offset semantics. The streaming
  primitive makes no cross-chunk order claim and preserves every equal-cost supplier.

There is no TPC-H query name in the engine, no cap increase, no new physical operator, and no
arbitrary output-spill claim. Off-shape sorts execute the prior materialized path.

## Discriminating tests

Three new unit locks cover the mechanism:

- the exact terminal correlated shape must be admitted directly and match a deliberately
  off-shape materialized twin;
- a two-batch partsupp fixture retains both suppliers tied at the minimum, retains the second
  part's supplier, and matches exact final order; every table is scanned once;
- both a non-singleton terminal exchange and a non-contiguous replay chain decline before any
  storage I/O;
- the survivor accumulator accepts its exact byte cap and fails one byte over with the exact named
  memory producer.

The existing q02 correctness suite continues to lock the eight-column schema, exact comparator,
duplicate/missing/extra rows, nulls, and dropped MIN-cost ties. Final release small-scale
correctness is `pass@compare` at sf=0.01 in 700.7 ms.

## Final live proof

Both final commands used the hook-free release binary, fresh `target/tpch/join_spill`, persisted
Parquet input, strict mode, `MemoryMax=4G`, `MemoryHigh=4G`, `MemorySwapMax=0`, and
`RuntimeMaxSec=600s`.

| rail | result | engine/compare wall | rows | spill bytes | max RSS fallback |
| --- | --- | ---: | ---: | ---: | ---: |
| `perf-run-one` | pass | 7,366.7 ms | 4,667 | 234,983,488 | 1,444,828 KiB |
| persisted correctness | `phase=compare status=pass` | 12,018.4 ms | oracle-owned | not emitted | 1,443,664 KiB |

The performance row keeps plan fingerprint `cf586e71755bbca3`, output schema fingerprint
`fnv1a64:929885357de5dde4`, and Arrow IPC digest
`d0cd101af3f2c86f852b15de3295f4b793acedd1067c1cd7386755568e483d03`. The persisted correctness
rail, not row count or digest alone, authorizes the board move. `/usr/bin/time` maximum RSS is
reported as the available peak fallback; no cgroup `MemoryPeak` claim is invented.

Normal completion left `target/tpch/join_spill` empty. Evidence remains separately under
`target/tpch/adr432`; no evidence path is tracked.

## Verification

Final gates:

- `scripts/cargo-cgroup fmt -- --check` - pass;
- `check -p toron --all-targets` and `check -p toron-tpch --all-targets` - pass;
- Toron library - 1,285 passed, 0 failed;
- Toron architecture - 2,079 passed, 0 failed;
- full Toron `--no-fail-fast` - every unit, integration, and doc-test binary green;
- TPC-H with `polars-sql-reference --no-fail-fast` - every unit, integration, and doc-test binary
  green, with only the pre-existing documented ignores;
- focused terminal-sort, tie/order, shape-fence, and budget tests - pass;
- hook-free release build and both fixed-cap sf=10 live rails - pass;
- `git diff --check` - clean; no tracked `.perf.data`.

## Verdict

ADR 432 removes the replay-0 1.738 GB output monolith and greens q02 under the unchanged
4 GiB / 600-second envelope. The sf=10 strict board is now **22/22**. The residency/throughput/oracle
board arc is closed; broader substrate comparison and article-grade performance work remain the
forward thread.
