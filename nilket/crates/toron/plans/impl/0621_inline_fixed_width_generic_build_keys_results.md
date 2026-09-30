# ADR 621 results - inline fixed-width keys for generic in-memory join builds

**Verdict: RETAINED.** `GENERIC_BUILD_KEY_STORAGE = Inline`. On q09, the
deciding cell, the median paired gain is **1,053.4 ms (15.6% of the
control median)** against ADR 588's bar of 118.8 ms, and every pair is
positive. Digests match on every row, neither arm spills, and peak RSS
falls. The q01 no-change control is reported, not interpreted. q05 and
q20 also change representation but have **no paired latency coverage**.

## Identity

- Arms (branch `adr621-arms`, tags `artifact/adr621-{control,treatment}`):
  - control `6873916c` (tree `4f3e5612…`), with `GENERIC_BUILD_KEY_STORAGE = Owned`;
  - treatment `f0b63449` (tree `8d9c8cb5…`). Its diff is that token line alone.
- Executables:
  - control `ff953a842fa85afa251438ff7af402b8e32bd1e7b02bad19429e1efea9ca3225`;
  - treatment `18df5747a7dc247cc0caeb5db2e84cd05440d2375f1254697f29c35246cd7ee5`.
- Manifests: control `8a06d17a…`, treatment `2b5cb8ac…`. Both executables were
  hash-verified on strix after staging.
- Anchor: ADR 613 board `2f029448…`; sealed manifest `576dab40…`.
- Coordinate: SF8 strict parquet; task memory and spill reserve 2 GiB;
  partition parallelism 1; affinity 0-15.

## Ladder (bigfoot)

Both trees: fmt clean. toron: 46 suites, 5,533 passed, 0 failed. tpch:
674 passed, 0 failed. Release builds: 0 warnings in both trees.

## Launches (strix)

Each step was a separated launch: a systemd unit with a 120 s delay and
an attempt token, one attempt, exit status 0. All scripts were staged,
hash-matched and fixture-tested on strix before the first launch.
There was no SSH to strix between a launch and its completion signal.

| step | scheduled (unix ms) | completion (unix ms) |
|---|---|---|
| gate | 1790795189036 | 1790795543537 |
| q09 pair | 1790795628864 | 1790795863000 |
| q01 pair | 1790795879195 | 1790796078689 |
| mechanism | 1790796094916 | 1790796306116 |

## Gate and census

`adr621_gate_read.py`: **GATE: PASS.**
- The log has all 44 blocks in declared order, each rc 0, each with 0 leftover
  children and 0 scratch entries.
- Every row passed: pass status, sealed digest and plan fingerprint,
  `full_hash`, coordinate, and the arm's executable.
- Spill statuses agree across the arms in every cell.

| cell | record | replay | rows | status |
|---|---|---|---|---|
| q09 | 1 | 2 | 6,400,000 unique | **validated**: `generic_hash` → `generic_inline` |
| q05 | 1 | 2 | 80,000 unique | changed, **unverified** (no reviewed association) |
| q20 | 3 | 3 | 68,812 unique | changed, **unverified** (no reviewed association) |

- **Coverage unknown** (no join-build profile file): q01, q04, q06, q14, q19, q21.
  These are named as unknown, not unchanged. q21 spills in both arms.
- Every other cell's records match across the arms, with no representation change.
- No validated breadth cell exists.

## q09 pair (decides)

`adr621_pair_read.py q09 decides`: **PAIR: VALID.** The 12-row contract
held: two warmups, then five alternating pairs, with exact pair and
position tags. The driver's summary figures, variability record and
verdict agree with values recomputed from the rows.

| pair | control ms | treatment ms | gain ms |
|---|---|---|---|
| 0 | 6,760.4 | 5,742.0 | 1,018.4 |
| 1 | 6,719.7 | 5,741.2 | 978.5 |
| 2 | 6,762.7 | 5,709.3 | 1,053.4 |
| 3 | 6,739.1 | 5,685.7 | 1,053.4 |
| 4 | 6,779.1 | 5,680.7 | 1,098.4 |

- Control median 6,760.4 ms; treatment median 5,709.3 ms.
- Median gain **1,053.4 ms** (0.156 of the control median); range 978.5–1,098.4 ms.
- Control range 59.4 ms, so the required gain is 118.8 ms. Noise: range/median
  0.0088, within ADR 593's 0.04.
- ADR 588: **retained**. Every pair is positive, and the median exceeds twice the
  control range.
- One digest across all rows. No spill observed; spill bytes 0.
- Peak RSS (KiB, label `above_parent_high_water`, parent high-water 17,444):
  control 2,238,184–2,239,260; treatment 2,025,796–2,041,072.

## q01 pair (no-change control)

q01 was chosen for its independently known join-free plan. The missing
build profile was not the reason. It is a no-change control, not
changed-path breadth. **PAIR: VALID.**

| pair | control ms | treatment ms | gain ms |
|---|---|---|---|
| 0 | 3,349.4 | 3,318.1 | 31.3 |
| 1 | 3,275.3 | 3,315.2 | −39.9 |
| 2 | 3,288.6 | 3,404.0 | −115.3 |
| 3 | 3,282.5 | 3,310.3 | −27.8 |
| 4 | 3,276.6 | 3,321.9 | −45.4 |

- Median gain −39.9 ms; bar 148.1 ms. Noise: range/median 0.0226, within bound.
- Signs are mixed, so ADR 607's regression rule does not fire.
- No spill observed. Peak RSS (KiB, label `above_parent_high_water`, parent
  high-water 17,324): control 31,584–33,644; treatment 32,912–33,872.
- This is reported only; no interpretation is drawn from it.

## Mechanism (diagnostic, not a paired decomposition)

Five alternating armed q09 runs per arm (`adr621_mechanism_read.py`):
**none withheld.** Every run passed the row checks, the node tree reconciled,
and the route was `reoriented_inner_chain_grouped` with ADR 618's topology.
Top-level and refinement reconciliation passed. Strict status is `false` in
every run: the known missing replay-2 suffix sub-stages, as in ADR 620.
Replay 2 had one generic build record, 6.4 M / 6.4 M / 0. Its representation
was `generic_hash` in every control run and `generic_inline` in every
treatment run. Peak RSS is unavailable in these armed rows.

Median [min, max] ms per arm:

| stage | control | treatment |
|---|---|---|
| execute | 6,967.4 [6,945.0, 7,005.9] | 5,904.4 [5,863.1, 5,956.1] |
| `prep_build` | 928.8 [924.4, 940.0] | 738.0 [729.8, 769.5] |
| `prep_table_build` | 783.3 [779.3, 792.8] | 586.2 [584.8, 614.4] |
| `suffix_probe` | 1,231.3 [1,228.7, 1,239.1] | 932.6 [926.8, 944.2] |
| `payload_take` | 111.9 [111.3, 112.8] | 113.2 [112.7, 115.9] |
| `cleanup_build` | 562.8 [558.3, 576.4] | 50.7 [49.1, 51.7] |

Reading, against ADR 620's benchmark differences:
- The table build fell by 197.1 ms (medians); ADR 620's C1 was 198.3.
- Cleanup fell by 512.1 ms; C2 was 461.0.
- The probe fell by 298.7 ms. ADR 620 reported, as context, an owned − inline
  prototype lookup-layout difference of 287.8 ms. The observed difference is
  consistent with that observation. However, neither figure was a production
  prediction, and neither isolates pointer chasing or key comparison as the
  cause. In both arms the probe keeps its fresh per-probe encoding and key
  allocation. The cause of the probe change remains a hypothesis.
- These are differences between arm medians of armed runs. Differences of
  component medians need not sum to the median total. They are diagnostic,
  not a decomposition of the paired gain, and ADR 620's figures were not a
  prediction to be summed.

## Provenance

All sources are on strix under `~/work/ket/nilket/target/tpch/`. The
figures above were read by the readers below, and the hashes are SHA-256
values of the saved files.

Readers (`adr621-scripts/`):

| file | sha256 |
|---|---|
| `adr621_validate.py` | `298a65cf9c20e45d28cd83992628df2977cfbd3091b3c79ef477f366447ba428` |
| `adr621_gate_read.py` | `4a69cbbb551f7ed39c8c47b1d98d1e5136c097c7f7564d831db7757c5981474b` |
| `adr621_pair_read.py` | `ecb8788f73011e86b4a166cb3bf29701bd98a738b9518b762f1f03e6a975fe8f` |
| `adr621_mechanism_read.py` | `790d784bb9454df4cde0324ebc5617e177b26ad9c77b0732817891e54bfe17f9` |
| `adr620_envelopes_read.py` | `6f3987308e173956897b00aa5c7dd7239259fd6162b853b84829b2aad6770711` |
| `../adr620-scripts/adr619_validate.py` (imported) | `a7b7832b81215697003fd9184ad3bdc0eff10d0e26b99d6510b360ed9d1c3d7c` |

Sources:

| artifact | sha256 |
|---|---|
| gate: `adr621-gate/gate.log` (rows and profiles in `adr621-gate/{control,treatment}/`) | `071cb11aa9ddf3b4b66560a96b010fb0a123f0b15d647973befeabe02a280d51` |
| gate census: `adr621-gate/census.json` | `10a37da9802782d7292572ce59ac037ff74a007824aecfa0642f5206782a11de` |
| `adr621-q09-pair/samples.jsonl` | `1e4f5db274b268ebcde72776ab9f42ac06f945958d11125c592426fd91f63cd8` |
| `adr621-q09-pair/summary.json` | `6931e82d9759240a4ef27e508afe10704ac8afe6c139906289923ce735f2b393` |
| `adr621-q01-pair/samples.jsonl` | `a98e53341e8b8804c6c0667a1bd24d234ba199e664eb3dffbcf19830f0969671` |
| `adr621-q01-pair/summary.json` | `0a89609a622b4e06f7b758965eac36d9528535a47d0d88a3c96713f436ea458a` |
| mechanism: `adr621-mechanism/` (per run: `{arm}_{n}_row.json`, `_node.json`, `_join_build.jsonl`, `_stderr.log`); log `adr621-strix-host-record/mechanism/driver_stdout.log` | log `b63ff2ea836f4cd9ff7680f5e1af7532ccd52bfeb9c968f9e2eadacd22f92584` |

Launch host records are in `adr621-strix-host-record/{gate,pair-q09,pair-q01,mechanism}/`
(`host_config_before.txt`, driver logs, `completion.marker`). Attempt tokens:

| step | unit | attempt token |
|---|---|---|
| gate | `adr621-gate` | `24e142e1fe906776175b6f6a9aebc2d0` |
| q09 pair | `adr621-q09-pair` | `749f75c101075ca19ec04858002e22f7` |
| q01 pair | `adr621-q01-pair` | `db639daa282bf13f01f3144cab240eba` |
| mechanism | `adr621-mechanism` | `e9765b43c65c37a5ab429fd06117b7a5` |

The gate's `driver_stdout.log` has sha256
`3e8b7e98bdd8022b21700e341d53d35a06efbb58bc64177e80c01410e142d587`.

## Scope of the claim

- Retention follows q09's rule and the declared spill and refusal guard.
- There is no whole-board inference.
- q05 and q20 change representation without paired latency coverage.
- Six cells have unknown coverage.
