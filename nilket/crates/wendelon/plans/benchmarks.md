# Benchmarking Plan — reproducible, fair comparisons

## Purpose
Produce reproducible, headline-grabbing but defensible benchmarks for HashRaft's metadata (Raft) and data-plane (consistent-hash) paths. Benchmarks must be repeatable, fully documented, and automated.

## Workloads
- **Metadata microbench:** leader append latency under varying concurrency (1,8,32 clients); measure commit latency, throughput.
- **Data-plane micro:** small writes (1 KiB), medium (64 KiB), large (1 MiB+); read-dominant and write-dominant mixes; random vs Zipfian key distributions.
- **Bulk/batched writes:** Arrow N-row batches (N up to 1024).
- **Rebalance stress:** epoch commit + streaming transfers while serving mixed client load.

## Metrics
- Throughput: ops/s and MB/s
- Latency: p50/p90/p99/p999 (client-observed) and Raft commit latency
- Resource: CPU%, memory, disk IOPS, NIC MB/s, context-switches
- System: e_safe lag, transfer success/retry counts, percent of writes that waited for finalization

## Methodology
- Baseline hardware: document exact machine types, kernel version, NIC, NVMe model. Example baseline in repo: bench/config/baseline.yaml.
- Warmup: 60s; Run: 300s; Trials: 5 independent runs; report median and 90% CI.
- Use fixed RNG seeds and fixed dataset generators. Provide Zipfian and uniform datasets.
- Collect system counters (perf, iostat, sar), tokio metrics, and quinn connection stats.

## Tooling
- Primary harness: Rust (tokio + quinn) bench client that emits Arrow IPC batches and measures per-op latency. Provide bench/config/*.yaml workload descriptors.
- Orchestration: bench/run.sh (local) and bench/ci-run.sh (CI), with optional Terraform/Ansible scripts for cloud provisioning.
- Analysis: tools to aggregate CSV outputs, plot p99 over time, and produce flamegraphs (perf / flamegraph.pl).

## Reproducibility & Publication
- Record git SHA, exact quinn and Rust toolchain versions, CPU governor, NUMA and sysctl settings, and kernel release in every artifact bundle.
- Publish raw CSVs, config files, and scripts alongside summarized results. Prefer repeatable container images or AMI/OVF images.

## Fairness
- Match replication factor, ring_bits, and runtime config when comparing algorithms or knobs. Disable caching layers or explicitly document their effects.
- When comparing hashing algorithms, keep ring topology and vnode counts identical.

## Ethics & Claims
- Publish full configs and error bars. Avoid cherry-picking single runs; disclose outliers and failed runs.
- Provide scripts for third-party verification and reproducible artifact bundles.