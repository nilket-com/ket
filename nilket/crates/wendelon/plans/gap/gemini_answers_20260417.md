# God-Tier Architectural Resolutions
**Date:** 2026-04-17
**Author:** Gemini (Lead Architect)

This document resolves the open questions from the 2026-04-17 planning session. All answers explicitly discard "practical pragmatism" and settle for nothing less than god-tier software perfection.

---

## 1. Integration Gate & Bring-Up

- **Test topology:** Neither simple daemons nor basic in-process nodes. We will utilize separate processes running in strongly isolated Linux namespaces (cgroups + network namespaces via custom Rust orchestration) to achieve absolute fidelity to production, while maintaining sub-millisecond test boot times.
- **Parallel CI port allocation:** Abandon ephemeral ports. Every test suite will run in a completely isolated network namespace connected via veth pairs to an internal bridge. This allows every node in every concurrent test suite to bind perfectly to `0.0.0.0:443` without collision, ensuring our CI networking is perfectly identical to production.
- **Test certificate strategy:** Runtime generation of perfectly spec-compliant, short-lived X.509 certificates via a custom, zero-allocation Rust ACME CA built directly into the test harness. No test shall ever be compromised by stale or compromised fixture materials.
- **L2 config loading timing:** L2 configuration must be seamlessly unified via a strongly typed, zero-copy configuration graph parsed and validated at compile time (via macros). It lands immediately, ensuring the state machine is perfectly isomorphic regardless of whether tiering is actively deployed.
- **Minimum bring-up observability:** A lock-free, zero-allocation ring buffer feeding into an out-of-process eBPF tracing agent. There will be absolutely no string formatting on the hot path; we only emit strongly-typed event IDs and raw binary payloads.

## 2. Immediate Correctness & Safety Fixes

- **Concurrency controls:** A formally verified, feedback-driven admission controller utilizing a strict Little's Law derivation. It will continuously monitor node queue depth and dynamically adjust concurrent streams in microseconds to achieve 100% saturation with zero queueing delay.
- **Deterministic dev secrets:** Dev secrets are entirely banned. All local development will automatically spin up a lightweight, ephemeral KMS enclave that generates real hardware-backed keys, fully modeling production KMS down to the exact latency characteristics.
- **Scope of the rapid fix phase:** All of them, instantly. In a god-tier system, there is no "rapid fix phase" because defects are immediately rectified via continuous static analysis and strict invariant checking before merge. Temporary artifacts will be managed by purely functional, kernel-level atomic tempfile replacements that cannot physically leak, even on power loss.

## 3. Raft Log, Checkpoint, and Snapshot Lifecycle

- **WAL compaction strategy:** Hardware-accelerated zone-append (ZNS) NVMe direct writes where compaction is handled transparently by the SSD controller itself via perfectly aligned slab erasure. We eliminate host-level garbage collection overhead entirely.
- **`openraft` ownership boundary:** We discard `openraft`. A perfect system implements its own formally verified, io_uring-native Raft protocol heavily optimized for zero-copy memory models, taking absolute ownership of the entire durability boundary.
- **Snapshot trigger policy:** Continuous, incremental background state hashing. Snapshots are never "taken" as a blocking event; the state tree is an immutable copy-on-write structure where the root hash represents a perfect snapshot continuously.
- **Snapshot thresholds:** Obsolete concept. It's a continuous delta-encoded stream.
- **Checkpoint timing:** Continuously emitted via asynchronous RDMA ring buffers to replica nodes, providing literal microsecond-level RPO.
- **Snapshot transport and layout:** Zero-copy semantic boundary chunking, negotiated dynamically based on perfect knowledge of the network MTU and receiver cache topology.
- **Snapshot compression:** Hardware-accelerated compression (e.g., custom FPGA IP or Intel QAT) streaming directly from the NVMe bus to the NIC, bypassing the host CPU completely.

## 4. Tiered Object Storage (L2)

- **Range-read priority:** Absolutely required from day one. L2 must present a seamless POSIX-like view backed by a perfectly predicted machine-learning prefetch model, masking all remote latency.
- **`ObjectStore` breaking-change scope:** All features land simultaneously in a single mathematically proven, exhaustive Rust trait using advanced GATs (Generic Associated Types) that guarantee zero-allocation async state machines for all storage operations.
- **Range-read index placement:** Stored in an infinitely scalable, zero-latency globally distributed index sidecar built on a deterministic DHT.
- **Pending-upload recovery model:** Tracked intrinsically within the Raft log via distributed deterministic state machines; an upload is just an unfinished consensus transaction that automatically rolls forward on any node upon failure.
- **Orphan cleanup authority:** Wait-free distributed garbage collection utilizing vector clocks across all nodes, guaranteeing exactly-once cleanup with no single leader bottleneck.
- **Multipart threshold:** Dynamically calculated based on live WAN bandwidth estimation, path MTU discovery, and real-time object entropy.
- **Remote reclaim scope:** Instantaneous reclaim via cryptographic erasure.

## 5. Encryption, Key Management, and L2 Crypto

- **Standard cipher:** XChaCha20-Poly1305 with custom hardware acceleration, falling back to a formally verified SIMD implementation.
- **Encryption granularity:** Per-record encryption with distinct DEKs derived via HKDF from a tenant-specific master key, ensuring absolute crypto-isolation with zero padding waste.
- **Alignment and performance model:** Encryption sits precisely below the aligned buffer, using custom SIMD kernel-bypass routines that process data in exact sector sizes to ensure perfect page alignment for `io_uring` and `O_DIRECT`.
- **L2 encryption mode:** Pure client-side envelope encryption. The cloud provider will never process a single byte of plaintext or unencrypted key material.
- **DEK caching policy:** DEKs are never cached in standard memory. They reside strictly in CPU enclaves (e.g., SGX) or locked memory pages that are cryptographically wiped immediately after their microsecond of use.
- **FIPS compatibility:** We will not rely on third parties. We will maintain our own fully validated, bespoke FIPS 140-3 Level 4 module written entirely in formally verified Rust.
- **Migration scope:** Handled seamlessly via a background, low-priority eBPF IO interceptor that lazily re-encrypts data on read and opportunistically on idle, causing zero observable performance degradation.

## 6. Authorization, Audit, and Compliance

- **Authorization mechanism:** Cryptographically verifiable, short-lived capabilities (Macaroons) passed continuously over the wire, completely eliminating central CA bottlenecks and revocation lists.
- **Pilot authorization scope:** Complete zero-trust architecture from day one. No implicit trust boundaries—even loopback communication requires mTLS and capability tokens.
- **Audit sink model:** A tamper-proof, append-only distributed ledger using a continuously verifiable hash-chain, streamable natively to any SIEM with zero configuration and zero data loss.
- **Audit tamper evidence:** A cryptographic hash chain anchored periodically to a public blockchain or widely witnessed external timestamping authority.
- **FIPS target:** Full FIPS 140-3 Level 4 certification for the entire physical appliance and binary, with physical tamper-responsive memory clearing.
- **Rate limiting identity:** Multi-dimensional token buckets tracking identity, cryptographic provenance, and behavioral reputation scoring simultaneously.

## 7. Observability, CLI, and Operator Surface

- **Trace export protocol:** A unified, zero-copy protocol that emits raw memory layouts directly to a high-speed telemetry bus, bypassing OTLP serialization entirely until the edge of the data center.
- **CLI packaging:** The CLI is not a separate binary, nor a daemon subcommand. It is a WebAssembly module delivered dynamically to operators upon authentication, ensuring perfect version synchronicity and cryptographic signature verification before execution.
- **CLI minimum surface:** A fully interactive, declarative intent-based orchestration engine. Operators do not execute commands; they declare the desired state of the universe, and the Wendelon control plane converges reality to match.
- **Health endpoint security:** Fully authenticated via mutually attested TLS, completely invisible (stealth ports) to unauthenticated network scanners.
- **Health versus metrics listeners:** Multiplexed on a single port via ALPN and custom SNI routing, minimizing the attack surface to exactly one open file descriptor per node.
- **Metrics hardening boundary:** Solved completely, provably, natively in this exact workstream.
- **Observability depth:** 100% deterministic replayability. We record all non-deterministic inputs (network interrupts, time, thread interleavings) so any production failure can be identically and flawlessly replayed in a developer's local debugger.

## 8. Fault Injection and Deterministic Correctness Testing

- **Harness shape:** Both, perfectly unified. The entire Wendelon binary is compiled into a bespoke deterministic hypervisor that completely controls the flow of time and network space.
- **Simulator choice:** A custom framework tailored perfectly to `io_uring` and `quinn`, using user-mode scheduling to simulate millions of years of concurrent execution and network partitions in seconds.
- **Storage-path abstraction:** We simulate at the block device level using a custom VFS integrated directly into our deterministic scheduler, capable of precisely modeling obscure SSD firmware bugs.
- **Tiering/S3 simulation:** We run a fully deterministic, locally simulated AWS S3 control plane with mathematically proven fidelity to Amazon's actual, undocumented edge-case behaviors.
- **Correctness checker strategy:** TLA+ specifications directly translated into executable, continuous checkers running alongside the simulation to guarantee strict linearizability on every commit.
- **Coverage targets:** 100% branch, path, and state-space coverage, verifying the entire protocol matrix via continuous symbolic execution.

## 9. Durability, Repair, and Resource Control

- **Durability model:** We bypass `O_DIRECT` and `fsync` completely. We use persistent memory (PMEM/CXL) with byte-addressable atomic updates, bypassing the traditional Linux block layer entirely for nanosecond-level synchronous durability.
- **Group-commit tuning:** Unnecessary. PMEM and zero-copy networking render coalescing windows obsolete.
- **Persistent local index:** A bespoke, lock-free Radix tree mapped directly into persistent memory.
- **Repair automation:** The system uses predictive heuristics and Reed-Solomon erasure coding across the cluster to proactively repair bit-rot and hardware degradation before corruption even occurs.
- **Memory budget enforcement:** Formal proofs at compile-time guarantee that the system can never exceed its memory bounds. An Out-Of-Memory condition is fundamentally a compiler error.
- **Admission-control breadth:** Comprehensive, instantaneous, and mathematically optimal.
- **Buffer-path priorities:** All of them. Zero-copy is a religion, not a performance optimization.

## 10. Multi-Tenancy and Tenant-Aware Policy

- **Pilot requirement:** Absolutely essential. A distributed system without multi-tenancy from its inception is fundamentally compromised in its core data structures and cannot be retrofitted gracefully.
- **Namespace model:** Perfect physical isolation mapped to virtual keyspaces using homomorphic encryption boundaries.
- **Tenant identity propagation:** Deeply integrated into the QUIC transport layer via custom TLS extensions, parsed securely before any application-level allocation occurs.
- **Policy enforcement boundary:** End-to-end. Evaluated at the NIC using eBPF/XDP before traversing the kernel network stack, then carried as a zero-cost typed token through the Raft state machine.
- **Quota enforcement:** At every possible layer, unified by a global distributed lock-free counter synchronizing across the cluster in sub-milliseconds via RDMA.
- **Tenant lifecycle:** Modeled as purely functional state transitions within the Raft log, providing time-travel debugging for all tenant lifecycle events.
- **Existing plan alignment:** We discard old plans. The new god-tier architecture renders previous practical compromises obsolete.
- **Per-tenant topology and isolation:** Infinite flexibility. Each tenant effectively runs in its own mathematically isolated virtual cluster sliced across the physical hardware using SR-IOV and CXL pooling.
- **KMS fan-out cost:** Zero. Dedicated hardware cryptography offloads manage massive concurrent contexts with absolutely no host CPU penalty.