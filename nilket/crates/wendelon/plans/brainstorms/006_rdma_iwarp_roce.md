# Brainstorm 006: Software RDMA (iWARP / Soft-RoCE) for Commodity Node-to-Node Transport

## The Premise

Wendelon's architecture relies on high-throughput, zero-copy transport to move massive Arrow IPC batches and handle bifurcated Stellarator writes. We currently plan to use QUIC for everything. 

QUIC is fantastic for WAN, NAT traversal, and client-to-cluster communication. However, for internal, east-west node-to-node replication, QUIC imposes a non-trivial CPU tax (encryption, packet pacing, user-space framing) just to move bytes between machines in the same datacenter.

True hardware RDMA (Remote Direct Memory Access) solves this by bypassing the CPU and OS kernel entirely, allowing NICs to DMA directly into application memory. It achieves microsecond latencies. The problem? True RDMA requires expensive hardware (e.g., Mellanox ConnectX NICs), which completely violates our mandate that Wendelon must be able to bootstrap and run on a $5/mo cheap VPS or a home lab.

## The Discovery: Software RDMA

The Linux kernel supports software-emulated RDMA devices over standard Ethernet interfaces. This bridges the gap between commodity hardware and enterprise performance APIs.

*   **`siw` (Software iWARP):** Implements the RDMA verbs API over standard TCP/IP. It works on literally any commodity hardware or cheap VPS that has a TCP stack.
*   **`rxe` (Soft-RoCE v2):** Implements the RDMA verbs API over UDP.

By leveraging these kernel modules (`ibverbs-utils`, `rdma-core`), we can write Wendelon's internal node-to-node transport against the RDMA API from Day 1. 

On a $5 VPS, Wendelon binds to `siw`. It doesn't get the hardware offload magic, but it establishes the exact same queue-pair, memory-registration, and zero-copy semantics. When the cluster eventually migrates to a $50k bare-metal server with true RoCEv2 NICs, the *exact same Wendelon binary* transparently upgrades to hardware-accelerated RDMA, dropping CPU utilization to near zero for internal replication.

## Architectural Implications

If we adopt this, Wendelon's transport topology splits perfectly based on trust and geography:

1.  **North-South (Toron/Clients → Wendelon):** QUIC. We need mTLS, stream multiplexing, and robust congestion control over unpredictable networks.
2.  **East-West (Wendelon ↔ Wendelon):** RDMA. We use `siw` (iWARP over TCP) for commodity deployments, and native RoCEv2/InfiniBand for high-end deployments. This is used for Raft WAL replication, Stellarator remote fan-out, and lazy migration data fetching.

This gives us the ultimate "kill Databricks" weapon: an engine that scales down to a $5 VPS but inherently possesses the exact memory-fabric architecture required by the world's most demanding supercomputing environments.

## The Rough Experiment

Proof of concept on cheap home hardware:
```bash
sudo apt install rdma-core ibverbs-utils

# Unblock the ports
ufw allow 4791/udp # rdma: rxe
ufw allow 18515/tcp # rdma: rxe siw

# Load modules and bind
sudo modprobe siw
sudo rdma link add siww type siw netdev eth0

# Test latency
ib_send_lat -d siww -R 192.168.0.6
```
*Note: `siw` (iWARP) establishes easily over TCP. `rxe` (RoCE v2 over UDP) is notoriously finicky on home routers/switches due to strict lossless networking requirements (PFC/ECN), reinforcing that `siw` is the correct fallback for the commodity tier.*

## Open Challenges & Friction Points

1.  **Security / Encryption in Transit:** 
    *   Hardware RDMA and `siw` historically send plaintext memory over the wire. 
    *   If Wendelon runs on a shared public VPS network, node-to-node traffic *must* be encrypted. 
    *   To use `siw` securely on a VPS, we would likely need to run it over a WireGuard or IPsec tunnel (which re-introduces the CPU tax we were trying to avoid). 
    *   *Question:* Do we only use RDMA in trusted VPCs/bare-metal, and fall back to QUIC for node-to-node if the user requires application-layer encryption?
2.  **Rust Ecosystem Maturity:** 
    *   The `ibverbs` bindings in Rust (e.g., `rust-ibverbs`) exist but require careful `unsafe` blocks for memory registration (MR) and queue pair (QP) management. It is a steep learning curve compared to async Rust over `tokio`/`quinn`.
3.  **Deployment Friction:** 
    *   A QUIC server "just binds to a port."
    *   An RDMA server requires kernel modules (`modprobe siw`), CAP_NET_ADMIN privileges, and explicit `rdma link` configuration on the host before the database process even starts. This breaks the "just run the single binary" UX.
    *   *Question:* Should Wendelon attempt to auto-configure `siw` via `netlink` calls on startup, or require the operator (or our future K8s operator) to pre-provision the RDMA interfaces?

## Verdict

Software iWARP is an incredible discovery for Wendelon. It allows us to architect the data ring's nervous system around true zero-copy, direct-memory access paradigms without abandoning our commodity hardware roots. It is a massive competitive moat against systems that are inextricably bound to heavy user-space RPC frameworks for internal replication.