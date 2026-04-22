# Brainstorm 008: Absolute Zero-Copy (PCIe P2PDMA & io_uring SPLICE)

## The Premise
We claim Wendelon uses "zero-copy transport" because we use Arrow IPC. In standard database terminology, "zero-copy" usually just means avoiding user-space serialization (e.g., not converting JSON to objects and back). We memory-map the file, hand the pointer to the network stack, and call it a day.

However, physically, the data still travels a long, expensive path: 
**NVMe Drive -> PCIe Bus -> Main Memory (RAM) -> CPU Cache -> PCIe Bus -> Network Interface Card (NIC)**. 

This burns memory bandwidth and pollutes the CPU cache. If Toron (the compute tier) requests a 100GB historical dataset for a massive distributed join, piping that through the Wendelon user-space process will bottleneck on RAM speed, not NVMe or Network speed.

## The Wild Idea: True Hardware Zero-Copy
Because Wendelon's sealed L1 slabs and cached L2 slabs are immutable, byte-perfect Arrow IPC files sitting on an NVMe drive, we have a unique opportunity. If a Toron query doesn't require complex WPBC row-level filtering (i.e., it just needs the whole slab), **we shouldn't even read the file into Wendelon's memory.**

Modern Linux kernels support `SPLICE` operations via `io_uring`, and modern enterprise hardware supports **PCIe Peer-to-Peer DMA (P2PDMA)**. 

We can instruct the Linux kernel to stream the data directly from the NVMe controller to the RDMA/QUIC network controller, bypassing the CPU and RAM entirely.

## How it Works

1. **The Request:** Toron requests `Slab_XYZ`.
2. **The Fast-Path Gate:** Wendelon authenticates the request and checks the query AST. If no WPBC filtering is required (it's a full-slab scan), Wendelon routes the request to the Splice Fast-Path.
3. **The Setup:** Wendelon opens the file descriptor for the sealed slab on the NVMe drive, and identifies the socket file descriptor for the QUIC/RDMA stream connected to Toron.
4. **The Execution:** Wendelon issues an `io_uring` `IORING_OP_SPLICE` command, linking the NVMe file descriptor directly to the network socket file descriptor.
5. **The Magic:** The Linux kernel takes over. On standard hardware (or a $5 VPS), the kernel uses the page cache to stream the file to the TCP/UDP stack without ever copying it into Wendelon's user-space memory. On high-end hardware with P2PDMA, the kernel negotiates a direct DMA transfer across the PCIe bus. The bytes flow straight from the NVMe drive out the network port.

## The Implications

*   **Infinite Egress Scaling:** The Wendelon storage node's CPU utilization drops to virtually 0% during massive historical data exports. Egress throughput is limited strictly by the PCIe bus lanes and the NIC line rate (e.g., 100 Gbps or 400 Gbps).
*   **The "Dumb Pipe" Superpower:** When acting as a pure storage tier for Toron, Wendelon can saturate network links that would normally melt a traditional database process.
*   **Perfect Synergy with RDMA (Brainstorm 006):** If we are using `siw` (Software iWARP) on a cheap VPS, `splice()` feeds the kernel's RDMA emulation directly. If we are using true hardware RoCEv2, P2PDMA allows the NIC to pull the Arrow IPC bytes straight off the NVMe drive.

## Why it's dangerous (The Friction)

1. **Encryption in Transit:** `SPLICE` and P2PDMA work on raw bytes. If we are using mTLS over QUIC, the payload must be encrypted before it hits the wire. To maintain the zero-copy bypass, we would need to rely on **Kernel TLS (kTLS)** or NIC hardware TLS offload. If the Wendelon user-space process has to perform the AES-GCM encryption, the P2PDMA dream dies.
2. **Loss of Granularity:** We can't apply WPBC row-level filtering if we don't look at the bytes. This "absolute zero-copy" path is only valid for full-slab reads or exact byte-range fetches (which we can get from the Slab Footer Index).
3. **Hardware Finickiness:** PCIe P2PDMA requires specific motherboard topologies, IOMMU support, and compatible NVMe/NIC pairings.

## Verdict

This is the logical endgame of the "storage as an isolated, ultra-fast island" architecture. By treating sealed Arrow slabs as opaque blobs that can be hardware-spliced directly to the network, Wendelon transforms from a standard database into an intelligent PCIe traffic controller. 

It fulfills the ultimate promise: doing the absolute minimum amount of work necessary to blast data into Toron's compute engines.