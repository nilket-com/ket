# ADR 009: `io_uring` Threading Model

## Status
Accepted

## Context
In `ADR 008`, we committed to using `tokio` (multi-threaded) combined with the low-level `io-uring` crate. We need to decide how many `io_uring` instances to create and how they map to threads. Options included:
1. One global `io_uring` instance for the entire node.
2. One `io_uring` instance per Tokio worker thread.
3. One `io_uring` instance per physical NVMe block device.

## Decision
Wendelon will utilize **One `io_uring` instance and one dedicated blocking I/O thread per physical NVMe device**.

## Rationale
- **Device Saturation:** Modern NVMe drives have deep internal queues. A single `io_uring` instance with a large queue depth (e.g., 1024 or 4096) submitted by a single thread can fully saturate a PCIe 4.0/5.0 SSD.
- **Lock-Free Submission:** By isolating an `io_uring` instance to a single dedicated OS thread, we avoid the need for cross-thread synchronization (locks) when building Submission Queue Entries (SQEs). 
- **Tokio Decoupling:** `tokio` async tasks will push pointers (page-aligned buffer addresses and offsets) into a fast lock-free MPSC channel or ring buffer. The dedicated I/O thread drains this channel, builds SQEs, submits them, reaps Completion Queue Entries (CQEs), and fires `tokio::sync::oneshot` channels to wake the waiting async tasks.

## Consequences (The "Negatives" / Rejected Ideas)
- **Rejected Global Ring:** A single global ring for multiple NVMe drives could create a bottleneck in the single polling thread.
- **Rejected Per-Core Rings:** Running an `io_uring` on every tokio worker thread requires complex synchronization if multiple threads write to the same slab file, and risks polluting the tokio scheduler with I/O blocking if `io_uring_enter` blocks. We accept the overhead of the MPSC channel hop to strictly isolate network parsing (tokio) from disk submission (I/O thread).