# Testing & Resilience

Given the mathematical complexity of HashRaft, standard unit tests are insufficient. Wendelon requires rigorous, adversarial testing.

## 1. Deterministic Simulation
- **MadSim / Turmoil:** The entire `wendelon` binary must be capable of compiling into a deterministic simulator. This allows us to run cluster-wide tests (Raft elections, ring rebalancing) on a single thread, injecting network drops and pausing time, ensuring every failure is 100% reproducible.

## 2. Chaos Engineering (Jepsen-style)
- **Network Partitions:** Simulating asymmetric network splits (minority leader isolated from majority).
- **Disk Failures:** Simulating `io_uring` returning `EIO` or `ENOSPC` midway through a slab write.
- **Clock Skew:** Injecting extreme NTP drift to validate the HLC causality rules and `E_safe` staleness rejection.

## 3. Fuzzing
- **Bytemuck Casting:** The `FrameHeader` and metadata structs must be aggressively fuzzed against random byte arrays to ensure the zero-copy casts never panic or cause out-of-bounds reads.
- **WPBC Execution:** Fuzzing the pushdown stack machine with malformed opcodes.