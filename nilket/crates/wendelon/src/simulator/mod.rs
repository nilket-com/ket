//! Deterministic simulator surfaces for W10.b.
//!
//! The simulated transport in [`network`] replaces ad-hoc in-test
//! loopback providers with a reusable fault-injectable substrate.
//! The simulator's L2 backend remains [`crate::storage::l2::MockObjectStore`];
//! this module provides the network, checker, and history-export halves that
//! sit beside it.

pub mod checkers;
pub mod history;
pub mod network;
