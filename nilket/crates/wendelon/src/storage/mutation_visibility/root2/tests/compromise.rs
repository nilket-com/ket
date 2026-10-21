//! C-S6 compromise-audit acceptance: contract r2 A1-A12 with Addendum A A13-A17, and the runbook
//! dry runs. Every history is real carrier entries through the sole production verifier, written
//! as real Control WAL records and read back by the CLI through the read-only scan. The C-S6b
//! context export is checked the same way, and re-verified by a test-only reader.
use super::governance_verification::support as wire;
mod attribution;
mod containment;
mod containment_form;
mod coverage;
mod custody;
mod export;
mod export_chain;
mod output_pin;
mod reader;
mod runbook;
mod support;
