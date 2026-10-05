//! Dormant ADR0100 authority substrate. No production admission/read/Finalize path uses it.
//! Canonical logical input and committed log lineage are trusted integration boundaries.
mod authority;
mod codec;
mod commitment;
mod encoding;
mod error;
mod identity;
mod manifest;

pub use authority::*;
pub use commitment::*;
pub use identity::*;
pub use manifest::*;

pub use error::VisibilityError;
use error::redacted_debug;

#[cfg(test)]
mod tests;
