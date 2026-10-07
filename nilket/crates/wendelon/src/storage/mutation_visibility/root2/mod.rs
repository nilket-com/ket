//! Dormant S1 authority2. No production adapter can construct its private trust tokens.
mod codec;
mod command;
mod incremental;
mod persistent;
mod records;
mod reducer;
mod state;
mod stored;
mod trust;
mod types;
mod validation;

pub use command::{ApplyResult2, Command2};
pub use state::{Root2, UnverifiedRoot2};
pub use trust::{TrustedEntry2, TrustedView2, VerifiedCommand2};
pub use types::*;

#[cfg(test)]
mod tests;
