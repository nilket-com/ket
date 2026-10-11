//! Dormant S1 authority2. No production adapter can construct its private trust tokens.
#[allow(dead_code)] // Dormant manifest math; no production caller until reviewed integration.
mod admin;
#[allow(dead_code)]
// Dormant derived administrator view; no production caller until reviewed integration.
mod admin_install;
#[allow(dead_code)]
// Dormant administrator statement authentication; no production caller until reviewed integration.
mod admin_statement;
#[allow(dead_code)]
// Dormant Admission issuer gate; no production caller until reviewed integration.
mod admission_gate;
#[allow(dead_code)]
// Dormant Admission permission registry; no production caller until reviewed integration.
mod admission_registry;
#[allow(dead_code, unused_imports)] // Deliberately dormant until reviewed authority integration.
mod auth;
mod codec;
mod command;
#[allow(dead_code)]
// Dormant committed-entry adapter; no production caller until reviewed integration.
mod committed;
pub mod durable;
#[allow(dead_code)] // Dormant entry-identity math; no production caller until reviewed integration.
mod entry_identity;
mod incremental;
#[allow(dead_code)] // Dormant origin layer; no production caller until reviewed integration.
mod origin;
mod persistent;
#[allow(dead_code)]
// Dormant committed-prefix shape validation; no production caller until reviewed integration.
mod prefix;
#[allow(dead_code)] // Dormant provisioning reads; no production caller until reviewed integration.
mod provisioning;
mod records;
#[allow(dead_code)]
// Dormant replay progress and recovery mark; no production caller until reviewed integration.
mod recovery;
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
