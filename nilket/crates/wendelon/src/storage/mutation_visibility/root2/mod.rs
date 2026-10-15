//! Dormant authority2. Internal replay can bind trust tokens; no production caller runs it.
#[allow(dead_code)]
// Dormant committed-prefix acquisition; no production caller until reviewed integration.
mod acquisition;
#[allow(dead_code)] // Dormant manifest math; no production caller until reviewed integration.
mod admin;
#[allow(dead_code)]
// Dormant administrator command envelope check; no production caller until reviewed integration.
mod admin_command;
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
#[allow(dead_code)] // Dormant pure governance check; no production caller.
mod governance_command;
#[allow(dead_code)] // Dormant pure governance verification; no external production caller.
mod governance_verification;
mod incremental;
mod issuer_registry;
#[allow(dead_code)] // Dormant pure lease payload check; no production caller.
mod lease_payload;
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
#[allow(dead_code)] // Dormant owned replay; S1 exports no authority or replay result.
mod replay;
#[allow(dead_code)] // Dormant role proof verification; no runtime producer.
mod role_command;
mod role_epochs;
mod state;
mod stored;
mod trust;
mod types;
mod validation;
#[allow(dead_code)]
// Dormant command verification (C); no production caller until reviewed integration.
mod verification;

pub use command::{ApplyResult2, Command2};
pub use state::{Root2, UnverifiedRoot2};
pub use trust::{TrustedEntry2, TrustedView2, VerifiedCommand2};
pub use types::*;

#[cfg(test)]
mod tests;
