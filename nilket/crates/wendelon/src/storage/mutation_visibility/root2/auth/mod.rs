//! Dormant fixed-profile math only; no authority or capability producers.
pub mod errors;
pub mod preimage;
pub mod signature;

use errors::{AuthError2, AuthResult2};
use preimage::{admission_preimage2, claim_mac_preimage2, holder_preimage2};
use signature::{verify_admission_signature2, verify_holder_signature2};

#[cfg(test)]
mod tests;
