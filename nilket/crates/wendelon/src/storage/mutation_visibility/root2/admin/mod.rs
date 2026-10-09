//! Dormant administrator math only: manifest and bootstrap-record bytes, digest comparison and
//! key enrollability. No provenance, pin, inventory, readiness, signing or trusted output is produced here.
pub mod errors;
pub mod key;
pub mod manifest;
pub mod record;

#[cfg(test)]
mod tests;
