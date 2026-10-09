//! Dormant administrator-manifest math only: bytes, digest comparison and key enrollability.
//! No provenance, pin, record, inventory, signing or trusted output is produced here.
pub mod errors;
pub mod key;
pub mod manifest;

#[cfg(test)]
mod tests;
