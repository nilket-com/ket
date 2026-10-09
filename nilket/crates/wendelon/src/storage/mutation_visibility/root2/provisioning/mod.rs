//! Dormant provisioning: kernel-anchored bounded reads, pure owner-inventory event and
//! replica-snapshot codecs, and pure owner-core decisions and repair-evidence codec. No owner-log
//! I/O, config, readiness or trusted output is produced here.
pub mod anchored;
pub mod inventory;
pub mod owner_core;
pub mod snapshot;

#[cfg(test)]
mod tests;
