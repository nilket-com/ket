//! Dormant provisioning: kernel-anchored bounded reads, and pure owner-inventory event and
//! replica-snapshot codecs. No owner-log I/O, config, readiness or trusted output is produced here.
pub mod anchored;
pub mod inventory;
pub mod snapshot;

#[cfg(test)]
mod tests;
