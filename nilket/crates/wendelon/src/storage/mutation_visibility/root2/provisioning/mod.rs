//! Dormant provisioning: kernel-anchored bounded reads, pure owner-inventory event and
//! replica-snapshot codecs, pure owner-core decisions and repair-evidence codec, and the owner-side
//! I/O layer over one trusted owner directory, and pure validation of a replica's pinned tuple. No
//! config, readiness or trusted output is produced here.
pub mod anchored;
pub mod inventory;
pub mod owner_core;
pub mod owner_log;
pub mod replica_anchor;
pub mod replica_read;
pub mod snapshot;
pub mod terminal;

#[cfg(test)]
mod tests;
