//! Dormant provisioning reads: kernel-anchored opens beneath an operator directory and bounded
//! reads. No owner log, snapshot, config, readiness or trusted output is produced here.
pub mod anchored;

#[cfg(test)]
mod tests;
