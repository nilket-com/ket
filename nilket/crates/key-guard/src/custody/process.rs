//! Process hardening (K3, G7) and the host's network probe (K4, G8).
use crate::custody::{CustodyError, sys};

/// Makes the process non-dumpable (`PR_SET_DUMPABLE=0`) with no core dumps (`RLIMIT_CORE=0`),
/// then reads both back and refuses unless they took effect.
pub fn harden_process() -> Result<(), CustodyError> {
	sys::set_not_dumpable().map_err(CustodyError::io("PR_SET_DUMPABLE"))?;
	sys::set_core_limit_zero().map_err(CustodyError::io("RLIMIT_CORE"))?;
	let dumpable = is_dumpable().map_err(CustodyError::io("PR_GET_DUMPABLE"))?;
	let core = core_dump_limit().map_err(CustodyError::io("RLIMIT_CORE"))?;
	if dumpable || core != (0, 0) {
		return Err(CustodyError::io("process hardening")(
			std::io::Error::other("the settings did not take effect"),
		));
	}
	Ok(())
}

/// `PR_GET_DUMPABLE`, as a flag.
pub fn is_dumpable() -> std::io::Result<bool> {
	sys::dumpable()
}

/// The `(soft, hard)` core-dump size limit.
pub fn core_dump_limit() -> std::io::Result<(u64, u64)> {
	sys::core_limit()
}

/// Whether an interface with these flags counts against an offline host: up and not loopback.
pub(crate) fn counts(flags: u32) -> bool {
	let up = flags & libc::IFF_UP as u32 != 0;
	let loopback = flags & libc::IFF_LOOPBACK as u32 != 0;
	up && !loopback
}

/// The names of the up, non-loopback interfaces (`getifaddrs`), sorted and without repeats.
pub fn non_loopback_interfaces_up() -> std::io::Result<Vec<String>> {
	let mut names: Vec<String> = sys::interfaces()?
		.into_iter()
		.filter(|(_, flags)| counts(*flags))
		.map(|(name, _)| name)
		.collect();
	names.sort();
	names.dedup();
	Ok(names)
}
