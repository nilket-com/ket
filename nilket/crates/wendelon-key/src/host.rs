//! K4 host checks and the K3 swap warning. A recovery-role operation refuses on a host with a
//! node data directory or an up, non-loopback interface, unless the operator overrides it
//! explicitly, and the override is logged.
use crate::error::ToolError;
use std::path::PathBuf;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HostFacts {
	/// Node data directories found: `<XDG data home>/nilket/local` and `$HOME/.wendelon`.
	pub(crate) data_dirs: Vec<PathBuf>,
	/// Up, non-loopback interfaces.
	pub(crate) interfaces: Vec<String>,
	/// Whether `/proc/swaps` lists a swap device.
	pub(crate) swap: bool,
	pub(crate) hostname: String,
}

fn env_dir(name: &str) -> Option<PathBuf> {
	std::env::var_os(name)
		.filter(|v| !v.is_empty())
		.map(PathBuf::from)
}

/// The node data directories this host would have if it ran a node (the `nilket local` profile
/// and a `.wendelon` directory in the home directory).
fn data_dir_candidates() -> Vec<PathBuf> {
	let home = env_dir("HOME");
	let data_home =
		env_dir("XDG_DATA_HOME").or_else(|| home.as_ref().map(|h| h.join(".local/share")));
	data_home
		.map(|d| d.join("nilket/local"))
		.into_iter()
		.chain(home.map(|h| h.join(".wendelon")))
		.collect()
}

pub(crate) fn probe() -> Result<HostFacts, ToolError> {
	let data_dirs = data_dir_candidates()
		.into_iter()
		.filter(|p| std::fs::symlink_metadata(p).is_ok())
		.collect();
	let interfaces = key_guard::custody::non_loopback_interfaces_up()
		.map_err(ToolError::read("the network interface list"))?;
	let swap = std::fs::read_to_string("/proc/swaps").map_or(false, |s| s.lines().count() > 1);
	let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname")
		.map(|h| h.trim().to_string())
		.unwrap_or_else(|_| "unknown".into());
	Ok(HostFacts {
		data_dirs,
		interfaces,
		swap,
		hostname,
	})
}

impl HostFacts {
	/// What fails the recovery-role checks, or `None` on a clean host.
	pub(crate) fn failures(&self) -> Option<String> {
		let mut found = Vec::new();
		if !self.data_dirs.is_empty() {
			let dirs: Vec<String> = self
				.data_dirs
				.iter()
				.map(|d| d.display().to_string())
				.collect();
			found.push(format!("node data directory {}", dirs.join(", ")));
		}
		if !self.interfaces.is_empty() {
			found.push(format!(
				"non-loopback interface {}",
				self.interfaces.join(", ")
			));
		}
		(!found.is_empty()).then(|| found.join("; "))
	}
}
