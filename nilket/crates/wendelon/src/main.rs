//! Wendelon single-node daemon entry point.
//!
//! Minimal CLI wrapper around [`wendelon::node::WendelonNode::bootstrap_single_node`].
//! Production deployments will grow this into a TOML config loader
//! (see `plans/ops/configuration.md`); the current implementation
//! takes arguments straight off the command line so the binary is
//! useful for local smoke-testing without any filesystem state
//! beyond a writable data directory.
//!
//! ## Usage
//!
//! ```text
//!   wendelon <node_id> <data_dir> <advertise_addr>
//! ```
//!
//! All three arguments are required; any parse failure prints a
//! usage line and exits non-zero.

use std::path::PathBuf;
use std::process::ExitCode;
use wendelon::node::{NodeBootstrapConfig, WendelonNode};

#[tokio::main]
async fn main() -> ExitCode {
	let args: Vec<String> = std::env::args().collect();
	if args.len() != 4 {
		eprintln!("usage: {} <node_id> <data_dir> <advertise_addr>", args[0]);
		return ExitCode::from(2);
	}
	let node_id: u32 = match args[1].parse() {
		Ok(v) => v,
		Err(e) => {
			eprintln!("node_id must be u32: {e}");
			return ExitCode::from(2);
		}
	};
	let data_dir = PathBuf::from(&args[2]);
	if !data_dir.is_dir() {
		eprintln!("data_dir {:?} is not an existing directory", data_dir);
		return ExitCode::from(2);
	}
	let advertise = args[3].clone();

	let cfg = NodeBootstrapConfig::dev(node_id, data_dir, advertise);
	let node = match WendelonNode::bootstrap_single_node(cfg).await {
		Ok(n) => n,
		Err(e) => {
			eprintln!("bootstrap failed: {e}");
			return ExitCode::from(1);
		}
	};
	eprintln!(
		"wendelon: bootstrapped node {} as single-node leader",
		node.local_node.id
	);

	// Block until the process is signalled. The watchdogs and ring
	// updater are held by `node.handles` and abort on drop.
	tokio::signal::ctrl_c().await.ok();
	eprintln!("wendelon: shutting down");
	ExitCode::SUCCESS
}
