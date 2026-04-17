//! Wendelon single-node daemon entry point.
//!
//! CLI wrapper around [`wendelon::node::WendelonNode::bootstrap_single_node`]
//! driven by a `wendelon.toml` file.
//!
//! ## Usage
//!
//! ```text
//!   wendelon <path/to/wendelon.toml>
//!   wendelon --print-example-config
//!   wendelon --write-example-config <path/to/wendelon.toml>
//! ```
//!
//! The config path is required for normal startup; the helper flags
//! print or write a starter config instead.

use std::process::ExitCode;
use wendelon::config::{load_node_bootstrap_config, render_example_config, write_example_config};
use wendelon::node::{NodeBootstrapConfig, WendelonNode};

#[tokio::main]
async fn main() -> ExitCode {
	let args: Vec<String> = std::env::args().collect();
	match args.as_slice() {
		[_bin, flag] if flag == "--print-example-config" => {
			print!("{}", render_example_config());
			return ExitCode::SUCCESS;
		}
		[_bin, flag, path] if flag == "--write-example-config" => {
			match write_example_config(path) {
				Ok(()) => {
					eprintln!("wendelon: wrote starter config to {path}");
					return ExitCode::SUCCESS;
				}
				Err(e) => {
					eprintln!("failed to write starter config to {path}: {e}");
					return ExitCode::from(2);
				}
			}
		}
		[_bin, _config_path] => {}
		_ => {
			eprintln!(
				"usage:\n  {} <path/to/wendelon.toml>\n  {} --print-example-config\n  {} --write-example-config <path/to/wendelon.toml>",
				args[0], args[0], args[0]
			);
			return ExitCode::from(2);
		}
	}

	let cfg: NodeBootstrapConfig = match load_node_bootstrap_config(&args[1]) {
		Ok(cfg) => cfg,
		Err(e) => {
			eprintln!("failed to load {}: {e}", args[1]);
			return ExitCode::from(2);
		}
	};
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
