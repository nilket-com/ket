//! Wendelon daemon entry point.
//!
//! CLI wrapper around [`wendelon::node::WendelonNode::bootstrap`]
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
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use wendelon::config::{load_node_bootstrap_config, render_example_config, write_example_config};
use wendelon::metrics::observability::{self, ObservabilityHandles};
use wendelon::metrics::telemetry;
use wendelon::network::{ActorTrustRoots, TlsMaterial, rustls_server_config_with_peer_roots};
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
	let transport_tls = cfg.transport_tls.clone();
	let observability_cfg = cfg.observability.clone();
	let telemetry = match telemetry::install_global(
		observability_cfg
			.as_ref()
			.and_then(|observability| observability.traces.as_ref()),
		cfg.node_id,
	) {
		Ok(handle) => handle,
		Err(e) => {
			eprintln!("failed to initialize tracing subscriber: {e}");
			return ExitCode::from(2);
		}
	};
	let node = match WendelonNode::bootstrap(cfg).await {
		Ok(n) => n,
		Err(e) => {
			eprintln!("bootstrap failed: {e}");
			return ExitCode::from(1);
		}
	};
	eprintln!("wendelon: bootstrapped node {}", node.local_node.id);

	let observability = match (
		transport_tls.as_ref(),
		observability_cfg
			.as_ref()
			.and_then(|obs| obs.listeners.clone()),
	) {
		(Some(tls), Some(listeners)) => {
			let material = match TlsMaterial::load(&tls.cert_path, &tls.key_path, &tls.node_ca_path)
			{
				Ok(material) => material,
				Err(e) => {
					eprintln!("failed to load observability TLS material: {e}");
					return ExitCode::from(2);
				}
			};
			let trust_roots = match ActorTrustRoots::load(&tls.node_ca_path, &tls.operator_ca_path)
			{
				Ok(roots) => Arc::new(roots),
				Err(e) => {
					eprintln!("failed to load observability trust roots: {e}");
					return ExitCode::from(2);
				}
			};
			let server_config = match rustls_server_config_with_peer_roots(
				&material,
				trust_roots.combined_roots(),
				Vec::new(),
			) {
				Ok(material) => material,
				Err(e) => {
					eprintln!("failed to build observability TLS server config: {e}");
					return ExitCode::from(2);
				}
			};
			let reporter = Arc::new(node.readiness_reporter());
			match observability::spawn(
				listeners,
				TlsAcceptor::from(Arc::new(server_config)),
				reporter,
				node.local_node.id,
				&tls.cluster_domain,
				trust_roots,
				node.audit_sink.clone(),
			)
			.await
			{
				Ok(handles) => {
					eprintln!(
						"wendelon: health listening on {}",
						handles.health.local_addr
					);
					eprintln!("wendelon: ready listening on {}", handles.ready.local_addr);
					eprintln!(
						"wendelon: metrics listening on {}",
						handles.metrics.local_addr
					);
					Some(handles)
				}
				Err(e) => {
					eprintln!("failed to start observability listeners: {e}");
					return ExitCode::from(1);
				}
			}
		}
		_ => None,
	};

	tokio::signal::ctrl_c().await.ok();
	eprintln!("wendelon: draining");
	if let Err(e) = node.shutdown().await {
		eprintln!("shutdown failed: {e}");
		return ExitCode::from(1);
	}
	if let Some(handles) = observability {
		if let Err(e) = shutdown_observability(handles).await {
			eprintln!("observability shutdown failed: {e}");
			return ExitCode::from(1);
		}
	}
	if let Err(e) = telemetry.shutdown().await {
		eprintln!("telemetry shutdown failed: {e}");
		return ExitCode::from(1);
	}
	eprintln!("wendelon: shutdown complete");
	ExitCode::SUCCESS
}

async fn shutdown_observability(handles: ObservabilityHandles) -> std::io::Result<()> {
	for handle in [&handles.health, &handles.ready, &handles.metrics] {
		handle.task.abort();
	}
	for (label, task) in [
		("health listener", handles.health.task),
		("ready listener", handles.ready.task),
		("metrics listener", handles.metrics.task),
	] {
		match task.await {
			Ok(()) => {}
			Err(err) if err.is_cancelled() => {}
			Err(err) => {
				return Err(std::io::Error::other(format!(
					"{label} failed during shutdown: {err}",
				)));
			}
		}
	}
	Ok(())
}
