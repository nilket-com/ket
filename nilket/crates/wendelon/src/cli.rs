//! Wendelon role command-line runner.
//!
//! This module owns Wendelon's branch grammar for both the legacy
//! `wendelon` binary and the unified `nilket wendelon` branch.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use tokio_rustls::TlsAcceptor;

use crate::audit::verify_local_audit_evidence;
use crate::config::{load_node_bootstrap_config, render_example_config, write_example_config};
use crate::metrics::observability::{self, ObservabilityHandles};
use crate::metrics::telemetry;
use crate::network::{ActorTrustRoots, TlsMaterial, rustls_server_config_with_peer_roots};
use crate::node::{NodeBootstrapConfig, WendelonNode};

pub fn run<I>(program: &str, args: I) -> ExitCode
where
	I: IntoIterator<Item = String>,
{
	let args: Vec<String> = args.into_iter().collect();
	match args.as_slice() {
		[flag] if flag == "--help" || flag == "-h" => {
			print_usage(program);
			ExitCode::SUCCESS
		}
		[flag] if flag == "--version" || flag == "-V" => {
			println!("wendelon {}", env!("CARGO_PKG_VERSION"));
			ExitCode::SUCCESS
		}
		[flag] if flag == "--print-example-config" => {
			print!("{}", render_example_config());
			ExitCode::SUCCESS
		}
		[cmd, subcmd, audit_path] if cmd == "audit" && subcmd == "verify" => {
			verify_audit(audit_path)
		}
		[flag, path] if flag == "--write-example-config" => match write_example_config(path) {
			Ok(()) => {
				eprintln!("wendelon: wrote starter config to {path}");
				ExitCode::SUCCESS
			}
			Err(e) => {
				eprintln!("failed to write starter config to {path}: {e}");
				ExitCode::from(2)
			}
		},
		[config_path] => run_daemon(PathBuf::from(config_path)),
		_ => {
			print_usage(program);
			ExitCode::from(2)
		}
	}
}

fn print_usage(program: &str) {
	if program == "wendelon" {
		eprintln!("This binary is deprecated; use `nilket wendelon` instead.");
	}
	eprintln!(
		"usage:\n  {program} <path/to/wendelon.toml>\n  {program} audit verify <path/to/audit.ndjson>\n  {program} --print-example-config\n  {program} --write-example-config <path/to/wendelon.toml>"
	);
}

fn verify_audit(audit_path: &str) -> ExitCode {
	match verify_local_audit_evidence(audit_path) {
		Ok(report) => match serde_json::to_string_pretty(&report) {
			Ok(json) => {
				print!("{json}");
				print!("\n");
				if report.ok() {
					ExitCode::SUCCESS
				} else {
					ExitCode::from(1)
				}
			}
			Err(err) => {
				eprintln!("failed to serialize audit verification report: {err}");
				ExitCode::from(2)
			}
		},
		Err(err) => {
			eprintln!("audit verify failed for {audit_path}: {err}");
			ExitCode::from(2)
		}
	}
}

fn run_daemon(config_path: PathBuf) -> ExitCode {
	let runtime = match tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
	{
		Ok(runtime) => runtime,
		Err(err) => {
			eprintln!("failed to initialize Wendelon runtime: {err}");
			return ExitCode::from(2);
		}
	};
	runtime.block_on(start_daemon(config_path))
}

async fn start_daemon(config_path: PathBuf) -> ExitCode {
	let cfg: NodeBootstrapConfig = match load_node_bootstrap_config(&config_path) {
		Ok(cfg) => cfg,
		Err(e) => {
			eprintln!("failed to load {}: {e}", config_path.display());
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
				Some(node.state_machine.subscribe_revocation_snapshot()),
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
