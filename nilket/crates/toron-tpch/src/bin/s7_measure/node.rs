//! `s7-measure node`: the measured process. It builds its `NodeBootstrapConfig`
//! programmatically (TOML reaches neither L2 nor the sweeper on either commit),
//! records the resolved values, bootstraps an ordinary `WendelonNode` and is
//! the single writer of the block's JSONL observations: periodic ticks, the
//! reset acknowledgement, the final capture and a drained diagnostic.
//!
//! Signals arrive through Tokio's async handlers and are served from the same
//! loop that writes ticks, so no observation is written from a raw signal
//! handler and no two records interleave. Each request is correlated by the
//! control file the driver writes before signalling: a missing, stale,
//! duplicate or misaddressed request is recorded as `control_error` (which
//! invalidates the block) and never acted on.

use crate::common::{
	ARM, BlockPaths, ControlRequest, NodeReady, mono_ns, pid_start_time, read_json, self_rss_kib,
	write_json_atomic,
};
use crate::manifest::Manifest;
use crate::pki;
use serde_json::{Value, json};
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use tokio::signal::unix::{SignalKind, signal};
use wendelon::node::{NodeBootstrapConfig, TransportTlsConfig, WendelonNode};
use wendelon::storage::{MvccRetentionPolicy, ObjectStoreConfig, SweeperConfig};

struct Observations {
	out: std::io::BufWriter<std::fs::File>,
	pid: u32,
	pid_start_time: u64,
}

impl Observations {
	fn create(path: &Path, pid: u32, pid_start_time: u64) -> std::io::Result<Self> {
		let file = std::fs::OpenOptions::new()
			.create_new(true)
			.append(true)
			.open(path)?;
		Ok(Self {
			out: std::io::BufWriter::new(file),
			pid,
			pid_start_time,
		})
	}

	/// One complete line per record, flushed and synced before returning.
	fn write(&mut self, mut record: Value) -> std::io::Result<Value> {
		let obj = record.as_object_mut().expect("records are objects");
		obj.insert("pid".into(), json!(self.pid));
		obj.insert("pid_start_time".into(), json!(self.pid_start_time));
		let line = serde_json::to_string(&record).map_err(std::io::Error::other)?;
		self.out.write_all(line.as_bytes())?;
		self.out.write_all(b"\n")?;
		self.out.flush()?;
		self.out.get_ref().sync_data()?;
		Ok(record)
	}
}

fn scrape() -> String {
	wendelon::metrics::global().render_prometheus()
}

fn free_loopback_port() -> std::io::Result<u16> {
	for _ in 0..32 {
		let tcp = std::net::TcpListener::bind("127.0.0.1:0")?;
		let port = tcp.local_addr()?.port();
		if std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok() {
			return Ok(port);
		}
	}
	Err(std::io::Error::other(
		"no free loopback port for TCP and UDP",
	))
}

/// The configuration the campaign runs, built from `NodeBootstrapConfig::dev`.
fn build_config(
	m: &Manifest,
	block: &BlockPaths,
	address: String,
	pki: &pki::Pki,
) -> NodeBootstrapConfig {
	let mut cfg = NodeBootstrapConfig::dev(m.node_id, block.data_dir(), address);
	cfg.slab_max_bytes = m.slab_max_bytes;
	cfg.l2 = Some(ObjectStoreConfig::LocalFs {
		root: block.l2_dir(),
	});
	cfg.tiering = SweeperConfig {
		scan_interval: Duration::from_millis(m.sweeper_scan_interval_ms),
		min_slab_bytes: m.sweeper_min_slab_bytes,
		..SweeperConfig::default()
	};
	cfg.mvcc_retention_policy =
		MvccRetentionPolicy::FixedWindow(Duration::from_secs(m.cluster_retention_window_secs));
	#[cfg(s7_fix)]
	{
		cfg.tier_history_budget_bytes = wendelon::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES;
	}
	cfg.transport_tls = Some(TransportTlsConfig {
		cert_path: pki.server_cert.clone(),
		key_path: pki.server_key.clone(),
		node_ca_path: pki.ca_cert.clone(),
		operator_ca_path: pki.ops_ca_cert.clone(),
		cluster_domain: pki::CLUSTER_DOMAIN.into(),
	});
	cfg
}

/// A duration in whole milliseconds, as the record carries it.
fn ms(d: Duration) -> u64 {
	d.as_millis() as u64
}

/// The L2 backend as structured fields: its kind and location, never a credential (DC-309).
fn l2_fields(l2: Option<&ObjectStoreConfig>) -> Value {
	use wendelon::storage::RemoteObjectStoreProvider as P;
	match l2 {
		None => Value::Null,
		Some(ObjectStoreConfig::LocalFs { root }) => json!({"kind": "local_fs", "root": root}),
		Some(ObjectStoreConfig::Remote(r)) => json!({
			"kind": "remote",
			"bucket": r.bucket,
			"root": r.root,
			"provider": match r.provider {
				P::S3Compatible(_) => "s3_compatible",
				P::BackblazeB2(_) => "backblaze_b2",
				P::GoogleCloudStorage(_) => "google_cloud_storage",
			},
		}),
	}
}

/// The values read back from the built config: what the node actually runs. DC-309: explicitly
/// selected nonsecret fields only (every one present in the base arm's configuration too), nested
/// configuration as structured fields; no signing secret, credential or `Debug` rendering of the
/// configuration is recorded.
fn effective_config(cfg: &NodeBootstrapConfig) -> Value {
	use wendelon::consensus::ControlRetention;
	use wendelon::crypto::RootKekProviderConfig as Kek;
	use wendelon::node::ClusterBootstrapConfig;
	#[cfg(s7_fix)]
	let budget = json!(cfg.tier_history_budget_bytes);
	#[cfg(not(s7_fix))]
	let budget = json!("N/A: NodeBootstrapConfig has no tier_history_budget_bytes on base");
	let signer_ids =
		|m: &std::collections::BTreeMap<u32, [u8; 32]>| m.keys().copied().collect::<Vec<_>>();
	json!({
		"arm": ARM,
		"cfg_s7_fix": cfg!(s7_fix),
		"node_id": cfg.node_id,
		"data_dir": cfg.data_dir,
		"advertise_address": cfg.advertise_address,
		"slab_max_bytes": cfg.slab_max_bytes,
		"tier_history_budget_bytes": budget,
		"rf": cfg.rf,
		"l2": l2_fields(cfg.l2.as_ref()),
		"tiering": {
			"scan_interval_ms": ms(cfg.tiering.scan_interval),
			"min_slab_bytes": cfg.tiering.min_slab_bytes,
			"max_upload_bytes_per_sec": cfg.tiering.max_upload_bytes_per_sec,
			"key_prefix": cfg.tiering.key_prefix,
			"max_plaintext_rewrites_per_pass": cfg.tiering.max_plaintext_rewrites_per_pass,
			"max_dek_rewraps_per_pass": cfg.tiering.max_dek_rewraps_per_pass,
		},
		"mvcc_retention_policy": match cfg.mvcc_retention_policy {
			MvccRetentionPolicy::Disabled => json!({"kind": "disabled"}),
			MvccRetentionPolicy::FixedWindow(w) => json!({"kind": "fixed_window", "window_ms": ms(w)}),
		},
		"mvcc_retention_paused": cfg.mvcc_retention_paused,
		"lsm_compaction_paused": cfg.lsm_compaction_paused,
		"group_commit": {
			"floor_us": cfg.group_commit.floor.as_micros() as u64,
			"ceiling_us": cfg.group_commit.ceiling.as_micros() as u64,
			"per_write_cost_estimate_us": cfg.group_commit.per_write_cost_estimate.as_micros() as u64,
		},
		"cluster": match &cfg.cluster {
			ClusterBootstrapConfig::Bootstrap => json!({"mode": "bootstrap"}),
			ClusterBootstrapConfig::Join { seeds } => json!({
				"mode": "join",
				"seeds": seeds.iter().map(|s| json!({"node_id": s.node_id, "address": s.address})).collect::<Vec<_>>(),
			}),
		},
		// The identity, by what it permits and whom it trusts: node ids only, never a key.
		"allow_dev_keys": cfg.allow_dev_keys,
		"trusted_signer_node_ids": {
			"lease": signer_ids(&cfg.trusted_lease_signers),
			"transfer": signer_ids(&cfg.trusted_transfer_signers),
			"audit_root": signer_ids(&cfg.trusted_audit_root_keys),
		},
		"wal_io_depth": cfg.wal_io_depth,
		"slab_io_depth": cfg.slab_io_depth,
		"storage_shutdown_timeout_ms": ms(cfg.storage_shutdown_timeout),
		"cdc_locality_heartbeat_interval_ms": ms(cfg.cdc_locality_heartbeat_interval),
		"data_ring_lease_ttl_ms": ms(cfg.data_ring_lease_ttl),
		"drift_max_offset_ms": cfg.drift_max_offset_ms,
		"control_retention": match cfg.control_retention {
			ControlRetention::Compacting => "compacting",
			ControlRetention::Full => "full",
		},
		"rehydration": {
			"promote_after_l2_reads": cfg.rehydration.promote_after_l2_reads,
			"min_local_residency_ms": ms(cfg.rehydration.min_local_residency),
			"tail_read_window_bytes": cfg.rehydration.tail_read_window_bytes,
		},
		"crypto_cache": {
			"tenant_kek_cache_max_entries": cfg.crypto_cache.tenant_kek_cache_max_entries,
			"slab_dek_cache_max_entries": cfg.crypto_cache.slab_dek_cache_max_entries,
		},
		"root_kek_provider": match cfg.root_kek_provider {
			Kek::DevInsecure => "dev_insecure",
			Kek::LocalSealed(_) => "local_sealed",
			Kek::AwsKms(_) => "aws_kms",
			Kek::Pkcs11(_) => "pkcs11",
		},
		"transport_admission": {
			"max_inbound_connections": cfg.transport_admission.max_inbound_connections,
			"max_inbound_connections_per_ip": cfg.transport_admission.max_inbound_connections_per_ip,
			"max_inbound_streams_per_connection": cfg.transport_admission.max_inbound_streams_per_connection,
			"max_concurrent_stream_tasks": cfg.transport_admission.max_concurrent_stream_tasks,
		},
		"counter_retention": {
			"attempt_horizon_ms": cfg.counter_retention.attempt_horizon_ms,
			"clock_skew_ms": cfg.counter_retention.clock_skew_ms,
			"expiry_ms": cfg.counter_retention.expiry_ms,
		},
		"row_group_writer_enabled": cfg.row_group_writer.enabled,
		"l2_row_group_fetch": {
			"enabled": cfg.l2_row_group_fetch.enabled,
			"max_selected_row_group_fraction_micros": cfg.l2_row_group_fetch.max_selected_row_group_fraction_micros,
			"max_selected_byte_fraction_micros": cfg.l2_row_group_fetch.max_selected_byte_fraction_micros,
		},
		// Data-only strategy types with their own serde form (no key material).
		"l2_column_fetch": serde_json::to_value(&cfg.l2_column_fetch).unwrap_or(Value::Null),
		"encoding_writer": serde_json::to_value(&cfg.encoding_writer).unwrap_or(Value::Null),
		"l2_chunk_layout": serde_json::to_value(&cfg.l2_chunk_layout).unwrap_or(Value::Null),
		"l2_encoding_fetch": serde_json::to_value(&cfg.l2_encoding_fetch).unwrap_or(Value::Null),
		"encoding_compute": serde_json::to_value(&cfg.encoding_compute).unwrap_or(Value::Null),
		"protocol": serde_json::to_value(cfg.protocol).unwrap_or(Value::Null),
		// Present or not; their paths are per block (the block's PKI) or unset.
		"transport_tls": cfg.transport_tls.is_some(),
		"observability": cfg.observability.is_some(),
		"audit_sinks": !cfg.audit.is_empty(),
	})
}

pub async fn run(manifest_path: &Path, block_dir: &Path) -> std::io::Result<()> {
	let (m, manifest_bytes) = Manifest::load(manifest_path)?;
	// The node's production log path (WENDELON_LOG filter) into node.log.
	let _telemetry = wendelon::metrics::telemetry::install_global(None, m.node_id)?;
	let block = BlockPaths::new(block_dir);
	let block_id = block.block_id()?;
	for dir in [block.data_dir(), block.l2_dir(), block.control_dir()] {
		std::fs::create_dir_all(dir)?;
	}
	let pid = std::process::id();
	let start_time = pid_start_time(pid)?;
	let mut obs = Observations::create(&block.observations(), pid, start_time)?;
	let pki = pki::write(&block.pki_dir(), m.node_id)?;
	let address = format!("127.0.0.1:{}", free_loopback_port()?);
	let cfg = build_config(&m, &block, address.clone(), &pki);
	let effective = effective_config(&cfg);
	write_json_atomic(&block.effective_config(), &effective)?;
	obs.write(json!({
		"kind": "start",
		"arm": ARM,
		"label": m.label,
		"block_id": block_id,
		"manifest_sha256": crate::common::sha256_hex(&manifest_bytes),
		"t_mono_ns": mono_ns(),
		"effective_config": effective,
	}))?;

	let node = match Box::pin(WendelonNode::bootstrap(cfg)).await {
		Ok(node) => node,
		Err(error) => {
			obs.write(
				json!({"kind": "fatal", "t_mono_ns": mono_ns(), "error": error.to_string()}),
			)?;
			return Err(error);
		}
	};
	let deadline = tokio::time::Instant::now() + Duration::from_secs(m.node_ready_timeout_secs);
	while !(node.ring.current_epoch() >= 1 && node.ring.snapshot().data_capable_nodes() == 1) {
		if tokio::time::Instant::now() >= deadline {
			let error = std::io::Error::other("node never formed its one-node cluster");
			obs.write(
				json!({"kind": "fatal", "t_mono_ns": mono_ns(), "error": error.to_string()}),
			)?;
			let _ = node.shutdown().await;
			return Err(error);
		}
		tokio::time::sleep(Duration::from_millis(50)).await;
	}

	let mut usr1 = signal(SignalKind::user_defined1())?;
	let mut usr2 = signal(SignalKind::user_defined2())?;
	let mut term = signal(SignalKind::terminate())?;
	let mut int = signal(SignalKind::interrupt())?;

	let t0_mono = mono_ns();
	let t0 = tokio::time::Instant::now();
	let ready = NodeReady {
		arm: ARM.into(),
		label: m.label.clone(),
		pid,
		pid_start_time: start_time,
		address,
		cluster_domain: pki::CLUSTER_DOMAIN.into(),
		node_id: m.node_id,
		admin_cert: pki.operator_cert.clone(),
		admin_key: pki.operator_key.clone(),
		writer_cert: pki.writer_cert.clone(),
		writer_key: pki.writer_key.clone(),
		ca_cert: pki.ca_cert.clone(),
		t_mono_ns: t0_mono,
	};
	obs.write(json!({"kind": "ready", "t_mono_ns": t0_mono, "ready": ready}))?;
	write_json_atomic(&block.node_ready(), &ready)?;

	let tick_ns = m.tick_secs * 1_000_000_000;
	let mut state = Control::default();
	let mut index: u64 = 0;
	let reason = loop {
		let sched = t0 + Duration::from_nanos(index * tick_ns);
		tokio::select! {
			_ = term.recv() => break "SIGTERM",
			_ = int.recv() => break "SIGINT",
			_ = usr1.recv() => state.serve(&block, &mut obs, Signal::Usr1, pid, start_time, &node)?,
			_ = usr2.recv() => state.serve(&block, &mut obs, Signal::Usr2, pid, start_time, &node)?,
			_ = tokio::time::sleep_until(sched) => {
				let t_mono = mono_ns();
				let metrics = scrape();
				let rss_kib = self_rss_kib()?;
				obs.write(json!({
					"kind": "tick",
					"index": index,
					"t_sched_ns": t0_mono + index * tick_ns,
					"t_mono_ns": t_mono,
					"rss_kib": rss_kib,
					"metrics": metrics,
				}))?;
				index += 1;
			}
		}
	};
	obs.write(json!({"kind": "shutdown_begin", "t_mono_ns": mono_ns(), "reason": reason}))?;
	let result = node.shutdown().await;
	obs.write(json!({
		"kind": "shutdown_done",
		"t_mono_ns": mono_ns(),
		"ok": result.is_ok(),
		"error": result.as_ref().err().map(|e| e.to_string()),
	}))?;
	result
}

#[derive(Clone, Copy, Debug)]
enum Signal {
	Usr1,
	Usr2,
}

#[derive(Default)]
struct Control {
	last_seq: u64,
	reset_done: bool,
	final_done: bool,
	diagnostic_done: bool,
}

impl Control {
	/// Serve one signalled request: validate the correlation, act once, and
	/// publish the acknowledgement (JSONL line first, then the ack file).
	fn serve(
		&mut self,
		block: &BlockPaths,
		obs: &mut Observations,
		sig: Signal,
		pid: u32,
		start_time: u64,
		node: &WendelonNode,
	) -> std::io::Result<()> {
		let t_signal = mono_ns();
		let request: Result<ControlRequest, String> =
			read_json(&block.control_request()).map_err(|e| e.to_string());
		let check = request.clone().and_then(|req| {
			if req.pid != pid || req.pid_start_time != start_time {
				return Err(format!("request addressed to another process: {req:?}"));
			}
			if req.seq <= self.last_seq {
				return Err(format!(
					"stale or duplicate seq {} (last {})",
					req.seq, self.last_seq
				));
			}
			let expected = match (sig, req.kind.as_str()) {
				(Signal::Usr1, "reset") => !self.reset_done,
				(Signal::Usr2, "final") => self.reset_done && !self.final_done,
				(Signal::Usr2, "diagnostic") => self.final_done && !self.diagnostic_done,
				_ => false,
			};
			if !expected {
				return Err(format!(
					"request {:?} not expected on {sig:?} now",
					req.kind
				));
			}
			Ok(req)
		});
		let req = match check {
			Ok(req) => req,
			Err(reason) => {
				let record = obs.write(json!({
					"kind": "control_error",
					"signal": format!("{sig:?}"),
					"t_signal_ns": t_signal,
					"request": request.ok(),
					"reason": reason,
				}))?;
				// Ack whatever seq the file named so the driver stops waiting.
				if let Ok(req) = read_json::<ControlRequest>(&block.control_request()) {
					write_json_atomic(&block.control_ack(req.seq), &record)?;
				}
				return Ok(());
			}
		};
		self.last_seq = req.seq;
		let record = match req.kind.as_str() {
			"reset" => {
				let t_pre = mono_ns();
				let pre_scrape = scrape();
				let pre_rss = self_rss_kib()?;
				let t_reset = mono_ns();
				#[cfg(s7_fix)]
				let reset_done = {
					node.slab.reset_tier_measurement_block();
					json!(true)
				};
				#[cfg(not(s7_fix))]
				let reset_done = {
					let _ = node;
					json!("N/A: base 9e7e3ad1 has no tier measurement instrument")
				};
				let t_post = mono_ns();
				let post_scrape = scrape();
				self.reset_done = true;
				json!({
					"kind": "reset_ack",
					"seq": req.seq,
					"t_signal_ns": t_signal,
					"t_pre_ns": t_pre,
					"pre_scrape": pre_scrape,
					"pre_rss_kib": pre_rss,
					"t_reset_ns": t_reset,
					"reset_done": reset_done,
					"t_post_ns": t_post,
					"post_scrape": post_scrape,
					"t_ack_ns": mono_ns(),
				})
			}
			kind @ ("final" | "diagnostic") => {
				let t_capture = mono_ns();
				let metrics = scrape();
				let rss_kib = self_rss_kib()?;
				if kind == "final" {
					self.final_done = true;
				} else {
					self.diagnostic_done = true;
				}
				json!({
					"kind": kind,
					"seq": req.seq,
					"t_signal_ns": t_signal,
					"t_mono_ns": t_capture,
					"rss_kib": rss_kib,
					"metrics": metrics,
					"t_ack_ns": mono_ns(),
				})
			}
			_ => unreachable!("validated above"),
		};
		let record = obs.write(record)?;
		let mut ack = record;
		// The ack file is a handshake, not evidence: drop the bulky scrapes.
		if let Some(obj) = ack.as_object_mut() {
			for key in ["pre_scrape", "post_scrape", "metrics"] {
				obj.remove(key);
			}
		}
		write_json_atomic(&block.control_ack(req.seq), &ack)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn frozen() -> Manifest {
		let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
			.join("../../scripts/s7-measure/manifest.json");
		Manifest::load(&path).unwrap().0
	}

	fn scratch(tag: &str) -> BlockPaths {
		let root = std::env::temp_dir().join(format!(
			"s7-node-config-{tag}-{}-{}",
			std::process::id(),
			mono_ns()
		));
		std::fs::create_dir_all(&root).unwrap();
		BlockPaths::new(root)
	}

	/// DC-309 sentinels: one distinctive byte per secret field, never a real key.
	const LEASE_SENTINEL: [u8; 32] = [0xA5; 32];
	const TRANSFER_SENTINEL: [u8; 32] = [0x5A; 32];
	const AUDIT_SENTINEL: [u8; 32] = [0xC3; 32];
	const CREDENTIAL_SENTINELS: [&str; 3] = [
		"dc309-access-sentinel",
		"dc309-secret-key-sentinel",
		"dc309-session-sentinel",
	];

	/// Whether `text` carries `secret` as its decimal array (compact or pretty), its `{:x?}`
	/// array or its hex string, in either case.
	fn renders(text: &str, secret: [u8; 32]) -> bool {
		let flat: String = text.chars().filter(|c| !c.is_whitespace()).collect();
		let list = |each: &dyn Fn(u8) -> String| secret.map(each).join(",");
		[
			list(&|b| b.to_string()),
			list(&|b| format!("{b:x}")),
			crate::common::hex(&secret),
		]
		.into_iter()
		.any(|form| flat.contains(&form) || flat.contains(&form.to_uppercase()))
	}

	/// Whether the record carries `secret`: in its serialized text, or in any decoded string
	/// value or key (a multi-line string serializes its line breaks as escapes).
	fn renders_anywhere(v: &Value, secret: [u8; 32]) -> bool {
		fn strings(v: &Value, out: &mut Vec<String>) {
			match v {
				Value::String(s) => out.push(s.clone()),
				Value::Array(a) => a.iter().for_each(|x| strings(x, out)),
				Value::Object(o) => o.iter().for_each(|(k, x)| {
					out.push(k.clone());
					strings(x, out)
				}),
				_ => {}
			}
		}
		let mut all = vec![serde_json::to_string(v).unwrap()];
		strings(v, &mut all);
		all.iter().any(|s| renders(s, secret))
	}

	/// DC-309: the effective configuration (sealed as `effective-config.json` and copied into the
	/// start record) records explicitly selected nonsecret fields, structured, with no signing
	/// secret, nested credential or `Debug` rendering; the provenance fields carry their values.
	#[test]
	fn the_effective_config_records_selected_nonsecret_fields_and_no_secret() {
		use wendelon::storage::{
			RemoteObjectStoreConfig, RemoteObjectStoreProvider, S3CompatibleProviderConfig,
		};
		let m = frozen();
		let block = scratch("dc309");
		let pki = pki::write(&block.pki_dir(), m.node_id).unwrap();
		let mut cfg = build_config(&m, &block, "127.0.0.1:1".into(), &pki);
		cfg.lease_signing_secret = LEASE_SENTINEL;
		cfg.transfer_signing_secret = TRANSFER_SENTINEL;
		cfg.audit_root_signing_secret = AUDIT_SENTINEL;
		let local = effective_config(&cfg);
		let [access, secret_key, session] = CREDENTIAL_SENTINELS;
		let mut remote_cfg = cfg.clone();
		remote_cfg.l2 = Some(ObjectStoreConfig::Remote(RemoteObjectStoreConfig {
			bucket: "dc309-bucket".into(),
			root: "dc309-root".into(),
			provider: RemoteObjectStoreProvider::S3Compatible(S3CompatibleProviderConfig {
				endpoint: "http://dc309.invalid".into(),
				region: "dc309-region".into(),
				access_key_id: Some(access.into()),
				secret_access_key: Some(secret_key.into()),
				session_token: Some(session.into()),
				allow_anonymous: false,
				virtual_host_style: false,
			}),
		}));
		let remote = effective_config(&remote_cfg);
		for (form, effective) in [("local L2", &local), ("remote L2", &remote)] {
			for (name, secret) in [
				("lease", LEASE_SENTINEL),
				("transfer", TRANSFER_SENTINEL),
				("audit root", AUDIT_SENTINEL),
			] {
				assert!(
					!renders_anywhere(effective, secret),
					"{form}: the {name} signing secret is recorded"
				);
			}
			let text = serde_json::to_string(effective).unwrap();
			for credential in CREDENTIAL_SENTINELS {
				assert!(
					!text.contains(credential),
					"{form}: an object-store credential is recorded"
				);
			}
			assert!(
				effective.get("debug_full").is_none(),
				"{form}: a Debug dump is recorded"
			);
			// The Debug opener: a base build's budget placeholder names the type in prose.
			assert!(
				!text.contains("<redacted>") && !text.contains("NodeBootstrapConfig {"),
				"{form}: a Debug rendering is recorded"
			);
			// The provenance, with its values.
			assert_eq!(effective["node_id"], m.node_id);
			assert_eq!(effective["allow_dev_keys"], true);
			assert_eq!(effective["slab_max_bytes"], m.slab_max_bytes);
			assert_eq!(effective["rf"], cfg.rf);
			assert_eq!(
				effective["tiering"]["scan_interval_ms"],
				m.sweeper_scan_interval_ms
			);
			assert_eq!(
				effective["tiering"]["min_slab_bytes"],
				m.sweeper_min_slab_bytes
			);
			assert_eq!(
				effective["mvcc_retention_policy"],
				json!({"kind": "fixed_window", "window_ms": m.cluster_retention_window_secs * 1000})
			);
			assert_eq!(effective["cluster"], json!({"mode": "bootstrap"}));
			assert_eq!(
				effective["trusted_signer_node_ids"],
				json!({"lease": [m.node_id], "transfer": [m.node_id], "audit_root": [m.node_id]})
			);
			assert_eq!(
				effective["group_commit"]["floor_us"],
				cfg.group_commit.floor.as_micros() as u64
			);
			assert_eq!(
				effective["crypto_cache"]["tenant_kek_cache_max_entries"],
				cfg.crypto_cache.tenant_kek_cache_max_entries
			);
			assert_eq!(
				effective["crypto_cache"]["slab_dek_cache_max_entries"],
				cfg.crypto_cache.slab_dek_cache_max_entries
			);
			assert_eq!(effective["wal_io_depth"], cfg.wal_io_depth);
			assert_eq!(effective["transport_tls"], true);
			assert_eq!(effective["control_retention"], "compacting");
			assert!(effective["encoding_compute"].is_object());
		}
		assert_eq!(
			local["l2"],
			json!({"kind": "local_fs", "root": block.l2_dir()})
		);
		assert_eq!(
			remote["l2"],
			json!({"kind": "remote", "bucket": "dc309-bucket", "root": "dc309-root", "provider": "s3_compatible"})
		);
		let _ = std::fs::remove_dir_all(&block.root);
	}
}
