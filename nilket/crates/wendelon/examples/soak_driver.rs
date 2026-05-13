use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use wendelon::schema::{Locality, TenantId};
use wendelon_client::{
	BootstrapPeer, Client, ClientConfig, ClientError, DurabilityClass, QueryLocalityRange,
	QueryOptions, ReadConsistency, ReadOptions, TlsPaths, WriteOptions,
};

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:0";
const DEFAULT_CLUSTER_DOMAIN: &str = "cluster.local";
const DEFAULT_DURATION_SECS: u64 = 3_600;
const DEFAULT_KEY_COUNT: usize = 100_000;
const DEFAULT_PREFILL_KEYS: usize = 1_000;
const DEFAULT_CONCURRENCY: usize = 16;
const DEFAULT_STATS_INTERVAL_SECS: u64 = 5;
const DEFAULT_SMALL_VALUE_BYTES: usize = 256;
const DEFAULT_MEDIUM_VALUE_BYTES: usize = 4_096;
const DEFAULT_MEDIUM_WRITE_EVERY: u64 = 10;
const DEFAULT_QUERY_WIDTH_LOCALITIES: u64 = 1_024;
const DEFAULT_WRITE_PERCENT: u32 = 40;
const DEFAULT_READ_PERCENT: u32 = 40;
const DEFAULT_QUERY_PERCENT: u32 = 20;
const DEFAULT_SEED: u64 = 0xD650_A001;
// Keep soak summaries bounded on long runs; percentiles are estimated
// from this in-memory reservoir rather than from every observed latency.
const MAX_LATENCY_SAMPLES: usize = 100_000;

#[derive(Clone, Debug, Serialize)]
struct SoakConfig {
	bind_addr: String,
	bootstrap: Vec<String>,
	tls_cert: String,
	tls_key: String,
	tls_ca: String,
	cluster_domain: String,
	duration_secs: u64,
	key_count: usize,
	prefill_keys: usize,
	concurrency: usize,
	stats_interval_secs: u64,
	target_ops_per_sec: u64,
	small_value_bytes: usize,
	medium_value_bytes: usize,
	medium_write_every: u64,
	query_width_localities: u64,
	write_percent: u32,
	read_percent: u32,
	query_percent: u32,
	read_consistency: &'static str,
	query_consistency: &'static str,
	await_finalization: bool,
	seed: u64,
	output_dir: Option<String>,
}

#[derive(Clone)]
struct KeySpec {
	key: Vec<u8>,
	locality: Locality,
}

#[derive(Default)]
struct OpAccumulator {
	total: u64,
	ok: u64,
	errors: BTreeMap<String, u64>,
	latencies_ns: Vec<u64>,
	latency_observations: u64,
	reservoir_state: u64,
	query_rows: u64,
}

#[derive(Default)]
struct SharedStats {
	writes: Mutex<OpAccumulator>,
	reads: Mutex<OpAccumulator>,
	queries: Mutex<OpAccumulator>,
}

#[derive(Clone, Copy)]
enum OpKind {
	Write,
	Read,
	Query,
}

#[derive(Serialize)]
struct SampleSnapshot {
	elapsed_secs: u64,
	writes_total: u64,
	writes_ok: u64,
	reads_total: u64,
	reads_ok: u64,
	queries_total: u64,
	queries_ok: u64,
}

#[derive(Serialize)]
struct SummaryStats {
	total: u64,
	ok: u64,
	errors: BTreeMap<String, u64>,
	avg_ms: f64,
	p50_ms: f64,
	p95_ms: f64,
	p99_ms: f64,
	max_ms: f64,
	query_rows: u64,
}

#[derive(Serialize)]
struct SoakSummary {
	duration_secs: u64,
	write: SummaryStats,
	read: SummaryStats,
	query: SummaryStats,
}

fn usage() -> &'static str {
	"usage:
  cargo run --example soak_driver -- \\
    --bootstrap <node_id=host:port> [--bootstrap <node_id=host:port> ...] \\
    --tls-cert <path> --tls-key <path> --tls-ca <path> \\
    [--bind-addr 0.0.0.0:0] [--cluster-domain cluster.local] \\
    [--duration-secs 3600] [--key-count 100000] [--prefill-keys 1000] \\
    [--concurrency 16] [--stats-interval-secs 5] [--target-ops-per-sec 0] \\
    [--small-value-bytes 256] [--medium-value-bytes 4096] [--medium-write-every 10] \\
    [--query-width-localities 1024] \\
    [--write-percent 40] [--read-percent 40] [--query-percent 20] \\
    [--read-consistency linearizable|stale] [--query-consistency linearizable|stale] \\
    [--await-finalization] [--seed <u64>] [--output-dir <path>]

notes:
  - repeat --bootstrap for multiple peers
  - the driver emits periodic samples to stdout and, when --output-dir is set,
    writes soak_config.json, samples.jsonl, and summary.json there"
}

fn parse_u64(flag: &str, value: &str) -> Result<u64, String> {
	value
		.parse::<u64>()
		.map_err(|err| format!("{flag} parse failed for '{value}': {err}"))
}

fn parse_usize(flag: &str, value: &str) -> Result<usize, String> {
	value
		.parse::<usize>()
		.map_err(|err| format!("{flag} parse failed for '{value}': {err}"))
}

fn parse_u32(flag: &str, value: &str) -> Result<u32, String> {
	value
		.parse::<u32>()
		.map_err(|err| format!("{flag} parse failed for '{value}': {err}"))
}

fn parse_bootstrap(value: &str) -> Result<Vec<BootstrapPeer>, String> {
	let mut out = Vec::new();
	for raw in value.split(',').filter(|part| !part.trim().is_empty()) {
		let (node_id, address) = raw.split_once('=').ok_or_else(|| {
			format!("bootstrap peer must look like node_id=host:port, got '{raw}'")
		})?;
		let node_id = node_id
			.trim()
			.parse::<u32>()
			.map_err(|err| format!("bootstrap node id parse failed in '{raw}': {err}"))?;
		let address = address.trim();
		if address.is_empty() {
			return Err(format!("bootstrap address must not be empty in '{raw}'"));
		}
		out.push(BootstrapPeer {
			node_id,
			address: address.to_string(),
		});
	}
	if out.is_empty() {
		return Err("at least one bootstrap peer is required".to_string());
	}
	Ok(out)
}

fn parse_consistency(value: &str) -> Result<ReadConsistency, String> {
	match value {
		"linearizable" => Ok(ReadConsistency::LINEARIZABLE),
		"stale" => Ok(ReadConsistency::STALE),
		other => Err(format!(
			"consistency must be 'linearizable' or 'stale', got '{other}'"
		)),
	}
}

impl SoakConfig {
	fn parse() -> Result<Self, String> {
		let mut bind_addr = DEFAULT_BIND_ADDR.parse::<SocketAddr>().unwrap();
		let mut bootstrap = Vec::new();
		let mut tls_cert: Option<PathBuf> = None;
		let mut tls_key: Option<PathBuf> = None;
		let mut tls_ca: Option<PathBuf> = None;
		let mut cluster_domain = DEFAULT_CLUSTER_DOMAIN.to_string();
		let mut duration_secs = DEFAULT_DURATION_SECS;
		let mut key_count = DEFAULT_KEY_COUNT;
		let mut prefill_keys = DEFAULT_PREFILL_KEYS;
		let mut concurrency = DEFAULT_CONCURRENCY;
		let mut stats_interval_secs = DEFAULT_STATS_INTERVAL_SECS;
		let mut target_ops_per_sec = 0_u64;
		let mut small_value_bytes = DEFAULT_SMALL_VALUE_BYTES;
		let mut medium_value_bytes = DEFAULT_MEDIUM_VALUE_BYTES;
		let mut medium_write_every = DEFAULT_MEDIUM_WRITE_EVERY;
		let mut query_width_localities = DEFAULT_QUERY_WIDTH_LOCALITIES;
		let mut write_percent = DEFAULT_WRITE_PERCENT;
		let mut read_percent = DEFAULT_READ_PERCENT;
		let mut query_percent = DEFAULT_QUERY_PERCENT;
		let mut read_consistency = ReadConsistency::LINEARIZABLE;
		let mut query_consistency = ReadConsistency::STALE;
		let mut await_finalization = false;
		let mut seed = DEFAULT_SEED;
		let mut output_dir: Option<PathBuf> = None;

		let mut args = std::env::args().skip(1);
		while let Some(arg) = args.next() {
			match arg.as_str() {
				"--help" | "-h" => return Err(usage().to_string()),
				"--bind-addr" => {
					let value = args.next().ok_or("--bind-addr requires a value")?;
					bind_addr = SocketAddr::from_str(&value)
						.map_err(|err| format!("--bind-addr parse failed for '{value}': {err}"))?;
				}
				"--bootstrap" => {
					let value = args.next().ok_or("--bootstrap requires a value")?;
					bootstrap.extend(parse_bootstrap(&value)?);
				}
				"--tls-cert" => {
					tls_cert = Some(PathBuf::from(
						args.next().ok_or("--tls-cert requires a value")?,
					));
				}
				"--tls-key" => {
					tls_key = Some(PathBuf::from(
						args.next().ok_or("--tls-key requires a value")?,
					));
				}
				"--tls-ca" => {
					tls_ca = Some(PathBuf::from(
						args.next().ok_or("--tls-ca requires a value")?,
					));
				}
				"--cluster-domain" => {
					cluster_domain = args.next().ok_or("--cluster-domain requires a value")?;
				}
				"--duration-secs" => {
					duration_secs = parse_u64(
						"--duration-secs",
						&args.next().ok_or("--duration-secs requires a value")?,
					)?;
				}
				"--key-count" => {
					key_count = parse_usize(
						"--key-count",
						&args.next().ok_or("--key-count requires a value")?,
					)?;
				}
				"--prefill-keys" => {
					prefill_keys = parse_usize(
						"--prefill-keys",
						&args.next().ok_or("--prefill-keys requires a value")?,
					)?;
				}
				"--concurrency" => {
					concurrency = parse_usize(
						"--concurrency",
						&args.next().ok_or("--concurrency requires a value")?,
					)?;
				}
				"--stats-interval-secs" => {
					stats_interval_secs = parse_u64(
						"--stats-interval-secs",
						&args
							.next()
							.ok_or("--stats-interval-secs requires a value")?,
					)?;
				}
				"--target-ops-per-sec" => {
					target_ops_per_sec = parse_u64(
						"--target-ops-per-sec",
						&args.next().ok_or("--target-ops-per-sec requires a value")?,
					)?;
				}
				"--small-value-bytes" => {
					small_value_bytes = parse_usize(
						"--small-value-bytes",
						&args.next().ok_or("--small-value-bytes requires a value")?,
					)?;
				}
				"--medium-value-bytes" => {
					medium_value_bytes = parse_usize(
						"--medium-value-bytes",
						&args.next().ok_or("--medium-value-bytes requires a value")?,
					)?;
				}
				"--medium-write-every" => {
					medium_write_every = parse_u64(
						"--medium-write-every",
						&args.next().ok_or("--medium-write-every requires a value")?,
					)?;
				}
				"--query-width-localities" => {
					query_width_localities = parse_u64(
						"--query-width-localities",
						&args
							.next()
							.ok_or("--query-width-localities requires a value")?,
					)?;
				}
				"--write-percent" => {
					write_percent = parse_u32(
						"--write-percent",
						&args.next().ok_or("--write-percent requires a value")?,
					)?;
				}
				"--read-percent" => {
					read_percent = parse_u32(
						"--read-percent",
						&args.next().ok_or("--read-percent requires a value")?,
					)?;
				}
				"--query-percent" => {
					query_percent = parse_u32(
						"--query-percent",
						&args.next().ok_or("--query-percent requires a value")?,
					)?;
				}
				"--read-consistency" => {
					read_consistency = parse_consistency(
						&args.next().ok_or("--read-consistency requires a value")?,
					)?;
				}
				"--query-consistency" => {
					query_consistency = parse_consistency(
						&args.next().ok_or("--query-consistency requires a value")?,
					)?;
				}
				"--await-finalization" => {
					await_finalization = true;
				}
				"--seed" => {
					seed = parse_u64("--seed", &args.next().ok_or("--seed requires a value")?)?;
				}
				"--output-dir" => {
					output_dir = Some(PathBuf::from(
						args.next().ok_or("--output-dir requires a value")?,
					));
				}
				other => return Err(format!("unknown argument '{other}'\n\n{}", usage())),
			}
		}

		let tls_cert = tls_cert.ok_or("--tls-cert is required")?;
		let tls_key = tls_key.ok_or("--tls-key is required")?;
		let tls_ca = tls_ca.ok_or("--tls-ca is required")?;
		if bootstrap.is_empty() {
			return Err(format!(
				"at least one --bootstrap is required\n\n{}",
				usage()
			));
		}
		if key_count == 0 {
			return Err("--key-count must be greater than zero".to_string());
		}
		if prefill_keys > key_count {
			return Err("--prefill-keys must be less than or equal to --key-count".to_string());
		}
		if concurrency == 0 {
			return Err("--concurrency must be greater than zero".to_string());
		}
		if stats_interval_secs == 0 {
			return Err("--stats-interval-secs must be greater than zero".to_string());
		}
		if small_value_bytes < 16 || medium_value_bytes < 16 {
			return Err("value sizes must be at least 16 bytes".to_string());
		}
		if medium_write_every == 0 {
			return Err("--medium-write-every must be greater than zero".to_string());
		}
		if write_percent + read_percent + query_percent != 100 {
			return Err("write/read/query percentages must sum to 100".to_string());
		}

		Ok(Self {
			bind_addr: bind_addr.to_string(),
			bootstrap: bootstrap
				.iter()
				.map(|peer| format!("{}={}", peer.node_id, peer.address))
				.collect(),
			tls_cert: tls_cert.display().to_string(),
			tls_key: tls_key.display().to_string(),
			tls_ca: tls_ca.display().to_string(),
			cluster_domain,
			duration_secs,
			key_count,
			prefill_keys,
			concurrency,
			stats_interval_secs,
			target_ops_per_sec,
			small_value_bytes,
			medium_value_bytes,
			medium_write_every,
			query_width_localities,
			write_percent,
			read_percent,
			query_percent,
			read_consistency: consistency_label(read_consistency),
			query_consistency: consistency_label(query_consistency),
			await_finalization,
			seed,
			output_dir: output_dir.as_ref().map(|path| path.display().to_string()),
		})
	}
}

fn consistency_label(value: ReadConsistency) -> &'static str {
	match value {
		ReadConsistency::LINEARIZABLE => "linearizable",
		ReadConsistency::STALE => "stale",
		_ => "other",
	}
}

fn encode_value(key_index: u64, version: u64, total_len: usize) -> Vec<u8> {
	let len = total_len.max(16);
	let mut out = vec![0_u8; len];
	out[..8].copy_from_slice(&key_index.to_le_bytes());
	out[8..16].copy_from_slice(&version.to_le_bytes());
	for (idx, byte) in out[16..].iter_mut().enumerate() {
		*byte = (key_index as u8)
			.wrapping_add(version as u8)
			.wrapping_add((idx as u8).wrapping_mul(31));
	}
	out
}

fn build_keys(key_count: usize) -> Vec<KeySpec> {
	(0..key_count)
		.map(|idx| {
			let key = format!("soak-key-{idx:08}").into_bytes();
			let identity = Client::identity_for_key(TenantId::ZERO, &key);
			KeySpec {
				key,
				locality: Locality::from(identity),
			}
		})
		.collect()
}

fn record_result(
	stats: &SharedStats,
	kind: OpKind,
	latency: Duration,
	error: Option<String>,
	query_rows: u64,
) {
	let guard = match kind {
		OpKind::Write => &stats.writes,
		OpKind::Read => &stats.reads,
		OpKind::Query => &stats.queries,
	};
	let mut guard = guard.lock().unwrap();
	guard.total += 1;
	if let Some(label) = error {
		*guard.errors.entry(label).or_insert(0) += 1;
	} else {
		guard.ok += 1;
	}
	guard.latency_observations = guard.latency_observations.saturating_add(1);
	let latency_ns = latency.as_nanos() as u64;
	if guard.latencies_ns.len() < MAX_LATENCY_SAMPLES {
		guard.latencies_ns.push(latency_ns);
	} else {
		guard.reservoir_state = guard
			.reservoir_state
			.wrapping_mul(6_364_136_223_846_793_005)
			.wrapping_add(1);
		let slot = guard.reservoir_state % guard.latency_observations;
		if slot < MAX_LATENCY_SAMPLES as u64 {
			guard.latencies_ns[slot as usize] = latency_ns;
		}
	}
	guard.query_rows += query_rows;
}

fn snapshot_counts(stats: &SharedStats, started: Instant) -> SampleSnapshot {
	let writes = stats.writes.lock().unwrap();
	let reads = stats.reads.lock().unwrap();
	let queries = stats.queries.lock().unwrap();
	SampleSnapshot {
		elapsed_secs: started.elapsed().as_secs(),
		writes_total: writes.total,
		writes_ok: writes.ok,
		reads_total: reads.total,
		reads_ok: reads.ok,
		queries_total: queries.total,
		queries_ok: queries.ok,
	}
}

fn percentile(sorted: &[u64], q: f64) -> f64 {
	if sorted.is_empty() {
		return 0.0;
	}
	let index = ((sorted.len().saturating_sub(1)) as f64 * q).round() as usize;
	sorted[index.min(sorted.len().saturating_sub(1))] as f64 / 1_000_000.0
}

fn summarize(acc: &OpAccumulator) -> SummaryStats {
	let mut sorted = acc.latencies_ns.clone();
	sorted.sort_unstable();
	let avg_ms = if sorted.is_empty() {
		0.0
	} else {
		sorted.iter().sum::<u64>() as f64 / sorted.len() as f64 / 1_000_000.0
	};
	let max_ms = sorted.last().copied().unwrap_or(0) as f64 / 1_000_000.0;
	SummaryStats {
		total: acc.total,
		ok: acc.ok,
		errors: acc.errors.clone(),
		avg_ms,
		p50_ms: percentile(&sorted, 0.50),
		p95_ms: percentile(&sorted, 0.95),
		p99_ms: percentile(&sorted, 0.99),
		max_ms,
		query_rows: acc.query_rows,
	}
}

fn error_label(err: &ClientError) -> String {
	match err {
		ClientError::QuotaExceeded { .. } => "quota_exceeded".to_string(),
		ClientError::TenantSuspended { .. } => "tenant_suspended".to_string(),
		ClientError::TenantUnavailable { .. } => "tenant_unavailable".to_string(),
		ClientError::AuthorizationFailed => "authorization_failed".to_string(),
		ClientError::WriteFailed { status } => format!("write_status_{status:?}"),
		ClientError::ReadFailed { status } => format!("read_status_{status:?}"),
		ClientError::Io(err) => format!("io_{}", err.kind()),
		ClientError::TableWriteRejected { reject_code } => {
			format!("table_write_rejected_{}", reject_code.0)
		}
		ClientError::TableDescriptorEvolved { .. } => "table_descriptor_evolved".to_string(),
		ClientError::SchemaUpdateRejected { reason_code, .. } => {
			format!("schema_update_rejected_{reason_code}")
		}
	}
}

async fn prefill(
	client: &Client,
	keys: &[KeySpec],
	versions: &[AtomicU64],
	count: usize,
	small_value_bytes: usize,
	await_finalization: bool,
) -> Result<(), ClientError> {
	for idx in 0..count {
		let version = versions[idx].fetch_add(1, Ordering::Relaxed) + 1;
		let value = encode_value(idx as u64, version, small_value_bytes);
		client
			.write(
				TenantId::ZERO,
				&keys[idx].key,
				&value,
				WriteOptions {
					durability: DurabilityClass::QUORUM,
					await_finalization,
					client_hlc: None,
				},
			)
			.await?;
	}
	Ok(())
}

fn append_json_line(path: &PathBuf, value: &impl Serialize) -> std::io::Result<()> {
	let mut file = OpenOptions::new().create(true).append(true).open(path)?;
	serde_json::to_writer(&mut file, value)?;
	file.write_all(b"\n")?;
	Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = match SoakConfig::parse() {
		Ok(config) => config,
		Err(message) => {
			eprintln!("{message}");
			std::process::exit(2);
		}
	};

	let bind_addr = SocketAddr::from_str(&config.bind_addr)?;
	let mut bootstrap = Vec::new();
	for raw in &config.bootstrap {
		bootstrap.extend(parse_bootstrap(raw)?);
	}
	let tls = TlsPaths {
		cert_path: PathBuf::from(&config.tls_cert),
		key_path: PathBuf::from(&config.tls_key),
		ca_path: PathBuf::from(&config.tls_ca),
	};
	let mut client_cfg = ClientConfig::new(bind_addr, bootstrap, tls);
	client_cfg.cluster_domain = config.cluster_domain.clone();
	let client = Arc::new(Client::connect_quic(client_cfg).await?);
	let keys = Arc::new(build_keys(config.key_count));
	let versions = Arc::new(
		(0..config.key_count)
			.map(|_| AtomicU64::new(0))
			.collect::<Vec<_>>(),
	);
	let stats = Arc::new(SharedStats::default());

	let output_dir = config.output_dir.as_ref().map(PathBuf::from);
	if let Some(dir) = &output_dir {
		fs::create_dir_all(dir)?;
		fs::write(
			dir.join("soak_config.json"),
			serde_json::to_vec_pretty(&config)?,
		)?;
	}

	println!(
		"SOAK_START duration_secs={} concurrency={} key_count={} prefill_keys={} bootstrap_peers={}",
		config.duration_secs,
		config.concurrency,
		config.key_count,
		config.prefill_keys,
		config.bootstrap.len()
	);
	prefill(
		&client,
		keys.as_ref(),
		versions.as_ref(),
		config.prefill_keys,
		config.small_value_bytes,
		config.await_finalization,
	)
	.await?;
	println!("SOAK_PREFILL_OK count={}", config.prefill_keys);

	let started = Instant::now();
	let deadline = started + Duration::from_secs(config.duration_secs);
	let samples_path = output_dir.as_ref().map(|dir| dir.join("samples.jsonl"));

	let reporter_stats = stats.clone();
	let reporter_samples = samples_path.clone();
	let reporter_started = started;
	let reporter_deadline = deadline;
	let stats_interval = Duration::from_secs(config.stats_interval_secs);
	let reporter = tokio::spawn(async move {
		let mut ticker = tokio::time::interval(stats_interval);
		ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
		let deadline = tokio::time::Instant::from_std(reporter_deadline);
		loop {
			tokio::select! {
				_ = tokio::time::sleep_until(deadline) => break,
				_ = ticker.tick() => {}
			}
			let sample = snapshot_counts(&reporter_stats, reporter_started);
			println!(
				"SOAK_SAMPLE elapsed_secs={} writes={}/{} reads={}/{} queries={}/{}",
				sample.elapsed_secs,
				sample.writes_ok,
				sample.writes_total,
				sample.reads_ok,
				sample.reads_total,
				sample.queries_ok,
				sample.queries_total
			);
			if let Some(path) = &reporter_samples {
				let _ = append_json_line(path, &sample);
			}
		}
	});

	let mut workers = Vec::with_capacity(config.concurrency);
	let per_worker_period = if config.target_ops_per_sec == 0 {
		None
	} else {
		Some(Duration::from_secs_f64(
			config.concurrency as f64 / config.target_ops_per_sec as f64,
		))
	};
	for worker_id in 0..config.concurrency {
		let client = client.clone();
		let keys = keys.clone();
		let versions = versions.clone();
		let stats = stats.clone();
		let cfg = config.clone();
		workers.push(tokio::spawn(async move {
			let mut rng =
				StdRng::seed_from_u64(cfg.seed ^ (worker_id as u64).wrapping_mul(0x9E37_79B9));
			while Instant::now() < deadline {
				let cycle_started = Instant::now();
				let roll = rng.random_range(0..100);
				let key_index = rng.random_range(0..keys.len());
				let key = &keys[key_index];

				if roll < cfg.write_percent {
					let version = versions[key_index].fetch_add(1, Ordering::Relaxed) + 1;
					let size = if version % cfg.medium_write_every == 0 {
						cfg.medium_value_bytes
					} else {
						cfg.small_value_bytes
					};
					let value = encode_value(key_index as u64, version, size);
					let started = Instant::now();
					let result = client
						.write(
							TenantId::ZERO,
							&key.key,
							&value,
							WriteOptions {
								durability: DurabilityClass::QUORUM,
								await_finalization: cfg.await_finalization,
								client_hlc: None,
							},
						)
						.await;
					record_result(
						&stats,
						OpKind::Write,
						started.elapsed(),
						result.err().as_ref().map(error_label),
						0,
					);
				} else if roll < cfg.write_percent + cfg.read_percent {
					let started = Instant::now();
					let result = client
						.read(
							TenantId::ZERO,
							&key.key,
							ReadOptions {
								consistency: parse_consistency(cfg.read_consistency).unwrap(),
								min_hlc: None,
								as_of_hlc: None,
							},
						)
						.await;
					record_result(
						&stats,
						OpKind::Read,
						started.elapsed(),
						result.err().as_ref().map(error_label),
						0,
					);
				} else {
					let half = cfg.query_width_localities / 2;
					let range = QueryLocalityRange {
						start_locality: key.locality.saturating_sub(half),
						end_locality: key.locality.saturating_add(half),
					};
					let started = Instant::now();
					let result = client
						.query(
							TenantId::ZERO,
							Vec::new(),
							QueryOptions {
								range,
								consistency: parse_consistency(cfg.query_consistency).unwrap(),
								min_hlc: None,
								as_of_hlc: None,
								stats_pruning_enabled: true,
							},
						)
						.await;
					let rows = result.as_ref().map(|query| query.rows).unwrap_or(0);
					record_result(
						&stats,
						OpKind::Query,
						started.elapsed(),
						result.err().as_ref().map(error_label),
						rows,
					);
				}

				if let Some(period) = per_worker_period {
					let elapsed = cycle_started.elapsed();
					if elapsed < period {
						tokio::time::sleep(period - elapsed).await;
					}
				}
			}
		}));
	}

	for worker in workers {
		worker.await?;
	}
	reporter.await?;

	let writes = stats.writes.lock().unwrap();
	let reads = stats.reads.lock().unwrap();
	let queries = stats.queries.lock().unwrap();
	let summary = SoakSummary {
		duration_secs: started.elapsed().as_secs(),
		write: summarize(&writes),
		read: summarize(&reads),
		query: summarize(&queries),
	};
	drop(writes);
	drop(reads);
	drop(queries);

	if let Some(dir) = &output_dir {
		fs::write(
			dir.join("summary.json"),
			serde_json::to_vec_pretty(&summary)?,
		)?;
	}
	println!("{}", serde_json::to_string_pretty(&summary)?);
	println!("SOAK_DONE duration_secs={}", summary.duration_secs);

	Ok(())
}
