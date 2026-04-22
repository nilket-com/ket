use async_trait::async_trait;
use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::io::{ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use wendelon::network::core::{HRFT_MAGIC, HrftFrame, read_frame, write_frame};
use wendelon::network::pool::BufPool;
use wendelon::network::server::{HrftService, RequestContext};
use wendelon::network::transport::{TransportProvider, TransportStream, TransportTier};
use wendelon::schema::{FrameHeader, MsgType, NodeId};
use wendelon::simulator::checkers::{
	CounterHistoryEntry, CounterOperation, MonotonicCounterChecker, RegisterHistoryEntry,
	RegisterLinearizabilityChecker, RegisterOperation,
};
use wendelon::simulator::history::{RegisterHistoryArtifact, RegisterHistoryBatch};
use wendelon::simulator::network::{
	InjectedSimulatedTransportFault, SimulatedTransportEventKind, SimulatedTransportFaultAction,
	SimulatedTransportFaultOp, SimulatedTransportNetwork, SimulatedTransportProvider,
};
use wendelon::storage::l2::{
	InjectedMockObjectStoreFault, MockObjectStore, MockObjectStoreConfig,
	MockObjectStoreFaultAction, MockObjectStoreOp, ObjectStore,
};

const DEFAULT_LADDER_SEED: u64 = 0x57A1_4D3E_2026_0421;
const SIM_SEED_DIR: &str = "tests/sim/seeds";

#[derive(Debug, Clone, Deserialize)]
struct FrozenSimulatorSeed {
	issue: u64,
	scenario: String,
	seed: u64,
	ops: u64,
	introduced_at: String,
	notes: Option<String>,
}

#[derive(Debug)]
struct SimFailure {
	scenario: String,
	seed: u64,
	iteration: u64,
	message: String,
}

impl SimFailure {
	fn new(
		scenario: impl Into<String>,
		seed: u64,
		iteration: u64,
		message: impl Into<String>,
	) -> Self {
		Self {
			scenario: scenario.into(),
			seed,
			iteration,
			message: message.into(),
		}
	}
}

impl std::fmt::Display for SimFailure {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"FAILED: seed={} scenario={} iteration={}\nReproduce: WENDELON_SIM_SEED={} cargo test --test simulator_ladder {} -- --ignored --nocapture\n{}",
			self.seed, self.scenario, self.iteration, self.seed, self.scenario, self.message
		)
	}
}

impl std::error::Error for SimFailure {}

#[derive(Clone, Copy, Debug)]
struct CoverageBudget {
	counter_ops: u64,
	register_ops: u64,
	object_store_ops: u64,
	transport_ops: u64,
}

fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

fn choose_index(rng: &mut StdRng, len: usize) -> usize {
	debug_assert!(len > 0);
	(rng.next_u64() as usize) % len
}

fn small_random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
	let mut out = vec![0_u8; len];
	rng.fill_bytes(&mut out);
	out
}

fn bucket_share(total: u64, numerator: u64, denominator: u64, min: u64, max: u64) -> u64 {
	if total == 0 {
		return 0;
	}
	let mut share = total.saturating_mul(numerator) / denominator;
	if share < min {
		share = min.min(total);
	}
	share.min(max.min(total))
}

fn coverage_budget(total_ops: u64) -> CoverageBudget {
	let mut remaining = total_ops;
	let transport_ops = bucket_share(total_ops, 1, 10_000, 8, 4_096).min(remaining);
	remaining = remaining.saturating_sub(transport_ops);
	let object_store_ops = bucket_share(total_ops, 1, 5_000, 16, 8_192).min(remaining);
	remaining = remaining.saturating_sub(object_store_ops);
	let register_ops = bucket_share(total_ops, 1, 100, 32, 100_000).min(remaining);
	remaining = remaining.saturating_sub(register_ops);
	CoverageBudget {
		counter_ops: remaining,
		register_ops,
		object_store_ops,
		transport_ops,
	}
}

fn ladder_ops_from_env() -> u64 {
	std::env::var("WENDELON_SIM_OPS")
		.ok()
		.and_then(|raw| raw.parse::<u64>().ok())
		.unwrap_or(10_000)
}

fn ladder_seed_from_env() -> u64 {
	std::env::var("WENDELON_SIM_SEED")
		.ok()
		.and_then(|raw| raw.parse::<u64>().ok())
		.unwrap_or(DEFAULT_LADDER_SEED)
}

fn register_history_dir_from_env() -> Option<PathBuf> {
	std::env::var("WENDELON_SIM_HISTORY_DIR")
		.ok()
		.map(|raw| raw.trim().to_string())
		.filter(|raw| !raw.is_empty())
		.map(PathBuf::from)
}

fn is_iso_date(raw: &str) -> bool {
	raw.len() == 10
		&& raw.as_bytes()[4] == b'-'
		&& raw.as_bytes()[7] == b'-'
		&& raw
			.bytes()
			.enumerate()
			.all(|(idx, byte)| matches!(idx, 4 | 7) || byte.is_ascii_digit())
}

fn supported_scenario(name: &str) -> bool {
	matches!(name, "coverage_ladder")
}

fn frozen_seed_paths() -> Result<Vec<PathBuf>> {
	let mut out = Vec::new();
	let dir = Path::new(SIM_SEED_DIR);
	if !dir.exists() {
		return Ok(out);
	}
	for entry in fs::read_dir(dir)? {
		let path = entry?.path();
		if path.extension().and_then(|ext| ext.to_str()) == Some("seed") {
			out.push(path);
		}
	}
	out.sort();
	Ok(out)
}

fn load_frozen_seed(path: &Path) -> Result<FrozenSimulatorSeed> {
	let text = fs::read_to_string(path)?;
	toml::from_str(&text).map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err))
}

fn validate_frozen_seed_file(path: &Path, seed: &FrozenSimulatorSeed) {
	let stem = path
		.file_stem()
		.and_then(|stem| stem.to_str())
		.expect("seed file stem must be valid utf-8");
	let issue_from_name = stem
		.parse::<u64>()
		.expect("seed filename must be the issue number");
	assert_eq!(
		issue_from_name,
		seed.issue,
		"seed file {} must match issue field {}",
		path.display(),
		seed.issue
	);
	assert!(
		supported_scenario(&seed.scenario),
		"seed file {} uses unsupported scenario {}",
		path.display(),
		seed.scenario
	);
	assert!(
		seed.ops > 0,
		"seed file {} must request ops > 0",
		path.display()
	);
	assert!(
		is_iso_date(&seed.introduced_at),
		"seed file {} must use YYYY-MM-DD introduced_at",
		path.display()
	);
	if let Some(notes) = &seed.notes {
		assert!(
			!notes.trim().is_empty(),
			"seed file {} notes must be non-empty when present",
			path.display()
		);
	}
}

fn export_register_history_artifact(
	dir: &Path,
	scenario: &str,
	seed: u64,
	ops: u64,
	batches: &[RegisterHistoryBatch],
) -> Result<PathBuf> {
	let artifact = RegisterHistoryArtifact::new(scenario, seed, ops, batches.to_vec());
	let paths = artifact.write_to_dir(dir)?;
	Ok(paths.json)
}

fn run_counter_scenario(seed: u64, ops: u64) -> std::result::Result<u64, SimFailure> {
	if ops == 0 {
		return Ok(0);
	}
	let scenario = "coverage_ladder";
	let mut rng = StdRng::seed_from_u64(seed);
	let mut clock = 0_u64;
	let mut executed = 0_u64;
	let chunk_size = 8_192_u64;

	while executed < ops {
		let chunk_ops = (ops - executed).min(chunk_size);
		let mut values = [0_u64; 32];
		let mut history = Vec::with_capacity(chunk_ops as usize);
		for offset in 0..chunk_ops {
			let key_index = choose_index(&mut rng, values.len());
			let key = format!("tenant/{:02x}", key_index);
			let process = (rng.next_u64() % 8) + 1;
			let started_at = clock;
			let completed_at = clock + 1;
			clock += 2;
			if rng.next_u64() % 4 == 0 {
				history.push(CounterHistoryEntry {
					process,
					key,
					started_at,
					completed_at,
					op: CounterOperation::Read {
						observed: values[key_index],
					},
				});
			} else {
				let by = (rng.next_u64() % 3) + 1;
				values[key_index] = values[key_index].saturating_add(by);
				history.push(CounterHistoryEntry {
					process,
					key,
					started_at,
					completed_at,
					op: CounterOperation::Add { by },
				});
			}
			let iteration = executed + offset + 1;
			if history.len() == chunk_size as usize || iteration == ops {
				MonotonicCounterChecker::check(&history).map_err(|err| {
					SimFailure::new(
						scenario,
						seed,
						iteration,
						format!("monotonic counter checker rejected chunk: {}", err.message),
					)
				})?;
			}
		}
		executed += chunk_ops;
	}

	Ok(executed)
}

fn run_register_scenario(
	seed: u64,
	ops: u64,
	history_dir: Option<&Path>,
) -> std::result::Result<u64, SimFailure> {
	if ops == 0 {
		return Ok(0);
	}
	let scenario = "coverage_ladder";
	let mut rng = StdRng::seed_from_u64(seed);
	let mut clock = 10_000_u64;
	let mut executed = 0_u64;
	let mut batch_index = 0_u64;
	let mut history_batches = Vec::new();

	while executed < ops {
		let remaining = ops - executed;
		let batch_len = remaining.min(6).max(1);
		let mut states: BTreeMap<String, Option<i64>> = BTreeMap::from([
			("tenant/register/0".to_string(), None),
			("tenant/register/1".to_string(), None),
		]);
		let mut history = Vec::with_capacity(batch_len as usize);
		for offset in 0..batch_len {
			let key = if rng.next_u64() % 2 == 0 {
				"tenant/register/0".to_string()
			} else {
				"tenant/register/1".to_string()
			};
			let current = states.get(&key).copied().flatten();
			let process = (rng.next_u64() % 3) + 1;
			let started_at = clock;
			let completed_at = clock + 1;
			clock += 2;
			let op = if current.is_none() || rng.next_u64() % 10 < 6 {
				let value = (rng.next_u64() % 10_000) as i64;
				states.insert(key.clone(), Some(value));
				RegisterOperation::Write { value }
			} else {
				RegisterOperation::Read { observed: current }
			};
			history.push(RegisterHistoryEntry {
				process,
				key,
				started_at,
				completed_at,
				op,
			});
			let iteration = executed + offset + 1;
			if iteration == ops || history.len() == batch_len as usize {
				if history_dir.is_some() {
					history_batches.push(RegisterHistoryBatch::from_entries(
						batch_index,
						iteration,
						&history,
					));
				}
				RegisterLinearizabilityChecker::check(&history).map_err(|err| {
					let artifact_note = history_dir.and_then(|dir| {
						export_register_history_artifact(dir, scenario, seed, ops, &history_batches)
							.ok()
							.map(|path| format!(" history_json={}", path.display()))
					});
					SimFailure::new(
						scenario,
						seed,
						iteration,
						format!(
							"register checker rejected batch: {}{}",
							err.message,
							artifact_note.unwrap_or_default()
						),
					)
				})?;
				batch_index += 1;
			}
		}
		executed += batch_len;
	}

	if let Some(dir) = history_dir {
		let path = export_register_history_artifact(dir, scenario, seed, ops, &history_batches)
			.map_err(|err| {
				SimFailure::new(
					scenario,
					seed,
					executed.max(1),
					format!("failed to write register history artifact: {err}"),
				)
			})?;
		println!(
			"SIMULATOR_ELLE_HISTORY scenario={} seed={} json={}",
			scenario,
			seed,
			path.display()
		);
	}

	Ok(executed)
}

async fn run_object_store_scenario(seed: u64, ops: u64) -> std::result::Result<u64, SimFailure> {
	if ops == 0 {
		return Ok(0);
	}
	let scenario = "coverage_ladder";
	let mut executed = 0_u64;

	let delayed_store = MockObjectStore::new(MockObjectStoreConfig {
		list_visibility_delay: 1,
		delete_visibility_delay: 1,
		..Default::default()
	});
	let delayed_uri = delayed_store
		.put("delayed/item.wnd", b"abc")
		.await
		.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
	executed += 1;
	if executed < ops {
		let first = delayed_store
			.list("delayed/")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
		if !first.is_empty() {
			return Err(SimFailure::new(
				scenario,
				seed,
				executed,
				"delayed put became visible too early",
			));
		}
	}
	if executed < ops {
		let second = delayed_store
			.list("delayed/")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
		if second.len() != 1 {
			return Err(SimFailure::new(
				scenario,
				seed,
				executed,
				format!(
					"expected delayed put to appear on second list, got {}",
					second.len()
				),
			));
		}
	}
	if executed < ops {
		delayed_store
			.delete(&delayed_uri)
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
	}
	if executed < ops {
		let third = delayed_store
			.list("delayed/")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
		if third.len() != 1 {
			return Err(SimFailure::new(
				scenario,
				seed,
				executed,
				format!(
					"expected deleted object to remain visible for one list epoch, got {}",
					third.len()
				),
			));
		}
	}
	if executed < ops {
		let fourth = delayed_store
			.list("delayed/")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
		if !fourth.is_empty() {
			return Err(SimFailure::new(
				scenario,
				seed,
				executed,
				"deleted object remained visible after delay window",
			));
		}
	}

	let store = MockObjectStore::default();
	let mut rng = StdRng::seed_from_u64(seed);
	let mut live: Vec<(String, String, Bytes)> = Vec::new();
	if executed < ops {
		let uri = store
			.put("faulty/item.wnd", b"fault")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
		store.inject(
			InjectedMockObjectStoreFault::on(
				MockObjectStoreOp::GetBytes,
				MockObjectStoreFaultAction::Error {
					kind: ErrorKind::TimedOut,
					message: "simulated timeout".to_string(),
				},
			)
			.for_key("faulty/item.wnd"),
		);
		if executed < ops {
			let err = store
				.get(&uri)
				.await
				.expect_err("one-shot get_bytes fault must fire once");
			executed += 1;
			if err.kind() != ErrorKind::TimedOut {
				return Err(SimFailure::new(
					scenario,
					seed,
					executed,
					format!("expected TimedOut fault, got {:?}", err.kind()),
				));
			}
		}
		if executed < ops {
			let bytes = store
				.get_bytes(&uri)
				.await
				.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
			executed += 1;
			if bytes.as_ref() != b"fault" {
				return Err(SimFailure::new(
					scenario,
					seed,
					executed,
					"one-shot get_bytes fault did not clear",
				));
			}
		}
		live.push((
			"faulty/item.wnd".to_string(),
			uri,
			Bytes::from_static(b"fault"),
		));
	}

	while executed < ops {
		let choice = (rng.next_u64() % 5) as u8;
		if live.is_empty() || choice == 0 {
			let key = format!(
				"tenant/{:02x}/slab_{:016x}.wnd",
				(rng.next_u64() % 4) as u8,
				rng.next_u64()
			);
			let payload_len = ((rng.next_u64() % 24) + 1) as usize;
			let payload = Bytes::from(small_random_bytes(&mut rng, payload_len));
			let uri = store
				.put(&key, &payload)
				.await
				.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
			live.push((key, uri, payload));
			executed += 1;
			continue;
		}

		let idx = choose_index(&mut rng, live.len());
		let (key, uri, payload) = live[idx].clone();
		match choice {
			1 => {
				let meta = store.head(&uri).await.map_err(|err| {
					SimFailure::new(scenario, seed, executed + 1, err.to_string())
				})?;
				if meta.key != key || meta.size_bytes != payload.len() as u64 {
					return Err(SimFailure::new(
						scenario,
						seed,
						executed + 1,
						"head metadata did not match live object state",
					));
				}
			}
			2 => {
				let bytes = store.get_bytes(&uri).await.map_err(|err| {
					SimFailure::new(scenario, seed, executed + 1, err.to_string())
				})?;
				if bytes != payload {
					return Err(SimFailure::new(
						scenario,
						seed,
						executed + 1,
						"get_bytes payload mismatch",
					));
				}
			}
			3 => {
				let start = if payload.is_empty() {
					0
				} else {
					(rng.next_u64() as usize) % payload.len()
				};
				let end = start + ((rng.next_u64() as usize) % (payload.len() - start + 1));
				let range = store
					.get_range(&uri, start as u64..end as u64)
					.await
					.map_err(|err| {
						SimFailure::new(scenario, seed, executed + 1, err.to_string())
					})?;
				if range != payload.slice(start..end) {
					return Err(SimFailure::new(
						scenario,
						seed,
						executed + 1,
						"get_range payload mismatch",
					));
				}
			}
			4 => {
				let prefix = key.rsplit_once('/').map(|(left, _)| left).unwrap_or("");
				let listed = store.list(prefix).await.map_err(|err| {
					SimFailure::new(scenario, seed, executed + 1, err.to_string())
				})?;
				if !listed.iter().any(|entry| entry.key == key) {
					return Err(SimFailure::new(
						scenario,
						seed,
						executed + 1,
						format!("list with prefix {prefix} did not include expected key {key}"),
					));
				}
			}
			_ => {
				store.delete(&uri).await.map_err(|err| {
					SimFailure::new(scenario, seed, executed + 1, err.to_string())
				})?;
				live.swap_remove(idx);
			}
		}
		executed += 1;
	}

	Ok(executed)
}

struct EchoService;

#[async_trait]
impl HrftService for EchoService {
	async fn handle_frame(
		&self,
		_context: RequestContext,
		frame: HrftFrame,
	) -> Result<Option<HrftFrame>> {
		Ok(Some(frame))
	}
}

async fn run_transport_scenario(seed: u64, ops: u64) -> std::result::Result<u64, SimFailure> {
	if ops == 0 {
		return Ok(0);
	}
	let scenario = "coverage_ladder";
	let pool = BufPool::new(32, 64 * 1024);
	let network = Arc::new(SimulatedTransportNetwork::default());
	let provider = SimulatedTransportProvider::new(
		node(1),
		network.clone(),
		pool.clone(),
		TransportTier::QuicIoUring,
	);
	provider
		.publish_local(node(2), "sim://node-2", Arc::new(EchoService))
		.map_err(|err| SimFailure::new(scenario, seed, 0, err.to_string()))?;
	if ops > 4 {
		network.inject(
			InjectedSimulatedTransportFault::on(
				SimulatedTransportFaultOp::Connect,
				SimulatedTransportFaultAction::Delay(Duration::from_millis(1)),
			)
			.from(node(1))
			.to(node(2))
			.after_matches(2),
		);
	}
	let mut saw_delay_fault = false;

	for iteration in 0..ops {
		let conn = provider
			.connect(node(2), "sim://node-2")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, iteration + 1, err.to_string()))?;
		let mut stream: Box<dyn TransportStream> = conn
			.open_bi()
			.await
			.map_err(|err| SimFailure::new(scenario, seed, iteration + 1, err.to_string()))?;
		let payload = iteration.to_le_bytes();
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::STATUS_REQ.0,
			flags: 0,
			stream_id: iteration as u32,
			meta_len: payload.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, &payload, None, None)
			.await
			.map_err(|err| SimFailure::new(scenario, seed, iteration + 1, err.to_string()))?;
		stream
			.finish()
			.await
			.map_err(|err| SimFailure::new(scenario, seed, iteration + 1, err.to_string()))?;
		let frame = read_frame(&mut *stream, &pool)
			.await
			.map_err(|err| SimFailure::new(scenario, seed, iteration + 1, err.to_string()))?;
		if frame.header.msg_type != MsgType::STATUS_REQ.0 || frame.meta.as_ref() != payload {
			return Err(SimFailure::new(
				scenario,
				seed,
				iteration + 1,
				"transport echo frame was corrupted",
			));
		}
		if iteration % 256 == 255 {
			saw_delay_fault |= network.events().iter().any(|event| {
				matches!(
					event.kind,
					SimulatedTransportEventKind::Fault {
						op: SimulatedTransportFaultOp::Connect,
						action: SimulatedTransportFaultAction::Delay(_),
					}
				)
			});
			network.clear_events();
		}
	}

	saw_delay_fault |= network.events().iter().any(|event| {
		matches!(
			event.kind,
			SimulatedTransportEventKind::Fault {
				op: SimulatedTransportFaultOp::Connect,
				action: SimulatedTransportFaultAction::Delay(_),
			}
		)
	});
	if ops > 4 && !saw_delay_fault {
		return Err(SimFailure::new(
			scenario,
			seed,
			ops,
			"expected transport delay fault was never observed",
		));
	}

	Ok(ops)
}

async fn run_coverage_ladder_with_history_export(
	seed: u64,
	ops: u64,
	history_dir: Option<&Path>,
) -> std::result::Result<u64, SimFailure> {
	let budget = coverage_budget(ops);
	let mut executed = 0_u64;
	executed += run_counter_scenario(seed ^ 0xC0A1_C0A1, budget.counter_ops)?;
	executed += run_register_scenario(seed ^ 0xE11E_0001, budget.register_ops, history_dir)?;
	executed += run_object_store_scenario(seed ^ 0x0B5E_C7E0, budget.object_store_ops).await?;
	executed += run_transport_scenario(seed ^ 0x7A4A_F001, budget.transport_ops).await?;
	assert_eq!(
		executed, ops,
		"coverage ladder budget accounting drifted: expected {ops}, executed {executed}"
	);
	Ok(executed)
}

async fn run_coverage_ladder(seed: u64, ops: u64) -> std::result::Result<u64, SimFailure> {
	let history_dir = register_history_dir_from_env();
	run_coverage_ladder_with_history_export(seed, ops, history_dir.as_deref()).await
}

async fn run_named_scenario(
	name: &str,
	seed: u64,
	ops: u64,
) -> std::result::Result<u64, SimFailure> {
	match name {
		"coverage_ladder" => run_coverage_ladder(seed, ops).await,
		other => Err(SimFailure::new(
			other,
			seed,
			0,
			format!("unsupported simulator scenario {other}"),
		)),
	}
}

#[test]
fn simulator_seed_registry_files_are_well_formed() {
	for path in frozen_seed_paths().expect("seed directory must be readable") {
		let seed = load_frozen_seed(&path).expect("seed file must parse");
		validate_frozen_seed_file(&path, &seed);
	}
}

#[tokio::test(flavor = "current_thread")]
async fn simulator_frozen_seed_registry_replays() {
	let paths = frozen_seed_paths().expect("seed directory must be readable");
	for path in paths {
		let seed = load_frozen_seed(&path).expect("seed file must parse");
		validate_frozen_seed_file(&path, &seed);
		run_named_scenario(&seed.scenario, seed.seed, seed.ops)
			.await
			.unwrap_or_else(|err| panic!("{err}"));
	}
}

fn tempdir(tag: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-simulator-ladder-{tag}-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

#[tokio::test(flavor = "current_thread")]
async fn simulator_register_history_export_surface_writes_json_artifact() {
	let dir = tempdir("register-history");
	let seed = 7_u64;
	let ops = 128_u64;
	let executed = run_coverage_ladder_with_history_export(seed, ops, Some(&dir))
		.await
		.unwrap();
	assert_eq!(executed, ops);

	let mut artifacts = fs::read_dir(&dir)
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
		.collect::<Vec<_>>();
	artifacts.sort();
	assert_eq!(artifacts.len(), 1, "expected exactly one history artifact");

	let body = fs::read_to_string(&artifacts[0]).unwrap();
	let artifact: RegisterHistoryArtifact = serde_json::from_str(&body).unwrap();
	assert_eq!(artifact.scenario, "coverage_ladder");
	assert_eq!(artifact.seed, seed ^ 0xE11E_0001);
	assert_eq!(artifact.requested_ops, coverage_budget(ops).register_ops);
	assert!(!artifact.batches.is_empty());
	assert!(!artifact.events.is_empty());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "run via ci/check_simulator_ladder.sh"]
async fn coverage_ladder() {
	let seed = ladder_seed_from_env();
	let ops = ladder_ops_from_env();
	let executed = run_coverage_ladder(seed, ops)
		.await
		.unwrap_or_else(|err| panic!("{err}"));
	println!(
		"SIMULATOR_OK seed={} scenario=coverage_ladder ops={} executed={}",
		seed, ops, executed
	);
}
