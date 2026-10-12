//! Full Control retention: configuration, startup history check, refusal of
//! every snapshot and purge path, and status reporting.
//!
//! Every fixture is an isolated temporary directory. The Raft fixtures run
//! the production openraft configuration for the mode under test; no policy
//! is replaced or lowered. Byte-identity captures are taken only after the
//! writes that precede them have been applied, and the single-node fixture
//! attaches no checkpoint observer, so nothing writes to the directory in the
//! background.

use crate::consensus::retention::{
	ControlRetention, HistoryCheck, RetentionConfigError, RetentionGuard, RetentionStartError,
	RetentionStatus, RetentionViolation,
};
use crate::consensus::{
	DurableBootstrapMode, MigrationLedgerEntry, MigrationState, SeedMigration, WendelonLogStore,
	WendelonRaft, WendelonRaftInternalServer, WendelonRaftNetworkFactory, WendelonRequest,
	WendelonStateMachine,
};
use crate::metrics::observability::{DrainState, ReadinessReporter, ReadinessSnapshot};
use crate::network::core::{HRFT_MAGIC, HrftFrame};
use crate::network::pool::{BufPool, PooledBuf};
use crate::network::router::RaftInternalServer;
use crate::network::server::{HrftService, RequestContext};
use crate::network::tls::ActorIdentity;
use crate::node::NoDialTransportProvider;
use crate::schema::{FrameHeader, HybridTimestamp, MsgType, NodeId};
use openraft::storage::{RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{BasicNode, Config, LogId, SnapshotMeta, SnapshotPolicy};
use proc_macro2::{Delimiter, TokenStream, TokenTree};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Bound on every wait for openraft progress. A wait that runs out fails.
const BOUND: Duration = Duration::from_secs(60);
/// Small segments force WAL rollover; this is a WAL layout knob, not a
/// retention policy.
const SEGMENT_BYTES: u64 = 64 * 4096;
/// Beyond openraft's default `since_last:5000` snapshot threshold plus its
/// default 1000 retained in-snapshot entries.
const PAST_DEFAULT_THRESHOLD: u64 = 6_100;

fn fresh_dir(tag: &str) -> crate::test_support::TestDir {
	crate::test_support::TestDir::new(&format!("wendelon-retention-{tag}"))
}

/// Every file under `dir`, by relative path, with its exact bytes.
fn dir_bytes(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
	fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
		for entry in std::fs::read_dir(dir).unwrap() {
			let path = entry.unwrap().path();
			if path.is_dir() {
				walk(root, &path, out);
			} else {
				out.insert(
					path.strip_prefix(root).unwrap().to_path_buf(),
					std::fs::read(&path).unwrap(),
				);
			}
		}
	}
	let mut out = BTreeMap::new();
	walk(dir, dir, &mut out);
	out
}

fn segment_files(dir: &Path) -> Vec<PathBuf> {
	let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.filter(|path| {
			path.file_name()
				.and_then(|name| name.to_str())
				.is_some_and(|name| name.starts_with("control.wal") && name != "control.wal")
		})
		.collect();
	out.sort();
	out
}

fn receive_tmp(dir: &Path) -> PathBuf {
	dir.join(".snapshot.wnd.receive.tmp")
}

fn start_error(err: &std::io::Error) -> Option<RetentionStartError> {
	err.get_ref()
		.and_then(|inner| inner.downcast_ref::<RetentionStartError>())
		.copied()
}

/// The semantic Raft position: what a refused request must not change.
#[derive(Debug, PartialEq, Eq)]
struct Position {
	term: u64,
	last_log_index: Option<u64>,
	last_applied: Option<LogId<u32>>,
	snapshot: Option<LogId<u32>>,
	purged: Option<LogId<u32>>,
	leader: Option<u32>,
	running: bool,
}

fn position(raft: &WendelonRaft) -> Position {
	let m = raft.metrics().borrow().clone();
	Position {
		term: m.current_term,
		last_log_index: m.last_log_index,
		last_applied: m.last_applied,
		snapshot: m.snapshot,
		purged: m.purged,
		leader: m.current_leader,
		running: m.running_state.is_ok(),
	}
}

/// A single-node Control Ring on the production openraft configuration for
/// `mode`, over a real WAL store and recovered state machine.
struct Single {
	raft: WendelonRaft,
	store: WendelonLogStore,
	guard: Arc<RetentionGuard>,
	/// A handle on the same state machine Raft applies to, for observation.
	state_machine: WendelonStateMachine,
}

async fn start(dir: &Path, mode: ControlRetention) -> std::io::Result<Single> {
	let guard = RetentionGuard::new(mode);
	let store =
		WendelonLogStore::open_for_retention_test(dir, SEGMENT_BYTES, Some(guard.clone())).await?;
	let fresh = store.bootstrap_mode().await == DurableBootstrapMode::FreshBootstrap;
	let state_machine = store.recovered_state_machine().await?;
	let network = WendelonRaftNetworkFactory::new(
		Arc::new(NoDialTransportProvider),
		BufPool::new(8, 1 << 20),
	);
	let config = Arc::new(mode.openraft_config("wendelon").validate().unwrap());
	let observed = state_machine.clone();
	let raft = WendelonRaft::new(1, config, network, store.clone(), state_machine)
		.await
		.unwrap();
	if fresh {
		raft.initialize(BTreeMap::from([(
			1u32,
			BasicNode {
				addr: "127.0.0.1:0".into(),
			},
		)]))
		.await
		.unwrap();
	}
	raft.wait(Some(BOUND))
		.current_leader(1, "single node leads")
		.await
		.unwrap();
	Ok(Single {
		raft,
		store,
		guard,
		state_machine: observed,
	})
}

/// Commits `count` entries. The payload is a deliberately undecodable schema
/// update: it is stored and replicated like any entry and applies as a
/// deterministic error response with no state change.
async fn commit(raft: &WendelonRaft, count: u64) {
	let mut written = 0;
	while written < count {
		let batch = (count - written).min(64);
		let mut set = tokio::task::JoinSet::new();
		for _ in 0..batch {
			let raft = raft.clone();
			set.spawn(async move {
				raft.client_write(WendelonRequest::SchemaUpdate(vec![0xff]))
					.await
					.map(|_| ())
			});
		}
		while let Some(result) = set.join_next().await {
			result.unwrap().unwrap();
		}
		written += batch;
	}
	let last = raft.metrics().borrow().last_log_index;
	raft.wait(Some(BOUND))
		.applied_index_at_least(last, "every committed entry applied")
		.await
		.unwrap();
}

async fn stored_indexes(store: &mut WendelonLogStore) -> Vec<u64> {
	let state = store.get_log_state().await.unwrap();
	let Some(last) = state.last_log_id else {
		return Vec::new();
	};
	let first = state.last_purged_log_id.map_or(0, |id| id.index + 1);
	store
		.try_get_log_entries(first..=last.index)
		.await
		.unwrap()
		.into_iter()
		.map(|entry| entry.log_id.index)
		.collect()
}

/// Every stored entry from 0 through `last`, canonically serialized: the log
/// id and payload exactly as retained.
async fn entry_bytes(store: &mut WendelonLogStore, last: u64) -> Vec<Vec<u8>> {
	store
		.try_get_log_entries(0..=last)
		.await
		.unwrap()
		.iter()
		.map(|entry| postcard::to_allocvec(entry).unwrap())
		.collect()
}

/// A deterministic, successful, state-changing command.
fn seed(migration_id: u64) -> WendelonRequest {
	let node = NodeId {
		id: 1,
		_reserved: 0,
	};
	WendelonRequest::SeedMigration(SeedMigration {
		entry: MigrationLedgerEntry {
			migration_id,
			range_start: migration_id * 10,
			range_end: migration_id * 10 + 5,
			from_epoch: 1,
			to_epoch: 2,
			logical_owner: node,
			upstream_migration_id: None,
			source_replicas: vec![node],
			migration_hlc_floor: HybridTimestamp {
				physical_ms: 1_000 + migration_id,
				logical: 0,
				node_id: 1,
			},
			state: MigrationState::Pending,
			completed_by: None,
			completed_at_hlc: None,
		},
	})
}

/// A history with several segments, written under `Compacting` (which purges
/// nothing at this size) and shut down cleanly.
async fn written_history(tag: &str, entries: u64) -> crate::test_support::TestDir {
	let dir = fresh_dir(tag);
	let node = start(&dir, ControlRetention::Compacting).await.unwrap();
	commit(&node.raft, entries).await;
	node.raft.shutdown().await.unwrap();
	drop(node);
	assert!(
		segment_files(&dir).len() >= 3,
		"fixture needs several segments"
	);
	dir
}

#[test]
fn default_retention_is_todays_openraft_config() {
	let today = Config {
		cluster_name: "wendelon".into(),
		..Default::default()
	}
	.validate()
	.unwrap();
	let compacting = ControlRetention::Compacting
		.openraft_config("wendelon")
		.validate()
		.unwrap();
	assert_eq!(format!("{compacting:?}"), format!("{today:?}"));
	assert_eq!(ControlRetention::default(), ControlRetention::Compacting);

	let root = fresh_dir("config-parse");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let load = |body: String| {
		let path = root.join("wendelon.toml");
		std::fs::write(&path, body).unwrap();
		crate::config::load_node_bootstrap_config(&path)
	};
	let example = crate::config::render_example_config();
	let commented = "# control_retention = \"compacting\"";
	assert!(example.contains(commented));
	assert_eq!(
		load(example.clone()).unwrap().control_retention,
		ControlRetention::Compacting
	);
	assert_eq!(
		load(example.replace(commented, "control_retention = \"compacting\""))
			.unwrap()
			.control_retention,
		ControlRetention::Compacting
	);
	assert_eq!(
		load(example.replace(commented, "control_retention = \"full\""))
			.unwrap()
			.control_retention,
		ControlRetention::Full
	);
	let err = load(example.replace(commented, "control_retention = \"bogus\"")).unwrap_err();
	assert_eq!(
		err.get_ref()
			.and_then(|inner| inner.downcast_ref::<RetentionConfigError>()),
		Some(&RetentionConfigError::UnknownMode)
	);
	assert_eq!(
		crate::node::NodeBootstrapConfig::dev(1, root.to_path_buf(), "127.0.0.1:0".into())
			.control_retention,
		ControlRetention::Compacting
	);
}

#[test]
fn full_retention_sets_only_never_and_max_keep() {
	let full = ControlRetention::Full
		.openraft_config("wendelon")
		.validate()
		.unwrap();
	assert!(matches!(full.snapshot_policy, SnapshotPolicy::Never));
	assert_eq!(full.max_in_snapshot_log_to_keep, u64::MAX);
	let compacting = ControlRetention::Compacting
		.openraft_config("wendelon")
		.validate()
		.unwrap();
	assert!(!matches!(compacting.snapshot_policy, SnapshotPolicy::Never));
	assert_ne!(compacting.max_in_snapshot_log_to_keep, u64::MAX);
	let reset = Config {
		snapshot_policy: compacting.snapshot_policy.clone(),
		max_in_snapshot_log_to_keep: compacting.max_in_snapshot_log_to_keep,
		..full
	};
	assert_eq!(format!("{reset:?}"), format!("{compacting:?}"));
}

#[tokio::test]
async fn full_startup_refuses_incomplete_history() {
	// Each fixture: (name, directory, expected Full outcome).
	enum Expect {
		Refused(RetentionStartError),
		/// Replay's or the resolver's own refusal: its existing kind and
		/// diagnostic, with no retention error.
		ExistingError(std::io::ErrorKind, &'static str),
		Opens(HistoryCheck),
	}
	let base = written_history("startup-base", 400).await;
	let segments = segment_files(&base);
	let copy = |tag: &str| {
		let dir = fresh_dir(tag);
		for (path, bytes) in dir_bytes(&base) {
			std::fs::write(dir.join(path), bytes).unwrap();
		}
		dir
	};
	let segment_start = |path: &Path| -> u64 {
		let name = path.file_name().unwrap().to_str().unwrap();
		name.chars()
			.filter(char::is_ascii_digit)
			.collect::<String>()
			.parse()
			.unwrap()
	};
	let mut fixtures: Vec<(&str, crate::test_support::TestDir, Expect)> = Vec::new();

	let dir = copy("a1-snapshot");
	std::fs::write(dir.join("snapshot.wnd"), b"").unwrap();
	fixtures.push((
		"A1 snapshot artifact (empty file counts)",
		dir,
		Expect::Refused(RetentionStartError::SnapshotArtifactPresent),
	));

	let dir = copy("a2-receive");
	std::fs::write(receive_tmp(&dir), b"partial").unwrap();
	fixtures.push((
		"A2 receive temp",
		dir,
		Expect::Refused(RetentionStartError::SnapshotReceivePresent),
	));

	let dir = copy("a3-legacy");
	std::fs::write(dir.join("control.wal"), b"").unwrap();
	fixtures.push((
		"A3 legacy monolithic WAL",
		dir,
		Expect::Refused(RetentionStartError::LegacySegment),
	));

	let dir = copy("a4-prefix");
	let second = segment_start(&segments[1]);
	std::fs::remove_file(dir.join(segments[0].file_name().unwrap())).unwrap();
	fixtures.push((
		"A4 lowest segment above 0",
		dir,
		Expect::Refused(RetentionStartError::MissingPrefix { first: second }),
	));

	// Segment 0 holding a later segment's entries passes replay's own check
	// (a start of 0 is exempt) and is caught after replay.
	let dir = copy("b2-prefix");
	std::fs::remove_file(dir.join(segments[0].file_name().unwrap())).unwrap();
	std::fs::rename(
		dir.join(segments[1].file_name().unwrap()),
		dir.join(segments[0].file_name().unwrap()),
	)
	.unwrap();
	fixtures.push((
		"B2 first stored entry above 0",
		dir,
		Expect::Refused(RetentionStartError::MissingPrefix { first: second }),
	));

	let dir = copy("b3-gap");
	let third = segment_start(&segments[2]);
	std::fs::remove_file(dir.join(segments[1].file_name().unwrap())).unwrap();
	fixtures.push((
		"B3 gap",
		dir,
		Expect::Refused(RetentionStartError::Gap { after: second - 1 }),
	));
	assert!(third > second);

	// Replay's own refusals keep their existing errors; no typed claim.
	let dir = copy("existing-start-mismatch");
	std::fs::rename(
		dir.join(segments[1].file_name().unwrap()),
		WendelonLogStore::segment_path_for_retention_test(&dir, second + 1),
	)
	.unwrap();
	fixtures.push((
		"existing: segment start mismatch",
		dir,
		Expect::ExistingError(
			std::io::ErrorKind::InvalidData,
			"does not match first log entry",
		),
	));
	let dir = copy("existing-duplicate");
	std::fs::copy(
		dir.join(segments[1].file_name().unwrap()),
		dir.join(segments[0].file_name().unwrap()),
	)
	.unwrap();
	fixtures.push((
		"existing: duplicate index",
		dir,
		Expect::ExistingError(std::io::ErrorKind::InvalidData, "duplicate log index"),
	));

	// No stored entry and a positive committed checkpoint: the existing
	// resolver refuses before Phase B, with its own diagnostic.
	let dir = fresh_dir("checkpoint-only");
	std::fs::write(
		WendelonLogStore::segment_path_for_retention_test(&dir, 0),
		b"",
	)
	.unwrap();
	{
		let mut store = WendelonLogStore::open_for_retention_test(&dir, SEGMENT_BYTES, None)
			.await
			.unwrap();
		store
			.save_committed(Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 5)))
			.await
			.unwrap();
	}
	fixtures.push((
		"existing: checkpoint without entries",
		dir,
		Expect::ExistingError(
			std::io::ErrorKind::InvalidData,
			"recovery checkpoint commit_index 5 is not present in the retained WAL tail",
		),
	));

	fixtures.push((
		"fresh",
		fresh_dir("fresh"),
		Expect::Opens(HistoryCheck::Fresh),
	));

	let dir = fresh_dir("empty-segment");
	std::fs::write(
		WendelonLogStore::segment_path_for_retention_test(&dir, 0),
		b"",
	)
	.unwrap();
	fixtures.push((
		"empty segment 0",
		dir,
		Expect::Opens(HistoryCheck::EmptyLog),
	));

	let dir = fresh_dir("vote-only");
	{
		let mut store = WendelonLogStore::open_for_retention_test(&dir, SEGMENT_BYTES, None)
			.await
			.unwrap();
		store.save_vote(&openraft::Vote::new(1, 1)).await.unwrap();
	}
	fixtures.push(("vote only", dir, Expect::Opens(HistoryCheck::EmptyLog)));

	let complete = copy("complete");
	let stored_last = {
		let mut store = WendelonLogStore::open_for_retention_test(&complete, SEGMENT_BYTES, None)
			.await
			.unwrap();
		*stored_indexes(&mut store).await.last().unwrap()
	};
	fixtures.push((
		"complete history",
		complete,
		Expect::Opens(HistoryCheck::Complete { stored_last }),
	));

	for (name, dir, expect) in fixtures {
		let before = dir_bytes(&dir);
		let guard = RetentionGuard::new(ControlRetention::Full);
		let opened =
			WendelonLogStore::open_for_retention_test(&dir, SEGMENT_BYTES, Some(guard.clone()))
				.await;
		match (&expect, &opened) {
			(Expect::Refused(want), Err(err)) => {
				assert_eq!(start_error(err), Some(*want), "{name}: {err}");
			}
			(Expect::ExistingError(kind, diagnostic), Err(err)) => {
				assert_eq!(start_error(err), None, "{name}: {err}");
				assert_eq!(err.kind(), *kind, "{name}: {err}");
				assert!(err.to_string().contains(diagnostic), "{name}: {err}");
			}
			(Expect::Opens(want), Ok(_)) => {
				assert_eq!(guard.status().history, *want, "{name}");
			}
			(_, Err(err)) => panic!("{name}: unexpected refusal {err}"),
			(_, Ok(_)) => panic!("{name}: opened but should have been refused"),
		}
		if !matches!(expect, Expect::Opens(_)) {
			assert_eq!(
				dir_bytes(&dir),
				before,
				"{name}: a refused open changed the directory"
			);
			assert_eq!(guard.status().violation, None, "{name}");
			// Control, after the byte-identity check: Compacting keeps its
			// baseline outcome for this exact directory.
			let compacting = WendelonLogStore::open_for_retention_test(
				&dir,
				SEGMENT_BYTES,
				Some(RetentionGuard::new(ControlRetention::Compacting)),
			)
			.await;
			let baseline: Option<(std::io::ErrorKind, &str)> = match name {
				// Zero-length snapshot is no artifact; the receive temp and an
				// empty legacy file are accepted by the existing open.
				"A1 snapshot artifact (empty file counts)"
				| "A2 receive temp"
				| "A3 legacy monolithic WAL" => None,
				// The replay contiguity guard refuses a relabelled segment 0 whose
				// first entry is past 1, and an interior gap, before any effect.
				"B2 first stored entry above 0" => Some((
					std::io::ErrorKind::InvalidData,
					"not right after the snapshot boundary",
				)),
				"B3 gap" => Some((
					std::io::ErrorKind::InvalidData,
					"is missing entries between",
				)),
				"A4 lowest segment above 0" => Some((
					std::io::ErrorKind::InvalidData,
					"no snapshot provides the purged prefix",
				)),
				_ => match &expect {
					Expect::ExistingError(kind, diagnostic) => Some((*kind, *diagnostic)),
					_ => panic!("{name}: no compacting baseline"),
				},
			};
			match (baseline, &compacting) {
				(None, Ok(_)) => {}
				(Some((kind, diagnostic)), Err(err)) => {
					assert_eq!(start_error(err), None, "{name}: compacting {err}");
					assert_eq!(err.kind(), kind, "{name}: compacting {err}");
					assert!(
						err.to_string().contains(diagnostic),
						"{name}: compacting {err}"
					);
				}
				(None, Err(err)) => panic!("{name}: compacting baseline opens, got {err}"),
				(Some(_), Ok(_)) => panic!("{name}: compacting baseline refuses, but opened"),
			}
		}
	}
}

#[tokio::test]
async fn full_retains_genesis_history_past_the_default_threshold() {
	let dir = fresh_dir("full-threshold");
	let node = start(&dir, ControlRetention::Full).await.unwrap();
	assert_eq!(node.guard.status().history, HistoryCheck::Fresh);
	// Successful state transitions at the start and the end of the history,
	// around the invalid filler entries.
	node.raft.client_write(seed(41)).await.unwrap();
	commit(&node.raft, PAST_DEFAULT_THRESHOLD).await;
	node.raft.client_write(seed(42)).await.unwrap();
	commit(&node.raft, 0).await;
	let sm_state = node.state_machine.state_snapshot().await;
	assert_eq!(sm_state.migrations.len(), 2);
	assert_eq!(
		Some(&sm_state.migrations[&42]),
		match seed(42) {
			WendelonRequest::SeedMigration(seed) => Some(seed.entry),
			_ => None,
		}
		.as_ref()
	);
	let metrics = node.raft.metrics().borrow().clone();
	assert_eq!(metrics.snapshot, None);
	assert_eq!(metrics.purged, None);
	let mut store = node.store.clone();
	let state = store.get_log_state().await.unwrap();
	assert_eq!(state.last_purged_log_id, None);
	let last = state.last_log_id.unwrap().index;
	assert!(last > PAST_DEFAULT_THRESHOLD);
	let before: Vec<u64> = stored_indexes(&mut store).await;
	assert_eq!(before, (0..=last).collect::<Vec<_>>());
	let before_entries = entry_bytes(&mut store, last).await;
	assert_eq!(before_entries.len() as u64, last + 1);
	assert!(!dir.join("snapshot.wnd").exists());
	assert!(segment_files(&dir).len() > 1, "rollover expected");
	assert_eq!(node.guard.status().violation, None);
	node.raft.shutdown().await.unwrap();
	drop(store);
	drop(node);

	// Full Raft and state-machine restart from the retained WAL.
	let node = start(&dir, ControlRetention::Full).await.unwrap();
	assert_eq!(
		node.guard.status().history,
		HistoryCheck::Complete { stored_last: last }
	);
	let mut store = node.store.clone();
	let after = stored_indexes(&mut store).await;
	assert_eq!(after[..=last as usize], before[..]);
	// Every entry through the pinned last index, log id and payload, is the
	// one written before shutdown; a new-term suffix may follow.
	assert_eq!(entry_bytes(&mut store, last).await, before_entries);
	node.raft
		.wait(Some(BOUND))
		.applied_index_at_least(Some(last), "restart re-applies the retained log")
		.await
		.unwrap();
	// The recovered state machine reached the same meaningful state.
	let recovered = node.state_machine.state_snapshot().await;
	assert_eq!(recovered.migrations, sm_state.migrations);
	assert_eq!(
		format!("{:?}", recovered.epoch_history),
		format!("{:?}", sm_state.epoch_history)
	);
	assert_eq!(
		recovered.last_membership.membership(),
		sm_state.last_membership.membership()
	);
	let metrics = node.raft.metrics().borrow().clone();
	assert_eq!(metrics.snapshot, None);
	assert_eq!(metrics.purged, None);
	assert_eq!(node.guard.status().violation, None);
	node.raft.shutdown().await.unwrap();

	// Control: the same harness under Compacting snapshots and purges.
	let dir = fresh_dir("compacting-threshold");
	let node = start(&dir, ControlRetention::Compacting).await.unwrap();
	let first_segment = segment_files(&dir)[0].clone();
	commit(&node.raft, PAST_DEFAULT_THRESHOLD).await;
	node.raft
		.wait(Some(BOUND))
		.metrics(
			|m| m.snapshot.is_some() && m.purged.is_some(),
			"compacting snapshots and purges past the threshold",
		)
		.await
		.unwrap();
	assert!(!first_segment.exists(), "compacting purged segment 0");
	assert_eq!(node.guard.status().violation, None);
	node.raft.shutdown().await.unwrap();
}

fn snapshot_frame(meta: Vec<u8>) -> HrftFrame {
	HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::INSTALL_SNAPSHOT_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: meta.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: PooledBuf::orphan(meta),
		arrow_payload: None,
		trailing: None,
	}
}

fn peer_context() -> RequestContext {
	let peer = NodeId {
		id: 2,
		_reserved: 0,
	};
	RequestContext {
		remote: Some(peer),
		actor: ActorIdentity::data_node(peer),
	}
}

#[tokio::test]
async fn full_refuses_snapshot_request_before_decode() {
	for mode in [ControlRetention::Full, ControlRetention::Compacting] {
		let dir = fresh_dir("snapshot-request");
		let node = start(&dir, mode).await.unwrap();
		commit(&node.raft, 8).await;
		let server = WendelonRaftInternalServer::new(
			node.raft.clone(),
			BufPool::new(8, 1 << 20),
			node.guard.clone(),
		);
		let files = dir_bytes(&dir);
		let at = position(&node.raft);

		let err = server
			.handle_raft_frame(peer_context(), snapshot_frame(vec![0xde, 0xad]))
			.await
			.unwrap_err();
		let refused = err
			.get_ref()
			.and_then(|inner| inner.downcast_ref::<RetentionViolation>())
			.copied();
		let status = node.guard.status();
		match mode {
			ControlRetention::Full => {
				assert_eq!(refused, Some(RetentionViolation::SnapshotRequest));
				assert_eq!(status.violation, Some(RetentionViolation::SnapshotRequest));
				assert_eq!(status.refusals.snapshot_request, 1);
				assert_eq!(status.refusals.snapshot_receive, 0);
			}
			ControlRetention::Compacting => {
				// Today's behaviour: the undecodable meta is a decode error.
				assert_eq!(refused, None);
				assert_eq!(status, RetentionStatus::compacting());
			}
		}
		assert!(!receive_tmp(&dir).exists());
		assert_eq!(position(&node.raft), at, "{mode:?}: Raft position changed");
		assert_eq!(dir_bytes(&dir), files, "{mode:?}: directory changed");

		// After the comparison: Raft keeps serving.
		commit(&node.raft, 1).await;
		assert!(position(&node.raft).running);
		node.raft.shutdown().await.unwrap();
	}
}

#[tokio::test]
async fn full_storage_backstops_refuse_before_effect() {
	let dir = written_history("backstops", 200).await;
	let guard = RetentionGuard::new(ControlRetention::Full);
	let mut store =
		WendelonLogStore::open_for_retention_test(&dir, SEGMENT_BYTES, Some(guard.clone()))
			.await
			.unwrap();
	let mut state_machine = store.recovered_state_machine().await.unwrap();
	let indexes = stored_indexes(&mut store).await;
	let purge_to = store
		.try_get_log_entries(indexes[indexes.len() / 2]..=indexes[indexes.len() / 2])
		.await
		.unwrap()[0]
		.log_id;

	// A snapshot handle to offer L3, received by a Compacting store elsewhere.
	let elsewhere = fresh_dir("backstops-elsewhere");
	let other = WendelonLogStore::open_for_retention_test(&elsewhere, SEGMENT_BYTES, None)
		.await
		.unwrap();
	let offered = other
		.recovered_state_machine()
		.await
		.unwrap()
		.begin_receiving_snapshot()
		.await
		.unwrap();

	let files = dir_bytes(&dir);
	assert!(state_machine.begin_receiving_snapshot().await.is_err());
	assert_eq!(dir_bytes(&dir), files, "L2");
	assert!(
		state_machine
			.install_snapshot(&SnapshotMeta::default(), offered)
			.await
			.is_err()
	);
	assert_eq!(dir_bytes(&dir), files, "L3");
	assert!(
		state_machine
			.get_snapshot_builder()
			.await
			.build_snapshot()
			.await
			.is_err()
	);
	assert_eq!(dir_bytes(&dir), files, "L4");
	assert!(store.purge(purge_to).await.is_err());
	assert_eq!(dir_bytes(&dir), files, "L5");

	let status = guard.status();
	assert_eq!(status.violation, Some(RetentionViolation::SnapshotReceive));
	assert_eq!(status.refusals.snapshot_receive, 1);
	assert_eq!(status.refusals.snapshot_install, 1);
	assert_eq!(status.refusals.snapshot_build, 1);
	assert_eq!(status.refusals.purge, 1);
	assert_eq!(status.refusals.snapshot_request, 0);

	// Control: Compacting purges as before.
	drop(state_machine);
	drop(store);
	let mut store = WendelonLogStore::open_for_retention_test(
		&dir,
		SEGMENT_BYTES,
		Some(RetentionGuard::new(ControlRetention::Compacting)),
	)
	.await
	.unwrap();
	// Physical deletion needs a durable snapshot covering the purge: a real local build
	// publishes one, as in production.
	let built = store
		.recovered_state_machine()
		.await
		.unwrap()
		.get_snapshot_builder()
		.await
		.build_snapshot()
		.await
		.unwrap();
	assert!(
		built
			.meta
			.last_log_id
			.is_some_and(|id| id.index >= purge_to.index),
		"the built snapshot covers the purge"
	);
	let first_segment = segment_files(&dir)[0].clone();
	store.purge(purge_to).await.unwrap();
	assert!(!first_segment.exists());
	drop(store);
	WendelonLogStore::open_for_retention_test(
		&dir,
		SEGMENT_BYTES,
		Some(RetentionGuard::new(ControlRetention::Compacting)),
	)
	.await
	.expect("the purged store reopens over its snapshot");
}

#[tokio::test]
async fn full_manual_purge_is_a_harmless_noop() {
	let dir = fresh_dir("manual-purge");
	let node = start(&dir, ControlRetention::Full).await.unwrap();
	commit(&node.raft, 200).await;
	let files = dir_bytes(&dir);
	let at = position(&node.raft);
	let last = at.last_log_index.unwrap();
	node.raft.trigger().purge_log(last).await.unwrap();
	// Ordered after the purge command on the same Raft API channel.
	node.raft.with_raft_state(|_| ()).await.unwrap();
	assert_eq!(position(&node.raft), at);
	assert_eq!(dir_bytes(&dir), files);
	assert_eq!(node.guard.status().violation, None);
	node.raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn full_manual_snapshot_is_fatal_and_latched() {
	let dir = fresh_dir("manual-snapshot");
	let node = start(&dir, ControlRetention::Full).await.unwrap();
	commit(&node.raft, 200).await;
	let files = dir_bytes(&dir);
	node.raft.trigger().snapshot().await.unwrap();
	node.raft
		.wait(Some(BOUND))
		.metrics(|m| m.running_state.is_err(), "refused build is fatal")
		.await
		.unwrap();
	assert_eq!(
		node.guard.status().violation,
		Some(RetentionViolation::SnapshotBuild)
	);
	assert_eq!(dir_bytes(&dir), files);
	assert!(!dir.join("snapshot.wnd").exists());
}

#[tokio::test]
async fn retention_status_is_additive_and_shared() {
	// Exact readiness JSON: every key that existed before, unchanged, plus
	// `control_retention`.
	let ready = |control_retention| ReadinessSnapshot {
		node_id: 7,
		epoch: 3,
		leader_id: Some(7),
		cluster_members: vec![7],
		drain_state: DrainState::None,
		audit_materialization_lag_entries: 0,
		degraded_reason: None,
		fips_mode: false,
		fips_self_test_last_ts: None,
		control_retention,
	};
	let json = |snapshot: &ReadinessSnapshot| {
		let mut value: serde_json::Value = serde_json::from_str(&snapshot.to_json()).unwrap();
		let object = value.as_object_mut().unwrap();
		object.remove("fips_mode");
		object.remove("fips_self_test_last_ts");
		value
	};
	let refusals_zero = serde_json::json!({
		"snapshot_request": 0, "snapshot_receive": 0, "snapshot_install": 0,
		"snapshot_build": 0, "purge": 0
	});
	let compacting = ready(RetentionStatus::compacting());
	assert!(compacting.is_ready());
	assert_eq!(
		json(&compacting),
		serde_json::json!({
			"node_id": "n-07", "epoch": 3, "leader_id": "n-07", "cluster_members": ["n-07"],
			"drain_state": "none", "audit_materialization_lag_entries": 0,
			"degraded_reason": null,
			"control_retention": {
				"configured": "compacting", "history": "not_checked", "violation": null,
				"refusals": refusals_zero
			}
		})
	);

	// One guard shared by the store, the state machine and the server.
	let dir = fresh_dir("shared");
	let node = start(&dir, ControlRetention::Full).await.unwrap();
	let fresh = ready(node.guard.status());
	assert!(fresh.is_ready());
	assert_eq!(
		json(&fresh)["control_retention"],
		serde_json::json!({
			"configured": "full", "history": "fresh", "violation": null,
			"refusals": refusals_zero
		})
	);
	let server = WendelonRaftInternalServer::new(
		node.raft.clone(),
		BufPool::new(8, 1 << 20),
		node.guard.clone(),
	);
	assert!(
		server
			.handle_raft_frame(peer_context(), snapshot_frame(vec![1]))
			.await
			.is_err()
	);
	let mut store = node.store.clone();
	assert!(
		store
			.purge(LogId::new(openraft::CommittedLeaderId::new(0, 0), 0))
			.await
			.is_err()
	);
	let mut state_machine = store.recovered_state_machine().await.unwrap();
	assert!(state_machine.begin_receiving_snapshot().await.is_err());
	let violated = ready(node.guard.status());
	assert!(!violated.is_ready());
	assert_eq!(violated.degraded_reason, None);
	assert_eq!(
		json(&violated)["control_retention"],
		serde_json::json!({
			"configured": "full", "history": "fresh", "violation": "snapshot_request",
			"refusals": {
				"snapshot_request": 1, "snapshot_receive": 1, "snapshot_install": 0,
				"snapshot_build": 0, "purge": 1
			}
		})
	);
	node.raft.shutdown().await.unwrap();

	// The node wires one guard to its server and its readiness view.
	let data_dir = fresh_dir("shared-node");
	let mut cfg =
		crate::node::NodeBootstrapConfig::dev(31, data_dir.to_path_buf(), "127.0.0.1:0".into());
	cfg.control_retention = ControlRetention::Full;
	let node = crate::node::WendelonNode::bootstrap_single_node(cfg)
		.await
		.unwrap();
	let before = node.readiness_reporter().snapshot().await;
	assert_eq!(before.control_retention.configured, ControlRetention::Full);
	assert_eq!(before.control_retention.history, HistoryCheck::Fresh);
	assert_eq!(before.control_retention.violation, None);
	let frame = snapshot_frame(vec![1]);
	assert!(
		node.router
			.handle_frame(peer_context(), frame)
			.await
			.is_err()
	);
	let after = node.readiness_reporter().snapshot().await;
	assert_eq!(
		after.control_retention.violation,
		Some(RetentionViolation::SnapshotRequest)
	);
	assert!(!after.is_ready());
	tokio::time::timeout(BOUND, node.shutdown())
		.await
		.expect("shared node shutdown exceeded the progress bound")
		.unwrap();

	// Metric names are present.
	let rendered = crate::metrics::global().render_prometheus();
	for name in [
		"wendelon_control_retention_full ",
		"wendelon_control_retention_violated ",
		"wendelon_control_retention_refusals_total{path=\"snapshot_request\"} ",
		"wendelon_control_retention_refusals_total{path=\"purge\"} ",
	] {
		assert!(rendered.contains(name), "missing metric {name}");
	}
}

/// Architectural source guard over the non-test library and binary sources.
///
/// It walks `mod` declarations from the crate roots, skips only items whose
/// `cfg` predicate holds solely under test (`cfg(test)` or
/// `cfg(all(.., test, ..))`), and matches token shapes, so spacing
/// and line breaks do not matter. It refuses, outside tests: any call of
/// `purge_log` or `install_full_snapshot` on any receiver; any use of the
/// openraft `Trigger` type; any `.trigger()` not immediately followed by
/// `.elect()` (so no trigger handle can be bound and later asked for a
/// snapshot); and any `install_snapshot` call other than the one in the Raft
/// internal server. Trait method definitions (`fn install_snapshot`) are not
/// calls. Limits: it is not a semantic proof. It cannot see through macros
/// that assemble these names, re-exports under another name, or code outside
/// this crate.
#[test]
fn retention_paths_are_guarded() {
	#[derive(Default)]
	struct Findings {
		files: BTreeSet<PathBuf>,
		refused: Vec<String>,
		install_calls: Vec<(PathBuf, bool)>,
		install_definitions: usize,
	}

	/// True only for a predicate that holds solely under `cfg(test)`:
	/// `test` itself or `all(.., test, ..)`. `any(test, ..)` and `not(..)`
	/// can hold in a production build and are scanned.
	fn test_only(predicate: &[TokenTree]) -> bool {
		match predicate {
			[TokenTree::Ident(id)] => id == "test",
			[TokenTree::Ident(id), TokenTree::Group(group)] if id == "all" => {
				let items: Vec<TokenTree> = group.stream().into_iter().collect();
				items
					.split(|t| matches!(t, TokenTree::Punct(p) if p.as_char() == ','))
					.any(test_only)
			}
			_ => false,
		}
	}

	fn is_test_cfg(attr: &TokenStream) -> bool {
		let tokens: Vec<TokenTree> = attr.clone().into_iter().collect();
		match tokens.as_slice() {
			[TokenTree::Ident(id), TokenTree::Group(group)]
				if id == "cfg" && group.delimiter() == Delimiter::Parenthesis =>
			{
				test_only(&group.stream().into_iter().collect::<Vec<_>>())
			}
			_ => false,
		}
	}

	fn ident(tree: Option<&TokenTree>, name: &str) -> bool {
		matches!(tree, Some(TokenTree::Ident(id)) if id == name)
	}
	fn punct(tree: Option<&TokenTree>, ch: char) -> bool {
		matches!(tree, Some(TokenTree::Punct(p)) if p.as_char() == ch)
	}
	fn parens(tree: Option<&TokenTree>, empty: bool) -> bool {
		matches!(tree, Some(TokenTree::Group(g))
			if g.delimiter() == Delimiter::Parenthesis && (!empty || g.stream().is_empty()))
	}

	fn walk(stream: TokenStream, file: &Path, module_dir: &Path, out: &mut Findings) {
		let tokens: Vec<TokenTree> = stream.into_iter().collect();
		let mut i = 0;
		while i < tokens.len() {
			// `#[cfg(..test..)] item`: skip to the end of the item.
			if punct(tokens.get(i), '#')
				&& let Some(TokenTree::Group(attr)) = tokens.get(i + 1)
				&& attr.delimiter() == Delimiter::Bracket
			{
				if is_test_cfg(&attr.stream()) {
					let mut j = i + 2;
					while j < tokens.len() {
						match &tokens[j] {
							TokenTree::Punct(p) if p.as_char() == ';' => break,
							TokenTree::Group(g) if g.delimiter() == Delimiter::Brace => break,
							_ => j += 1,
						}
					}
					i = j + 1;
					continue;
				}
				let text = attr.stream().to_string();
				assert!(
					!text.starts_with("path"),
					"{}: a production #[path] module defeats this guard",
					file.display()
				);
				i += 2;
				continue;
			}
			// `mod name;` and `mod name { .. }`.
			if ident(tokens.get(i), "mod")
				&& let Some(TokenTree::Ident(name)) = tokens.get(i + 1)
			{
				let child_dir = module_dir.join(name.to_string());
				match tokens.get(i + 2) {
					Some(TokenTree::Punct(p)) if p.as_char() == ';' => {
						let flat = module_dir.join(format!("{name}.rs"));
						let nested = child_dir.join("mod.rs");
						let path = if flat.exists() { flat } else { nested };
						scan(&path, &child_dir, out);
						i += 3;
						continue;
					}
					Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace => {
						walk(g.stream(), file, &child_dir, out);
						i += 3;
						continue;
					}
					_ => {}
				}
			}
			if let Some(TokenTree::Ident(id)) = tokens.get(i) {
				let name = id.to_string();
				let defined = i > 0 && ident(tokens.get(i - 1), "fn");
				let called = parens(tokens.get(i + 1), false);
				let method = i > 0 && punct(tokens.get(i - 1), '.');
				match name.as_str() {
					"purge_log" | "install_full_snapshot" if called && !defined => {
						out.refused.push(format!("{}: {name}", file.display()));
					}
					"Trigger" => out.refused.push(format!("{}: Trigger", file.display())),
					"trigger"
						if method
							&& parens(tokens.get(i + 1), true)
							&& !(punct(tokens.get(i + 2), '.')
								&& ident(tokens.get(i + 3), "elect")) =>
					{
						out.refused.push(format!(
							"{}: trigger() not followed by elect()",
							file.display()
						));
					}
					"install_snapshot" if defined => out.install_definitions += 1,
					"install_snapshot" if called => {
						out.install_calls.push((file.to_path_buf(), method));
					}
					_ => {}
				}
			}
			if let Some(TokenTree::Group(group)) = tokens.get(i) {
				walk(group.stream(), file, module_dir, out);
			}
			i += 1;
		}
	}

	fn scan(path: &Path, module_dir: &Path, out: &mut Findings) {
		let source =
			std::fs::read_to_string(path).unwrap_or_else(|err| panic!("{}: {err}", path.display()));
		let stream: TokenStream = source
			.parse()
			.unwrap_or_else(|err| panic!("{}: {err:?}", path.display()));
		out.files.insert(path.to_path_buf());
		walk(stream, path, module_dir, out);
	}

	let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let mut found = Findings::default();
	scan(&src.join("lib.rs"), &src, &mut found);
	for entry in std::fs::read_dir(src.join("bin")).unwrap() {
		let path = entry.unwrap().path();
		scan(&path, &src.join("bin"), &mut found);
	}
	if src.join("main.rs").exists() {
		scan(&src.join("main.rs"), &src, &mut found);
	}

	for required in [
		"consensus/raft_server.rs",
		"consensus/raft_storage.rs",
		"consensus/retention.rs",
		"network/control_server.rs",
	] {
		assert!(
			found.files.contains(&src.join(required)),
			"guard did not visit {required}"
		);
	}
	assert!(
		!found
			.files
			.contains(&src.join("consensus/retention/tests.rs")),
		"guard visited a test-only module"
	);
	assert!(found.install_definitions >= 2, "trait definitions not seen");
	assert_eq!(found.refused, Vec::<String>::new());
	assert_eq!(
		found.install_calls,
		vec![(src.join("consensus/raft_server.rs"), true)]
	);
}
