#[path = "support/control_ring.rs"]
mod support;

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use wendelon::consensus::{TenantFinalization, WendelonRequest};
use wendelon::network::transport::TransportProvider;
use wendelon::network::{
	dev_lease_signing_secret, dev_transfer_signing_secret, lease_verifying_key_bytes,
	transfer_verifying_key_bytes,
};
use wendelon::node::{ClusterBootstrapConfig, NodeBootstrapConfig, SeedNodeConfig, WendelonNode};
use wendelon::schema::{
	DurabilityLevel, FinalizationMarker, HybridTimestamp, Identity, NodeId, TenantId,
};
use wendelon::simulator::network::{
	InjectedSimulatedTransportFault, SimulatedTransportEventKind, SimulatedTransportFaultAction,
	SimulatedTransportFaultOp, SimulatedTransportNetwork,
};

const DEFAULT_CHAOS_SEED: u64 = 0xC4A0_57E0_2026_0421;
const CHAOS_SEED_DIR: &str = "tests/chaos/seeds";
const CHAOS_SMOKE_SCENARIOS: &[&str] = &[
	"leader_partition_and_rejoin",
	"leader_partition_restart_and_rejoin",
	"follower_partition_restart_and_rejoin",
	"follower_loss_under_control_write_load",
	"rolling_kill",
	"packet_drop_single_stream",
];

#[derive(Debug, Clone, Deserialize)]
struct FrozenChaosSeed {
	issue: u64,
	scenario: String,
	seed: u64,
	introduced_at: String,
	notes: Option<String>,
}

#[derive(Debug)]
struct ChaosFailure {
	scenario: String,
	seed: u64,
	iteration: u64,
	message: String,
}

impl ChaosFailure {
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

impl std::fmt::Display for ChaosFailure {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"FAILED: seed={} scenario={} iteration={}\nReproduce: WENDELON_CHAOS_SCENARIO={} WENDELON_CHAOS_SEED={} cargo test --test chaos chaos_smoke_catalog -- --nocapture\n{}",
			self.seed, self.scenario, self.iteration, self.scenario, self.seed, self.message
		)
	}
}

impl std::error::Error for ChaosFailure {}

fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

fn tenant() -> TenantId {
	TenantId::ZERO
}

fn id(locality: u64) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&locality.to_le_bytes());
	Identity::new(bytes)
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
	CHAOS_SMOKE_SCENARIOS.contains(&name)
}

fn wall_clock_ms(offset_ms: u64) -> u64 {
	let now = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	now.saturating_add(offset_ms % 50)
}

fn chaos_seed_from_env() -> u64 {
	std::env::var("WENDELON_CHAOS_SEED")
		.ok()
		.and_then(|raw| raw.parse::<u64>().ok())
		.unwrap_or(DEFAULT_CHAOS_SEED)
}

fn chaos_scenario_from_env() -> Option<String> {
	std::env::var("WENDELON_CHAOS_SCENARIO")
		.ok()
		.map(|raw| raw.trim().to_string())
		.filter(|raw| !raw.is_empty())
}

fn chaos_require_from_env() -> bool {
	std::env::var("WENDELON_CHAOS_REQUIRE")
		.ok()
		.map(|raw| {
			matches!(
				raw.trim().to_ascii_lowercase().as_str(),
				"1" | "true" | "yes" | "on"
			)
		})
		.unwrap_or(false)
}

fn handle_bootstrap_permission_denied(
	scenario: &str,
	seed: u64,
	err: &std::io::Error,
) -> std::result::Result<(), ChaosFailure> {
	if chaos_require_from_env() {
		return Err(ChaosFailure::new(
			scenario,
			seed,
			0,
			format!(
				"cluster bootstrap hit PermissionDenied in a required chaos environment: {err}"
			),
		));
	}
	eprintln!("skipping {scenario} in this environment: {err}");
	Ok(())
}

fn frozen_seed_paths() -> Result<Vec<PathBuf>> {
	let mut out = Vec::new();
	let dir = Path::new(CHAOS_SEED_DIR);
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

fn load_frozen_seed(path: &Path) -> Result<FrozenChaosSeed> {
	let text = fs::read_to_string(path)?;
	toml::from_str(&text).map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err))
}

fn validate_frozen_seed_file(path: &Path, seed: &FrozenChaosSeed) {
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

fn provider_for(
	node_id: u32,
	registry: Arc<support::LoopbackRegistry>,
) -> Arc<dyn TransportProvider> {
	support::loopback_provider(node(node_id), registry)
}

fn trusted_cfg(
	tag: &str,
	node_id: u32,
	advertise_address: &str,
	rf: usize,
	all_nodes: &[u32],
	join_seeds: &[(u32, &str)],
) -> (support::TestDir, NodeBootstrapConfig) {
	let scratch = support::tempdir(tag);
	let mut cfg = NodeBootstrapConfig::dev(
		node_id,
		scratch.to_path_buf(),
		advertise_address.to_string(),
	);
	cfg.rf = rf;
	cfg.drift_max_offset_ms = 5_000;
	cfg.cluster = if join_seeds.is_empty() {
		ClusterBootstrapConfig::Bootstrap
	} else {
		ClusterBootstrapConfig::Join {
			seeds: join_seeds
				.iter()
				.map(|(seed_id, address)| SeedNodeConfig {
					node_id: *seed_id,
					address: (*address).to_string(),
				})
				.collect(),
		}
	};
	for trusted_node_id in all_nodes {
		let lease = dev_lease_signing_secret(*trusted_node_id);
		let transfer = dev_transfer_signing_secret(*trusted_node_id);
		cfg.trusted_lease_signers
			.insert(*trusted_node_id, lease_verifying_key_bytes(lease));
		cfg.trusted_transfer_signers
			.insert(*trusted_node_id, transfer_verifying_key_bytes(transfer));
	}
	(scratch, cfg)
}

async fn wait_for_cluster(node: &WendelonNode, expected_epoch: u64, expected_nodes: usize) {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	loop {
		let snapshot = node.ring.snapshot();
		if node.ring.current_epoch() >= expected_epoch
			&& snapshot.data_capable_nodes() == expected_nodes
		{
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for epoch >= {expected_epoch} and {expected_nodes} nodes; got epoch={} nodes={}",
			node.ring.current_epoch(),
			snapshot.data_capable_nodes()
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

async fn wait_for_leader(node: &WendelonNode, expected_leader: u32) {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	loop {
		if node.raft.current_leader().await == Some(expected_leader) {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader {expected_leader}; current leader = {:?}",
			node.raft.current_leader().await
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

async fn wait_for_voters(node: &WendelonNode, expected: &[u32]) {
	let expected: BTreeSet<u32> = expected.iter().copied().collect();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	loop {
		let snapshot = node.state_machine.state_snapshot().await;
		let voters: BTreeSet<u32> = snapshot.last_membership.voter_ids().collect();
		if voters == expected {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for voters {:?}; got {:?}",
			expected,
			voters
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

async fn wait_for_shared_leader(nodes: &BTreeMap<u32, WendelonNode>, exclude: Option<u32>) -> u32 {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	loop {
		let mut leaders = BTreeSet::new();
		let mut all_report_leader = true;
		for (id, node) in nodes {
			if exclude == Some(*id) {
				continue;
			}
			match node.raft.current_leader().await {
				Some(leader) if exclude != Some(leader) => {
					leaders.insert(leader);
				}
				_ => all_report_leader = false,
			}
		}
		if all_report_leader && leaders.len() == 1 {
			return *leaders.iter().next().unwrap();
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for shared leader; observed {:?}",
			leaders
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

fn finalize_marker(epoch: u64, physical_ms: u64, node_id: u32) -> FinalizationMarker {
	FinalizationMarker {
		log_index: 0,
		term: 0,
		epoch,
		commit_hlc: HybridTimestamp {
			physical_ms,
			logical: 0,
			node_id,
		},
		payload_hash: [0; 32],
		e_safe_index: 0,
		durability: DurabilityLevel::QUORUM,
		_pad: [0; 7],
	}
}

fn tenant_finalization(marker: FinalizationMarker) -> TenantFinalization {
	TenantFinalization {
		marker,
		tenant_id: tenant(),
		key_identity: id(marker.epoch),
		charged_bytes: marker.epoch,
		record_count: 1,
	}
}

async fn wait_for_finalization(node: &WendelonNode, marker: FinalizationMarker) {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	loop {
		let snapshot = node.state_machine.state_snapshot().await;
		if snapshot
			.e_safe_watermarks
			.get(&marker.epoch)
			.copied()
			.is_some_and(|hlc| hlc >= marker.commit_hlc)
		{
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for finalization epoch {} on node {}",
			marker.epoch,
			node.local_node.id
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

async fn commit_control_write(
	leader: &WendelonNode,
	nodes: &BTreeMap<u32, WendelonNode>,
	replicate_to: Vec<u32>,
	epoch: u64,
	physical_ms: u64,
) -> FinalizationMarker {
	let marker = finalize_marker(epoch, wall_clock_ms(physical_ms), leader.local_node.id);
	leader
		.raft
		.client_write(WendelonRequest::Finalize(tenant_finalization(marker)))
		.await
		.unwrap();
	for node_id in replicate_to {
		wait_for_finalization(nodes.get(&node_id).unwrap(), marker).await;
	}
	marker
}

async fn kill_node(node: WendelonNode, registry: &Arc<SimulatedTransportNetwork>) {
	// Every caller reboots a node on the same data directory, which stays
	// exclusively owned until shutdown has joined Raft, every worker and
	// storage. Abort-and-drop leaves the owner held by unjoined tasks.
	registry.unpublish(node.local_node);
	tokio::time::timeout(Duration::from_secs(10), node.shutdown())
		.await
		.expect("killed node shutdown should finish")
		.unwrap();
}

struct ChaosCluster {
	registry: Arc<SimulatedTransportNetwork>,
	configs: BTreeMap<u32, NodeBootstrapConfig>,
	nodes: BTreeMap<u32, WendelonNode>,
	/// The nodes' data dirs, kept across every restart for the cluster's whole life.
	_dirs: Vec<support::TestDir>,
}

impl ChaosCluster {
	async fn bootstrap_node(
		cfg: NodeBootstrapConfig,
		provider: Arc<dyn TransportProvider>,
	) -> std::io::Result<WendelonNode> {
		let mut attempts = 0;
		loop {
			match WendelonNode::bootstrap_with_provider(cfg.clone(), provider.clone()).await {
				Ok(node) => return Ok(node),
				Err(err) if err.kind() == ErrorKind::PermissionDenied && attempts < 2 => {
					attempts += 1;
					tokio::time::sleep(Duration::from_millis(100)).await;
				}
				Err(err) => return Err(err),
			}
		}
	}

	async fn bootstrap(tag: &str) -> std::io::Result<Self> {
		let registry = support::shared_loopback_registry();
		let all_nodes = [1u32, 2, 3];
		let node1_addr = format!("loopback://{tag}-node-1");
		let node2_addr = format!("loopback://{tag}-node-2");
		let node3_addr = format!("loopback://{tag}-node-3");

		let (node1_dir, node1_cfg) = trusted_cfg(tag, 1, &node1_addr, 3, &all_nodes, &[]);
		let (node2_dir, node2_cfg) =
			trusted_cfg(tag, 2, &node2_addr, 3, &all_nodes, &[(1, &node1_addr)]);
		let (node3_dir, node3_cfg) =
			trusted_cfg(tag, 3, &node3_addr, 3, &all_nodes, &[(1, &node1_addr)]);

		let node1 =
			Self::bootstrap_node(node1_cfg.clone(), provider_for(1, registry.clone())).await?;
		// ADR 0726 enrollment: each joiner is given the forming member's root KEK.
		for joiner in [&node2_cfg, &node3_cfg] {
			wendelon::crypto::test_support::enroll_root_artifact(
				&node1_cfg.data_dir,
				&joiner.data_dir,
			)?;
		}
		let node2 =
			Self::bootstrap_node(node2_cfg.clone(), provider_for(2, registry.clone())).await?;
		let node3 =
			Self::bootstrap_node(node3_cfg.clone(), provider_for(3, registry.clone())).await?;

		wait_for_cluster(&node1, 3, 3).await;
		wait_for_cluster(&node2, 3, 3).await;
		wait_for_cluster(&node3, 3, 3).await;
		wait_for_voters(&node1, &[1, 2, 3]).await;
		// Elections may move leadership away from the founding node while
		// the remaining voters join, especially under concurrent test load.
		let nodes = BTreeMap::from([(1, node1), (2, node2), (3, node3)]);
		wait_for_shared_leader(&nodes, None).await;

		Ok(Self {
			registry,
			configs: BTreeMap::from([(1, node1_cfg), (2, node2_cfg), (3, node3_cfg)]),
			nodes,
			_dirs: vec![node1_dir, node2_dir, node3_dir],
		})
	}

	async fn shutdown(mut self) {
		let ids = self.nodes.keys().copied().collect::<Vec<_>>();
		for node_id in ids {
			let node = self.nodes.remove(&node_id).unwrap();
			self.registry.unpublish(node.local_node);
			// The in-process transport can strand background tasks on shutdown after
			// we unpublish routes; aborting and giving the runtime a short drain window
			// keeps the smoke scenarios deterministic.
			node.handles.abort();
			drop(node);
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

async fn run_leader_partition_and_rejoin(seed: u64) -> std::result::Result<(), ChaosFailure> {
	let cluster = match ChaosCluster::bootstrap("chaos-partition").await {
		Ok(cluster) => cluster,
		Err(err) if err.kind() == ErrorKind::PermissionDenied => {
			handle_bootstrap_permission_denied("leader_partition_and_rejoin", seed, &err)?;
			return Ok(());
		}
		Err(err) => {
			return Err(ChaosFailure::new(
				"leader_partition_and_rejoin",
				seed,
				0,
				format!("cluster bootstrap failed: {err}"),
			));
		}
	};
	run_partition_and_rejoin(cluster, seed, None).await
}

async fn run_partition_and_rejoin(
	cluster: ChaosCluster,
	seed: u64,
	expected_initial_leader: Option<u32>,
) -> std::result::Result<(), ChaosFailure> {
	let scenario_event_start = cluster.registry.events().len();
	let original_leader = wait_for_shared_leader(&cluster.nodes, None).await;
	if let Some(expected) = expected_initial_leader
		&& original_leader != expected
	{
		cluster.shutdown().await;
		return Err(ChaosFailure::new(
			"leader_partition_and_rejoin",
			seed,
			0,
			format!(
				"setup leadership changed before partition: expected {expected}, observed {original_leader}"
			),
		));
	}
	commit_control_write(
		cluster.nodes.get(&original_leader).unwrap(),
		&cluster.nodes,
		vec![1, 2, 3],
		1,
		1_000,
	)
	.await;

	for peer in cluster
		.nodes
		.keys()
		.copied()
		.filter(|id| *id != original_leader)
	{
		cluster
			.registry
			.block_bidirectional(node(original_leader), node(peer));
	}

	let survivors = cluster
		.nodes
		.iter()
		.filter(|(id, _)| **id != original_leader)
		.map(|(id, node)| (*id, node))
		.collect::<Vec<_>>();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	let new_leader = loop {
		let left = survivors[0].1.raft.current_leader().await;
		let right = survivors[1].1.raft.current_leader().await;
		if let (Some(left), Some(right)) = (left, right)
			&& left == right
			&& left != original_leader
		{
			break left;
		}
		if tokio::time::Instant::now() >= deadline {
			cluster.shutdown().await;
			return Err(ChaosFailure::new(
				"leader_partition_and_rejoin",
				seed,
				2,
				format!(
					"partitioned leader {} did not fail over; survivors reported {:?} and {:?}",
					original_leader, left, right
				),
			));
		}
		tokio::time::sleep(Duration::from_millis(20)).await;
	};

	let leader = cluster.nodes.get(&new_leader).unwrap();
	let survivor_ids = survivors.iter().map(|(id, _)| *id).collect();
	commit_control_write(leader, &cluster.nodes, survivor_ids, 2, 2_000).await;

	for peer in cluster
		.nodes
		.keys()
		.copied()
		.filter(|id| *id != original_leader)
	{
		cluster
			.registry
			.heal_bidirectional(node(original_leader), node(peer));
	}

	let rejoined_leader = wait_for_shared_leader(&cluster.nodes, Some(original_leader)).await;
	if rejoined_leader != new_leader {
		cluster.shutdown().await;
		return Err(ChaosFailure::new(
			"leader_partition_and_rejoin",
			seed,
			4,
			format!(
				"expected healed cluster to converge on leader {}, observed {}",
				new_leader, rejoined_leader
			),
		));
	}
	for node in cluster.nodes.values() {
		wait_for_leader(node, new_leader).await;
	}
	for node in cluster.nodes.values() {
		wait_for_cluster(node, 3, 3).await;
	}
	let leader = cluster.nodes.get(&new_leader).unwrap();
	commit_control_write(leader, &cluster.nodes, vec![1, 2, 3], 3, 3_000).await;

	let events = cluster.registry.events();
	let blocked_events = events
		.iter()
		.skip(scenario_event_start)
		.filter(|event| matches!(event.kind, SimulatedTransportEventKind::RouteBlocked))
		.count();
	let healed_events = events
		.iter()
		.skip(scenario_event_start)
		.filter(|event| matches!(event.kind, SimulatedTransportEventKind::RouteHealed))
		.count();
	cluster.shutdown().await;
	if blocked_events < 4 || healed_events < 4 {
		return Err(ChaosFailure::new(
			"leader_partition_and_rejoin",
			seed,
			5,
			format!(
				"expected partition/heal events for both routes; saw blocked={} healed={}",
				blocked_events, healed_events
			),
		));
	}
	Ok(())
}

async fn run_leader_partition_restart_and_rejoin(
	seed: u64,
) -> std::result::Result<(), ChaosFailure> {
	let mut cluster = match ChaosCluster::bootstrap("chaos-leader-partition-restart").await {
		Ok(cluster) => cluster,
		Err(err) if err.kind() == ErrorKind::PermissionDenied => {
			handle_bootstrap_permission_denied("leader_partition_restart_and_rejoin", seed, &err)?;
			return Ok(());
		}
		Err(err) => {
			return Err(ChaosFailure::new(
				"leader_partition_restart_and_rejoin",
				seed,
				0,
				format!("cluster bootstrap failed: {err}"),
			));
		}
	};

	let original_leader = wait_for_shared_leader(&cluster.nodes, None).await;
	commit_control_write(
		cluster.nodes.get(&original_leader).unwrap(),
		&cluster.nodes,
		vec![1, 2, 3],
		60,
		60_000,
	)
	.await;
	cluster.registry.clear_events();

	for peer in [1u32, 2, 3] {
		if peer == original_leader {
			continue;
		}
		cluster
			.registry
			.block_bidirectional(node(original_leader), node(peer));
	}

	let survivors = cluster
		.nodes
		.iter()
		.filter(|(id, _)| **id != original_leader)
		.map(|(id, node)| (*id, node))
		.collect::<Vec<_>>();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	let new_leader = loop {
		let left = survivors[0].1.raft.current_leader().await;
		let right = survivors[1].1.raft.current_leader().await;
		if let (Some(left), Some(right)) = (left, right)
			&& left == right
			&& left != original_leader
		{
			break left;
		}
		if tokio::time::Instant::now() >= deadline {
			cluster.shutdown().await;
			return Err(ChaosFailure::new(
				"leader_partition_restart_and_rejoin",
				seed,
				1,
				format!(
					"partitioned leader {} did not fail over; survivors reported {:?} and {:?}",
					original_leader, left, right
				),
			));
		}
		tokio::time::sleep(Duration::from_millis(20)).await;
	};

	let survivor_targets = cluster
		.nodes
		.keys()
		.copied()
		.filter(|candidate| *candidate != original_leader)
		.collect::<Vec<_>>();
	commit_control_write(
		cluster.nodes.get(&new_leader).unwrap(),
		&cluster.nodes,
		survivor_targets,
		61,
		61_000,
	)
	.await;

	let isolated = cluster.nodes.remove(&original_leader).unwrap();
	kill_node(isolated, &cluster.registry).await;

	for peer in [1u32, 2, 3] {
		if peer == original_leader {
			continue;
		}
		cluster
			.registry
			.heal_bidirectional(node(original_leader), node(peer));
	}

	let mut restart_cfg = cluster.configs.get(&original_leader).unwrap().clone();
	restart_cfg.cluster = ClusterBootstrapConfig::Join {
		seeds: vec![SeedNodeConfig {
			node_id: new_leader,
			address: cluster
				.configs
				.get(&new_leader)
				.unwrap()
				.advertise_address
				.clone(),
		}],
	};
	let restarted = ChaosCluster::bootstrap_node(
		restart_cfg,
		provider_for(original_leader, cluster.registry.clone()),
	)
	.await
	.map_err(|err| {
		ChaosFailure::new(
			"leader_partition_restart_and_rejoin",
			seed,
			2,
			format!("restart of partitioned leader {original_leader} failed: {err}"),
		)
	})?;
	cluster.nodes.insert(original_leader, restarted);

	let healed_leader = wait_for_shared_leader(&cluster.nodes, None).await;
	if healed_leader != new_leader {
		cluster.shutdown().await;
		return Err(ChaosFailure::new(
			"leader_partition_restart_and_rejoin",
			seed,
			3,
			format!(
				"healed cluster did not converge on survivor leader {}; observed {}",
				new_leader, healed_leader
			),
		));
	}
	for node in cluster.nodes.values() {
		wait_for_leader(node, new_leader).await;
		wait_for_cluster(node, 3, 3).await;
	}
	commit_control_write(
		cluster.nodes.get(&new_leader).unwrap(),
		&cluster.nodes,
		vec![1, 2, 3],
		62,
		62_000,
	)
	.await;

	let events = cluster.registry.events();
	let blocked_events = events
		.iter()
		.filter(|event| matches!(event.kind, SimulatedTransportEventKind::RouteBlocked))
		.count();
	let healed_events = events
		.iter()
		.filter(|event| matches!(event.kind, SimulatedTransportEventKind::RouteHealed))
		.count();
	let unpublished_events = events
		.iter()
		.filter(|event| matches!(event.kind, SimulatedTransportEventKind::Unpublished))
		.count();
	cluster.shutdown().await;
	if blocked_events < 4 || healed_events < 4 || unpublished_events == 0 {
		return Err(ChaosFailure::new(
			"leader_partition_restart_and_rejoin",
			seed,
			4,
			format!(
				"expected partition/heal/restart evidence; saw blocked={} healed={} unpublished={}",
				blocked_events, healed_events, unpublished_events
			),
		));
	}
	Ok(())
}

async fn run_follower_partition_restart_and_rejoin(
	seed: u64,
) -> std::result::Result<(), ChaosFailure> {
	let mut cluster = match ChaosCluster::bootstrap("chaos-partition-restart").await {
		Ok(cluster) => cluster,
		Err(err) if err.kind() == ErrorKind::PermissionDenied => {
			handle_bootstrap_permission_denied(
				"follower_partition_restart_and_rejoin",
				seed,
				&err,
			)?;
			return Ok(());
		}
		Err(err) => {
			return Err(ChaosFailure::new(
				"follower_partition_restart_and_rejoin",
				seed,
				0,
				format!("cluster bootstrap failed: {err}"),
			));
		}
	};
	let leader_id = wait_for_shared_leader(&cluster.nodes, None).await;
	let victim = [1u32, 2, 3]
		.into_iter()
		.find(|candidate| *candidate != leader_id)
		.unwrap();
	commit_control_write(
		cluster.nodes.get(&leader_id).unwrap(),
		&cluster.nodes,
		vec![1, 2, 3],
		30,
		30_000,
	)
	.await;
	cluster.registry.clear_events();

	for peer in [1u32, 2, 3] {
		if peer == victim {
			continue;
		}
		cluster
			.registry
			.block_bidirectional(node(victim), node(peer));
	}

	let survivor_leader = wait_for_shared_leader(&cluster.nodes, Some(victim)).await;
	if survivor_leader != leader_id {
		cluster.shutdown().await;
		return Err(ChaosFailure::new(
			"follower_partition_restart_and_rejoin",
			seed,
			1,
			format!(
				"partitioned follower {} should not change leader {}; observed {}",
				victim, leader_id, survivor_leader
			),
		));
	}
	let survivor_targets = cluster
		.nodes
		.keys()
		.copied()
		.filter(|candidate| *candidate != victim)
		.collect::<Vec<_>>();
	commit_control_write(
		cluster.nodes.get(&leader_id).unwrap(),
		&cluster.nodes,
		survivor_targets,
		31,
		31_000,
	)
	.await;

	let isolated = cluster.nodes.remove(&victim).unwrap();
	kill_node(isolated, &cluster.registry).await;

	for peer in [1u32, 2, 3] {
		if peer == victim {
			continue;
		}
		cluster
			.registry
			.heal_bidirectional(node(victim), node(peer));
	}

	let survivor_leader = wait_for_shared_leader(&cluster.nodes, Some(victim)).await;
	if survivor_leader != leader_id {
		cluster.shutdown().await;
		return Err(ChaosFailure::new(
			"follower_partition_restart_and_rejoin",
			seed,
			2,
			format!(
				"survivors changed leader during follower restart; expected {} observed {}",
				leader_id, survivor_leader
			),
		));
	}

	let restart_cfg = cluster.configs.get(&victim).unwrap().clone();
	let restarted =
		ChaosCluster::bootstrap_node(restart_cfg, provider_for(victim, cluster.registry.clone()))
			.await
			.map_err(|err| {
				ChaosFailure::new(
					"follower_partition_restart_and_rejoin",
					seed,
					3,
					format!("restart of partitioned follower {victim} failed: {err}"),
				)
			})?;
	cluster.nodes.insert(victim, restarted);

	let healed_leader = wait_for_shared_leader(&cluster.nodes, None).await;
	if healed_leader != leader_id {
		cluster.shutdown().await;
		return Err(ChaosFailure::new(
			"follower_partition_restart_and_rejoin",
			seed,
			4,
			format!(
				"healed cluster did not converge on original leader {}; observed {}",
				leader_id, healed_leader
			),
		));
	}
	for node in cluster.nodes.values() {
		wait_for_leader(node, leader_id).await;
		wait_for_cluster(node, 3, 3).await;
	}
	commit_control_write(
		cluster.nodes.get(&leader_id).unwrap(),
		&cluster.nodes,
		vec![1, 2, 3],
		32,
		32_000,
	)
	.await;

	let events = cluster.registry.events();
	let blocked_events = events
		.iter()
		.filter(|event| matches!(event.kind, SimulatedTransportEventKind::RouteBlocked))
		.count();
	let healed_events = events
		.iter()
		.filter(|event| matches!(event.kind, SimulatedTransportEventKind::RouteHealed))
		.count();
	let unpublished_events = events
		.iter()
		.filter(|event| matches!(event.kind, SimulatedTransportEventKind::Unpublished))
		.count();
	cluster.shutdown().await;
	if blocked_events < 4 || healed_events < 4 || unpublished_events == 0 {
		return Err(ChaosFailure::new(
			"follower_partition_restart_and_rejoin",
			seed,
			5,
			format!(
				"expected partition/heal/restart evidence; saw blocked={} healed={} unpublished={}",
				blocked_events, healed_events, unpublished_events
			),
		));
	}
	Ok(())
}

async fn run_packet_drop_single_stream(seed: u64) -> std::result::Result<(), ChaosFailure> {
	let cluster = match ChaosCluster::bootstrap("chaos-packet-drop").await {
		Ok(cluster) => cluster,
		Err(err) if err.kind() == ErrorKind::PermissionDenied => {
			handle_bootstrap_permission_denied("packet_drop_single_stream", seed, &err)?;
			return Ok(());
		}
		Err(err) => {
			return Err(ChaosFailure::new(
				"packet_drop_single_stream",
				seed,
				0,
				format!("cluster bootstrap failed: {err}"),
			));
		}
	};
	let leader_id = wait_for_shared_leader(&cluster.nodes, None).await;
	let dropped_peer = [1u32, 2, 3]
		.into_iter()
		.find(|candidate| *candidate != leader_id)
		.unwrap();
	let quorum_targets = [1u32, 2, 3]
		.into_iter()
		.filter(|candidate| *candidate != dropped_peer)
		.collect::<Vec<_>>();

	commit_control_write(
		cluster.nodes.get(&leader_id).unwrap(),
		&cluster.nodes,
		vec![1, 2, 3],
		20,
		20_000,
	)
	.await;
	cluster.registry.inject(
		InjectedSimulatedTransportFault::on(
			SimulatedTransportFaultOp::OpenBi,
			SimulatedTransportFaultAction::Error {
				kind: ErrorKind::ConnectionReset,
				message: "simulated packet drop".to_string(),
			},
		)
		.from(node(leader_id))
		.to(node(dropped_peer)),
	);
	commit_control_write(
		cluster.nodes.get(&leader_id).unwrap(),
		&cluster.nodes,
		quorum_targets,
		21,
		21_000,
	)
	.await;
	let converged_leader = wait_for_shared_leader(&cluster.nodes, None).await;
	commit_control_write(
		cluster.nodes.get(&converged_leader).unwrap(),
		&cluster.nodes,
		vec![1, 2, 3],
		22,
		22_000,
	)
	.await;

	let drop_faults = cluster
		.registry
		.events()
		.into_iter()
		.filter(|event| {
			matches!(
				event.kind,
				SimulatedTransportEventKind::Fault {
					op: SimulatedTransportFaultOp::OpenBi,
					action: SimulatedTransportFaultAction::Error {
						kind: ErrorKind::ConnectionReset,
						ref message,
					},
				} if message == "simulated packet drop"
			)
		})
		.count();
	cluster.shutdown().await;
	if drop_faults == 0 {
		return Err(ChaosFailure::new(
			"packet_drop_single_stream",
			seed,
			3,
			"expected at least one simulated packet-drop fault event",
		));
	}
	Ok(())
}

async fn run_follower_loss_under_control_write_load(
	seed: u64,
) -> std::result::Result<(), ChaosFailure> {
	let mut cluster = match ChaosCluster::bootstrap("chaos-follower-loss-load").await {
		Ok(cluster) => cluster,
		Err(err) if err.kind() == ErrorKind::PermissionDenied => {
			handle_bootstrap_permission_denied(
				"follower_loss_under_control_write_load",
				seed,
				&err,
			)?;
			return Ok(());
		}
		Err(err) => {
			return Err(ChaosFailure::new(
				"follower_loss_under_control_write_load",
				seed,
				0,
				format!("cluster bootstrap failed: {err}"),
			));
		}
	};
	let mut rng = StdRng::seed_from_u64(seed);
	let leader_id = wait_for_shared_leader(&cluster.nodes, None).await;
	let mut followers = [1u32, 2, 3]
		.into_iter()
		.filter(|candidate| *candidate != leader_id)
		.collect::<Vec<_>>();
	followers.shuffle(&mut rng);
	let victim = followers[0];

	commit_control_write(
		cluster.nodes.get(&leader_id).unwrap(),
		&cluster.nodes,
		vec![1, 2, 3],
		40,
		40_000,
	)
	.await;
	cluster.registry.clear_events();

	let killed = cluster.nodes.remove(&victim).unwrap();
	kill_node(killed, &cluster.registry).await;

	for iteration in 0..4u64 {
		let current_leader = wait_for_shared_leader(&cluster.nodes, Some(victim)).await;
		let survivor_targets = cluster.nodes.keys().copied().collect::<Vec<_>>();
		commit_control_write(
			cluster.nodes.get(&current_leader).unwrap(),
			&cluster.nodes,
			survivor_targets,
			41 + iteration,
			41_000 + iteration,
		)
		.await;
	}

	let restarted = ChaosCluster::bootstrap_node(
		cluster.configs.get(&victim).unwrap().clone(),
		provider_for(victim, cluster.registry.clone()),
	)
	.await
	.map_err(|err| {
		ChaosFailure::new(
			"follower_loss_under_control_write_load",
			seed,
			2,
			format!("restart of follower {victim} failed: {err}"),
		)
	})?;
	wait_for_cluster(&restarted, 3, 3).await;
	cluster.nodes.insert(victim, restarted);
	for node in cluster.nodes.values() {
		wait_for_cluster(node, 3, 3).await;
	}
	wait_for_voters(cluster.nodes.get(&victim).unwrap(), &[1, 2, 3]).await;

	let converged_leader = wait_for_shared_leader(&cluster.nodes, None).await;
	commit_control_write(
		cluster.nodes.get(&converged_leader).unwrap(),
		&cluster.nodes,
		vec![1, 2, 3],
		50,
		50_000,
	)
	.await;

	let events = cluster.registry.events();
	let unpublished_events = events
		.iter()
		.filter(|event| matches!(event.kind, SimulatedTransportEventKind::Unpublished))
		.count();
	cluster.shutdown().await;
	if unpublished_events == 0 {
		return Err(ChaosFailure::new(
			"follower_loss_under_control_write_load",
			seed,
			3,
			"expected follower kill/restart to emit at least one unpublish event",
		));
	}
	Ok(())
}

async fn run_rolling_kill(seed: u64) -> std::result::Result<(), ChaosFailure> {
	let mut cluster = match ChaosCluster::bootstrap("chaos-rolling-kill").await {
		Ok(cluster) => cluster,
		Err(err) if err.kind() == ErrorKind::PermissionDenied => {
			handle_bootstrap_permission_denied("rolling_kill", seed, &err)?;
			return Ok(());
		}
		Err(err) => {
			return Err(ChaosFailure::new(
				"rolling_kill",
				seed,
				0,
				format!("cluster bootstrap failed: {err}"),
			));
		}
	};
	let mut rng = StdRng::seed_from_u64(seed);
	let mut kill_order = [1u32, 2, 3];
	kill_order.shuffle(&mut rng);

	for (iteration, node_id) in kill_order.iter().copied().enumerate() {
		let leader_id = wait_for_shared_leader(&cluster.nodes, None).await;
		commit_control_write(
			cluster.nodes.get(&leader_id).unwrap(),
			&cluster.nodes,
			cluster.nodes.keys().copied().collect(),
			10 + iteration as u64 * 3,
			10_000 + iteration as u64 * 3,
		)
		.await;

		let killed = cluster.nodes.remove(&node_id).unwrap();
		kill_node(killed, &cluster.registry).await;

		let survivor_leader = wait_for_shared_leader(&cluster.nodes, Some(node_id)).await;
		if !cluster.nodes.contains_key(&survivor_leader) {
			cluster.shutdown().await;
			return Err(ChaosFailure::new(
				"rolling_kill",
				seed,
				iteration as u64 + 1,
				format!(
					"survivor leader {} not present after killing {}",
					survivor_leader, node_id
				),
			));
		}

		let leader = cluster.nodes.get(&survivor_leader).unwrap();
		commit_control_write(
			leader,
			&cluster.nodes,
			cluster.nodes.keys().copied().collect(),
			11 + iteration as u64 * 3,
			11_000 + iteration as u64 * 3,
		)
		.await;

		let restarted = ChaosCluster::bootstrap_node(
			cluster.configs.get(&node_id).unwrap().clone(),
			provider_for(node_id, cluster.registry.clone()),
		)
		.await
		.map_err(|err| {
			ChaosFailure::new(
				"rolling_kill",
				seed,
				iteration as u64 + 1,
				format!("restart of node {node_id} failed: {err}"),
			)
		})?;
		wait_for_cluster(&restarted, 3, 3).await;
		cluster.nodes.insert(node_id, restarted);
		wait_for_voters(cluster.nodes.get(&node_id).unwrap(), &[1, 2, 3]).await;

		let leader = cluster.nodes.get(&survivor_leader).unwrap();
		commit_control_write(
			leader,
			&cluster.nodes,
			vec![1, 2, 3],
			12 + iteration as u64 * 3,
			12_000 + iteration as u64 * 3,
		)
		.await;
	}

	cluster.shutdown().await;
	Ok(())
}

async fn run_chaos_scenario(scenario: &str, seed: u64) -> std::result::Result<(), ChaosFailure> {
	match scenario {
		"leader_partition_and_rejoin" => run_leader_partition_and_rejoin(seed).await,
		"leader_partition_restart_and_rejoin" => {
			run_leader_partition_restart_and_rejoin(seed).await
		}
		"follower_partition_restart_and_rejoin" => {
			run_follower_partition_restart_and_rejoin(seed).await
		}
		"follower_loss_under_control_write_load" => {
			run_follower_loss_under_control_write_load(seed).await
		}
		"packet_drop_single_stream" => run_packet_drop_single_stream(seed).await,
		"rolling_kill" => run_rolling_kill(seed).await,
		other => Err(ChaosFailure::new(
			other,
			seed,
			0,
			format!("unsupported chaos scenario {other}"),
		)),
	}
}

#[test]
fn frozen_chaos_seeds_are_well_formed() {
	for path in frozen_seed_paths().unwrap() {
		let seed = load_frozen_seed(&path).unwrap();
		validate_frozen_seed_file(&path, &seed);
	}
}

#[tokio::test]
async fn replay_frozen_chaos_seeds() {
	for path in frozen_seed_paths().unwrap() {
		let seed = load_frozen_seed(&path).unwrap();
		validate_frozen_seed_file(&path, &seed);
		run_chaos_scenario(&seed.scenario, seed.seed)
			.await
			.unwrap_or_else(|err| panic!("{err}"));
	}
}

#[tokio::test]
async fn chaos_smoke_catalog() {
	let seed = chaos_seed_from_env();
	if let Some(scenario) = chaos_scenario_from_env() {
		run_chaos_scenario(&scenario, seed)
			.await
			.unwrap_or_else(|err| panic!("{err}"));
		return;
	}
	for scenario in CHAOS_SMOKE_SCENARIOS {
		run_chaos_scenario(scenario, seed)
			.await
			.unwrap_or_else(|err| panic!("{err}"));
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

#[tokio::test]
async fn leader_partition_restart_and_rejoin() {
	let seed = chaos_seed_from_env();
	run_leader_partition_restart_and_rejoin(seed)
		.await
		.unwrap_or_else(|err| panic!("{err}"));
}

#[tokio::test]
async fn leader_partition_and_rejoin_after_nonfounder_election() {
	let cluster = ChaosCluster::bootstrap("chaos-nonfounder-partition")
		.await
		.unwrap();
	// Isolate the founder until the other two voters agree on its
	// replacement. An election trigger alone cannot override a live leader.
	for peer in [2u32, 3] {
		cluster.registry.block_bidirectional(node(1), node(peer));
	}
	let nonfounder = wait_for_shared_leader(&cluster.nodes, Some(1)).await;
	assert_ne!(nonfounder, 1);
	for peer in [2u32, 3] {
		cluster.registry.heal_bidirectional(node(1), node(peer));
	}
	for node in cluster.nodes.values() {
		wait_for_leader(node, nonfounder).await;
	}
	run_partition_and_rejoin(cluster, chaos_seed_from_env(), Some(nonfounder))
		.await
		.unwrap_or_else(|err| panic!("{err}"));
}
