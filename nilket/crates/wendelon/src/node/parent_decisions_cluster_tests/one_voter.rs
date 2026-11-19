//! A fresh ONE-voter ParentDecisions store admits its first write: formed with
//! its formation intent, it creates a primary-key table, commits a write with
//! supported placement, reads exactly that back, and after a clean stop and a
//! restart without an intent reads it again. `a_fresh_store_forms_only_with_a_formation_intent`
//! proves formation and restart only; this proves first-write admission.

use super::*;

const ONE: u32 = 51;

fn one_voter() -> (crate::test_support::TestDir, NodeBootstrapConfig) {
	let dir = crate::test_support::TestDir::new("parent-decisions-one-voter");
	let mut cfg = NodeBootstrapConfig::dev(ONE, dir.to_path_buf(), address(ONE));
	cfg.rf = 1;
	cfg.cluster = ClusterBootstrapConfig::Bootstrap;
	cfg.trusted_lease_signers.insert(
		ONE,
		lease_verifying_key_bytes(dev_lease_signing_secret(ONE)),
	);
	cfg.trusted_transfer_signers.insert(
		ONE,
		transfer_verifying_key_bytes(dev_transfer_signing_secret(ONE)),
	);
	cfg.decision_mode = crate::consensus::decision_mode::DecisionMode::ParentDecisions;
	crate::node::provision_formation_intent(&cfg.data_dir).unwrap();
	(dir, cfg)
}

async fn open(cfg: &NodeBootstrapConfig) -> WendelonNode {
	let network = Arc::new(SimulatedTransportNetwork::default());
	let provider: Arc<dyn crate::network::transport::TransportProvider> =
		Arc::new(SimulatedTransportProvider::new(
			crate::schema::NodeId {
				id: ONE,
				_reserved: 0,
			},
			network,
			crate::network::pool::BufPool::new(64, 1024 * 1024),
			TransportTier::QuicIoUring,
		));
	WendelonNode::bootstrap_with_provider(cfg.clone(), provider)
		.await
		.unwrap()
}

/// The one voter leads, its signer is bound at its current incarnation, every
/// replica segment of its one-member ring lies in a lineage it holds at that
/// incarnation, the table (if named) is applied, and it holds a lease over
/// each of its lineages at the current epoch.
async fn until_settled(node: &WendelonNode, table: Option<TableObjectId>) {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let ready = node.raft.current_leader().await.is_some()
			&& node
				.state_machine
				.read_state(move |state| {
					let Some(decisions) = state.decisions.as_ref() else {
						return false;
					};
					let current = state
						.incarnations
						.get(&ONE)
						.and_then(|registry| registry.current);
					let bound = current.is_some_and(|incarnation| {
						decisions
							.signer_key(crate::consensus::decisions::Signer {
								node: ONE,
								incarnation,
							})
							.is_some()
					});
					let covered =
						state
							.epoch_history
							.last_key_value()
							.is_some_and(|(_, config)| {
								crate::network::ring::RingState::build(
									&crate::network::ring::membership_from_epoch_config(config),
									1,
								)
								.replica_segments()
								.iter()
								.all(|segment| {
									decisions
										.lineages()
										.range(..=segment.start)
										.next_back()
										.is_some_and(|(_, lineage)| {
											segment.end <= lineage.range_end
												&& lineage.holder.id == ONE && current
												== Some(lineage.incarnation)
										})
								})
							});
					bound
						&& covered && !decisions.lineages().is_empty()
						&& state.tenants.contains_key(&TenantId::ZERO)
						&& table.is_none_or(|table| {
							state
								.table_schemas
								.latest_by_id(TenantId::ZERO, table)
								.is_some()
						})
				})
				.await;
		if ready {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the one voter never settled (signer, ring, schema)"
		);
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
	loop {
		let (epoch, held) = node
			.state_machine
			.read_state(|state| {
				let epoch = state
					.epoch_history
					.last_key_value()
					.map(|(_, config)| config.epoch);
				let held: Vec<(u64, u64)> = state
					.decisions
					.as_ref()
					.map(|decisions| {
						decisions
							.lineages()
							.iter()
							.map(|(start, lineage)| (*start, lineage.range_end))
							.collect()
					})
					.unwrap_or_default();
				(epoch, held)
			})
			.await;
		let leases = node.data_server.held_leases_for_test().await;
		if !held.is_empty()
			&& held.iter().all(|(start, end)| {
				leases.iter().any(|(low, high, lease_epoch)| {
					Some(*lease_epoch) == epoch && *low <= *start && *end <= *high
				})
			}) {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the one voter never leased its lineages"
		);
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
}

fn query(
	node: &WendelonNode,
	descriptor: &TableSchemaDescriptor,
) -> crate::schema::TableQueryRequestMeta {
	static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
	let mut meta: crate::schema::TableQueryRequestMeta = bytemuck::Zeroable::zeroed();
	meta.expected_epoch = node.ring.current_epoch();
	meta.tenant_id = TenantId::ZERO;
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.physical_descriptor_fingerprint = *descriptor.physical_descriptor_fingerprint.as_bytes();
	meta.consistency = crate::schema::Consistency::LINEARIZABLE;
	meta.request_id = crate::schema::TableQueryRequestId::from_u128(u128::from(
		NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
	));
	meta
}

/// Reads until OK, accepting only the retryable refusal while the restarted
/// node validates its certified base; returns the rows.
async fn read_validated(node: &WendelonNode, descriptor: &TableSchemaDescriptor) -> Expected {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let answer = node
			.data_server
			.table_query_for_test(query(node, descriptor))
			.await
			.unwrap();
		if answer.status == Some(crate::schema::ReadStatus::OK) {
			return answered(&answer);
		}
		assert_eq!(
			answer.status,
			Some(crate::schema::ReadStatus::BACKPRESSURED),
			"refused only until validated: {answer:?}"
		);
		assert!(tokio::time::Instant::now() < deadline, "never validated");
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_one_voter_store_commits_its_first_write_and_reads_it_after_a_restart() {
	let (_dir, cfg) = one_voter();
	let node = open(&cfg).await;
	assert!(
		!cfg.data_dir.join(crate::node::FORMATION_INTENT).exists(),
		"formation consumes the intent"
	);
	until_settled(&node, None).await;
	let descriptor = descriptor();
	node.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&descriptor),
		))
		.await
		.unwrap();
	until_settled(&node, Some(descriptor.table_object_id)).await;

	// The first admitted write: committed, at the one voter, with no redirect.
	let response = write_retrying(
		&node.data_server,
		write_meta(&descriptor, 2, uuid_v7()),
		rows(&descriptor, &[1, 2]),
	)
	.await;
	assert_committed(&response, "the first write of a fresh one-voter store");
	let expected: Expected = [(1, "row-1".to_owned()), (2, "row-2".to_owned())].into();
	assert_eq!(
		read_validated(&node, &descriptor).await,
		expected,
		"exactly the committed rows, read back"
	);

	// A clean stop, then a restart with no intent: the store is itself.
	tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
		.await
		.expect("a complete close")
		.unwrap();
	assert!(
		cfg.data_dir.join("CLEAN_SHUTDOWN").exists(),
		"a clean close"
	);
	assert!(!cfg.data_dir.join(crate::node::FORMATION_INTENT).exists());
	let node = open(&cfg).await;
	until_settled(&node, Some(descriptor.table_object_id)).await;
	assert_eq!(
		read_validated(&node, &descriptor).await,
		expected,
		"the same rows after the restart"
	);
	tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
		.await
		.expect("a complete close")
		.unwrap();
}
