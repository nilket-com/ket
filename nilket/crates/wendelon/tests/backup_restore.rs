#[path = "../src/test_support/test_dir.rs"]
mod test_dir;
use std::time::Duration;
use wendelon::audit::replay_audit_chain;
use wendelon::consensus::raft_storage::DurableBootstrapMode;
use wendelon::consensus::{TenantFinalization, WendelonRequest};
use wendelon::node::{ClusterBootstrapConfig, NodeBootstrapConfig, SeedNodeConfig, WendelonNode};
use wendelon::schema::{
	DurabilityLevel, FinalizationMarker, HybridTimestamp, Identity, SlabId, TenantId,
};
use wendelon::storage::StorageEngine;

fn tempdir(tag: &str) -> test_dir::TestDir {
	test_dir::TestDir::new(&format!("wendelon-backup-restore-{tag}"))
}

async fn bootstrap_single_node(cfg: NodeBootstrapConfig) -> std::io::Result<WendelonNode> {
	let mut attempts = 0;
	loop {
		match WendelonNode::bootstrap_single_node(cfg.clone()).await {
			Ok(node) => return Ok(node),
			Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied && attempts < 2 => {
				attempts += 1;
				tokio::time::sleep(Duration::from_millis(100)).await;
			}
			Err(err) => return Err(err),
		}
	}
}

async fn wait_for_local_leader(node: &WendelonNode) {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for node {} to become leader; current leader = {:?}",
			node.local_node.id,
			node.raft.current_leader().await
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

fn tenant_finalization(
	tenant_id: TenantId,
	charged_bytes: u64,
	marker: FinalizationMarker,
) -> TenantFinalization {
	TenantFinalization {
		marker,
		tenant_id,
		key_identity: id(1),
		charged_bytes,
		record_count: 1,
	}
}

fn id(locality: u64) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&locality.to_le_bytes());
	Identity::new(bytes)
}

fn tenant() -> TenantId {
	TenantId::ZERO
}

#[tokio::test]
async fn backup_generation_exports_control_and_storage_artifacts() {
	let dir = tempdir("export");
	let node = match bootstrap_single_node(NodeBootstrapConfig::dev(
		41,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	{
		Ok(node) => node,
		Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
			eprintln!(
				"skipping backup_generation_exports_control_and_storage_artifacts in this environment: {err}"
			);
			return;
		}
		Err(err) => panic!("backup generation bootstrap failed: {err}"),
	};
	wait_for_local_leader(&node).await;

	let key_locality = 0x41_u64;
	let frame = b"backup-me".to_vec();
	node.slab
		.append_durable(
			tenant(),
			id(key_locality),
			HybridTimestamp {
				physical_ms: 10,
				logical: 0,
				node_id: node.local_node.id,
			},
			&frame,
		)
		.await
		.unwrap();
	node.raft
		.client_write(WendelonRequest::Finalize(tenant_finalization(
			tenant(),
			frame.len() as u64,
			finalize_marker(77, 4_200, node.local_node.id),
		)))
		.await
		.unwrap();

	node.handles.abort();
	tokio::time::sleep(Duration::from_millis(25)).await;

	let backup = node.export_cluster_backup().await.unwrap();
	let state = node.state_machine.state_snapshot().await;

	assert_eq!(backup.manifest.format_version, 1);
	assert_eq!(
		backup.manifest.consistent_cut.last_applied_log_id,
		backup.control.manifest.last_applied_log_id
	);
	assert_eq!(
		backup.manifest.consistent_cut.last_e_safe_index,
		state.last_e_safe_index
	);
	assert_eq!(
		backup.manifest.consistent_cut.control_state_hash,
		backup.control.manifest.state_hash
	);
	assert_eq!(backup.manifest.nodes.len(), 1);
	assert_eq!(backup.manifest.nodes[0].node_id, node.local_node.id);
	assert_eq!(backup.manifest.nodes[0].active_slab_id, SlabId::ZERO);
	assert!(!backup.nodes[0].storage.local_slabs.is_empty());
	assert!(
		backup.manifest.nodes[0].local_copy_slab_bytes >= frame.len() as u64,
		"cluster backup manifest should surface the copied local slab bytes"
	);
	assert!(backup.manifest.nodes[0].tiered_references.is_empty());
	assert!(backup.manifest.nodes[0].omitted_rehydrated_slabs.is_empty());
	backup.validate_integrity().unwrap();

	drop(node);
}

#[tokio::test]
async fn backup_manifest_detects_tampered_embedded_storage_artifact() {
	let dir = tempdir("integrity");
	let node = match bootstrap_single_node(NodeBootstrapConfig::dev(
		42,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	{
		Ok(node) => node,
		Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
			eprintln!(
				"skipping backup_manifest_detects_tampered_embedded_storage_artifact in this environment: {err}"
			);
			return;
		}
		Err(err) => panic!("backup generation bootstrap failed: {err}"),
	};
	wait_for_local_leader(&node).await;

	let mut backup = node.export_cluster_backup().await.unwrap();
	backup.nodes[0].storage.local_slabs[0].bytes.push(0xFF);
	let err = backup.validate_integrity().unwrap_err();
	assert!(err.to_string().contains("storage image hash mismatch"));

	drop(node);
}

#[tokio::test]
async fn backup_manifest_detects_tampered_control_image() {
	let dir = tempdir("integrity-control");
	let node = match bootstrap_single_node(NodeBootstrapConfig::dev(
		43,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	{
		Ok(node) => node,
		Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
			eprintln!(
				"skipping backup_manifest_detects_tampered_control_image in this environment: {err}"
			);
			return;
		}
		Err(err) => panic!("backup generation bootstrap failed: {err}"),
	};
	wait_for_local_leader(&node).await;

	let mut backup = node.export_cluster_backup().await.unwrap();
	backup.control.wal.push(0xAB);
	let err = backup.validate_integrity().unwrap_err();
	assert!(err.to_string().contains("control image hash mismatch"));

	drop(node);
}

#[tokio::test]
async fn backup_manifest_detects_voter_id_mismatch() {
	let dir = tempdir("integrity-voters");
	let node = match bootstrap_single_node(NodeBootstrapConfig::dev(
		44,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	{
		Ok(node) => node,
		Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
			eprintln!(
				"skipping backup_manifest_detects_voter_id_mismatch in this environment: {err}"
			);
			return;
		}
		Err(err) => panic!("backup generation bootstrap failed: {err}"),
	};
	wait_for_local_leader(&node).await;

	let mut backup = node.export_cluster_backup().await.unwrap();
	backup.manifest.consistent_cut.voter_ids.push(999);
	let err = backup.validate_integrity().unwrap_err();
	assert!(err.to_string().contains("voter_ids"));

	drop(node);
}

#[tokio::test]
async fn whole_node_loss_restore_bootstraps_in_recovery_mode_and_serves_recovered_state() {
	let source_dir = tempdir("source");
	let source = match bootstrap_single_node(NodeBootstrapConfig::dev(
		52,
		source_dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	{
		Ok(node) => node,
		Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
			eprintln!(
				"skipping whole_node_loss_restore_bootstraps_in_recovery_mode_and_serves_recovered_state in this environment: {err}"
			);
			return;
		}
		Err(err) => panic!("source bootstrap failed: {err}"),
	};
	wait_for_local_leader(&source).await;

	let key_locality = 0x52_u64;
	let first_hlc = HybridTimestamp {
		physical_ms: 100,
		logical: 0,
		node_id: source.local_node.id,
	};
	let second_hlc = HybridTimestamp {
		physical_ms: 200,
		logical: 0,
		node_id: source.local_node.id,
	};
	let first_frame = b"restored-alpha".to_vec();
	let second_frame = b"restored-beta".to_vec();
	source
		.slab
		.append_durable(tenant(), id(key_locality), first_hlc, &first_frame)
		.await
		.unwrap();
	source
		.slab
		.append_durable(tenant(), id(key_locality + 1), second_hlc, &second_frame)
		.await
		.unwrap();
	let marker = finalize_marker(88, 9_900, source.local_node.id);
	source
		.raft
		.client_write(WendelonRequest::Finalize(tenant_finalization(
			tenant(),
			(first_frame.len() + second_frame.len()) as u64,
			marker,
		)))
		.await
		.unwrap();
	let source_audit_head = replay_audit_chain(source_dir.join("audit.ndjson"))
		.unwrap()
		.unwrap();

	source.handles.abort();
	tokio::time::sleep(Duration::from_millis(25)).await;

	let control = source.export_cluster_backup().await.unwrap();

	drop(source);
	tokio::time::sleep(Duration::from_millis(25)).await;

	let restore_dir = tempdir("restore");
	let restored = WendelonNode::restore_from_cluster_backup(
		&control,
		NodeBootstrapConfig::dev(52, restore_dir.to_path_buf(), "127.0.0.1:0".into()),
	)
	.await
	.unwrap();
	wait_for_local_leader(&restored).await;
	assert_eq!(
		restored.log_store.bootstrap_mode().await,
		DurableBootstrapMode::Recovery
	);

	let snapshot = restored.state_machine.state_snapshot().await;
	assert!(
		snapshot
			.e_safe_watermarks
			.get(&marker.epoch)
			.copied()
			.is_some_and(|hlc| hlc >= marker.commit_hlc)
	);
	assert_eq!(
		snapshot.last_membership.voter_ids().collect::<Vec<_>>(),
		vec![restored.local_node.id]
	);

	let restored_location = restored
		.slab
		.lookup(tenant(), id(key_locality))
		.unwrap()
		.unwrap();
	assert_eq!(
		restored.slab.read(restored_location).await.unwrap(),
		first_frame
	);
	let restored_second = restored
		.slab
		.lookup(tenant(), id(key_locality + 1))
		.unwrap()
		.unwrap();
	assert_eq!(
		restored.slab.read(restored_second).await.unwrap(),
		second_frame
	);

	let post_restore_frame = b"fresh-after-restore".to_vec();
	let post_restore_location = restored
		.slab
		.append_durable(
			tenant(),
			id(key_locality + 2),
			HybridTimestamp {
				physical_ms: 300,
				logical: 0,
				node_id: restored.local_node.id,
			},
			&post_restore_frame,
		)
		.await
		.unwrap();
	assert_eq!(
		restored.slab.read(post_restore_location).await.unwrap(),
		post_restore_frame
	);
	let restored_audit_head = replay_audit_chain(restored.slab.data_dir().join("audit.ndjson"))
		.unwrap()
		.unwrap();
	assert_eq!(
		restored_audit_head.hash_algorithm_tag,
		source_audit_head.hash_algorithm_tag
	);
	assert!(
		restored_audit_head.sequence_number > 0,
		"restored node should emit a fresh audit chain after recovery"
	);

	restored.shutdown().await.unwrap();
}

#[tokio::test]
async fn replacement_restore_rejects_reusing_failed_node_id() {
	let dir = tempdir("replacement-same-node-id");
	let source = match bootstrap_single_node(NodeBootstrapConfig::dev(
		62,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	{
		Ok(node) => node,
		Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
			eprintln!(
				"skipping replacement_restore_rejects_reusing_failed_node_id in this environment: {err}"
			);
			return;
		}
		Err(err) => panic!("source bootstrap failed: {err}"),
	};
	wait_for_local_leader(&source).await;

	let backup = source.export_cluster_backup().await.unwrap();
	let replacement_dir = tempdir("replacement-same-node-id-target");
	let mut cfg = NodeBootstrapConfig::dev(62, replacement_dir.to_path_buf(), "127.0.0.1:0".into());
	cfg.cluster = ClusterBootstrapConfig::Join {
		seeds: vec![SeedNodeConfig {
			node_id: 1,
			address: "127.0.0.1:1".into(),
		}],
	};

	let err = match WendelonNode::restore_replacement_from_cluster_backup(&backup, 62, cfg).await {
		Ok(_) => panic!("replacement restore should reject reusing the failed node id"),
		Err(err) => err,
	};
	assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
	assert!(
		err.to_string()
			.contains("fresh node_id distinct from the failed node")
	);

	source.shutdown().await.unwrap();
}
