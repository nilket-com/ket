use super::super::support::manifest_dir;

#[test]
pub(crate) fn adr543_receipt_and_lease_bounds_are_pinned_and_share_worker_loss_policy() {
	let lifecycle =
		std::fs::read_to_string(manifest_dir().join("src/distributed/namespace_lifecycle.rs"))
			.expect("read namespace lifecycle");
	let workers =
		std::fs::read_to_string(manifest_dir().join("src/distributed/worker_registry.rs"))
			.expect("read worker registry");

	for required in [
		"DEFAULT_RECEIPT_CAPACITY: usize = 4_096",
		"Duration::from_secs(15 * 60)",
		"DEFAULT_QUERY_LEASE_RENEW_CADENCE: Duration = Duration::from_secs(5)",
		"if lease_term <= minimum",
		"capacity_evictions",
		"horizon_evictions",
	] {
		assert!(
			lifecycle.contains(required),
			"missing lifecycle bound {required}"
		);
	}
	assert!(workers.contains("DEFAULT_WORKER_LOST_AFTER: Duration = Duration::from_secs(30)"));
}

#[test]
pub(crate) fn adr543_worker_protocol_carries_query_lease_and_retirement_as_typed_exchanges() {
	let wire = std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	for required in [
		"QueryLease,",
		"QueryLeaseAccepted,",
		"RetireQuery,",
		"QueryRetired,",
		"EnvelopeIdentity::Query",
		"pub struct QueryLeasePayload",
		"pub struct RetireQueryPayload",
	] {
		assert!(
			wire.contains(required),
			"missing typed lifecycle carrier {required}"
		);
	}
	for required in [
		"MessageKind::QueryLease =>",
		"MessageKind::RetireQuery =>",
		"issue_or_renew_query_lease(query_id)",
		"retire_query(query_id, payload.cause)",
	] {
		assert!(
			server.contains(required),
			"worker does not drive {required}"
		);
	}
}

#[test]
pub(crate) fn adr543_namespace_creation_is_positive_lease_gated_and_receipt_evidence_based() {
	let lifecycle =
		std::fs::read_to_string(manifest_dir().join("src/distributed/namespace_lifecycle.rs"))
			.expect("read namespace lifecycle");
	let store = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");

	assert!(lifecycle.contains("pub fn authorize_write"));
	assert!(lifecycle.contains("pub fn mark_namespace_present"));
	assert!(lifecycle.contains("receipt_licensed"));
	assert!(lifecycle.contains("PartitionMissingReason::NamespaceUnknown"));
	assert!(lifecycle.contains("PartitionMissingReason::Reclaimed"));
	assert!(store.matches("self.authorize_write(").count() >= 5);
	assert!(store.matches("mark_namespace_present(").count() >= 6);
}

#[test]
pub(crate) fn adr543_coordinator_owns_growing_census_query_renewal_and_terminal_retirement() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	for required in [
		"pub fn remote_workers",
		"entry(task.worker.worker_id)",
		"pub fn fail_query_lease",
		"pub fn mark_terminal",
		"set_failure_hook",
		".fail_query_lease(worker_id)",
	] {
		assert!(
			cancellation.contains(required),
			"missing census state {required}"
		);
	}
	// ADR 628 B1.C: renewal is per worker, driven by the query's reliance ledger (a worker is
	// enrolled when it first becomes Reliable). A renewal failure reaches the query through the
	// ledger, whose failure hook calls `fail_query_lease` naming the worker. Retirement still
	// addresses the growing census.
	for required in [
		"spawn_query_lease_renewal_task",
		"worker_renewal_loop",
		"reliance.on_fatal(worker.worker_id, cause)",
		"active_query_handle.remote_workers()",
		"spawn_remote_retire_fanout_task",
		"active_query_handle.mark_terminal()",
	] {
		assert!(
			coordinator.contains(required),
			"missing coordinator lifecycle {required}"
		);
	}
}

#[test]
pub(crate) fn adr543_cleanup_debt_restart_sweep_and_every_retained_surface_are_observable() {
	let store = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let metrics = std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
		.expect("read observability");

	assert!(!store.contains("let _ = std::fs::remove_file"));
	for required in [
		"cleanup_debt",
		"sweep_owned_spill_files",
		"owned_spill_file_name",
		"startup_swept_files",
	] {
		assert!(
			store.contains(required),
			"missing cleanup mechanism {required}"
		);
	}
	for metric in [
		"shuffle_namespaces",
		"shuffle_retained_bytes",
		"shuffle_physical_spill_files",
		"shuffle_cleanup_debt",
		"shuffle_receipts",
		"shuffle_cancelled_tombstones",
		"shuffle_live_partition_records",
		"shuffle_oldest_namespace_age_millis",
		"shuffle_reclamations_total",
		"shuffle_receipt_evictions_total",
	] {
		assert!(
			metrics.contains(metric),
			"missing shuffle lifecycle metric {metric}"
		);
	}
}

#[test]
pub(crate) fn adr543_production_worker_uses_managed_store_and_periodic_abandonment_reaper() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(daemon.contains("InMemoryPartitionStore::managed("));
	assert!(daemon.contains("attach_shuffle_lifecycle"));
	assert!(daemon.matches("reap_expired_shuffle_namespaces").count() >= 2);
	assert_eq!(
		daemon
			.matches("accepted = endpoint.accept_sql_incoming()")
			.count(),
		2,
		"SQL and worker QUIC admission must dequeue Incoming before any competing timer can cancel the handshake"
	);
	assert!(
		!daemon.contains("accepted = endpoint.accept_connection()"),
		"a lifecycle tick must not cancel a combined dequeue-plus-handshake future"
	);
	assert!(server.contains("pub fn reap_expired_shuffle_namespaces"));
}
