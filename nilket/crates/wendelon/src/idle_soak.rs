//! ADR 079 local-idle soak evidence envelope and evaluator.
//!
//! This module is public only so the repository's sibling soak command can
//! share one format and one verdict implementation. It is not a storage API.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SOAK_FORMAT_VERSION: u32 = 2;
pub const LOCAL_IDLE_SOAK_FORMAT_VERSION: u32 = SOAK_FORMAT_VERSION;
pub const LOCAL_IDLE_SOAK_MODE: &str = "local_idle";
pub const COMMODITY_SOAK_MODE: &str = "commodity";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalIdleSoakConfig {
	pub format_version: u32,
	pub mode: String,
	pub duration_millis: u64,
	pub heartbeat_interval_millis: u64,
	pub locality_count: u64,
	pub lease_ttl_millis: u64,
	pub lease_renew_interval_millis: u64,
	pub primary_range_count: u64,
	pub evaluator_owned_raft_entries: u64,
	pub wendelon_process_starts: u64,
	pub ready_probe_count: u64,
	pub metrics_probe_count: u64,
	pub query_probe_count: u64,
}

impl LocalIdleSoakConfig {
	pub fn validate(&self) -> Result<(), String> {
		if self.format_version != LOCAL_IDLE_SOAK_FORMAT_VERSION {
			return Err(format!(
				"unsupported local-idle soak format {}; expected {}",
				self.format_version, LOCAL_IDLE_SOAK_FORMAT_VERSION
			));
		}
		if self.mode != LOCAL_IDLE_SOAK_MODE {
			return Err(format!(
				"local-idle soak mode must be {LOCAL_IDLE_SOAK_MODE}"
			));
		}
		if self.duration_millis == 0
			|| self.heartbeat_interval_millis == 0
			|| self.locality_count == 0
			|| self.lease_ttl_millis < 3
			|| self.lease_renew_interval_millis == 0
			|| self.primary_range_count == 0
			|| self.wendelon_process_starts == 0
		{
			return Err(
				"duration, heartbeat/lease intervals, locality/range counts, and process starts must be nonzero"
					.to_owned(),
			);
		}
		if self.lease_renew_interval_millis != self.lease_ttl_millis / 3 {
			return Err(
				"lease renewal interval must be derived as integer lease TTL / 3".to_owned(),
			);
		}
		Ok(())
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalIdleSoakSample {
	pub elapsed_millis: u64,
	pub heartbeat_interval_millis: u64,
	pub locality_count: u64,
	pub committed_heartbeat_delta: u64,
	pub lease_ttl_millis: u64,
	pub lease_renew_interval_millis: u64,
	pub primary_range_count: u64,
	pub committed_lease_grant_delta: u64,
	pub raft_entries_applied_delta: u64,
	pub audit_byte_delta: u64,
	pub audit_record_delta: u64,
	pub audit_operations: BTreeMap<String, u64>,
	/// Independently observed exact-heartbeat records materialized under
	/// `data.write_finalized`; expected to remain zero.
	pub heartbeat_data_write_audit_records: u64,
	pub audit_materialization_lag_entries: u64,
	pub healthy: bool,
	pub queryable: bool,
	pub warn_counts: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalIdleAuditBudget {
	pub per_operation_max: BTreeMap<String, u64>,
	pub total_record_max: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalIdleSoakSummary {
	pub format_version: u32,
	pub mode: String,
	pub passed: bool,
	pub failures: Vec<String>,
	pub duration_millis: u64,
	pub committed_heartbeats: u64,
	pub committed_heartbeat_max: u64,
	pub committed_lease_grants: u64,
	pub committed_lease_grant_max: u64,
	pub raft_entries_applied: u64,
	pub named_raft_entries: u64,
	pub raft_residual_entries: u64,
	pub raft_named_ahead_entries: u64,
	pub raft_boundary_allowance: u64,
	pub audit_bytes: u64,
	pub audit_records: u64,
	pub audit_operations: BTreeMap<String, u64>,
	pub audit_budget: LocalIdleAuditBudget,
	pub warn_counts: BTreeMap<String, u64>,
}

pub fn local_idle_audit_budget(config: &LocalIdleSoakConfig) -> LocalIdleAuditBudget {
	let duration_seconds = config.duration_millis.div_ceil(1_000);
	let mut per_operation_max = BTreeMap::new();
	per_operation_max.insert("crypto.startup".to_owned(), config.wendelon_process_starts);
	per_operation_max.insert(
		"audit.chain_root".to_owned(),
		duration_seconds.div_ceil(60).saturating_add(1),
	);
	per_operation_max.insert("observability.ready".to_owned(), config.ready_probe_count);
	per_operation_max.insert(
		"observability.metrics".to_owned(),
		config.metrics_probe_count,
	);
	let total_record_max = per_operation_max.values().copied().sum();
	LocalIdleAuditBudget {
		per_operation_max,
		total_record_max,
	}
}

pub fn evaluate_local_idle_soak(
	config: &LocalIdleSoakConfig,
	samples: &[LocalIdleSoakSample],
) -> Result<LocalIdleSoakSummary, String> {
	config.validate()?;
	if samples.is_empty() {
		return Err("local-idle soak requires at least one sample".to_owned());
	}

	let budget = local_idle_audit_budget(config);
	let mut failures = Vec::new();
	let mut committed_heartbeats = 0u64;
	let mut committed_lease_grants = 0u64;
	let mut raft_entries_applied = 0u64;
	let mut audit_bytes = 0u64;
	let mut audit_records = 0u64;
	let mut heartbeat_audit_records = 0u64;
	let mut audit_operations = BTreeMap::<String, u64>::new();
	let mut warn_counts = BTreeMap::<String, u64>::new();
	if config.query_probe_count != samples.len() as u64 {
		failures.push(format!(
			"query probe count {} does not equal sample count {}",
			config.query_probe_count,
			samples.len()
		));
	}

	for sample in samples {
		if sample.heartbeat_interval_millis != config.heartbeat_interval_millis
			|| sample.locality_count != config.locality_count
			|| sample.lease_ttl_millis != config.lease_ttl_millis
			|| sample.lease_renew_interval_millis != config.lease_renew_interval_millis
			|| sample.primary_range_count != config.primary_range_count
		{
			failures.push(format!(
				"sample at {} ms changed idle-writer configuration",
				sample.elapsed_millis
			));
		}
		committed_heartbeats =
			committed_heartbeats.saturating_add(sample.committed_heartbeat_delta);
		committed_lease_grants =
			committed_lease_grants.saturating_add(sample.committed_lease_grant_delta);
		raft_entries_applied =
			raft_entries_applied.saturating_add(sample.raft_entries_applied_delta);
		audit_bytes = audit_bytes.saturating_add(sample.audit_byte_delta);
		audit_records = audit_records.saturating_add(sample.audit_record_delta);
		heartbeat_audit_records =
			heartbeat_audit_records.saturating_add(sample.heartbeat_data_write_audit_records);
		for (operation, count) in &sample.audit_operations {
			let total = audit_operations.entry(operation.clone()).or_default();
			*total = total.saturating_add(*count);
		}
		for (source, count) in &sample.warn_counts {
			let total = warn_counts.entry(source.clone()).or_default();
			*total = total.saturating_add(*count);
		}
		if sample.audit_materialization_lag_entries != 0 {
			failures.push(format!(
				"audit materialization lag was {} at {} ms",
				sample.audit_materialization_lag_entries, sample.elapsed_millis
			));
		}
		if !sample.healthy || !sample.queryable {
			failures.push(format!(
				"deployment was not healthy/queryable at {} ms",
				sample.elapsed_millis
			));
		}
	}

	let interval_count = config
		.duration_millis
		.div_ceil(config.heartbeat_interval_millis);
	let committed_heartbeat_max = interval_count
		.saturating_mul(config.locality_count)
		// One scheduling-boundary allowance per locality.
		.saturating_add(config.locality_count);
	if committed_heartbeats > committed_heartbeat_max {
		failures.push(format!(
			"committed heartbeat count {committed_heartbeats} exceeds cadence/locality maximum {committed_heartbeat_max}"
		));
	}
	let lease_interval_count = config
		.duration_millis
		.div_ceil(config.lease_renew_interval_millis);
	let committed_lease_grant_max = lease_interval_count
		.saturating_mul(config.primary_range_count)
		// One scheduling-boundary allowance per primary range.
		.saturating_add(config.primary_range_count);
	if committed_lease_grants > committed_lease_grant_max {
		failures.push(format!(
			"committed lease-grant count {committed_lease_grants} exceeds cadence/range maximum {committed_lease_grant_max}"
		));
	}
	let named_raft_entries = committed_lease_grants
		.saturating_add(committed_heartbeats)
		.saturating_add(config.evaluator_owned_raft_entries);
	let raft_residual_entries = raft_entries_applied.saturating_sub(named_raft_entries);
	let raft_named_ahead_entries = named_raft_entries.saturating_sub(raft_entries_applied);
	let raft_boundary_allowance = config
		.primary_range_count
		.saturating_add(config.locality_count);
	if raft_residual_entries > raft_boundary_allowance {
		failures.push(format!(
			"unexplained idle Raft entries {raft_residual_entries} exceed named-writer boundary allowance {raft_boundary_allowance}"
		));
	}
	if raft_named_ahead_entries > raft_boundary_allowance {
		failures.push(format!(
			"named idle Raft counters lead total applied entries by {raft_named_ahead_entries}, exceeding boundary allowance {raft_boundary_allowance}"
		));
	}
	if heartbeat_audit_records != 0 {
		failures.push(format!(
			"{heartbeat_audit_records} exact CDC heartbeats appeared as data.write_finalized"
		));
	}
	for (source, count) in &warn_counts {
		if *count > 1 {
			failures.push(format!(
				"idle WARN source {source:?} repeated {count} times"
			));
		}
	}
	for (operation, count) in &audit_operations {
		let maximum = budget
			.per_operation_max
			.get(operation)
			.copied()
			.unwrap_or(0);
		if *count > maximum {
			failures.push(format!(
				"idle audit operation {operation:?} count {count} exceeds maximum {maximum}"
			));
		}
	}
	let startup_count = audit_operations.get("crypto.startup").copied().unwrap_or(0);
	if startup_count != config.wendelon_process_starts {
		failures.push(format!(
			"crypto.startup count {startup_count} does not equal Wendelon process starts {}",
			config.wendelon_process_starts
		));
	}
	if audit_records > budget.total_record_max {
		failures.push(format!(
			"total idle audit records {audit_records} exceed named-operation maximum {}",
			budget.total_record_max
		));
	}
	let operation_total: u64 = audit_operations.values().copied().sum();
	if operation_total != audit_records {
		failures.push(format!(
			"audit operation total {operation_total} does not reconcile with record delta {audit_records}"
		));
	}

	Ok(LocalIdleSoakSummary {
		format_version: LOCAL_IDLE_SOAK_FORMAT_VERSION,
		mode: LOCAL_IDLE_SOAK_MODE.to_owned(),
		passed: failures.is_empty(),
		failures,
		duration_millis: config.duration_millis,
		committed_heartbeats,
		committed_heartbeat_max,
		committed_lease_grants,
		committed_lease_grant_max,
		raft_entries_applied,
		named_raft_entries,
		raft_residual_entries,
		raft_named_ahead_entries,
		raft_boundary_allowance,
		audit_bytes,
		audit_records,
		audit_operations,
		audit_budget: budget,
		warn_counts,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn config() -> LocalIdleSoakConfig {
		LocalIdleSoakConfig {
			format_version: LOCAL_IDLE_SOAK_FORMAT_VERSION,
			mode: LOCAL_IDLE_SOAK_MODE.to_owned(),
			duration_millis: 2_000,
			heartbeat_interval_millis: 100,
			locality_count: 1,
			lease_ttl_millis: 300,
			lease_renew_interval_millis: 100,
			primary_range_count: 1,
			evaluator_owned_raft_entries: 0,
			wendelon_process_starts: 1,
			ready_probe_count: 1,
			metrics_probe_count: 1,
			query_probe_count: 1,
		}
	}

	fn sample() -> LocalIdleSoakSample {
		LocalIdleSoakSample {
			elapsed_millis: 2_000,
			heartbeat_interval_millis: 100,
			locality_count: 1,
			committed_heartbeat_delta: 20,
			lease_ttl_millis: 300,
			lease_renew_interval_millis: 100,
			primary_range_count: 1,
			committed_lease_grant_delta: 20,
			raft_entries_applied_delta: 40,
			audit_byte_delta: 500,
			audit_record_delta: 3,
			audit_operations: BTreeMap::from([
				("crypto.startup".to_owned(), 1),
				("observability.ready".to_owned(), 1),
				("observability.metrics".to_owned(), 1),
			]),
			heartbeat_data_write_audit_records: 0,
			audit_materialization_lag_entries: 0,
			healthy: true,
			queryable: true,
			warn_counts: BTreeMap::new(),
		}
	}

	#[test]
	fn clean_local_idle_evidence_passes() {
		let summary = evaluate_local_idle_soak(&config(), &[sample()]).unwrap();
		assert!(summary.passed, "{:?}", summary.failures);
	}

	#[test]
	fn evaluator_rejects_each_idle_failure_class() {
		let mut cases = Vec::new();
		let mut value = sample();
		value.committed_heartbeat_delta = 100;
		value.raft_entries_applied_delta = 120;
		cases.push(value);
		let mut value = sample();
		value.committed_lease_grant_delta = 100;
		value.raft_entries_applied_delta = 120;
		cases.push(value);
		let mut value = sample();
		value.heartbeat_data_write_audit_records = 1;
		cases.push(value);
		let mut value = sample();
		value.audit_materialization_lag_entries = 1;
		cases.push(value);
		let mut value = sample();
		value.healthy = false;
		cases.push(value);
		let mut value = sample();
		value.warn_counts.insert("wendelon".to_owned(), 2);
		cases.push(value);
		let mut value = sample();
		value.audit_operations.remove("crypto.startup");
		value.audit_record_delta -= 1;
		cases.push(value);

		for value in cases {
			assert!(
				!evaluate_local_idle_soak(&config(), &[value])
					.unwrap()
					.passed
			);
		}
	}

	#[test]
	fn unexpected_idle_raft_writer_trips_total_substrate_budget() {
		let mut value = sample();
		value.raft_entries_applied_delta += 4;
		let summary = evaluate_local_idle_soak(&config(), &[value]).unwrap();
		assert!(!summary.passed);
		assert_eq!(summary.raft_residual_entries, 4);
		assert!(
			summary
				.failures
				.iter()
				.any(|failure| failure.contains("unexplained idle Raft entries"))
		);
	}

	#[test]
	fn unexpected_idle_audit_operation_trips_operation_and_total_budgets() {
		let mut value = sample();
		value.audit_operations.insert("data.write".to_owned(), 3);
		value.audit_record_delta += 3;
		let summary = evaluate_local_idle_soak(&config(), &[value]).unwrap();
		assert!(!summary.passed);
		assert!(
			summary
				.failures
				.iter()
				.any(|failure| failure.contains("data.write"))
		);
		assert!(
			summary
				.failures
				.iter()
				.any(|failure| failure.contains("total idle audit records"))
		);
	}
}
