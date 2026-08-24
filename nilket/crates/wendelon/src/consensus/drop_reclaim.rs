//! ADR 0095 deterministic reclaim participant derivation.

use crate::consensus::raft_storage::SmState;
use crate::network::ring::{RingState, membership_from_epoch_config};
use crate::schema::table::{partitioned_prefix_width, table_possible_locality_ranges};
use crate::schema::{PartitionId, TableDropParticipantSet, TableSchemaDescriptor};
use std::io::{Error, ErrorKind};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DropParticipantStamp {
	pub ownership_epoch: u64,
	pub participants: TableDropParticipantSet,
}

/// Compute the immutable ADR 0095 participant stamp exclusively from the
/// replicated state visible to one finalize apply.
pub(crate) fn derive_drop_participant_stamp(
	state: &SmState,
	tenant_id: crate::schema::TenantId,
	descriptor: &TableSchemaDescriptor,
) -> std::io::Result<DropParticipantStamp> {
	let (_, epoch_config) = state.epoch_history.last_key_value().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"table drop finalize cannot stamp participants without a committed ring epoch",
		)
	})?;
	let tenant = state.tenants.get(&tenant_id).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			format!("table drop finalize cannot find tenant {}", tenant_id.get()),
		)
	})?;
	let membership = membership_from_epoch_config(epoch_config);
	let ring = RingState::build(&membership, tenant.replication_factor as usize);
	let ranges = table_possible_locality_ranges(tenant_id, descriptor)?;
	let mut participants = ring.replicas_intersecting_locality_ranges(&ranges);

	if let Some(prefix_bits) = partitioned_prefix_width(descriptor) {
		for (start, _) in &ranges {
			let partition = PartitionId::new(start.get());
			let derived = crate::schema::table::partition_for_locality(*start, prefix_bits)?;
			if derived != partition {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"descriptor locality range does not start at its partition representative",
				));
			}
			participants.extend(ring.replicas_for_partition(partition));
		}
	}

	for migration in state.migrations.values() {
		if ranges.iter().any(|(start, end)| {
			migration.range_start <= end.get() && start.get() <= migration.range_end
		}) {
			// Every source remains included after completion: migration completion
			// moves logical ownership but is not source-byte erasure evidence.
			participants.extend(migration.source_replicas.iter().copied());
		}
	}

	let participants = TableDropParticipantSet::new(participants);
	participants.validate().map_err(|error| {
		Error::new(
			ErrorKind::InvalidData,
			format!("table drop participant derivation failed: {error}"),
		)
	})?;
	Ok(DropParticipantStamp {
		ownership_epoch: epoch_config.epoch,
		participants,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::consensus::{
		EpochConfig, EpochPeer, MigrationLedgerEntry, MigrationState, TenantLifecycleState,
		TenantRecord,
	};
	use crate::integration::compute::SchemaGeneration;
	use crate::schema::{
		HybridTimestamp, MultiRowLayoutDescriptor, NodeId, PrimaryKeyDescriptor,
		RecordLayoutPolicy, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
		TableSpreadLocalityDescriptor, TenantId,
	};
	use arrow::datatypes::{DataType, Field, Schema};

	fn node(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	fn hts(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 1,
		}
	}

	fn epoch() -> EpochConfig {
		let peers = [1, 2, 3]
			.into_iter()
			.map(|id| {
				(
					id,
					EpochPeer {
						node_id: node(id),
						address: format!("127.0.0.1:{}", 7000 + id),
						tier_bits: 0,
						role_bits: 1,
						capability_bits: 0,
					},
				)
			})
			.collect();
		EpochConfig { epoch: 8, peers }
	}

	fn descriptor(id: u8) -> TableSchemaDescriptor {
		TableSchemaDescriptor::new_with_layout(
			TableObjectId::new([id; 16]),
			SchemaGeneration::new(1).get(),
			"analytics",
			"events",
			&Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			crate::schema::LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
				spread_factor: 1,
				prefix_bits: 16,
			}),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap()
	}

	fn state() -> SmState {
		let tenant_id = TenantId::new(7);
		let mut state = SmState::default();
		state.epoch_history.insert(8, epoch());
		state.tenants.insert(
			tenant_id,
			TenantRecord {
				tenant_id,
				state: TenantLifecycleState::Active,
				quota_bytes: u64::MAX,
				used_bytes: 0,
				replication_factor: 1,
			},
		);
		state
	}

	#[test]
	fn adr0095_mixed_placement_and_completed_sources_are_both_stamped() {
		let tenant_id = TenantId::new(7);
		let mut state = state();
		let membership = membership_from_epoch_config(state.epoch_history.get(&8).unwrap());
		let ring = RingState::build(&membership, 1);

		let (descriptor, partition_nodes, raw_nodes, ranges) = (1..=u8::MAX)
			.find_map(|id| {
				let descriptor = descriptor(id);
				let ranges = table_possible_locality_ranges(tenant_id, &descriptor).unwrap();
				let partition_nodes =
					ring.replicas_for_partition(PartitionId::new(ranges[0].0.get()));
				let raw_nodes = ring.replicas_intersecting_locality_ranges(&ranges);
				(raw_nodes.iter().any(|node| !partition_nodes.contains(node))).then_some((
					descriptor,
					partition_nodes,
					raw_nodes,
					ranges,
				))
			})
			.expect("fixture must distinguish raw-locality from partition ownership");

		state.migrations.insert(
			99,
			MigrationLedgerEntry {
				migration_id: 99,
				range_start: ranges[0].0.get(),
				range_end: ranges[0].1.get(),
				from_epoch: 7,
				to_epoch: 8,
				logical_owner: partition_nodes[0],
				upstream_migration_id: None,
				source_replicas: vec![node(77)],
				migration_hlc_floor: hts(1),
				state: MigrationState::Completed,
				completed_by: Some(partition_nodes[0]),
				completed_at_hlc: Some(hts(2)),
			},
		);
		state.migrations.insert(
			100,
			MigrationLedgerEntry {
				migration_id: 100,
				range_start: ranges[0].0.get(),
				range_end: ranges[0].1.get(),
				from_epoch: 8,
				to_epoch: 9,
				logical_owner: partition_nodes[0],
				upstream_migration_id: None,
				source_replicas: vec![node(88)],
				migration_hlc_floor: hts(3),
				state: MigrationState::BackfillInProgress,
				completed_by: None,
				completed_at_hlc: None,
			},
		);

		let stamp = derive_drop_participant_stamp(&state, tenant_id, &descriptor).unwrap();
		assert_eq!(stamp.ownership_epoch, 8);
		assert!(
			partition_nodes
				.iter()
				.all(|node| stamp.participants.contains(*node))
		);
		assert!(
			raw_nodes
				.iter()
				.all(|node| stamp.participants.contains(*node))
		);
		assert!(
			stamp.participants.contains(node(77)),
			"a completed migration source remains a physical-erasure participant"
		);
		assert!(
			stamp.participants.contains(node(88)),
			"an in-flight migration source is included even when it holds no fixture bytes"
		);
	}
}
