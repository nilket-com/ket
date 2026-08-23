//! ADR 0092: versioned snapshot-body replay.
//!
//! The snapshot body is `postcard(SmState)`. As with the WAL body, a
//! superseded version is decoded by its own frozen shape and upconverted.
//! What each version module freezes is the **changed container spine**: the
//! `SmState` field spine and the CTAS catalog/entry containers, because
//! derived Serde cannot substitute a leaf inside a derived container.
//! Unchanged field types are referenced from the live schema; the byte
//! fixtures pinning this shape fail the moment any referenced type's encoding
//! moves, which is when this module must fork its own copy.
//!
//! Replay machinery, not an API: nothing outside the snapshot decode dispatch
//! and the format-fixture controls may name these modules.

pub(crate) mod pre_v4_ctas {
	use crate::schema::{
		ClientWriteId, CtasExpectedBinding, CtasIntentFingerprint, CtasOperationId,
		CtasSagaEntry as CurrentEntry, CtasSagaPhase, CtasTerminalCause, TableSchemaDescriptor,
		TenantId,
	};
	use serde::{Deserialize, Serialize};
	use std::collections::BTreeMap;

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	pub(crate) struct Catalog {
		pub entries: BTreeMap<(TenantId, CtasOperationId), Entry>,
		pub expired_through_ms: BTreeMap<TenantId, u64>,
	}

	/// Snapshot-v2/v3 CTAS entry, before ADR 0094 added chain declaration
	/// and durable pin-cleanup state.
	#[derive(Clone, Debug, Serialize, Deserialize)]
	pub(crate) struct Entry {
		pub tenant_id: TenantId,
		pub operation_id: CtasOperationId,
		pub intent_fingerprint: CtasIntentFingerprint,
		pub descriptor: TableSchemaDescriptor,
		pub expected_binding: CtasExpectedBinding,
		pub prior_descriptor: Option<TableSchemaDescriptor>,
		pub row_write_id: ClientWriteId,
		pub row_count: Option<u64>,
		pub phase: CtasSagaPhase,
		pub terminal_cause: Option<CtasTerminalCause>,
		pub owner_epoch: u64,
		pub owner_lease_expires_at_ms: u64,
		pub retry_debt: u32,
		pub terminal_at_ms: Option<u64>,
		pub certificate: Option<Vec<u8>>,
	}

	impl From<Entry> for CurrentEntry {
		fn from(entry: Entry) -> Self {
			let declared = entry.row_count.is_some();
			let pins_released = !declared
				|| matches!(
					entry.phase,
					CtasSagaPhase::Published
						| CtasSagaPhase::RetiringOld
						| CtasSagaPhase::Complete
						| CtasSagaPhase::Conflict
				);
			Self {
				tenant_id: entry.tenant_id,
				operation_id: entry.operation_id,
				intent_fingerprint: entry.intent_fingerprint,
				descriptor: entry.descriptor,
				expected_binding: entry.expected_binding,
				prior_descriptor: entry.prior_descriptor,
				row_write_id: entry.row_write_id,
				row_count: entry.row_count,
				chunk_count: declared.then_some(1),
				chain_pins_released: pins_released,
				phase: entry.phase,
				terminal_cause: entry.terminal_cause,
				owner_epoch: entry.owner_epoch,
				owner_lease_expires_at_ms: entry.owner_lease_expires_at_ms,
				retry_debt: entry.retry_debt,
				terminal_at_ms: entry.terminal_at_ms,
				certificate: entry.certificate,
			}
		}
	}

	pub(super) fn convert(catalog: Catalog) -> crate::schema::CtasSagaCatalog {
		crate::schema::CtasSagaCatalog::from_replay_parts(
			catalog
				.entries
				.into_iter()
				.map(|(key, entry)| (key, entry.into()))
				.collect(),
			catalog.expired_through_ms,
		)
	}

	#[cfg(test)]
	pub(crate) fn project(catalog: crate::schema::CtasSagaCatalog) -> Catalog {
		let (entries, expired_through_ms) = catalog.into_replay_parts();
		Catalog {
			entries: entries
				.into_iter()
				.map(|(key, entry)| {
					(
						key,
						Entry {
							tenant_id: entry.tenant_id,
							operation_id: entry.operation_id,
							intent_fingerprint: entry.intent_fingerprint,
							descriptor: entry.descriptor,
							expected_binding: entry.expected_binding,
							prior_descriptor: entry.prior_descriptor,
							row_write_id: entry.row_write_id,
							row_count: entry.row_count,
							phase: entry.phase,
							terminal_cause: entry.terminal_cause,
							owner_epoch: entry.owner_epoch,
							owner_lease_expires_at_ms: entry.owner_lease_expires_at_ms,
							retry_debt: entry.retry_debt,
							terminal_at_ms: entry.terminal_at_ms,
							certificate: entry.certificate,
						},
					)
				})
				.collect(),
			expired_through_ms,
		}
	}
}

/// The version-1 snapshot shape: ADR 0085's `SmState`, before ADR 0087
/// appended the materialization-certificate catalog and the per-entry
/// certificate field.
pub(crate) mod v1 {
	use crate::consensus::core::{
		EpochConfig, MigratedRangeTombstone, MigrationLedgerEntry, RevokedActorKeyId, TenantRecord,
		TieredSlab,
	};
	use crate::consensus::raft_storage::SmState as CurrentSmState;
	use crate::crypto::TenantKeyset;
	use crate::integration::compute::LogicalTypeCatalog;
	use crate::schema::lifecycle::{TableDropFenceCatalog, TableLifecycleCatalog};
	use crate::schema::secondary_index::SecondaryIndexCatalog;
	use crate::schema::table::TableSchemaCatalog;
	use crate::schema::{
		ClientWriteId, CtasExpectedBinding, CtasIntentFingerprint, CtasOperationId,
		CtasSagaEntry as CurrentEntry, CtasSagaPhase, CtasTerminalCause, HybridTimestamp, SlabId,
		TableSchemaDescriptor, TenantId,
	};
	use openraft::{BasicNode, LogId, StoredMembership};
	use serde::{Deserialize, Serialize};
	use std::collections::{BTreeMap, HashMap};

	/// The v1 `SmState` field spine: eighteen fields, ending at the CTAS saga
	/// catalog. Field order is the contract and must never change.
	#[derive(Debug, Default, Serialize, Deserialize)]
	pub(crate) struct SmState {
		pub last_applied_log_id: Option<LogId<u32>>,
		pub last_membership: StoredMembership<u32, BasicNode>,
		pub epoch_history: BTreeMap<u64, EpochConfig>,
		pub leases: HashMap<u64, crate::schema::LeaseDescriptor>,
		pub e_safe_watermarks: HashMap<u64, HybridTimestamp>,
		pub last_e_safe_index: u64,
		pub tiered_slabs: HashMap<SlabId, TieredSlab>,
		pub migrations: HashMap<u64, MigrationLedgerEntry>,
		pub tombstones: Vec<MigratedRangeTombstone>,
		pub tenants: HashMap<TenantId, TenantRecord>,
		pub tenant_keysets: HashMap<TenantId, TenantKeyset>,
		pub revoked_actor_keys: BTreeMap<RevokedActorKeyId, u64>,
		pub table_schemas: TableSchemaCatalog,
		pub logical_types: LogicalTypeCatalog,
		pub secondary_indexes: SecondaryIndexCatalog,
		pub table_lifecycle: TableLifecycleCatalog,
		pub table_drop_fences: TableDropFenceCatalog,
		pub ctas_sagas: CtasSagaCatalog,
	}

	/// The v1 CTAS catalog container, holding v1 entries.
	#[derive(Debug, Default, Serialize, Deserialize)]
	pub(crate) struct CtasSagaCatalog {
		pub entries: BTreeMap<(TenantId, CtasOperationId), CtasSagaEntry>,
		pub expired_through_ms: BTreeMap<TenantId, u64>,
	}

	/// The v1 saga entry: the current shape minus the trailing certificate.
	#[derive(Debug, Serialize, Deserialize)]
	pub(crate) struct CtasSagaEntry {
		pub tenant_id: TenantId,
		pub operation_id: CtasOperationId,
		pub intent_fingerprint: CtasIntentFingerprint,
		pub descriptor: TableSchemaDescriptor,
		pub expected_binding: CtasExpectedBinding,
		pub prior_descriptor: Option<TableSchemaDescriptor>,
		pub row_write_id: ClientWriteId,
		pub row_count: Option<u64>,
		pub phase: CtasSagaPhase,
		pub terminal_cause: Option<CtasTerminalCause>,
		pub owner_epoch: u64,
		pub owner_lease_expires_at_ms: u64,
		pub retry_debt: u32,
		pub terminal_at_ms: Option<u64>,
	}

	/// The total v1 → current entry conversion: every field moves and the
	/// certificate becomes `None`, which is exactly what every v1 writer
	/// meant — pre-0087 sagas had no certificates.
	impl From<CtasSagaEntry> for CurrentEntry {
		fn from(entry: CtasSagaEntry) -> Self {
			let declared = entry.row_count.is_some();
			let pins_released = !declared
				|| matches!(
					entry.phase,
					CtasSagaPhase::Published
						| CtasSagaPhase::RetiringOld
						| CtasSagaPhase::Complete
						| CtasSagaPhase::Conflict
				);
			Self {
				tenant_id: entry.tenant_id,
				operation_id: entry.operation_id,
				intent_fingerprint: entry.intent_fingerprint,
				descriptor: entry.descriptor,
				expected_binding: entry.expected_binding,
				prior_descriptor: entry.prior_descriptor,
				row_write_id: entry.row_write_id,
				row_count: entry.row_count,
				chunk_count: entry.row_count.map(|_| 1),
				chain_pins_released: pins_released,
				phase: entry.phase,
				terminal_cause: entry.terminal_cause,
				owner_epoch: entry.owner_epoch,
				owner_lease_expires_at_ms: entry.owner_lease_expires_at_ms,
				retry_debt: entry.retry_debt,
				terminal_at_ms: entry.terminal_at_ms,
				certificate: None,
			}
		}
	}

	/// Decode a v1 snapshot body and upconvert it. The conversion is
	/// complete: every catalog entry is converted — the certificate sits
	/// per-entry, mid-stream inside the map, so an empty-catalog shortcut
	/// would silently drop active sagas — and the materialization catalog
	/// starts empty, because no v1 writer ever attached a certificate.
	pub(crate) fn decode_state(body: &[u8]) -> std::io::Result<CurrentSmState> {
		let (state, rest) = postcard::take_from_bytes::<SmState>(body).map_err(|err| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("snapshot postcard decode failed: {err}"),
			)
		})?;
		if !rest.is_empty() {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("snapshot decoded with {} unconsumed body bytes", rest.len()),
			));
		}
		let SmState {
			last_applied_log_id,
			last_membership,
			epoch_history,
			leases,
			e_safe_watermarks,
			last_e_safe_index,
			tiered_slabs,
			migrations,
			tombstones,
			tenants,
			tenant_keysets,
			revoked_actor_keys,
			table_schemas,
			logical_types,
			secondary_indexes,
			table_lifecycle,
			table_drop_fences,
			ctas_sagas,
		} = state;
		let CtasSagaCatalog {
			entries,
			expired_through_ms,
		} = ctas_sagas;
		let converted_sagas = crate::schema::CtasSagaCatalog::from_replay_parts(
			entries
				.into_iter()
				.map(|(key, entry)| (key, entry.into()))
				.collect(),
			expired_through_ms,
		);
		Ok(CurrentSmState {
			last_applied_log_id,
			last_membership,
			epoch_history,
			leases,
			e_safe_watermarks,
			last_e_safe_index,
			tiered_slabs,
			migrations,
			tombstones,
			tenants,
			tenant_keysets,
			revoked_actor_keys,
			table_schemas,
			logical_types,
			secondary_indexes,
			table_lifecycle,
			table_drop_fences,
			table_drop_outcomes: Default::default(),
			ctas_sagas: converted_sagas,
			materialization_certificates: Default::default(),
		})
	}
}

/// The version-2 snapshot shape: the live state after ADR 0087/0092 and
/// before ADR 0093 appended durable DROP outcomes.
pub(crate) mod v2 {
	use super::pre_v4_ctas;
	use crate::consensus::core::{
		EpochConfig, MigratedRangeTombstone, MigrationLedgerEntry, RevokedActorKeyId, TenantRecord,
		TieredSlab,
	};
	use crate::consensus::raft_storage::SmState as CurrentSmState;
	use crate::crypto::TenantKeyset;
	use crate::integration::compute::LogicalTypeCatalog;
	use crate::schema::lifecycle::{TableDropFenceCatalog, TableLifecycleCatalog};
	use crate::schema::secondary_index::SecondaryIndexCatalog;
	use crate::schema::table::TableSchemaCatalog;
	use crate::schema::{HybridTimestamp, MaterializationCertificateCatalog, SlabId, TenantId};
	use openraft::{BasicNode, LogId, StoredMembership};
	use serde::{Deserialize, Serialize};
	use std::collections::{BTreeMap, HashMap};

	/// The v2 `SmState` field spine. Field order is the postcard contract.
	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	pub(crate) struct SmState {
		pub last_applied_log_id: Option<LogId<u32>>,
		pub last_membership: StoredMembership<u32, BasicNode>,
		pub epoch_history: BTreeMap<u64, EpochConfig>,
		pub leases: HashMap<u64, crate::schema::LeaseDescriptor>,
		pub e_safe_watermarks: HashMap<u64, HybridTimestamp>,
		pub last_e_safe_index: u64,
		pub tiered_slabs: HashMap<SlabId, TieredSlab>,
		pub migrations: HashMap<u64, MigrationLedgerEntry>,
		pub tombstones: Vec<MigratedRangeTombstone>,
		pub tenants: HashMap<TenantId, TenantRecord>,
		pub tenant_keysets: HashMap<TenantId, TenantKeyset>,
		pub revoked_actor_keys: BTreeMap<RevokedActorKeyId, u64>,
		pub table_schemas: TableSchemaCatalog,
		pub logical_types: LogicalTypeCatalog,
		pub secondary_indexes: SecondaryIndexCatalog,
		pub table_lifecycle: TableLifecycleCatalog,
		pub table_drop_fences: TableDropFenceCatalog,
		pub ctas_sagas: pre_v4_ctas::Catalog,
		pub materialization_certificates: MaterializationCertificateCatalog,
	}

	pub(crate) fn decode_state(body: &[u8]) -> std::io::Result<CurrentSmState> {
		let (state, rest) = postcard::take_from_bytes::<SmState>(body).map_err(|err| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("snapshot postcard decode failed: {err}"),
			)
		})?;
		if !rest.is_empty() {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("snapshot decoded with {} unconsumed body bytes", rest.len()),
			));
		}
		let SmState {
			last_applied_log_id,
			last_membership,
			epoch_history,
			leases,
			e_safe_watermarks,
			last_e_safe_index,
			tiered_slabs,
			migrations,
			tombstones,
			tenants,
			tenant_keysets,
			revoked_actor_keys,
			table_schemas,
			logical_types,
			secondary_indexes,
			table_lifecycle,
			table_drop_fences,
			ctas_sagas,
			materialization_certificates,
		} = state;
		Ok(CurrentSmState {
			last_applied_log_id,
			last_membership,
			epoch_history,
			leases,
			e_safe_watermarks,
			last_e_safe_index,
			tiered_slabs,
			migrations,
			tombstones,
			tenants,
			tenant_keysets,
			revoked_actor_keys,
			table_schemas,
			logical_types,
			secondary_indexes,
			table_lifecycle,
			table_drop_fences,
			table_drop_outcomes: Default::default(),
			ctas_sagas: pre_v4_ctas::convert(ctas_sagas),
			materialization_certificates,
		})
	}
}

/// Snapshot v3: ADR 0093 added retained DROP outcomes; ADR 0094 had not yet
/// added CTAS chain declaration and pin-cleanup fields.
pub(crate) mod v3 {
	use super::pre_v4_ctas;
	use crate::consensus::core::{
		EpochConfig, MigratedRangeTombstone, MigrationLedgerEntry, RevokedActorKeyId, TenantRecord,
		TieredSlab,
	};
	use crate::consensus::raft_storage::SmState as CurrentSmState;
	use crate::crypto::TenantKeyset;
	use crate::integration::compute::LogicalTypeCatalog;
	use crate::schema::lifecycle::{
		TableDropFenceCatalog, TableDropOutcomeCatalog, TableLifecycleCatalog,
	};
	use crate::schema::secondary_index::SecondaryIndexCatalog;
	use crate::schema::table::TableSchemaCatalog;
	use crate::schema::{HybridTimestamp, MaterializationCertificateCatalog, SlabId, TenantId};
	use openraft::{BasicNode, LogId, StoredMembership};
	use serde::{Deserialize, Serialize};
	use std::collections::{BTreeMap, HashMap};

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	pub(crate) struct SmState {
		pub last_applied_log_id: Option<LogId<u32>>,
		pub last_membership: StoredMembership<u32, BasicNode>,
		pub epoch_history: BTreeMap<u64, EpochConfig>,
		pub leases: HashMap<u64, crate::schema::LeaseDescriptor>,
		pub e_safe_watermarks: HashMap<u64, HybridTimestamp>,
		pub last_e_safe_index: u64,
		pub tiered_slabs: HashMap<SlabId, TieredSlab>,
		pub migrations: HashMap<u64, MigrationLedgerEntry>,
		pub tombstones: Vec<MigratedRangeTombstone>,
		pub tenants: HashMap<TenantId, TenantRecord>,
		pub tenant_keysets: HashMap<TenantId, TenantKeyset>,
		pub revoked_actor_keys: BTreeMap<RevokedActorKeyId, u64>,
		pub table_schemas: TableSchemaCatalog,
		pub logical_types: LogicalTypeCatalog,
		pub secondary_indexes: SecondaryIndexCatalog,
		pub table_lifecycle: TableLifecycleCatalog,
		pub table_drop_fences: TableDropFenceCatalog,
		pub table_drop_outcomes: TableDropOutcomeCatalog,
		pub ctas_sagas: pre_v4_ctas::Catalog,
		pub materialization_certificates: MaterializationCertificateCatalog,
	}

	pub(crate) fn decode_state(body: &[u8]) -> std::io::Result<CurrentSmState> {
		let (state, rest) = postcard::take_from_bytes::<SmState>(body).map_err(|err| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("snapshot postcard decode failed: {err}"),
			)
		})?;
		if !rest.is_empty() {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("snapshot decoded with {} unconsumed body bytes", rest.len()),
			));
		}
		Ok(CurrentSmState {
			last_applied_log_id: state.last_applied_log_id,
			last_membership: state.last_membership,
			epoch_history: state.epoch_history,
			leases: state.leases,
			e_safe_watermarks: state.e_safe_watermarks,
			last_e_safe_index: state.last_e_safe_index,
			tiered_slabs: state.tiered_slabs,
			migrations: state.migrations,
			tombstones: state.tombstones,
			tenants: state.tenants,
			tenant_keysets: state.tenant_keysets,
			revoked_actor_keys: state.revoked_actor_keys,
			table_schemas: state.table_schemas,
			logical_types: state.logical_types,
			secondary_indexes: state.secondary_indexes,
			table_lifecycle: state.table_lifecycle,
			table_drop_fences: state.table_drop_fences,
			table_drop_outcomes: state.table_drop_outcomes,
			ctas_sagas: pre_v4_ctas::convert(state.ctas_sagas),
			materialization_certificates: state.materialization_certificates,
		})
	}
}
