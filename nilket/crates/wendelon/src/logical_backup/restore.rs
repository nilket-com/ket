//! ADR 0736: verification and restore of a published backup.
//!
//! [`verify`] is R1: before the target is touched, the key unwraps, the
//! manifest authenticates and is well formed, the chunk directory holds exactly
//! its chunks, and every chunk's length, hash, tag, associated data, rows and
//! schema match the pinned manifest; each table's canonical digest is recomputed
//! and equal. [`restore`] takes a [`Verified`] backup, so nothing reaches the
//! target before R1 has passed; it checks that the target is fresh (only
//! the formation's default tenant, nothing in the catalog, no KV, settled),
//! establishes the roster through the public routes, imports every chunk as one
//! table write under a freshly minted operation id (re-offered with the SAME id
//! only while its outcome is unknown, within the id's lifetime), and finally,
//! under a hold on the target, requires the target's roster to equal the
//! backup's and every table's count and digest to equal the manifest's. A
//! failed restore is reported and discarded; it is never resumed.

use super::artifact::{DataKey, MAX_MANIFEST_BYTES, WRAP_BYTES};
use super::digest::{TableDigest, canonical_schema};
use super::export::{
	BackupRefusal, Clients, ExportError, digest_batch, next_batch, query_table, random_u128,
	settled_roster, terminal_rows,
};
use super::format::{
	CHUNK_DIR, ChunkEntry, KEY_FILE, MANIFEST_FILE, MANIFEST_HEADER_BYTES, Manifest, chunk_header,
	chunk_path, decode_batch, manifest_backup_id, manifest_header, sha256,
};
use super::hold::{Bounds, HoldError, Release};
use super::roster::BackupRoster;
use crate::client::core::{TableWriteRequest, WendelonClient};
use crate::integration::compute::{
	LOGICAL_TYPE_INTERNAL_NAMESPACE, LogicalTypeDefinitionCreate, SchemaGeneration, SchemaIntentId,
	SchemaMutationKind, SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope,
	encode_logical_type_definition_create,
};
use crate::network::control_server::TenantAdminRequestWire;
use crate::schema::TABLE_WRITE_MAX_ROWS;
use crate::schema::{
	ClientWriteId, MutationAnswer, TableObjectId, TableSchemaDescriptor, TenantId, WriteStatus,
	encode_secondary_index_descriptor, encode_table_schema_descriptor_v1,
	encode_table_write_row_count, mutation_answer,
};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Read};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long one chunk's write is re-offered while its outcome is unknown; well
/// inside its operation id's idempotency lifetime ([`WRITE_TTL_MS`]).
const WRITE_BOUND: Duration = Duration::from_secs(45);
const WRITE_TTL_MS: u32 = 60_000;
const WRITE_RETRY: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestoreRefusal {
	/// R1: the artifact is not what its manifest pins.
	Artifact { why: String },
	/// A tenant holds a table and no storage credential for it on the target
	/// was given (the default tenant 0 can have none).
	NoDataCredential { tenant_id: TenantId },
	/// R2: the target holds something besides what formation creates.
	TargetNotFresh { why: String },
	/// A setting differs on the target and no route reproduces it.
	SettingNotReproducible { what: String },
	/// The target's own hold or roster refused.
	Target(BackupRefusal),
	/// R4: the cluster recorded a refusal for a chunk's write.
	RowsRefused { ordinal: u64, status: u8 },
	/// R4: a chunk's write outcome stayed unknown within its bound.
	RowsUnknown { ordinal: u64, why: String },
	/// R5: the target's roster is not the backup's.
	RosterMismatch { why: String },
	/// R5: a table's count or digest on the target is not the manifest's.
	VerificationFailed {
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		why: String,
	},
	/// Wendelon ADR 0121 D4: the target binds a positive retention window and the
	/// backup holds a table it would govern; nothing was established or imported.
	/// Appended.
	RetentionWindowBound {
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		window_ms: u64,
	},
	/// Wendelon ADR 0121 R1: the backup holds a table a positive window would
	/// govern and the target gave no explicit Disabled binding (unbound, or an
	/// answer without one); nothing was established or imported. Appended.
	RetentionBindingUnknown {
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	},
}

#[derive(Debug)]
pub enum RestoreError {
	Refused(RestoreRefusal),
	Io(Error),
	/// A hold on the target was not entered, or its entry was not confirmed.
	Hold(HoldError),
	/// The step under a hold on the target ended (`outcome`) but that hold's
	/// release did not succeed.
	HoldNotReleased {
		id: u128,
		entered_at: u64,
		outcome: String,
		release: Release,
	},
}

impl From<Error> for RestoreError {
	fn from(error: Error) -> Self {
		Self::Io(error)
	}
}

impl std::fmt::Display for RestoreError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Refused(refusal) => write!(f, "restore refused: {refusal:?}"),
			Self::Io(error) => write!(f, "restore failed: {error}"),
			Self::Hold(error) => write!(f, "restore stopped at the target's hold: {error}"),
			Self::HoldNotReleased {
				id,
				entered_at,
				outcome,
				release,
			} => write!(
				f,
				"{outcome}; the target's hold (id {id:#x}, entered at {entered_at}) release was \
				 {release:?}: release it with `backup release --id {id:#x} --entered-at \
				 {entered_at}`"
			),
		}
	}
}

fn artifact(why: impl Into<String>) -> RestoreError {
	RestoreError::Refused(RestoreRefusal::Artifact { why: why.into() })
}

/// A backup that passed R1.
pub struct Verified {
	pub manifest: Manifest,
	key: DataKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreReport {
	pub backup_id: u128,
	pub tables: usize,
	pub rows: u64,
	pub chunks: u64,
	/// The target cut at which its roster and every table were verified.
	pub verified_cut: u64,
}

/// R1 over the backup in `dir`.
pub fn verify(dir: &Path, passphrase: &[u8]) -> Result<Verified, RestoreError> {
	let wrapped = read_bounded(&dir.join(KEY_FILE), WRAP_BYTES as u64)?;
	let key = DataKey::unwrap(&wrapped, passphrase).map_err(|error| artifact(error.to_string()))?;
	let file = read_bounded(
		&dir.join(MANIFEST_FILE),
		(MANIFEST_HEADER_BYTES + MAX_MANIFEST_BYTES) as u64,
	)?;
	let backup_id = manifest_backup_id(&file).map_err(|error| artifact(error.to_string()))?;
	let opened = key
		.open_manifest(&manifest_header(backup_id), &file[MANIFEST_HEADER_BYTES..])
		.map_err(|error| artifact(error.to_string()))?;
	let manifest = Manifest::decode(&opened).map_err(|error| artifact(error.to_string()))?;
	if manifest.backup_id != backup_id || manifest.roster.hold.0 != backup_id {
		return Err(artifact("the manifest names another backup"));
	}
	// Exactly the pinned chunk files, and nothing else; enumeration stops as
	// soon as it passes the manifest's count.
	let mut names = Vec::new();
	for entry in std::fs::read_dir(dir.join(CHUNK_DIR))? {
		if names.len() == verified_chunk_limit(&manifest) {
			return Err(artifact(
				"the chunk directory holds more than the manifest's chunks",
			));
		}
		names.push(entry?.file_name());
	}
	names.sort();
	let expected: Vec<_> = (1..=manifest.chunks.len() as u64)
		.map(|ordinal| std::ffi::OsString::from(format!("{ordinal:010}")))
		.collect();
	if names != expected {
		return Err(artifact("the chunk directory is not the manifest's chunks"));
	}
	let verified = Verified { manifest, key };
	for (table, rostered) in verified
		.manifest
		.tables
		.iter()
		.zip(&verified.manifest.roster.tables)
	{
		let descriptor = &rostered.descriptor;
		let mut digest = TableDigest::new(canonical_schema(&descriptor.arrow_schema()?));
		let range =
			(table.first_chunk - 1) as usize..(table.first_chunk + table.chunk_count - 1) as usize;
		for entry in &verified.manifest.chunks[range] {
			let batch = open_chunk(dir, &verified, entry, descriptor)?;
			digest_batch(&mut digest, table.tenant_id, descriptor, &batch)
				.map_err(|refusal| artifact(format!("{refusal:?}")))?;
		}
		if digest.rows() as u64 != table.rows || digest.finish() != table.digest {
			return Err(artifact(format!(
				"table {:?} of tenant {} does not match its digest",
				table.table_object_id,
				table.tenant_id.get()
			)));
		}
	}
	Ok(verified)
}

fn verified_chunk_limit(manifest: &Manifest) -> usize {
	manifest.chunks.len()
}

/// One chunk, read again and checked against the pinned manifest.
fn open_chunk(
	dir: &Path,
	verified: &Verified,
	entry: &ChunkEntry,
	descriptor: &TableSchemaDescriptor,
) -> Result<RecordBatch, RestoreError> {
	let sealed = read_bounded(&chunk_path(dir, entry.ordinal), entry.sealed_len)?;
	if sealed.len() as u64 != entry.sealed_len || sha256(&sealed) != entry.sealed_sha256 {
		return Err(artifact(format!(
			"chunk {} is not the pinned one",
			entry.ordinal
		)));
	}
	let header = chunk_header(
		verified.manifest.backup_id,
		entry.tenant_id,
		entry.table_object_id,
		entry.schema_generation,
		entry.ordinal,
	);
	let plain = verified
		.key
		.open_chunk(entry.ordinal, &header, &sealed)
		.map_err(|error| artifact(format!("chunk {}: {error}", entry.ordinal)))?;
	let batch = decode_batch(&plain).map_err(|error| artifact(error.to_string()))?;
	if batch.num_rows() != entry.rows as usize
		|| batch.num_rows() > TABLE_WRITE_MAX_ROWS
		|| canonical_schema(&batch.schema()) != canonical_schema(&descriptor.arrow_schema()?)
	{
		return Err(artifact(format!(
			"chunk {} does not hold its pinned rows of its table",
			entry.ordinal
		)));
	}
	Ok(batch)
}

/// Reads at most `limit` bytes; a longer file is refused before it is read.
fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>, RestoreError> {
	let file = std::fs::File::open(path)?;
	if file.metadata()?.len() > limit {
		return Err(artifact(format!(
			"{} is longer than admitted",
			path.display()
		)));
	}
	let mut bytes = Vec::new();
	file.take(limit + 1).read_to_end(&mut bytes)?;
	if bytes.len() as u64 > limit {
		return Err(artifact(format!("{} grew while read", path.display())));
	}
	Ok(bytes)
}

/// Restores the `verified` backup in `dir` into the fresh cluster `clients`
/// reach. Each chunk is read again and revalidated against the pinned
/// manifest before it is imported.
pub async fn restore(
	clients: &Clients,
	dir: &Path,
	verified: &Verified,
) -> Result<RestoreReport, RestoreError> {
	restore_bounded(clients, dir, verified, Bounds::DEFAULT).await
}

/// [`restore`] under the budgets `bounds`.
pub async fn restore_bounded(
	clients: &Clients,
	dir: &Path,
	verified: &Verified,
	bounds: Bounds,
) -> Result<RestoreReport, RestoreError> {
	let roster = &verified.manifest.roster;
	clients.cover(roster).map_err(|tenant_id| {
		RestoreError::Refused(RestoreRefusal::NoDataCredential { tenant_id })
	})?;
	let client = clients.admin.as_ref();
	let target = under_target_hold(client, bounds, |token| async move {
		let target = target_roster(client, token, bounds).await?;
		check_fresh(&target)?;
		Ok(target)
	})
	.await?;
	// Wendelon ADR 0121 D4: before anything is established or imported, the
	// incoming roster against the target's authoritative window binding.
	check_retention(client, roster, bounds).await?;
	establish(client, roster, &target).await?;
	for table in &verified.manifest.tables {
		let rostered = roster
			.tables
			.iter()
			.find(|rostered| {
				(rostered.tenant_id, rostered.descriptor.table_object_id)
					== (table.tenant_id, table.table_object_id)
			})
			.expect("the manifest's tables are the roster's");
		let range =
			(table.first_chunk - 1) as usize..(table.first_chunk + table.chunk_count - 1) as usize;
		for entry in &verified.manifest.chunks[range] {
			let batch = open_chunk(dir, verified, entry, &rostered.descriptor)?;
			import_chunk(
				clients.data_for(table.tenant_id),
				entry,
				&rostered.descriptor,
				batch,
			)
			.await?;
		}
	}
	// Under one hold: the target's roster is the backup's, and every table
	// holds exactly the backup's rows.
	let manifest = &verified.manifest;
	let verified_cut = under_target_hold(client, bounds, |token| async move {
		let restored = target_roster(client, token, bounds).await?;
		check_restored(roster, &restored)?;
		verify_tables(clients, manifest, bounds).await?;
		Ok(restored.cut)
	})
	.await?;
	Ok(RestoreReport {
		backup_id: manifest.backup_id,
		tables: manifest.tables.len(),
		rows: manifest.tables.iter().map(|table| table.rows).sum(),
		chunks: manifest.chunks.len() as u64,
		verified_cut,
	})
}

/// Runs `step` under a hold of its own on the target, released on every path.
async fn under_target_hold<T, F, Fut>(
	client: &WendelonClient,
	bounds: Bounds,
	step: F,
) -> Result<T, RestoreError>
where
	F: FnOnce((u128, u64)) -> Fut,
	Fut: std::future::Future<Output = Result<T, RestoreError>>,
{
	let token = super::hold::enter(client, random_u128()?, bounds)
		.await
		.map_err(RestoreError::Hold)?;
	let outcome = step(token).await;
	match super::hold::release(client, token, bounds).await {
		Release::Released => outcome,
		release => Err(RestoreError::HoldNotReleased {
			id: token.0,
			entered_at: token.1,
			outcome: match &outcome {
				Ok(_) => "the step under the hold completed".to_owned(),
				Err(error) => error.to_string(),
			},
			release,
		}),
	}
}

/// The target's settled roster under `token`.
async fn target_roster(
	client: &WendelonClient,
	token: (u128, u64),
	bounds: Bounds,
) -> Result<BackupRoster, RestoreError> {
	match settled_roster(client, token, bounds).await {
		Ok(roster) => Ok(roster),
		Err(ExportError::Refused(refusal)) => {
			Err(RestoreError::Refused(RestoreRefusal::Target(refusal)))
		}
		Err(ExportError::Io(error)) => Err(RestoreError::Io(error)),
		Err(other) => Err(RestoreError::Io(Error::other(other.to_string()))),
	}
}

/// Wendelon ADR 0121 D4, R1: a target store that binds a positive ParentDecisions window
/// would govern (hide by age) every AppendOnly table restored with the cluster's window, so
/// such a backup is refused there before anything is established or imported. Only an
/// explicit Disabled binding (`Some(0)`) is evidence to proceed: an unbound store, or an
/// answer carrying no binding (a bare reply decodes to `None`), is refused too. A backup
/// with no such table never asks.
async fn check_retention(
	client: &WendelonClient,
	roster: &BackupRoster,
	bounds: Bounds,
) -> Result<(), RestoreError> {
	let Some(table) = governed_incoming(roster) else {
		return Ok(());
	};
	let answer = tokio::time::timeout(
		bounds.call,
		client.retention_admin(&TenantAdminRequestWire::GetRetentionBinding),
	)
	.await
	.map_err(|_| {
		RestoreError::Io(Error::new(
			ErrorKind::TimedOut,
			format!(
				"no retention binding answer within {} s",
				bounds.call.as_secs()
			),
		))
	})??;
	retention_admission((table.tenant_id, table.descriptor.table_object_id), answer)
}

/// The first incoming table a positive window would govern: AppendOnly, inheriting the
/// cluster's window.
fn governed_incoming(roster: &BackupRoster) -> Option<&super::roster::RosterTable> {
	roster.tables.iter().find(|table| {
		matches!(
			table.descriptor.row_identity_policy,
			crate::schema::RowIdentityPolicy::AppendOnly
		) && table.retention == crate::schema::TableRetentionPolicy::InheritCluster
	})
}

/// Whether a restore of `table` may proceed on the target's answer to
/// `GetRetentionBinding`: only an accepted answer naming the Disabled binding.
fn retention_admission(
	(tenant_id, table_object_id): (TenantId, TableObjectId),
	(response, extension): (
		crate::network::control_server::TenantAdminResponseWire,
		crate::network::control_server::RetentionAdminExtensionWire,
	),
) -> Result<(), RestoreError> {
	if !response.accepted {
		return Err(RestoreError::Io(Error::other(format!(
			"the target refused the retention binding read: {:?}",
			response.message
		))));
	}
	match extension.window_ms {
		Some(0) => Ok(()),
		Some(window_ms) => Err(RestoreError::Refused(
			RestoreRefusal::RetentionWindowBound {
				tenant_id,
				table_object_id,
				window_ms,
			},
		)),
		None => Err(RestoreError::Refused(
			RestoreRefusal::RetentionBindingUnknown {
				tenant_id,
				table_object_id,
			},
		)),
	}
}

/// R2: nothing but the formation's default tenant.
fn check_fresh(target: &BackupRoster) -> Result<(), RestoreError> {
	let not_fresh = |why: &str| {
		Err(RestoreError::Refused(RestoreRefusal::TargetNotFresh {
			why: why.to_owned(),
		}))
	};
	if target
		.tenants
		.iter()
		.any(|tenant| tenant.tenant_id != TenantId::ZERO)
	{
		return not_fresh("the target has tenants besides the default one");
	}
	if !target.logical_types.is_empty() || !target.tables.is_empty() || !target.indexes.is_empty() {
		return not_fresh("the target's catalog is not empty");
	}
	Ok(())
}

/// R5: the restored roster is the backup's (its hold and cut aside).
fn check_restored(backup: &BackupRoster, restored: &BackupRoster) -> Result<(), RestoreError> {
	let mismatch = |what: &str| {
		Err(RestoreError::Refused(RestoreRefusal::RosterMismatch {
			why: what.to_owned(),
		}))
	};
	if backup.tenants != restored.tenants {
		return mismatch("tenants");
	}
	if backup.logical_types != restored.logical_types {
		return mismatch("logical types");
	}
	if backup.tables != restored.tables {
		return mismatch("tables");
	}
	if backup.indexes != restored.indexes {
		return mismatch("indexes");
	}
	Ok(())
}

/// R3: tenants, logical types (ascending id), tables, then secondary indexes,
/// each through its public route. The default tenant is compared, not created.
async fn establish(
	client: &WendelonClient,
	roster: &BackupRoster,
	target: &BackupRoster,
) -> Result<(), RestoreError> {
	let existing: BTreeMap<_, _> = target
		.tenants
		.iter()
		.map(|tenant| (tenant.tenant_id, tenant))
		.collect();
	for tenant in &roster.tenants {
		match existing.get(&tenant.tenant_id) {
			Some(present) if *present == tenant => {}
			Some(present) => {
				return Err(RestoreError::Refused(
					RestoreRefusal::SettingNotReproducible {
						what: format!(
							"tenant {} is {present:?} on the target, {tenant:?} in the backup",
							tenant.tenant_id.get()
						),
					},
				));
			}
			None => {
				client
					.tenant_admin(&TenantAdminRequestWire::Create {
						target_tenant_id: tenant.tenant_id,
						quota_bytes: tenant.quota_bytes,
						replication_factor: tenant.replication_factor,
					})
					.await?;
			}
		}
	}
	for (tenant_id, definition) in &roster.logical_types {
		let payload = encode_logical_type_definition_create(&LogicalTypeDefinitionCreate {
			name: definition.name.clone(),
			physical: definition.physical,
			refinement: definition.refinement.clone(),
			unit: definition.unit.clone(),
		})?;
		client
			.submit_schema_update_envelope(&SchemaUpdateEnvelope::compute_client_intent(
				*tenant_id,
				SchemaObjectRef {
					kind: SchemaObjectKind::LogicalType,
					namespace: LOGICAL_TYPE_INTERNAL_NAMESPACE.to_owned(),
					name: definition.name.clone(),
				},
				SchemaGeneration::ZERO,
				SchemaGeneration::new(1),
				intent_id()?,
				SchemaMutationKind::Create,
				payload,
			))
			.await?;
	}
	for table in &roster.tables {
		let descriptor = &table.descriptor;
		client
			.submit_schema_update_envelope(&SchemaUpdateEnvelope::compute_client_intent(
				table.tenant_id,
				SchemaObjectRef {
					kind: SchemaObjectKind::Table,
					namespace: descriptor.namespace.clone(),
					name: descriptor.table_name.clone(),
				},
				SchemaGeneration::ZERO,
				SchemaGeneration::new(descriptor.schema_generation),
				intent_id()?,
				SchemaMutationKind::Create,
				encode_table_schema_descriptor_v1(descriptor)?,
			))
			.await?;
	}
	for (tenant_id, descriptor) in &roster.indexes {
		client
			.submit_schema_update_envelope(&SchemaUpdateEnvelope::wendelon_operator_intent(
				*tenant_id,
				SchemaObjectRef {
					kind: SchemaObjectKind::Index,
					namespace: descriptor.namespace.clone(),
					name: descriptor.index_name.clone(),
				},
				SchemaGeneration::ZERO,
				SchemaGeneration::new(1),
				intent_id()?,
				SchemaMutationKind::Create,
				encode_secondary_index_descriptor(descriptor)?,
			))
			.await?;
	}
	Ok(())
}

/// R4: one chunk as one table write under one freshly minted operation id,
/// re-offered with that same id only while its outcome is unknown.
async fn import_chunk(
	client: &WendelonClient,
	entry: &ChunkEntry,
	descriptor: &TableSchemaDescriptor,
	batch: RecordBatch,
) -> Result<(), RestoreError> {
	let payload = RecordBatch::try_new(
		Arc::new(descriptor.arrow_schema()?),
		batch.columns().to_vec(),
	)
	.map_err(|error| artifact(error.to_string()))?;
	let mut meta: crate::schema::TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.parent_write_id = ClientWriteId {
		client_uuid: uuid_v7_now()?,
		sequence_no: 0,
		idempotency_ttl_ms: WRITE_TTL_MS,
		_pad: 0,
	};
	meta.tenant_id = entry.tenant_id;
	meta.table_object_id = entry.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(payload.num_rows())?;
	let deadline = Instant::now() + WRITE_BOUND;
	loop {
		let offer = client.table_write(TableWriteRequest {
			meta,
			descriptor: descriptor.clone(),
			payload: payload.clone(),
			deadline: Some((Instant::now() + Duration::from_secs(10)).min(deadline)),
		});
		let answer = match tokio::time::timeout_at(deadline.into(), offer).await {
			Ok(answer) => answer,
			Err(_) => Err(Error::new(
				ErrorKind::TimedOut,
				"no answer within the write's budget",
			)),
		};
		let why = match answer {
			Ok(result) => {
				let write = result.response.write;
				match mutation_answer(write.status, write.mutation_commit_knowledge()) {
					MutationAnswer::Committed { .. } => return Ok(()),
					MutationAnswer::Refused => {
						return Err(RestoreError::Refused(RestoreRefusal::RowsRefused {
							ordinal: entry.ordinal,
							status: write.status.0,
						}));
					}
					_ if write.status == WriteStatus::WRITE_OPERATION_EXPIRED => {
						return Err(RestoreError::Refused(RestoreRefusal::RowsUnknown {
							ordinal: entry.ordinal,
							why: "the operation id expired before its outcome was known".into(),
						}));
					}
					other => format!("{other:?} (status {})", write.status.0),
				}
			}
			Err(error) => error.to_string(),
		};
		if Instant::now() + WRITE_RETRY >= deadline {
			return Err(RestoreError::Refused(RestoreRefusal::RowsUnknown {
				ordinal: entry.ordinal,
				why,
			}));
		}
		tokio::time::sleep(WRITE_RETRY).await;
	}
}

/// R5: every table's count and canonical digest on the target.
async fn verify_tables(
	clients: &Clients,
	manifest: &Manifest,
	bounds: Bounds,
) -> Result<(), RestoreError> {
	for (table, rostered) in manifest.tables.iter().zip(&manifest.roster.tables) {
		let read = verify_table(clients, table, &rostered.descriptor);
		let Ok(verified) = tokio::time::timeout(bounds.table, read).await else {
			return Err(RestoreError::Refused(RestoreRefusal::VerificationFailed {
				tenant_id: table.tenant_id,
				table_object_id: table.table_object_id,
				why: format!(
					"the verification read did not finish within its {} s budget",
					bounds.table.as_secs()
				),
			}));
		};
		verified?;
	}
	Ok(())
}

async fn verify_table(
	clients: &Clients,
	table: &super::format::TableEntry,
	descriptor: &TableSchemaDescriptor,
) -> Result<(), RestoreError> {
	{
		let tenant_id = table.tenant_id;
		let table_object_id = descriptor.table_object_id;
		let failed = |why: String| {
			RestoreError::Refused(RestoreRefusal::VerificationFailed {
				tenant_id,
				table_object_id,
				why,
			})
		};
		let mut digest = TableDigest::new(canonical_schema(&descriptor.arrow_schema()?));
		let mut result = query_table(clients.data_for(tenant_id), tenant_id, descriptor)
			.await
			.map_err(|refusal| failed(format!("{refusal:?}")))?;
		let mut rows = 0u64;
		while let Some(batch) = next_batch(&mut result, tenant_id, table_object_id)
			.await
			.map_err(|refusal| failed(format!("{refusal:?}")))?
		{
			digest_batch(&mut digest, tenant_id, descriptor, &batch)
				.map_err(|refusal| failed(format!("{refusal:?}")))?;
			rows += batch.num_rows() as u64;
		}
		terminal_rows(&result, rows, tenant_id, table_object_id)
			.map_err(|refusal| failed(format!("{refusal:?}")))?;
		if rows != table.rows {
			return Err(failed(format!(
				"{rows} rows, the backup holds {}",
				table.rows
			)));
		}
		if digest.finish() != table.digest {
			return Err(failed("the canonical digest differs".into()));
		}
	}
	Ok(())
}

fn intent_id() -> std::io::Result<SchemaIntentId> {
	Ok(SchemaIntentId::new(random_u128()?.to_be_bytes()))
}

/// A UUIDv7 for now: its issuance time is the operation's.
fn uuid_v7_now() -> std::io::Result<[u8; 16]> {
	let ms = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_err(Error::other)?
		.as_millis() as u64;
	let mut bytes = random_u128()?.to_be_bytes();
	bytes[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
	bytes[6] = 0x70 | (bytes[6] & 0x0F);
	bytes[8] = 0x80 | (bytes[8] & 0x3F);
	Ok(bytes)
}

#[cfg(test)]
mod retention_admission_tests {
	use super::*;
	use crate::network::control_server::{
		RetentionAdminExtensionWire, TenantAdminResponseWire, decode_retention_admin_answer,
	};

	const TABLE: (TenantId, TableObjectId) = (TenantId::new(1), TableObjectId::new([0xA0; 16]));

	fn response(accepted: bool) -> TenantAdminResponseWire {
		TenantAdminResponseWire {
			accepted,
			reason_code: 0,
			leader_hint: Some(1),
			tenant: None,
			tenants: Vec::new(),
			message: None,
		}
	}

	fn table(policy: crate::schema::RowIdentityPolicy) -> super::super::roster::RosterTable {
		use arrow::datatypes::{DataType, Field, Schema};
		super::super::roster::RosterTable {
			tenant_id: TABLE.0,
			descriptor: TableSchemaDescriptor::new_with_layout(
				TABLE.1,
				1,
				"analytics",
				"events_append",
				&Schema::new(vec![Field::new("id", DataType::Int64, false)]),
				policy,
				crate::schema::LocalityPolicy::default_table_spread(),
				crate::schema::RecordLayoutPolicy::MultiRow(
					crate::schema::MultiRowLayoutDescriptor::default(),
				),
				crate::schema::SchemaEvolutionPolicy::AdditiveOnly,
			)
			.unwrap(),
			retention: crate::schema::TableRetentionPolicy::InheritCluster,
		}
	}

	/// Wendelon ADR 0121 R1: only an accepted answer naming the Disabled binding lets a
	/// governed restore proceed; a positive window, an explicit `None` and a bare reply (no
	/// extension, decoded to `None`) are refused, typed; a rejected answer stays an error.
	#[test]
	fn a_governed_restore_proceeds_only_on_an_explicit_disabled_binding() {
		let answer = |window_ms| (response(true), RetentionAdminExtensionWire { window_ms });
		assert!(retention_admission(TABLE, answer(Some(0))).is_ok());
		assert!(matches!(
			retention_admission(TABLE, answer(Some(15_000))),
			Err(RestoreError::Refused(
				RestoreRefusal::RetentionWindowBound {
					window_ms: 15_000,
					..
				}
			))
		));
		assert!(matches!(
			retention_admission(TABLE, answer(None)),
			Err(RestoreError::Refused(
				RestoreRefusal::RetentionBindingUnknown { .. }
			))
		));
		let bare = crate::network::wire::encode(&response(true)).unwrap();
		let decoded = decode_retention_admin_answer(&bare).unwrap();
		assert_eq!(decoded.1, RetentionAdminExtensionWire { window_ms: None });
		assert!(matches!(
			retention_admission(TABLE, decoded),
			Err(RestoreError::Refused(
				RestoreRefusal::RetentionBindingUnknown { .. }
			))
		));
		assert!(matches!(
			retention_admission(
				TABLE,
				(
					response(false),
					RetentionAdminExtensionWire { window_ms: Some(0) }
				)
			),
			Err(RestoreError::Io(_))
		));
	}

	/// Only an incoming AppendOnly table that inherits the cluster's window is governed; a
	/// backup without one never asks.
	#[test]
	fn only_an_inheriting_append_only_table_is_governed_on_restore() {
		let mut roster = BackupRoster {
			hold: (1, 1),
			cut: 1,
			tenants: Vec::new(),
			logical_types: Vec::new(),
			tables: vec![table(crate::schema::RowIdentityPolicy::AppendOnly)],
			indexes: Vec::new(),
		};
		assert!(governed_incoming(&roster).is_some());
		roster.tables[0].retention = crate::schema::TableRetentionPolicy::Disabled;
		assert!(governed_incoming(&roster).is_none());
		roster.tables.clear();
		assert!(governed_incoming(&roster).is_none());
	}
}
