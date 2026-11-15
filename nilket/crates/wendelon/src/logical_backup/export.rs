//! ADR 0736: the exporter, an operator tool over the cluster's public routes.
//!
//! Sequence: create the directory exclusively and the wrapped key; enter the
//! hold; wait (bounded) for the roster at a settled cut; export every table
//! through the ordinary linearizable table query, which must reach its success
//! terminal; read the roster again under the same hold and require it equal;
//! publish the manifest last; release the hold with its exact token. Every wait
//! has an absolute budget ([`Bounds`]). The hold is released on every path that
//! entered it; an entry or a release whose answer did not arrive is reported
//! with the id and, when known, the token, so the operator can find and release
//! an orphan (`backup hold-status`, `backup release`).

use super::artifact::{DataKey, MAX_CHUNK_BYTES};
use super::digest::{DigestRefusal, TableDigest, canonical_schema};
use super::format::{
	CHUNK_DIR, ChunkEntry, FORMAT_VERSION, KEY_FILE, MANIFEST_FILE, Manifest, TableEntry,
	chunk_header, chunk_path, encode_batch, manifest_header, sha256,
};
use super::hold::{Bounds, HoldError, Release};
use super::roster::{BackupRoster, RosterRefusal, RosterTable};
use crate::client::core::{
	QueryStatusError, RowIdentityProjection, TableQueryRequest, WendelonClient,
};
use crate::network::control_server::TenantAdminRequestWire;
use crate::schema::{
	Consistency, ReadStatus, TABLE_WRITE_MAX_ROWS, TableObjectId, TableQueryRequestId, TenantId,
};
use crate::storage::content_hash::encode_row_canonical;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind, Write};
use std::path::Path;
use std::time::Duration;

const SETTLE_POLL: Duration = Duration::from_millis(250);

/// The credentials a backup or a restore uses, each for what its role admits:
/// the operator's client enters and releases holds, reads rosters and
/// administers tenants and schemas; table reads and writes go through a
/// storage client, a `data_node` certificate confined to one tenant. No
/// storage certificate exists for the default tenant 0.
pub struct Clients {
	pub admin: std::sync::Arc<WendelonClient>,
	pub data: std::collections::BTreeMap<TenantId, std::sync::Arc<WendelonClient>>,
}

impl Clients {
	/// Every tenant with a table has a storage client, or the first that has none.
	pub fn cover(&self, roster: &BackupRoster) -> Result<(), TenantId> {
		match roster
			.tables
			.iter()
			.find(|table| !self.data.contains_key(&table.tenant_id))
		{
			Some(table) => Err(table.tenant_id),
			None => Ok(()),
		}
	}

	pub(crate) fn data_for(&self, tenant_id: TenantId) -> &WendelonClient {
		&self.data[&tenant_id]
	}
}

/// Why the whole backup is refused. Nothing is published.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupRefusal {
	/// The roster names why this cluster cannot be backed up as it is.
	Roster(RosterRefusal),
	/// Not settled within the settle budget ([`Bounds::settle`]).
	HoldNotSettled { why: String },
	/// A tenant holds a table and no storage credential for it was given (the
	/// default tenant 0 can have none).
	NoDataCredential { tenant_id: TenantId },
	/// The table's statement was refused by the reader.
	ReaderRefused {
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		reason: String,
	},
	/// The table exceeds the reader's supported version bound.
	TableTooLarge {
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	},
	UnsupportedEncoding {
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		reason: String,
	},
	TableTooLargeForDigest {
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	},
	TooManyRowsForDigest {
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	},
	/// One row does not fit one chunk.
	RowTooLargeForChunk {
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	},
	/// After the tables, the hold or the roster was no longer the same.
	ContinuityLost { why: String },
}

#[derive(Debug)]
pub enum ExportError {
	Refused(BackupRefusal),
	Io(Error),
	/// No hold was entered, or its entry was not confirmed.
	Hold(HoldError),
	/// The export ended (`outcome`) but its hold's release did not succeed.
	HoldNotReleased {
		id: u128,
		entered_at: u64,
		outcome: Box<Result<ExportReport, ExportError>>,
		release: Release,
	},
}

impl From<Error> for ExportError {
	fn from(error: Error) -> Self {
		Self::Io(error)
	}
}

impl std::fmt::Display for ExportError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Refused(refusal) => write!(f, "backup refused: {refusal:?}"),
			Self::Io(error) => write!(f, "backup failed: {error}"),
			Self::Hold(error) => write!(f, "backup not started: {error}"),
			Self::HoldNotReleased {
				id,
				entered_at,
				outcome,
				release,
			} => write!(
				f,
				"{}; the backup hold (id {id:#x}, entered at {entered_at}) release was \
				 {release:?}: new writes may stay refused until `backup release --id {id:#x} \
				 --entered-at {entered_at}` (`backup hold-status` shows the current hold)",
				match outcome.as_ref() {
					Ok(report) => format!("backup {:#x} published", report.backup_id),
					Err(error) => error.to_string(),
				}
			),
		}
	}
}

/// What a published backup holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportReport {
	pub backup_id: u128,
	pub hold: (u128, u64),
	pub cut: u64,
	pub continuity_cut: u64,
	pub tables: usize,
	pub rows: u64,
	pub chunks: u64,
}

/// Exports the cluster `clients` reach into the new directory `dir`.
pub async fn export(
	clients: &Clients,
	dir: &Path,
	passphrase: &[u8],
) -> Result<ExportReport, ExportError> {
	export_bounded(clients, dir, passphrase, Bounds::DEFAULT).await
}

/// [`export`] under the budgets `bounds`.
pub async fn export_bounded(
	clients: &Clients,
	dir: &Path,
	passphrase: &[u8],
	bounds: Bounds,
) -> Result<ExportReport, ExportError> {
	std::fs::create_dir(dir)?;
	std::fs::create_dir(dir.join(CHUNK_DIR))?;
	let key = DataKey::generate()?;
	write_new(&dir.join(KEY_FILE), &key.wrap(passphrase)?)?;
	let backup_id = random_u128()?;
	let client = clients.admin.as_ref();
	let token = super::hold::enter(client, backup_id, bounds)
		.await
		.map_err(ExportError::Hold)?;
	let outcome = export_held(clients, dir, &key, backup_id, token, bounds).await;
	match super::hold::release(client, token, bounds).await {
		Release::Released => outcome,
		release => Err(ExportError::HoldNotReleased {
			id: token.0,
			entered_at: token.1,
			outcome: Box::new(outcome),
			release,
		}),
	}
}

async fn export_held(
	clients: &Clients,
	dir: &Path,
	key: &DataKey,
	backup_id: u128,
	token: (u128, u64),
	bounds: Bounds,
) -> Result<ExportReport, ExportError> {
	let roster = settled_roster(&clients.admin, token, bounds).await?;
	clients
		.cover(&roster)
		.map_err(|tenant_id| ExportError::Refused(BackupRefusal::NoDataCredential { tenant_id }))?;
	let mut chunks = Vec::new();
	let mut tables = Vec::new();
	for table in &roster.tables {
		let data = clients.data_for(table.tenant_id);
		let read = export_table(data, dir, key, backup_id, table, &mut chunks);
		match tokio::time::timeout(bounds.table, read).await {
			Ok(entry) => tables.push(entry?),
			Err(_) => {
				return Err(ExportError::Refused(BackupRefusal::ReaderRefused {
					tenant_id: table.tenant_id,
					table_object_id: table.descriptor.table_object_id,
					reason: format!(
						"the table read did not finish within its {} s budget",
						bounds.table.as_secs()
					),
				}));
			}
		}
	}
	let again = settled_roster(&clients.admin, token, bounds)
		.await
		.map_err(|error| match error {
			ExportError::Refused(refusal) => ExportError::Refused(BackupRefusal::ContinuityLost {
				why: format!("{refusal:?}"),
			}),
			other => other,
		})?;
	let same = |a: &BackupRoster, b: &BackupRoster| {
		(a.hold, &a.tenants, &a.logical_types, &a.tables, &a.indexes)
			== (b.hold, &b.tenants, &b.logical_types, &b.tables, &b.indexes)
	};
	if !same(&roster, &again) {
		return Err(ExportError::Refused(BackupRefusal::ContinuityLost {
			why: "the roster changed under the hold".into(),
		}));
	}
	let report = ExportReport {
		backup_id,
		hold: token,
		cut: roster.cut,
		continuity_cut: again.cut,
		tables: tables.len(),
		rows: tables.iter().map(|table| table.rows).sum(),
		chunks: chunks.len() as u64,
	};
	let manifest = Manifest {
		format: FORMAT_VERSION,
		backup_id,
		roster,
		continuity_cut: again.cut,
		tables,
		chunks,
	};
	let header = manifest_header(backup_id);
	let sealed = key.seal_manifest(&header, &manifest.encode()?)?;
	let mut file = header.to_vec();
	file.extend_from_slice(&sealed);
	publish(dir, &file)?;
	Ok(report)
}

/// The roster under `token`, waiting while it is only unsettled, within the
/// settle budget for all of its reads together.
pub(crate) async fn settled_roster(
	client: &WendelonClient,
	token: (u128, u64),
	bounds: Bounds,
) -> Result<BackupRoster, ExportError> {
	let deadline = tokio::time::Instant::now() + bounds.settle;
	loop {
		let request = TenantAdminRequestWire::BackupRoster {
			id: token.0,
			entered_at: token.1,
		};
		let why = match tokio::time::timeout_at(deadline, client.backup_admin(&request)).await {
			Err(_) => "no roster answer within the settle budget".to_owned(),
			Ok(Ok((_, extension))) => match extension.roster {
				Some(Ok(roster)) => return Ok(roster),
				Some(Err(
					RosterRefusal::Unsettled { why } | RosterRefusal::KvCheckInconclusive { why },
				)) => why,
				Some(Err(refusal)) => {
					return Err(ExportError::Refused(BackupRefusal::Roster(refusal)));
				}
				None => "the cluster answered without a roster".into(),
			},
			Ok(Err(error)) => error.to_string(),
		};
		if tokio::time::Instant::now() + SETTLE_POLL > deadline {
			return Err(ExportError::Refused(BackupRefusal::HoldNotSettled { why }));
		}
		tokio::time::sleep(SETTLE_POLL).await;
	}
}

async fn export_table(
	client: &WendelonClient,
	dir: &Path,
	key: &DataKey,
	backup_id: u128,
	table: &RosterTable,
	chunks: &mut Vec<ChunkEntry>,
) -> Result<TableEntry, ExportError> {
	let (tenant_id, descriptor) = (table.tenant_id, &table.descriptor);
	let table_object_id = descriptor.table_object_id;
	let refused = |refusal| ExportError::Refused(refusal);
	let mut digest = TableDigest::new(canonical_schema(&descriptor.arrow_schema()?));
	let mut result = query_table(client, tenant_id, descriptor)
		.await
		.map_err(refused)?;
	let first_chunk = chunks.len() as u64 + 1;
	let mut rows = 0u64;
	while let Some(batch) = next_batch(&mut result, tenant_id, table_object_id)
		.await
		.map_err(refused)?
	{
		digest_batch(&mut digest, tenant_id, descriptor, &batch).map_err(refused)?;
		rows += batch.num_rows() as u64;
		for piece in fitting_pieces(&batch).map_err(|()| {
			refused(BackupRefusal::RowTooLargeForChunk {
				tenant_id,
				table_object_id,
			})
		})? {
			let ordinal = chunks.len() as u64 + 1;
			let header = chunk_header(
				backup_id,
				tenant_id,
				table_object_id,
				descriptor.schema_generation,
				ordinal,
			);
			let sealed = key.seal_chunk(ordinal, &header, &piece.1)?;
			write_new(&chunk_path(dir, ordinal), &sealed)?;
			chunks.push(ChunkEntry {
				ordinal,
				tenant_id,
				table_object_id,
				schema_generation: descriptor.schema_generation,
				rows: piece.0,
				sealed_len: sealed.len() as u64,
				sealed_sha256: sha256(&sealed),
			});
		}
	}
	terminal_rows(&result, rows, tenant_id, table_object_id).map_err(refused)?;
	if digest.rows() as u64 != rows {
		return Err(Error::other("digest row count drifted").into());
	}
	Ok(TableEntry {
		tenant_id,
		table_object_id,
		rows,
		digest: digest.finish(),
		first_chunk,
		chunk_count: chunks.len() as u64 + 1 - first_chunk,
	})
}

/// The table's whole contents through the ordinary linearizable table query.
pub(crate) async fn query_table(
	client: &WendelonClient,
	tenant_id: TenantId,
	descriptor: &crate::schema::TableSchemaDescriptor,
) -> Result<crate::client::core::StreamingQueryResult, BackupRefusal> {
	let table_object_id = descriptor.table_object_id;
	let mut meta: crate::schema::TableQueryRequestMeta = bytemuck::Zeroable::zeroed();
	meta.expected_epoch = client.epoch();
	meta.tenant_id = tenant_id;
	meta.table_object_id = table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.physical_descriptor_fingerprint = *descriptor.physical_descriptor_fingerprint.as_bytes();
	meta.consistency = Consistency::LINEARIZABLE;
	meta.row_identity_projection = RowIdentityProjection::None.as_u8();
	meta.request_id = TableQueryRequestId::from_u128(random_u128().map_err(|error| {
		BackupRefusal::ReaderRefused {
			tenant_id,
			table_object_id,
			reason: error.to_string(),
		}
	})?);
	client
		.table_query(TableQueryRequest {
			meta,
			descriptor: descriptor.clone(),
			pbc: Vec::new(),
			runtime_filter: None,
			row_identity: RowIdentityProjection::None,
		})
		.await
		.map_err(|error| reader_refusal(tenant_id, table_object_id, error))
}

/// The next batch of a table query; a terminal other than success refuses.
pub(crate) async fn next_batch(
	result: &mut crate::client::core::StreamingQueryResult,
	tenant_id: TenantId,
	table_object_id: TableObjectId,
) -> Result<Option<RecordBatch>, BackupRefusal> {
	result
		.next_batch()
		.await
		.map_err(|error| reader_refusal(tenant_id, table_object_id, error))
}

/// After the last batch: the stream received an explicit successful terminal,
/// and that terminal's row count is the rows received.
pub(crate) fn terminal_rows(
	result: &crate::client::core::StreamingQueryResult,
	rows: u64,
	tenant_id: TenantId,
	table_object_id: TableObjectId,
) -> Result<(), BackupRefusal> {
	if result.terminal_status() != Some(ReadStatus::OK) {
		return Err(BackupRefusal::ReaderRefused {
			tenant_id,
			table_object_id,
			reason: format!(
				"the read ended without a successful terminal ({:?})",
				result.terminal_status()
			),
		});
	}
	if result.rows != rows {
		return Err(BackupRefusal::ReaderRefused {
			tenant_id,
			table_object_id,
			reason: format!("the terminal counted {} rows, {rows} arrived", result.rows),
		});
	}
	Ok(())
}

fn reader_refusal(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	error: Error,
) -> BackupRefusal {
	let status = error
		.get_ref()
		.and_then(|inner| inner.downcast_ref::<QueryStatusError>())
		.map(|failure| failure.status);
	if status == Some(ReadStatus::QUERY_BUDGET_EXCEEDED) {
		BackupRefusal::TableTooLarge {
			tenant_id,
			table_object_id,
		}
	} else {
		BackupRefusal::ReaderRefused {
			tenant_id,
			table_object_id,
			reason: error.to_string(),
		}
	}
}

/// Adds every row of `batch` to the table's digest, in the canonical encoding.
pub(crate) fn digest_batch(
	digest: &mut TableDigest,
	tenant_id: TenantId,
	descriptor: &crate::schema::TableSchemaDescriptor,
	batch: &RecordBatch,
) -> Result<(), BackupRefusal> {
	let table_object_id = descriptor.table_object_id;
	for row in 0..batch.num_rows() {
		let encoded = encode_row_canonical(descriptor, batch, row).map_err(|error| {
			BackupRefusal::UnsupportedEncoding {
				tenant_id,
				table_object_id,
				reason: format!("{error:?}"),
			}
		})?;
		digest
			.push(encoded.into_vec())
			.map_err(|refusal| match refusal {
				DigestRefusal::TooManyRows => BackupRefusal::TooManyRowsForDigest {
					tenant_id,
					table_object_id,
				},
				DigestRefusal::TooManyBytes | DigestRefusal::RowTooLong => {
					BackupRefusal::TableTooLargeForDigest {
						tenant_id,
						table_object_id,
					}
				}
			})?;
	}
	Ok(())
}

/// `batch` as consecutive slices each encoding within one chunk, with their
/// row counts. `Err` when one row alone does not fit.
fn fitting_pieces(batch: &RecordBatch) -> Result<Vec<(u32, Vec<u8>)>, ()> {
	let mut pieces = Vec::new();
	let mut pending = vec![(0usize, batch.num_rows())];
	while let Some((offset, len)) = pending.pop() {
		if len == 0 {
			continue;
		}
		let encoded = encode_batch(&batch.slice(offset, len)).map_err(|_| ())?;
		if encoded.len() <= MAX_CHUNK_BYTES && len <= TABLE_WRITE_MAX_ROWS {
			pieces.push((offset, len as u32, encoded));
			continue;
		}
		if len == 1 {
			return Err(());
		}
		// Each chunk is also one table write on restore.
		let half = if len > TABLE_WRITE_MAX_ROWS {
			TABLE_WRITE_MAX_ROWS
		} else {
			len / 2
		};
		// Pushed last-first, so slices come out in row order.
		pending.push((offset + half, len - half));
		pending.push((offset, half));
	}
	pieces.sort_by_key(|piece| piece.0);
	Ok(pieces
		.into_iter()
		.map(|(_, rows, bytes)| (rows, bytes))
		.collect())
}

pub(crate) fn random_u128() -> std::io::Result<u128> {
	let mut bytes = [0u8; 16];
	aws_lc_rs::rand::fill(&mut bytes).map_err(|_| Error::other("no randomness"))?;
	Ok(u128::from_be_bytes(bytes))
}

/// Creates `path` (refused if it exists), writes `bytes` and syncs it.
pub(crate) fn write_new(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
	let mut file = std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(path)?;
	file.write_all(bytes)?;
	file.sync_all()
}

/// Publishes the manifest: a temporary file, synced, renamed, then the
/// directory synced.
fn publish(dir: &Path, file: &[u8]) -> std::io::Result<()> {
	let temporary = dir.join(format!("{MANIFEST_FILE}.tmp"));
	write_new(&temporary, file)?;
	let chunks = std::fs::File::open(dir.join(CHUNK_DIR))?;
	chunks.sync_all()?;
	std::fs::rename(&temporary, dir.join(MANIFEST_FILE))?;
	std::fs::File::open(dir)?.sync_all()?;
	if !dir.join(MANIFEST_FILE).is_file() {
		return Err(Error::new(
			ErrorKind::NotFound,
			"the manifest was not published",
		));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use arrow::array::{BinaryArray, Int64Array};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc;

	#[test]
	fn a_batch_is_cut_into_chunks_in_row_order_and_a_row_too_large_is_refused() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Binary, false),
		]));
		let rows = 6i64;
		let blob = vec![7u8; MAX_CHUNK_BYTES / 4];
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(Int64Array::from_iter_values(0..rows)),
				Arc::new(BinaryArray::from_iter_values(
					(0..rows).map(|_| blob.as_slice()),
				)),
			],
		)
		.unwrap();
		let pieces = fitting_pieces(&batch).unwrap();
		assert!(pieces.len() > 1);
		assert_eq!(pieces.iter().map(|piece| piece.0 as i64).sum::<i64>(), rows);
		let mut ids = Vec::new();
		for (count, bytes) in &pieces {
			assert!(bytes.len() <= MAX_CHUNK_BYTES);
			let decoded = super::super::format::decode_batch(bytes).unwrap();
			assert_eq!(decoded.num_rows(), *count as usize);
			let column = decoded
				.column(0)
				.as_any()
				.downcast_ref::<Int64Array>()
				.unwrap();
			ids.extend(column.values().iter().copied());
		}
		assert_eq!(ids, (0..rows).collect::<Vec<_>>());

		let huge = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from_iter_values([0])),
				Arc::new(BinaryArray::from_iter_values([
					vec![0u8; MAX_CHUNK_BYTES].as_slice()
				])),
			],
		)
		.unwrap();
		assert!(fitting_pieces(&huge).is_err());
	}
}
