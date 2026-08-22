use crate::schema::table::PlacementSemantics;
use crate::schema::{
	ByteCount, CompactionLevelIndex, HybridTimestamp, Identity, IndexObjectId, Locality,
	LogicalSchemaFingerprint, ManifestEpoch, PartitionId, RecordRowIndex, RowCount, SlabId,
	SupersessionMetadata, TableObjectId, TenantId, TenantKekId, TombstoneMetadata,
};
use crate::storage::RecordLocation;
use crate::storage::lsm::{LsmRunSummary, SidecarCoverage, SlabState};
use crate::storage::manager::TieredSlabPointer;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

const RECORDS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("slab_records_v1");
/// V1 layout: `payload_hash[32] || l2_uri_utf8`. Older rows had no
/// `is_encrypted` byte. Read-only; one-shot migrated into V2 the
/// first time a process opens an index that still has V1 rows.
const TIERED_SLABS_TABLE_V1: TableDefinition<u64, &[u8]> = TableDefinition::new("tiered_slabs_v1");
/// V2 layout: `payload_hash[32] || flags_byte || flags2_byte || [optional metadata] ||
/// l2_uri_utf8`.
///
/// Flags:
/// - bit 0 = `is_encrypted`
/// - bit 1 = `tenant_id` present (8 LE bytes)
/// - bit 2 = `tenant_kek_id` present (8 LE bytes)
/// - bit 3 = `stats_format_version` present (2 LE bytes)
/// - bit 4 = `stats_block_len` present (4 LE bytes)
/// - bit 5 = `stats_block_hash128` present (16 raw bytes)
/// - bit 6 = earliest tombstone HLC present (16 LE bytes)
/// - bit 7 = tombstone metadata is tracked
///
/// Flags2:
/// - bit 0 = earliest supersession HLC present (16 LE bytes)
/// - bit 1 = supersession metadata is tracked
///
const TIERED_SLABS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("tiered_slabs_v2");
const SECONDARY_INDEX_SEGMENTS_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("secondary_index_segments_v1");
const LSM_RUNS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("lsm_runs_v1");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("storage_index_meta_v1");
const META_LAST_CONTROL_LOG_INDEX: &str = "last_control_log_index";

const RECORD_KEY_LEN: usize = 8 + 32 + 8 + 4 + 4 + 8 + 8;
const RECORD_VALUE_LEN_V1: usize = 4;
const RECORD_VALUE_LEN_V2: usize = 4 + 8;
const RECORD_VALUE_LEN_V3: usize = 4 + 8 + 4 + 4;
/// V4 adds the table-object identity that owned this published record
/// location.  The location key cannot be reverse-mapped to its table after
/// the slab is missing, so this attribution must travel with the durable
/// reference itself.
const RECORD_VALUE_LEN_V4: usize = 4 + 8 + 4 + 4 + 16;
/// ADR 0090 V5 appends the durable placement stamp and its partition. A value
/// shorter than this is an earlier version and decodes as `RawLocality`; a value
/// of this length with an unknown stamp is malformed and is refused, so "written
/// before partitions" and "corrupt" never share an outcome.
const RECORD_VALUE_LEN_V5: usize = 4 + 8 + 4 + 4 + 16 + 1 + 8;
const RECORD_VALUE_LEN: usize = RECORD_VALUE_LEN_V5;
const PLACEMENT_STAMP_RAW_LOCALITY: u8 = 0;
const PLACEMENT_STAMP_PARTITIONED: u8 = 1;
const RECORD_FLAG_IS_TOMBSTONE: u32 = 1 << 0;
const RECORD_FLAG_HAS_TABLE_OBJECT_ID: u32 = 1 << 1;
const TIERED_POINTER_HASH_LEN: usize = 32;
const TIERED_POINTER_FLAGS_LEN: usize = 2;
const TIERED_POINTER_PREFIX_LEN: usize = TIERED_POINTER_HASH_LEN + TIERED_POINTER_FLAGS_LEN;
const TIERED_FLAG_IS_ENCRYPTED: u8 = 0b0000_0001;
const TIERED_FLAG_HAS_TENANT_ID: u8 = 0b0000_0010;
const TIERED_FLAG_HAS_TENANT_KEK_ID: u8 = 0b0000_0100;
const TIERED_FLAG_HAS_STATS_FORMAT_VERSION: u8 = 0b0000_1000;
const TIERED_FLAG_HAS_STATS_BLOCK_LEN: u8 = 0b0001_0000;
const TIERED_FLAG_HAS_STATS_BLOCK_HASH128: u8 = 0b0010_0000;
const TIERED_FLAG_HAS_FIRST_TOMBSTONE_HLC: u8 = 0b0100_0000;
const TIERED_FLAG_TOMBSTONE_METADATA_TRACKED: u8 = 0b1000_0000;
const TIERED_FLAG_KNOWN_MASK: u8 = TIERED_FLAG_IS_ENCRYPTED
	| TIERED_FLAG_HAS_TENANT_ID
	| TIERED_FLAG_HAS_TENANT_KEK_ID
	| TIERED_FLAG_HAS_STATS_FORMAT_VERSION
	| TIERED_FLAG_HAS_STATS_BLOCK_LEN
	| TIERED_FLAG_HAS_STATS_BLOCK_HASH128
	| TIERED_FLAG_HAS_FIRST_TOMBSTONE_HLC
	| TIERED_FLAG_TOMBSTONE_METADATA_TRACKED;
const TIERED_FLAG2_HAS_FIRST_SUPERSESSION_HLC: u8 = 0b0000_0001;
const TIERED_FLAG2_SUPERSESSION_METADATA_TRACKED: u8 = 0b0000_0010;
const TIERED_FLAG2_KNOWN_MASK: u8 =
	TIERED_FLAG2_HAS_FIRST_SUPERSESSION_HLC | TIERED_FLAG2_SUPERSESSION_METADATA_TRACKED;
const SECONDARY_INDEX_SEGMENT_KEY_LEN: usize = 8 + 16 + 16 + 32 + 8;
const SECONDARY_INDEX_SEGMENT_VALUE_PREFIX_LEN: usize = 32 + 32 + 2 + 8 + 4 + 4;
const SECONDARY_INDEX_SEGMENT_FLAG_HAS_L2_URI: u32 = 1 << 0;
const SECONDARY_INDEX_SEGMENT_FLAG_KNOWN_MASK: u32 = SECONDARY_INDEX_SEGMENT_FLAG_HAS_L2_URI;
const LSM_RUN_VALUE_PREFIX_LEN: usize = 1 + 1 + 1 + 1 + 8 + 16 + 16 + 8 + 8 + 4 + 4 + 4 + 4 + 4;
const LSM_SLAB_STATE_LIVE: u8 = 0;
const LSM_SLAB_STATE_RETIRED: u8 = 1;
const LSM_SLAB_STATE_DELETE_PENDING: u8 = 2;
const LSM_SIDECAR_COVERAGE_NONE: u8 = 0;
const LSM_SIDECAR_COVERAGE_PARTIAL: u8 = 1;
const LSM_SIDECAR_COVERAGE_COMPLETE: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PersistentRecordRow {
	pub tenant_id: TenantId,
	pub key_identity: Identity,
	pub routing_locality: Locality,
	/// Present for typed table rows written after ADR 0539. Legacy and
	/// untyped KV rows deliberately remain unattributed rather than guessed.
	pub table_object_id: Option<TableObjectId>,
	/// ADR 0090: how this record resolves to a replica set. Absent stamp means
	/// [`PlacementSemantics::RawLocality`], decoded from a shorter value rather
	/// than inferred from a failure.
	pub placement: PlacementSemantics,
	/// Set only when `placement` is `Partitioned`.
	pub partition: Option<PartitionId>,
	pub location: RecordLocation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PersistentSecondaryIndexSegmentKey {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub index_object_id: IndexObjectId,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
	pub source_slab_id: SlabId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PersistentSecondaryIndexSegmentRow {
	pub key: PersistentSecondaryIndexSegmentKey,
	pub segment_path: String,
	pub l2_uri: Option<String>,
	pub payload_hash: [u8; 32],
	pub state_digest: [u8; 32],
	pub format_version: u16,
	pub entry_count: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PersistentIndexSnapshot {
	/// Every durable record reference, including local L1 records.  Recovery
	/// still rebuilds the authoritative location index from slab files, but the
	/// durable rows carry attribution that cannot be reconstructed once a slab
	/// is missing.
	pub record_rows: Vec<PersistentRecordRow>,
	pub tiered_slabs: Vec<TieredSlabPointer>,
	pub tiered_records: Vec<PersistentRecordRow>,
	pub record_localities: Vec<(TenantId, Identity, Locality)>,
	pub secondary_index_segments: Vec<PersistentSecondaryIndexSegmentRow>,
	pub lsm_runs: Vec<LsmRunSummary>,
	pub last_control_log_index: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PersistentIndexUpdate {
	pub put_records: Vec<PersistentRecordRow>,
	pub delete_records: Vec<PersistentRecordRow>,
	pub put_tiered_slabs: Vec<TieredSlabPointer>,
	pub delete_tiered_slab_ids: Vec<SlabId>,
	pub put_secondary_index_segments: Vec<PersistentSecondaryIndexSegmentRow>,
	pub delete_secondary_index_segments: Vec<PersistentSecondaryIndexSegmentKey>,
	pub put_lsm_runs: Vec<LsmRunSummary>,
	pub delete_lsm_run_ids: Vec<SlabId>,
	pub last_control_log_index: Option<u64>,
}

pub(crate) struct PersistentLocalIndex {
	db: Arc<Database>,
	write_lock: Mutex<()>,
}

impl PersistentLocalIndex {
	pub(crate) async fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
		let path = path.into();
		let db = spawn_blocking_io(move || Database::create(path).map_err(redb_to_io)).await?;
		let db = Arc::new(db);
		let init_db = db.clone();
		spawn_blocking_io(move || {
			let write_txn = init_db.begin_write().map_err(redb_to_io)?;
			{
				let _table = write_txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
			}
			{
				let _table = write_txn
					.open_table(TIERED_SLABS_TABLE)
					.map_err(redb_to_io)?;
			}
			{
				let _table = write_txn
					.open_table(SECONDARY_INDEX_SEGMENTS_TABLE)
					.map_err(redb_to_io)?;
			}
			{
				let _table = write_txn.open_table(LSM_RUNS_TABLE).map_err(redb_to_io)?;
			}
			{
				let _table = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
			}
			// One-shot v1 → v2 migration. Every v1 row becomes a v2
			// row with `is_encrypted = false`; the rewrite sweeper
			// will then GET, encrypt, and stamp the truth back. v1
			// is left in place but unused — operators can drop it
			// in a later release once every cluster has cycled.
			let mut migrated_keys: Vec<u64> = Vec::new();
			{
				let v1 = write_txn
					.open_table(TIERED_SLABS_TABLE_V1)
					.map_err(redb_to_io)?;
				let mut v2 = write_txn
					.open_table(TIERED_SLABS_TABLE)
					.map_err(redb_to_io)?;
				for entry in v1.iter().map_err(redb_to_io)? {
					let (slab_id_key, value) = entry.map_err(redb_to_io)?;
					let slab_id = SlabId::new(slab_id_key.value());
					if v2.get(&slab_id.get()).map_err(redb_to_io)?.is_some() {
						continue;
					}
					let pointer = decode_tiered_pointer_v1(slab_id, value.value())?;
					let encoded = encode_tiered_pointer(&pointer);
					v2.insert(&slab_id.get(), encoded.as_slice())
						.map_err(redb_to_io)?;
					migrated_keys.push(slab_id.get());
				}
			}
			if !migrated_keys.is_empty() {
				let mut v1 = write_txn
					.open_table(TIERED_SLABS_TABLE_V1)
					.map_err(redb_to_io)?;
				for key in migrated_keys {
					let _ = v1.remove(&key).map_err(redb_to_io)?;
				}
			}
			write_txn.commit().map_err(redb_to_io)
		})
		.await?;
		Ok(Self {
			db,
			write_lock: Mutex::new(()),
		})
	}

	pub(crate) async fn load_snapshot(&self) -> std::io::Result<PersistentIndexSnapshot> {
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let read_txn = db.begin_read().map_err(redb_to_io)?;
			let tiered_table = read_txn
				.open_table(TIERED_SLABS_TABLE)
				.map_err(redb_to_io)?;
			let mut tiered_slabs = Vec::new();
			let mut tiered_ids = HashSet::new();
			for entry in tiered_table.iter().map_err(redb_to_io)? {
				let (slab_id, value) = entry.map_err(redb_to_io)?;
				let pointer = decode_tiered_pointer(SlabId::new(slab_id.value()), value.value())?;
				tiered_ids.insert(pointer.slab_id);
				tiered_slabs.push(pointer);
			}

			let records_table = read_txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
			let mut record_rows = Vec::new();
			let mut tiered_records = Vec::new();
			let mut locality_by_key: HashMap<(TenantId, Identity), (Locality, HybridTimestamp)> =
				HashMap::new();
			for entry in records_table.iter().map_err(redb_to_io)? {
				let (key, value) = entry.map_err(redb_to_io)?;
				let row = decode_record_row(key.value(), value.value())?;
				record_rows.push(row);
				locality_by_key
					.entry((row.tenant_id, row.key_identity))
					.and_modify(|(locality, hlc)| {
						if row.location.hlc >= *hlc {
							*locality = row.routing_locality;
							*hlc = row.location.hlc;
						}
					})
					.or_insert((row.routing_locality, row.location.hlc));
				if tiered_ids.contains(&row.location.slab_id) {
					tiered_records.push(row);
				}
			}
			let mut record_localities = locality_by_key
				.into_iter()
				.map(|((tenant_id, key_identity), (locality, _))| {
					(tenant_id, key_identity, locality)
				})
				.collect::<Vec<_>>();
			record_localities
				.sort_by_key(|(tenant_id, key_identity, _)| (tenant_id.get(), *key_identity));
			let secondary_index_segments_table = read_txn
				.open_table(SECONDARY_INDEX_SEGMENTS_TABLE)
				.map_err(redb_to_io)?;
			let mut secondary_index_segments = Vec::new();
			for entry in secondary_index_segments_table.iter().map_err(redb_to_io)? {
				let (key, value) = entry.map_err(redb_to_io)?;
				secondary_index_segments.push(decode_secondary_index_segment_row(
					key.value(),
					value.value(),
				)?);
			}
			let lsm_runs_table = read_txn.open_table(LSM_RUNS_TABLE).map_err(redb_to_io)?;
			let mut lsm_runs = Vec::new();
			for entry in lsm_runs_table.iter().map_err(redb_to_io)? {
				let (slab_id, value) = entry.map_err(redb_to_io)?;
				lsm_runs.push(decode_lsm_run_summary(
					SlabId::new(slab_id.value()),
					value.value(),
				)?);
			}
			let meta_table = read_txn.open_table(META_TABLE).map_err(redb_to_io)?;
			let last_control_log_index = meta_table
				.get(META_LAST_CONTROL_LOG_INDEX)
				.map_err(redb_to_io)?
				.map(|value| value.value());

			Ok(PersistentIndexSnapshot {
				record_rows,
				tiered_slabs,
				tiered_records,
				record_localities,
				secondary_index_segments,
				lsm_runs,
				last_control_log_index,
			})
		})
		.await
	}

	pub(crate) async fn rebuild(
		&self,
		rows: Vec<PersistentRecordRow>,
		tiered_slabs: Vec<TieredSlabPointer>,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		let secondary_index_segments = self
			.load_snapshot()
			.await
			.map(|snapshot| snapshot.secondary_index_segments)
			.unwrap_or_default();
		let lsm_runs = self
			.load_snapshot()
			.await
			.map(|snapshot| snapshot.lsm_runs)
			.unwrap_or_default();
		self.rebuild_with_secondary_index_segments_and_lsm_runs(
			rows,
			tiered_slabs,
			secondary_index_segments,
			lsm_runs,
			last_control_log_index,
		)
		.await
	}

	#[cfg(test)]
	pub(crate) async fn rebuild_with_secondary_index_segments(
		&self,
		rows: Vec<PersistentRecordRow>,
		tiered_slabs: Vec<TieredSlabPointer>,
		secondary_index_segments: Vec<PersistentSecondaryIndexSegmentRow>,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		let lsm_runs = self
			.load_snapshot()
			.await
			.map(|snapshot| snapshot.lsm_runs)
			.unwrap_or_default();
		self.rebuild_with_secondary_index_segments_and_lsm_runs(
			rows,
			tiered_slabs,
			secondary_index_segments,
			lsm_runs,
			last_control_log_index,
		)
		.await
	}

	pub(crate) async fn rebuild_with_secondary_index_segments_and_lsm_runs(
		&self,
		rows: Vec<PersistentRecordRow>,
		tiered_slabs: Vec<TieredSlabPointer>,
		secondary_index_segments: Vec<PersistentSecondaryIndexSegmentRow>,
		lsm_runs: Vec<LsmRunSummary>,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let write_txn = db.begin_write().map_err(redb_to_io)?;
			let _ = write_txn.delete_table(RECORDS_TABLE);
			let _ = write_txn.delete_table(TIERED_SLABS_TABLE);
			let _ = write_txn.delete_table(SECONDARY_INDEX_SEGMENTS_TABLE);
			let _ = write_txn.delete_table(LSM_RUNS_TABLE);
			{
				let mut table = write_txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
				for row in rows {
					let key = encode_record_key(row);
					let value = encode_record_value(row);
					table
						.insert(key.as_slice(), value.as_slice())
						.map_err(redb_to_io)?;
				}
			}
			{
				let mut table = write_txn
					.open_table(TIERED_SLABS_TABLE)
					.map_err(redb_to_io)?;
				for pointer in tiered_slabs {
					let value = encode_tiered_pointer(&pointer);
					table
						.insert(&pointer.slab_id.get(), value.as_slice())
						.map_err(redb_to_io)?;
				}
			}
			{
				let mut table = write_txn
					.open_table(SECONDARY_INDEX_SEGMENTS_TABLE)
					.map_err(redb_to_io)?;
				for row in secondary_index_segments {
					let key = encode_secondary_index_segment_key(row.key);
					let value = encode_secondary_index_segment_value(&row);
					table
						.insert(key.as_slice(), value.as_slice())
						.map_err(redb_to_io)?;
				}
			}
			{
				let mut table = write_txn.open_table(LSM_RUNS_TABLE).map_err(redb_to_io)?;
				for run in lsm_runs {
					let value = encode_lsm_run_summary(&run)?;
					table
						.insert(&run.slab_id.get(), value.as_slice())
						.map_err(redb_to_io)?;
				}
			}
			if let Some(index) = last_control_log_index {
				let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
				meta.insert(META_LAST_CONTROL_LOG_INDEX, &index)
					.map_err(redb_to_io)?;
			}
			write_txn.commit().map_err(redb_to_io)
		})
		.await
	}

	pub(crate) async fn apply(&self, update: PersistentIndexUpdate) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let write_txn = db.begin_write().map_err(redb_to_io)?;
			{
				let mut table = write_txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
				for row in update.delete_records {
					let key = encode_record_key(row);
					let _ = table.remove(key.as_slice()).map_err(redb_to_io)?;
				}
				for row in update.put_records {
					let key = encode_record_key(row);
					let value = encode_record_value(row);
					table
						.insert(key.as_slice(), value.as_slice())
						.map_err(redb_to_io)?;
				}
			}
			{
				let mut table = write_txn
					.open_table(TIERED_SLABS_TABLE)
					.map_err(redb_to_io)?;
				for slab_id in update.delete_tiered_slab_ids {
					let _ = table.remove(&slab_id.get()).map_err(redb_to_io)?;
				}
				for pointer in update.put_tiered_slabs {
					let value = encode_tiered_pointer(&pointer);
					table
						.insert(&pointer.slab_id.get(), value.as_slice())
						.map_err(redb_to_io)?;
				}
			}
			{
				let mut table = write_txn
					.open_table(SECONDARY_INDEX_SEGMENTS_TABLE)
					.map_err(redb_to_io)?;
				for key in update.delete_secondary_index_segments {
					let encoded = encode_secondary_index_segment_key(key);
					let _ = table.remove(encoded.as_slice()).map_err(redb_to_io)?;
				}
				for row in update.put_secondary_index_segments {
					let key = encode_secondary_index_segment_key(row.key);
					let value = encode_secondary_index_segment_value(&row);
					table
						.insert(key.as_slice(), value.as_slice())
						.map_err(redb_to_io)?;
				}
			}
			{
				let mut table = write_txn.open_table(LSM_RUNS_TABLE).map_err(redb_to_io)?;
				for slab_id in update.delete_lsm_run_ids {
					let _ = table.remove(&slab_id.get()).map_err(redb_to_io)?;
				}
				for run in update.put_lsm_runs {
					let value = encode_lsm_run_summary(&run)?;
					table
						.insert(&run.slab_id.get(), value.as_slice())
						.map_err(redb_to_io)?;
				}
			}
			if let Some(index) = update.last_control_log_index {
				let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
				let current = meta
					.get(META_LAST_CONTROL_LOG_INDEX)
					.map_err(redb_to_io)?
					.map(|value| value.value())
					.unwrap_or(0);
				if index >= current {
					meta.insert(META_LAST_CONTROL_LOG_INDEX, &index)
						.map_err(redb_to_io)?;
				}
			}
			write_txn.commit().map_err(redb_to_io)
		})
		.await
	}

	pub(crate) async fn upsert_record(&self, row: PersistentRecordRow) -> std::io::Result<()> {
		self.apply(PersistentIndexUpdate {
			put_records: vec![row],
			..Default::default()
		})
		.await
	}

	pub(crate) async fn upsert_tiered_slab(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		self.apply(PersistentIndexUpdate {
			put_tiered_slabs: vec![pointer],
			last_control_log_index,
			..Default::default()
		})
		.await
	}

	pub(crate) async fn upsert_secondary_index_segment(
		&self,
		row: PersistentSecondaryIndexSegmentRow,
	) -> std::io::Result<()> {
		self.apply(PersistentIndexUpdate {
			put_secondary_index_segments: vec![row],
			..Default::default()
		})
		.await
	}

	pub(crate) async fn delete_secondary_index_segment(
		&self,
		key: PersistentSecondaryIndexSegmentKey,
	) -> std::io::Result<()> {
		self.apply(PersistentIndexUpdate {
			delete_secondary_index_segments: vec![key],
			..Default::default()
		})
		.await
	}
}

pub(crate) fn persistent_index_path(dir: &Path) -> PathBuf {
	dir.join("storage_index.redb")
}

async fn spawn_blocking_io<T>(
	f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T>
where
	T: Send + 'static,
{
	tokio::task::spawn_blocking(f)
		.await
		.map_err(|err| Error::other(format!("blocking redb task failed: {err}")))?
}

fn redb_to_io(err: impl std::fmt::Display) -> std::io::Error {
	Error::other(format!("redb error: {err}"))
}

fn encode_record_key(row: PersistentRecordRow) -> [u8; RECORD_KEY_LEN] {
	let mut bytes = [0u8; RECORD_KEY_LEN];
	let mut cursor = 0;
	bytes[cursor..cursor + 8].copy_from_slice(&row.tenant_id.get().to_le_bytes());
	cursor += 8;
	bytes[cursor..cursor + 32].copy_from_slice(row.key_identity.as_bytes());
	cursor += 32;
	bytes[cursor..cursor + 8].copy_from_slice(&row.location.hlc.physical_ms.to_le_bytes());
	cursor += 8;
	bytes[cursor..cursor + 4].copy_from_slice(&row.location.hlc.logical.to_le_bytes());
	cursor += 4;
	bytes[cursor..cursor + 4].copy_from_slice(&row.location.hlc.node_id.to_le_bytes());
	cursor += 4;
	bytes[cursor..cursor + 8].copy_from_slice(&row.location.slab_id.get().to_le_bytes());
	cursor += 8;
	bytes[cursor..cursor + 8].copy_from_slice(&row.location.offset.to_le_bytes());
	bytes
}

fn encode_record_value(row: PersistentRecordRow) -> [u8; RECORD_VALUE_LEN] {
	let mut bytes = [0u8; RECORD_VALUE_LEN];
	bytes[..4].copy_from_slice(&row.location.length.to_le_bytes());
	bytes[4..12].copy_from_slice(&row.routing_locality.get().to_le_bytes());
	bytes[12..16].copy_from_slice(&row.location.record_row_index.get().to_le_bytes());
	let mut flags = 0_u32;
	if row.location.is_tombstone {
		flags |= RECORD_FLAG_IS_TOMBSTONE;
	}
	if row.table_object_id.is_some() {
		flags |= RECORD_FLAG_HAS_TABLE_OBJECT_ID;
	}
	bytes[16..20].copy_from_slice(&flags.to_le_bytes());
	if let Some(table_object_id) = row.table_object_id {
		bytes[20..36].copy_from_slice(table_object_id.as_bytes());
	}
	bytes[36] = match row.placement {
		PlacementSemantics::RawLocality => PLACEMENT_STAMP_RAW_LOCALITY,
		PlacementSemantics::Partitioned => PLACEMENT_STAMP_PARTITIONED,
	};
	let partition = row.partition.map_or(0, |partition| partition.get());
	bytes[37..45].copy_from_slice(&partition.to_le_bytes());
	bytes
}

fn decode_record_row(key: &[u8], value: &[u8]) -> std::io::Result<PersistentRecordRow> {
	if key.len() != RECORD_KEY_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"persistent record key length mismatch: expected {RECORD_KEY_LEN}, got {}",
				key.len()
			),
		));
	}
	if value.len() != RECORD_VALUE_LEN
		&& value.len() != RECORD_VALUE_LEN_V4
		&& value.len() != RECORD_VALUE_LEN_V3
		&& value.len() != RECORD_VALUE_LEN_V2
		&& value.len() != RECORD_VALUE_LEN_V1
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"persistent record value length mismatch: expected {RECORD_VALUE_LEN_V1}, {RECORD_VALUE_LEN_V2}, {RECORD_VALUE_LEN_V3}, {RECORD_VALUE_LEN_V4}, or {RECORD_VALUE_LEN}, got {}",
				value.len()
			),
		));
	}

	let mut cursor = 0;
	let tenant_id = TenantId::new(read_u64(key, &mut cursor)?);
	let key_identity = Identity::new(read_array_32(key, &mut cursor)?);
	let physical_ms = read_u64(key, &mut cursor)?;
	let logical = read_u32(key, &mut cursor)?;
	let node_id = read_u32(key, &mut cursor)?;
	let slab_id = read_u64(key, &mut cursor)?;
	let offset = read_u64(key, &mut cursor)?;
	let length = u32::from_le_bytes(value[..4].try_into().expect("validated above"));
	let routing_locality = if value.len() >= RECORD_VALUE_LEN_V2 {
		Locality::new(u64::from_le_bytes(
			value[4..12].try_into().expect("validated above"),
		))
	} else {
		Locality::from(key_identity)
	};
	let record_row_index = if value.len() >= RECORD_VALUE_LEN_V3 {
		RecordRowIndex::new(u32::from_le_bytes(
			value[12..16].try_into().expect("validated above"),
		))
	} else {
		RecordRowIndex::ZERO
	};
	let flags = if value.len() >= RECORD_VALUE_LEN_V3 {
		u32::from_le_bytes(value[16..20].try_into().expect("validated above"))
	} else {
		0
	};
	if flags & !(RECORD_FLAG_IS_TOMBSTONE | RECORD_FLAG_HAS_TABLE_OBJECT_ID) != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("persistent record row has unknown flags 0x{flags:08x}"),
		));
	}
	let table_object_id = if flags & RECORD_FLAG_HAS_TABLE_OBJECT_ID != 0 {
		if value.len() < RECORD_VALUE_LEN_V4 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"persistent record row declares a table object id without V4 storage",
			));
		}
		Some(TableObjectId::new(
			value[20..36].try_into().expect("validated V4 record value"),
		))
	} else {
		None
	};

	// ADR 0090: a value shorter than V5 predates partitions and is legacy by
	// length. A V5-length value carries an explicit stamp, and an unrecognized
	// stamp is malformed rather than quietly legacy — absence and damage must
	// not share an outcome.
	let (placement, partition) = if value.len() >= RECORD_VALUE_LEN_V5 {
		let stamp = value[36];
		match stamp {
			PLACEMENT_STAMP_RAW_LOCALITY => (PlacementSemantics::RawLocality, None),
			PLACEMENT_STAMP_PARTITIONED => (
				PlacementSemantics::Partitioned,
				Some(PartitionId::new(u64::from_le_bytes(
					value[37..45].try_into().expect("validated V5 record value"),
				))),
			),
			other => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"persistent record row has unknown placement stamp {other}; \
						 a V5 value with an unrecognized stamp is malformed, not legacy"
					),
				));
			}
		}
	} else {
		(PlacementSemantics::RawLocality, None)
	};

	Ok(PersistentRecordRow {
		tenant_id,
		key_identity,
		routing_locality,
		table_object_id,
		placement,
		partition,
		location: RecordLocation {
			placement,
			partition,
			slab_id: SlabId::new(slab_id),
			offset,
			length,
			hlc: HybridTimestamp {
				physical_ms,
				logical,
				node_id,
			},
			record_row_index,
			is_tombstone: flags & RECORD_FLAG_IS_TOMBSTONE != 0,
		},
	})
}

fn encode_secondary_index_segment_key(
	key: PersistentSecondaryIndexSegmentKey,
) -> [u8; SECONDARY_INDEX_SEGMENT_KEY_LEN] {
	let mut bytes = [0u8; SECONDARY_INDEX_SEGMENT_KEY_LEN];
	let mut cursor = 0;
	bytes[cursor..cursor + 8].copy_from_slice(&key.tenant_id.get().to_le_bytes());
	cursor += 8;
	bytes[cursor..cursor + 16].copy_from_slice(key.table_object_id.as_bytes());
	cursor += 16;
	bytes[cursor..cursor + 16].copy_from_slice(key.index_object_id.as_bytes());
	cursor += 16;
	bytes[cursor..cursor + 32].copy_from_slice(key.logical_schema_fingerprint.as_bytes());
	cursor += 32;
	bytes[cursor..cursor + 8].copy_from_slice(&key.source_slab_id.get().to_le_bytes());
	bytes
}

fn encode_secondary_index_segment_value(row: &PersistentSecondaryIndexSegmentRow) -> Vec<u8> {
	let segment_path = row.segment_path.as_bytes();
	let l2_uri = row.l2_uri.as_deref().unwrap_or("").as_bytes();
	let mut flags = 0_u32;
	if row.l2_uri.is_some() {
		flags |= SECONDARY_INDEX_SEGMENT_FLAG_HAS_L2_URI;
	}
	let mut bytes = Vec::with_capacity(
		SECONDARY_INDEX_SEGMENT_VALUE_PREFIX_LEN + segment_path.len() + l2_uri.len(),
	);
	bytes.extend_from_slice(&row.payload_hash);
	bytes.extend_from_slice(&row.state_digest);
	bytes.extend_from_slice(&row.format_version.to_le_bytes());
	bytes.extend_from_slice(&row.entry_count.to_le_bytes());
	bytes.extend_from_slice(&(segment_path.len() as u32).to_le_bytes());
	bytes.extend_from_slice(&flags.to_le_bytes());
	bytes.extend_from_slice(segment_path);
	bytes.extend_from_slice(l2_uri);
	bytes
}

fn decode_secondary_index_segment_row(
	key: &[u8],
	value: &[u8],
) -> std::io::Result<PersistentSecondaryIndexSegmentRow> {
	if key.len() != SECONDARY_INDEX_SEGMENT_KEY_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"secondary index segment key length mismatch: expected {SECONDARY_INDEX_SEGMENT_KEY_LEN}, got {}",
				key.len()
			),
		));
	}
	if value.len() < SECONDARY_INDEX_SEGMENT_VALUE_PREFIX_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"secondary index segment value too short: expected at least {SECONDARY_INDEX_SEGMENT_VALUE_PREFIX_LEN}, got {}",
				value.len()
			),
		));
	}

	let mut key_cursor = 0;
	let tenant_id = TenantId::new(read_u64(key, &mut key_cursor)?);
	let table_object_id = TableObjectId::new(read_array_16(key, &mut key_cursor)?);
	let index_object_id = IndexObjectId::new(read_array_16(key, &mut key_cursor)?);
	let logical_schema_fingerprint =
		LogicalSchemaFingerprint::from_bytes(read_array_32(key, &mut key_cursor)?);
	let source_slab_id = SlabId::new(read_u64(key, &mut key_cursor)?);

	let mut value_cursor = 0;
	let payload_hash = read_array_32(value, &mut value_cursor)?;
	let state_digest = read_array_32(value, &mut value_cursor)?;
	let format_version = read_u16(value, &mut value_cursor)?;
	let entry_count = read_u64(value, &mut value_cursor)?;
	let segment_path_len = read_u32(value, &mut value_cursor)? as usize;
	let flags = read_u32(value, &mut value_cursor)?;
	if flags & !SECONDARY_INDEX_SEGMENT_FLAG_KNOWN_MASK != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("secondary index segment row has unknown flags 0x{flags:08x}"),
		));
	}
	let segment_path_end = value_cursor.checked_add(segment_path_len).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"secondary index path length overflows",
		)
	})?;
	let segment_path_bytes = value.get(value_cursor..segment_path_end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"secondary index segment value truncated while reading segment_path",
		)
	})?;
	let segment_path = std::str::from_utf8(segment_path_bytes)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("segment_path utf8: {err}")))?
		.to_string();
	value_cursor = segment_path_end;
	let remaining = &value[value_cursor..];
	let l2_uri = if flags & SECONDARY_INDEX_SEGMENT_FLAG_HAS_L2_URI != 0 {
		Some(
			std::str::from_utf8(remaining)
				.map_err(|err| Error::new(ErrorKind::InvalidData, format!("l2_uri utf8: {err}")))?
				.to_string(),
		)
	} else if remaining.is_empty() {
		None
	} else {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"secondary index segment row has trailing bytes without l2_uri flag",
		));
	};

	Ok(PersistentSecondaryIndexSegmentRow {
		key: PersistentSecondaryIndexSegmentKey {
			tenant_id,
			table_object_id,
			index_object_id,
			logical_schema_fingerprint,
			source_slab_id,
		},
		segment_path,
		l2_uri,
		payload_hash,
		state_digest,
		format_version,
		entry_count,
	})
}

fn encode_lsm_run_summary(run: &LsmRunSummary) -> std::io::Result<Vec<u8>> {
	let mut bytes = Vec::with_capacity(LSM_RUN_VALUE_PREFIX_LEN);
	bytes.push(run.compaction_level.get());
	bytes.push(encode_lsm_slab_state_tag(&run.state));
	bytes.push(encode_lsm_sidecar_coverage(run.sidecar_coverage));
	bytes.push(0);
	bytes.extend_from_slice(&run.manifest_epoch.get().to_le_bytes());
	encode_hlc(&mut bytes, run.min_row_hlc);
	encode_hlc(&mut bytes, run.max_row_hlc);
	bytes.extend_from_slice(&run.size_bytes.get().to_le_bytes());
	bytes.extend_from_slice(&run.row_count.get().to_le_bytes());
	bytes.extend_from_slice(&run.retention_debt_score.to_le_bytes());
	bytes.extend_from_slice(&run.tombstone_debt_score.to_le_bytes());
	bytes.extend_from_slice(&run.index_coverage_debt_score.to_le_bytes());
	bytes.extend_from_slice(&run.layout_debt_score.to_le_bytes());
	let successors = match &run.state {
		SlabState::Retired { successors } => successors.as_slice(),
		_ => &[],
	};
	let successor_count = u32::try_from(successors.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"LSM run successor count does not fit in u32",
		)
	})?;
	bytes.extend_from_slice(&successor_count.to_le_bytes());
	for successor in successors {
		bytes.extend_from_slice(&successor.get().to_le_bytes());
	}
	Ok(bytes)
}

fn decode_lsm_run_summary(slab_id: SlabId, value: &[u8]) -> std::io::Result<LsmRunSummary> {
	if value.len() < LSM_RUN_VALUE_PREFIX_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"LSM run value too short: expected at least {LSM_RUN_VALUE_PREFIX_LEN}, got {}",
				value.len()
			),
		));
	}
	let mut cursor = 0usize;
	let compaction_level = read_u8(value, &mut cursor).and_then(|raw| {
		CompactionLevelIndex::try_new(raw).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!("LSM run has invalid compaction level {raw}"),
			)
		})
	})?;
	let state_tag = read_u8(value, &mut cursor)?;
	let sidecar_coverage = decode_lsm_sidecar_coverage(read_u8(value, &mut cursor)?)?;
	let reserved = read_u8(value, &mut cursor)?;
	if reserved != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("LSM run reserved byte is non-zero: {reserved}"),
		));
	}
	let manifest_epoch = ManifestEpoch::new(read_u64(value, &mut cursor)?);
	let min_row_hlc = read_hlc(value, &mut cursor)?;
	let max_row_hlc = read_hlc(value, &mut cursor)?;
	let size_bytes = ByteCount::new(read_u64(value, &mut cursor)?);
	let row_count = RowCount::new(read_u64(value, &mut cursor)?);
	let retention_debt_score = read_u32(value, &mut cursor)?;
	let tombstone_debt_score = read_u32(value, &mut cursor)?;
	let index_coverage_debt_score = read_u32(value, &mut cursor)?;
	let layout_debt_score = read_u32(value, &mut cursor)?;
	let successor_count = read_u32(value, &mut cursor)? as usize;
	let successor_bytes = successor_count
		.checked_mul(8)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "LSM successor byte count overflows"))?;
	if value.len() != cursor + successor_bytes {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"LSM run value has truncated or trailing successor bytes",
		));
	}
	let mut successors = Vec::with_capacity(successor_count);
	for _ in 0..successor_count {
		successors.push(SlabId::new(read_u64(value, &mut cursor)?));
	}
	let state = match state_tag {
		LSM_SLAB_STATE_LIVE if successors.is_empty() => SlabState::Live,
		LSM_SLAB_STATE_RETIRED => SlabState::Retired { successors },
		LSM_SLAB_STATE_DELETE_PENDING if successors.is_empty() => SlabState::DeletePending,
		_ => {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("LSM run has invalid state tag/successor combination {state_tag}"),
			));
		}
	};
	Ok(LsmRunSummary {
		slab_id,
		compaction_level,
		state,
		min_row_hlc,
		max_row_hlc,
		size_bytes,
		row_count,
		sidecar_coverage,
		manifest_epoch,
		retention_debt_score,
		tombstone_debt_score,
		index_coverage_debt_score,
		layout_debt_score,
	})
}

fn encode_hlc(bytes: &mut Vec<u8>, hlc: HybridTimestamp) {
	bytes.extend_from_slice(&hlc.physical_ms.to_le_bytes());
	bytes.extend_from_slice(&hlc.logical.to_le_bytes());
	bytes.extend_from_slice(&hlc.node_id.to_le_bytes());
}

fn read_hlc(bytes: &[u8], cursor: &mut usize) -> std::io::Result<HybridTimestamp> {
	Ok(HybridTimestamp {
		physical_ms: read_u64(bytes, cursor)?,
		logical: read_u32(bytes, cursor)?,
		node_id: read_u32(bytes, cursor)?,
	})
}

fn encode_lsm_slab_state_tag(state: &SlabState) -> u8 {
	match state {
		SlabState::Live => LSM_SLAB_STATE_LIVE,
		SlabState::Retired { .. } => LSM_SLAB_STATE_RETIRED,
		SlabState::DeletePending => LSM_SLAB_STATE_DELETE_PENDING,
	}
}

fn encode_lsm_sidecar_coverage(coverage: SidecarCoverage) -> u8 {
	match coverage {
		SidecarCoverage::None => LSM_SIDECAR_COVERAGE_NONE,
		SidecarCoverage::Partial => LSM_SIDECAR_COVERAGE_PARTIAL,
		SidecarCoverage::Complete => LSM_SIDECAR_COVERAGE_COMPLETE,
	}
}

fn decode_lsm_sidecar_coverage(raw: u8) -> std::io::Result<SidecarCoverage> {
	match raw {
		LSM_SIDECAR_COVERAGE_NONE => Ok(SidecarCoverage::None),
		LSM_SIDECAR_COVERAGE_PARTIAL => Ok(SidecarCoverage::Partial),
		LSM_SIDECAR_COVERAGE_COMPLETE => Ok(SidecarCoverage::Complete),
		_ => Err(Error::new(
			ErrorKind::InvalidData,
			format!("LSM run has invalid sidecar coverage {raw}"),
		)),
	}
}

fn encode_tiered_pointer(pointer: &TieredSlabPointer) -> Vec<u8> {
	let mut bytes = Vec::with_capacity(TIERED_POINTER_PREFIX_LEN + pointer.l2_uri.len());
	bytes.extend_from_slice(&pointer.payload_hash);
	let mut flags = 0u8;
	let mut flags2 = 0u8;
	if pointer.is_encrypted {
		flags |= TIERED_FLAG_IS_ENCRYPTED;
	}
	if pointer.tenant_id.is_some() {
		flags |= TIERED_FLAG_HAS_TENANT_ID;
	}
	if pointer.tenant_kek_id.is_some() {
		flags |= TIERED_FLAG_HAS_TENANT_KEK_ID;
	}
	if pointer.stats_format_version.is_some() {
		flags |= TIERED_FLAG_HAS_STATS_FORMAT_VERSION;
	}
	if pointer.stats_block_len.is_some() {
		flags |= TIERED_FLAG_HAS_STATS_BLOCK_LEN;
	}
	if pointer.stats_block_hash128.is_some() {
		flags |= TIERED_FLAG_HAS_STATS_BLOCK_HASH128;
	}
	match pointer.tombstone_metadata {
		TombstoneMetadata::Untracked => {}
		TombstoneMetadata::None => {
			flags |= TIERED_FLAG_TOMBSTONE_METADATA_TRACKED;
		}
		TombstoneMetadata::Earliest(_) => {
			flags |= TIERED_FLAG_TOMBSTONE_METADATA_TRACKED | TIERED_FLAG_HAS_FIRST_TOMBSTONE_HLC;
		}
	}
	match pointer.supersession_metadata {
		SupersessionMetadata::Untracked => {}
		SupersessionMetadata::None => {
			flags2 |= TIERED_FLAG2_SUPERSESSION_METADATA_TRACKED;
		}
		SupersessionMetadata::Earliest(_) => {
			flags2 |= TIERED_FLAG2_SUPERSESSION_METADATA_TRACKED
				| TIERED_FLAG2_HAS_FIRST_SUPERSESSION_HLC;
		}
	}
	bytes.push(flags);
	bytes.push(flags2);
	if let Some(tenant_id) = pointer.tenant_id {
		bytes.extend_from_slice(&tenant_id.get().to_le_bytes());
	}
	if let Some(tenant_kek_id) = pointer.tenant_kek_id {
		bytes.extend_from_slice(&tenant_kek_id.get().to_le_bytes());
	}
	if let Some(stats_format_version) = pointer.stats_format_version {
		bytes.extend_from_slice(&stats_format_version.to_le_bytes());
	}
	if let Some(stats_block_len) = pointer.stats_block_len {
		bytes.extend_from_slice(&stats_block_len.to_le_bytes());
	}
	if let Some(stats_block_hash128) = pointer.stats_block_hash128 {
		bytes.extend_from_slice(&stats_block_hash128);
	}
	if let TombstoneMetadata::Earliest(tombstone_metadata) = pointer.tombstone_metadata {
		bytes.extend_from_slice(&tombstone_metadata.physical_ms.to_le_bytes());
		bytes.extend_from_slice(&tombstone_metadata.logical.to_le_bytes());
		bytes.extend_from_slice(&tombstone_metadata.node_id.to_le_bytes());
	}
	if let SupersessionMetadata::Earliest(supersession_metadata) = pointer.supersession_metadata {
		bytes.extend_from_slice(&supersession_metadata.physical_ms.to_le_bytes());
		bytes.extend_from_slice(&supersession_metadata.logical.to_le_bytes());
		bytes.extend_from_slice(&supersession_metadata.node_id.to_le_bytes());
	}
	bytes.extend_from_slice(pointer.l2_uri.as_bytes());
	bytes
}

fn decode_tiered_pointer(slab_id: SlabId, value: &[u8]) -> std::io::Result<TieredSlabPointer> {
	if value.len() < TIERED_POINTER_PREFIX_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"tiered slab value too short: expected at least {TIERED_POINTER_PREFIX_LEN}, got {}",
				value.len()
			),
		));
	}
	let mut payload_hash = [0u8; TIERED_POINTER_HASH_LEN];
	payload_hash.copy_from_slice(&value[..TIERED_POINTER_HASH_LEN]);
	let flags = value[TIERED_POINTER_HASH_LEN];
	let flags2 = value[TIERED_POINTER_HASH_LEN + 1];
	if flags & !TIERED_FLAG_KNOWN_MASK != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("tiered slab value has unknown flags 0x{flags:02x}"),
		));
	}
	if flags2 & !TIERED_FLAG2_KNOWN_MASK != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("tiered slab value has unknown flags2 0x{flags2:02x}"),
		));
	}
	let is_encrypted = (flags & TIERED_FLAG_IS_ENCRYPTED) != 0;
	let mut cursor = TIERED_POINTER_PREFIX_LEN;
	let tenant_id = if (flags & TIERED_FLAG_HAS_TENANT_ID) != 0 {
		let end = cursor.saturating_add(8);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding tenant_id",
			)
		})?;
		cursor = end;
		Some(TenantId::new(u64::from_le_bytes(
			slot.try_into().expect("fixed width"),
		)))
	} else {
		None
	};
	let tenant_kek_id = if (flags & TIERED_FLAG_HAS_TENANT_KEK_ID) != 0 {
		let end = cursor.saturating_add(8);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding tenant_kek_id",
			)
		})?;
		cursor = end;
		Some(
			TenantKekId::try_from(u64::from_le_bytes(slot.try_into().expect("fixed width")))
				.map_err(|err| {
					Error::new(
						ErrorKind::InvalidData,
						format!("invalid tiered slab tenant_kek_id: {err}"),
					)
				})?,
		)
	} else {
		None
	};
	let stats_format_version = if (flags & TIERED_FLAG_HAS_STATS_FORMAT_VERSION) != 0 {
		let end = cursor.saturating_add(2);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding stats_format_version",
			)
		})?;
		cursor = end;
		Some(u16::from_le_bytes(slot.try_into().expect("fixed width")))
	} else {
		None
	};
	let stats_block_len = if (flags & TIERED_FLAG_HAS_STATS_BLOCK_LEN) != 0 {
		let end = cursor.saturating_add(4);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding stats_block_len",
			)
		})?;
		cursor = end;
		Some(u32::from_le_bytes(slot.try_into().expect("fixed width")))
	} else {
		None
	};
	let stats_block_hash128 = if (flags & TIERED_FLAG_HAS_STATS_BLOCK_HASH128) != 0 {
		let end = cursor.saturating_add(16);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding stats_block_hash128",
			)
		})?;
		cursor = end;
		let mut hash = [0_u8; 16];
		hash.copy_from_slice(slot);
		Some(hash)
	} else {
		None
	};
	let tombstone_metadata = if (flags & TIERED_FLAG_HAS_FIRST_TOMBSTONE_HLC) != 0 {
		let end = cursor.saturating_add(16);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding tombstone_metadata",
			)
		})?;
		cursor = end;
		TombstoneMetadata::Earliest(HybridTimestamp {
			physical_ms: u64::from_le_bytes(slot[0..8].try_into().expect("fixed width")),
			logical: u32::from_le_bytes(slot[8..12].try_into().expect("fixed width")),
			node_id: u32::from_le_bytes(slot[12..16].try_into().expect("fixed width")),
		})
	} else if (flags & TIERED_FLAG_TOMBSTONE_METADATA_TRACKED) != 0 {
		TombstoneMetadata::None
	} else {
		TombstoneMetadata::Untracked
	};
	let supersession_metadata = if (flags2 & TIERED_FLAG2_HAS_FIRST_SUPERSESSION_HLC) != 0 {
		let end = cursor.saturating_add(16);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding supersession_metadata",
			)
		})?;
		cursor = end;
		SupersessionMetadata::Earliest(HybridTimestamp {
			physical_ms: u64::from_le_bytes(slot[0..8].try_into().expect("fixed width")),
			logical: u32::from_le_bytes(slot[8..12].try_into().expect("fixed width")),
			node_id: u32::from_le_bytes(slot[12..16].try_into().expect("fixed width")),
		})
	} else if (flags2 & TIERED_FLAG2_SUPERSESSION_METADATA_TRACKED) != 0 {
		SupersessionMetadata::None
	} else {
		SupersessionMetadata::Untracked
	};
	let l2_uri = std::str::from_utf8(&value[cursor..])
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("tiered slab URI is not utf8: {err}"),
			)
		})?
		.to_string();
	Ok(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash,
		is_encrypted,
		tenant_id,
		tenant_kek_id,
		stats_format_version,
		stats_block_len,
		stats_block_hash128,
		tombstone_metadata,
		supersession_metadata,
	})
}

/// Decode a v1 tiered pointer record. v1 lacked the
/// `flags_byte`, so any pointer recovered from v1 defaults to
/// `is_encrypted = false`. The plaintext-rewrite sweeper will
/// GET each one, encrypt it, and stamp `is_encrypted = true`
/// back through the v2 table on the next pass — converging on the
/// steady-state short-circuit.
fn decode_tiered_pointer_v1(slab_id: SlabId, value: &[u8]) -> std::io::Result<TieredSlabPointer> {
	if value.len() < TIERED_POINTER_HASH_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"v1 tiered slab value too short: expected at least {TIERED_POINTER_HASH_LEN}, got {}",
				value.len()
			),
		));
	}
	let mut payload_hash = [0u8; TIERED_POINTER_HASH_LEN];
	payload_hash.copy_from_slice(&value[..TIERED_POINTER_HASH_LEN]);
	let l2_uri = std::str::from_utf8(&value[TIERED_POINTER_HASH_LEN..])
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("v1 tiered slab URI is not utf8: {err}"),
			)
		})?
		.to_string();
	Ok(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash,
		is_encrypted: false,
		tenant_id: None,
		tenant_kek_id: None,
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::Untracked,
		supersession_metadata: SupersessionMetadata::Untracked,
	})
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> std::io::Result<u64> {
	let end = cursor.saturating_add(8);
	let slice = bytes.get(*cursor..end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading u64",
		)
	})?;
	*cursor = end;
	Ok(u64::from_le_bytes(slice.try_into().expect("fixed width")))
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> std::io::Result<u16> {
	let end = cursor.saturating_add(2);
	let slice = bytes.get(*cursor..end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading u16",
		)
	})?;
	*cursor = end;
	Ok(u16::from_le_bytes(slice.try_into().expect("fixed width")))
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> std::io::Result<u8> {
	let byte = bytes.get(*cursor).copied().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading u8",
		)
	})?;
	*cursor = (*cursor).saturating_add(1);
	Ok(byte)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> std::io::Result<u32> {
	let end = cursor.saturating_add(4);
	let slice = bytes.get(*cursor..end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading u32",
		)
	})?;
	*cursor = end;
	Ok(u32::from_le_bytes(slice.try_into().expect("fixed width")))
}

fn read_array_16(bytes: &[u8], cursor: &mut usize) -> std::io::Result<[u8; 16]> {
	let end = cursor.saturating_add(16);
	let slice = bytes.get(*cursor..end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading 16 bytes",
		)
	})?;
	*cursor = end;
	Ok(slice.try_into().expect("fixed width"))
}

fn read_array_32(bytes: &[u8], cursor: &mut usize) -> std::io::Result<[u8; 32]> {
	let end = cursor.saturating_add(32);
	let slice = bytes.get(*cursor..end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading identity bytes",
		)
	})?;
	*cursor = end;
	Ok(slice.try_into().expect("fixed width"))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn temp_path(tag: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"wendelon-redb-{tag}-{}-{}.redb",
			std::process::id(),
			uuid::Uuid::new_v4()
		))
	}

	fn row() -> PersistentRecordRow {
		PersistentRecordRow {
			tenant_id: TenantId::new(7),
			key_identity: Identity::blake3(b"row-key"),
			routing_locality: Locality::new(0x8877),
			table_object_id: None,
			placement: PlacementSemantics::RawLocality,
			partition: None,
			location: RecordLocation {
				slab_id: SlabId::new(9),
				offset: 128,
				length: 4096,
				record_row_index: RecordRowIndex::new(7),
				hlc: HybridTimestamp {
					physical_ms: 55,
					logical: 3,
					node_id: 11,
				},
				is_tombstone: false,
				placement: PlacementSemantics::RawLocality,
				partition: None,
			},
		}
	}

	fn secondary_index_segment_row() -> PersistentSecondaryIndexSegmentRow {
		PersistentSecondaryIndexSegmentRow {
			key: PersistentSecondaryIndexSegmentKey {
				tenant_id: TenantId::new(7),
				table_object_id: TableObjectId::new([0x10; 16]),
				index_object_id: IndexObjectId::new([0x52; 16]),
				logical_schema_fingerprint: LogicalSchemaFingerprint::from_bytes([0x51; 32]),
				source_slab_id: SlabId::new(41),
			},
			segment_path: "secondary_index/7/table/index/0000000000000029.idx".to_owned(),
			l2_uri: Some("file:///tmp/secondary-index/0000000000000029.idx".to_owned()),
			payload_hash: [0xAB; 32],
			state_digest: [0xCD; 32],
			format_version: crate::storage::SECONDARY_INDEX_SEGMENT_FORMAT_VERSION,
			entry_count: 17,
		}
	}

	fn lsm_run() -> LsmRunSummary {
		LsmRunSummary {
			slab_id: SlabId::new(51),
			compaction_level: CompactionLevelIndex::L1,
			state: SlabState::Retired {
				successors: vec![SlabId::new(71), SlabId::new(72)],
			},
			min_row_hlc: HybridTimestamp {
				physical_ms: 10,
				logical: 1,
				node_id: 2,
			},
			max_row_hlc: HybridTimestamp {
				physical_ms: 20,
				logical: 3,
				node_id: 4,
			},
			size_bytes: ByteCount::new(8192),
			row_count: RowCount::new(7),
			sidecar_coverage: SidecarCoverage::Complete,
			manifest_epoch: ManifestEpoch::new(9),
			retention_debt_score: 1,
			tombstone_debt_score: 2,
			index_coverage_debt_score: 3,
			layout_debt_score: 4,
		}
	}

	#[test]
	fn record_row_roundtrip_preserves_every_field() {
		let row = row();
		let key = encode_record_key(row);
		let value = encode_record_value(row);
		let decoded = decode_record_row(&key, &value).unwrap();
		assert_eq!(decoded, row);
	}

	#[test]
	fn secondary_index_segment_row_roundtrip_preserves_every_field() {
		let row = secondary_index_segment_row();
		let key = encode_secondary_index_segment_key(row.key);
		let value = encode_secondary_index_segment_value(&row);
		let decoded = decode_secondary_index_segment_row(&key, &value).unwrap();
		assert_eq!(decoded, row);
	}

	#[test]
	fn lsm_run_summary_roundtrip_preserves_every_field() {
		let run = lsm_run();
		let value = encode_lsm_run_summary(&run).unwrap();
		let decoded = decode_lsm_run_summary(run.slab_id, &value).unwrap();
		assert_eq!(decoded, run);
	}

	#[test]
	fn secondary_index_segment_row_rejects_trailing_l2_without_flag() {
		let mut row = secondary_index_segment_row();
		row.l2_uri = None;
		let key = encode_secondary_index_segment_key(row.key);
		let mut value = encode_secondary_index_segment_value(&row);
		value.extend_from_slice(b"unexpected");
		let err = decode_secondary_index_segment_row(&key, &value).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(err.to_string().contains("trailing bytes"));
	}

	#[test]
	fn record_row_value_encoding_matches_row_index_golden_bytes() {
		let value = encode_record_value(row());
		assert_eq!(value.len(), RECORD_VALUE_LEN);
		assert_eq!(
			value,
			[
				0x00, 0x10, 0x00, 0x00, // length = 4096
				0x77, 0x88, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // locality = 0x8877
				0x07, 0x00, 0x00, 0x00, // record_row_index = 7
				0x00, 0x00, 0x00, 0x00, // flags
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // table object id absent
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// ADR 0090 V5 tail: placement stamp then partition.
				0x00, // stamp = RawLocality
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // partition absent
			]
		);
	}

	#[test]
	fn record_row_value_roundtrips_tombstone_flag() {
		let mut row = row();
		row.location.is_tombstone = true;
		let value = encode_record_value(row);
		assert_eq!(
			u32::from_le_bytes(value[16..20].try_into().unwrap()) & RECORD_FLAG_IS_TOMBSTONE,
			RECORD_FLAG_IS_TOMBSTONE
		);
		assert_eq!(
			decode_record_row(&encode_record_key(row), &value).unwrap(),
			row
		);
	}

	#[test]
	fn record_row_value_roundtrips_table_object_id() {
		let mut row = row();
		row.table_object_id = Some(TableObjectId::new([0x53; 16]));
		let value = encode_record_value(row);
		assert_eq!(
			u32::from_le_bytes(value[16..20].try_into().unwrap()) & RECORD_FLAG_HAS_TABLE_OBJECT_ID,
			RECORD_FLAG_HAS_TABLE_OBJECT_ID
		);
		assert_eq!(&value[20..36], &[0x53; 16]);
		assert_eq!(
			decode_record_row(&encode_record_key(row), &value).unwrap(),
			row
		);
	}

	#[test]
	fn legacy_v3_record_row_decodes_without_table_attribution() {
		let row = row();
		let key = encode_record_key(row);
		let v4 = encode_record_value(row);
		let decoded = decode_record_row(&key, &v4[..RECORD_VALUE_LEN_V3]).unwrap();
		assert_eq!(decoded.table_object_id, None);
		assert_eq!(decoded.location, row.location);
	}

	#[test]
	fn tiered_pointer_roundtrip_preserves_every_field() {
		let pointer = TieredSlabPointer {
			slab_id: SlabId::new(3),
			l2_uri: "file:///tmp/example".into(),
			payload_hash: [0xAB; 32],
			is_encrypted: true,
			tenant_id: Some(TenantId::new(9)),
			tenant_kek_id: Some(TenantKekId::try_from(11).unwrap()),
			stats_format_version: Some(1),
			stats_block_len: Some(128),
			stats_block_hash128: Some([0x5A; 16]),
			tombstone_metadata: TombstoneMetadata::Earliest(HybridTimestamp {
				physical_ms: 99,
				logical: 1,
				node_id: 2,
			}),
			supersession_metadata: SupersessionMetadata::Earliest(HybridTimestamp {
				physical_ms: 199,
				logical: 2,
				node_id: 3,
			}),
		};
		let encoded = encode_tiered_pointer(&pointer);
		let decoded = decode_tiered_pointer(pointer.slab_id, &encoded).unwrap();
		assert_eq!(decoded, pointer);

		for tombstone_metadata in [
			TombstoneMetadata::Untracked,
			TombstoneMetadata::None,
			TombstoneMetadata::Earliest(HybridTimestamp {
				physical_ms: 100,
				logical: 2,
				node_id: 3,
			}),
		] {
			let pointer = TieredSlabPointer {
				tombstone_metadata,
				..pointer.clone()
			};
			let decoded =
				decode_tiered_pointer(pointer.slab_id, &encode_tiered_pointer(&pointer)).unwrap();
			assert_eq!(decoded.tombstone_metadata, tombstone_metadata);
		}
		for supersession_metadata in [
			SupersessionMetadata::Untracked,
			SupersessionMetadata::None,
			SupersessionMetadata::Earliest(HybridTimestamp {
				physical_ms: 200,
				logical: 3,
				node_id: 4,
			}),
		] {
			let pointer = TieredSlabPointer {
				supersession_metadata,
				..pointer.clone()
			};
			let decoded =
				decode_tiered_pointer(pointer.slab_id, &encode_tiered_pointer(&pointer)).unwrap();
			assert_eq!(decoded.supersession_metadata, supersession_metadata);
		}
	}

	#[test]
	fn tiered_pointer_v1_decode_defaults_to_plaintext() {
		let mut v1 = Vec::new();
		v1.extend_from_slice(&[0xCD; 32]);
		v1.extend_from_slice(b"file:///tmp/legacy");
		let decoded = decode_tiered_pointer_v1(SlabId::new(7), &v1).unwrap();
		assert!(!decoded.is_encrypted);
		assert_eq!(decoded.payload_hash, [0xCD; 32]);
		assert_eq!(decoded.l2_uri, "file:///tmp/legacy");
		assert_eq!(decoded.stats_format_version, None);
		assert_eq!(decoded.stats_block_len, None);
		assert_eq!(decoded.stats_block_hash128, None);
		assert_eq!(decoded.tombstone_metadata, TombstoneMetadata::Untracked);
		assert_eq!(
			decoded.supersession_metadata,
			SupersessionMetadata::Untracked
		);
	}

	#[tokio::test]
	async fn secondary_index_segments_persist_across_snapshot_and_delete() {
		let index = PersistentLocalIndex::open(temp_path("secondary-index-segment"))
			.await
			.unwrap();
		let row = secondary_index_segment_row();
		index
			.upsert_secondary_index_segment(row.clone())
			.await
			.unwrap();
		let snapshot = index.load_snapshot().await.unwrap();
		assert_eq!(snapshot.secondary_index_segments, vec![row.clone()]);

		index.delete_secondary_index_segment(row.key).await.unwrap();
		let snapshot = index.load_snapshot().await.unwrap();
		assert!(snapshot.secondary_index_segments.is_empty());
	}

	#[tokio::test]
	async fn lsm_runs_persist_across_snapshot_and_delete() {
		let index = PersistentLocalIndex::open(temp_path("lsm-runs"))
			.await
			.unwrap();
		let run = lsm_run();
		index
			.apply(PersistentIndexUpdate {
				put_lsm_runs: vec![run.clone()],
				..Default::default()
			})
			.await
			.unwrap();
		let snapshot = index.load_snapshot().await.unwrap();
		assert_eq!(snapshot.lsm_runs, vec![run.clone()]);

		index
			.apply(PersistentIndexUpdate {
				delete_lsm_run_ids: vec![run.slab_id],
				..Default::default()
			})
			.await
			.unwrap();
		let snapshot = index.load_snapshot().await.unwrap();
		assert!(snapshot.lsm_runs.is_empty());
	}

	#[tokio::test]
	async fn rebuild_preserves_existing_secondary_index_segments() {
		let index = PersistentLocalIndex::open(temp_path("secondary-index-preserve"))
			.await
			.unwrap();
		let segment = secondary_index_segment_row();
		index
			.rebuild_with_secondary_index_segments(
				Vec::new(),
				Vec::new(),
				vec![segment.clone()],
				None,
			)
			.await
			.unwrap();
		index
			.rebuild(vec![row()], Vec::new(), Some(33))
			.await
			.unwrap();
		let snapshot = index.load_snapshot().await.unwrap();
		assert_eq!(snapshot.secondary_index_segments, vec![segment]);
		assert_eq!(snapshot.last_control_log_index, Some(33));
	}

	#[tokio::test]
	async fn snapshot_only_surfaces_tiered_records() {
		let index = PersistentLocalIndex::open(temp_path("snapshot"))
			.await
			.unwrap();
		let tiered_pointer = TieredSlabPointer {
			slab_id: SlabId::new(41),
			l2_uri: "file:///tmp/tiered".into(),
			payload_hash: [0x11; 32],
			is_encrypted: true,
			tenant_id: Some(TenantId::new(5)),
			tenant_kek_id: Some(TenantKekId::try_from(15).unwrap()),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		let local_row = PersistentRecordRow {
			location: RecordLocation {
				slab_id: SlabId::new(7),
				..row().location
			},
			..row()
		};
		let tiered_row = PersistentRecordRow {
			location: RecordLocation {
				slab_id: tiered_pointer.slab_id,
				..row().location
			},
			..row()
		};
		index
			.rebuild(
				vec![local_row, tiered_row],
				vec![tiered_pointer.clone()],
				None,
			)
			.await
			.unwrap();

		let snapshot = index.load_snapshot().await.unwrap();
		assert_eq!(snapshot.tiered_slabs, vec![tiered_pointer]);
		assert_eq!(snapshot.tiered_records, vec![tiered_row]);
		assert_eq!(
			snapshot.record_localities,
			vec![(row().tenant_id, row().key_identity, row().routing_locality)]
		);
	}

	#[tokio::test]
	async fn tiered_update_stamps_monotonic_control_log_index() {
		let index = PersistentLocalIndex::open(temp_path("watermark"))
			.await
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id: SlabId::new(9),
			l2_uri: "file:///tmp/tiered-watermark".into(),
			payload_hash: [0x22; 32],
			is_encrypted: true,
			tenant_id: Some(TenantId::new(6)),
			tenant_kek_id: Some(TenantKekId::try_from(16).unwrap()),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};

		index
			.upsert_tiered_slab(pointer.clone(), Some(12))
			.await
			.unwrap();
		let first = index.load_snapshot().await.unwrap();
		assert_eq!(first.tiered_slabs, vec![pointer.clone()]);
		assert_eq!(first.last_control_log_index, Some(12));

		index
			.upsert_tiered_slab(pointer.clone(), Some(7))
			.await
			.unwrap();
		let second = index.load_snapshot().await.unwrap();
		assert_eq!(second.last_control_log_index, Some(12));

		index.upsert_tiered_slab(pointer, Some(19)).await.unwrap();
		let third = index.load_snapshot().await.unwrap();
		assert_eq!(third.last_control_log_index, Some(19));
	}
}

/// ADR 0090: the length-dispatched compatibility ladder. A value shorter than V5
/// predates partitions and decodes as legacy; a V5-length value carries an
/// explicit stamp, and an unrecognized stamp is malformed. "Written before
/// partitions" and "corrupt" must never resolve to the same outcome.
#[cfg(test)]
mod adr0090_record_value_versions {
	use super::*;

	fn row() -> PersistentRecordRow {
		PersistentRecordRow {
			tenant_id: TenantId::new(7),
			key_identity: Identity::new([3; 32]),
			routing_locality: Locality::new(0x00AB_0000_0000_1234),
			table_object_id: Some(TableObjectId::new([9; 16])),
			placement: PlacementSemantics::RawLocality,
			partition: None,
			location: RecordLocation {
				slab_id: SlabId::new(11),
				offset: 4096,
				length: 512,
				hlc: HybridTimestamp {
					physical_ms: 100,
					logical: 1,
					node_id: 2,
				},
				record_row_index: RecordRowIndex::new(3),
				is_tombstone: false,
				placement: PlacementSemantics::RawLocality,
				partition: None,
			},
		}
	}

	#[test]
	fn adr0090_v5_round_trips_both_placements() {
		for (placement, partition) in [
			(PlacementSemantics::RawLocality, None),
			(
				PlacementSemantics::Partitioned,
				Some(PartitionId::new(0x00AB_0000_0000_0000)),
			),
			// Partition zero is a legitimate partition, not an absent one.
			(PlacementSemantics::Partitioned, Some(PartitionId::new(0))),
		] {
			let mut original = row();
			original.placement = placement;
			original.partition = partition;
			let key = encode_record_key(original);
			let value = encode_record_value(original);
			assert_eq!(value.len(), RECORD_VALUE_LEN_V5);
			let decoded = decode_record_row(&key, &value).expect("V5 decodes");
			assert_eq!(decoded.placement, placement);
			assert_eq!(decoded.partition, partition);
			assert_eq!(decoded.routing_locality, original.routing_locality);
			assert_eq!(decoded.table_object_id, original.table_object_id);
		}
	}

	#[test]
	fn adr0090_values_written_before_v5_decode_as_legacy() {
		let original = row();
		let key = encode_record_key(original);
		let full = encode_record_value(original);
		// Genuine earlier artifacts: the same value truncated to each prior
		// version's length, which is exactly what an older build wrote.
		for (len, label) in [
			(RECORD_VALUE_LEN_V4, "V4"),
			(RECORD_VALUE_LEN_V3, "V3"),
			(RECORD_VALUE_LEN_V2, "V2"),
			(RECORD_VALUE_LEN_V1, "V1"),
		] {
			let mut truncated = full[..len].to_vec();
			if len < RECORD_VALUE_LEN_V4 && len >= RECORD_VALUE_LEN_V3 {
				// A pre-V4 writer never set the table-object flag.
				let mut flags = u32::from_le_bytes(truncated[16..20].try_into().unwrap());
				flags &= !RECORD_FLAG_HAS_TABLE_OBJECT_ID;
				truncated[16..20].copy_from_slice(&flags.to_le_bytes());
			}
			let decoded = decode_record_row(&key, &truncated)
				.unwrap_or_else(|err| panic!("{label} value must still decode: {err}"));
			assert_eq!(
				decoded.placement,
				PlacementSemantics::RawLocality,
				"{label} predates partitions and is legacy"
			);
			assert_eq!(decoded.partition, None, "{label} carries no partition");
		}
	}

	/// The discriminating control: a V5-length value whose stamp is unrecognized
	/// is malformed. An implementation that fell back to legacy on an unknown
	/// stamp would pass every other test here and silently misroute a record
	/// whose placement it did not understand.
	#[test]
	fn adr0090_a_v5_value_with_an_unknown_stamp_is_malformed_not_legacy() {
		let original = row();
		let key = encode_record_key(original);
		let mut value = encode_record_value(original).to_vec();
		assert_eq!(value.len(), RECORD_VALUE_LEN_V5);
		for stamp in [2u8, 7, 200, u8::MAX] {
			value[36] = stamp;
			let err =
				decode_record_row(&key, &value).expect_err("an unknown V5 stamp must be refused");
			assert!(
				err.to_string().contains("unknown placement stamp"),
				"refusal must name the cause, got: {err}"
			);
		}
	}

	#[test]
	fn adr0090_an_unrecognized_length_is_still_refused() {
		let original = row();
		let key = encode_record_key(original);
		let full = encode_record_value(original);
		for len in [0usize, 5, 21, 37, 44] {
			assert!(
				decode_record_row(&key, &full[..len.min(full.len())]).is_err(),
				"length {len} is not a known version and must be refused"
			);
		}
	}
}
