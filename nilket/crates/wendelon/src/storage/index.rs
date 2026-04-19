use crate::schema::{HybridTimestamp, Identity, TenantId};
use crate::storage::RecordLocation;
use crate::storage::manager::TieredSlabPointer;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashSet;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

const RECORDS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("slab_records_v1");
const TIERED_SLABS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("tiered_slabs_v1");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("storage_index_meta_v1");
const META_LAST_CONTROL_LOG_INDEX: &str = "last_control_log_index";

const RECORD_KEY_LEN: usize = 8 + 32 + 8 + 4 + 4 + 8 + 8;
const RECORD_VALUE_LEN: usize = 4;
const TIERED_POINTER_HASH_LEN: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PersistentRecordRow {
	pub tenant_id: TenantId,
	pub key_identity: Identity,
	pub location: RecordLocation,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PersistentIndexSnapshot {
	pub tiered_slabs: Vec<TieredSlabPointer>,
	pub tiered_records: Vec<PersistentRecordRow>,
	pub last_control_log_index: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PersistentIndexUpdate {
	pub put_records: Vec<PersistentRecordRow>,
	pub delete_records: Vec<PersistentRecordRow>,
	pub put_tiered_slabs: Vec<TieredSlabPointer>,
	pub delete_tiered_slab_ids: Vec<u64>,
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
				let _table = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
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
				let pointer = decode_tiered_pointer(slab_id.value(), value.value())?;
				tiered_ids.insert(pointer.slab_id);
				tiered_slabs.push(pointer);
			}

			let records_table = read_txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
			let mut tiered_records = Vec::new();
			for entry in records_table.iter().map_err(redb_to_io)? {
				let (key, value) = entry.map_err(redb_to_io)?;
				let row = decode_record_row(key.value(), value.value())?;
				if tiered_ids.contains(&row.location.slab_id) {
					tiered_records.push(row);
				}
			}
			let meta_table = read_txn.open_table(META_TABLE).map_err(redb_to_io)?;
			let last_control_log_index = meta_table
				.get(META_LAST_CONTROL_LOG_INDEX)
				.map_err(redb_to_io)?
				.map(|value| value.value());

			Ok(PersistentIndexSnapshot {
				tiered_slabs,
				tiered_records,
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
		let _guard = self.write_lock.lock().await;
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let write_txn = db.begin_write().map_err(redb_to_io)?;
			let _ = write_txn.delete_table(RECORDS_TABLE);
			let _ = write_txn.delete_table(TIERED_SLABS_TABLE);
			{
				let mut table = write_txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
				for row in rows {
					let key = encode_record_key(row);
					let value = encode_record_value(row.location);
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
						.insert(&pointer.slab_id, value.as_slice())
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
					let value = encode_record_value(row.location);
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
					let _ = table.remove(&slab_id).map_err(redb_to_io)?;
				}
				for pointer in update.put_tiered_slabs {
					let value = encode_tiered_pointer(&pointer);
					table
						.insert(&pointer.slab_id, value.as_slice())
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
	bytes[cursor..cursor + 8].copy_from_slice(&row.location.slab_id.to_le_bytes());
	cursor += 8;
	bytes[cursor..cursor + 8].copy_from_slice(&row.location.offset.to_le_bytes());
	bytes
}

fn encode_record_value(location: RecordLocation) -> [u8; RECORD_VALUE_LEN] {
	location.length.to_le_bytes()
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
	if value.len() != RECORD_VALUE_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"persistent record value length mismatch: expected {RECORD_VALUE_LEN}, got {}",
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
	let length = u32::from_le_bytes(value.try_into().expect("validated above"));

	Ok(PersistentRecordRow {
		tenant_id,
		key_identity,
		location: RecordLocation {
			slab_id,
			offset,
			length,
			hlc: HybridTimestamp {
				physical_ms,
				logical,
				node_id,
			},
		},
	})
}

fn encode_tiered_pointer(pointer: &TieredSlabPointer) -> Vec<u8> {
	let mut bytes = Vec::with_capacity(TIERED_POINTER_HASH_LEN + pointer.l2_uri.len());
	bytes.extend_from_slice(&pointer.payload_hash);
	bytes.extend_from_slice(pointer.l2_uri.as_bytes());
	bytes
}

fn decode_tiered_pointer(slab_id: u64, value: &[u8]) -> std::io::Result<TieredSlabPointer> {
	if value.len() < TIERED_POINTER_HASH_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"tiered slab value too short: expected at least {TIERED_POINTER_HASH_LEN}, got {}",
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
				format!("tiered slab URI is not utf8: {err}"),
			)
		})?
		.to_string();
	Ok(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash,
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
			location: RecordLocation {
				slab_id: 9,
				offset: 128,
				length: 4096,
				hlc: HybridTimestamp {
					physical_ms: 55,
					logical: 3,
					node_id: 11,
				},
			},
		}
	}

	#[test]
	fn record_row_roundtrip_preserves_every_field() {
		let row = row();
		let key = encode_record_key(row);
		let value = encode_record_value(row.location);
		let decoded = decode_record_row(&key, &value).unwrap();
		assert_eq!(decoded, row);
	}

	#[test]
	fn tiered_pointer_roundtrip_preserves_every_field() {
		let pointer = TieredSlabPointer {
			slab_id: 3,
			l2_uri: "file:///tmp/example".into(),
			payload_hash: [0xAB; 32],
		};
		let encoded = encode_tiered_pointer(&pointer);
		let decoded = decode_tiered_pointer(pointer.slab_id, &encoded).unwrap();
		assert_eq!(decoded, pointer);
	}

	#[tokio::test]
	async fn snapshot_only_surfaces_tiered_records() {
		let index = PersistentLocalIndex::open(temp_path("snapshot"))
			.await
			.unwrap();
		let tiered_pointer = TieredSlabPointer {
			slab_id: 41,
			l2_uri: "file:///tmp/tiered".into(),
			payload_hash: [0x11; 32],
		};
		let local_row = PersistentRecordRow {
			location: RecordLocation {
				slab_id: 7,
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
	}

	#[tokio::test]
	async fn tiered_update_stamps_monotonic_control_log_index() {
		let index = PersistentLocalIndex::open(temp_path("watermark"))
			.await
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id: 9,
			l2_uri: "file:///tmp/tiered-watermark".into(),
			payload_hash: [0x22; 32],
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
