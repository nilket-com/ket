use crate::schema::{HybridTimestamp, Identity, SlabId, TenantId, TenantKekId};
use crate::storage::RecordLocation;
use crate::storage::manager::TieredSlabPointer;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashSet;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

const RECORDS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("slab_records_v1");
/// V1 layout: `payload_hash[32] || l2_uri_utf8`. Older rows had no
/// `is_encrypted` byte. Read-only; one-shot migrated into V2 the
/// first time a process opens an index that still has V1 rows.
const TIERED_SLABS_TABLE_V1: TableDefinition<u64, &[u8]> = TableDefinition::new("tiered_slabs_v1");
/// V2 layout: `payload_hash[32] || flags_byte || [optional metadata] ||
/// l2_uri_utf8`.
///
/// Flags:
/// - bit 0 = `is_encrypted`
/// - bit 1 = `tenant_id` present (8 LE bytes)
/// - bit 2 = `tenant_kek_id` present (8 LE bytes)
///
/// Older rows only use bit 0 and therefore decode cleanly with
/// missing tenant / tenant-KEK metadata.
const TIERED_SLABS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("tiered_slabs_v2");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("storage_index_meta_v1");
const META_LAST_CONTROL_LOG_INDEX: &str = "last_control_log_index";

const RECORD_KEY_LEN: usize = 8 + 32 + 8 + 4 + 4 + 8 + 8;
const RECORD_VALUE_LEN: usize = 4;
const TIERED_POINTER_HASH_LEN: usize = 32;
const TIERED_POINTER_FLAGS_LEN: usize = 1;
const TIERED_POINTER_PREFIX_LEN: usize = TIERED_POINTER_HASH_LEN + TIERED_POINTER_FLAGS_LEN;
const TIERED_FLAG_IS_ENCRYPTED: u8 = 0b0000_0001;
const TIERED_FLAG_HAS_TENANT_ID: u8 = 0b0000_0010;
const TIERED_FLAG_HAS_TENANT_KEK_ID: u8 = 0b0000_0100;

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
	pub delete_tiered_slab_ids: Vec<SlabId>,
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
						.insert(&pointer.slab_id.get(), value.as_slice())
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
					let _ = table.remove(&slab_id.get()).map_err(redb_to_io)?;
				}
				for pointer in update.put_tiered_slabs {
					let value = encode_tiered_pointer(&pointer);
					table
						.insert(&pointer.slab_id.get(), value.as_slice())
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
	bytes[cursor..cursor + 8].copy_from_slice(&row.location.slab_id.get().to_le_bytes());
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
			slab_id: SlabId::new(slab_id),
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
	let mut bytes = Vec::with_capacity(TIERED_POINTER_PREFIX_LEN + pointer.l2_uri.len());
	bytes.extend_from_slice(&pointer.payload_hash);
	let mut flags = 0u8;
	if pointer.is_encrypted {
		flags |= TIERED_FLAG_IS_ENCRYPTED;
	}
	if pointer.tenant_id.is_some() {
		flags |= TIERED_FLAG_HAS_TENANT_ID;
	}
	if pointer.tenant_kek_id.is_some() {
		flags |= TIERED_FLAG_HAS_TENANT_KEK_ID;
	}
	bytes.push(flags);
	if let Some(tenant_id) = pointer.tenant_id {
		bytes.extend_from_slice(&tenant_id.get().to_le_bytes());
	}
	if let Some(tenant_kek_id) = pointer.tenant_kek_id {
		bytes.extend_from_slice(&tenant_kek_id.get().to_le_bytes());
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
				slab_id: SlabId::new(9),
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
			slab_id: SlabId::new(3),
			l2_uri: "file:///tmp/example".into(),
			payload_hash: [0xAB; 32],
			is_encrypted: true,
			tenant_id: Some(TenantId::new(9)),
			tenant_kek_id: Some(TenantKekId::try_from(11).unwrap()),
		};
		let encoded = encode_tiered_pointer(&pointer);
		let decoded = decode_tiered_pointer(pointer.slab_id, &encoded).unwrap();
		assert_eq!(decoded, pointer);
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
