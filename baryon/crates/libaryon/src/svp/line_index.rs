use crate::svp::backing_store::BackingIdentity;
use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"BRYNLIDX";
// Version 2 adds one terminal-newline byte per block. Newline counts alone
// cannot decide which side of a block boundary owns a physical line.
const VERSION: u32 = 2;
pub const DEFAULT_INDEX_BLOCK_SIZE: u32 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseLineIndex {
	identity: BackingIdentity,
	block_size: u32,
	newline_counts: Vec<u32>,
	ends_with_newline: Vec<bool>,
	prefix_newlines: Vec<u64>,
}

impl SparseLineIndex {
	pub fn new(
		identity: BackingIdentity,
		block_size: u32,
		newline_counts: Vec<u32>,
		ends_with_newline: Vec<bool>,
	) -> io::Result<Self> {
		if block_size == 0 {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"zero line-index block size",
			));
		}
		let expected = identity.length.div_ceil(u64::from(block_size));
		if newline_counts.len() as u64 != expected {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index block count does not match backing length",
			));
		}
		if ends_with_newline.len() != newline_counts.len() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index terminal-newline count does not match block count",
			));
		}
		let mut prefix_newlines = Vec::new();
		prefix_newlines
			.try_reserve_exact(newline_counts.len() + 1)
			.map_err(|_| io::Error::other("line-index prefix allocation failed"))?;
		prefix_newlines.push(0);
		for &count in &newline_counts {
			let next = prefix_newlines
				.last()
				.copied()
				.unwrap_or(0u64)
				.checked_add(u64::from(count))
				.ok_or_else(|| {
					io::Error::new(io::ErrorKind::InvalidData, "newline total overflow")
				})?;
			prefix_newlines.push(next);
		}
		Ok(Self {
			identity,
			block_size,
			newline_counts,
			ends_with_newline,
			prefix_newlines,
		})
	}

	pub fn identity(&self) -> BackingIdentity {
		self.identity
	}

	pub fn block_size(&self) -> u32 {
		self.block_size
	}

	pub fn newline_counts(&self) -> &[u32] {
		&self.newline_counts
	}

	pub fn terminal_newlines(&self) -> &[bool] {
		&self.ends_with_newline
	}

	pub fn total_newlines(&self) -> u64 {
		self.prefix_newlines.last().copied().unwrap_or(0)
	}

	pub fn physical_line_count(&self) -> u64 {
		self.total_newlines().saturating_add(1)
	}

	pub fn block_for_line(&self, line: u64) -> Option<(usize, u64)> {
		if self.newline_counts.is_empty() {
			return (line == 0).then_some((0, 0));
		}
		if line > self.total_newlines() {
			return None;
		}
		let mut block = self.prefix_newlines[1..]
			.partition_point(|&end_line| end_line < line)
			.min(self.newline_counts.len() - 1);
		if self.prefix_newlines[block + 1] == line
			&& self.ends_with_newline[block]
			&& block + 1 < self.newline_counts.len()
		{
			block += 1;
		}
		Some((block, line - self.prefix_newlines[block]))
	}

	pub fn block_byte_range(&self, block: usize) -> Option<(u64, u64)> {
		if block >= self.newline_counts.len() {
			return None;
		}
		let start = (block as u64).checked_mul(u64::from(self.block_size))?;
		Some((
			start,
			start
				.saturating_add(u64::from(self.block_size))
				.min(self.identity.length),
		))
	}

	pub fn save_atomic(&self, path: &Path) -> io::Result<()> {
		let mut encoded = self.encode()?;
		let checksum = checksum64(&encoded);
		encoded.extend_from_slice(&checksum.to_le_bytes());
		let temp = temporary_sidecar_path(path);
		let result = (|| {
			let mut file = OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(&temp)?;
			file.write_all(&encoded)?;
			file.sync_all()?;
			std::fs::rename(&temp, path)
		})();
		if result.is_err() {
			let _ = std::fs::remove_file(&temp);
		}
		result
	}

	pub fn load_validated(path: &Path, expected: BackingIdentity) -> io::Result<Self> {
		let mut file = std::fs::File::open(path)?;
		let length = file.metadata()?.len();
		const HEADER_LEN: usize = 65;
		let mut header = [0u8; HEADER_LEN];
		file.read_exact(&mut header)?;
		let mut cursor = 0usize;
		if take(&header, &mut cursor, 8)? != MAGIC {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index magic mismatch",
			));
		}
		if read_u32(&header, &mut cursor)? != VERSION {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index version mismatch",
			));
		}
		let block_size = read_u32(&header, &mut cursor)?;
		if block_size == 0 {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"zero line-index block size",
			));
		}
		let identity = BackingIdentity {
			length: read_u64(&header, &mut cursor)?,
			modified_nanos: read_u128(&header, &mut cursor)?,
			file_id: {
				let present = take(&header, &mut cursor, 1)?[0] != 0;
				let value = read_u128(&header, &mut cursor)?;
				present.then_some(value)
			},
		};
		if identity != expected {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index backing identity mismatch",
			));
		}
		let count = read_u64(&header, &mut cursor)?;
		if count != expected.length.div_ceil(u64::from(block_size)) {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index block count mismatch",
			));
		}
		let expected_length = (HEADER_LEN as u64)
			.checked_add(count.checked_mul(5).ok_or_else(|| {
				io::Error::new(io::ErrorKind::InvalidData, "line-index length overflow")
			})?)
			.and_then(|value| value.checked_add(8))
			.ok_or_else(|| {
				io::Error::new(io::ErrorKind::InvalidData, "line-index length overflow")
			})?;
		if length != expected_length {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index file length mismatch",
			));
		}
		let encoded_len = usize::try_from(length).map_err(|_| {
			io::Error::new(io::ErrorKind::InvalidData, "line index does not fit memory")
		})?;
		let mut encoded = Vec::new();
		encoded
			.try_reserve_exact(encoded_len)
			.map_err(|_| io::Error::other("line-index allocation failed"))?;
		file.seek(SeekFrom::Start(0))?;
		file.read_to_end(&mut encoded)?;
		if encoded.len() < 8 {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"truncated line index",
			));
		}
		let checksum_offset = encoded.len() - 8;
		let stored_checksum = u64::from_le_bytes(
			encoded[checksum_offset..]
				.try_into()
				.expect("checksum slice length"),
		);
		if checksum64(&encoded[..checksum_offset]) != stored_checksum {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index checksum mismatch",
			));
		}
		Self::decode(&encoded[..checksum_offset], expected)
	}

	fn encode(&self) -> io::Result<Vec<u8>> {
		let capacity = 8 + 4 + 4 + 8 + 16 + 1 + 16 + 8 + self.newline_counts.len() * 5;
		let mut out = Vec::new();
		out.try_reserve_exact(capacity)
			.map_err(|_| io::Error::other("line-index encoding allocation failed"))?;
		out.extend_from_slice(MAGIC);
		out.extend_from_slice(&VERSION.to_le_bytes());
		out.extend_from_slice(&self.block_size.to_le_bytes());
		out.extend_from_slice(&self.identity.length.to_le_bytes());
		out.extend_from_slice(&self.identity.modified_nanos.to_le_bytes());
		out.push(u8::from(self.identity.file_id.is_some()));
		out.extend_from_slice(&self.identity.file_id.unwrap_or(0).to_le_bytes());
		out.extend_from_slice(&(self.newline_counts.len() as u64).to_le_bytes());
		for (count, ends_with_newline) in self.newline_counts.iter().zip(&self.ends_with_newline) {
			out.extend_from_slice(&count.to_le_bytes());
			out.push(u8::from(*ends_with_newline));
		}
		Ok(out)
	}

	fn decode(bytes: &[u8], expected: BackingIdentity) -> io::Result<Self> {
		let mut cursor = 0usize;
		let magic = take(bytes, &mut cursor, 8)?;
		if magic != MAGIC {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index magic mismatch",
			));
		}
		let version = read_u32(bytes, &mut cursor)?;
		if version != VERSION {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index version mismatch",
			));
		}
		let block_size = read_u32(bytes, &mut cursor)?;
		let identity = BackingIdentity {
			length: read_u64(bytes, &mut cursor)?,
			modified_nanos: read_u128(bytes, &mut cursor)?,
			file_id: {
				let present = take(bytes, &mut cursor, 1)?[0] != 0;
				let value = read_u128(bytes, &mut cursor)?;
				present.then_some(value)
			},
		};
		if identity != expected {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index backing identity mismatch",
			));
		}
		let count = usize::try_from(read_u64(bytes, &mut cursor)?).map_err(|_| {
			io::Error::new(io::ErrorKind::InvalidData, "line-index count too large")
		})?;
		let expected_bytes = count.checked_mul(5).ok_or_else(|| {
			io::Error::new(io::ErrorKind::InvalidData, "line-index size overflow")
		})?;
		if bytes.len().saturating_sub(cursor) != expected_bytes {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"line-index payload length mismatch",
			));
		}
		let mut counts = Vec::new();
		let mut terminal_newlines = Vec::new();
		counts
			.try_reserve_exact(count)
			.map_err(|_| io::Error::other("line-index counts allocation failed"))?;
		terminal_newlines
			.try_reserve_exact(count)
			.map_err(|_| io::Error::other("line-index terminal allocation failed"))?;
		for _ in 0..count {
			counts.push(read_u32(bytes, &mut cursor)?);
			let flag = take(bytes, &mut cursor, 1)?[0];
			if flag > 1 {
				return Err(io::Error::new(
					io::ErrorKind::InvalidData,
					"invalid line-index terminal-newline flag",
				));
			}
			terminal_newlines.push(flag != 0);
		}
		Self::new(identity, block_size, counts, terminal_newlines)
	}
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> io::Result<&'a [u8]> {
	let end = cursor
		.checked_add(len)
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "line-index cursor overflow"))?;
	let value = bytes
		.get(*cursor..end)
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated line index"))?;
	*cursor = end;
	Ok(value)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> io::Result<u32> {
	Ok(u32::from_le_bytes(
		take(bytes, cursor, 4)?.try_into().unwrap(),
	))
}
fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
	Ok(u64::from_le_bytes(
		take(bytes, cursor, 8)?.try_into().unwrap(),
	))
}
fn read_u128(bytes: &[u8], cursor: &mut usize) -> io::Result<u128> {
	Ok(u128::from_le_bytes(
		take(bytes, cursor, 16)?.try_into().unwrap(),
	))
}

fn checksum64(bytes: &[u8]) -> u64 {
	bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
		(hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
	})
}

fn temporary_sidecar_path(path: &Path) -> PathBuf {
	let mut temp = path.as_os_str().to_owned();
	temp.push(format!(".tmp-{}", std::process::id()));
	PathBuf::from(temp)
}

pub fn cache_path_for_backing(backing_path: &Path) -> PathBuf {
	let cache_root = std::env::var_os("XDG_CACHE_HOME")
		.map(PathBuf::from)
		.or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
		.unwrap_or_else(std::env::temp_dir)
		.join("baryon")
		.join("line-index");
	let key = checksum64(backing_path.as_os_str().to_string_lossy().as_bytes());
	cache_root.join(format!("{key:016x}.idx"))
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::{SystemTime, UNIX_EPOCH};

	fn temp_path(name: &str) -> PathBuf {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_nanos();
		std::env::temp_dir().join(format!("baryon-{name}-{}-{nonce}", std::process::id()))
	}

	#[test]
	fn sidecar_round_trip_validates_identity_and_supports_line_lookup() {
		let identity = BackingIdentity {
			length: 10,
			modified_nanos: 42,
			file_id: Some(7),
		};
		let index =
			SparseLineIndex::new(identity, 4, vec![2, 1, 3], vec![true, false, true]).unwrap();
		let path = temp_path("line-index");
		index.save_atomic(&path).unwrap();
		let loaded = SparseLineIndex::load_validated(&path, identity).unwrap();
		assert_eq!(loaded, index);
		assert_eq!(loaded.physical_line_count(), 7);
		assert_eq!(loaded.block_for_line(2), Some((1, 0)));
		assert_eq!(loaded.block_for_line(3), Some((1, 1)));
		assert_eq!(loaded.block_byte_range(2), Some((8, 10)));

		let stale = BackingIdentity {
			length: 11,
			..identity
		};
		assert!(SparseLineIndex::load_validated(&path, stale).is_err());
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn sidecar_rejects_checksum_corruption() {
		let identity = BackingIdentity {
			length: 4,
			modified_nanos: 1,
			file_id: None,
		};
		let index = SparseLineIndex::new(identity, 4, vec![1], vec![true]).unwrap();
		let path = temp_path("line-index-corrupt");
		index.save_atomic(&path).unwrap();
		let mut bytes = std::fs::read(&path).unwrap();
		bytes[12] ^= 0x80;
		std::fs::write(&path, bytes).unwrap();
		assert!(SparseLineIndex::load_validated(&path, identity).is_err());
		let _ = std::fs::remove_file(path);
	}
}
