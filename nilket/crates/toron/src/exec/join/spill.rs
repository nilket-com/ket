use super::*;
use ahash::RandomState;
use twox_hash::XxHash3_64;

pub(in crate::exec) const JOIN_SPILL_PAGE_SIZE: usize = 64 * 1024;
const JOIN_SPILL_CACHE_TARGET_BYTES: u64 = 8 * 1024 * 1024;
const COMPACT_SPILL_INDEX_MAX_RETAINED_CAP_OVERSHOOT_BYTES: u64 = 1024 * 1024;
const COMPACT_SPILL_INDEX_MAX_INPUT_ROWS: u64 = 20_000_000;
static NEXT_JOIN_SPILL_ID: AtomicU64 = AtomicU64::new(1);

/// Preserve the direct unindexed writer's empty-page behavior before a fragment
/// enters the coalescing buffer. A fragment containing no joinable keys was
/// previously skipped rather than spilled.
fn batch_has_spillable_join_key(
	batch: &RecordBatch,
	key_plan: JoinKeyPlan,
	build_keys: &[crate::ir::ColumnOrdinal],
	null_safe_keys: bool,
) -> Result<bool, ExecutionError> {
	let arrays = key_arrays(batch, build_keys)?;
	for row in 0..batch.num_rows() {
		if join_key_at(key_plan, &arrays, row, null_safe_keys)?.is_some() {
			return Ok(true);
		}
	}
	Ok(false)
}

pub(crate) fn partition_bucket(
	key_arrays: &[ArrayRef],
	row: usize,
	partition_count: u32,
) -> Result<usize, ExecutionError> {
	let mut encoded = Vec::new();
	partition_bucket_with_scratch(key_arrays, row, partition_count, &mut encoded)
}

/// ADR 484: reduce a partition hash to a bucket. For a power-of-two partition
/// count, `hash % count == hash & (count - 1)` **exactly**, so bucket identity and
/// the ADR 480 partition contract are preserved — only a runtime u64 division
/// becomes an AND. Non-power-of-two counts keep the modulo unchanged. The hash
/// input (XxHash3 bytes) is untouched; this is purely the post-hash reduction.
#[inline]
fn partition_index_for_hash(hash: u64, partition_count: u32) -> usize {
	let count = u64::from(partition_count);
	if partition_count.is_power_of_two() {
		(hash & (count - 1)) as usize
	} else {
		(hash % count) as usize
	}
}

pub(crate) fn partition_bucket_with_scratch(
	key_arrays: &[ArrayRef],
	row: usize,
	partition_count: u32,
	encoded: &mut Vec<u8>,
) -> Result<usize, ExecutionError> {
	if partition_count == 0 {
		return Err(ExecutionError::InvalidExpression {
			message: "exchange partition_count must be greater than zero".to_string(),
		});
	}
	Ok(partition_index_for_hash(
		partition_key_hash_with_scratch(key_arrays, row, encoded)?,
		partition_count,
	))
}

pub(in crate::exec) fn partition_bucket_if_non_null_with_scratch(
	key_arrays: &[ArrayRef],
	row: usize,
	partition_count: u32,
	encoded: &mut Vec<u8>,
) -> Result<Option<usize>, ExecutionError> {
	if partition_count == 0 {
		return Err(ExecutionError::InvalidExpression {
			message: "exchange partition_count must be greater than zero".to_string(),
		});
	}
	partition_key_hash_if_non_null_with_scratch(key_arrays, row, encoded)
		.map(|hash| hash.map(|hash| partition_index_for_hash(hash, partition_count)))
}

/// ADR 479: a single fixed-width integer key column is the common case (m06 joins
/// on Int64 order keys). Serializing a tag byte + LE bytes into a scratch `Vec<u8>`
/// only to hash the 9-byte buffer is redundant; hash the raw value directly. The
/// encoding is stable and lives in this one place, so build and probe rows — which
/// both route through the partition hash functions below — stay consistent by
/// construction. Returns `None` for any shape that is not the fast case, falling
/// back to the generic serialize-then-hash path.
fn fixed_width_partition_hash(key_arrays: &[ArrayRef], row: usize) -> Option<u64> {
	if key_arrays.len() != 1 {
		return None;
	}
	let array = key_arrays[0].as_ref();
	match array.data_type() {
		arrow::datatypes::DataType::Int32 => {
			let values = array.as_any().downcast_ref::<Int32Array>()?;
			// Tag byte mirrors encode_partition_key_value's Int32 discriminant (4)
			// so the fast path is a faithful shortcut of the generic encoding.
			let mut buf = [0u8; 5];
			buf[0] = 4;
			buf[1..].copy_from_slice(&values.value(row).to_le_bytes());
			Some(XxHash3_64::oneshot(&buf))
		}
		arrow::datatypes::DataType::Int64 => {
			let values = array.as_any().downcast_ref::<Int64Array>()?;
			let mut buf = [0u8; 9];
			buf[0] = 5;
			buf[1..].copy_from_slice(&values.value(row).to_le_bytes());
			Some(XxHash3_64::oneshot(&buf))
		}
		_ => None,
	}
}

fn partition_key_hash_with_scratch(
	key_arrays: &[ArrayRef],
	row: usize,
	encoded: &mut Vec<u8>,
) -> Result<u64, ExecutionError> {
	if let Some(hash) = fixed_width_partition_hash(key_arrays, row) {
		return Ok(hash);
	}
	encoded.clear();
	for array in key_arrays {
		encode_partition_key_value(array.as_ref(), row, encoded)?;
	}
	Ok(XxHash3_64::oneshot(encoded.as_slice()))
}

fn partition_key_hash_if_non_null_with_scratch(
	key_arrays: &[ArrayRef],
	row: usize,
	encoded: &mut Vec<u8>,
) -> Result<Option<u64>, ExecutionError> {
	if key_arrays.len() == 1 && !key_arrays[0].is_null(row) {
		if let Some(hash) = fixed_width_partition_hash(key_arrays, row) {
			return Ok(Some(hash));
		}
	}
	encoded.clear();
	for array in key_arrays {
		if array.is_null(row) {
			return Ok(None);
		}
		encode_partition_key_value(array.as_ref(), row, encoded)?;
	}
	Ok(Some(XxHash3_64::oneshot(encoded.as_slice())))
}

#[derive(Debug, Clone, Copy)]
struct JoinSpillPage {
	offset: u64,
	ciphertext_len: usize,
	plaintext_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) enum SpillResidentGuardSite {
	Write,
	RebuildIndex,
	PinDecodedPages,
	Finish,
	/// ADR 601: the finish-built index's reservation and finalization, guarded before allocating.
	Reserve,
	FinishIndex,
}

impl SpillResidentGuardSite {
	fn as_str(self) -> &'static str {
		match self {
			Self::Write => "write",
			Self::RebuildIndex => "rebuild_index",
			Self::PinDecodedPages => "pin_decoded_pages",
			Self::Reserve => "reserve",
			Self::FinishIndex => "finish_index",
			Self::Finish => "finish",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SpillResidentState {
	resident_baseline_bytes: u64,
	resident_index_bytes: u64,
	writer_peak_bytes: u64,
	page_reserve_bytes: u64,
	pinned_decoded_bytes: u64,
}

impl SpillResidentState {
	fn total(self) -> u64 {
		self.resident_baseline_bytes
			.saturating_add(self.resident_index_bytes)
			.saturating_add(self.writer_peak_bytes)
			.saturating_add(self.page_reserve_bytes)
			.saturating_add(self.pinned_decoded_bytes)
	}

	fn detail(self, site: SpillResidentGuardSite) -> crate::error::BudgetExhaustedDetail {
		crate::error::BudgetExhaustedDetail::message(format!(
			"site={}, resident_baseline_bytes={}, resident_index_bytes={}, writer_peak_bytes={}, page_reserve_bytes={}, pinned_decoded_bytes={}",
			site.as_str(),
			self.resident_baseline_bytes,
			self.resident_index_bytes,
			self.writer_peak_bytes,
			self.page_reserve_bytes,
			self.pinned_decoded_bytes
		))
	}
}

#[derive(Debug, Clone)]
struct CachedJoinSpillPage {
	bytes: Vec<u8>,
	decoded_batch: Option<RecordBatch>,
	decoded_bytes: u64,
}

impl CachedJoinSpillPage {
	fn new(bytes: Vec<u8>) -> Self {
		Self {
			bytes,
			decoded_batch: None,
			decoded_bytes: 0,
		}
	}

	fn total_bytes(&self) -> u64 {
		u64::try_from(self.bytes.len())
			.unwrap_or(u64::MAX)
			.saturating_add(self.decoded_bytes)
	}
}

fn cache_evictions_for_page(page: &CachedJoinSpillPage) -> SpillCacheEvictions {
	SpillCacheEvictions {
		plaintext_entries: u64::from(!page.bytes.is_empty()),
		plaintext_bytes: u64::try_from(page.bytes.len()).unwrap_or(u64::MAX),
		decoded_entries: u64::from(page.decoded_batch.is_some()),
		decoded_bytes: page.decoded_bytes,
	}
}

fn evict_join_spill_cache(
	cache: &mut HashMap<usize, CachedJoinSpillPage>,
	lru: &mut VecDeque<usize>,
	cache_bytes: &mut u64,
	cache_cap: u64,
	collect_stats: bool,
) -> SpillCacheEvictions {
	let mut evictions = SpillCacheEvictions::default();
	while cache_bytes.saturating_sub(cache_cap) > 0 {
		let Some(evicted) = lru.pop_front() else {
			break;
		};
		if let Some(page) = cache.remove(&evicted) {
			*cache_bytes = cache_bytes.saturating_sub(page.total_bytes());
			if collect_stats {
				evictions.add(cache_evictions_for_page(&page));
			}
		}
	}
	evictions
}

const JOIN_SPILL_INT32_KEY_TAG: u8 = 4;
const JOIN_SPILL_INT64_KEY_TAG: u8 = 5;
const EMPTY_JOIN_SPILL_PAGE_BUCKET: u32 = u32::MAX;

fn spill_capacity_bytes<T>(capacity: usize) -> u64 {
	u64::try_from(capacity.saturating_mul(std::mem::size_of::<T>())).unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, Copy)]
struct CompactJoinSpillPageEntry<K> {
	key: K,
	value: u32,
}

struct CompactJoinSpillPageLocator<K> {
	buckets: Vec<u32>,
	entries: Vec<CompactJoinSpillPageEntry<K>>,
	hasher: RandomState,
}

impl<K> CompactJoinSpillPageLocator<K>
where
	K: Copy + Eq + std::hash::Hash,
{
	fn with_key_count(key_count: usize) -> Self {
		let bucket_count = key_count.saturating_mul(2).next_power_of_two().max(1);
		Self {
			buckets: vec![EMPTY_JOIN_SPILL_PAGE_BUCKET; bucket_count],
			entries: Vec::new(),
			hasher: RandomState::new(),
		}
	}

	fn insert(&mut self, key: K, value: u32) -> Result<(), ExecutionError> {
		let mut bucket = self.bucket_for_key(key);
		loop {
			let entry_index = self.buckets[bucket];
			if entry_index == EMPTY_JOIN_SPILL_PAGE_BUCKET {
				let entry_index =
					u32::try_from(self.entries.len()).map_err(|_| ExecutionError::Kernel {
						message: "join spill compact locator entry count exceeds u32".to_string(),
					})?;
				self.entries.push(CompactJoinSpillPageEntry { key, value });
				self.buckets[bucket] = entry_index;
				return Ok(());
			}
			if self.entries[entry_index as usize].key == key {
				return Err(ExecutionError::Kernel {
					message: "join spill compact locator received a duplicate key".to_string(),
				});
			}
			bucket = (bucket + 1) & (self.buckets.len() - 1);
		}
	}

	fn lookup(&self, key: K) -> Option<u32> {
		let mut bucket = self.bucket_for_key(key);
		loop {
			let entry_index = self.buckets[bucket];
			if entry_index == EMPTY_JOIN_SPILL_PAGE_BUCKET {
				return None;
			}
			let entry = self.entries[entry_index as usize];
			if entry.key == key {
				return Some(entry.value);
			}
			bucket = (bucket + 1) & (self.buckets.len() - 1);
		}
	}

	fn accounted_bytes(&self) -> u64 {
		u64::try_from(
			self.buckets
				.capacity()
				.saturating_mul(std::mem::size_of::<u32>())
				.saturating_add(
					self.entries
						.capacity()
						.saturating_mul(std::mem::size_of::<CompactJoinSpillPageEntry<K>>()),
				),
		)
		.unwrap_or(u64::MAX)
	}

	fn bucket_for_key(&self, key: K) -> usize {
		(self.hasher.hash_one(key) as usize) & (self.buckets.len() - 1)
	}
}

enum JoinSpillPageLocatorStorage {
	Encoded(HashMap<Vec<u8>, Vec<usize>>),
	Int32Pages(CompactJoinSpillPageLocator<i32>),
	Int64Pages(CompactJoinSpillPageLocator<i64>),
	Int32Rows(CompactJoinSpillPageLocator<i32>),
	Int64Rows(CompactJoinSpillPageLocator<i64>),
}

#[derive(Debug, PartialEq, Eq)]
enum JoinSpillLocatorLookup {
	None,
	Row(u32),
	Page(usize),
	Pages(Vec<usize>),
}

struct JoinSpillPageLocator {
	storage: JoinSpillPageLocatorStorage,
	#[cfg(test)]
	legacy_insert_count: u64,
}

impl JoinSpillPageLocator {
	fn for_key_plan(key_plan: JoinKeyPlan, key_count: usize, all_singleton: bool) -> Self {
		let storage = match (key_plan, all_singleton) {
			(JoinKeyPlan::Int32, true) => JoinSpillPageLocatorStorage::Int32Rows(
				CompactJoinSpillPageLocator::with_key_count(key_count),
			),
			(JoinKeyPlan::Int64, true) => JoinSpillPageLocatorStorage::Int64Rows(
				CompactJoinSpillPageLocator::with_key_count(key_count),
			),
			(JoinKeyPlan::Int32, false) => JoinSpillPageLocatorStorage::Int32Pages(
				CompactJoinSpillPageLocator::with_key_count(key_count),
			),
			(JoinKeyPlan::Int64, false) => JoinSpillPageLocatorStorage::Int64Pages(
				CompactJoinSpillPageLocator::with_key_count(key_count),
			),
			(JoinKeyPlan::Generic, _) => JoinSpillPageLocatorStorage::Encoded(HashMap::new()),
		};
		Self {
			storage,
			#[cfg(test)]
			legacy_insert_count: 0,
		}
	}

	fn insert(
		&mut self,
		encoded_key: &[u8],
		page_index: usize,
		rows: &[u32],
	) -> Result<(), ExecutionError> {
		match &mut self.storage {
			JoinSpillPageLocatorStorage::Encoded(index) => {
				index
					.entry(encoded_key.to_vec())
					.or_default()
					.push(page_index);
				#[cfg(test)]
				{
					self.legacy_insert_count = self.legacy_insert_count.saturating_add(1);
				}
				Ok(())
			}
			JoinSpillPageLocatorStorage::Int32Pages(index) => index.insert(
				decode_join_spill_int32_key(encoded_key)?,
				compact_join_spill_page_index(page_index)?,
			),
			JoinSpillPageLocatorStorage::Int64Pages(index) => index.insert(
				decode_join_spill_int64_key(encoded_key)?,
				compact_join_spill_page_index(page_index)?,
			),
			JoinSpillPageLocatorStorage::Int32Rows(index) => index.insert(
				decode_join_spill_int32_key(encoded_key)?,
				singleton_join_spill_row(rows)?,
			),
			JoinSpillPageLocatorStorage::Int64Rows(index) => index.insert(
				decode_join_spill_int64_key(encoded_key)?,
				singleton_join_spill_row(rows)?,
			),
		}
	}

	fn lookup(&self, encoded_key: &[u8]) -> Result<JoinSpillLocatorLookup, ExecutionError> {
		match &self.storage {
			JoinSpillPageLocatorStorage::Encoded(index) => Ok(match index.get(encoded_key) {
				None => JoinSpillLocatorLookup::None,
				Some(pages) if pages.len() == 1 => JoinSpillLocatorLookup::Page(pages[0]),
				Some(pages) => JoinSpillLocatorLookup::Pages(pages.clone()),
			}),
			JoinSpillPageLocatorStorage::Int32Pages(index) => Ok(index
				.lookup(decode_join_spill_int32_key(encoded_key)?)
				.map_or(JoinSpillLocatorLookup::None, |page| {
					JoinSpillLocatorLookup::Page(page as usize)
				})),
			JoinSpillPageLocatorStorage::Int64Pages(index) => Ok(index
				.lookup(decode_join_spill_int64_key(encoded_key)?)
				.map_or(JoinSpillLocatorLookup::None, |page| {
					JoinSpillLocatorLookup::Page(page as usize)
				})),
			JoinSpillPageLocatorStorage::Int32Rows(index) => Ok(index
				.lookup(decode_join_spill_int32_key(encoded_key)?)
				.map_or(JoinSpillLocatorLookup::None, JoinSpillLocatorLookup::Row)),
			JoinSpillPageLocatorStorage::Int64Rows(index) => Ok(index
				.lookup(decode_join_spill_int64_key(encoded_key)?)
				.map_or(JoinSpillLocatorLookup::None, JoinSpillLocatorLookup::Row)),
		}
	}

	fn accounted_bytes(&self) -> u64 {
		match &self.storage {
			JoinSpillPageLocatorStorage::Encoded(index) => join_spill_index_bytes(index),
			JoinSpillPageLocatorStorage::Int32Pages(index)
			| JoinSpillPageLocatorStorage::Int32Rows(index) => index.accounted_bytes(),
			JoinSpillPageLocatorStorage::Int64Pages(index)
			| JoinSpillPageLocatorStorage::Int64Rows(index) => index.accounted_bytes(),
		}
	}
}

fn compact_join_spill_page_index(page_index: usize) -> Result<u32, ExecutionError> {
	u32::try_from(page_index).map_err(|_| ExecutionError::Kernel {
		message: format!("join spill page index {page_index} exceeds compact locator range"),
	})
}

fn singleton_join_spill_row(rows: &[u32]) -> Result<u32, ExecutionError> {
	let [row] = rows else {
		return Err(ExecutionError::Kernel {
			message: "join spill row locator requires exactly one build row".to_string(),
		});
	};
	Ok(*row)
}

fn decode_join_spill_int32_key(encoded_key: &[u8]) -> Result<i32, ExecutionError> {
	if encoded_key.len() != 1 + std::mem::size_of::<i32>()
		|| encoded_key.first().copied() != Some(JOIN_SPILL_INT32_KEY_TAG)
	{
		return Err(invalid_compact_join_spill_key("Int32", encoded_key));
	}
	let bytes: [u8; 4] = encoded_key[1..]
		.try_into()
		.expect("validated Int32 join spill key length");
	Ok(i32::from_le_bytes(bytes))
}

fn decode_join_spill_int64_key(encoded_key: &[u8]) -> Result<i64, ExecutionError> {
	if encoded_key.len() != 1 + std::mem::size_of::<i64>()
		|| encoded_key.first().copied() != Some(JOIN_SPILL_INT64_KEY_TAG)
	{
		return Err(invalid_compact_join_spill_key("Int64", encoded_key));
	}
	let bytes: [u8; 8] = encoded_key[1..]
		.try_into()
		.expect("validated Int64 join spill key length");
	Ok(i64::from_le_bytes(bytes))
}

fn invalid_compact_join_spill_key(expected: &str, encoded_key: &[u8]) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!(
			"join spill compact {expected} locator received malformed encoded key (tag={:?}, len={})",
			encoded_key.first(),
			encoded_key.len()
		),
	}
}

pub(in crate::exec) struct SpilledJoinBuild {
	spill_id: u64,
	path: PathBuf,
	file_ops: Arc<dyn AsyncFileOps>,
	file: FileHandle,
	key: LessSafeKey,
	pages: Vec<JoinSpillPage>,
	index: JoinSpillPageLocator,
	cache: HashMap<usize, CachedJoinSpillPage>,
	lru: VecDeque<usize>,
	cache_bytes: u64,
	cache_cap: u64,
	#[cfg(test)]
	page_read_count: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::exec) enum JoinSpillRows {
	NoMatch,
	OneRow(u32),
	Rows(Vec<u32>),
}

pub(in crate::exec) enum JoinSpillRowsIntoIter {
	NoMatch,
	OneRow(std::option::IntoIter<u32>),
	Rows(std::vec::IntoIter<u32>),
}

impl Iterator for JoinSpillRowsIntoIter {
	type Item = u32;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			Self::NoMatch => None,
			Self::OneRow(row) => row.next(),
			Self::Rows(rows) => rows.next(),
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		match self {
			Self::NoMatch => (0, Some(0)),
			Self::OneRow(row) => row.size_hint(),
			Self::Rows(rows) => rows.size_hint(),
		}
	}
}

impl ExactSizeIterator for JoinSpillRowsIntoIter {}

impl IntoIterator for JoinSpillRows {
	type Item = u32;
	type IntoIter = JoinSpillRowsIntoIter;

	fn into_iter(self) -> Self::IntoIter {
		match self {
			Self::NoMatch => JoinSpillRowsIntoIter::NoMatch,
			Self::OneRow(row) => JoinSpillRowsIntoIter::OneRow(Some(row).into_iter()),
			Self::Rows(rows) => JoinSpillRowsIntoIter::Rows(rows.into_iter()),
		}
	}
}

impl SpilledJoinBuild {
	pub(in crate::exec) async fn write(
		build_rows: HashMap<Vec<u8>, Vec<u32>>,
		key_plan: JoinKeyPlan,
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
		spill_dir: PathBuf,
	) -> Result<Self, ExecutionError> {
		let spill_id = NEXT_JOIN_SPILL_ID.fetch_add(1, Ordering::SeqCst);
		let path = spill_dir.join(format!("spill_{}_{}.join", std::process::id(), spill_id));
		match Self::write_inner(
			spill_id,
			path.clone(),
			build_rows,
			key_plan,
			task_budget,
			file_ops.clone(),
		)
		.await
		{
			Ok(spill) => Ok(spill),
			Err(error) => {
				let _ = file_ops.unlink(&path).await;
				Err(error)
			}
		}
	}

	pub(in crate::exec) async fn write_inner(
		spill_id: u64,
		path: PathBuf,
		build_rows: HashMap<Vec<u8>, Vec<u32>>,
		key_plan: JoinKeyPlan,
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
	) -> Result<Self, ExecutionError> {
		let spill_dir = path.parent().ok_or_else(|| ExecutionError::Kernel {
			message: "join spill path has no parent directory".to_string(),
		})?;
		file_ops
			.create_dir_all(spill_dir)
			.await
			.map_err(|error| join_spill_io_error("create_dir_all", error))?;
		let mut key_bytes = [0u8; 32];
		aws_lc_rs::rand::fill(&mut key_bytes).map_err(|error| ExecutionError::Kernel {
			message: format!("join spill key generation failed: {error:?}"),
		})?;
		let unbound = UnboundKey::new(&AES_256_GCM_SIV, &key_bytes).map_err(|error| {
			ExecutionError::Kernel {
				message: format!("join spill key construction failed: {error:?}"),
			}
		})?;
		let key = LessSafeKey::new(unbound);
		let file = file_ops
			.open(
				&path,
				OpenFlags {
					read: true,
					write: true,
					create_new: true,
					..OpenFlags::default()
				},
			)
			.await
			.map_err(|error| ExecutionError::Kernel {
				message: format!("join spill open failed: {error}"),
			})?;
		let key_count = build_rows.len();
		let all_singleton = build_rows.values().all(|rows| rows.len() == 1);
		let mut pages = Vec::new();
		let mut index = JoinSpillPageLocator::for_key_plan(key_plan, key_count, all_singleton);
		let mut current_entries: Vec<(Vec<u8>, Vec<u32>)> = Vec::new();
		let mut current_size = std::mem::size_of::<u32>();
		let mut spill_bytes = 0u64;
		let mut writer_peak = 0u64;
		let mut next_offset = 0u64;

		for (join_key, rows) in build_rows {
			let entry_size = encoded_join_spill_entry_size(&join_key, &rows);
			if !current_entries.is_empty()
				&& current_size.saturating_add(entry_size) > JOIN_SPILL_PAGE_SIZE
			{
				let page_index = pages.len();
				let page = write_join_spill_page(
					file_ops.as_ref(),
					&file,
					&key,
					spill_id,
					page_index,
					next_offset,
					&current_entries,
				)
				.await?;
				spill_bytes = spill_bytes.saturating_add(page.ciphertext_len as u64);
				writer_peak = writer_peak.max(page.plaintext_len as u64);
				if spill_bytes > task_budget.spill_bytes.get() {
					return Err(budget_exhausted(
						BudgetDimension::SpillBytes,
						"join spill writer",
						spill_bytes,
						task_budget.spill_bytes.get(),
					));
				}
				for (key, rows) in &current_entries {
					index.insert(key, page_index, rows)?;
				}
				next_offset = next_offset.saturating_add(page.ciphertext_len as u64);
				pages.push(page);
				current_entries.clear();
				current_size = std::mem::size_of::<u32>();
			}

			current_size = current_size.saturating_add(entry_size);
			current_entries.push((join_key, rows));
		}

		if !current_entries.is_empty() {
			let page_index = pages.len();
			let page = write_join_spill_page(
				file_ops.as_ref(),
				&file,
				&key,
				spill_id,
				page_index,
				next_offset,
				&current_entries,
			)
			.await?;
			spill_bytes = spill_bytes.saturating_add(page.ciphertext_len as u64);
			writer_peak = writer_peak.max(page.plaintext_len as u64);
			if spill_bytes > task_budget.spill_bytes.get() {
				return Err(budget_exhausted(
					BudgetDimension::SpillBytes,
					"join spill writer",
					spill_bytes,
					task_budget.spill_bytes.get(),
				));
			}
			for (key, rows) in &current_entries {
				index.insert(key, page_index, rows)?;
			}
			next_offset = next_offset.saturating_add(page.ciphertext_len as u64);
			pages.push(page);
		}
		file_ops
			.set_len(&file, next_offset)
			.await
			.map_err(|error| join_spill_io_error("set_len", error))?;
		file_ops
			.fdatasync(&file)
			.await
			.map_err(|error| join_spill_io_error("fdatasync", error))?;

		let index_bytes = index.accounted_bytes();
		let resident_without_cache = index_bytes
			.saturating_add(writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		if resident_without_cache > task_budget.memory.get() {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"join spill writer resident state",
				resident_without_cache,
				task_budget.memory.get(),
			));
		}
		let cache_cap = JOIN_SPILL_CACHE_TARGET_BYTES.min(
			task_budget
				.memory
				.get()
				.saturating_sub(resident_without_cache),
		);

		Ok(Self {
			spill_id,
			path,
			file_ops,
			file,
			key,
			pages,
			index,
			cache: HashMap::new(),
			lru: VecDeque::new(),
			cache_bytes: 0,
			cache_cap,
			#[cfg(test)]
			page_read_count: 0,
		})
	}

	pub(in crate::exec) fn spill_bytes(&self) -> u64 {
		self.pages
			.iter()
			.map(|page| page.ciphertext_len as u64)
			.sum()
	}

	pub(in crate::exec) async fn lookup(
		&mut self,
		join_key: &[u8],
	) -> Result<JoinSpillRows, ExecutionError> {
		match self.index.lookup(join_key)? {
			JoinSpillLocatorLookup::None => Ok(JoinSpillRows::NoMatch),
			JoinSpillLocatorLookup::Row(row) => Ok(JoinSpillRows::OneRow(row)),
			JoinSpillLocatorLookup::Page(page_index) => {
				let page = self.read_page(page_index).await?;
				Ok(JoinSpillRows::Rows(decode_join_spill_page_for_key(
					&page, join_key,
				)?))
			}
			JoinSpillLocatorLookup::Pages(page_indices) => {
				let mut rows = Vec::new();
				for page_index in page_indices {
					let page = self.read_page(page_index).await?;
					rows.extend(decode_join_spill_page_for_key(&page, join_key)?);
				}
				Ok(JoinSpillRows::Rows(rows))
			}
		}
	}

	pub(in crate::exec) async fn read_page(
		&mut self,
		page_index: usize,
	) -> Result<Vec<u8>, ExecutionError> {
		#[cfg(test)]
		{
			self.page_read_count = self.page_read_count.saturating_add(1);
		}
		if let Some(cached) = self
			.cache
			.get(&page_index)
			.filter(|cached| !cached.bytes.is_empty())
		{
			let bytes = cached.bytes.clone();
			self.touch_cache_entry(page_index);
			return Ok(bytes);
		}

		let page = self
			.pages
			.get(page_index)
			.copied()
			.ok_or_else(|| ExecutionError::Kernel {
				message: format!("join spill page {page_index} is missing"),
			})?;
		let mut ciphertext = read_plain_exact(
			self.file_ops.as_ref(),
			&self.file,
			page.offset,
			page.ciphertext_len,
			None,
			None,
		)
		.await?;
		let nonce = Nonce::assume_unique_for_key(join_spill_nonce(self.spill_id, page_index));
		let plaintext = self
			.key
			.open_in_place(nonce, Aad::empty(), &mut ciphertext)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("join spill decrypt failed: {error:?}"),
			})?
			.to_vec();
		self.insert_cache_entry(page_index, plaintext.clone());
		Ok(plaintext)
	}

	pub(in crate::exec) async fn cleanup(self) {
		let file_ops = self.file_ops.clone();
		let path = self.path.clone();
		drop(self);
		let _ = file_ops.unlink(&path).await;
	}

	pub(in crate::exec) fn touch_cache_entry(&mut self, page_index: usize) {
		self.lru.retain(|cached| *cached != page_index);
		self.lru.push_back(page_index);
	}

	pub(in crate::exec) fn insert_cache_entry(&mut self, page_index: usize, bytes: Vec<u8>) {
		let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
		if self.cache_cap == 0 || bytes_len > self.cache_cap {
			return;
		}
		if let Some(page) = self.cache.remove(&page_index) {
			self.cache_bytes = self.cache_bytes.saturating_sub(page.total_bytes());
		}
		self.cache_bytes = self.cache_bytes.saturating_add(bytes_len);
		self.cache
			.insert(page_index, CachedJoinSpillPage::new(bytes));
		self.touch_cache_entry(page_index);
		evict_join_spill_cache(
			&mut self.cache,
			&mut self.lru,
			&mut self.cache_bytes,
			self.cache_cap,
			false,
		);
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) struct SpilledBatchRowRef {
	pub(super) page_index: usize,
	pub(super) row: u32,
}

/// ADR 602: how a compact-index hit returns its refs. `Collected` gathers them into a `Vec`
/// even when there is one; `Inline` returns a lone ref as `One`, with no allocation. A hit
/// with overflow refs is `Many` under either form.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::exec) enum SpilledLookupForm {
	Collected,
	Inline,
}

impl SpilledLookupForm {
	pub(in crate::exec) fn name(self) -> &'static str {
		match self {
			Self::Collected => "collected",
			Self::Inline => "inline",
		}
	}
}

/// ADR 602: a lookup's refs, owned, so a caller may use the build again while it consumes
/// them. Refs keep insertion order in every shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::exec) enum SpilledLookupMatches {
	Miss,
	One(SpilledBatchRowRef),
	Many(Vec<SpilledBatchRowRef>),
}

impl SpilledLookupMatches {
	fn from_vec(refs: Vec<SpilledBatchRowRef>) -> Self {
		if refs.is_empty() {
			Self::Miss
		} else {
			Self::Many(refs)
		}
	}

	pub(in crate::exec) fn len(&self) -> usize {
		match self {
			Self::Miss => 0,
			Self::One(_) => 1,
			Self::Many(refs) => refs.len(),
		}
	}

	#[cfg(test)]
	pub(in crate::exec) fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

impl IntoIterator for SpilledLookupMatches {
	type Item = SpilledBatchRowRef;
	type IntoIter = SpilledLookupMatchesIter;

	fn into_iter(self) -> Self::IntoIter {
		match self {
			Self::Miss => SpilledLookupMatchesIter::One(None),
			Self::One(row_ref) => SpilledLookupMatchesIter::One(Some(row_ref)),
			Self::Many(refs) => SpilledLookupMatchesIter::Many(refs.into_iter()),
		}
	}
}

pub(in crate::exec) enum SpilledLookupMatchesIter {
	One(Option<SpilledBatchRowRef>),
	Many(std::vec::IntoIter<SpilledBatchRowRef>),
}

impl Iterator for SpilledLookupMatchesIter {
	type Item = SpilledBatchRowRef;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			Self::One(row_ref) => row_ref.take(),
			Self::Many(refs) => refs.next(),
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		match self {
			Self::One(row_ref) => {
				let len = usize::from(row_ref.is_some());
				(len, Some(len))
			}
			Self::Many(refs) => refs.size_hint(),
		}
	}
}

impl ExactSizeIterator for SpilledLookupMatchesIter {}

impl std::iter::FusedIterator for SpilledLookupMatchesIter {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompactSpilledBatchRowRef {
	page_index: u32,
	row: u32,
}

impl CompactSpilledBatchRowRef {
	fn try_from_ref(row_ref: SpilledBatchRowRef) -> Result<Self, ExecutionError> {
		let page_index = u32::try_from(row_ref.page_index).map_err(|_| ExecutionError::Kernel {
			message: format!(
				"streamed inner join spill page index {} exceeds compact index range",
				row_ref.page_index
			),
		})?;
		Ok(Self {
			page_index,
			row: row_ref.row,
		})
	}

	fn unpack(self) -> SpilledBatchRowRef {
		SpilledBatchRowRef {
			page_index: self.page_index as usize,
			row: self.row,
		}
	}
}

#[derive(Debug, Clone)]
struct SpilledBatchRefList {
	first: CompactSpilledBatchRowRef,
	overflow: Option<Box<Vec<CompactSpilledBatchRowRef>>>,
}

impl SpilledBatchRefList {
	fn new(first: CompactSpilledBatchRowRef) -> Self {
		Self {
			first,
			overflow: None,
		}
	}

	fn push(&mut self, row_ref: CompactSpilledBatchRowRef) -> u64 {
		match &mut self.overflow {
			Some(rows) => {
				let before = rows.capacity();
				rows.push(row_ref);
				let after = rows.capacity();
				after
					.saturating_sub(before)
					.saturating_mul(std::mem::size_of::<CompactSpilledBatchRowRef>())
					.try_into()
					.unwrap_or(u64::MAX)
			}
			None => {
				let mut rows = Box::new(Vec::with_capacity(1));
				rows.push(row_ref);
				let bytes = std::mem::size_of::<Vec<CompactSpilledBatchRowRef>>().saturating_add(
					rows.capacity()
						.saturating_mul(std::mem::size_of::<CompactSpilledBatchRowRef>()),
				);
				self.overflow = Some(rows);
				bytes.try_into().unwrap_or(u64::MAX)
			}
		}
	}

	fn refs(&self) -> Vec<SpilledBatchRowRef> {
		let overflow_len = self.overflow.as_ref().map_or(0, |rows| rows.len());
		let mut refs = Vec::with_capacity(1usize.saturating_add(overflow_len));
		refs.push(self.first.unpack());
		if let Some(rows) = &self.overflow {
			refs.extend(rows.iter().map(|row| row.unpack()));
		}
		refs
	}

	/// ADR 602: the refs as a lookup returns them — a lone ref inline under `Inline`, else the
	/// collected `Vec` as `refs` builds it.
	fn matches(&self, form: SpilledLookupForm) -> SpilledLookupMatches {
		if form == SpilledLookupForm::Inline && self.overflow.is_none() {
			SpilledLookupMatches::One(self.first.unpack())
		} else {
			SpilledLookupMatches::Many(self.refs())
		}
	}

	fn len(&self) -> usize {
		1usize.saturating_add(self.overflow.as_ref().map_or(0, |rows| rows.len()))
	}

	#[cfg(test)]
	fn heap_bytes(&self) -> u64 {
		self.overflow
			.as_ref()
			.map(|rows| {
				std::mem::size_of::<Vec<CompactSpilledBatchRowRef>>()
					.saturating_add(
						rows.capacity()
							.saturating_mul(std::mem::size_of::<CompactSpilledBatchRowRef>()),
					)
					.try_into()
					.unwrap_or(u64::MAX)
			})
			.unwrap_or(0)
	}
}

enum SpilledBatchIndex {
	Hash(HashMap<JoinKey, Vec<SpilledBatchRowRef>>),
	Int32(PrimitiveSpilledBatchIndex<i32>),
	Int64(PrimitiveSpilledBatchIndex<i64>),
}

impl SpilledBatchIndex {
	fn for_key_plan(key_plan: JoinKeyPlan, null_safe_keys: bool) -> Self {
		if null_safe_keys {
			return Self::new_hash();
		}
		match key_plan {
			JoinKeyPlan::Int32 => Self::Int32(PrimitiveSpilledBatchIndex::with_capacity(0)),
			JoinKeyPlan::Int64 => Self::Int64(PrimitiveSpilledBatchIndex::with_capacity(0)),
			JoinKeyPlan::Generic => Self::new_hash(),
		}
	}

	fn new_hash() -> Self {
		Self::Hash(HashMap::new())
	}

	fn finish_built_admitted(key_plan: JoinKeyPlan, null_safe_keys: bool) -> bool {
		finish_built_index_admitted(key_plan, null_safe_keys)
	}

	/// ADR 601: the collecting form for an admitted plan, reserved at `expected_rows`.
	fn reserved(key_plan: JoinKeyPlan, expected_rows: usize) -> Result<Self, ExecutionError> {
		match key_plan {
			JoinKeyPlan::Int32 => Ok(Self::Int32(PrimitiveSpilledBatchIndex::reserved(
				expected_rows,
			))),
			JoinKeyPlan::Int64 => Ok(Self::Int64(PrimitiveSpilledBatchIndex::reserved(
				expected_rows,
			))),
			JoinKeyPlan::Generic => Err(ExecutionError::Kernel {
				message: "finish-built spill index reserved for a generic key plan".to_string(),
			}),
		}
	}

	fn is_collecting(&self) -> bool {
		match self {
			Self::Int32(index) => index.is_collecting(),
			Self::Int64(index) => index.is_collecting(),
			Self::Hash(_) => false,
		}
	}

	fn finalize(&mut self) -> Result<FinishIndexStats, ExecutionError> {
		match self {
			Self::Int32(index) => index.finalize(),
			Self::Int64(index) => index.finalize(),
			Self::Hash(_) => Err(ExecutionError::Kernel {
				message: "finish-built spill index finalized on the hash variant".to_string(),
			}),
		}
	}

	fn finish_stats(&self) -> Option<FinishIndexStats> {
		match self {
			Self::Int32(index) => index.finish_stats(),
			Self::Int64(index) => index.finish_stats(),
			Self::Hash(_) => None,
		}
	}

	fn capacity_growths(&self) -> u64 {
		match self {
			Self::Int32(index) => index.capacity_growths(),
			Self::Int64(index) => index.capacity_growths(),
			Self::Hash(_) => 0,
		}
	}

	/// ADR 601: the bucket bytes a finalization would obtain, from the entries collected.
	fn planned_finish_bucket_bytes(&self) -> Result<u64, ExecutionError> {
		let entry_count = match self {
			Self::Int32(index) => index.entries.len(),
			Self::Int64(index) => index.entries.len(),
			Self::Hash(_) => 0,
		};
		finish_index_plan(entry_count).map(|(_, bytes)| bytes)
	}

	fn insert(&mut self, key: JoinKey, row_ref: SpilledBatchRowRef) -> Result<(), ExecutionError> {
		match (self, key) {
			(Self::Int32(index), JoinKey::Int32(key)) => {
				index.insert(key, CompactSpilledBatchRowRef::try_from_ref(row_ref)?)
			}
			(Self::Int64(index), JoinKey::Int64(key)) => {
				index.insert(key, CompactSpilledBatchRowRef::try_from_ref(row_ref)?)
			}
			(Self::Hash(index), key) => {
				match index.entry(key) {
					Entry::Occupied(mut entry) => {
						entry.get_mut().push(row_ref);
					}
					Entry::Vacant(entry) => {
						entry.insert(vec![row_ref]);
					}
				}
				Ok(())
			}
			(Self::Int32(_), key) => Err(ExecutionError::Kernel {
				message: format!("non-Int32 key inserted into compact Int32 spill index: {key:?}"),
			}),
			(Self::Int64(_), key) => Err(ExecutionError::Kernel {
				message: format!("non-Int64 key inserted into compact Int64 spill index: {key:?}"),
			}),
		}
	}

	fn lookup(&self, join_key: &JoinKey, form: SpilledLookupForm) -> SpilledLookupMatches {
		match (self, join_key) {
			(Self::Int32(index), JoinKey::Int32(key)) => index.lookup(*key, form),
			(Self::Int64(index), JoinKey::Int64(key)) => index.lookup(*key, form),
			(Self::Hash(index), key) => {
				SpilledLookupMatches::from_vec(index.get(key).cloned().unwrap_or_default())
			}
			(Self::Int32(_) | Self::Int64(_), _) => SpilledLookupMatches::Miss,
		}
	}

	fn accounted_bytes(&self) -> u64 {
		match self {
			Self::Int32(index) => index.accounted_bytes(),
			Self::Int64(index) => index.accounted_bytes(),
			Self::Hash(index) => spilled_batch_join_hash_index_bytes(index),
		}
	}

	/// ADR 600: the variant `for_key_plan` chose — a fact distinct from the wrapper being enabled.
	fn variant_name(&self) -> &'static str {
		match self {
			Self::Int32(_) => "int32",
			Self::Int64(_) => "int64",
			Self::Hash(_) => "hash",
		}
	}

	fn row_ref_count(&self) -> u64 {
		match self {
			Self::Int32(index) => index.row_ref_count(),
			Self::Int64(index) => index.row_ref_count(),
			Self::Hash(index) => index
				.values()
				.map(|refs| u64::try_from(refs.len()).unwrap_or(u64::MAX))
				.fold(0u64, u64::saturating_add),
		}
	}

	#[cfg(test)]
	fn computed_bytes(&self) -> u64 {
		match self {
			Self::Int32(index) => index.computed_bytes(),
			Self::Int64(index) => index.computed_bytes(),
			Self::Hash(index) => spilled_batch_join_hash_index_bytes(index),
		}
	}

	#[cfg(test)]
	fn is_int64(&self) -> bool {
		matches!(self, Self::Int64(_))
	}
}

struct PrimitiveSpilledBatchIndex<K> {
	buckets: Vec<u32>,
	entries: Vec<PrimitiveSpilledBatchIndexEntry<K>>,
	hasher: RandomState,
	accounted_bytes: u64,
	build: PrimitiveIndexBuild,
}

/// ADR 601: how the primitive index is built. `Incremental` hashes every insert into buckets
/// that double as they fill; `Collecting` appends entries without hashing until `finalize`
/// builds the buckets once, after which the index is `Finalized` and takes no more inserts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PrimitiveIndexBuild {
	Incremental,
	Collecting { growths: u64 },
	Finalized(FinishIndexStats),
}

/// ADR 601: what the finalization pass produced — the distinct keys kept, the repeated rows
/// moved onto overflow lists, the bucket count obtained, and the entry-vector growths the
/// collecting phase needed past its reservation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::exec) struct FinishIndexStats {
	pub(in crate::exec) entries: u64,
	pub(in crate::exec) duplicates: u64,
	pub(in crate::exec) buckets: u64,
	pub(in crate::exec) growths: u64,
}

/// ADR 601: whether a plan takes the finish-built form — the primitive variants' admission,
/// exactly `for_key_plan`'s. A `Generic` or null-safe plan takes the `Hash` variant, incremental,
/// with neither of the finish-built clocks.
pub(in crate::exec) fn finish_built_index_admitted(
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
) -> bool {
	!null_safe_keys && matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64)
}

/// ADR 601: the bucket array a finalization needs for `entry_count` entries — the smallest
/// power of two at or above twice the count, and its bytes — refused by name when the count
/// cannot be indexed by the `u32` buckets or the array cannot be represented. Pure: nothing is
/// allocated here.
pub(in crate::exec) fn finish_index_plan(
	entry_count: usize,
) -> Result<(usize, u64), ExecutionError> {
	let representable = u32::try_from(entry_count)
		.ok()
		.is_some_and(|count| count < EMPTY_SPILLED_BATCH_BUCKET);
	if !representable {
		return Err(ExecutionError::Kernel {
			message: format!(
				"finish-built spill index entry count {entry_count} exceeds the u32 bucket range"
			),
		});
	}
	let bucket_count = entry_count
		.checked_mul(2)
		.and_then(usize::checked_next_power_of_two)
		.map(|count| count.max(1))
		.ok_or_else(|| ExecutionError::Kernel {
			message: format!(
				"finish-built spill index bucket count for {entry_count} entries is not representable"
			),
		})?;
	let bucket_bytes = bucket_count
		.checked_mul(std::mem::size_of::<u32>())
		.and_then(|bytes| u64::try_from(bytes).ok())
		.ok_or_else(|| ExecutionError::Kernel {
			message: format!(
				"finish-built spill index bucket bytes for {bucket_count} buckets overflowed"
			),
		})?;
	Ok((bucket_count, bucket_bytes))
}

/// ADR 601: the entry vector a reservation of `expected_rows` needs, in bytes, checked.
fn reserved_entry_bytes<K>(expected_rows: usize) -> Result<u64, ExecutionError> {
	expected_rows
		.checked_mul(std::mem::size_of::<PrimitiveSpilledBatchIndexEntry<K>>())
		.and_then(|bytes| u64::try_from(bytes).ok())
		.ok_or_else(|| ExecutionError::Kernel {
			message: format!(
				"finish-built spill index reservation for {expected_rows} rows overflowed"
			),
		})
}

struct PrimitiveSpilledBatchIndexEntry<K> {
	key: K,
	refs: SpilledBatchRefList,
}

const EMPTY_SPILLED_BATCH_BUCKET: u32 = u32::MAX;

impl<K> PrimitiveSpilledBatchIndex<K>
where
	K: Copy + Eq + std::hash::Hash,
{
	fn with_capacity(capacity: usize) -> Self {
		let bucket_count = capacity.saturating_mul(2).next_power_of_two().max(1);
		let buckets = vec![EMPTY_SPILLED_BATCH_BUCKET; bucket_count];
		let entries = Vec::with_capacity(capacity);
		let accounted_bytes = spilled_primitive_index_base_bytes::<K>(&buckets, &entries, 0);
		Self {
			buckets,
			entries,
			hasher: RandomState::new(),
			accounted_bytes,
			build: PrimitiveIndexBuild::Incremental,
		}
	}

	/// ADR 601: a collecting index — the entry vector obtained at `expected_rows` capacity and
	/// accounted at the capacity obtained; no buckets until `finalize`.
	fn reserved(expected_rows: usize) -> Self {
		let entries: Vec<PrimitiveSpilledBatchIndexEntry<K>> = Vec::with_capacity(expected_rows);
		let accounted_bytes = spilled_primitive_index_base_bytes::<K>(&[], &entries, 0);
		Self {
			buckets: Vec::new(),
			entries,
			hasher: RandomState::new(),
			accounted_bytes,
			build: PrimitiveIndexBuild::Collecting { growths: 0 },
		}
	}

	#[cfg(test)]
	fn seed_hasher(&mut self, seeds: (u64, u64, u64, u64)) {
		self.hasher = RandomState::with_seeds(seeds.0, seeds.1, seeds.2, seeds.3);
	}

	fn is_collecting(&self) -> bool {
		matches!(self.build, PrimitiveIndexBuild::Collecting { .. })
	}

	fn finish_stats(&self) -> Option<FinishIndexStats> {
		match self.build {
			PrimitiveIndexBuild::Finalized(stats) => Some(stats),
			_ => None,
		}
	}

	fn capacity_growths(&self) -> u64 {
		match self.build {
			PrimitiveIndexBuild::Incremental => 0,
			PrimitiveIndexBuild::Collecting { growths } => growths,
			PrimitiveIndexBuild::Finalized(stats) => stats.growths,
		}
	}

	/// ADR 601: the collecting append — no bucket, no hash, no probe; a push past the obtained
	/// capacity grows the vector as `Vec` grows it, the delta accounted after and counted.
	fn collect(
		&mut self,
		key: K,
		row_ref: CompactSpilledBatchRowRef,
	) -> Result<(), ExecutionError> {
		let PrimitiveIndexBuild::Collecting { growths } = &mut self.build else {
			return Err(ExecutionError::Kernel {
				message: "finish-built spill index took an insert after finalization".to_string(),
			});
		};
		if self.entries.len() == self.entries.capacity() {
			*growths = growths.saturating_add(1);
		}
		let before_capacity = self.entries.capacity();
		self.entries.push(PrimitiveSpilledBatchIndexEntry {
			key,
			refs: SpilledBatchRefList::new(row_ref),
		});
		let capacity_delta = self
			.entries
			.capacity()
			.saturating_sub(before_capacity)
			.saturating_mul(std::mem::size_of::<PrimitiveSpilledBatchIndexEntry<K>>());
		self.accounted_bytes = self
			.accounted_bytes
			.saturating_add(capacity_delta.try_into().unwrap_or(u64::MAX));
		Ok(())
	}

	/// ADR 601: the finalization pass. The buckets are obtained at the planned count (the caller
	/// guarded the planned bytes) and accounted; then one pass in insertion order with a write
	/// cursor: a new key takes the cursor's slot (the entry moved down to it, the bucket pointing
	/// at it); a repeated key pushes its ref onto the earlier entry's overflow list, accounted as
	/// obtained, and is dropped, its slot free for a later move. Refs stay in insertion order.
	/// The vector is truncated to the cursor; its capacity is kept and stays accounted.
	fn finalize(&mut self) -> Result<FinishIndexStats, ExecutionError> {
		let PrimitiveIndexBuild::Collecting { growths } = self.build else {
			return Err(ExecutionError::Kernel {
				message: "finish-built spill index finalized twice".to_string(),
			});
		};
		let (bucket_count, bucket_bytes) = finish_index_plan(self.entries.len())?;
		let mut buckets = vec![EMPTY_SPILLED_BATCH_BUCKET; bucket_count];
		self.accounted_bytes = self.accounted_bytes.saturating_add(bucket_bytes);
		let mut write = 0usize;
		let mut duplicates = 0u64;
		for read in 0..self.entries.len() {
			let key = self.entries[read].key;
			let mut bucket = Self::bucket_for_key(&self.hasher, buckets.len(), key);
			loop {
				let slot = buckets[bucket];
				if slot == EMPTY_SPILLED_BATCH_BUCKET {
					// A new key: the buckets only ever point below the cursor, so the move
					// downward (write <= read) never disturbs an entry a bucket references.
					let entry = std::mem::replace(
						&mut self.entries[read].refs,
						SpilledBatchRefList::new(CompactSpilledBatchRowRef {
							page_index: 0,
							row: 0,
						}),
					);
					self.entries[write] = PrimitiveSpilledBatchIndexEntry { key, refs: entry };
					buckets[bucket] = u32::try_from(write).map_err(|_| ExecutionError::Kernel {
						message: "finish-built spill index cursor exceeds u32".to_string(),
					})?;
					write += 1;
					break;
				}
				let slot = usize::try_from(slot).map_err(|_| ExecutionError::Kernel {
					message: "finish-built spill index bucket entry does not fit usize".to_string(),
				})?;
				if self.entries[slot].key == key {
					// A repeated key: the earlier entry (below the cursor) takes the ref, in
					// insertion order; this slot is dropped and free for a later move.
					let row_ref = self.entries[read].refs.first;
					let delta = self.entries[slot].refs.push(row_ref);
					self.accounted_bytes = self.accounted_bytes.saturating_add(delta);
					duplicates = duplicates.saturating_add(1);
					break;
				}
				bucket = (bucket + 1) & (buckets.len() - 1);
			}
		}
		self.entries.truncate(write);
		self.buckets = buckets;
		let stats = FinishIndexStats {
			entries: u64::try_from(write).unwrap_or(u64::MAX),
			duplicates,
			buckets: u64::try_from(bucket_count).unwrap_or(u64::MAX),
			growths,
		};
		self.build = PrimitiveIndexBuild::Finalized(stats);
		Ok(stats)
	}

	fn insert(&mut self, key: K, row_ref: CompactSpilledBatchRowRef) -> Result<(), ExecutionError> {
		match self.build {
			PrimitiveIndexBuild::Incremental => {}
			PrimitiveIndexBuild::Collecting { .. } => return self.collect(key, row_ref),
			PrimitiveIndexBuild::Finalized(_) => {
				return Err(ExecutionError::Kernel {
					message: "finish-built spill index took an insert after finalization"
						.to_string(),
				});
			}
		}
		let mut bucket = self.find_bucket(key);
		let entry_index = self.buckets[bucket];
		if entry_index == EMPTY_SPILLED_BATCH_BUCKET {
			if self.entries.len().saturating_add(1).saturating_mul(2) > self.buckets.len() {
				self.resize_buckets(self.buckets.len().saturating_mul(2).max(1))?;
				bucket = self.find_bucket(key);
			}
			let before_capacity = self.entries.capacity();
			let entry_index =
				u32::try_from(self.entries.len()).map_err(|_| ExecutionError::Kernel {
					message: "compact spill index entry count exceeds u32".to_string(),
				})?;
			self.entries.push(PrimitiveSpilledBatchIndexEntry {
				key,
				refs: SpilledBatchRefList::new(row_ref),
			});
			let after_capacity = self.entries.capacity();
			let capacity_delta = after_capacity
				.saturating_sub(before_capacity)
				.saturating_mul(std::mem::size_of::<PrimitiveSpilledBatchIndexEntry<K>>());
			self.accounted_bytes = self
				.accounted_bytes
				.saturating_add(capacity_delta.try_into().unwrap_or(u64::MAX));
			self.buckets[bucket] = entry_index;
		} else {
			let entry_index = usize::try_from(entry_index).map_err(|_| ExecutionError::Kernel {
				message: "compact spill index bucket entry does not fit usize".to_string(),
			})?;
			let delta = self.entries[entry_index].refs.push(row_ref);
			self.accounted_bytes = self.accounted_bytes.saturating_add(delta);
		}
		Ok(())
	}

	fn lookup(&self, key: K, form: SpilledLookupForm) -> SpilledLookupMatches {
		let bucket = self.find_bucket(key);
		let entry_index = self.buckets[bucket];
		if entry_index == EMPTY_SPILLED_BATCH_BUCKET {
			SpilledLookupMatches::Miss
		} else {
			self.entries[entry_index as usize].refs.matches(form)
		}
	}

	fn accounted_bytes(&self) -> u64 {
		self.accounted_bytes
	}

	fn row_ref_count(&self) -> u64 {
		self.entries
			.iter()
			.map(|entry| u64::try_from(entry.refs.len()).unwrap_or(u64::MAX))
			.fold(0u64, u64::saturating_add)
	}

	#[cfg(test)]
	fn computed_bytes(&self) -> u64 {
		spilled_primitive_index_base_bytes(&self.buckets, &self.entries, self.overflow_heap_bytes())
	}

	#[cfg(test)]
	fn overflow_heap_bytes(&self) -> u64 {
		self.entries
			.iter()
			.map(|entry| entry.refs.heap_bytes())
			.fold(0u64, u64::saturating_add)
	}

	fn resize_buckets(&mut self, bucket_count: usize) -> Result<(), ExecutionError> {
		let old_bucket_bytes = self
			.buckets
			.len()
			.saturating_mul(std::mem::size_of::<u32>());
		let mut buckets = vec![EMPTY_SPILLED_BATCH_BUCKET; bucket_count.next_power_of_two().max(1)];
		for (entry_index, entry) in self.entries.iter().enumerate() {
			let entry_index = u32::try_from(entry_index).map_err(|_| ExecutionError::Kernel {
				message: "compact spill index entry count exceeds u32".to_string(),
			})?;
			let mut bucket = Self::bucket_for_key(&self.hasher, buckets.len(), entry.key);
			loop {
				if buckets[bucket] == EMPTY_SPILLED_BATCH_BUCKET {
					buckets[bucket] = entry_index;
					break;
				}
				bucket = (bucket + 1) & (buckets.len() - 1);
			}
		}
		let new_bucket_bytes = buckets.len().saturating_mul(std::mem::size_of::<u32>());
		if new_bucket_bytes >= old_bucket_bytes {
			self.accounted_bytes = self.accounted_bytes.saturating_add(
				(new_bucket_bytes - old_bucket_bytes)
					.try_into()
					.unwrap_or(u64::MAX),
			);
		} else {
			self.accounted_bytes = self.accounted_bytes.saturating_sub(
				(old_bucket_bytes - new_bucket_bytes)
					.try_into()
					.unwrap_or(u64::MAX),
			);
		}
		self.buckets = buckets;
		Ok(())
	}

	fn find_bucket(&self, key: K) -> usize {
		let mut bucket = Self::bucket_for_key(&self.hasher, self.buckets.len(), key);
		loop {
			let entry_index = self.buckets[bucket];
			if entry_index == EMPTY_SPILLED_BATCH_BUCKET
				|| self.entries[entry_index as usize].key == key
			{
				return bucket;
			}
			bucket = (bucket + 1) & (self.buckets.len() - 1);
		}
	}

	fn bucket_for_key(hasher: &RandomState, bucket_count: usize, key: K) -> usize {
		(hasher.hash_one(key) as usize) & (bucket_count - 1)
	}
}

fn spilled_primitive_index_base_bytes<K>(
	buckets: &[u32],
	entries: &Vec<PrimitiveSpilledBatchIndexEntry<K>>,
	overflow_heap_bytes: u64,
) -> u64 {
	u64::try_from(
		buckets
			.len()
			.saturating_mul(std::mem::size_of::<u32>())
			.saturating_add(
				entries
					.capacity()
					.saturating_mul(std::mem::size_of::<PrimitiveSpilledBatchIndexEntry<K>>()),
			),
	)
	.unwrap_or(u64::MAX)
	.saturating_add(overflow_heap_bytes)
}

/// ADR 514: the owner-complete resident bound for a compact single-key streamed
/// scan build at one partition, priced from the pre-scan's exact row count.
/// Every simultaneously resident owner is charged with the same layout this
/// module actually allocates: power-of-two bucket capacity (4 B per bucket),
/// power-of-two entry-vector capacity (the widest compact entry), a worst-case
/// duplicate-ref overflow term covering any key distribution, the decoded-page
/// cache target, page metadata, and write scratch. Returns `None` when any term
/// overflows; callers fail closed to the legacy row-overhead estimate.
pub(in crate::exec) fn compact_streamed_build_owner_bound_bytes(
	rows: u64,
	decoded_bytes: u64,
) -> Option<u64> {
	let rows_usize = usize::try_from(rows).ok()?;
	let bucket_count = rows_usize
		.checked_mul(2)?
		.checked_next_power_of_two()?
		.max(1);
	let entry_count = rows_usize.checked_next_power_of_two()?.max(1);
	let bucket_bytes = u64::try_from(bucket_count.checked_mul(std::mem::size_of::<u32>())?).ok()?;
	let entry_bytes = u64::try_from(
		entry_count.checked_mul(std::mem::size_of::<PrimitiveSpilledBatchIndexEntry<i64>>())?,
	)
	.ok()?;
	let per_overflow_ref = u64::try_from(std::mem::size_of::<CompactSpilledBatchRowRef>())
		.ok()?
		.checked_add(8)?;
	let overflow_bytes = rows.checked_mul(per_overflow_ref)?;
	let page_size = u64::try_from(JOIN_SPILL_PAGE_SIZE).ok()?;
	let page_count = decoded_bytes.checked_div(page_size)?.checked_add(1)?;
	let metadata_bytes = page_count.checked_mul(256)?;
	let scratch_bytes = page_size.checked_mul(4)?;
	bucket_bytes
		.checked_add(entry_bytes)?
		.checked_add(overflow_bytes)?
		.checked_add(JOIN_SPILL_CACHE_TARGET_BYTES)?
		.checked_add(metadata_bytes)?
		.checked_add(scratch_bytes)
}

#[derive(Debug, Clone, Copy)]
pub(in crate::exec) struct SpilledInnerJoinMatch {
	pub(super) build: SpilledBatchRowRef,
	pub(super) probe: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) struct PinnedSpilledInnerBuildStats {
	pub(in crate::exec) page_reads: u64,
	pub(in crate::exec) decoded_bytes: u64,
	pub(in crate::exec) index_bytes: u64,
	pub(in crate::exec) rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::exec) enum PinDecodedPagesOutcome {
	Admitted,
	RejectedPreflight,
	RejectedMidDecode,
}

impl PinDecodedPagesOutcome {
	pub(in crate::exec) const fn is_admitted(self) -> bool {
		matches!(self, Self::Admitted)
	}
}

fn record_grouped_chain_pin_outcome(
	observer: Option<&GroupedChainSpillObserver<'_>>,
	outcome: PinDecodedPagesOutcome,
) -> Result<(), ExecutionError> {
	let Some(observer) = observer else {
		return Ok(());
	};
	let outcome = match outcome {
		PinDecodedPagesOutcome::Admitted => GroupedChainPinOutcome::Admitted,
		PinDecodedPagesOutcome::RejectedPreflight => GroupedChainPinOutcome::RejectedPreflight,
		PinDecodedPagesOutcome::RejectedMidDecode => GroupedChainPinOutcome::RejectedMidDecode,
	};
	observer.record_pin_outcome(outcome)
}

/// How the partitioned scan-build route constructs its resident page index.
///
/// This value is latched at the route boundary and carried through every coalescing flush. The
/// writer never rereads process state, so one route invocation cannot mix indexed and unindexed
/// pages.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::exec) enum CoalescedPageIndexMode {
	RebuildAfterWrite,
	IndexAtFlush,
}

impl CoalescedPageIndexMode {
	const fn indexes_at_flush(self) -> bool {
		matches!(self, Self::IndexAtFlush)
	}
}

/// ADR 596: the eleven children of the spilled inner build's write, in the order the writer
/// runs them. Each is one span per call of the code it names; the caller's awaits are
/// sequential, so the spans are disjoint and their sum is bounded by `inner_1_build_write`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::exec) enum BuildWriteChild {
	Create,
	ScanOpen,
	ScanPoll,
	Filter,
	Repartition,
	Project,
	KeyExtract,
	Encode,
	PageWrite,
	Index,
	Finish,
	/// ADR 601: the finish-built index's reservation, around the admitted activation call.
	Reserve,
	/// ADR 601: the finish-built index's finalization pass, before `finish`.
	FinishIndex,
}

impl BuildWriteChild {
	pub(in crate::exec) const ALL: [Self; 13] = [
		Self::Create,
		Self::ScanOpen,
		Self::ScanPoll,
		Self::Filter,
		Self::Repartition,
		Self::Project,
		Self::KeyExtract,
		Self::Encode,
		Self::PageWrite,
		Self::Index,
		Self::Finish,
		Self::Reserve,
		Self::FinishIndex,
	];

	/// The receipt's term name; `build_write_scan_open` and `build_write_scan_poll` are reported
	/// beside each other and summed as `scan_wait` by the rule, never ranked beside it.
	pub(in crate::exec) const fn name(self) -> &'static str {
		match self {
			Self::Create => "build_write_create",
			Self::ScanOpen => "build_write_scan_open",
			Self::ScanPoll => "build_write_scan_poll",
			Self::Filter => "build_write_filter",
			Self::Repartition => "build_write_repartition",
			Self::Project => "build_write_project",
			Self::KeyExtract => "build_write_key_extract",
			Self::Encode => "build_write_encode",
			Self::PageWrite => "build_write_page_write",
			Self::Index => "build_write_index",
			Self::Finish => "build_write_finish",
			Self::Reserve => "build_write_reserve",
			Self::FinishIndex => "build_write_finish_index",
		}
	}

	const fn slot(self) -> usize {
		self as usize
	}
}

/// How the writer's batch loop ended: a final `None` from the stream, or the row limit reached
/// with no final poll. The poll count depends on which.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::exec) enum BuildWriteTermination {
	StreamEnd,
	RowLimit,
}

impl BuildWriteTermination {
	const fn name(self) -> &'static str {
		match self {
			Self::StreamEnd => "stream_end",
			Self::RowLimit => "row_limit",
		}
	}
}

/// ADR 597: how the spilled build's writer treats an Exchange on its scan input. `Split` is
/// today's loop — `repartition_exchange_batch` per batch, each non-empty piece to the chunk
/// writer, the partition index discarded. `PassThrough` hands each batch to the chunk writer
/// whole; it is taken only under the admission the writer checks, and falls back to `Split`
/// with the refusal recorded otherwise.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::exec) enum ExchangeHandling {
	Split,
	PassThrough,
}

impl ExchangeHandling {
	pub(in crate::exec) const fn name(self) -> &'static str {
		match self {
			Self::Split => "split",
			Self::PassThrough => "pass_through",
		}
	}
}

/// ADR 596: a local accumulator for the spilled build's write, threaded as `Option<&mut>`
/// through the writer on replay 1's streamed-scan path only and `None` everywhere else. The
/// ledger is never locked; the caller publishes it once when the branch completes. Facts follow
/// the branches the writer actually took: a child that did not run has zero calls and zero
/// time, and the counts say which rules apply.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(in crate::exec) struct BuildWriteLedger {
	micros: [u64; 13],
	calls: [u64; 13],
	pub(in crate::exec) filter_present: bool,
	pub(in crate::exec) exchange_partitions: Option<u32>,
	pub(in crate::exec) projected: bool,
	pub(in crate::exec) termination: Option<BuildWriteTermination>,
	pub(in crate::exec) index_mode: Option<&'static str>,
	/// A shape outside the band's vocabulary (a preserved-rows write, a join-partition
	/// selection); the receipt reports it rather than reading the terms through the band.
	pub(in crate::exec) unsupported: Option<&'static str>,
	pub(in crate::exec) batches_read: u64,
	/// Batches a poll returned that the row limit discarded whole (the limit already met);
	/// read but never admitted to the filter, the Exchange, or the writer.
	pub(in crate::exec) batches_limit_discarded: u64,
	pub(in crate::exec) batches_filtered_empty: u64,
	pub(in crate::exec) batches_written: u64,
	pub(in crate::exec) rows_read: u64,
	pub(in crate::exec) rows_kept: u64,
	pub(in crate::exec) chunks: u64,
	pub(in crate::exec) chunks_unkeyed: u64,
	pub(in crate::exec) pages: u64,
	pub(in crate::exec) plaintext_bytes: u64,
	pub(in crate::exec) ciphertext_bytes: u64,
	pub(in crate::exec) index_entries: u64,
	pub(in crate::exec) index_compactions: u64,
	/// ADR 600: the index's variant (`hash`, `int32`, `int64`) as the build reports it after a
	/// page, distinct from `index_mode` (whether the compact wrapper was enabled).
	pub(in crate::exec) index_variant: Option<&'static str>,
	/// ADR 601: how the index was built (`incremental` / `finish`), the rows the reservation
	/// expected, the vector's growths past it, and what the finalization pass produced.
	pub(in crate::exec) index_build: Option<&'static str>,
	pub(in crate::exec) index_expected_rows: Option<u64>,
	pub(in crate::exec) index_capacity_growths: u64,
	pub(in crate::exec) finish_index: Option<FinishIndexStats>,
	/// ADR 597: how the Exchange was handled (`None` when the input carries none), whether
	/// pass-through was requested, and why it was refused when it was.
	pub(in crate::exec) exchange_handling: Option<ExchangeHandling>,
	pub(in crate::exec) pass_through_requested: bool,
	pub(in crate::exec) pass_through_refusal: Option<&'static str>,
	/// ADR 597: the writer's transient peaks, recorded and not guarded — the obtained
	/// capacities of the two scratch buffers, the plaintext high-water mark the guard reads,
	/// and the largest resident-state total seen at a guard site.
	pub(in crate::exec) scratch_plaintext_capacity_peak: u64,
	pub(in crate::exec) scratch_refs_capacity_peak: u64,
	pub(in crate::exec) writer_peak_bytes: u64,
	pub(in crate::exec) resident_state_peak_bytes: u64,
	/// ADR 597: the write guard's scratch releases — the guard's answer to a resident total
	/// over the cap is to drop the scratch and re-check, not to refuse on the page's size.
	pub(in crate::exec) scratch_releases: u64,
}

impl BuildWriteLedger {
	/// Take the clock only when a ledger is armed; the off arm reads nothing.
	pub(in crate::exec) fn begin(ledger: &Option<&mut Self>) -> Option<Instant> {
		ledger.as_ref().map(|_| Instant::now())
	}

	/// Close one span into its child; a `None` start (no ledger) records nothing.
	pub(in crate::exec) fn finish(
		ledger: &mut Option<&mut Self>,
		child: BuildWriteChild,
		started: Option<Instant>,
	) {
		if let (Some(ledger), Some(started)) = (ledger.as_deref_mut(), started) {
			ledger.micros[child.slot()] = ledger.micros[child.slot()]
				.saturating_add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
			ledger.calls[child.slot()] = ledger.calls[child.slot()].saturating_add(1);
		}
	}

	pub(in crate::exec) fn micros(&self, child: BuildWriteChild) -> u64 {
		self.micros[child.slot()]
	}

	pub(in crate::exec) fn calls(&self, child: BuildWriteChild) -> u64 {
		self.calls[child.slot()]
	}

	/// The path facts and counts as the receipt carries them.
	pub(in crate::exec) fn facts(&self) -> serde_json::Value {
		serde_json::json!({
			"filter_present": self.filter_present,
			"exchange": self.exchange_partitions.is_some(),
			"exchange_partitions": self.exchange_partitions,
			"projected": self.projected,
			"termination": self.termination.map(BuildWriteTermination::name),
			"index_mode": self.index_mode.unwrap_or("none"),
			"unsupported": self.unsupported,
			"batches_read": self.batches_read,
			"batches_limit_discarded": self.batches_limit_discarded,
			"batches_admitted": self.batches_read.saturating_sub(self.batches_limit_discarded),
			"batches_filtered_empty": self.batches_filtered_empty,
			"batches_written": self.batches_written,
			"rows_read": self.rows_read,
			"rows_kept": self.rows_kept,
			"chunks": self.chunks,
			"chunks_unkeyed": self.chunks_unkeyed,
			"pages": self.pages,
			"plaintext_bytes": self.plaintext_bytes,
			"ciphertext_bytes": self.ciphertext_bytes,
			"index_entries": self.index_entries,
			"index_compactions": self.index_compactions,
			"index_variant": self.index_variant.unwrap_or("none"),
			"index_build": self.index_build.unwrap_or("none"),
			"index_expected_rows": self.index_expected_rows,
			"index_capacity_growths": self.index_capacity_growths,
			"finish_index_entries": self.finish_index.map(|stats| stats.entries),
			"finish_index_duplicates": self.finish_index.map(|stats| stats.duplicates),
			"finish_index_buckets": self.finish_index.map(|stats| stats.buckets),
			"exchange_handling": self.exchange_handling.map_or("none", ExchangeHandling::name),
			"pass_through_requested": self.pass_through_requested,
			"pass_through_refusal": self.pass_through_refusal,
			"scratch_plaintext_capacity_peak": self.scratch_plaintext_capacity_peak,
			"scratch_refs_capacity_peak": self.scratch_refs_capacity_peak,
			"writer_peak_bytes": self.writer_peak_bytes,
			"resident_state_peak_bytes": self.resident_state_peak_bytes,
			"scratch_releases": self.scratch_releases,
		})
	}

	/// The same observation after `finish`, from the build's accessors.
	/// ADR 601: the finish-built index's facts as it stands after finalization — the form, the
	/// mode and variant (settled even when no page carried a keyed row), the reservation's
	/// rows, the growths, and the pass's counts. The incremental form keeps its per-page facts.
	pub(in crate::exec) fn observe_finish_built_index(&mut self, build: &SpilledInnerJoinBuild) {
		if build.index_build_name() != "finish" {
			return;
		}
		self.index_build = Some("finish");
		self.index_mode = Some("compact");
		self.index_variant = Some(build.index_variant_name());
		self.index_expected_rows = build.finish_built_expected_rows();
		self.index_capacity_growths = build
			.compact_index
			.as_ref()
			.map_or(0, SpilledBatchIndex::capacity_growths);
		self.finish_index = build.finish_index_stats();
	}

	pub(in crate::exec) fn observe_peaks_after_finish(
		&mut self,
		plaintext_capacity: usize,
		refs_capacity: usize,
		writer_peak: u64,
		resident_total: u64,
	) {
		self.observe_peaks(
			plaintext_capacity,
			refs_capacity,
			writer_peak,
			resident_total,
		);
	}

	/// Record the writer's transient peaks after a page: capacities as obtained, the plaintext
	/// high-water mark, and the resident total the guard just read.
	fn observe_peaks(
		&mut self,
		plaintext_capacity: usize,
		refs_capacity: usize,
		writer_peak: u64,
		resident_total: u64,
	) {
		self.scratch_plaintext_capacity_peak = self
			.scratch_plaintext_capacity_peak
			.max(u64::try_from(plaintext_capacity).unwrap_or(u64::MAX));
		self.scratch_refs_capacity_peak = self
			.scratch_refs_capacity_peak
			.max(u64::try_from(refs_capacity).unwrap_or(u64::MAX));
		self.writer_peak_bytes = self.writer_peak_bytes.max(writer_peak);
		self.resident_state_peak_bytes = self.resident_state_peak_bytes.max(resident_total);
	}
}

pub(in crate::exec) struct SpilledInnerJoinBuild {
	spill_id: u64,
	path: PathBuf,
	file_ops: Arc<dyn AsyncFileOps>,
	file: FileHandle,
	key: LessSafeKey,
	pages: Vec<JoinSpillPage>,
	page_rows: Vec<u32>,
	index: HashMap<JoinKey, Vec<SpilledBatchRowRef>>,
	compact_index: Option<SpilledBatchIndex>,
	resident_index_bytes: u64,
	cache: HashMap<usize, CachedJoinSpillPage>,
	lru: VecDeque<usize>,
	cache_bytes: u64,
	cache_cap: u64,
	cache_peak_bytes: u64,
	pinned_decoded_pages: Option<Vec<RecordBatch>>,
	pinned_decoded_bytes: u64,
	pin_decoded_outcome: Option<PinDecodedPagesOutcome>,
	spill_bytes: u64,
	writer_peak: u64,
	// Only the unindexed reservoir writer uses this buffer. Indexed writers publish
	// page references as they write, so changing their page boundaries would change
	// the meaning of already-recorded row references.
	pending_unindexed_batches: Vec<RecordBatch>,
	pending_unindexed_rows: usize,
	pending_unindexed_bytes: u64,
	rows: u64,
	input_rows: u64,
	input_batches_written: u64,
	#[cfg(test)]
	legacy_index_insert_count: u64,
	#[cfg(test)]
	page_decode_count: u64,
	#[cfg(test)]
	fail_candidate_index_insert_at: Option<u64>,
	#[cfg(test)]
	candidate_index_insert_attempts: u64,
	indexed_flush_pages: u64,
	indexed_flush_guard_checks: u64,
	index_rebuild_calls: u64,
	physical_read_pages: u64,
	coalesced_index_mode: Option<CoalescedPageIndexMode>,
	compact_index_enabled: bool,
	compact_index_rescue_enabled: bool,
	next_offset: u64,
	scratch_plaintext: Vec<u8>,
	scratch_refs: Vec<(JoinKey, u32)>,
	/// ADR 597: how many times the write guard released the scratch instead of refusing.
	write_scratch_releases: u64,
	/// ADR 601: the rows the finish-built reservation expected, when that form was taken.
	finish_built_expected_rows: Option<u64>,
	/// ADR 600: lookup counting, enabled on the profiled arm only.
	count_lookups: bool,
	lookup_hits: u64,
	lookup_misses: u64,
	/// ADR 602: how a compact-index hit returns its refs, and, under counting, how many hits
	/// came back as `One`.
	lookup_form: SpilledLookupForm,
	lookup_inline_hits: u64,
	resident_baseline_bytes: u64,
	throughput_profile: Option<Box<StreamedSpilledLeftOuterProfile>>,
}

/// Query-neutral ownership census for the grouped-reservoir residency profile (ADR 429).
/// Historical writer high-water is kept separate from current retained ownership because the
/// former can overlap the latter. `current_resident_bytes` is therefore a checked sum of only
/// the currently owned components.
#[derive(Clone, Debug, serde::Serialize)]
pub(in crate::exec) struct SpilledInnerJoinResidencySnapshot {
	pub(in crate::exec) resident_baseline_guard_bytes: u64,
	pub(in crate::exec) resident_index_bytes: u64,
	pub(in crate::exec) writer_scratch_peak_bytes: u64,
	pub(in crate::exec) page_reserve_bytes: u64,
	pub(in crate::exec) cache_payload_bytes: u64,
	pub(in crate::exec) cache_cap_bytes: u64,
	pub(in crate::exec) cache_peak_bytes: u64,
	pub(in crate::exec) pinned_decoded_bytes: u64,
	pub(in crate::exec) page_metadata_bytes: Option<u64>,
	pub(in crate::exec) cache_metadata_bytes: Option<u64>,
	pub(in crate::exec) pinned_handle_capacity_bytes: Option<u64>,
	pub(in crate::exec) write_scratch_capacity_bytes: Option<u64>,
	pub(in crate::exec) retained_legacy_index_bytes: Option<u64>,
	pub(in crate::exec) path_capacity_bytes: Option<u64>,
	pub(in crate::exec) open_file_handles: u64,
	pub(in crate::exec) input_rows: u64,
	pub(in crate::exec) spill_disk_bytes: u64,
	pub(in crate::exec) current_resident_bytes: Option<u64>,
	pub(in crate::exec) current_resident_error: Option<&'static str>,
	pub(in crate::exec) guard_accounted_bytes: Option<u64>,
	pub(in crate::exec) guard_accounted_error: Option<&'static str>,
	pub(in crate::exec) overlap_label: &'static str,
}

fn checked_spill_capacity_bytes<T>(capacity: usize) -> Option<u64> {
	let bytes = capacity.checked_mul(std::mem::size_of::<T>())?;
	u64::try_from(bytes).ok()
}

fn checked_spill_sum(values: impl IntoIterator<Item = u64>) -> Option<u64> {
	values
		.into_iter()
		.try_fold(0u64, |total, value| total.checked_add(value))
}

impl SpilledInnerJoinBuild {
	pub(in crate::exec) async fn create(
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
		spill_dir: PathBuf,
	) -> Result<Self, ExecutionError> {
		Self::create_with_resident_baseline(task_budget, file_ops, spill_dir, 0).await
	}

	pub(in crate::exec) async fn create_with_resident_baseline(
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
		spill_dir: PathBuf,
		resident_baseline_bytes: u64,
	) -> Result<Self, ExecutionError> {
		let spill_id = NEXT_JOIN_SPILL_ID.fetch_add(1, Ordering::SeqCst);
		let path = spill_dir.join(format!("spill_{}_{}.inner", std::process::id(), spill_id));
		match Self::create_inner_with_resident_baseline(
			spill_id,
			path.clone(),
			task_budget,
			file_ops.clone(),
			resident_baseline_bytes,
		)
		.await
		{
			Ok(spill) => Ok(spill),
			Err(error) => {
				let _ = file_ops.unlink(&path).await;
				Err(error)
			}
		}
	}

	async fn create_inner_with_resident_baseline(
		spill_id: u64,
		path: PathBuf,
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
		resident_baseline_bytes: u64,
	) -> Result<Self, ExecutionError> {
		let spill_dir = path.parent().ok_or_else(|| ExecutionError::Kernel {
			message: "inner join batch spill path has no parent directory".to_string(),
		})?;
		file_ops
			.create_dir_all(spill_dir)
			.await
			.map_err(|error| join_spill_io_error("create_dir_all", error))?;
		let mut key_bytes = [0u8; 32];
		aws_lc_rs::rand::fill(&mut key_bytes).map_err(|error| ExecutionError::Kernel {
			message: format!("inner join batch spill key generation failed: {error:?}"),
		})?;
		let unbound = UnboundKey::new(&AES_256_GCM_SIV, &key_bytes).map_err(|error| {
			ExecutionError::Kernel {
				message: format!("inner join batch spill key construction failed: {error:?}"),
			}
		})?;
		let key = LessSafeKey::new(unbound);
		let file = file_ops
			.open(
				&path,
				OpenFlags {
					read: true,
					write: true,
					create_new: true,
					..OpenFlags::default()
				},
			)
			.await
			.map_err(|error| ExecutionError::Kernel {
				message: format!("inner join batch spill open failed: {error}"),
			})?;
		let cache_cap = JOIN_SPILL_CACHE_TARGET_BYTES.min(task_budget.memory.get());
		let throughput_profile = StreamedSpilledLeftOuterProfile::from_env()?.map(Box::new);
		Ok(Self {
			spill_id,
			path,
			file_ops,
			file,
			key,
			pages: Vec::new(),
			page_rows: Vec::new(),
			index: HashMap::new(),
			compact_index: None,
			resident_index_bytes: 0,
			cache: HashMap::new(),
			lru: VecDeque::new(),
			cache_bytes: 0,
			cache_cap,
			cache_peak_bytes: 0,
			pinned_decoded_pages: None,
			pinned_decoded_bytes: 0,
			pin_decoded_outcome: None,
			spill_bytes: 0,
			writer_peak: 0,
			pending_unindexed_batches: Vec::new(),
			pending_unindexed_rows: 0,
			pending_unindexed_bytes: 0,
			rows: 0,
			input_rows: 0,
			input_batches_written: 0,
			#[cfg(test)]
			legacy_index_insert_count: 0,
			#[cfg(test)]
			page_decode_count: 0,
			#[cfg(test)]
			fail_candidate_index_insert_at: None,
			#[cfg(test)]
			candidate_index_insert_attempts: 0,
			indexed_flush_pages: 0,
			indexed_flush_guard_checks: 0,
			index_rebuild_calls: 0,
			physical_read_pages: 0,
			coalesced_index_mode: None,
			compact_index_enabled: false,
			compact_index_rescue_enabled: false,
			next_offset: 0,
			scratch_plaintext: Vec::new(),
			write_scratch_releases: 0,
			finish_built_expected_rows: None,
			count_lookups: false,
			lookup_hits: 0,
			lookup_misses: 0,
			lookup_form: SpilledLookupForm::Collected,
			lookup_inline_hits: 0,
			scratch_refs: Vec::new(),
			resident_baseline_bytes,
			throughput_profile,
		})
	}

	/// ADR 602: the lookup form for this build's compact index. `Inline` without the compact
	/// index is refused: the legacy index has no inline shape to return.
	pub(in crate::exec) fn use_lookup_form(
		&mut self,
		form: SpilledLookupForm,
	) -> Result<(), ExecutionError> {
		if form == SpilledLookupForm::Inline && !self.compact_index_enabled {
			return Err(ExecutionError::Kernel {
				message: "inline lookup form requested without the compact spill index".to_string(),
			});
		}
		self.lookup_form = form;
		Ok(())
	}

	pub(in crate::exec) fn lookup_form(&self) -> SpilledLookupForm {
		self.lookup_form
	}

	/// ADR 602: `None` when counting was never enabled; else the hits returned as `One`.
	pub(in crate::exec) fn lookup_inline_hits(&self) -> Option<u64> {
		self.count_lookups.then_some(self.lookup_inline_hits)
	}

	pub(in crate::exec) fn use_compact_resident_index(&mut self) {
		self.compact_index_enabled = true;
		self.index.clear();
		self.compact_index = None;
		self.resident_index_bytes = 0;
	}

	/// ADR 601: the finish-built form — admitted plans only (`finish_built_admitted`); the entry
	/// vector obtained at `expected_rows` capacity after a guard on the planned bytes against
	/// the retained cap, accounted at the capacity obtained. The index collects until
	/// `finalize_index`; a lookup before that is refused.
	pub(in crate::exec) fn use_compact_resident_index_built_at_finish(
		&mut self,
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		expected_rows: u64,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if !SpilledBatchIndex::finish_built_admitted(key_plan, null_safe_keys) {
			return Err(ExecutionError::Kernel {
				message: format!(
					"finish-built spill index requested for {key_plan:?} null_safe={null_safe_keys}"
				),
			});
		}
		if self.rows != 0 || !self.pages.is_empty() {
			return Err(ExecutionError::Kernel {
				message: "finish-built spill index requested after the first page".to_string(),
			});
		}
		let expected = usize::try_from(expected_rows).map_err(|_| ExecutionError::Kernel {
			message: format!(
				"finish-built spill index reservation of {expected_rows} rows does not fit usize"
			),
		})?;
		let planned = match key_plan {
			JoinKeyPlan::Int32 => reserved_entry_bytes::<i32>(expected)?,
			_ => reserved_entry_bytes::<i64>(expected)?,
		};
		self.index.clear();
		self.compact_index = None;
		self.resident_index_bytes = 0;
		self.guard_planned_resident_bytes(
			planned,
			task_budget.memory.get(),
			SpillResidentGuardSite::Reserve,
		)?;
		let index = SpilledBatchIndex::reserved(key_plan, expected)?;
		self.resident_index_bytes = index.accounted_bytes();
		self.compact_index = Some(index);
		self.compact_index_enabled = true;
		self.finish_built_expected_rows = Some(expected_rows);
		Ok(())
	}

	/// ADR 601: the finalization pass, once, before `finish`: the bucket bytes are planned from
	/// the entries collected and guarded against the retained cap before the allocation — a
	/// refusal leaves the build cleanable with the vector still accounted — then the index is
	/// built and the accounting moves to what was obtained.
	pub(in crate::exec) fn finalize_index(
		&mut self,
		task_budget: TaskBudget,
	) -> Result<FinishIndexStats, ExecutionError> {
		let Some(index) = self.compact_index.as_ref() else {
			return Err(ExecutionError::Kernel {
				message: "finalize_index on a build without a compact index".to_string(),
			});
		};
		if !index.is_collecting() {
			return Err(ExecutionError::Kernel {
				message: "finalize_index on an index that is not collecting".to_string(),
			});
		}
		let planned = index.planned_finish_bucket_bytes()?;
		self.guard_planned_resident_bytes(
			planned,
			task_budget.memory.get(),
			SpillResidentGuardSite::FinishIndex,
		)?;
		let index = self.compact_index.as_mut().expect("checked above");
		let stats = index.finalize()?;
		self.resident_index_bytes = index.accounted_bytes();
		Ok(stats)
	}

	/// ADR 601: the writer's boundary — a finalized index takes no more rows; every writer
	/// entry refuses here, before any page I/O or early return, and leaves the build as it was.
	fn refuse_write_after_finalization(&self) -> Result<(), ExecutionError> {
		if self
			.compact_index
			.as_ref()
			.is_some_and(|index| index.finish_stats().is_some())
		{
			return Err(ExecutionError::Kernel {
				message: "inner join spill write after the finish-built index was finalized"
					.to_string(),
			});
		}
		Ok(())
	}

	/// ADR 601: the ledger's `index_build` fact — `finish` while collecting or finalized, else
	/// `incremental`.
	pub(in crate::exec) fn index_build_name(&self) -> &'static str {
		match self.compact_index.as_ref() {
			Some(index) if index.is_collecting() || index.finish_stats().is_some() => "finish",
			_ => "incremental",
		}
	}

	pub(in crate::exec) fn finish_index_stats(&self) -> Option<FinishIndexStats> {
		self.compact_index
			.as_ref()
			.and_then(SpilledBatchIndex::finish_stats)
	}

	pub(in crate::exec) fn finish_built_expected_rows(&self) -> Option<u64> {
		self.finish_built_expected_rows
	}

	/// The retained-cap guard on a planned allocation: refused before the bytes are obtained.
	fn guard_planned_resident_bytes(
		&self,
		planned: u64,
		memory_cap: u64,
		site: SpillResidentGuardSite,
	) -> Result<(), ExecutionError> {
		let state = self.resident_state();
		let resident = state.total().saturating_add(planned);
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		if resident > cap {
			return Err(budget_exhausted_with_detail(
				BudgetDimension::Memory,
				"streamed inner join build spill resident state",
				resident,
				cap,
				state.detail(site),
			));
		}
		Ok(())
	}

	fn prepare_coalesced_index(
		&mut self,
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		index_mode: CoalescedPageIndexMode,
	) -> Result<(), ExecutionError> {
		if let Some(latched) = self.coalesced_index_mode {
			if latched != index_mode {
				return Err(ExecutionError::Kernel {
					message: format!(
						"coalesced spill writer changed index mode mid-build: {latched:?} -> {index_mode:?}"
					),
				});
			}
			return Ok(());
		}
		if !self.pages.is_empty() || self.rows != 0 {
			return Err(ExecutionError::Kernel {
				message: "coalesced spill index mode was not selected before the first page"
					.to_string(),
			});
		}
		if index_mode == CoalescedPageIndexMode::IndexAtFlush
			&& !null_safe_keys
			&& matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64)
		{
			self.use_compact_resident_index();
		}
		self.coalesced_index_mode = Some(index_mode);
		Ok(())
	}

	/// Disable caching for a resident single-touch drain. The streamed probe path
	/// deliberately leaves the cache enabled because it may revisit pages.
	pub(in crate::exec) fn disable_page_cache(&mut self) {
		self.cache_cap = 0;
		self.cache.clear();
		self.lru.clear();
		self.cache_bytes = 0;
	}

	#[cfg(test)]
	pub(in crate::exec) fn enable_compact_resident_index_rescue(&mut self) {
		self.compact_index_rescue_enabled = true;
	}

	/// `Ok(true)` when the legacy index was compacted here; `Ok(false)` when nothing applied.
	fn try_compact_near_cap_resident_index(
		&mut self,
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		memory_cap: u64,
	) -> Result<bool, ExecutionError> {
		if !self.compact_index_rescue_enabled {
			return Ok(false);
		}
		if null_safe_keys || !matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64) {
			return Ok(false);
		}
		if self.compact_index.is_some() {
			return Ok(false);
		}
		if self.input_rows > COMPACT_SPILL_INDEX_MAX_INPUT_ROWS {
			return Ok(false);
		}
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		let resident = self.resident_state().total();
		if resident <= cap
			|| resident > cap.saturating_add(COMPACT_SPILL_INDEX_MAX_RETAINED_CAP_OVERSHOOT_BYTES)
		{
			return Ok(false);
		}
		let index = std::mem::take(&mut self.index);
		let mut compact = SpilledBatchIndex::for_key_plan(key_plan, null_safe_keys);
		for (key, refs) in index {
			for row_ref in refs {
				compact.insert(key.clone(), row_ref)?;
			}
		}
		self.compact_index = Some(compact);
		self.compact_index_enabled = true;
		self.resident_index_bytes = self
			.compact_index
			.as_ref()
			.map_or(0, SpilledBatchIndex::accounted_bytes);
		Ok(true)
	}

	pub(in crate::exec) async fn write_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		self.write_batch_chunks_ledgered(
			batch,
			key_plan,
			build_keys,
			null_safe_keys,
			task_budget,
			None,
		)
		.await
	}

	/// ADR 596: the whole-row chunk loop with an optional ledger; `None` is `write_batch_chunks`.
	pub(in crate::exec) async fn write_batch_chunks_ledgered(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
		mut ledger: Option<&mut BuildWriteLedger>,
	) -> Result<(), ExecutionError> {
		self.refuse_write_after_finalization()?;
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		if let Some(ledger) = ledger.as_deref_mut() {
			ledger.batches_written = ledger.batches_written.saturating_add(1);
			ledger.rows_kept = ledger.rows_kept.saturating_add(batch.num_rows() as u64);
		}
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			self.write_page_batch_inner(
				&chunk,
				key_plan,
				build_keys,
				null_safe_keys,
				false,
				true,
				task_budget,
				None,
				ledger.as_deref_mut(),
			)
			.await?;
		}
		Ok(())
	}

	pub(in crate::exec) async fn write_batch_chunks_unindexed(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		self.write_batch_chunks_unindexed_profiled(
			batch,
			key_plan,
			build_keys,
			null_safe_keys,
			task_budget,
			None,
		)
		.await?;
		// This direct API has historically sealed each call. Keep that contract for
		// non-reservoir callers and unit tests; the streaming reservoir owns the
		// cross-batch flush barrier through the profiled path below.
		self.flush_unindexed_page_buffer(key_plan, build_keys, null_safe_keys, task_budget, None)
			.await
	}

	pub(in crate::exec) async fn write_batch_chunks_unindexed_profiled(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<(), ExecutionError> {
		self.write_batch_chunks_coalesced_profiled(
			batch,
			key_plan,
			build_keys,
			null_safe_keys,
			CoalescedPageIndexMode::RebuildAfterWrite,
			task_budget,
			observer,
		)
		.await
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn write_batch_chunks_coalesced_profiled(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		index_mode: CoalescedPageIndexMode,
		task_budget: TaskBudget,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<(), ExecutionError> {
		self.refuse_write_after_finalization()?;
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.prepare_coalesced_index(key_plan, null_safe_keys, index_mode)?;
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			self.buffer_unindexed_page_batch(
				&chunk,
				key_plan,
				build_keys,
				null_safe_keys,
				index_mode,
				task_budget,
				observer,
			)
			.await?;
		}
		Ok(())
	}

	pub(in crate::exec) async fn write_preserved_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		self.refuse_write_after_finalization()?;
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			self.write_page_batch(
				&chunk,
				key_plan,
				build_keys,
				null_safe_keys,
				true,
				task_budget,
			)
			.await?;
		}
		Ok(())
	}

	pub(in crate::exec) async fn write_projected_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		retained_columns: &[ColumnOrdinal],
		key_plan: JoinKeyPlan,
		projected_build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		self.write_projected_batch_chunks_ledgered(
			batch,
			retained_columns,
			key_plan,
			projected_build_keys,
			null_safe_keys,
			task_budget,
			None,
		)
		.await
	}

	/// ADR 596: the projected chunk loop with an optional ledger; `None` is
	/// `write_projected_batch_chunks`. The projection is its own span per chunk.
	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn write_projected_batch_chunks_ledgered(
		&mut self,
		batch: &RecordBatch,
		retained_columns: &[ColumnOrdinal],
		key_plan: JoinKeyPlan,
		projected_build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
		mut ledger: Option<&mut BuildWriteLedger>,
	) -> Result<(), ExecutionError> {
		self.refuse_write_after_finalization()?;
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		if let Some(ledger) = ledger.as_deref_mut() {
			ledger.batches_written = ledger.batches_written.saturating_add(1);
			ledger.rows_kept = ledger.rows_kept.saturating_add(batch.num_rows() as u64);
		}
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			let project_started = BuildWriteLedger::begin(&ledger);
			let projected = project_batch_columns(&chunk, retained_columns)?;
			BuildWriteLedger::finish(&mut ledger, BuildWriteChild::Project, project_started);
			self.write_page_batch_inner(
				&projected,
				key_plan,
				projected_build_keys,
				null_safe_keys,
				false,
				true,
				task_budget,
				None,
				ledger.as_deref_mut(),
			)
			.await?;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn write_projected_batch_chunks_coalesced_profiled(
		&mut self,
		batch: &RecordBatch,
		retained_columns: &[ColumnOrdinal],
		key_plan: JoinKeyPlan,
		projected_build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		index_mode: CoalescedPageIndexMode,
		task_budget: TaskBudget,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<(), ExecutionError> {
		self.refuse_write_after_finalization()?;
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.prepare_coalesced_index(key_plan, null_safe_keys, index_mode)?;
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			let projected = project_batch_columns(&chunk, retained_columns)?;
			self.buffer_unindexed_page_batch(
				&projected,
				key_plan,
				projected_build_keys,
				null_safe_keys,
				index_mode,
				task_budget,
				observer,
			)
			.await?;
		}
		Ok(())
	}

	/// Buffer the coalesced reservoir writer up to the existing output chunk cap.
	///
	/// The grouped reservoir rebuilds its index from finished pages, so it is safe to
	/// compose fragments here.  Indexed writers deliberately stay on the direct path:
	/// they expose page indices as they write.
	#[allow(clippy::too_many_arguments)]
	async fn buffer_unindexed_page_batch(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		index_mode: CoalescedPageIndexMode,
		task_budget: TaskBudget,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0
			|| !batch_has_spillable_join_key(batch, key_plan, build_keys, null_safe_keys)?
		{
			return Ok(());
		}
		if self.pending_unindexed_rows > 0
			&& self.pending_unindexed_rows.saturating_add(batch.num_rows())
				> CROSS_JOIN_OUTPUT_CHUNK_ROWS
		{
			self.flush_coalesced_page_buffer(
				key_plan,
				build_keys,
				null_safe_keys,
				index_mode,
				task_budget,
				observer,
			)
			.await?;
		}
		if self.pending_unindexed_rows == 0 && batch.num_rows() >= CROSS_JOIN_OUTPUT_CHUNK_ROWS {
			return self
				.write_page_batch_inner(
					batch,
					key_plan,
					build_keys,
					null_safe_keys,
					false,
					index_mode.indexes_at_flush(),
					task_budget,
					observer,
					None,
				)
				.await;
		}

		self.pending_unindexed_rows = self.pending_unindexed_rows.saturating_add(batch.num_rows());
		self.pending_unindexed_bytes = self
			.pending_unindexed_bytes
			.saturating_add(record_batch_memory_bytes(batch));
		self.pending_unindexed_batches.push(batch.clone());
		// This is deliberately a high-water value: the retained-footprint guard
		// must see the live buffer, and keeping its peak preserves the existing
		// preflight accounting after a flush.
		self.writer_peak = self.writer_peak.max(self.pending_unindexed_bytes);
		self.guard_resident_state(task_budget.memory.get(), SpillResidentGuardSite::Write)?;

		if self.pending_unindexed_rows >= CROSS_JOIN_OUTPUT_CHUNK_ROWS {
			self.flush_coalesced_page_buffer(
				key_plan,
				build_keys,
				null_safe_keys,
				index_mode,
				task_budget,
				observer,
			)
			.await?;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn flush_unindexed_page_buffer(
		&mut self,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<(), ExecutionError> {
		self.flush_coalesced_page_buffer(
			key_plan,
			build_keys,
			null_safe_keys,
			CoalescedPageIndexMode::RebuildAfterWrite,
			task_budget,
			observer,
		)
		.await
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn flush_coalesced_page_buffer(
		&mut self,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		index_mode: CoalescedPageIndexMode,
		task_budget: TaskBudget,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<(), ExecutionError> {
		if self.pending_unindexed_batches.is_empty() {
			return Ok(());
		}
		let pending = std::mem::take(&mut self.pending_unindexed_batches);
		self.pending_unindexed_rows = 0;
		self.pending_unindexed_bytes = 0;
		let batch = if pending.len() == 1 {
			pending.into_iter().next().expect("one pending spill batch")
		} else {
			arrow::compute::concat_batches(&pending[0].schema(), pending.iter())?
		};
		self.write_page_batch_inner(
			&batch,
			key_plan,
			build_keys,
			null_safe_keys,
			false,
			index_mode.indexes_at_flush(),
			task_budget,
			observer,
			None,
		)
		.await
	}

	pub(in crate::exec) async fn write_projected_preserved_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		retained_columns: &[ColumnOrdinal],
		key_plan: JoinKeyPlan,
		projected_build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			let projected = project_batch_columns(&chunk, retained_columns)?;
			self.write_page_batch(
				&projected,
				key_plan,
				projected_build_keys,
				null_safe_keys,
				true,
				task_budget,
			)
			.await?;
		}
		Ok(())
	}

	pub(in crate::exec) async fn write_page_batch(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		retain_unkeyed_rows: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		self.write_page_batch_inner(
			batch,
			key_plan,
			build_keys,
			null_safe_keys,
			retain_unkeyed_rows,
			true,
			task_budget,
			None,
			None,
		)
		.await
	}

	#[allow(clippy::too_many_arguments)]
	async fn write_page_batch_inner(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		retain_unkeyed_rows: bool,
		index_rows: bool,
		task_budget: TaskBudget,
		observer: Option<&GroupedChainSpillObserver<'_>>,
		mut ledger: Option<&mut BuildWriteLedger>,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		// ADR 596: one chunk; key extraction is counted for every chunk, the page children only
		// for a chunk that yields a keyed row.
		if let Some(ledger) = ledger.as_deref_mut() {
			ledger.chunks = ledger.chunks.saturating_add(1);
		}
		let key_started = BuildWriteLedger::begin(&ledger);
		let key_timer = observer
			.map(|observer| {
				observer.begin_node_named(
					GroupedChainSubLeaf::WritePartitionRouting,
					"write_page_key_extraction",
				)
			})
			.transpose()?;
		let key_arrays = key_arrays(batch, build_keys)?;
		let mut refs = std::mem::take(&mut self.scratch_refs);
		refs.clear();
		refs.reserve(batch.num_rows());
		for row in 0..batch.num_rows() {
			let Some(key) = join_key_at(key_plan, &key_arrays, row, null_safe_keys)? else {
				continue;
			};
			refs.push((key, join_build_row_index(row)?));
		}
		let keyed_rows = refs.len() as u64;
		BuildWriteLedger::finish(&mut ledger, BuildWriteChild::KeyExtract, key_started);
		if refs.is_empty() && !retain_unkeyed_rows {
			if let Some(timer) = key_timer {
				timer.finish_with(batch.num_rows() as u64, 0, 0)?;
			}
			if let Some(ledger) = ledger.as_deref_mut() {
				ledger.chunks_unkeyed = ledger.chunks_unkeyed.saturating_add(1);
			}
			self.scratch_refs = refs;
			return Ok(());
		}
		if let Some(timer) = key_timer {
			timer.finish_with(batch.num_rows() as u64, 0, 0)?;
		}

		let page_index = self.pages.len();
		self.scratch_plaintext.clear();
		let encode_timer = observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::WriteIpcEncode))
			.transpose()?;
		let encode_started = BuildWriteLedger::begin(&ledger);
		encode_record_batch_ipc_into(&mut self.scratch_plaintext, batch)?;
		BuildWriteLedger::finish(&mut ledger, BuildWriteChild::Encode, encode_started);
		if let Some(timer) = encode_timer {
			timer.finish_with(
				batch.num_rows() as u64,
				self.scratch_plaintext.len() as u64,
				1,
			)?;
		}
		let plaintext = std::mem::take(&mut self.scratch_plaintext);
		let page_started = BuildWriteLedger::begin(&ledger);
		let (page, mut plaintext) = write_join_spill_plaintext_page_returning_buffer(
			self.file_ops.as_ref(),
			&self.file,
			&self.key,
			self.spill_id,
			page_index,
			self.next_offset,
			plaintext,
			observer,
		)
		.await?;
		BuildWriteLedger::finish(&mut ledger, BuildWriteChild::PageWrite, page_started);
		if let Some(ledger) = ledger.as_deref_mut() {
			ledger.pages = ledger.pages.saturating_add(1);
			ledger.plaintext_bytes = ledger
				.plaintext_bytes
				.saturating_add(page.plaintext_len as u64);
			ledger.ciphertext_bytes = ledger
				.ciphertext_bytes
				.saturating_add(page.ciphertext_len as u64);
		}
		plaintext.clear();
		self.scratch_plaintext = plaintext;
		self.spill_bytes = self.spill_bytes.saturating_add(page.ciphertext_len as u64);
		self.writer_peak = self.writer_peak.max(page.plaintext_len as u64);
		if self.spill_bytes > task_budget.spill_bytes.get() {
			refs.clear();
			self.scratch_refs = refs;
			return Err(budget_exhausted(
				BudgetDimension::SpillBytes,
				"streamed inner join build spill writer",
				self.spill_bytes,
				task_budget.spill_bytes.get(),
			));
		}
		let index_started = BuildWriteLedger::begin(&ledger);
		let mut compacted = false;
		if index_rows {
			if self.rows == 0 {
				if self.compact_index_enabled {
					self.index.clear();
					// ADR 601: a finish-built index (collecting, or finalized — though the writer's
					// boundary refuses before this point) is kept; the incremental one is
					// constructed here, on the first page.
					if !self.compact_index.as_ref().is_some_and(|index| {
						index.is_collecting() || index.finish_stats().is_some()
					}) {
						self.compact_index =
							Some(SpilledBatchIndex::for_key_plan(key_plan, null_safe_keys));
					}
				} else {
					self.index.clear();
					self.compact_index = None;
				}
			}
			for (key, row) in refs.drain(..) {
				#[cfg(test)]
				{
					self.candidate_index_insert_attempts = self
						.candidate_index_insert_attempts
						.checked_add(1)
						.expect("test candidate insertion count");
					if self.fail_candidate_index_insert_at
						== Some(self.candidate_index_insert_attempts)
					{
						return Err(ExecutionError::Kernel {
							message: "injected coalesced index insertion failure".to_string(),
						});
					}
				}
				let row_ref = SpilledBatchRowRef { page_index, row };
				if let Some(index) = &mut self.compact_index {
					index.insert(key, row_ref)?;
				} else {
					#[cfg(test)]
					{
						self.legacy_index_insert_count =
							self.legacy_index_insert_count.saturating_add(1);
					}
					append_spilled_batch_row_ref(
						&mut self.index,
						&mut self.resident_index_bytes,
						key,
						row_ref,
					);
				}
				self.rows = self.rows.saturating_add(1);
			}
			if let Some(index) = &self.compact_index {
				self.resident_index_bytes = index.accounted_bytes();
			}
			compacted = self.try_compact_near_cap_resident_index(
				key_plan,
				null_safe_keys,
				task_budget.memory.get(),
			)?;
		} else {
			self.rows = self
				.rows
				.saturating_add(u64::try_from(refs.len()).unwrap_or(u64::MAX));
			refs.clear();
		}
		refs.clear();
		self.scratch_refs = refs;
		self.next_offset = self.next_offset.saturating_add(page.ciphertext_len as u64);
		self.page_rows.push(join_build_row_index(batch.num_rows())?);
		self.pages.push(page);
		if index_rows {
			self.indexed_flush_pages =
				self.indexed_flush_pages
					.checked_add(1)
					.ok_or_else(|| ExecutionError::Kernel {
						message: "coalesced indexed-flush page count overflowed".to_string(),
					})?;
		}
		// ADR 597: the transient peaks as the guard is about to read them — the capacities the
		// scratch obtained, the plaintext high-water mark, the resident total — before the guard
		// may release the scratch; a release is counted.
		if let Some(ledger) = ledger.as_deref_mut() {
			let refs_capacity = self.scratch_refs.capacity();
			let plaintext_capacity = self.scratch_plaintext.capacity();
			let resident_total = self.resident_state().total();
			ledger.observe_peaks(
				plaintext_capacity,
				refs_capacity,
				self.writer_peak,
				resident_total,
			);
		}
		let releases_before = self.write_scratch_releases;
		self.guard_resident_state_after_write(task_budget.memory.get())?;
		if index_rows {
			self.indexed_flush_guard_checks = self
				.indexed_flush_guard_checks
				.checked_add(1)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "coalesced indexed-flush guard count overflowed".to_string(),
				})?;
		}
		BuildWriteLedger::finish(&mut ledger, BuildWriteChild::Index, index_started);
		if let Some(ledger) = ledger {
			ledger.scratch_releases = ledger
				.scratch_releases
				.saturating_add(self.write_scratch_releases.saturating_sub(releases_before));
			if index_rows {
				ledger.index_entries = ledger.index_entries.saturating_add(keyed_rows);
			}
			// The mode the index is in after this page; a near-cap compaction is counted; the
			// variant is the build's own answer.
			ledger.index_mode = Some(if self.compact_index.is_some() {
				"compact"
			} else {
				"legacy"
			});
			ledger.index_variant = Some(self.index_variant_name());
			ledger.index_build = Some(self.index_build_name());
			ledger.index_capacity_growths = self
				.compact_index
				.as_ref()
				.map_or(0, SpilledBatchIndex::capacity_growths);
			if compacted {
				ledger.index_compactions = ledger.index_compactions.saturating_add(1);
			}
		}
		Ok(())
	}

	pub(in crate::exec) async fn rebuild_index_from_pages(
		&mut self,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		self.rebuild_index_from_pages_profiled(
			key_plan,
			build_keys,
			null_safe_keys,
			task_budget,
			None,
		)
		.await
	}

	pub(in crate::exec) async fn rebuild_index_from_pages_profiled(
		&mut self,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<(), ExecutionError> {
		self.index_rebuild_calls =
			self.index_rebuild_calls
				.checked_add(1)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "spill index rebuild call count overflowed".to_string(),
				})?;
		self.index.clear();
		self.compact_index = if self.compact_index_enabled {
			Some(SpilledBatchIndex::for_key_plan(key_plan, null_safe_keys))
		} else {
			None
		};
		self.resident_index_bytes = 0;
		self.rows = 0;
		for page_index in 0..self.pages.len() {
			let batch = self.read_batch_page_profiled(page_index, observer).await?;
			let cpu_timer = observer
				.map(|observer| observer.begin(GroupedChainSubLeaf::ReadIndexRebuildCpu))
				.transpose()?;
			let key_arrays = key_arrays(&batch, build_keys)?;
			for row in 0..batch.num_rows() {
				let Some(key) = join_key_at(key_plan, &key_arrays, row, null_safe_keys)? else {
					continue;
				};
				let row_ref = SpilledBatchRowRef {
					page_index,
					row: join_build_row_index(row)?,
				};
				if let Some(index) = &mut self.compact_index {
					index.insert(key, row_ref)?;
				} else {
					#[cfg(test)]
					{
						self.legacy_index_insert_count =
							self.legacy_index_insert_count.saturating_add(1);
					}
					append_spilled_batch_row_ref(
						&mut self.index,
						&mut self.resident_index_bytes,
						key,
						row_ref,
					);
				}
				self.rows = self.rows.saturating_add(1);
			}
			if let Some(index) = &self.compact_index {
				self.resident_index_bytes = index.accounted_bytes();
			}
			self.guard_resident_state(
				task_budget.memory.get(),
				SpillResidentGuardSite::RebuildIndex,
			)?;
			if let Some(timer) = cpu_timer {
				timer.finish_with(batch.num_rows() as u64, self.resident_index_bytes, 1)?;
			}
		}
		Ok(())
	}

	pub(in crate::exec) async fn rebuild_compact_index_and_pin_pages(
		&mut self,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
		resident_baseline_bytes: u64,
	) -> Result<PinnedSpilledInnerBuildStats, ExecutionError> {
		self.resident_baseline_bytes = resident_baseline_bytes;
		self.index.clear();
		self.compact_index = Some(SpilledBatchIndex::for_key_plan(key_plan, null_safe_keys));
		self.compact_index_enabled = true;
		self.resident_index_bytes = 0;
		self.rows = 0;
		self.cache.clear();
		self.lru.clear();
		self.cache_bytes = 0;
		self.pinned_decoded_bytes = 0;
		self.pinned_decoded_pages = Some(Vec::with_capacity(self.pages.len()));
		self.pin_decoded_outcome = Some(PinDecodedPagesOutcome::Admitted);

		let mut page_reads = 0u64;
		for page_index in 0..self.pages.len() {
			let batch = self.read_batch_page(page_index).await?;
			page_reads = page_reads
				.checked_add(1)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "streamed inner join pinned page read count overflow".to_string(),
				})?;
			if let Some(cached) = self.cache.remove(&page_index) {
				self.cache_bytes = self.cache_bytes.saturating_sub(cached.total_bytes());
			}
			self.lru.retain(|cached| *cached != page_index);
			let batch_bytes = record_batch_memory_bytes(&batch);
			let key_arrays = key_arrays(&batch, build_keys)?;
			for row in 0..batch.num_rows() {
				let Some(key) = join_key_at(key_plan, &key_arrays, row, null_safe_keys)? else {
					continue;
				};
				let row_ref = SpilledBatchRowRef {
					page_index,
					row: join_build_row_index(row)?,
				};
				self.compact_index
					.as_mut()
					.expect("combined rebuild initialized compact index")
					.insert(key, row_ref)?;
				self.rows = self.rows.saturating_add(1);
			}
			self.resident_index_bytes = self
				.compact_index
				.as_ref()
				.map_or(0, SpilledBatchIndex::accounted_bytes);
			self.pinned_decoded_bytes = self.pinned_decoded_bytes.saturating_add(batch_bytes);
			self.pinned_decoded_pages
				.as_mut()
				.expect("combined rebuild initialized pinned pages")
				.push(batch);
			self.guard_resident_state(
				task_budget.memory.get(),
				SpillResidentGuardSite::PinDecodedPages,
			)?;
		}
		self.cache.clear();
		self.lru.clear();
		self.cache_bytes = 0;
		Ok(PinnedSpilledInnerBuildStats {
			page_reads,
			decoded_bytes: self.pinned_decoded_bytes,
			index_bytes: self.resident_index_bytes,
			rows: self.rows,
		})
	}

	pub(in crate::exec) async fn read_all_batch_pages(
		&mut self,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		self.read_all_batch_pages_profiled(None).await
	}

	pub(in crate::exec) async fn read_all_batch_pages_profiled(
		&mut self,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut pages = Vec::with_capacity(self.pages.len());
		for page_index in 0..self.pages.len() {
			let page = self.read_batch_page_profiled(page_index, observer).await?;
			let handoff_timer = observer
				.map(|observer| observer.begin(GroupedChainSubLeaf::ReadMaterialization))
				.transpose()?;
			pages.push(page);
			if let Some(timer) = handoff_timer {
				timer.finish_with(0, 0, 1)?;
			}
		}
		Ok(pages)
	}

	pub(in crate::exec) async fn try_pin_decoded_pages(
		&mut self,
		memory_cap: u64,
	) -> Result<PinDecodedPagesOutcome, ExecutionError> {
		self.try_pin_decoded_pages_profiled(memory_cap, None).await
	}

	pub(in crate::exec) async fn try_pin_decoded_pages_profiled(
		&mut self,
		memory_cap: u64,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<PinDecodedPagesOutcome, ExecutionError> {
		if self.pinned_decoded_pages.is_some() {
			let outcome = PinDecodedPagesOutcome::Admitted;
			record_grouped_chain_pin_outcome(observer, outcome)?;
			return Ok(outcome);
		}
		if let Some(outcome) = self.pin_decoded_outcome {
			record_grouped_chain_pin_outcome(observer, outcome)?;
			return Ok(outcome);
		}
		if let Some(profile) = &mut self.throughput_profile {
			profile.set_operation_phase(ThroughputOperationPhase::Preload);
		}
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		let loaded_page_estimate = self.loaded_page_bytes_estimate();
		let estimated_resident = self
			.resident_baseline_bytes
			.saturating_add(self.resident_index_bytes)
			.saturating_add(self.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64)
			.saturating_add(loaded_page_estimate);
		if estimated_resident > cap {
			let outcome = PinDecodedPagesOutcome::RejectedPreflight;
			self.record_pin_decoded_pages_outcome(
				memory_cap,
				cap,
				loaded_page_estimate,
				estimated_resident,
				0,
				0,
				0,
				outcome,
			);
			record_grouped_chain_pin_outcome(observer, outcome)?;
			return Ok(outcome);
		}

		let mut pages = Vec::with_capacity(self.pages.len());
		let mut decoded_bytes = 0u64;
		let mut pages_attempted = 0u64;
		let mut pages_completed = 0u64;
		for page_index in 0..self.pages.len() {
			pages_attempted =
				pages_attempted
					.checked_add(1)
					.ok_or_else(|| ExecutionError::Kernel {
						message: "streamed inner join pin attempted-page count overflow"
							.to_string(),
					})?;
			let batch = self.read_batch_page_profiled(page_index, observer).await?;
			pages_completed =
				pages_completed
					.checked_add(1)
					.ok_or_else(|| ExecutionError::Kernel {
						message: "streamed inner join pin completed-page count overflow"
							.to_string(),
					})?;
			decoded_bytes = decoded_bytes.saturating_add(record_batch_memory_bytes(&batch));
			let resident = self
				.resident_baseline_bytes
				.saturating_add(self.resident_index_bytes)
				.saturating_add(self.writer_peak)
				.saturating_add(JOIN_SPILL_PAGE_SIZE as u64)
				.saturating_add(decoded_bytes);
			if resident > cap {
				let outcome = PinDecodedPagesOutcome::RejectedMidDecode;
				self.record_pin_decoded_pages_outcome(
					memory_cap,
					cap,
					loaded_page_estimate,
					estimated_resident,
					decoded_bytes,
					pages_attempted,
					pages_completed,
					outcome,
				);
				record_grouped_chain_pin_outcome(observer, outcome)?;
				return Ok(outcome);
			}
			pages.push(batch);
		}
		self.cache.clear();
		self.lru.clear();
		self.cache_bytes = 0;
		self.pinned_decoded_bytes = decoded_bytes;
		self.pinned_decoded_pages = Some(pages);
		self.guard_resident_state(memory_cap, SpillResidentGuardSite::PinDecodedPages)?;
		let outcome = PinDecodedPagesOutcome::Admitted;
		self.record_pin_decoded_pages_outcome(
			memory_cap,
			cap,
			loaded_page_estimate,
			estimated_resident,
			decoded_bytes,
			pages_attempted,
			pages_completed,
			outcome,
		);
		record_grouped_chain_pin_outcome(observer, outcome)?;
		Ok(outcome)
	}

	#[allow(clippy::too_many_arguments)]
	fn record_pin_decoded_pages_outcome(
		&mut self,
		memory_cap: u64,
		retained_cap: u64,
		loaded_page_estimate: u64,
		preflight_estimate: u64,
		decoded_bytes: u64,
		pages_attempted: u64,
		pages_completed: u64,
		outcome: PinDecodedPagesOutcome,
	) {
		self.pin_decoded_outcome = Some(outcome);
		if let Some(profile) = &mut self.throughput_profile {
			profile.record_pin_receipt(
				memory_cap,
				retained_cap,
				self.resident_baseline_bytes,
				self.resident_index_bytes,
				self.writer_peak,
				JOIN_SPILL_PAGE_SIZE as u64,
				loaded_page_estimate,
				preflight_estimate,
				decoded_bytes,
				pages_attempted,
				pages_completed,
				outcome,
			);
			profile.set_operation_phase(ThroughputOperationPhase::Probe);
		}
	}

	pub(in crate::exec) fn release_write_scratch(&mut self) {
		self.scratch_plaintext = Vec::new();
		self.scratch_refs = Vec::new();
		self.writer_peak = 0;
		self.write_scratch_releases = self.write_scratch_releases.saturating_add(1);
	}

	/// Proves that every keyed row accepted by the coalesced writer is represented in the resident
	/// index before the rebuild pass is skipped.
	pub(in crate::exec) fn assert_coalesced_index_complete(&self) -> Result<(), ExecutionError> {
		if !self.pending_unindexed_batches.is_empty() {
			return Err(ExecutionError::Kernel {
				message: "coalesced index completeness checked before the partial page was flushed"
					.to_string(),
			});
		}
		let indexed_rows = self.compact_index.as_ref().map_or_else(
			|| {
				self.index
					.values()
					.map(|refs| u64::try_from(refs.len()).unwrap_or(u64::MAX))
					.fold(0u64, u64::saturating_add)
			},
			SpilledBatchIndex::row_ref_count,
		);
		if indexed_rows != self.rows {
			return Err(ExecutionError::Kernel {
				message: format!(
					"coalesced resident spill index is incomplete: indexed_rows={indexed_rows}, expected_rows={}",
					self.rows
				),
			});
		}
		Ok(())
	}

	pub(in crate::exec) async fn complete_coalesced_index_after_finish(
		&mut self,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		index_mode: CoalescedPageIndexMode,
		task_budget: TaskBudget,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<(), ExecutionError> {
		self.release_write_scratch();
		if self.input_rows == 0 {
			return Ok(());
		}
		if self.coalesced_index_mode != Some(index_mode) {
			return Err(ExecutionError::Kernel {
				message: format!(
					"coalesced spill completion mode disagreed with the writer: writer={:?}, completion={index_mode:?}",
					self.coalesced_index_mode
				),
			});
		}
		match index_mode {
			CoalescedPageIndexMode::IndexAtFlush => self.assert_coalesced_index_complete(),
			CoalescedPageIndexMode::RebuildAfterWrite => {
				if !null_safe_keys && matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64) {
					self.use_compact_resident_index();
				}
				self.rebuild_index_from_pages_profiled(
					key_plan,
					build_keys,
					null_safe_keys,
					task_budget,
					observer,
				)
				.await
			}
		}
	}

	/// Seal one reservoir partition: flush the partial page the buffering writer holds back, then
	/// finish and drop the write scratch.
	///
	/// Named as one operation because the flush is a caller obligation, not something `finish` may
	/// do for itself — `finish` stays fail-loud so a barrier that goes missing is a hard error
	/// rather than a silently truncated spill file. Every reservoir that writes through
	/// `write_*_unindexed_profiled` seals through here.
	pub(in crate::exec) async fn seal_unindexed_reservoir_partition(
		&mut self,
		key_plan: JoinKeyPlan,
		keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<(), ExecutionError> {
		self.flush_unindexed_page_buffer(key_plan, keys, null_safe_keys, task_budget, observer)
			.await?;
		self.finish(task_budget).await?;
		self.release_write_scratch();
		Ok(())
	}

	pub(in crate::exec) async fn finish(
		&self,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if !self.pending_unindexed_batches.is_empty() {
			return Err(ExecutionError::Kernel {
				message: "inner join spill finish reached with an unflushed unindexed page buffer"
					.to_string(),
			});
		}
		if self
			.compact_index
			.as_ref()
			.is_some_and(SpilledBatchIndex::is_collecting)
		{
			return Err(ExecutionError::Kernel {
				message:
					"inner join spill finish reached with a finish-built index still collecting"
						.to_string(),
			});
		}
		self.file_ops
			.set_len(&self.file, self.next_offset)
			.await
			.map_err(|error| join_spill_io_error("set_len", error))?;
		self.file_ops
			.fdatasync(&self.file)
			.await
			.map_err(|error| join_spill_io_error("fdatasync", error))?;
		self.guard_resident_state(task_budget.memory.get(), SpillResidentGuardSite::Finish)
	}

	pub(in crate::exec) fn guard_resident_state(
		&self,
		memory_cap: u64,
		site: SpillResidentGuardSite,
	) -> Result<(), ExecutionError> {
		let state = self.resident_state();
		let resident = state.total();
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		if resident > cap {
			return Err(budget_exhausted_with_detail(
				BudgetDimension::Memory,
				"streamed inner join build spill resident state",
				resident,
				cap,
				state.detail(site),
			));
		}
		Ok(())
	}

	fn guard_resident_state_after_write(&mut self, memory_cap: u64) -> Result<(), ExecutionError> {
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		if self.resident_state().total() > cap && self.writer_peak > 0 {
			self.release_write_scratch();
		}
		self.guard_resident_state(memory_cap, SpillResidentGuardSite::Write)
	}

	/// ADR 600: the index's variant — the compact wrapper's choice, or `hash` for the legacy map.
	pub(in crate::exec) fn index_variant_name(&self) -> &'static str {
		self.compact_index
			.as_ref()
			.map_or("hash", SpilledBatchIndex::variant_name)
	}

	/// ADR 600: count lookups on this build — hits and misses — for the profiled arm only; the
	/// counts are published by the chain's cleanup, after the whole drive.
	pub(in crate::exec) fn enable_lookup_counting(&mut self) {
		self.count_lookups = true;
	}

	/// `None` when counting was never enabled; else `(hits, misses)` so far.
	pub(in crate::exec) fn lookup_counts(&self) -> Option<(u64, u64)> {
		self.count_lookups
			.then_some((self.lookup_hits, self.lookup_misses))
	}

	/// ADR 597: the resident total the guards read, for the ledger's peak after `finish`.
	pub(in crate::exec) fn resident_state_total(&self) -> u64 {
		self.resident_state().total()
	}

	/// ADR 597: the writer's scratch capacities as obtained, for the ledger after `finish`.
	pub(in crate::exec) fn write_scratch_capacities(&self) -> (usize, usize) {
		(
			self.scratch_plaintext.capacity(),
			self.scratch_refs.capacity(),
		)
	}

	#[cfg(test)]
	pub(in crate::exec) fn spill_path(&self) -> &std::path::Path {
		&self.path
	}

	/// ADR 600: the compact index's obtained bucket length, entry capacity, overflow heap
	/// bytes, and accounted bytes, for the accounting fixture; `None` on the legacy index.
	#[cfg(test)]
	pub(in crate::exec) fn compact_index_accounting_parts(
		&self,
	) -> Option<(usize, usize, u64, u64)> {
		match self.compact_index.as_ref()? {
			SpilledBatchIndex::Int64(index) => Some((
				index.buckets.len(),
				index.entries.capacity(),
				index.overflow_heap_bytes(),
				index.accounted_bytes(),
			)),
			SpilledBatchIndex::Int32(index) => Some((
				index.buckets.len(),
				index.entries.capacity(),
				index.overflow_heap_bytes(),
				index.accounted_bytes(),
			)),
			SpilledBatchIndex::Hash(_) => None,
		}
	}

	/// ADR 601: the compact index's bytes recomputed from its parts, for the accounting fixtures.
	#[cfg(test)]
	pub(in crate::exec) fn compact_index_computed_bytes(&self) -> Option<u64> {
		self.compact_index
			.as_ref()
			.map(SpilledBatchIndex::computed_bytes)
	}

	#[cfg(test)]
	pub(in crate::exec) fn compact_int64_entry_size() -> usize {
		std::mem::size_of::<PrimitiveSpilledBatchIndexEntry<i64>>()
	}

	fn resident_state(&self) -> SpillResidentState {
		SpillResidentState {
			resident_baseline_bytes: self.resident_baseline_bytes,
			resident_index_bytes: self.resident_index_bytes,
			writer_peak_bytes: self.writer_peak,
			page_reserve_bytes: JOIN_SPILL_PAGE_SIZE as u64,
			pinned_decoded_bytes: self.pinned_decoded_bytes,
		}
	}

	pub(in crate::exec) async fn lookup(
		&mut self,
		join_key: &JoinKey,
	) -> Result<SpilledLookupMatches, ExecutionError> {
		let matches = if let Some(index) = &self.compact_index {
			if index.is_collecting() {
				return Err(ExecutionError::Kernel {
					message: "lookup on a finish-built spill index that is still collecting"
						.to_string(),
				});
			}
			index.lookup(join_key, self.lookup_form)
		} else {
			SpilledLookupMatches::from_vec(self.index.get(join_key).cloned().unwrap_or_default())
		};
		if let Some(profile) = &mut self.throughput_profile {
			profile.record_lookup(matches.len());
		}
		if self.count_lookups {
			match &matches {
				SpilledLookupMatches::Miss => {
					self.lookup_misses = self.lookup_misses.saturating_add(1);
				}
				SpilledLookupMatches::One(_) => {
					self.lookup_hits = self.lookup_hits.saturating_add(1);
					self.lookup_inline_hits = self.lookup_inline_hits.saturating_add(1);
				}
				SpilledLookupMatches::Many(_) => {
					self.lookup_hits = self.lookup_hits.saturating_add(1);
				}
			}
		}
		Ok(matches)
	}

	pub(in crate::exec) fn page_count(&self) -> usize {
		self.pages.len()
	}

	pub(in crate::exec) fn record_probe_chunk(
		&mut self,
		probe_rows: usize,
		matches: &[SpilledInnerJoinMatch],
	) {
		let Some(profile) = &mut self.throughput_profile else {
			return;
		};
		let unique_pages = matches
			.iter()
			.map(|matched| matched.build.page_index)
			.collect::<BTreeSet<_>>()
			.len();
		profile.record_probe_chunk(probe_rows, unique_pages);
	}

	pub(in crate::exec) fn record_materialized_batch(
		&mut self,
		rows: usize,
		unique_page_requests: usize,
	) {
		if let Some(profile) = &mut self.throughput_profile {
			profile.record_materialized_batch(rows, unique_page_requests);
		}
	}

	pub(in crate::exec) fn throughput_profile_enabled(&self) -> bool {
		self.throughput_profile.is_some()
	}

	pub(in crate::exec) fn record_grouped_fold(
		&mut self,
		rows: usize,
		elapsed: std::time::Duration,
	) {
		if let Some(profile) = &mut self.throughput_profile {
			profile.record_grouped_fold(rows, elapsed);
		}
	}

	pub(in crate::exec) fn record_throughput_phase_micros(
		&mut self,
		build_micros: u64,
		probe_micros: u64,
	) {
		if let Some(profile) = &mut self.throughput_profile {
			profile.record_phase_micros(build_micros, probe_micros);
		}
	}

	pub(in crate::exec) fn persist_throughput_profile(&self) -> Result<(), ExecutionError> {
		let Some(profile) = &self.throughput_profile else {
			return Ok(());
		};
		profile.persist(
			self.spill_id,
			self.spill_bytes,
			self.pages.len(),
			self.input_rows,
			self.cache_cap,
		)
	}

	pub(in crate::exec) fn page_row_count(
		&self,
		page_index: usize,
	) -> Result<usize, ExecutionError> {
		self.page_rows
			.get(page_index)
			.copied()
			.map(|rows| rows as usize)
			.ok_or_else(|| ExecutionError::Kernel {
				message: format!("inner join batch spill page {page_index} is missing row count"),
			})
	}

	pub(in crate::exec) fn empty_page_match_bitmap(&self) -> Vec<Vec<u64>> {
		self.page_rows
			.iter()
			.map(|rows| vec![0; (*rows as usize).div_ceil(64)])
			.collect()
	}

	pub(in crate::exec) fn mark_page_row_matched(
		&self,
		matched: &mut [Vec<u64>],
		row: SpilledBatchRowRef,
	) {
		let Some(bitmap) = matched.get_mut(row.page_index) else {
			return;
		};
		set_build_row_matched(bitmap, row.row);
	}

	pub(in crate::exec) fn page_row_matched(
		&self,
		matched: &[Vec<u64>],
		page_index: usize,
		row: usize,
	) -> bool {
		matched
			.get(page_index)
			.is_some_and(|bitmap| build_row_matched(bitmap, row))
	}

	pub(in crate::exec) async fn read_batch_page(
		&mut self,
		page_index: usize,
	) -> Result<RecordBatch, ExecutionError> {
		self.read_batch_page_profiled(page_index, None).await
	}

	pub(in crate::exec) async fn read_batch_page_profiled(
		&mut self,
		page_index: usize,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<RecordBatch, ExecutionError> {
		let mut cache_timer = observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::ReadCacheLru))
			.transpose()?;
		if let Some(batch) = self
			.pinned_decoded_pages
			.as_ref()
			.and_then(|pages| pages.get(page_index))
			.cloned()
		{
			if let Some(observer) = observer {
				observer.record_decoded_cache(true, record_batch_memory_bytes(&batch))?;
			}
			if let Some(timer) = cache_timer.take() {
				timer.finish()?;
			}
			return Ok(batch);
		}
		if let Some(batch) = self
			.cache
			.get(&page_index)
			.and_then(|cached| cached.decoded_batch.clone())
		{
			if let Some(profile) = &mut self.throughput_profile {
				profile.record_decoded_hit();
			}
			self.touch_cache_entry(page_index);
			if let Some(observer) = observer {
				observer.record_decoded_cache(true, record_batch_memory_bytes(&batch))?;
			}
			if let Some(timer) = cache_timer.take() {
				timer.finish()?;
			}
			return Ok(batch);
		}
		if let Some(observer) = observer {
			observer.record_decoded_cache(false, 0)?;
		}
		if let Some(timer) = cache_timer.take() {
			timer.finish()?;
		}
		if let Some(profile) = &mut self.throughput_profile {
			profile.record_decoded_miss();
		}
		let bytes = self.read_page_profiled(page_index, observer).await?;
		let decode_started = self.throughput_profile.as_ref().map(|_| Instant::now());
		let decode_timer = observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::ReadIpcDecode))
			.transpose()?;
		let batch = decode_record_batch_ipc(&bytes)?;
		if let Some(observer) = observer {
			observer.record_decode()?;
		}
		if let Some(timer) = decode_timer {
			timer.finish_with(
				batch.num_rows() as u64,
				record_batch_memory_bytes(&batch),
				1,
			)?;
		}
		if let (Some(profile), Some(decode_started)) =
			(&mut self.throughput_profile, decode_started)
		{
			profile.record_page_decode(
				page_index,
				record_batch_memory_bytes(&batch),
				decode_started.elapsed(),
			);
		}
		#[cfg(test)]
		{
			self.page_decode_count = self.page_decode_count.saturating_add(1);
		}
		let cache_timer = observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::ReadCacheLru))
			.transpose()?;
		self.insert_decoded_cache_entry(page_index, batch.clone());
		if let Some(timer) = cache_timer {
			timer.finish()?;
		}
		Ok(batch)
	}

	async fn read_page_profiled(
		&mut self,
		page_index: usize,
		observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<Vec<u8>, ExecutionError> {
		let mut cache_timer = observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::ReadCacheLru))
			.transpose()?;
		if let Some(cached) = self
			.cache
			.get(&page_index)
			.filter(|cached| !cached.bytes.is_empty())
		{
			if let Some(profile) = &mut self.throughput_profile {
				profile.record_plaintext_hit();
			}
			let bytes = cached.bytes.clone();
			self.touch_cache_entry(page_index);
			if let Some(observer) = observer {
				observer
					.record_plaintext_cache(true, u64::try_from(bytes.len()).unwrap_or(u64::MAX))?;
			}
			if let Some(timer) = cache_timer.take() {
				timer.finish()?;
			}
			return Ok(bytes);
		}
		if let Some(observer) = observer {
			observer.record_plaintext_cache(false, 0)?;
		}
		if let Some(timer) = cache_timer.take() {
			timer.finish()?;
		}
		if let Some(profile) = &mut self.throughput_profile {
			profile.record_plaintext_miss();
		}

		let read_timer = observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::ReadPhysicalRead))
			.transpose()?;
		let page = self
			.pages
			.get(page_index)
			.copied()
			.ok_or_else(|| ExecutionError::Kernel {
				message: format!("inner join batch spill page {page_index} is missing"),
			})?;
		let mut ciphertext = read_plain_exact(
			self.file_ops.as_ref(),
			&self.file,
			page.offset,
			page.ciphertext_len,
			self.throughput_profile.as_deref_mut(),
			observer,
		)
		.await?;
		self.physical_read_pages =
			self.physical_read_pages
				.checked_add(1)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "spill physical-read page count overflowed".to_string(),
				})?;
		if let Some(timer) = read_timer {
			timer.finish_with(0, ciphertext.len() as u64, 1)?;
		}
		let nonce = Nonce::assume_unique_for_key(join_spill_nonce(self.spill_id, page_index));
		let decrypt_timer = observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::ReadDecrypt))
			.transpose()?;
		let plaintext = self
			.key
			.open_in_place(nonce, Aad::empty(), &mut ciphertext)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("inner join batch spill decrypt failed: {error:?}"),
			})?
			.to_vec();
		if let Some(timer) = decrypt_timer {
			timer.finish_with(0, plaintext.len() as u64, 1)?;
		}
		let cache_timer = observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::ReadCacheLru))
			.transpose()?;
		self.insert_cache_entry(page_index, plaintext.clone());
		if let Some(timer) = cache_timer {
			timer.finish()?;
		}
		Ok(plaintext)
	}

	pub(in crate::exec) async fn cleanup(self) {
		let file_ops = self.file_ops.clone();
		let path = self.path.clone();
		drop(self);
		let _ = file_ops.unlink(&path).await;
	}

	pub(in crate::exec) fn touch_cache_entry(&mut self, page_index: usize) {
		self.lru.retain(|cached| *cached != page_index);
		self.lru.push_back(page_index);
	}

	pub(in crate::exec) fn insert_cache_entry(&mut self, page_index: usize, bytes: Vec<u8>) {
		let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
		if self.cache_cap == 0 || bytes_len > self.cache_cap {
			return;
		}
		if let Some(page) = self.cache.remove(&page_index) {
			self.cache_bytes = self.cache_bytes.saturating_sub(page.total_bytes());
			if let Some(profile) = &mut self.throughput_profile {
				profile.record_cache_evictions(cache_evictions_for_page(&page));
			}
		}
		self.cache_bytes = self.cache_bytes.saturating_add(bytes_len);
		self.cache_peak_bytes = self.cache_peak_bytes.max(self.cache_bytes);
		if let Some(profile) = &mut self.throughput_profile {
			profile.record_plaintext_insert(bytes_len);
		}
		self.cache
			.insert(page_index, CachedJoinSpillPage::new(bytes));
		self.touch_cache_entry(page_index);
		let evictions = evict_join_spill_cache(
			&mut self.cache,
			&mut self.lru,
			&mut self.cache_bytes,
			self.cache_cap,
			self.throughput_profile.is_some(),
		);
		if let Some(profile) = &mut self.throughput_profile {
			profile.record_cache_evictions(evictions);
		}
	}

	pub(in crate::exec) fn insert_decoded_cache_entry(
		&mut self,
		page_index: usize,
		batch: RecordBatch,
	) {
		if self.cache_cap == 0 {
			return;
		}
		let decoded_bytes = record_batch_memory_bytes(&batch);
		if decoded_bytes > self.cache_cap {
			return;
		}
		let Some(cached) = self.cache.get_mut(&page_index) else {
			return;
		};
		let old_total = cached.total_bytes();
		let removed_plaintext_bytes = self
			.throughput_profile
			.as_ref()
			.map(|_| u64::try_from(cached.bytes.len()).unwrap_or(u64::MAX));
		cached.bytes.clear();
		cached.decoded_batch = Some(batch);
		cached.decoded_bytes = decoded_bytes;
		let new_total = cached.total_bytes();
		if new_total >= old_total {
			self.cache_bytes = self.cache_bytes.saturating_add(new_total - old_total);
		} else {
			self.cache_bytes = self.cache_bytes.saturating_sub(old_total - new_total);
		}
		self.cache_peak_bytes = self.cache_peak_bytes.max(self.cache_bytes);
		if let (Some(profile), Some(removed_plaintext_bytes)) =
			(&mut self.throughput_profile, removed_plaintext_bytes)
		{
			profile.record_plaintext_promotion(removed_plaintext_bytes);
			profile.record_decoded_insert(decoded_bytes);
		}
		self.touch_cache_entry(page_index);
		let evictions = evict_join_spill_cache(
			&mut self.cache,
			&mut self.lru,
			&mut self.cache_bytes,
			self.cache_cap,
			self.throughput_profile.is_some(),
		);
		if let Some(profile) = &mut self.throughput_profile {
			profile.record_cache_evictions(evictions);
		}
	}

	#[cfg(test)]
	pub(in crate::exec) fn input_batches_written(&self) -> u64 {
		self.input_batches_written
	}

	#[cfg(test)]
	pub(in crate::exec) fn legacy_index_insert_count(&self) -> u64 {
		self.legacy_index_insert_count
	}

	#[cfg(test)]
	pub(in crate::exec) fn legacy_index_is_empty(&self) -> bool {
		self.index.is_empty()
	}

	#[cfg(test)]
	pub(in crate::exec) fn page_decode_count(&self) -> u64 {
		self.page_decode_count
	}

	#[cfg(test)]
	pub(in crate::exec) fn resident_index_bytes(&self) -> u64 {
		self.resident_index_bytes
	}

	#[cfg(test)]
	pub(in crate::exec) fn folded_index_bytes(&self) -> u64 {
		self.compact_index.as_ref().map_or_else(
			|| spilled_batch_join_hash_index_bytes(&self.index),
			SpilledBatchIndex::accounted_bytes,
		)
	}

	#[cfg(test)]
	pub(in crate::exec) fn computed_index_bytes(&self) -> u64 {
		self.compact_index.as_ref().map_or_else(
			|| spilled_batch_join_hash_index_bytes(&self.index),
			SpilledBatchIndex::computed_bytes,
		)
	}

	#[cfg(test)]
	pub(in crate::exec) fn uses_compact_int64_index(&self) -> bool {
		self.compact_index
			.as_ref()
			.is_some_and(SpilledBatchIndex::is_int64)
	}

	#[cfg(test)]
	pub(in crate::exec) fn uses_hash_index(&self) -> bool {
		self.compact_index.is_none()
	}

	pub(in crate::exec) fn input_row_count(&self) -> u64 {
		self.input_rows
	}

	pub(in crate::exec) fn spill_bytes(&self) -> u64 {
		self.spill_bytes
	}

	pub(in crate::exec) fn indexed_flush_pages(&self) -> u64 {
		self.indexed_flush_pages
	}

	pub(in crate::exec) fn indexed_flush_guard_checks(&self) -> u64 {
		self.indexed_flush_guard_checks
	}

	pub(in crate::exec) fn index_rebuild_calls(&self) -> u64 {
		self.index_rebuild_calls
	}

	pub(in crate::exec) fn physical_read_pages(&self) -> u64 {
		self.physical_read_pages
	}

	pub(in crate::exec) fn page_geometry(&self) -> (Vec<u32>, Vec<u64>) {
		(
			self.page_rows.clone(),
			self.pages
				.iter()
				.map(|page| page.ciphertext_len as u64)
				.collect(),
		)
	}

	pub(in crate::exec) fn retained_resident_bytes(&self) -> u64 {
		self.resident_index_bytes
			.saturating_add(self.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64)
			.saturating_add(self.cache_bytes)
			.saturating_add(self.pinned_decoded_bytes)
	}

	pub(in crate::exec) fn owner_retained_resident_bytes(&self) -> u64 {
		let page_metadata = spill_capacity_bytes::<JoinSpillPage>(self.pages.capacity())
			.saturating_add(spill_capacity_bytes::<u32>(self.page_rows.capacity()));
		let cache_metadata = u64::try_from(
			self.cache
				.capacity()
				.saturating_mul(std::mem::size_of::<(usize, CachedJoinSpillPage)>() + 16),
		)
		.unwrap_or(u64::MAX)
		.saturating_add(spill_capacity_bytes::<usize>(self.lru.capacity()));
		let pinned_handles = self.pinned_decoded_pages.as_ref().map_or(0, |pages| {
			spill_capacity_bytes::<RecordBatch>(pages.capacity())
		});
		let scratch_capacity = u64::try_from(self.scratch_plaintext.capacity())
			.unwrap_or(u64::MAX)
			.saturating_add(spill_capacity_bytes::<(JoinKey, u32)>(
				self.scratch_refs.capacity(),
			));
		let retained_legacy_index = self
			.compact_index
			.as_ref()
			.map_or(0, |_| spilled_batch_join_hash_index_bytes(&self.index));
		self.retained_resident_bytes()
			.saturating_add(page_metadata)
			.saturating_add(cache_metadata)
			.saturating_add(pinned_handles)
			.saturating_add(scratch_capacity)
			.saturating_add(retained_legacy_index)
			.saturating_add(u64::try_from(self.path.capacity()).unwrap_or(u64::MAX))
	}

	pub(in crate::exec) fn residency_snapshot(&self) -> SpilledInnerJoinResidencySnapshot {
		let page_metadata_bytes = checked_spill_capacity_bytes::<JoinSpillPage>(
			self.pages.capacity(),
		)
		.and_then(|pages| {
			checked_spill_capacity_bytes::<u32>(self.page_rows.capacity())
				.and_then(|rows| pages.checked_add(rows))
		});
		let cache_metadata_bytes = self
			.cache
			.capacity()
			.checked_mul(std::mem::size_of::<(usize, CachedJoinSpillPage)>() + 16)
			.and_then(|bytes| u64::try_from(bytes).ok())
			.and_then(|cache| {
				checked_spill_capacity_bytes::<usize>(self.lru.capacity())
					.and_then(|lru| cache.checked_add(lru))
			});
		let pinned_handle_capacity_bytes =
			self.pinned_decoded_pages.as_ref().map_or(Some(0), |pages| {
				checked_spill_capacity_bytes::<RecordBatch>(pages.capacity())
			});
		let write_scratch_capacity_bytes = u64::try_from(self.scratch_plaintext.capacity())
			.ok()
			.and_then(|plaintext| {
				checked_spill_capacity_bytes::<(JoinKey, u32)>(self.scratch_refs.capacity())
					.and_then(|refs| plaintext.checked_add(refs))
			});
		let retained_legacy_index_bytes = self.compact_index.as_ref().map_or(Some(0), |_| {
			Some(spilled_batch_join_hash_index_bytes(&self.index))
		});
		let path_capacity_bytes = u64::try_from(self.path.capacity()).ok();
		let fixed = [
			self.resident_index_bytes,
			self.cache_bytes,
			self.pinned_decoded_bytes,
		];
		let optional = [
			page_metadata_bytes,
			cache_metadata_bytes,
			pinned_handle_capacity_bytes,
			write_scratch_capacity_bytes,
			retained_legacy_index_bytes,
			path_capacity_bytes,
		];
		let current_resident_bytes = optional
			.into_iter()
			.collect::<Option<Vec<_>>>()
			.and_then(|optional| checked_spill_sum(fixed.into_iter().chain(optional)));
		let guard_accounted_bytes = checked_spill_sum([
			self.resident_baseline_bytes,
			self.resident_index_bytes,
			self.writer_peak,
			JOIN_SPILL_PAGE_SIZE as u64,
			self.cache_bytes,
			self.pinned_decoded_bytes,
		]);
		SpilledInnerJoinResidencySnapshot {
			resident_baseline_guard_bytes: self.resident_baseline_bytes,
			resident_index_bytes: self.resident_index_bytes,
			writer_scratch_peak_bytes: self.writer_peak,
			page_reserve_bytes: JOIN_SPILL_PAGE_SIZE as u64,
			cache_payload_bytes: self.cache_bytes,
			cache_cap_bytes: self.cache_cap,
			cache_peak_bytes: self.cache_peak_bytes,
			pinned_decoded_bytes: self.pinned_decoded_bytes,
			page_metadata_bytes,
			cache_metadata_bytes,
			pinned_handle_capacity_bytes,
			write_scratch_capacity_bytes,
			retained_legacy_index_bytes,
			path_capacity_bytes,
			open_file_handles: 1,
			input_rows: self.input_rows,
			spill_disk_bytes: self.spill_bytes,
			current_resident_bytes,
			current_resident_error: current_resident_bytes
				.is_none()
				.then_some("writer current-resident accounting overflow"),
			guard_accounted_bytes,
			guard_accounted_error: guard_accounted_bytes
				.is_none()
				.then_some("writer guard-accounted accounting overflow"),
			overlap_label: "baseline_page_reserve_and_writer_peak_are_guard_inputs_excluded_from_current_ownership",
		}
	}

	pub(in crate::exec) fn clear_read_cache(&mut self) {
		self.cache.clear();
		self.lru.clear();
		self.cache_bytes = 0;
	}

	pub(in crate::exec) fn loaded_page_bytes_estimate(&self) -> u64 {
		self.pages
			.iter()
			.map(|page| page.plaintext_len as u64)
			.fold(0u64, u64::saturating_add)
	}

	pub(in crate::exec) fn writer_peak_bytes(&self) -> u64 {
		self.writer_peak
	}
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SpilledExistenceRowRef {
	page_index: usize,
	row: u32,
}

pub(in crate::exec) struct SpilledSemiAntiExistenceBuild {
	spill_id: u64,
	path: PathBuf,
	file_ops: Arc<dyn AsyncFileOps>,
	file: FileHandle,
	key: LessSafeKey,
	projection: Option<SpilledSemiAntiExistenceProjection>,
	pages: Vec<JoinSpillPage>,
	index: HashMap<JoinKey, Vec<SpilledExistenceRowRef>>,
	resident_index_bytes: u64,
	cache: HashMap<usize, CachedJoinSpillPage>,
	lru: VecDeque<usize>,
	cache_bytes: u64,
	cache_cap: u64,
	spill_bytes: u64,
	writer_peak: u64,
	rows: u64,
	input_rows: u64,
	input_batches_written: u64,
	next_offset: u64,
}

#[derive(Clone)]
struct SpilledSemiAntiExistenceProjection {
	full_schema: SchemaRef,
	retained_columns: Vec<crate::ir::ColumnOrdinal>,
}

impl SpilledSemiAntiExistenceBuild {
	pub(in crate::exec) async fn create(
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
		spill_dir: PathBuf,
	) -> Result<Self, ExecutionError> {
		let spill_id = NEXT_JOIN_SPILL_ID.fetch_add(1, Ordering::SeqCst);
		let path = spill_dir.join(format!(
			"spill_{}_{}.existence",
			std::process::id(),
			spill_id
		));
		match Self::create_inner(spill_id, path.clone(), task_budget, file_ops.clone()).await {
			Ok(spill) => Ok(spill),
			Err(error) => {
				let _ = file_ops.unlink(&path).await;
				Err(error)
			}
		}
	}

	pub(in crate::exec) async fn create_inner(
		spill_id: u64,
		path: PathBuf,
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
	) -> Result<Self, ExecutionError> {
		let spill_dir = path.parent().ok_or_else(|| ExecutionError::Kernel {
			message: "semi/anti existence spill path has no parent directory".to_string(),
		})?;
		file_ops
			.create_dir_all(spill_dir)
			.await
			.map_err(|error| join_spill_io_error("create_dir_all", error))?;
		let mut key_bytes = [0u8; 32];
		aws_lc_rs::rand::fill(&mut key_bytes).map_err(|error| ExecutionError::Kernel {
			message: format!("semi/anti existence spill key generation failed: {error:?}"),
		})?;
		let unbound = UnboundKey::new(&AES_256_GCM_SIV, &key_bytes).map_err(|error| {
			ExecutionError::Kernel {
				message: format!("semi/anti existence spill key construction failed: {error:?}"),
			}
		})?;
		let key = LessSafeKey::new(unbound);
		let file = file_ops
			.open(
				&path,
				OpenFlags {
					read: true,
					write: true,
					create_new: true,
					..OpenFlags::default()
				},
			)
			.await
			.map_err(|error| ExecutionError::Kernel {
				message: format!("semi/anti existence spill open failed: {error}"),
			})?;
		let cache_cap = JOIN_SPILL_CACHE_TARGET_BYTES.min(task_budget.memory.get());
		Ok(Self {
			spill_id,
			path,
			file_ops,
			file,
			key,
			projection: None,
			pages: Vec::new(),
			index: HashMap::new(),
			resident_index_bytes: 0,
			cache: HashMap::new(),
			lru: VecDeque::new(),
			cache_bytes: 0,
			cache_cap,
			spill_bytes: 0,
			writer_peak: 0,
			rows: 0,
			input_rows: 0,
			input_batches_written: 0,
			next_offset: 0,
		})
	}

	pub(in crate::exec) fn set_projection(
		&mut self,
		full_schema: SchemaRef,
		retained_columns: Vec<crate::ir::ColumnOrdinal>,
	) -> Result<(), ExecutionError> {
		if retained_columns.is_empty() || retained_columns.len() >= full_schema.fields().len() {
			self.projection = None;
			return Ok(());
		}
		let mut seen = BTreeSet::new();
		for ordinal in &retained_columns {
			let index = ordinal.0 as usize;
			if index >= full_schema.fields().len() {
				return Err(ExecutionError::InvalidExpression {
					message: format!(
						"projected semi/anti existence column ordinal {} out of bounds",
						ordinal.0
					),
				});
			}
			if !seen.insert(*ordinal) {
				return Err(ExecutionError::InvalidExpression {
					message: format!(
						"projected semi/anti existence column ordinal {} repeated",
						ordinal.0
					),
				});
			}
		}
		self.projection = Some(SpilledSemiAntiExistenceProjection {
			full_schema,
			retained_columns,
		});
		Ok(())
	}

	pub(in crate::exec) async fn write_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		existence_keys: &[crate::ir::ColumnOrdinal],
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			self.write_page_batch(&chunk, key_plan, existence_keys, task_budget)
				.await?;
		}
		Ok(())
	}

	pub(in crate::exec) async fn write_projected_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		projected_existence_keys: &[crate::ir::ColumnOrdinal],
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		let retained_columns = self
			.projection
			.as_ref()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "projected semi/anti existence spill missing projection".to_string(),
			})?
			.retained_columns
			.clone();
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			let projected = project_batch_columns(&chunk, &retained_columns)?;
			self.write_page_batch(&projected, key_plan, projected_existence_keys, task_budget)
				.await?;
		}
		Ok(())
	}

	pub(in crate::exec) fn write_membership_key_chunks(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		existence_keys: &[crate::ir::ColumnOrdinal],
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			self.index_membership_keys(&chunk, key_plan, existence_keys, task_budget)?;
		}
		Ok(())
	}

	pub(in crate::exec) fn write_projected_membership_key_chunks(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		projected_existence_keys: &[crate::ir::ColumnOrdinal],
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		let retained_columns = self
			.projection
			.as_ref()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "projected semi/anti existence spill missing projection".to_string(),
			})?
			.retained_columns
			.clone();
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			let projected = project_batch_columns(&chunk, &retained_columns)?;
			self.index_membership_keys(
				&projected,
				key_plan,
				projected_existence_keys,
				task_budget,
			)?;
		}
		Ok(())
	}

	fn index_membership_keys(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		existence_keys: &[crate::ir::ColumnOrdinal],
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		let key_arrays = key_arrays(batch, existence_keys)?;
		for row in 0..batch.num_rows() {
			let Some(key) = join_key_at(key_plan, &key_arrays, row, false)? else {
				continue;
			};
			index_membership_key(&mut self.index, &mut self.resident_index_bytes, key);
			self.rows = self.rows.saturating_add(1);
		}
		self.guard_resident_state(task_budget.memory.get())
	}

	pub(in crate::exec) async fn write_page_batch(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		existence_keys: &[crate::ir::ColumnOrdinal],
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		let key_arrays = key_arrays(batch, existence_keys)?;
		let mut refs = Vec::new();
		for row in 0..batch.num_rows() {
			let Some(key) = join_key_at(key_plan, &key_arrays, row, false)? else {
				continue;
			};
			refs.push((key, join_build_row_index(row)?));
		}
		if refs.is_empty() {
			return Ok(());
		}

		let page_index = self.pages.len();
		let plaintext = encode_record_batch_ipc(batch)?;
		let page = write_join_spill_plaintext_page(
			self.file_ops.as_ref(),
			&self.file,
			&self.key,
			self.spill_id,
			page_index,
			self.next_offset,
			plaintext,
		)
		.await?;
		self.spill_bytes = self.spill_bytes.saturating_add(page.ciphertext_len as u64);
		self.writer_peak = self.writer_peak.max(page.plaintext_len as u64);
		if self.spill_bytes > task_budget.spill_bytes.get() {
			return Err(budget_exhausted(
				BudgetDimension::SpillBytes,
				"semi/anti existence-set spill writer",
				self.spill_bytes,
				task_budget.spill_bytes.get(),
			));
		}
		for (key, row) in refs {
			append_spilled_existence_row_ref(
				&mut self.index,
				&mut self.resident_index_bytes,
				key,
				SpilledExistenceRowRef { page_index, row },
			);
			self.rows = self.rows.saturating_add(1);
		}
		self.next_offset = self.next_offset.saturating_add(page.ciphertext_len as u64);
		self.pages.push(page);
		self.guard_resident_state(task_budget.memory.get())?;
		Ok(())
	}

	pub(in crate::exec) async fn finish(
		&self,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		self.file_ops
			.set_len(&self.file, self.next_offset)
			.await
			.map_err(|error| join_spill_io_error("set_len", error))?;
		self.file_ops
			.fdatasync(&self.file)
			.await
			.map_err(|error| join_spill_io_error("fdatasync", error))?;
		self.guard_resident_state(task_budget.memory.get())
	}

	pub(in crate::exec) fn guard_resident_state(
		&self,
		memory_cap: u64,
	) -> Result<(), ExecutionError> {
		let resident = self
			.resident_index_bytes
			.saturating_add(self.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		if resident > cap {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"semi/anti existence-set spill resident state",
				resident,
				cap,
			));
		}
		Ok(())
	}

	#[cfg(test)]
	pub(in crate::exec) fn row_count(&self) -> u64 {
		self.rows
	}

	pub(in crate::exec) fn input_row_count(&self) -> u64 {
		self.input_rows
	}

	pub(in crate::exec) fn spill_bytes(&self) -> u64 {
		self.spill_bytes
	}

	#[cfg(test)]
	pub(in crate::exec) fn resident_index_bytes(&self) -> u64 {
		self.resident_index_bytes
	}

	#[cfg(test)]
	pub(in crate::exec) fn folded_index_bytes(&self) -> u64 {
		semi_anti_existence_spill_index_bytes(&self.index)
	}

	#[cfg(test)]
	pub(in crate::exec) fn input_batches_written(&self) -> u64 {
		self.input_batches_written
	}

	#[cfg(test)]
	pub(in crate::exec) fn page_count(&self) -> usize {
		self.pages.len()
	}

	pub(in crate::exec) async fn matching_preserved_rows(
		&mut self,
		preserved_batch: &RecordBatch,
		preserved_by_key: &HashMap<JoinKey, Vec<u32>>,
		match_filter: Option<&BoundExprProgram>,
	) -> Result<Vec<bool>, ExecutionError> {
		if match_filter.is_none() {
			let mut matched = vec![false; preserved_batch.num_rows()];
			for (key, preserved_rows) in preserved_by_key {
				if self.index.contains_key(key) {
					for preserved_row in preserved_rows {
						matched[*preserved_row as usize] = true;
					}
				}
			}
			return Ok(matched);
		}

		let mut page_work: BTreeMap<usize, Vec<(Vec<u32>, Vec<u32>)>> = BTreeMap::new();
		for (key, preserved_rows) in preserved_by_key {
			let Some(row_refs) = self.index.get(key) else {
				continue;
			};
			let mut rows_by_page: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
			for row_ref in row_refs {
				rows_by_page
					.entry(row_ref.page_index)
					.or_default()
					.push(row_ref.row);
			}
			for (page_index, existence_rows) in rows_by_page {
				page_work
					.entry(page_index)
					.or_default()
					.push((preserved_rows.clone(), existence_rows));
			}
		}

		let mut matched = vec![false; preserved_batch.num_rows()];
		for (page_index, groups) in page_work {
			let existence_batch = self.read_batch_page(page_index).await?;
			let existence_batch = if match_filter.is_some() {
				self.restore_projected_batch(existence_batch)?
			} else {
				existence_batch
			};
			let mut residual =
				JoinMatchResidual::new(match_filter, preserved_batch, &existence_batch);
			for (preserved_rows, existence_rows) in groups {
				for preserved_row in preserved_rows {
					if matched[preserved_row as usize] {
						continue;
					}
					for existence_row in &existence_rows {
						if residual.matches(preserved_row, *existence_row)? {
							matched[preserved_row as usize] = true;
							break;
						}
					}
				}
			}
		}
		Ok(matched)
	}

	fn restore_projected_batch(&self, batch: RecordBatch) -> Result<RecordBatch, ExecutionError> {
		let Some(projection) = &self.projection else {
			return Ok(batch);
		};
		let retained_positions = projection
			.retained_columns
			.iter()
			.enumerate()
			.map(|(position, ordinal)| (ordinal.0 as usize, position))
			.collect::<BTreeMap<_, _>>();
		let mut columns = Vec::with_capacity(projection.full_schema.fields().len());
		for (column_index, field) in projection.full_schema.fields().iter().enumerate() {
			if let Some(retained_position) = retained_positions.get(&column_index).copied() {
				let column = batch
					.columns()
					.get(retained_position)
					.cloned()
					.ok_or_else(|| ExecutionError::Kernel {
						message: format!(
							"projected semi/anti existence page missing retained column {}",
							column_index
						),
					})?;
				columns.push(column);
			} else {
				columns.push(default_array_for_unused_column(
					field.data_type(),
					batch.num_rows(),
				)?);
			}
		}
		Ok(RecordBatch::try_new(
			projection.full_schema.clone(),
			columns,
		)?)
	}

	pub(in crate::exec) async fn read_batch_page(
		&mut self,
		page_index: usize,
	) -> Result<RecordBatch, ExecutionError> {
		let bytes = self.read_page(page_index).await?;
		decode_record_batch_ipc(&bytes)
	}

	pub(in crate::exec) async fn read_page(
		&mut self,
		page_index: usize,
	) -> Result<Vec<u8>, ExecutionError> {
		if let Some(cached) = self
			.cache
			.get(&page_index)
			.filter(|cached| !cached.bytes.is_empty())
		{
			let bytes = cached.bytes.clone();
			self.touch_cache_entry(page_index);
			return Ok(bytes);
		}

		let page = self
			.pages
			.get(page_index)
			.copied()
			.ok_or_else(|| ExecutionError::Kernel {
				message: format!("semi/anti existence spill page {page_index} is missing"),
			})?;
		let mut ciphertext = read_plain_exact(
			self.file_ops.as_ref(),
			&self.file,
			page.offset,
			page.ciphertext_len,
			None,
			None,
		)
		.await?;
		let nonce = Nonce::assume_unique_for_key(join_spill_nonce(self.spill_id, page_index));
		let plaintext = self
			.key
			.open_in_place(nonce, Aad::empty(), &mut ciphertext)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("semi/anti existence spill decrypt failed: {error:?}"),
			})?
			.to_vec();
		self.insert_cache_entry(page_index, plaintext.clone());
		Ok(plaintext)
	}

	pub(in crate::exec) async fn cleanup(self) {
		let file_ops = self.file_ops.clone();
		let path = self.path.clone();
		drop(self);
		let _ = file_ops.unlink(&path).await;
	}

	pub(in crate::exec) fn touch_cache_entry(&mut self, page_index: usize) {
		self.lru.retain(|cached| *cached != page_index);
		self.lru.push_back(page_index);
	}

	pub(in crate::exec) fn insert_cache_entry(&mut self, page_index: usize, bytes: Vec<u8>) {
		let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
		if self.cache_cap == 0 || bytes_len > self.cache_cap {
			return;
		}
		if let Some(page) = self.cache.remove(&page_index) {
			self.cache_bytes = self.cache_bytes.saturating_sub(page.total_bytes());
		}
		self.cache_bytes = self.cache_bytes.saturating_add(bytes_len);
		self.cache
			.insert(page_index, CachedJoinSpillPage::new(bytes));
		self.touch_cache_entry(page_index);
		evict_join_spill_cache(
			&mut self.cache,
			&mut self.lru,
			&mut self.cache_bytes,
			self.cache_cap,
			false,
		);
	}
}

async fn write_join_spill_page(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	key: &LessSafeKey,
	spill_id: u64,
	page_index: usize,
	offset: u64,
	entries: &[(Vec<u8>, Vec<u32>)],
) -> Result<JoinSpillPage, ExecutionError> {
	let plaintext = encode_join_spill_page(entries)?;
	write_join_spill_plaintext_page(file_ops, file, key, spill_id, page_index, offset, plaintext)
		.await
}

async fn write_join_spill_plaintext_page(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	key: &LessSafeKey,
	spill_id: u64,
	page_index: usize,
	offset: u64,
	plaintext: Vec<u8>,
) -> Result<JoinSpillPage, ExecutionError> {
	let (page, _) = write_join_spill_plaintext_page_returning_buffer(
		file_ops, file, key, spill_id, page_index, offset, plaintext, None,
	)
	.await?;
	Ok(page)
}

async fn write_join_spill_plaintext_page_returning_buffer(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	key: &LessSafeKey,
	spill_id: u64,
	page_index: usize,
	offset: u64,
	mut plaintext: Vec<u8>,
	observer: Option<&GroupedChainSpillObserver<'_>>,
) -> Result<(JoinSpillPage, Vec<u8>), ExecutionError> {
	let plaintext_len = plaintext.len();
	let nonce = Nonce::assume_unique_for_key(join_spill_nonce(spill_id, page_index));
	let encryption_timer = observer
		.map(|observer| observer.begin(GroupedChainSubLeaf::WriteEncryption))
		.transpose()?;
	key.seal_in_place_append_tag(nonce, Aad::empty(), &mut plaintext)
		.map_err(|error| ExecutionError::Kernel {
			message: format!("join spill encrypt failed: {error:?}"),
		})?;
	if let Some(timer) = encryption_timer {
		timer.finish_with(0, plaintext.len() as u64, 1)?;
	}
	let ciphertext_len = plaintext.len();
	let write_timer = observer
		.map(|observer| observer.begin(GroupedChainSubLeaf::WritePhysicalWrite))
		.transpose()?;
	let plaintext =
		write_plain_all_returning_buffer(file_ops, file, offset, plaintext, observer).await?;
	if let Some(observer) = observer {
		observer.record_written_page(spill_id, page_index, ciphertext_len)?;
	}
	if let Some(timer) = write_timer {
		timer.finish_with(0, ciphertext_len as u64, 1)?;
	}
	Ok((
		JoinSpillPage {
			offset,
			ciphertext_len,
			plaintext_len,
		},
		plaintext,
	))
}

async fn write_plain_all_returning_buffer(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	offset: u64,
	bytes: Vec<u8>,
	observer: Option<&GroupedChainSpillObserver<'_>>,
) -> Result<Vec<u8>, ExecutionError> {
	let mut written_total = 0usize;
	while written_total < bytes.len() {
		let written = file_ops
			.write_plain_at(
				file,
				offset.saturating_add(written_total as u64),
				bytes[written_total..].to_vec(),
			)
			.await
			.map_err(|error| join_spill_io_error("write", error))?;
		if let Some(observer) = observer {
			observer.record_physical_write(written)?;
		}
		if written == 0 {
			return Err(join_spill_io_error(
				"write",
				std::io::Error::new(
					ErrorKind::WriteZero,
					"join spill write completed with zero bytes",
				),
			));
		}
		written_total = written_total.saturating_add(written);
	}
	Ok(bytes)
}

async fn read_plain_exact(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	offset: u64,
	len: usize,
	mut profile: Option<&mut StreamedSpilledLeftOuterProfile>,
	observer: Option<&GroupedChainSpillObserver<'_>>,
) -> Result<Vec<u8>, ExecutionError> {
	let mut out = Vec::with_capacity(len);
	while out.len() < len {
		let read_offset = offset.saturating_add(out.len() as u64);
		let requested = len.saturating_sub(out.len());
		let chunk = if let Some(profile) = profile.as_deref_mut() {
			let started = Instant::now();
			let mut returned_pending = false;
			let mut read = file_ops.read_plain_at(file, read_offset, requested);
			let result = std::future::poll_fn(|context| match read.as_mut().poll(context) {
				std::task::Poll::Pending => {
					returned_pending = true;
					std::task::Poll::Pending
				}
				std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
			})
			.await
			.map_err(|error| join_spill_io_error("read", error))?;
			profile.record_file_read(requested, result.len(), started.elapsed(), returned_pending);
			result
		} else {
			file_ops
				.read_plain_at(file, read_offset, requested)
				.await
				.map_err(|error| join_spill_io_error("read", error))?
		};
		if let Some(observer) = observer {
			observer.record_physical_read(chunk.len())?;
		}
		if chunk.is_empty() {
			return Err(join_spill_io_error(
				"read",
				std::io::Error::new(ErrorKind::UnexpectedEof, "join spill page truncated"),
			));
		}
		out.extend_from_slice(&chunk);
	}
	Ok(out)
}

fn join_spill_io_error(operation: &'static str, error: std::io::Error) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("join spill {operation} failed: {error}"),
	}
}

fn encode_join_spill_page(entries: &[(Vec<u8>, Vec<u32>)]) -> Result<Vec<u8>, ExecutionError> {
	let mut out = Vec::new();
	out.extend_from_slice(
		&u32::try_from(entries.len())
			.map_err(|_| ExecutionError::Kernel {
				message: "join spill page has too many entries".to_string(),
			})?
			.to_le_bytes(),
	);
	for (join_key, rows) in entries {
		out.extend_from_slice(
			&u32::try_from(join_key.len())
				.map_err(|_| ExecutionError::Kernel {
					message: "join spill key is too large".to_string(),
				})?
				.to_le_bytes(),
		);
		out.extend_from_slice(join_key);
		out.extend_from_slice(
			&u32::try_from(rows.len())
				.map_err(|_| ExecutionError::Kernel {
					message: "join spill row list is too large".to_string(),
				})?
				.to_le_bytes(),
		);
		for row in rows {
			out.extend_from_slice(&row.to_le_bytes());
		}
	}
	Ok(out)
}

fn decode_join_spill_page_for_key(
	page: &[u8],
	lookup_key: &[u8],
) -> Result<Vec<u32>, ExecutionError> {
	let mut cursor = 0usize;
	let entries = read_u32_le(page, &mut cursor)? as usize;
	let mut rows = Vec::new();
	for _ in 0..entries {
		let key_len = read_u32_le(page, &mut cursor)? as usize;
		let key_end = cursor
			.checked_add(key_len)
			.ok_or_else(join_spill_decode_error)?;
		let key = page
			.get(cursor..key_end)
			.ok_or_else(join_spill_decode_error)?;
		cursor = key_end;
		let row_count = read_u32_le(page, &mut cursor)? as usize;
		for _ in 0..row_count {
			let row = read_u32_le(page, &mut cursor)?;
			if key == lookup_key {
				rows.push(row);
			}
		}
	}
	Ok(rows)
}

fn encode_record_batch_ipc(batch: &RecordBatch) -> Result<Vec<u8>, ExecutionError> {
	let mut buffer = Vec::new();
	encode_record_batch_ipc_into(&mut buffer, batch)?;
	Ok(buffer)
}

fn encode_record_batch_ipc_into(
	buffer: &mut Vec<u8>,
	batch: &RecordBatch,
) -> Result<(), ExecutionError> {
	buffer.clear();
	{
		let options =
			IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME))?;
		let mut writer =
			StreamWriter::try_new_with_options(&mut *buffer, &batch.schema(), options)?;
		writer.write(batch)?;
		writer.finish()?;
	}
	Ok(())
}

fn decode_record_batch_ipc(bytes: &[u8]) -> Result<RecordBatch, ExecutionError> {
	let cursor = Cursor::new(bytes);
	let mut reader = StreamReader::try_new(cursor, None)?;
	let Some(batch) = reader.next() else {
		return Err(ExecutionError::Kernel {
			message: "semi/anti existence spill page contained no batch".to_string(),
		});
	};
	let batch = batch?;
	if reader.next().is_some() {
		return Err(ExecutionError::Kernel {
			message: "semi/anti existence spill page contained multiple batches".to_string(),
		});
	}
	Ok(batch)
}

fn read_u32_le(page: &[u8], cursor: &mut usize) -> Result<u32, ExecutionError> {
	let end = cursor.checked_add(4).ok_or_else(join_spill_decode_error)?;
	let bytes = page.get(*cursor..end).ok_or_else(join_spill_decode_error)?;
	*cursor = end;
	Ok(u32::from_le_bytes(bytes.try_into().expect("four bytes")))
}

fn join_spill_decode_error() -> ExecutionError {
	ExecutionError::Kernel {
		message: "join spill page decode failed".to_string(),
	}
}

fn encoded_join_spill_entry_size(join_key: &[u8], rows: &[u32]) -> usize {
	std::mem::size_of::<u32>()
		.saturating_add(join_key.len())
		.saturating_add(std::mem::size_of::<u32>())
		.saturating_add(rows.len().saturating_mul(std::mem::size_of::<u32>()))
}

fn join_spill_index_bytes(index: &HashMap<Vec<u8>, Vec<usize>>) -> u64 {
	index
		.iter()
		.map(|(key, pages)| {
			u64::try_from(
				64usize
					.saturating_add(key.len())
					.saturating_add(pages.len().saturating_mul(std::mem::size_of::<usize>())),
			)
			.unwrap_or(u64::MAX)
		})
		.fold(0u64, u64::saturating_add)
}

fn append_spilled_batch_row_ref(
	index: &mut HashMap<JoinKey, Vec<SpilledBatchRowRef>>,
	resident_index_bytes: &mut u64,
	key: JoinKey,
	row_ref: SpilledBatchRowRef,
) {
	match index.entry(key) {
		Entry::Occupied(mut entry) => {
			entry.get_mut().push(row_ref);
		}
		Entry::Vacant(entry) => {
			*resident_index_bytes =
				resident_index_bytes.saturating_add(join_key_index_base_bytes(entry.key()));
			entry.insert(vec![row_ref]);
		}
	}
	*resident_index_bytes =
		resident_index_bytes.saturating_add(row_ref_size::<SpilledBatchRowRef>());
}

fn index_membership_key(
	index: &mut HashMap<JoinKey, Vec<SpilledExistenceRowRef>>,
	resident_index_bytes: &mut u64,
	key: JoinKey,
) {
	if let Entry::Vacant(entry) = index.entry(key) {
		*resident_index_bytes =
			resident_index_bytes.saturating_add(join_key_index_base_bytes(entry.key()));
		entry.insert(Vec::new());
	}
}

fn append_spilled_existence_row_ref(
	index: &mut HashMap<JoinKey, Vec<SpilledExistenceRowRef>>,
	resident_index_bytes: &mut u64,
	key: JoinKey,
	row_ref: SpilledExistenceRowRef,
) {
	match index.entry(key) {
		Entry::Occupied(mut entry) => {
			entry.get_mut().push(row_ref);
		}
		Entry::Vacant(entry) => {
			*resident_index_bytes =
				resident_index_bytes.saturating_add(join_key_index_base_bytes(entry.key()));
			entry.insert(vec![row_ref]);
		}
	}
	*resident_index_bytes =
		resident_index_bytes.saturating_add(row_ref_size::<SpilledExistenceRowRef>());
}

#[cfg(test)]
fn semi_anti_existence_spill_index_bytes(
	index: &HashMap<JoinKey, Vec<SpilledExistenceRowRef>>,
) -> u64 {
	index
		.iter()
		.map(|(key, refs)| {
			u64::try_from(
				64usize
					.saturating_add(join_key_heap_bytes(key))
					.saturating_add(
						refs.len()
							.saturating_mul(std::mem::size_of::<SpilledExistenceRowRef>()),
					),
			)
			.unwrap_or(u64::MAX)
		})
		.fold(0u64, u64::saturating_add)
}

fn spilled_batch_join_hash_index_bytes(index: &HashMap<JoinKey, Vec<SpilledBatchRowRef>>) -> u64 {
	index
		.iter()
		.map(|(key, refs)| {
			u64::try_from(
				64usize
					.saturating_add(join_key_heap_bytes(key))
					.saturating_add(
						refs.len()
							.saturating_mul(std::mem::size_of::<SpilledBatchRowRef>()),
					),
			)
			.unwrap_or(u64::MAX)
		})
		.fold(0u64, u64::saturating_add)
}

fn join_key_heap_bytes(key: &JoinKey) -> usize {
	match key {
		JoinKey::Int32(_) => std::mem::size_of::<i32>(),
		JoinKey::Int64(_) => std::mem::size_of::<i64>(),
		JoinKey::Generic(bytes) => bytes.len(),
	}
}

fn join_key_index_base_bytes(key: &JoinKey) -> u64 {
	u64::try_from(64usize.saturating_add(join_key_heap_bytes(key))).unwrap_or(u64::MAX)
}

fn row_ref_size<T>() -> u64 {
	u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX)
}

fn join_spill_nonce(spill_id: u64, page_index: usize) -> [u8; 12] {
	let mut nonce = [0u8; 12];
	nonce[..8].copy_from_slice(&spill_id.to_le_bytes());
	nonce[8..].copy_from_slice(&u32::try_from(page_index).unwrap_or(u32::MAX).to_le_bytes());
	nonce
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ir::{BoundExprProgramBuilder, ColumnName, ResolvedColumn};
	use std::io::{Error, ErrorKind};
	use std::path::Path;
	use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
	use wendelon::storage::{AlignedBuf, DirectoryEntry};

	// -----------------------------------------------------------------------------------------
	// ADR 601: the finish-built primitive index — its states, its pass, and its arithmetic.
	// -----------------------------------------------------------------------------------------

	fn adr601_ref(page_index: u32, row: u32) -> CompactSpilledBatchRowRef {
		CompactSpilledBatchRowRef { page_index, row }
	}

	fn adr601_refs(index: &PrimitiveSpilledBatchIndex<i64>, key: i64) -> Vec<(usize, u32)> {
		index
			.lookup(key, SpilledLookupForm::Collected)
			.into_iter()
			.map(|row_ref| (row_ref.page_index, row_ref.row))
			.collect()
	}

	/// Keys whose buckets, under the seeded hasher and `bucket_count` buckets, are the ones
	/// asked for — found by search, so a fixture can force collisions and wraparound.
	fn adr601_keys_at_buckets(
		hasher: &RandomState,
		bucket_count: usize,
		wanted: &[usize],
		exclude: &[i64],
	) -> Vec<i64> {
		let mut keys = Vec::with_capacity(wanted.len());
		let mut candidate = 1_000i64;
		for bucket in wanted {
			loop {
				candidate += 1;
				if !exclude.contains(&candidate)
					&& !keys.contains(&candidate)
					&& PrimitiveSpilledBatchIndex::<i64>::bucket_for_key(
						hasher,
						bucket_count,
						candidate,
					) == *bucket
				{
					keys.push(candidate);
					break;
				}
			}
		}
		keys
	}

	#[test]
	fn adr601_the_pass_keeps_refs_in_order_through_collisions_wraparound_and_moved_slots() {
		// Seven distinct keys and five repeats: 12 entries, so the pass plans 32 buckets.
		let seeds = (11, 22, 33, 44);
		let hasher = RandomState::with_seeds(seeds.0, seeds.1, seeds.2, seeds.3);
		let (bucket_count, _) = finish_index_plan(12).expect("plan");
		assert_eq!(bucket_count, 32);
		// a, b collide on the last bucket, so b probes past the end and wraps to bucket 0;
		// c sits on bucket 0 and is pushed to 1 by the wrapped b; d, e, f, g elsewhere.
		let last = bucket_count - 1;
		let keys =
			adr601_keys_at_buckets(&hasher, bucket_count, &[last, last, 0, 5, 9, 9, 17], &[]);
		let [a, b, c, d, e, f, g] = keys[..] else {
			panic!("seven keys");
		};
		// Insertion order: a, d, a(rep), b, c, e, d(rep), f, b(rep), g, a(rep), g(rep).
		// After a, d, and the repeat, b's slot moves down over the dropped repeat's slot; the
		// later repeats of b and a land on entries that were moved.
		let order = [
			(a, 0, 0),
			(d, 0, 1),
			(a, 0, 2),
			(b, 1, 0),
			(c, 1, 1),
			(e, 1, 2),
			(d, 2, 0),
			(f, 2, 1),
			(b, 2, 2),
			(g, 3, 0),
			(a, 3, 1),
			(g, 3, 2),
		];
		let mut index = PrimitiveSpilledBatchIndex::<i64>::reserved(12);
		index.seed_hasher(seeds);
		assert!(index.is_collecting());
		assert!(index.buckets.is_empty(), "no buckets while collecting");
		for (key, page, row) in order {
			index.insert(key, adr601_ref(page, row)).expect("collect");
		}
		assert_eq!(index.entries.len(), 12);
		let stats = index.finalize().expect("finalize");
		assert_eq!(
			(
				stats.entries,
				stats.duplicates,
				stats.buckets,
				stats.growths
			),
			(7, 5, 32, 0)
		);
		assert_eq!(index.entries.len(), 7, "truncated to the distinct keys");
		assert!(index.entries.capacity() >= 12, "capacity kept");
		// Every bucket points below the cursor, and every entry is reachable.
		for slot in &index.buckets {
			assert!(*slot == EMPTY_SPILLED_BATCH_BUCKET || (*slot as usize) < 7);
		}
		assert_eq!(index.buckets[last], 0, "a took the last bucket, entry 0");
		assert_eq!(adr601_refs(&index, a), vec![(0, 0), (0, 2), (3, 1)]);
		assert_eq!(adr601_refs(&index, b), vec![(1, 0), (2, 2)]);
		assert_eq!(adr601_refs(&index, c), vec![(1, 1)]);
		assert_eq!(adr601_refs(&index, d), vec![(0, 1), (2, 0)]);
		assert_eq!(adr601_refs(&index, e), vec![(1, 2)]);
		assert_eq!(adr601_refs(&index, f), vec![(2, 1)]);
		assert_eq!(adr601_refs(&index, g), vec![(3, 0), (3, 2)]);
		assert!(adr601_refs(&index, 7).is_empty(), "an absent key misses");
		// The same rows through the incremental index, for parity of the refs and their order.
		let mut incremental = PrimitiveSpilledBatchIndex::<i64>::with_capacity(0);
		incremental.seed_hasher(seeds);
		for (key, page, row) in order {
			incremental
				.insert(key, adr601_ref(page, row))
				.expect("insert");
		}
		for key in [a, b, c, d, e, f, g] {
			assert_eq!(adr601_refs(&index, key), adr601_refs(&incremental, key));
		}
		assert_eq!(index.row_ref_count(), 12);
		assert_eq!(
			index.accounted_bytes(),
			index.computed_bytes(),
			"accounted from the obtained capacities and the overflow heap"
		);
	}

	#[test]
	fn adr601_the_states_refuse_what_they_must() {
		let mut index = PrimitiveSpilledBatchIndex::<i64>::reserved(2);
		index.insert(1, adr601_ref(0, 0)).expect("collect");
		index.finalize().expect("finalize");
		assert!(!index.is_collecting());
		let again = index.finalize().expect_err("a second finalization");
		assert!(again.to_string().contains("finalized twice"), "{again}");
		let write = index
			.insert(2, adr601_ref(0, 1))
			.expect_err("a write after finalization");
		assert!(write.to_string().contains("after finalization"), "{write}");
		assert_eq!(adr601_refs(&index, 1), vec![(0, 0)]);
		// An empty collection finalizes to one bucket and misses everything.
		let mut empty = PrimitiveSpilledBatchIndex::<i64>::reserved(0);
		let stats = empty.finalize().expect("finalize empty");
		assert_eq!((stats.entries, stats.duplicates, stats.buckets), (0, 0, 1));
		assert!(adr601_refs(&empty, 1).is_empty());
		// A short reservation grows, counted; the accounting follows the obtained capacity.
		let mut short = PrimitiveSpilledBatchIndex::<i64>::reserved(1);
		for key in 0..5 {
			short
				.insert(key, adr601_ref(0, key as u32))
				.expect("collect");
		}
		let stats = short.finalize().expect("finalize");
		assert!(stats.growths >= 1, "{stats:?}");
		assert_eq!(stats.entries, 5);
		assert_eq!(short.accounted_bytes(), short.computed_bytes());
	}

	#[test]
	fn adr601_the_plan_refuses_unrepresentable_counts_without_allocating() {
		assert_eq!(finish_index_plan(0).expect("zero"), (1, 4));
		assert_eq!(finish_index_plan(1).expect("one"), (2, 8));
		assert_eq!(
			finish_index_plan(5_845_938).expect("the measured build"),
			(1 << 24, 4 << 24)
		);
		let at_sentinel = finish_index_plan(EMPTY_SPILLED_BATCH_BUCKET as usize)
			.expect_err("the sentinel is not an entry index");
		assert!(
			at_sentinel.to_string().contains("u32 bucket range"),
			"{at_sentinel}"
		);
		assert!(finish_index_plan((EMPTY_SPILLED_BATCH_BUCKET as usize) - 1).is_ok());
		assert!(finish_index_plan(usize::MAX).is_err());
		assert!(reserved_entry_bytes::<i64>(usize::MAX).is_err());
		assert_eq!(reserved_entry_bytes::<i64>(10).expect("ten"), 240);
	}

	#[derive(Clone, Copy, Debug, Eq, PartialEq)]
	enum CoalescedFileFault {
		Write,
		Finish,
		Cleanup,
	}

	struct CoalescedFaultFileOps {
		inner: BlockingFileOps,
		fault: CoalescedFileFault,
		fired: AtomicBool,
	}

	impl CoalescedFaultFileOps {
		fn new(fault: CoalescedFileFault) -> Self {
			Self {
				inner: BlockingFileOps,
				fault,
				fired: AtomicBool::new(false),
			}
		}

		fn should_fail(&self, fault: CoalescedFileFault) -> bool {
			self.fault == fault && !self.fired.swap(true, AtomicOrdering::SeqCst)
		}

		fn fired(&self) -> bool {
			self.fired.load(AtomicOrdering::SeqCst)
		}
	}

	#[async_trait::async_trait]
	impl AsyncFileOps for CoalescedFaultFileOps {
		async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
			self.inner.open(path, flags).await
		}

		async fn read_plain_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			len: usize,
		) -> std::io::Result<Vec<u8>> {
			self.inner.read_plain_at(handle, offset, len).await
		}

		async fn write_plain_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			bytes: Vec<u8>,
		) -> std::io::Result<usize> {
			if self.should_fail(CoalescedFileFault::Write) {
				return Err(Error::new(ErrorKind::Other, "injected coalesced write"));
			}
			self.inner.write_plain_at(handle, offset, bytes).await
		}

		async fn read_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			buf: AlignedBuf,
		) -> std::io::Result<(usize, AlignedBuf)> {
			self.inner.read_at(handle, offset, buf).await
		}

		async fn write_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			buf: AlignedBuf,
		) -> std::io::Result<(usize, AlignedBuf)> {
			self.inner.write_at(handle, offset, buf).await
		}

		async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.inner.fsync(handle).await
		}

		async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
			if self.should_fail(CoalescedFileFault::Finish) {
				return Err(Error::new(ErrorKind::Other, "injected coalesced finish"));
			}
			self.inner.fdatasync(handle).await
		}

		async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>> {
			self.inner.read_all(path).await
		}

		async fn write_all(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
			self.inner.write_all(path, bytes).await
		}

		async fn file_len(&self, path: &Path) -> std::io::Result<Option<u64>> {
			self.inner.file_len(path).await
		}

		async fn set_len(&self, handle: &FileHandle, len: u64) -> std::io::Result<()> {
			self.inner.set_len(handle, len).await
		}

		async fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
			self.inner.create_dir_all(path).await
		}

		async fn read_dir(&self, path: &Path) -> std::io::Result<Vec<DirectoryEntry>> {
			self.inner.read_dir(path).await
		}

		async fn fsync_dir(&self, path: &Path) -> std::io::Result<()> {
			self.inner.fsync_dir(path).await
		}

		async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
			self.inner.rename(from, to).await
		}

		async fn unlink(&self, path: &Path) -> std::io::Result<()> {
			if self.should_fail(CoalescedFileFault::Cleanup) {
				return Err(Error::new(ErrorKind::Other, "injected coalesced cleanup"));
			}
			self.inner.unlink(path).await
		}
	}

	fn test_task_budget() -> TaskBudget {
		let mut query_budget = QueryBudget::test_default();
		query_budget.memory_per_task = ByteCount::new(64 * 1024 * 1024);
		query_budget.spill_bytes_reserved_per_query = ByteCount::new(512 * 1024 * 1024);
		TaskBudget::from_query_budget(query_budget)
	}

	fn test_task_budget_with_memory(memory: u64) -> TaskBudget {
		let mut query_budget = QueryBudget::test_default();
		query_budget.memory_per_task = ByteCount::new(memory);
		query_budget.spill_bytes_reserved_per_query = ByteCount::new(512 * 1024 * 1024);
		TaskBudget::from_query_budget(query_budget)
	}

	fn test_spill_dir(name: &str) -> PathBuf {
		let id = NEXT_JOIN_SPILL_ID.fetch_add(1, Ordering::SeqCst);
		std::env::temp_dir().join(format!(
			"toron_spill_accounting_{name}_{}_{}",
			std::process::id(),
			id
		))
	}

	fn batch_with_keys(keys: &[i64]) -> RecordBatch {
		let values = (0..keys.len())
			.map(|row| i64::try_from(row).unwrap_or(i64::MAX).saturating_mul(10))
			.collect::<Vec<_>>();
		let extras = (0..keys.len())
			.map(|row| i64::try_from(row).unwrap_or(i64::MAX).saturating_mul(100))
			.collect::<Vec<_>>();
		let schema = Arc::new(Schema::new(vec![
			arrow::datatypes::Field::new("k", DataType::Int64, false),
			arrow::datatypes::Field::new("v", DataType::Int64, false),
			arrow::datatypes::Field::new("x", DataType::Int64, false),
		]));
		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef,
				Arc::new(Int64Array::from(values)) as ArrayRef,
				Arc::new(Int64Array::from(extras)) as ArrayRef,
			],
		)
		.expect("test batch")
	}

	fn nullable_batch_with_keys(keys: &[Option<i64>]) -> RecordBatch {
		let values = keys
			.iter()
			.enumerate()
			.map(|(row, key)| {
				key.map(|_| i64::try_from(row).unwrap_or(i64::MAX).saturating_mul(10))
			})
			.collect::<Vec<_>>();
		let extras = keys
			.iter()
			.enumerate()
			.map(|(row, key)| {
				key.map(|_| i64::try_from(row).unwrap_or(i64::MAX).saturating_mul(100))
			})
			.collect::<Vec<_>>();
		let schema = Arc::new(Schema::new(vec![
			arrow::datatypes::Field::new("k", DataType::Int64, true),
			arrow::datatypes::Field::new("v", DataType::Int64, true),
			arrow::datatypes::Field::new("x", DataType::Int64, true),
		]));
		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef,
				Arc::new(Int64Array::from(values)) as ArrayRef,
				Arc::new(Int64Array::from(extras)) as ArrayRef,
			],
		)
		.expect("nullable test batch")
	}

	fn batch_with_int32_keys(keys: &[Option<i32>]) -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![
			arrow::datatypes::Field::new("k", DataType::Int32, true),
			arrow::datatypes::Field::new("v", DataType::Int64, false),
		]));
		let values = (0..keys.len())
			.map(|row| i64::try_from(row).unwrap_or(i64::MAX))
			.collect::<Vec<_>>();
		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(arrow::array::Int32Array::from(keys.to_vec())) as ArrayRef,
				Arc::new(Int64Array::from(values)) as ArrayRef,
			],
		)
		.expect("Int32 test batch")
	}

	async fn coalesced_test_build(
		label: &str,
		batches: &[RecordBatch],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		mode: CoalescedPageIndexMode,
	) -> (SpilledInnerJoinBuild, PathBuf) {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir(label);
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create coalesced spill");
		for batch in batches {
			build
				.write_batch_chunks_coalesced_profiled(
					batch,
					key_plan,
					&[ColumnOrdinal(0)],
					null_safe_keys,
					mode,
					budget,
					None,
				)
				.await
				.expect("write coalesced spill batch");
		}
		build
			.flush_coalesced_page_buffer(
				key_plan,
				&[ColumnOrdinal(0)],
				null_safe_keys,
				mode,
				budget,
				None,
			)
			.await
			.expect("flush coalesced spill tail");
		build.finish(budget).await.expect("finish coalesced spill");
		build
			.complete_coalesced_index_after_finish(
				key_plan,
				&[ColumnOrdinal(0)],
				null_safe_keys,
				mode,
				budget,
				None,
			)
			.await
			.expect("complete coalesced index");
		(build, spill_dir)
	}

	async fn coalesced_projected_test_build(
		label: &str,
		batches: &[RecordBatch],
		mode: CoalescedPageIndexMode,
	) -> (SpilledInnerJoinBuild, PathBuf) {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir(label);
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create projected coalesced spill");
		for batch in batches {
			build
				.write_projected_batch_chunks_coalesced_profiled(
					batch,
					&[ColumnOrdinal(0), ColumnOrdinal(2)],
					JoinKeyPlan::Int64,
					&[ColumnOrdinal(0)],
					false,
					mode,
					budget,
					None,
				)
				.await
				.expect("write projected coalesced spill");
		}
		build
			.flush_coalesced_page_buffer(
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				mode,
				budget,
				None,
			)
			.await
			.expect("flush projected coalesced spill");
		build.finish(budget).await.expect("finish projected spill");
		build
			.complete_coalesced_index_after_finish(
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				mode,
				budget,
				None,
			)
			.await
			.expect("complete projected coalesced index");
		(build, spill_dir)
	}

	#[tokio::test]
	async fn coalesced_index_at_flush_matches_rebuild_across_full_and_partial_pages() {
		let mut keys = (0..CROSS_JOIN_OUTPUT_CHUNK_ROWS)
			.map(|row| match row % 7 {
				0 => None,
				1 | 2 => Some(11),
				_ => Some(i64::try_from(row % 97).expect("small key")),
			})
			.collect::<Vec<_>>();
		keys.extend([
			Some(11),
			None,
			Some(42),
			Some(42),
			Some(7),
			Some(11),
			None,
			Some(99),
			Some(7),
			Some(42),
			Some(101),
			Some(11),
			Some(99),
			None,
			Some(7),
			Some(42),
			Some(11),
			Some(5),
			Some(5),
			None,
			Some(42),
			Some(7),
			Some(11),
		]);
		let batch = nullable_batch_with_keys(&keys);
		let batches = [
			batch.slice(0, CROSS_JOIN_OUTPUT_CHUNK_ROWS),
			batch.slice(CROSS_JOIN_OUTPUT_CHUNK_ROWS, 7),
			batch.slice(CROSS_JOIN_OUTPUT_CHUNK_ROWS + 7, 16),
		];
		let (mut control, control_dir) = coalesced_test_build(
			"coalesced_control",
			&batches,
			JoinKeyPlan::Int64,
			false,
			CoalescedPageIndexMode::RebuildAfterWrite,
		)
		.await;
		let (mut candidate, candidate_dir) = coalesced_test_build(
			"coalesced_candidate",
			&batches,
			JoinKeyPlan::Int64,
			false,
			CoalescedPageIndexMode::IndexAtFlush,
		)
		.await;

		assert_eq!(control.page_geometry(), candidate.page_geometry());
		assert_eq!(control.spill_bytes(), candidate.spill_bytes());
		assert_eq!(control.indexed_flush_pages(), 0);
		assert_eq!(control.index_rebuild_calls(), 1);
		assert!(control.physical_read_pages() > 0);
		assert_eq!(
			candidate.indexed_flush_pages(),
			candidate.page_count() as u64
		);
		assert_eq!(
			candidate.indexed_flush_guard_checks(),
			candidate.indexed_flush_pages()
		);
		assert_eq!(candidate.index_rebuild_calls(), 0);
		assert_eq!(candidate.physical_read_pages(), 0);
		assert_eq!(control.page_decode_count(), control.page_count() as u64);
		assert_eq!(candidate.page_decode_count(), 0);
		assert!(control.uses_compact_int64_index());
		assert!(candidate.uses_compact_int64_index());
		assert!(control.scratch_plaintext.is_empty());
		assert!(control.scratch_refs.is_empty());
		assert!(candidate.scratch_plaintext.is_empty());
		assert!(candidate.scratch_refs.is_empty());
		for key in [5, 7, 11, 42, 99, 101, 10_000] {
			assert_eq!(
				control
					.lookup(&JoinKey::Int64(key))
					.await
					.expect("control lookup"),
				candidate
					.lookup(&JoinKey::Int64(key))
					.await
					.expect("candidate lookup"),
				"lookup multiset differs for key {key}"
			);
		}
		for page in 0..control.page_count() {
			let control_page = control.read_batch_page(page).await.expect("control page");
			let candidate_page = candidate
				.read_batch_page(page)
				.await
				.expect("candidate page");
			assert_record_batch_equal(&control_page, &candidate_page);
		}
		candidate.rows = candidate.rows.saturating_add(1);
		assert!(
			candidate
				.assert_coalesced_index_complete()
				.expect_err("missing indexed row must fail closed")
				.to_string()
				.contains("incomplete")
		);
		candidate.rows = candidate.rows.saturating_sub(1);

		control.cleanup().await;
		candidate.cleanup().await;
		assert_eq!(std::fs::read_dir(&control_dir).unwrap().count(), 0);
		assert_eq!(std::fs::read_dir(&candidate_dir).unwrap().count(), 0);
		let _ = std::fs::remove_dir_all(control_dir);
		let _ = std::fs::remove_dir_all(candidate_dir);
	}

	#[tokio::test]
	async fn coalesced_index_at_flush_matches_rebuild_when_fragments_cross_page_capacity() {
		let keys = (0..(CROSS_JOIN_OUTPUT_CHUNK_ROWS + 808))
			.map(|row| i64::try_from(row % 31).expect("small key"))
			.collect::<Vec<_>>();
		let batch = batch_with_keys(&keys);
		let batches = [batch.slice(0, 5_000), batch.slice(5_000, 4_000)];
		let (mut control, control_dir) = coalesced_test_build(
			"coalesced_crossing_control",
			&batches,
			JoinKeyPlan::Int64,
			false,
			CoalescedPageIndexMode::RebuildAfterWrite,
		)
		.await;
		let (mut candidate, candidate_dir) = coalesced_test_build(
			"coalesced_crossing_candidate",
			&batches,
			JoinKeyPlan::Int64,
			false,
			CoalescedPageIndexMode::IndexAtFlush,
		)
		.await;
		assert_eq!(control.page_count(), 2);
		assert_eq!(control.page_geometry(), candidate.page_geometry());
		assert_eq!(control.spill_bytes(), candidate.spill_bytes());
		for key in 0..31 {
			assert_eq!(
				control
					.lookup(&JoinKey::Int64(key))
					.await
					.expect("control crossing lookup"),
				candidate
					.lookup(&JoinKey::Int64(key))
					.await
					.expect("candidate crossing lookup")
			);
		}
		control.cleanup().await;
		candidate.cleanup().await;
		let _ = std::fs::remove_dir_all(control_dir);
		let _ = std::fs::remove_dir_all(candidate_dir);
	}

	#[tokio::test]
	async fn coalesced_index_at_flush_uses_compact_int32_and_hash_for_null_safe_generic() {
		let int32_batch =
			batch_with_int32_keys(&[Some(3), None, Some(3), Some(-1), Some(9), Some(9)]);
		let (mut int32, int32_dir) = coalesced_test_build(
			"coalesced_int32",
			&[int32_batch],
			JoinKeyPlan::Int32,
			false,
			CoalescedPageIndexMode::IndexAtFlush,
		)
		.await;
		assert!(matches!(
			int32.compact_index,
			Some(SpilledBatchIndex::Int32(_))
		));
		assert_eq!(
			int32
				.lookup(&JoinKey::Int32(3))
				.await
				.expect("Int32 lookup")
				.len(),
			2
		);
		assert_eq!(int32.page_decode_count(), 0);

		let generic_batch =
			nullable_batch_with_keys(&[Some(3), None, Some(3), Some(-1), None, Some(9)]);
		let (generic, generic_dir) = coalesced_test_build(
			"coalesced_generic_null_safe",
			&[generic_batch],
			JoinKeyPlan::Generic,
			true,
			CoalescedPageIndexMode::IndexAtFlush,
		)
		.await;
		assert!(generic.uses_hash_index());
		assert_eq!(generic.index_rebuild_calls(), 0);
		assert_eq!(generic.page_decode_count(), 0);
		generic
			.assert_coalesced_index_complete()
			.expect("null-safe hash index is complete");

		int32.cleanup().await;
		generic.cleanup().await;
		let _ = std::fs::remove_dir_all(int32_dir);
		let _ = std::fs::remove_dir_all(generic_dir);
	}

	#[tokio::test]
	async fn coalesced_index_at_flush_preserves_projected_page_and_lookup_geometry() {
		let batch = batch_with_keys(&[5, 5, 7, 9, 5, 7, 11, 13, 13]);
		let batches = [batch.slice(0, 4), batch.slice(4, 5)];
		let (mut control, control_dir) = coalesced_projected_test_build(
			"coalesced_projected_control",
			&batches,
			CoalescedPageIndexMode::RebuildAfterWrite,
		)
		.await;
		let (mut candidate, candidate_dir) = coalesced_projected_test_build(
			"coalesced_projected_candidate",
			&batches,
			CoalescedPageIndexMode::IndexAtFlush,
		)
		.await;
		assert_eq!(control.page_geometry(), candidate.page_geometry());
		for key in [5, 7, 9, 11, 13] {
			assert_eq!(
				control
					.lookup(&JoinKey::Int64(key))
					.await
					.expect("control projected lookup"),
				candidate
					.lookup(&JoinKey::Int64(key))
					.await
					.expect("candidate projected lookup")
			);
		}
		let page = candidate.read_batch_page(0).await.expect("projected page");
		assert_eq!(page.num_columns(), 2);
		assert_eq!(control.index_rebuild_calls(), 1);
		assert_eq!(candidate.index_rebuild_calls(), 0);
		control.cleanup().await;
		candidate.cleanup().await;
		let _ = std::fs::remove_dir_all(control_dir);
		let _ = std::fs::remove_dir_all(candidate_dir);
	}

	#[tokio::test]
	async fn coalesced_index_at_flush_rejects_mode_changes_and_tight_live_residency() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("coalesced_mode_change");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create mode-change spill");
		let batch = batch_with_keys(&[1, 2, 3, 4]);
		build
			.write_batch_chunks_coalesced_profiled(
				&batch,
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				CoalescedPageIndexMode::IndexAtFlush,
				budget,
				None,
			)
			.await
			.expect("first mode");
		let error = build
			.write_batch_chunks_coalesced_profiled(
				&batch,
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				CoalescedPageIndexMode::RebuildAfterWrite,
				budget,
				None,
			)
			.await
			.expect_err("mode cannot change after writing starts");
		assert!(error.to_string().contains("changed index mode mid-build"));
		build.cleanup().await;

		let tight_dir = test_spill_dir("coalesced_tight_guard");
		let tight_budget = test_task_budget_with_memory(1);
		let mut tight = SpilledInnerJoinBuild::create(
			tight_budget,
			Arc::new(BlockingFileOps),
			tight_dir.clone(),
		)
		.await
		.expect("create tight spill");
		let tight_batch = batch_with_keys(&vec![1; CROSS_JOIN_OUTPUT_CHUNK_ROWS]);
		let error = tight
			.write_batch_chunks_coalesced_profiled(
				&tight_batch,
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				CoalescedPageIndexMode::IndexAtFlush,
				tight_budget,
				None,
			)
			.await
			.expect_err("live index plus writer must respect the checked resident guard");
		assert!(matches!(error, ExecutionError::BudgetExhausted { .. }));
		tight.cleanup().await;
		assert_eq!(std::fs::read_dir(&spill_dir).unwrap().count(), 0);
		assert_eq!(std::fs::read_dir(&tight_dir).unwrap().count(), 0);
		let _ = std::fs::remove_dir_all(spill_dir);
		let _ = std::fs::remove_dir_all(tight_dir);
	}

	#[tokio::test]
	async fn coalesced_index_at_flush_handles_empty_all_null_and_two_partition_builds() {
		let budget = test_task_budget();
		let empty_dir = test_spill_dir("coalesced_empty_build");
		let mut empty =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), empty_dir.clone())
				.await
				.expect("create empty spill");
		empty.finish(budget).await.expect("finish empty spill");
		empty
			.complete_coalesced_index_after_finish(
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				CoalescedPageIndexMode::IndexAtFlush,
				budget,
				None,
			)
			.await
			.expect("complete empty candidate");
		assert_eq!(empty.page_count(), 0);
		assert_eq!(empty.indexed_flush_pages(), 0);
		assert_eq!(empty.indexed_flush_guard_checks(), 0);
		assert_eq!(empty.input_row_count(), 0);

		let all_null = nullable_batch_with_keys(&[None, None, None]);
		let keyed = nullable_batch_with_keys(&[Some(7), None, Some(7), Some(9)]);
		let mut builds = Vec::new();
		let mut dirs = Vec::new();
		for (partition, batches) in [
			(0usize, vec![all_null.clone(), keyed.clone()]),
			(
				1usize,
				vec![nullable_batch_with_keys(&[Some(11), Some(11), None])],
			),
		] {
			let (build, dir) = coalesced_test_build(
				&format!("coalesced_partition_{partition}"),
				&batches,
				JoinKeyPlan::Int64,
				false,
				CoalescedPageIndexMode::IndexAtFlush,
			)
			.await;
			assert_eq!(
				build.indexed_flush_pages(),
				build.indexed_flush_guard_checks()
			);
			builds.push(build);
			dirs.push(dir);
		}
		assert_eq!(
			builds[0]
				.lookup(&JoinKey::Int64(7))
				.await
				.expect("partition zero lookup")
				.len(),
			2
		);
		assert_eq!(
			builds[1]
				.lookup(&JoinKey::Int64(11))
				.await
				.expect("partition one lookup")
				.len(),
			2
		);
		assert_eq!(builds[0].page_count(), 1);
		assert_eq!(builds[1].page_count(), 1);

		empty.cleanup().await;
		for build in builds {
			build.cleanup().await;
		}
		assert_eq!(std::fs::read_dir(&empty_dir).unwrap().count(), 0);
		let _ = std::fs::remove_dir_all(empty_dir);
		for dir in dirs {
			assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
			let _ = std::fs::remove_dir_all(dir);
		}
	}

	#[tokio::test]
	async fn coalesced_index_at_flush_fails_closed_on_write_finish_insert_and_cleanup_errors() {
		let budget = test_task_budget();
		let full_batch = batch_with_keys(&vec![7; CROSS_JOIN_OUTPUT_CHUNK_ROWS]);

		let write_dir = test_spill_dir("coalesced_write_failure");
		let write_ops = Arc::new(CoalescedFaultFileOps::new(CoalescedFileFault::Write));
		let mut write_build =
			SpilledInnerJoinBuild::create(budget, write_ops.clone(), write_dir.clone())
				.await
				.expect("create write-failure spill");
		let error = write_build
			.write_batch_chunks_coalesced_profiled(
				&full_batch,
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				CoalescedPageIndexMode::IndexAtFlush,
				budget,
				None,
			)
			.await
			.expect_err("write failure must propagate");
		assert!(error.to_string().contains("write"));
		assert!(write_ops.fired());
		write_build.release_write_scratch();
		assert!(write_build.scratch_plaintext.is_empty());
		assert!(write_build.scratch_refs.is_empty());
		write_build.cleanup().await;

		let finish_dir = test_spill_dir("coalesced_finish_failure");
		let finish_ops = Arc::new(CoalescedFaultFileOps::new(CoalescedFileFault::Finish));
		let mut finish_build =
			SpilledInnerJoinBuild::create(budget, finish_ops.clone(), finish_dir.clone())
				.await
				.expect("create finish-failure spill");
		finish_build
			.write_batch_chunks_coalesced_profiled(
				&batch_with_keys(&[1, 2, 3]),
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				CoalescedPageIndexMode::IndexAtFlush,
				budget,
				None,
			)
			.await
			.expect("buffer finish-failure rows");
		finish_build
			.flush_coalesced_page_buffer(
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				CoalescedPageIndexMode::IndexAtFlush,
				budget,
				None,
			)
			.await
			.expect("flush before finish failure");
		assert!(
			finish_build
				.finish(budget)
				.await
				.expect_err("fdatasync failure must propagate")
				.to_string()
				.contains("fdatasync")
		);
		assert!(finish_ops.fired());
		finish_build.release_write_scratch();
		assert!(finish_build.scratch_plaintext.is_empty());
		assert!(finish_build.scratch_refs.is_empty());
		finish_build.cleanup().await;

		let insert_dir = test_spill_dir("coalesced_insert_failure");
		let mut insert_build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), insert_dir.clone())
				.await
				.expect("create insert-failure spill");
		insert_build.fail_candidate_index_insert_at = Some(1);
		assert!(
			insert_build
				.write_batch_chunks_coalesced_profiled(
					&full_batch,
					JoinKeyPlan::Int64,
					&[ColumnOrdinal(0)],
					false,
					CoalescedPageIndexMode::IndexAtFlush,
					budget,
					None,
				)
				.await
				.expect_err("index insertion failure must propagate")
				.to_string()
				.contains("injected coalesced index insertion")
		);
		assert_eq!(
			insert_build.coalesced_index_mode,
			Some(CoalescedPageIndexMode::IndexAtFlush)
		);
		assert_eq!(insert_build.index_rebuild_calls(), 0);
		insert_build.release_write_scratch();
		assert!(insert_build.scratch_plaintext.is_empty());
		assert!(insert_build.scratch_refs.is_empty());
		insert_build.cleanup().await;

		let cleanup_dir = test_spill_dir("coalesced_cleanup_failure");
		let cleanup_ops = Arc::new(CoalescedFaultFileOps::new(CoalescedFileFault::Cleanup));
		let cleanup_build =
			SpilledInnerJoinBuild::create(budget, cleanup_ops.clone(), cleanup_dir.clone())
				.await
				.expect("create cleanup-failure spill");
		cleanup_build.cleanup().await;
		assert!(cleanup_ops.fired());

		for dir in [write_dir, finish_dir, insert_dir, cleanup_dir] {
			let _ = std::fs::remove_dir_all(dir);
		}
	}

	fn encoded_int64_join_key(key: i64) -> Vec<u8> {
		let mut encoded = Vec::with_capacity(1 + std::mem::size_of::<i64>());
		encoded.push(JOIN_SPILL_INT64_KEY_TAG);
		encoded.extend_from_slice(&key.to_le_bytes());
		encoded
	}

	fn encoded_int32_join_key(key: i32) -> Vec<u8> {
		let mut encoded = Vec::with_capacity(1 + std::mem::size_of::<i32>());
		encoded.push(JOIN_SPILL_INT32_KEY_TAG);
		encoded.extend_from_slice(&key.to_le_bytes());
		encoded
	}

	fn legacy_join_spill_rows(keys: usize) -> HashMap<Vec<u8>, Vec<u32>> {
		(0..keys)
			.map(|key| {
				(
					encoded_int64_join_key(key as i64),
					vec![u32::try_from(key).expect("test row index")],
				)
			})
			.collect()
	}

	fn joined_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
		Arc::new(Schema::new(
			left.fields()
				.iter()
				.chain(right.fields())
				.map(|field| field.as_ref().clone())
				.collect::<Vec<_>>(),
		))
	}

	fn less_than_join_residual(
		left_ordinal: ColumnOrdinal,
		right_ordinal: ColumnOrdinal,
	) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let left = builder.append_column(ResolvedColumn {
			name: ColumnName("left_value".to_string()),
			ordinal: left_ordinal,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let right = builder.append_column(ResolvedColumn {
			name: ColumnName("right_value".to_string()),
			ordinal: right_ordinal,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let predicate = builder.append_binary(BoundBinaryOp::Lt, left, right, DataType::Boolean);
		builder.push_root(predicate);
		builder.build()
	}

	#[test]
	fn compact_join_spill_page_locator_fences_and_accounts() {
		assert_eq!(std::mem::size_of::<CompactJoinSpillPageEntry<i32>>(), 8);
		assert_eq!(std::mem::size_of::<CompactJoinSpillPageEntry<i64>>(), 16);
		assert_eq!(
			4_194_304usize
				.saturating_mul(std::mem::size_of::<u32>())
				.saturating_add(
					2_097_152usize
						.saturating_mul(std::mem::size_of::<PrimitiveSpilledBatchIndexEntry<i64>>()),
				),
			64 * 1024 * 1024,
			"the ref-list entry consumes the full task budget before writer terms"
		);

		let mut int64 = JoinSpillPageLocator::for_key_plan(JoinKeyPlan::Int64, 100, false);
		for key in 0..100i64 {
			int64
				.insert(&encoded_int64_join_key(key), key as usize, &[key as u32])
				.expect("insert compact Int64 page");
		}
		assert_eq!(int64.legacy_insert_count, 0);
		let JoinSpillPageLocatorStorage::Int64Pages(int64_index) = &int64.storage else {
			panic!("expected compact Int64 locator");
		};
		assert_eq!(int64_index.lookup(42), Some(42));
		assert_eq!(int64_index.lookup(101), None);
		let computed = u64::try_from(
			int64_index
				.buckets
				.capacity()
				.saturating_mul(std::mem::size_of::<u32>())
				.saturating_add(
					int64_index
						.entries
						.capacity()
						.saturating_mul(std::mem::size_of::<CompactJoinSpillPageEntry<i64>>()),
				),
		)
		.expect("computed locator bytes");
		assert_eq!(int64.accounted_bytes(), computed);
		let before_shrink = int64.accounted_bytes();
		let JoinSpillPageLocatorStorage::Int64Pages(int64_index) = &mut int64.storage else {
			unreachable!("Int64 locator kind already asserted");
		};
		int64_index.entries.shrink_to_fit();
		int64_index.buckets.shrink_to_fit();
		let after_shrink = int64_index.accounted_bytes();
		assert!(after_shrink < before_shrink);
		assert_eq!(int64.accounted_bytes(), after_shrink);

		let mut int32 = JoinSpillPageLocator::for_key_plan(JoinKeyPlan::Int32, 1, false);
		int32
			.insert(&encoded_int32_join_key(7), 3, &[7])
			.expect("insert compact Int32 page");
		assert_eq!(
			int32.lookup(&encoded_int32_join_key(7)).unwrap(),
			JoinSpillLocatorLookup::Page(3)
		);

		let mut fallback = JoinSpillPageLocator::for_key_plan(JoinKeyPlan::Generic, 1, false);
		fallback
			.insert(b"generic-key", 9, &[7])
			.expect("insert encoded fallback page");
		assert_eq!(fallback.legacy_insert_count, 1);
		assert_eq!(
			fallback.lookup(b"generic-key").unwrap(),
			JoinSpillLocatorLookup::Page(9)
		);

		let mut int64_rows = JoinSpillPageLocator::for_key_plan(JoinKeyPlan::Int64, 100, true);
		for key in 0..100i64 {
			int64_rows
				.insert(&encoded_int64_join_key(key), 999, &[key as u32])
				.expect("insert compact Int64 row");
		}
		assert!(matches!(
			int64_rows.storage,
			JoinSpillPageLocatorStorage::Int64Rows(_)
		));
		assert_eq!(
			int64_rows.lookup(&encoded_int64_join_key(42)).unwrap(),
			JoinSpillLocatorLookup::Row(42)
		);
		assert_eq!(int64_rows.accounted_bytes(), before_shrink);
		assert_eq!(
			JoinSpillRows::NoMatch.into_iter().collect::<Vec<_>>(),
			Vec::<u32>::new()
		);
		assert_eq!(
			JoinSpillRows::OneRow(17).into_iter().collect::<Vec<_>>(),
			vec![17]
		);
		assert_eq!(
			JoinSpillRows::Rows(vec![3, 5])
				.into_iter()
				.collect::<Vec<_>>(),
			vec![3, 5]
		);

		let mut int32_rows = JoinSpillPageLocator::for_key_plan(JoinKeyPlan::Int32, 1, true);
		int32_rows
			.insert(&encoded_int32_join_key(7), 999, &[11])
			.expect("insert compact Int32 row");
		assert_eq!(
			int32_rows.lookup(&encoded_int32_join_key(7)).unwrap(),
			JoinSpillLocatorLookup::Row(11)
		);

		assert!(int64.lookup(&encoded_int32_join_key(42)).is_err());
		assert!(int64.lookup(&encoded_int64_join_key(42)[..8]).is_err());
		let mut truncated = JoinSpillPageLocator::for_key_plan(JoinKeyPlan::Int64, 1, false);
		assert!(
			truncated
				.insert(&encoded_int64_join_key(1), usize::MAX, &[0])
				.is_err()
		);

		let int64_array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
		let int32_array = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
		assert_eq!(
			JoinKeyPlan::from_key_arrays(
				std::slice::from_ref(&int64_array),
				std::slice::from_ref(&int64_array),
				false,
			),
			JoinKeyPlan::Int64
		);
		assert_eq!(
			JoinKeyPlan::from_key_arrays(
				std::slice::from_ref(&int32_array),
				std::slice::from_ref(&int32_array),
				false,
			),
			JoinKeyPlan::Int32
		);
		assert_eq!(
			JoinKeyPlan::from_key_arrays(
				std::slice::from_ref(&int64_array),
				std::slice::from_ref(&int64_array),
				true,
			),
			JoinKeyPlan::Generic
		);
		assert_eq!(
			JoinKeyPlan::from_key_arrays(
				&[int64_array.clone(), int64_array.clone()],
				&[int64_array.clone(), int64_array],
				false,
			),
			JoinKeyPlan::Generic
		);
	}

	#[tokio::test]
	async fn join_spill_compact_locator_matches_legacy_across_pages() {
		let mut rows = legacy_join_spill_rows(5_000);
		rows.insert(encoded_int64_join_key(0), (0..20_000u32).collect());
		let budget = test_task_budget();
		let compact_dir = test_spill_dir("compact_page_locator");
		let legacy_dir = test_spill_dir("legacy_page_locator");

		let mut compact = SpilledJoinBuild::write(
			rows.clone(),
			JoinKeyPlan::Int64,
			budget,
			Arc::new(BlockingFileOps),
			compact_dir.clone(),
		)
		.await
		.expect("write compact join spill");
		let mut legacy = SpilledJoinBuild::write(
			rows,
			JoinKeyPlan::Generic,
			budget,
			Arc::new(BlockingFileOps),
			legacy_dir.clone(),
		)
		.await
		.expect("write legacy join spill");

		assert!(compact.pages.len() > 1);
		assert!(legacy.pages.len() > 1);
		assert_eq!(compact.index.legacy_insert_count, 0);
		let JoinSpillPageLocatorStorage::Int64Pages(compact_index) = &compact.index.storage else {
			panic!("expected compact Int64 locator");
		};
		assert_eq!(compact_index.entries.len(), 5_000);
		assert_eq!(
			compact.index.accounted_bytes(),
			compact_index.accounted_bytes()
		);
		let JoinSpillPageLocatorStorage::Encoded(legacy_index) = &legacy.index.storage else {
			panic!("expected encoded fallback locator");
		};
		assert!(legacy_index.values().all(|pages| pages.len() == 1));

		for key in [0i64, 1, 2_500, 4_999, 9_999] {
			let encoded = encoded_int64_join_key(key);
			assert_eq!(
				compact.lookup(&encoded).await.expect("compact lookup"),
				legacy.lookup(&encoded).await.expect("legacy lookup")
			);
		}
		assert!(compact.page_read_count > 0);
		assert!(legacy.page_read_count > 0);

		compact.cleanup().await;
		legacy.cleanup().await;
		assert_eq!(std::fs::read_dir(&compact_dir).unwrap().count(), 0);
		assert_eq!(std::fs::read_dir(&legacy_dir).unwrap().count(), 0);
		let _ = std::fs::remove_dir_all(compact_dir);
		let _ = std::fs::remove_dir_all(legacy_dir);
	}

	#[tokio::test]
	async fn join_spill_singleton_row_mode_eliminates_page_reads() {
		let keys = (0..5_000).map(i64::from).collect::<Vec<_>>();
		let left = batch_with_keys(&keys);
		let right = batch_with_keys(&[0, 2_500, 4_999, 9_999]);
		let right_keys = key_arrays(&right, &[ColumnOrdinal(0)]).expect("right key arrays");
		let output_schema = joined_schema(&left.schema(), &right.schema());
		let spill_dir = test_spill_dir("singleton_row_locator");
		let mut build = SpilledJoinBuild::write(
			legacy_join_spill_rows(keys.len()),
			JoinKeyPlan::Int64,
			test_task_budget(),
			Arc::new(BlockingFileOps),
			spill_dir.clone(),
		)
		.await
		.expect("write singleton row locator");

		assert!(build.pages.len() > 1);
		assert!(matches!(
			build.index.storage,
			JoinSpillPageLocatorStorage::Int64Rows(_)
		));
		assert_eq!(build.index.legacy_insert_count, 0);
		assert_eq!(
			build.lookup(&encoded_int64_join_key(2_500)).await.unwrap(),
			JoinSpillRows::OneRow(2_500)
		);
		assert_eq!(
			build.lookup(&encoded_int64_join_key(9_999)).await.unwrap(),
			JoinSpillRows::NoMatch
		);
		assert_eq!(build.page_read_count, 0);

		let output = super::super::probe_spilled_hash_join(
			JoinKind::Inner,
			&mut build,
			&right,
			&right_keys,
			&left,
			&output_schema,
			None,
			false,
			test_task_budget().memory.get(),
		)
		.await
		.expect("probe singleton row locator");
		assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
		assert_eq!(build.page_read_count, 0);

		build.cleanup().await;
		assert_eq!(std::fs::read_dir(&spill_dir).unwrap().count(), 0);
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn join_spill_singleton_left_anti_residual_matches_encoded() {
		let left = batch_with_keys(&[1, 2, 3]);
		let right = batch_with_keys(&[2, 1, 9]);
		let right_keys = key_arrays(&right, &[ColumnOrdinal(0)]).expect("right key arrays");
		let residual = less_than_join_residual(ColumnOrdinal(1), ColumnOrdinal(4));
		let rows = HashMap::from([
			(encoded_int64_join_key(1), vec![0]),
			(encoded_int64_join_key(2), vec![1]),
			(encoded_int64_join_key(3), vec![2]),
		]);
		let row_dir = test_spill_dir("singleton_left_anti_row");
		let encoded_dir = test_spill_dir("singleton_left_anti_encoded");
		let mut row_build = SpilledJoinBuild::write(
			rows.clone(),
			JoinKeyPlan::Int64,
			test_task_budget(),
			Arc::new(BlockingFileOps),
			row_dir.clone(),
		)
		.await
		.expect("write row-mode anti build");
		let mut encoded_build = SpilledJoinBuild::write(
			rows,
			JoinKeyPlan::Generic,
			test_task_budget(),
			Arc::new(BlockingFileOps),
			encoded_dir.clone(),
		)
		.await
		.expect("write encoded anti build");
		assert_eq!(row_build.spill_bytes(), encoded_build.spill_bytes());
		assert_eq!(
			row_build
				.pages
				.iter()
				.map(|page| (page.plaintext_len, page.ciphertext_len))
				.collect::<Vec<_>>(),
			encoded_build
				.pages
				.iter()
				.map(|page| (page.plaintext_len, page.ciphertext_len))
				.collect::<Vec<_>>()
		);

		let row_output = super::super::probe_spilled_hash_join(
			JoinKind::LeftAnti,
			&mut row_build,
			&right,
			&right_keys,
			&left,
			&left.schema(),
			Some(&residual),
			false,
			test_task_budget().memory.get(),
		)
		.await
		.expect("probe row-mode LeftAnti");
		let encoded_output = super::super::probe_spilled_hash_join(
			JoinKind::LeftAnti,
			&mut encoded_build,
			&right,
			&right_keys,
			&left,
			&left.schema(),
			Some(&residual),
			false,
			test_task_budget().memory.get(),
		)
		.await
		.expect("probe encoded LeftAnti");

		assert_eq!(row_output.len(), 1);
		assert_eq!(encoded_output.len(), 1);
		assert_record_batch_equal(&row_output[0], &encoded_output[0]);
		assert_eq!(
			row_output[0]
				.column(0)
				.as_any()
				.downcast_ref::<Int64Array>()
				.expect("anti output keys")
				.values(),
			&[2, 3]
		);
		assert_eq!(row_build.page_read_count, 0);
		assert!(encoded_build.page_read_count > 0);

		row_build.cleanup().await;
		encoded_build.cleanup().await;
		assert_eq!(std::fs::read_dir(&row_dir).unwrap().count(), 0);
		assert_eq!(std::fs::read_dir(&encoded_dir).unwrap().count(), 0);
		let _ = std::fs::remove_dir_all(row_dir);
		let _ = std::fs::remove_dir_all(encoded_dir);
	}

	#[tokio::test]
	async fn join_spill_compact_locator_clears_legacy_resident_wall() {
		let rows = legacy_join_spill_rows(200);
		let high_budget = test_task_budget();
		let compact_profile_dir = test_spill_dir("compact_wall_profile");
		let legacy_profile_dir = test_spill_dir("legacy_wall_profile");
		let compact_profile = SpilledJoinBuild::write(
			rows.clone(),
			JoinKeyPlan::Int64,
			high_budget,
			Arc::new(BlockingFileOps),
			compact_profile_dir.clone(),
		)
		.await
		.expect("profile compact resident state");
		let legacy_profile = SpilledJoinBuild::write(
			rows.clone(),
			JoinKeyPlan::Generic,
			high_budget,
			Arc::new(BlockingFileOps),
			legacy_profile_dir.clone(),
		)
		.await
		.expect("profile legacy resident state");
		let compact_resident = compact_profile
			.index
			.accounted_bytes()
			.saturating_add(compact_profile.pages[0].plaintext_len as u64)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		let legacy_resident = legacy_profile
			.index
			.accounted_bytes()
			.saturating_add(legacy_profile.pages[0].plaintext_len as u64)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		assert!(compact_resident < legacy_resident);
		let crossing = compact_resident.saturating_add(legacy_resident) / 2;
		compact_profile.cleanup().await;
		legacy_profile.cleanup().await;
		let _ = std::fs::remove_dir_all(compact_profile_dir);
		let _ = std::fs::remove_dir_all(legacy_profile_dir);

		let compact_dir = test_spill_dir("compact_wall_win");
		let legacy_dir = test_spill_dir("legacy_wall_fail");
		let compact_guard_dir = test_spill_dir("compact_guard_armed");
		let low_budget = test_task_budget_with_memory(crossing);
		let compact = SpilledJoinBuild::write(
			rows.clone(),
			JoinKeyPlan::Int64,
			low_budget,
			Arc::new(BlockingFileOps),
			compact_dir.clone(),
		)
		.await
		.expect("compact locator must clear the crossing cap");
		let error = match SpilledJoinBuild::write(
			rows,
			JoinKeyPlan::Generic,
			low_budget,
			Arc::new(BlockingFileOps),
			legacy_dir.clone(),
		)
		.await
		{
			Err(error) => error,
			Ok(build) => {
				build.cleanup().await;
				panic!("legacy locator must fail the crossing cap");
			}
		};
		assert!(matches!(
			error,
			ExecutionError::BudgetExhausted {
				producer: "join spill writer resident state",
				..
			}
		));
		let compact_guard_budget = test_task_budget_with_memory(compact_resident - 1);
		let compact_guard_error = match SpilledJoinBuild::write(
			legacy_join_spill_rows(200),
			JoinKeyPlan::Int64,
			compact_guard_budget,
			Arc::new(BlockingFileOps),
			compact_guard_dir.clone(),
		)
		.await
		{
			Err(error) => error,
			Ok(build) => {
				build.cleanup().await;
				panic!("compact locator must retain the resident guard");
			}
		};
		assert!(matches!(
			compact_guard_error,
			ExecutionError::BudgetExhausted {
				producer: "join spill writer resident state",
				..
			}
		));
		compact.cleanup().await;
		assert_eq!(std::fs::read_dir(&compact_dir).unwrap().count(), 0);
		assert_eq!(std::fs::read_dir(&legacy_dir).unwrap().count(), 0);
		assert_eq!(std::fs::read_dir(&compact_guard_dir).unwrap().count(), 0);
		let _ = std::fs::remove_dir_all(compact_dir);
		let _ = std::fs::remove_dir_all(legacy_dir);
		let _ = std::fs::remove_dir_all(compact_guard_dir);
	}

	#[tokio::test]
	async fn spilled_hash_join_helper_selects_compact_locator() {
		let keys = (0..200).map(i64::from).collect::<Vec<_>>();
		let left = batch_with_keys(&keys);
		let right = batch_with_keys(&keys);
		let left_keys = key_arrays(&left, &[ColumnOrdinal(0)]).expect("left key arrays");
		let right_keys = key_arrays(&right, &[ColumnOrdinal(0)]).expect("right key arrays");
		let output_schema = joined_schema(&left.schema(), &right.schema());
		let spill_dir = test_spill_dir("production_helper_compact_locator");
		let output = super::super::execute_spilled_hash_join(
			JoinKind::Inner,
			&left,
			&right,
			&left_keys,
			&right_keys,
			&output_schema,
			None,
			false,
			test_task_budget_with_memory(80_000),
			JoinTiming::disabled(None),
			Arc::new(BlockingFileOps),
			spill_dir.clone(),
		)
		.await
		.expect("primitive production helper must clear the compact-only cap");
		assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 200);
		assert_eq!(std::fs::read_dir(&spill_dir).unwrap().count(), 0);
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn compact_join_spill_malformed_key_cleans_scratch() {
		let spill_dir = test_spill_dir("malformed_compact_key");
		let probe_dir = test_spill_dir("malformed_compact_probe_key");
		let rows = HashMap::from([(vec![JOIN_SPILL_INT32_KEY_TAG, 1, 2, 3, 4], vec![0])]);
		let error = match SpilledJoinBuild::write(
			rows,
			JoinKeyPlan::Int64,
			test_task_budget(),
			Arc::new(BlockingFileOps),
			spill_dir.clone(),
		)
		.await
		{
			Err(error) => error,
			Ok(build) => {
				build.cleanup().await;
				panic!("wrong primitive tag must fail loudly");
			}
		};
		assert!(matches!(error, ExecutionError::Kernel { .. }));
		assert_eq!(std::fs::read_dir(&spill_dir).unwrap().count(), 0);

		let mut build = SpilledJoinBuild::write(
			legacy_join_spill_rows(1),
			JoinKeyPlan::Int64,
			test_task_budget(),
			Arc::new(BlockingFileOps),
			probe_dir.clone(),
		)
		.await
		.expect("write valid compact join spill");
		assert!(build.lookup(&encoded_int32_join_key(0)).await.is_err());
		build.cleanup().await;
		assert_eq!(std::fs::read_dir(&probe_dir).unwrap().count(), 0);

		let _ = std::fs::remove_dir_all(spill_dir);
		let _ = std::fs::remove_dir_all(probe_dir);
	}

	fn assert_record_batch_equal(left: &RecordBatch, right: &RecordBatch) {
		assert_eq!(left.schema(), right.schema());
		assert_eq!(left.num_rows(), right.num_rows());
		assert_eq!(left.num_columns(), right.num_columns());
		for column in 0..left.num_columns() {
			assert_eq!(
				format!("{:?}", left.column(column)),
				format!("{:?}", right.column(column)),
				"column {column} differs"
			);
		}
	}

	fn partition_test_batch() -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![
			arrow::datatypes::Field::new("k_int", DataType::Int64, true),
			arrow::datatypes::Field::new("k_str", DataType::Utf8, true),
			arrow::datatypes::Field::new("k_date", DataType::Date32, true),
			arrow::datatypes::Field::new("k_decimal", DataType::Decimal128(15, 2), true),
		]));
		let decimal = Decimal128Array::from(vec![
			Some(12_345_i128),
			Some(67_890_i128),
			None,
			Some(-42_i128),
			Some(777_i128),
			Some(888_i128),
		])
		.with_precision_and_scale(15, 2)
		.expect("decimal test column");
		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(vec![
					Some(1),
					Some(2),
					Some(3),
					None,
					Some(5),
					Some(6),
				])) as ArrayRef,
				Arc::new(StringArray::from(vec![
					Some("alpha"),
					None,
					Some("gamma"),
					Some("delta"),
					Some("epsilon"),
					Some("zeta"),
				])) as ArrayRef,
				Arc::new(Date32Array::from(vec![
					Some(19_000),
					Some(19_001),
					Some(19_002),
					Some(19_003),
					None,
					Some(19_005),
				])) as ArrayRef,
				Arc::new(decimal) as ArrayRef,
			],
		)
		.expect("partition test batch")
	}

	#[test]
	fn adr484_mask_equals_modulo_for_power_of_two_and_preserves_bucket_identity() {
		// For power-of-two counts the mask must equal the modulo for every hash,
		// so bucket identity (and the ADR 480 partition contract) is unchanged.
		let hashes = [
			0u64,
			1,
			2,
			15,
			16,
			17,
			u64::MAX,
			u64::MAX - 1,
			0x9E37_79B9_7F4A_7C15,
			12_000_000,
			47_989_007,
		];
		for &count in &[1u32, 2, 4, 8, 16, 64, 256] {
			assert!(count.is_power_of_two());
			for &h in &hashes {
				assert_eq!(
					partition_index_for_hash(h, count),
					(h % u64::from(count)) as usize,
					"mask != modulo at count {count} hash {h}"
				);
			}
		}
		// Non-power-of-two counts keep the modulo unchanged.
		for &count in &[3u32, 5, 7, 17, 100] {
			assert!(!count.is_power_of_two());
			for &h in &hashes {
				assert_eq!(
					partition_index_for_hash(h, count),
					(h % u64::from(count)) as usize
				);
			}
		}
	}

	#[test]
	fn adr479_fixed_width_fast_path_matches_generic_encoding() {
		use arrow::array::{Int32Array, Int64Array};
		// The fast path must hash identically to the generic serialize-then-hash
		// path for every value — else build and probe rows partition differently
		// and the join silently loses matches.
		let i32vals = vec![i32::MIN, -7, -1, 0, 1, 42, i32::MAX];
		let i64vals = vec![i64::MIN, -7, -1, 0, 1, 42, 12_000_000, i64::MAX];
		let int32: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(i32vals.clone()))];
		let int64: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(i64vals.clone()))];
		let mut scratch = Vec::new();
		for (arrays, len) in [(&int32, i32vals.len()), (&int64, i64vals.len())] {
			for row in 0..len {
				let fast = fixed_width_partition_hash(arrays, row).expect("fast path applies");
				scratch.clear();
				encode_partition_key_value(arrays[0].as_ref(), row, &mut scratch)
					.expect("generic encode");
				let generic = XxHash3_64::oneshot(scratch.as_slice());
				assert_eq!(fast, generic, "fast/generic hash mismatch at row {row}");
			}
		}
		// Multi-column and non-fixed-width keys decline the fast path.
		let two: Vec<ArrayRef> = vec![
			Arc::new(Int64Array::from(vec![1i64])),
			Arc::new(Int64Array::from(vec![2i64])),
		];
		assert!(fixed_width_partition_hash(&two, 0).is_none());
	}

	#[test]
	fn partition_bucket_with_scratch_matches_fresh_encoding() {
		let batch = partition_test_batch();
		let key_arrays = key_arrays(
			&batch,
			&[
				ColumnOrdinal(0),
				ColumnOrdinal(1),
				ColumnOrdinal(2),
				ColumnOrdinal(3),
			],
		)
		.expect("key arrays");
		let mut scratch = Vec::with_capacity(1);

		for partition_count in [1, 3, 17, 64] {
			for row in 0..batch.num_rows() {
				let fresh =
					partition_bucket(&key_arrays, row, partition_count).expect("fresh bucket");
				let reused =
					partition_bucket_with_scratch(&key_arrays, row, partition_count, &mut scratch)
						.expect("scratch bucket");
				assert_eq!(fresh, reused);
			}
		}
		assert!(scratch.capacity() > 0);
	}

	#[test]
	fn precomputed_partition_buckets_match_recompute_and_selector() {
		let batch = partition_test_batch();
		let keys = [
			ColumnOrdinal(0),
			ColumnOrdinal(1),
			ColumnOrdinal(2),
			ColumnOrdinal(3),
		];
		let key_arrays = key_arrays(&batch, &keys).expect("key arrays");

		for partition_count in [1, 3, 17, 64] {
			for (null_safe_keys, retain_unkeyed_rows) in
				[(false, false), (false, true), (true, false)]
			{
				let buckets = super::super::precompute_join_partition_buckets(
					&batch,
					&keys,
					partition_count,
					null_safe_keys,
					retain_unkeyed_rows,
				)
				.expect("precompute buckets");
				assert_eq!(buckets.len(), batch.num_rows());

				for (row, bucket) in buckets.iter().enumerate() {
					let expected = if null_safe_keys {
						Some(
							u32::try_from(
								partition_bucket(&key_arrays, row, partition_count)
									.expect("null-safe bucket"),
							)
							.expect("bucket fits u32"),
						)
					} else if super::super::join_key_bytes(&key_arrays, row)
						.expect("join key")
						.is_some()
					{
						Some(
							u32::try_from(
								partition_bucket(&key_arrays, row, partition_count)
									.expect("non-null bucket"),
							)
							.expect("bucket fits u32"),
						)
					} else if retain_unkeyed_rows {
						Some(0)
					} else {
						None
					};
					assert_eq!(*bucket, expected, "row {row}");
				}

				for partition in 0..partition_count {
					let cached = super::super::select_join_partition_batch_with_buckets(
						&batch, &buckets, partition,
					)
					.expect("cached selector");
					let recomputed = super::super::select_join_partition_batch(
						&batch,
						&keys,
						partition_count,
						partition,
						null_safe_keys,
						retain_unkeyed_rows,
					)
					.expect("recomputed selector");
					assert_record_batch_equal(&cached, &recomputed);
				}
			}
		}
	}

	fn int64_base(value: i64) -> u64 {
		join_key_index_base_bytes(&JoinKey::Int64(value))
	}

	fn fold_based_inner_guard_rejects(build: &SpilledInnerJoinBuild, memory_cap: u64) -> bool {
		let resident = build
			.folded_index_bytes()
			.saturating_add(build.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		resident > join_spill_retained_footprint_memory_cap(memory_cap)
	}

	fn fold_based_existence_guard_rejects(
		build: &SpilledSemiAntiExistenceBuild,
		memory_cap: u64,
	) -> bool {
		let resident = build
			.folded_index_bytes()
			.saturating_add(build.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		resident > join_spill_retained_footprint_memory_cap(memory_cap)
	}

	fn align_inner_guard_resident_to_multiplier(build: &mut SpilledInnerJoinBuild) -> u64 {
		let resident = build
			.folded_index_bytes()
			.saturating_add(build.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		let remainder = resident % JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER;
		if remainder != 0 {
			build.writer_peak = build
				.writer_peak
				.saturating_add(JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER - remainder);
		}
		build
			.folded_index_bytes()
			.saturating_add(build.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64)
	}

	fn align_existence_guard_resident_to_multiplier(
		build: &mut SpilledSemiAntiExistenceBuild,
	) -> u64 {
		let resident = build
			.folded_index_bytes()
			.saturating_add(build.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		let remainder = resident % JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER;
		if remainder != 0 {
			build.writer_peak = build
				.writer_peak
				.saturating_add(JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER - remainder);
		}
		build
			.folded_index_bytes()
			.saturating_add(build.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64)
	}

	#[test]
	fn encode_record_batch_ipc_into_matches_fresh_and_reuses_scratch() {
		let first = batch_with_keys(&[1, 2, 3]);
		let second = nullable_batch_with_keys(&[Some(7), None, Some(9), Some(10)]);
		let first_fresh = encode_record_batch_ipc(&first).expect("fresh first encode");
		let second_fresh = encode_record_batch_ipc(&second).expect("fresh second encode");
		let mut scratch = Vec::with_capacity(1);

		encode_record_batch_ipc_into(&mut scratch, &first).expect("scratch first encode");
		assert_eq!(scratch, first_fresh);
		let first_capacity = scratch.capacity();
		assert!(first_capacity >= scratch.len());

		encode_record_batch_ipc_into(&mut scratch, &second).expect("scratch second encode");
		assert_eq!(scratch, second_fresh);
		assert!(scratch.capacity() >= first_capacity.min(second_fresh.len()));
	}

	#[tokio::test]
	async fn unindexed_spill_coalesces_fragments_and_flushes_the_partial_page() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("unindexed_page_coalescing");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		let keys = [ColumnOrdinal(0)];
		let first = batch_with_keys(&[1, 2, 3]);
		let second = batch_with_keys(&[4, 5]);

		for batch in [&first, &second] {
			build
				.write_batch_chunks_unindexed_profiled(
					batch,
					JoinKeyPlan::Int64,
					&keys,
					false,
					budget,
					None,
				)
				.await
				.expect("buffer fragment");
		}
		assert_eq!(build.page_count(), 0, "small fragments stay buffered");
		assert!(
			build.finish(budget).await.is_err(),
			"a caller cannot seal an unflushed partial page"
		);
		build
			.flush_unindexed_page_buffer(JoinKeyPlan::Int64, &keys, false, budget, None)
			.await
			.expect("flush partial page");
		assert_eq!(build.page_count(), 1, "two fragments compose one page");
		build.finish(budget).await.expect("finish flushed spill");
		let decoded = build.read_batch_page(0).await.expect("read coalesced page");
		assert_record_batch_equal(
			&decoded,
			&arrow::compute::concat_batches(&first.schema(), [&first, &second]).expect("concat"),
		);
		assert_eq!(build.input_row_count(), 5);

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	/// The caller-side half of the contract above. The partition-at-a-time replay reservoir wrote
	/// probe batches through the buffering writer and then called `finish` directly, so any
	/// partition whose final page was partial died on the fail-loud check — deterministically, for
	/// the whole query. Sealing is one named operation so the flush cannot be omitted at a barrier
	/// again.
	#[tokio::test]
	async fn sealing_a_reservoir_partition_flushes_its_partial_page() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("seal_reservoir_partition");
		let keys = [ColumnOrdinal(0)];
		// Fewer rows than CROSS_JOIN_OUTPUT_CHUNK_ROWS, so the writer holds the page back: this is
		// the partial-final-page shape every partition ends on.
		let partial = batch_with_keys(&[7, 8, 9]);

		let mut partition =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		partition
			.write_batch_chunks_unindexed_profiled(
				&partial,
				JoinKeyPlan::Int64,
				&keys,
				false,
				budget,
				None,
			)
			.await
			.expect("buffer the partial page");
		assert_eq!(
			partition.page_count(),
			0,
			"the fixture must leave a page pending, or it cannot regress the barrier"
		);

		partition
			.seal_unindexed_reservoir_partition(JoinKeyPlan::Int64, &keys, false, budget, None)
			.await
			.expect("sealing flushes before it finishes");

		assert_eq!(partition.page_count(), 1, "the held page reached the file");
		let decoded = partition
			.read_batch_page(0)
			.await
			.expect("read sealed page");
		assert_record_batch_equal(&decoded, &partial);
		assert_eq!(partition.input_row_count(), 3);

		partition.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn inner_spill_page_write_reuses_scratch_and_round_trips() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("inner_scratch_reuse");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		let keys = [ColumnOrdinal(0)];
		let first = batch_with_keys(&[1, 2, 2, 3]);
		let second = batch_with_keys(&[4, 5]);
		let first_plaintext = encode_record_batch_ipc(&first).expect("fresh first plaintext");
		let second_plaintext = encode_record_batch_ipc(&second).expect("fresh second plaintext");

		build
			.write_batch_chunks(&first, JoinKeyPlan::Int64, &keys, false, budget)
			.await
			.expect("write first batch");
		let first_scratch_capacity = build.scratch_plaintext.capacity();
		let first_refs_capacity = build.scratch_refs.capacity();
		assert_eq!(build.scratch_plaintext.len(), 0);
		assert_eq!(build.scratch_refs.len(), 0);
		assert!(first_scratch_capacity >= first_plaintext.len());
		assert!(first_refs_capacity >= first.num_rows());

		build
			.write_batch_chunks(&second, JoinKeyPlan::Int64, &keys, false, budget)
			.await
			.expect("write second batch");
		assert_eq!(build.scratch_plaintext.len(), 0);
		assert_eq!(build.scratch_refs.len(), 0);
		assert!(build.scratch_plaintext.capacity() >= first_scratch_capacity);
		assert!(build.scratch_refs.capacity() >= first_refs_capacity);
		assert_eq!(build.pages[0].plaintext_len, first_plaintext.len());
		assert_eq!(build.pages[1].plaintext_len, second_plaintext.len());
		assert_eq!(
			build.writer_peak,
			u64::try_from(first_plaintext.len().max(second_plaintext.len())).unwrap()
		);
		assert_eq!(
			build.spill_bytes,
			build
				.pages
				.iter()
				.map(|page| page.ciphertext_len as u64)
				.sum::<u64>()
		);

		let decoded_first = build.read_batch_page(0).await.expect("read first page");
		let decoded_second = build.read_batch_page(1).await.expect("read second page");
		assert_record_batch_equal(&decoded_first, &first);
		assert_record_batch_equal(&decoded_second, &second);
		assert_eq!(
			build
				.lookup(&JoinKey::Int64(2))
				.await
				.expect("lookup key 2"),
			SpilledLookupMatches::Many(vec![
				SpilledBatchRowRef {
					page_index: 0,
					row: 1
				},
				SpilledBatchRowRef {
					page_index: 0,
					row: 2
				}
			])
		);
		assert_eq!(
			build
				.lookup(&JoinKey::Int64(5))
				.await
				.expect("lookup key 5"),
			SpilledLookupMatches::Many(vec![SpilledBatchRowRef {
				page_index: 1,
				row: 1
			}])
		);

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn combined_rebuild_and_pin_decodes_each_build_page_once() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("combined_rebuild_pin");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		let values = (0..(CROSS_JOIN_OUTPUT_CHUNK_ROWS + 17))
			.map(|value| value as i64)
			.collect::<Vec<_>>();
		build
			.write_batch_chunks_unindexed(
				&batch_with_keys(&values),
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				budget,
			)
			.await
			.expect("write unindexed pages");
		build.finish(budget).await.expect("finish spill");
		build.release_write_scratch();
		assert!(build.page_count() > 1);
		let stats = build
			.rebuild_compact_index_and_pin_pages(
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				budget,
				1234,
			)
			.await
			.expect("combined rebuild and pin");
		assert_eq!(stats.page_reads, build.page_count() as u64);
		assert_eq!(stats.rows, values.len() as u64);
		assert!(stats.decoded_bytes > 0);
		assert!(stats.index_bytes > 0);
		assert_eq!(build.page_decode_count(), build.page_count() as u64);
		assert!(build.uses_compact_int64_index());
		assert_eq!(
			build
				.lookup(&JoinKey::Int64(values[values.len() - 1]))
				.await
				.expect("lookup pinned key")
				.len(),
			1
		);
		for page in 0..build.page_count() {
			let _ = build.read_batch_page(page).await.expect("read pinned page");
		}
		assert_eq!(
			build.page_decode_count(),
			build.page_count() as u64,
			"pinned probe reads must not decode build pages again"
		);
		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn decoded_page_pin_admits_and_probe_reads_do_not_decode_again() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("decoded_pin_admitted");
		let profile_path = spill_dir.with_extension("profile.json");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		let values = (0..(CROSS_JOIN_OUTPUT_CHUNK_ROWS + 17))
			.map(|value| value as i64)
			.collect::<Vec<_>>();
		build
			.write_batch_chunks(
				&batch_with_keys(&values),
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				budget,
			)
			.await
			.expect("write pages");
		build.finish(budget).await.expect("finish spill");
		build.release_write_scratch();
		build.throughput_profile = Some(Box::new(StreamedSpilledLeftOuterProfile::at_test_path(
			profile_path.clone(),
		)));
		build.clear_read_cache();

		let outcome = build
			.try_pin_decoded_pages(budget.memory.get())
			.await
			.expect("pin decoded pages");
		assert_eq!(outcome, PinDecodedPagesOutcome::Admitted);
		assert!(build.pinned_decoded_pages.is_some());
		assert_eq!(build.page_decode_count(), build.page_count() as u64);
		for page_index in 0..build.page_count() {
			let _ = build
				.read_batch_page(page_index)
				.await
				.expect("read pinned page");
		}
		assert_eq!(build.page_decode_count(), build.page_count() as u64);
		build
			.persist_throughput_profile()
			.expect("persist admitted pin receipt");
		let receipt: serde_json::Value = serde_json::from_slice(
			&std::fs::read(&profile_path).expect("read admitted pin receipt"),
		)
		.expect("decode admitted pin receipt");
		assert_eq!(receipt["pin"]["outcome"], "admitted");
		assert_eq!(
			receipt["operation_phases"]["preload"]["file_read_calls"],
			build.page_count() as u64
		);
		assert_eq!(receipt["operation_phases"]["probe"]["file_read_calls"], 0);
		assert_eq!(receipt["operation_phases"]["probe"]["decoded_evictions"], 0);

		build.cleanup().await;
		let _ = std::fs::remove_file(profile_path);
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn decoded_page_pin_preflight_reject_falls_back_value_identical() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("decoded_pin_preflight_reject");
		let profile_path = spill_dir.with_extension("profile.json");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		let expected = batch_with_keys(&[1, 2, 3, 4]);
		build
			.write_batch_chunks(
				&expected,
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				budget,
			)
			.await
			.expect("write page");
		build.finish(budget).await.expect("finish spill");
		build.throughput_profile = Some(Box::new(StreamedSpilledLeftOuterProfile::at_test_path(
			profile_path.clone(),
		)));

		let outcome = build
			.try_pin_decoded_pages(1)
			.await
			.expect("preflight rejection is a fallback");
		assert_eq!(outcome, PinDecodedPagesOutcome::RejectedPreflight);
		assert!(build.pinned_decoded_pages.is_none());
		let fallback = build
			.read_batch_page(0)
			.await
			.expect("cached fallback read");
		assert_record_batch_equal(&fallback, &expected);
		build
			.persist_throughput_profile()
			.expect("persist preflight rejection receipt");
		let receipt: serde_json::Value = serde_json::from_slice(
			&std::fs::read(&profile_path).expect("read preflight rejection receipt"),
		)
		.expect("decode preflight rejection receipt");
		assert_eq!(receipt["pin"]["outcome"], "rejected_preflight");
		assert_eq!(receipt["pin"]["pages_attempted"], 0);
		assert_eq!(receipt["pin"]["pages_completed"], 0);

		build.cleanup().await;
		let _ = std::fs::remove_file(profile_path);
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn decoded_page_pin_mid_decode_reject_falls_back_value_identical() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("decoded_pin_mid_decode_reject");
		let profile_path = spill_dir.with_extension("profile.json");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		let expected = batch_with_keys(&[1, 2, 3, 4]);
		build
			.write_batch_chunks(
				&expected,
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				budget,
			)
			.await
			.expect("write page");
		build.finish(budget).await.expect("finish spill");
		build.release_write_scratch();
		for page in &mut build.pages {
			page.plaintext_len = 0;
		}
		build.resident_index_bytes = 0;
		build.throughput_profile = Some(Box::new(StreamedSpilledLeftOuterProfile::at_test_path(
			profile_path.clone(),
		)));
		let memory_cap = (JOIN_SPILL_PAGE_SIZE as u64
			+ JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER)
			/ JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER;

		let outcome = build
			.try_pin_decoded_pages(memory_cap)
			.await
			.expect("mid-decode rejection is a fallback");
		assert_eq!(outcome, PinDecodedPagesOutcome::RejectedMidDecode);
		assert!(build.pinned_decoded_pages.is_none());
		let fallback = build
			.read_batch_page(0)
			.await
			.expect("cached fallback read");
		assert_record_batch_equal(&fallback, &expected);
		build
			.persist_throughput_profile()
			.expect("persist mid-decode rejection receipt");
		let receipt: serde_json::Value = serde_json::from_slice(
			&std::fs::read(&profile_path).expect("read mid-decode rejection receipt"),
		)
		.expect("decode mid-decode rejection receipt");
		assert_eq!(receipt["pin"]["outcome"], "rejected_mid_decode");
		assert_eq!(receipt["pin"]["pages_attempted"], 1);
		assert_eq!(receipt["pin"]["pages_completed"], 1);

		build.cleanup().await;
		let _ = std::fs::remove_file(profile_path);
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn inner_spill_read_batch_page_reuses_decoded_cache() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("inner_decoded_cache");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		let keys = [ColumnOrdinal(0)];
		let batch = batch_with_keys(&[1, 2, 3, 4]);

		build
			.write_batch_chunks(&batch, JoinKeyPlan::Int64, &keys, false, budget)
			.await
			.expect("write batch");
		assert!(
			build
				.cache
				.get(&0)
				.and_then(|cached| cached.decoded_batch.as_ref())
				.is_none(),
			"writes cache plaintext only"
		);

		let decoded_first = build.read_batch_page(0).await.expect("first read");
		assert_record_batch_equal(&decoded_first, &batch);
		let cache_bytes_after_first = build.cache_bytes;
		let cached = build.cache.get_mut(&0).expect("page cached");
		assert!(
			cached.decoded_batch.is_some(),
			"first read caches decoded batch"
		);
		assert!(
			cached.bytes.is_empty(),
			"decoded cache entry drops plaintext bytes"
		);
		assert!(cached.decoded_bytes > 0);
		cached.bytes.clear();

		let decoded_second = build
			.read_batch_page(0)
			.await
			.expect("second read uses decoded cache despite invalid plaintext bytes");
		assert_record_batch_equal(&decoded_second, &batch);
		assert_eq!(build.cache_bytes, cache_bytes_after_first);

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn resident_single_touch_drain_can_disable_page_cache() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("resident_cache_bypass");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		let batch = batch_with_keys(&[1, 2, 3, 4]);
		build
			.write_batch_chunks(
				&batch,
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				budget,
			)
			.await
			.expect("write batch");
		let first = build.read_batch_page(0).await.expect("prime page cache");
		assert_record_batch_equal(&first, &batch);
		assert!(!build.cache.is_empty());
		build.disable_page_cache();

		let decoded = build.read_batch_page(0).await.expect("read resident page");
		assert_record_batch_equal(&decoded, &batch);
		assert_eq!(build.cache_cap, 0);
		assert!(build.cache.is_empty());
		assert!(build.lru.is_empty());
		assert_eq!(build.cache_bytes, 0);
		assert_eq!(build.page_decode_count(), 2);

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn incremental_inner_resident_index_tracks_new_and_duplicate_refs() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("inner_refs");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		build.use_compact_resident_index();
		let keys = [ColumnOrdinal(0)];

		build
			.write_batch_chunks(
				&batch_with_keys(&[1]),
				JoinKeyPlan::Int64,
				&keys,
				false,
				budget,
			)
			.await
			.expect("write first key");
		assert!(build.uses_compact_int64_index());
		assert_eq!(build.resident_index_bytes(), build.computed_index_bytes());

		build
			.write_batch_chunks(
				&batch_with_keys(&[1, 2]),
				JoinKeyPlan::Int64,
				&keys,
				false,
				budget,
			)
			.await
			.expect("write duplicate and new key");
		assert!(build.uses_compact_int64_index());
		assert_eq!(build.resident_index_bytes(), build.computed_index_bytes());
		assert_eq!(
			build
				.lookup(&JoinKey::Int64(1))
				.await
				.expect("lookup duplicate key"),
			SpilledLookupMatches::Many(vec![
				SpilledBatchRowRef {
					page_index: 0,
					row: 0
				},
				SpilledBatchRowRef {
					page_index: 1,
					row: 0
				}
			]),
			"compact spill index must preserve duplicate ref insertion order"
		);

		build
			.write_batch_chunks(
				&batch_with_keys(&[2, 3, 3]),
				JoinKeyPlan::Int64,
				&keys,
				false,
				budget,
			)
			.await
			.expect("write multi-batch keys");
		assert!(build.uses_compact_int64_index());
		assert_eq!(build.resident_index_bytes(), build.computed_index_bytes());
		assert_eq!(
			build
				.lookup(&JoinKey::Int64(3))
				.await
				.expect("lookup duplicate key"),
			SpilledLookupMatches::Many(vec![
				SpilledBatchRowRef {
					page_index: 2,
					row: 1
				},
				SpilledBatchRowRef {
					page_index: 2,
					row: 2
				}
			])
		);

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	// -----------------------------------------------------------------------------------------
	// ADR 602: a hit's refs without an allocation — the lookup forms, their shapes, and counts.
	// -----------------------------------------------------------------------------------------

	fn adr602_ref(page_index: usize, row: u32) -> SpilledBatchRowRef {
		SpilledBatchRowRef { page_index, row }
	}

	fn adr602_key(plan: JoinKeyPlan, key: i64) -> JoinKey {
		match plan {
			JoinKeyPlan::Int32 => JoinKey::Int32(key as i32),
			_ => JoinKey::Int64(key),
		}
	}

	fn adr602_batch(plan: JoinKeyPlan, keys: &[i64]) -> RecordBatch {
		match plan {
			JoinKeyPlan::Int32 => {
				batch_with_int32_keys(&keys.iter().map(|key| Some(*key as i32)).collect::<Vec<_>>())
			}
			_ => batch_with_keys(keys),
		}
	}

	#[tokio::test]
	async fn adr602_lookup_forms_return_the_same_refs_in_order() {
		let budget = test_task_budget();
		let keys = [ColumnOrdinal(0)];
		for plan in [JoinKeyPlan::Int64, JoinKeyPlan::Int32] {
			for form in [SpilledLookupForm::Collected, SpilledLookupForm::Inline] {
				let label = format!("{plan:?} {}", form.name());
				let spill_dir = test_spill_dir("adr602_forms");
				let mut build = SpilledInnerJoinBuild::create(
					budget,
					Arc::new(BlockingFileOps),
					spill_dir.clone(),
				)
				.await
				.expect("create inner spill");
				assert_eq!(build.lookup_form(), SpilledLookupForm::Collected, "{label}");
				build.use_compact_resident_index();
				build
					.use_lookup_form(form)
					.expect("compact index admits either form");
				build.enable_lookup_counting();
				for page in [[1, 2], [2, 3]] {
					build
						.write_batch_chunks(&adr602_batch(plan, &page), plan, &keys, false, budget)
						.await
						.expect("write keys");
				}
				assert!(!build.uses_hash_index(), "{label}");

				// A unique key: `One` under `Inline`, the one-element `Vec` under `Collected`.
				let unique = build.lookup(&adr602_key(plan, 1)).await.expect("lookup");
				let expected_unique = match form {
					SpilledLookupForm::Inline => SpilledLookupMatches::One(adr602_ref(0, 0)),
					SpilledLookupForm::Collected => {
						SpilledLookupMatches::Many(vec![adr602_ref(0, 0)])
					}
				};
				assert_eq!(unique, expected_unique, "{label}: unique key's shape");
				assert_eq!(unique.len(), 1, "{label}");
				assert!(!unique.is_empty(), "{label}");
				assert_eq!(
					unique.into_iter().collect::<Vec<_>>(),
					vec![adr602_ref(0, 0)],
					"{label}"
				);

				// A repeated key across pages: `Many` in insertion order under either form.
				let repeated = build.lookup(&adr602_key(plan, 2)).await.expect("lookup");
				assert_eq!(
					repeated,
					SpilledLookupMatches::Many(vec![adr602_ref(0, 1), adr602_ref(1, 0)]),
					"{label}: repeated key's refs and their order"
				);
				assert_eq!(repeated.len(), 2, "{label}");

				let last = build.lookup(&adr602_key(plan, 3)).await.expect("lookup");
				assert_eq!(
					last.into_iter().collect::<Vec<_>>(),
					vec![adr602_ref(1, 1)],
					"{label}"
				);

				// An absent key, and a key of the other primitive variant: `Miss`.
				let absent = build.lookup(&adr602_key(plan, 9)).await.expect("lookup");
				assert_eq!(absent, SpilledLookupMatches::Miss, "{label}: absent key");
				assert!(absent.is_empty(), "{label}");
				let mismatched = match plan {
					JoinKeyPlan::Int32 => JoinKey::Int64(1),
					_ => JoinKey::Int32(1),
				};
				assert_eq!(
					build.lookup(&mismatched).await.expect("lookup"),
					SpilledLookupMatches::Miss,
					"{label}: mismatched variant"
				);

				// Three hits, two misses; the unique keys 1 and 3 return inline under `Inline`.
				assert_eq!(build.lookup_counts(), Some((3, 2)), "{label}");
				let inline_hits = match form {
					SpilledLookupForm::Inline => 2,
					SpilledLookupForm::Collected => 0,
				};
				assert_eq!(build.lookup_inline_hits(), Some(inline_hits), "{label}");

				build.cleanup().await;
				let _ = std::fs::remove_dir_all(spill_dir);
			}
		}
	}

	#[tokio::test]
	async fn adr602_inline_form_needs_the_compact_index_and_hash_paths_are_unchanged() {
		let budget = test_task_budget();
		let keys = [ColumnOrdinal(0)];

		// The legacy index: `Inline` refused, the form left `Collected`, lookups as before.
		let spill_dir = test_spill_dir("adr602_legacy");
		let mut legacy =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		assert!(legacy.use_lookup_form(SpilledLookupForm::Inline).is_err());
		assert_eq!(legacy.lookup_form(), SpilledLookupForm::Collected);
		assert_eq!(legacy.lookup_inline_hits(), None, "counting not enabled");
		legacy.enable_lookup_counting();
		legacy
			.write_batch_chunks(
				&batch_with_keys(&[1, 2, 2]),
				JoinKeyPlan::Int64,
				&keys,
				false,
				budget,
			)
			.await
			.expect("write keys");
		assert!(legacy.uses_hash_index());
		assert_eq!(
			legacy.lookup(&JoinKey::Int64(1)).await.expect("lookup"),
			SpilledLookupMatches::Many(vec![adr602_ref(0, 0)])
		);
		assert_eq!(
			legacy.lookup(&JoinKey::Int64(2)).await.expect("lookup"),
			SpilledLookupMatches::Many(vec![adr602_ref(0, 1), adr602_ref(0, 2)])
		);
		assert_eq!(
			legacy.lookup(&JoinKey::Int64(9)).await.expect("lookup"),
			SpilledLookupMatches::Miss
		);
		assert_eq!(legacy.lookup_counts(), Some((2, 1)));
		assert_eq!(legacy.lookup_inline_hits(), Some(0));
		legacy.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);

		// The compact wrapper's `Hash` variant (null-safe keys): `Inline` admitted, every hit
		// still the cloned `Vec`.
		let spill_dir = test_spill_dir("adr602_compact_hash");
		let mut hashed =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		hashed.use_compact_resident_index();
		hashed
			.use_lookup_form(SpilledLookupForm::Inline)
			.expect("compact index admits inline");
		hashed.enable_lookup_counting();
		hashed
			.write_batch_chunks(
				&batch_with_keys(&[1, 2]),
				JoinKeyPlan::Int64,
				&keys,
				true,
				budget,
			)
			.await
			.expect("write null-safe keys");
		let arrays = key_arrays(&batch_with_keys(&[1]), &keys).expect("key arrays");
		let key = join_key_at(JoinKeyPlan::Int64, &arrays, 0, true)
			.expect("encode null-safe key")
			.expect("non-null key");
		assert_eq!(
			hashed.lookup(&key).await.expect("lookup"),
			SpilledLookupMatches::Many(vec![adr602_ref(0, 0)])
		);
		assert_eq!(hashed.lookup_counts(), Some((1, 0)));
		assert_eq!(hashed.lookup_inline_hits(), Some(0));
		hashed.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[test]
	fn adr602_matches_iterate_exactly_and_stay_fused() {
		let shapes = [
			(SpilledLookupMatches::Miss, vec![]),
			(
				SpilledLookupMatches::One(adr602_ref(4, 7)),
				vec![adr602_ref(4, 7)],
			),
			(
				SpilledLookupMatches::Many(vec![adr602_ref(0, 1), adr602_ref(2, 3)]),
				vec![adr602_ref(0, 1), adr602_ref(2, 3)],
			),
		];
		for (matches, expected) in shapes {
			assert_eq!(matches.len(), expected.len());
			assert_eq!(matches.is_empty(), expected.is_empty());
			let mut iter = matches.into_iter();
			for (taken, row_ref) in expected.iter().enumerate() {
				let remaining = expected.len() - taken;
				assert_eq!(iter.size_hint(), (remaining, Some(remaining)));
				assert_eq!(iter.next(), Some(*row_ref));
			}
			assert_eq!(iter.size_hint(), (0, Some(0)));
			assert_eq!(iter.next(), None);
			assert_eq!(iter.next(), None, "fused after exhaustion");
		}
		assert_eq!(
			SpilledLookupMatches::from_vec(Vec::new()),
			SpilledLookupMatches::Miss
		);
	}

	#[tokio::test]
	async fn partitioned_unindexed_rebuild_selects_primitive_int64_before_probe() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("partitioned_primitive_rebuild");
		let keys = [ColumnOrdinal(0)];
		for partition_keys in [vec![1, 2, 2], vec![3, 4, 4]] {
			let mut primitive =
				SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
					.await
					.expect("create primitive partition spill");
			let mut legacy =
				SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
					.await
					.expect("create legacy partition spill");
			let batch = batch_with_keys(&partition_keys);
			for build in [&mut primitive, &mut legacy] {
				build
					.write_batch_chunks_unindexed(&batch, JoinKeyPlan::Int64, &keys, false, budget)
					.await
					.expect("write unindexed partition");
				build.finish(budget).await.expect("finish partition spill");
				build.release_write_scratch();
			}

			primitive.use_compact_resident_index();
			primitive
				.rebuild_index_from_pages(JoinKeyPlan::Int64, &keys, false, budget)
				.await
				.expect("rebuild primitive partition index");
			legacy
				.rebuild_index_from_pages(JoinKeyPlan::Int64, &keys, false, budget)
				.await
				.expect("rebuild legacy partition index");

			assert!(primitive.uses_compact_int64_index());
			assert!(primitive.legacy_index_is_empty());
			assert_eq!(primitive.legacy_index_insert_count(), 0);
			assert_eq!(
				primitive.resident_index_bytes(),
				primitive.computed_index_bytes()
			);
			assert!(legacy.uses_hash_index());
			assert_eq!(
				legacy.legacy_index_insert_count(),
				partition_keys.len() as u64
			);
			for key in partition_keys {
				assert_eq!(
					primitive
						.lookup(&JoinKey::Int64(key))
						.await
						.expect("primitive lookup"),
					legacy
						.lookup(&JoinKey::Int64(key))
						.await
						.expect("legacy lookup")
				);
			}

			primitive.cleanup().await;
			legacy.cleanup().await;
		}
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn inner_spill_generic_and_null_safe_keep_hash_index_fence() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("inner_hash_fence");
		let mut generic =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create generic inner spill");
		let keys = [ColumnOrdinal(0)];
		let generic_batch = batch_with_keys(&[1, 2]);
		generic
			.write_batch_chunks_unindexed(
				&generic_batch,
				JoinKeyPlan::Generic,
				&keys,
				false,
				budget,
			)
			.await
			.expect("write generic keys");
		generic.finish(budget).await.expect("finish generic spill");
		generic.release_write_scratch();
		generic
			.rebuild_index_from_pages(JoinKeyPlan::Generic, &keys, false, budget)
			.await
			.expect("rebuild generic index");
		assert!(generic.uses_hash_index());
		assert_eq!(generic.legacy_index_insert_count(), 2);
		assert_eq!(
			generic.resident_index_bytes(),
			generic.computed_index_bytes()
		);
		generic.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);

		let spill_dir = test_spill_dir("inner_null_safe_hash_fence");
		let mut null_safe =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create null-safe inner spill");
		let null_safe_batch = nullable_batch_with_keys(&[Some(1), None, Some(2)]);
		let null_safe_probe_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
			"probe_k",
			DataType::Int64,
			true,
		)]));
		let null_safe_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&null_safe_batch.schema(),
			&keys,
			&null_safe_probe_schema,
			&keys,
			true,
		)
		.expect("derive null-safe key plan");
		assert_eq!(null_safe_plan, JoinKeyPlan::Generic);
		null_safe
			.write_batch_chunks_unindexed(&null_safe_batch, null_safe_plan, &keys, true, budget)
			.await
			.expect("write null-safe keys");
		null_safe
			.finish(budget)
			.await
			.expect("finish null-safe spill");
		null_safe.release_write_scratch();
		null_safe
			.rebuild_index_from_pages(null_safe_plan, &keys, true, budget)
			.await
			.expect("rebuild null-safe index");
		assert!(null_safe.uses_hash_index());
		assert_eq!(null_safe.legacy_index_insert_count(), 3);
		assert_eq!(
			null_safe.resident_index_bytes(),
			null_safe.computed_index_bytes()
		);
		let arrays = key_arrays(&null_safe_batch, &keys).expect("null-safe key arrays");
		let null_key = join_key_at(null_safe_plan, &arrays, 1, true)
			.expect("encode null-safe null")
			.expect("null-safe null key");
		assert_eq!(
			null_safe.lookup(&null_key).await.expect("lookup null key"),
			SpilledLookupMatches::Many(vec![SpilledBatchRowRef {
				page_index: 0,
				row: 1,
			}])
		);
		null_safe.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn primitive_rebuild_resident_guard_fires_at_rebuild_site() {
		let write_budget = test_task_budget();
		let tight_budget = test_task_budget_with_memory(1);
		let spill_dir = test_spill_dir("primitive_rebuild_guard");
		let mut build = SpilledInnerJoinBuild::create(
			write_budget,
			Arc::new(BlockingFileOps),
			spill_dir.clone(),
		)
		.await
		.expect("create guarded primitive spill");
		let keys = [ColumnOrdinal(0)];
		build
			.write_batch_chunks_unindexed(
				&batch_with_keys(&(0..512).collect::<Vec<_>>()),
				JoinKeyPlan::Int64,
				&keys,
				false,
				write_budget,
			)
			.await
			.expect("write guarded primitive spill");
		build
			.finish(write_budget)
			.await
			.expect("finish guarded spill");
		build.release_write_scratch();
		build.use_compact_resident_index();
		let error = build
			.rebuild_index_from_pages(JoinKeyPlan::Int64, &keys, false, tight_budget)
			.await
			.expect_err("tight budget must reject primitive rebuild");
		match error {
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer,
				detail,
				..
			} => {
				assert_eq!(producer, "streamed inner join build spill resident state");
				assert!(detail.to_string().contains("rebuild_index"));
			}
			other => panic!("unexpected primitive rebuild guard error: {other:?}"),
		}
		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn inner_spill_default_resident_index_keeps_int64_on_hash_path() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("inner_hash_override");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		let keys = [ColumnOrdinal(0)];
		build
			.write_batch_chunks(
				&batch_with_keys(&[1, 2, 2]),
				JoinKeyPlan::Int64,
				&keys,
				false,
				budget,
			)
			.await
			.expect("write int64 keys on hash override");
		assert!(build.uses_hash_index());
		assert_eq!(build.resident_index_bytes(), build.computed_index_bytes());
		assert_eq!(
			build
				.lookup(&JoinKey::Int64(2))
				.await
				.expect("lookup duplicate key"),
			SpilledLookupMatches::Many(vec![
				SpilledBatchRowRef {
					page_index: 0,
					row: 1
				},
				SpilledBatchRowRef {
					page_index: 0,
					row: 2
				}
			])
		);
		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn inner_spill_compact_rescue_keeps_resident_guard_armed() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("inner_compact_rescue_guard");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		let keys = [ColumnOrdinal(0)];
		let input_keys = (0..20_000).map(i64::from).collect::<Vec<_>>();
		build
			.write_batch_chunks(
				&batch_with_keys(&input_keys),
				JoinKeyPlan::Int64,
				&keys,
				false,
				budget,
			)
			.await
			.expect("write hash-indexed keys");
		assert!(build.uses_hash_index());
		let hash_resident = build.resident_state().total();
		let trigger_cap = hash_resident.saturating_sub(512 * 1024);
		let trigger_memory = trigger_cap / JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER;

		build.enable_compact_resident_index_rescue();
		let compacted = build
			.try_compact_near_cap_resident_index(JoinKeyPlan::Int64, false, trigger_memory)
			.expect("near-cap hash index compacts");
		assert!(compacted, "the near-cap rescue reports that it compacted");
		assert!(build.uses_compact_int64_index());
		assert_eq!(build.resident_index_bytes(), build.computed_index_bytes());
		assert!(
			build.resident_state().total()
				<= join_spill_retained_footprint_memory_cap(trigger_memory),
			"compact rescue should bring the synthetic build under the retained cap"
		);

		let compact_resident = build.resident_state().total();
		let failing_memory = compact_resident.saturating_sub(1)
			/ JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER;
		match build
			.guard_resident_state(failing_memory, SpillResidentGuardSite::Write)
			.expect_err("compact index must still fail when genuinely over cap")
		{
			ExecutionError::BudgetExhausted {
				producer,
				requested,
				available,
				detail: crate::error::BudgetExhaustedDetail::Message(detail),
				..
			} => {
				assert_eq!(producer, "streamed inner join build spill resident state");
				assert_eq!(requested.get(), compact_resident);
				assert!(available.get() < requested.get());
				assert!(detail.contains("site=write"));
				assert!(detail.contains("resident_index_bytes="));
			}
			other => panic!("expected resident budget exhaustion, got {other:?}"),
		}

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[test]
	fn compact_spilled_batch_ref_rejects_page_index_truncation() {
		let err = CompactSpilledBatchRowRef::try_from_ref(SpilledBatchRowRef {
			page_index: usize::try_from(u64::from(u32::MAX) + 1).unwrap_or(usize::MAX),
			row: 7,
		})
		.expect_err("oversized page index must not truncate");
		match err {
			ExecutionError::Kernel { message } => {
				assert!(message.contains("exceeds compact index range"));
			}
			other => panic!("expected kernel error, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn incremental_existence_membership_duplicates_add_no_bytes() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("existence_membership");
		let mut build = SpilledSemiAntiExistenceBuild::create(
			budget,
			Arc::new(BlockingFileOps),
			spill_dir.clone(),
		)
		.await
		.expect("create existence spill");
		let keys = [ColumnOrdinal(0)];

		build
			.write_membership_key_chunks(
				&batch_with_keys(&[1, 1, 2]),
				JoinKeyPlan::Int64,
				&keys,
				budget,
			)
			.expect("write membership keys");
		let expected = int64_base(1).saturating_add(int64_base(2));
		assert_eq!(build.resident_index_bytes(), expected);
		assert_eq!(build.resident_index_bytes(), build.folded_index_bytes());

		build
			.set_projection(batch_with_keys(&[0]).schema(), vec![ColumnOrdinal(0)])
			.expect("set projection");
		build
			.write_projected_membership_key_chunks(
				&batch_with_keys(&[1, 2, 2]),
				JoinKeyPlan::Int64,
				&keys,
				budget,
			)
			.expect("write projected duplicate membership keys");
		assert_eq!(build.resident_index_bytes(), expected);
		assert_eq!(build.resident_index_bytes(), build.folded_index_bytes());

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn incremental_existence_payload_index_tracks_projected_writes() {
		let budget = test_task_budget();
		let spill_dir = test_spill_dir("existence_payload");
		let mut build = SpilledSemiAntiExistenceBuild::create(
			budget,
			Arc::new(BlockingFileOps),
			spill_dir.clone(),
		)
		.await
		.expect("create existence spill");
		let full_batch = batch_with_keys(&[0]);
		build
			.set_projection(
				full_batch.schema(),
				vec![ColumnOrdinal(0), ColumnOrdinal(1)],
			)
			.expect("set projection");
		let keys = [ColumnOrdinal(0)];
		let ref_size = row_ref_size::<SpilledExistenceRowRef>();

		build
			.write_projected_batch_chunks(&batch_with_keys(&[1]), JoinKeyPlan::Int64, &keys, budget)
			.await
			.expect("write projected first key");
		let mut expected = int64_base(1).saturating_add(ref_size);
		assert_eq!(build.resident_index_bytes(), expected);
		assert_eq!(build.resident_index_bytes(), build.folded_index_bytes());

		build
			.write_projected_batch_chunks(
				&batch_with_keys(&[1, 2]),
				JoinKeyPlan::Int64,
				&keys,
				budget,
			)
			.await
			.expect("write projected duplicate and new key");
		expected = expected
			.saturating_add(ref_size)
			.saturating_add(int64_base(2))
			.saturating_add(ref_size);
		assert_eq!(build.resident_index_bytes(), expected);
		assert_eq!(build.resident_index_bytes(), build.folded_index_bytes());

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn incremental_resident_guard_decisions_match_fold() {
		let budget = test_task_budget();
		let inner_dir = test_spill_dir("inner_guard");
		let mut inner =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), inner_dir.clone())
				.await
				.expect("create inner spill");
		inner
			.write_batch_chunks(
				&batch_with_keys(&[1, 2, 2]),
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				budget,
			)
			.await
			.expect("write inner guard batch");
		assert_eq!(inner.resident_index_bytes(), inner.folded_index_bytes());
		let inner_resident = align_inner_guard_resident_to_multiplier(&mut inner);
		let inner_equal = inner_resident / JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER;
		for memory_cap in [
			inner_equal.saturating_sub(1),
			inner_equal,
			inner_equal.saturating_add(1),
		] {
			assert_eq!(
				inner
					.guard_resident_state(memory_cap, SpillResidentGuardSite::Finish)
					.is_err(),
				fold_based_inner_guard_rejects(&inner, memory_cap)
			);
		}
		inner.cleanup().await;
		let _ = std::fs::remove_dir_all(inner_dir);

		let existence_dir = test_spill_dir("existence_guard");
		let mut existence = SpilledSemiAntiExistenceBuild::create(
			budget,
			Arc::new(BlockingFileOps),
			existence_dir.clone(),
		)
		.await
		.expect("create existence spill");
		existence
			.write_batch_chunks(
				&batch_with_keys(&[1, 2, 2]),
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				budget,
			)
			.await
			.expect("write existence guard batch");
		assert_eq!(
			existence.resident_index_bytes(),
			existence.folded_index_bytes()
		);
		let existence_resident = align_existence_guard_resident_to_multiplier(&mut existence);
		let existence_equal =
			existence_resident / JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER;
		for memory_cap in [
			existence_equal.saturating_sub(1),
			existence_equal,
			existence_equal.saturating_add(1),
		] {
			assert_eq!(
				existence.guard_resident_state(memory_cap).is_err(),
				fold_based_existence_guard_rejects(&existence, memory_cap)
			);
		}
		existence.cleanup().await;
		let _ = std::fs::remove_dir_all(existence_dir);
	}

	#[tokio::test]
	async fn inner_spill_guard_reports_component_detail_and_site() {
		let budget = test_task_budget();
		let memory_cap = 1024;
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		let spill_dir = test_spill_dir("inner_guard_detail");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");
		build.resident_index_bytes = cap;
		build.writer_peak = 1;

		let err = build
			.guard_resident_state(memory_cap, SpillResidentGuardSite::Finish)
			.expect_err("resident state must fail");
		match err {
			ExecutionError::BudgetExhausted {
				producer,
				requested,
				available,
				detail: crate::error::BudgetExhaustedDetail::Message(detail),
				..
			} => {
				assert_eq!(producer, "streamed inner join build spill resident state");
				assert_eq!(requested.get(), cap + 1 + JOIN_SPILL_PAGE_SIZE as u64);
				assert_eq!(available.get(), cap);
				assert!(detail.contains("site=finish"));
				assert!(detail.contains("resident_baseline_bytes=0"));
				assert!(detail.contains("resident_index_bytes=16384"));
				assert!(detail.contains("writer_peak_bytes=1"));
				assert!(detail.contains("page_reserve_bytes=65536"));
				assert!(detail.contains("pinned_decoded_bytes=0"));
			}
			other => panic!("expected detailed budget exhaustion, got {other:?}"),
		}

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn inner_spill_guard_adds_explicit_baseline_without_double_counting_it() {
		let budget = test_task_budget();
		let memory_cap = 8 * 1024;
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		let baseline = 60_000;
		let local_index = 10_000;
		assert!(baseline + JOIN_SPILL_PAGE_SIZE as u64 <= cap);
		assert!(local_index + JOIN_SPILL_PAGE_SIZE as u64 <= cap);
		let spill_dir = test_spill_dir("inner_guard_baseline");
		let mut build = SpilledInnerJoinBuild::create_with_resident_baseline(
			budget,
			Arc::new(BlockingFileOps),
			spill_dir.clone(),
			baseline,
		)
		.await
		.expect("create inner spill with baseline");
		assert_eq!(
			build.retained_resident_bytes(),
			JOIN_SPILL_PAGE_SIZE as u64,
			"only the spill's local page reserve is exported; the owner baseline is not"
		);
		build.resident_index_bytes = local_index;
		let error = build
			.guard_resident_state(memory_cap, SpillResidentGuardSite::Finish)
			.expect_err("combined baseline and local state must cross the guard");
		match error {
			ExecutionError::BudgetExhausted {
				requested,
				available,
				detail: crate::error::BudgetExhaustedDetail::Message(detail),
				..
			} => {
				assert_eq!(
					requested.get(),
					baseline + local_index + JOIN_SPILL_PAGE_SIZE as u64
				);
				assert_eq!(available.get(), cap);
				assert!(detail.contains("resident_baseline_bytes=60000"));
				assert!(detail.contains("resident_index_bytes=10000"));
			}
			other => panic!("expected combined resident guard failure, got {other:?}"),
		}

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn inner_spill_write_guard_sheds_writer_scratch_but_not_index_pressure() {
		let budget = test_task_budget();
		let memory_cap = 8 * 1024;
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		let spill_dir = test_spill_dir("inner_guard_sheds_writer");
		let mut build =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("create inner spill");

		build.resident_index_bytes = cap - JOIN_SPILL_PAGE_SIZE as u64;
		build.writer_peak = 1;
		build.scratch_plaintext = Vec::with_capacity(64);
		build.scratch_refs = Vec::with_capacity(64);
		build
			.guard_resident_state_after_write(memory_cap)
			.expect("writer scratch should be shed before failing");
		assert_eq!(build.writer_peak, 0);
		assert_eq!(build.scratch_plaintext.capacity(), 0);
		assert_eq!(build.scratch_refs.capacity(), 0);

		build.resident_index_bytes = cap - JOIN_SPILL_PAGE_SIZE as u64 + 1;
		build
			.guard_resident_state_after_write(memory_cap)
			.expect_err("resident index plus page reserve still exceeds the cap");

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn incremental_resident_counters_start_fresh_for_new_spills() {
		let budget = test_task_budget();
		let inner_dir = test_spill_dir("inner_fresh");
		let mut first_inner =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), inner_dir.clone())
				.await
				.expect("create first inner spill");
		first_inner
			.write_batch_chunks(
				&batch_with_keys(&[1, 2]),
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				false,
				budget,
			)
			.await
			.expect("write first inner spill");
		assert!(first_inner.resident_index_bytes() > 0);
		first_inner.cleanup().await;
		let second_inner =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), inner_dir.clone())
				.await
				.expect("create second inner spill");
		assert_eq!(second_inner.resident_index_bytes(), 0);
		assert_eq!(
			second_inner.resident_index_bytes(),
			second_inner.folded_index_bytes()
		);
		second_inner.cleanup().await;
		let _ = std::fs::remove_dir_all(inner_dir);

		let existence_dir = test_spill_dir("existence_fresh");
		let mut first_existence = SpilledSemiAntiExistenceBuild::create(
			budget,
			Arc::new(BlockingFileOps),
			existence_dir.clone(),
		)
		.await
		.expect("create first existence spill");
		first_existence
			.write_membership_key_chunks(
				&batch_with_keys(&[1, 2]),
				JoinKeyPlan::Int64,
				&[ColumnOrdinal(0)],
				budget,
			)
			.expect("write first existence spill");
		assert!(first_existence.resident_index_bytes() > 0);
		first_existence.cleanup().await;
		let second_existence = SpilledSemiAntiExistenceBuild::create(
			budget,
			Arc::new(BlockingFileOps),
			existence_dir.clone(),
		)
		.await
		.expect("create second existence spill");
		assert_eq!(second_existence.resident_index_bytes(), 0);
		assert_eq!(
			second_existence.resident_index_bytes(),
			second_existence.folded_index_bytes()
		);
		second_existence.cleanup().await;
		let _ = std::fs::remove_dir_all(existence_dir);
	}
}
