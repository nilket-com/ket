use super::*;
use twox_hash::XxHash3_64;

const JOIN_SPILL_PAGE_SIZE: usize = 64 * 1024;
const JOIN_SPILL_CACHE_TARGET_BYTES: u64 = 8 * 1024 * 1024;
static NEXT_JOIN_SPILL_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn partition_bucket(
	key_arrays: &[ArrayRef],
	row: usize,
	partition_count: u32,
) -> Result<usize, ExecutionError> {
	let mut encoded = Vec::new();
	partition_bucket_with_scratch(key_arrays, row, partition_count, &mut encoded)
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
	Ok(
		(partition_key_hash_with_scratch(key_arrays, row, encoded)? % u64::from(partition_count))
			as usize,
	)
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
		.map(|hash| hash.map(|hash| (hash % u64::from(partition_count)) as usize))
}

fn partition_key_hash_with_scratch(
	key_arrays: &[ArrayRef],
	row: usize,
	encoded: &mut Vec<u8>,
) -> Result<u64, ExecutionError> {
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

#[derive(Debug, Clone)]
struct CachedJoinSpillPage {
	bytes: Vec<u8>,
}

pub(in crate::exec) struct SpilledJoinBuild {
	spill_id: u64,
	path: PathBuf,
	file_ops: Arc<dyn AsyncFileOps>,
	file: FileHandle,
	key: LessSafeKey,
	pages: Vec<JoinSpillPage>,
	index: HashMap<Vec<u8>, Vec<usize>>,
	cache: HashMap<usize, CachedJoinSpillPage>,
	lru: VecDeque<usize>,
	cache_bytes: u64,
	cache_cap: u64,
}

impl SpilledJoinBuild {
	pub(in crate::exec) async fn write(
		build_rows: HashMap<Vec<u8>, Vec<u32>>,
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
		let mut pages = Vec::new();
		let mut index: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
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
				for (key, _) in &current_entries {
					index.entry(key.clone()).or_default().push(page_index);
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
			for (key, _) in &current_entries {
				index.entry(key.clone()).or_default().push(page_index);
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

		let index_bytes = join_spill_index_bytes(&index);
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
	) -> Result<Vec<u32>, ExecutionError> {
		let Some(page_indices) = self.index.get(join_key).cloned() else {
			return Ok(Vec::new());
		};
		let mut rows = Vec::new();
		for page_index in page_indices {
			let page = self.read_page(page_index).await?;
			rows.extend(decode_join_spill_page_for_key(&page, join_key)?);
		}
		Ok(rows)
	}

	pub(in crate::exec) async fn read_page(
		&mut self,
		page_index: usize,
	) -> Result<Vec<u8>, ExecutionError> {
		if let Some(cached) = self.cache.get(&page_index) {
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
		while self.cache_bytes.saturating_add(bytes_len) > self.cache_cap {
			let Some(evicted) = self.lru.pop_front() else {
				break;
			};
			if let Some(page) = self.cache.remove(&evicted) {
				self.cache_bytes = self
					.cache_bytes
					.saturating_sub(u64::try_from(page.bytes.len()).unwrap_or(u64::MAX));
			}
		}
		self.cache_bytes = self.cache_bytes.saturating_add(bytes_len);
		self.cache.insert(page_index, CachedJoinSpillPage { bytes });
		self.touch_cache_entry(page_index);
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) struct SpilledBatchRowRef {
	pub(super) page_index: usize,
	pub(super) row: u32,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::exec) struct SpilledInnerJoinMatch {
	pub(super) build: SpilledBatchRowRef,
	pub(super) probe: u32,
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
	scratch_plaintext: Vec<u8>,
	scratch_refs: Vec<(JoinKey, u32)>,
}

impl SpilledInnerJoinBuild {
	pub(in crate::exec) async fn create(
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
		spill_dir: PathBuf,
	) -> Result<Self, ExecutionError> {
		let spill_id = NEXT_JOIN_SPILL_ID.fetch_add(1, Ordering::SeqCst);
		let path = spill_dir.join(format!("spill_{}_{}.inner", std::process::id(), spill_id));
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
		Ok(Self {
			spill_id,
			path,
			file_ops,
			file,
			key,
			pages: Vec::new(),
			page_rows: Vec::new(),
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
			scratch_plaintext: Vec::new(),
			scratch_refs: Vec::new(),
		})
	}

	pub(in crate::exec) async fn write_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
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
			self.write_page_batch(
				&chunk,
				key_plan,
				build_keys,
				null_safe_keys,
				false,
				task_budget,
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
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			self.write_page_batch_inner(
				&chunk,
				key_plan,
				build_keys,
				null_safe_keys,
				false,
				false,
				task_budget,
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
				false,
				task_budget,
			)
			.await?;
		}
		Ok(())
	}

	pub(in crate::exec) async fn write_projected_batch_chunks_unindexed(
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
			self.write_page_batch_inner(
				&projected,
				key_plan,
				projected_build_keys,
				null_safe_keys,
				false,
				false,
				task_budget,
			)
			.await?;
		}
		Ok(())
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
		)
		.await
	}

	async fn write_page_batch_inner(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		retain_unkeyed_rows: bool,
		index_rows: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
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
		if refs.is_empty() && !retain_unkeyed_rows {
			self.scratch_refs = refs;
			return Ok(());
		}

		let page_index = self.pages.len();
		self.scratch_plaintext.clear();
		encode_record_batch_ipc_into(&mut self.scratch_plaintext, batch)?;
		let plaintext = std::mem::take(&mut self.scratch_plaintext);
		let (page, mut plaintext) = write_join_spill_plaintext_page_returning_buffer(
			self.file_ops.as_ref(),
			&self.file,
			&self.key,
			self.spill_id,
			page_index,
			self.next_offset,
			plaintext,
		)
		.await?;
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
		if index_rows {
			for (key, row) in refs.drain(..) {
				append_spilled_batch_row_ref(
					&mut self.index,
					&mut self.resident_index_bytes,
					key,
					SpilledBatchRowRef { page_index, row },
				);
				self.rows = self.rows.saturating_add(1);
			}
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
		self.guard_resident_state(task_budget.memory.get())?;
		Ok(())
	}

	pub(in crate::exec) async fn rebuild_index_from_pages(
		&mut self,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		self.index.clear();
		self.resident_index_bytes = 0;
		self.rows = 0;
		for page_index in 0..self.pages.len() {
			let batch = self.read_batch_page(page_index).await?;
			let key_arrays = key_arrays(&batch, build_keys)?;
			for row in 0..batch.num_rows() {
				let Some(key) = join_key_at(key_plan, &key_arrays, row, null_safe_keys)? else {
					continue;
				};
				append_spilled_batch_row_ref(
					&mut self.index,
					&mut self.resident_index_bytes,
					key,
					SpilledBatchRowRef {
						page_index,
						row: join_build_row_index(row)?,
					},
				);
				self.rows = self.rows.saturating_add(1);
			}
			self.guard_resident_state(task_budget.memory.get())?;
		}
		Ok(())
	}

	pub(in crate::exec) fn release_write_scratch(&mut self) {
		self.scratch_plaintext = Vec::new();
		self.scratch_refs = Vec::new();
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
				"streamed inner join build spill resident state",
				resident,
				cap,
			));
		}
		Ok(())
	}

	pub(in crate::exec) async fn lookup(
		&mut self,
		join_key: &JoinKey,
	) -> Result<Vec<SpilledBatchRowRef>, ExecutionError> {
		Ok(self.index.get(join_key).cloned().unwrap_or_default())
	}

	pub(in crate::exec) fn page_count(&self) -> usize {
		self.pages.len()
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
		let bytes = self.read_page(page_index).await?;
		decode_record_batch_ipc(&bytes)
	}

	pub(in crate::exec) async fn read_page(
		&mut self,
		page_index: usize,
	) -> Result<Vec<u8>, ExecutionError> {
		if let Some(cached) = self.cache.get(&page_index) {
			let bytes = cached.bytes.clone();
			self.touch_cache_entry(page_index);
			return Ok(bytes);
		}

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
		)
		.await?;
		let nonce = Nonce::assume_unique_for_key(join_spill_nonce(self.spill_id, page_index));
		let plaintext = self
			.key
			.open_in_place(nonce, Aad::empty(), &mut ciphertext)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("inner join batch spill decrypt failed: {error:?}"),
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
		while self.cache_bytes.saturating_add(bytes_len) > self.cache_cap {
			let Some(evicted) = self.lru.pop_front() else {
				break;
			};
			if let Some(page) = self.cache.remove(&evicted) {
				self.cache_bytes = self
					.cache_bytes
					.saturating_sub(u64::try_from(page.bytes.len()).unwrap_or(u64::MAX));
			}
		}
		self.cache_bytes = self.cache_bytes.saturating_add(bytes_len);
		self.cache.insert(page_index, CachedJoinSpillPage { bytes });
		self.touch_cache_entry(page_index);
	}

	#[cfg(test)]
	pub(in crate::exec) fn input_batches_written(&self) -> u64 {
		self.input_batches_written
	}

	#[cfg(test)]
	pub(in crate::exec) fn resident_index_bytes(&self) -> u64 {
		self.resident_index_bytes
	}

	#[cfg(test)]
	pub(in crate::exec) fn folded_index_bytes(&self) -> u64 {
		spilled_batch_join_index_bytes(&self.index)
	}

	pub(in crate::exec) fn input_row_count(&self) -> u64 {
		self.input_rows
	}

	pub(in crate::exec) fn spill_bytes(&self) -> u64 {
		self.spill_bytes
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
		if let Some(cached) = self.cache.get(&page_index) {
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
		while self.cache_bytes.saturating_add(bytes_len) > self.cache_cap {
			let Some(evicted) = self.lru.pop_front() else {
				break;
			};
			if let Some(page) = self.cache.remove(&evicted) {
				self.cache_bytes = self
					.cache_bytes
					.saturating_sub(u64::try_from(page.bytes.len()).unwrap_or(u64::MAX));
			}
		}
		self.cache_bytes = self.cache_bytes.saturating_add(bytes_len);
		self.cache.insert(page_index, CachedJoinSpillPage { bytes });
		self.touch_cache_entry(page_index);
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
		file_ops, file, key, spill_id, page_index, offset, plaintext,
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
) -> Result<(JoinSpillPage, Vec<u8>), ExecutionError> {
	let plaintext_len = plaintext.len();
	let nonce = Nonce::assume_unique_for_key(join_spill_nonce(spill_id, page_index));
	key.seal_in_place_append_tag(nonce, Aad::empty(), &mut plaintext)
		.map_err(|error| ExecutionError::Kernel {
			message: format!("join spill encrypt failed: {error:?}"),
		})?;
	let ciphertext_len = plaintext.len();
	let plaintext = write_plain_all_returning_buffer(file_ops, file, offset, plaintext).await?;
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
) -> Result<Vec<u8>, ExecutionError> {
	let mut out = Vec::with_capacity(len);
	while out.len() < len {
		let chunk = file_ops
			.read_plain_at(
				file,
				offset.saturating_add(out.len() as u64),
				len.saturating_sub(out.len()),
			)
			.await
			.map_err(|error| join_spill_io_error("read", error))?;
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

#[cfg(test)]
fn spilled_batch_join_index_bytes(index: &HashMap<JoinKey, Vec<SpilledBatchRowRef>>) -> u64 {
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

	fn test_task_budget() -> TaskBudget {
		let mut query_budget = QueryBudget::default();
		query_budget.memory_per_task = ByteCount::new(64 * 1024 * 1024);
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
			vec![
				SpilledBatchRowRef {
					page_index: 0,
					row: 1
				},
				SpilledBatchRowRef {
					page_index: 0,
					row: 2
				}
			]
		);
		assert_eq!(
			build
				.lookup(&JoinKey::Int64(5))
				.await
				.expect("lookup key 5"),
			vec![SpilledBatchRowRef {
				page_index: 1,
				row: 1
			}]
		);

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
		let keys = [ColumnOrdinal(0)];
		let ref_size = row_ref_size::<SpilledBatchRowRef>();

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
		let mut expected = int64_base(1).saturating_add(ref_size);
		assert_eq!(build.resident_index_bytes(), expected);
		assert_eq!(build.resident_index_bytes(), build.folded_index_bytes());

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
		expected = expected
			.saturating_add(ref_size)
			.saturating_add(int64_base(2))
			.saturating_add(ref_size);
		assert_eq!(build.resident_index_bytes(), expected);
		assert_eq!(build.resident_index_bytes(), build.folded_index_bytes());

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
		expected = expected
			.saturating_add(ref_size)
			.saturating_add(int64_base(3))
			.saturating_add(ref_size)
			.saturating_add(ref_size);
		assert_eq!(build.resident_index_bytes(), expected);
		assert_eq!(build.resident_index_bytes(), build.folded_index_bytes());

		build.cleanup().await;
		let _ = std::fs::remove_dir_all(spill_dir);
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
				inner.guard_resident_state(memory_cap).is_err(),
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
