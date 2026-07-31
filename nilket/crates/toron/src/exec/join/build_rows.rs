use std::collections::HashMap;
use std::time::Instant;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array};

use crate::error::ExecutionError;

use super::{
	InMemoryProbeProfile, JoinKey, JoinKeyPlan, JoinKind, JoinMatchResidual, JoinOutputRow,
	append_join_matches_for_kind, append_unmatched_probe_row, join_key_bytes,
	join_key_bytes_null_safe,
};

#[derive(Debug, Clone)]
pub(in crate::exec) struct BuildRowList {
	first: u32,
	overflow: BuildRowOverflow,
}

pub(in crate::exec) fn new_join_build_rows(capacity: usize) -> JoinBuildRows {
	JoinBuildRows::new_hash(capacity)
}

// Executor-internal join maps are private, per-task structures, but their keys still come from
// table data. Use a per-map keyed fast hasher here rather than a fixed hash function.
// The random hash seed is safe for determinism: join output is emitted in probe-row order,
// never by iterating the build map.
pub(in crate::exec) fn new_int64_join_build_rows(capacity: usize) -> JoinBuildRows {
	JoinBuildRows::new_int64(capacity)
}

pub(in crate::exec) fn new_int32_join_build_rows(capacity: usize) -> JoinBuildRows {
	JoinBuildRows::new_int32(capacity)
}

pub(in crate::exec) struct JoinBuildRows {
	inner: JoinBuildRowsInner,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct JoinBuildProfileShape {
	pub(super) representation: &'static str,
	pub(super) distinct_keys: u64,
	pub(super) entry_capacity: u64,
	pub(super) bucket_capacity: u64,
	pub(super) index_accounted_bytes: Option<u64>,
	pub(super) index_accounted_bytes_unknown_reason: Option<&'static str>,
}

enum JoinBuildRowsInner {
	Hash(HashMap<JoinKey, BuildRowList, RandomState>),
	Int32(PrimitiveBuildTable<i32>),
	Int64(PrimitiveBuildTable<i64>),
}

struct PrimitiveBuildTable<K> {
	buckets: Vec<usize>,
	entries: Vec<PrimitiveBuildEntry<K>>,
	hasher: RandomState,
}

struct PrimitiveBuildEntry<K> {
	key: K,
	rows: BuildRowList,
}

const EMPTY_PRIMITIVE_BUCKET: usize = usize::MAX;

/// ADR 497: how many probe keys are staged together so their bucket/entry
/// loads are in flight concurrently instead of each probe paying its two
/// dependent misses serially.
pub(in crate::exec) const PROBE_BATCH_WINDOW: usize = 64;

/// ADR 524: profile generic-key probes in the same batch-scoped cadence as the primitive path.
/// The production arm keeps its historical scalar loop; the profiler arm stages key encoding and
/// lookups so lookup/key reconstruction and match expansion have disjoint clocks.
#[allow(clippy::too_many_arguments)]
pub(super) fn probe_generic_key_rows_profiled(
	kind: JoinKind,
	rows: usize,
	key_arrays: &[ArrayRef],
	null_safe_keys: bool,
	build_rows: &JoinBuildRows,
	matched_build_rows: &mut Option<Vec<u64>>,
	selection: &mut Vec<JoinOutputRow>,
	residual: &mut JoinMatchResidual<'_>,
	profile: &mut InMemoryProbeProfile,
) -> Result<(), ExecutionError> {
	let mut window_keys = Vec::with_capacity(PROBE_BATCH_WINDOW);
	let mut window_lookups = Vec::with_capacity(PROBE_BATCH_WINDOW);
	let mut row = 0usize;
	while row < rows {
		let end = (row + PROBE_BATCH_WINDOW).min(rows);
		let lookup_started = Instant::now();
		window_keys.clear();
		for key_row in row..end {
			let key = if null_safe_keys {
				Some(JoinKey::Generic(join_key_bytes_null_safe(
					key_arrays, key_row,
				)?))
			} else {
				join_key_bytes(key_arrays, key_row)?.map(JoinKey::Generic)
			};
			window_keys.push(key);
		}
		profile.record_lookup_window(window_keys.iter().filter(|key| key.is_some()).count());
		window_lookups.clear();
		window_lookups.extend(
			window_keys
				.iter()
				.map(|key| key.as_ref().and_then(|key| build_rows.get(key))),
		);
		profile.record_lookup_duration(lookup_started.elapsed());

		let match_started = Instant::now();
		for (offset, (key, lookup)) in window_keys.iter().zip(&window_lookups).enumerate() {
			let probe_row = row + offset;
			let matched = if key.is_none() {
				false
			} else if let Some(matches) = lookup {
				append_join_matches_for_kind(
					kind,
					probe_row,
					matches.rows(),
					matched_build_rows.as_deref_mut(),
					selection,
					residual,
				)?
			} else {
				false
			};
			if !matched {
				append_unmatched_probe_row(kind, probe_row, selection)?;
			}
		}
		profile.record_match_expand_duration(match_started.elapsed());
		row = end;
	}
	Ok(())
}

const BATCHED_PROBE_PREFETCH_ENV: &str = "TORON_BATCHED_PROBE_PREFETCH";

pub(super) fn batched_probe_prefetch_enabled_from_env(value: Option<&str>) -> bool {
	!matches!(value, Some("0"))
}

/// ADR 497 ablation: `=0` restores the one-key-at-a-time probe. Default on.
pub(super) fn batched_probe_prefetch_enabled() -> bool {
	static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
	*ENABLED.get_or_init(|| {
		batched_probe_prefetch_enabled_from_env(
			std::env::var(BATCHED_PROBE_PREFETCH_ENV).ok().as_deref(),
		)
	})
}

const BATCHED_BUILD_PREFETCH_ENV: &str = "TORON_BATCHED_BUILD_PREFETCH";

pub(super) fn batched_build_prefetch_enabled_from_env(value: Option<&str>) -> bool {
	!matches!(value, Some("0"))
}

/// ADR 503 ablation: `=0` restores the one-key-at-a-time insert. Default on.
pub(super) fn batched_build_prefetch_enabled() -> bool {
	static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
	*ENABLED.get_or_init(|| {
		batched_build_prefetch_enabled_from_env(
			std::env::var(BATCHED_BUILD_PREFETCH_ENV).ok().as_deref(),
		)
	})
}

/// How a primitive build feeds its keys to the table: staged through a prefetch window, or one key
/// at a time. Passed into the driver rather than read inside it because the environment answer is
/// latched in a `OnceLock` — a process can only ever observe one arm, which would leave the ablation
/// path permanently unexercisable from a test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) enum BuildInsertMode {
	Windowed,
	Scalar,
}

impl BuildInsertMode {
	/// How many keys are staged before a flush. `Scalar` is window 1, which is what makes it the
	/// true one-key-at-a-time path rather than a smaller batch.
	fn window(self) -> usize {
		match self {
			BuildInsertMode::Windowed => PROBE_BATCH_WINDOW,
			BuildInsertMode::Scalar => 1,
		}
	}
}

/// The mode production runs in, from the latched ablation env.
pub(in crate::exec) fn build_insert_mode() -> BuildInsertMode {
	if batched_build_prefetch_enabled() {
		BuildInsertMode::Windowed
	} else {
		BuildInsertMode::Scalar
	}
}

/// Best-effort prefetch of the cache line holding `value`. Additive over the
/// batching itself; on non-x86_64 targets the staged loads alone provide the
/// memory-level parallelism and results are byte-identical.
#[inline(always)]
fn prefetch_read<T>(value: *const T) {
	#[cfg(target_arch = "x86_64")]
	// SAFETY: prefetch is a hint; it has no effect on program semantics and
	// tolerates any address.
	unsafe {
		core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(value as *const i8);
	}
	#[cfg(not(target_arch = "x86_64"))]
	let _ = value;
}

/// ADR 503: windows primitive build keys through the batched-prefetch insert, mirroring
/// `probe_primitive_key_rows`. `TORON_BATCHED_BUILD_PREFETCH=0` (window=1, scalar insert)
/// restores the true one-key-at-a-time path. Returns `Ok(None)` on the memory bail so the
/// caller can propagate it.
pub(in crate::exec) fn build_primitive_key_rows<K: Copy>(
	build_rows: &mut JoinBuildRows,
	rows: usize,
	memory_cap: u64,
	left_key_arrays: &[ArrayRef],
	inserted_rows: &mut usize,
	mode: BuildInsertMode,
	key_at: impl Fn(usize) -> Option<K>,
	insert_window: impl Fn(&mut JoinBuildRows, &[K], &[u32]),
	insert_scalar: impl Fn(&mut JoinBuildRows, K, u32),
) -> Result<Option<()>, ExecutionError> {
	let window = mode.window();
	let mut key_window: Vec<K> = Vec::with_capacity(window);
	let mut row_window: Vec<u32> = Vec::with_capacity(window);
	let flush = |build_rows: &mut JoinBuildRows, keys: &[K], row_ids: &[u32]| match mode {
		BuildInsertMode::Windowed => insert_window(build_rows, keys, row_ids),
		BuildInsertMode::Scalar => insert_scalar(build_rows, keys[0], row_ids[0]),
	};
	for row in 0..rows {
		let Some(key) = key_at(row) else {
			continue;
		};
		if !super::join_build_memory_fits(
			left_key_arrays,
			inserted_rows.saturating_add(1),
			memory_cap,
		) {
			// The pending (un-flushed) window is intentionally dropped: every caller discards the
			// partial table on `None` (build_in_memory_join_rows → streaming.rs `let Some(..) else`
			// fallbacks), so the counted-but-uninserted rows are never observed.
			return Ok(None);
		}
		key_window.push(key);
		row_window.push(super::join_build_row_index(row)?);
		*inserted_rows = inserted_rows.saturating_add(1);
		if key_window.len() == window {
			flush(build_rows, &key_window, &row_window);
			key_window.clear();
			row_window.clear();
		}
	}
	// The tail is only ever non-empty in `Windowed` mode — `Scalar` flushes on every key — but it
	// dispatches on the mode anyway so there is exactly one notion of "flush" in this driver.
	if !key_window.is_empty() {
		flush(build_rows, &key_window, &row_window);
	}
	Ok(Some(()))
}

pub(in crate::exec) fn build_in_memory_join_rows(
	join_key_plan: JoinKeyPlan,
	left_key_arrays: &[ArrayRef],
	rows: usize,
	memory_cap: u64,
	null_safe_keys: bool,
) -> Result<Option<JoinBuildRows>, ExecutionError> {
	build_in_memory_join_rows_with_insert_mode(
		join_key_plan,
		left_key_arrays,
		rows,
		memory_cap,
		null_safe_keys,
		build_insert_mode(),
	)
}

/// The build with its insert mode named by the caller. Production goes through
/// [`build_in_memory_join_rows`], which supplies the latched ablation answer; naming the mode here
/// is what lets a test drive both arms of ADR 503 in one process and compare their tables.
pub(in crate::exec) fn build_in_memory_join_rows_with_insert_mode(
	join_key_plan: JoinKeyPlan,
	left_key_arrays: &[ArrayRef],
	rows: usize,
	memory_cap: u64,
	null_safe_keys: bool,
	insert_mode: BuildInsertMode,
) -> Result<Option<JoinBuildRows>, ExecutionError> {
	let mut build_rows = match (join_key_plan, null_safe_keys) {
		(JoinKeyPlan::Int32, false) => new_int32_join_build_rows(rows),
		(JoinKeyPlan::Int64, false) => new_int64_join_build_rows(rows),
		_ => new_join_build_rows(rows),
	};
	let mut inserted_rows = 0usize;

	match join_key_plan {
		JoinKeyPlan::Int32 => {
			let values = super::single_join_key_array::<Int32Array>(left_key_arrays, "Int32")?;
			if build_primitive_key_rows(
				&mut build_rows,
				rows,
				memory_cap,
				left_key_arrays,
				&mut inserted_rows,
				insert_mode,
				|row| (!values.is_null(row)).then(|| values.value(row)),
				|build_rows, keys, rows| build_rows.insert_int32_batch(keys, rows),
				|build_rows, key, row| {
					super::insert_join_build_row(build_rows, JoinKey::Int32(key), row)
				},
			)?
			.is_none()
			{
				return Ok(None);
			}
		}
		JoinKeyPlan::Int64 => {
			let values = super::single_join_key_array::<Int64Array>(left_key_arrays, "Int64")?;
			if build_primitive_key_rows(
				&mut build_rows,
				rows,
				memory_cap,
				left_key_arrays,
				&mut inserted_rows,
				insert_mode,
				|row| (!values.is_null(row)).then(|| values.value(row)),
				|build_rows, keys, rows| build_rows.insert_int64_batch(keys, rows),
				|build_rows, key, row| {
					super::insert_join_build_row(build_rows, JoinKey::Int64(key), row)
				},
			)?
			.is_none()
			{
				return Ok(None);
			}
		}
		JoinKeyPlan::Generic => {
			for row in 0..rows {
				let key = if null_safe_keys {
					super::join_key_bytes_null_safe(left_key_arrays, row)?
				} else {
					let Some(key) = super::join_key_bytes(left_key_arrays, row)? else {
						continue;
					};
					key
				};
				if !super::join_build_memory_fits(
					left_key_arrays,
					inserted_rows.saturating_add(1),
					memory_cap,
				) {
					return Ok(None);
				}
				super::insert_join_build_row(
					&mut build_rows,
					JoinKey::Generic(key),
					super::join_build_row_index(row)?,
				);
				inserted_rows = inserted_rows.saturating_add(1);
			}
		}
	}

	Ok(Some(build_rows))
}

impl JoinBuildRows {
	fn new_hash(capacity: usize) -> Self {
		Self {
			inner: JoinBuildRowsInner::Hash(HashMap::with_capacity_and_hasher(
				capacity,
				RandomState::new(),
			)),
		}
	}

	fn new_int32(capacity: usize) -> Self {
		Self {
			inner: JoinBuildRowsInner::Int32(PrimitiveBuildTable::with_capacity(capacity)),
		}
	}

	fn new_int64(capacity: usize) -> Self {
		Self {
			inner: JoinBuildRowsInner::Int64(PrimitiveBuildTable::with_capacity(capacity)),
		}
	}

	pub(in crate::exec) fn insert(&mut self, key: JoinKey, row: u32) {
		match (&mut self.inner, key) {
			(JoinBuildRowsInner::Int32(table), JoinKey::Int32(key)) => {
				table.insert(key, row);
			}
			(JoinBuildRowsInner::Int64(table), JoinKey::Int64(key)) => {
				table.insert(key, row);
			}
			(JoinBuildRowsInner::Hash(rows), key) => match rows.entry(key) {
				std::collections::hash_map::Entry::Occupied(mut entry) => {
					entry.get_mut().push(row);
				}
				std::collections::hash_map::Entry::Vacant(entry) => {
					entry.insert(BuildRowList::new(row));
				}
			},
			(JoinBuildRowsInner::Int32(_), key) => {
				panic!("non-Int32 key inserted into Int32 join build table: {key:?}");
			}
			(JoinBuildRowsInner::Int64(_), key) => {
				panic!("non-Int64 key inserted into Int64 join build table: {key:?}");
			}
		}
	}

	pub(in crate::exec) fn get(&self, key: &JoinKey) -> Option<&BuildRowList> {
		match (&self.inner, key) {
			(JoinBuildRowsInner::Int32(table), JoinKey::Int32(key)) => table.get(*key),
			(JoinBuildRowsInner::Int64(table), JoinKey::Int64(key)) => table.get(*key),
			(JoinBuildRowsInner::Hash(rows), key) => rows.get(key),
			(JoinBuildRowsInner::Int32(_) | JoinBuildRowsInner::Int64(_), _) => None,
		}
	}

	pub(in crate::exec) fn get_int32(&self, key: i32) -> Option<&BuildRowList> {
		match &self.inner {
			JoinBuildRowsInner::Int32(table) => table.get(key),
			JoinBuildRowsInner::Hash(rows) => rows.get(&JoinKey::Int32(key)),
			JoinBuildRowsInner::Int64(_) => None,
		}
	}

	pub(in crate::exec) fn get_int64(&self, key: i64) -> Option<&BuildRowList> {
		match &self.inner {
			JoinBuildRowsInner::Int64(table) => table.get(key),
			JoinBuildRowsInner::Hash(rows) => rows.get(&JoinKey::Int64(key)),
			JoinBuildRowsInner::Int32(_) => None,
		}
	}

	/// ADR 497: batched Int64 probe window (≤ [`PROBE_BATCH_WINDOW`] keys).
	/// Falls back to per-key lookups on non-Int64 tables.
	pub(in crate::exec) fn get_int64_batch<'table>(
		&'table self,
		keys: &[Option<i64>],
		out: &mut Vec<Option<&'table BuildRowList>>,
	) {
		match &self.inner {
			JoinBuildRowsInner::Int64(table) => table.get_batch(keys, out),
			_ => out.extend(
				keys.iter()
					.map(|key| key.and_then(|key| self.get_int64(key))),
			),
		}
	}

	/// ADR 497: batched Int32 probe window (≤ [`PROBE_BATCH_WINDOW`] keys).
	pub(in crate::exec) fn get_int32_batch<'table>(
		&'table self,
		keys: &[Option<i32>],
		out: &mut Vec<Option<&'table BuildRowList>>,
	) {
		match &self.inner {
			JoinBuildRowsInner::Int32(table) => table.get_batch(keys, out),
			_ => out.extend(
				keys.iter()
					.map(|key| key.and_then(|key| self.get_int32(key))),
			),
		}
	}

	/// ADR 503: batched Int64 insert window. Per-key insert on non-Int64 tables.
	pub(in crate::exec) fn insert_int64_batch(&mut self, keys: &[i64], rows: &[u32]) {
		if let JoinBuildRowsInner::Int64(table) = &mut self.inner {
			table.insert_batch(keys, rows);
		} else {
			for (key, row) in keys.iter().zip(rows) {
				self.insert(JoinKey::Int64(*key), *row);
			}
		}
	}

	/// ADR 503: batched Int32 insert window. Per-key insert on non-Int32 tables.
	pub(in crate::exec) fn insert_int32_batch(&mut self, keys: &[i32], rows: &[u32]) {
		if let JoinBuildRowsInner::Int32(table) = &mut self.inner {
			table.insert_batch(keys, rows);
		} else {
			for (key, row) in keys.iter().zip(rows) {
				self.insert(JoinKey::Int32(*key), *row);
			}
		}
	}

	pub(in crate::exec) fn values(&self) -> Box<dyn Iterator<Item = &BuildRowList> + '_> {
		match &self.inner {
			JoinBuildRowsInner::Hash(rows) => Box::new(rows.values()),
			JoinBuildRowsInner::Int32(table) => Box::new(table.values()),
			JoinBuildRowsInner::Int64(table) => Box::new(table.values()),
		}
	}

	pub(in crate::exec) fn len(&self) -> usize {
		match &self.inner {
			JoinBuildRowsInner::Hash(rows) => rows.len(),
			JoinBuildRowsInner::Int32(table) => table.len(),
			JoinBuildRowsInner::Int64(table) => table.len(),
		}
	}

	pub(in crate::exec) fn accounted_bytes(&self) -> u64 {
		match &self.inner {
			JoinBuildRowsInner::Hash(rows) => {
				let buckets = rows.capacity().saturating_mul(
					std::mem::size_of::<(JoinKey, BuildRowList)>().saturating_add(16),
				);
				rows.iter().fold(
					u64::try_from(buckets).unwrap_or(u64::MAX),
					|total, (key, row_list)| {
						let key_bytes = match key {
							JoinKey::Generic(bytes) => bytes.capacity(),
							JoinKey::Int32(_) | JoinKey::Int64(_) => 0,
						};
						total.saturating_add(
							u64::try_from(
								key_bytes.saturating_add(
									row_list
										.heap_capacity_rows()
										.saturating_mul(std::mem::size_of::<u32>()),
								),
							)
							.unwrap_or(u64::MAX),
						)
					},
				)
			}
			JoinBuildRowsInner::Int32(table) => table.accounted_bytes(),
			JoinBuildRowsInner::Int64(table) => table.accounted_bytes(),
		}
	}

	pub(super) fn capacity(&self) -> usize {
		match &self.inner {
			JoinBuildRowsInner::Hash(rows) => rows.capacity(),
			JoinBuildRowsInner::Int32(table) => table.capacity(),
			JoinBuildRowsInner::Int64(table) => table.capacity(),
		}
	}

	pub(super) fn profile_shape(&self) -> JoinBuildProfileShape {
		let (
			representation,
			bucket_capacity,
			index_accounted_bytes,
			index_accounted_bytes_unknown_reason,
		) = match &self.inner {
			JoinBuildRowsInner::Hash(rows) => (
				"generic_hash",
				rows.capacity(),
				None,
				Some("generic variable-width ownership is not tracked without an entry walk"),
			),
			JoinBuildRowsInner::Int32(table) => (
				"int32_primitive",
				table.bucket_capacity(),
				Some(table.accounted_bytes()),
				None,
			),
			JoinBuildRowsInner::Int64(table) => (
				"int64_primitive",
				table.bucket_capacity(),
				Some(table.accounted_bytes()),
				None,
			),
		};
		JoinBuildProfileShape {
			representation,
			distinct_keys: u64::try_from(self.len()).unwrap_or(u64::MAX),
			entry_capacity: u64::try_from(self.capacity()).unwrap_or(u64::MAX),
			bucket_capacity: u64::try_from(bucket_capacity).unwrap_or(u64::MAX),
			index_accounted_bytes,
			index_accounted_bytes_unknown_reason,
		}
	}

	#[cfg(test)]
	pub(super) fn is_int32_table(&self) -> bool {
		matches!(self.inner, JoinBuildRowsInner::Int32(_))
	}

	#[cfg(test)]
	pub(super) fn is_int64_table(&self) -> bool {
		matches!(self.inner, JoinBuildRowsInner::Int64(_))
	}

	#[cfg(test)]
	pub(super) fn int32_bucket_capacity(&self) -> Option<usize> {
		match &self.inner {
			JoinBuildRowsInner::Int32(table) => Some(table.bucket_capacity()),
			JoinBuildRowsInner::Hash(_) | JoinBuildRowsInner::Int64(_) => None,
		}
	}

	#[cfg(test)]
	pub(super) fn int64_bucket_capacity(&self) -> Option<usize> {
		match &self.inner {
			JoinBuildRowsInner::Int64(table) => Some(table.bucket_capacity()),
			JoinBuildRowsInner::Hash(_) | JoinBuildRowsInner::Int32(_) => None,
		}
	}
}

impl<K> PrimitiveBuildTable<K>
where
	K: Copy + Eq + std::hash::Hash,
{
	fn with_capacity(capacity: usize) -> Self {
		let bucket_count = capacity.saturating_mul(2).next_power_of_two().max(1);
		Self {
			buckets: vec![EMPTY_PRIMITIVE_BUCKET; bucket_count],
			entries: Vec::with_capacity(capacity),
			hasher: RandomState::new(),
		}
	}

	fn insert(&mut self, key: K, row: u32) {
		let bucket = self.find_bucket(key);
		let entry_index = self.buckets[bucket];
		if entry_index == EMPTY_PRIMITIVE_BUCKET {
			let entry_index = self.entries.len();
			self.entries.push(PrimitiveBuildEntry {
				key,
				rows: BuildRowList::new(row),
			});
			self.buckets[bucket] = entry_index;
		} else {
			self.entries[entry_index].rows.push(row);
		}
	}

	/// ADR 503: batched insert, the mirror of [`Self::get_batch`]. Stages a window —
	/// hash + bucket-slot prefetch, then entry-line prefetch for occupied slots — then
	/// runs the *serial* insert-resolve, re-walking `buckets[]` fresh from each key's
	/// home slot. The prefetch is only a hint; the resolve reads the live table and
	/// processes keys in slot order, so a within-window key that collides with an
	/// earlier same-window insert resolves to exactly the slot `find_bucket` + `insert`
	/// would choose. Output is byte-identical to the one-key-at-a-time path.
	fn insert_batch(&mut self, keys: &[K], rows: &[u32]) {
		debug_assert!(keys.len() <= PROBE_BATCH_WINDOW);
		debug_assert_eq!(keys.len(), rows.len());
		let mut staged_buckets = [0usize; PROBE_BATCH_WINDOW];
		for (slot, key) in keys.iter().enumerate() {
			let bucket = self.bucket_for_key(*key);
			staged_buckets[slot] = bucket;
			prefetch_read(&self.buckets[bucket]);
		}
		for slot in 0..keys.len() {
			let entry_index = self.buckets[staged_buckets[slot]];
			if entry_index != EMPTY_PRIMITIVE_BUCKET {
				prefetch_read(&self.entries[entry_index]);
			}
		}
		let mask = self.buckets.len() - 1;
		for (slot, (key, row)) in keys.iter().zip(rows).enumerate() {
			let mut bucket = staged_buckets[slot];
			loop {
				let entry_index = self.buckets[bucket];
				if entry_index == EMPTY_PRIMITIVE_BUCKET {
					let entry_index = self.entries.len();
					self.entries.push(PrimitiveBuildEntry {
						key: *key,
						rows: BuildRowList::new(*row),
					});
					self.buckets[bucket] = entry_index;
					break;
				}
				if self.entries[entry_index].key == *key {
					self.entries[entry_index].rows.push(*row);
					break;
				}
				bucket = (bucket + 1) & mask;
			}
		}
	}

	fn get(&self, key: K) -> Option<&BuildRowList> {
		let bucket = self.find_bucket(key);
		let entry_index = self.buckets[bucket];
		if entry_index == EMPTY_PRIMITIVE_BUCKET {
			None
		} else {
			Some(&self.entries[entry_index].rows)
		}
	}

	/// ADR 497: batched probe. Stages a window of keys — hash + bucket-slot
	/// prefetch, then entry prefetch, then resolution — so the two dependent
	/// out-of-cache loads of independent probes overlap. A key whose first
	/// probed slot neither hits nor is empty falls to the scalar linear-probe
	/// tail, so every key resolves to exactly the bucket `find_bucket` would
	/// have chosen. `out[i]` corresponds to `keys[i]`; `None` keys are probe
	/// nulls and resolve to `None`.
	fn get_batch<'table>(
		&'table self,
		keys: &[Option<K>],
		out: &mut Vec<Option<&'table BuildRowList>>,
	) {
		debug_assert!(keys.len() <= PROBE_BATCH_WINDOW);
		let mut staged_buckets = [0usize; PROBE_BATCH_WINDOW];
		for (slot, key) in keys.iter().enumerate() {
			if let Some(key) = key {
				let bucket = self.bucket_for_key(*key);
				staged_buckets[slot] = bucket;
				prefetch_read(&self.buckets[bucket]);
			}
		}
		let mut staged_entries = [EMPTY_PRIMITIVE_BUCKET; PROBE_BATCH_WINDOW];
		for (slot, key) in keys.iter().enumerate() {
			if key.is_some() {
				let entry_index = self.buckets[staged_buckets[slot]];
				staged_entries[slot] = entry_index;
				if entry_index != EMPTY_PRIMITIVE_BUCKET {
					prefetch_read(&self.entries[entry_index]);
				}
			}
		}
		for (slot, key) in keys.iter().enumerate() {
			out.push(key.and_then(|key| {
				let entry_index = staged_entries[slot];
				if entry_index == EMPTY_PRIMITIVE_BUCKET {
					return None;
				}
				if self.entries[entry_index].key == key {
					return Some(&self.entries[entry_index].rows);
				}
				// Collision: continue the scalar linear probe from the next
				// slot, exactly as the unbatched path would.
				let mask = self.buckets.len() - 1;
				let mut bucket = (staged_buckets[slot] + 1) & mask;
				loop {
					let entry_index = self.buckets[bucket];
					if entry_index == EMPTY_PRIMITIVE_BUCKET {
						return None;
					}
					if self.entries[entry_index].key == key {
						return Some(&self.entries[entry_index].rows);
					}
					bucket = (bucket + 1) & mask;
				}
			}));
		}
	}

	fn values(&self) -> impl Iterator<Item = &BuildRowList> {
		self.entries.iter().map(|entry| &entry.rows)
	}

	fn len(&self) -> usize {
		self.entries.len()
	}

	fn capacity(&self) -> usize {
		self.entries.capacity()
	}

	fn bucket_capacity(&self) -> usize {
		self.buckets.len()
	}

	fn find_bucket(&self, key: K) -> usize {
		let mask = self.buckets.len() - 1;
		let mut bucket = self.bucket_for_key(key);
		loop {
			let entry_index = self.buckets[bucket];
			if entry_index == EMPTY_PRIMITIVE_BUCKET || self.entries[entry_index].key == key {
				return bucket;
			}
			bucket = (bucket + 1) & mask;
		}
	}

	fn bucket_for_key(&self, key: K) -> usize {
		(self.hasher.hash_one(key) as usize) & (self.buckets.len() - 1)
	}

	fn accounted_bytes(&self) -> u64 {
		let fixed = self
			.buckets
			.capacity()
			.saturating_mul(std::mem::size_of::<usize>())
			.saturating_add(
				self.entries
					.capacity()
					.saturating_mul(std::mem::size_of::<PrimitiveBuildEntry<K>>()),
			);
		self.entries
			.iter()
			.fold(u64::try_from(fixed).unwrap_or(u64::MAX), |total, entry| {
				total.saturating_add(
					u64::try_from(
						entry
							.rows
							.heap_capacity_rows()
							.saturating_mul(std::mem::size_of::<u32>()),
					)
					.unwrap_or(u64::MAX),
				)
			})
	}
}

// The common TPC-H duplicate-key shape has up to six rows after the first row; keeping that
// overflow inline avoids per-key heap churn while preserving a small fixed row-list footprint.
const BUILD_ROW_INLINE_OVERFLOW_CAP: usize = 6;

#[derive(Debug, Clone)]
enum BuildRowOverflow {
	Empty,
	Inline {
		len: u8,
		rows: [u32; BUILD_ROW_INLINE_OVERFLOW_CAP],
	},
	Heap(Vec<u32>),
}

impl BuildRowList {
	#[inline(always)]
	pub(super) fn new(first: u32) -> Self {
		Self {
			first,
			overflow: BuildRowOverflow::Empty,
		}
	}

	#[inline(always)]
	pub(super) fn push(&mut self, row: u32) {
		match &mut self.overflow {
			BuildRowOverflow::Empty => {
				let mut rows = [0; BUILD_ROW_INLINE_OVERFLOW_CAP];
				rows[0] = row;
				self.overflow = BuildRowOverflow::Inline { len: 1, rows };
			}
			BuildRowOverflow::Inline { len, rows }
				if usize::from(*len) < BUILD_ROW_INLINE_OVERFLOW_CAP =>
			{
				rows[usize::from(*len)] = row;
				*len = len.saturating_add(1);
			}
			BuildRowOverflow::Inline { len, rows } => {
				let mut heap = Vec::with_capacity(BUILD_ROW_INLINE_OVERFLOW_CAP.saturating_mul(2));
				heap.extend_from_slice(&rows[..usize::from(*len)]);
				heap.push(row);
				self.overflow = BuildRowOverflow::Heap(heap);
			}
			BuildRowOverflow::Heap(rows) => rows.push(row),
		}
	}

	#[inline(always)]
	pub(super) fn rows(&self) -> BuildRowListRows<'_> {
		BuildRowListRows {
			first: Some(self.first),
			overflow: match &self.overflow {
				BuildRowOverflow::Empty => BuildRowOverflowRows::Empty,
				BuildRowOverflow::Inline { len, rows } => BuildRowOverflowRows::Inline {
					rows,
					len: usize::from(*len),
					index: 0,
				},
				BuildRowOverflow::Heap(rows) => BuildRowOverflowRows::Heap(rows.iter()),
			},
		}
	}

	#[inline(always)]
	pub(super) fn len(&self) -> usize {
		1usize.saturating_add(match &self.overflow {
			BuildRowOverflow::Empty => 0,
			BuildRowOverflow::Inline { len, .. } => usize::from(*len),
			BuildRowOverflow::Heap(rows) => rows.len(),
		})
	}

	#[inline(always)]
	pub(super) fn heap_capacity_rows(&self) -> usize {
		match &self.overflow {
			BuildRowOverflow::Heap(rows) => rows.capacity(),
			BuildRowOverflow::Empty | BuildRowOverflow::Inline { .. } => 0,
		}
	}

	#[cfg(test)]
	pub(super) fn overflow_len(&self) -> usize {
		match &self.overflow {
			BuildRowOverflow::Empty => 0,
			BuildRowOverflow::Inline { len, .. } => usize::from(*len),
			BuildRowOverflow::Heap(rows) => rows.len(),
		}
	}
}

pub(in crate::exec) struct BuildRowListRows<'a> {
	first: Option<u32>,
	overflow: BuildRowOverflowRows<'a>,
}

enum BuildRowOverflowRows<'a> {
	Empty,
	Inline {
		rows: &'a [u32; BUILD_ROW_INLINE_OVERFLOW_CAP],
		len: usize,
		index: usize,
	},
	Heap(std::slice::Iter<'a, u32>),
}

impl Iterator for BuildRowListRows<'_> {
	type Item = u32;

	#[inline(always)]
	fn next(&mut self) -> Option<Self::Item> {
		if let Some(first) = self.first.take() {
			return Some(first);
		}
		match &mut self.overflow {
			BuildRowOverflowRows::Empty => None,
			BuildRowOverflowRows::Inline { rows, len, index } => {
				if *index >= *len {
					None
				} else {
					let row = rows[*index];
					*index = index.saturating_add(1);
					Some(row)
				}
			}
			BuildRowOverflowRows::Heap(rows) => rows.next().copied(),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
	use arrow::datatypes::{DataType, Field, Schema};
	use arrow::record_batch::RecordBatch;

	use crate::exec::join::build_rows::new_join_build_rows;
	use crate::exec::join::{
		InMemoryProbeProfile, JoinBuildRows, JoinKey, JoinKeyPlan, JoinOutputRow,
		build_in_memory_join_rows, estimated_build_hash_bytes, insert_join_build_row,
		join_key_bytes, probe_in_memory_join_rows, probe_in_memory_join_rows_profiled,
	};
	use crate::ir::JoinKind;

	#[test]
	fn adr497_batched_probe_ablation_defaults_on() {
		use super::batched_probe_prefetch_enabled_from_env;
		assert!(batched_probe_prefetch_enabled_from_env(None));
		assert!(batched_probe_prefetch_enabled_from_env(Some("1")));
		assert!(!batched_probe_prefetch_enabled_from_env(Some("0")));
	}

	#[test]
	fn adr497_batched_probe_matches_scalar_across_collisions_nulls_and_misses() {
		use crate::exec::join::build_rows::new_int64_join_build_rows;

		// Small table → few buckets → dense collision chains; keys straddle
		// multiple windows and include duplicates, absent keys, and nulls.
		let mut rows = new_int64_join_build_rows(8);
		for (row, key) in [3i64, 11, 19, 27, 35, 43, 51, 59, 3, 27].iter().enumerate() {
			insert_join_build_row(&mut rows, JoinKey::Int64(*key), row as u32);
		}
		let probes: Vec<Option<i64>> = (0..(super::PROBE_BATCH_WINDOW as i64 * 2 + 7))
			.map(|i| match i % 5 {
				0 => None,
				1 => Some(i % 64),
				_ => Some((i * 8) % 64 + 3),
			})
			.collect();
		for window in probes.chunks(super::PROBE_BATCH_WINDOW) {
			let mut batched = Vec::new();
			rows.get_int64_batch(window, &mut batched);
			assert_eq!(batched.len(), window.len());
			for (key, lookup) in window.iter().zip(batched) {
				let scalar = key.and_then(|key| rows.get_int64(key));
				assert_eq!(
					scalar.map(|list| list.rows().collect::<Vec<_>>()),
					lookup.map(|list| list.rows().collect::<Vec<_>>()),
					"batched lookup diverged from scalar for key {key:?}"
				);
			}
		}
	}

	#[test]
	fn adr516_primitive_probe_profile_preserves_selection_and_counts_windows() {
		let build = int64_batch(vec![Some(7), Some(7), Some(9), Some(11)]);
		let probe_keys: Vec<Option<i64>> = (0..(super::PROBE_BATCH_WINDOW + 5))
			.map(|row| match row % 4 {
				0 => None,
				1 => Some(7),
				2 => Some(9),
				_ => Some(404),
			})
			.collect();
		let probe = int64_batch(probe_keys.clone());
		let rows = build_in_memory_join_rows(
			JoinKeyPlan::Int64,
			&key_arrays(&build),
			build.num_rows(),
			u64::MAX,
			false,
		)
		.expect("build")
		.expect("in-memory build");
		let expected = probe_in_memory_join_rows(
			JoinKind::Inner,
			JoinKeyPlan::Int64,
			&rows,
			build.num_rows(),
			&key_arrays(&probe),
			probe.num_rows(),
			None,
			&build,
			&probe,
			false,
		)
		.expect("unprofiled probe");
		let mut profile = InMemoryProbeProfile::default();
		let observed = probe_in_memory_join_rows_profiled(
			JoinKind::Inner,
			JoinKeyPlan::Int64,
			&rows,
			build.num_rows(),
			&key_arrays(&probe),
			probe.num_rows(),
			None,
			&build,
			&probe,
			false,
			Some(&mut profile),
		)
		.expect("profiled probe");

		assert_eq!(observed, expected, "profiling must not alter the selection");
		assert!(profile.supported);
		assert_eq!(profile.lookup_windows, 2, "one full window plus a tail");
		assert_eq!(
			profile.non_null_keys,
			probe_keys.iter().filter(|key| key.is_some()).count() as u64
		);
	}

	#[test]
	fn adr516_int32_probe_profile_preserves_selection_and_exact_counts() {
		let build = int32_batch(vec![Some(7), Some(7), Some(9)]);
		let probe = int32_batch(vec![Some(7), None, Some(9), Some(404)]);
		let rows = build_in_memory_join_rows(
			JoinKeyPlan::Int32,
			&key_arrays(&build),
			build.num_rows(),
			u64::MAX,
			false,
		)
		.expect("build")
		.expect("in-memory build");
		let expected = probe_in_memory_join_rows(
			JoinKind::Inner,
			JoinKeyPlan::Int32,
			&rows,
			build.num_rows(),
			&key_arrays(&probe),
			probe.num_rows(),
			None,
			&build,
			&probe,
			false,
		)
		.expect("unprofiled probe");
		let mut profile = InMemoryProbeProfile::default();
		let observed = probe_in_memory_join_rows_profiled(
			JoinKind::Inner,
			JoinKeyPlan::Int32,
			&rows,
			build.num_rows(),
			&key_arrays(&probe),
			probe.num_rows(),
			None,
			&build,
			&probe,
			false,
			Some(&mut profile),
		)
		.expect("profiled probe");

		assert_eq!(observed, expected, "profiling must not alter the selection");
		assert!(profile.supported);
		assert_eq!(profile.lookup_windows, 1);
		assert_eq!(profile.non_null_keys, 3);
	}

	#[test]
	fn adr524_generic_probe_times_windows_without_changing_selection() {
		let build = RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, true)])),
			vec![Arc::new(StringArray::from(vec![
				Some("a"),
				Some("a"),
				Some("b"),
			]))],
		)
		.expect("generic build");
		let probe = RecordBatch::try_new(
			build.schema(),
			vec![Arc::new(StringArray::from(vec![
				Some("a"),
				None,
				Some("z"),
			]))],
		)
		.expect("generic probe");
		let rows = build_in_memory_join_rows(
			JoinKeyPlan::Generic,
			&key_arrays(&build),
			build.num_rows(),
			u64::MAX,
			false,
		)
		.expect("build")
		.expect("in-memory build");
		let mut profile = InMemoryProbeProfile::default();
		let observed = probe_in_memory_join_rows_profiled(
			JoinKind::Inner,
			JoinKeyPlan::Generic,
			&rows,
			build.num_rows(),
			&key_arrays(&probe),
			probe.num_rows(),
			None,
			&build,
			&probe,
			false,
			Some(&mut profile),
		)
		.expect("profiled generic probe");
		let expected = probe_in_memory_join_rows(
			JoinKind::Inner,
			JoinKeyPlan::Generic,
			&rows,
			build.num_rows(),
			&key_arrays(&probe),
			probe.num_rows(),
			None,
			&build,
			&probe,
			false,
		)
		.expect("unprofiled generic probe");

		assert_eq!(observed, expected);
		assert!(profile.supported);
		assert_eq!(profile.lookup_windows, 1);
		assert_eq!(profile.non_null_keys, 2);
	}

	#[test]
	fn adr503_batched_insert_ablation_defaults_on() {
		use super::batched_build_prefetch_enabled_from_env;
		assert!(batched_build_prefetch_enabled_from_env(None));
		assert!(batched_build_prefetch_enabled_from_env(Some("1")));
		assert!(!batched_build_prefetch_enabled_from_env(Some("0")));
	}

	#[test]
	fn adr503_batched_insert_matches_scalar_across_collisions_and_duplicates() {
		use crate::exec::join::build_rows::new_int64_join_build_rows;

		// 40 distinct keys sized into 128 buckets (capacity 64) — the open-addressing table
		// is pre-sized with no resize, so it must hold every distinct key with headroom.
		// 40/128 ≈ 31% occupancy (near production's 37.5%) gives real hash collisions; keys
		// span multiple windows with duplicates and within-window repeats, so the batched
		// insert must re-walk the live table and preserve insertion order in every row list.
		let n = super::PROBE_BATCH_WINDOW * 2 + 7;
		let keys: Vec<i64> = (0..n as i64).map(|i| (i * 13) % 40).collect();
		let row_ids: Vec<u32> = (0..n as u32).collect();

		let mut scalar = new_int64_join_build_rows(64);
		for (key, row) in keys.iter().zip(&row_ids) {
			insert_join_build_row(&mut scalar, JoinKey::Int64(*key), *row);
		}

		let mut batched = new_int64_join_build_rows(64);
		for (kw, rw) in keys
			.chunks(super::PROBE_BATCH_WINDOW)
			.zip(row_ids.chunks(super::PROBE_BATCH_WINDOW))
		{
			batched.insert_int64_batch(kw, rw);
		}

		let mut distinct = keys.clone();
		distinct.sort_unstable();
		distinct.dedup();
		for key in distinct {
			let s = scalar.get_int64(key).map(|l| l.rows().collect::<Vec<_>>());
			let b = batched.get_int64(key).map(|l| l.rows().collect::<Vec<_>>());
			assert_eq!(s, b, "batched insert diverged from scalar for key {key}");
		}
		assert!(scalar.get_int64(9_999).is_none());
		assert!(batched.get_int64(9_999).is_none());
	}

	/// ADR 503's driver — the windowing, null-skip and bail logic in `build_primitive_key_rows` —
	/// as opposed to `insert_int64_batch`, which the ablation test above drives with hand-chunked
	/// keys and so never enters the driver at all. Every case here crosses a window boundary; the
	/// pre-existing `build_in_memory_join_rows` tests all pass fewer than `PROBE_BATCH_WINDOW` rows,
	/// so the full-window flush had no coverage.
	mod adr503_driver {
		use super::*;
		use crate::exec::join::build_rows::{
			BuildInsertMode, PROBE_BATCH_WINDOW, build_in_memory_join_rows_with_insert_mode,
		};
		use crate::exec::join::{estimated_build_hash_bytes, join_build_memory_fits};

		/// Keys spanning two full windows plus a partial tail, with nulls at an interval coprime to
		/// the window so null-skipping desynchronizes window packing from the row index — the case
		/// where a driver that staged by row rather than by admitted key would drift.
		fn keys_across_windows() -> Vec<Option<i64>> {
			(0..(PROBE_BATCH_WINDOW * 2 + 7))
				.map(|row| {
					if row % 7 == 3 {
						None
					} else {
						Some((row as i64 * 13) % 40)
					}
				})
				.collect()
		}

		fn int64_key_arrays(keys: &[Option<i64>]) -> Vec<ArrayRef> {
			vec![Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef]
		}

		fn table_by_key(rows: &JoinBuildRows, keys: &[Option<i64>]) -> Vec<(i64, Vec<u32>)> {
			let mut distinct: Vec<i64> = keys.iter().flatten().copied().collect();
			distinct.sort_unstable();
			distinct.dedup();
			distinct
				.into_iter()
				.map(|key| {
					let list = rows
						.get(&JoinKey::Int64(key))
						.unwrap_or_else(|| panic!("key {key} absent from build table"));
					(key, list.rows().collect::<Vec<_>>())
				})
				.collect()
		}

		#[test]
		fn windowed_and_scalar_agree_across_boundaries_nulls_and_tail() {
			let keys = keys_across_windows();
			let key_arrays = int64_key_arrays(&keys);
			let rows = keys.len();
			assert!(
				rows > PROBE_BATCH_WINDOW * 2,
				"the fixture must cross more than one full window flush"
			);
			assert!(
				rows % PROBE_BATCH_WINDOW != 0,
				"the fixture must leave a partial tail to flush"
			);

			let windowed = build_in_memory_join_rows_with_insert_mode(
				JoinKeyPlan::Int64,
				&key_arrays,
				rows,
				u64::MAX,
				false,
				BuildInsertMode::Windowed,
			)
			.expect("windowed build")
			.expect("windowed build fits in memory");
			let scalar = build_in_memory_join_rows_with_insert_mode(
				JoinKeyPlan::Int64,
				&key_arrays,
				rows,
				u64::MAX,
				false,
				BuildInsertMode::Scalar,
			)
			.expect("scalar build")
			.expect("scalar build fits in memory");

			assert_eq!(
				table_by_key(&windowed, &keys),
				table_by_key(&scalar, &keys),
				"windowed insert diverged from the one-key-at-a-time path"
			);
			assert_eq!(windowed.len(), scalar.len());

			// The row ids are the *source* row indices, so a null-skip that shifted packing would
			// show up here as an off-by-one even though both arms agreed with each other.
			let expected_rows_for_first_key: Vec<u32> = keys
				.iter()
				.enumerate()
				.filter(|(_, key)| **key == keys.iter().flatten().next().copied())
				.map(|(row, _)| row as u32)
				.collect();
			let first_key = keys.iter().flatten().next().copied().expect("a live key");
			assert_eq!(
				windowed
					.get(&JoinKey::Int64(first_key))
					.expect("first key present")
					.rows()
					.collect::<Vec<_>>(),
				expected_rows_for_first_key,
				"row ids must index source rows, skipping nulls rather than renumbering"
			);
		}

		#[test]
		fn nulls_are_skipped_not_inserted() {
			let keys = keys_across_windows();
			let key_arrays = int64_key_arrays(&keys);
			let live = keys.iter().flatten().count();
			assert!(keys.len() > live, "the fixture must contain nulls");

			let rows = build_in_memory_join_rows_with_insert_mode(
				JoinKeyPlan::Int64,
				&key_arrays,
				keys.len(),
				u64::MAX,
				false,
				BuildInsertMode::Windowed,
			)
			.expect("build")
			.expect("fits in memory");

			let inserted: usize = table_by_key(&rows, &keys)
				.iter()
				.map(|(_, row_ids)| row_ids.len())
				.sum();
			assert_eq!(
				inserted, live,
				"exactly the non-null rows are inserted; nulls never reach the table"
			);
		}

		/// The bail is checked before the key is staged, so a cap that admits `admitted` rows must
		/// refuse the next one — landing mid-window, with staged-but-unflushed keys pending.
		#[test]
		fn mid_window_memory_bail_discards_the_partial_table() {
			let keys: Vec<Option<i64>> = (0..(PROBE_BATCH_WINDOW as i64 * 3))
				.map(|row| Some(row % 40))
				.collect();
			let key_arrays = int64_key_arrays(&keys);
			let admitted = PROBE_BATCH_WINDOW + PROBE_BATCH_WINDOW / 2;
			let cap = estimated_build_hash_bytes(&key_arrays, admitted);

			// Pin the boundary the driver is being held to, so a change in the estimator surfaces
			// here rather than silently turning this into a test that never bails.
			assert!(join_build_memory_fits(&key_arrays, admitted, cap));
			assert!(!join_build_memory_fits(&key_arrays, admitted + 1, cap));
			assert!(
				admitted % PROBE_BATCH_WINDOW != 0,
				"the bail must land mid-window, with keys staged but not yet flushed"
			);

			for mode in [BuildInsertMode::Windowed, BuildInsertMode::Scalar] {
				let bailed = build_in_memory_join_rows_with_insert_mode(
					JoinKeyPlan::Int64,
					&key_arrays,
					keys.len(),
					cap,
					false,
					mode,
				)
				.expect("bail is not an error");
				assert!(
					bailed.is_none(),
					"{mode:?} must hand the caller None, never a partially-built table"
				);
			}
		}

		#[test]
		fn a_cap_that_admits_every_row_builds_the_whole_table() {
			let keys: Vec<Option<i64>> = (0..(PROBE_BATCH_WINDOW as i64 * 3))
				.map(|row| Some(row % 40))
				.collect();
			let key_arrays = int64_key_arrays(&keys);
			let cap = estimated_build_hash_bytes(&key_arrays, keys.len());

			let rows = build_in_memory_join_rows_with_insert_mode(
				JoinKeyPlan::Int64,
				&key_arrays,
				keys.len(),
				cap,
				false,
				BuildInsertMode::Windowed,
			)
			.expect("build")
			.expect("a cap sized for every row must not bail");

			let inserted: usize = table_by_key(&rows, &keys)
				.iter()
				.map(|(_, row_ids)| row_ids.len())
				.sum();
			assert_eq!(inserted, keys.len());
		}

		/// Answer-level parity: the two arms must not merely build equal tables, they must produce
		/// the same join output when probed.
		#[test]
		fn probing_both_arms_yields_identical_join_output() {
			let keys = keys_across_windows();
			let left_batch = int64_batch(keys.clone());
			let key_arrays = key_arrays(&left_batch);
			let right_batch = int64_batch(
				(0..(PROBE_BATCH_WINDOW as i64 + 11))
					.map(|row| match row % 6 {
						0 => None,
						1 => Some(row % 97),
						_ => Some((row * 13) % 40),
					})
					.collect(),
			);

			let mut outputs = Vec::new();
			for mode in [BuildInsertMode::Windowed, BuildInsertMode::Scalar] {
				let rows = build_in_memory_join_rows_with_insert_mode(
					JoinKeyPlan::Int64,
					&key_arrays,
					keys.len(),
					u64::MAX,
					false,
					mode,
				)
				.expect("build")
				.expect("fits in memory");
				outputs.push(probe_int64(
					JoinKind::Inner,
					&rows,
					left_batch.num_rows(),
					&left_batch,
					&right_batch,
				));
			}

			assert!(!outputs[0].is_empty(), "the fixture must produce matches");
			assert_eq!(
				outputs[0], outputs[1],
				"windowed and scalar builds answered the same probe differently"
			);
		}
	}

	#[test]
	fn join_build_row_list_stores_unique_row_inline() {
		let mut rows = new_join_build_rows(0);

		insert_join_build_row(&mut rows, JoinKey::Int64(7), 11);

		let stored = rows.get(&JoinKey::Int64(7)).expect("row list");
		assert_eq!(stored.rows().collect::<Vec<_>>(), vec![11]);
		assert_eq!(stored.overflow_len(), 0);
	}

	#[test]
	fn join_build_row_list_allocates_overflow_for_duplicates() {
		let mut rows = new_join_build_rows(0);

		insert_join_build_row(&mut rows, JoinKey::Int64(7), 11);
		insert_join_build_row(&mut rows, JoinKey::Int64(7), 12);
		insert_join_build_row(&mut rows, JoinKey::Int64(7), 13);

		let stored = rows.get(&JoinKey::Int64(7)).expect("row list");
		assert_eq!(stored.rows().collect::<Vec<_>>(), vec![11, 12, 13]);
		assert_eq!(stored.overflow_len(), 2);
	}

	#[test]
	fn join_build_rows_preallocates_for_input_rows() {
		let key_arrays =
			vec![Arc::new(Int64Array::from((0_i64..32).collect::<Vec<_>>())) as ArrayRef];

		let rows = build_in_memory_join_rows(JoinKeyPlan::Int64, &key_arrays, 32, u64::MAX, false)
			.expect("build rows")
			.expect("in memory rows");

		assert!(rows.is_int64_table());
		assert!(rows.capacity() >= 32);
		assert!(rows.values().all(|list| list.overflow_len() == 0));
	}

	#[test]
	fn join_build_rows_skip_int32_nulls_and_preserve_duplicate_order() {
		let key_arrays =
			vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(1), Some(2)])) as ArrayRef];

		let rows = build_in_memory_join_rows(JoinKeyPlan::Int32, &key_arrays, 4, u64::MAX, false)
			.expect("build rows")
			.expect("in memory rows");

		assert!(rows.is_int32_table());
		assert_eq!(
			rows.get(&JoinKey::Int32(1))
				.expect("duplicate key")
				.rows()
				.collect::<Vec<_>>(),
			vec![0, 2]
		);
		assert_eq!(
			rows.get(&JoinKey::Int32(2))
				.expect("unique key")
				.rows()
				.collect::<Vec<_>>(),
			vec![3]
		);
		assert_eq!(rows.len(), 2);
	}

	#[test]
	fn join_build_rows_preserve_generic_key_order() {
		let key_arrays =
			vec![Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])) as ArrayRef];
		let a_key = JoinKey::Generic(
			join_key_bytes(&key_arrays, 0)
				.expect("encode key")
				.expect("non-null key"),
		);
		let b_key = JoinKey::Generic(
			join_key_bytes(&key_arrays, 1)
				.expect("encode key")
				.expect("non-null key"),
		);

		let rows = build_in_memory_join_rows(JoinKeyPlan::Generic, &key_arrays, 3, u64::MAX, false)
			.expect("build rows")
			.expect("in memory rows");

		assert_eq!(
			rows.get(&a_key)
				.expect("duplicate generic key")
				.rows()
				.collect::<Vec<_>>(),
			vec![0, 2]
		);
		assert_eq!(
			rows.get(&b_key)
				.expect("unique generic key")
				.rows()
				.collect::<Vec<_>>(),
			vec![1]
		);
	}

	#[test]
	fn int32_join_build_rows_are_fenced_to_non_null_safe_int32_plan() {
		let key_arrays = vec![Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef];

		let int32_rows =
			build_in_memory_join_rows(JoinKeyPlan::Int32, &key_arrays, 2, u64::MAX, false)
				.expect("build rows")
				.expect("in memory rows");
		assert!(int32_rows.is_int32_table());

		let null_safe_rows =
			build_in_memory_join_rows(JoinKeyPlan::Int32, &key_arrays, 2, u64::MAX, true)
				.expect("build rows")
				.expect("in memory rows");
		assert!(!null_safe_rows.is_int32_table());

		let generic_rows =
			build_in_memory_join_rows(JoinKeyPlan::Generic, &key_arrays, 2, u64::MAX, false)
				.expect("build rows")
				.expect("in memory rows");
		assert!(!generic_rows.is_int32_table());
	}

	#[test]
	fn int32_join_build_rows_preserve_duplicate_probe_order_and_null_routing() {
		let left = int32_batch(vec![Some(1), Some(1), Some(2), None, Some(1)]);
		let right = int32_batch(vec![Some(1), None, Some(2), Some(1)]);
		let left_keys = key_arrays(&left);

		let rows = build_in_memory_join_rows(
			JoinKeyPlan::Int32,
			&left_keys,
			left.num_rows(),
			u64::MAX,
			false,
		)
		.expect("build rows")
		.expect("in memory rows");
		assert!(rows.is_int32_table());

		let selection = probe_int32(JoinKind::Inner, &rows, left.num_rows(), &left, &right);

		assert_eq!(
			selection,
			vec![
				JoinOutputRow::Paired { left: 0, right: 0 },
				JoinOutputRow::Paired { left: 1, right: 0 },
				JoinOutputRow::Paired { left: 4, right: 0 },
				JoinOutputRow::Paired { left: 2, right: 2 },
				JoinOutputRow::Paired { left: 0, right: 3 },
				JoinOutputRow::Paired { left: 1, right: 3 },
				JoinOutputRow::Paired { left: 4, right: 3 },
			]
		);
	}

	#[test]
	fn int32_join_build_rows_match_hash_fallback_for_join_kinds() {
		let left = int32_batch(vec![Some(1), Some(1), Some(2), Some(4), None]);
		let right = int32_batch(vec![Some(1), Some(3), Some(2), Some(1), None]);
		let left_values = left
			.column(0)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("left int32 values");
		let left_keys = key_arrays(&left);

		let primitive = build_in_memory_join_rows(
			JoinKeyPlan::Int32,
			&left_keys,
			left.num_rows(),
			u64::MAX,
			false,
		)
		.expect("build rows")
		.expect("in memory rows");
		assert!(primitive.is_int32_table());

		let hash = hash_int32_rows(left_values);
		assert!(!hash.is_int32_table());

		for kind in [
			JoinKind::Inner,
			JoinKind::LeftOuter,
			JoinKind::RightOuter,
			JoinKind::FullOuter,
			JoinKind::LeftSemi,
			JoinKind::LeftAnti,
			JoinKind::RightSemi,
			JoinKind::RightAnti,
		] {
			assert_eq!(
				probe_int32(kind, &primitive, left.num_rows(), &left, &right),
				probe_int32(kind, &hash, left.num_rows(), &left, &right),
				"{kind:?} selection differs between primitive and hash join rows"
			);
		}
	}

	#[test]
	fn int32_join_build_rows_preserve_memory_guard_decision() {
		let key_arrays =
			vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef];
		let two_row_cap = estimated_build_hash_bytes(&key_arrays, 2);

		let accepted =
			build_in_memory_join_rows(JoinKeyPlan::Int32, &key_arrays, 2, two_row_cap, false)
				.expect("build rows")
				.expect("two rows fit");
		assert!(accepted.is_int32_table());

		assert!(
			build_in_memory_join_rows(JoinKeyPlan::Int32, &key_arrays, 3, two_row_cap, false)
				.expect("build rows")
				.is_none(),
			"Int32 primitive table must keep the existing hash-estimate guard decision"
		);
	}

	#[test]
	fn int32_join_build_rows_keep_spare_bucket_for_absent_probe() {
		let left = int32_batch(vec![Some(1), Some(2), Some(3), Some(4)]);
		let right = int32_batch(vec![Some(9)]);
		let left_keys = key_arrays(&left);

		let rows = build_in_memory_join_rows(
			JoinKeyPlan::Int32,
			&left_keys,
			left.num_rows(),
			u64::MAX,
			false,
		)
		.expect("build rows")
		.expect("in memory rows");

		assert!(rows.int32_bucket_capacity().expect("int32 buckets") > rows.len());
		assert_eq!(
			probe_int32(JoinKind::Inner, &rows, left.num_rows(), &left, &right),
			Vec::<JoinOutputRow>::new()
		);
	}

	#[test]
	fn int64_join_build_rows_are_fenced_to_non_null_safe_int64_plan() {
		let key_arrays = vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as ArrayRef];

		let int64_rows =
			build_in_memory_join_rows(JoinKeyPlan::Int64, &key_arrays, 2, u64::MAX, false)
				.expect("build rows")
				.expect("in memory rows");
		assert!(int64_rows.is_int64_table());

		let null_safe_rows =
			build_in_memory_join_rows(JoinKeyPlan::Int64, &key_arrays, 2, u64::MAX, true)
				.expect("build rows")
				.expect("in memory rows");
		assert!(!null_safe_rows.is_int64_table());

		let generic_rows =
			build_in_memory_join_rows(JoinKeyPlan::Generic, &key_arrays, 2, u64::MAX, false)
				.expect("build rows")
				.expect("in memory rows");
		assert!(!generic_rows.is_int64_table());
	}

	#[test]
	fn int64_join_build_rows_preserve_duplicate_probe_order_and_null_routing() {
		let left = int64_batch(vec![Some(1), Some(1), Some(2), None, Some(1)]);
		let right = int64_batch(vec![Some(1), None, Some(2), Some(1)]);
		let left_keys = key_arrays(&left);

		let rows = build_in_memory_join_rows(
			JoinKeyPlan::Int64,
			&left_keys,
			left.num_rows(),
			u64::MAX,
			false,
		)
		.expect("build rows")
		.expect("in memory rows");
		assert!(rows.is_int64_table());

		let selection = probe_int64(JoinKind::Inner, &rows, left.num_rows(), &left, &right);

		assert_eq!(
			selection,
			vec![
				JoinOutputRow::Paired { left: 0, right: 0 },
				JoinOutputRow::Paired { left: 1, right: 0 },
				JoinOutputRow::Paired { left: 4, right: 0 },
				JoinOutputRow::Paired { left: 2, right: 2 },
				JoinOutputRow::Paired { left: 0, right: 3 },
				JoinOutputRow::Paired { left: 1, right: 3 },
				JoinOutputRow::Paired { left: 4, right: 3 },
			]
		);
	}

	#[test]
	fn int64_join_build_rows_match_hash_fallback_for_join_kinds() {
		let left = int64_batch(vec![Some(1), Some(1), Some(2), Some(4), None]);
		let right = int64_batch(vec![Some(1), Some(3), Some(2), Some(1), None]);
		let left_values = left
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("left int64 values");
		let left_keys = key_arrays(&left);

		let primitive = build_in_memory_join_rows(
			JoinKeyPlan::Int64,
			&left_keys,
			left.num_rows(),
			u64::MAX,
			false,
		)
		.expect("build rows")
		.expect("in memory rows");
		assert!(primitive.is_int64_table());

		let hash = hash_int64_rows(left_values);
		assert!(!hash.is_int64_table());

		for kind in [
			JoinKind::Inner,
			JoinKind::LeftOuter,
			JoinKind::RightOuter,
			JoinKind::FullOuter,
			JoinKind::LeftSemi,
			JoinKind::LeftAnti,
			JoinKind::RightSemi,
			JoinKind::RightAnti,
		] {
			assert_eq!(
				probe_int64(kind, &primitive, left.num_rows(), &left, &right),
				probe_int64(kind, &hash, left.num_rows(), &left, &right),
				"{kind:?} selection differs between primitive and hash join rows"
			);
		}
	}

	#[test]
	fn int64_join_build_rows_preserve_memory_guard_decision() {
		let key_arrays =
			vec![Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef];
		let two_row_cap = estimated_build_hash_bytes(&key_arrays, 2);

		let accepted =
			build_in_memory_join_rows(JoinKeyPlan::Int64, &key_arrays, 2, two_row_cap, false)
				.expect("build rows")
				.expect("two rows fit");
		assert!(accepted.is_int64_table());

		assert!(
			build_in_memory_join_rows(JoinKeyPlan::Int64, &key_arrays, 3, two_row_cap, false)
				.expect("build rows")
				.is_none(),
			"Int64 primitive table must keep the existing hash-estimate guard decision"
		);
	}

	#[test]
	fn int64_join_build_rows_keep_spare_bucket_for_absent_probe() {
		let left = int64_batch(vec![Some(1), Some(2), Some(3), Some(4)]);
		let right = int64_batch(vec![Some(9)]);
		let left_keys = key_arrays(&left);

		let rows = build_in_memory_join_rows(
			JoinKeyPlan::Int64,
			&left_keys,
			left.num_rows(),
			u64::MAX,
			false,
		)
		.expect("build rows")
		.expect("in memory rows");

		assert!(rows.int64_bucket_capacity().expect("int64 buckets") > rows.len());
		assert_eq!(
			probe_int64(JoinKind::Inner, &rows, left.num_rows(), &left, &right),
			Vec::<JoinOutputRow>::new()
		);
	}

	fn int64_batch(values: Vec<Option<i64>>) -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
		RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values)) as ArrayRef])
			.expect("int64 batch")
	}

	fn key_arrays(batch: &RecordBatch) -> Vec<ArrayRef> {
		vec![batch.column(0).clone()]
	}

	fn int32_batch(values: Vec<Option<i32>>) -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
		RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values)) as ArrayRef])
			.expect("int32 batch")
	}

	fn hash_int32_rows(values: &Int32Array) -> JoinBuildRows {
		let mut rows = new_join_build_rows(values.len());
		for row in 0..values.len() {
			if values.is_null(row) {
				continue;
			}
			insert_join_build_row(&mut rows, JoinKey::Int32(values.value(row)), row as u32);
		}
		rows
	}

	fn hash_int64_rows(values: &Int64Array) -> JoinBuildRows {
		let mut rows = new_join_build_rows(values.len());
		for row in 0..values.len() {
			if values.is_null(row) {
				continue;
			}
			insert_join_build_row(&mut rows, JoinKey::Int64(values.value(row)), row as u32);
		}
		rows
	}

	fn probe_int32(
		kind: JoinKind,
		build_rows: &JoinBuildRows,
		left_rows: usize,
		left_batch: &RecordBatch,
		right_batch: &RecordBatch,
	) -> Vec<JoinOutputRow> {
		probe_in_memory_join_rows(
			kind,
			JoinKeyPlan::Int32,
			build_rows,
			left_rows,
			&key_arrays(right_batch),
			right_batch.num_rows(),
			None,
			left_batch,
			right_batch,
			false,
		)
		.expect("probe rows")
	}

	fn probe_int64(
		kind: JoinKind,
		build_rows: &JoinBuildRows,
		left_rows: usize,
		left_batch: &RecordBatch,
		right_batch: &RecordBatch,
	) -> Vec<JoinOutputRow> {
		probe_in_memory_join_rows(
			kind,
			JoinKeyPlan::Int64,
			build_rows,
			left_rows,
			&key_arrays(right_batch),
			right_batch.num_rows(),
			None,
			left_batch,
			right_batch,
			false,
		)
		.expect("probe rows")
	}
}
