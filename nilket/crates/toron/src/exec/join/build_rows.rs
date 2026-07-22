use std::collections::HashMap;

use ahash::RandomState;

use super::JoinKey;

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

	#[cfg(test)]
	pub(super) fn capacity(&self) -> usize {
		match &self.inner {
			JoinBuildRowsInner::Hash(rows) => rows.capacity(),
			JoinBuildRowsInner::Int32(table) => table.capacity(),
			JoinBuildRowsInner::Int64(table) => table.capacity(),
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

	#[cfg(test)]
	fn capacity(&self) -> usize {
		self.entries.capacity()
	}

	#[cfg(test)]
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

	use crate::exec::join::{
		JoinBuildRows, JoinKey, JoinKeyPlan, JoinOutputRow, build_in_memory_join_rows,
		estimated_build_hash_bytes, insert_join_build_row, join_key_bytes, new_join_build_rows,
		probe_in_memory_join_rows,
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
		use crate::exec::join::new_int64_join_build_rows;

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
