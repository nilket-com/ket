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

pub(in crate::exec) struct JoinBuildRows {
	inner: JoinBuildRowsInner,
}

enum JoinBuildRowsInner {
	Hash(HashMap<JoinKey, BuildRowList, RandomState>),
	Int64(Int64BuildTable),
}

struct Int64BuildTable {
	buckets: Vec<usize>,
	entries: Vec<Int64BuildEntry>,
	hasher: RandomState,
}

struct Int64BuildEntry {
	key: i64,
	rows: BuildRowList,
}

const EMPTY_INT64_BUCKET: usize = usize::MAX;

impl JoinBuildRows {
	fn new_hash(capacity: usize) -> Self {
		Self {
			inner: JoinBuildRowsInner::Hash(HashMap::with_capacity_and_hasher(
				capacity,
				RandomState::new(),
			)),
		}
	}

	fn new_int64(capacity: usize) -> Self {
		Self {
			inner: JoinBuildRowsInner::Int64(Int64BuildTable::with_capacity(capacity)),
		}
	}

	pub(in crate::exec) fn insert(&mut self, key: JoinKey, row: u32) {
		match (&mut self.inner, key) {
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
			(JoinBuildRowsInner::Int64(_), key) => {
				panic!("non-Int64 key inserted into Int64 join build table: {key:?}");
			}
		}
	}

	pub(in crate::exec) fn get(&self, key: &JoinKey) -> Option<&BuildRowList> {
		match (&self.inner, key) {
			(JoinBuildRowsInner::Int64(table), JoinKey::Int64(key)) => table.get(*key),
			(JoinBuildRowsInner::Hash(rows), key) => rows.get(key),
			(JoinBuildRowsInner::Int64(_), _) => None,
		}
	}

	pub(in crate::exec) fn get_int64(&self, key: i64) -> Option<&BuildRowList> {
		match &self.inner {
			JoinBuildRowsInner::Int64(table) => table.get(key),
			JoinBuildRowsInner::Hash(rows) => rows.get(&JoinKey::Int64(key)),
		}
	}

	pub(in crate::exec) fn values(&self) -> Box<dyn Iterator<Item = &BuildRowList> + '_> {
		match &self.inner {
			JoinBuildRowsInner::Hash(rows) => Box::new(rows.values()),
			JoinBuildRowsInner::Int64(table) => Box::new(table.values()),
		}
	}

	pub(in crate::exec) fn len(&self) -> usize {
		match &self.inner {
			JoinBuildRowsInner::Hash(rows) => rows.len(),
			JoinBuildRowsInner::Int64(table) => table.len(),
		}
	}

	#[cfg(test)]
	pub(super) fn capacity(&self) -> usize {
		match &self.inner {
			JoinBuildRowsInner::Hash(rows) => rows.capacity(),
			JoinBuildRowsInner::Int64(table) => table.capacity(),
		}
	}

	#[cfg(test)]
	pub(super) fn is_int64_table(&self) -> bool {
		matches!(self.inner, JoinBuildRowsInner::Int64(_))
	}

	#[cfg(test)]
	pub(super) fn int64_bucket_capacity(&self) -> Option<usize> {
		match &self.inner {
			JoinBuildRowsInner::Int64(table) => Some(table.bucket_capacity()),
			JoinBuildRowsInner::Hash(_) => None,
		}
	}
}

impl Int64BuildTable {
	fn with_capacity(capacity: usize) -> Self {
		let bucket_count = capacity.saturating_mul(2).next_power_of_two().max(1);
		Self {
			buckets: vec![EMPTY_INT64_BUCKET; bucket_count],
			entries: Vec::with_capacity(capacity),
			hasher: RandomState::new(),
		}
	}

	fn insert(&mut self, key: i64, row: u32) {
		let bucket = self.find_bucket(key);
		let entry_index = self.buckets[bucket];
		if entry_index == EMPTY_INT64_BUCKET {
			let entry_index = self.entries.len();
			self.entries.push(Int64BuildEntry {
				key,
				rows: BuildRowList::new(row),
			});
			self.buckets[bucket] = entry_index;
		} else {
			self.entries[entry_index].rows.push(row);
		}
	}

	fn get(&self, key: i64) -> Option<&BuildRowList> {
		let bucket = self.find_bucket(key);
		let entry_index = self.buckets[bucket];
		if entry_index == EMPTY_INT64_BUCKET {
			None
		} else {
			Some(&self.entries[entry_index].rows)
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

	fn find_bucket(&self, key: i64) -> usize {
		let mask = self.buckets.len() - 1;
		let mut bucket = self.bucket_for_key(key);
		loop {
			let entry_index = self.buckets[bucket];
			if entry_index == EMPTY_INT64_BUCKET || self.entries[entry_index].key == key {
				return bucket;
			}
			bucket = (bucket + 1) & mask;
		}
	}

	fn bucket_for_key(&self, key: i64) -> usize {
		(self.hasher.hash_one(key) as usize) & (self.buckets.len() - 1)
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
