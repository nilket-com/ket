use std::collections::HashMap;

use ahash::RandomState;

use super::JoinKey;

#[derive(Debug, Clone)]
pub(in crate::exec) struct BuildRowList {
	first: u32,
	overflow: BuildRowOverflow,
}

pub(in crate::exec) fn new_join_build_rows(capacity: usize) -> JoinBuildRows {
	HashMap::with_capacity_and_hasher(capacity, RandomState::new())
}

// Executor-internal join maps are private, per-task structures, but their keys still come from
// table data. Use a per-map keyed fast hasher here rather than a fixed hash function.
// The random hash seed is safe for determinism: join output is emitted in probe-row order,
// never by iterating the build map.
pub(in crate::exec) type JoinBuildRows = HashMap<JoinKey, BuildRowList, RandomState>;

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

	use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};

	use crate::exec::join::{
		JoinKey, JoinKeyPlan, build_in_memory_join_rows, insert_join_build_row, join_key_bytes,
		new_join_build_rows,
	};

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
	fn join_build_hash_map_preallocates_for_input_rows() {
		let key_arrays =
			vec![Arc::new(Int64Array::from((0_i64..32).collect::<Vec<_>>())) as ArrayRef];

		let rows = build_in_memory_join_rows(JoinKeyPlan::Int64, &key_arrays, 32, u64::MAX, false)
			.expect("build rows")
			.expect("in memory rows");

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
}
