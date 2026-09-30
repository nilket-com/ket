use super::{AggState, GroupValue, MAX_QUANTILE_STATES_PER_EXECUTION, reserve_quantile_states};
use crate::distributed::budget::BudgetDimension;
use crate::error::ExecutionError;
use ahash::RandomState;
use arrow::datatypes::{DataType, TimeUnit};
use serde::Serialize;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const EMPTY_BUCKET: u32 = u32::MAX;
const INITIAL_ENTRY_CAPACITY: usize = 1024;
const GROUPED_FOLD_PRODUCER: &str = "generic bound grouped-fold hash table";
const PROFILE_SCHEMA_VERSION: u32 = 2;
const PROBE_HISTOGRAM_EXACT_MAX: usize = 1023;
const PROBE_HISTOGRAM_BIN_COUNT: usize = PROBE_HISTOGRAM_EXACT_MAX + 1;
const PROBE_HISTOGRAM_OVERFLOW_DENOMINATOR: u64 = 1_000;
const PROBE_TAIL_THRESHOLDS: [usize; 4] = [8, 16, 32, 64];
pub(in crate::exec) const GENERIC_GROUPED_FOLD_PROFILE_ENV: &str =
	"TORON_GENERIC_GROUPED_FOLD_PROFILE_DIR";

static NEXT_PROFILE_ID: AtomicU64 = AtomicU64::new(1);

/// ADR 609: how the grouped fold forms a lookup key for a multi-column key set with a UTF-8
/// column. `Owned` builds each row's `GroupValue`s (one `String` per UTF-8 key per row);
/// `Borrowed` hashes and compares the Arrow values in place and owns a key only on insert.
// A `const` token builds with one variant constructed; the other is the measured alternative, so
// each arm's build leaves one variant unconstructed by design.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::exec) enum GroupKeyLookup {
	Owned,
	Borrowed,
}

pub(in crate::exec) const GROUP_KEY_LOOKUP: GroupKeyLookup = GroupKeyLookup::Borrowed;

/// ADR 609: diagnostic, one JSON line per aggregate instance at finalize; absent, nothing counts.
pub(in crate::exec) const GROUP_KEY_LOOKUP_RECEIPT_ENV: &str =
	"TORON_GROUP_KEY_LOOKUP_RECEIPT_PATH";

/// ADR 609: the lookup path a batch's key set takes, classified from its column types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::exec) enum GroupKeyLookupPath {
	SingleUtf8,
	SingleInt64,
	BorrowedMulti,
	OwnedGeneric,
	StringAgg,
}

impl GroupKeyLookupPath {
	/// `string_agg` names a fold with a `STRING_AGG` call: its multi-column UTF-8 lookup keeps
	/// the owned path, labelled apart, so ADR 609 leaves such folds untouched.
	pub(in crate::exec) fn classify(
		group_types: &[DataType],
		borrow_single_utf8: bool,
		form: GroupKeyLookup,
		string_agg: bool,
	) -> Self {
		match group_types {
			[DataType::Utf8] if borrow_single_utf8 => Self::SingleUtf8,
			[DataType::Int64] => Self::SingleInt64,
			types if types.len() >= 2 && types.iter().any(|t| *t == DataType::Utf8) => {
				if string_agg {
					Self::StringAgg
				} else {
					// ADR 610 cleanup: exhaustive, so both forms are named in production code.
					match form {
						GroupKeyLookup::Owned => Self::OwnedGeneric,
						GroupKeyLookup::Borrowed => Self::BorrowedMulti,
					}
				}
			}
			_ => Self::OwnedGeneric,
		}
	}

	fn name(self) -> &'static str {
		match self {
			Self::SingleUtf8 => "single_utf8",
			Self::SingleInt64 => "single_int64",
			Self::BorrowedMulti => "borrowed_multi",
			Self::OwnedGeneric => "owned_generic",
			Self::StringAgg => "string_agg",
		}
	}
}

/// ADR 609: one group-key value read in place from its Arrow array. Mirrors `GroupValue` variant
/// for variant, with the same null test and float canonicalization, so hashing and equality agree
/// with the owned key exactly.
#[derive(Clone, Copy, Debug)]
pub(in crate::exec) enum GroupKeyRef<'a> {
	Null,
	Boolean(bool),
	Int32(i32),
	Int64(i64),
	Float32(u32),
	Float64(u64),
	Date32(i32),
	Timestamp(TimeUnit, i64),
	Utf8(&'a str),
	Decimal128(i128),
}

impl GroupKeyRef<'_> {
	fn to_owned_value(self) -> GroupValue {
		match self {
			Self::Null => GroupValue::Null,
			Self::Boolean(value) => GroupValue::Boolean(value),
			Self::Int32(value) => GroupValue::Int32(value),
			Self::Int64(value) => GroupValue::Int64(value),
			Self::Float32(bits) => GroupValue::Float32(bits),
			Self::Float64(bits) => GroupValue::Float64(bits),
			Self::Date32(value) => GroupValue::Date32(value),
			Self::Timestamp(unit, value) => GroupValue::Timestamp(unit, value),
			Self::Utf8(value) => GroupValue::Utf8(value.to_owned()),
			Self::Decimal128(value) => GroupValue::Decimal128(value),
		}
	}

	fn matches(self, stored: &GroupValue) -> bool {
		match (self, stored) {
			(Self::Null, GroupValue::Null) => true,
			(Self::Boolean(left), GroupValue::Boolean(right)) => left == *right,
			(Self::Int32(left), GroupValue::Int32(right)) => left == *right,
			(Self::Int64(left), GroupValue::Int64(right)) => left == *right,
			(Self::Float32(left), GroupValue::Float32(right)) => left == *right,
			(Self::Float64(left), GroupValue::Float64(right)) => left == *right,
			(Self::Date32(left), GroupValue::Date32(right)) => left == *right,
			(Self::Timestamp(left_unit, left), GroupValue::Timestamp(right_unit, right)) => {
				left_unit == *right_unit && left == *right
			}
			(Self::Utf8(left), GroupValue::Utf8(right)) => left == right.as_str(),
			(Self::Decimal128(left), GroupValue::Decimal128(right)) => left == *right,
			_ => false,
		}
	}

	/// The owned value's derived `Hash`: its `GroupValue` discriminant, then its payload hashed as
	/// the owned payload hashes (`String` delegates to `str`).
	fn hash_into<H: std::hash::Hasher>(self, state: &mut H) {
		use std::hash::Hash;
		use std::mem::discriminant;

		match self {
			Self::Null => discriminant(&GroupValue::Null).hash(state),
			Self::Boolean(value) => {
				discriminant(&GroupValue::Boolean(value)).hash(state);
				value.hash(state);
			}
			Self::Int32(value) => {
				discriminant(&GroupValue::Int32(value)).hash(state);
				value.hash(state);
			}
			Self::Int64(value) => {
				discriminant(&GroupValue::Int64(value)).hash(state);
				value.hash(state);
			}
			Self::Float32(bits) => {
				discriminant(&GroupValue::Float32(bits)).hash(state);
				bits.hash(state);
			}
			Self::Float64(bits) => {
				discriminant(&GroupValue::Float64(bits)).hash(state);
				bits.hash(state);
			}
			Self::Date32(value) => {
				discriminant(&GroupValue::Date32(value)).hash(state);
				value.hash(state);
			}
			Self::Timestamp(unit, value) => {
				discriminant(&GroupValue::Timestamp(unit, value)).hash(state);
				unit.hash(state);
				value.hash(state);
			}
			Self::Utf8(value) => {
				discriminant(&GroupValue::Utf8(String::new())).hash(state);
				value.hash(state);
			}
			Self::Decimal128(value) => {
				discriminant(&GroupValue::Decimal128(value)).hash(state);
				value.hash(state);
			}
		}
	}
}

/// The owned `[GroupValue]` slice's derived `Hash`: its length, then each element.
fn hash_borrowed_key(hasher: &RandomState, key: &[GroupKeyRef<'_>]) -> u64 {
	use std::hash::{BuildHasher, Hash, Hasher};

	let mut state = hasher.build_hasher();
	key.len().hash(&mut state);
	for value in key {
		value.hash_into(&mut state);
	}
	state.finish()
}

/// ADR 609: code-path counters for one aggregate instance, not allocator readings.
#[derive(Debug)]
struct GroupKeyLookupReceipt {
	path: PathBuf,
	function: Option<&'static str>,
	form: Option<GroupKeyLookup>,
	lookup_path: Option<GroupKeyLookupPath>,
	lookup_path_changed: bool,
	key_types: Vec<String>,
	utf8_keys: usize,
	rows_folded: u64,
	lookup_key_values_built: u64,
	lookup_utf8_strings_built: u64,
	misses: u64,
	written: bool,
}

impl GroupKeyLookupReceipt {
	fn new(path: PathBuf) -> Self {
		Self {
			path,
			function: None,
			form: None,
			lookup_path: None,
			lookup_path_changed: false,
			key_types: Vec::new(),
			utf8_keys: 0,
			rows_folded: 0,
			lookup_key_values_built: 0,
			lookup_utf8_strings_built: 0,
			misses: 0,
			written: false,
		}
	}

	fn count_built(&mut self, values: usize, utf8_strings: usize) {
		self.lookup_key_values_built = self
			.lookup_key_values_built
			.saturating_add(u64::try_from(values).unwrap_or(u64::MAX));
		self.lookup_utf8_strings_built = self
			.lookup_utf8_strings_built
			.saturating_add(u64::try_from(utf8_strings).unwrap_or(u64::MAX));
	}

	fn json(&self, groups_final: usize) -> serde_json::Value {
		serde_json::json!({
			"kind": "group_key_lookup_instance",
			"function": self.function,
			"route": serde_json::Value::Null,
			"form": self.form.map(|form| format!("{form:?}")),
			"lookup_path": self.lookup_path.map(GroupKeyLookupPath::name),
			"lookup_path_changed": self.lookup_path_changed,
			"key_types": self.key_types,
			"utf8_keys": self.utf8_keys,
			"rows_folded": self.rows_folded,
			"groups_final": groups_final,
			"misses": self.misses,
			"lookup_key_values_built": self.lookup_key_values_built,
			"lookup_utf8_strings_built": self.lookup_utf8_strings_built,
		})
	}
}

fn group_key_lookup_receipt_path() -> Option<PathBuf> {
	static PATH: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();
	PATH.get_or_init(|| std::env::var_os(GROUP_KEY_LOOKUP_RECEIPT_ENV).map(PathBuf::from))
		.clone()
}

#[derive(Clone, Copy)]
pub(in crate::exec) struct BoundGroupedAggregateEntry<'a> {
	pub(in crate::exec) key: &'a [GroupValue],
	pub(in crate::exec) states: &'a [AggState],
}

pub(in crate::exec) struct BoundGroupedAggregateGroups {
	state: Box<BoundGroupedAggregateGroupState>,
}

struct BoundGroupedAggregateGroupState {
	buckets: Vec<u32>,
	keys: GroupKeyStorage,
	states: Vec<Vec<AggState>>,
	entry_limit: usize,
	initial_entry_capacity: usize,
	hasher: RandomState,
	growth_budget_bytes: Option<u64>,
	current_resident_bytes: u64,
	peak_resident_bytes: u64,
	maximum_projected_growth_peak_bytes: u64,
	maximum_finalize_sort_scratch_bytes: u64,
	quantile_populations_per_group: usize,
	quantile_declared_bytes_per_group: u64,
	quantile_states_admitted: usize,
	quantile_state_limit: usize,
	quantile_execution_admission: Option<Arc<std::sync::atomic::AtomicUsize>>,
	profile: Option<Box<GenericGroupedFoldProfile>>,
	/// Present only while the growth census is armed.
	growth_census: Option<Box<GrowthCensus>>,
	/// ADR 609: present only while the group-key lookup receipt is armed.
	lookup_receipt: Option<Box<GroupKeyLookupReceipt>>,
	/// ADR 610: one group id per row of the current two-phase batch, reused across batches. Its
	/// actual capacity is held in `current_resident_bytes`.
	group_ids: Vec<u32>,
	group_ids_peak_capacity_bytes: u64,
	/// ADR 610: present only while the fold-kernel receipt is armed.
	fold_kernel_receipt: Option<Box<super::fold_kernel::FoldKernelReceipt>>,
}

/// Growth is censused, never sampled: it is rare and large enough that row sampling would miss it
/// entirely, which is precisely the cost an insertion-dominated fold needs explained. Nanoseconds are
/// local to this census — the exact stage contract keeps its microseconds.
#[derive(Debug, Default, Clone, Copy)]
struct GrowthCensus {
	events: u64,
	nanos: u64,
}

impl GrowthCensus {
	fn record(&mut self, elapsed: Duration) {
		self.events = self.events.saturating_add(1);
		self.nanos = self
			.nanos
			.saturating_add(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
	}
}

/// Key ownership selected once from the bound grouping schema.  The compact arm
/// keeps m05's one fixed-width key inline with its neighbours instead of giving
/// every key its own heap `Vec`.
enum GroupKeyStorage {
	Generic(Vec<Vec<GroupValue>>),
	SingleInt64(Vec<[GroupValue; 1]>),
}

impl GroupKeyStorage {
	fn generic() -> Self {
		Self::Generic(Vec::new())
	}

	fn len(&self) -> usize {
		match self {
			Self::Generic(keys) => keys.len(),
			Self::SingleInt64(keys) => keys.len(),
		}
	}

	fn outer_capacity_bytes(&self) -> u64 {
		match self {
			Self::Generic(keys) => capacity_bytes::<Vec<GroupValue>>(keys.capacity()),
			Self::SingleInt64(keys) => capacity_bytes::<[GroupValue; 1]>(keys.capacity()),
		}
	}

	fn outer_bytes_for_capacity(&self, capacity: usize) -> u64 {
		match self {
			Self::Generic(_) => capacity_bytes::<Vec<GroupValue>>(capacity),
			Self::SingleInt64(_) => capacity_bytes::<[GroupValue; 1]>(capacity),
		}
	}

	fn key_at(&self, entry: usize) -> Option<&[GroupValue]> {
		match self {
			Self::Generic(keys) => keys.get(entry).map(Vec::as_slice),
			Self::SingleInt64(keys) => keys.get(entry).map(<[GroupValue; 1]>::as_slice),
		}
	}

	fn matches_key(&self, entry: usize, key: &[GroupValue]) -> bool {
		self.key_at(entry).is_some_and(|candidate| candidate == key)
	}

	fn key_resident_bytes(&self, key: &[GroupValue]) -> Result<u64, ExecutionError> {
		match self {
			Self::Generic(_) => Ok(group_key_resident_bytes(key)),
			Self::SingleInt64(_) => {
				if matches!(key, [GroupValue::Int64(_) | GroupValue::Null]) {
					Ok(0)
				} else {
					Err(table_error(
						"single-Int64 key store received a non-Int64 key",
					))
				}
			}
		}
	}

	fn push(&mut self, key: &[GroupValue]) -> Result<(), ExecutionError> {
		match self {
			Self::Generic(keys) => keys.push(key.to_vec()),
			Self::SingleInt64(keys) => match key {
				[GroupValue::Int64(_) | GroupValue::Null] => keys.push([key[0].clone()]),
				_ => {
					return Err(table_error(
						"single-Int64 key store received a non-Int64 key",
					));
				}
			},
		}
		Ok(())
	}

	fn empty_with_capacity(&self, capacity: usize) -> Result<Self, ExecutionError> {
		match self {
			Self::Generic(_) => {
				let mut keys = Vec::new();
				keys.try_reserve_exact(capacity).map_err(|error| {
					table_error(format!("key-table allocation failed: {error}"))
				})?;
				Ok(Self::Generic(keys))
			}
			Self::SingleInt64(_) => {
				let mut keys = Vec::new();
				keys.try_reserve_exact(capacity).map_err(|error| {
					table_error(format!("single-Int64 key-table allocation failed: {error}"))
				})?;
				Ok(Self::SingleInt64(keys))
			}
		}
	}

	fn drain_into(&mut self, destination: &mut Self) -> Result<(), ExecutionError> {
		match (self, destination) {
			(Self::Generic(source), Self::Generic(destination)) => {
				destination.extend(source.drain(..));
			}
			(Self::SingleInt64(source), Self::SingleInt64(destination)) => {
				destination.extend(source.drain(..));
			}
			_ => {
				return Err(table_error(
					"grouped-fold key storage changed during growth",
				));
			}
		}
		Ok(())
	}

	#[cfg(test)]
	fn inner_resident_bytes(&self) -> u64 {
		match self {
			Self::Generic(keys) => keys
				.iter()
				.map(|key| group_key_resident_bytes(key))
				.fold(0u64, u64::saturating_add),
			Self::SingleInt64(_) => 0,
		}
	}

	fn select_single_int64(&mut self) {
		if matches!(self, Self::Generic(keys) if keys.is_empty()) {
			*self = Self::SingleInt64(Vec::new());
		}
	}
}

impl BoundGroupedAggregateGroups {
	/// Census every growth event for this fold. The row loop itself is untouched: an earlier sampled
	/// estimator that bracketed individual rows was rejected — it measured each row's latency while
	/// the exact stage measures the loop's throughput, and its components summed to 119% and 203% of
	/// the span they claimed to decompose. The lookup/update split is measured from outside instead,
	/// by CPU sampling (`scripts/classify_fold_perf.py`).
	pub(in crate::exec) fn arm_growth_census(&mut self) {
		self.state.growth_census = Some(Box::default());
	}

	/// The growth census, with the exact stage span it sits inside so the share is computable from
	/// the artifact alone.
	pub(in crate::exec) fn growth_census_json(
		&self,
		row_loop_micros: u64,
	) -> Option<serde_json::Value> {
		let census = self.state.growth_census.as_ref()?;
		Some(serde_json::json!({
			"method": "growth_census",
			"units": "nanoseconds",
			"growth_events": census.events,
			"growth_nanos": census.nanos,
			"row_lookup_update_micros": row_loop_micros,
		}))
	}

	pub(in crate::exec) fn new() -> Self {
		let mut groups = Self::from_lookup(|name| std::env::var_os(name), INITIAL_ENTRY_CAPACITY);
		if let Some(path) = group_key_lookup_receipt_path() {
			groups.arm_group_key_lookup_receipt(path);
		}
		if let Some(path) = super::fold_kernel::fold_kernel_receipt_path() {
			groups.arm_fold_kernel_receipt(path);
		}
		groups
	}

	/// ADR 610: record each batch's shape and kernels and write one receipt line at finalize.
	pub(in crate::exec) fn arm_fold_kernel_receipt(&mut self, path: PathBuf) {
		self.state.fold_kernel_receipt =
			Some(Box::new(super::fold_kernel::FoldKernelReceipt::new(path)));
	}

	pub(in crate::exec) fn fold_kernel_receipt_mut(
		&mut self,
	) -> Option<&mut super::fold_kernel::FoldKernelReceipt> {
		self.state.fold_kernel_receipt.as_deref_mut()
	}

	/// ADR 610: empty the group-id buffer and make room for `rows` ids. The request is guarded
	/// before reserving; the actual capacity, which `try_reserve_exact` may exceed, is what the
	/// resident accounting then holds and guards.
	pub(in crate::exec) fn prepare_group_ids(&mut self, rows: usize) -> Result<(), ExecutionError> {
		let state = &mut *self.state;
		state.group_ids.clear();
		let before = state.group_ids.capacity();
		if before >= rows {
			return Ok(());
		}
		let requested = capacity_bytes::<u32>(rows).saturating_sub(capacity_bytes::<u32>(before));
		state.guard(state.current_resident_bytes().saturating_add(requested))?;
		state
			.group_ids
			.try_reserve_exact(rows)
			.map_err(|error| table_error(format!("group-id buffer reservation failed: {error}")))?;
		let after = state.group_ids.capacity();
		let grown = capacity_bytes::<u32>(after).saturating_sub(capacity_bytes::<u32>(before));
		state.current_resident_bytes = state.current_resident_bytes.saturating_add(grown);
		state.observe_peak();
		state.group_ids_peak_capacity_bytes = state
			.group_ids_peak_capacity_bytes
			.max(capacity_bytes::<u32>(after));
		state.guard(state.current_resident_bytes())
	}

	/// ADR 610: append a row's group id; `prepare_group_ids` has reserved the batch.
	pub(in crate::exec) fn push_group_id(&mut self, entry: usize) -> Result<(), ExecutionError> {
		let id = u32::try_from(entry)
			.map_err(|_| table_error("group id exceeds the u32 entry domain"))?;
		if self.state.group_ids.len() == self.state.group_ids.capacity() {
			return Err(table_error("group-id buffer exceeded its reserved batch"));
		}
		self.state.group_ids.push(id);
		Ok(())
	}

	/// ADR 610: the batch's group ids beside the aggregate states they address.
	pub(in crate::exec) fn with_group_ids_and_states<R>(
		&mut self,
		update: impl FnOnce(&[u32], &mut [Vec<AggState>]) -> Result<R, ExecutionError>,
	) -> Result<R, ExecutionError> {
		let state = &mut *self.state;
		update(&state.group_ids, &mut state.states)
	}

	#[cfg(test)]
	pub(in crate::exec) fn group_ids_peak_capacity_bytes(&self) -> u64 {
		self.state.group_ids_peak_capacity_bytes
	}

	/// ADR 609: count this instance's lookup-key constructions and write one receipt line at
	/// finalize.
	pub(in crate::exec) fn arm_group_key_lookup_receipt(&mut self, path: PathBuf) {
		self.state.lookup_receipt = Some(Box::new(GroupKeyLookupReceipt::new(path)));
	}

	pub(in crate::exec) fn group_key_lookup_receipt_armed(&self) -> bool {
		self.state.lookup_receipt.is_some()
	}

	/// ADR 609: one fold batch's function, path, key types, and rows. No-op unless armed.
	pub(in crate::exec) fn note_group_key_lookup_batch(
		&mut self,
		function: &'static str,
		path: GroupKeyLookupPath,
		group_types: &[DataType],
		form: GroupKeyLookup,
		rows: usize,
	) {
		let Some(receipt) = self.state.lookup_receipt.as_mut() else {
			return;
		};
		receipt.function.get_or_insert(function);
		receipt.form.get_or_insert(form);
		match receipt.lookup_path {
			None => receipt.lookup_path = Some(path),
			Some(previous) if previous != path => receipt.lookup_path_changed = true,
			Some(_) => {}
		}
		if receipt.key_types.is_empty() {
			receipt.key_types = group_types.iter().map(|t| format!("{t:?}")).collect();
			receipt.utf8_keys = group_types.iter().filter(|t| **t == DataType::Utf8).count();
		}
		receipt.rows_folded = receipt
			.rows_folded
			.saturating_add(u64::try_from(rows).unwrap_or(u64::MAX));
	}

	/// ADR 609: an owned lookup key the row loop built. No-op unless armed.
	pub(in crate::exec) fn count_owned_lookup_key(&mut self, key: &[GroupValue]) {
		if let Some(receipt) = self.state.lookup_receipt.as_mut() {
			let utf8 = key
				.iter()
				.filter(|value| matches!(value, GroupValue::Utf8(_)))
				.count();
			receipt.count_built(key.len(), utf8);
		}
	}

	fn write_group_key_lookup_receipt(&mut self) -> Result<(), ExecutionError> {
		let groups_final = self.len();
		let Some(receipt) = self.state.lookup_receipt.as_mut() else {
			return Ok(());
		};
		if receipt.written {
			return Ok(());
		}
		let line = receipt.json(groups_final);
		append_group_key_lookup_receipt(&receipt.path, &line)?;
		receipt.written = true;
		Ok(())
	}

	fn from_lookup(
		lookup: impl FnOnce(&str) -> Option<OsString>,
		initial_entry_capacity: usize,
	) -> Self {
		let profile = GenericGroupedFoldProfile::from_lookup(lookup).map(Box::new);
		let state = BoundGroupedAggregateGroupState {
			buckets: Vec::new(),
			keys: GroupKeyStorage::generic(),
			states: Vec::new(),
			entry_limit: 0,
			initial_entry_capacity: initial_entry_capacity.max(1).next_power_of_two(),
			hasher: RandomState::new(),
			growth_budget_bytes: None,
			current_resident_bytes: 0,
			peak_resident_bytes: 0,
			maximum_projected_growth_peak_bytes: 0,
			maximum_finalize_sort_scratch_bytes: 0,
			quantile_populations_per_group: 0,
			quantile_declared_bytes_per_group: 0,
			quantile_states_admitted: 0,
			quantile_state_limit: MAX_QUANTILE_STATES_PER_EXECUTION,
			quantile_execution_admission: None,
			profile,
			growth_census: None,
			lookup_receipt: None,
			group_ids: Vec::new(),
			group_ids_peak_capacity_bytes: 0,
			fold_kernel_receipt: None,
		};
		let mut groups = Self {
			state: Box::new(state),
		};
		groups.state.current_resident_bytes =
			u64::try_from(std::mem::size_of::<BoundGroupedAggregateGroupState>())
				.unwrap_or(u64::MAX);
		groups.state.peak_resident_bytes = groups.state.current_resident_bytes;
		groups
	}

	#[cfg(test)]
	pub(in crate::exec) fn with_test_profile_lookup(
		initial_entry_capacity: usize,
		lookup: impl FnOnce(&str) -> Option<OsString>,
	) -> Self {
		Self::from_lookup(lookup, initial_entry_capacity)
	}

	pub(in crate::exec) fn len(&self) -> usize {
		self.state.keys.len()
	}

	/// ADR 482: m05's bound schema is one Int64 group column. Select its
	/// contiguous key representation before the first insert; every other shape
	/// retains the generic `Vec<Vec<GroupValue>>` representation.
	pub(in crate::exec) fn configure_key_storage(
		&mut self,
		group_types: &[arrow::datatypes::DataType],
	) {
		if matches!(group_types, [arrow::datatypes::DataType::Int64]) {
			self.state.keys.select_single_int64();
		}
	}

	pub(in crate::exec) fn profile_enabled(&self) -> bool {
		self.state.profile.is_some()
	}

	pub(in crate::exec) fn set_growth_budget_bytes(&mut self, budget: Option<u64>) {
		self.state.growth_budget_bytes = budget;
	}

	pub(in crate::exec) fn configure_quantile_admission(
		&mut self,
		populations_per_group: usize,
		declared_bytes_per_group: u64,
		state_limit: usize,
		execution_admission: Arc<std::sync::atomic::AtomicUsize>,
	) {
		self.state.quantile_populations_per_group = populations_per_group;
		self.state.quantile_declared_bytes_per_group = declared_bytes_per_group;
		self.state.quantile_state_limit = state_limit.min(MAX_QUANTILE_STATES_PER_EXECUTION);
		self.state.quantile_execution_admission = Some(execution_admission);
	}

	pub(in crate::exec) fn current_resident_bytes(&self) -> u64 {
		self.state.current_resident_bytes()
	}

	pub(in crate::exec) fn peak_resident_bytes(&self) -> u64 {
		self.state.peak_resident_bytes
	}

	pub(in crate::exec) fn maximum_projected_growth_peak_bytes(&self) -> u64 {
		self.state.maximum_projected_growth_peak_bytes
	}

	pub(in crate::exec) fn maximum_finalize_sort_scratch_bytes(&self) -> u64 {
		self.state.maximum_finalize_sort_scratch_bytes
	}

	pub(in crate::exec) fn record_group_expression_evaluation(
		&mut self,
		rows: usize,
		group_values: usize,
		elapsed: Duration,
	) {
		if let Some(profile) = self.state.profile.as_mut() {
			profile.record_group_expression_evaluation(rows, group_values, elapsed);
		}
	}

	pub(in crate::exec) fn record_fold_batch(
		&mut self,
		rows: usize,
		aggregate_updates: usize,
		elapsed: Duration,
	) {
		if let Some(profile) = self.state.profile.as_mut() {
			profile.record_fold_batch(rows, aggregate_updates, elapsed);
		}
	}

	/// ADR 476: the probe key is borrowed. Every lookup path below already works
	/// on `&[GroupValue]`; only insertion needs an owned key, so a hit costs no
	/// allocation at all (48M rows finding three groups previously allocated 48M
	/// vectors). Resident accounting is unchanged: the guard still charges the
	/// owned key it actually stores.
	pub(in crate::exec) fn entry_index_or_init(
		&mut self,
		key: &[GroupValue],
		state_count: usize,
		init: impl FnOnce() -> Result<Vec<AggState>, ExecutionError>,
	) -> Result<usize, ExecutionError> {
		let lookup = self.state.lookup_entry(key)?;
		if let Some(profile) = self.state.profile.as_mut() {
			profile.record_lookup(lookup.probes, lookup.entry.is_some());
			if lookup.probes > 0 {
				profile.record_probe(
					if lookup.entry.is_some() {
						ProbePhase::PrimaryOccupied
					} else {
						ProbePhase::PrimaryVacant
					},
					&lookup,
					self.state.buckets.len(),
					self.state.entry_limit,
				);
			}
		}
		if let Some(entry) = lookup.entry {
			return Ok(entry);
		}
		let requested_quantile_states = self
			.state
			.quantile_states_admitted
			.saturating_add(self.state.quantile_populations_per_group);

		if self.state.keys.len() == self.state.entry_limit {
			self.state.grow()?;
		}
		let vacant = self.state.find_bucket(key)?;
		if vacant.entry.is_some() {
			return Err(table_error("vacant grouped-fold lookup became occupied"));
		}
		if let Some(profile) = self.state.profile.as_mut() {
			profile.record_probe(
				ProbePhase::VacantPlacement,
				&vacant,
				self.state.buckets.len(),
				self.state.entry_limit,
			);
		}
		// Own the key only now, on the insert path. The generic arm retains
		// ADR 476's accounting; the compact arm stores its one fixed-width key
		// inside the already-accounted outer key buffer.
		let key_bytes = self.state.keys.key_resident_bytes(key)?;
		let requested_state_bytes = capacity_bytes::<AggState>(state_count);
		self.state.guard(
			self.state
				.current_resident_bytes()
				.saturating_add(key_bytes)
				.saturating_add(requested_state_bytes)
				.saturating_add(self.state.quantile_declared_bytes_per_group),
		)?;
		let reservation = match (
			self.state.quantile_execution_admission.clone(),
			self.state.quantile_populations_per_group,
		) {
			(Some(admission), count) if count != 0 => Some(reserve_quantile_states(
				admission,
				self.state.quantile_state_limit,
				count,
			)?),
			_ => None,
		};
		let states = init()?;
		let actual_state_bytes = capacity_bytes::<AggState>(states.capacity());
		self.state.guard(
			self.state
				.current_resident_bytes()
				.saturating_add(key_bytes)
				.saturating_add(actual_state_bytes)
				.saturating_add(self.state.quantile_declared_bytes_per_group),
		)?;
		let entry = u32::try_from(self.state.keys.len()).map_err(|_| {
			table_error("generic grouped-fold hash table exceeds the u32 entry domain")
		})?;
		if entry == EMPTY_BUCKET {
			return Err(table_error(
				"generic grouped-fold hash table exhausted the u32 entry domain",
			));
		}
		let state_capacity = self.state.states.capacity();
		self.state.keys.push(key)?;
		self.state.states.push(states);
		if let Some(receipt) = self.state.lookup_receipt.as_mut() {
			receipt.misses = receipt.misses.saturating_add(1);
		}
		debug_assert_eq!(self.state.states.capacity(), state_capacity);
		self.state.buckets[vacant.bucket] = entry;
		self.state.current_resident_bytes = self
			.state
			.current_resident_bytes
			.saturating_add(key_bytes)
			.saturating_add(actual_state_bytes)
			.saturating_add(self.state.quantile_declared_bytes_per_group);
		self.state.quantile_states_admitted = requested_quantile_states;
		if let Some(reservation) = reservation {
			reservation.commit();
		}
		self.state.observe_peak();
		Ok(self.state.states.len() - 1)
	}

	pub(in crate::exec) fn entry_or_init(
		&mut self,
		key: &[GroupValue],
		state_count: usize,
		init: impl FnOnce() -> Result<Vec<AggState>, ExecutionError>,
	) -> Result<&mut Vec<AggState>, ExecutionError> {
		let entry = self.entry_index_or_init(key, state_count, init)?;
		self.states_at_mut(entry)
	}

	/// ADR 610: the aggregate states of the entry an index-returning lookup produced.
	pub(in crate::exec) fn states_at_mut(
		&mut self,
		entry: usize,
	) -> Result<&mut Vec<AggState>, ExecutionError> {
		self.state
			.states
			.get_mut(entry)
			.ok_or_else(|| table_error("hash bucket points past aggregate states"))
	}

	/// Probe a single UTF-8 group key without allocating an owned `String` on
	/// every row. The cold insertion path delegates to `entry_or_init`, which
	/// retains the established ownership and resident-accounting contract.
	pub(in crate::exec) fn entry_index_or_init_single_utf8(
		&mut self,
		value: Option<&str>,
		state_count: usize,
		init: impl FnOnce() -> Result<Vec<AggState>, ExecutionError>,
	) -> Result<usize, ExecutionError> {
		let lookup = self.state.lookup_single_utf8(value)?;
		if let Some(entry) = lookup.entry {
			if let Some(profile) = self.state.profile.as_mut() {
				profile.record_lookup(lookup.probes, true);
				profile.record_probe(
					ProbePhase::PrimaryOccupied,
					&lookup,
					self.state.buckets.len(),
					self.state.entry_limit,
				);
			}
			return Ok(entry);
		}

		let owned = [match value {
			Some(value) => GroupValue::Utf8(value.to_owned()),
			None => GroupValue::Null,
		}];
		if let Some(receipt) = self.state.lookup_receipt.as_mut() {
			receipt.count_built(1, usize::from(value.is_some()));
		}
		self.entry_index_or_init(&owned, state_count, init)
	}

	pub(in crate::exec) fn entry_or_init_single_utf8(
		&mut self,
		value: Option<&str>,
		state_count: usize,
		init: impl FnOnce() -> Result<Vec<AggState>, ExecutionError>,
	) -> Result<&mut Vec<AggState>, ExecutionError> {
		let entry = self.entry_index_or_init_single_utf8(value, state_count, init)?;
		self.states_at_mut(entry)
	}

	/// ADR 609: probe a multi-column key read in place. A hit records one lookup, as the owned
	/// path does; a miss records nothing here, owns the key once, and falls through to
	/// `entry_or_init`, whose own lookup is the one recorded, so the profile counts one logical
	/// lookup per row either way (two physical lookups per miss).
	pub(in crate::exec) fn entry_index_or_init_borrowed(
		&mut self,
		key: &[GroupKeyRef<'_>],
		state_count: usize,
		init: impl FnOnce(&[GroupValue]) -> Result<Vec<AggState>, ExecutionError>,
	) -> Result<usize, ExecutionError> {
		let lookup = self.state.lookup_borrowed(key)?;
		if let Some(entry) = lookup.entry {
			if let Some(profile) = self.state.profile.as_mut() {
				profile.record_lookup(lookup.probes, true);
				profile.record_probe(
					ProbePhase::PrimaryOccupied,
					&lookup,
					self.state.buckets.len(),
					self.state.entry_limit,
				);
			}
			return Ok(entry);
		}

		let owned = key
			.iter()
			.map(|value| value.to_owned_value())
			.collect::<Vec<_>>();
		if let Some(receipt) = self.state.lookup_receipt.as_mut() {
			let utf8 = key
				.iter()
				.filter(|value| matches!(value, GroupKeyRef::Utf8(_)))
				.count();
			receipt.count_built(owned.len(), utf8);
		}
		self.entry_index_or_init(&owned, state_count, || init(&owned))
	}

	pub(in crate::exec) fn entry_or_init_borrowed(
		&mut self,
		key: &[GroupKeyRef<'_>],
		state_count: usize,
		init: impl FnOnce(&[GroupValue]) -> Result<Vec<AggState>, ExecutionError>,
	) -> Result<&mut Vec<AggState>, ExecutionError> {
		let entry = self.entry_index_or_init_borrowed(key, state_count, init)?;
		self.states_at_mut(entry)
	}

	pub(in crate::exec) fn finalize_sorted(
		&mut self,
	) -> Result<Vec<BoundGroupedAggregateEntry<'_>>, ExecutionError> {
		self.write_group_key_lookup_receipt()?;
		let peak = self.state.group_ids_peak_capacity_bytes;
		if let Some(receipt) = self.state.fold_kernel_receipt.as_mut() {
			receipt.write(peak)?;
		}
		let requested_scratch = capacity_bytes::<BoundGroupedAggregateEntry<'_>>(self.len());
		let projected = self
			.current_resident_bytes()
			.saturating_add(requested_scratch);
		self.state.maximum_finalize_sort_scratch_bytes = self
			.state
			.maximum_finalize_sort_scratch_bytes
			.max(requested_scratch);
		self.state.guard(projected)?;

		let started = self.profile_enabled().then(Instant::now);
		let mut entries = Vec::new();
		entries
			.try_reserve_exact(self.len())
			.map_err(|error| table_error(format!("finalize sort allocation failed: {error}")))?;
		let actual_scratch = capacity_bytes::<BoundGroupedAggregateEntry<'_>>(entries.capacity());
		self.state.maximum_finalize_sort_scratch_bytes = self
			.state
			.maximum_finalize_sort_scratch_bytes
			.max(actual_scratch);
		self.state
			.guard(self.current_resident_bytes().saturating_add(actual_scratch))?;
		entries.extend((0..self.len()).map(|entry| {
			BoundGroupedAggregateEntry {
				key: self
					.state
					.keys
					.key_at(entry)
					.expect("group key index is valid"),
				states: self.state.states[entry].as_slice(),
			}
		}));
		entries.sort_unstable_by(|left, right| left.key.cmp(right.key));
		if let (Some(profile), Some(started)) = (self.state.profile.as_mut(), started) {
			profile.record_finalize_sort(entries.len(), actual_scratch, started.elapsed());
		}
		Ok(entries)
	}

	pub(in crate::exec) fn record_finalize_and_persist(
		&mut self,
		elapsed: Duration,
	) -> Result<(), ExecutionError> {
		let current_resident_bytes = self.current_resident_bytes();
		let peak_resident_bytes = self.peak_resident_bytes();
		let maximum_projected_growth_peak_bytes = self.maximum_projected_growth_peak_bytes();
		let maximum_finalize_sort_scratch_bytes = self.maximum_finalize_sort_scratch_bytes();
		let final_occupied_cluster_runs = self
			.profile_enabled()
			.then(|| occupied_cluster_run_histogram(&self.state.buckets));
		if let (Some(profile), Some(final_occupied_cluster_runs)) =
			(self.state.profile.as_mut(), final_occupied_cluster_runs)
		{
			profile.record_finalize_total(elapsed);
			profile.persist(
				self.state.keys.len(),
				current_resident_bytes,
				peak_resident_bytes,
				maximum_projected_growth_peak_bytes,
				maximum_finalize_sort_scratch_bytes,
				final_occupied_cluster_runs,
			)?;
		}
		Ok(())
	}

	#[cfg(test)]
	pub(in crate::exec) fn lookup_for_test(
		&self,
		key: &[GroupValue],
	) -> Result<Option<&[AggState]>, ExecutionError> {
		let lookup = self.state.lookup_entry(key)?;
		Ok(lookup
			.entry
			.and_then(|entry| self.state.states.get(entry))
			.map(Vec::as_slice))
	}

	#[cfg(test)]
	fn recomputed_resident_bytes_for_test(&self) -> u64 {
		let outer = u64::try_from(std::mem::size_of::<BoundGroupedAggregateGroupState>())
			.unwrap_or(u64::MAX)
			.saturating_add(capacity_bytes::<u32>(self.state.buckets.capacity()))
			.saturating_add(self.state.keys.outer_capacity_bytes())
			.saturating_add(capacity_bytes::<Vec<AggState>>(
				self.state.states.capacity(),
			))
			.saturating_add(capacity_bytes::<u32>(self.state.group_ids.capacity()));
		let keys = self.state.keys.inner_resident_bytes();
		let states = self
			.state
			.states
			.iter()
			.map(|states| {
				capacity_bytes::<AggState>(states.capacity())
					.saturating_add(self.state.quantile_declared_bytes_per_group)
			})
			.fold(0u64, u64::saturating_add);
		outer.saturating_add(keys).saturating_add(states)
	}
}

struct HashLookup {
	bucket: usize,
	entry: Option<usize>,
	probes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProbePhase {
	PrimaryOccupied,
	PrimaryVacant,
	VacantPlacement,
	Rehash,
}

impl ProbePhase {
	const ALL: [Self; 4] = [
		Self::PrimaryOccupied,
		Self::PrimaryVacant,
		Self::VacantPlacement,
		Self::Rehash,
	];

	fn index(self) -> usize {
		match self {
			Self::PrimaryOccupied => 0,
			Self::PrimaryVacant => 1,
			Self::VacantPlacement => 2,
			Self::Rehash => 3,
		}
	}

	fn label(self) -> &'static str {
		match self {
			Self::PrimaryOccupied => "primary_occupied",
			Self::PrimaryVacant => "primary_vacant",
			Self::VacantPlacement => "vacant_placement",
			Self::Rehash => "rehash",
		}
	}
}

#[derive(Debug, Serialize)]
struct ProbeHistogram {
	exact_bins: Vec<u64>,
	overflow: u64,
	maximum: u64,
}

impl ProbeHistogram {
	fn new() -> Self {
		Self {
			exact_bins: vec![0; PROBE_HISTOGRAM_BIN_COUNT],
			overflow: 0,
			maximum: 0,
		}
	}

	fn record(&mut self, value: usize, counter_overflowed: &mut bool) {
		let value_u64 = u64::try_from(value).unwrap_or(u64::MAX);
		self.maximum = self.maximum.max(value_u64);
		if let Some(bin) = self.exact_bins.get_mut(value) {
			add_counter(bin, 1, counter_overflowed);
		} else {
			add_counter(&mut self.overflow, 1, counter_overflowed);
		}
	}

	fn total(&self) -> u64 {
		self.exact_bins
			.iter()
			.copied()
			.fold(self.overflow, u64::saturating_add)
	}

	fn overflow_requires_widening(&self) -> bool {
		let total = self.total();
		total > 0
			&& u128::from(self.overflow) * u128::from(PROBE_HISTOGRAM_OVERFLOW_DENOMINATOR)
				>= u128::from(total)
	}
}

#[derive(Debug, Serialize)]
struct ProbePhaseCounters {
	phase: &'static str,
	hash_calls: u64,
	bucket_visits: u64,
	equality_invocations: u64,
	matching_equalities: u64,
	collision_mismatches: u64,
	probe_histogram: ProbeHistogram,
}

impl ProbePhaseCounters {
	fn new(phase: ProbePhase) -> Self {
		Self {
			phase: phase.label(),
			hash_calls: 0,
			bucket_visits: 0,
			equality_invocations: 0,
			matching_equalities: 0,
			collision_mismatches: 0,
			probe_histogram: ProbeHistogram::new(),
		}
	}

	fn record(&mut self, lookup: &HashLookup, counter_overflowed: &mut bool) {
		let probes = u64::try_from(lookup.probes).unwrap_or(u64::MAX);
		let matching = u64::from(lookup.entry.is_some());
		let equalities = if lookup.entry.is_some() {
			probes
		} else {
			probes.saturating_sub(1)
		};
		let collisions = equalities.saturating_sub(matching);
		add_counter(&mut self.hash_calls, 1, counter_overflowed);
		add_counter(&mut self.bucket_visits, probes, counter_overflowed);
		add_counter(
			&mut self.equality_invocations,
			equalities,
			counter_overflowed,
		);
		add_counter(&mut self.matching_equalities, matching, counter_overflowed);
		add_counter(
			&mut self.collision_mismatches,
			collisions,
			counter_overflowed,
		);
		self.probe_histogram
			.record(lookup.probes, counter_overflowed);
	}

	fn overflow_requires_widening(&self) -> bool {
		self.probe_histogram.overflow_requires_widening()
	}
}

fn empty_phase_counters() -> [ProbePhaseCounters; 4] {
	ProbePhase::ALL.map(ProbePhaseCounters::new)
}

#[derive(Debug, Serialize)]
struct GrowthEpochProbeCounters {
	epoch: u64,
	bucket_count: u64,
	entry_limit: u64,
	phases: [ProbePhaseCounters; 4],
}

impl GrowthEpochProbeCounters {
	fn new(epoch: u64, bucket_count: usize, entry_limit: usize) -> Self {
		Self {
			epoch,
			bucket_count: u64::try_from(bucket_count).unwrap_or(u64::MAX),
			entry_limit: u64::try_from(entry_limit).unwrap_or(u64::MAX),
			phases: empty_phase_counters(),
		}
	}

	fn overflow_requires_widening(&self) -> bool {
		self.phases
			.iter()
			.any(ProbePhaseCounters::overflow_requires_widening)
	}
}

impl BoundGroupedAggregateGroupState {
	fn lookup_entry(&self, key: &[GroupValue]) -> Result<HashLookup, ExecutionError> {
		if self.buckets.is_empty() {
			return Ok(HashLookup {
				bucket: 0,
				entry: None,
				probes: 0,
			});
		}
		self.find_bucket(key)
	}

	fn find_bucket(&self, key: &[GroupValue]) -> Result<HashLookup, ExecutionError> {
		find_bucket_in(&self.hasher, &self.buckets, &self.keys, key)
	}

	fn lookup_single_utf8(&self, value: Option<&str>) -> Result<HashLookup, ExecutionError> {
		if self.buckets.is_empty() {
			return Ok(HashLookup {
				bucket: 0,
				entry: None,
				probes: 0,
			});
		}
		find_single_utf8_bucket_in(&self.hasher, &self.buckets, &self.keys, value)
	}

	fn lookup_borrowed(&self, key: &[GroupKeyRef<'_>]) -> Result<HashLookup, ExecutionError> {
		if self.buckets.is_empty() {
			return Ok(HashLookup {
				bucket: 0,
				entry: None,
				probes: 0,
			});
		}
		find_borrowed_bucket_in(&self.hasher, &self.buckets, &self.keys, key)
	}

	/// Census wrapper: every growth event is timed in full, none are sampled.
	fn grow(&mut self) -> Result<(), ExecutionError> {
		if self.growth_census.is_none() {
			return self.grow_table();
		}
		let started = Instant::now();
		let outcome = self.grow_table();
		let elapsed = started.elapsed();
		if let Some(census) = self.growth_census.as_mut() {
			census.record(elapsed);
		}
		outcome
	}

	fn grow_table(&mut self) -> Result<(), ExecutionError> {
		let target = if self.entry_limit == 0 {
			self.initial_entry_capacity
		} else {
			self.entry_limit
				.checked_mul(2)
				.ok_or_else(|| table_error("grouped-fold entry capacity overflow"))?
		};
		if target >= EMPTY_BUCKET as usize {
			return Err(table_error(
				"generic grouped-fold hash table exceeds the u32 entry domain",
			));
		}
		let bucket_count = target
			.checked_mul(2)
			.ok_or_else(|| table_error("grouped-fold bucket capacity overflow"))?;
		let old_outer_buffers = capacity_bytes::<u32>(self.buckets.capacity())
			.saturating_add(self.keys.outer_capacity_bytes())
			.saturating_add(capacity_bytes::<Vec<AggState>>(self.states.capacity()));
		let requested_new_buffers = self
			.keys
			.outer_bytes_for_capacity(target)
			.saturating_add(capacity_bytes::<Vec<AggState>>(target))
			.saturating_add(capacity_bytes::<u32>(bucket_count));
		let projected_peak = self
			.current_resident_bytes()
			.saturating_add(requested_new_buffers);
		self.maximum_projected_growth_peak_bytes =
			self.maximum_projected_growth_peak_bytes.max(projected_peak);
		if let Some(profile) = self.profile.as_mut() {
			profile.record_growth_attempt(projected_peak);
		}
		self.guard(projected_peak)?;

		let mut keys = self.keys.empty_with_capacity(target)?;
		let mut states = Vec::new();
		states
			.try_reserve_exact(target)
			.map_err(|error| table_error(format!("state-table allocation failed: {error}")))?;
		let mut buckets = Vec::new();
		buckets
			.try_reserve_exact(bucket_count)
			.map_err(|error| table_error(format!("bucket-table allocation failed: {error}")))?;
		let actual_new_buffers = keys
			.outer_capacity_bytes()
			.saturating_add(capacity_bytes::<Vec<AggState>>(states.capacity()))
			.saturating_add(capacity_bytes::<u32>(buckets.capacity()));
		let actual_peak = self
			.current_resident_bytes()
			.saturating_add(actual_new_buffers);
		self.maximum_projected_growth_peak_bytes =
			self.maximum_projected_growth_peak_bytes.max(actual_peak);
		self.guard(actual_peak)?;

		buckets.resize(bucket_count, EMPTY_BUCKET);
		for entry in 0..self.keys.len() {
			let key = self.keys.key_at(entry).expect("group key index is valid");
			let lookup = find_bucket_in(&self.hasher, &buckets, &self.keys, key)?;
			if lookup.entry.is_some() {
				return Err(table_error(
					"duplicate key while rebuilding grouped-fold hash table",
				));
			}
			if let Some(profile) = self.profile.as_mut() {
				profile.record_rehash_probe(&lookup, bucket_count, target);
			}
			buckets[lookup.bucket] = u32::try_from(entry)
				.map_err(|_| table_error("grouped-fold entry index exceeds u32"))?;
		}

		self.keys.drain_into(&mut keys)?;
		states.extend(self.states.drain(..));
		self.keys = keys;
		self.states = states;
		self.buckets = buckets;
		self.entry_limit = target;
		self.current_resident_bytes = self
			.current_resident_bytes
			.saturating_sub(old_outer_buffers)
			.saturating_add(actual_new_buffers);
		self.peak_resident_bytes = self.peak_resident_bytes.max(actual_peak);
		self.observe_peak();
		if let Some(profile) = self.profile.as_mut() {
			profile.record_growth_completed();
		}
		Ok(())
	}

	fn current_resident_bytes(&self) -> u64 {
		self.current_resident_bytes
	}

	fn guard(&self, requested: u64) -> Result<(), ExecutionError> {
		let Some(available) = self.growth_budget_bytes else {
			return Ok(());
		};
		if requested > available {
			return Err(super::super::join::budget_exhausted(
				BudgetDimension::Memory,
				GROUPED_FOLD_PRODUCER,
				requested,
				available,
			));
		}
		Ok(())
	}

	fn observe_peak(&mut self) {
		self.peak_resident_bytes = self.peak_resident_bytes.max(self.current_resident_bytes);
	}
}

fn find_bucket_in(
	hasher: &RandomState,
	buckets: &[u32],
	keys: &GroupKeyStorage,
	key: &[GroupValue],
) -> Result<HashLookup, ExecutionError> {
	if buckets.is_empty() || !buckets.len().is_power_of_two() {
		return Err(table_error(
			"grouped-fold hash table has invalid bucket capacity",
		));
	}
	let mut bucket = (hasher.hash_one(key) as usize) & (buckets.len() - 1);
	for probes in 1..=buckets.len() {
		let entry = buckets[bucket];
		if entry == EMPTY_BUCKET {
			return Ok(HashLookup {
				bucket,
				entry: None,
				probes,
			});
		}
		let entry = entry as usize;
		if keys.matches_key(entry, key) {
			return Ok(HashLookup {
				bucket,
				entry: Some(entry),
				probes,
			});
		}
		bucket = (bucket + 1) & (buckets.len() - 1);
	}
	Err(table_error(
		"generic grouped-fold hash table has no free bucket",
	))
}

fn find_single_utf8_bucket_in(
	hasher: &RandomState,
	buckets: &[u32],
	keys: &GroupKeyStorage,
	value: Option<&str>,
) -> Result<HashLookup, ExecutionError> {
	if buckets.is_empty() || !buckets.len().is_power_of_two() {
		return Err(table_error(
			"grouped-fold hash table has invalid bucket capacity",
		));
	}
	let mut bucket = (hash_single_utf8_key(hasher, value) as usize) & (buckets.len() - 1);
	for probes in 1..=buckets.len() {
		let entry = buckets[bucket];
		if entry == EMPTY_BUCKET {
			return Ok(HashLookup {
				bucket,
				entry: None,
				probes,
			});
		}
		let entry = entry as usize;
		let matches = match (keys.key_at(entry), value) {
			(Some([GroupValue::Utf8(stored)]), Some(value)) => stored == value,
			(Some([GroupValue::Null]), None) => true,
			_ => false,
		};
		if matches {
			return Ok(HashLookup {
				bucket,
				entry: Some(entry),
				probes,
			});
		}
		bucket = (bucket + 1) & (buckets.len() - 1);
	}
	Err(table_error(
		"generic grouped-fold hash table has no free bucket",
	))
}

fn find_borrowed_bucket_in(
	hasher: &RandomState,
	buckets: &[u32],
	keys: &GroupKeyStorage,
	key: &[GroupKeyRef<'_>],
) -> Result<HashLookup, ExecutionError> {
	if buckets.is_empty() || !buckets.len().is_power_of_two() {
		return Err(table_error(
			"grouped-fold hash table has invalid bucket capacity",
		));
	}
	let mut bucket = (hash_borrowed_key(hasher, key) as usize) & (buckets.len() - 1);
	for probes in 1..=buckets.len() {
		let entry = buckets[bucket];
		if entry == EMPTY_BUCKET {
			return Ok(HashLookup {
				bucket,
				entry: None,
				probes,
			});
		}
		let entry = entry as usize;
		let matches = keys.key_at(entry).is_some_and(|stored| {
			stored.len() == key.len()
				&& key
					.iter()
					.zip(stored)
					.all(|(value, stored)| value.matches(stored))
		});
		if matches {
			return Ok(HashLookup {
				bucket,
				entry: Some(entry),
				probes,
			});
		}
		bucket = (bucket + 1) & (buckets.len() - 1);
	}
	Err(table_error(
		"borrowed grouped-fold hash table has no free bucket",
	))
}

fn append_group_key_lookup_receipt(
	path: &Path,
	line: &serde_json::Value,
) -> Result<(), ExecutionError> {
	use std::io::Write;

	let mut file = std::fs::OpenOptions::new()
		.create(true)
		.append(true)
		.open(path)
		.map_err(|error| {
			table_error(format!(
				"group-key lookup receipt {} could not be opened: {error}",
				path.display()
			))
		})?;
	writeln!(file, "{line}").map_err(|error| {
		table_error(format!(
			"group-key lookup receipt {} could not be written: {error}",
			path.display()
		))
	})
}

fn hash_single_utf8_key(hasher: &RandomState, value: Option<&str>) -> u64 {
	use std::hash::{BuildHasher, Hash, Hasher};

	let mut state = hasher.build_hasher();
	// Match `[GroupValue; 1]`'s derived hash exactly without constructing its
	// owned UTF-8 payload: slice length, enum discriminant, then string bytes.
	1usize.hash(&mut state);
	match value {
		Some(value) => {
			std::mem::discriminant(&GroupValue::Utf8(String::new())).hash(&mut state);
			value.hash(&mut state);
		}
		None => std::mem::discriminant(&GroupValue::Null).hash(&mut state),
	}
	state.finish()
}

fn occupied_cluster_run_histogram(buckets: &[u32]) -> ProbeHistogram {
	let mut histogram = ProbeHistogram::new();
	let Some(empty) = buckets.iter().position(|entry| *entry == EMPTY_BUCKET) else {
		if !buckets.is_empty() {
			let mut overflowed = false;
			histogram.record(buckets.len(), &mut overflowed);
		}
		return histogram;
	};
	let mut run = 0usize;
	let mut overflowed = false;
	for offset in 1..=buckets.len() {
		let bucket = (empty + offset) & (buckets.len() - 1);
		if buckets[bucket] == EMPTY_BUCKET {
			if run > 0 {
				histogram.record(run, &mut overflowed);
				run = 0;
			}
		} else {
			run = run.saturating_add(1);
		}
	}
	debug_assert_eq!(run, 0, "scan ends at the selected empty bucket");
	histogram
}

fn group_key_resident_bytes(key: &[GroupValue]) -> u64 {
	capacity_bytes::<GroupValue>(key.len()).saturating_add(
		key.iter()
			.map(|value| match value {
				GroupValue::Utf8(value) => u64::try_from(value.capacity()).unwrap_or(u64::MAX),
				_ => 0,
			})
			.fold(0u64, u64::saturating_add),
	)
}

fn capacity_bytes<T>(capacity: usize) -> u64 {
	u64::try_from(capacity.saturating_mul(std::mem::size_of::<T>())).unwrap_or(u64::MAX)
}

fn table_error(message: impl Into<String>) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("generic bound grouped-fold hash table: {}", message.into()),
	}
}

#[derive(Debug)]
struct GenericGroupedFoldProfile {
	directory: PathBuf,
	fold_id: u64,
	batches: u64,
	input_rows: u64,
	group_values: u64,
	key_allocations: u64,
	entry_lookups: u64,
	occupied_lookups: u64,
	vacant_lookups: u64,
	hash_probes: u64,
	maximum_hash_probes: u64,
	aggregate_updates: u64,
	group_expression_micros: u64,
	fold_micros: u64,
	finalize_sort_micros: u64,
	finalize_total_micros: u64,
	finalize_groups: u64,
	growth_attempts: u64,
	growths_completed: u64,
	maximum_projected_growth_peak_bytes: u64,
	finalize_sort_scratch_bytes: u64,
	probe_phases: [ProbePhaseCounters; 4],
	growth_epochs: Vec<GrowthEpochProbeCounters>,
	counter_overflowed: bool,
}

impl GenericGroupedFoldProfile {
	fn from_lookup(lookup: impl FnOnce(&str) -> Option<OsString>) -> Option<Self> {
		let raw = lookup(GENERIC_GROUPED_FOLD_PROFILE_ENV)?;
		if raw.is_empty() {
			return None;
		}
		Some(Self {
			directory: PathBuf::from(raw),
			fold_id: NEXT_PROFILE_ID.fetch_add(1, Ordering::Relaxed),
			batches: 0,
			input_rows: 0,
			group_values: 0,
			key_allocations: 0,
			entry_lookups: 0,
			occupied_lookups: 0,
			vacant_lookups: 0,
			hash_probes: 0,
			maximum_hash_probes: 0,
			aggregate_updates: 0,
			group_expression_micros: 0,
			fold_micros: 0,
			finalize_sort_micros: 0,
			finalize_total_micros: 0,
			finalize_groups: 0,
			growth_attempts: 0,
			growths_completed: 0,
			maximum_projected_growth_peak_bytes: 0,
			finalize_sort_scratch_bytes: 0,
			probe_phases: empty_phase_counters(),
			growth_epochs: Vec::new(),
			counter_overflowed: false,
		})
	}

	fn record_group_expression_evaluation(
		&mut self,
		rows: usize,
		group_values: usize,
		elapsed: Duration,
	) {
		add_counter(&mut self.batches, 1, &mut self.counter_overflowed);
		add_counter(
			&mut self.input_rows,
			u64::try_from(rows).unwrap_or(u64::MAX),
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.group_values,
			u64::try_from(group_values).unwrap_or(u64::MAX),
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.key_allocations,
			u64::try_from(rows).unwrap_or(u64::MAX),
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.group_expression_micros,
			duration_micros(elapsed),
			&mut self.counter_overflowed,
		);
	}

	fn record_lookup(&mut self, probes: usize, occupied: bool) {
		add_counter(&mut self.entry_lookups, 1, &mut self.counter_overflowed);
		let probes = u64::try_from(probes).unwrap_or(u64::MAX);
		add_counter(&mut self.hash_probes, probes, &mut self.counter_overflowed);
		self.maximum_hash_probes = self.maximum_hash_probes.max(probes);
		if occupied {
			add_counter(&mut self.occupied_lookups, 1, &mut self.counter_overflowed);
		} else {
			add_counter(&mut self.vacant_lookups, 1, &mut self.counter_overflowed);
		}
	}

	fn record_probe(
		&mut self,
		phase: ProbePhase,
		lookup: &HashLookup,
		bucket_count: usize,
		entry_limit: usize,
	) {
		let epoch = self.growths_completed;
		self.record_probe_at_epoch(phase, lookup, epoch, bucket_count, entry_limit);
	}

	fn record_rehash_probe(
		&mut self,
		lookup: &HashLookup,
		bucket_count: usize,
		entry_limit: usize,
	) {
		let epoch = self.growths_completed.saturating_add(1);
		self.record_probe_at_epoch(ProbePhase::Rehash, lookup, epoch, bucket_count, entry_limit);
	}

	fn record_probe_at_epoch(
		&mut self,
		phase: ProbePhase,
		lookup: &HashLookup,
		epoch: u64,
		bucket_count: usize,
		entry_limit: usize,
	) {
		self.probe_phases[phase.index()].record(lookup, &mut self.counter_overflowed);
		let needs_epoch = self
			.growth_epochs
			.last()
			.is_none_or(|current| current.epoch != epoch);
		if needs_epoch {
			self.growth_epochs.push(GrowthEpochProbeCounters::new(
				epoch,
				bucket_count,
				entry_limit,
			));
		}
		let current = self
			.growth_epochs
			.last_mut()
			.expect("probe epoch was just initialized");
		debug_assert_eq!(current.epoch, epoch);
		debug_assert_eq!(
			current.bucket_count,
			u64::try_from(bucket_count).unwrap_or(u64::MAX)
		);
		debug_assert_eq!(
			current.entry_limit,
			u64::try_from(entry_limit).unwrap_or(u64::MAX)
		);
		current.phases[phase.index()].record(lookup, &mut self.counter_overflowed);
	}

	fn record_fold_batch(&mut self, rows: usize, aggregate_updates: usize, elapsed: Duration) {
		let updates = rows.saturating_mul(aggregate_updates);
		add_counter(
			&mut self.aggregate_updates,
			u64::try_from(updates).unwrap_or(u64::MAX),
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.fold_micros,
			duration_micros(elapsed),
			&mut self.counter_overflowed,
		);
	}

	fn record_growth_attempt(&mut self, projected_peak: u64) {
		add_counter(&mut self.growth_attempts, 1, &mut self.counter_overflowed);
		self.maximum_projected_growth_peak_bytes =
			self.maximum_projected_growth_peak_bytes.max(projected_peak);
	}

	fn record_growth_completed(&mut self) {
		add_counter(&mut self.growths_completed, 1, &mut self.counter_overflowed);
	}

	fn record_finalize_sort(&mut self, groups: usize, scratch: u64, elapsed: Duration) {
		self.finalize_groups = u64::try_from(groups).unwrap_or(u64::MAX);
		self.finalize_sort_scratch_bytes = scratch;
		add_counter(
			&mut self.finalize_sort_micros,
			duration_micros(elapsed),
			&mut self.counter_overflowed,
		);
	}

	fn record_finalize_total(&mut self, elapsed: Duration) {
		add_counter(
			&mut self.finalize_total_micros,
			duration_micros(elapsed),
			&mut self.counter_overflowed,
		);
	}

	fn persist(
		&self,
		groups: usize,
		current_resident_bytes: u64,
		peak_resident_bytes: u64,
		maximum_projected_growth_peak_bytes: u64,
		maximum_finalize_sort_scratch_bytes: u64,
		final_occupied_cluster_runs: ProbeHistogram,
	) -> Result<(), ExecutionError> {
		let histogram_overflow_requires_widening = self
			.probe_phases
			.iter()
			.any(ProbePhaseCounters::overflow_requires_widening)
			|| self
				.growth_epochs
				.iter()
				.any(GrowthEpochProbeCounters::overflow_requires_widening)
			|| final_occupied_cluster_runs.overflow_requires_widening();
		let snapshot = GenericGroupedFoldSnapshot {
			schema_version: PROFILE_SCHEMA_VERSION,
			fold_id: self.fold_id,
			batches: self.batches,
			input_rows: self.input_rows,
			group_values: self.group_values,
			key_allocations: self.key_allocations,
			entry_lookups: self.entry_lookups,
			occupied_lookups: self.occupied_lookups,
			vacant_lookups: self.vacant_lookups,
			hash_probes: self.hash_probes,
			maximum_hash_probes: self.maximum_hash_probes,
			aggregate_updates: self.aggregate_updates,
			group_expression_micros: self.group_expression_micros,
			fold_micros: self.fold_micros,
			finalize_sort_micros: self.finalize_sort_micros,
			finalize_total_micros: self.finalize_total_micros,
			groups: u64::try_from(groups).unwrap_or(u64::MAX),
			growth_attempts: self.growth_attempts,
			growths_completed: self.growths_completed,
			current_resident_bytes,
			peak_resident_bytes,
			maximum_projected_growth_peak_bytes: maximum_projected_growth_peak_bytes
				.max(self.maximum_projected_growth_peak_bytes),
			finalize_sort_scratch_bytes: maximum_finalize_sort_scratch_bytes
				.max(self.finalize_sort_scratch_bytes),
			probe_ledger: ProbeLedgerSnapshot {
				histogram_exact_max: u64::try_from(PROBE_HISTOGRAM_EXACT_MAX).unwrap_or(u64::MAX),
				histogram_overflow_threshold_denominator: PROBE_HISTOGRAM_OVERFLOW_DENOMINATOR,
				tail_thresholds: PROBE_TAIL_THRESHOLDS,
				phases: &self.probe_phases,
				growth_epochs: &self.growth_epochs,
				final_occupied_cluster_runs,
				histogram_overflow_requires_widening,
			},
			counter_overflowed: self.counter_overflowed,
		};
		write_snapshot(&self.directory, self.fold_id, &snapshot)
	}
}

#[derive(Serialize)]
struct ProbeLedgerSnapshot<'a> {
	histogram_exact_max: u64,
	histogram_overflow_threshold_denominator: u64,
	tail_thresholds: [usize; 4],
	phases: &'a [ProbePhaseCounters; 4],
	growth_epochs: &'a [GrowthEpochProbeCounters],
	final_occupied_cluster_runs: ProbeHistogram,
	histogram_overflow_requires_widening: bool,
}

#[derive(Serialize)]
struct GenericGroupedFoldSnapshot<'a> {
	schema_version: u32,
	fold_id: u64,
	batches: u64,
	input_rows: u64,
	group_values: u64,
	key_allocations: u64,
	entry_lookups: u64,
	occupied_lookups: u64,
	vacant_lookups: u64,
	hash_probes: u64,
	maximum_hash_probes: u64,
	aggregate_updates: u64,
	group_expression_micros: u64,
	fold_micros: u64,
	finalize_sort_micros: u64,
	finalize_total_micros: u64,
	groups: u64,
	growth_attempts: u64,
	growths_completed: u64,
	current_resident_bytes: u64,
	peak_resident_bytes: u64,
	maximum_projected_growth_peak_bytes: u64,
	finalize_sort_scratch_bytes: u64,
	probe_ledger: ProbeLedgerSnapshot<'a>,
	counter_overflowed: bool,
}

fn write_snapshot(
	directory: &Path,
	fold_id: u64,
	snapshot: &GenericGroupedFoldSnapshot<'_>,
) -> Result<(), ExecutionError> {
	std::fs::create_dir_all(directory)
		.map_err(|error| profile_error(format!("create profile directory: {error}")))?;
	let bytes = serde_json::to_vec_pretty(snapshot)
		.map_err(|error| profile_error(format!("serialize snapshot: {error}")))?;
	let final_path = directory.join(format!("fold_{fold_id:04}.json"));
	let temporary = directory.join(format!("fold_{fold_id:04}.tmp"));
	std::fs::write(&temporary, bytes)
		.map_err(|error| profile_error(format!("write temporary snapshot: {error}")))?;
	std::fs::rename(&temporary, final_path)
		.map_err(|error| profile_error(format!("publish snapshot: {error}")))
}

fn profile_error(message: impl Into<String>) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("generic grouped-fold profile: {}", message.into()),
	}
}

fn add_counter(counter: &mut u64, delta: u64, overflowed: &mut bool) {
	match counter.checked_add(delta) {
		Some(total) => *counter = total,
		None => {
			*counter = u64::MAX;
			*overflowed = true;
		}
	}
}

fn duration_micros(duration: Duration) -> u64 {
	u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::atomic::{AtomicU64, Ordering};

	static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(1);

	fn test_directory(label: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"toron_generic_grouped_fold_{label}_{}_{}",
			std::process::id(),
			NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed)
		))
	}

	fn count_state(value: i64) -> Vec<AggState> {
		vec![AggState::Count(value)]
	}

	fn int64_count_entries(groups: &mut BoundGroupedAggregateGroups) -> Vec<(i64, i64)> {
		groups
			.finalize_sorted()
			.expect("finalize groups")
			.iter()
			.map(|entry| match (&entry.key[0], &entry.states[0]) {
				(GroupValue::Int64(key), AggState::Count(count)) => (*key, *count),
				other => panic!("unexpected entry {other:?}"),
			})
			.collect()
	}

	#[test]
	fn single_int64_store_is_contiguous_and_preserves_null_and_sorted_output() {
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		groups.configure_key_storage(&[arrow::datatypes::DataType::Int64]);
		assert!(matches!(groups.state.keys, GroupKeyStorage::SingleInt64(_)));

		for (key, count) in [
			(GroupValue::Int64(3), 30),
			(GroupValue::Null, 5),
			(GroupValue::Int64(1), 10),
			(GroupValue::Int64(3), 31),
		] {
			let states = groups
				.entry_or_init(&[key], 1, || Ok(count_state(count)))
				.expect("fold compact group");
			if let AggState::Count(value) = &mut states[0] {
				*value += 1;
			}
		}

		let entries = groups.finalize_sorted().expect("finalize compact groups");
		let observed = entries
			.iter()
			.map(|entry| match &entry.states[0] {
				AggState::Count(count) => (entry.key[0].clone(), *count),
				other => panic!("unexpected compact state {other:?}"),
			})
			.collect::<Vec<_>>();
		assert_eq!(
			observed,
			vec![
				(GroupValue::Null, 6),
				(GroupValue::Int64(1), 11),
				(GroupValue::Int64(3), 32),
			]
		);
		drop(entries);
		assert_eq!(
			groups.current_resident_bytes(),
			groups.recomputed_resident_bytes_for_test(),
			"the resident guard must charge the compact outer key store"
		);
	}

	#[test]
	fn non_int64_or_multi_column_grouping_keeps_generic_key_store() {
		let mut utf8 = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		utf8.configure_key_storage(&[arrow::datatypes::DataType::Utf8]);
		assert!(matches!(utf8.state.keys, GroupKeyStorage::Generic(_)));

		let mut compound = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		compound.configure_key_storage(&[
			arrow::datatypes::DataType::Int64,
			arrow::datatypes::DataType::Int64,
		]);
		assert!(matches!(compound.state.keys, GroupKeyStorage::Generic(_)));
	}

	#[test]
	fn disabled_profile_constructs_no_state_or_sink() {
		let directory = test_directory("disabled");
		let groups = BoundGroupedAggregateGroups::with_test_profile_lookup(4, |_| None);
		assert!(!groups.profile_enabled());
		assert!(!directory.exists());
	}

	#[test]
	fn enabled_and_disabled_probe_ledgers_preserve_output_and_residency_telemetry() {
		let directory = test_directory("probe_parity");
		let mut disabled = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		let mut enabled = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| {
			Some(directory.clone().into_os_string())
		});
		for (key, count) in [(3, 30), (1, 10), (2, 20), (1, 11), (4, 40)] {
			for groups in [&mut disabled, &mut enabled] {
				let states = groups
					.entry_or_init(&[GroupValue::Int64(key)], 1, || Ok(count_state(count)))
					.expect("fold group");
				if let AggState::Count(value) = &mut states[0] {
					*value += 1;
				}
			}
		}

		assert_eq!(
			int64_count_entries(&mut disabled),
			int64_count_entries(&mut enabled)
		);
		assert_eq!(
			(
				disabled.current_resident_bytes(),
				disabled.peak_resident_bytes(),
				disabled.maximum_projected_growth_peak_bytes(),
				disabled.maximum_finalize_sort_scratch_bytes(),
			),
			(
				enabled.current_resident_bytes(),
				enabled.peak_resident_bytes(),
				enabled.maximum_projected_growth_peak_bytes(),
				enabled.maximum_finalize_sort_scratch_bytes(),
			)
		);
		assert!(!directory.exists(), "receipt is published only at finalize");
	}

	#[test]
	fn adr494_borrowed_single_utf8_probe_matches_owned_hash_and_allocates_only_on_insert() {
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		let owned = [GroupValue::Utf8("R".to_string())];
		assert_eq!(
			groups.state.hasher.hash_one(owned.as_slice()),
			hash_single_utf8_key(&groups.state.hasher, Some("R"))
		);
		assert_eq!(
			groups.state.hasher.hash_one([GroupValue::Null].as_slice()),
			hash_single_utf8_key(&groups.state.hasher, None)
		);

		groups
			.entry_or_init_single_utf8(Some("R"), 1, || Ok(count_state(1)))
			.expect("insert UTF-8 group");
		let resident_after_insert = groups.current_resident_bytes();
		let states = groups
			.entry_or_init_single_utf8(Some("R"), 1, || panic!("hit must not initialize"))
			.expect("probe UTF-8 group");
		assert!(matches!(states.as_slice(), [AggState::Count(1)]));
		assert_eq!(groups.current_resident_bytes(), resident_after_insert);

		groups
			.entry_or_init_single_utf8(None, 1, || Ok(count_state(2)))
			.expect("insert null group");
		let entries = groups.finalize_sorted().expect("sorted UTF-8 groups");
		assert!(matches!(entries[0].key, [GroupValue::Null]));
		assert!(matches!(entries[1].key, [GroupValue::Utf8(value)] if value == "R"));
	}

	#[test]
	fn probe_ledger_reconciles_phase_hash_equality_and_collision_identities() {
		let directory = test_directory("probe_reconciliation");
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| {
			Some(directory.clone().into_os_string())
		});
		groups
			.entry_or_init(&[GroupValue::Int64(1)], 1, || Ok(count_state(10)))
			.expect("first vacant insert");
		groups
			.entry_or_init(&[GroupValue::Int64(1)], 1, || Ok(count_state(11)))
			.expect("occupied lookup");

		let mask = groups.state.buckets.len() - 1;
		let first_bucket = (groups
			.state
			.hasher
			.hash_one([GroupValue::Int64(1)].as_slice()) as usize)
			& mask;
		let collision = (2..10_000)
			.find(|candidate| {
				(groups
					.state
					.hasher
					.hash_one([GroupValue::Int64(*candidate)].as_slice()) as usize)
					& mask == first_bucket
			})
			.expect("find a colliding test key");
		groups
			.entry_or_init(&[GroupValue::Int64(collision)], 1, || Ok(count_state(20)))
			.expect("colliding vacant insert");
		let third = (2..10_000)
			.find(|candidate| *candidate != collision)
			.expect("third test key");
		groups
			.entry_or_init(&[GroupValue::Int64(third)], 1, || Ok(count_state(30)))
			.expect("growth and rehash insert");

		let profile = groups.state.profile.as_ref().expect("enabled profile");
		let occupied = &profile.probe_phases[ProbePhase::PrimaryOccupied.index()];
		assert_eq!(occupied.hash_calls, 1);
		assert_eq!(occupied.matching_equalities, occupied.hash_calls);
		assert_eq!(
			occupied.collision_mismatches,
			occupied
				.equality_invocations
				.saturating_sub(occupied.matching_equalities)
		);

		for phase in [
			ProbePhase::PrimaryVacant,
			ProbePhase::VacantPlacement,
			ProbePhase::Rehash,
		] {
			let counters = &profile.probe_phases[phase.index()];
			assert_eq!(counters.matching_equalities, 0, "{phase:?}");
			assert_eq!(
				counters.collision_mismatches, counters.equality_invocations,
				"{phase:?}"
			);
		}
		for phase in [ProbePhase::VacantPlacement, ProbePhase::Rehash] {
			let counters = &profile.probe_phases[phase.index()];
			assert_eq!(
				counters.equality_invocations,
				counters.bucket_visits - counters.hash_calls,
				"{phase:?}"
			);
		}
		let total_hash_calls = profile
			.probe_phases
			.iter()
			.map(|phase| phase.hash_calls)
			.sum::<u64>();
		assert_eq!(total_hash_calls, 8);
		assert_eq!(
			profile.probe_phases[ProbePhase::VacantPlacement.index()].hash_calls,
			3
		);
		assert_eq!(
			profile.probe_phases[ProbePhase::Rehash.index()].hash_calls,
			2
		);
		assert_eq!(
			profile
				.growth_epochs
				.iter()
				.map(|epoch| epoch.epoch)
				.collect::<Vec<_>>(),
			vec![1, 2]
		);
	}

	#[test]
	fn probe_histogram_overflow_at_one_per_thousand_requires_widening() {
		let mut histogram = ProbeHistogram::new();
		let mut counter_overflowed = false;
		for _ in 0..999 {
			histogram.record(1, &mut counter_overflowed);
		}
		histogram.record(PROBE_HISTOGRAM_BIN_COUNT, &mut counter_overflowed);
		assert!(!counter_overflowed);
		assert_eq!(histogram.total(), 1_000);
		assert_eq!(histogram.overflow, 1);
		assert!(histogram.overflow_requires_widening());

		let mut below_threshold = ProbeHistogram::new();
		for _ in 0..1_000 {
			below_threshold.record(1, &mut counter_overflowed);
		}
		below_threshold.record(PROBE_HISTOGRAM_BIN_COUNT, &mut counter_overflowed);
		assert!(!below_threshold.overflow_requires_widening());
	}

	#[test]
	fn paired_finalize_keeps_states_with_sorted_keys_and_persists_route_receipt() {
		let directory = test_directory("paired");
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(4, |_| {
			Some(directory.clone().into_os_string())
		});
		for (key, count) in [(3, 30), (1, 10), (2, 20)] {
			groups
				.entry_or_init(&[GroupValue::Int64(key)], 1, || Ok(count_state(count)))
				.expect("insert group");
		}
		let entries = groups.finalize_sorted().expect("sorted entries");
		let observed = entries
			.iter()
			.map(|entry| match (&entry.key[0], &entry.states[0]) {
				(GroupValue::Int64(key), AggState::Count(count)) => (*key, *count),
				other => panic!("unexpected entry {other:?}"),
			})
			.collect::<Vec<_>>();
		assert_eq!(observed, vec![(1, 10), (2, 20), (3, 30)]);
		drop(entries);
		groups
			.record_finalize_and_persist(Duration::from_micros(9))
			.expect("persist profile");
		let path = std::fs::read_dir(&directory)
			.expect("profile directory")
			.find_map(|entry| {
				let path = entry.ok()?.path();
				(path.extension().and_then(|value| value.to_str()) == Some("json")).then_some(path)
			})
			.expect("profile receipt");
		let snapshot: serde_json::Value =
			serde_json::from_slice(&std::fs::read(path).expect("read profile"))
				.expect("parse profile");
		assert_eq!(snapshot["schema_version"], 2);
		assert_eq!(snapshot["groups"], 3);
		assert_eq!(snapshot["vacant_lookups"], 3);
		assert_eq!(snapshot["probe_ledger"]["histogram_exact_max"], 1_023);
		assert_eq!(
			snapshot["probe_ledger"]["histogram_overflow_requires_widening"],
			false
		);
		assert!(snapshot["finalize_sort_scratch_bytes"].as_u64().unwrap() > 0);
		std::fs::remove_dir_all(directory).expect("remove profile directory");
	}

	#[test]
	fn enabled_profile_snapshot_failures_are_loud() {
		let sink = test_directory("loud_failure");
		std::fs::write(&sink, b"not a directory").expect("create invalid profile sink");
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(4, |_| {
			Some(sink.clone().into_os_string())
		});
		groups
			.entry_or_init(&[GroupValue::Int64(1)], 1, || Ok(count_state(1)))
			.expect("insert group");
		let entries = groups.finalize_sorted().expect("finalize groups");
		drop(entries);
		let error = groups
			.record_finalize_and_persist(Duration::ZERO)
			.expect_err("profile I/O failures must fail execution");
		assert!(matches!(
			error,
			ExecutionError::Kernel { message }
				if message.contains("generic grouped-fold profile: create profile directory")
		));
		std::fs::remove_file(sink).expect("remove invalid profile sink");
	}

	#[test]
	fn growth_guard_rejects_before_rehash_and_preserves_existing_table() {
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		for key in [1, 2] {
			groups
				.entry_or_init(&[GroupValue::Int64(key)], 1, || Ok(count_state(key)))
				.expect("initial insert");
		}
		let current = groups.current_resident_bytes();
		let new_buffers = capacity_bytes::<Vec<GroupValue>>(4)
			.saturating_add(capacity_bytes::<Vec<AggState>>(4))
			.saturating_add(capacity_bytes::<u32>(8));
		let projected = current.saturating_add(new_buffers);
		groups.set_growth_budget_bytes(Some(projected - 1));
		let mut init_called = false;
		let error = groups
			.entry_or_init(&[GroupValue::Int64(3)], 1, || {
				init_called = true;
				Ok(count_state(3))
			})
			.expect_err("rehash peak must be rejected");
		assert!(!init_called, "guard fires before state allocation");
		assert!(matches!(
			error,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: GROUPED_FOLD_PRODUCER,
				..
			}
		));
		assert_eq!(groups.len(), 2);
		assert_eq!(groups.current_resident_bytes(), current);
		assert_eq!(current, groups.recomputed_resident_bytes_for_test());
		assert!(
			groups
				.lookup_for_test(&[GroupValue::Int64(1)])
				.unwrap()
				.is_some()
		);
		assert!(
			groups
				.lookup_for_test(&[GroupValue::Int64(2)])
				.unwrap()
				.is_some()
		);
		assert!(
			groups
				.lookup_for_test(&[GroupValue::Int64(3)])
				.unwrap()
				.is_none()
		);
		assert_eq!(groups.maximum_projected_growth_peak_bytes(), projected);

		groups.set_growth_budget_bytes(Some(projected));
		groups
			.entry_or_init(&[GroupValue::Int64(3)], 1, || Ok(count_state(3)))
			.expect("exact projected peak admits growth");
		assert_eq!(groups.len(), 3);
		assert_eq!(
			groups.current_resident_bytes(),
			groups.recomputed_resident_bytes_for_test()
		);
	}

	// ADR 609: one owned/borrowed pair per variant and edge the plan names.
	fn adr609_key_pairs() -> Vec<(GroupKeyRef<'static>, GroupValue)> {
		let f32_nan = super::super::canonical_group_f32_bits(f32::NAN);
		let f32_neg_zero = super::super::canonical_group_f32_bits(-0.0);
		let f64_nan = super::super::canonical_group_f64_bits(f64::from_bits(0x7ff8_0000_0000_0001));
		let f64_neg_zero = super::super::canonical_group_f64_bits(-0.0);
		vec![
			(GroupKeyRef::Null, GroupValue::Null),
			(GroupKeyRef::Boolean(false), GroupValue::Boolean(false)),
			(GroupKeyRef::Boolean(true), GroupValue::Boolean(true)),
			(GroupKeyRef::Int32(i32::MIN), GroupValue::Int32(i32::MIN)),
			(GroupKeyRef::Int32(i32::MAX), GroupValue::Int32(i32::MAX)),
			(GroupKeyRef::Int64(i64::MIN), GroupValue::Int64(i64::MIN)),
			(GroupKeyRef::Int64(i64::MAX), GroupValue::Int64(i64::MAX)),
			(GroupKeyRef::Float32(f32_nan), GroupValue::Float32(f32_nan)),
			(
				GroupKeyRef::Float32(f32_neg_zero),
				GroupValue::Float32(f32_neg_zero),
			),
			(GroupKeyRef::Float64(f64_nan), GroupValue::Float64(f64_nan)),
			(
				GroupKeyRef::Float64(f64_neg_zero),
				GroupValue::Float64(f64_neg_zero),
			),
			(GroupKeyRef::Date32(-1), GroupValue::Date32(-1)),
			(
				GroupKeyRef::Timestamp(TimeUnit::Second, 7),
				GroupValue::Timestamp(TimeUnit::Second, 7),
			),
			(
				GroupKeyRef::Timestamp(TimeUnit::Millisecond, 7),
				GroupValue::Timestamp(TimeUnit::Millisecond, 7),
			),
			(
				GroupKeyRef::Timestamp(TimeUnit::Microsecond, 7),
				GroupValue::Timestamp(TimeUnit::Microsecond, 7),
			),
			(
				GroupKeyRef::Timestamp(TimeUnit::Nanosecond, 7),
				GroupValue::Timestamp(TimeUnit::Nanosecond, 7),
			),
			(GroupKeyRef::Utf8(""), GroupValue::Utf8(String::new())),
			(GroupKeyRef::Utf8("R"), GroupValue::Utf8("R".to_string())),
			(
				GroupKeyRef::Utf8("naïve ☃"),
				GroupValue::Utf8("naïve ☃".to_string()),
			),
			(
				GroupKeyRef::Decimal128(i128::MIN),
				GroupValue::Decimal128(i128::MIN),
			),
			(
				GroupKeyRef::Decimal128(i128::MAX),
				GroupValue::Decimal128(i128::MAX),
			),
		]
	}

	#[test]
	fn adr609_borrowed_key_hash_and_equality_match_the_owned_key() {
		let groups = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		let hasher = &groups.state.hasher;
		let pairs = adr609_key_pairs();
		// -0.0 canonicalizes to +0.0, so the owned and borrowed forms must both collapse it.
		assert_eq!(
			super::super::canonical_group_f64_bits(-0.0),
			super::super::canonical_group_f64_bits(0.0)
		);
		for (borrowed, owned) in &pairs {
			assert_eq!(
				hasher.hash_one([owned.clone()].as_slice()),
				hash_borrowed_key(hasher, &[*borrowed]),
				"single-element hash differs for {owned:?}"
			);
			assert!(borrowed.matches(owned), "{owned:?} must equal itself");
			assert_eq!(&borrowed.to_owned_value(), owned);
		}
		// Multi-element slices hash as the owned slice does, length prefix included.
		for window in pairs.windows(3) {
			let borrowed = window.iter().map(|(b, _)| *b).collect::<Vec<_>>();
			let owned = window.iter().map(|(_, o)| o.clone()).collect::<Vec<_>>();
			assert_eq!(
				hasher.hash_one(owned.as_slice()),
				hash_borrowed_key(hasher, &borrowed)
			);
		}
		// Mismatched variants and values are unequal, NULL included.
		for (index, (borrowed, _)) in pairs.iter().enumerate() {
			for (other, (_, owned)) in pairs.iter().enumerate() {
				if index != other {
					assert!(
						!borrowed.matches(owned),
						"{borrowed:?} must not equal {owned:?}"
					);
				}
			}
		}
		assert!(!GroupKeyRef::Utf8("R").matches(&GroupValue::Utf8("r".to_string())));
		assert!(!GroupKeyRef::Int32(1).matches(&GroupValue::Int64(1)));
	}

	#[test]
	fn adr609_reader_value_ref_agrees_with_value() {
		use arrow::array::{
			ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
			Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
			TimestampMillisecondArray, TimestampNanosecondArray,
		};
		let arrays: Vec<ArrayRef> = vec![
			Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
			Arc::new(Int32Array::from(vec![Some(1), None, Some(i32::MIN)])),
			Arc::new(Int64Array::from(vec![Some(1), None, Some(i64::MAX)])),
			Arc::new(Float32Array::from(vec![Some(f32::NAN), None, Some(-0.0)])),
			Arc::new(Float64Array::from(vec![Some(-0.0), None, Some(f64::NAN)])),
			Arc::new(Date32Array::from(vec![Some(1), None, Some(-1)])),
			Arc::new(TimestampMillisecondArray::from(vec![
				Some(1),
				None,
				Some(2),
			])),
			Arc::new(TimestampMicrosecondArray::from(vec![
				Some(1),
				None,
				Some(2),
			])),
			Arc::new(TimestampNanosecondArray::from(vec![Some(1), None, Some(2)])),
			Arc::new(StringArray::from(vec![Some("A"), None, Some("")])),
			Arc::new(
				Decimal128Array::from(vec![Some(12_345), None, Some(-1)])
					.with_precision_and_scale(15, 2)
					.expect("decimal type"),
			),
		];
		let hasher = RandomState::new();
		for array in &arrays {
			let reader = super::super::group_array_reader(array).expect("reader");
			for row in 0..array.len() {
				let owned = reader.value(row);
				let borrowed = reader.value_ref(row);
				assert!(
					borrowed.matches(&owned),
					"{:?} row {row}",
					array.data_type()
				);
				assert_eq!(borrowed.to_owned_value(), owned);
				assert_eq!(
					hasher.hash_one([owned.clone()].as_slice()),
					hash_borrowed_key(&hasher, &[borrowed])
				);
			}
		}
	}

	#[test]
	fn adr609_lookup_path_classification() {
		use GroupKeyLookup::{Borrowed, Owned};
		use GroupKeyLookupPath as P;
		let multi = [DataType::Utf8, DataType::Utf8];
		let mixed = [DataType::Int64, DataType::Utf8, DataType::Date32];
		let numeric = [DataType::Int64, DataType::Int32];
		assert_eq!(P::classify(&multi, true, Borrowed, false), P::BorrowedMulti);
		assert_eq!(P::classify(&mixed, true, Borrowed, false), P::BorrowedMulti);
		assert_eq!(P::classify(&multi, true, Owned, false), P::OwnedGeneric);
		assert_eq!(
			P::classify(&numeric, true, Borrowed, false),
			P::OwnedGeneric
		);
		assert_eq!(
			P::classify(&[DataType::Utf8], true, Borrowed, false),
			P::SingleUtf8
		);
		assert_eq!(
			P::classify(&[DataType::Utf8], false, Borrowed, false),
			P::OwnedGeneric
		);
		assert_eq!(
			P::classify(&[DataType::Int64], true, Borrowed, false),
			P::SingleInt64
		);
		assert_eq!(
			P::classify(&[DataType::Date32], true, Borrowed, false),
			P::OwnedGeneric
		);
		// A STRING_AGG fold keeps the owned lookup under both forms, labelled apart; its
		// single-key paths are unchanged.
		assert_eq!(P::classify(&multi, true, Borrowed, true), P::StringAgg);
		assert_eq!(P::classify(&mixed, true, Owned, true), P::StringAgg);
		assert_eq!(
			P::classify(&[DataType::Utf8], true, Borrowed, true),
			P::SingleUtf8
		);
		assert_eq!(P::classify(&numeric, true, Borrowed, true), P::OwnedGeneric);
	}

	/// A multi-batch, two-UTF-8-key fold folded through one form; returns its sorted
	/// (key, count) output, group-table resident bytes, profile lookups, and receipt line.
	fn adr609_fold(
		form: GroupKeyLookup,
		label: &str,
	) -> (Vec<(Vec<GroupValue>, i64)>, u64, u64, serde_json::Value) {
		let profile_directory = test_directory(label);
		let receipt = test_directory(&format!("{label}_receipt"));
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| {
			Some(profile_directory.clone().into_os_string())
		});
		groups.arm_group_key_lookup_receipt(receipt.clone());
		let batches: [&[(&str, Option<&str>)]; 3] = [
			&[
				("A", Some("F")),
				("N", Some("O")),
				("A", Some("F")),
				("R", None),
			],
			&[
				("N", Some("O")),
				("N", Some("F")),
				("R", None),
				("A", Some("F")),
			],
			&[("R", Some("F")), ("A", Some("F")), ("N", Some("O"))],
		];
		let types = [DataType::Utf8, DataType::Utf8];
		let path = GroupKeyLookupPath::classify(&types, true, form, false);
		for batch in batches {
			groups.note_group_key_lookup_batch("adr609_fixture", path, &types, form, batch.len());
			for &(flag, status) in batch {
				let states = match path {
					GroupKeyLookupPath::BorrowedMulti => {
						let key = [
							GroupKeyRef::Utf8(flag),
							status.map_or(GroupKeyRef::Null, GroupKeyRef::Utf8),
						];
						groups
							.entry_or_init_borrowed(&key, 1, |_| Ok(count_state(0)))
							.expect("borrowed lookup")
					}
					_ => {
						let key = [
							GroupValue::Utf8(flag.to_string()),
							status.map_or(GroupValue::Null, |s| GroupValue::Utf8(s.to_string())),
						];
						groups.count_owned_lookup_key(&key);
						groups
							.entry_or_init(&key, 1, || Ok(count_state(0)))
							.expect("owned lookup")
					}
				};
				if let [AggState::Count(total)] = states.as_mut_slice() {
					*total += 1;
				}
			}
		}
		let resident = groups.current_resident_bytes();
		let lookups = groups
			.state
			.profile
			.as_ref()
			.expect("profile armed")
			.entry_lookups;
		let entries = groups
			.finalize_sorted()
			.expect("finalize")
			.iter()
			.map(|entry| match entry.states {
				[AggState::Count(total)] => (entry.key.to_vec(), *total),
				other => panic!("unexpected states {other:?}"),
			})
			.collect::<Vec<_>>();
		let line = std::fs::read_to_string(&receipt).expect("receipt written at finalize");
		let _ = std::fs::remove_file(&receipt);
		let _ = std::fs::remove_dir_all(&profile_directory);
		let mut lines = line.lines();
		let json = serde_json::from_str(lines.next().expect("one line")).expect("json line");
		assert!(lines.next().is_none(), "one line per instance");
		(entries, resident, lookups, json)
	}

	#[test]
	fn adr609_borrowed_and_owned_folds_agree_and_count_constructions() {
		let (owned, owned_resident, owned_lookups, owned_receipt) =
			adr609_fold(GroupKeyLookup::Owned, "adr609_owned");
		let (borrowed, borrowed_resident, borrowed_lookups, borrowed_receipt) =
			adr609_fold(GroupKeyLookup::Borrowed, "adr609_borrowed");
		assert_eq!(
			owned, borrowed,
			"identical groups, counts, and sorted order"
		);
		assert_eq!(owned.len(), 5);
		// Group-table resident accounting (not process RSS) is identical between forms.
		assert_eq!(owned_resident, borrowed_resident);
		// One logical lookup per row under both forms.
		assert_eq!(owned_lookups, 11);
		assert_eq!(borrowed_lookups, 11);

		assert_eq!(owned_receipt["lookup_path"], "owned_generic");
		assert_eq!(borrowed_receipt["lookup_path"], "borrowed_multi");
		// The receipt names the form that ran, not the build's default token.
		assert_eq!(owned_receipt["form"], "Owned");
		assert_eq!(borrowed_receipt["form"], "Borrowed");
		for receipt in [&owned_receipt, &borrowed_receipt] {
			assert_eq!(receipt["rows_folded"], 11);
			assert_eq!(receipt["groups_final"], 5);
			assert_eq!(receipt["misses"], 5);
			assert_eq!(receipt["utf8_keys"], 2);
			assert_eq!(receipt["lookup_path_changed"], false);
			assert_eq!(receipt["function"], "adr609_fixture");
		}
		// Owned builds every row's key: 11 rows x 2 values, of which 20 are UTF-8 strings (the
		// fixture's two NULL statuses build none: 11 x 2 - 2).
		assert_eq!(owned_receipt["lookup_key_values_built"], 22);
		assert_eq!(owned_receipt["lookup_utf8_strings_built"], 20);
		// Borrowed builds only the five inserted keys; one of them carries a NULL status.
		assert_eq!(borrowed_receipt["lookup_key_values_built"], 10);
		assert_eq!(borrowed_receipt["lookup_utf8_strings_built"], 9);
	}

	#[test]
	fn adr609_borrowed_lookup_survives_growth_and_rehash() {
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		let names = (0..200)
			.map(|index| format!("k{index}"))
			.collect::<Vec<_>>();
		for (index, name) in names.iter().enumerate() {
			let key = [GroupKeyRef::Utf8(name), GroupKeyRef::Int64(index as i64)];
			groups
				.entry_or_init_borrowed(&key, 1, |_| Ok(count_state(1)))
				.expect("insert through growth");
		}
		assert_eq!(groups.len(), 200);
		for (index, name) in names.iter().enumerate() {
			let key = [GroupKeyRef::Utf8(name), GroupKeyRef::Int64(index as i64)];
			groups
				.entry_or_init_borrowed(&key, 1, |_| panic!("a hit after rehash must not insert"))
				.expect("hit after rehash");
			// The owned lookup finds the same entry.
			groups
				.entry_or_init(
					&[
						GroupValue::Utf8(name.clone()),
						GroupValue::Int64(index as i64),
					],
					1,
					|| panic!("owned lookup must hit the borrowed insert"),
				)
				.expect("owned hit");
		}
		assert_eq!(groups.len(), 200);
	}

	#[test]
	fn adr609_receipt_write_error_is_returned() {
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		let missing = test_directory("adr609_missing")
			.join("no_such_directory")
			.join("r.jsonl");
		groups.arm_group_key_lookup_receipt(missing);
		groups
			.entry_or_init_borrowed(&[GroupKeyRef::Utf8("A"), GroupKeyRef::Null], 1, |_| {
				Ok(count_state(1))
			})
			.expect("insert");
		assert!(groups.finalize_sorted().is_err());
	}

	#[test]
	fn adr610_index_lookups_equal_the_mut_forms_through_growth() {
		let profile_a = test_directory("adr610_index_a");
		let profile_b = test_directory("adr610_index_b");
		let receipt_a = test_directory("adr610_index_a_receipt");
		let receipt_b = test_directory("adr610_index_b_receipt");
		let mut by_index = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| {
			Some(profile_a.clone().into_os_string())
		});
		let mut by_mut = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| {
			Some(profile_b.clone().into_os_string())
		});
		by_index.arm_group_key_lookup_receipt(receipt_a.clone());
		by_mut.arm_group_key_lookup_receipt(receipt_b.clone());
		let names = (0..300)
			.map(|i| format!("k{}", i % 120))
			.collect::<Vec<_>>();
		for (row, name) in names.iter().enumerate() {
			let value = (row % 120) as i64;
			// Borrowed multi-key, single UTF-8, and owned generic, each through growth.
			let borrowed = [GroupKeyRef::Utf8(name), GroupKeyRef::Int64(value)];
			let id = by_index
				.entry_index_or_init_borrowed(&borrowed, 1, |_| Ok(count_state(0)))
				.expect("index borrowed");
			if let [AggState::Count(total)] =
				by_index.states_at_mut(id).expect("state").as_mut_slice()
			{
				*total += 1;
			}
			if let [AggState::Count(total)] = by_mut
				.entry_or_init_borrowed(&borrowed, 1, |_| Ok(count_state(0)))
				.expect("mut borrowed")
				.as_mut_slice()
			{
				*total += 1;
			}
		}
		let lookups = |groups: &BoundGroupedAggregateGroups| {
			groups
				.state
				.profile
				.as_ref()
				.expect("profile")
				.entry_lookups
		};
		assert_eq!(lookups(&by_index), 300);
		assert_eq!(lookups(&by_mut), 300);
		let entries = |groups: &mut BoundGroupedAggregateGroups| {
			groups
				.finalize_sorted()
				.expect("finalize")
				.iter()
				.map(|entry| (entry.key.to_vec(), format!("{:?}", entry.states)))
				.collect::<Vec<_>>()
		};
		let index_entries = entries(&mut by_index);
		assert_eq!(index_entries.len(), 120);
		assert_eq!(index_entries, entries(&mut by_mut));
		let receipt = |path: &PathBuf| std::fs::read_to_string(path).expect("receipt");
		let (a, b) = (receipt(&receipt_a), receipt(&receipt_b));
		for path in [&receipt_a, &receipt_b] {
			let _ = std::fs::remove_file(path);
		}
		for directory in [&profile_a, &profile_b] {
			let _ = std::fs::remove_dir_all(directory);
		}
		let a: serde_json::Value = serde_json::from_str(a.trim()).expect("json");
		let b: serde_json::Value = serde_json::from_str(b.trim()).expect("json");
		assert_eq!(a["misses"], b["misses"]);
		assert_eq!(a["lookup_key_values_built"], b["lookup_key_values_built"]);
		assert_eq!(a["misses"], 120);

		// Single-UTF-8 and single-Int64 paths: index forms address the same entries.
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		groups.configure_key_storage(&[DataType::Int64]);
		let ids = (0..500i64)
			.map(|value| {
				groups
					.entry_index_or_init(&[GroupValue::Int64(value % 200)], 1, || {
						Ok(count_state(value % 200))
					})
					.expect("int64 index")
			})
			.collect::<Vec<_>>();
		for (row, id) in ids.iter().enumerate() {
			assert!(matches!(
				groups.states_at_mut(*id).expect("state").as_slice(),
				[AggState::Count(seed)] if *seed == (row as i64) % 200
			));
		}
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		let first = groups
			.entry_index_or_init_single_utf8(Some("R"), 1, || Ok(count_state(7)))
			.expect("utf8 insert");
		for index in 0..100 {
			groups
				.entry_index_or_init_single_utf8(Some(&format!("x{index}")), 1, || {
					Ok(count_state(0))
				})
				.expect("growth");
		}
		let again = groups
			.entry_index_or_init_single_utf8(Some("R"), 1, || panic!("hit"))
			.expect("utf8 hit");
		assert_eq!(first, again);
		assert!(matches!(
			groups.states_at_mut(again).expect("state").as_slice(),
			[AggState::Count(7)]
		));
	}

	#[test]
	fn adr610_group_id_buffer_accounts_its_actual_capacity() {
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(2, |_| None);
		let before = groups.current_resident_bytes();
		groups.prepare_group_ids(1_000).expect("reserve");
		let capacity = groups.state.group_ids.capacity();
		assert!(capacity >= 1_000);
		assert_eq!(
			groups.current_resident_bytes(),
			before + capacity_bytes::<u32>(capacity)
		);
		assert_eq!(
			groups.current_resident_bytes(),
			groups.recomputed_resident_bytes_for_test()
		);
		assert_eq!(
			groups.group_ids_peak_capacity_bytes(),
			capacity_bytes::<u32>(capacity)
		);
		// A smaller batch reuses the buffer without new bytes.
		groups.prepare_group_ids(10).expect("reuse");
		assert_eq!(groups.state.group_ids.capacity(), capacity);
		assert_eq!(
			groups.current_resident_bytes(),
			groups.recomputed_resident_bytes_for_test()
		);
		for id in 0..10 {
			groups.push_group_id(id).expect("push");
		}
		// A budget below the next request refuses the reservation with the table's budget error.
		groups.set_growth_budget_bytes(Some(groups.current_resident_bytes()));
		assert!(matches!(
			groups.prepare_group_ids(capacity + 1),
			Err(ExecutionError::BudgetExhausted { .. })
		));
	}
}
