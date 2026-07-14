use super::{AggState, GroupValue};
use crate::distributed::budget::BudgetDimension;
use crate::error::ExecutionError;
use ahash::RandomState;
use serde::Serialize;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const EMPTY_BUCKET: u32 = u32::MAX;
const INITIAL_ENTRY_CAPACITY: usize = 1024;
const GROUPED_FOLD_PRODUCER: &str = "generic bound grouped-fold hash table";
const PROFILE_SCHEMA_VERSION: u32 = 1;
pub(in crate::exec) const GENERIC_GROUPED_FOLD_PROFILE_ENV: &str =
	"TORON_GENERIC_GROUPED_FOLD_PROFILE_DIR";

static NEXT_PROFILE_ID: AtomicU64 = AtomicU64::new(1);

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
	keys: Vec<Vec<GroupValue>>,
	states: Vec<Vec<AggState>>,
	entry_limit: usize,
	initial_entry_capacity: usize,
	hasher: RandomState,
	growth_budget_bytes: Option<u64>,
	current_resident_bytes: u64,
	peak_resident_bytes: u64,
	maximum_projected_growth_peak_bytes: u64,
	maximum_finalize_sort_scratch_bytes: u64,
	profile: Option<Box<GenericGroupedFoldProfile>>,
}

impl BoundGroupedAggregateGroups {
	pub(in crate::exec) fn new() -> Self {
		Self::from_lookup(|name| std::env::var_os(name), INITIAL_ENTRY_CAPACITY)
	}

	fn from_lookup(
		lookup: impl FnOnce(&str) -> Option<OsString>,
		initial_entry_capacity: usize,
	) -> Self {
		let profile = GenericGroupedFoldProfile::from_lookup(lookup).map(Box::new);
		let state = BoundGroupedAggregateGroupState {
			buckets: Vec::new(),
			keys: Vec::new(),
			states: Vec::new(),
			entry_limit: 0,
			initial_entry_capacity: initial_entry_capacity.max(1).next_power_of_two(),
			hasher: RandomState::new(),
			growth_budget_bytes: None,
			current_resident_bytes: 0,
			peak_resident_bytes: 0,
			maximum_projected_growth_peak_bytes: 0,
			maximum_finalize_sort_scratch_bytes: 0,
			profile,
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

	pub(in crate::exec) fn profile_enabled(&self) -> bool {
		self.state.profile.is_some()
	}

	pub(in crate::exec) fn set_growth_budget_bytes(&mut self, budget: Option<u64>) {
		self.state.growth_budget_bytes = budget;
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

	pub(in crate::exec) fn entry_or_init(
		&mut self,
		key: Vec<GroupValue>,
		state_count: usize,
		init: impl FnOnce() -> Result<Vec<AggState>, ExecutionError>,
	) -> Result<&mut Vec<AggState>, ExecutionError> {
		let lookup = self.state.lookup_entry(&key)?;
		if let Some(profile) = self.state.profile.as_mut() {
			profile.record_lookup(lookup.probes, lookup.entry.is_some());
		}
		if let Some(entry) = lookup.entry {
			return self
				.state
				.states
				.get_mut(entry)
				.ok_or_else(|| table_error("hash bucket points past aggregate states"));
		}

		if self.state.keys.len() == self.state.entry_limit {
			self.state.grow()?;
		}
		let vacant = self.state.find_bucket(&key)?;
		if vacant.entry.is_some() {
			return Err(table_error("vacant grouped-fold lookup became occupied"));
		}
		let key_bytes = group_key_resident_bytes(&key);
		let requested_state_bytes = capacity_bytes::<AggState>(state_count);
		self.state.guard(
			self.state
				.current_resident_bytes()
				.saturating_add(key_bytes)
				.saturating_add(requested_state_bytes),
		)?;
		let states = init()?;
		let actual_state_bytes = capacity_bytes::<AggState>(states.capacity());
		self.state.guard(
			self.state
				.current_resident_bytes()
				.saturating_add(key_bytes)
				.saturating_add(actual_state_bytes),
		)?;
		let entry = u32::try_from(self.state.keys.len()).map_err(|_| {
			table_error("generic grouped-fold hash table exceeds the u32 entry domain")
		})?;
		if entry == EMPTY_BUCKET {
			return Err(table_error(
				"generic grouped-fold hash table exhausted the u32 entry domain",
			));
		}
		let key_capacity = self.state.keys.capacity();
		let state_capacity = self.state.states.capacity();
		self.state.keys.push(key);
		self.state.states.push(states);
		debug_assert_eq!(self.state.keys.capacity(), key_capacity);
		debug_assert_eq!(self.state.states.capacity(), state_capacity);
		self.state.buckets[vacant.bucket] = entry;
		self.state.current_resident_bytes = self
			.state
			.current_resident_bytes
			.saturating_add(key_bytes)
			.saturating_add(actual_state_bytes);
		self.state.observe_peak();
		self.state
			.states
			.last_mut()
			.ok_or_else(|| table_error("inserted aggregate state is missing"))
	}

	pub(in crate::exec) fn finalize_sorted(
		&mut self,
	) -> Result<Vec<BoundGroupedAggregateEntry<'_>>, ExecutionError> {
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
		entries.extend(self.state.keys.iter().zip(self.state.states.iter()).map(
			|(key, states)| BoundGroupedAggregateEntry {
				key: key.as_slice(),
				states: states.as_slice(),
			},
		));
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
		if let Some(profile) = self.state.profile.as_mut() {
			profile.record_finalize_total(elapsed);
			profile.persist(
				self.state.keys.len(),
				current_resident_bytes,
				peak_resident_bytes,
				maximum_projected_growth_peak_bytes,
				maximum_finalize_sort_scratch_bytes,
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
			.saturating_add(capacity_bytes::<Vec<GroupValue>>(
				self.state.keys.capacity(),
			))
			.saturating_add(capacity_bytes::<Vec<AggState>>(
				self.state.states.capacity(),
			));
		let keys = self
			.state
			.keys
			.iter()
			.map(group_key_resident_bytes)
			.fold(0u64, u64::saturating_add);
		let states = self
			.state
			.states
			.iter()
			.map(|states| capacity_bytes::<AggState>(states.capacity()))
			.fold(0u64, u64::saturating_add);
		outer.saturating_add(keys).saturating_add(states)
	}
}

struct HashLookup {
	bucket: usize,
	entry: Option<usize>,
	probes: usize,
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

	fn grow(&mut self) -> Result<(), ExecutionError> {
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
			.saturating_add(capacity_bytes::<Vec<GroupValue>>(self.keys.capacity()))
			.saturating_add(capacity_bytes::<Vec<AggState>>(self.states.capacity()));
		let requested_new_buffers = capacity_bytes::<Vec<GroupValue>>(target)
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

		let mut keys = Vec::new();
		keys.try_reserve_exact(target)
			.map_err(|error| table_error(format!("key-table allocation failed: {error}")))?;
		let mut states = Vec::new();
		states
			.try_reserve_exact(target)
			.map_err(|error| table_error(format!("state-table allocation failed: {error}")))?;
		let mut buckets = Vec::new();
		buckets
			.try_reserve_exact(bucket_count)
			.map_err(|error| table_error(format!("bucket-table allocation failed: {error}")))?;
		let actual_new_buffers = capacity_bytes::<Vec<GroupValue>>(keys.capacity())
			.saturating_add(capacity_bytes::<Vec<AggState>>(states.capacity()))
			.saturating_add(capacity_bytes::<u32>(buckets.capacity()));
		let actual_peak = self
			.current_resident_bytes()
			.saturating_add(actual_new_buffers);
		self.maximum_projected_growth_peak_bytes =
			self.maximum_projected_growth_peak_bytes.max(actual_peak);
		self.guard(actual_peak)?;

		buckets.resize(bucket_count, EMPTY_BUCKET);
		for (entry, key) in self.keys.iter().enumerate() {
			let lookup = find_bucket_in(&self.hasher, &buckets, &self.keys, key)?;
			if lookup.entry.is_some() {
				return Err(table_error(
					"duplicate key while rebuilding grouped-fold hash table",
				));
			}
			buckets[lookup.bucket] = u32::try_from(entry)
				.map_err(|_| table_error("grouped-fold entry index exceeds u32"))?;
		}

		keys.extend(self.keys.drain(..));
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
	keys: &[Vec<GroupValue>],
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
		if keys.get(entry).is_some_and(|candidate| candidate == key) {
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

fn group_key_resident_bytes(key: &Vec<GroupValue>) -> u64 {
	capacity_bytes::<GroupValue>(key.capacity()).saturating_add(
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
	) -> Result<(), ExecutionError> {
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
			counter_overflowed: self.counter_overflowed,
		};
		write_snapshot(&self.directory, self.fold_id, &snapshot)
	}
}

#[derive(Serialize)]
struct GenericGroupedFoldSnapshot {
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
	counter_overflowed: bool,
}

fn write_snapshot(
	directory: &Path,
	fold_id: u64,
	snapshot: &GenericGroupedFoldSnapshot,
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

	#[test]
	fn disabled_profile_constructs_no_state_or_sink() {
		let directory = test_directory("disabled");
		let groups = BoundGroupedAggregateGroups::with_test_profile_lookup(4, |_| None);
		assert!(!groups.profile_enabled());
		assert!(!directory.exists());
	}

	#[test]
	fn paired_finalize_keeps_states_with_sorted_keys_and_persists_route_receipt() {
		let directory = test_directory("paired");
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(4, |_| {
			Some(directory.clone().into_os_string())
		});
		for (key, count) in [(3, 30), (1, 10), (2, 20)] {
			groups
				.entry_or_init(vec![GroupValue::Int64(key)], 1, || Ok(count_state(count)))
				.expect("insert group");
		}
		let entries = groups.finalize_sorted().expect("sorted entries");
		let observed = entries
			.iter()
			.map(|entry| match (&entry.key[0], entry.states[0]) {
				(GroupValue::Int64(key), AggState::Count(count)) => (*key, count),
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
		assert_eq!(snapshot["schema_version"], 1);
		assert_eq!(snapshot["groups"], 3);
		assert_eq!(snapshot["vacant_lookups"], 3);
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
			.entry_or_init(vec![GroupValue::Int64(1)], 1, || Ok(count_state(1)))
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
				.entry_or_init(vec![GroupValue::Int64(key)], 1, || Ok(count_state(key)))
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
			.entry_or_init(vec![GroupValue::Int64(3)], 1, || {
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
			.entry_or_init(vec![GroupValue::Int64(3)], 1, || Ok(count_state(3)))
			.expect("exact projected peak admits growth");
		assert_eq!(groups.len(), 3);
		assert_eq!(
			groups.current_resident_bytes(),
			groups.recomputed_resident_bytes_for_test()
		);
	}
}
