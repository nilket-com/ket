use crate::schema::aggregate::{
	AGG_KLL_MAX_K, AGG_KLL_MIN_K, AggregateState, KllSamplerState, validate_kll_k,
};
use crate::schema::{HllPrecision, KllK, RangeDistributionBucket};
use crate::storage::ColumnStatsHllPayload;
use crate::storage::stats::estimate_hll_distinct_count;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HllRegisters {
	precision: HllPrecision,
	registers: Vec<u8>,
}

impl HllRegisters {
	pub fn new(precision: HllPrecision) -> Self {
		let register_count = 1_usize << precision.get();
		Self {
			precision,
			registers: vec![0; register_count],
		}
	}

	pub fn from_registers(precision: HllPrecision, registers: Vec<u8>) -> std::io::Result<Self> {
		let expected = 1_usize << precision.get();
		if registers.len() != expected {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"HLL register count {} does not match precision {}",
					registers.len(),
					precision.get()
				),
			));
		}
		Ok(Self {
			precision,
			registers,
		})
	}

	pub fn precision(&self) -> HllPrecision {
		self.precision
	}

	pub fn registers(&self) -> &[u8] {
		&self.registers
	}

	pub fn insert(&mut self, canonical_value: &[u8]) {
		let hash = u64::from_be_bytes(
			blake3::hash(canonical_value).as_bytes()[..8]
				.try_into()
				.expect("fixed hash prefix"),
		);
		let (index, rank) = hll_register_index_and_rank(hash, self.precision.get());
		if let Some(register) = self.registers.get_mut(index) {
			*register = (*register).max(rank);
		}
	}

	pub fn merge(&mut self, other: &Self) -> std::io::Result<()> {
		if self.precision != other.precision {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"HLL precision mismatch during merge",
			));
		}
		for (left, right) in self.registers.iter_mut().zip(&other.registers) {
			*left = (*left).max(*right);
		}
		Ok(())
	}

	pub fn estimate(&self) -> std::io::Result<f64> {
		estimate_hll_distinct_count(&ColumnStatsHllPayload {
			precision: self.precision.get(),
			registers: self.registers.clone(),
		})
	}

	pub fn into_state(self) -> AggregateState {
		AggregateState::Hll {
			precision: self.precision,
			registers: self.registers,
		}
	}
}

fn hll_register_index_and_rank(hash: u64, precision: u8) -> (usize, u8) {
	let index = (hash >> (64 - precision)) as usize;
	let remaining = hash << precision;
	let max_rank = 64 - precision + 1;
	let rank = (remaining.leading_zeros() + 1).min(u32::from(max_rank)) as u8;
	(index, rank)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpaceSaving<K> {
	capacity: usize,
	counts: BTreeMap<K, u64>,
}

impl<K> SpaceSaving<K>
where
	K: Ord + Clone,
{
	pub fn new(capacity: usize) -> Self {
		Self {
			capacity,
			counts: BTreeMap::new(),
		}
	}

	pub fn insert(&mut self, key: K) {
		if self.capacity == 0 {
			return;
		}
		if let Some(count) = self.counts.get_mut(&key) {
			*count = count.saturating_add(1);
			return;
		}
		if self.counts.len() < self.capacity {
			self.counts.insert(key, 1);
			return;
		}
		let Some((evicted_key, evicted_count)) = self
			.counts
			.iter()
			.min_by(|(left_key, left_count), (right_key, right_count)| {
				left_count
					.cmp(right_count)
					.then_with(|| left_key.cmp(right_key))
			})
			.map(|(key, count)| (key.clone(), *count))
		else {
			return;
		};
		self.counts.remove(&evicted_key);
		self.counts.insert(key, evicted_count.saturating_add(1));
	}

	pub fn keys(&self) -> Vec<K> {
		self.counts.keys().cloned().collect()
	}
}

const KLL_TOP_LEVELS_AT_FULL_CAPACITY: usize = 4;
const KLL_RANDOMNESS_BYTES: usize = 32;
const KLL_COMPACTION_DOMAIN: &[u8; 8] = b"KLLCMP01";
const KLL_SAMPLER_DOMAIN: &[u8; 8] = b"KLLSMP01";
const KLL_MERGE_SEED_DOMAIN: &[u8] = b"nilket-kll-merge-seed-v1";
const KLL_MERGE_IDENTITY_DOMAIN: &[u8] = b"nilket-kll-merge-identity-v1";
const KLL_LEGACY_SEED_DOMAIN: &[u8] = b"nilket-kll-legacy-seed-v2";
const KLL_LEGACY_IDENTITY_DOMAIN: &[u8] = b"nilket-kll-legacy-identity-v2";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CanonicalValue([u8; 8]);

impl CanonicalValue {
	fn from_bytes(value: &[u8], width: usize) -> std::io::Result<Self> {
		if value.len() != width || width > 8 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"KLL value width {} does not match expected {width}",
					value.len()
				),
			));
		}
		let mut bytes = [0_u8; 8];
		bytes[..width].copy_from_slice(value);
		Ok(Self(bytes))
	}

	fn to_vec(self, width: usize) -> Vec<u8> {
		self.0[..width].to_vec()
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct KllSampler {
	item: Option<CanonicalValue>,
	weight: u64,
	height: u32,
}

impl Default for KllSampler {
	fn default() -> Self {
		Self {
			item: None,
			weight: 0,
			height: 0,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KllSketch {
	arrow_type_tag: u8,
	value_width: usize,
	k: KllK,
	total_item_count: u64,
	levels: Vec<Vec<CanonicalValue>>,
	seed: [u8; KLL_RANDOMNESS_BYTES],
	identity: [u8; KLL_RANDOMNESS_BYTES],
	compaction_counters: Vec<u64>,
	sampler: KllSampler,
}

impl KllSketch {
	pub fn new_with_randomness(
		arrow_type_tag: u8,
		k: KllK,
		seed: [u8; KLL_RANDOMNESS_BYTES],
		identity: [u8; KLL_RANDOMNESS_BYTES],
	) -> std::io::Result<Self> {
		validate_kll_k(k.get()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("KLL k must be in {}..={}", AGG_KLL_MIN_K, AGG_KLL_MAX_K),
			)
		})?;
		let value_width = kll_value_width(arrow_type_tag).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("KLL arrow type tag {arrow_type_tag:#04x} is unsupported"),
			)
		})?;
		Ok(Self {
			arrow_type_tag,
			value_width,
			k,
			total_item_count: 0,
			levels: vec![Vec::new()],
			seed,
			identity,
			compaction_counters: vec![0],
			sampler: KllSampler::default(),
		})
	}

	pub(crate) fn from_legacy_levels(
		arrow_type_tag: u8,
		k: KllK,
		total_item_count: u64,
		levels: Vec<Vec<Vec<u8>>>,
		compaction_seed: u32,
	) -> std::io::Result<Self> {
		let represented = levels
			.iter()
			.enumerate()
			.try_fold(0_u64, |sum, (height, level)| {
				let weight = 1_u64.checked_shl(height as u32)?;
				sum.checked_add(weight.checked_mul(level.len() as u64)?)
			})
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "legacy KLL weight overflow"))?;
		if represented != total_item_count {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"compacted legacy KLL state discarded its level weights",
			));
		}
		let (seed, identity) = legacy_randomness_from_content(
			arrow_type_tag,
			k,
			total_item_count,
			&levels,
			compaction_seed,
		)?;
		let mut sketch = Self::new_with_randomness(arrow_type_tag, k, seed, identity)?;
		sketch.total_item_count = total_item_count;
		sketch.levels = levels
			.into_iter()
			.map(|level| {
				level
					.into_iter()
					.map(|value| CanonicalValue::from_bytes(&value, sketch.value_width))
					.collect::<std::io::Result<Vec<_>>>()
			})
			.collect::<std::io::Result<Vec<_>>>()?;
		if sketch.levels.is_empty() {
			sketch.levels.push(Vec::new());
		}
		sketch.compaction_counters.resize(sketch.levels.len(), 0);
		Ok(sketch)
	}

	pub fn from_state(
		arrow_type_tag: u8,
		k: KllK,
		total_item_count: u64,
		levels: Vec<Vec<Vec<u8>>>,
		seed: [u8; 32],
		identity: [u8; 32],
		compaction_counters: Vec<u64>,
		sampler: KllSamplerState,
	) -> std::io::Result<Self> {
		let mut sketch = Self::new_with_randomness(arrow_type_tag, k, seed, identity)?;
		sketch.total_item_count = total_item_count;
		sketch.levels = levels
			.into_iter()
			.map(|level| {
				level
					.into_iter()
					.map(|value| CanonicalValue::from_bytes(&value, sketch.value_width))
					.collect::<std::io::Result<Vec<_>>>()
			})
			.collect::<std::io::Result<Vec<_>>>()?;
		if sketch.levels.is_empty() {
			sketch.levels.push(Vec::new());
		}
		sketch.compaction_counters = compaction_counters;
		sketch
			.compaction_counters
			.resize(sketch.levels.len().max(sampler.height as usize + 1), 0);
		let sampler_item = sampler
			.item
			.as_deref()
			.map(|item| CanonicalValue::from_bytes(item, sketch.value_width))
			.transpose()?;
		let capacity = 1_u64.checked_shl(sampler.height).unwrap_or(u64::MAX);
		if sampler.weight >= capacity || (sampler.weight == 0) != sampler_item.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"invalid KLL sampler state",
			));
		}
		let represented = sketch
			.levels
			.iter()
			.enumerate()
			.try_fold(0_u64, |sum, (height, level)| {
				let weight = 1_u64.checked_shl(height as u32)?;
				sum.checked_add(weight.checked_mul(level.len() as u64)?)
			})
			.and_then(|sum| sum.checked_add(sampler.weight))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "KLL state weight overflow"))?;
		if represented != total_item_count {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"KLL state weights do not equal total item count",
			));
		}
		sketch.sampler = KllSampler {
			item: sampler_item,
			weight: sampler.weight,
			height: sampler.height,
		};
		Ok(sketch)
	}

	pub fn insert(&mut self, canonical_value: Vec<u8>) {
		let value = CanonicalValue::from_bytes(&canonical_value, self.value_width)
			.expect("KLL callers validate canonical value width before insertion");
		self.total_item_count = self.total_item_count.saturating_add(1);
		if let Some(sampled) = self.update_sampler(value, 1) {
			self.ensure_level(self.sampler.height as usize);
			self.levels[self.sampler.height as usize].push(sampled);
		}
		self.compact_if_needed();
	}

	pub fn merge(&mut self, other: &Self) -> std::io::Result<()> {
		if self.arrow_type_tag != other.arrow_type_tag || self.k != other.k {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"KLL type or k mismatch during merge",
			));
		}
		// The lower sampler is the donor. Equal heights use the ADR 0083
		// lexicographic `(identity, seed)` tie-break before any state is touched.
		let self_key = (self.sampler.height, self.identity, self.seed);
		let other_key = (other.sampler.height, other.identity, other.seed);
		let (donor, receiver) = if self_key <= other_key {
			(&*self, other)
		} else {
			(other, &*self)
		};
		let mut merged = receiver.clone();
		merged.seed = merge_randomness(KLL_MERGE_SEED_DOMAIN, donor.seed, receiver.seed);
		merged.identity =
			merge_randomness(KLL_MERGE_IDENTITY_DOMAIN, donor.identity, receiver.identity);
		merged.total_item_count = donor
			.total_item_count
			.saturating_add(receiver.total_item_count);
		let level_count = donor.levels.len().max(receiver.levels.len());
		merged.levels.resize_with(level_count, Vec::new);
		merged.compaction_counters.resize(level_count, 0);
		for height in 0..level_count {
			merged.compaction_counters[height] = donor
				.compaction_counters
				.get(height)
				.copied()
				.unwrap_or(0)
				.max(
					receiver
						.compaction_counters
						.get(height)
						.copied()
						.unwrap_or(0),
				);
		}
		if let Some(item) = donor.sampler.item
			&& let Some(sampled) = merged.update_sampler(item, donor.sampler.weight)
		{
			merged.ensure_level(merged.sampler.height as usize);
			merged.levels[merged.sampler.height as usize].push(sampled);
		}
		for (height, values) in donor.levels.iter().enumerate() {
			if height <= merged.sampler.height as usize {
				let weight = 1_u64.checked_shl(height as u32).unwrap_or(u64::MAX);
				for value in values {
					if let Some(sampled) = merged.update_sampler(*value, weight) {
						merged.ensure_level(merged.sampler.height as usize);
						merged.levels[merged.sampler.height as usize].push(sampled);
					}
				}
			} else {
				merged.ensure_level(height);
				merged.levels[height].extend_from_slice(values);
			}
		}
		for level in &mut merged.levels {
			level.sort();
		}
		merged.compact_if_needed();
		for level in &mut merged.levels {
			level.sort();
		}
		*self = merged;
		Ok(())
	}

	pub fn retained_values(&self) -> Vec<Vec<u8>> {
		let mut values = self
			.levels
			.iter()
			.flatten()
			.copied()
			.map(|value| value.to_vec(self.value_width))
			.collect::<Vec<_>>();
		if let Some(item) = self.sampler.item {
			values.push(item.to_vec(self.value_width));
		}
		values
	}

	pub fn arrow_type_tag(&self) -> u8 {
		self.arrow_type_tag
	}

	pub fn k(&self) -> KllK {
		self.k
	}

	pub fn total_item_count(&self) -> u64 {
		self.total_item_count
	}

	pub fn retained_item_count(&self) -> usize {
		self.levels.iter().map(Vec::len).sum::<usize>() + usize::from(self.sampler.item.is_some())
	}

	pub fn retained_payload_bytes(&self) -> usize {
		self.retained_item_count().saturating_mul(self.value_width)
	}

	pub fn allocated_payload_bytes(&self) -> usize {
		self.levels
			.iter()
			.map(|level| level.capacity().saturating_mul(self.value_width))
			.sum::<usize>()
			.saturating_add(usize::from(self.sampler.item.is_some()) * self.value_width)
	}

	pub(crate) fn compaction_seed(&self) -> u32 {
		u32::from_be_bytes(self.seed[..4].try_into().expect("seed prefix"))
	}

	pub fn query_quantile(&self, quantile: f64) -> std::io::Result<Option<Vec<u8>>> {
		if !quantile.is_finite() || !(0.0..=1.0).contains(&quantile) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"KLL quantile must be finite and in [0, 1]",
			));
		}
		if self.total_item_count == 0
			|| (self.levels.iter().all(Vec::is_empty) && self.sampler.item.is_none())
		{
			return Ok(None);
		}
		let target_rank =
			(quantile * self.total_item_count.saturating_sub(1) as f64).round() as u64;
		let mut weighted = self
			.levels
			.iter()
			.enumerate()
			.flat_map(|(height, values)| {
				let weight = 1_u64.checked_shl(height as u32).unwrap_or(u64::MAX);
				values.iter().copied().map(move |value| (value, weight))
			})
			.collect::<Vec<_>>();
		if let Some(item) = self.sampler.item {
			weighted.push((item, self.sampler.weight));
		}
		weighted.sort_by_key(|(value, _)| *value);
		let mut cumulative = 0_u64;
		let mut last = None;
		for (value, weight) in weighted {
			last = Some(value);
			cumulative = cumulative.saturating_add(weight);
			if cumulative > target_rank {
				return Ok(Some(value.to_vec(self.value_width)));
			}
		}
		Ok(last.map(|value| value.to_vec(self.value_width)))
	}

	pub fn query_buckets(
		&self,
		requested_count: usize,
	) -> std::io::Result<Vec<RangeDistributionBucket>> {
		if requested_count == 0
			|| self.total_item_count == 0
			|| (self.levels.iter().all(Vec::is_empty) && self.sampler.item.is_none())
		{
			return Ok(Vec::new());
		}
		// ADR 0083 withdraws the unsupported `1/k` rank-error formula. The
		// selected aggregate contract is epsilon=1%, hence at most 100 useful
		// equal-rank buckets. CSB v1 never calls this path after suppression.
		let resolution_cap = 100_usize;
		let total_count = usize::try_from(self.total_item_count).unwrap_or(usize::MAX);
		let bucket_count = requested_count.min(resolution_cap).min(total_count).max(1);
		let base_count = self.total_item_count / bucket_count as u64;
		let mut remainder = self.total_item_count % bucket_count as u64;
		let mut start_rank = 0_u64;
		let mut buckets: Vec<RangeDistributionBucket> = Vec::with_capacity(bucket_count);
		for _ in 0..bucket_count {
			let mut count = base_count;
			if remainder != 0 {
				count = count.saturating_add(1);
				remainder -= 1;
			}
			if count == 0 {
				continue;
			}
			let end_rank = start_rank.saturating_add(count).saturating_sub(1);
			let lower = self
				.query_quantile(
					start_rank as f64 / self.total_item_count.saturating_sub(1).max(1) as f64,
				)?
				.ok_or_else(|| Error::new(ErrorKind::InvalidData, "KLL lower rank missing"))?;
			let upper = self
				.query_quantile(
					end_rank as f64 / self.total_item_count.saturating_sub(1).max(1) as f64,
				)?
				.ok_or_else(|| Error::new(ErrorKind::InvalidData, "KLL upper rank missing"))?;
			if let Some(previous) = buckets.last_mut()
				&& lower <= previous.upper
			{
				if upper > previous.upper {
					previous.upper = upper;
				}
				previous.count = previous.count.checked_add(count).ok_or_else(|| {
					Error::new(ErrorKind::InvalidData, "KLL bucket count overflow")
				})?;
				start_rank = start_rank.saturating_add(count);
				continue;
			}
			buckets.push(RangeDistributionBucket {
				lower,
				upper,
				count,
			});
			start_rank = start_rank.saturating_add(count);
		}
		Ok(buckets)
	}

	pub fn into_state(mut self) -> AggregateState {
		for level in &mut self.levels {
			level.sort();
		}
		AggregateState::Kll {
			arrow_type_tag: self.arrow_type_tag,
			k: self.k,
			total_item_count: self.total_item_count,
			levels: self
				.levels
				.into_iter()
				.map(|level| {
					level
						.into_iter()
						.map(|value| value.to_vec(self.value_width))
						.collect()
				})
				.collect(),
			seed: self.seed,
			identity: self.identity,
			compaction_counters: self.compaction_counters,
			sampler: KllSamplerState {
				item: self.sampler.item.map(|item| item.to_vec(self.value_width)),
				weight: self.sampler.weight,
				height: self.sampler.height,
			},
		}
	}

	fn compact_if_needed(&mut self) {
		let mut compacted = false;
		loop {
			let top_height = self.levels.len().saturating_sub(1);
			let target_sampler_height = self.target_sampler_height(top_height);
			if target_sampler_height > self.sampler.height {
				self.grow_sampler(target_sampler_height);
				compacted = true;
				continue;
			}
			let Some(height) = (0..self.levels.len()).find(|height| {
				*height >= self.sampler.height as usize
					&& self.levels[*height].len() >= self.level_capacity(*height, top_height)
			}) else {
				break;
			};
			self.compact_level(height);
			compacted = true;
		}
		if compacted {
			for level in &mut self.levels {
				level.shrink_to_fit();
			}
		}
	}

	fn level_capacity(&self, height: usize, top_height: usize) -> usize {
		let distance_from_top = top_height.saturating_sub(height);
		let reductions = distance_from_top.saturating_sub(KLL_TOP_LEVELS_AT_FULL_CAPACITY - 1);
		let mut capacity = self.k.get() as usize;
		for _ in 0..reductions {
			capacity = capacity.saturating_mul(2).div_ceil(3);
		}
		capacity.max(2)
	}

	fn target_sampler_height(&self, top_height: usize) -> u32 {
		let log_k = u32::BITS - self.k.get().saturating_sub(1).leading_zeros();
		(top_height as u32).saturating_sub((2 * KLL_TOP_LEVELS_AT_FULL_CAPACITY) as u32 + log_k)
	}

	fn grow_sampler(&mut self, target_height: u32) {
		while self.sampler.height < target_height {
			let absorbed_height = self.sampler.height as usize;
			self.sampler.height += 1;
			let values = if absorbed_height < self.levels.len() {
				std::mem::take(&mut self.levels[absorbed_height])
			} else {
				Vec::new()
			};
			let weight = 1_u64
				.checked_shl(absorbed_height as u32)
				.unwrap_or(u64::MAX);
			for value in values {
				if let Some(sampled) = self.update_sampler(value, weight) {
					self.ensure_level(self.sampler.height as usize);
					self.levels[self.sampler.height as usize].push(sampled);
				}
			}
		}
	}

	/// Mergeable sampler update from KLL section 4. Inputs may be weighted only
	/// when compactors or another sampler are folded into this one during merge.
	fn update_sampler(
		&mut self,
		incoming: CanonicalValue,
		incoming_weight: u64,
	) -> Option<CanonicalValue> {
		let capacity = 1_u64.checked_shl(self.sampler.height).unwrap_or(u64::MAX);
		let current_weight = self.sampler.weight;
		let combined = current_weight.saturating_add(incoming_weight);
		if combined <= capacity {
			if current_weight == 0 || self.sampler_draw(incoming_weight, combined) {
				self.sampler.item = Some(incoming);
			}
			self.sampler.weight = combined;
			if combined == capacity {
				self.sampler.weight = 0;
				return self.sampler.item.take();
			}
			return None;
		}

		let current = self
			.sampler
			.item
			.expect("positive sampler weight has an item");
		let (heavier, heavier_weight, lighter, lighter_weight) =
			if current_weight >= incoming_weight {
				(current, current_weight, incoming, incoming_weight)
			} else {
				(incoming, incoming_weight, current, current_weight)
			};
		self.sampler.item = (lighter_weight != 0).then_some(lighter);
		self.sampler.weight = lighter_weight;
		self.sampler_draw(heavier_weight, capacity)
			.then_some(heavier)
	}

	fn sampler_draw(&mut self, numerator: u64, denominator: u64) -> bool {
		if numerator >= denominator {
			return true;
		}
		let height = self.sampler.height as usize;
		self.ensure_level(height);
		let counter = self.compaction_counters[height];
		self.compaction_counters[height] = counter.saturating_add(1);
		let sample = random_u64(
			self.seed,
			self.identity,
			KLL_SAMPLER_DOMAIN,
			height,
			counter,
		);
		u128::from(sample) * u128::from(denominator)
			< u128::from(numerator) * (u128::from(u64::MAX) + 1)
	}

	fn ensure_level(&mut self, height: usize) {
		if height >= self.levels.len() {
			self.levels.resize_with(height + 1, Vec::new);
		}
		if height >= self.compaction_counters.len() {
			self.compaction_counters.resize(height + 1, 0);
		}
	}

	fn compact_level(&mut self, height: usize) {
		if height + 1 == self.levels.len() {
			self.levels.push(Vec::new());
			self.compaction_counters.push(0);
		}
		let values = &mut self.levels[height];
		values.sort();
		let counter = self.compaction_counters[height];
		let choice = compaction_choice(self.seed, self.identity, height, counter);
		self.compaction_counters[height] = counter.saturating_add(1);
		let retained = if values.len() % 2 == 0 {
			None
		} else if choice & 0b10 == 0 {
			Some(values.remove(0))
		} else {
			values.pop()
		};
		let phase = choice & 0b01;
		let promoted = values
			.iter()
			.skip(phase)
			.step_by(2)
			.copied()
			.collect::<Vec<_>>();
		values.clear();
		if let Some(value) = retained {
			values.push(value);
		}
		self.levels[height + 1].extend(promoted);
	}
}

/// Legacy aggregate state has no execution context from which to obtain fresh
/// randomness. Deriving its compatibility-only stream from the complete encoded
/// state keeps decode pure and separates distinct states without a process-global
/// ordinal. Byte-identical legacy states intentionally resume the same stream;
/// fresh sketches never use this path. Because the derivation observes retained
/// data, later compaction of recovered legacy state does not inherit the
/// fresh-sketch probability guarantee; this is a rolling-compatibility path only.
fn legacy_randomness_from_content(
	arrow_type_tag: u8,
	k: KllK,
	total_item_count: u64,
	levels: &[Vec<Vec<u8>>],
	compaction_seed: u32,
) -> std::io::Result<([u8; 32], [u8; 32])> {
	let level_count = u32::try_from(levels.len())
		.map_err(|_| Error::new(ErrorKind::InvalidData, "legacy KLL level count exceeds u32"))?;
	let mut content = Vec::new();
	content.push(arrow_type_tag);
	content.extend_from_slice(&k.get().to_be_bytes());
	content.extend_from_slice(&total_item_count.to_be_bytes());
	content.extend_from_slice(&compaction_seed.to_be_bytes());
	content.extend_from_slice(&level_count.to_be_bytes());
	for level in levels {
		let value_count = u64::try_from(level.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"legacy KLL level value count exceeds u64",
			)
		})?;
		content.extend_from_slice(&value_count.to_be_bytes());
		for value in level {
			let value_len = u32::try_from(value.len()).map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"legacy KLL retained value length exceeds u32",
				)
			})?;
			content.extend_from_slice(&value_len.to_be_bytes());
			content.extend_from_slice(value);
		}
	}
	Ok((
		derive_bytes(KLL_LEGACY_SEED_DOMAIN, &content),
		derive_bytes(KLL_LEGACY_IDENTITY_DOMAIN, &content),
	))
}

fn derive_bytes(domain: &[u8], input: &[u8]) -> [u8; 32] {
	let key = *blake3::hash(domain).as_bytes();
	*blake3::keyed_hash(&key, input).as_bytes()
}

fn merge_randomness(domain: &[u8], left: [u8; 32], right: [u8; 32]) -> [u8; 32] {
	let (lower, upper) = if left <= right {
		(left, right)
	} else {
		(right, left)
	};
	let mut input = [0_u8; 64];
	input[..32].copy_from_slice(&lower);
	input[32..].copy_from_slice(&upper);
	derive_bytes(domain, &input)
}

#[cfg(test)]
fn compaction_coin(seed: [u8; 32], identity: [u8; 32], height: usize, counter: u64) -> usize {
	compaction_choice(seed, identity, height, counter) & 0b01
}

fn compaction_choice(seed: [u8; 32], identity: [u8; 32], height: usize, counter: u64) -> usize {
	let mut input = Vec::with_capacity(KLL_COMPACTION_DOMAIN.len() + 4 + 32 + 4 + 8);
	input.extend_from_slice(KLL_COMPACTION_DOMAIN);
	input.extend_from_slice(&(identity.len() as u32).to_be_bytes());
	input.extend_from_slice(&identity);
	input.extend_from_slice(&(height as u32).to_be_bytes());
	input.extend_from_slice(&counter.to_be_bytes());
	(blake3::keyed_hash(&seed, &input).as_bytes()[0] & 0b11) as usize
}

fn random_u64(
	seed: [u8; 32],
	identity: [u8; 32],
	domain: &[u8],
	height: usize,
	counter: u64,
) -> u64 {
	let mut input = Vec::with_capacity(domain.len() + 4 + 32 + 4 + 8);
	input.extend_from_slice(domain);
	input.extend_from_slice(&(identity.len() as u32).to_be_bytes());
	input.extend_from_slice(&identity);
	input.extend_from_slice(&(height as u32).to_be_bytes());
	input.extend_from_slice(&counter.to_be_bytes());
	u64::from_be_bytes(
		blake3::keyed_hash(&seed, &input).as_bytes()[..8]
			.try_into()
			.expect("BLAKE3 prefix"),
	)
}

fn kll_value_width(arrow_type_tag: u8) -> Option<usize> {
	match arrow_type_tag {
		0x01 | 0x05 => Some(1),
		0x02 | 0x06 => Some(2),
		0x03 | 0x07 | 0x09 | 0x30 => Some(4),
		0x04 | 0x08 | 0x0A | 0x20 | 0x21 | 0x22 | 0x31 => Some(8),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn adr083_legacy_identity_is_a_pure_function_of_validated_content() {
		fn decode(values: [u32; 2]) -> KllSketch {
			KllSketch::from_legacy_levels(
				0x03,
				KllK::new(64),
				2,
				vec![vec![
					values[0].to_be_bytes().to_vec(),
					values[1].to_be_bytes().to_vec(),
				]],
				42,
			)
			.unwrap()
		}

		let first = decode([10, 20]);
		let repeated = decode([10, 20]);
		let distinct = decode([10, 21]);
		assert_eq!(
			first, repeated,
			"decoding the same legacy state twice must be deterministic"
		);
		assert_ne!(
			first.identity, distinct.identity,
			"distinct legacy states need distinct compaction domains"
		);
	}

	#[test]
	fn hll_merge_takes_register_maxima() {
		let precision = HllPrecision::new(10).unwrap();
		let mut left = HllRegisters::new(precision);
		let mut right = HllRegisters::new(precision);
		left.insert(b"a");
		right.insert(b"b");
		let left_before = left.registers.clone();
		left.merge(&right).unwrap();
		assert!(
			left.registers
				.iter()
				.zip(left_before.iter().zip(right.registers.iter()))
				.all(|(merged, (l, r))| *merged == (*l).max(*r))
		);
	}

	#[test]
	fn kll_compaction_replays_for_same_randomness_and_input() {
		let seed = [7_u8; 32];
		let identity = [11_u8; 32];
		let mut a = KllSketch::new_with_randomness(0x02, KllK::new(64), seed, identity).unwrap();
		let mut b = KllSketch::new_with_randomness(0x02, KllK::new(64), seed, identity).unwrap();
		for value in 0_u16..200 {
			let bytes = value.to_be_bytes().to_vec();
			a.insert(bytes.clone());
			b.insert(bytes);
		}
		assert_eq!(a.retained_values(), b.retained_values());
		assert_eq!(a.total_item_count(), 200);
	}

	#[test]
	fn adr083_retained_payload_stays_within_selected_bound_under_insert_and_merge() {
		const K: u32 = 800;
		const RETAINED_BOUND: usize = 7 * K as usize + 1;
		let mut sketches = Vec::new();
		for fragment in 0_u32..16 {
			let seed = derive_bytes(b"adr083-memory-seed", &fragment.to_be_bytes());
			let identity = derive_bytes(b"adr083-memory-id", &fragment.to_be_bytes());
			let mut sketch =
				KllSketch::new_with_randomness(0x08, KllK::new(K), seed, identity).unwrap();
			for value in 0_u64..65_536 {
				sketch.insert(
					(u64::from(fragment) * 65_536 + value)
						.to_be_bytes()
						.to_vec(),
				);
				assert!(sketch.retained_item_count() <= RETAINED_BOUND);
			}
			sketches.push(sketch);
		}

		let mut merged = sketches.remove(0);
		for sketch in sketches {
			merged.merge(&sketch).unwrap();
			assert!(merged.retained_item_count() <= RETAINED_BOUND);
			assert!(merged.retained_payload_bytes() <= RETAINED_BOUND * 8);
			assert!(merged.allocated_payload_bytes() <= RETAINED_BOUND * 8);
		}
	}

	#[test]
	fn kll_query_buckets_merges_duplicate_boundaries_and_preserves_count() {
		let mut sketch = KllSketch::new_with_randomness(
			0x04,
			KllK::new(64),
			derive_bytes(b"bucket-test-seed", b"bucket-test"),
			derive_bytes(b"bucket-test-identity", b"bucket-test"),
		)
		.unwrap();
		for _ in 0..90 {
			sketch.insert(0_i64.to_be_bytes().to_vec());
		}
		for value in 1_i64..=10 {
			sketch.insert(value.to_be_bytes().to_vec());
		}

		let buckets = sketch.query_buckets(64).unwrap();
		assert!(!buckets.is_empty());
		assert!(buckets.windows(2).all(|pair| pair[0].upper < pair[1].lower));
		assert_eq!(
			buckets.iter().map(|bucket| bucket.count).sum::<u64>(),
			sketch.total_item_count()
		);
	}

	#[test]
	fn adr083_arrival_order_and_shape_corpus_stays_within_the_selected_rank_bound() {
		fn observed(sketch: &KllSketch, quantile: f64) -> u32 {
			let bytes = sketch
				.query_quantile(quantile)
				.unwrap()
				.expect("non-empty sketch");
			u32::from_be_bytes(bytes.as_slice().try_into().unwrap())
		}

		fn rank_distance(population: &[u32], value: u32, target: i64) -> i64 {
			let first = population.partition_point(|candidate| *candidate < value) as i64;
			let after = population.partition_point(|candidate| *candidate <= value) as i64;
			if target < first {
				first - target
			} else if target >= after {
				target - (after - 1)
			} else {
				0
			}
		}
		fn sketch(name: &str, values: impl IntoIterator<Item = u32>) -> (String, KllSketch) {
			let mut sketch = KllSketch::new_with_randomness(
				0x03,
				KllK::new(800),
				derive_bytes(b"adr083-corpus-seed", name.as_bytes()),
				derive_bytes(b"adr083-corpus-id", name.as_bytes()),
			)
			.unwrap();
			for value in values {
				sketch.insert(value.to_be_bytes().to_vec());
			}
			(name.to_string(), sketch)
		}

		let ascending = (0_u32..10_000).collect::<Vec<_>>();
		let descending = (0_u32..10_000).rev().collect::<Vec<_>>();
		let shuffled = (0_u32..10_000)
			.map(|value| value.wrapping_mul(7_919) % 10_000)
			.collect::<Vec<_>>();
		let identical = vec![42_u32; 10_000];
		let two_valued = (0..10_000)
			.map(|index| if index < 5_000 { 10_u32 } else { 90_u32 })
			.collect::<Vec<_>>();
		let cases = [
			sketch("ascending", ascending.clone()),
			sketch("descending", descending),
			sketch("shuffled", shuffled),
			sketch("all-identical", identical.clone()),
			sketch("two-valued", two_valued.clone()),
		];
		for (name, sketch) in &cases {
			assert!(sketch.retained_item_count() < sketch.total_item_count() as usize);
			let mut truth = match name.as_str() {
				"all-identical" => identical.clone(),
				"two-valued" => two_valued.clone(),
				_ => ascending.clone(),
			};
			truth.sort_unstable();
			for quantile in [0.01, 0.25, 0.5, 0.75, 0.99] {
				let exact_rank = (quantile * 9_999.0_f64).round() as i64;
				let value = observed(sketch, quantile);
				let distance = rank_distance(&truth, value, exact_rank);
				assert!(
					distance <= 100,
					"{name} q={quantile} returned {value} at rank distance {distance}, expected rank {exact_rank} ±100",
				);
			}
		}
	}

	#[test]
	fn adr083_each_merge_tree_is_bounded_against_the_exact_population() {
		fn query(sketch: &KllSketch, quantile: f64) -> i64 {
			let bytes = sketch
				.query_quantile(quantile)
				.unwrap()
				.expect("non-empty merged sketch");
			i64::from_be_bytes(bytes.try_into().unwrap())
		}
		let seed = [0x83; 32];
		let mut fragments = (0_u8..8)
			.map(|fragment| {
				let mut sketch = KllSketch::new_with_randomness(
					0x04,
					KllK::new(800),
					seed,
					derive_bytes(b"adr083-merge-tree-id", &[fragment]),
				)
				.unwrap();
				for value in (i64::from(fragment)..10_000).step_by(8) {
					sketch.insert(value.to_be_bytes().to_vec());
				}
				sketch
			})
			.collect::<Vec<_>>();
		let mut left_fold = fragments.remove(0);
		for fragment in &fragments {
			left_fold.merge(fragment).unwrap();
		}

		let mut balanced = fragments;
		balanced.push(
			KllSketch::new_with_randomness(
				0x04,
				KllK::new(800),
				seed,
				derive_bytes(b"adr083-merge-tree-id", &[0]),
			)
			.unwrap(),
		);
		for value in (0_i64..10_000).step_by(8) {
			balanced
				.last_mut()
				.unwrap()
				.insert(value.to_be_bytes().to_vec());
		}
		while balanced.len() > 1 {
			let mut next = Vec::new();
			for pair in balanced.chunks(2) {
				let mut merged = pair[0].clone();
				if let Some(right) = pair.get(1) {
					merged.merge(right).unwrap();
				}
				next.push(merged);
			}
			balanced = next;
		}
		let balanced = balanced.pop().unwrap();

		for quantile in [0.01_f64, 0.25, 0.5, 0.75, 0.99] {
			let truth = (quantile * 9_999.0).round() as i64;
			for (name, sketch) in [("left-fold", &left_fold), ("balanced", &balanced)] {
				let actual = query(sketch, quantile);
				assert!(
					(actual - truth).abs() <= 100,
					"{name} q={quantile} returned rank {actual}, expected {truth} ±100"
				);
			}
		}
	}

	#[test]
	fn adr083_pair_merge_is_receiver_independent() {
		let mut a =
			KllSketch::new_with_randomness(0x03, KllK::new(800), [0x42; 32], [0xA1; 32]).unwrap();
		let mut b =
			KllSketch::new_with_randomness(0x03, KllK::new(800), [0x42; 32], [0xB2; 32]).unwrap();
		for value in 0_u32..4_001 {
			a.insert(value.to_be_bytes().to_vec());
		}
		for value in (4_001_u32..6_000).rev() {
			b.insert(value.to_be_bytes().to_vec());
		}

		let mut a_then_b = a.clone();
		a_then_b.merge(&b).unwrap();
		let mut b_then_a = b;
		b_then_a.merge(&a).unwrap();

		assert_eq!(
			a_then_b, b_then_a,
			"pair merge must not depend on which sketch is the receiver",
		);
	}

	#[test]
	fn adr083_equal_height_sampler_tie_break_is_receiver_independent() {
		let mut a =
			KllSketch::new_with_randomness(0x03, KllK::new(800), [0x11; 32], [0x21; 32]).unwrap();
		let mut b =
			KllSketch::new_with_randomness(0x03, KllK::new(800), [0x12; 32], [0x22; 32]).unwrap();
		a.sampler = KllSampler {
			item: Some(CanonicalValue::from_bytes(&10_u32.to_be_bytes(), 4).unwrap()),
			weight: 3,
			height: 3,
		};
		b.sampler = KllSampler {
			item: Some(CanonicalValue::from_bytes(&20_u32.to_be_bytes(), 4).unwrap()),
			weight: 2,
			height: 3,
		};
		a.total_item_count = 3;
		b.total_item_count = 2;

		let mut a_then_b = a.clone();
		a_then_b.merge(&b).unwrap();
		let mut b_then_a = b;
		b_then_a.merge(&a).unwrap();
		assert_eq!(
			a_then_b, b_then_a,
			"equal sampler heights with different items require a complete tie-break",
		);
	}

	#[test]
	fn adr083_sketch_identity_domain_separates_compaction_coins() {
		let seed = [0x51; 32];
		let left = (0_u64..8)
			.map(|index| compaction_coin(seed, [0x61; 32], 4, index))
			.collect::<Vec<_>>();
		let right = (0_u64..8)
			.map(|index| compaction_coin(seed, [0x62; 32], 4, index))
			.collect::<Vec<_>>();
		assert_ne!(left, right);
		assert_eq!(
			left,
			(0_u64..8)
				.map(|index| compaction_coin(seed, [0x61; 32], 4, index))
				.collect::<Vec<_>>()
		);
	}
}
