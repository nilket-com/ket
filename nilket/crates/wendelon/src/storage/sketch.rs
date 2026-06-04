use crate::schema::aggregate::{
	AGG_KLL_DEFAULT_K, AGG_KLL_MAX_K, AGG_KLL_MIN_K, AggregateState, validate_kll_k,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KllSketch {
	arrow_type_tag: u8,
	k: KllK,
	total_item_count: u64,
	values: Vec<Vec<u8>>,
	compaction_seed: u32,
}

impl KllSketch {
	pub fn new(arrow_type_tag: u8, k: KllK, compaction_seed: u32) -> std::io::Result<Self> {
		validate_kll_k(k.get()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("KLL k must be in {}..={}", AGG_KLL_MIN_K, AGG_KLL_MAX_K),
			)
		})?;
		Ok(Self {
			arrow_type_tag,
			k,
			total_item_count: 0,
			values: Vec::new(),
			compaction_seed,
		})
	}

	pub fn default_for_type(arrow_type_tag: u8, compaction_seed: u32) -> std::io::Result<Self> {
		Self::new(
			arrow_type_tag,
			KllK::new(AGG_KLL_DEFAULT_K),
			compaction_seed,
		)
	}

	pub fn from_retained_values(
		arrow_type_tag: u8,
		k: KllK,
		total_item_count: u64,
		values: Vec<Vec<u8>>,
		compaction_seed: u32,
	) -> std::io::Result<Self> {
		validate_kll_k(k.get()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("KLL k must be in {}..={}", AGG_KLL_MIN_K, AGG_KLL_MAX_K),
			)
		})?;
		Ok(Self {
			arrow_type_tag,
			k,
			total_item_count,
			values,
			compaction_seed,
		})
	}

	pub fn insert(&mut self, canonical_value: Vec<u8>) {
		self.total_item_count = self.total_item_count.saturating_add(1);
		self.values.push(canonical_value);
		self.compact_if_needed();
	}

	pub fn merge(&mut self, other: &Self) -> std::io::Result<()> {
		if self.arrow_type_tag != other.arrow_type_tag || self.k != other.k {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"KLL type or k mismatch during merge",
			));
		}
		self.total_item_count = self.total_item_count.saturating_add(other.total_item_count);
		self.values.extend(other.values.iter().cloned());
		self.compact_if_needed();
		Ok(())
	}

	pub fn retained_values(&self) -> &[Vec<u8>] {
		&self.values
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

	pub fn compaction_seed(&self) -> u32 {
		self.compaction_seed
	}

	pub fn rank_error_ppm(&self) -> u32 {
		ceil_div_u64(1_000_000, u64::from(self.k.get()))
			.try_into()
			.unwrap_or(u32::MAX)
	}

	pub fn query_buckets(
		&self,
		requested_count: usize,
	) -> std::io::Result<Vec<RangeDistributionBucket>> {
		if requested_count == 0 || self.total_item_count == 0 || self.values.is_empty() {
			return Ok(Vec::new());
		}
		let rank_error_ppm = self.rank_error_ppm().max(1);
		let resolution_cap = (1_000_000 / rank_error_ppm).max(1) as usize;
		let total_count = usize::try_from(self.total_item_count).unwrap_or(usize::MAX);
		let bucket_count = requested_count.min(resolution_cap).min(total_count).max(1);
		let mut values = self.values.clone();
		values.sort();
		let base_count = self.total_item_count / bucket_count as u64;
		let mut remainder = self.total_item_count % bucket_count as u64;
		let mut start_rank = 0_u64;
		let mut buckets: Vec<RangeDistributionBucket> =
			Vec::with_capacity(bucket_count.min(values.len()));
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
			let start_index =
				kll_rank_to_retained_index(start_rank, self.total_item_count, values.len());
			let end_index =
				kll_rank_to_retained_index(end_rank, self.total_item_count, values.len());
			let lower = values[start_index].clone();
			let upper = values[end_index.max(start_index)].clone();
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
		self.values.sort();
		AggregateState::Kll {
			arrow_type_tag: self.arrow_type_tag,
			k: self.k,
			total_item_count: self.total_item_count,
			levels: vec![self.values],
			compaction_seed: self.compaction_seed,
		}
	}

	fn compact_if_needed(&mut self) {
		let k = self.k.get() as usize;
		if self.values.len() <= k {
			return;
		}
		self.values.sort();
		let phase = (self.compaction_seed as usize) & 1;
		let mut compacted = self
			.values
			.iter()
			.enumerate()
			.filter_map(|(index, value)| {
				if index % 2 == phase {
					Some(value.clone())
				} else {
					None
				}
			})
			.collect::<Vec<_>>();
		while compacted.len() > k {
			compacted = compacted.into_iter().step_by(2).collect();
		}
		self.values = compacted;
		self.compaction_seed = self.compaction_seed.rotate_left(1) ^ 0x9E37_79B9;
	}
}

fn ceil_div_u64(numerator: u64, denominator: u64) -> u64 {
	if denominator == 0 {
		return u64::MAX;
	}
	numerator.div_ceil(denominator)
}

fn kll_rank_to_retained_index(rank: u64, total_count: u64, retained_count: usize) -> usize {
	if retained_count == 0 || total_count == 0 {
		return 0;
	}
	let index = (u128::from(rank) * retained_count as u128) / u128::from(total_count);
	usize::try_from(index)
		.unwrap_or(usize::MAX)
		.min(retained_count.saturating_sub(1))
}

#[cfg(test)]
mod tests {
	use super::*;

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
	fn kll_compaction_is_deterministic_for_same_seed() {
		let mut a = KllSketch::new(0x04, KllK::new(64), 7).unwrap();
		let mut b = KllSketch::new(0x04, KllK::new(64), 7).unwrap();
		for value in 0_u16..200 {
			let bytes = value.to_be_bytes().to_vec();
			a.insert(bytes.clone());
			b.insert(bytes);
		}
		assert_eq!(a.retained_values(), b.retained_values());
		assert_eq!(a.total_item_count(), 200);
	}

	#[test]
	fn kll_query_buckets_merges_duplicate_boundaries_and_preserves_count() {
		let mut sketch = KllSketch::new(0x04, KllK::new(64), 7).unwrap();
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
		assert_eq!(sketch.rank_error_ppm(), 15_625);
	}
}
