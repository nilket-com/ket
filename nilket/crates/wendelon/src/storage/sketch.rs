use crate::schema::aggregate::{
	AGG_KLL_DEFAULT_K, AGG_KLL_MAX_K, AGG_KLL_MIN_K, AggregateState, validate_kll_k,
};
use crate::schema::{HllPrecision, KllK};
use crate::storage::ColumnStatsHllPayload;
use crate::storage::stats::estimate_hll_distinct_count;
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

	pub fn total_item_count(&self) -> u64 {
		self.total_item_count
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
}
