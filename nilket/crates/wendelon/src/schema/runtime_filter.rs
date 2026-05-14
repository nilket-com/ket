use serde::{Deserialize, Serialize};

use crate::schema::PhysicalDescriptorFingerprint;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RuntimeFilterKeyType {
	Int8,
	Int16,
	Int32,
	Int64,
	UInt8,
	UInt16,
	UInt32,
	UInt64,
	Utf8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFilterDescriptor {
	pub key_encoding_version: u32,
	pub key_types: Vec<RuntimeFilterKeyType>,
	pub probe_key_columns: Vec<u32>,
	pub hash_seed: u64,
	pub bit_count: u32,
	pub hash_count: u8,
	pub byte_budget: u32,
	pub target_fp_rate_bp: u16,
	pub payload: Vec<u8>,
	pub probe_descriptor_fingerprint: PhysicalDescriptorFingerprint,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn runtime_filter_descriptor_round_trips_through_postcard() {
		let descriptor = RuntimeFilterDescriptor {
			key_encoding_version: 1,
			key_types: vec![RuntimeFilterKeyType::Int64],
			probe_key_columns: vec![2],
			hash_seed: 7,
			bit_count: 16,
			hash_count: 2,
			byte_budget: 2,
			target_fp_rate_bp: 100,
			payload: vec![0b1010_0101, 0b0101_1010],
			probe_descriptor_fingerprint: PhysicalDescriptorFingerprint::from_bytes([6; 32]),
		};

		let encoded = postcard::to_allocvec(&descriptor).expect("encode runtime filter");
		let decoded: RuntimeFilterDescriptor =
			postcard::from_bytes(&encoded).expect("decode runtime filter");

		assert_eq!(decoded, descriptor);
	}

	#[test]
	fn runtime_filter_descriptor_preserves_version_field() {
		let descriptor = RuntimeFilterDescriptor {
			key_encoding_version: 42,
			key_types: vec![RuntimeFilterKeyType::Utf8],
			probe_key_columns: vec![1],
			hash_seed: 9,
			bit_count: 8,
			hash_count: 1,
			byte_budget: 1,
			target_fp_rate_bp: 250,
			payload: vec![0xFF],
			probe_descriptor_fingerprint: PhysicalDescriptorFingerprint::from_bytes([7; 32]),
		};

		let encoded = postcard::to_allocvec(&descriptor).expect("encode runtime filter");
		let decoded: RuntimeFilterDescriptor =
			postcard::from_bytes(&encoded).expect("decode runtime filter");

		assert_eq!(decoded.key_encoding_version, 42);
		assert_eq!(decoded, descriptor);
	}
}
