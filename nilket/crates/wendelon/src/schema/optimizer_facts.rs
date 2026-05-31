use crate::schema::{PhysicalDescriptorFingerprint, TableObjectId};
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableOptimizerFacts {
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
	pub producer_epoch: u64,
	#[serde(default)]
	pub column_ndv: Vec<ColumnNdvFact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnNdvFact {
	pub column_ordinal: u16,
	pub distinct: NdvFact,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NdvFact {
	Exact { count: u64 },
	Approx { estimate: u64, rel_err_ppm: u32 },
}

pub fn encode_table_optimizer_facts(facts: &TableOptimizerFacts) -> std::io::Result<Vec<u8>> {
	postcard::to_allocvec(facts).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to encode table optimizer facts: {err}"),
		)
	})
}

pub fn decode_table_optimizer_facts(bytes: &[u8]) -> std::io::Result<TableOptimizerFacts> {
	postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to decode table optimizer facts: {err}"),
		)
	})
}
