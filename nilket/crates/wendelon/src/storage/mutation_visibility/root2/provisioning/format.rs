//! Explicit directory format dispatch. No format is inferred from log length.
use super::{inventory as log, owner_core as core};
use crate::storage::mutation_visibility::root2::types::Digest2;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum DirectoryFormat2 {
	One,
	Two,
}
impl DirectoryFormat2 {
	pub(super) fn max_log(self) -> usize {
		match self {
			Self::One => log::MAX_LOG_BYTES2,
			Self::Two => log::v2::MAX_LOG_BYTES2,
		}
	}
	pub(super) fn max_evidence(self) -> usize {
		match self {
			Self::One => core::MAX_EVIDENCE_BYTES2,
			Self::Two => core::v2::MAX_EVIDENCE_BYTES2,
		}
	}
	pub(super) fn fold(self, b: &[u8]) -> Result<log::LogState2, log::LogError2> {
		match self {
			Self::One => log::fold_events2(b),
			Self::Two => log::v2::fold_events2(b),
		}
	}
	pub(super) fn encode(self, e: &log::Event2) -> Result<Vec<u8>, log::LogError2> {
		match self {
			Self::One => log::encode_event2(e).map(|b| b.to_vec()),
			Self::Two => log::v2::encode_event2(e).map(|b| b.to_vec()),
		}
	}
	pub(super) fn decide(
		self,
		b: &[u8],
		op: &core::OpId2,
		req: &core::OwnerRequest2,
	) -> core::Decision2 {
		match self {
			Self::One => core::decide2(b, op, req),
			Self::Two => core::v2::decide2(b, op, req),
		}
	}
	pub(super) fn classify(self, b: &[u8], e: &[(&str, &[u8])]) -> core::Classification2 {
		match self {
			Self::One => core::classify2(b, e),
			Self::Two => core::v2::classify2(b, e),
		}
	}
	pub(super) fn decode_evidence(
		self,
		n: &str,
		b: &[u8],
	) -> Result<core::RepairEvidence2, core::EvidenceError2> {
		match self {
			Self::One => core::decode_evidence2(n, b),
			Self::Two => core::v2::decode_evidence2(n, b),
		}
	}
	pub(super) fn encode_evidence(
		self,
		b: &[u8],
	) -> Result<(String, Vec<u8>), core::EvidenceError2> {
		match self {
			Self::One => core::encode_evidence2(b),
			Self::Two => core::v2::encode_evidence2(b),
		}
	}
	pub(super) fn context(self, e: &core::RepairEvidence2) -> Digest2 {
		match self {
			Self::One => {
				core::repair_context2(&e.prefix_sha256, e.boundary, e.original_len, &e.tail_sha256)
			}
			Self::Two => core::v2::repair_context2(
				&e.prefix_sha256,
				e.boundary,
				e.original_len,
				&e.tail_sha256,
			),
		}
	}
	pub(super) fn expected(
		self,
		e: &core::RepairEvidence2,
	) -> Result<Vec<u8>, core::EvidenceError2> {
		match self {
			Self::One => core::expected_repair_event2(e).map(|b| b.to_vec()),
			Self::Two => core::v2::expected_repair_event2(e).map(|b| b.to_vec()),
		}
	}
}
