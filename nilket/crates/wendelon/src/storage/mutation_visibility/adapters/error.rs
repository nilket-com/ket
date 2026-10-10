use crate::storage::mutation_visibility::{CommitmentError, VisibilityError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterError {
	UnsupportedType,
	SchemaMismatch,
	InvalidValue,
	KeyChanged,
	LimitExceeded,
	InvalidProvenance,
	KeyUnavailable,
}
impl From<VisibilityError> for AdapterError {
	fn from(e: VisibilityError) -> Self {
		if e == VisibilityError::LimitExceeded {
			Self::LimitExceeded
		} else {
			Self::InvalidProvenance
		}
	}
}
impl From<CommitmentError> for AdapterError {
	fn from(e: CommitmentError) -> Self {
		match e {
			CommitmentError::Key(_) => Self::KeyUnavailable,
			CommitmentError::InvalidInput(e) => e.into(),
		}
	}
}
pub(super) const KEY_MAGIC: &[u8; 8] = b"VTKEY001";
pub(super) const ROW_MAGIC: &[u8; 8] = b"VTROW001";
