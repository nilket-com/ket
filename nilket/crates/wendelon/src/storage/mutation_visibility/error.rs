/// Bounded errors deliberately carry no customer bytes, digests or secret material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityError {
	InvalidIdentity,
	InvalidManifest,
	InvalidCoordinate,
	InvalidDecision,
	InvalidCut,
	LimitExceeded,
	Truncated,
	TrailingBytes,
	UnknownFormat,
	Corrupt,
	UnorderedReplay,
	CoordinateConflict,
	WrongGeneration,
	Poisoned,
}
impl std::fmt::Display for VisibilityError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "visibility authority: {self:?}")
	}
}
impl std::error::Error for VisibilityError {}

macro_rules! redacted_debug {
	($name:ty, $label:literal) => {
		impl std::fmt::Debug for $name {
			fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
				f.write_str(concat!($label, " { redacted }"))
			}
		}
	};
}
pub(super) use redacted_debug;
