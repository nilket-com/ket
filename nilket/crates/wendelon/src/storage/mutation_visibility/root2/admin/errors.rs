#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum AdminManifestError2 {
	OverReadBound,
	Length,
	Magic,
	UnsupportedFormat,
	UnsupportedProfile,
	ZeroField,
	UnsupportedAction,
	Namespace,
	NonCanonicalKey,
	NotCanonical,
	DigestMismatch,
}
pub(in crate::storage::mutation_visibility::root2) type AdminResult2<T> =
	Result<T, AdminManifestError2>;
