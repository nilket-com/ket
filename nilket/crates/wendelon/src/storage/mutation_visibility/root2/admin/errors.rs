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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum BootstrapRecordError2 {
	Length,
	Magic,
	UnsupportedFormat,
	UnsupportedProfile,
	ZeroField,
	InvalidLog,
	Sequence,
	NotCanonical,
}
pub(in crate::storage::mutation_visibility::root2) type BootstrapResult2<T> =
	Result<T, BootstrapRecordError2>;
