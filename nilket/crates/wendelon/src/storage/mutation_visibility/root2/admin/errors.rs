#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum AdminManifestError2 {
	OverReadBound,
	Length,
	Magic,
	UnsupportedFormat,
	UnsupportedProfile,
	ZeroField,
	Namespace,
	NonCanonicalKey,
	NotCanonical,
	ActionThree(ManifestRole3),
	ReuseThree { pair: RolePair3, field: ReuseField3 },
	KeyThree(ManifestRole3),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum ManifestRole3 {
	Admin,
	Governance,
	Recovery,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum RolePair3 {
	AdminGovernance,
	AdminRecovery,
	GovernanceRecovery,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum ReuseField3 {
	Id,
	Key,
}
