use crate::storage::mutation_visibility::root2::RootError2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum AuthError2 {
	Structural,
	Limit,
	Overflow,
	ScopeMismatch,
	CryptoInvalid,
}
pub(in crate::storage::mutation_visibility::root2) type AuthResult2<T> = Result<T, AuthError2>;
impl From<RootError2> for AuthError2 {
	fn from(error: RootError2) -> Self {
		match error {
			RootError2::Limit => Self::Limit,
			RootError2::Overflow => Self::Overflow,
			RootError2::TrustMismatch => Self::ScopeMismatch,
			_ => Self::Structural,
		}
	}
}
