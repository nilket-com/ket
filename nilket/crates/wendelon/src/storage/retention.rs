use std::io::{Error, ErrorKind};
use std::time::Duration;

/// Cluster-local MVCC history retention policy.
///
/// Today W8 only implements a fixed global window or a disabled mode,
/// but carrying a policy enum through bootstrap avoids baking
/// `Option<Duration>` into the long-lived config surface and leaves room
/// for future per-tenant or class-based retention families.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MvccRetentionPolicy {
	Disabled,
	FixedWindow(Duration),
}

impl MvccRetentionPolicy {
	pub const fn disabled() -> Self {
		Self::Disabled
	}

	pub const fn fixed_window(window: Duration) -> Self {
		Self::FixedWindow(window)
	}

	pub fn window(self) -> Option<Duration> {
		match self {
			Self::Disabled => None,
			Self::FixedWindow(window) => Some(window),
		}
	}

	pub fn validate(self) -> std::io::Result<()> {
		match self {
			Self::Disabled => Ok(()),
			Self::FixedWindow(window) if window.is_zero() => Err(Error::new(
				ErrorKind::InvalidInput,
				"mvcc_retention_policy fixed window must be greater than zero",
			)),
			Self::FixedWindow(_) => Ok(()),
		}
	}

	pub fn from_optional_window(window: Option<Duration>) -> Self {
		match window {
			Some(window) if !window.is_zero() => Self::FixedWindow(window),
			_ => Self::Disabled,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::MvccRetentionPolicy;
	use std::time::Duration;

	#[test]
	fn optional_window_helpers_preserve_disabled_and_fixed_modes() {
		assert_eq!(
			MvccRetentionPolicy::from_optional_window(None),
			MvccRetentionPolicy::Disabled
		);
		assert_eq!(
			MvccRetentionPolicy::from_optional_window(Some(Duration::ZERO)),
			MvccRetentionPolicy::Disabled
		);
		assert_eq!(
			MvccRetentionPolicy::from_optional_window(Some(Duration::from_secs(30))),
			MvccRetentionPolicy::FixedWindow(Duration::from_secs(30))
		);
	}

	#[test]
	fn validate_rejects_zero_fixed_window() {
		assert!(
			MvccRetentionPolicy::FixedWindow(Duration::ZERO)
				.validate()
				.is_err()
		);
		assert!(MvccRetentionPolicy::Disabled.validate().is_ok());
	}
}
