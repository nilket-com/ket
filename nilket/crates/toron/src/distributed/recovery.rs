//! ADR 626 Stage B: the unannounced-loss recovery experiment.
//!
//! The establishment deadline `H` and the confirmation deadline `C` are provisional experiment
//! values, not production defaults: they take effect only through an explicit, validated
//! [`RecoveryExperiment`], enabled on the coordinator and on every worker of a fixture. Unset,
//! nothing changes. `H` bounds establishment only (connect and stream open); an established,
//! silent execution keeps ADR 577's policy. `C` is one outer deadline around a whole
//! confirmation, never added after `H`.

use std::time::Duration;

/// At most this many replacement attempts per task, counted across every cause.
pub const REPLACEMENT_LIMIT: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryExperiment {
	establishment_deadline: Duration,
	confirmation_deadline: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecoveryConfigError {
	#[error("the establishment deadline must be nonzero")]
	ZeroEstablishmentDeadline,
	#[error("the confirmation deadline must be at least the establishment deadline")]
	ConfirmationShorterThanEstablishment,
	#[error("{name} of {millis} ms must stay below the QUIC idle timeout of {idle_millis} ms")]
	NotBelowIdleTimeout {
		name: &'static str,
		millis: u64,
		idle_millis: u64,
	},
}

impl RecoveryExperiment {
	/// Validates `H` and `C` in milliseconds. Both must be nonzero, `C >= H`, and both must stay
	/// below ADR 577's idle timeout (a deadline at or past it would bound nothing new).
	pub fn from_millis(
		establishment_millis: u64,
		confirmation_millis: u64,
	) -> Result<Self, RecoveryConfigError> {
		let idle_millis =
			u64::try_from(toron_client::QUIC_MAX_IDLE_TIMEOUT.as_millis()).unwrap_or(u64::MAX);
		if establishment_millis == 0 {
			return Err(RecoveryConfigError::ZeroEstablishmentDeadline);
		}
		if confirmation_millis < establishment_millis {
			return Err(RecoveryConfigError::ConfirmationShorterThanEstablishment);
		}
		for (name, millis) in [
			("the establishment deadline", establishment_millis),
			("the confirmation deadline", confirmation_millis),
		] {
			if millis >= idle_millis {
				return Err(RecoveryConfigError::NotBelowIdleTimeout {
					name,
					millis,
					idle_millis,
				});
			}
		}
		Ok(Self {
			establishment_deadline: Duration::from_millis(establishment_millis),
			confirmation_deadline: Duration::from_millis(confirmation_millis),
		})
	}

	/// `H`: connect plus stream open, at the scoped worker operations.
	pub const fn establishment_deadline(&self) -> Duration {
		self.establishment_deadline
	}

	/// `C`: one outer deadline from the start of a confirmation through its acknowledgement.
	pub const fn confirmation_deadline(&self) -> Duration {
		self.confirmation_deadline
	}

	pub fn establishment_deadline_millis(&self) -> u64 {
		u64::try_from(self.establishment_deadline.as_millis()).unwrap_or(u64::MAX)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_declared_values_validate() {
		let experiment = RecoveryExperiment::from_millis(5_000, 12_000).expect("H 5 s, C 12 s");
		assert_eq!(experiment.establishment_deadline(), Duration::from_secs(5));
		assert_eq!(experiment.confirmation_deadline(), Duration::from_secs(12));
	}

	#[test]
	fn zero_inverted_and_unbounded_values_refuse() {
		assert_eq!(
			RecoveryExperiment::from_millis(0, 12_000),
			Err(RecoveryConfigError::ZeroEstablishmentDeadline)
		);
		assert_eq!(
			RecoveryExperiment::from_millis(5_000, 4_999),
			Err(RecoveryConfigError::ConfirmationShorterThanEstablishment)
		);
		assert!(matches!(
			RecoveryExperiment::from_millis(5_000, 300_000),
			Err(RecoveryConfigError::NotBelowIdleTimeout { .. })
		));
		assert!(matches!(
			RecoveryExperiment::from_millis(u64::MAX, u64::MAX),
			Err(RecoveryConfigError::NotBelowIdleTimeout { .. })
		));
	}
}
