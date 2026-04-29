use crate::schema::{HybridTimestamp, TableRetentionPolicy};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EffectiveRetention {
	pub policy: TableRetentionPolicy,
	pub floor: Option<HybridTimestamp>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionReadError {
	pub as_of_hlc: HybridTimestamp,
	pub floor: HybridTimestamp,
}

pub fn cluster_retention_floor(
	cluster_policy: MvccRetentionPolicy,
	now_ms: u64,
) -> Option<HybridTimestamp> {
	let window = cluster_policy.window()?;
	let window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
	Some(retention_floor_from_window_ms(now_ms, window_ms))
}

pub fn effective_table_retention(
	cluster_policy: &MvccRetentionPolicy,
	table_policy: Option<&TableRetentionPolicy>,
	now_ms: u64,
) -> EffectiveRetention {
	let policy = table_policy
		.copied()
		.unwrap_or(TableRetentionPolicy::InheritCluster);
	let floor = match policy {
		TableRetentionPolicy::InheritCluster => cluster_retention_floor(*cluster_policy, now_ms),
		TableRetentionPolicy::Disabled => None,
		TableRetentionPolicy::FixedWindow(window) => {
			Some(retention_floor_from_window_ms(now_ms, window.get()))
		}
	};
	EffectiveRetention { policy, floor }
}

pub fn check_read_against_retention(
	as_of_hlc: HybridTimestamp,
	effective: &EffectiveRetention,
) -> Result<(), RetentionReadError> {
	let Some(floor) = effective.floor else {
		return Ok(());
	};
	if crate::storage::core::is_zero_hlc(as_of_hlc) || as_of_hlc >= floor {
		return Ok(());
	}
	Err(RetentionReadError { as_of_hlc, floor })
}

pub fn check_multi_table_read_against_retention<'a>(
	as_of_hlc: HybridTimestamp,
	effective_retentions: impl IntoIterator<Item = &'a EffectiveRetention>,
) -> Result<(), RetentionReadError> {
	for effective in effective_retentions {
		check_read_against_retention(as_of_hlc, effective)?;
	}
	Ok(())
}

pub fn row_visible_under_retention(
	row_hlc: HybridTimestamp,
	effective: &EffectiveRetention,
) -> bool {
	effective.floor.is_none_or(|floor| row_hlc >= floor)
}

fn retention_floor_from_window_ms(now_ms: u64, window_ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: now_ms.saturating_sub(window_ms),
		logical: 0,
		node_id: 0,
	}
}

#[cfg(test)]
mod tests {
	use super::{
		EffectiveRetention, MvccRetentionPolicy, check_multi_table_read_against_retention,
		check_read_against_retention, effective_table_retention, row_visible_under_retention,
	};
	use crate::schema::{HybridTimestamp, RetentionWindowMillis, TableRetentionPolicy};
	use std::time::Duration;

	fn hlc(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 0,
		}
	}

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

	#[test]
	fn effective_table_retention_resolves_three_modes() {
		let cluster = MvccRetentionPolicy::FixedWindow(Duration::from_secs(30));
		let now_ms = 100_000;
		assert_eq!(
			effective_table_retention(
				&cluster,
				Some(&TableRetentionPolicy::InheritCluster),
				now_ms
			)
			.floor,
			Some(hlc(70_000))
		);
		assert_eq!(
			effective_table_retention(&cluster, Some(&TableRetentionPolicy::Disabled), now_ms)
				.floor,
			None
		);
		let fixed =
			TableRetentionPolicy::FixedWindow(RetentionWindowMillis::try_new(60_000).unwrap());
		assert_eq!(
			effective_table_retention(&cluster, Some(&fixed), now_ms).floor,
			Some(hlc(40_000))
		);
	}

	#[test]
	fn read_admission_and_row_filter_use_floor() {
		let effective = EffectiveRetention {
			policy: TableRetentionPolicy::InheritCluster,
			floor: Some(hlc(50)),
		};
		assert!(check_read_against_retention(hlc(50), &effective).is_ok());
		assert!(check_read_against_retention(hlc(49), &effective).is_err());
		assert!(check_read_against_retention(hlc(0), &effective).is_ok());
		assert!(!row_visible_under_retention(hlc(49), &effective));
		assert!(row_visible_under_retention(hlc(50), &effective));
	}

	#[test]
	fn multi_table_read_admission_requires_all_tables() {
		let valid = EffectiveRetention {
			policy: TableRetentionPolicy::FixedWindow(
				RetentionWindowMillis::try_new(90_000).unwrap(),
			),
			floor: Some(hlc(20)),
		};
		let expired = EffectiveRetention {
			policy: TableRetentionPolicy::FixedWindow(
				RetentionWindowMillis::try_new(60_000).unwrap(),
			),
			floor: Some(hlc(50)),
		};

		assert!(check_multi_table_read_against_retention(hlc(60), [&valid, &expired]).is_ok());
		let err = check_multi_table_read_against_retention(hlc(40), [&valid, &expired])
			.expect_err("one expired table must reject the whole request");
		assert_eq!(err.floor, hlc(50));
	}
}
