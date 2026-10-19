//! The counter dedup horizon: which write keys a leader still accepts, and the
//! durable issuance cutoff `Q` that lets compaction delete applied records.
//!
//! A write key `W` is `client_uuid ‖ sequence_no LE`; its issuance time is the
//! unix milliseconds its UUIDv7 uuid carries (`crate::schema::uuid_v7_unix_millis`,
//! the one issuance predicate). With the attempt horizon `H`, the clock-skew
//! allowance `S` and the expiry `E`:
//!
//! - **E0, identity form.** An add whose uuid is not v7, or whose issuance is
//!   more than `S` ahead of the server clock, is refused
//!   (`WRITE_IDENTITY_INVALID`) before its write-key gate.
//! - **E1, no reapply.** Under the gate and after the record lookup, an add
//!   issued at or before `max(Q_mem, now - E)` is refused
//!   (`WRITE_OPERATION_EXPIRED`), whether or not a record was found.
//! - **E3, prune rule.** Compaction may delete an applied record only when its
//!   uuid is not v7 (E0 makes it unpresentable) or it was issued at or before
//!   the cutoff the new generation records, and no witness file of its
//!   operation exists.
//!
//! Every deleted `W` therefore has `issued(W) <= Q`, and `Q` never decreases and
//! survives restart, so a wall clock stepping back or a later change of `E`
//! never makes a deleted identity presentable again. `H`, `S`, `E` and clock
//! skew only decide liveness.
use std::io::{Error, ErrorKind};

use crate::schema::{
	COUNTER_ATTEMPT_HORIZON_MS, COUNTER_INCARNATION_LIFETIME_MS, uuid_v7_unix_millis,
};
use crate::storage::counter::CounterWriteKey;

/// The default clock-skew allowance `S`: how far ahead of the server clock a
/// write key's issuance may be. Operators run NTP within it.
pub const DEFAULT_COUNTER_CLOCK_SKEW_MS: u64 = 60_000;

/// The leader's dedup horizon. Validated once at startup; never changed live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CounterRetentionConfig {
	/// `H`: the longest a client keeps retrying one add.
	pub attempt_horizon_ms: u64,
	/// `S`: the tolerated lead of a client clock over the server clock.
	pub clock_skew_ms: u64,
	/// `E`: how long after issuance a write key stays presentable.
	pub expiry_ms: u64,
}

impl Default for CounterRetentionConfig {
	fn default() -> Self {
		Self {
			attempt_horizon_ms: COUNTER_ATTEMPT_HORIZON_MS,
			clock_skew_ms: DEFAULT_COUNTER_CLOCK_SKEW_MS,
			expiry_ms: COUNTER_INCARNATION_LIFETIME_MS
				+ COUNTER_ATTEMPT_HORIZON_MS
				+ DEFAULT_COUNTER_CLOCK_SKEW_MS,
		}
	}
}

impl CounterRetentionConfig {
	/// Refuse a zero value, `H` below the SDK's attempt horizon (Addendum A7:
	/// `E` must cover what the SDK actually retries), `S > H`, and
	/// `E < L_sdk + H + S`: the expiry must outlast an incarnation's whole life
	/// plus its last add's retries and skew.
	pub fn validate(&self) -> std::io::Result<()> {
		let invalid = |detail: &str| Err(Error::new(ErrorKind::InvalidInput, detail.to_owned()));
		if self.attempt_horizon_ms == 0 || self.clock_skew_ms == 0 || self.expiry_ms == 0 {
			return invalid("counter retention values must be greater than zero");
		}
		if self.attempt_horizon_ms < COUNTER_ATTEMPT_HORIZON_MS {
			return invalid("counter attempt horizon must be at least the SDK's attempt horizon");
		}
		if self.clock_skew_ms > self.attempt_horizon_ms {
			return invalid("counter clock-skew allowance must not exceed the attempt horizon");
		}
		let floor = COUNTER_INCARNATION_LIFETIME_MS
			.checked_add(self.attempt_horizon_ms)
			.and_then(|sum| sum.checked_add(self.clock_skew_ms));
		match floor {
			Some(floor) if self.expiry_ms >= floor => Ok(()),
			_ => invalid(
				"counter expiry must be at least the client incarnation lifetime plus the attempt horizon plus the clock-skew allowance",
			),
		}
	}
}

/// The issuance time of `write_key`, or `None` when its uuid is not v7.
pub(super) fn issued_ms(write_key: &CounterWriteKey) -> Option<u64> {
	let mut uuid = [0u8; 16];
	uuid.copy_from_slice(&write_key[..16]);
	uuid_v7_unix_millis(uuid)
}

/// E0: the write key is a v7 identity issued no more than `S` after `now_ms`.
pub(super) fn presentable(
	write_key: &CounterWriteKey,
	now_ms: u64,
	config: &CounterRetentionConfig,
) -> bool {
	issued_ms(write_key).is_some_and(|issued| issued <= now_ms.saturating_add(config.clock_skew_ms))
}

/// E1: the issuance time at or before which an add is refused.
pub(super) fn expiry_cutoff(cutoff: u64, now_ms: u64, config: &CounterRetentionConfig) -> u64 {
	cutoff.max(now_ms.saturating_sub(config.expiry_ms))
}

/// The cutoff a compaction proposes from `now_ms`. The clock is bounded by the
/// newest accepted issuance plus `E`, so a forward clock jump cannot carry `Q`
/// past the traffic the store actually accepted.
pub(super) fn compaction_cutoff(
	now_ms: u64,
	max_issued_accepted: u64,
	config: &CounterRetentionConfig,
) -> u64 {
	let bounded = now_ms.min(max_issued_accepted.saturating_add(config.expiry_ms));
	bounded.saturating_sub(config.expiry_ms)
}

/// E3: a record may be deleted under `cutoff` when no witness file of its
/// operation exists and it is either not a v7 identity or issued by `cutoff`.
pub(super) fn prunable(write_key: &CounterWriteKey, cutoff: u64, witnessed: bool) -> bool {
	!witnessed && issued_ms(write_key).is_none_or(|issued| issued <= cutoff)
}

#[cfg(test)]
mod tests {
	use super::*;

	// T-CFGVAL: the defaults validate; the expiry floor is exact; zero values and
	// a skew allowance above the attempt horizon are refused.
	#[test]
	fn retention_config_validates_its_floor_and_ranges() {
		let defaults = CounterRetentionConfig::default();
		defaults.validate().unwrap();
		assert_eq!(
			defaults.expiry_ms,
			COUNTER_INCARNATION_LIFETIME_MS + defaults.attempt_horizon_ms + defaults.clock_skew_ms
		);
		let below = CounterRetentionConfig {
			expiry_ms: defaults.expiry_ms - 1,
			..defaults
		};
		let zero_h = CounterRetentionConfig {
			attempt_horizon_ms: 0,
			..defaults
		};
		let zero_s = CounterRetentionConfig {
			clock_skew_ms: 0,
			..defaults
		};
		let zero_e = CounterRetentionConfig {
			expiry_ms: 0,
			..defaults
		};
		let skew_above_horizon = CounterRetentionConfig {
			attempt_horizon_ms: 600_000,
			clock_skew_ms: 600_001,
			expiry_ms: u64::MAX,
		};
		let overflow = CounterRetentionConfig {
			attempt_horizon_ms: u64::MAX,
			clock_skew_ms: 1,
			expiry_ms: u64::MAX,
		};
		for invalid in [below, zero_h, zero_s, zero_e, skew_above_horizon, overflow] {
			assert_eq!(
				invalid.validate().map_err(|error| error.kind()),
				Err(ErrorKind::InvalidInput),
				"{invalid:?}"
			);
		}
	}

	// T-CFGVAL (Addendum A7): `H` one millisecond below the SDK's attempt
	// horizon is refused; `H` equal to it is accepted.
	#[test]
	fn attempt_horizon_is_floored_at_the_sdk_horizon() {
		let with_horizon = |attempt_horizon_ms: u64| CounterRetentionConfig {
			attempt_horizon_ms,
			clock_skew_ms: DEFAULT_COUNTER_CLOCK_SKEW_MS,
			expiry_ms: COUNTER_INCARNATION_LIFETIME_MS
				+ attempt_horizon_ms
				+ DEFAULT_COUNTER_CLOCK_SKEW_MS,
		};
		assert_eq!(
			with_horizon(COUNTER_ATTEMPT_HORIZON_MS - 1)
				.validate()
				.map_err(|error| error.kind()),
			Err(ErrorKind::InvalidInput)
		);
		with_horizon(COUNTER_ATTEMPT_HORIZON_MS).validate().unwrap();
	}

	// The predicates at their boundaries.
	#[test]
	fn horizon_predicates_hold_at_their_boundaries() {
		let config = CounterRetentionConfig::default();
		let mut key = [0u8; 24];
		key[..6].copy_from_slice(&1_000_000u64.to_be_bytes()[2..]);
		key[6] = 0x70;
		key[8] = 0x80;
		assert_eq!(issued_ms(&key), Some(1_000_000));
		assert!(presentable(&key, 1_000_000 - config.clock_skew_ms, &config));
		assert!(!presentable(
			&key,
			1_000_000 - config.clock_skew_ms - 1,
			&config
		));
		assert_eq!(
			expiry_cutoff(7, 1_000_000 + config.expiry_ms, &config),
			1_000_000
		);
		assert_eq!(expiry_cutoff(2_000_000, 0, &config), 2_000_000);
		// A clock far ahead of the newest accepted issuance is capped at it plus E.
		assert_eq!(compaction_cutoff(u64::MAX, 1_000_000, &config), 1_000_000);
		assert!(prunable(&key, 1_000_000, false));
		assert!(!prunable(&key, 999_999, false));
		assert!(!prunable(&key, 1_000_000, true), "a witness file keeps it");
		let mut v4 = key;
		v4[6] = 0x40;
		assert!(prunable(&v4, 0, false), "a non-v7 record is unpresentable");
		assert!(!presentable(&v4, u64::MAX, &config));
	}
}
