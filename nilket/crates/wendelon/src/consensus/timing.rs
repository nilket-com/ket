//! The control group's consensus timing profile.
//!
//! One declared number, the timing base, decides every timing value openraft is given.
//! [`ConsensusTiming::apply`] is the only code that writes those fields, so no node can run
//! with a heartbeat from one source and an election timeout from another.
//!
//! The values are nominal: openraft's timeouts are cooperative (a call that completes is
//! returned even when it is polled after its timer), and each covers a whole call, not a
//! network round trip. A profile is a declaration of how patient the group is, not a latency
//! guarantee.

use openraft::Config;
use serde_json::{Map, Value};

/// A validated timing base and the openraft values derived from it, in milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsensusTiming {
	base: u64,
	heartbeat: u64,
	election_min: u64,
	election_max: u64,
	snapshot_chunk: u64,
}

/// The timing base is outside the accepted range, or its derived values do not fit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsensusTimingOutOfRange;

impl ConsensusTiming {
	pub const MIN_BASE_MILLIS: u64 = 10;
	pub const MAX_BASE_MILLIS: u64 = 1000;
	/// The base whose derived values are openraft's own defaults.
	pub const DEFAULT_BASE_MILLIS: u64 = 50;

	/// The profile of `base` milliseconds: heartbeat `base`, election timeout `3 base` to
	/// `6 base`, snapshot chunk `4 base` and never under 200. Refused outside
	/// [`Self::MIN_BASE_MILLIS`]..=[`Self::MAX_BASE_MILLIS`]; nothing is clamped.
	pub fn from_base_millis(base: u64) -> Result<Self, ConsensusTimingOutOfRange> {
		if !(Self::MIN_BASE_MILLIS..=Self::MAX_BASE_MILLIS).contains(&base) {
			return Err(ConsensusTimingOutOfRange);
		}
		let times = |factor: u64| base.checked_mul(factor).ok_or(ConsensusTimingOutOfRange);
		Ok(Self {
			base,
			heartbeat: base,
			election_min: times(3)?,
			election_max: times(6)?,
			snapshot_chunk: times(4)?.max(200),
		})
	}

	pub fn base_millis(self) -> u64 {
		self.base
	}

	/// `config` with its four timing fields set from this profile and nothing else changed.
	pub(crate) fn apply(self, config: Config) -> Config {
		Config {
			heartbeat_interval: self.heartbeat,
			election_timeout_min: self.election_min,
			election_timeout_max: self.election_max,
			install_snapshot_timeout: self.snapshot_chunk,
			..config
		}
	}

	/// What a node reports: its declared base beside the values in `effective`, the
	/// configuration its Raft instance was actually built with.
	pub(crate) fn status_of(self, effective: &Config) -> ConsensusTimingStatus {
		ConsensusTimingStatus {
			base_millis: self.base,
			heartbeat_millis: effective.heartbeat_interval,
			election_min_millis: effective.election_timeout_min,
			election_max_millis: effective.election_timeout_max,
			snapshot_chunk_millis: effective.install_snapshot_timeout,
		}
	}
}

impl Default for ConsensusTiming {
	fn default() -> Self {
		Self::from_base_millis(Self::DEFAULT_BASE_MILLIS).expect("the default base is in range")
	}
}

/// The timing a node's Raft instance was built with, for its readiness answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsensusTimingStatus {
	pub base_millis: u64,
	pub heartbeat_millis: u64,
	pub election_min_millis: u64,
	pub election_max_millis: u64,
	pub snapshot_chunk_millis: u64,
}

impl ConsensusTimingStatus {
	/// The status of a node on the default profile.
	pub fn default_profile() -> Self {
		let timing = ConsensusTiming::default();
		timing.status_of(&timing.apply(Config::default()))
	}

	pub fn to_json(&self) -> Value {
		let mut body = Map::new();
		for (name, millis) in [
			("base_millis", self.base_millis),
			("heartbeat_millis", self.heartbeat_millis),
			("election_min_millis", self.election_min_millis),
			("election_max_millis", self.election_max_millis),
			("snapshot_chunk_millis", self.snapshot_chunk_millis),
		] {
			body.insert(name.into(), Value::from(millis));
		}
		Value::Object(body)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn fields(config: &Config) -> [u64; 4] {
		[
			config.heartbeat_interval,
			config.election_timeout_min,
			config.election_timeout_max,
			config.install_snapshot_timeout,
		]
	}

	/// The default profile is the dependency's own defaults, read from the dependency: if
	/// openraft's defaults move, this fails instead of the product's timing moving silently.
	#[test]
	fn the_default_profile_is_openrafts_default_timing() {
		let dependency = Config::default();
		let applied = ConsensusTiming::default().apply(Config::default());
		assert_eq!(fields(&applied), fields(&dependency));
		assert_eq!(fields(&dependency), [50, 150, 300, 200]);
		// Nothing but the four timing fields is touched.
		assert_eq!(format!("{applied:?}"), format!("{dependency:?}"));
	}

	#[test]
	fn each_profile_derives_its_four_values_from_the_base_alone() {
		for (base, expected) in [
			(10, [10, 30, 60, 200]),
			(50, [50, 150, 300, 200]),
			(250, [250, 750, 1500, 1000]),
			(1000, [1000, 3000, 6000, 4000]),
		] {
			let timing = ConsensusTiming::from_base_millis(base).unwrap();
			let applied = timing.apply(Config::default());
			assert_eq!(fields(&applied), expected, "base {base}");
			assert_eq!(timing.base_millis(), base);
			let status = timing.status_of(&applied);
			assert_eq!(
				[
					status.base_millis,
					status.heartbeat_millis,
					status.election_min_millis,
					status.election_max_millis,
					status.snapshot_chunk_millis
				],
				[base, expected[0], expected[1], expected[2], expected[3]]
			);
		}
	}

	/// Every accepted base gives a configuration openraft itself accepts, with values that
	/// never fall as the base rises.
	#[test]
	fn every_accepted_base_validates_and_the_values_never_fall() {
		let mut previous = [0; 4];
		for base in ConsensusTiming::MIN_BASE_MILLIS..=ConsensusTiming::MAX_BASE_MILLIS {
			let applied = ConsensusTiming::from_base_millis(base)
				.unwrap()
				.apply(Config::default());
			let now = fields(&applied);
			assert!(now.iter().zip(previous).all(|(now, before)| *now >= before));
			previous = now;
			applied
				.validate()
				.unwrap_or_else(|error| panic!("base {base}: {error}"));
		}
	}

	#[test]
	fn a_base_outside_the_range_is_refused_and_never_clamped() {
		for base in [0, 1, 9, 1001, 5000, u64::MAX / 3, u64::MAX] {
			assert_eq!(
				ConsensusTiming::from_base_millis(base),
				Err(ConsensusTimingOutOfRange),
				"base {base}"
			);
		}
		assert!(ConsensusTiming::from_base_millis(10).is_ok());
		assert!(ConsensusTiming::from_base_millis(1000).is_ok());
	}

	#[test]
	fn the_status_is_reported_as_five_numbers() {
		assert_eq!(
			ConsensusTimingStatus::default_profile().to_json(),
			serde_json::json!({
				"base_millis": 50,
				"heartbeat_millis": 50,
				"election_min_millis": 150,
				"election_max_millis": 300,
				"snapshot_chunk_millis": 200,
			})
		);
	}
}
